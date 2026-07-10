//! DistributedExecutor: drop-in replacement for the local TaskExecutor.
//!
//! Instead of running tasks locally, it translates SchedulerCommand::DispatchTask
//! into TaskAssignment messages and routes them through the Coordinator to remote
//! workers. Results flow back via the Coordinator's result channel.
//!
//! # Integration
//!
//! The distributed executor plugs into the same MPSC channel interface that the
//! local executor uses, making it transparent to the scheduler:
//!
//! ```text
//!   Scheduler ─── cmd_rx ───▶ DistributedExecutor ─── Coordinator ─── Workers
//!   Scheduler ◀── evt_tx ──── DistributedExecutor ◀── Coordinator ◀── Workers
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::coordinator::{Coordinator, CoordinatorConfig};
use crate::proto_types::*;

/// Configuration for the distributed executor.
#[derive(Debug, Clone)]
pub struct DistributedExecutorConfig {
    /// Coordinator configuration.
    pub coordinator: CoordinatorConfig,

    /// Default task timeout in seconds.
    pub default_timeout_secs: u64,
}

impl Default for DistributedExecutorConfig {
    fn default() -> Self {
        Self {
            coordinator: CoordinatorConfig::default(),
            default_timeout_secs: 3600,
        }
    }
}

/// Execution mode: local, distributed, or hybrid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// All tasks run locally (single-node, current behavior).
    Local,
    /// All tasks are distributed to workers.
    Distributed,
    /// Tasks with pool affinity go to workers; others run locally.
    Hybrid,
}

/// A task dispatch request from the scheduler (mirrors the scheduler's command).
#[derive(Debug, Clone)]
pub struct DispatchRequest {
    pub dag_id: String,
    pub run_id: String,
    pub task_id: String,
    pub attempt: u32,
    pub task_type: TaskType,
    pub script: String,
    pub connection: String,
    pub query: String,
    pub command: String,
    pub args: Vec<String>,
    pub timeout_secs: u64,
    pub pool: String,
    pub logical_date: DateTime<Utc>,
    pub environment: String,
    pub params: HashMap<String, String>,
    pub resources: ResourceLimits,
}

/// Result callback that maps back to the scheduler's event model.
#[derive(Debug, Clone)]
pub struct DispatchResult {
    pub dag_id: String,
    pub run_id: String,
    pub task_id: String,
    pub attempt: u32,
    pub success: bool,
    pub duration_ms: u64,
    pub error: Option<String>,
    pub xcom: Option<serde_json::Value>,
    pub metrics: HashMap<String, f64>,
}

/// The distributed executor manages the coordinator and translates
/// between the scheduler's command model and the distributed protocol.
pub struct DistributedExecutor {
    config: DistributedExecutorConfig,
    coordinator: Arc<Coordinator>,
    result_rx: mpsc::UnboundedReceiver<TaskResult>,
}

impl DistributedExecutor {
    /// Create a new distributed executor with a non-durable coordinator
    /// (in-flight state is lost if the process restarts).
    pub fn new(config: DistributedExecutorConfig) -> Self {
        let (coordinator, result_rx) = Coordinator::new(config.coordinator.clone());

        Self {
            config,
            coordinator: Arc::new(coordinator),
            result_rx,
        }
    }

    /// Create a distributed executor whose coordinator persists in-flight
    /// assignments to RocksDB at `store_path` (typically
    /// `{state_dir}/coordinator_assignments`) and recovers them on startup
    /// (PRD E3). If the process restarts, tasks that were in flight are
    /// reconstructed and re-queued for dispatch instead of being lost.
    pub async fn with_persistence(
        config: DistributedExecutorConfig,
        store_path: &std::path::Path,
    ) -> Result<Self, rocksdb::Error> {
        let store = Arc::new(crate::assignment_store::RocksAssignmentStore::open(
            store_path,
        )?);
        let (coordinator, result_rx) = Coordinator::with_store(config.coordinator.clone(), store);
        let coordinator = Arc::new(coordinator);
        coordinator.recover().await;

        Ok(Self {
            config,
            coordinator,
            result_rx,
        })
    }

    /// Get a reference to the coordinator.
    pub fn coordinator(&self) -> &Arc<Coordinator> {
        &self.coordinator
    }

    /// Dispatch a task for distributed execution.
    pub async fn dispatch(&self, req: DispatchRequest) {
        let spec = TaskSpec {
            task_type: req.task_type,
            script: req.script,
            connection: req.connection,
            query: req.query,
            command: req.command,
            args: req.args,
            timeout_secs: if req.timeout_secs > 0 {
                req.timeout_secs
            } else {
                self.config.default_timeout_secs
            },
            resources: req.resources,
        };

        let context = TaskContext {
            dag_id: req.dag_id.clone(),
            run_id: req.run_id.clone(),
            task_id: req.task_id.clone(),
            attempt: req.attempt,
            logical_date_epoch_ms: req.logical_date.timestamp_millis(),
            environment: req.environment,
            params: req.params,
        };

        let timeout = spec.timeout_secs;
        let assignment = self.coordinator.create_assignment(
            &req.dag_id,
            &req.run_id,
            &req.task_id,
            req.attempt,
            spec,
            context,
            timeout,
        );

        let pool = if req.pool.is_empty() {
            "default"
        } else {
            &req.pool
        };

        self.coordinator.submit_task(assignment, pool).await;
    }

    /// Receive the next completed task result.
    ///
    /// Returns None if all senders have been dropped.
    pub async fn recv_result(&mut self) -> Option<DispatchResult> {
        let result = self.result_rx.recv().await?;

        let xcom = if result.xcom_json.is_empty() {
            None
        } else {
            serde_json::from_str(&result.xcom_json).ok()
        };

        Some(DispatchResult {
            dag_id: result.dag_id,
            run_id: result.run_id,
            task_id: result.task_id,
            attempt: result.attempt,
            success: result.outcome == TaskOutcome::Success,
            duration_ms: result.duration_ms,
            error: if result.error.is_empty() {
                None
            } else {
                Some(result.error)
            },
            xcom,
            metrics: result.metrics,
        })
    }

    /// Start the coordinator's health checker background task.
    pub fn start_health_checker(&self) -> tokio::task::JoinHandle<()> {
        self.coordinator.start_health_checker()
    }

    /// Get cluster status.
    pub fn cluster_status(&self) -> ClusterStatusResponse {
        self.coordinator.cluster_status()
    }

    /// Number of pending tasks.
    pub async fn pending_count(&self) -> usize {
        self.coordinator.pending_count().await
    }

    /// Number of inflight tasks.
    pub fn inflight_count(&self) -> usize {
        self.coordinator.inflight_count()
    }
}

/// The run loop that bridges the scheduler's MPSC channels to the
/// distributed executor.
///
/// This function replaces the local executor loop in `conduit-cli/src/main.rs`:
///
/// ```rust,ignore
/// // Before (local):
/// while let Some(cmd) = cmd_rx.recv().await {
///     match cmd {
///         SchedulerCommand::DispatchTask { .. } => {
///             ProcessRunner::run(task, context).await;
///             event_tx.send(TaskCompleted { .. });
///         }
///     }
/// }
///
/// // After (distributed):
/// distributed_executor::run(cmd_rx, event_tx, config).await;
/// ```
pub async fn run_distributed_loop(
    mut dispatch_rx: mpsc::UnboundedReceiver<DispatchRequest>,
    result_tx: mpsc::UnboundedSender<DispatchResult>,
    config: DistributedExecutorConfig,
) {
    let mut executor = DistributedExecutor::new(config);
    let _health_handle = executor.start_health_checker();

    info!("Distributed executor loop started");

    loop {
        tokio::select! {
            // Receive dispatch requests from the scheduler.
            Some(req) = dispatch_rx.recv() => {
                executor.dispatch(req).await;
            }

            // Receive results from workers via coordinator.
            Some(result) = executor.recv_result() => {
                if let Err(e) = result_tx.send(result) {
                    error!(error = %e, "Failed to forward result to scheduler");
                }
            }

            // Both channels closed.
            else => {
                info!("Distributed executor loop shutting down");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distributed_executor_creation() {
        let executor = DistributedExecutor::new(DistributedExecutorConfig::default());
        assert_eq!(executor.inflight_count(), 0);
    }

    #[tokio::test]
    async fn test_dispatch_queues_when_no_workers() {
        let executor = DistributedExecutor::new(DistributedExecutorConfig::default());

        executor
            .dispatch(DispatchRequest {
                dag_id: "dag1".to_string(),
                run_id: "run1".to_string(),
                task_id: "task1".to_string(),
                attempt: 0,
                task_type: TaskType::Bash,
                script: "echo hi".to_string(),
                connection: String::new(),
                query: String::new(),
                command: String::new(),
                args: vec![],
                timeout_secs: 300,
                pool: "default".to_string(),
                logical_date: Utc::now(),
                environment: "dev".to_string(),
                params: HashMap::new(),
                resources: ResourceLimits::default(),
            })
            .await;

        // No workers → task should be pending
        assert_eq!(executor.pending_count().await, 1);
        assert_eq!(executor.inflight_count(), 0);
    }

    #[tokio::test]
    async fn test_dispatch_with_worker() {
        let executor = DistributedExecutor::new(DistributedExecutorConfig::default());

        // Register a worker.
        let _worker_rx = executor.coordinator().register_worker(&RegisterRequest {
            worker_id: "w1".to_string(),
            hostname: "w1.local".to_string(),
            capacity: 4,
            pool_affinity: vec!["default".to_string()],
            labels: HashMap::new(),
            version: "0.1.0".to_string(),
            health_port: 0,
        });

        executor
            .dispatch(DispatchRequest {
                dag_id: "dag1".to_string(),
                run_id: "run1".to_string(),
                task_id: "task1".to_string(),
                attempt: 0,
                task_type: TaskType::Bash,
                script: "echo hi".to_string(),
                connection: String::new(),
                query: String::new(),
                command: String::new(),
                args: vec![],
                timeout_secs: 300,
                pool: "default".to_string(),
                logical_date: Utc::now(),
                environment: "dev".to_string(),
                params: HashMap::new(),
                resources: ResourceLimits::default(),
            })
            .await;

        // Worker available → task should be inflight
        assert_eq!(executor.pending_count().await, 0);
        assert_eq!(executor.inflight_count(), 1);
    }

    #[test]
    fn test_cluster_status_empty() {
        let executor = DistributedExecutor::new(DistributedExecutorConfig::default());
        let status = executor.cluster_status();
        assert_eq!(status.health, ClusterHealth::Unhealthy);
        assert_eq!(status.workers.len(), 0);
    }

    #[test]
    fn test_execution_mode_variants() {
        assert_ne!(ExecutionMode::Local, ExecutionMode::Distributed);
        assert_ne!(ExecutionMode::Distributed, ExecutionMode::Hybrid);
    }
}
