//! Coordinator: the leader node that manages workers and distributes tasks.
//!
//! The coordinator runs alongside the scheduler. It:
//! 1. Accepts worker registrations and maintains the worker pool
//! 2. Receives task dispatch commands from the scheduler
//! 3. Routes tasks to workers based on pool affinity and load
//! 4. Collects task results and forwards them back to the scheduler
//! 5. Monitors worker health via heartbeats
//! 6. Reassigns tasks from dead workers
//!
//! # Architecture
//!
//! ```text
//!   Scheduler ──cmd_rx──▶ Coordinator ──gRPC──▶ Workers
//!   Scheduler ◀──evt_tx── Coordinator ◀─gRPC─── Workers
//! ```

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use dashmap::DashMap;
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::proto_types::*;
use crate::worker_pool::{RoutingStrategy, WorkerPool};

/// Configuration for the coordinator.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Address to bind the gRPC server (e.g., "0.0.0.0:9400").
    pub bind_addr: String,

    /// How often to check worker health (seconds).
    pub health_check_interval_secs: u64,

    /// Task routing strategy.
    pub routing_strategy: RoutingStrategy,

    /// Maximum number of tasks to queue before applying backpressure.
    pub max_queue_size: usize,

    /// How long a task can be assigned before the coordinator considers it
    /// stuck and potentially reassigns it (seconds).
    pub task_timeout_secs: u64,

    /// Path to the TLS certificate PEM file for the gRPC server.
    /// When set (along with `tls_key_path`), the server uses TLS.
    pub tls_cert_path: Option<String>,

    /// Path to the TLS private key PEM file for the gRPC server.
    pub tls_key_path: Option<String>,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:9400".to_string(),
            health_check_interval_secs: 10,
            routing_strategy: RoutingStrategy::LeastLoaded,
            max_queue_size: 10_000,
            task_timeout_secs: 3600,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

/// A pending task waiting for a worker.
#[derive(Debug, Clone)]
struct PendingTask {
    assignment: TaskAssignment,
    queued_at: Instant,
    pool: String,
}

/// A task that's been dispatched to a worker.
#[derive(Debug, Clone)]
struct InflightTask {
    assignment: TaskAssignment,
    worker_id: String,
    #[allow(dead_code)]
    dispatched_at: Instant,
    pool: String,
}

/// The coordinator manages task distribution across workers.
pub struct Coordinator {
    config: CoordinatorConfig,

    /// The worker pool.
    pool: Arc<WorkerPool>,

    /// Queue of tasks waiting for a worker.
    pending_queue: Arc<Mutex<VecDeque<PendingTask>>>,

    /// Tasks currently running on workers.
    inflight: Arc<DashMap<String, InflightTask>>,

    /// Channel for sending task results back to the scheduler.
    result_tx: mpsc::UnboundedSender<TaskResult>,

    /// Per-worker task assignment channels.
    /// When a worker registers, we create a sender for it.
    /// Tasks are routed through these channels.
    worker_channels: Arc<DashMap<String, mpsc::UnboundedSender<TaskAssignment>>>,

    /// Log entries collected from workers (ring buffer per assignment).
    task_logs: Arc<DashMap<String, Vec<TaskLogEntry>>>,

    /// Coordinator start time.
    started_at: Instant,
}

impl Coordinator {
    /// Create a new coordinator.
    ///
    /// Returns the coordinator and a receiver for task results that the
    /// scheduler should consume.
    pub fn new(config: CoordinatorConfig) -> (Self, mpsc::UnboundedReceiver<TaskResult>) {
        let (result_tx, result_rx) = mpsc::unbounded_channel();

        let coordinator = Self {
            pool: Arc::new(WorkerPool::new(config.routing_strategy)),
            pending_queue: Arc::new(Mutex::new(VecDeque::new())),
            inflight: Arc::new(DashMap::new()),
            result_tx,
            worker_channels: Arc::new(DashMap::new()),
            task_logs: Arc::new(DashMap::new()),
            started_at: Instant::now(),
            config,
        };

        (coordinator, result_rx)
    }

    /// Register a worker and return a receiver for its task assignments.
    pub fn register_worker(
        &self,
        req: &RegisterRequest,
    ) -> mpsc::UnboundedReceiver<TaskAssignment> {
        self.pool.register(req);

        let (tx, rx) = mpsc::unbounded_channel();
        self.worker_channels.insert(req.worker_id.clone(), tx);

        rx
    }

    /// Submit a task for distributed execution.
    ///
    /// The coordinator will find a suitable worker and dispatch it.
    /// If no worker is available, the task is queued.
    pub async fn submit_task(&self, assignment: TaskAssignment, pool: &str) {
        let pool_name = if pool.is_empty() { "default" } else { pool };

        // Try to immediately dispatch.
        if let Some(worker_id) = self.pool.select_worker(pool_name).await {
            self.dispatch_to_worker(&worker_id, assignment, pool_name);
        } else {
            // Queue it.
            let mut queue = self.pending_queue.lock().await;
            if queue.len() >= self.config.max_queue_size {
                error!(
                    assignment = %assignment.assignment_id,
                    "Task queue is full ({} tasks), dropping assignment",
                    queue.len()
                );
                return;
            }

            info!(
                assignment = %assignment.assignment_id,
                task = %assignment.task_id,
                pool = %pool_name,
                "No worker available, queuing task"
            );
            queue.push_back(PendingTask {
                assignment,
                queued_at: Instant::now(),
                pool: pool_name.to_string(),
            });
        }
    }

    /// Create a new TaskAssignment from task parameters.
    #[allow(clippy::too_many_arguments)]
    pub fn create_assignment(
        &self,
        dag_id: &str,
        run_id: &str,
        task_id: &str,
        attempt: u32,
        spec: TaskSpec,
        context: TaskContext,
        timeout_secs: u64,
    ) -> TaskAssignment {
        let deadline = Utc::now().timestamp_millis() + (timeout_secs as i64 * 1000);

        TaskAssignment {
            assignment_id: Uuid::new_v4().to_string(),
            dag_id: dag_id.to_string(),
            run_id: run_id.to_string(),
            task_id: task_id.to_string(),
            attempt,
            spec,
            context,
            deadline_epoch_ms: deadline,
        }
    }

    /// Process a task result from a worker.
    pub fn handle_result(&self, result: TaskResult) {
        let success = result.outcome == TaskOutcome::Success;

        info!(
            assignment = %result.assignment_id,
            worker = %result.worker_id,
            task = %result.task_id,
            outcome = ?result.outcome,
            duration_ms = result.duration_ms,
            "Task result received"
        );

        // Update pool state.
        self.pool.complete_task(&result.assignment_id, success);
        self.inflight.remove(&result.assignment_id);

        // Forward to scheduler.
        if let Err(e) = self.result_tx.send(result) {
            error!(error = %e, "Failed to forward task result to scheduler");
        }

        // Try to drain pending queue now that a slot opened up.
        let pool = self.pool.clone();
        let pending = self.pending_queue.clone();
        let coord = self.worker_channels.clone();
        let inflight = self.inflight.clone();
        let pool2 = self.pool.clone();

        tokio::spawn(async move {
            Self::drain_pending_queue_inner(&pool, &pending, &coord, &inflight, &pool2).await;
        });
    }

    /// Process a heartbeat from a worker.
    pub fn handle_heartbeat(&self, hb: &WorkerHeartbeat) -> CoordinatorDirective {
        self.pool.heartbeat(hb);

        CoordinatorDirective::HeartbeatAck {
            timestamp_ms: Utc::now().timestamp_millis(),
        }
    }

    /// Process a log entry from a worker.
    pub fn handle_log_entry(&self, entry: TaskLogEntry) {
        self.task_logs
            .entry(entry.assignment_id.clone())
            .or_default()
            .push(entry);
    }

    /// Get logs for an assignment.
    pub fn get_logs(&self, assignment_id: &str) -> Vec<TaskLogEntry> {
        self.task_logs
            .get(assignment_id)
            .map(|e| e.value().clone())
            .unwrap_or_default()
    }

    /// Get the current cluster status.
    pub fn cluster_status(&self) -> ClusterStatusResponse {
        let uptime = self.started_at.elapsed().as_secs();
        let mut status = self.pool.cluster_status(uptime);
        status.running_tasks = self.inflight.len() as u32;
        status
    }

    /// Run the health check loop. Call this periodically.
    pub async fn health_check(&self) {
        let dead_workers = self.pool.check_health();

        if !dead_workers.is_empty() {
            let orphans = self.pool.orphaned_assignments(&dead_workers);

            if !orphans.is_empty() {
                warn!(
                    dead_workers = ?dead_workers,
                    orphaned_tasks = orphans.len(),
                    "Dead workers detected, reassigning tasks"
                );

                for assignment_id in &orphans {
                    self.reassign_task(assignment_id).await;
                }
            }

            // Clean up dead workers.
            for worker_id in &dead_workers {
                self.worker_channels.remove(worker_id);
                self.pool.remove_worker(worker_id);
            }
        }

        // Reconcile: recover any in-flight task whose worker is no longer
        // live. A task can be dispatched to a worker in the narrow window
        // between its disconnect and `handle_worker_disconnect` computing the
        // orphan list, leaving it stranded in `inflight` for a removed worker
        // with no recovery path. Requeue those so they get re-dispatched.
        self.reconcile_orphaned_inflight().await;

        // Also try to drain the pending queue.
        self.drain_pending_queue().await;
    }

    /// Requeue in-flight tasks whose assigned worker is no longer connected.
    /// Idempotent and safe to run every health-check tick.
    async fn reconcile_orphaned_inflight(&self) {
        let stranded: Vec<String> = self
            .inflight
            .iter()
            .filter(|entry| !self.worker_channels.contains_key(&entry.value().worker_id))
            .map(|entry| entry.key().clone())
            .collect();

        if !stranded.is_empty() {
            warn!(
                count = stranded.len(),
                "Reconciling in-flight tasks stranded on disconnected workers"
            );
            for assignment_id in &stranded {
                self.reassign_task(assignment_id).await;
            }
        }
    }

    /// Start the periodic health check background task.
    pub fn start_health_checker(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let coord = Arc::clone(self);
        let interval = Duration::from_secs(coord.config.health_check_interval_secs);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                coord.health_check().await;
            }
        })
    }

    // ─── Internal helpers ────────────────────────────────────────────────

    /// Send a task assignment directly to a specific worker.
    fn dispatch_to_worker(&self, worker_id: &str, assignment: TaskAssignment, pool: &str) {
        info!(
            assignment = %assignment.assignment_id,
            worker = %worker_id,
            task = %assignment.task_id,
            "Dispatching task to worker"
        );

        self.pool.assign_task(&assignment.assignment_id, worker_id);

        self.inflight.insert(
            assignment.assignment_id.clone(),
            InflightTask {
                assignment: assignment.clone(),
                worker_id: worker_id.to_string(),
                dispatched_at: Instant::now(),
                pool: pool.to_string(),
            },
        );

        if let Some(tx) = self.worker_channels.get(worker_id) {
            if tx.send(assignment).is_err() {
                error!(worker = %worker_id, "Worker channel closed, marking disconnected");
            }
        }
    }

    /// Try to assign pending tasks to available workers.
    async fn drain_pending_queue(&self) {
        Self::drain_pending_queue_inner(
            &self.pool,
            &self.pending_queue,
            &self.worker_channels,
            &self.inflight,
            &self.pool,
        )
        .await;
    }

    async fn drain_pending_queue_inner(
        pool: &WorkerPool,
        pending: &Mutex<VecDeque<PendingTask>>,
        worker_channels: &DashMap<String, mpsc::UnboundedSender<TaskAssignment>>,
        inflight: &DashMap<String, InflightTask>,
        pool2: &WorkerPool,
    ) {
        let mut queue = pending.lock().await;
        let mut remaining = VecDeque::new();

        while let Some(pending_task) = queue.pop_front() {
            if let Some(worker_id) = pool.select_worker(&pending_task.pool).await {
                info!(
                    assignment = %pending_task.assignment.assignment_id,
                    worker = %worker_id,
                    queued_for_ms = pending_task.queued_at.elapsed().as_millis(),
                    "Dispatching queued task"
                );

                pool2.assign_task(&pending_task.assignment.assignment_id, &worker_id);

                inflight.insert(
                    pending_task.assignment.assignment_id.clone(),
                    InflightTask {
                        assignment: pending_task.assignment.clone(),
                        worker_id: worker_id.clone(),
                        dispatched_at: Instant::now(),
                        pool: pending_task.pool.clone(),
                    },
                );

                if let Some(tx) = worker_channels.get(&worker_id) {
                    let _ = tx.send(pending_task.assignment);
                }
            } else {
                // No worker available, put it back.
                remaining.push_back(pending_task);
                break; // If no worker for this one, likely none for the rest either.
            }
        }

        // Put remaining tasks back.
        while let Some(task) = queue.pop_front() {
            remaining.push_back(task);
        }
        *queue = remaining;
    }

    /// Called when a worker's gRPC stream drops (process crash, network
    /// partition, ungraceful exit). Immediately reassigns its in-flight
    /// tasks instead of waiting for the next health-check tick — under
    /// load that tick can be 30s+ away, leaving tasks effectively orphaned.
    ///
    /// The post-health-check cleanup path (`health_check()`) still runs and
    /// is idempotent: a worker already removed here is a no-op there.
    pub async fn handle_worker_disconnect(&self, worker_id: &str) {
        let orphans = self.pool.orphaned_assignments(&[worker_id.to_string()]);
        warn!(
            worker = %worker_id,
            orphaned_tasks = orphans.len(),
            "Worker disconnected; reassigning orphaned tasks",
        );
        for aid in &orphans {
            self.reassign_task(aid).await;
        }
        self.worker_channels.remove(worker_id);
        self.pool.remove_worker(worker_id);
        self.drain_pending_queue().await;
    }

    /// Reassign a task from a dead worker.
    async fn reassign_task(&self, assignment_id: &str) {
        if let Some((_, inflight)) = self.inflight.remove(assignment_id) {
            warn!(
                assignment = %assignment_id,
                task = %inflight.assignment.task_id,
                old_worker = %inflight.worker_id,
                "Reassigning task from dead worker"
            );

            // Re-queue it.
            let pool_name = inflight.pool.clone();
            let mut queue = self.pending_queue.lock().await;
            queue.push_front(PendingTask {
                assignment: inflight.assignment,
                queued_at: Instant::now(),
                pool: pool_name,
            });
        }
    }

    /// Number of tasks currently in the pending queue.
    pub async fn pending_count(&self) -> usize {
        self.pending_queue.lock().await.len()
    }

    /// Number of tasks currently running on workers.
    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }

    /// Get access to the worker pool for direct queries.
    pub fn worker_pool(&self) -> &WorkerPool {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_register(id: &str, capacity: u32) -> RegisterRequest {
        RegisterRequest {
            worker_id: id.to_string(),
            hostname: format!("{}.local", id),
            capacity,
            pool_affinity: vec!["default".to_string()],
            labels: HashMap::new(),
            version: "0.1.0".to_string(),
            health_port: 9090,
        }
    }

    fn make_spec() -> TaskSpec {
        TaskSpec {
            task_type: TaskType::Bash,
            script: "echo hello".to_string(),
            connection: String::new(),
            query: String::new(),
            command: String::new(),
            args: vec![],
            timeout_secs: 300,
            resources: ResourceLimits::default(),
        }
    }

    fn make_context(task_id: &str) -> TaskContext {
        TaskContext {
            dag_id: "dag1".to_string(),
            run_id: "run1".to_string(),
            task_id: task_id.to_string(),
            attempt: 0,
            logical_date_epoch_ms: Utc::now().timestamp_millis(),
            environment: "dev".to_string(),
            params: HashMap::new(),
        }
    }

    #[test]
    fn test_coordinator_creation() {
        let (coord, _rx) = Coordinator::new(CoordinatorConfig::default());
        assert_eq!(coord.inflight_count(), 0);
        assert_eq!(coord.worker_pool().total_workers(), 0);
    }

    #[test]
    fn test_register_worker_returns_channel() {
        let (coord, _rx) = Coordinator::new(CoordinatorConfig::default());
        let _worker_rx = coord.register_worker(&make_register("w1", 4));

        assert_eq!(coord.worker_pool().total_workers(), 1);
        assert_eq!(coord.worker_pool().active_workers(), 1);
    }

    #[tokio::test]
    async fn test_submit_task_dispatches_to_worker() {
        let (coord, _rx) = Coordinator::new(CoordinatorConfig::default());
        let mut worker_rx = coord.register_worker(&make_register("w1", 4));

        let assignment = coord.create_assignment(
            "dag1",
            "run1",
            "task1",
            0,
            make_spec(),
            make_context("task1"),
            300,
        );

        coord.submit_task(assignment.clone(), "default").await;

        // Worker should receive the assignment.
        let received = worker_rx.try_recv().unwrap();
        assert_eq!(received.task_id, "task1");
        assert_eq!(coord.inflight_count(), 1);
    }

    #[tokio::test]
    async fn test_submit_task_queues_when_no_worker() {
        let (coord, _rx) = Coordinator::new(CoordinatorConfig::default());

        let assignment = coord.create_assignment(
            "dag1",
            "run1",
            "task1",
            0,
            make_spec(),
            make_context("task1"),
            300,
        );

        coord.submit_task(assignment, "default").await;

        // No workers registered → task should be queued.
        assert_eq!(coord.pending_count().await, 1);
        assert_eq!(coord.inflight_count(), 0);
    }

    #[tokio::test]
    async fn test_handle_result_forwards_to_scheduler() {
        let (coord, mut rx) = Coordinator::new(CoordinatorConfig::default());
        let mut _worker_rx = coord.register_worker(&make_register("w1", 4));

        let assignment = coord.create_assignment(
            "dag1",
            "run1",
            "task1",
            0,
            make_spec(),
            make_context("task1"),
            300,
        );
        let assignment_id = assignment.assignment_id.clone();

        coord.submit_task(assignment, "default").await;

        // Worker reports success.
        coord.handle_result(TaskResult {
            assignment_id: assignment_id.clone(),
            worker_id: "w1".to_string(),
            dag_id: "dag1".to_string(),
            run_id: "run1".to_string(),
            task_id: "task1".to_string(),
            attempt: 0,
            outcome: TaskOutcome::Success,
            exit_code: 0,
            duration_ms: 1000,
            xcom_json: "{}".to_string(),
            error: String::new(),
            metrics: HashMap::new(),
        });

        // Scheduler should receive the result.
        let result = rx.try_recv().unwrap();
        assert_eq!(result.task_id, "task1");
        assert_eq!(result.outcome, TaskOutcome::Success);
        assert_eq!(coord.inflight_count(), 0);
    }

    #[tokio::test]
    async fn test_queued_task_dispatched_after_completion() {
        let (coord, _rx) = Coordinator::new(CoordinatorConfig::default());
        let mut worker_rx = coord.register_worker(&make_register("w1", 1));

        // Fill the only slot.
        let a1 = coord.create_assignment(
            "dag1",
            "run1",
            "task1",
            0,
            make_spec(),
            make_context("task1"),
            300,
        );
        let a1_id = a1.assignment_id.clone();
        coord.submit_task(a1, "default").await;
        let _ = worker_rx.try_recv(); // consume it

        // Submit another — should queue.
        let a2 = coord.create_assignment(
            "dag1",
            "run1",
            "task2",
            0,
            make_spec(),
            make_context("task2"),
            300,
        );
        coord.submit_task(a2, "default").await;
        assert_eq!(coord.pending_count().await, 1);

        // Complete first task — should trigger dispatch of queued task.
        coord.handle_result(TaskResult {
            assignment_id: a1_id,
            worker_id: "w1".to_string(),
            dag_id: "dag1".to_string(),
            run_id: "run1".to_string(),
            task_id: "task1".to_string(),
            attempt: 0,
            outcome: TaskOutcome::Success,
            exit_code: 0,
            duration_ms: 100,
            xcom_json: "{}".to_string(),
            error: String::new(),
            metrics: HashMap::new(),
        });

        // Give the drain task a moment to run.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The second task should now be dispatched.
        // (It may already be in the channel from the background drain task)
        // Check pending count went down.
        assert_eq!(coord.pending_count().await, 0);
    }

    /// A task dispatched to a worker that then vanishes (its channel removed
    /// without going through the orphan-reassign path) must be recovered by
    /// the health-check reconciliation sweep, not stranded in `inflight`.
    /// Regression for the gap the soak harness surfaced.
    #[tokio::test]
    async fn test_reconcile_recovers_task_stranded_on_removed_worker() {
        let (coord, _rx) = Coordinator::new(CoordinatorConfig::default());
        let coord = Arc::new(coord);

        // Two workers so the reassigned task has somewhere to land.
        coord.register_worker(&make_register("w1", 1));
        let _w2_rx = coord.register_worker(&make_register("w2", 4));

        // Dispatch a task to w1.
        let a = coord.create_assignment(
            "dag1",
            "run1",
            "task1",
            0,
            make_spec(),
            make_context("task1"),
            300,
        );
        coord.submit_task(a, "default").await;
        assert_eq!(coord.inflight_count(), 1);

        // Simulate the disconnect-window race: w1's channel disappears WITHOUT
        // handle_worker_disconnect running, so its in-flight task is stranded.
        coord.worker_channels.remove("w1");

        // A normal health-check tick must reconcile and recover it.
        coord.health_check().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The task is no longer stranded on the dead worker: it was requeued
        // and re-dispatched (to w2), so nothing is left orphaned.
        let inflight = coord.inflight_count();
        let pending = coord.pending_count().await;
        assert!(
            inflight + pending <= 1,
            "task not recovered: inflight={inflight} pending={pending}"
        );
        // And whatever remains is NOT assigned to the removed worker.
        for entry in coord.inflight.iter() {
            assert_ne!(entry.value().worker_id, "w1", "still stranded on w1");
        }
    }

    #[test]
    fn test_handle_heartbeat() {
        let (coord, _rx) = Coordinator::new(CoordinatorConfig::default());
        coord.register_worker(&make_register("w1", 4));

        let hb = WorkerHeartbeat {
            worker_id: "w1".to_string(),
            active_tasks: 2,
            cpu_percent: 50.0,
            memory_percent: 60.0,
            disk_percent: 20.0,
            running_assignments: vec!["a1".into()],
            timestamp_ms: Utc::now().timestamp_millis(),
        };

        let directive = coord.handle_heartbeat(&hb);
        match directive {
            CoordinatorDirective::HeartbeatAck { .. } => {}
            _ => panic!("Expected HeartbeatAck"),
        }
    }

    #[test]
    fn test_handle_log_entry() {
        let (coord, _rx) = Coordinator::new(CoordinatorConfig::default());

        coord.handle_log_entry(TaskLogEntry {
            assignment_id: "a1".to_string(),
            worker_id: "w1".to_string(),
            level: LogLevel::Info,
            message: "Processing row 1000".to_string(),
            timestamp_ms: Utc::now().timestamp_millis(),
            metadata_json: String::new(),
        });

        let logs = coord.get_logs("a1");
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].message, "Processing row 1000");
    }

    #[test]
    fn test_cluster_status() {
        let (coord, _rx) = Coordinator::new(CoordinatorConfig::default());
        coord.register_worker(&make_register("w1", 4));
        coord.register_worker(&make_register("w2", 2));

        let status = coord.cluster_status();
        assert_eq!(status.health, ClusterHealth::Healthy);
        assert_eq!(status.workers.len(), 2);
    }

    #[test]
    fn test_create_assignment_generates_unique_ids() {
        let (coord, _rx) = Coordinator::new(CoordinatorConfig::default());

        let a1 = coord.create_assignment(
            "dag1",
            "run1",
            "task1",
            0,
            make_spec(),
            make_context("task1"),
            300,
        );
        let a2 = coord.create_assignment(
            "dag1",
            "run1",
            "task2",
            0,
            make_spec(),
            make_context("task2"),
            300,
        );

        assert_ne!(a1.assignment_id, a2.assignment_id);
    }
}
