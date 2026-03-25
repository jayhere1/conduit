//! Task execution engine that manages task dispatch and lifecycle.
//!
//! The TaskExecutor receives ExecutorCommand::DispatchTask commands from the scheduler
//! and manages their execution as isolated child processes.

use crate::process_runner::{ProcessRunner, TaskContext};
use conduit_common::{
    ConduitError, ConduitResult,
    dag::Task,
    metrics,
};
use conduit_providers::ProviderRegistry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Simple task completion state returned by the executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcome {
    Success,
    Failed,
    Retry,
    Skipped,
}

/// Commands the executor receives from the scheduler.
#[derive(Debug, Clone)]
pub enum ExecutorCommand {
    /// Dispatch a task for execution.
    DispatchTask {
        task: Task,
        dag_id: String,
        run_id: String,
        attempt: u32,
        logical_date: chrono::DateTime<chrono::Utc>,
        environment: String,
        params: HashMap<String, String>,
    },
    /// Cancel a running task.
    CancelTask { task_id: String, run_id: String },
    /// Graceful shutdown.
    Shutdown,
}

/// Events the executor sends back to the scheduler.
#[derive(Debug, Clone)]
pub enum ExecutorEvent {
    /// A task completed (success, failed, retry, or skipped).
    TaskCompleted {
        task_id: String,
        run_id: String,
        attempt: u32,
        outcome: TaskOutcome,
        xcom: Option<serde_json::Value>,
        duration: Duration,
    },
    /// A task failed with an error.
    TaskFailed {
        task_id: String,
        run_id: String,
        attempt: u32,
        error: String,
    },
}

/// Task executor that manages task dispatch and execution.
///
/// Optionally holds a reference to a `ProviderRegistry` so SQL tasks can be
/// executed natively through database drivers instead of spawning child processes.
///
/// When the concurrency limit is reached, tasks are queued in a backlog and
/// dispatched as slots become available (instead of being silently dropped).
pub struct TaskExecutor {
    command_rx: mpsc::UnboundedReceiver<ExecutorCommand>,
    event_tx: mpsc::UnboundedSender<ExecutorEvent>,
    active_tasks: HashMap<String, JoinHandle<()>>,
    max_concurrent: usize,
    provider_registry: Option<Arc<ProviderRegistry>>,
    /// Backlog of tasks waiting for a concurrency slot.
    deferred_queue: VecDeque<ExecutorCommand>,
    /// Configurable shutdown timeout in seconds.
    shutdown_timeout_secs: u64,
}

impl TaskExecutor {
    pub fn new(
        command_rx: mpsc::UnboundedReceiver<ExecutorCommand>,
        event_tx: mpsc::UnboundedSender<ExecutorEvent>,
        max_concurrent: usize,
    ) -> Self {
        Self {
            command_rx,
            event_tx,
            active_tasks: HashMap::new(),
            max_concurrent,
            provider_registry: None,
            deferred_queue: VecDeque::new(),
            shutdown_timeout_secs: 300,
        }
    }

    /// Create a new TaskExecutor with an attached provider registry for native SQL execution.
    pub fn with_providers(
        command_rx: mpsc::UnboundedReceiver<ExecutorCommand>,
        event_tx: mpsc::UnboundedSender<ExecutorEvent>,
        max_concurrent: usize,
        registry: Arc<ProviderRegistry>,
    ) -> Self {
        Self {
            command_rx,
            event_tx,
            active_tasks: HashMap::new(),
            max_concurrent,
            provider_registry: Some(registry),
            deferred_queue: VecDeque::new(),
            shutdown_timeout_secs: 300,
        }
    }

    /// Set the shutdown timeout (default: 300 seconds).
    pub fn with_shutdown_timeout(mut self, secs: u64) -> Self {
        self.shutdown_timeout_secs = secs;
        self
    }

    pub async fn run(&mut self) -> ConduitResult<()> {
        info!(
            max_concurrent = self.max_concurrent,
            "Starting TaskExecutor"
        );

        while let Some(command) = self.command_rx.recv().await {
            match command {
                ExecutorCommand::DispatchTask {
                    task,
                    dag_id,
                    run_id,
                    attempt,
                    logical_date,
                    environment,
                    params,
                } => {
                    self.handle_dispatch_task(
                        task,
                        dag_id,
                        run_id,
                        attempt,
                        logical_date,
                        environment,
                        params,
                    )
                    .await?;
                }
                ExecutorCommand::CancelTask { task_id, run_id } => {
                    self.handle_cancel_task(&task_id, &run_id).await?;
                }
                ExecutorCommand::Shutdown => {
                    info!("Shutdown command received, waiting for active tasks");
                    self.shutdown().await?;
                    break;
                }
            }

            self.cleanup_completed_tasks().await;
            self.drain_deferred_queue().await?;
        }

        info!("TaskExecutor shutdown complete");
        Ok(())
    }

    async fn handle_dispatch_task(
        &mut self,
        task: Task,
        dag_id: String,
        run_id: String,
        attempt: u32,
        logical_date: chrono::DateTime<chrono::Utc>,
        environment: String,
        params: HashMap<String, String>,
    ) -> ConduitResult<()> {
        let task_id = task.id.clone();

        if self.active_tasks.len() >= self.max_concurrent {
            warn!(
                current = self.active_tasks.len(),
                max = self.max_concurrent,
                task_id = %task_id,
                queue_depth = self.deferred_queue.len(),
                "Task dispatch deferred: concurrency limit reached, queuing"
            );

            if let Some(m) = metrics::try_global() {
                m.executor_deferred_total.inc();
            }

            // Queue instead of silently dropping
            self.deferred_queue.push_back(ExecutorCommand::DispatchTask {
                task,
                dag_id,
                run_id,
                attempt,
                logical_date,
                environment,
                params,
            });
            return Ok(());
        }

        self.spawn_task(task, dag_id, run_id, attempt, logical_date, environment, params);
        Ok(())
    }

    fn spawn_task(
        &mut self,
        task: Task,
        dag_id: String,
        run_id: String,
        attempt: u32,
        logical_date: chrono::DateTime<chrono::Utc>,
        environment: String,
        params: HashMap<String, String>,
    ) {
        let task_id = task.id.clone();
        let event_tx = self.event_tx.clone();
        let task_id_clone = task_id.clone();
        let run_id_clone = run_id.clone();

        let context = TaskContext {
            dag_id,
            run_id,
            task_id: task_id_clone.clone(),
            attempt,
            logical_date,
            environment,
            params,
        };

        let registry = self.provider_registry.clone();
        let handle = tokio::spawn(async move {
            debug!(task_id = %task_id_clone, attempt = attempt, "Task execution started");

            match Self::execute_task(&task, &context, registry.as_ref().map(|r| r.as_ref())).await {
                Ok((outcome, xcom, duration)) => {
                    info!(
                        task_id = %task_id_clone,
                        duration_ms = duration.as_millis(),
                        "Task execution completed"
                    );

                    let event = ExecutorEvent::TaskCompleted {
                        task_id: task_id_clone,
                        run_id: run_id_clone,
                        attempt,
                        outcome,
                        xcom,
                        duration,
                    };

                    if let Err(e) = event_tx.send(event) {
                        error!(error = %e, "Failed to send TaskCompleted event");
                    }
                }
                Err(e) => {
                    error!(
                        task_id = %task_id_clone,
                        error = %e,
                        "Task execution failed"
                    );

                    let event = ExecutorEvent::TaskFailed {
                        task_id: task_id_clone,
                        run_id: run_id_clone,
                        attempt,
                        error: e.to_string(),
                    };

                    if let Err(send_err) = event_tx.send(event) {
                        error!(error = %send_err, "Failed to send TaskFailed event");
                    }
                }
            }
        });

        self.active_tasks.insert(task_id, handle);
    }

    async fn execute_task(
        task: &Task,
        context: &TaskContext,
        registry: Option<&ProviderRegistry>,
    ) -> ConduitResult<(TaskOutcome, Option<serde_json::Value>, Duration)> {
        let start = std::time::Instant::now();

        let timeout_duration = parse_timeout(&task.timeout);

        // The ProcessRunner uses ChildGuard internally to kill child processes
        // on future cancellation (e.g., when timeout fires).
        let result = tokio::time::timeout(
            timeout_duration,
            ProcessRunner::run_with_providers(task, context, registry),
        )
        .await;

        let duration = start.elapsed();

        match result {
            Ok(Ok(output)) => {
                debug!(exit_code = output.exit_code, "Process completed");

                let outcome = match output.exit_code {
                    0 => TaskOutcome::Success,
                    1 => TaskOutcome::Failed,
                    2 => TaskOutcome::Retry,
                    3 => TaskOutcome::Skipped,
                    code => {
                        warn!(unexpected_exit_code = code, "Unexpected exit code");
                        TaskOutcome::Failed
                    }
                };

                Ok((outcome, output.xcom, duration))
            }
            Ok(Err(e)) => {
                error!(error = %e, "ProcessRunner failed");
                Err(e)
            }
            Err(_) => {
                error!(
                    timeout_seconds = timeout_duration.as_secs(),
                    "Task execution timeout — child process killed via ChildGuard"
                );

                if let Some(m) = metrics::try_global() {
                    m.task_timeouts_total.inc();
                }

                Err(ConduitError::ExecutionError(format!(
                    "Task timed out after {} seconds",
                    timeout_duration.as_secs()
                )))
            }
        }
    }

    async fn handle_cancel_task(&mut self, task_id: &str, run_id: &str) -> ConduitResult<()> {
        info!(task_id = task_id, run_id = run_id, "Cancel task requested");

        if let Some(handle) = self.active_tasks.remove(task_id) {
            // Aborting the tokio task will drop the future, which triggers
            // ChildGuard::drop → start_kill on the child process.
            handle.abort();
            debug!(task_id = task_id, "Task handle aborted, child process will be killed");
        }

        Ok(())
    }

    /// Drain deferred tasks into available slots after a task completes.
    async fn drain_deferred_queue(&mut self) -> ConduitResult<()> {
        while self.active_tasks.len() < self.max_concurrent {
            match self.deferred_queue.pop_front() {
                Some(ExecutorCommand::DispatchTask {
                    task,
                    dag_id,
                    run_id,
                    attempt,
                    logical_date,
                    environment,
                    params,
                }) => {
                    debug!(
                        task_id = %task.id,
                        remaining_queued = self.deferred_queue.len(),
                        "Dispatching deferred task"
                    );
                    self.spawn_task(task, dag_id, run_id, attempt, logical_date, environment, params);
                }
                _ => break,
            }
        }
        Ok(())
    }

    async fn cleanup_completed_tasks(&mut self) {
        self.active_tasks.retain(|_, handle| !handle.is_finished());
    }

    async fn shutdown(&mut self) -> ConduitResult<()> {
        let count = self.active_tasks.len();
        let queued = self.deferred_queue.len();
        info!(
            active_tasks = count,
            queued_tasks = queued,
            timeout_secs = self.shutdown_timeout_secs,
            "Waiting for active tasks to complete"
        );

        if queued > 0 {
            warn!(queued_tasks = queued, "Dropping queued tasks on shutdown");
            self.deferred_queue.clear();
        }

        let shutdown_timeout = Duration::from_secs(self.shutdown_timeout_secs);
        let start = std::time::Instant::now();

        while !self.active_tasks.is_empty() {
            if start.elapsed() > shutdown_timeout {
                warn!(
                    remaining_tasks = self.active_tasks.len(),
                    "Shutdown timeout reached, aborting remaining tasks"
                );
                // Abort remaining tasks — ChildGuard will kill child processes
                for (task_id, handle) in self.active_tasks.drain() {
                    warn!(task_id = %task_id, "Force-aborting task on shutdown timeout");
                    handle.abort();
                }
                break;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
            self.cleanup_completed_tasks().await;
        }

        info!("TaskExecutor shutdown complete");
        Ok(())
    }
}

fn parse_timeout(timeout: &Option<String>) -> Duration {
    timeout
        .as_ref()
        .and_then(|s| crate::retry::parse_duration(s).ok())
        .unwrap_or_else(|| Duration::from_secs(3600))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timeout() {
        assert_eq!(
            parse_timeout(&Some("30s".to_string())),
            Duration::from_secs(30)
        );
        assert_eq!(
            parse_timeout(&Some("5m".to_string())),
            Duration::from_secs(300)
        );
        assert_eq!(
            parse_timeout(&Some("1h".to_string())),
            Duration::from_secs(3600)
        );
        assert_eq!(parse_timeout(&None), Duration::from_secs(3600));
    }

    #[tokio::test]
    async fn test_task_executor_creation() {
        let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        let executor = TaskExecutor::new(cmd_rx, event_tx, 4);
        assert_eq!(executor.max_concurrent, 4);
        assert!(executor.active_tasks.is_empty());
        assert!(executor.deferred_queue.is_empty());
    }
}
