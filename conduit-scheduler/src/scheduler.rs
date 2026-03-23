//! Core event-driven scheduling loop.
//!
//! The Scheduler is an async event loop that:
//! 1. Receives events from multiple sources (task completions, cron ticks, sensors)
//! 2. Updates in-memory DAG run and task state
//! 3. Evaluates trigger rules to determine which tasks are ready
//! 4. Dispatches ready tasks via command channel
//!
//! All state mutations are derived from events, enabling deterministic replay
//! and time-travel debugging.

use std::collections::HashMap;
use chrono::{DateTime, Duration, Utc};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use conduit_common::dag::{Dag, DagId, TaskId, TriggerRule};
use conduit_common::error::ConduitResult;

use crate::cron::CronSchedule;
use crate::trigger::TriggerRuleEvaluator;
use crate::pool_manager::PoolManager;

/// An event that the scheduler reacts to.
#[derive(Debug, Clone)]
pub enum SchedulerEvent {
    /// A new DAG run was requested.
    DagRunRequested {
        dag_id: DagId,
        run_id: String,
        logical_date: DateTime<Utc>,
        config: HashMap<String, String>,
    },
    /// A task completed successfully.
    TaskCompleted {
        dag_id: DagId,
        run_id: String,
        task_id: TaskId,
        snapshot_id: Option<String>,
        duration_ms: u64,
    },
    /// A task failed.
    TaskFailed {
        dag_id: DagId,
        run_id: String,
        task_id: TaskId,
        error: String,
        attempt: u32,
    },
    /// Periodic cron tick (evaluates all scheduled DAGs).
    CronTick {
        timestamp: DateTime<Utc>,
    },
    /// An external sensor triggered.
    SensorTriggered {
        sensor_id: String,
        payload: HashMap<String, String>,
    },
    /// Graceful shutdown signal.
    Shutdown,
}

/// A command the scheduler issues to downstream workers/executors.
#[derive(Debug, Clone)]
pub enum SchedulerCommand {
    /// Dispatch a task for execution.
    DispatchTask {
        dag_id: DagId,
        run_id: String,
        task_id: TaskId,
        attempt: u32,
    },
    /// Retry a failed task after a delay.
    RetryTask {
        dag_id: DagId,
        run_id: String,
        task_id: TaskId,
        delay: Duration,
    },
    /// Mark a DAG run as complete.
    CompleteDagRun {
        dag_id: DagId,
        run_id: String,
        status: RunStatus,
    },
    /// Skip a task (e.g., upstream failure with AllSuccess trigger).
    SkipTask {
        dag_id: DagId,
        run_id: String,
        task_id: TaskId,
        reason: String,
    },
}

/// Status of a completed DAG run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Success,
    Failed,
    Cancelled,
}

/// Per-run state tracking for active DAG executions.
#[derive(Debug, Clone)]
pub struct DagRunState {
    pub dag_id: DagId,
    pub run_id: String,
    pub logical_date: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub task_states: HashMap<TaskId, TaskState>,
    pub config: HashMap<String, String>,
}

/// State of a task within a DAG run.
#[derive(Debug, Clone)]
pub enum TaskState {
    /// Waiting for upstream dependencies.
    Pending,
    /// Ready to run, waiting for pool slot.
    Queued,
    /// Currently executing on a worker.
    Running {
        worker_id: String,
        attempt: u32,
        started_at: DateTime<Utc>,
    },
    /// Task completed successfully.
    Success {
        snapshot_id: Option<String>,
        duration_ms: u64,
        completed_at: DateTime<Utc>,
    },
    /// Task failed.
    Failed {
        error: String,
        attempt: u32,
        completed_at: DateTime<Utc>,
    },
    /// Task was skipped (e.g., upstream failure with OneFailed trigger).
    Skipped {
        reason: String,
        skipped_at: DateTime<Utc>,
    },
    /// Waiting for retry delay to pass.
    Retrying {
        attempt: u32,
        next_retry_at: DateTime<Utc>,
    },
}

/// The core event-driven scheduler.
pub struct Scheduler {
    event_rx: mpsc::UnboundedReceiver<SchedulerEvent>,
    command_tx: mpsc::UnboundedSender<SchedulerCommand>,
    dag_runs: HashMap<String, DagRunState>,
    #[allow(dead_code)]
    pools: PoolManager,
    plans: HashMap<DagId, Dag>,
    cron_schedules: HashMap<DagId, CronSchedule>,
}

impl Scheduler {
    /// Create a new scheduler.
    pub fn new(
        event_rx: mpsc::UnboundedReceiver<SchedulerEvent>,
        command_tx: mpsc::UnboundedSender<SchedulerCommand>,
        pools: PoolManager,
        plans: HashMap<DagId, Dag>,
    ) -> ConduitResult<Self> {
        let mut cron_schedules = HashMap::new();
        for (dag_id, dag) in &plans {
            if let Some(schedule_expr) = &dag.schedule {
                match CronSchedule::parse(schedule_expr) {
                    Ok(cron) => {
                        cron_schedules.insert(dag_id.clone(), cron);
                    }
                    Err(e) => {
                        warn!(
                            dag_id = %dag_id,
                            schedule = %schedule_expr,
                            error = ?e,
                            "Failed to parse cron schedule"
                        );
                    }
                }
            }
        }

        Ok(Self {
            event_rx,
            command_tx,
            dag_runs: HashMap::new(),
            pools,
            plans,
            cron_schedules,
        })
    }

    /// Run the scheduler event loop (blocks until shutdown).
    pub async fn run(mut self) -> ConduitResult<()> {
        info!("Scheduler event loop started");

        while let Some(event) = self.event_rx.recv().await {
            match event {
                SchedulerEvent::DagRunRequested {
                    dag_id,
                    run_id,
                    logical_date,
                    config,
                } => {
                    self.handle_dag_run_requested(&dag_id, &run_id, logical_date, config);
                }
                SchedulerEvent::TaskCompleted {
                    dag_id,
                    run_id,
                    task_id,
                    snapshot_id,
                    duration_ms,
                } => {
                    self.handle_task_completed(
                        &dag_id,
                        &run_id,
                        &task_id,
                        snapshot_id,
                        duration_ms,
                    );
                }
                SchedulerEvent::TaskFailed {
                    dag_id,
                    run_id,
                    task_id,
                    error,
                    attempt,
                } => {
                    self.handle_task_failed(&dag_id, &run_id, &task_id, error, attempt);
                }
                SchedulerEvent::CronTick { timestamp } => {
                    self.handle_cron_tick(timestamp);
                }
                SchedulerEvent::SensorTriggered {
                    sensor_id,
                    payload,
                } => {
                    self.handle_sensor_triggered(&sensor_id, payload);
                }
                SchedulerEvent::Shutdown => {
                    info!("Scheduler shutdown requested");
                    break;
                }
            }
        }

        info!("Scheduler event loop exited");
        Ok(())
    }

    /// Handle a new DAG run request.
    fn handle_dag_run_requested(
        &mut self,
        dag_id: &DagId,
        run_id: &str,
        logical_date: DateTime<Utc>,
        config: HashMap<String, String>,
    ) {
        let dag = match self.plans.get(dag_id) {
            Some(d) => d,
            None => {
                error!(dag_id = %dag_id, "DAG not found");
                return;
            }
        };

        // Create run state
        let mut run_state = DagRunState {
            dag_id: dag_id.clone(),
            run_id: run_id.to_string(),
            logical_date,
            started_at: Utc::now(),
            task_states: HashMap::new(),
            config,
        };

        // Initialize all tasks as Pending
        for task_id in &dag.execution_order {
            run_state
                .task_states
                .insert(task_id.clone(), TaskState::Pending);
        }

        let run_key = format!("{}/{}", dag_id, run_id);
        self.dag_runs.insert(run_key, run_state.clone());

        info!(
            dag_id = %dag_id,
            run_id = %run_id,
            "DAG run created"
        );

        // Evaluate which root tasks are ready
        self.evaluate_ready_tasks(dag, &run_state);
    }

    /// Handle task completion.
    fn handle_task_completed(
        &mut self,
        dag_id: &DagId,
        run_id: &str,
        task_id: &TaskId,
        snapshot_id: Option<String>,
        duration_ms: u64,
    ) {
        let run_key = format!("{}/{}", dag_id, run_id);

        // Mutate first, then drop the mutable borrow
        {
            let run_state = match self.dag_runs.get_mut(&run_key) {
                Some(r) => r,
                None => {
                    error!(dag_id = %dag_id, run_id = %run_id, "Run not found");
                    return;
                }
            };

            run_state.task_states.insert(
                task_id.clone(),
                TaskState::Success {
                    snapshot_id,
                    duration_ms,
                    completed_at: Utc::now(),
                },
            );
        }

        debug!(
            dag_id = %dag_id,
            run_id = %run_id,
            task_id = %task_id,
            duration_ms = %duration_ms,
            "Task completed"
        );

        let dag = match self.plans.get(dag_id) {
            Some(d) => d,
            None => {
                error!(dag_id = %dag_id, "DAG not found");
                return;
            }
        };

        let run_state = match self.dag_runs.get(&run_key) {
            Some(r) => r,
            None => return,
        };

        // Evaluate downstream tasks
        self.evaluate_ready_tasks(dag, run_state);

        // Check if DAG run is complete
        self.check_dag_run_complete(dag, run_state);
    }

    /// Handle task failure.
    fn handle_task_failed(
        &mut self,
        dag_id: &DagId,
        run_id: &str,
        task_id: &TaskId,
        error: String,
        attempt: u32,
    ) {
        let run_key = format!("{}/{}", dag_id, run_id);

        // Look up retry config from the plan before mutating run state
        let (should_retry, retry_delay) = match self.plans.get(dag_id) {
            Some(dag) => match dag.tasks.get(task_id) {
                Some(task) => {
                    if attempt < task.retries {
                        (true, parse_duration(&task.retry_delay))
                    } else {
                        (false, Duration::zero())
                    }
                }
                None => {
                    error!(dag_id = %dag_id, task_id = %task_id, "Task not found");
                    return;
                }
            },
            None => {
                error!(dag_id = %dag_id, "DAG not found");
                return;
            }
        };

        // Now mutate run state
        {
            let run_state = match self.dag_runs.get_mut(&run_key) {
                Some(r) => r,
                None => {
                    error!(dag_id = %dag_id, run_id = %run_id, "Run not found");
                    return;
                }
            };

            if should_retry {
                run_state.task_states.insert(
                    task_id.clone(),
                    TaskState::Retrying {
                        attempt: attempt + 1,
                        next_retry_at: Utc::now() + retry_delay,
                    },
                );
            } else {
                run_state.task_states.insert(
                    task_id.clone(),
                    TaskState::Failed {
                        error: error.clone(),
                        attempt,
                        completed_at: Utc::now(),
                    },
                );
            }
        }

        if should_retry {
            debug!(
                dag_id = %dag_id,
                run_id = %run_id,
                task_id = %task_id,
                attempt = %attempt,
                "Task will be retried"
            );

            let _ = self.command_tx.send(SchedulerCommand::RetryTask {
                dag_id: dag_id.clone(),
                run_id: run_id.to_string(),
                task_id: task_id.clone(),
                delay: retry_delay,
            });
        } else {
            warn!(
                dag_id = %dag_id,
                run_id = %run_id,
                task_id = %task_id,
                error = %error,
                "Task failed with no retries remaining"
            );

            // Evaluate impact — clone dag to avoid borrow conflict with &mut self
            if let Some(dag) = self.plans.get(dag_id).cloned() {
                self.evaluate_failed_task_impact(&dag, &run_key);
            }
        }
    }

    /// Handle periodic cron tick.
    fn handle_cron_tick(&mut self, timestamp: DateTime<Utc>) {
        // Collect due DAGs first to avoid borrowing self immutably while mutating
        let due_dags: Vec<DagId> = self
            .cron_schedules
            .iter()
            .filter(|(_, cron)| cron.is_due(timestamp))
            .map(|(dag_id, _)| dag_id.clone())
            .collect();

        for dag_id in due_dags {
            let run_id = format!("{}_{}", dag_id, timestamp.timestamp());

            info!(
                dag_id = %dag_id,
                run_id = %run_id,
                "Creating DAG run from cron schedule"
            );

            self.handle_dag_run_requested(&dag_id, &run_id, timestamp, HashMap::new());
        }
    }

    /// Handle external sensor trigger.
    fn handle_sensor_triggered(&mut self, _sensor_id: &str, _payload: HashMap<String, String>) {
        // Future: unblock sensor-waiting tasks
        debug!(sensor_id = %_sensor_id, "Sensor triggered (not yet implemented)");
    }

    /// Evaluate which tasks in a run are ready to execute.
    fn evaluate_ready_tasks(&self, dag: &Dag, run_state: &DagRunState) {
        let evaluator = TriggerRuleEvaluator::new();

        for task_id in &dag.execution_order {
            let current_state = run_state
                .task_states
                .get(task_id)
                .cloned()
                .unwrap_or(TaskState::Pending);

            // Only evaluate Pending tasks
            if !matches!(current_state, TaskState::Pending) {
                continue;
            }

            let task = &dag.tasks[task_id];

            // Check if this task is ready
            let is_ready = evaluator.evaluate(
                &task.trigger_rule,
                task_id,
                dag,
                run_state,
            );

            if is_ready {
                // Queue the task
                let _ = self.command_tx.send(SchedulerCommand::DispatchTask {
                    dag_id: dag.id.clone(),
                    run_id: run_state.run_id.clone(),
                    task_id: task_id.clone(),
                    attempt: 0,
                });

                debug!(
                    dag_id = %dag.id,
                    run_id = %run_state.run_id,
                    task_id = %task_id,
                    "Task is ready to dispatch"
                );
            }
        }
    }

    /// Check if a DAG run is complete (all tasks terminal).
    fn check_dag_run_complete(&self, dag: &Dag, run_state: &DagRunState) {
        let all_terminal = dag.execution_order.iter().all(|task_id| {
            matches!(
                run_state.task_states.get(task_id),
                Some(TaskState::Success { .. } | TaskState::Failed { .. } | TaskState::Skipped { .. })
            )
        });

        if all_terminal {
            let has_failures = dag.execution_order.iter().any(|task_id| {
                matches!(run_state.task_states.get(task_id), Some(TaskState::Failed { .. }))
            });

            let status = if has_failures {
                RunStatus::Failed
            } else {
                RunStatus::Success
            };

            info!(
                dag_id = %dag.id,
                run_id = %run_state.run_id,
                status = ?status,
                "DAG run completed"
            );

            let _ = self.command_tx.send(SchedulerCommand::CompleteDagRun {
                dag_id: dag.id.clone(),
                run_id: run_state.run_id.clone(),
                status,
            });
        }
    }

    /// Evaluate the impact of a task failure on downstream tasks.
    fn evaluate_failed_task_impact(&mut self, dag: &Dag, run_key: &str) {
        // Read task states snapshot to determine which tasks to skip.
        let tasks_to_skip: Vec<TaskId> = if let Some(run_state) = self.dag_runs.get(run_key) {
            dag.tasks
                .iter()
                .filter(|(task_id, task)| {
                    let is_pending = matches!(
                        run_state.task_states.get(*task_id),
                        Some(TaskState::Pending) | None
                    );
                    if !is_pending {
                        return false;
                    }
                    let has_failed_upstream = task.dependencies.iter().any(|dep| {
                        matches!(
                            run_state.task_states.get(&dep.task_id),
                            Some(TaskState::Failed { .. } | TaskState::Skipped { .. })
                        )
                    });
                    has_failed_upstream && matches!(task.trigger_rule, TriggerRule::AllSuccess)
                })
                .map(|(task_id, _)| task_id.clone())
                .collect()
        } else {
            return;
        };

        // Apply skips to internal state and send commands.
        if let Some(run_state_mut) = self.dag_runs.get_mut(run_key) {
            for task_id in &tasks_to_skip {
                run_state_mut.task_states.insert(
                    task_id.clone(),
                    TaskState::Skipped {
                        reason: "Upstream task failed".to_string(),
                        skipped_at: Utc::now(),
                    },
                );

                let _ = self.command_tx.send(SchedulerCommand::SkipTask {
                    dag_id: dag.id.clone(),
                    run_id: run_state_mut.run_id.clone(),
                    task_id: task_id.clone(),
                    reason: "Upstream task failed".to_string(),
                });
            }
        }

        // Re-borrow immutably to check if the run is now complete.
        if let Some(updated_state) = self.dag_runs.get(run_key) {
            self.check_dag_run_complete(dag, updated_state);
        }
    }
}

/// Parse a duration string (e.g., "5m", "30s", "1h").
fn parse_duration(duration_str: &Option<String>) -> Duration {
    match duration_str {
        None => Duration::minutes(5), // Default retry delay
        Some(s) => {
            if let Ok(secs) = s.trim_end_matches('s').parse::<i64>() {
                Duration::seconds(secs)
            } else if let Ok(mins) = s.trim_end_matches('m').parse::<i64>() {
                Duration::minutes(mins)
            } else if let Ok(hours) = s.trim_end_matches('h').parse::<i64>() {
                Duration::hours(hours)
            } else {
                Duration::minutes(5) // Fallback
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration(&Some("30s".to_string())), Duration::seconds(30));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(
            parse_duration(&Some("5m".to_string())),
            Duration::minutes(5)
        );
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(
            parse_duration(&Some("2h".to_string())),
            Duration::hours(2)
        );
    }

    #[test]
    fn test_parse_duration_default() {
        assert_eq!(parse_duration(&None), Duration::minutes(5));
    }
}
