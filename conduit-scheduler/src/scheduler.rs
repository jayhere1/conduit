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

use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use conduit_common::dag::{Dag, DagId, TaskId, TriggerRule};
use conduit_common::error::ConduitResult;
use conduit_common::metrics;
use conduit_state::EventStore;

use crate::cron::CronSchedule;
use crate::pool_manager::PoolManager;
use crate::trigger::TriggerRuleEvaluator;

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
    CronTick { timestamp: DateTime<Utc> },
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
    /// Optional persistent event store for catchup queries.
    event_store: Option<Arc<EventStore>>,
    /// Alert hooks fired on every non-success DAG-run completion. Each hook
    /// is spawned on the tokio runtime so a slow notifier (PagerDuty, Slack)
    /// can't stall the scheduler event loop. Empty by default.
    alert_hooks: Vec<Arc<dyn crate::alerts::AlertHook>>,
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
            event_store: None,
            alert_hooks: Vec::new(),
        })
    }

    /// Attach a persistent event store for catchup-on-missed-runs.
    pub fn with_event_store(mut self, store: Arc<EventStore>) -> Self {
        self.event_store = Some(store);
        self
    }

    /// Register an alert hook fired on every non-success DAG-run completion.
    /// Hooks fire in registration order, each in its own spawned task so a
    /// slow / failing hook doesn't block the scheduler or other hooks.
    pub fn with_alert_hook(mut self, hook: Arc<dyn crate::alerts::AlertHook>) -> Self {
        self.alert_hooks.push(hook);
        self
    }

    /// Scan every DAG with a `Dag.on_failure` webhook URL set and
    /// auto-register a scoped `WebhookAlertHook` for it. Each hook only
    /// fires for its own DAG (via `ScopedHook`), so multiple DAGs with
    /// different webhooks each get their own notifications.
    ///
    /// Webhook construction failures (TLS config errors, malformed URLs
    /// rejected by reqwest's client builder) are logged and skipped —
    /// the scheduler still starts, just without the alert path for that
    /// DAG. Same fail-quiet posture as the rest of the hook surface.
    ///
    /// Closes the long-standing gap where `Dag.on_failure` was parsed by
    /// the compiler since the first lineage commit but never wired to
    /// actually fire anything.
    pub fn with_dag_failure_webhooks(mut self) -> Self {
        for (dag_id, dag) in &self.plans {
            let Some(url) = dag.on_failure.as_ref() else {
                continue;
            };
            match crate::alerts::WebhookAlertHook::new(url.clone()) {
                Ok(webhook) => {
                    let scoped = crate::alerts::ScopedHook::new(dag_id.clone(), webhook);
                    self.alert_hooks
                        .push(Arc::new(scoped) as Arc<dyn crate::alerts::AlertHook>);
                    info!(
                        dag_id = %dag_id,
                        url = %url,
                        "Registered DAG-failure webhook alert"
                    );
                }
                Err(e) => {
                    warn!(
                        dag_id = %dag_id,
                        url = %url,
                        error = %e,
                        "Failed to build webhook client; DAG's on_failure URL will not fire"
                    );
                }
            }
        }
        self
    }

    /// Send a command to the executor, logging and recording metrics on failure.
    fn send_command(&self, cmd: SchedulerCommand) {
        if let Err(e) = self.command_tx.send(cmd) {
            error!(error = %e, "Failed to send scheduler command — executor channel closed");
            if let Some(m) = metrics::try_global() {
                m.command_send_errors_total.inc();
            }
        }
    }

    // ─── Catchup ─────────────────────────────────────────────────────────

    /// Check for missed cron runs since the last recorded run and schedule them.
    ///
    /// Called once on startup before entering the event loop. For each DAG with
    /// `catchup: true`, queries the event store for the last run's logical_date,
    /// then calculates all cron occurrences between that time and now.
    pub fn perform_catchup(&mut self) {
        let store = match &self.event_store {
            Some(s) => Arc::clone(s),
            None => {
                debug!("No event store attached, skipping catchup");
                return;
            }
        };

        let now = Utc::now();

        // Collect catchup work to avoid borrow conflicts with handle_dag_run_requested
        let mut catchup_runs: Vec<(DagId, DateTime<Utc>)> = Vec::new();

        for (dag_id, cron) in &self.cron_schedules {
            let dag = match self.plans.get(dag_id) {
                Some(d) => d,
                None => continue,
            };

            if !dag.catchup {
                debug!(dag_id = %dag_id, "Catchup disabled, skipping");
                continue;
            }

            let last_run = match store.last_run_logical_date(dag_id) {
                Ok(Some(dt)) => dt,
                Ok(None) => {
                    debug!(dag_id = %dag_id, "No previous runs found, skipping catchup");
                    continue;
                }
                Err(e) => {
                    warn!(dag_id = %dag_id, error = %e, "Failed to query last run for catchup");
                    continue;
                }
            };

            let limit = dag.max_catchup_runs.map(|n| n as usize).unwrap_or(100);
            let missed = cron.occurrences_between(last_run, now, limit);

            if !missed.is_empty() {
                info!(
                    dag_id = %dag_id,
                    missed_count = missed.len(),
                    last_run = %last_run,
                    "Scheduling catchup runs for missed intervals"
                );
            }

            for logical_date in missed {
                catchup_runs.push((dag_id.clone(), logical_date));
            }
        }

        // Now schedule the collected catchup runs
        for (dag_id, logical_date) in catchup_runs {
            let run_id = format!("catchup_{}_{}", dag_id, logical_date.timestamp());
            info!(dag_id = %dag_id, logical_date = %logical_date, run_id = %run_id, "Scheduling catchup run");
            self.handle_dag_run_requested(&dag_id, &run_id, logical_date, HashMap::new());

            if let Some(m) = metrics::try_global() {
                m.catchup_runs_total.inc();
            }
        }
    }

    // ─── Event Loop ──────────────────────────────────────────────────────

    /// Run the scheduler event loop (blocks until shutdown).
    pub async fn run(mut self) -> ConduitResult<()> {
        info!("Scheduler event loop started");

        // Perform catchup on startup before processing new events
        self.perform_catchup();

        while let Some(event) = self.event_rx.recv().await {
            // Record scheduler event metric
            if let Some(m) = metrics::try_global() {
                let event_type = match &event {
                    SchedulerEvent::DagRunRequested { .. } => "dag_run_requested",
                    SchedulerEvent::TaskCompleted { .. } => "task_completed",
                    SchedulerEvent::TaskFailed { .. } => "task_failed",
                    SchedulerEvent::CronTick { .. } => "cron_tick",
                    SchedulerEvent::SensorTriggered { .. } => "sensor_triggered",
                    SchedulerEvent::Shutdown => "shutdown",
                };
                m.scheduler_events_total
                    .get_or_create(&metrics::EventLabels {
                        event_type: event_type.to_string(),
                    })
                    .inc();
            }

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
                    if let Some(m) = metrics::try_global() {
                        m.cron_ticks_total.inc();
                    }
                    self.handle_cron_tick(timestamp);
                }
                SchedulerEvent::SensorTriggered { sensor_id, payload } => {
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
        self.dag_runs.insert(run_key.clone(), run_state);

        info!(
            dag_id = %dag_id,
            run_id = %run_id,
            "DAG run created"
        );

        // Record metrics
        if let Some(m) = metrics::try_global() {
            m.dag_runs_total
                .get_or_create(&metrics::DagStatusLabels {
                    dag_id: dag_id.clone(),
                    status: "created".to_string(),
                })
                .inc();
            m.active_dag_runs.inc();
        }

        // Evaluate ready tasks and dispatch them. We compute the ready list
        // against the immutably-borrowed run_state, then transition each to
        // Queued in the stored state before emitting commands — so any later
        // evaluate call sees them as already-dispatched and doesn't re-elect.
        let ready = match self.dag_runs.get(&run_key) {
            Some(rs) => self.evaluate_ready_tasks(dag, rs),
            None => return,
        };
        self.transition_and_dispatch(dag_id, &run_key, ready);
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

        // Mutate first, then drop the mutable borrow. Drop late or duplicate
        // completion events for already-terminal tasks instead of overwriting
        // — overwriting could turn a Failed task into Success on a stale event.
        {
            let run_state = match self.dag_runs.get_mut(&run_key) {
                Some(r) => r,
                None => {
                    error!(dag_id = %dag_id, run_id = %run_id, "Run not found");
                    return;
                }
            };

            if let Some(
                TaskState::Success { .. } | TaskState::Failed { .. } | TaskState::Skipped { .. },
            ) = run_state.task_states.get(task_id)
            {
                warn!(
                    dag_id = %dag_id,
                    run_id = %run_id,
                    task_id = %task_id,
                    "Ignoring duplicate TaskCompleted event — task already terminal"
                );
                return;
            }

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

        // Record metrics
        if let Some(m) = metrics::try_global() {
            m.record_task_event(dag_id, task_id, "completed");
            m.observe_task_duration(dag_id, task_id, duration_ms as f64 / 1000.0);
            m.active_tasks.dec();
        }

        // Compute ready set while holding only immutable borrows.
        let ready = {
            let dag = match self.plans.get(dag_id) {
                Some(d) => d,
                None => {
                    error!(dag_id = %dag_id, "DAG not found");
                    return;
                }
            };
            match self.dag_runs.get(&run_key) {
                Some(rs) => self.evaluate_ready_tasks(dag, rs),
                None => return,
            }
        };

        self.transition_and_dispatch(dag_id, &run_key, ready);

        // Re-borrow to check completion after dispatch state mutations.
        if let (Some(dag), Some(rs)) = (self.plans.get(dag_id), self.dag_runs.get(&run_key)) {
            self.check_dag_run_complete(dag, rs);
        }
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

            if let Some(m) = metrics::try_global() {
                m.record_task_event(dag_id, task_id, "retried");
            }

            self.send_command(SchedulerCommand::RetryTask {
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

            if let Some(m) = metrics::try_global() {
                m.record_task_event(dag_id, task_id, "failed");
                m.active_tasks.dec();
            }

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
    /// Find tasks ready to dispatch. Pure of dispatch side-effects so the
    /// caller can atomically mutate state and emit commands together — the
    /// previous `&self`-only signature caused duplicate dispatches when
    /// multiple events arrived between a dispatch and its completion.
    fn evaluate_ready_tasks(&self, dag: &Dag, run_state: &DagRunState) -> Vec<TaskId> {
        let evaluator = TriggerRuleEvaluator::new();
        let mut ready = Vec::new();

        for task_id in &dag.execution_order {
            let current_state = run_state
                .task_states
                .get(task_id)
                .cloned()
                .unwrap_or(TaskState::Pending);

            // Only Pending tasks are candidates for dispatch.
            if !matches!(current_state, TaskState::Pending) {
                continue;
            }

            let task = &dag.tasks[task_id];
            if evaluator.evaluate(&task.trigger_rule, task_id, dag, run_state) {
                ready.push(task_id.clone());
            }
        }
        ready
    }

    /// Check if a DAG run is complete (all tasks terminal).
    fn check_dag_run_complete(&self, dag: &Dag, run_state: &DagRunState) {
        let all_terminal = dag.execution_order.iter().all(|task_id| {
            matches!(
                run_state.task_states.get(task_id),
                Some(
                    TaskState::Success { .. }
                        | TaskState::Failed { .. }
                        | TaskState::Skipped { .. }
                )
            )
        });

        if all_terminal {
            let has_failures = dag.execution_order.iter().any(|task_id| {
                matches!(
                    run_state.task_states.get(task_id),
                    Some(TaskState::Failed { .. })
                )
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

            // Record metrics
            if let Some(m) = metrics::try_global() {
                let status_str = match status {
                    RunStatus::Success => "success",
                    RunStatus::Failed => "failed",
                    RunStatus::Cancelled => "cancelled",
                };
                m.dag_runs_total
                    .get_or_create(&metrics::DagStatusLabels {
                        dag_id: dag.id.clone(),
                        status: status_str.to_string(),
                    })
                    .inc();
                let duration_seconds =
                    (Utc::now() - run_state.started_at).num_milliseconds() as f64 / 1000.0;
                m.observe_dag_run_duration(&dag.id, duration_seconds);
                m.active_dag_runs.dec();
            }

            // Fire alert hooks for non-success terminal states. Each hook
            // runs on its own tokio task so a slow / failing notifier can't
            // stall the scheduler event loop or other hooks. Errors are
            // logged and swallowed — alert delivery is never load-bearing.
            if let Some(alert_status) = crate::alerts::AlertStatus::from_run_status(status) {
                if !self.alert_hooks.is_empty() {
                    let event = self.build_alert_event(dag, run_state, alert_status);
                    for hook in &self.alert_hooks {
                        let hook = Arc::clone(hook);
                        let event = event.clone();
                        tokio::spawn(async move {
                            if let Err(e) = hook.fire(&event).await {
                                error!(
                                    hook = hook.name(),
                                    dag_id = %event.dag_id,
                                    run_id = %event.run_id,
                                    error = %e,
                                    "Alert hook failed"
                                );
                            }
                        });
                    }
                }
            }

            self.send_command(SchedulerCommand::CompleteDagRun {
                dag_id: dag.id.clone(),
                run_id: run_state.run_id.clone(),
                status,
            });
        }
    }

    /// Build the `AlertEvent` payload for a terminal DAG run. Collects the
    /// failed-task list with each task's last error message so alert
    /// recipients have enough context to triage without re-querying state.
    fn build_alert_event(
        &self,
        dag: &Dag,
        run_state: &DagRunState,
        status: crate::alerts::AlertStatus,
    ) -> crate::alerts::AlertEvent {
        let failed_tasks: Vec<(TaskId, String)> = dag
            .execution_order
            .iter()
            .filter_map(|task_id| match run_state.task_states.get(task_id) {
                Some(TaskState::Failed { error, .. }) => Some((task_id.clone(), error.clone())),
                _ => None,
            })
            .collect();

        crate::alerts::AlertEvent {
            dag_id: dag.id.clone(),
            run_id: run_state.run_id.clone(),
            status,
            started_at: run_state.started_at,
            completed_at: Utc::now(),
            failed_tasks,
            config: run_state.config.clone(),
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

        // Apply skips to internal state, collecting commands to send afterward.
        let mut commands_to_send: Vec<SchedulerCommand> = Vec::new();
        if let Some(run_state_mut) = self.dag_runs.get_mut(run_key) {
            for task_id in &tasks_to_skip {
                run_state_mut.task_states.insert(
                    task_id.clone(),
                    TaskState::Skipped {
                        reason: "Upstream task failed".to_string(),
                        skipped_at: Utc::now(),
                    },
                );

                if let Some(m) = metrics::try_global() {
                    m.record_task_event(&dag.id, task_id, "skipped");
                }

                commands_to_send.push(SchedulerCommand::SkipTask {
                    dag_id: dag.id.clone(),
                    run_id: run_state_mut.run_id.clone(),
                    task_id: task_id.clone(),
                    reason: "Upstream task failed".to_string(),
                });
            }
        }

        // Send commands after releasing the mutable borrow.
        for cmd in commands_to_send {
            self.send_command(cmd);
        }

        // Re-borrow immutably to evaluate tasks that may now be ready
        // (e.g., AllDone, OneSuccess, OneFailed downstream of the failure)
        // and check if the run is now complete.
        let ready = match self.dag_runs.get(run_key) {
            Some(rs) => self.evaluate_ready_tasks(dag, rs),
            None => return,
        };
        self.transition_and_dispatch(&dag.id, run_key, ready);
        if let Some(rs) = self.dag_runs.get(run_key) {
            self.check_dag_run_complete(dag, rs);
        }
    }

    /// Atomically move each task in `ready` to `Queued` in the stored run
    /// state and emit one `DispatchTask` command per task. The state
    /// transition happens *before* the command is sent, so any concurrent
    /// `evaluate_ready_tasks` (triggered by interleaved events) sees the
    /// task as no-longer-Pending and skips it. This is the invariant that
    /// prevents duplicate dispatches.
    fn transition_and_dispatch(&mut self, dag_id: &DagId, run_key: &str, ready: Vec<TaskId>) {
        if ready.is_empty() {
            return;
        }

        let run_id = {
            let Some(rs) = self.dag_runs.get_mut(run_key) else {
                return;
            };
            for task_id in &ready {
                rs.task_states.insert(task_id.clone(), TaskState::Queued);
            }
            rs.run_id.clone()
        };

        for task_id in ready {
            if let Some(m) = metrics::try_global() {
                m.record_task_event(dag_id, &task_id, "dispatched");
                m.active_tasks.inc();
            }

            self.send_command(SchedulerCommand::DispatchTask {
                dag_id: dag_id.clone(),
                run_id: run_id.clone(),
                task_id: task_id.clone(),
                attempt: 0,
            });

            debug!(
                dag_id = %dag_id,
                run_id = %run_id,
                task_id = %task_id,
                "Task dispatched"
            );
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
        assert_eq!(
            parse_duration(&Some("30s".to_string())),
            Duration::seconds(30)
        );
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
        assert_eq!(parse_duration(&Some("2h".to_string())), Duration::hours(2));
    }

    #[test]
    fn test_parse_duration_default() {
        assert_eq!(parse_duration(&None), Duration::minutes(5));
    }
}
