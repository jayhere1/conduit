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
use conduit_common::event::{EventKind, RunStatus as EventRunStatus};
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
    /// A task's retry delay has elapsed; re-dispatch it. Produced internally
    /// by the scheduler's own retry timers, never by external callers.
    TaskRetryReady {
        dag_id: DagId,
        run_id: String,
        task_id: TaskId,
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
    /// Internal wake channel for the scheduler's own timers (retry delays).
    /// Kept separate from `event_rx` so retry wakeups work even when the
    /// external event sender has been cloned/dropped by the embedding app.
    self_tx: mpsc::UnboundedSender<SchedulerEvent>,
    self_rx: mpsc::UnboundedReceiver<SchedulerEvent>,
    dag_runs: HashMap<String, DagRunState>,
    pools: PoolManager,
    plans: HashMap<DagId, Dag>,
    cron_schedules: HashMap<DagId, CronSchedule>,
    /// Optional persistent event store for catchup queries.
    event_store: Option<Arc<EventStore>>,
    /// Last minute (unix_ts / 60) each DAG was cron-fired. Five-field cron
    /// has minute resolution; this guard makes ticks idempotent within a
    /// minute so the tick source's frequency can't create duplicate runs.
    last_cron_fire: HashMap<DagId, i64>,
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

        let (self_tx, self_rx) = mpsc::unbounded_channel();

        Ok(Self {
            event_rx,
            command_tx,
            self_tx,
            self_rx,
            dag_runs: HashMap::new(),
            pools,
            plans,
            cron_schedules,
            event_store: None,
            last_cron_fire: HashMap::new(),
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

    /// Release the pool slot a task held, if it draws from a pool.
    /// Returns true when a slot was actually freed.
    fn release_pool_slot(&mut self, dag_id: &DagId, run_key: &str, task_id: &TaskId) -> bool {
        let Some(pool) = self
            .plans
            .get(dag_id)
            .and_then(|d| d.tasks.get(task_id))
            .and_then(|t| t.pool.clone())
        else {
            return false;
        };
        self.pools
            .release(&pool, &format!("{}/{}", run_key, task_id));
        true
    }

    /// Re-evaluate every active run and dispatch newly-eligible tasks.
    /// Called after a pool slot frees up: the waiter may live in a
    /// different run than the task that released the slot.
    fn dispatch_waiting_runs(&mut self) {
        let runs: Vec<(DagId, String)> = self
            .dag_runs
            .values()
            .map(|rs| (rs.dag_id.clone(), format!("{}/{}", rs.dag_id, rs.run_id)))
            .collect();
        for (dag_id, run_key) in runs {
            let ready = {
                let (Some(dag), Some(rs)) = (self.plans.get(&dag_id), self.dag_runs.get(&run_key))
                else {
                    continue;
                };
                self.evaluate_ready_tasks(dag, rs)
            };
            self.transition_and_dispatch(&dag_id, &run_key, ready);
        }
    }

    /// Best-effort append to the attached event store. Persistence powers
    /// `conduit replay` and post-hoc debugging; it must never stall or fail
    /// scheduling, so errors are logged and swallowed.
    fn persist_event(&self, kind: EventKind) {
        if let Some(store) = &self.event_store {
            if let Err(e) = store.append(kind) {
                warn!(error = %e, "Failed to persist scheduler event");
            }
        }
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

        loop {
            // Merge external events with the scheduler's own timer wakeups.
            // `self_rx` can never yield None while `self` holds `self_tx`;
            // a closed external channel ends the loop like it always did.
            let event = tokio::select! {
                ev = self.event_rx.recv() => match ev {
                    Some(e) => e,
                    None => break,
                },
                ev = self.self_rx.recv() => match ev {
                    Some(e) => e,
                    None => break,
                },
            };

            // Record scheduler event metric
            if let Some(m) = metrics::try_global() {
                let event_type = match &event {
                    SchedulerEvent::DagRunRequested { .. } => "dag_run_requested",
                    SchedulerEvent::TaskCompleted { .. } => "task_completed",
                    SchedulerEvent::TaskFailed { .. } => "task_failed",
                    SchedulerEvent::CronTick { .. } => "cron_tick",
                    SchedulerEvent::SensorTriggered { .. } => "sensor_triggered",
                    SchedulerEvent::TaskRetryReady { .. } => "task_retry_ready",
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
                SchedulerEvent::TaskRetryReady {
                    dag_id,
                    run_id,
                    task_id,
                } => {
                    self.handle_task_retry_ready(&dag_id, &run_id, &task_id);
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

        // Callers can pass environment/triggered_by via run config.
        let (environment, triggered_by) = {
            let config = &self.dag_runs[&run_key].config;
            (
                config
                    .get("environment")
                    .cloned()
                    .unwrap_or_else(|| "production".to_string()),
                config
                    .get("triggered_by")
                    .cloned()
                    .unwrap_or_else(|| "scheduler".to_string()),
            )
        };
        self.persist_event(EventKind::DagRunCreated {
            dag_id: dag_id.clone(),
            run_id: run_id.to_string(),
            logical_date,
            environment,
            triggered_by,
        });

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
        let snapshot_id_for_event = snapshot_id.clone();

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

        self.persist_event(EventKind::TaskCompleted {
            dag_id: dag_id.clone(),
            run_id: run_id.to_string(),
            task_id: task_id.clone(),
            duration_ms,
            snapshot_id: snapshot_id_for_event,
        });

        let released_pool_slot = self.release_pool_slot(dag_id, &run_key, task_id);

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

        // A freed pool slot may unblock tasks in other runs.
        if released_pool_slot {
            self.dispatch_waiting_runs();
        }

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
                        let base = parse_duration(&task.retry_delay);
                        (
                            true,
                            retry_delay_for_attempt(base, attempt, task.retry_backoff),
                        )
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

        // The failed attempt no longer occupies its pool slot. A retry will
        // re-acquire when it is re-dispatched.
        if self.release_pool_slot(dag_id, &run_key, task_id) {
            self.dispatch_waiting_runs();
        }

        if should_retry {
            debug!(
                dag_id = %dag_id,
                run_id = %run_id,
                task_id = %task_id,
                attempt = %attempt,
                "Task will be retried"
            );

            self.persist_event(EventKind::TaskRetrying {
                dag_id: dag_id.clone(),
                run_id: run_id.to_string(),
                task_id: task_id.clone(),
                attempt: attempt + 1,
                next_retry_at: Utc::now() + retry_delay,
            });

            if let Some(m) = metrics::try_global() {
                m.record_task_event(dag_id, task_id, "retried");
            }

            self.send_command(SchedulerCommand::RetryTask {
                dag_id: dag_id.clone(),
                run_id: run_id.to_string(),
                task_id: task_id.clone(),
                delay: retry_delay,
            });

            // Arm the retry timer: once the delay elapses, wake the event
            // loop via the internal channel and re-dispatch the task. The
            // RetryTask command above is a notification for executors/UIs;
            // this timer is what actually makes the retry happen.
            let self_tx = self.self_tx.clone();
            let (wake_dag, wake_run, wake_task) =
                (dag_id.clone(), run_id.to_string(), task_id.clone());
            let sleep_for = retry_delay.to_std().unwrap_or_default();
            tokio::spawn(async move {
                tokio::time::sleep(sleep_for).await;
                let _ = self_tx.send(SchedulerEvent::TaskRetryReady {
                    dag_id: wake_dag,
                    run_id: wake_run,
                    task_id: wake_task,
                });
            });
        } else {
            warn!(
                dag_id = %dag_id,
                run_id = %run_id,
                task_id = %task_id,
                error = %error,
                "Task failed with no retries remaining"
            );

            self.persist_event(EventKind::TaskFailed {
                dag_id: dag_id.clone(),
                run_id: run_id.to_string(),
                task_id: task_id.clone(),
                error: error.clone(),
                traceback: None,
                attempt,
            });

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

    /// Re-dispatch a task whose retry delay has elapsed.
    ///
    /// Idempotent: only acts if the task is still `Retrying` — a duplicate
    /// or stale wakeup (run finished, task re-resolved some other way) is
    /// silently ignored.
    fn handle_task_retry_ready(&mut self, dag_id: &DagId, run_id: &str, task_id: &TaskId) {
        let run_key = format!("{}/{}", dag_id, run_id);

        let attempt = match self
            .dag_runs
            .get(&run_key)
            .and_then(|rs| rs.task_states.get(task_id))
        {
            Some(TaskState::Retrying { attempt, .. }) => *attempt,
            _ => {
                debug!(
                    dag_id = %dag_id,
                    run_id = %run_id,
                    task_id = %task_id,
                    "Ignoring stale retry wakeup — task no longer Retrying"
                );
                return;
            }
        };

        // Pooled retries re-acquire their slot; if the pool is full, check
        // again shortly rather than stealing or deadlocking.
        let pool = self
            .plans
            .get(dag_id)
            .and_then(|d| d.tasks.get(task_id))
            .and_then(|t| t.pool.clone());
        if let Some(p) = &pool {
            if !self.pools.acquire(p, &format!("{}/{}", run_key, task_id)) {
                debug!(
                    dag_id = %dag_id,
                    task_id = %task_id,
                    pool = %p,
                    "Pool full at retry time — re-checking in 1s"
                );
                let self_tx = self.self_tx.clone();
                let (wake_dag, wake_run, wake_task) =
                    (dag_id.clone(), run_id.to_string(), task_id.clone());
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let _ = self_tx.send(SchedulerEvent::TaskRetryReady {
                        dag_id: wake_dag,
                        run_id: wake_run,
                        task_id: wake_task,
                    });
                });
                return;
            }
        }

        if let Some(rs) = self.dag_runs.get_mut(&run_key) {
            rs.task_states.insert(task_id.clone(), TaskState::Queued);
        }

        info!(
            dag_id = %dag_id,
            run_id = %run_id,
            task_id = %task_id,
            attempt = %attempt,
            "Retry delay elapsed — re-dispatching task"
        );

        if let Some(m) = metrics::try_global() {
            m.record_task_event(dag_id, task_id, "dispatched");
            m.active_tasks.inc();
        }

        self.send_command(SchedulerCommand::DispatchTask {
            dag_id: dag_id.clone(),
            run_id: run_id.to_string(),
            task_id: task_id.clone(),
            attempt,
        });
    }

    /// Handle periodic cron tick.
    fn handle_cron_tick(&mut self, timestamp: DateTime<Utc>) {
        let minute = timestamp.timestamp() / 60;

        // Collect due DAGs first to avoid borrowing self immutably while
        // mutating. Skip DAGs already fired this minute — `is_due` matches
        // the whole minute, so ticks arriving more frequently than 1/min
        // must not create duplicate runs.
        let due_dags: Vec<DagId> = self
            .cron_schedules
            .iter()
            .filter(|(dag_id, cron)| {
                cron.is_due(timestamp) && self.last_cron_fire.get(*dag_id) != Some(&minute)
            })
            .map(|(dag_id, _)| dag_id.clone())
            .collect();

        for dag_id in due_dags {
            self.last_cron_fire.insert(dag_id.clone(), minute);
            let run_id = format!("{}_{}", dag_id, timestamp.timestamp());

            info!(
                dag_id = %dag_id,
                run_id = %run_id,
                "Creating DAG run from cron schedule"
            );

            self.handle_dag_run_requested(&dag_id, &run_id, timestamp, HashMap::new());
        }
    }

    /// Handle an externally-delivered sensor trigger.
    ///
    /// Not supported: Conduit sensors are **poll-only** — a `Sensor` task
    /// polls its condition at `poke_interval` in the executor
    /// (`conduit-executor`) until it succeeds or times out. There is no
    /// event-driven "push" path that unblocks a waiting sensor from an
    /// external signal. This handler exists only so the scheduler doesn't
    /// silently drop such an event; it warns loudly instead. If a real
    /// push-trigger use case appears, it needs its own design (waiting-task
    /// registry + wake path), not a fill-in here.
    fn handle_sensor_triggered(&mut self, sensor_id: &str, _payload: HashMap<String, String>) {
        warn!(
            sensor_id = %sensor_id,
            "External sensor trigger received but Conduit sensors are poll-only — ignoring. \
             Use a Sensor task with a poke_interval instead."
        );
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

            self.persist_event(EventKind::DagRunCompleted {
                dag_id: dag.id.clone(),
                run_id: run_state.run_id.clone(),
                status: match status {
                    RunStatus::Success => EventRunStatus::Success,
                    RunStatus::Failed => EventRunStatus::Failed,
                    RunStatus::Cancelled => EventRunStatus::Cancelled,
                },
                duration_ms: (Utc::now() - run_state.started_at)
                    .num_milliseconds()
                    .max(0) as u64,
            });

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
            if let SchedulerCommand::SkipTask {
                dag_id,
                run_id,
                task_id,
                reason,
            } = &cmd
            {
                self.persist_event(EventKind::TaskSkipped {
                    dag_id: dag_id.clone(),
                    run_id: run_id.clone(),
                    task_id: task_id.clone(),
                    reason: reason.clone(),
                });
            }
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

        // Pool gate: a task in a full pool stays Pending and is re-elected
        // on a later event (a pooled task completing releases its slot and
        // re-evaluates all runs).
        let mut dispatchable = Vec::new();
        for task_id in ready {
            let pool = self
                .plans
                .get(dag_id)
                .and_then(|d| d.tasks.get(&task_id))
                .and_then(|t| t.pool.clone());
            let acquired = match &pool {
                Some(p) => self.pools.acquire(p, &format!("{}/{}", run_key, task_id)),
                None => true,
            };
            if acquired {
                dispatchable.push(task_id);
            } else {
                debug!(
                    dag_id = %dag_id,
                    task_id = %task_id,
                    pool = %pool.as_deref().unwrap_or(""),
                    "Pool full — task waits for a slot"
                );
            }
        }
        if dispatchable.is_empty() {
            return;
        }

        let run_id = {
            let Some(rs) = self.dag_runs.get_mut(run_key) else {
                return;
            };
            for task_id in &dispatchable {
                rs.task_states.insert(task_id.clone(), TaskState::Queued);
            }
            rs.run_id.clone()
        };

        for task_id in dispatchable {
            if let Some(m) = metrics::try_global() {
                m.record_task_event(dag_id, &task_id, "dispatched");
                m.active_tasks.inc();
            }

            let (priority, pool) = self
                .plans
                .get(dag_id)
                .and_then(|d| d.tasks.get(&task_id))
                .map(|t| (t.priority, t.pool.clone()))
                .unwrap_or((0, None));
            self.persist_event(EventKind::TaskQueued {
                dag_id: dag_id.clone(),
                run_id: run_id.clone(),
                task_id: task_id.clone(),
                priority,
                pool,
                snapshot_fingerprint: None,
            });

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

/// Compute the retry delay for a given attempt.
///
/// Without a backoff multiplier (or with a multiplier <= 1.0) the delay is
/// fixed at `base` for every attempt. With a multiplier > 1.0 the delay
/// grows exponentially: `base * backoff^attempt`, capped at one day so a
/// large attempt count can never overflow or park a task for years.
fn retry_delay_for_attempt(base: Duration, attempt: u32, backoff: Option<f64>) -> Duration {
    const MAX_DELAY_SECS: f64 = 86_400.0; // 1 day

    let factor = match backoff {
        Some(b) if b > 1.0 => b,
        _ => return base,
    };

    let base_secs = base.num_milliseconds() as f64 / 1000.0;
    let scaled = base_secs * factor.powi(attempt.min(1_000) as i32);
    let capped = scaled.min(MAX_DELAY_SECS);
    Duration::milliseconds((capped * 1000.0) as i64)
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

    #[test]
    fn retry_delay_without_backoff_is_fixed_across_attempts() {
        let base = Duration::seconds(10);
        assert_eq!(retry_delay_for_attempt(base, 0, None), base);
        assert_eq!(retry_delay_for_attempt(base, 3, None), base);
    }

    #[test]
    fn retry_delay_with_backoff_grows_exponentially() {
        let base = Duration::seconds(10);
        assert_eq!(
            retry_delay_for_attempt(base, 0, Some(2.0)),
            Duration::seconds(10)
        );
        assert_eq!(
            retry_delay_for_attempt(base, 1, Some(2.0)),
            Duration::seconds(20)
        );
        assert_eq!(
            retry_delay_for_attempt(base, 2, Some(2.0)),
            Duration::seconds(40)
        );
    }

    #[test]
    fn retry_delay_with_backoff_is_capped() {
        // A huge attempt count must not overflow — capped at 1 day.
        let base = Duration::seconds(60);
        assert_eq!(
            retry_delay_for_attempt(base, 1000, Some(10.0)),
            Duration::days(1)
        );
    }

    #[test]
    fn retry_delay_backoff_of_one_or_less_behaves_as_fixed() {
        let base = Duration::seconds(10);
        assert_eq!(retry_delay_for_attempt(base, 5, Some(1.0)), base);
        assert_eq!(retry_delay_for_attempt(base, 5, Some(0.5)), base);
    }
}
