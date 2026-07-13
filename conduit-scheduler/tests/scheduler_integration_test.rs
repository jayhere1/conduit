//! Integration tests for the Conduit scheduler.
//!
//! These tests exercise the full scheduler event loop by sending events
//! through channels and asserting on the commands that come out.

use std::collections::HashMap;

use chrono::{TimeZone, Utc};
use tokio::sync::mpsc;

use conduit_common::dag::{
    Dag, DependencyType, Pool, ResourceLimits, Task, TaskDependency, TaskType, TriggerRule,
};
use conduit_scheduler::{CronSchedule, PoolManager, Scheduler, SchedulerCommand, SchedulerEvent};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a minimal Task definition.
fn make_task(
    id: &str,
    deps: Vec<&str>,
    trigger_rule: TriggerRule,
    retries: u32,
    retry_delay: Option<&str>,
) -> Task {
    Task {
        id: id.to_string(),
        task_type: TaskType::Bash {
            command: format!("echo {}", id),
        },
        dependencies: deps
            .into_iter()
            .map(|d| TaskDependency {
                task_id: d.to_string(),
                dependency_type: DependencyType::ExecutionOrder,
            })
            .collect(),
        retries,
        retry_delay: retry_delay.map(|s| s.to_string()),
        retry_backoff: None,
        source_hash: None,
        pool: None,
        timeout: None,
        priority: 0,
        resources: ResourceLimits::default(),
        trigger_rule,
        incremental: None,
        contracts: None,
        inputs: Vec::new(),
        outputs: Vec::new(),
    }
}

/// Build a Dag from a list of tasks and an explicit execution order.
fn make_dag(id: &str, tasks: Vec<Task>, execution_order: Vec<&str>) -> Dag {
    let mut task_map = HashMap::new();
    for t in tasks {
        task_map.insert(t.id.clone(), t);
    }
    Dag {
        id: id.to_string(),
        description: None,
        schedule: None,
        tags: vec![],
        max_active_runs: 1,
        on_failure: None,
        tasks: task_map,
        execution_order: execution_order.into_iter().map(|s| s.to_string()).collect(),
        source_file: "test.rs".to_string(),
        compiled_at: Utc::now(),
        catchup: false,
        max_catchup_runs: None,
        lineage_strict: false,
    }
}

/// Create a Scheduler wired to unbounded channels and return the pieces
/// needed by a test: the event sender, command receiver, and a future that
/// drives the scheduler event loop.
fn create_test_scheduler(
    plans: HashMap<String, Dag>,
) -> (
    mpsc::UnboundedSender<SchedulerEvent>,
    mpsc::UnboundedReceiver<SchedulerCommand>,
    impl std::future::Future<Output = ()>,
) {
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (command_tx, command_rx) = mpsc::unbounded_channel();

    let pools = PoolManager::new(vec![Pool {
        name: "default".to_string(),
        slots: 128,
        description: None,
    }]);

    let scheduler =
        Scheduler::new(event_rx, command_tx, pools, plans).expect("Scheduler::new should succeed");

    let handle = async move {
        let _ = scheduler.run().await;
    };

    (event_tx, command_rx, handle)
}

/// Drain all currently buffered commands from the receiver.
fn drain_commands(rx: &mut mpsc::UnboundedReceiver<SchedulerCommand>) -> Vec<SchedulerCommand> {
    let mut cmds = Vec::new();
    while let Ok(cmd) = rx.try_recv() {
        cmds.push(cmd);
    }
    cmds
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// DAG: A -> B (AllSuccess on B).
/// Complete A successfully. Assert B gets dispatched.
#[tokio::test]
async fn test_all_success_trigger_rule() {
    let task_a = make_task("A", vec![], TriggerRule::AllSuccess, 0, None);
    let task_b = make_task("B", vec!["A"], TriggerRule::AllSuccess, 0, None);
    let dag = make_dag("dag1", vec![task_a, task_b], vec!["A", "B"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, scheduler_fut) = create_test_scheduler(plans);

    // Request a DAG run
    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();

    // Complete task A
    tx.send(SchedulerEvent::TaskCompleted {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "A".to_string(),
        snapshot_id: None,
        duration_ms: 100,
    })
    .unwrap();

    // Shutdown so the event loop exits
    tx.send(SchedulerEvent::Shutdown).unwrap();

    scheduler_fut.await;

    let cmds = drain_commands(&mut rx);

    // First command: DispatchTask for A (root task, dispatched immediately)
    assert!(
        cmds.iter()
            .any(|c| matches!(c, SchedulerCommand::DispatchTask { task_id, .. } if task_id == "A")),
        "Expected task A to be dispatched, got: {:?}",
        cmds
    );

    // Second: DispatchTask for B (after A completes)
    assert!(
        cmds.iter()
            .any(|c| matches!(c, SchedulerCommand::DispatchTask { task_id, .. } if task_id == "B")),
        "Expected task B to be dispatched after A completed, got: {:?}",
        cmds
    );
}

/// DAG: A -> B (AllDone on B).
/// Fail A. Assert B is still dispatched because AllDone only needs terminal state.
#[tokio::test]
async fn test_all_done_trigger_rule() {
    let task_a = make_task("A", vec![], TriggerRule::AllSuccess, 0, None);
    let mut task_b = make_task("B", vec!["A"], TriggerRule::AllDone, 0, None);
    task_b.trigger_rule = TriggerRule::AllDone;
    let dag = make_dag("dag1", vec![task_a, task_b], vec!["A", "B"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, scheduler_fut) = create_test_scheduler(plans);

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();

    // Fail task A (no retries)
    tx.send(SchedulerEvent::TaskFailed {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "A".to_string(),
        error: "boom".to_string(),
        attempt: 0,
    })
    .unwrap();

    tx.send(SchedulerEvent::Shutdown).unwrap();
    scheduler_fut.await;

    let cmds = drain_commands(&mut rx);

    // B should be dispatched even though A failed (AllDone)
    assert!(
        cmds.iter()
            .any(|c| matches!(c, SchedulerCommand::DispatchTask { task_id, .. } if task_id == "B")),
        "Expected task B to be dispatched (AllDone) even after A failed, got: {:?}",
        cmds
    );
}

/// DAG: A, B -> C (OneSuccess on C).
/// Complete A, fail B. Assert C is dispatched.
#[tokio::test]
async fn test_one_success_trigger_rule() {
    let task_a = make_task("A", vec![], TriggerRule::AllSuccess, 0, None);
    let task_b = make_task("B", vec![], TriggerRule::AllSuccess, 0, None);
    let task_c = make_task("C", vec!["A", "B"], TriggerRule::OneSuccess, 0, None);
    let dag = make_dag("dag1", vec![task_a, task_b, task_c], vec!["A", "B", "C"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, scheduler_fut) = create_test_scheduler(plans);

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();

    // Complete A
    tx.send(SchedulerEvent::TaskCompleted {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "A".to_string(),
        snapshot_id: None,
        duration_ms: 50,
    })
    .unwrap();

    // Fail B
    tx.send(SchedulerEvent::TaskFailed {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "B".to_string(),
        error: "fail".to_string(),
        attempt: 0,
    })
    .unwrap();

    tx.send(SchedulerEvent::Shutdown).unwrap();
    scheduler_fut.await;

    let cmds = drain_commands(&mut rx);

    // C should be dispatched because A succeeded (OneSuccess)
    assert!(
        cmds.iter()
            .any(|c| matches!(c, SchedulerCommand::DispatchTask { task_id, .. } if task_id == "C")),
        "Expected task C to be dispatched (OneSuccess) since A succeeded, got: {:?}",
        cmds
    );
}

/// Task with retries: 2. Fail on first attempt. Assert a RetryTask command is issued.
#[tokio::test]
async fn test_retry_on_failure() {
    let task_a = make_task("A", vec![], TriggerRule::AllSuccess, 2, Some("5s"));
    let dag = make_dag("dag1", vec![task_a], vec!["A"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, scheduler_fut) = create_test_scheduler(plans);

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();

    // First attempt fails (attempt 0 < retries 2, so retry expected)
    tx.send(SchedulerEvent::TaskFailed {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "A".to_string(),
        error: "transient error".to_string(),
        attempt: 0,
    })
    .unwrap();

    tx.send(SchedulerEvent::Shutdown).unwrap();
    scheduler_fut.await;

    let cmds = drain_commands(&mut rx);

    assert!(
        cmds.iter()
            .any(|c| matches!(c, SchedulerCommand::RetryTask { task_id, .. } if task_id == "A")),
        "Expected a RetryTask command for task A, got: {:?}",
        cmds
    );
}

/// Test CronSchedule::parse() with valid expressions.
#[test]
fn test_cron_expression_parsing() {
    let valid_expressions = vec!["0 6 * * *", "*/15 * * * *", "0 0 1 * *"];

    for expr in valid_expressions {
        assert!(
            CronSchedule::parse(expr).is_ok(),
            "Expected '{}' to parse successfully",
            expr
        );
    }
}

/// Parse "0 6 * * *". Check is_due at 06:00 (true) and 07:00 (false).
#[test]
fn test_cron_is_due() {
    let cron = CronSchedule::parse("0 6 * * *").expect("valid cron");

    let at_6am = Utc.with_ymd_and_hms(2026, 4, 5, 6, 0, 0).unwrap();
    assert!(cron.is_due(at_6am), "Expected cron to be due at 06:00");

    let at_7am = Utc.with_ymd_and_hms(2026, 4, 5, 7, 0, 0).unwrap();
    assert!(!cron.is_due(at_7am), "Expected cron to NOT be due at 07:00");
}

/// Test CronSchedule::parse() with invalid expressions.
#[test]
fn test_invalid_cron_expression() {
    let invalid_expressions = vec!["invalid", "1 2 3", "60 * * * *"];

    for expr in invalid_expressions {
        assert!(
            CronSchedule::parse(expr).is_err(),
            "Expected '{}' to fail parsing",
            expr
        );
    }
}

/// Task with NoDeps trigger rule and no dependencies should be immediately dispatchable.
#[tokio::test]
async fn test_trigger_evaluation_no_deps() {
    let task_a = make_task("A", vec![], TriggerRule::NoDeps, 0, None);
    let dag = make_dag("dag1", vec![task_a], vec!["A"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, scheduler_fut) = create_test_scheduler(plans);

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();

    tx.send(SchedulerEvent::Shutdown).unwrap();
    scheduler_fut.await;

    let cmds = drain_commands(&mut rx);

    assert!(
        cmds.iter()
            .any(|c| matches!(c, SchedulerCommand::DispatchTask { task_id, .. } if task_id == "A")),
        "Expected task A (NoDeps) to be dispatched immediately, got: {:?}",
        cmds
    );
}

/// Repro for the duplicate-dispatch bug: when a task is Pending and multiple
/// events arrive before the executor's TaskCompleted comes back, the scheduler
/// would re-elect it. After the fix, a task transitions to Queued at dispatch
/// time and is never re-dispatched on subsequent evaluate passes.
#[tokio::test]
async fn dispatch_is_idempotent_under_event_storm() {
    let task_a = make_task("A", vec![], TriggerRule::AllSuccess, 0, None);
    let task_b = make_task("B", vec!["A"], TriggerRule::AllSuccess, 0, None);
    let dag = make_dag("dag1", vec![task_a, task_b], vec!["A", "B"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, scheduler_fut) = create_test_scheduler(plans);

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();

    // Fire 50 spurious CronTicks before A completes — each previously re-elected
    // A because state was still Pending.
    for _ in 0..50 {
        tx.send(SchedulerEvent::CronTick {
            timestamp: Utc::now(),
        })
        .unwrap();
    }

    tx.send(SchedulerEvent::TaskCompleted {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "A".to_string(),
        snapshot_id: None,
        duration_ms: 1,
    })
    .unwrap();

    // Another storm after A completes — same logic for B.
    for _ in 0..50 {
        tx.send(SchedulerEvent::CronTick {
            timestamp: Utc::now(),
        })
        .unwrap();
    }

    tx.send(SchedulerEvent::Shutdown).unwrap();
    scheduler_fut.await;

    let cmds = drain_commands(&mut rx);
    let dispatches_a = cmds
        .iter()
        .filter(|c| matches!(c, SchedulerCommand::DispatchTask { task_id, .. } if task_id == "A"))
        .count();
    let dispatches_b = cmds
        .iter()
        .filter(|c| matches!(c, SchedulerCommand::DispatchTask { task_id, .. } if task_id == "B"))
        .count();

    assert_eq!(
        dispatches_a, 1,
        "A dispatched {} times, expected 1",
        dispatches_a
    );
    assert_eq!(
        dispatches_b, 1,
        "B dispatched {} times, expected 1",
        dispatches_b
    );
}

/// Duplicate TaskCompleted events should be dropped, not allowed to overwrite
/// terminal state.
#[tokio::test]
async fn duplicate_task_completed_is_ignored() {
    let task_a = make_task("A", vec![], TriggerRule::AllSuccess, 0, None);
    let task_b = make_task("B", vec!["A"], TriggerRule::AllSuccess, 0, None);
    let dag = make_dag("dag1", vec![task_a, task_b], vec!["A", "B"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, scheduler_fut) = create_test_scheduler(plans);

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();

    for _ in 0..5 {
        tx.send(SchedulerEvent::TaskCompleted {
            dag_id: "dag1".to_string(),
            run_id: "run1".to_string(),
            task_id: "A".to_string(),
            snapshot_id: None,
            duration_ms: 1,
        })
        .unwrap();
    }

    tx.send(SchedulerEvent::Shutdown).unwrap();
    scheduler_fut.await;

    let cmds = drain_commands(&mut rx);
    let dispatches_b = cmds
        .iter()
        .filter(|c| matches!(c, SchedulerCommand::DispatchTask { task_id, .. } if task_id == "B"))
        .count();
    assert_eq!(
        dispatches_b, 1,
        "B dispatched {} times after duplicate completions, expected 1",
        dispatches_b
    );
}

// ---------------------------------------------------------------------------
// Alert hooks (Bet 3)
// ---------------------------------------------------------------------------

use conduit_scheduler::{AlertEvent, AlertHook, AlertStatus};
use std::sync::Arc as StdArc;
use std::sync::Mutex;

/// Test-only hook that captures every `AlertEvent::fire` call. Mirrors the
/// in-crate `RecordingHook` but is local to this integration test so we don't
/// have to promote that fixture to public API.
#[derive(Default, Clone)]
struct CapturingHook {
    events: StdArc<Mutex<Vec<AlertEvent>>>,
}

impl CapturingHook {
    fn calls(&self) -> Vec<AlertEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl AlertHook for CapturingHook {
    async fn fire(&self, event: &AlertEvent) -> Result<(), String> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
    fn name(&self) -> &'static str {
        "capturing-hook"
    }
}

fn create_test_scheduler_with_hook(
    plans: HashMap<String, Dag>,
    hook: StdArc<dyn AlertHook>,
) -> (
    mpsc::UnboundedSender<SchedulerEvent>,
    mpsc::UnboundedReceiver<SchedulerCommand>,
    impl std::future::Future<Output = ()>,
) {
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (command_tx, command_rx) = mpsc::unbounded_channel();

    let pools = PoolManager::new(vec![Pool {
        name: "default".to_string(),
        slots: 128,
        description: None,
    }]);

    let scheduler = Scheduler::new(event_rx, command_tx, pools, plans)
        .expect("Scheduler::new should succeed")
        .with_alert_hook(hook);

    let handle = async move {
        let _ = scheduler.run().await;
    };

    (event_tx, command_rx, handle)
}

/// Failed DAG run fires the alert hook exactly once with the failed task's
/// error captured in `failed_tasks`. Retries are 0 so the failure is terminal.
#[tokio::test]
async fn alert_hook_fires_on_dag_failure() {
    let task = make_task("A", vec![], TriggerRule::AllSuccess, 0, None);
    let dag = make_dag("dag1", vec![task], vec!["A"]);
    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let hook = CapturingHook::default();
    let hook_handle: StdArc<dyn AlertHook> = StdArc::new(hook.clone());
    let (tx, _rx, scheduler_fut) = create_test_scheduler_with_hook(plans, hook_handle);

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();
    tx.send(SchedulerEvent::TaskFailed {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "A".to_string(),
        error: "boom".to_string(),
        attempt: 1,
    })
    .unwrap();
    tx.send(SchedulerEvent::Shutdown).unwrap();
    scheduler_fut.await;

    // Spawned hook tasks need a moment to drain after the scheduler exits.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let calls = hook.calls();
    assert_eq!(
        calls.len(),
        1,
        "hook should fire exactly once for a failed run"
    );
    let evt = &calls[0];
    assert_eq!(evt.dag_id, "dag1");
    assert_eq!(evt.run_id, "run1");
    assert_eq!(evt.status, AlertStatus::Failed);
    assert_eq!(evt.failed_tasks.len(), 1);
    assert_eq!(evt.failed_tasks[0].0, "A");
    assert!(
        evt.failed_tasks[0].1.contains("boom"),
        "failed-task error must propagate: got {:?}",
        evt.failed_tasks[0].1
    );
}

/// Successful DAG run does NOT fire the hook — alerts are non-success only.
#[tokio::test]
async fn alert_hook_does_not_fire_on_success() {
    let task = make_task("A", vec![], TriggerRule::AllSuccess, 0, None);
    let dag = make_dag("dag1", vec![task], vec!["A"]);
    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let hook = CapturingHook::default();
    let hook_handle: StdArc<dyn AlertHook> = StdArc::new(hook.clone());
    let (tx, _rx, scheduler_fut) = create_test_scheduler_with_hook(plans, hook_handle);

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();
    tx.send(SchedulerEvent::TaskCompleted {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "A".to_string(),
        snapshot_id: None,
        duration_ms: 1,
    })
    .unwrap();
    tx.send(SchedulerEvent::Shutdown).unwrap();
    scheduler_fut.await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert!(
        hook.calls().is_empty(),
        "hook must not fire for a successful run"
    );
}

// ---------------------------------------------------------------------------
// Retry re-dispatch (the scheduler must act on its own retry decision)
// ---------------------------------------------------------------------------

/// Await the next command, panicking after `secs` seconds.
async fn next_command(
    rx: &mut mpsc::UnboundedReceiver<SchedulerCommand>,
    secs: u64,
    context: &str,
) -> SchedulerCommand {
    tokio::time::timeout(std::time::Duration::from_secs(secs), rx.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for command: {}", context))
        .unwrap_or_else(|| panic!("command channel closed while waiting for: {}", context))
}

/// A failing task with retries must be re-dispatched after its retry delay,
/// and the run must complete once the retried attempt succeeds.
#[tokio::test]
async fn failed_task_with_retries_is_redispatched_and_run_completes() {
    let task_a = make_task("A", vec![], TriggerRule::AllSuccess, 1, Some("1s"));
    let dag = make_dag("dag1", vec![task_a], vec!["A"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, scheduler_fut) = create_test_scheduler(plans);
    let driver = tokio::spawn(scheduler_fut);

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();

    // Initial dispatch, attempt 0.
    let cmd = next_command(&mut rx, 5, "initial DispatchTask for A").await;
    assert!(
        matches!(&cmd, SchedulerCommand::DispatchTask { task_id, attempt: 0, .. } if task_id == "A"),
        "expected initial DispatchTask for A attempt 0, got: {:?}",
        cmd
    );

    // The task fails on attempt 0 — one retry remains.
    tx.send(SchedulerEvent::TaskFailed {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "A".to_string(),
        error: "boom".to_string(),
        attempt: 0,
    })
    .unwrap();

    // The scheduler announces the retry...
    let cmd = next_command(&mut rx, 5, "RetryTask notification for A").await;
    assert!(
        matches!(&cmd, SchedulerCommand::RetryTask { task_id, .. } if task_id == "A"),
        "expected RetryTask for A, got: {:?}",
        cmd
    );

    // ...and after the delay elapses it must RE-DISPATCH the task itself.
    let cmd = next_command(&mut rx, 5, "re-dispatch of A after retry delay").await;
    assert!(
        matches!(&cmd, SchedulerCommand::DispatchTask { task_id, attempt: 1, .. } if task_id == "A"),
        "expected re-dispatched DispatchTask for A attempt 1, got: {:?}",
        cmd
    );

    // The retried attempt succeeds — the run must complete Success.
    tx.send(SchedulerEvent::TaskCompleted {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "A".to_string(),
        snapshot_id: None,
        duration_ms: 5,
    })
    .unwrap();

    let cmd = next_command(&mut rx, 5, "CompleteDagRun after retried success").await;
    assert!(
        matches!(
            &cmd,
            SchedulerCommand::CompleteDagRun {
                status: conduit_scheduler::RunStatus::Success,
                ..
            }
        ),
        "expected CompleteDagRun Success, got: {:?}",
        cmd
    );

    tx.send(SchedulerEvent::Shutdown).unwrap();
    let _ = driver.await;
}

/// When retries are exhausted the run must complete Failed (no hang).
#[tokio::test]
async fn exhausted_retries_complete_the_run_as_failed() {
    let task_a = make_task("A", vec![], TriggerRule::AllSuccess, 1, Some("1s"));
    let dag = make_dag("dag1", vec![task_a], vec!["A"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, scheduler_fut) = create_test_scheduler(plans);
    let driver = tokio::spawn(scheduler_fut);

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();

    let _ = next_command(&mut rx, 5, "initial DispatchTask for A").await;
    tx.send(SchedulerEvent::TaskFailed {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "A".to_string(),
        error: "boom".to_string(),
        attempt: 0,
    })
    .unwrap();

    let _ = next_command(&mut rx, 5, "RetryTask notification").await;
    let cmd = next_command(&mut rx, 5, "re-dispatch attempt 1").await;
    assert!(
        matches!(&cmd, SchedulerCommand::DispatchTask { attempt: 1, .. }),
        "expected re-dispatch attempt 1, got: {:?}",
        cmd
    );

    // Fail the retried attempt too — retries are now exhausted.
    tx.send(SchedulerEvent::TaskFailed {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "A".to_string(),
        error: "boom again".to_string(),
        attempt: 1,
    })
    .unwrap();

    let cmd = next_command(&mut rx, 5, "CompleteDagRun Failed").await;
    assert!(
        matches!(
            &cmd,
            SchedulerCommand::CompleteDagRun {
                status: conduit_scheduler::RunStatus::Failed,
                ..
            }
        ),
        "expected CompleteDagRun Failed after exhausted retries, got: {:?}",
        cmd
    );

    tx.send(SchedulerEvent::Shutdown).unwrap();
    let _ = driver.await;
}

/// With a retry_backoff multiplier, each successive retry's announced delay
/// must grow exponentially (1s, then 2s for backoff=2.0).
#[tokio::test]
async fn retry_backoff_multiplier_grows_the_delay() {
    let mut task_a = make_task("A", vec![], TriggerRule::AllSuccess, 2, Some("1s"));
    task_a.retry_backoff = Some(2.0);
    let dag = make_dag("dag1", vec![task_a], vec!["A"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, scheduler_fut) = create_test_scheduler(plans);
    let driver = tokio::spawn(scheduler_fut);

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();

    let _ = next_command(&mut rx, 5, "initial DispatchTask").await;

    let fail = |attempt: u32| SchedulerEvent::TaskFailed {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: "A".to_string(),
        error: "boom".to_string(),
        attempt,
    };

    // Attempt 0 fails → first retry delay = base = 1s.
    tx.send(fail(0)).unwrap();
    let cmd = next_command(&mut rx, 5, "first RetryTask").await;
    match &cmd {
        SchedulerCommand::RetryTask { delay, .. } => {
            assert_eq!(
                delay.num_seconds(),
                1,
                "first retry delay must be 1s (base)"
            )
        }
        other => panic!("expected RetryTask, got {:?}", other),
    }
    let _ = next_command(&mut rx, 5, "re-dispatch attempt 1").await;

    // Attempt 1 fails → second retry delay = base * 2.0 = 2s.
    tx.send(fail(1)).unwrap();
    let cmd = next_command(&mut rx, 5, "second RetryTask").await;
    match &cmd {
        SchedulerCommand::RetryTask { delay, .. } => {
            assert_eq!(
                delay.num_seconds(),
                2,
                "second retry delay must double with backoff=2.0"
            )
        }
        other => panic!("expected RetryTask, got {:?}", other),
    }

    tx.send(SchedulerEvent::Shutdown).unwrap();
    let _ = driver.await;
}

// ---------------------------------------------------------------------------
// Event persistence (event sourcing must actually record run history)
// ---------------------------------------------------------------------------

/// With an event store attached, a completed run must leave a persistent
/// TaskCompleted + DagRunCompleted trail that `conduit replay` can fold.
#[tokio::test]
async fn scheduler_persists_run_lifecycle_events() {
    use std::sync::Arc;

    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(conduit_state::EventStore::open(tmp.path()).unwrap());

    let task_a = make_task("A", vec![], TriggerRule::AllSuccess, 0, None);
    let dag = make_dag("dag1", vec![task_a], vec!["A"]);
    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (command_tx, mut command_rx) = mpsc::unbounded_channel();
    let scheduler = Scheduler::new(event_rx, command_tx, PoolManager::new(vec![]), plans)
        .unwrap()
        .with_event_store(Arc::clone(&store));
    let driver = tokio::spawn(async move {
        let _ = scheduler.run().await;
    });

    event_tx
        .send(SchedulerEvent::DagRunRequested {
            dag_id: "dag1".to_string(),
            run_id: "run1".to_string(),
            logical_date: Utc::now(),
            config: HashMap::new(),
        })
        .unwrap();

    let _ = next_command(&mut command_rx, 5, "DispatchTask A").await;
    event_tx
        .send(SchedulerEvent::TaskCompleted {
            dag_id: "dag1".to_string(),
            run_id: "run1".to_string(),
            task_id: "A".to_string(),
            snapshot_id: None,
            duration_ms: 7,
        })
        .unwrap();

    let _ = next_command(&mut command_rx, 5, "CompleteDagRun").await;
    event_tx.send(SchedulerEvent::Shutdown).unwrap();
    let _ = driver.await;

    let events = store.range(0, store.current_sequence()).unwrap();
    use conduit_common::event::EventKind;

    assert!(
        events.iter().any(|e| matches!(
            &e.kind,
            EventKind::DagRunCreated { dag_id, run_id, .. }
                if dag_id == "dag1" && run_id == "run1"
        )),
        "expected a persisted DagRunCreated event, got: {:?}",
        events.iter().map(|e| &e.kind).collect::<Vec<_>>()
    );
    assert!(
        events.iter().any(|e| matches!(
            &e.kind,
            EventKind::TaskCompleted { task_id, .. } if task_id == "A"
        )),
        "expected a persisted TaskCompleted event"
    );
    assert!(
        events.iter().any(|e| matches!(
            &e.kind,
            EventKind::DagRunCompleted {
                status: conduit_common::event::RunStatus::Success,
                ..
            }
        )),
        "expected a persisted DagRunCompleted Success event"
    );
}

// ---------------------------------------------------------------------------
// Cron scheduling
// ---------------------------------------------------------------------------

/// Two cron ticks inside the same minute must create exactly one run for a
/// `* * * * *` DAG; a tick in the next minute creates the second run.
#[tokio::test]
async fn cron_tick_fires_due_dag_once_per_minute() {
    let task_a = make_task("A", vec![], TriggerRule::AllSuccess, 0, None);
    let mut dag = make_dag("cron_dag", vec![task_a], vec!["A"]);
    dag.schedule = Some("* * * * *".to_string());

    let mut plans = HashMap::new();
    plans.insert("cron_dag".to_string(), dag);

    let (tx, mut rx, scheduler_fut) = create_test_scheduler(plans);
    let driver = tokio::spawn(scheduler_fut);

    let t0 = Utc.with_ymd_and_hms(2026, 7, 13, 10, 30, 5).unwrap();
    let t0_later = Utc.with_ymd_and_hms(2026, 7, 13, 10, 30, 45).unwrap(); // same minute
    let t1 = Utc.with_ymd_and_hms(2026, 7, 13, 10, 31, 5).unwrap(); // next minute

    tx.send(SchedulerEvent::CronTick { timestamp: t0 }).unwrap();
    tx.send(SchedulerEvent::CronTick {
        timestamp: t0_later,
    })
    .unwrap();
    tx.send(SchedulerEvent::CronTick { timestamp: t1 }).unwrap();
    tx.send(SchedulerEvent::Shutdown).unwrap();
    let _ = driver.await;

    let cmds = drain_commands(&mut rx);
    let dispatches = cmds
        .iter()
        .filter(
            |c| matches!(c, SchedulerCommand::DispatchTask { dag_id, .. } if dag_id == "cron_dag"),
        )
        .count();
    assert_eq!(
        dispatches, 2,
        "expected exactly 2 dispatches (one per due minute, deduped within a minute), got {} in {:?}",
        dispatches, cmds
    );
}

// ---------------------------------------------------------------------------
// Resource pool enforcement
// ---------------------------------------------------------------------------

fn create_pool_scheduler(
    plans: HashMap<String, Dag>,
    pools: Vec<Pool>,
) -> (
    mpsc::UnboundedSender<SchedulerEvent>,
    mpsc::UnboundedReceiver<SchedulerCommand>,
    tokio::task::JoinHandle<()>,
) {
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let scheduler = Scheduler::new(event_rx, command_tx, PoolManager::new(pools), plans).unwrap();
    let handle = tokio::spawn(async move {
        let _ = scheduler.run().await;
    });
    (event_tx, command_rx, handle)
}

/// Two independent tasks sharing a 1-slot pool must execute serially:
/// only one dispatches up front; the second dispatches after the first
/// completes and releases the slot.
#[tokio::test]
async fn one_slot_pool_serializes_independent_tasks() {
    let mut task_a = make_task("A", vec![], TriggerRule::AllSuccess, 0, None);
    let mut task_b = make_task("B", vec![], TriggerRule::AllSuccess, 0, None);
    task_a.pool = Some("solo".to_string());
    task_b.pool = Some("solo".to_string());
    let dag = make_dag("dag1", vec![task_a, task_b], vec!["A", "B"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, driver) = create_pool_scheduler(
        plans,
        vec![Pool {
            name: "solo".to_string(),
            slots: 1,
            description: None,
        }],
    );

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();

    // Exactly one task may dispatch while the pool has one slot.
    let first = next_command(&mut rx, 5, "first pooled dispatch").await;
    let first_task = match &first {
        SchedulerCommand::DispatchTask { task_id, .. } => task_id.clone(),
        other => panic!("expected DispatchTask, got {:?}", other),
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        rx.try_recv().is_err(),
        "second task must NOT dispatch while the 1-slot pool is occupied"
    );

    // Completing the first frees the slot; the second must now dispatch.
    tx.send(SchedulerEvent::TaskCompleted {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        task_id: first_task.clone(),
        snapshot_id: None,
        duration_ms: 1,
    })
    .unwrap();

    let second = next_command(&mut rx, 5, "second pooled dispatch after release").await;
    match &second {
        SchedulerCommand::DispatchTask { task_id, .. } => {
            assert_ne!(task_id, &first_task, "the other task must dispatch next")
        }
        other => panic!("expected DispatchTask, got {:?}", other),
    }

    tx.send(SchedulerEvent::Shutdown).unwrap();
    let _ = driver.await;
}

/// Pool slots are shared across runs: a waiter in run2 must wake up when
/// run1's task releases the slot.
#[tokio::test]
async fn pool_release_wakes_waiters_in_other_runs() {
    let mut task_a = make_task("A", vec![], TriggerRule::AllSuccess, 0, None);
    task_a.pool = Some("solo".to_string());
    let dag = make_dag("dag1", vec![task_a], vec!["A"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, driver) = create_pool_scheduler(
        plans,
        vec![Pool {
            name: "solo".to_string(),
            slots: 1,
            description: None,
        }],
    );

    for run in ["run1", "run2"] {
        tx.send(SchedulerEvent::DagRunRequested {
            dag_id: "dag1".to_string(),
            run_id: run.to_string(),
            logical_date: Utc::now(),
            config: HashMap::new(),
        })
        .unwrap();
    }

    let first = next_command(&mut rx, 5, "first cross-run dispatch").await;
    let first_run = match &first {
        SchedulerCommand::DispatchTask { run_id, .. } => run_id.clone(),
        other => panic!("expected DispatchTask, got {:?}", other),
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        rx.try_recv().is_err(),
        "run2's task must wait for the pool slot"
    );

    tx.send(SchedulerEvent::TaskCompleted {
        dag_id: "dag1".to_string(),
        run_id: first_run.clone(),
        task_id: "A".to_string(),
        snapshot_id: None,
        duration_ms: 1,
    })
    .unwrap();

    // run1 completes (CompleteDagRun) and run2's task dispatches.
    let mut saw_second_dispatch = false;
    for _ in 0..3 {
        let cmd = next_command(&mut rx, 5, "commands after release").await;
        if let SchedulerCommand::DispatchTask { run_id, .. } = &cmd {
            assert_ne!(run_id, &first_run);
            saw_second_dispatch = true;
            break;
        }
    }
    assert!(
        saw_second_dispatch,
        "run2's task must dispatch after release"
    );

    tx.send(SchedulerEvent::Shutdown).unwrap();
    let _ = driver.await;
}

/// A task referencing an undefined pool must still run (unlimited, warn)
/// rather than deadlock.
#[tokio::test]
async fn undefined_pool_does_not_block_dispatch() {
    let mut task_a = make_task("A", vec![], TriggerRule::AllSuccess, 0, None);
    task_a.pool = Some("never_defined".to_string());
    let dag = make_dag("dag1", vec![task_a], vec!["A"]);

    let mut plans = HashMap::new();
    plans.insert("dag1".to_string(), dag);

    let (tx, mut rx, driver) = create_pool_scheduler(plans, vec![]);

    tx.send(SchedulerEvent::DagRunRequested {
        dag_id: "dag1".to_string(),
        run_id: "run1".to_string(),
        logical_date: Utc::now(),
        config: HashMap::new(),
    })
    .unwrap();

    let cmd = next_command(&mut rx, 5, "dispatch with undefined pool").await;
    assert!(
        matches!(&cmd, SchedulerCommand::DispatchTask { task_id, .. } if task_id == "A"),
        "task with undefined pool must dispatch, got {:?}",
        cmd
    );

    tx.send(SchedulerEvent::Shutdown).unwrap();
    let _ = driver.await;
}
