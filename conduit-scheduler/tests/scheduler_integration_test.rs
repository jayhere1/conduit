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

use std::sync::Arc as StdArc;
use std::sync::Mutex;
use conduit_scheduler::{AlertEvent, AlertHook, AlertStatus};

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
    assert_eq!(calls.len(), 1, "hook should fire exactly once for a failed run");
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
