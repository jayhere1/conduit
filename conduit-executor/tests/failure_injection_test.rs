//! Failure injection tests for the executor.
//!
//! Verifies that the executor handles edge cases correctly:
//! - Multiple concurrent tasks with timeouts
//! - Tasks that produce partial output before failing
//! - Rapid task submission under load
//! - Signal handling (SIGKILL simulation via immediate exit)

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc;

use conduit_common::dag::{ResourceLimits, Task, TaskType, TriggerRule};
use conduit_executor::{ExecutorCommand, ExecutorEvent, TaskExecutor, TaskOutcome};

fn make_task(id: &str, command: &str, timeout: Option<&str>) -> Task {
    Task {
        id: id.to_string(),
        task_type: TaskType::Bash {
            command: command.to_string(),
        },
        dependencies: vec![],
        retries: 0,
        retry_delay: None,
        timeout: timeout.map(String::from),
        trigger_rule: TriggerRule::AllSuccess,
        pool: None,
        priority: 0,
        resources: ResourceLimits::default(),
        incremental: None,
        contracts: None,
    }
}

fn dispatch(task: Task) -> ExecutorCommand {
    ExecutorCommand::DispatchTask {
        task,
        dag_id: "test".to_string(),
        run_id: uuid::Uuid::new_v4().to_string(),
        attempt: 0,
        logical_date: Utc::now(),
        environment: "test".to_string(),
        params: HashMap::new(),
    }
}

/// Multiple tasks timing out concurrently should all report correctly.
#[tokio::test]
async fn concurrent_timeouts() {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    let mut executor = TaskExecutor::new(cmd_rx, evt_tx, 4);

    // Dispatch 4 tasks that all sleep longer than their timeout
    for i in 0..4 {
        let task = make_task(&format!("timeout_{}", i), "sleep 30", Some("2s"));
        cmd_tx.send(dispatch(task)).unwrap();
    }

    tokio::spawn(async move { executor.run().await.unwrap() });

    let mut timed_out = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);

    while timed_out < 4 && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(10), evt_rx.recv()).await {
            Ok(Some(ExecutorEvent::TaskFailed { error, .. })) => {
                assert!(
                    error.contains("timed out"),
                    "Expected timeout error, got: {}",
                    error
                );
                timed_out += 1;
            }
            Ok(Some(ExecutorEvent::TaskCompleted { outcome, .. })) => {
                // Some platforms report timeout as Failed outcome
                assert_eq!(outcome, TaskOutcome::Failed);
                timed_out += 1;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    assert_eq!(timed_out, 4, "All 4 tasks should have timed out");
    cmd_tx.send(ExecutorCommand::Shutdown).unwrap();
}

/// Task that produces output before failing should still be marked as failed.
#[tokio::test]
async fn partial_output_then_failure() {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    let mut executor = TaskExecutor::new(cmd_rx, evt_tx, 4);

    // Task prints valid XCOM then exits non-zero
    let task = make_task(
        "partial",
        "echo 'CONDUIT::XCOM::{\"key\": \"value\"}' && exit 1",
        None,
    );
    cmd_tx.send(dispatch(task)).unwrap();
    cmd_tx.send(ExecutorCommand::Shutdown).unwrap();

    tokio::spawn(async move { executor.run().await.unwrap() });

    let event = tokio::time::timeout(Duration::from_secs(10), evt_rx.recv())
        .await
        .unwrap()
        .unwrap();

    match event {
        ExecutorEvent::TaskCompleted { outcome, .. } => {
            assert_eq!(outcome, TaskOutcome::Failed);
        }
        other => panic!(
            "Expected TaskCompleted with Failed outcome, got {:?}",
            other
        ),
    }
}

/// Rapid submission of many short tasks should all complete.
#[tokio::test]
async fn rapid_submission_under_load() {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    let mut executor = TaskExecutor::new(cmd_rx, evt_tx, 2); // Only 2 slots

    let n = 10;

    // Submit tasks from a separate task with small delays
    let tx = cmd_tx.clone();
    tokio::spawn(async move {
        for i in 0..n {
            let task = make_task(&format!("rapid_{}", i), "echo ok", None);
            tx.send(dispatch(task)).unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    tokio::spawn(async move { executor.run().await.unwrap() });

    let mut completed = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while completed < n && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(15), evt_rx.recv()).await {
            Ok(Some(ExecutorEvent::TaskCompleted { outcome, .. })) => {
                assert_eq!(outcome, TaskOutcome::Success);
                completed += 1;
            }
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break,
        }
    }

    assert_eq!(completed, n, "All {} tasks should complete", n);
    cmd_tx.send(ExecutorCommand::Shutdown).unwrap();
}

/// Task that exits immediately (SIGKILL simulation) should be handled.
#[tokio::test]
async fn immediate_exit_task() {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    let mut executor = TaskExecutor::new(cmd_rx, evt_tx, 4);

    // Kill -9 $$: on macOS, this may not work in shell; use exit 137 instead
    let task = make_task("killed", "exit 137", None);
    cmd_tx.send(dispatch(task)).unwrap();
    cmd_tx.send(ExecutorCommand::Shutdown).unwrap();

    tokio::spawn(async move { executor.run().await.unwrap() });

    let event = tokio::time::timeout(Duration::from_secs(10), evt_rx.recv())
        .await
        .unwrap()
        .unwrap();

    match event {
        ExecutorEvent::TaskCompleted { outcome, .. } => {
            // exit 137 = Failed (non-zero, non-special exit code)
            assert_eq!(outcome, TaskOutcome::Failed);
        }
        other => panic!("Expected TaskCompleted, got {:?}", other),
    }
}
