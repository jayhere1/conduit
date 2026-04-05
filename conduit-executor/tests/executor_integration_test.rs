use conduit_executor::{TaskExecutor, ExecutorCommand, ExecutorEvent, TaskOutcome};
use conduit_common::dag::{Task, TaskType, ResourceLimits, TriggerRule};
use tokio::sync::mpsc;
use std::collections::HashMap;
use chrono::Utc;

fn make_bash_task(id: &str, command: &str) -> Task {
    Task {
        id: id.to_string(),
        task_type: TaskType::Bash { command: command.to_string() },
        dependencies: vec![],
        retries: 0,
        retry_delay: None,
        timeout: None,
        trigger_rule: TriggerRule::AllSuccess,
        pool: None,
        priority: 0,
        resources: ResourceLimits::default(),
        incremental: None,
        contracts: None,
    }
}

fn dispatch_cmd(task: Task) -> ExecutorCommand {
    ExecutorCommand::DispatchTask {
        task,
        dag_id: "test_dag".to_string(),
        run_id: uuid::Uuid::new_v4().to_string(),
        attempt: 0,
        logical_date: Utc::now(),
        environment: "test".to_string(),
        params: HashMap::new(),
    }
}

#[tokio::test]
async fn test_bash_task_success() {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    let mut executor = TaskExecutor::new(cmd_rx, evt_tx, 4);

    let task = make_bash_task("t1", "echo hello && exit 0");
    cmd_tx.send(dispatch_cmd(task)).unwrap();
    cmd_tx.send(ExecutorCommand::Shutdown).unwrap();

    tokio::spawn(async move { executor.run().await.unwrap() });

    let event = tokio::time::timeout(std::time::Duration::from_secs(10), evt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    match event {
        ExecutorEvent::TaskCompleted { outcome, .. } => assert_eq!(outcome, TaskOutcome::Success),
        other => panic!("Expected TaskCompleted, got {:?}", other),
    }
}

#[tokio::test]
async fn test_bash_task_failure() {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    let mut executor = TaskExecutor::new(cmd_rx, evt_tx, 4);

    let task = make_bash_task("t1", "exit 1");
    cmd_tx.send(dispatch_cmd(task)).unwrap();
    cmd_tx.send(ExecutorCommand::Shutdown).unwrap();

    tokio::spawn(async move { executor.run().await.unwrap() });

    let event = tokio::time::timeout(std::time::Duration::from_secs(10), evt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    match event {
        ExecutorEvent::TaskCompleted { outcome, .. } => assert_eq!(outcome, TaskOutcome::Failed),
        other => panic!("Expected TaskCompleted, got {:?}", other),
    }
}

#[tokio::test]
async fn test_bash_task_retry_exit_code() {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    let mut executor = TaskExecutor::new(cmd_rx, evt_tx, 4);

    let task = make_bash_task("t1", "exit 2");
    cmd_tx.send(dispatch_cmd(task)).unwrap();
    cmd_tx.send(ExecutorCommand::Shutdown).unwrap();

    tokio::spawn(async move { executor.run().await.unwrap() });

    let event = tokio::time::timeout(std::time::Duration::from_secs(10), evt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    match event {
        ExecutorEvent::TaskCompleted { outcome, .. } => assert_eq!(outcome, TaskOutcome::Retry),
        other => panic!("Expected TaskCompleted, got {:?}", other),
    }
}

#[tokio::test]
async fn test_bash_task_skip_exit_code() {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    let mut executor = TaskExecutor::new(cmd_rx, evt_tx, 4);

    let task = make_bash_task("t1", "exit 3");
    cmd_tx.send(dispatch_cmd(task)).unwrap();
    cmd_tx.send(ExecutorCommand::Shutdown).unwrap();

    tokio::spawn(async move { executor.run().await.unwrap() });

    let event = tokio::time::timeout(std::time::Duration::from_secs(10), evt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    match event {
        ExecutorEvent::TaskCompleted { outcome, .. } => assert_eq!(outcome, TaskOutcome::Skipped),
        other => panic!("Expected TaskCompleted, got {:?}", other),
    }
}

#[tokio::test]
async fn test_python_task_success() {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    let mut executor = TaskExecutor::new(cmd_rx, evt_tx, 4);

    let task = make_bash_task("t1", "python3 -c 'print(42)'");
    cmd_tx.send(dispatch_cmd(task)).unwrap();
    cmd_tx.send(ExecutorCommand::Shutdown).unwrap();

    tokio::spawn(async move { executor.run().await.unwrap() });

    let event = tokio::time::timeout(std::time::Duration::from_secs(10), evt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    match event {
        ExecutorEvent::TaskCompleted { outcome, .. } => assert_eq!(outcome, TaskOutcome::Success),
        other => panic!("Expected TaskCompleted, got {:?}", other),
    }
}

#[tokio::test]
async fn test_timeout_enforcement() {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    let mut executor = TaskExecutor::new(cmd_rx, evt_tx, 4);

    let mut task = make_bash_task("t1", "sleep 30");
    task.timeout = Some("2s".to_string());
    cmd_tx.send(dispatch_cmd(task)).unwrap();
    cmd_tx.send(ExecutorCommand::Shutdown).unwrap();

    tokio::spawn(async move { executor.run().await.unwrap() });

    let event = tokio::time::timeout(std::time::Duration::from_secs(10), evt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    match event {
        ExecutorEvent::TaskFailed { error, .. } => {
            assert!(
                error.contains("timed out"),
                "Expected timeout error, got: {}",
                error
            );
        }
        ExecutorEvent::TaskCompleted { outcome, .. } => {
            assert_eq!(outcome, TaskOutcome::Failed);
        }
    }
}

#[tokio::test]
async fn test_xcom_protocol_capture() {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    let mut executor = TaskExecutor::new(cmd_rx, evt_tx, 4);

    let task = make_bash_task("t1", r#"echo 'CONDUIT::XCOM::{"rows": 100}'"#);
    cmd_tx.send(dispatch_cmd(task)).unwrap();
    cmd_tx.send(ExecutorCommand::Shutdown).unwrap();

    tokio::spawn(async move { executor.run().await.unwrap() });

    let event = tokio::time::timeout(std::time::Duration::from_secs(10), evt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    match event {
        ExecutorEvent::TaskCompleted { outcome, xcom, .. } => {
            assert_eq!(outcome, TaskOutcome::Success);
            let xcom = xcom.expect("Expected xcom to be Some");
            assert_eq!(xcom.as_i64(), Some(100));
        }
        other => panic!("Expected TaskCompleted, got {:?}", other),
    }
}

#[tokio::test]
async fn test_concurrent_task_limit() {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel();
    let mut executor = TaskExecutor::new(cmd_rx, evt_tx, 2);

    // Send tasks from a separate async task with yields in between so the
    // executor has a chance to spawn and complete tasks between dispatches.
    // This allows the deferred queue to drain as slots free up.
    let cmd_tx_clone = cmd_tx.clone();
    tokio::spawn(async move {
        for i in 0..4 {
            let task = make_bash_task(&format!("t{}", i), "echo ok");
            cmd_tx_clone.send(dispatch_cmd(task)).unwrap();
            // Yield to let the executor process and complete tasks
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    });

    tokio::spawn(async move { executor.run().await.unwrap() });

    let mut completed = 0;
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_secs(30), evt_rx.recv()).await
    {
        match event {
            ExecutorEvent::TaskCompleted { outcome, .. } => {
                assert_eq!(outcome, TaskOutcome::Success);
                completed += 1;
            }
            other => panic!("Expected TaskCompleted, got {:?}", other),
        }
        if completed == 4 {
            break;
        }
    }

    // Send shutdown after all tasks have completed
    cmd_tx.send(ExecutorCommand::Shutdown).unwrap();

    assert_eq!(completed, 4, "Expected all 4 tasks to complete");
}
