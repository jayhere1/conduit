//! Python SDK -> Rust pipeline end-to-end integration test.
//!
//! Proves the full path:
//!   Python DAG file  ->  tree-sitter compilation  ->  scheduler dispatch
//!   ->  executor runs tasks  ->  events recorded in event store
//!
//! The test writes a Python file using Conduit SDK decorators, compiles it
//! via the tree-sitter parser (no Python execution), overrides task types
//! to bash for testability, then runs the full pipeline in-process.

use std::collections::HashMap;
use std::fs;
use std::time::Duration;

use chrono::Utc;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::time::timeout;

use conduit_common::dag::{Dag, TaskType};
use conduit_common::event::{EventKind, RunStatus};
use conduit_compiler::ConduitPlan;
use conduit_executor::{ExecutorCommand, ExecutorEvent, TaskExecutor, TaskOutcome};
use conduit_scheduler::{
    PoolManager, RunStatus as SchedulerRunStatus, Scheduler, SchedulerCommand, SchedulerEvent,
};
use conduit_state::EventStore;

// ─── Mediator ───────────────────────────────────────────────────────────────
// Bridges Scheduler commands -> Executor commands, and Executor events ->
// Scheduler events, simulating the role the API layer normally plays.

#[allow(clippy::too_many_arguments)]
async fn run_mediator(
    dag: &Dag,
    mut command_rx: mpsc::UnboundedReceiver<SchedulerCommand>,
    event_tx: mpsc::UnboundedSender<SchedulerEvent>,
    executor_command_tx: mpsc::UnboundedSender<ExecutorCommand>,
    mut executor_event_rx: mpsc::UnboundedReceiver<ExecutorEvent>,
    event_store: &EventStore,
    dag_id: &str,
    run_id: &str,
) {
    loop {
        tokio::select! {
            cmd = command_rx.recv() => {
                match cmd {
                    Some(SchedulerCommand::DispatchTask { dag_id: d, run_id: r, task_id, attempt }) => {
                        let _ = event_store.append(EventKind::TaskStarted {
                            dag_id: d.clone(),
                            run_id: r.clone(),
                            task_id: task_id.clone(),
                            worker_id: "local".to_string(),
                            attempt,
                            pid: None,
                        });

                        if let Some(task) = dag.tasks.get(&task_id) {
                            let _ = executor_command_tx.send(ExecutorCommand::DispatchTask {
                                task: task.clone(),
                                dag_id: d,
                                run_id: r,
                                attempt,
                                logical_date: Utc::now(),
                                environment: "test".to_string(),
                                params: HashMap::new(),
                            });
                        }
                    }
                    Some(SchedulerCommand::CompleteDagRun { dag_id: d, run_id: r, status }) => {
                        let event_status = match status {
                            SchedulerRunStatus::Success => RunStatus::Success,
                            SchedulerRunStatus::Failed => RunStatus::Failed,
                            SchedulerRunStatus::Cancelled => RunStatus::Cancelled,
                        };
                        let _ = event_store.append(EventKind::DagRunCompleted {
                            dag_id: d,
                            run_id: r,
                            status: event_status,
                            duration_ms: 0,
                        });
                        return;
                    }
                    Some(SchedulerCommand::SkipTask { dag_id: d, run_id: r, task_id, reason }) => {
                        let _ = event_store.append(EventKind::TaskSkipped {
                            dag_id: d, run_id: r, task_id, reason,
                        });
                    }
                    Some(SchedulerCommand::RetryTask { dag_id: d, run_id: r, task_id, delay }) => {
                        let _ = event_store.append(EventKind::TaskRetrying {
                            dag_id: d, run_id: r, task_id, attempt: 1,
                            next_retry_at: Utc::now() + chrono::Duration::milliseconds(delay.num_milliseconds()),
                        });
                    }
                    None => return,
                }
            }
            evt = executor_event_rx.recv() => {
                match evt {
                    Some(ExecutorEvent::TaskCompleted { task_id, run_id: _, attempt, outcome, xcom: _, duration }) => {
                        match outcome {
                            TaskOutcome::Failed => {
                                let _ = event_store.append(EventKind::TaskFailed {
                                    dag_id: dag_id.to_string(),
                                    run_id: run_id.to_string(),
                                    task_id: task_id.clone(),
                                    error: "Task exited with non-zero status".to_string(),
                                    traceback: None,
                                    attempt,
                                });
                                let _ = event_tx.send(SchedulerEvent::TaskFailed {
                                    dag_id: dag_id.to_string(),
                                    run_id: run_id.to_string(),
                                    task_id,
                                    error: "Task exited with non-zero status".to_string(),
                                    attempt,
                                });
                            }
                            _ => {
                                let _ = event_store.append(EventKind::TaskCompleted {
                                    dag_id: dag_id.to_string(),
                                    run_id: run_id.to_string(),
                                    task_id: task_id.clone(),
                                    duration_ms: duration.as_millis() as u64,
                                    snapshot_id: None,
                                });
                                let _ = event_tx.send(SchedulerEvent::TaskCompleted {
                                    dag_id: dag_id.to_string(),
                                    run_id: run_id.to_string(),
                                    task_id,
                                    snapshot_id: None,
                                    duration_ms: duration.as_millis() as u64,
                                });
                            }
                        }
                    }
                    Some(ExecutorEvent::TaskFailed { task_id, run_id: _, attempt, error }) => {
                        let _ = event_store.append(EventKind::TaskFailed {
                            dag_id: dag_id.to_string(),
                            run_id: run_id.to_string(),
                            task_id: task_id.clone(),
                            error: error.clone(),
                            traceback: None,
                            attempt,
                        });
                        let _ = event_tx.send(SchedulerEvent::TaskFailed {
                            dag_id: dag_id.to_string(),
                            run_id: run_id.to_string(),
                            task_id,
                            error,
                            attempt,
                        });
                    }
                    None => return,
                }
            }
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

/// Full E2E: compile a Python SDK DAG, run it through the pipeline, verify events.
#[tokio::test]
async fn e2e_python_sdk_dag_compiles_and_runs() {
    // 1. Set up temp dir with Python DAG
    let tmp = TempDir::new().unwrap();
    let dags_dir = tmp.path().join("dags");
    fs::create_dir_all(&dags_dir).unwrap();

    let dag_source = include_str!("fixtures/sdk_e2e_dag.py");
    fs::write(dags_dir.join("sdk_e2e_dag.py"), dag_source).unwrap();

    // 2. Compile via tree-sitter (no Python execution)
    let (plan, stats) = ConduitPlan::compile(&dags_dir).unwrap();
    assert!(
        stats.errors.is_empty(),
        "Compilation errors: {:?}",
        stats.errors
    );
    assert!(
        plan.dags.contains_key("sdk_e2e_pipeline"),
        "DAG 'sdk_e2e_pipeline' not found. Available: {:?}",
        plan.dags.keys().collect::<Vec<_>>()
    );

    let mut dag = plan.dags.get("sdk_e2e_pipeline").unwrap().clone();

    // 3. Verify compiled structure
    assert_eq!(
        dag.tasks.len(),
        3,
        "Expected 3 tasks, got {:?}",
        dag.tasks.keys().collect::<Vec<_>>()
    );
    assert!(dag.tasks.contains_key("extract"), "Missing 'extract' task");
    assert!(
        dag.tasks.contains_key("transform"),
        "Missing 'transform' task"
    );
    assert!(dag.tasks.contains_key("load"), "Missing 'load' task");

    // Verify dependencies were inferred
    let transform_deps: Vec<&str> = dag.tasks["transform"]
        .dependencies
        .iter()
        .map(|d| d.task_id.as_str())
        .collect();
    assert!(
        transform_deps.contains(&"extract"),
        "transform should depend on extract, got: {:?}",
        transform_deps
    );

    let load_deps: Vec<&str> = dag.tasks["load"]
        .dependencies
        .iter()
        .map(|d| d.task_id.as_str())
        .collect();
    assert!(
        load_deps.contains(&"transform"),
        "load should depend on transform, got: {:?}",
        load_deps
    );

    // 4. Override to bash tasks (avoid needing Python runtime)
    for task in dag.tasks.values_mut() {
        task.task_type = TaskType::Bash {
            command: format!("echo 'running {}'", task.id),
        };
    }

    let dag_id = dag.id.clone();
    let run_id = "sdk-e2e-run-001".to_string();

    // 5. Wire scheduler + executor + event store
    let event_store_dir = TempDir::new().unwrap();
    let event_store = EventStore::open(event_store_dir.path()).unwrap();

    let (scheduler_event_tx, scheduler_event_rx) = mpsc::unbounded_channel();
    let (scheduler_command_tx, scheduler_command_rx) = mpsc::unbounded_channel();
    let (executor_command_tx, executor_command_rx) = mpsc::unbounded_channel();
    let (executor_event_tx, executor_event_rx) = mpsc::unbounded_channel();

    let mut plans = HashMap::new();
    plans.insert(dag_id.clone(), dag.clone());
    let scheduler = Scheduler::new(
        scheduler_event_rx,
        scheduler_command_tx,
        PoolManager::default(),
        plans,
    )
    .unwrap();

    let mut executor = TaskExecutor::new(executor_command_rx, executor_event_tx, 4);

    event_store
        .append(EventKind::DagRunCreated {
            dag_id: dag_id.clone(),
            run_id: run_id.clone(),
            logical_date: Utc::now(),
            environment: "test".to_string(),
            triggered_by: "python_sdk_e2e_test".to_string(),
        })
        .unwrap();

    // Launch scheduler + executor
    let scheduler_handle = tokio::spawn(async move { scheduler.run().await });
    let executor_handle = tokio::spawn(async move { executor.run().await });

    // Request a DAG run
    scheduler_event_tx
        .send(SchedulerEvent::DagRunRequested {
            dag_id: dag_id.clone(),
            run_id: run_id.clone(),
            logical_date: Utc::now(),
            config: HashMap::new(),
        })
        .unwrap();

    // Run mediator
    let mediator_result = timeout(
        Duration::from_secs(30),
        run_mediator(
            &dag,
            scheduler_command_rx,
            scheduler_event_tx.clone(),
            executor_command_tx.clone(),
            executor_event_rx,
            &event_store,
            &dag_id,
            &run_id,
        ),
    )
    .await;

    assert!(
        mediator_result.is_ok(),
        "SDK E2E pipeline timed out — DAG did not complete within 30 seconds"
    );

    // Shut down
    let _ = scheduler_event_tx.send(SchedulerEvent::Shutdown);
    let _ = executor_command_tx.send(ExecutorCommand::Shutdown);
    let _ = scheduler_handle.await;
    let _ = executor_handle.await;

    // 6. Verify events
    let events = event_store.range(1, 100).unwrap();

    // First event: DagRunCreated
    assert!(
        matches!(
            &events[0].kind,
            EventKind::DagRunCreated { dag_id, .. } if dag_id == "sdk_e2e_pipeline"
        ),
        "First event should be DagRunCreated"
    );

    // All 3 tasks should have completed
    let task_completed_events: Vec<String> = events
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::TaskCompleted { task_id, .. } => Some(task_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        task_completed_events.len(),
        3,
        "All 3 tasks should complete, got: {:?}",
        task_completed_events
    );

    // Verify topological ordering
    let extract_pos = task_completed_events
        .iter()
        .position(|t| t == "extract")
        .expect("extract not in completions");
    let transform_pos = task_completed_events
        .iter()
        .position(|t| t == "transform")
        .expect("transform not in completions");
    let load_pos = task_completed_events
        .iter()
        .position(|t| t == "load")
        .expect("load not in completions");

    assert!(
        extract_pos < transform_pos,
        "extract should complete before transform"
    );
    assert!(
        transform_pos < load_pos,
        "transform should complete before load"
    );

    // DAG should have succeeded
    let last = events.last().unwrap();
    match &last.kind {
        EventKind::DagRunCompleted {
            status: RunStatus::Success,
            ..
        } => {}
        other => panic!("Expected DagRunCompleted Success, got {:?}", other),
    }
}

/// Verify compilation catches the structure even when tasks have parameters.
#[tokio::test]
async fn e2e_python_sdk_compilation_preserves_metadata() {
    let tmp = TempDir::new().unwrap();
    let dags_dir = tmp.path().join("dags");
    fs::create_dir_all(&dags_dir).unwrap();

    let dag_source = include_str!("fixtures/sdk_e2e_dag.py");
    fs::write(dags_dir.join("sdk_e2e_dag.py"), dag_source).unwrap();

    let (plan, stats) = ConduitPlan::compile(&dags_dir).unwrap();
    assert!(stats.errors.is_empty());

    let dag = plan.dags.get("sdk_e2e_pipeline").unwrap();

    // Tags should be preserved
    assert!(
        dag.tags.contains(&"sdk-e2e-test".to_string()),
        "Tags should contain 'sdk-e2e-test', got: {:?}",
        dag.tags
    );

    // Schedule should be preserved
    assert_eq!(
        dag.schedule.as_deref(),
        Some("0 6 * * *"),
        "Schedule should be '0 6 * * *'"
    );

    // Execution order should be topologically sorted
    assert_eq!(dag.execution_order.len(), 3);
    let extract_idx = dag
        .execution_order
        .iter()
        .position(|t| t == "extract")
        .unwrap();
    let transform_idx = dag
        .execution_order
        .iter()
        .position(|t| t == "transform")
        .unwrap();
    let load_idx = dag
        .execution_order
        .iter()
        .position(|t| t == "load")
        .unwrap();
    assert!(extract_idx < transform_idx);
    assert!(transform_idx < load_idx);
}
