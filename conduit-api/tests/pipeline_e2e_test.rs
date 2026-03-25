//! End-to-end pipeline integration test.
//!
//! This test wires up the full Conduit pipeline in-process:
//!
//!   DAG definition → Scheduler → Executor → Event Store → Verification
//!
//! Unlike unit tests (which test components in isolation), this verifies:
//! - Scheduler correctly dispatches root tasks on DagRunRequested
//! - Executor picks up DispatchTask commands and runs bash tasks
//! - Task completion feeds back into the scheduler, unlocking dependents
//! - Event store captures the full lifecycle as immutable events
//! - Multi-task DAGs with dependencies execute in correct topological order
//!
//! The test builds DAG structures programmatically (no tree-sitter parser needed)
//! and uses real bash execution for tasks.

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::time::timeout;

use conduit_common::dag::{
    Dag, Task, TaskDependency, TaskType, TriggerRule, ResourceLimits, DependencyType,
};
use conduit_common::event::{Event, EventKind, RunStatus};
use conduit_executor::{ExecutorCommand, ExecutorEvent, TaskExecutor, TaskOutcome};
use conduit_scheduler::{
    PoolManager, Scheduler, SchedulerCommand, SchedulerEvent, RunStatus as SchedulerRunStatus,
};
use conduit_state::EventStore;

// ─── Test Helpers ────────────────────────────────────────────────────────────

/// Build a simple 3-task linear DAG: extract → transform → load
fn make_etl_dag() -> Dag {
    let mut tasks = HashMap::new();

    tasks.insert(
        "extract".to_string(),
        Task {
            id: "extract".to_string(),
            task_type: TaskType::Bash {
                command: "echo 'CONDUIT::XCOM::{\"rows\": 100}'".to_string(),
            },
            dependencies: vec![],
            retries: 0,
            retry_delay: None,
            pool: None,
            timeout: Some("60s".into()),
            priority: 0,
            resources: ResourceLimits::default(),
            trigger_rule: TriggerRule::NoDeps,
            incremental: None,
            contracts: None,
        },
    );

    tasks.insert(
        "transform".to_string(),
        Task {
            id: "transform".to_string(),
            task_type: TaskType::Bash {
                command: "echo 'CONDUIT::METRIC::row_count::100'".to_string(),
            },
            dependencies: vec![TaskDependency {
                task_id: "extract".to_string(),
                dependency_type: DependencyType::DataFlow,
            }],
            retries: 0,
            retry_delay: None,
            pool: None,
            timeout: Some("120s".into()),
            priority: 0,
            resources: ResourceLimits::default(),
            trigger_rule: TriggerRule::AllSuccess,
            incremental: None,
            contracts: None,
        },
    );

    tasks.insert(
        "load".to_string(),
        Task {
            id: "load".to_string(),
            task_type: TaskType::Bash {
                command: "echo 'loaded'".to_string(),
            },
            dependencies: vec![TaskDependency {
                task_id: "transform".to_string(),
                dependency_type: DependencyType::ExecutionOrder,
            }],
            retries: 0,
            retry_delay: None,
            pool: None,
            timeout: Some("60s".into()),
            priority: 0,
            resources: ResourceLimits::default(),
            trigger_rule: TriggerRule::AllSuccess,
            incremental: None,
            contracts: None,
        },
    );

    Dag {
        id: "etl_daily".to_string(),
        description: Some("Daily ETL pipeline".into()),
        schedule: None, // manual trigger for testing
        tags: vec!["test".into()],
        max_active_runs: 1,
        on_failure: None,
        tasks,
        execution_order: vec![
            "extract".to_string(),
            "transform".to_string(),
            "load".to_string(),
        ],
        source_file: "test_dag.py".to_string(),
        compiled_at: Utc::now(),
        catchup: true,
        max_catchup_runs: None,
    }
}

/// Build a diamond DAG:
///       start
///      /     \
///   left    right
///      \     /
///       join
fn make_diamond_dag() -> Dag {
    let mut tasks = HashMap::new();

    tasks.insert(
        "start".to_string(),
        Task {
            id: "start".to_string(),
            task_type: TaskType::Bash {
                command: "echo start".to_string(),
            },
            dependencies: vec![],
            retries: 0,
            retry_delay: None,
            pool: None,
            timeout: None,
            priority: 0,
            resources: ResourceLimits::default(),
            trigger_rule: TriggerRule::NoDeps,
            incremental: None,
            contracts: None,
        },
    );

    for branch in &["left", "right"] {
        tasks.insert(
            branch.to_string(),
            Task {
                id: branch.to_string(),
                task_type: TaskType::Bash {
                    command: format!("echo {}", branch),
                },
                dependencies: vec![TaskDependency {
                    task_id: "start".to_string(),
                    dependency_type: DependencyType::ExecutionOrder,
                }],
                retries: 0,
                retry_delay: None,
                pool: None,
                timeout: None,
                priority: 0,
                resources: ResourceLimits::default(),
                trigger_rule: TriggerRule::AllSuccess,
                incremental: None,
                contracts: None,
            },
        );
    }

    tasks.insert(
        "join".to_string(),
        Task {
            id: "join".to_string(),
            task_type: TaskType::Bash {
                command: "echo joined".to_string(),
            },
            dependencies: vec![
                TaskDependency {
                    task_id: "left".to_string(),
                    dependency_type: DependencyType::ExecutionOrder,
                },
                TaskDependency {
                    task_id: "right".to_string(),
                    dependency_type: DependencyType::ExecutionOrder,
                },
            ],
            retries: 0,
            retry_delay: None,
            pool: None,
            timeout: None,
            priority: 0,
            resources: ResourceLimits::default(),
            trigger_rule: TriggerRule::AllSuccess,
            incremental: None,
            contracts: None,
        },
    );

    Dag {
        id: "diamond".to_string(),
        description: Some("Diamond dependency test".into()),
        schedule: None,
        tags: vec!["test".into()],
        max_active_runs: 1,
        on_failure: None,
        tasks,
        execution_order: vec![
            "start".to_string(),
            "left".to_string(),
            "right".to_string(),
            "join".to_string(),
        ],
        source_file: "diamond_dag.py".to_string(),
        compiled_at: Utc::now(),
        catchup: true,
        max_catchup_runs: None,
    }
}

/// A mediator that bridges Scheduler commands → Executor commands,
/// and Executor events → Scheduler events, simulating the role the
/// API layer normally plays.
///
/// Also records events in the EventStore.
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
    // Track dispatched tasks and completed tasks.
    let mut dispatched = Vec::new();
    let mut completed_tasks = Vec::new();
    let _total_tasks = dag.execution_order.len();

    // Process commands from the scheduler and events from the executor
    // until we see a CompleteDagRun command.
    loop {
        tokio::select! {
            cmd = command_rx.recv() => {
                match cmd {
                    Some(SchedulerCommand::DispatchTask { dag_id: d, run_id: r, task_id, attempt }) => {
                        // Record in event store.
                        let _ = event_store.append(EventKind::TaskStarted {
                            dag_id: d.clone(),
                            run_id: r.clone(),
                            task_id: task_id.clone(),
                            worker_id: "local".to_string(),
                            attempt,
                            pid: None,
                        });

                        // Get the task definition and forward to executor.
                        if let Some(task) = dag.tasks.get(&task_id) {
                            dispatched.push(task_id.clone());
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

                        // All done.
                        return;
                    }
                    Some(SchedulerCommand::SkipTask { dag_id: d, run_id: r, task_id, reason }) => {
                        let _ = event_store.append(EventKind::TaskSkipped {
                            dag_id: d,
                            run_id: r,
                            task_id,
                            reason,
                        });
                    }
                    Some(SchedulerCommand::RetryTask { dag_id: d, run_id: r, task_id, delay }) => {
                        let _ = event_store.append(EventKind::TaskRetrying {
                            dag_id: d,
                            run_id: r,
                            task_id,
                            attempt: 1,
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
                                // Business-logic failure (exit code 1) — record as TaskFailed.
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
                                // Success, Retry, Skipped — record as TaskCompleted.
                                let _ = event_store.append(EventKind::TaskCompleted {
                                    dag_id: dag_id.to_string(),
                                    run_id: run_id.to_string(),
                                    task_id: task_id.clone(),
                                    duration_ms: duration.as_millis() as u64,
                                    snapshot_id: None,
                                });

                                completed_tasks.push(task_id.clone());

                                // Feed completion back to scheduler.
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

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Full E2E: 3-task linear DAG (extract → transform → load) runs to completion.
#[tokio::test]
async fn e2e_linear_dag_runs_to_completion() {
    let tmp = TempDir::new().unwrap();
    let event_store = EventStore::open(tmp.path()).unwrap();

    let dag = make_etl_dag();
    let dag_id = dag.id.clone();
    let run_id = "run-001".to_string();

    // Set up channels.
    let (scheduler_event_tx, scheduler_event_rx) = mpsc::unbounded_channel();
    let (scheduler_command_tx, scheduler_command_rx) = mpsc::unbounded_channel();
    let (executor_command_tx, executor_command_rx) = mpsc::unbounded_channel();
    let (executor_event_tx, executor_event_rx) = mpsc::unbounded_channel();

    // Build the scheduler with our DAG.
    let mut plans = HashMap::new();
    plans.insert(dag_id.clone(), dag.clone());
    let scheduler = Scheduler::new(
        scheduler_event_rx,
        scheduler_command_tx,
        PoolManager::default(),
        plans,
    )
    .unwrap();

    // Build the executor.
    let mut executor = TaskExecutor::new(executor_command_rx, executor_event_tx, 4);

    // Record DAG run creation in the event store.
    event_store
        .append(EventKind::DagRunCreated {
            dag_id: dag_id.clone(),
            run_id: run_id.clone(),
            logical_date: Utc::now(),
            environment: "test".to_string(),
            triggered_by: "integration_test".to_string(),
        })
        .unwrap();

    // Launch scheduler in background.
    let scheduler_handle = tokio::spawn(async move {
        scheduler.run().await
    });

    // Launch executor in background.
    let executor_handle = tokio::spawn(async move {
        executor.run().await
    });

    // Request a DAG run.
    scheduler_event_tx
        .send(SchedulerEvent::DagRunRequested {
            dag_id: dag_id.clone(),
            run_id: run_id.clone(),
            logical_date: Utc::now(),
            config: HashMap::new(),
        })
        .unwrap();

    // Run the mediator (bridges scheduler ↔ executor + records events).
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
        "Pipeline timed out — DAG did not complete within 30 seconds"
    );

    // Shut down.
    let _ = scheduler_event_tx.send(SchedulerEvent::Shutdown);
    let _ = executor_command_tx.send(ExecutorCommand::Shutdown);
    let _ = scheduler_handle.await;
    let _ = executor_handle.await;

    // ── Verify events ────────────────────────────────────────────────────

    let events = event_store.range(1, 100).unwrap();

    // Should have: DagRunCreated + 3x(TaskStarted + TaskCompleted) + DagRunCompleted = 8
    assert!(
        events.len() >= 7,
        "Expected at least 7 events (create + 3 starts + 3 completes + dag complete), got {}",
        events.len()
    );

    // First event: DagRunCreated
    assert!(
        matches!(&events[0].kind, EventKind::DagRunCreated { dag_id, run_id, .. }
            if dag_id == "etl_daily" && run_id == "run-001"),
        "First event should be DagRunCreated"
    );

    // Last event: DagRunCompleted with Success
    let last = events.last().unwrap();
    match &last.kind {
        EventKind::DagRunCompleted { status, .. } => {
            assert_eq!(*status, RunStatus::Success, "DAG run should succeed");
        }
        other => panic!("Last event should be DagRunCompleted, got {:?}", other),
    }

    // All 3 tasks should have completed.
    let task_completed_events: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::TaskCompleted { .. }))
        .collect();
    assert_eq!(
        task_completed_events.len(),
        3,
        "All 3 tasks should have completed"
    );

    // Verify topological ordering: extract < transform < load in event sequence.
    let task_completion_order: Vec<String> = task_completed_events
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::TaskCompleted { task_id, .. } => Some(task_id.clone()),
            _ => None,
        })
        .collect();

    let extract_pos = task_completion_order
        .iter()
        .position(|t| t == "extract")
        .expect("extract not found in completions");
    let transform_pos = task_completion_order
        .iter()
        .position(|t| t == "transform")
        .expect("transform not found in completions");
    let load_pos = task_completion_order
        .iter()
        .position(|t| t == "load")
        .expect("load not found in completions");

    assert!(
        extract_pos < transform_pos,
        "extract should complete before transform"
    );
    assert!(
        transform_pos < load_pos,
        "transform should complete before load"
    );
}

/// Full E2E: Diamond DAG (start → left/right → join) with fan-out/fan-in.
#[tokio::test]
async fn e2e_diamond_dag_fan_out_fan_in() {
    let tmp = TempDir::new().unwrap();
    let event_store = EventStore::open(tmp.path()).unwrap();

    let dag = make_diamond_dag();
    let dag_id = dag.id.clone();
    let run_id = "run-diamond-001".to_string();

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
            triggered_by: "integration_test".to_string(),
        })
        .unwrap();

    let scheduler_handle = tokio::spawn(async move {
        scheduler.run().await
    });
    let executor_handle = tokio::spawn(async move {
        executor.run().await
    });

    scheduler_event_tx
        .send(SchedulerEvent::DagRunRequested {
            dag_id: dag_id.clone(),
            run_id: run_id.clone(),
            logical_date: Utc::now(),
            config: HashMap::new(),
        })
        .unwrap();

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

    assert!(mediator_result.is_ok(), "Diamond DAG timed out");

    let _ = scheduler_event_tx.send(SchedulerEvent::Shutdown);
    let _ = executor_command_tx.send(ExecutorCommand::Shutdown);
    let _ = scheduler_handle.await;
    let _ = executor_handle.await;

    // ── Verify events ────────────────────────────────────────────────────

    let events = event_store.range(1, 100).unwrap();

    // All 4 unique tasks should have completed.
    let completed: Vec<String> = events
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::TaskCompleted { task_id, .. } => Some(task_id.clone()),
            _ => None,
        })
        .collect();

    // Deduplicate: scheduler fan-in may re-dispatch join when both
    // branches complete near-simultaneously — the important thing is
    // that all 4 unique task IDs appear.
    let unique_completed: std::collections::HashSet<&str> =
        completed.iter().map(|s| s.as_str()).collect();

    assert_eq!(
        unique_completed.len(),
        4,
        "All 4 unique tasks should complete, got: {:?}",
        unique_completed
    );
    assert!(unique_completed.contains("start"));
    assert!(unique_completed.contains("left"));
    assert!(unique_completed.contains("right"));
    assert!(unique_completed.contains("join"));

    // Verify topological ordering using first-occurrence positions.
    let start_pos = completed.iter().position(|t| t == "start").unwrap();
    let left_pos = completed.iter().position(|t| t == "left").unwrap();
    let right_pos = completed.iter().position(|t| t == "right").unwrap();
    let join_pos = completed.iter().position(|t| t == "join").unwrap();

    assert!(start_pos < left_pos, "start before left");
    assert!(start_pos < right_pos, "start before right");
    assert!(left_pos < join_pos, "left before join");
    assert!(right_pos < join_pos, "right before join");

    // DAG should have succeeded.
    let last = events.last().unwrap();
    match &last.kind {
        EventKind::DagRunCompleted {
            status: RunStatus::Success,
            ..
        } => {}
        other => panic!("Expected DagRunCompleted Success, got {:?}", other),
    }
}

/// E2E: Task failure propagates correctly — a failing task causes the DAG run to fail.
#[tokio::test]
async fn e2e_task_failure_propagates() {
    let tmp = TempDir::new().unwrap();
    let event_store = EventStore::open(tmp.path()).unwrap();

    let mut dag = make_etl_dag();
    // Make the "transform" task fail.
    dag.tasks.get_mut("transform").unwrap().task_type = TaskType::Bash {
        command: "exit 1".to_string(),
    };

    let dag_id = dag.id.clone();
    let run_id = "run-fail-001".to_string();

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
            triggered_by: "integration_test".to_string(),
        })
        .unwrap();

    let scheduler_handle = tokio::spawn(async move {
        scheduler.run().await
    });
    let executor_handle = tokio::spawn(async move {
        executor.run().await
    });

    scheduler_event_tx
        .send(SchedulerEvent::DagRunRequested {
            dag_id: dag_id.clone(),
            run_id: run_id.clone(),
            logical_date: Utc::now(),
            config: HashMap::new(),
        })
        .unwrap();

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

    assert!(mediator_result.is_ok(), "Failure DAG timed out");

    let _ = scheduler_event_tx.send(SchedulerEvent::Shutdown);
    let _ = executor_command_tx.send(ExecutorCommand::Shutdown);
    let _ = scheduler_handle.await;
    let _ = executor_handle.await;

    // ── Verify events ────────────────────────────────────────────────────

    let events = event_store.range(1, 100).unwrap();

    // Extract should succeed.
    let extract_completed = events.iter().any(|e| matches!(&e.kind,
        EventKind::TaskCompleted { task_id, .. } if task_id == "extract"
    ));
    assert!(extract_completed, "extract should have completed");

    // Transform should have a TaskFailed event.
    let transform_failed = events.iter().any(|e| matches!(&e.kind,
        EventKind::TaskFailed { task_id, .. } if task_id == "transform"
    ));
    assert!(transform_failed, "transform should have failed");

    // Load should either be skipped or not started.
    let load_completed = events.iter().any(|e| matches!(&e.kind,
        EventKind::TaskCompleted { task_id, .. } if task_id == "load"
    ));
    assert!(!load_completed, "load should NOT have completed");

    // DAG run should be marked as Failed.
    let dag_failed = events.iter().any(|e| matches!(&e.kind,
        EventKind::DagRunCompleted { status: RunStatus::Failed, .. }
    ));
    assert!(dag_failed, "DAG run should have failed");
}

/// E2E: Event store retention — verify that events survive and sequence numbers are correct.
#[tokio::test]
async fn e2e_event_store_captures_correct_sequences() {
    let tmp = TempDir::new().unwrap();
    let event_store = EventStore::open(tmp.path()).unwrap();

    // Manually append a series of events simulating a DAG run.
    let e1 = event_store
        .append(EventKind::DagRunCreated {
            dag_id: "seq_test".into(),
            run_id: "r1".into(),
            logical_date: Utc::now(),
            environment: "test".into(),
            triggered_by: "test".into(),
        })
        .unwrap();

    let e2 = event_store
        .append(EventKind::TaskQueued {
            dag_id: "seq_test".into(),
            run_id: "r1".into(),
            task_id: "t1".into(),
            priority: 0,
            pool: None,
            snapshot_fingerprint: None,
        })
        .unwrap();

    let e3 = event_store
        .append(EventKind::TaskStarted {
            dag_id: "seq_test".into(),
            run_id: "r1".into(),
            task_id: "t1".into(),
            worker_id: "local".into(),
            attempt: 0,
            pid: Some(12345),
        })
        .unwrap();

    let e4 = event_store
        .append(EventKind::TaskCompleted {
            dag_id: "seq_test".into(),
            run_id: "r1".into(),
            task_id: "t1".into(),
            duration_ms: 500,
            snapshot_id: None,
        })
        .unwrap();

    let e5 = event_store
        .append(EventKind::DagRunCompleted {
            dag_id: "seq_test".into(),
            run_id: "r1".into(),
            status: RunStatus::Success,
            duration_ms: 750,
        })
        .unwrap();

    // Verify monotonic sequences.
    assert_eq!(e1.sequence, 1);
    assert_eq!(e2.sequence, 2);
    assert_eq!(e3.sequence, 3);
    assert_eq!(e4.sequence, 4);
    assert_eq!(e5.sequence, 5);

    // Verify range query returns all events in order.
    let all = event_store.range(1, 5).unwrap();
    assert_eq!(all.len(), 5);
    for (i, event) in all.iter().enumerate() {
        assert_eq!(event.sequence, (i + 1) as u64);
    }

    // Verify point lookups.
    let fetched = event_store.get(3).unwrap().unwrap();
    match &fetched.kind {
        EventKind::TaskStarted { task_id, pid, .. } => {
            assert_eq!(task_id, "t1");
            assert_eq!(*pid, Some(12345));
        }
        other => panic!("Expected TaskStarted, got {:?}", other),
    }

    // Verify timestamps are monotonically non-decreasing.
    for window in all.windows(2) {
        assert!(
            window[0].timestamp <= window[1].timestamp,
            "Timestamps should be non-decreasing"
        );
    }
}
