//! Coordinator crash-recovery tests (PRD E3).
//!
//! A coordinator backed by a durable `RocksAssignmentStore` must be able to
//! restart and reconstruct the tasks that were in flight, re-queuing them for
//! dispatch to whichever workers reconnect — nothing dispatched-but-unfinished
//! is silently lost across a restart.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;

use conduit_distributed::assignment_store::RocksAssignmentStore;
use conduit_distributed::coordinator::{Coordinator, CoordinatorConfig};
use conduit_distributed::proto_types::*;

fn make_register(id: &str, capacity: u32) -> RegisterRequest {
    RegisterRequest {
        worker_id: id.to_string(),
        hostname: format!("{id}.local"),
        capacity,
        pool_affinity: vec!["default".to_string()],
        labels: HashMap::new(),
        version: "test".to_string(),
        health_port: 0,
    }
}

fn make_spec() -> TaskSpec {
    TaskSpec {
        task_type: TaskType::Bash,
        script: "true".into(),
        connection: String::new(),
        query: String::new(),
        command: String::new(),
        args: vec![],
        timeout_secs: 60,
        resources: ResourceLimits::default(),
    }
}

fn make_context(task_id: &str) -> TaskContext {
    TaskContext {
        dag_id: "dag".into(),
        run_id: "run".into(),
        task_id: task_id.into(),
        attempt: 0,
        logical_date_epoch_ms: Utc::now().timestamp_millis(),
        environment: "test".into(),
        params: HashMap::new(),
    }
}

fn config() -> CoordinatorConfig {
    CoordinatorConfig {
        bind_addr: "127.0.0.1:0".into(),
        health_check_interval_secs: 3600,
        ..CoordinatorConfig::default()
    }
}

#[tokio::test]
async fn restarted_coordinator_recovers_inflight_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordinator_assignments");

    // ── Instance 1: dispatch three tasks to a worker, then "crash" ───────────
    {
        let store = Arc::new(RocksAssignmentStore::open(&db_path).unwrap());
        let (coord, _rx) = Coordinator::with_store(config(), store);
        let mut worker_rx = coord.register_worker(&make_register("w1", 8));

        for i in 0..3 {
            let task_id = format!("task{i}");
            let a = coord.create_assignment(
                "dag",
                "run",
                &task_id,
                0,
                make_spec(),
                make_context(&task_id),
                300,
            );
            coord.submit_task(a, "default").await;
        }
        assert_eq!(coord.inflight_count(), 3, "all three should be in flight");
        // Worker received them but never reports — simulating the process
        // dying with tasks still running.
        let mut received = 0;
        while worker_rx.try_recv().is_ok() {
            received += 1;
        }
        assert_eq!(received, 3);
        // Drop coord + store: the coordinator process is gone. The RocksDB
        // directory on disk retains the three assignments.
    }

    // ── Instance 2: restart on the same store, recover ───────────────────────
    let store = Arc::new(RocksAssignmentStore::open(&db_path).unwrap());
    let (coord, mut result_rx) = Coordinator::with_store(config(), store);
    let coord = Arc::new(coord);

    // Before recovery, nothing is dispatched and the coordinator reports it is
    // recovering only while recover() runs; the persisted work isn't lost.
    let recovered = coord.recover().await;
    assert_eq!(recovered, 3, "all three in-flight tasks must be recovered");
    assert!(!coord.is_recovering());

    // No workers yet → recovered tasks sit in the pending queue, not lost.
    assert_eq!(coord.pending_count().await, 3);
    assert_eq!(coord.inflight_count(), 0);

    // A fresh worker connects and drains the recovered work.
    let mut worker_rx = coord.register_worker(&make_register("w2", 8));
    coord.health_check().await; // triggers a drain
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The three recovered tasks are now dispatched to the new worker.
    let mut redispatched = Vec::new();
    while let Ok(a) = worker_rx.try_recv() {
        redispatched.push(a.task_id);
    }
    redispatched.sort();
    assert_eq!(redispatched, vec!["task0", "task1", "task2"]);
    assert_eq!(coord.pending_count().await, 0);
    assert_eq!(coord.inflight_count(), 3);

    // (The completed-tasks-are-dropped half of the durability contract is
    // covered by `completed_tasks_are_not_recovered` below.)
    let _ = &mut result_rx;
}

#[tokio::test]
async fn completed_tasks_are_not_recovered() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordinator_assignments");

    // ── Instance 1: dispatch a task, complete it, then "crash" ───────────────
    {
        let store = Arc::new(RocksAssignmentStore::open(&db_path).unwrap());
        let (coord, _rx) = Coordinator::with_store(config(), store);
        coord.register_worker(&make_register("w1", 8));

        let a = coord.create_assignment(
            "dag",
            "run",
            "task0",
            0,
            make_spec(),
            make_context("task0"),
            300,
        );
        let assignment_id = a.assignment_id.clone();
        coord.submit_task(a, "default").await;
        assert_eq!(coord.inflight_count(), 1);

        // Complete it — the durable record must be dropped.
        coord.handle_result(TaskResult {
            assignment_id,
            worker_id: "w1".into(),
            dag_id: "dag".into(),
            run_id: "run".into(),
            task_id: "task0".into(),
            attempt: 0,
            outcome: TaskOutcome::Success,
            exit_code: 0,
            duration_ms: 1,
            xcom_json: "{}".into(),
            error: String::new(),
            metrics: HashMap::new(),
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    // Restart: nothing to recover, because the completed task was removed.
    let store = Arc::new(RocksAssignmentStore::open(&db_path).unwrap());
    let (coord2, _rx2) = Coordinator::with_store(config(), store);
    let recovered = coord2.recover().await;
    assert_eq!(recovered, 0, "completed tasks must not be recovered");
}
