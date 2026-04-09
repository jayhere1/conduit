//! Distributed end-to-end tests for conduit-distributed.
//!
//! These tests exercise multi-worker scenarios that go beyond the single-worker
//! gRPC integration tests: task distribution across workers, failure-triggered
//! reassignment, inflight tracking accuracy, and result reporting lifecycle.
//!
//! All tests reuse the same `start_server()` / `connect_client()` pattern from
//! grpc_integration_test.rs and run on ephemeral ports.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tonic::Request;

use conduit_distributed::coordinator::{Coordinator, CoordinatorConfig};
use conduit_distributed::grpc_server::CoordinatorGrpcService;
use conduit_distributed::proto_types::*;

// ─── Generated proto types ──────────────────────────────────────────────────

mod proto {
    include!("../src/generated/conduit.distributed.rs");
}

use proto::coordinator_client::CoordinatorClient;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Start a coordinator gRPC server on an ephemeral port and return the
/// address plus a handle to the coordinator so tests can inspect state.
async fn start_server() -> (
    SocketAddr,
    Arc<Coordinator>,
    mpsc::UnboundedReceiver<TaskResult>,
) {
    let config = CoordinatorConfig {
        bind_addr: "127.0.0.1:0".into(),
        health_check_interval_secs: 60, // disable auto-checks during tests
        ..CoordinatorConfig::default()
    };

    let (coordinator, result_rx) = Coordinator::new(config);
    let coordinator = Arc::new(coordinator);

    let service = CoordinatorGrpcService::new(coordinator.clone());
    let server = service.into_server();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    (addr, coordinator, result_rx)
}

/// Connect a tonic client to the server.
async fn connect_client(addr: SocketAddr) -> CoordinatorClient<tonic::transport::Channel> {
    let endpoint = format!("http://{}", addr);
    CoordinatorClient::connect(endpoint)
        .await
        .expect("Failed to connect to coordinator")
}

/// Build a proto RegisterRequest.
fn make_proto_register(worker_id: &str, capacity: u32) -> proto::RegisterRequest {
    proto::RegisterRequest {
        worker_id: worker_id.into(),
        hostname: format!("{}.test", worker_id),
        capacity,
        pool_affinity: vec!["default".into()],
        labels: HashMap::new(),
        version: "0.1.0-test".into(),
        health_port: 0,
    }
}

/// Build a proto TaskResult.
fn make_proto_result(
    assignment_id: &str,
    worker_id: &str,
    task_id: &str,
    outcome: i32,
) -> proto::TaskResult {
    proto::TaskResult {
        assignment_id: assignment_id.into(),
        worker_id: worker_id.into(),
        dag_id: "dag1".into(),
        run_id: "run1".into(),
        task_id: task_id.into(),
        attempt: 0,
        outcome,
        exit_code: if outcome == 1 { 0 } else { 1 },
        duration_ms: 150,
        xcom_json: "{}".into(),
        error: String::new(),
        metrics: HashMap::new(),
    }
}

/// Create a TaskSpec + TaskContext and submit via the coordinator, returning
/// the assignment_id.
async fn submit_task_helper(coordinator: &Coordinator, task_id: &str, pool: &str) -> String {
    let spec = TaskSpec {
        task_type: TaskType::Bash,
        script: format!("echo {}", task_id),
        connection: String::new(),
        query: String::new(),
        command: String::new(),
        args: vec![],
        timeout_secs: 60,
        resources: ResourceLimits::default(),
    };
    let ctx = TaskContext {
        dag_id: "dag1".into(),
        run_id: "run1".into(),
        task_id: task_id.into(),
        attempt: 0,
        logical_date_epoch_ms: Utc::now().timestamp_millis(),
        environment: "test".into(),
        params: HashMap::new(),
    };
    let assignment = coordinator.create_assignment("dag1", "run1", task_id, 0, spec, ctx, 300);
    let id = assignment.assignment_id.clone();
    coordinator.submit_task(assignment, pool).await;
    id
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Test: Two workers registered, four tasks submitted. Both workers receive
/// assignments (least-loaded routing distributes across workers).
#[tokio::test]
async fn test_two_workers_task_distribution() {
    let (addr, coordinator, _result_rx) = start_server().await;

    // Register two workers, each with capacity 4.
    let mut client1 = connect_client(addr).await;
    let mut client2 = connect_client(addr).await;

    let resp1 = client1
        .register(Request::new(make_proto_register("dist-w1", 4)))
        .await
        .unwrap();
    let mut stream1 = resp1.into_inner();

    let resp2 = client2
        .register(Request::new(make_proto_register("dist-w2", 4)))
        .await
        .unwrap();
    let mut stream2 = resp2.into_inner();

    assert_eq!(coordinator.worker_pool().total_workers(), 2);

    // Submit 4 tasks.
    for i in 0..4 {
        let task_id = format!("dist-task-{}", i);
        submit_task_helper(&coordinator, &task_id, "default").await;
    }

    // Collect assignments from both streams.
    let mut w1_tasks = Vec::new();
    let mut w2_tasks = Vec::new();

    // Drain whatever each stream has within a timeout window.
    for _ in 0..4 {
        tokio::select! {
            result = stream1.next() => {
                if let Some(Ok(a)) = result {
                    w1_tasks.push(a.task_id.clone());
                }
            }
            result = stream2.next() => {
                if let Some(Ok(a)) = result {
                    w2_tasks.push(a.task_id.clone());
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                break;
            }
        }
    }

    let total_received = w1_tasks.len() + w2_tasks.len();

    // Both workers should have received at least one task (least-loaded
    // routing spreads evenly when both start empty).
    assert!(
        !w1_tasks.is_empty(),
        "Worker 1 should have received at least one task, got: w1={:?} w2={:?}",
        w1_tasks,
        w2_tasks
    );
    assert!(
        !w2_tasks.is_empty(),
        "Worker 2 should have received at least one task, got: w1={:?} w2={:?}",
        w1_tasks,
        w2_tasks
    );
    assert_eq!(
        total_received, 4,
        "All 4 tasks should be distributed, got: w1={:?} w2={:?}",
        w1_tasks, w2_tasks
    );
}

/// Test: Register 2 workers. Submit a task to worker-1 (the only worker at
/// that point). Drop worker-1's stream to simulate failure. Register worker-2.
/// Submit a retry task. Worker-2 picks it up.
#[tokio::test]
async fn test_worker_failure_triggers_reassignment() {
    let (addr, coordinator, _result_rx) = start_server().await;

    // Register only worker-1 first, so the task is guaranteed to route there.
    let mut client1 = connect_client(addr).await;
    let resp1 = client1
        .register(Request::new(make_proto_register("fail-w1", 4)))
        .await
        .unwrap();
    let mut stream1 = resp1.into_inner();

    // Submit a task — it must go to fail-w1 (the only worker).
    let _aid = submit_task_helper(&coordinator, "reassign-task", "default").await;

    // Consume the assignment on worker-1 to confirm it arrived.
    let received_w1 = timeout(Duration::from_secs(5), stream1.next())
        .await
        .expect("Timed out waiting for assignment on worker-1")
        .expect("Worker-1 stream ended")
        .expect("Worker-1 stream error");
    assert_eq!(received_w1.task_id, "reassign-task");

    // Verify inflight.
    assert_eq!(coordinator.inflight_count(), 1);

    // Drop worker-1's stream and client to simulate a crash.
    drop(stream1);
    drop(client1);

    // Mark worker-1 as draining (simulates what health_check would do
    // after heartbeat timeout; we can't wait 120s in a test).
    coordinator
        .worker_pool()
        .drain_worker("fail-w1", "simulated failure");

    // Now register worker-2.
    let mut client2 = connect_client(addr).await;
    let resp2 = client2
        .register(Request::new(make_proto_register("fail-w2", 4)))
        .await
        .unwrap();
    let mut stream2 = resp2.into_inner();

    // Submit a retry of the same logical task (attempt=1) to simulate the
    // scheduler's retry-after-failure behaviour.
    let spec = TaskSpec {
        task_type: TaskType::Bash,
        script: "echo reassign-task".into(),
        connection: String::new(),
        query: String::new(),
        command: String::new(),
        args: vec![],
        timeout_secs: 60,
        resources: ResourceLimits::default(),
    };
    let ctx = TaskContext {
        dag_id: "dag1".into(),
        run_id: "run1".into(),
        task_id: "reassign-task".into(),
        attempt: 1,
        logical_date_epoch_ms: Utc::now().timestamp_millis(),
        environment: "test".into(),
        params: HashMap::new(),
    };
    let new_assignment =
        coordinator.create_assignment("dag1", "run1", "reassign-task", 1, spec, ctx, 300);
    coordinator.submit_task(new_assignment, "default").await;

    // Worker-2 should receive the retried task.
    let received = timeout(Duration::from_secs(5), stream2.next())
        .await
        .expect("Timed out waiting for reassigned task on worker-2")
        .expect("Worker-2 stream ended unexpectedly")
        .expect("Worker-2 stream error");

    assert_eq!(received.task_id, "reassign-task");
    assert_eq!(received.attempt, 1);

    // We now have two inflight entries: the original stuck on dead fail-w1,
    // plus the new one on fail-w2.
    assert!(coordinator.inflight_count() >= 1);
}

/// Test: Submit tasks, complete some, and verify the inflight count is
/// tracked correctly throughout the lifecycle.
#[tokio::test]
async fn test_coordinator_tracks_inflight_correctly() {
    let (addr, coordinator, mut result_rx) = start_server().await;

    // Register a worker.
    let mut client = connect_client(addr).await;
    let resp = client
        .register(Request::new(make_proto_register("inflight-w1", 8)))
        .await
        .unwrap();
    let mut stream = resp.into_inner();

    // Initially: 0 inflight.
    assert_eq!(coordinator.inflight_count(), 0);

    // Submit 3 tasks.
    let mut assignment_ids = Vec::new();
    for i in 0..3 {
        let task_id = format!("inflight-task-{}", i);
        let aid = submit_task_helper(&coordinator, &task_id, "default").await;
        assignment_ids.push(aid);
    }

    // Drain the stream to ensure all are dispatched.
    for _ in 0..3 {
        let _ = timeout(Duration::from_secs(2), stream.next()).await;
    }

    // After submitting 3: inflight should be 3.
    assert_eq!(
        coordinator.inflight_count(),
        3,
        "Expected 3 inflight tasks after submitting 3"
    );

    // Complete task 0 (success).
    let result0 = make_proto_result(&assignment_ids[0], "inflight-w1", "inflight-task-0", 1);
    client.report_result(Request::new(result0)).await.unwrap();
    let _ = timeout(Duration::from_secs(1), result_rx.recv()).await;

    assert_eq!(
        coordinator.inflight_count(),
        2,
        "Expected 2 inflight after completing 1"
    );

    // Complete task 1 (failure).
    let result1 = make_proto_result(&assignment_ids[1], "inflight-w1", "inflight-task-1", 2);
    client.report_result(Request::new(result1)).await.unwrap();
    let _ = timeout(Duration::from_secs(1), result_rx.recv()).await;

    assert_eq!(
        coordinator.inflight_count(),
        1,
        "Expected 1 inflight after completing 2"
    );

    // Complete task 2 (success).
    let result2 = make_proto_result(&assignment_ids[2], "inflight-w1", "inflight-task-2", 1);
    client.report_result(Request::new(result2)).await.unwrap();
    let _ = timeout(Duration::from_secs(1), result_rx.recv()).await;

    assert_eq!(
        coordinator.inflight_count(),
        0,
        "Expected 0 inflight after completing all"
    );
}

/// Test: Submit a task, report success via gRPC, and verify the coordinator
/// clears it from inflight and forwards the result correctly.
#[tokio::test]
async fn test_task_result_reporting() {
    let (addr, coordinator, mut result_rx) = start_server().await;

    // Register worker.
    let mut client = connect_client(addr).await;
    let resp = client
        .register(Request::new(make_proto_register("result-w1", 4)))
        .await
        .unwrap();
    let mut stream = resp.into_inner();

    // Submit a task.
    let aid = submit_task_helper(&coordinator, "result-task", "default").await;

    // Receive the assignment on the worker stream.
    let assignment = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("Timed out waiting for assignment")
        .expect("Stream ended")
        .expect("Stream error");
    assert_eq!(assignment.task_id, "result-task");
    assert_eq!(assignment.assignment_id, aid);

    // Verify inflight before reporting result.
    assert_eq!(coordinator.inflight_count(), 1);

    // Report success with metrics and xcom data.
    let result = proto::TaskResult {
        assignment_id: aid.clone(),
        worker_id: "result-w1".into(),
        dag_id: "dag1".into(),
        run_id: "run1".into(),
        task_id: "result-task".into(),
        attempt: 0,
        outcome: 1, // Success
        exit_code: 0,
        duration_ms: 250,
        xcom_json: r#"{"rows": 1000}"#.into(),
        error: String::new(),
        metrics: HashMap::from([
            ("rows_processed".into(), 1000.0),
            ("duration_sec".into(), 0.25),
        ]),
    };

    let ack = client
        .report_result(Request::new(result))
        .await
        .expect("ReportResult RPC failed")
        .into_inner();

    assert!(ack.success);
    assert_eq!(ack.message, "Result accepted");

    // Verify the coordinator forwarded the result to the scheduler channel.
    let forwarded = timeout(Duration::from_secs(2), result_rx.recv())
        .await
        .expect("Timed out waiting for forwarded result")
        .expect("Result channel closed");

    assert_eq!(forwarded.assignment_id, aid);
    assert_eq!(forwarded.task_id, "result-task");
    assert_eq!(forwarded.outcome, TaskOutcome::Success);
    assert_eq!(forwarded.exit_code, 0);
    assert_eq!(forwarded.duration_ms, 250);
    assert_eq!(forwarded.xcom_json, r#"{"rows": 1000}"#);
    assert_eq!(forwarded.metrics["rows_processed"], 1000.0);
    assert!((forwarded.metrics["duration_sec"] - 0.25).abs() < f64::EPSILON);

    // Inflight should now be zero.
    assert_eq!(
        coordinator.inflight_count(),
        0,
        "Inflight should be 0 after result reported"
    );
}
