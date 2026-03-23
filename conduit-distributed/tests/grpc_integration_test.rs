//! gRPC integration tests for conduit-distributed.
//!
//! These tests spin up a real tonic gRPC server on localhost and connect
//! a client to it, exercising the full Register → TaskAssignment →
//! ReportResult → Heartbeat → StreamLogs → ClusterStatus flow.
//!
//! Unlike unit tests (which test coordinator/worker in isolation), these
//! tests verify:
//! - Proto ↔ local type conversion under real serialisation
//! - tonic streaming (server-streaming, bidirectional, client-streaming)
//! - Concurrent task dispatch + result collection over the wire
//! - Heartbeat directive propagation
//! - Log entry delivery
//!
//! All tests use ephemeral ports on 127.0.0.1 to avoid port conflicts.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::Request;

use conduit_distributed::coordinator::{Coordinator, CoordinatorConfig};
use conduit_distributed::grpc_server::CoordinatorGrpcService;
use conduit_distributed::proto_types::*;

// ─── Generated proto types ──────────────────────────────────────────────────

// We include the generated proto module so we can construct raw proto
// messages and call the generated client directly — the same path a
// real WorkerGrpcClient would use.
mod proto {
    include!("../src/generated/conduit.distributed.rs");
}

use proto::coordinator_client::CoordinatorClient;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Start a coordinator gRPC server on an ephemeral port and return the
/// address plus a handle to the coordinator so tests can inspect state.
async fn start_server() -> (SocketAddr, Arc<Coordinator>, mpsc::UnboundedReceiver<TaskResult>) {
    let config = CoordinatorConfig {
        bind_addr: "127.0.0.1:0".into(),
        health_check_interval_secs: 60, // disable auto-checks during tests
        ..CoordinatorConfig::default()
    };

    let (coordinator, result_rx) = Coordinator::new(config);
    let coordinator = Arc::new(coordinator);

    let service = CoordinatorGrpcService::new(coordinator.clone());
    let server = service.into_server();

    // Bind to port 0 to get an ephemeral port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Start the server in the background.
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give the server a moment to accept connections.
    tokio::time::sleep(Duration::from_millis(50)).await;

    (addr, coordinator, result_rx)
}

/// Connect a tonic client to the server.
async fn connect_client(
    addr: SocketAddr,
) -> CoordinatorClient<tonic::transport::Channel> {
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

/// Build a proto WorkerHeartbeat.
fn make_proto_heartbeat(worker_id: &str, active_tasks: u32) -> proto::WorkerHeartbeat {
    proto::WorkerHeartbeat {
        worker_id: worker_id.into(),
        active_tasks,
        cpu_percent: 25.0,
        memory_percent: 50.0,
        disk_percent: 10.0,
        running_assignments: vec![],
        timestamp_ms: Utc::now().timestamp_millis(),
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Test: Register a worker via gRPC, submit a task on the coordinator,
/// and verify the task arrives on the worker's assignment stream.
#[tokio::test]
async fn register_and_receive_task_assignment() {
    let (addr, coordinator, _result_rx) = start_server().await;
    let mut client = connect_client(addr).await;

    // Register the worker and get back a streaming response.
    let response = client
        .register(Request::new(make_proto_register("w1", 4)))
        .await
        .expect("Register RPC failed");

    let mut stream = response.into_inner();

    // Now submit a task on the coordinator (server-side).
    let spec = TaskSpec {
        task_type: TaskType::Bash,
        script: "echo hello".into(),
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
        task_id: "task1".into(),
        attempt: 0,
        logical_date_epoch_ms: Utc::now().timestamp_millis(),
        environment: "test".into(),
        params: HashMap::new(),
    };
    let assignment = coordinator.create_assignment("dag1", "run1", "task1", 0, spec, ctx, 300);
    let assignment_id = assignment.assignment_id.clone();

    coordinator.submit_task(assignment, "default").await;

    // The worker should receive the assignment on the stream.
    let proto_assignment = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("Timed out waiting for assignment")
        .expect("Stream ended")
        .expect("Stream error");

    assert_eq!(proto_assignment.task_id, "task1");
    assert_eq!(proto_assignment.dag_id, "dag1");
    assert_eq!(proto_assignment.assignment_id, assignment_id);
    assert!(proto_assignment.spec.is_some());
    assert!(proto_assignment.context.is_some());
}

/// Test: Register → submit → report result → verify coordinator gets it.
#[tokio::test]
async fn full_task_lifecycle() {
    let (addr, coordinator, mut result_rx) = start_server().await;
    let mut client = connect_client(addr).await;

    // Register.
    let response = client
        .register(Request::new(make_proto_register("w1", 4)))
        .await
        .unwrap();
    let mut stream = response.into_inner();

    // Submit task.
    let spec = TaskSpec {
        task_type: TaskType::Bash,
        script: "echo ok".into(),
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
        task_id: "lifecycle-task".into(),
        attempt: 0,
        logical_date_epoch_ms: Utc::now().timestamp_millis(),
        environment: "test".into(),
        params: HashMap::new(),
    };
    let assignment = coordinator.create_assignment("dag1", "run1", "lifecycle-task", 0, spec, ctx, 300);
    let assignment_id = assignment.assignment_id.clone();

    coordinator.submit_task(assignment, "default").await;

    // Receive assignment.
    let proto_assignment = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(proto_assignment.assignment_id, assignment_id);

    // Report result via gRPC.
    let proto_result = make_proto_result(&assignment_id, "w1", "lifecycle-task", 1);
    let ack = client
        .report_result(Request::new(proto_result))
        .await
        .expect("ReportResult RPC failed")
        .into_inner();

    assert!(ack.success);
    assert_eq!(ack.message, "Result accepted");

    // The coordinator should have forwarded the result to the scheduler channel.
    let forwarded = timeout(Duration::from_secs(2), result_rx.recv())
        .await
        .expect("Timed out waiting for result")
        .expect("Result channel closed");

    assert_eq!(forwarded.assignment_id, assignment_id);
    assert_eq!(forwarded.task_id, "lifecycle-task");
    assert_eq!(forwarded.outcome, TaskOutcome::Success);
    assert_eq!(forwarded.exit_code, 0);

    // Inflight should be zero now.
    assert_eq!(coordinator.inflight_count(), 0);
}

/// Test: Multiple workers register and receive tasks routed to them.
#[tokio::test]
async fn multi_worker_dispatch() {
    let (addr, coordinator, _result_rx) = start_server().await;

    // Register two workers.
    let mut client1 = connect_client(addr).await;
    let mut client2 = connect_client(addr).await;

    let resp1 = client1
        .register(Request::new(make_proto_register("w1", 2)))
        .await
        .unwrap();
    let mut stream1 = resp1.into_inner();

    let resp2 = client2
        .register(Request::new(make_proto_register("w2", 2)))
        .await
        .unwrap();
    let mut stream2 = resp2.into_inner();

    assert_eq!(coordinator.worker_pool().total_workers(), 2);

    // Submit two tasks — they should go to different workers (least-loaded).
    for i in 0..2 {
        let task_id = format!("task-{}", i);
        let spec = TaskSpec {
            task_type: TaskType::Bash,
            script: format!("echo {}", i),
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
            task_id: task_id.clone(),
            attempt: 0,
            logical_date_epoch_ms: Utc::now().timestamp_millis(),
            environment: "test".into(),
            params: HashMap::new(),
        };
        let assignment = coordinator.create_assignment("dag1", "run1", &task_id, 0, spec, ctx, 300);
        coordinator.submit_task(assignment, "default").await;
    }

    // Collect all assignments from both streams within a timeout.
    let mut received = Vec::new();

    // Try to get one from each stream. The routing is least-loaded so
    // with two fresh workers, one task should go to each.
    for stream in [&mut stream1, &mut stream2] {
        if let Ok(Some(Ok(a))) = timeout(Duration::from_secs(2), stream.next()).await {
            received.push(a.task_id.clone());
        }
    }

    // We should have received at least 2 tasks total across both workers.
    assert_eq!(received.len(), 2);
    assert!(received.contains(&"task-0".to_string()));
    assert!(received.contains(&"task-1".to_string()));
}

/// Test: Bidirectional heartbeat stream — send a heartbeat and receive an ack.
#[tokio::test]
async fn heartbeat_bidirectional_stream() {
    let (addr, _coordinator, _result_rx) = start_server().await;
    let mut client = connect_client(addr).await;

    // Register the worker first so the coordinator knows about it.
    let _resp = client
        .register(Request::new(make_proto_register("hb-worker", 4)))
        .await
        .unwrap();

    // Set up a heartbeat stream.
    let (tx, rx) = mpsc::unbounded_channel::<proto::WorkerHeartbeat>();
    let outbound = UnboundedReceiverStream::new(rx);

    let response = client
        .heartbeat(Request::new(outbound))
        .await
        .expect("Heartbeat RPC failed");

    let mut directives = response.into_inner();

    // Send a heartbeat.
    tx.send(make_proto_heartbeat("hb-worker", 0)).unwrap();

    // Should receive a HeartbeatAck directive.
    let directive = timeout(Duration::from_secs(5), directives.next())
        .await
        .expect("Timed out waiting for directive")
        .expect("Directive stream ended")
        .expect("Directive stream error");

    // Verify it's an ack.
    assert!(directive.directive.is_some());
    match directive.directive.unwrap() {
        proto::coordinator_directive::Directive::Ack(ack) => {
            assert!(ack.timestamp_ms > 0);
        }
        other => panic!("Expected HeartbeatAck, got {:?}", other),
    }

    // Send another heartbeat to verify the stream stays open.
    tx.send(make_proto_heartbeat("hb-worker", 1)).unwrap();

    let directive2 = timeout(Duration::from_secs(5), directives.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    match directive2.directive.unwrap() {
        proto::coordinator_directive::Directive::Ack(_) => {} // good
        other => panic!("Expected second HeartbeatAck, got {:?}", other),
    }
}

/// Test: Client-streaming log entries and verify they reach the coordinator.
#[tokio::test]
async fn stream_logs_delivers_entries() {
    let (addr, coordinator, _result_rx) = start_server().await;
    let mut client = connect_client(addr).await;

    // Register first.
    let _resp = client
        .register(Request::new(make_proto_register("log-worker", 4)))
        .await
        .unwrap();

    // Build log entries.
    let entries = vec![
        proto::TaskLogEntry {
            assignment_id: "assign-1".into(),
            worker_id: "log-worker".into(),
            level: 2, // Info
            message: "Starting data load".into(),
            timestamp_ms: Utc::now().timestamp_millis(),
            metadata_json: String::new(),
        },
        proto::TaskLogEntry {
            assignment_id: "assign-1".into(),
            worker_id: "log-worker".into(),
            level: 3, // Warn
            message: "Slow query detected".into(),
            timestamp_ms: Utc::now().timestamp_millis(),
            metadata_json: r#"{"query_ms": 5000}"#.into(),
        },
        proto::TaskLogEntry {
            assignment_id: "assign-1".into(),
            worker_id: "log-worker".into(),
            level: 2, // Info
            message: "Data load complete".into(),
            timestamp_ms: Utc::now().timestamp_millis(),
            metadata_json: String::new(),
        },
    ];

    let entry_count = entries.len();

    // Stream them via the RPC.
    let stream = tokio_stream::iter(entries);
    let ack = client
        .stream_logs(Request::new(stream))
        .await
        .expect("StreamLogs RPC failed")
        .into_inner();

    assert!(ack.success);
    assert!(
        ack.message.contains(&entry_count.to_string()),
        "Expected ack to mention {} entries, got: {}",
        entry_count,
        ack.message
    );

    // Verify the coordinator stored them.
    let stored = coordinator.get_logs("assign-1");
    assert_eq!(stored.len(), 3);
    assert_eq!(stored[0].message, "Starting data load");
    assert_eq!(stored[1].level, LogLevel::Warn);
    assert_eq!(stored[2].message, "Data load complete");
}

/// Test: ClusterStatus RPC returns correct worker count and health.
#[tokio::test]
async fn cluster_status_rpc() {
    let (addr, _coordinator, _result_rx) = start_server().await;
    let mut client = connect_client(addr).await;

    // Before any workers.
    let status = client
        .cluster_status(Request::new(proto::ClusterStatusRequest {}))
        .await
        .expect("ClusterStatus RPC failed")
        .into_inner();

    // No workers → health should still report (likely Healthy with 0 workers).
    assert_eq!(status.workers.len(), 0);
    assert!(status.uptime_secs < 5); // just started

    // Register two workers.
    let mut c1 = connect_client(addr).await;
    let mut c2 = connect_client(addr).await;
    let _r1 = c1
        .register(Request::new(make_proto_register("status-w1", 4)))
        .await
        .unwrap();
    let _r2 = c2
        .register(Request::new(make_proto_register("status-w2", 8)))
        .await
        .unwrap();

    // Query again.
    let status2 = client
        .cluster_status(Request::new(proto::ClusterStatusRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(status2.workers.len(), 2);
    assert_eq!(status2.health, 1); // Healthy

    // Verify individual worker info.
    let w1 = status2
        .workers
        .iter()
        .find(|w| w.worker_id == "status-w1")
        .expect("w1 not in status");
    assert_eq!(w1.capacity, 4);
    assert_eq!(w1.state, 1); // Active

    let w2 = status2
        .workers
        .iter()
        .find(|w| w.worker_id == "status-w2")
        .expect("w2 not in status");
    assert_eq!(w2.capacity, 8);
}

/// Test: Submitting a task when no workers are registered queues it;
/// registering a worker later and running health_check drains the queue.
#[tokio::test]
async fn queued_task_dispatched_on_worker_arrival() {
    let (addr, coordinator, _result_rx) = start_server().await;

    // Submit task before any workers register.
    let spec = TaskSpec {
        task_type: TaskType::Python,
        script: "print('hi')".into(),
        connection: String::new(),
        query: String::new(),
        command: String::new(),
        args: vec![],
        timeout_secs: 120,
        resources: ResourceLimits::default(),
    };
    let ctx = TaskContext {
        dag_id: "dag1".into(),
        run_id: "run1".into(),
        task_id: "queued-task".into(),
        attempt: 0,
        logical_date_epoch_ms: Utc::now().timestamp_millis(),
        environment: "test".into(),
        params: HashMap::new(),
    };
    let assignment = coordinator.create_assignment("dag1", "run1", "queued-task", 0, spec, ctx, 300);
    coordinator.submit_task(assignment, "default").await;

    assert_eq!(coordinator.pending_count().await, 1);
    assert_eq!(coordinator.inflight_count(), 0);

    // Now register a worker via gRPC.
    let mut client = connect_client(addr).await;
    let resp = client
        .register(Request::new(make_proto_register("late-worker", 4)))
        .await
        .unwrap();
    let mut stream = resp.into_inner();

    // Trigger a health check / queue drain.
    coordinator.health_check().await;

    // The queued task should now be dispatched to the worker.
    let proto_a = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("Timed out waiting for queued task dispatch")
        .expect("Stream ended")
        .expect("Stream error");

    assert_eq!(proto_a.task_id, "queued-task");
    assert_eq!(coordinator.pending_count().await, 0);
    assert_eq!(coordinator.inflight_count(), 1);
}

/// Test: Report a failed result and verify the outcome propagates correctly.
#[tokio::test]
async fn report_failed_result() {
    let (addr, coordinator, mut result_rx) = start_server().await;
    let mut client = connect_client(addr).await;

    // Register.
    let _resp = client
        .register(Request::new(make_proto_register("fail-worker", 4)))
        .await
        .unwrap();

    // Submit and receive a task (we need an assignment_id).
    let spec = TaskSpec {
        task_type: TaskType::Bash,
        script: "exit 1".into(),
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
        task_id: "fail-task".into(),
        attempt: 0,
        logical_date_epoch_ms: Utc::now().timestamp_millis(),
        environment: "test".into(),
        params: HashMap::new(),
    };
    let assignment = coordinator.create_assignment("dag1", "run1", "fail-task", 0, spec, ctx, 300);
    let aid = assignment.assignment_id.clone();
    coordinator.submit_task(assignment, "default").await;

    // Report failure.
    let failed_result = proto::TaskResult {
        assignment_id: aid.clone(),
        worker_id: "fail-worker".into(),
        dag_id: "dag1".into(),
        run_id: "run1".into(),
        task_id: "fail-task".into(),
        attempt: 0,
        outcome: 2, // Failed
        exit_code: 1,
        duration_ms: 50,
        xcom_json: String::new(),
        error: "Process exited with code 1".into(),
        metrics: HashMap::from([("rows_processed".into(), 0.0)]),
    };

    let ack = client
        .report_result(Request::new(failed_result))
        .await
        .unwrap()
        .into_inner();

    assert!(ack.success);

    // Verify the scheduler receives the failed result.
    let forwarded = timeout(Duration::from_secs(2), result_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(forwarded.outcome, TaskOutcome::Failed);
    assert_eq!(forwarded.exit_code, 1);
    assert_eq!(forwarded.error, "Process exited with code 1");
    assert_eq!(forwarded.metrics["rows_processed"], 0.0);
}

/// Test: Proto type conversion fidelity — a TaskAssignment with all fields
/// populated survives a round trip through the gRPC wire.
#[tokio::test]
async fn proto_conversion_fidelity_over_wire() {
    let (addr, coordinator, _result_rx) = start_server().await;
    let mut client = connect_client(addr).await;

    // Register.
    let resp = client
        .register(Request::new(make_proto_register("fidelity-worker", 4)))
        .await
        .unwrap();
    let mut stream = resp.into_inner();

    // Create a maximally populated assignment.
    let spec = TaskSpec {
        task_type: TaskType::Python,
        script: "import pandas as pd\ndf = pd.read_csv('data.csv')".into(),
        connection: "postgres-main".into(),
        query: "SELECT * FROM users WHERE active = true".into(),
        command: "/usr/bin/python3".into(),
        args: vec!["--verbose".into(), "--output=/tmp/result.json".into()],
        timeout_secs: 1800,
        resources: ResourceLimits {
            cpu_millicores: 2000,
            memory_mb: 4096,
        },
    };
    let ctx = TaskContext {
        dag_id: "etl-daily".into(),
        run_id: "run-20260323-001".into(),
        task_id: "transform-users".into(),
        attempt: 3,
        logical_date_epoch_ms: 1742688000000,
        environment: "production".into(),
        params: HashMap::from([
            ("batch_size".into(), "10000".into()),
            ("region".into(), "us-east-1".into()),
        ]),
    };
    let assignment = coordinator.create_assignment(
        "etl-daily",
        "run-20260323-001",
        "transform-users",
        3,
        spec,
        ctx,
        1800,
    );
    let original_id = assignment.assignment_id.clone();

    coordinator.submit_task(assignment, "default").await;

    // Receive the assignment over the wire.
    let received = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // Verify all fields survived serialisation.
    assert_eq!(received.assignment_id, original_id);
    assert_eq!(received.dag_id, "etl-daily");
    assert_eq!(received.run_id, "run-20260323-001");
    assert_eq!(received.task_id, "transform-users");
    assert_eq!(received.attempt, 3);
    assert_eq!(received.deadline_epoch_ms > 0, true);

    let spec = received.spec.unwrap();
    assert_eq!(spec.task_type, 1); // Python
    assert!(spec.script.contains("pandas"));
    assert_eq!(spec.connection, "postgres-main");
    assert_eq!(spec.query, "SELECT * FROM users WHERE active = true");
    assert_eq!(spec.command, "/usr/bin/python3");
    assert_eq!(spec.args, vec!["--verbose", "--output=/tmp/result.json"]);
    assert_eq!(spec.timeout_secs, 1800);
    let resources = spec.resources.unwrap();
    assert_eq!(resources.cpu_millicores, 2000);
    assert_eq!(resources.memory_mb, 4096);

    let ctx = received.context.unwrap();
    assert_eq!(ctx.dag_id, "etl-daily");
    assert_eq!(ctx.environment, "production");
    assert_eq!(ctx.attempt, 3);
    assert_eq!(ctx.logical_date_epoch_ms, 1742688000000);
    assert_eq!(ctx.params.get("batch_size").unwrap(), "10000");
    assert_eq!(ctx.params.get("region").unwrap(), "us-east-1");
}

/// Test: Multiple heartbeats in rapid succession all get acked.
#[tokio::test]
async fn rapid_heartbeat_burst() {
    let (addr, _coordinator, _result_rx) = start_server().await;
    let mut client = connect_client(addr).await;

    // Register.
    let _resp = client
        .register(Request::new(make_proto_register("burst-worker", 4)))
        .await
        .unwrap();

    let (tx, rx) = mpsc::unbounded_channel::<proto::WorkerHeartbeat>();
    let outbound = UnboundedReceiverStream::new(rx);

    let response = client.heartbeat(Request::new(outbound)).await.unwrap();
    let mut directives = response.into_inner();

    // Send 10 heartbeats in rapid succession.
    for i in 0..10 {
        tx.send(make_proto_heartbeat("burst-worker", i)).unwrap();
    }

    // Should receive 10 acks.
    let mut ack_count = 0;
    for _ in 0..10 {
        if let Ok(Some(Ok(d))) = timeout(Duration::from_secs(5), directives.next()).await {
            match d.directive {
                Some(proto::coordinator_directive::Directive::Ack(_)) => ack_count += 1,
                _ => panic!("Expected HeartbeatAck"),
            }
        }
    }

    assert_eq!(ack_count, 10);
}

/// Test: Empty log stream returns success ack with zero count.
#[tokio::test]
async fn empty_log_stream() {
    let (addr, _coordinator, _result_rx) = start_server().await;
    let mut client = connect_client(addr).await;

    // Send an empty stream.
    let stream = tokio_stream::empty::<proto::TaskLogEntry>();
    let ack = client
        .stream_logs(Request::new(stream))
        .await
        .expect("StreamLogs RPC failed")
        .into_inner();

    assert!(ack.success);
    assert!(ack.message.contains("0"));
}

/// Test: Cluster status reports task counts after submit + result.
#[tokio::test]
async fn cluster_status_reflects_inflight_tasks() {
    let (addr, coordinator, mut result_rx) = start_server().await;
    let mut client = connect_client(addr).await;

    // Register.
    let resp = client
        .register(Request::new(make_proto_register("status-worker", 4)))
        .await
        .unwrap();
    let mut _stream = resp.into_inner();

    // Submit 3 tasks.
    let mut assignment_ids = Vec::new();
    for i in 0..3 {
        let task_id = format!("status-task-{}", i);
        let spec = TaskSpec {
            task_type: TaskType::Bash,
            script: format!("echo {}", i),
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
            task_id: task_id.clone(),
            attempt: 0,
            logical_date_epoch_ms: Utc::now().timestamp_millis(),
            environment: "test".into(),
            params: HashMap::new(),
        };
        let a = coordinator.create_assignment("dag1", "run1", &task_id, 0, spec, ctx, 300);
        assignment_ids.push(a.assignment_id.clone());
        coordinator.submit_task(a, "default").await;
    }

    // Check status shows 3 running tasks.
    let status = client
        .cluster_status(Request::new(proto::ClusterStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.running_tasks, 3);

    // Complete one task.
    let result = make_proto_result(&assignment_ids[0], "status-worker", "status-task-0", 1);
    client.report_result(Request::new(result)).await.unwrap();

    // Drain the scheduler result channel.
    let _ = timeout(Duration::from_secs(1), result_rx.recv()).await;

    // Check status shows 2 running tasks.
    let status2 = client
        .cluster_status(Request::new(proto::ClusterStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status2.running_tasks, 2);
}

/// Test: Two independent clients can both call cluster_status concurrently.
#[tokio::test]
async fn concurrent_cluster_status_queries() {
    let (addr, _coordinator, _result_rx) = start_server().await;

    let mut client1 = connect_client(addr).await;
    let mut client2 = connect_client(addr).await;

    // Fire both requests concurrently.
    let (r1, r2) = tokio::join!(
        client1.cluster_status(Request::new(proto::ClusterStatusRequest {})),
        client2.cluster_status(Request::new(proto::ClusterStatusRequest {})),
    );

    let s1 = r1.unwrap().into_inner();
    let s2 = r2.unwrap().into_inner();

    // Both should return valid health status.
    assert!(s1.health > 0);
    assert!(s2.health > 0);
    assert_eq!(s1.health, s2.health);
}
