//! Conversion functions between generated protobuf types and hand-written Rust types.
//!
//! The generated types in `crate::generated::conduit.distributed` use `i32` for enums,
//! `Option<T>` for nested messages, and a `oneof` pattern for `CoordinatorDirective`.
//! The local types in `crate::proto_types` use rich Rust enums and non-optional fields.
//!
//! This module bridges the two so the gRPC server and client can delegate to the
//! existing coordinator/worker logic without leaking protobuf details.

use crate::proto_types as local;

// Use the canonical generated types from the crate root.
use crate::generated_proto as proto;

// ─── RegisterRequest ──────────────────────────────────────────────────────

pub fn register_request_from_proto(p: &proto::RegisterRequest) -> local::RegisterRequest {
    local::RegisterRequest {
        worker_id: p.worker_id.clone(),
        hostname: p.hostname.clone(),
        capacity: p.capacity,
        pool_affinity: p.pool_affinity.clone(),
        labels: p.labels.clone(),
        version: p.version.clone(),
        health_port: p.health_port,
    }
}

pub fn register_request_to_proto(l: &local::RegisterRequest) -> proto::RegisterRequest {
    proto::RegisterRequest {
        worker_id: l.worker_id.clone(),
        hostname: l.hostname.clone(),
        capacity: l.capacity,
        pool_affinity: l.pool_affinity.clone(),
        labels: l.labels.clone(),
        version: l.version.clone(),
        health_port: l.health_port,
    }
}

// ─── TaskSpec ─────────────────────────────────────────────────────────────

fn task_type_to_i32(tt: local::TaskType) -> i32 {
    tt as i32
}

fn task_type_from_i32(v: i32) -> local::TaskType {
    match v {
        1 => local::TaskType::Python,
        2 => local::TaskType::Bash,
        3 => local::TaskType::Sql,
        4 => local::TaskType::Sensor,
        5 => local::TaskType::Executable,
        _ => local::TaskType::Unspecified,
    }
}

fn resource_limits_from_proto(p: &proto::ResourceLimits) -> local::ResourceLimits {
    local::ResourceLimits {
        cpu_millicores: p.cpu_millicores,
        memory_mb: p.memory_mb,
    }
}

fn resource_limits_to_proto(l: &local::ResourceLimits) -> proto::ResourceLimits {
    proto::ResourceLimits {
        cpu_millicores: l.cpu_millicores,
        memory_mb: l.memory_mb,
    }
}

fn task_spec_from_proto(p: &proto::TaskSpec) -> local::TaskSpec {
    local::TaskSpec {
        task_type: task_type_from_i32(p.task_type),
        script: p.script.clone(),
        connection: p.connection.clone(),
        query: p.query.clone(),
        command: p.command.clone(),
        args: p.args.clone(),
        timeout_secs: p.timeout_secs,
        resources: p
            .resources
            .as_ref()
            .map(resource_limits_from_proto)
            .unwrap_or_default(),
    }
}

fn task_spec_to_proto(l: &local::TaskSpec) -> proto::TaskSpec {
    proto::TaskSpec {
        task_type: task_type_to_i32(l.task_type),
        script: l.script.clone(),
        connection: l.connection.clone(),
        query: l.query.clone(),
        command: l.command.clone(),
        args: l.args.clone(),
        timeout_secs: l.timeout_secs,
        resources: Some(resource_limits_to_proto(&l.resources)),
    }
}

// ─── TaskContext ──────────────────────────────────────────────────────────

fn task_context_from_proto(p: &proto::TaskContext) -> local::TaskContext {
    local::TaskContext {
        dag_id: p.dag_id.clone(),
        run_id: p.run_id.clone(),
        task_id: p.task_id.clone(),
        attempt: p.attempt,
        logical_date_epoch_ms: p.logical_date_epoch_ms,
        environment: p.environment.clone(),
        params: p.params.clone(),
    }
}

fn task_context_to_proto(l: &local::TaskContext) -> proto::TaskContext {
    proto::TaskContext {
        dag_id: l.dag_id.clone(),
        run_id: l.run_id.clone(),
        task_id: l.task_id.clone(),
        attempt: l.attempt,
        logical_date_epoch_ms: l.logical_date_epoch_ms,
        environment: l.environment.clone(),
        params: l.params.clone(),
    }
}

// ─── TaskAssignment ──────────────────────────────────────────────────────

pub fn task_assignment_from_proto(p: &proto::TaskAssignment) -> local::TaskAssignment {
    local::TaskAssignment {
        assignment_id: p.assignment_id.clone(),
        dag_id: p.dag_id.clone(),
        run_id: p.run_id.clone(),
        task_id: p.task_id.clone(),
        attempt: p.attempt,
        spec: p
            .spec
            .as_ref()
            .map(task_spec_from_proto)
            .unwrap_or_else(|| local::TaskSpec {
                task_type: local::TaskType::Unspecified,
                script: String::new(),
                connection: String::new(),
                query: String::new(),
                command: String::new(),
                args: vec![],
                timeout_secs: 0,
                resources: local::ResourceLimits::default(),
            }),
        context: p
            .context
            .as_ref()
            .map(task_context_from_proto)
            .unwrap_or_else(|| local::TaskContext {
                dag_id: p.dag_id.clone(),
                run_id: p.run_id.clone(),
                task_id: p.task_id.clone(),
                attempt: p.attempt,
                logical_date_epoch_ms: 0,
                environment: String::new(),
                params: Default::default(),
            }),
        deadline_epoch_ms: p.deadline_epoch_ms,
    }
}

pub fn task_assignment_to_proto(l: &local::TaskAssignment) -> proto::TaskAssignment {
    proto::TaskAssignment {
        assignment_id: l.assignment_id.clone(),
        dag_id: l.dag_id.clone(),
        run_id: l.run_id.clone(),
        task_id: l.task_id.clone(),
        attempt: l.attempt,
        spec: Some(task_spec_to_proto(&l.spec)),
        context: Some(task_context_to_proto(&l.context)),
        deadline_epoch_ms: l.deadline_epoch_ms,
    }
}

// ─── TaskResult ──────────────────────────────────────────────────────────

fn task_outcome_to_i32(o: local::TaskOutcome) -> i32 {
    o as i32
}

fn task_outcome_from_i32(v: i32) -> local::TaskOutcome {
    match v {
        1 => local::TaskOutcome::Success,
        2 => local::TaskOutcome::Failed,
        3 => local::TaskOutcome::Retry,
        4 => local::TaskOutcome::Skipped,
        _ => local::TaskOutcome::Unspecified,
    }
}

pub fn task_result_from_proto(p: &proto::TaskResult) -> local::TaskResult {
    local::TaskResult {
        assignment_id: p.assignment_id.clone(),
        worker_id: p.worker_id.clone(),
        dag_id: p.dag_id.clone(),
        run_id: p.run_id.clone(),
        task_id: p.task_id.clone(),
        attempt: p.attempt,
        outcome: task_outcome_from_i32(p.outcome),
        exit_code: p.exit_code,
        duration_ms: p.duration_ms,
        xcom_json: p.xcom_json.clone(),
        error: p.error.clone(),
        metrics: p.metrics.clone(),
    }
}

pub fn task_result_to_proto(l: &local::TaskResult) -> proto::TaskResult {
    proto::TaskResult {
        assignment_id: l.assignment_id.clone(),
        worker_id: l.worker_id.clone(),
        dag_id: l.dag_id.clone(),
        run_id: l.run_id.clone(),
        task_id: l.task_id.clone(),
        attempt: l.attempt,
        outcome: task_outcome_to_i32(l.outcome),
        exit_code: l.exit_code,
        duration_ms: l.duration_ms,
        xcom_json: l.xcom_json.clone(),
        error: l.error.clone(),
        metrics: l.metrics.clone(),
    }
}

// ─── WorkerHeartbeat ─────────────────────────────────────────────────────

pub fn heartbeat_from_proto(p: &proto::WorkerHeartbeat) -> local::WorkerHeartbeat {
    local::WorkerHeartbeat {
        worker_id: p.worker_id.clone(),
        active_tasks: p.active_tasks,
        cpu_percent: p.cpu_percent,
        memory_percent: p.memory_percent,
        disk_percent: p.disk_percent,
        running_assignments: p.running_assignments.clone(),
        timestamp_ms: p.timestamp_ms,
    }
}

pub fn heartbeat_to_proto(l: &local::WorkerHeartbeat) -> proto::WorkerHeartbeat {
    proto::WorkerHeartbeat {
        worker_id: l.worker_id.clone(),
        active_tasks: l.active_tasks,
        cpu_percent: l.cpu_percent,
        memory_percent: l.memory_percent,
        disk_percent: l.disk_percent,
        running_assignments: l.running_assignments.clone(),
        timestamp_ms: l.timestamp_ms,
    }
}

// ─── CoordinatorDirective ────────────────────────────────────────────────

pub fn directive_to_proto(l: &local::CoordinatorDirective) -> proto::CoordinatorDirective {
    use proto::coordinator_directive::Directive;

    let directive = match l {
        local::CoordinatorDirective::CancelTask {
            assignment_id,
            reason,
        } => Directive::CancelTask(proto::CancelTask {
            assignment_id: assignment_id.clone(),
            reason: reason.clone(),
        }),
        local::CoordinatorDirective::Drain {
            reason,
            grace_period_secs,
        } => Directive::Drain(proto::DrainWorker {
            reason: reason.clone(),
            grace_period_secs: *grace_period_secs,
        }),
        local::CoordinatorDirective::HeartbeatAck { timestamp_ms } => {
            Directive::Ack(proto::HeartbeatAck {
                timestamp_ms: *timestamp_ms,
            })
        }
    };

    proto::CoordinatorDirective {
        directive: Some(directive),
    }
}

pub fn directive_from_proto(p: &proto::CoordinatorDirective) -> local::CoordinatorDirective {
    use proto::coordinator_directive::Directive;

    match &p.directive {
        Some(Directive::CancelTask(ct)) => local::CoordinatorDirective::CancelTask {
            assignment_id: ct.assignment_id.clone(),
            reason: ct.reason.clone(),
        },
        Some(Directive::Drain(d)) => local::CoordinatorDirective::Drain {
            reason: d.reason.clone(),
            grace_period_secs: d.grace_period_secs,
        },
        Some(Directive::Ack(ack)) => local::CoordinatorDirective::HeartbeatAck {
            timestamp_ms: ack.timestamp_ms,
        },
        None => local::CoordinatorDirective::HeartbeatAck {
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
        },
    }
}

// ─── TaskLogEntry ────────────────────────────────────────────────────────

fn log_level_to_i32(l: local::LogLevel) -> i32 {
    l as i32
}

fn log_level_from_i32(v: i32) -> local::LogLevel {
    match v {
        1 => local::LogLevel::Debug,
        2 => local::LogLevel::Info,
        3 => local::LogLevel::Warn,
        4 => local::LogLevel::Error,
        _ => local::LogLevel::Unspecified,
    }
}

pub fn log_entry_from_proto(p: &proto::TaskLogEntry) -> local::TaskLogEntry {
    local::TaskLogEntry {
        assignment_id: p.assignment_id.clone(),
        worker_id: p.worker_id.clone(),
        level: log_level_from_i32(p.level),
        message: p.message.clone(),
        timestamp_ms: p.timestamp_ms,
        metadata_json: p.metadata_json.clone(),
    }
}

pub fn log_entry_to_proto(l: &local::TaskLogEntry) -> proto::TaskLogEntry {
    proto::TaskLogEntry {
        assignment_id: l.assignment_id.clone(),
        worker_id: l.worker_id.clone(),
        level: log_level_to_i32(l.level),
        message: l.message.clone(),
        timestamp_ms: l.timestamp_ms,
        metadata_json: l.metadata_json.clone(),
    }
}

// ─── ClusterStatus ───────────────────────────────────────────────────────

fn cluster_health_to_i32(h: local::ClusterHealth) -> i32 {
    h as i32
}

fn worker_state_to_i32(s: local::WorkerState) -> i32 {
    s as i32
}

fn worker_state_from_i32(v: i32) -> local::WorkerState {
    match v {
        1 => local::WorkerState::Active,
        2 => local::WorkerState::Draining,
        3 => local::WorkerState::Disconnected,
        4 => local::WorkerState::Dead,
        _ => local::WorkerState::Unspecified,
    }
}

fn cluster_health_from_i32(v: i32) -> local::ClusterHealth {
    match v {
        1 => local::ClusterHealth::Healthy,
        2 => local::ClusterHealth::Degraded,
        3 => local::ClusterHealth::Unhealthy,
        _ => local::ClusterHealth::Unspecified,
    }
}

fn worker_status_to_proto(l: &local::WorkerStatus) -> proto::WorkerStatus {
    proto::WorkerStatus {
        worker_id: l.worker_id.clone(),
        hostname: l.hostname.clone(),
        state: worker_state_to_i32(l.state),
        capacity: l.capacity,
        active_tasks: l.active_tasks,
        pool_affinity: l.pool_affinity.clone(),
        last_heartbeat_ms: l.last_heartbeat_ms,
        cpu_percent: l.cpu_percent,
        memory_percent: l.memory_percent,
        labels: l.labels.clone(),
        registered_at_ms: l.registered_at_ms,
        tasks_completed: l.tasks_completed,
        tasks_failed: l.tasks_failed,
    }
}

fn worker_status_from_proto(p: &proto::WorkerStatus) -> local::WorkerStatus {
    local::WorkerStatus {
        worker_id: p.worker_id.clone(),
        hostname: p.hostname.clone(),
        state: worker_state_from_i32(p.state),
        capacity: p.capacity,
        active_tasks: p.active_tasks,
        pool_affinity: p.pool_affinity.clone(),
        last_heartbeat_ms: p.last_heartbeat_ms,
        cpu_percent: p.cpu_percent,
        memory_percent: p.memory_percent,
        labels: p.labels.clone(),
        registered_at_ms: p.registered_at_ms,
        tasks_completed: p.tasks_completed,
        tasks_failed: p.tasks_failed,
    }
}

pub fn cluster_status_to_proto(l: &local::ClusterStatusResponse) -> proto::ClusterStatusResponse {
    proto::ClusterStatusResponse {
        health: cluster_health_to_i32(l.health),
        workers: l.workers.iter().map(worker_status_to_proto).collect(),
        active_runs: l.active_runs,
        running_tasks: l.running_tasks,
        queued_tasks: l.queued_tasks,
        uptime_secs: l.uptime_secs,
    }
}

pub fn cluster_status_from_proto(p: &proto::ClusterStatusResponse) -> local::ClusterStatusResponse {
    local::ClusterStatusResponse {
        health: cluster_health_from_i32(p.health),
        workers: p.workers.iter().map(worker_status_from_proto).collect(),
        active_runs: p.active_runs,
        running_tasks: p.running_tasks,
        queued_tasks: p.queued_tasks,
        uptime_secs: p.uptime_secs,
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn roundtrip_register_request() {
        let local_req = local::RegisterRequest {
            worker_id: "w1".into(),
            hostname: "host1".into(),
            capacity: 8,
            pool_affinity: vec!["default".into(), "gpu".into()],
            labels: HashMap::from([("region".into(), "us-east".into())]),
            version: "0.1.0".into(),
            health_port: 9090,
        };

        let proto_req = register_request_to_proto(&local_req);
        let back = register_request_from_proto(&proto_req);

        assert_eq!(back.worker_id, local_req.worker_id);
        assert_eq!(back.capacity, local_req.capacity);
        assert_eq!(back.pool_affinity, local_req.pool_affinity);
        assert_eq!(back.labels, local_req.labels);
    }

    #[test]
    fn roundtrip_task_assignment() {
        let local_a = local::TaskAssignment {
            assignment_id: "a1".into(),
            dag_id: "dag1".into(),
            run_id: "run1".into(),
            task_id: "task1".into(),
            attempt: 2,
            spec: local::TaskSpec {
                task_type: local::TaskType::Python,
                script: "print('hi')".into(),
                connection: String::new(),
                query: String::new(),
                command: String::new(),
                args: vec!["--flag".into()],
                timeout_secs: 300,
                resources: local::ResourceLimits {
                    cpu_millicores: 1000,
                    memory_mb: 512,
                },
            },
            context: local::TaskContext {
                dag_id: "dag1".into(),
                run_id: "run1".into(),
                task_id: "task1".into(),
                attempt: 2,
                logical_date_epoch_ms: 1234567890,
                environment: "prod".into(),
                params: HashMap::from([("key".into(), "val".into())]),
            },
            deadline_epoch_ms: 9999999999,
        };

        let proto_a = task_assignment_to_proto(&local_a);
        let back = task_assignment_from_proto(&proto_a);

        assert_eq!(back.assignment_id, "a1");
        assert_eq!(back.spec.task_type, local::TaskType::Python);
        assert_eq!(back.spec.resources.cpu_millicores, 1000);
        assert_eq!(back.context.environment, "prod");
    }

    #[test]
    fn roundtrip_task_result() {
        let local_r = local::TaskResult {
            assignment_id: "a1".into(),
            worker_id: "w1".into(),
            dag_id: "dag1".into(),
            run_id: "run1".into(),
            task_id: "task1".into(),
            attempt: 0,
            outcome: local::TaskOutcome::Success,
            exit_code: 0,
            duration_ms: 1500,
            xcom_json: r#"{"rows": 42}"#.into(),
            error: String::new(),
            metrics: HashMap::from([("row_count".into(), 42.0)]),
        };

        let proto_r = task_result_to_proto(&local_r);
        assert_eq!(proto_r.outcome, 1); // Success = 1
        let back = task_result_from_proto(&proto_r);
        assert_eq!(back.outcome, local::TaskOutcome::Success);
        assert_eq!(back.metrics["row_count"], 42.0);
    }

    #[test]
    fn roundtrip_directives() {
        let cases = vec![
            local::CoordinatorDirective::HeartbeatAck {
                timestamp_ms: 123456,
            },
            local::CoordinatorDirective::CancelTask {
                assignment_id: "a1".into(),
                reason: "timeout".into(),
            },
            local::CoordinatorDirective::Drain {
                reason: "maintenance".into(),
                grace_period_secs: 30,
            },
        ];

        for original in &cases {
            let proto_d = directive_to_proto(original);
            let back = directive_from_proto(&proto_d);

            match (original, &back) {
                (
                    local::CoordinatorDirective::HeartbeatAck { timestamp_ms: a },
                    local::CoordinatorDirective::HeartbeatAck { timestamp_ms: b },
                ) => assert_eq!(a, b),
                (
                    local::CoordinatorDirective::CancelTask {
                        assignment_id: a, ..
                    },
                    local::CoordinatorDirective::CancelTask {
                        assignment_id: b, ..
                    },
                ) => assert_eq!(a, b),
                (
                    local::CoordinatorDirective::Drain {
                        grace_period_secs: a,
                        ..
                    },
                    local::CoordinatorDirective::Drain {
                        grace_period_secs: b,
                        ..
                    },
                ) => assert_eq!(a, b),
                _ => panic!("Directive variant mismatch"),
            }
        }
    }

    #[test]
    fn roundtrip_heartbeat() {
        let local_hb = local::WorkerHeartbeat {
            worker_id: "w1".into(),
            active_tasks: 3,
            cpu_percent: 75.5,
            memory_percent: 60.0,
            disk_percent: 40.0,
            running_assignments: vec!["a1".into(), "a2".into()],
            timestamp_ms: 999888777,
        };

        let proto_hb = heartbeat_to_proto(&local_hb);
        let back = heartbeat_from_proto(&proto_hb);
        assert_eq!(back.worker_id, "w1");
        assert_eq!(back.active_tasks, 3);
        assert!((back.cpu_percent - 75.5).abs() < f64::EPSILON);
    }

    #[test]
    fn roundtrip_log_entry() {
        let local_e = local::TaskLogEntry {
            assignment_id: "a1".into(),
            worker_id: "w1".into(),
            level: local::LogLevel::Warn,
            message: "disk almost full".into(),
            timestamp_ms: 111222333,
            metadata_json: "{}".into(),
        };

        let proto_e = log_entry_to_proto(&local_e);
        assert_eq!(proto_e.level, 3); // Warn = 3
        let back = log_entry_from_proto(&proto_e);
        assert_eq!(back.level, local::LogLevel::Warn);
        assert_eq!(back.message, "disk almost full");
    }

    #[test]
    fn roundtrip_cluster_status() {
        let local_cs = local::ClusterStatusResponse {
            health: local::ClusterHealth::Healthy,
            workers: vec![local::WorkerStatus {
                worker_id: "w1".into(),
                hostname: "host1".into(),
                state: local::WorkerState::Active,
                capacity: 8,
                active_tasks: 3,
                pool_affinity: vec!["default".into()],
                last_heartbeat_ms: 123456789,
                cpu_percent: 50.0,
                memory_percent: 70.0,
                labels: HashMap::new(),
                registered_at_ms: 100000000,
                tasks_completed: 42,
                tasks_failed: 2,
            }],
            active_runs: 5,
            running_tasks: 3,
            queued_tasks: 10,
            uptime_secs: 3600,
        };

        let proto_cs = cluster_status_to_proto(&local_cs);
        assert_eq!(proto_cs.health, 1); // Healthy = 1
        assert_eq!(proto_cs.workers.len(), 1);

        let back = cluster_status_from_proto(&proto_cs);
        assert_eq!(back.health, local::ClusterHealth::Healthy);
        assert_eq!(back.workers[0].state, local::WorkerState::Active);
        assert_eq!(back.workers[0].tasks_completed, 42);
    }
}
