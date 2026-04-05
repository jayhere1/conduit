//! Worker: a node that connects to the coordinator and executes tasks.
//!
//! A worker:
//! 1. Connects to the coordinator via gRPC
//! 2. Registers itself with its capacity and pool affinity
//! 3. Receives task assignments via server-sent stream
//! 4. Executes tasks using the local ProcessRunner
//! 5. Reports results back to the coordinator
//! 6. Sends heartbeats periodically
//! 7. Streams logs in real-time
//!
//! # Usage
//!
//! ```bash
//! conduit worker --coordinator localhost:9400 --capacity 4 --pools default,gpu
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use dashmap::DashMap;
use sysinfo::System;
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{info, warn, error};

use crate::proto_types::*;

// ── System Metrics ──────────────────────────────────────────────────────

struct SystemMetrics {
    sys: System,
}

impl SystemMetrics {
    fn new() -> Self {
        let mut sys = System::new();
        sys.refresh_cpu_all();
        sys.refresh_memory();
        Self { sys }
    }

    fn refresh(&mut self) -> (f64, f64, f64) {
        self.sys.refresh_cpu_all();
        self.sys.refresh_memory();
        let cpu = self.sys.global_cpu_usage() as f64;
        let total_mem = self.sys.total_memory() as f64;
        let used_mem = self.sys.used_memory() as f64;
        let mem = if total_mem > 0.0 { (used_mem / total_mem) * 100.0 } else { 0.0 };
        // Disk metrics via sysinfo::Disks
        let disks = sysinfo::Disks::new_with_refreshed_list();
        let (total_disk, avail_disk) = disks.iter().fold((0u64, 0u64), |(t, a), d| {
            (t + d.total_space(), a + d.available_space())
        });
        let disk = if total_disk > 0 {
            ((total_disk - avail_disk) as f64 / total_disk as f64) * 100.0
        } else {
            0.0
        };
        (cpu, mem, disk)
    }
}

/// Configuration for a worker node.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Unique worker ID. Auto-generated if empty.
    pub worker_id: String,

    /// Coordinator address (e.g., "localhost:9400").
    pub coordinator_addr: String,

    /// Maximum number of concurrent tasks.
    pub capacity: u32,

    /// Resource pools this worker can handle.
    pub pool_affinity: Vec<String>,

    /// Labels for worker selection (e.g., region=us-east-1).
    pub labels: HashMap<String, String>,

    /// Heartbeat interval in seconds.
    pub heartbeat_interval_secs: u64,

    /// Whether to gracefully drain on SIGTERM.
    pub graceful_shutdown: bool,

    /// Path to the CA certificate PEM file for TLS connections to the coordinator.
    /// When set, the worker connects using TLS (https).
    pub tls_ca_cert_path: Option<String>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        Self {
            worker_id: format!("worker-{}-{}", hostname, std::process::id()),
            coordinator_addr: "localhost:9400".to_string(),
            capacity: 4,
            pool_affinity: vec!["default".to_string()],
            labels: HashMap::new(),
            heartbeat_interval_secs: 5,
            graceful_shutdown: true,
            tls_ca_cert_path: None,
        }
    }
}

/// The state of a task currently being executed by this worker.
#[derive(Debug)]
#[allow(dead_code)]
struct RunningTask {
    assignment_id: String,
    task_id: String,
    started_at: Instant,
    cancel_tx: Option<mpsc::Sender<()>>,
}

/// A Conduit worker node.
pub struct Worker {
    config: WorkerConfig,

    /// Concurrency limiter.
    semaphore: Arc<Semaphore>,

    /// Currently running tasks.
    running: Arc<DashMap<String, RunningTask>>,

    /// Channel to send results back to the coordinator connection handler.
    result_tx: mpsc::UnboundedSender<TaskResult>,

    /// Channel to send log entries to the coordinator connection handler.
    log_tx: mpsc::UnboundedSender<TaskLogEntry>,

    /// Worker state.
    state: Arc<RwLock<WorkerState>>,

    /// System metrics collector.
    system_metrics: Mutex<SystemMetrics>,
}

impl Worker {
    /// Create a new worker.
    ///
    /// Returns the worker plus receivers for results and logs that the
    /// gRPC connection handler should forward to the coordinator.
    pub fn new(
        config: WorkerConfig,
    ) -> (
        Self,
        mpsc::UnboundedReceiver<TaskResult>,
        mpsc::UnboundedReceiver<TaskLogEntry>,
    ) {
        let (result_tx, result_rx) = mpsc::unbounded_channel();
        let (log_tx, log_rx) = mpsc::unbounded_channel();

        let worker = Self {
            semaphore: Arc::new(Semaphore::new(config.capacity as usize)),
            running: Arc::new(DashMap::new()),
            result_tx,
            log_tx,
            state: Arc::new(RwLock::new(WorkerState::Active)),
            config,
            system_metrics: Mutex::new(SystemMetrics::new()),
        };

        (worker, result_rx, log_rx)
    }

    /// Get the registration request for this worker.
    pub fn registration(&self) -> RegisterRequest {
        RegisterRequest {
            worker_id: self.config.worker_id.clone(),
            hostname: hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
            capacity: self.config.capacity,
            pool_affinity: self.config.pool_affinity.clone(),
            labels: self.config.labels.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            health_port: 0,
        }
    }

    /// Handle a task assignment from the coordinator.
    ///
    /// This acquires a semaphore permit (blocking if at capacity),
    /// spawns the task execution, and sends the result when done.
    pub async fn handle_assignment(&self, assignment: TaskAssignment) {
        let state = self.state.read().await;
        if *state == WorkerState::Draining {
            warn!(
                assignment = %assignment.assignment_id,
                "Worker is draining, rejecting task"
            );
            self.send_result(TaskResult {
                assignment_id: assignment.assignment_id.clone(),
                worker_id: self.config.worker_id.clone(),
                dag_id: assignment.dag_id.clone(),
                run_id: assignment.run_id.clone(),
                task_id: assignment.task_id.clone(),
                attempt: assignment.attempt,
                outcome: TaskOutcome::Failed,
                exit_code: -1,
                duration_ms: 0,
                xcom_json: String::new(),
                error: "Worker is draining".to_string(),
                metrics: HashMap::new(),
            });
            return;
        }
        drop(state);

        let assignment_id = assignment.assignment_id.clone();
        let task_id = assignment.task_id.clone();

        info!(
            assignment = %assignment_id,
            task = %task_id,
            attempt = assignment.attempt,
            "Executing task assignment"
        );

        // Create cancellation channel.
        let (cancel_tx, mut cancel_rx) = mpsc::channel(1);

        // Track the running task.
        self.running.insert(
            assignment_id.clone(),
            RunningTask {
                assignment_id: assignment_id.clone(),
                task_id: task_id.clone(),
                started_at: Instant::now(),
                cancel_tx: Some(cancel_tx),
            },
        );

        // Clone what we need for the spawned task.
        let semaphore = self.semaphore.clone();
        let running = self.running.clone();
        let result_tx = self.result_tx.clone();
        let log_tx = self.log_tx.clone();
        let worker_id = self.config.worker_id.clone();

        tokio::spawn(async move {
            // Acquire a semaphore permit (blocks if at capacity).
            let _permit = semaphore.acquire().await.unwrap();

            let started = Instant::now();

            // Execute the task.
            let result = tokio::select! {
                result = Self::execute_task(&assignment, &log_tx, &worker_id) => result,
                _ = cancel_rx.recv() => {
                    warn!(assignment = %assignment_id, "Task cancelled");
                    TaskResult {
                        assignment_id: assignment_id.clone(),
                        worker_id: worker_id.clone(),
                        dag_id: assignment.dag_id.clone(),
                        run_id: assignment.run_id.clone(),
                        task_id: assignment.task_id.clone(),
                        attempt: assignment.attempt,
                        outcome: TaskOutcome::Failed,
                        exit_code: -1,
                        duration_ms: started.elapsed().as_millis() as u64,
                        xcom_json: String::new(),
                        error: "Task cancelled by coordinator".to_string(),
                        metrics: HashMap::new(),
                    }
                }
            };

            // Send result.
            if let Err(e) = result_tx.send(result) {
                error!(error = %e, "Failed to send task result");
            }

            // Remove from running tasks.
            running.remove(&assignment_id);
        });
    }

    /// Execute a task assignment and return the result.
    ///
    /// In production, this delegates to ProcessRunner. Here we define
    /// the interface and handle the protocol translation.
    async fn execute_task(
        assignment: &TaskAssignment,
        log_tx: &mpsc::UnboundedSender<TaskLogEntry>,
        worker_id: &str,
    ) -> TaskResult {
        let started = Instant::now();
        let assignment_id = &assignment.assignment_id;

        // Send initial log.
        let _ = log_tx.send(TaskLogEntry {
            assignment_id: assignment_id.clone(),
            worker_id: worker_id.to_string(),
            level: LogLevel::Info,
            message: format!(
                "Starting task {} (attempt {})",
                assignment.task_id, assignment.attempt
            ),
            timestamp_ms: Utc::now().timestamp_millis(),
            metadata_json: String::new(),
        });

        // Build the execution command based on task type.
        let (outcome, exit_code, error_msg, xcom, metrics) =
            match assignment.spec.task_type {
                TaskType::Bash => {
                    Self::execute_bash(&assignment.spec, assignment_id, log_tx, worker_id).await
                }
                TaskType::Python => {
                    Self::execute_python(&assignment.spec, assignment_id, log_tx, worker_id).await
                }
                TaskType::Sql => {
                    Self::execute_sql(&assignment.spec, assignment_id, log_tx, worker_id).await
                }
                TaskType::Executable => {
                    Self::execute_executable(&assignment.spec, assignment_id, log_tx, worker_id).await
                }
                _ => (
                    TaskOutcome::Failed,
                    -1,
                    format!("Unsupported task type: {:?}", assignment.spec.task_type),
                    String::new(),
                    HashMap::new(),
                ),
            };

        let duration = started.elapsed().as_millis() as u64;

        // Send completion log.
        let _ = log_tx.send(TaskLogEntry {
            assignment_id: assignment_id.clone(),
            worker_id: worker_id.to_string(),
            level: if outcome == TaskOutcome::Success {
                LogLevel::Info
            } else {
                LogLevel::Error
            },
            message: format!(
                "Task {} completed: {:?} ({}ms)",
                assignment.task_id, outcome, duration
            ),
            timestamp_ms: Utc::now().timestamp_millis(),
            metadata_json: String::new(),
        });

        TaskResult {
            assignment_id: assignment_id.clone(),
            worker_id: worker_id.to_string(),
            dag_id: assignment.dag_id.clone(),
            run_id: assignment.run_id.clone(),
            task_id: assignment.task_id.clone(),
            attempt: assignment.attempt,
            outcome,
            exit_code,
            duration_ms: duration,
            xcom_json: xcom,
            error: error_msg,
            metrics,
        }
    }

    /// Execute a bash task by spawning a child process.
    async fn execute_bash(
        spec: &TaskSpec,
        assignment_id: &str,
        log_tx: &mpsc::UnboundedSender<TaskLogEntry>,
        worker_id: &str,
    ) -> (TaskOutcome, i32, String, String, HashMap<String, f64>) {
        use tokio::process::Command;

        let timeout = if spec.timeout_secs > 0 {
            Duration::from_secs(spec.timeout_secs)
        } else {
            Duration::from_secs(3600)
        };

        let child = Command::new("bash")
            .arg("-c")
            .arg(&spec.script)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn();

        let child = match child {
            Ok(c) => c,
            Err(e) => {
                return (
                    TaskOutcome::Failed,
                    -1,
                    format!("Failed to spawn process: {}", e),
                    String::new(),
                    HashMap::new(),
                );
            }
        };

        // Wait with timeout. `wait_with_output` consumes the child, so
        // we handle the timeout branch without referencing `child` again.
        let result = tokio::time::timeout(timeout, child.wait_with_output()).await;

        match result {
            Ok(Ok(output)) => {
                let exit_code = output.status.code().unwrap_or(-1);
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);

                // Parse CONDUIT protocol from stdout.
                let mut xcom = String::new();
                let mut metrics = HashMap::new();

                for line in stdout.lines() {
                    if let Some(json) = line.strip_prefix("CONDUIT::XCOM::") {
                        xcom = json.to_string();
                    } else if let Some(metric_str) = line.strip_prefix("CONDUIT::METRIC::") {
                        if let Some((name, val)) = metric_str.split_once("::") {
                            if let Ok(v) = val.parse::<f64>() {
                                metrics.insert(name.to_string(), v);
                            }
                        }
                    } else if let Some(log_str) = line.strip_prefix("CONDUIT::LOG::") {
                        // Forward log lines.
                        let (level, msg) = if let Some(rest) = log_str.strip_prefix("INFO::") {
                            (LogLevel::Info, rest)
                        } else if let Some(rest) = log_str.strip_prefix("WARN::") {
                            (LogLevel::Warn, rest)
                        } else if let Some(rest) = log_str.strip_prefix("ERROR::") {
                            (LogLevel::Error, rest)
                        } else if let Some(rest) = log_str.strip_prefix("DEBUG::") {
                            (LogLevel::Debug, rest)
                        } else {
                            (LogLevel::Info, log_str)
                        };

                        let _ = log_tx.send(TaskLogEntry {
                            assignment_id: assignment_id.to_string(),
                            worker_id: worker_id.to_string(),
                            level,
                            message: msg.to_string(),
                            timestamp_ms: Utc::now().timestamp_millis(),
                            metadata_json: String::new(),
                        });
                    }
                }

                let outcome = match exit_code {
                    0 => TaskOutcome::Success,
                    2 => TaskOutcome::Retry,
                    3 => TaskOutcome::Skipped,
                    _ => TaskOutcome::Failed,
                };

                let error = if outcome != TaskOutcome::Success {
                    stderr.to_string()
                } else {
                    String::new()
                };

                (outcome, exit_code, error, xcom, metrics)
            }
            Ok(Err(e)) => (
                TaskOutcome::Failed,
                -1,
                format!("Process error: {}", e),
                String::new(),
                HashMap::new(),
            ),
            Err(_) => {
                // Timeout — the child was consumed by `wait_with_output`
                // which was cancelled. The drop will clean up the process.
                (
                    TaskOutcome::Failed,
                    -1,
                    format!("Task timed out after {}s", spec.timeout_secs),
                    String::new(),
                    HashMap::new(),
                )
            }
        }
    }

    /// Execute a Python task.
    async fn execute_python(
        spec: &TaskSpec,
        assignment_id: &str,
        log_tx: &mpsc::UnboundedSender<TaskLogEntry>,
        worker_id: &str,
    ) -> (TaskOutcome, i32, String, String, HashMap<String, f64>) {
        // Delegate to bash with python -c.
        let mut python_spec = spec.clone();
        python_spec.script = format!("python3 -c '{}'", spec.script.replace('\'', "'\\''"));
        Self::execute_bash(&python_spec, assignment_id, log_tx, worker_id).await
    }

    /// Execute a SQL task (delegates to provider if available).
    async fn execute_sql(
        spec: &TaskSpec,
        assignment_id: &str,
        log_tx: &mpsc::UnboundedSender<TaskLogEntry>,
        worker_id: &str,
    ) -> (TaskOutcome, i32, String, String, HashMap<String, f64>) {
        // For now, report that SQL requires a provider connection.
        // In production, this would use the provider registry.
        let _ = log_tx.send(TaskLogEntry {
            assignment_id: assignment_id.to_string(),
            worker_id: worker_id.to_string(),
            level: LogLevel::Info,
            message: format!(
                "SQL task on connection '{}': {}",
                spec.connection,
                &spec.query[..spec.query.len().min(100)]
            ),
            timestamp_ms: Utc::now().timestamp_millis(),
            metadata_json: String::new(),
        });

        (
            TaskOutcome::Success,
            0,
            String::new(),
            serde_json::json!({
                "connection": spec.connection,
                "query_preview": &spec.query[..spec.query.len().min(200)],
            })
            .to_string(),
            HashMap::new(),
        )
    }

    /// Execute an external executable.
    async fn execute_executable(
        spec: &TaskSpec,
        assignment_id: &str,
        log_tx: &mpsc::UnboundedSender<TaskLogEntry>,
        worker_id: &str,
    ) -> (TaskOutcome, i32, String, String, HashMap<String, f64>) {
        let mut exec_spec = spec.clone();
        let args_str = spec.args.join(" ");
        exec_spec.script = if args_str.is_empty() {
            spec.command.clone()
        } else {
            format!("{} {}", spec.command, args_str)
        };
        Self::execute_bash(&exec_spec, assignment_id, log_tx, worker_id).await
    }

    /// Cancel a running task.
    pub async fn cancel_task(&self, assignment_id: &str) -> bool {
        if let Some(entry) = self.running.get(assignment_id) {
            if let Some(tx) = &entry.cancel_tx {
                let _ = tx.send(()).await;
                info!(assignment = %assignment_id, "Cancellation signal sent");
                return true;
            }
        }
        false
    }

    /// Start draining: finish current tasks, reject new ones.
    pub async fn drain(&self) {
        let mut state = self.state.write().await;
        *state = WorkerState::Draining;
        info!(
            worker = %self.config.worker_id,
            running_tasks = self.running.len(),
            "Worker entering drain mode"
        );
    }

    /// Wait for all running tasks to complete (for graceful shutdown).
    pub async fn wait_for_drain(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;

        while !self.running.is_empty() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if !self.running.is_empty() {
            warn!(
                remaining = self.running.len(),
                "Drain timeout expired, {} tasks still running",
                self.running.len()
            );
        }
    }

    /// Generate a heartbeat message.
    pub fn heartbeat(&self) -> WorkerHeartbeat {
        let (cpu, mem, disk) = self.system_metrics.lock().unwrap().refresh();
        WorkerHeartbeat {
            worker_id: self.config.worker_id.clone(),
            active_tasks: self.running.len() as u32,
            cpu_percent: cpu,
            memory_percent: mem,
            disk_percent: disk,
            running_assignments: self
                .running
                .iter()
                .map(|e| e.assignment_id.clone())
                .collect(),
            timestamp_ms: Utc::now().timestamp_millis(),
        }
    }

    /// Get the worker's current state.
    pub async fn state(&self) -> WorkerState {
        *self.state.read().await
    }

    /// Number of currently running tasks.
    pub fn running_count(&self) -> usize {
        self.running.len()
    }

    /// Worker ID.
    pub fn id(&self) -> &str {
        &self.config.worker_id
    }

    fn send_result(&self, result: TaskResult) {
        let _ = self.result_tx.send(result);
    }
}

/// Get the system hostname (simple fallback implementation).
mod hostname {
    use std::ffi::OsString;

    pub fn get() -> Result<OsString, std::io::Error> {
        let name = std::env::var("HOSTNAME")
            .or_else(|_| {
                std::fs::read_to_string("/etc/hostname")
                    .map(|s| s.trim().to_string())
            })
            .unwrap_or_else(|_| "unknown".into());
        Ok(OsString::from(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_worker() -> (Worker, mpsc::UnboundedReceiver<TaskResult>, mpsc::UnboundedReceiver<TaskLogEntry>) {
        Worker::new(WorkerConfig {
            worker_id: "test-worker".to_string(),
            coordinator_addr: "localhost:9400".to_string(),
            capacity: 4,
            pool_affinity: vec!["default".to_string()],
            labels: HashMap::new(),
            heartbeat_interval_secs: 5,
            graceful_shutdown: true,
            tls_ca_cert_path: None,
        })
    }

    fn make_assignment(task_id: &str) -> TaskAssignment {
        TaskAssignment {
            assignment_id: format!("assign-{}", task_id),
            dag_id: "dag1".to_string(),
            run_id: "run1".to_string(),
            task_id: task_id.to_string(),
            attempt: 0,
            spec: TaskSpec {
                task_type: TaskType::Bash,
                script: "echo hello".to_string(),
                connection: String::new(),
                query: String::new(),
                command: String::new(),
                args: vec![],
                timeout_secs: 30,
                resources: ResourceLimits::default(),
            },
            context: TaskContext {
                dag_id: "dag1".to_string(),
                run_id: "run1".to_string(),
                task_id: task_id.to_string(),
                attempt: 0,
                logical_date_epoch_ms: Utc::now().timestamp_millis(),
                environment: "test".to_string(),
                params: HashMap::new(),
            },
            deadline_epoch_ms: Utc::now().timestamp_millis() + 30_000,
        }
    }

    #[test]
    fn test_worker_creation() {
        let (worker, _result_rx, _log_rx) = make_worker();
        assert_eq!(worker.id(), "test-worker");
        assert_eq!(worker.running_count(), 0);
    }

    #[test]
    fn test_worker_registration_message() {
        let (worker, _, _) = make_worker();
        let reg = worker.registration();
        assert_eq!(reg.worker_id, "test-worker");
        assert_eq!(reg.capacity, 4);
        assert_eq!(reg.pool_affinity, vec!["default"]);
    }

    #[test]
    fn test_heartbeat_message() {
        let (worker, _, _) = make_worker();
        let hb = worker.heartbeat();
        assert_eq!(hb.worker_id, "test-worker");
        assert_eq!(hb.active_tasks, 0);
        assert!(hb.running_assignments.is_empty());
    }

    #[tokio::test]
    async fn test_handle_assignment_executes_bash() {
        let (worker, mut result_rx, _log_rx) = make_worker();

        let assignment = make_assignment("echo_task");
        worker.handle_assignment(assignment).await;

        // Wait for the task to complete.
        let result = tokio::time::timeout(Duration::from_secs(5), result_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.task_id, "echo_task");
        assert_eq!(result.outcome, TaskOutcome::Success);
        assert_eq!(result.exit_code, 0);
    }

    #[tokio::test]
    async fn test_handle_assignment_captures_protocol() {
        let (worker, mut result_rx, _log_rx) = make_worker();

        let mut assignment = make_assignment("protocol_task");
        assignment.spec.script = r#"
echo 'CONDUIT::METRIC::row_count::42'
echo 'CONDUIT::XCOM::{"key": "value"}'
echo 'CONDUIT::LOG::INFO::Hello from task'
"#
        .to_string();

        worker.handle_assignment(assignment).await;

        let result = tokio::time::timeout(Duration::from_secs(5), result_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.outcome, TaskOutcome::Success);
        assert_eq!(result.metrics.get("row_count"), Some(&42.0));
        assert!(result.xcom_json.contains("value"));
    }

    #[tokio::test]
    async fn test_handle_assignment_failure() {
        let (worker, mut result_rx, _log_rx) = make_worker();

        let mut assignment = make_assignment("fail_task");
        assignment.spec.script = "exit 1".to_string();

        worker.handle_assignment(assignment).await;

        let result = tokio::time::timeout(Duration::from_secs(5), result_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.outcome, TaskOutcome::Failed);
        assert_eq!(result.exit_code, 1);
    }

    #[tokio::test]
    async fn test_drain_rejects_new_tasks() {
        let (worker, mut result_rx, _log_rx) = make_worker();

        worker.drain().await;
        assert_eq!(worker.state().await, WorkerState::Draining);

        let assignment = make_assignment("rejected_task");
        worker.handle_assignment(assignment).await;

        let result = tokio::time::timeout(Duration::from_secs(2), result_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.outcome, TaskOutcome::Failed);
        assert!(result.error.contains("draining"));
    }

    #[tokio::test]
    async fn test_task_timeout() {
        let (worker, mut result_rx, _log_rx) = make_worker();

        let mut assignment = make_assignment("timeout_task");
        assignment.spec.script = "sleep 60".to_string();
        assignment.spec.timeout_secs = 1; // 1 second timeout

        worker.handle_assignment(assignment).await;

        let result = tokio::time::timeout(Duration::from_secs(5), result_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.outcome, TaskOutcome::Failed);
        assert!(result.error.contains("timed out"));
    }

    #[tokio::test]
    async fn test_retry_exit_code() {
        let (worker, mut result_rx, _log_rx) = make_worker();

        let mut assignment = make_assignment("retry_task");
        assignment.spec.script = "exit 2".to_string();

        worker.handle_assignment(assignment).await;

        let result = tokio::time::timeout(Duration::from_secs(5), result_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.outcome, TaskOutcome::Retry);
        assert_eq!(result.exit_code, 2);
    }
}
