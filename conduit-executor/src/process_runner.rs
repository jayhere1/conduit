//! Child process execution and management.
//!
//! This module handles spawning tasks as isolated child processes and capturing
//! their output, including structured protocol messages for XCom, logs, metrics, and progress.

use crate::protocol::ProtocolMessage;
use chrono::{DateTime, Utc};
use conduit_common::{
    ConduitError, ConduitResult, Evidence,
    dag::{Task, TaskType},
};
use conduit_providers::ProviderRegistry;
use conduit_providers::registry::ProviderInstance;
use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tracing::{debug, info, trace};

/// Execution context for a task
#[derive(Clone, Debug)]
pub struct TaskContext {
    pub dag_id: String,
    pub run_id: String,
    pub task_id: String,
    pub attempt: u32,
    pub logical_date: DateTime<Utc>,
    pub environment: String,
    pub params: HashMap<String, String>,
}

/// Output from an executed process
#[derive(Debug, Clone)]
pub struct ProcessOutput {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub duration: Duration,
    pub xcom: Option<serde_json::Value>,
    /// Evidence collected from CONDUIT::METRIC:: protocol messages.
    /// Used by the contract evaluator to validate data quality assertions.
    pub evidence: Evidence,
}

/// Guard that kills the child process on drop, preventing orphaned processes
/// when tasks are cancelled or timed out.
struct ChildGuard {
    child: tokio::process::Child,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        // Best-effort kill. If the process already exited, start_kill returns Err
        // which we intentionally ignore.
        let _ = self.child.start_kill();
    }
}

/// Process runner for executing tasks as child processes.
///
/// For SQL tasks, it will attempt to use a registered provider from the
/// `ProviderRegistry` if one is available, falling back to a subprocess stub.
pub struct ProcessRunner;

impl ProcessRunner {
    /// Execute a task, using the provider registry for SQL tasks when available.
    pub async fn run(task: &Task, context: &TaskContext) -> ConduitResult<ProcessOutput> {
        Self::run_with_providers(task, context, None).await
    }

    /// Execute a task with an optional provider registry for native SQL execution.
    pub async fn run_with_providers(
        task: &Task,
        context: &TaskContext,
        registry: Option<&ProviderRegistry>,
    ) -> ConduitResult<ProcessOutput> {
        debug!(
            task_id = %context.task_id,
            dag_id = %context.dag_id,
            "Starting process execution"
        );

        // For SQL tasks, try to use a native provider first
        if let TaskType::Sql { connection, query } = &task.task_type {
            if let Some(reg) = registry {
                if let Some(provider) = reg.get(connection) {
                    if let ProviderInstance::Sql(sql_provider) = provider {
                        return Self::execute_sql_native(
                            sql_provider.as_ref(),
                            connection,
                            query,
                            context,
                        ).await;
                    }
                }
            }
            // Fall through to subprocess-based execution
        }

        // For Sensor tasks, poll at poke_interval until success or timeout
        if let TaskType::Sensor { poke_interval, .. } = &task.task_type {
            return Self::run_sensor_with_polling(task, context, poke_interval.as_deref()).await;
        }

        Self::run_subprocess(task, context).await
    }

    /// Execute a task as a subprocess (the common path for Python, Bash, SQL, Executable).
    async fn run_subprocess(task: &Task, context: &TaskContext) -> ConduitResult<ProcessOutput> {
        let start = std::time::Instant::now();

        let mut cmd = Self::build_command(task, context)?;

        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| ConduitError::ExecutionError(format!("Failed to spawn process: {}", e)))?;

        let stdout_reader = child
            .stdout
            .take()
            .ok_or_else(|| ConduitError::ExecutionError("Failed to capture stdout".to_string()))?;

        let stderr_reader = child
            .stderr
            .take()
            .ok_or_else(|| ConduitError::ExecutionError("Failed to capture stderr".to_string()))?;

        // Wrap in ChildGuard so the process is killed if this future is
        // dropped (e.g., due to timeout or cancellation).
        let mut guard = ChildGuard { child };

        let (stdout_str, xcom, evidence) = Self::read_stdout(stdout_reader).await?;
        let stderr_str = Self::read_stderr(stderr_reader).await?;

        let status = guard.child
            .wait()
            .await
            .map_err(|e| ConduitError::ExecutionError(format!("Failed to wait for process: {}", e)))?;

        let exit_code = status.code().unwrap_or(1);
        let duration = start.elapsed();

        debug!(
            task_id = %context.task_id,
            exit_code = exit_code,
            duration_ms = duration.as_millis(),
            metrics_collected = evidence.len(),
            "Process execution completed"
        );

        Ok(ProcessOutput {
            exit_code,
            stdout: stdout_str,
            stderr: stderr_str,
            duration,
            xcom,
            evidence,
        })
    }

    /// Execute a sensor task with polling.
    ///
    /// Runs the sensor's check command repeatedly at `poke_interval` until it
    /// exits with code 0 (success) or the task timeout is reached.
    /// Default poke interval: 30 seconds. Default timeout: 1 hour.
    async fn run_sensor_with_polling(
        task: &Task,
        context: &TaskContext,
        poke_interval: Option<&str>,
    ) -> ConduitResult<ProcessOutput> {
        let interval = Self::parse_duration_str(poke_interval.unwrap_or("30s"));
        let timeout = Self::parse_duration_str(task.timeout.as_deref().unwrap_or("1h"));

        let start = std::time::Instant::now();
        let mut attempt = 0u32;
        #[allow(unused_assignments)]
        let mut last_output: Option<ProcessOutput> = None;

        info!(
            task_id = %context.task_id,
            interval_secs = interval.as_secs(),
            timeout_secs = timeout.as_secs(),
            "Starting sensor polling"
        );

        loop {
            attempt += 1;

            let result = Self::run_subprocess(task, context).await?;

            if result.exit_code == 0 {
                info!(
                    task_id = %context.task_id,
                    attempts = attempt,
                    elapsed_secs = start.elapsed().as_secs(),
                    "Sensor condition met"
                );
                return Ok(ProcessOutput {
                    duration: start.elapsed(),
                    ..result
                });
            }

            last_output = Some(result);

            // Check timeout
            if start.elapsed() >= timeout {
                info!(
                    task_id = %context.task_id,
                    attempts = attempt,
                    "Sensor timed out"
                );
                let output = last_output.unwrap();
                return Ok(ProcessOutput {
                    exit_code: 1,
                    stdout: format!(
                        "{}\nCONDUIT::LOG::ERROR::Sensor timed out after {} attempts ({:.0}s)",
                        output.stdout, attempt, start.elapsed().as_secs_f64()
                    ),
                    stderr: output.stderr,
                    duration: start.elapsed(),
                    xcom: None,
                    evidence: output.evidence,
                });
            }

            trace!(
                task_id = %context.task_id,
                attempt = attempt,
                next_check_secs = interval.as_secs(),
                "Sensor condition not met, waiting for next poke"
            );

            tokio::time::sleep(interval).await;
        }
    }

    /// Parse a duration string like "30s", "5m", "1h", "2d" into a Duration.
    fn parse_duration_str(s: &str) -> Duration {
        let s = s.trim();
        if s.is_empty() {
            return Duration::from_secs(30);
        }

        let (num_str, unit) = if s.ends_with('s') {
            (&s[..s.len() - 1], "s")
        } else if s.ends_with('m') {
            (&s[..s.len() - 1], "m")
        } else if s.ends_with('h') {
            (&s[..s.len() - 1], "h")
        } else if s.ends_with('d') {
            (&s[..s.len() - 1], "d")
        } else {
            // Assume seconds if no unit
            (s, "s")
        };

        let num: u64 = num_str.parse().unwrap_or(30);
        match unit {
            "s" => Duration::from_secs(num),
            "m" => Duration::from_secs(num * 60),
            "h" => Duration::from_secs(num * 3600),
            "d" => Duration::from_secs(num * 86400),
            _ => Duration::from_secs(30),
        }
    }

    /// Execute a SQL query via a native provider (no child process).
    async fn execute_sql_native(
        provider: &dyn conduit_providers::traits::SqlProvider,
        connection_name: &str,
        query: &str,
        context: &TaskContext,
    ) -> ConduitResult<ProcessOutput> {
        let start = std::time::Instant::now();
        let mut stdout = String::new();
        let mut evidence = Evidence::new();

        info!(
            task_id = %context.task_id,
            connection = %connection_name,
            "Executing SQL via native provider"
        );

        stdout.push_str(&format!("[INFO] SQL execution started via provider '{}'\n", connection_name));
        stdout.push_str(&format!("[INFO] Executing: {}\n", query));

        let params: HashMap<String, String> = context.params.clone();

        let result = provider.execute(query, &params).await.map_err(|e| {
            ConduitError::ExecutionError(format!(
                "SQL execution failed on connection '{}': {}",
                connection_name, e
            ))
        })?;

        let row_count = result.rows_returned.unwrap_or(result.rows_affected) as usize;
        let col_count = result.columns.len();

        stdout.push_str(&format!("[INFO] SQL execution completed: {} rows, {} columns\n", row_count, col_count));

        evidence.record("row_count", row_count as f64);

        let protocol_output = result.to_protocol_output();
        stdout.push_str(&protocol_output);

        let duration = start.elapsed();

        let xcom = Some(serde_json::json!({
            "rows_affected": row_count,
            "columns": result.columns,
            "connection": connection_name,
        }));

        Ok(ProcessOutput {
            exit_code: 0,
            stdout,
            stderr: String::new(),
            duration,
            xcom,
            evidence,
        })
    }

    fn build_command(task: &Task, context: &TaskContext) -> ConduitResult<Command> {
        match &task.task_type {
            TaskType::Python { module, function } => {
                let mut cmd = Command::new("python3");
                Self::inject_context_env(&mut cmd, context);
                cmd.arg("-c").arg(format!(
                    "from {} import {}; {}()",
                    module, function, function
                ));
                Ok(cmd)
            }
            TaskType::Bash { command } => {
                let mut cmd = Command::new("bash");
                Self::inject_context_env(&mut cmd, context);
                cmd.arg("-c").arg(command);
                Ok(cmd)
            }
            TaskType::Sql { query, .. } => {
                let mut cmd = Command::new("python3");
                Self::inject_context_env(&mut cmd, context);
                let sql_executor = format!(
                    r#"
print("CONDUIT::LOG::INFO::SQL execution started")
print("CONDUIT::LOG::INFO::Executing: {}")
print("CONDUIT::LOG::INFO::SQL execution completed")
print('CONDUIT::XCOM::{{"rows_affected": 0}}')
"#,
                    query.replace('"', "\\\"")
                );
                cmd.arg("-c").arg(sql_executor);
                Ok(cmd)
            }
            TaskType::Executable { command, args } => {
                let mut cmd = Command::new(command);
                Self::inject_context_env(&mut cmd, context);
                for arg in args {
                    cmd.arg(arg);
                }
                Ok(cmd)
            }
            TaskType::Sensor { sensor_type, poke_interval } => {
                let interval = poke_interval.as_deref().unwrap_or("30s");

                // Sensor scripts use environment variables instead of shell
                // interpolation to prevent command injection. User-controlled
                // params are passed as CONDUIT_SENSOR_* env vars.
                let script = match sensor_type.as_str() {
                    "file" => {
                        // $CONDUIT_SENSOR_FILEPATH is set via env, never interpolated
                        r#"test -f "$CONDUIT_SENSOR_FILEPATH" && echo 'CONDUIT::LOG::INFO::File found' && exit 0 || exit 1"#.to_string()
                    }
                    "http" => {
                        // $CONDUIT_SENSOR_URL is set via env, never interpolated
                        r#"curl -sf "$CONDUIT_SENSOR_URL" > /dev/null && echo 'CONDUIT::LOG::INFO::Endpoint ready' && exit 0 || exit 1"#.to_string()
                    }
                    "sql" => {
                        // SQL sensor: the query must return at least one row for
                        // the condition to be met. Without a native provider
                        // connection, we can't execute the query from bash, so
                        // we fail with a clear message directing users to
                        // configure a provider connection for native execution.
                        r#"echo 'CONDUIT::LOG::ERROR::SQL sensor requires a native provider connection. Configure the connection in conduit.yaml.' && exit 1"#.to_string()
                    }
                    _ => {
                        match context.params.get("command") {
                            Some(command) => command.clone(),
                            None => {
                                return Err(ConduitError::ExecutionError(
                                    format!(
                                        "Unknown sensor type '{}' and no 'command' param provided",
                                        sensor_type
                                    ),
                                ));
                            }
                        }
                    }
                };

                let mut cmd = Command::new("bash");
                Self::inject_context_env(&mut cmd, context);
                cmd.env("CONDUIT_SENSOR_TYPE", sensor_type)
                    .env("CONDUIT_POKE_INTERVAL", interval);
                // Pass user params as env vars (safe from shell injection)
                if let Some(v) = context.params.get("filepath") {
                    cmd.env("CONDUIT_SENSOR_FILEPATH", v);
                }
                if let Some(v) = context.params.get("url") {
                    cmd.env("CONDUIT_SENSOR_URL", v);
                }
                if let Some(v) = context.params.get("query") {
                    cmd.env("CONDUIT_SENSOR_QUERY", v);
                }
                if let Some(v) = context.params.get("connection") {
                    cmd.env("CONDUIT_SENSOR_CONNECTION", v);
                }
                cmd.arg("-c").arg(script);
                Ok(cmd)
            }
        }
    }

    fn inject_context_env(cmd: &mut Command, context: &TaskContext) {
        cmd.env("CONDUIT_DAG_ID", &context.dag_id)
            .env("CONDUIT_RUN_ID", &context.run_id)
            .env("CONDUIT_TASK_ID", &context.task_id)
            .env("CONDUIT_ATTEMPT", context.attempt.to_string())
            .env("CONDUIT_LOGICAL_DATE", context.logical_date.to_rfc3339())
            .env("CONDUIT_ENVIRONMENT", &context.environment);

        for (key, value) in &context.params {
            cmd.env(format!("CONDUIT_PARAM_{}", key.to_uppercase()), value);
        }
    }

    async fn read_stdout(
        reader: tokio::process::ChildStdout,
    ) -> ConduitResult<(String, Option<serde_json::Value>, Evidence)> {
        let buf_reader = BufReader::new(reader);
        let mut lines = buf_reader.lines();

        let mut stdout = String::new();
        let mut xcom_value: Option<serde_json::Value> = None;
        let mut evidence = Evidence::new();

        while let Some(line) = lines.next_line().await
            .map_err(|e| ConduitError::ExecutionError(format!("Failed to read stdout: {}", e)))?
        {
            trace!(stdout_line = %line, "Received stdout line");

            if let Some(message) = crate::protocol::parse_stdout_line(&line) {
                match message {
                    ProtocolMessage::XCom { key: _, value } => {
                        xcom_value = Some(value);
                    }
                    ProtocolMessage::Log {
                        level,
                        message: msg,
                    } => {
                        stdout.push_str(&format!("[{}] {}\n", level, msg));
                    }
                    ProtocolMessage::Progress { percent } => {
                        stdout.push_str(&format!("[PROGRESS] {}%\n", percent));
                    }
                    ProtocolMessage::Metric { name, value } => {
                        evidence.record(&name, value);
                        stdout.push_str(&format!("[METRIC] {} = {}\n", name, value));
                    }
                }
            } else {
                stdout.push_str(&line);
                stdout.push('\n');
            }
        }

        Ok((stdout, xcom_value, evidence))
    }

    async fn read_stderr(reader: tokio::process::ChildStderr) -> ConduitResult<String> {
        let buf_reader = BufReader::new(reader);
        let mut lines = buf_reader.lines();

        let mut stderr = String::new();

        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| ConduitError::ExecutionError(format!("Failed to read stderr: {}", e)))?
        {
            trace!(stderr_line = %line, "Received stderr line");
            stderr.push_str(&line);
            stderr.push('\n');
        }

        Ok(stderr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_common::dag::{ResourceLimits, TriggerRule};

    fn make_bash_task(id: &str, command: &str) -> Task {
        Task {
            id: id.to_string(),
            task_type: TaskType::Bash { command: command.to_string() },
            dependencies: vec![],
            retries: 0,
            retry_delay: None,
            pool: None,
            timeout: None,
            priority: 0,
            resources: ResourceLimits::default(),
            trigger_rule: TriggerRule::default(),
            incremental: None,
            contracts: None,
        }
    }

    #[test]
    fn test_task_context_creation() {
        let context = TaskContext {
            dag_id: "test_dag".to_string(),
            run_id: "run_123".to_string(),
            task_id: "task_1".to_string(),
            attempt: 1,
            logical_date: Utc::now(),
            environment: "dev".to_string(),
            params: HashMap::new(),
        };

        assert_eq!(context.dag_id, "test_dag");
        assert_eq!(context.task_id, "task_1");
        assert_eq!(context.attempt, 1);
    }

    #[test]
    fn test_process_output_creation() {
        let output = ProcessOutput {
            exit_code: 0,
            stdout: "Success".to_string(),
            stderr: "".to_string(),
            duration: Duration::from_secs(5),
            xcom: None,
            evidence: conduit_common::Evidence::new(),
        };

        assert_eq!(output.exit_code, 0);
        assert_eq!(output.duration.as_secs(), 5);
        assert!(output.evidence.is_empty());
    }

    #[tokio::test]
    async fn test_bash_command_building() {
        let task = make_bash_task("test_task", "echo 'hello'");

        let context = TaskContext {
            dag_id: "test_dag".to_string(),
            run_id: "run_123".to_string(),
            task_id: "task_1".to_string(),
            attempt: 1,
            logical_date: Utc::now(),
            environment: "dev".to_string(),
            params: HashMap::new(),
        };

        let cmd = ProcessRunner::build_command(&task, &context);
        assert!(cmd.is_ok());
    }

    #[tokio::test]
    async fn test_simple_bash_execution() {
        let task = make_bash_task("test_task", "echo 'hello world'; exit 0");

        let context = TaskContext {
            dag_id: "test_dag".to_string(),
            run_id: "run_123".to_string(),
            task_id: "task_1".to_string(),
            attempt: 1,
            logical_date: Utc::now(),
            environment: "dev".to_string(),
            params: HashMap::new(),
        };

        let output = ProcessRunner::run(&task, &context).await;
        assert!(output.is_ok());

        let output = output.unwrap();
        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.contains("hello world"));
    }
}
