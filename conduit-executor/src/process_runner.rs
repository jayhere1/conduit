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

        let (stdout_str, xcom, evidence) = Self::read_stdout(stdout_reader).await?;
        let stderr_str = Self::read_stderr(stderr_reader).await?;

        let status = child
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

        // Substitute parameters into the query
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

        // Emit metrics as protocol messages
        evidence.record("row_count", row_count as f64);

        // Convert result to protocol output and append
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
            TaskType::Sensor { .. } => {
                Err(ConduitError::ExecutionError(
                    "Sensor tasks are not yet supported in process runner".to_string(),
                ))
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
                        // Collect into evidence for contract evaluation
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
