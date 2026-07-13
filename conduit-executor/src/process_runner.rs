//! Child process execution and management.
//!
//! This module handles spawning tasks as isolated child processes and capturing
//! their output, including structured protocol messages for XCom, logs, metrics, and progress.

use crate::protocol::ProtocolMessage;
use chrono::{DateTime, Utc};
use conduit_common::{
    dag::{Task, TaskType},
    ConduitError, ConduitResult, Evidence,
};
use conduit_providers::registry::ProviderInstance;
use conduit_providers::ProviderRegistry;
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
    /// Extra environment variables injected verbatim (no CONDUIT_PARAM_
    /// prefix). Used for incremental-processing context (CONDUIT_WATERMARK_*,
    /// CONDUIT_FULL_REFRESH, …).
    pub extra_env: Vec<(String, String)>,
}

/// Root directory under which per-run XCom JSON files live.
///
/// Each task's XCom (if any) is written to
/// `xcom_root()/{run_id}/{task_id}.json` after completion; downstream tasks
/// read from the same directory. Located under the OS temp dir so it doesn't
/// pollute the user's cwd; can be overridden with `CONDUIT_XCOM_ROOT`.
pub fn xcom_root() -> std::path::PathBuf {
    std::env::var_os("CONDUIT_XCOM_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("conduit-xcom"))
}

/// Per-run XCom directory: `xcom_root()/{run_id}`. Creates it lazily.
pub fn xcom_dir_for_run(run_id: &str) -> std::path::PathBuf {
    let dir = xcom_root().join(run_id);
    let _ = std::fs::create_dir_all(&dir);
    dir
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
    #[tracing::instrument(
        name = "executor.process",
        skip(task, context, registry),
        fields(
            dag_id = %context.dag_id,
            run_id = %context.run_id,
            task_id = %context.task_id,
            attempt = context.attempt,
            environment = %context.environment,
            task_type = %task.task_type.kind()
        )
    )]
    pub async fn run_with_providers(
        task: &Task,
        context: &TaskContext,
        registry: Option<&ProviderRegistry>,
    ) -> ConduitResult<ProcessOutput> {
        let output = Self::run_with_providers_inner(task, context, registry).await?;
        Self::persist_xcom(&context.run_id, &context.task_id, output.xcom.as_ref());
        Ok(output)
    }

    async fn run_with_providers_inner(
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
        if let TaskType::Sql {
            connection, query, ..
        } = &task.task_type
        {
            if let Some(reg) = registry {
                if let Some(ProviderInstance::Sql(sql_provider)) = reg.get(connection) {
                    return Self::execute_sql_native(
                        sql_provider.as_ref(),
                        connection,
                        query,
                        context,
                    )
                    .await;
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

    /// Persist a task's XCom value to `xcom_dir_for_run(run_id)/{task_id}.json`
    /// so downstream tasks in the same run can read it via CONDUIT_XCOM_DIR.
    /// No-op if there is no XCom value. Failures are logged but never propagated:
    /// a missing XCom degrades to a None in the downstream task body, which is
    /// preferable to failing the upstream after it already succeeded.
    fn persist_xcom(run_id: &str, task_id: &str, xcom: Option<&serde_json::Value>) {
        let Some(xcom) = xcom else {
            return;
        };
        let dir = xcom_dir_for_run(run_id);
        let path = dir.join(format!("{}.json", task_id));
        match serde_json::to_vec(xcom) {
            Ok(bytes) => {
                if let Err(e) = std::fs::write(&path, bytes) {
                    debug!(
                        task_id = %task_id,
                        path = %path.display(),
                        error = %e,
                        "Failed to persist XCom"
                    );
                }
            }
            Err(e) => {
                debug!(
                    task_id = %task_id,
                    error = %e,
                    "XCom value not serializable"
                );
            }
        }
    }

    /// Execute a task as a subprocess (the common path for Python, Bash, SQL, Executable).
    #[tracing::instrument(
        name = "executor.process.subprocess",
        skip(task, context),
        fields(
            dag_id = %context.dag_id,
            run_id = %context.run_id,
            task_id = %context.task_id,
            attempt = context.attempt,
            task_type = %task.task_type.kind()
        )
    )]
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

        let status = guard.child.wait().await.map_err(|e| {
            ConduitError::ExecutionError(format!("Failed to wait for process: {}", e))
        })?;

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
    #[tracing::instrument(
        name = "executor.process.sensor",
        skip(task, context),
        fields(
            dag_id = %context.dag_id,
            run_id = %context.run_id,
            task_id = %context.task_id,
            attempt = context.attempt,
            task_type = %task.task_type.kind()
        )
    )]
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
                        output.stdout,
                        attempt,
                        start.elapsed().as_secs_f64()
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

        let (num_str, unit) = if let Some(n) = s.strip_suffix('s') {
            (n, "s")
        } else if let Some(n) = s.strip_suffix('m') {
            (n, "m")
        } else if let Some(n) = s.strip_suffix('h') {
            (n, "h")
        } else if let Some(n) = s.strip_suffix('d') {
            (n, "d")
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
    #[tracing::instrument(
        name = "executor.process.sql_native",
        skip(provider, context),
        fields(
            dag_id = %context.dag_id,
            run_id = %context.run_id,
            task_id = %context.task_id,
            attempt = context.attempt,
            connection = %connection_name
        )
    )]
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

        stdout.push_str(&format!(
            "[INFO] SQL execution started via provider '{}'\n",
            connection_name
        ));
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

        stdout.push_str(&format!(
            "[INFO] SQL execution finished via provider: {} rows, {} columns\n",
            row_count, col_count
        ));

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
                Self::inject_python_path(&mut cmd);
                // CONDUIT_RUNTIME_MODE=1 tells conduit_sdk.decorators to
                // suppress task call-chain bodies on module import; this
                // process will explicitly run exactly one task via
                // conduit_sdk._runtime.run_task.
                cmd.env("CONDUIT_RUNTIME_MODE", "1");
                cmd.arg("-c").arg(format!(
                    "from conduit_sdk._runtime import run_task; run_task({m:?}, {d:?}, {t:?}, {f:?})",
                    m = module,
                    d = context.dag_id,
                    t = context.task_id,
                    f = function,
                ));
                Ok(cmd)
            }
            TaskType::Bash { command } => {
                let mut cmd = Command::new("bash");
                Self::inject_context_env(&mut cmd, context);
                cmd.arg("-c").arg(command);
                Ok(cmd)
            }
            TaskType::Sql { connection, .. } => Err(ConduitError::ExecutionError(format!(
                "SQL task '{}' requires a provider for connection '{}', but none is \
                 configured. Add the connection to conduit.yaml under `connections:` \
                 (e.g. type: duckdb / postgres / sqlite). Refusing to fake SQL execution.",
                task.id, connection
            ))),
            TaskType::Executable { command, args } => {
                let mut cmd = Command::new(command);
                Self::inject_context_env(&mut cmd, context);
                for arg in args {
                    cmd.arg(arg);
                }
                Ok(cmd)
            }
            TaskType::Sensor {
                sensor_type,
                poke_interval,
            } => {
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
                    _ => match context.params.get("command") {
                        Some(command) => command.clone(),
                        None => {
                            return Err(ConduitError::ExecutionError(format!(
                                "Unknown sensor type '{}' and no 'command' param provided",
                                sensor_type
                            )));
                        }
                    },
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

        cmd.env("CONDUIT_XCOM_DIR", xcom_dir_for_run(&context.run_id));

        for (key, value) in &context.params {
            cmd.env(format!("CONDUIT_PARAM_{}", key.to_uppercase()), value);
        }

        for (key, value) in &context.extra_env {
            cmd.env(key, value);
        }
    }

    /// Prepend the dags directory, project root, and bundled SDK location to
    /// PYTHONPATH so that:
    ///   - `from <module> import <function>` resolves against `./dags/<module>.py`
    ///   - DAG files' top-level `from conduit_sdk import dag, task` resolves
    ///
    /// Honors `CONDUIT_DAG_DIR` for the dags directory (defaults to `./dags`)
    /// and `CONDUIT_SDK_PATH` for the SDK (defaults to auto-discovery: look for
    /// `sdk/python` walking up from the binary's location, which finds the
    /// in-repo SDK during dev/quickstart).
    fn inject_python_path(cmd: &mut Command) {
        let dag_dir = std::env::var("CONDUIT_DAG_DIR").unwrap_or_else(|_| "./dags".to_string());
        let existing = std::env::var("PYTHONPATH").unwrap_or_default();
        let sdk_path = std::env::var("CONDUIT_SDK_PATH")
            .ok()
            .or_else(Self::discover_sdk_path);

        let sep = if cfg!(windows) { ";" } else { ":" };
        let mut parts: Vec<String> = vec![dag_dir, ".".to_string()];
        if let Some(sdk) = sdk_path {
            parts.push(sdk);
        }
        if !existing.is_empty() {
            parts.push(existing);
        }
        cmd.env("PYTHONPATH", parts.join(sep));
    }

    /// Locate the `conduit_sdk` Python package. Search order:
    ///
    /// 1. `.conduit/sdk` walking up from the working directory — the copy
    ///    vendored by `conduit init`, so installed binaries work outside a
    ///    repo checkout (PRD B3).
    /// 2. `sdk/python` walking up from the working directory — running with
    ///    an in-repo project.
    /// 3. `sdk/python` walking up from the binary's directory — running a
    ///    `target/…` build from inside the repo.
    ///
    /// An explicit `CONDUIT_SDK_PATH` (honored by `inject_python_path`)
    /// overrides all three; a pip-installed `conduit-sdk` needs none of them.
    fn discover_sdk_path() -> Option<String> {
        if let Ok(cwd) = std::env::current_dir() {
            if let Some(found) = Self::discover_sdk_path_from(&cwd) {
                return Some(found);
            }
        }

        let exe = std::env::current_exe().ok()?;
        let mut dir = exe.parent()?;
        for _ in 0..6 {
            let candidate = dir.join("sdk").join("python");
            if candidate.join("conduit_sdk").is_dir() {
                return Some(candidate.to_string_lossy().into_owned());
            }
            dir = dir.parent()?;
        }
        None
    }

    /// The working-directory tiers of [`Self::discover_sdk_path`], separated
    /// for testability: walk up from `start` looking for a vendored
    /// `.conduit/sdk` first, then an in-repo `sdk/python`.
    fn discover_sdk_path_from(start: &std::path::Path) -> Option<String> {
        let mut dir = start;
        for _ in 0..6 {
            let vendored = dir.join(".conduit").join("sdk");
            if vendored.join("conduit_sdk").is_dir() {
                return Some(vendored.to_string_lossy().into_owned());
            }
            let in_repo = dir.join("sdk").join("python");
            if in_repo.join("conduit_sdk").is_dir() {
                return Some(in_repo.to_string_lossy().into_owned());
            }
            dir = dir.parent()?;
        }
        None
    }

    async fn read_stdout(
        reader: tokio::process::ChildStdout,
    ) -> ConduitResult<(String, Option<serde_json::Value>, Evidence)> {
        let buf_reader = BufReader::new(reader);
        let mut lines = buf_reader.lines();

        let mut stdout = String::new();
        let mut xcom_value: Option<serde_json::Value> = None;
        let mut evidence = Evidence::new();

        while let Some(line) = lines
            .next_line()
            .await
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

    /// Vendored `.conduit/sdk` (written by `conduit init`) wins over an
    /// in-repo `sdk/python`, and both are found by walking up (PRD B3).
    #[test]
    fn discover_sdk_prefers_vendored_copy_walking_up() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path().join("proj");
        let nested_cwd = project.join("dags").join("sub");
        std::fs::create_dir_all(&nested_cwd).unwrap();

        // No SDK anywhere → None.
        assert_eq!(ProcessRunner::discover_sdk_path_from(&nested_cwd), None);

        // In-repo layout found while walking up.
        let in_repo = project.join("sdk").join("python").join("conduit_sdk");
        std::fs::create_dir_all(&in_repo).unwrap();
        let found = ProcessRunner::discover_sdk_path_from(&nested_cwd).unwrap();
        assert!(found.ends_with(&format!("sdk{}python", std::path::MAIN_SEPARATOR)));

        // A vendored copy takes precedence.
        let vendored = project.join(".conduit").join("sdk").join("conduit_sdk");
        std::fs::create_dir_all(&vendored).unwrap();
        let found = ProcessRunner::discover_sdk_path_from(&nested_cwd).unwrap();
        assert!(
            found.contains(".conduit"),
            "vendored copy must win: {found}"
        );
    }

    fn make_bash_task(id: &str, command: &str) -> Task {
        Task {
            id: id.to_string(),
            task_type: TaskType::Bash {
                command: command.to_string(),
            },
            dependencies: vec![],
            retries: 0,
            retry_delay: None,
            retry_backoff: None,
            source_hash: None,
            pool: None,
            timeout: None,
            priority: 0,
            resources: ResourceLimits::default(),
            trigger_rule: TriggerRule::default(),
            incremental: None,
            contracts: None,
            inputs: Vec::new(),
            outputs: Vec::new(),
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
            extra_env: Vec::new(),
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
            extra_env: Vec::new(),
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
            extra_env: Vec::new(),
        };

        let output = ProcessRunner::run(&task, &context).await;
        assert!(output.is_ok());

        let output = output.unwrap();
        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.contains("hello world"));
    }
}
