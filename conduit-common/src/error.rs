//! Unified error types for Forge.

use thiserror::Error;

pub type ConduitResult<T> = std::result::Result<T, ConduitError>;

/// Convenience alias used by executor and other crates.
pub use ConduitError as CError;

#[derive(Error, Debug)]
pub enum ConduitError {
    // ── Compiler errors ─────────────────────────────────────
    #[error("parse error in {file}: {message}")]
    ParseError { file: String, message: String },

    #[error("cycle detected in DAG: {cycle}")]
    CycleDetected { cycle: String },

    #[error("unknown task reference '{task_id}' in DAG '{dag_id}'")]
    UnknownTaskRef { dag_id: String, task_id: String },

    #[error("duplicate task ID '{task_id}' in DAG '{dag_id}'")]
    DuplicateTaskId { dag_id: String, task_id: String },

    // ── State errors ────────────────────────────────────────
    #[error("event store error: {0}")]
    EventStoreError(String),

    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),

    #[error("environment not found: {0}")]
    EnvironmentNotFound(String),

    // ── Scheduler errors ────────────────────────────────────
    #[error("scheduler error: {0}")]
    SchedulerError(String),

    // ── Executor errors ─────────────────────────────────────
    #[error("task execution failed: {task_id} (exit code {exit_code})")]
    TaskExecutionFailed { task_id: String, exit_code: i32 },

    #[error("task timed out: {task_id} after {timeout_secs}s")]
    TaskTimeout { task_id: String, timeout_secs: u64 },

    #[error("execution error: {0}")]
    ExecutionError(String),

    #[error("protocol error: {0}")]
    ProtocolError(String),

    // ── Config errors ───────────────────────────────────────
    #[error("configuration error: {0}")]
    ConfigError(String),

    #[error("file not found: {0}")]
    FileNotFound(String),

    // ── Wrapped errors ──────────────────────────────────────
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
