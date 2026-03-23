//! Core DAG and Task data model.
//!
//! A DAG is a directed acyclic graph of tasks. Each task defines
//! what to execute, its dependencies, retry policy, pool, and resource limits.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::contracts::TaskContracts;
use crate::incremental::IncrementalConfig;

/// Unique identifier for a DAG.
pub type DagId = String;

/// Unique identifier for a task within a DAG.
pub type TaskId = String;

/// A complete DAG definition, as emitted by the compiler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dag {
    /// Unique DAG identifier (derived from the Python function name or YAML id).
    pub id: DagId,

    /// Human-readable description.
    pub description: Option<String>,

    /// Cron schedule expression (e.g., "0 6 * * *").
    pub schedule: Option<String>,

    /// Tags for filtering and organization.
    pub tags: Vec<String>,

    /// Maximum concurrent runs of this DAG.
    pub max_active_runs: u32,

    /// Webhook URL for failure notifications.
    pub on_failure: Option<String>,

    /// All tasks in this DAG, keyed by task ID.
    pub tasks: HashMap<TaskId, Task>,

    /// Topologically sorted task execution order.
    /// Computed by the compiler after dependency resolution.
    pub execution_order: Vec<TaskId>,

    /// Source file path (for error reporting and hot-reload).
    pub source_file: String,

    /// When this DAG definition was last compiled.
    pub compiled_at: DateTime<Utc>,
}

/// A single task within a DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Unique task identifier within this DAG.
    pub id: TaskId,

    /// The type of task (determines how it's executed).
    pub task_type: TaskType,

    /// Tasks that must complete before this one runs.
    pub dependencies: Vec<TaskDependency>,

    /// Retry policy.
    pub retries: u32,

    /// Delay between retries (e.g., "5m", "30s").
    pub retry_delay: Option<String>,

    /// Named resource pool this task draws from.
    pub pool: Option<String>,

    /// Maximum execution time before the task is killed.
    pub timeout: Option<String>,

    /// Task priority within its pool (higher = runs first).
    pub priority: i32,

    /// Resource limits.
    pub resources: ResourceLimits,

    /// Trigger rule determining when this task is eligible to run.
    pub trigger_rule: TriggerRule,

    /// Incremental configuration. If None, the task does a full refresh every time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incremental: Option<IncrementalConfig>,

    /// Data quality contracts. Validated during plan/apply — errors block deployment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contracts: Option<TaskContracts>,
}

/// How a task dependency is expressed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDependency {
    /// The task ID this depends on.
    pub task_id: TaskId,

    /// Whether this is a data dependency (XCom) or just execution order.
    pub dependency_type: DependencyType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DependencyType {
    /// Task must complete successfully before this one starts.
    ExecutionOrder,
    /// Task's output (XCom) is used as input to this task.
    DataFlow,
}

/// The type of work a task performs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskType {
    /// Execute a Python function.
    Python {
        module: String,
        function: String,
    },
    /// Execute a bash command.
    Bash {
        command: String,
    },
    /// Execute a SQL query.
    Sql {
        connection: String,
        query: String,
    },
    /// An external sensor that polls for a condition.
    Sensor {
        sensor_type: String,
        poke_interval: Option<String>,
    },
    /// A generic executable (stdin/stdout protocol).
    Executable {
        command: String,
        args: Vec<String>,
    },
}

/// Resource limits for a task's cgroup.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceLimits {
    /// CPU limit in millicores (e.g., 1000 = 1 core).
    pub cpu_millicores: Option<u32>,
    /// Memory limit in megabytes.
    pub memory_mb: Option<u32>,
}

/// When a task becomes eligible to run.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum TriggerRule {
    /// All upstream tasks must succeed (default).
    #[default]
    AllSuccess,
    /// All upstream tasks must complete (success or failure).
    AllDone,
    /// At least one upstream task must succeed.
    OneSuccess,
    /// At least one upstream task must fail.
    OneFailed,
    /// No upstream dependencies (root task).
    NoDeps,
}

/// A named resource pool with a slot limit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pool {
    pub name: String,
    pub slots: u32,
    pub description: Option<String>,
}
