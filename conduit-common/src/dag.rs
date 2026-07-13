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

    /// Whether to automatically catch up on missed cron runs after downtime.
    /// Defaults to true for backward compatibility.
    #[serde(default = "default_catchup")]
    pub catchup: bool,

    /// Maximum number of catchup runs to schedule per startup.
    /// Prevents a flood of runs after extended downtime. None means no limit.
    #[serde(default)]
    pub max_catchup_runs: Option<u32>,

    /// When true, cross-task lineage stitching treats a column read with no
    /// matching declared upstream column as a compile error rather than a
    /// warning. Opt-in per DAG; default false to keep existing pipelines
    /// compiling.
    #[serde(default)]
    pub lineage_strict: bool,
}

/// A column within a [`Dataset`]. The `dtype` is informational — lineage
/// stitching only matches by name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dtype: Option<String>,
}

impl ColumnSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            dtype: None,
        }
    }
}

/// A named collection of columns produced or consumed by a task.
///
/// Datasets are the cross-task boundary: a Python task declares its outputs
/// as `Dataset`s, and a downstream SQL task reading a `FROM` clause that
/// matches the dataset's qualified `name` resolves columns against this
/// declared schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Dataset {
    /// Schema-qualified name (e.g. `"staging.orders"`). For anonymous
    /// datasets the name is the producing task id.
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    /// `true` when the dataset was synthesised from a task id (the SQL had
    /// no `INSERT INTO` / `CREATE TABLE AS` and the task didn't declare a
    /// `target`). Anonymous datasets are not resolvable by SQL `FROM`
    /// clauses; only direct task-graph consumers can see them.
    #[serde(default, skip_serializing_if = "is_false")]
    pub anonymous: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl Dataset {
    pub fn new(name: impl Into<String>, columns: Vec<ColumnSpec>) -> Self {
        Self {
            name: name.into(),
            columns,
            anonymous: false,
        }
    }

    /// Build an anonymous dataset keyed on a task id (used as the lineage
    /// fallback for plain `SELECT` tasks with no declared target).
    pub fn anonymous(task_id: impl Into<String>) -> Self {
        Self {
            name: task_id.into(),
            columns: Vec::new(),
            anonymous: true,
        }
    }
}

fn default_catchup() -> bool {
    true
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

    /// Exponential backoff multiplier applied to `retry_delay` per attempt
    /// (e.g., 2.0 doubles the delay each retry). None or <= 1.0 = fixed delay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_backoff: Option<f64>,

    /// Stable hash of the task's source text, set by the compiler for
    /// Python tasks (whose `task_type` carries only module/function names).
    /// Folded into the planner fingerprint so body edits are detected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_hash: Option<String>,

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

    /// Datasets this task reads. Populated either from explicit declarations
    /// (`@task(inputs=[…])`) or, for SQL tasks, inferred from the query's
    /// `FROM` / `JOIN` clauses by the compiler.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<Dataset>,

    /// Datasets this task writes. Populated from explicit declarations
    /// (`@task(outputs=[…])`) or, for SQL tasks, inferred from `INSERT INTO`
    /// / `CREATE TABLE AS` / the YAML `target:` field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<Dataset>,
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
    Python { module: String, function: String },
    /// Execute a bash command.
    Bash { command: String },
    /// Execute a SQL query.
    ///
    /// `target` names the dataset the query writes to. When `None`, the
    /// compiler falls back to extracting `INSERT INTO …` / `CREATE TABLE …`
    /// from the query AST, or to the task id as an anonymous dataset.
    Sql {
        connection: String,
        query: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<String>,
    },
    /// An external sensor that polls for a condition.
    Sensor {
        sensor_type: String,
        poke_interval: Option<String>,
    },
    /// A generic executable (stdin/stdout protocol).
    Executable { command: String, args: Vec<String> },
}

impl TaskType {
    /// Stable task kind label for logs, traces, and metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            TaskType::Python { .. } => "python",
            TaskType::Bash { .. } => "bash",
            TaskType::Sql { .. } => "sql",
            TaskType::Sensor { .. } => "sensor",
            TaskType::Executable { .. } => "executable",
        }
    }
}

/// Declared resource limits for a task. Carried through the task model
/// and distributed protocol, but not yet enforced at process spawn time.
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
