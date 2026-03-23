//! Evidence-based data quality contracts.
//!
//! Tasks emit **evidence** — structured measurements via the stdout protocol
//! (`CONDUIT::METRIC::name::value`). Contracts are assertions against this
//! evidence. The executor collects evidence during task execution, then the
//! contract evaluator validates it.
//!
//! This design is runtime-agnostic: a SQL task can emit `row_count` by running
//! `SELECT COUNT(*)`, a Python script can emit it with
//! `print("CONDUIT::METRIC::row_count::42")`, a shell task can `echo` it.
//! The contract doesn't know or care how the evidence was produced.
//!
//! # Architecture
//!
//! ```text
//! Task (any language)
//!   └─ emits CONDUIT::METRIC::row_count::5000
//!   └─ emits CONDUIT::METRIC::data_age_seconds::3600
//!   └─ emits CONDUIT::METRIC::null_rate.email::0.02
//!
//! Executor
//!   └─ collects metrics into Evidence { metrics: HashMap<String, f64> }
//!
//! ContractEvaluator
//!   └─ takes (Evidence, TaskContracts) → ValidationResult
//!   └─ each ContractCheck knows which metric(s) to look up
//!   └─ missing evidence = contract failure (task must emit what it promises)
//! ```
//!
//! # Contract Types
//!
//! High-level (ergonomic sugar over metric assertions):
//! - `RowCount` → asserts metric `row_count` within bounds
//! - `Freshness` → asserts metric `data_age_seconds` ≤ threshold
//! - `Unique` → asserts metric `duplicate_count` == 0
//! - `NotNullRate` → asserts metric `null_rate.{column}` ≤ threshold
//! - `AcceptedValues` → asserts metric `invalid_value_count.{column}` == 0
//! - `ValueRange` → asserts metric `out_of_range_count.{column}` == 0
//! - `RowCountDelta` → asserts metric `row_count_delta_pct` within bounds
//! - `ReferentialIntegrity` → asserts metric `orphan_count.{column}` == 0
//!
//! Generic (assert on any metric):
//! - `Metric` → assert a named metric against min/max/exact bounds
//!
//! Escape hatch (still runs logic outside the metric system):
//! - `Custom` → a named assertion; the task emits `pass.{name}::1` or `pass.{name}::0`

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

// ─── Evidence ───────────────────────────────────────────────────────────────

/// Evidence collected from a task's stdout during execution.
///
/// This is the bridge between tasks and contracts. Tasks emit measurements
/// via `CONDUIT::METRIC::name::value`, the executor collects them here,
/// and the contract evaluator asserts against them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Evidence {
    /// Named metrics emitted by the task.
    /// Keys are metric names (e.g., "row_count", "data_age_seconds", "null_rate.email").
    /// Values are numeric measurements.
    pub metrics: HashMap<String, f64>,
}

impl Evidence {
    pub fn new() -> Self {
        Self {
            metrics: HashMap::new(),
        }
    }

    /// Record a metric. If the same name is emitted twice, the last value wins.
    pub fn record(&mut self, name: impl Into<String>, value: f64) {
        self.metrics.insert(name.into(), value);
    }

    /// Get a metric value.
    pub fn get(&self, name: &str) -> Option<f64> {
        self.metrics.get(name).copied()
    }

    /// Check if a metric exists.
    pub fn has(&self, name: &str) -> bool {
        self.metrics.contains_key(name)
    }

    /// Number of metrics recorded.
    pub fn len(&self) -> usize {
        self.metrics.len()
    }

    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty()
    }
}

// ─── Core Types ─────────────────────────────────────────────────────────────

/// Severity of a contract: errors block deployment, warnings don't.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Severity {
    /// Blocks deployment. The pipeline cannot proceed.
    Error,
    /// Reported in plan output but does not block deployment.
    Warning,
}

impl Default for Severity {
    fn default() -> Self {
        Severity::Error
    }
}

/// A single data quality assertion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataContract {
    /// Human-readable name for this contract.
    pub name: String,

    /// What this contract checks (which evidence to look at).
    pub check: ContractCheck,

    /// Whether failure blocks deployment.
    #[serde(default)]
    pub severity: Severity,

    /// Human-readable description of why this contract exists.
    #[serde(default)]
    pub description: Option<String>,

    /// Tags for filtering/grouping contracts.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// The specific assertion to evaluate against collected evidence.
///
/// Each variant knows which metric(s) to look up in Evidence and what
/// constitutes a pass or fail. The task is responsible for emitting the
/// expected metrics via the stdout protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContractCheck {
    /// Row count must be within bounds.
    /// Expects metric: `row_count`
    RowCount {
        #[serde(default)]
        min: Option<u64>,
        #[serde(default)]
        max: Option<u64>,
        #[serde(default)]
        exact: Option<u64>,
    },

    /// Data must not be older than a threshold.
    /// Expects metric: `data_age_seconds`
    ///
    /// The task is responsible for computing freshness and emitting it.
    /// For SQL tasks, Conduit's built-in SQL executor computes this automatically.
    /// For Python/shell tasks, emit `CONDUIT::METRIC::data_age_seconds::<seconds>`.
    Freshness {
        /// Maximum age as a human-readable duration (e.g., "24h", "2d", "30m").
        max_age: String,
    },

    /// No duplicate values across the specified columns.
    /// Expects metric: `duplicate_count` (must be 0 to pass)
    Unique {
        /// Columns that form the unique key (used for display/docs, not evaluation).
        columns: Vec<String>,
    },

    /// Null rate for a column must not exceed a threshold.
    /// Expects metric: `null_rate.{column}` (value between 0.0 and 1.0)
    NotNullRate {
        /// Column name (used to derive the metric key).
        column: String,
        /// Minimum fraction of non-null values (0.0 to 1.0). Default: 1.0.
        #[serde(default = "default_not_null_threshold")]
        min_rate: f64,
    },

    /// Column values must be within an accepted set.
    /// Expects metric: `invalid_value_count.{column}` (must be 0 to pass)
    AcceptedValues {
        /// Column name.
        column: String,
        /// Allowed values (for display/docs).
        values: Vec<String>,
        /// If true, NULL values are acceptable.
        #[serde(default)]
        allow_null: bool,
    },

    /// Numeric column must be within a range.
    /// Expects metric: `out_of_range_count.{column}` (must be 0 to pass)
    ValueRange {
        /// Column name.
        column: String,
        #[serde(default)]
        min: Option<f64>,
        #[serde(default)]
        max: Option<f64>,
    },

    /// Referential integrity: values in a column must exist in another task's output.
    /// Expects metric: `orphan_count.{column}` (must be 0 to pass)
    ReferentialIntegrity {
        /// Column in this task's output.
        column: String,
        /// The referenced task ID (for display/docs).
        ref_task: String,
        /// The referenced column (for display/docs).
        ref_column: String,
    },

    /// Row count delta between runs must not exceed threshold.
    /// Expects metric: `row_count_delta_pct` (percentage change as decimal, e.g., 0.15 = 15%)
    RowCountDelta {
        /// Maximum allowed percentage change.
        max_percent_change: f64,
        /// Allow the count to decrease?
        #[serde(default)]
        allow_decrease: bool,
    },

    /// Generic metric assertion — the universal contract.
    /// Assert any named metric against bounds.
    /// Expects metric: `{metric_name}`
    Metric {
        /// The metric name to look up in evidence.
        metric_name: String,
        #[serde(default)]
        min: Option<f64>,
        #[serde(default)]
        max: Option<f64>,
        #[serde(default)]
        exact: Option<f64>,
    },

    /// Custom pass/fail assertion.
    /// The task emits `CONDUIT::METRIC::pass.{assertion_name}::1` (pass) or `::0` (fail).
    /// Expects metric: `pass.{assertion_name}`
    Custom {
        /// Name of the custom assertion.
        assertion_name: String,
    },
}

fn default_not_null_threshold() -> f64 {
    1.0
}

// ─── Contract Evaluator ─────────────────────────────────────────────────────

/// Evaluates contracts against collected evidence.
pub struct ContractEvaluator;

impl ContractEvaluator {
    /// Evaluate all contracts for a task against the evidence it emitted.
    pub fn evaluate(contracts: &TaskContracts, evidence: &Evidence) -> ValidationResult {
        let checks: Vec<CheckResult> = contracts
            .checks
            .iter()
            .map(|contract| Self::evaluate_one(contract, evidence))
            .collect();

        ValidationResult::from_checks(
            &contracts.task_id,
            contracts.dag_id.clone(),
            checks,
        )
    }

    /// Evaluate a single contract against evidence.
    fn evaluate_one(contract: &DataContract, evidence: &Evidence) -> CheckResult {
        let (passed, message, measured_value) = match &contract.check {
            ContractCheck::RowCount { min, max, exact } => {
                Self::eval_row_count(evidence, *min, *max, *exact)
            }

            ContractCheck::Freshness { max_age } => {
                Self::eval_freshness(evidence, max_age)
            }

            ContractCheck::Unique { .. } => {
                Self::eval_zero_metric(evidence, "duplicate_count", "duplicates")
            }

            ContractCheck::NotNullRate { column, min_rate } => {
                Self::eval_not_null_rate(evidence, column, *min_rate)
            }

            ContractCheck::AcceptedValues { column, .. } => {
                let metric = format!("invalid_value_count.{}", column);
                Self::eval_zero_metric(evidence, &metric, "invalid values")
            }

            ContractCheck::ValueRange { column, .. } => {
                let metric = format!("out_of_range_count.{}", column);
                Self::eval_zero_metric(evidence, &metric, "out-of-range values")
            }

            ContractCheck::ReferentialIntegrity { column, .. } => {
                let metric = format!("orphan_count.{}", column);
                Self::eval_zero_metric(evidence, &metric, "orphan records")
            }

            ContractCheck::RowCountDelta {
                max_percent_change,
                allow_decrease,
            } => Self::eval_row_count_delta(evidence, *max_percent_change, *allow_decrease),

            ContractCheck::Metric {
                metric_name,
                min,
                max,
                exact,
            } => Self::eval_metric(evidence, metric_name, *min, *max, *exact),

            ContractCheck::Custom { assertion_name } => {
                let metric = format!("pass.{}", assertion_name);
                match evidence.get(&metric) {
                    Some(v) if v >= 1.0 => (true, "PASS".to_string(), Some(v)),
                    Some(v) => (false, format!("Custom assertion '{}' failed", assertion_name), Some(v)),
                    None => (false, format!("Missing evidence: metric '{}' not emitted", metric), None),
                }
            }
        };

        CheckResult {
            contract_name: contract.name.clone(),
            passed,
            message,
            severity: contract.severity.clone(),
            measured_value,
            expected: Self::expected_description(&contract.check),
        }
    }

    fn eval_row_count(
        evidence: &Evidence,
        min: Option<u64>,
        max: Option<u64>,
        exact: Option<u64>,
    ) -> (bool, String, Option<f64>) {
        match evidence.get("row_count") {
            Some(count) => {
                let count_u64 = count as u64;
                if let Some(exact) = exact {
                    if count_u64 != exact {
                        return (false, format!("{} rows, expected exactly {}", count_u64, exact), Some(count));
                    }
                }
                if let Some(min) = min {
                    if count_u64 < min {
                        return (false, format!("{} rows, expected at least {}", count_u64, min), Some(count));
                    }
                }
                if let Some(max) = max {
                    if count_u64 > max {
                        return (false, format!("{} rows, expected at most {}", count_u64, max), Some(count));
                    }
                }
                (true, format!("row_count={}", count_u64), Some(count))
            }
            None => (
                false,
                "Missing evidence: metric 'row_count' not emitted by task".to_string(),
                None,
            ),
        }
    }

    fn eval_freshness(evidence: &Evidence, max_age: &str) -> (bool, String, Option<f64>) {
        let max_seconds = match parse_duration_to_seconds(max_age) {
            Some(s) => s,
            None => return (false, format!("Invalid max_age format: '{}'", max_age), None),
        };

        match evidence.get("data_age_seconds") {
            Some(age) => {
                if age <= max_seconds {
                    (true, format!("data_age={}s (max {}s)", age as u64, max_seconds as u64), Some(age))
                } else {
                    (
                        false,
                        format!(
                            "Data is {}s old, max allowed is {}s ({})",
                            age as u64, max_seconds as u64, max_age
                        ),
                        Some(age),
                    )
                }
            }
            None => (
                false,
                "Missing evidence: metric 'data_age_seconds' not emitted by task".to_string(),
                None,
            ),
        }
    }

    fn eval_zero_metric(
        evidence: &Evidence,
        metric_name: &str,
        label: &str,
    ) -> (bool, String, Option<f64>) {
        match evidence.get(metric_name) {
            Some(count) if count == 0.0 => {
                (true, format!("0 {}", label), Some(0.0))
            }
            Some(count) => {
                (false, format!("{} {} found", count as u64, label), Some(count))
            }
            None => (
                false,
                format!("Missing evidence: metric '{}' not emitted by task", metric_name),
                None,
            ),
        }
    }

    fn eval_not_null_rate(
        evidence: &Evidence,
        column: &str,
        min_rate: f64,
    ) -> (bool, String, Option<f64>) {
        let metric = format!("null_rate.{}", column);
        match evidence.get(&metric) {
            Some(null_rate) => {
                let non_null_rate = 1.0 - null_rate;
                if non_null_rate >= min_rate {
                    (true, format!("non_null_rate={:.2}%", non_null_rate * 100.0), Some(non_null_rate))
                } else {
                    (
                        false,
                        format!(
                            "{:.2}% non-null, required {:.2}%",
                            non_null_rate * 100.0,
                            min_rate * 100.0
                        ),
                        Some(non_null_rate),
                    )
                }
            }
            None => (
                false,
                format!("Missing evidence: metric '{}' not emitted by task", metric),
                None,
            ),
        }
    }

    fn eval_row_count_delta(
        evidence: &Evidence,
        max_percent_change: f64,
        allow_decrease: bool,
    ) -> (bool, String, Option<f64>) {
        match evidence.get("row_count_delta_pct") {
            Some(delta) => {
                if !allow_decrease && delta < 0.0 {
                    return (
                        false,
                        format!("Row count decreased by {:.1}% (decreases not allowed)", delta.abs() * 100.0),
                        Some(delta),
                    );
                }
                if delta.abs() > max_percent_change {
                    (
                        false,
                        format!(
                            "Row count changed by {:.1}%, max allowed is {:.1}%",
                            delta.abs() * 100.0,
                            max_percent_change * 100.0
                        ),
                        Some(delta),
                    )
                } else {
                    (true, format!("delta={:.1}%", delta.abs() * 100.0), Some(delta))
                }
            }
            None => (
                false,
                "Missing evidence: metric 'row_count_delta_pct' not emitted by task".to_string(),
                None,
            ),
        }
    }

    fn eval_metric(
        evidence: &Evidence,
        metric_name: &str,
        min: Option<f64>,
        max: Option<f64>,
        exact: Option<f64>,
    ) -> (bool, String, Option<f64>) {
        match evidence.get(metric_name) {
            Some(value) => {
                if let Some(exact) = exact {
                    if (value - exact).abs() > f64::EPSILON {
                        return (false, format!("{}={}, expected exactly {}", metric_name, value, exact), Some(value));
                    }
                }
                if let Some(min) = min {
                    if value < min {
                        return (false, format!("{}={}, expected at least {}", metric_name, value, min), Some(value));
                    }
                }
                if let Some(max) = max {
                    if value > max {
                        return (false, format!("{}={}, expected at most {}", metric_name, value, max), Some(value));
                    }
                }
                (true, format!("{}={}", metric_name, value), Some(value))
            }
            None => (
                false,
                format!("Missing evidence: metric '{}' not emitted by task", metric_name),
                None,
            ),
        }
    }

    fn expected_description(check: &ContractCheck) -> Option<String> {
        match check {
            ContractCheck::RowCount { min, max, exact } => {
                let mut parts = vec![];
                if let Some(v) = exact { parts.push(format!("exactly {}", v)); }
                if let Some(v) = min { parts.push(format!("min={}", v)); }
                if let Some(v) = max { parts.push(format!("max={}", v)); }
                Some(parts.join(", "))
            }
            ContractCheck::Freshness { max_age } => Some(format!("max_age={}", max_age)),
            ContractCheck::Unique { columns } => Some(format!("unique({})", columns.join(", "))),
            ContractCheck::NotNullRate { column, min_rate } => {
                Some(format!("not_null({}, {:.0}%)", column, min_rate * 100.0))
            }
            ContractCheck::Metric { metric_name, min, max, exact } => {
                let mut parts = vec![metric_name.clone()];
                if let Some(v) = exact { parts.push(format!("exactly {}", v)); }
                if let Some(v) = min { parts.push(format!("min={}", v)); }
                if let Some(v) = max { parts.push(format!("max={}", v)); }
                Some(parts.join(", "))
            }
            _ => None,
        }
    }
}

/// Parse a human-readable duration string into seconds.
/// Supports: "30s", "5m", "24h", "2d", "1w"
fn parse_duration_to_seconds(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (num_str, unit) = s.split_at(s.len() - 1);
    let value: f64 = num_str.parse().ok()?;

    match unit {
        "s" => Some(value),
        "m" => Some(value * 60.0),
        "h" => Some(value * 3600.0),
        "d" => Some(value * 86400.0),
        "w" => Some(value * 604800.0),
        _ => None,
    }
}

// ─── Contract Set ───────────────────────────────────────────────────────────

/// All contracts for a single task.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskContracts {
    /// The task ID these contracts apply to.
    pub task_id: String,

    /// Optional DAG scope.
    #[serde(default)]
    pub dag_id: Option<String>,

    /// The list of data quality assertions.
    #[serde(default)]
    pub checks: Vec<DataContract>,
}

impl TaskContracts {
    pub fn new(task_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            dag_id: None,
            checks: Vec::new(),
        }
    }

    pub fn with_dag(mut self, dag_id: impl Into<String>) -> Self {
        self.dag_id = Some(dag_id.into());
        self
    }

    /// Builder: add a row count bound.
    pub fn row_count(mut self, min: Option<u64>, max: Option<u64>) -> Self {
        self.checks.push(DataContract {
            name: format!(
                "row_count({}{})",
                min.map(|v| format!("min={}", v)).unwrap_or_default(),
                max.map(|v| format!(", max={}", v)).unwrap_or_default()
            ),
            check: ContractCheck::RowCount {
                min,
                max,
                exact: None,
            },
            severity: Severity::Error,
            description: None,
            tags: vec![],
        });
        self
    }

    /// Builder: add a freshness check.
    pub fn freshness(mut self, max_age: impl Into<String>) -> Self {
        let age = max_age.into();
        self.checks.push(DataContract {
            name: format!("freshness({})", age),
            check: ContractCheck::Freshness { max_age: age },
            severity: Severity::Error,
            description: None,
            tags: vec![],
        });
        self
    }

    /// Builder: add a uniqueness check.
    pub fn unique(mut self, columns: Vec<String>) -> Self {
        self.checks.push(DataContract {
            name: format!("unique({})", columns.join(", ")),
            check: ContractCheck::Unique { columns },
            severity: Severity::Error,
            description: None,
            tags: vec![],
        });
        self
    }

    /// Builder: add a not-null rate check.
    pub fn not_null(mut self, column: impl Into<String>, min_rate: f64) -> Self {
        let col = column.into();
        self.checks.push(DataContract {
            name: format!("not_null({}, {:.0}%)", col, min_rate * 100.0),
            check: ContractCheck::NotNullRate {
                column: col,
                min_rate,
            },
            severity: Severity::Error,
            description: None,
            tags: vec![],
        });
        self
    }

    /// Builder: add an accepted values check.
    pub fn accepted_values(
        mut self,
        column: impl Into<String>,
        values: Vec<String>,
    ) -> Self {
        let col = column.into();
        self.checks.push(DataContract {
            name: format!("accepted_values({}, [{}])", col, values.join(",")),
            check: ContractCheck::AcceptedValues {
                column: col,
                values,
                allow_null: false,
            },
            severity: Severity::Error,
            description: None,
            tags: vec![],
        });
        self
    }

    /// Builder: add a generic metric assertion.
    pub fn metric(
        mut self,
        metric_name: impl Into<String>,
        min: Option<f64>,
        max: Option<f64>,
    ) -> Self {
        let name = metric_name.into();
        self.checks.push(DataContract {
            name: format!("metric({})", name),
            check: ContractCheck::Metric {
                metric_name: name,
                min,
                max,
                exact: None,
            },
            severity: Severity::Error,
            description: None,
            tags: vec![],
        });
        self
    }

    /// Builder: add a custom pass/fail assertion.
    pub fn custom(mut self, assertion_name: impl Into<String>) -> Self {
        let name = assertion_name.into();
        self.checks.push(DataContract {
            name: format!("custom({})", name),
            check: ContractCheck::Custom {
                assertion_name: name,
            },
            severity: Severity::Error,
            description: None,
            tags: vec![],
        });
        self
    }

    /// Builder: add a referential integrity check.
    pub fn references(
        mut self,
        column: impl Into<String>,
        ref_task: impl Into<String>,
        ref_column: impl Into<String>,
    ) -> Self {
        let col = column.into();
        let rt = ref_task.into();
        let rc = ref_column.into();
        self.checks.push(DataContract {
            name: format!("{} -> {}.{}", col, rt, rc),
            check: ContractCheck::ReferentialIntegrity {
                column: col,
                ref_task: rt,
                ref_column: rc,
            },
            severity: Severity::Error,
            description: None,
            tags: vec![],
        });
        self
    }

    /// How many checks does this contract set have?
    pub fn len(&self) -> usize {
        self.checks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.checks.is_empty()
    }

    /// List the metric names that this contract set expects the task to emit.
    pub fn expected_metrics(&self) -> Vec<String> {
        self.checks
            .iter()
            .flat_map(|c| required_metrics(&c.check))
            .collect()
    }
}

/// Return the metric name(s) a contract check expects.
fn required_metrics(check: &ContractCheck) -> Vec<String> {
    match check {
        ContractCheck::RowCount { .. } => vec!["row_count".to_string()],
        ContractCheck::Freshness { .. } => vec!["data_age_seconds".to_string()],
        ContractCheck::Unique { .. } => vec!["duplicate_count".to_string()],
        ContractCheck::NotNullRate { column, .. } => vec![format!("null_rate.{}", column)],
        ContractCheck::AcceptedValues { column, .. } => {
            vec![format!("invalid_value_count.{}", column)]
        }
        ContractCheck::ValueRange { column, .. } => {
            vec![format!("out_of_range_count.{}", column)]
        }
        ContractCheck::ReferentialIntegrity { column, .. } => {
            vec![format!("orphan_count.{}", column)]
        }
        ContractCheck::RowCountDelta { .. } => vec!["row_count_delta_pct".to_string()],
        ContractCheck::Metric { metric_name, .. } => vec![metric_name.clone()],
        ContractCheck::Custom { assertion_name } => vec![format!("pass.{}", assertion_name)],
    }
}

// ─── Validation Results ─────────────────────────────────────────────────────

/// Result of evaluating a single contract check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    /// The contract that was checked.
    pub contract_name: String,
    /// Whether it passed.
    pub passed: bool,
    /// Human-readable result message.
    pub message: String,
    /// The severity (from the contract definition).
    pub severity: Severity,
    /// Measured value (from evidence).
    #[serde(default)]
    pub measured_value: Option<f64>,
    /// Expected value description.
    #[serde(default)]
    pub expected: Option<String>,
}

/// Aggregate validation result for all contracts on a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Task ID.
    pub task_id: String,
    /// DAG ID.
    pub dag_id: Option<String>,
    /// Individual check results.
    pub checks: Vec<CheckResult>,
    /// Overall pass/fail (true only if all Error-severity checks pass).
    pub passed: bool,
    /// Total checks run.
    pub total_checks: usize,
    /// Checks that passed.
    pub passed_checks: usize,
    /// Checks that failed with Error severity.
    pub error_count: usize,
    /// Checks that failed with Warning severity.
    pub warning_count: usize,
}

impl ValidationResult {
    /// Build from a list of check results.
    pub fn from_checks(
        task_id: impl Into<String>,
        dag_id: Option<String>,
        checks: Vec<CheckResult>,
    ) -> Self {
        let total_checks = checks.len();
        let passed_checks = checks.iter().filter(|c| c.passed).count();
        let error_count = checks
            .iter()
            .filter(|c| !c.passed && c.severity == Severity::Error)
            .count();
        let warning_count = checks
            .iter()
            .filter(|c| !c.passed && c.severity == Severity::Warning)
            .count();
        let passed = error_count == 0;

        ValidationResult {
            task_id: task_id.into(),
            dag_id,
            checks,
            passed,
            total_checks,
            passed_checks,
            error_count,
            warning_count,
        }
    }
}

impl std::fmt::Display for ValidationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = if self.passed { "PASSED" } else { "FAILED" };
        let scope = match &self.dag_id {
            Some(d) => format!("{}.{}", d, self.task_id),
            None => self.task_id.clone(),
        };
        writeln!(
            f,
            "Contracts for '{}': {} ({}/{} checks passed, {} errors, {} warnings)",
            scope, status, self.passed_checks, self.total_checks,
            self.error_count, self.warning_count
        )?;

        for check in &self.checks {
            if check.passed {
                writeln!(f, "  [PASS ] {}", check.contract_name)?;
            } else {
                let sev = match check.severity {
                    Severity::Error => "ERROR",
                    Severity::Warning => "WARN ",
                };
                writeln!(f, "  [{}] {}: {}", sev, check.contract_name, check.message)?;
            }
        }

        Ok(())
    }
}

// ─── Deployment Gate ────────────────────────────────────────────────────────

/// Aggregate result across all tasks in a deployment.
/// Used by plan/apply to decide whether to proceed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentValidation {
    /// Per-task validation results.
    pub results: Vec<ValidationResult>,
    /// Overall: can we deploy?
    pub can_deploy: bool,
    /// Total contract checks across all tasks.
    pub total_checks: usize,
    /// Total errors across all tasks.
    pub total_errors: usize,
    /// Total warnings across all tasks.
    pub total_warnings: usize,
}

impl DeploymentValidation {
    pub fn from_results(results: Vec<ValidationResult>) -> Self {
        let total_checks: usize = results.iter().map(|r| r.total_checks).sum();
        let total_errors: usize = results.iter().map(|r| r.error_count).sum();
        let total_warnings: usize = results.iter().map(|r| r.warning_count).sum();
        let can_deploy = total_errors == 0;

        Self {
            results,
            can_deploy,
            total_checks,
            total_errors,
            total_warnings,
        }
    }
}

impl std::fmt::Display for DeploymentValidation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Contract Validation Summary")?;
        writeln!(f, "─────────────────────────────")?;

        for result in &self.results {
            write!(f, "{}", result)?;
        }

        writeln!(f)?;
        if self.can_deploy {
            writeln!(
                f,
                "Result: PASS — {} checks passed ({} warnings)",
                self.total_checks, self.total_warnings
            )?;
        } else {
            writeln!(
                f,
                "Result: BLOCKED — {} errors must be fixed before deployment ({} warnings)",
                self.total_errors, self.total_warnings
            )?;
        }

        Ok(())
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_basics() {
        let mut ev = Evidence::new();
        assert!(ev.is_empty());

        ev.record("row_count", 5000.0);
        ev.record("data_age_seconds", 3600.0);

        assert_eq!(ev.len(), 2);
        assert_eq!(ev.get("row_count"), Some(5000.0));
        assert!(ev.has("data_age_seconds"));
        assert!(!ev.has("nonexistent"));
    }

    #[test]
    fn evidence_last_write_wins() {
        let mut ev = Evidence::new();
        ev.record("row_count", 100.0);
        ev.record("row_count", 200.0);
        assert_eq!(ev.get("row_count"), Some(200.0));
    }

    #[test]
    fn builder_creates_contracts() {
        let tc = TaskContracts::new("extract_orders")
            .with_dag("daily_etl")
            .row_count(Some(1), Some(10_000_000))
            .freshness("24h")
            .unique(vec!["id".to_string()])
            .not_null("customer_id", 0.99)
            .accepted_values(
                "status",
                vec!["pending".into(), "shipped".into(), "delivered".into()],
            );

        assert_eq!(tc.checks.len(), 5);
        assert_eq!(tc.task_id, "extract_orders");
        assert_eq!(tc.dag_id.as_deref(), Some("daily_etl"));
    }

    #[test]
    fn expected_metrics_listed() {
        let tc = TaskContracts::new("task")
            .row_count(Some(1), None)
            .freshness("24h")
            .not_null("email", 1.0)
            .metric("accuracy", Some(0.95), None);

        let metrics = tc.expected_metrics();
        assert!(metrics.contains(&"row_count".to_string()));
        assert!(metrics.contains(&"data_age_seconds".to_string()));
        assert!(metrics.contains(&"null_rate.email".to_string()));
        assert!(metrics.contains(&"accuracy".to_string()));
    }

    #[test]
    fn eval_row_count_passes() {
        let tc = TaskContracts::new("task").row_count(Some(1), Some(10000));
        let mut ev = Evidence::new();
        ev.record("row_count", 5000.0);

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(result.passed);
        assert_eq!(result.passed_checks, 1);
    }

    #[test]
    fn eval_row_count_fails_below_min() {
        let tc = TaskContracts::new("task").row_count(Some(100), None);
        let mut ev = Evidence::new();
        ev.record("row_count", 0.0);

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(!result.passed);
        assert!(result.checks[0].message.contains("at least"));
    }

    #[test]
    fn eval_missing_evidence_fails() {
        let tc = TaskContracts::new("task").row_count(Some(1), None);
        let ev = Evidence::new(); // empty — no metrics

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(!result.passed);
        assert!(result.checks[0].message.contains("Missing evidence"));
    }

    #[test]
    fn eval_freshness_passes() {
        let tc = TaskContracts::new("task").freshness("24h");
        let mut ev = Evidence::new();
        ev.record("data_age_seconds", 3600.0); // 1 hour old

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(result.passed);
    }

    #[test]
    fn eval_freshness_fails() {
        let tc = TaskContracts::new("task").freshness("1h");
        let mut ev = Evidence::new();
        ev.record("data_age_seconds", 7200.0); // 2 hours old

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(!result.passed);
        assert!(result.checks[0].message.contains("old"));
    }

    #[test]
    fn eval_not_null_rate() {
        let tc = TaskContracts::new("task").not_null("email", 0.95);
        let mut ev = Evidence::new();
        ev.record("null_rate.email", 0.02); // 2% null → 98% non-null → passes

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(result.passed);
    }

    #[test]
    fn eval_not_null_rate_fails() {
        let tc = TaskContracts::new("task").not_null("email", 0.99);
        let mut ev = Evidence::new();
        ev.record("null_rate.email", 0.10); // 10% null → 90% non-null → fails

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(!result.passed);
    }

    #[test]
    fn eval_generic_metric() {
        let tc = TaskContracts::new("task")
            .metric("accuracy", Some(0.95), None);
        let mut ev = Evidence::new();
        ev.record("accuracy", 0.98);

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(result.passed);
    }

    #[test]
    fn eval_generic_metric_fails() {
        let tc = TaskContracts::new("task")
            .metric("accuracy", Some(0.95), None);
        let mut ev = Evidence::new();
        ev.record("accuracy", 0.80);

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(!result.passed);
        assert!(result.checks[0].message.contains("at least"));
    }

    #[test]
    fn eval_custom_assertion() {
        let tc = TaskContracts::new("task").custom("no_orphans");
        let mut ev = Evidence::new();
        ev.record("pass.no_orphans", 1.0);

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(result.passed);
    }

    #[test]
    fn eval_custom_assertion_fails() {
        let tc = TaskContracts::new("task").custom("no_orphans");
        let mut ev = Evidence::new();
        ev.record("pass.no_orphans", 0.0);

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(!result.passed);
    }

    #[test]
    fn eval_unique_passes() {
        let tc = TaskContracts::new("task").unique(vec!["id".to_string()]);
        let mut ev = Evidence::new();
        ev.record("duplicate_count", 0.0);

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(result.passed);
    }

    #[test]
    fn eval_unique_fails() {
        let tc = TaskContracts::new("task").unique(vec!["id".to_string()]);
        let mut ev = Evidence::new();
        ev.record("duplicate_count", 5.0);

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(!result.passed);
        assert!(result.checks[0].message.contains("5 duplicates"));
    }

    #[test]
    fn eval_row_count_delta() {
        let mut tc = TaskContracts::new("task");
        tc.checks.push(DataContract {
            name: "row_count_delta".into(),
            check: ContractCheck::RowCountDelta {
                max_percent_change: 0.1,
                allow_decrease: false,
            },
            severity: Severity::Error,
            description: None,
            tags: vec![],
        });

        let mut ev = Evidence::new();
        ev.record("row_count_delta_pct", 0.05); // 5% increase

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(result.passed);
    }

    #[test]
    fn eval_row_count_delta_decrease_blocked() {
        let mut tc = TaskContracts::new("task");
        tc.checks.push(DataContract {
            name: "row_count_delta".into(),
            check: ContractCheck::RowCountDelta {
                max_percent_change: 0.1,
                allow_decrease: false,
            },
            severity: Severity::Error,
            description: None,
            tags: vec![],
        });

        let mut ev = Evidence::new();
        ev.record("row_count_delta_pct", -0.05); // 5% decrease

        let result = ContractEvaluator::evaluate(&tc, &ev);
        assert!(!result.passed);
        assert!(result.checks[0].message.contains("decreased"));
    }

    #[test]
    fn validation_result_passes_when_no_errors() {
        let checks = vec![
            CheckResult {
                contract_name: "row_count".into(),
                passed: true,
                message: "OK".into(),
                severity: Severity::Error,
                measured_value: Some(1000.0),
                expected: None,
            },
            CheckResult {
                contract_name: "docs".into(),
                passed: false,
                message: "2 undocumented columns".into(),
                severity: Severity::Warning,
                measured_value: None,
                expected: None,
            },
        ];

        let result = ValidationResult::from_checks("my_task", None, checks);
        assert!(result.passed); // Warning doesn't block
        assert_eq!(result.error_count, 0);
        assert_eq!(result.warning_count, 1);
    }

    #[test]
    fn validation_result_fails_on_error() {
        let checks = vec![CheckResult {
            contract_name: "row_count".into(),
            passed: false,
            message: "0 rows, expected at least 1".into(),
            severity: Severity::Error,
            measured_value: Some(0.0),
            expected: Some("min=1".into()),
        }];

        let result = ValidationResult::from_checks("my_task", None, checks);
        assert!(!result.passed);
        assert_eq!(result.error_count, 1);
    }

    #[test]
    fn deployment_validation_blocks_on_error() {
        let r1 = ValidationResult::from_checks(
            "task_a",
            None,
            vec![CheckResult {
                contract_name: "ok".into(),
                passed: true,
                message: "OK".into(),
                severity: Severity::Error,
                measured_value: None,
                expected: None,
            }],
        );
        let r2 = ValidationResult::from_checks(
            "task_b",
            None,
            vec![CheckResult {
                contract_name: "bad".into(),
                passed: false,
                message: "fail".into(),
                severity: Severity::Error,
                measured_value: None,
                expected: None,
            }],
        );

        let dv = DeploymentValidation::from_results(vec![r1, r2]);
        assert!(!dv.can_deploy);
        assert_eq!(dv.total_errors, 1);
    }

    #[test]
    fn deployment_validation_allows_warnings_only() {
        let r1 = ValidationResult::from_checks(
            "task_a",
            None,
            vec![
                CheckResult {
                    contract_name: "count".into(),
                    passed: true,
                    message: "OK".into(),
                    severity: Severity::Error,
                    measured_value: None,
                    expected: None,
                },
                CheckResult {
                    contract_name: "docs".into(),
                    passed: false,
                    message: "undocumented".into(),
                    severity: Severity::Warning,
                    measured_value: None,
                    expected: None,
                },
            ],
        );

        let dv = DeploymentValidation::from_results(vec![r1]);
        assert!(dv.can_deploy);
        assert_eq!(dv.total_warnings, 1);
    }

    #[test]
    fn display_format() {
        let result = ValidationResult::from_checks(
            "extract",
            Some("etl".into()),
            vec![
                CheckResult {
                    contract_name: "row_count(min=1)".into(),
                    passed: true,
                    message: "OK".into(),
                    severity: Severity::Error,
                    measured_value: Some(5000.0),
                    expected: None,
                },
                CheckResult {
                    contract_name: "freshness(24h)".into(),
                    passed: false,
                    message: "Data is 129600s old, max allowed is 86400s (24h)".into(),
                    severity: Severity::Error,
                    measured_value: None,
                    expected: None,
                },
            ],
        );

        let output = format!("{}", result);
        assert!(output.contains("FAILED"));
        assert!(output.contains("etl.extract"));
        assert!(output.contains("[PASS ]"));
        assert!(output.contains("[ERROR]"));
    }

    #[test]
    fn serde_roundtrip() {
        let tc = TaskContracts::new("my_task")
            .row_count(Some(1), None)
            .freshness("1h")
            .metric("accuracy", Some(0.95), None);

        let json = serde_json::to_string_pretty(&tc).unwrap();
        let restored: TaskContracts = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.checks.len(), 3);
        assert_eq!(restored.task_id, "my_task");
    }

    #[test]
    fn parse_duration() {
        assert_eq!(parse_duration_to_seconds("30s"), Some(30.0));
        assert_eq!(parse_duration_to_seconds("5m"), Some(300.0));
        assert_eq!(parse_duration_to_seconds("24h"), Some(86400.0));
        assert_eq!(parse_duration_to_seconds("2d"), Some(172800.0));
        assert_eq!(parse_duration_to_seconds("1w"), Some(604800.0));
        assert_eq!(parse_duration_to_seconds("bad"), None);
    }
}
