//! YAML workflow parser.
//!
//! Parses DAG definitions from YAML files as an alternative to Python.
//! YAML workflows are simpler and more declarative — ideal for pipelines
//! that are mostly configuration-driven (SQL queries, shell commands, sensors).
//!
//! Example YAML DAG:
//! ```yaml
//! id: daily_etl
//! description: Daily ETL pipeline
//! schedule: "0 6 * * *"
//! tags: [etl, warehouse]
//!
//! tasks:
//!   extract_orders:
//!     type: sql
//!     connection: warehouse
//!     query: "SELECT * FROM source.orders WHERE date = '{{ ds }}'"
//!     retries: 3
//!     timeout: 30m
//!     pool: extract_pool
//!
//!   extract_customers:
//!     type: shell
//!     command: "python scripts/extract_customers.py --date {{ ds }}"
//!     retries: 2
//!
//!   transform:
//!     type: python
//!     module: transforms.etl
//!     function: run_transform
//!     depends_on: [extract_orders, extract_customers]
//!     pool: transform_pool
//!     timeout: 1h
//!     incremental:
//!       strategy: append
//!       time_column: updated_at
//!       lookback: 2h
//!
//!   load:
//!     type: sql
//!     connection: warehouse
//!     query: "INSERT INTO target.orders SELECT * FROM staging.orders"
//!     depends_on: [transform]
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use conduit_common::contracts::{ContractCheck, DataContract, Severity, TaskContracts};
use conduit_common::dag::*;
use conduit_common::error::{ConduitError, ConduitResult};
use conduit_common::incremental::{IncrementalConfig, IncrementalStrategy, PartitionGranularity};

use crate::parser::{ParsedDag, ParsedTask};

// ─── YAML schema types ──────────────────────────────────────────────────────

/// A DAG definition as represented in YAML.
#[derive(Debug, Deserialize, Serialize)]
pub struct YamlDag {
    /// Unique DAG identifier.
    pub id: String,

    /// Human-readable description.
    #[serde(default)]
    pub description: Option<String>,

    /// Cron schedule expression.
    #[serde(default)]
    pub schedule: Option<String>,

    /// Tags for filtering.
    #[serde(default)]
    pub tags: Vec<String>,

    /// Maximum concurrent runs.
    #[serde(default = "default_max_active_runs")]
    pub max_active_runs: u32,

    /// Failure webhook.
    #[serde(default)]
    pub on_failure: Option<String>,

    /// Task definitions.
    pub tasks: HashMap<String, YamlTask>,
}

fn default_max_active_runs() -> u32 {
    1
}

/// A task definition as represented in YAML.
#[derive(Debug, Deserialize, Serialize)]
pub struct YamlTask {
    /// Task type: python, shell, sql, sensor, executable.
    #[serde(rename = "type")]
    pub task_type: String,

    // ── Type-specific fields ─────────────────────────────────
    /// Python module (for type: python).
    #[serde(default)]
    pub module: Option<String>,

    /// Python function (for type: python).
    #[serde(default)]
    pub function: Option<String>,

    /// Shell command (for type: shell).
    #[serde(default)]
    pub command: Option<String>,

    /// SQL connection name (for type: sql).
    #[serde(default)]
    pub connection: Option<String>,

    /// SQL query (for type: sql).
    #[serde(default)]
    pub query: Option<String>,

    /// Sensor type (for type: sensor).
    #[serde(default)]
    pub sensor_type: Option<String>,

    /// Poke interval for sensors.
    #[serde(default)]
    pub poke_interval: Option<String>,

    /// Executable args (for type: executable).
    #[serde(default)]
    pub args: Vec<String>,

    // ── Common fields ────────────────────────────────────────
    /// Task dependencies (by task ID).
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Number of retries.
    #[serde(default)]
    pub retries: u32,

    /// Delay between retries.
    #[serde(default)]
    pub retry_delay: Option<String>,

    /// Resource pool name.
    #[serde(default)]
    pub pool: Option<String>,

    /// Execution timeout.
    #[serde(default)]
    pub timeout: Option<String>,

    /// Task priority.
    #[serde(default)]
    pub priority: i32,

    /// Trigger rule.
    #[serde(default)]
    pub trigger_rule: Option<String>,

    /// Resource limits.
    #[serde(default)]
    pub resources: Option<YamlResourceLimits>,

    /// Incremental configuration.
    #[serde(default)]
    pub incremental: Option<YamlIncrementalConfig>,

    /// Data quality contracts.
    #[serde(default)]
    pub contracts: Vec<YamlContract>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct YamlResourceLimits {
    #[serde(default)]
    pub cpu_millicores: Option<u32>,
    #[serde(default)]
    pub memory_mb: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct YamlIncrementalConfig {
    pub strategy: String,
    #[serde(default)]
    pub time_column: Option<String>,
    #[serde(default)]
    pub lookback: Option<String>,
    #[serde(default)]
    pub unique_key: Vec<String>,
    #[serde(default)]
    pub partition_column: Option<String>,
    #[serde(default)]
    pub partition_granularity: Option<String>,
    #[serde(default)]
    pub check_columns: Vec<String>,
    #[serde(default)]
    pub scd_type_2: bool,
    #[serde(default)]
    pub batch_size: Option<u64>,
    #[serde(default)]
    pub max_partitions_per_run: Option<u32>,
    #[serde(default)]
    pub invalidate_hard_deletes: bool,
}

/// A data quality contract as represented in YAML.
///
/// Evidence-based: contracts assert against metrics emitted by the task.
/// The task emits measurements via `CONDUIT::METRIC::name::value`, and
/// contracts validate those measurements.
///
/// Example:
/// ```yaml
/// contracts:
///   - type: row_count
///     min: 1
///   - type: freshness
///     max_age: 24h
///   - type: unique
///     columns: [id]
///   - type: not_null
///     column: customer_id
///     min_rate: 0.99
///   - type: metric
///     metric_name: accuracy
///     min: 0.95
///   - type: custom
///     assertion_name: no_orphans
/// ```
#[derive(Debug, Deserialize, Serialize)]
pub struct YamlContract {
    /// Contract check type.
    #[serde(rename = "type")]
    pub check_type: String,

    /// Human-readable name (optional, auto-generated if missing).
    #[serde(default)]
    pub name: Option<String>,

    /// Severity: "error" (default) or "warning".
    #[serde(default)]
    pub severity: Option<String>,

    /// Description of why this contract exists.
    #[serde(default)]
    pub description: Option<String>,

    // ── Type-specific fields ─────────────────────────────────
    #[serde(default)]
    pub min: Option<f64>,
    #[serde(default)]
    pub max: Option<f64>,
    #[serde(default)]
    pub exact: Option<u64>,
    #[serde(default)]
    pub column: Option<String>,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub max_age: Option<String>,
    #[serde(default)]
    pub min_rate: Option<f64>,
    #[serde(default)]
    pub values: Vec<String>,
    #[serde(default)]
    pub allow_null: bool,
    #[serde(default)]
    pub ref_task: Option<String>,
    #[serde(default)]
    pub ref_column: Option<String>,
    #[serde(default)]
    pub max_percent_change: Option<f64>,
    #[serde(default)]
    pub allow_decrease: bool,
    /// Generic metric name (for type: metric).
    #[serde(default)]
    pub metric_name: Option<String>,
    /// Custom assertion name (for type: custom).
    #[serde(default)]
    pub assertion_name: Option<String>,
    /// Exact value for metric (for type: metric).
    #[serde(default)]
    pub exact_value: Option<f64>,
}

// ─── YAML Parser ────────────────────────────────────────────────────────────

/// Parser for YAML workflow definitions.
pub struct YamlDagParser;

impl YamlDagParser {
    /// Parse a single YAML file into a ParsedDag.
    pub fn parse_file(path: &Path) -> ConduitResult<ParsedDag> {
        let content = std::fs::read_to_string(path).map_err(|e| ConduitError::ParseError {
            file: path.display().to_string(),
            message: format!("Failed to read file: {}", e),
        })?;

        Self::parse_string(&content, path)
    }

    /// Parse a YAML string into a ParsedDag.
    pub fn parse_string(content: &str, source_path: &Path) -> ConduitResult<ParsedDag> {
        let yaml_dag: YamlDag =
            serde_yaml::from_str(content).map_err(|e| ConduitError::ParseError {
                file: source_path.display().to_string(),
                message: format!("YAML parse error: {}", e),
            })?;

        Self::convert_to_parsed_dag(yaml_dag, source_path)
    }

    /// Scan a directory for .yaml and .yml files and parse all DAGs.
    pub fn parse_directory(dir: &Path) -> ConduitResult<Vec<ParsedDag>> {
        let mut dags = Vec::new();

        if !dir.exists() {
            return Ok(dags);
        }

        let entries: Vec<PathBuf> = Self::find_yaml_files(dir)?;

        info!(
            files = entries.len(),
            dir = %dir.display(),
            "Scanning YAML workflow files"
        );

        for path in entries {
            match Self::parse_file(&path) {
                Ok(dag) => {
                    debug!(dag_id = %dag.id, file = %path.display(), "Parsed YAML DAG");
                    dags.push(dag);
                }
                Err(e) => {
                    warn!(file = %path.display(), error = %e, "Failed to parse YAML DAG");
                }
            }
        }

        Ok(dags)
    }

    /// Recursively find all .yaml and .yml files in a directory.
    fn find_yaml_files(dir: &Path) -> ConduitResult<Vec<PathBuf>> {
        let mut files = Vec::new();

        let entries = std::fs::read_dir(dir).map_err(|e| ConduitError::ParseError {
            file: dir.display().to_string(),
            message: format!("Failed to read directory: {}", e),
        })?;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(Self::find_yaml_files(&path)?);
            } else if let Some(ext) = path.extension() {
                let ext = ext.to_string_lossy().to_lowercase();
                if ext == "yaml" || ext == "yml" {
                    // Skip conduit.yaml config file
                    if path
                        .file_name()
                        .is_some_and(|n| n == "conduit.yaml" || n == "conduit.yml")
                    {
                        continue;
                    }
                    files.push(path);
                }
            }
        }

        Ok(files)
    }

    /// Convert a YamlDag into the compiler's ParsedDag format.
    fn convert_to_parsed_dag(yaml: YamlDag, source_path: &Path) -> ConduitResult<ParsedDag> {
        let mut tasks = Vec::new();

        for (task_id, yaml_task) in &yaml.tasks {
            let task_type = Self::resolve_task_type(task_id, yaml_task, source_path)?;

            // Resolve contracts if any
            let contracts = if yaml_task.contracts.is_empty() {
                None
            } else {
                Some(Self::resolve_contracts(
                    task_id,
                    &yaml.id,
                    &yaml_task.contracts,
                )?)
            };

            tasks.push(ParsedTask {
                id: task_id.clone(),
                task_type,
                retries: yaml_task.retries,
                retry_delay: yaml_task.retry_delay.clone(),
                pool: yaml_task.pool.clone(),
                timeout: yaml_task.timeout.clone(),
                priority: yaml_task.priority,
                raw_dependencies: yaml_task.depends_on.clone(),
                contracts,
                parameters_text: String::new(),
            });
        }

        Ok(ParsedDag {
            id: yaml.id.clone(),
            description: yaml.description,
            schedule: yaml.schedule,
            tags: yaml.tags,
            max_active_runs: yaml.max_active_runs,
            on_failure: yaml.on_failure,
            tasks,
            source_file: source_path.display().to_string(),
        })
    }

    /// Resolve the TaskType from YAML task fields.
    fn resolve_task_type(
        task_id: &str,
        task: &YamlTask,
        source_path: &Path,
    ) -> ConduitResult<TaskType> {
        match task.task_type.to_lowercase().as_str() {
            "python" => {
                let module = task.module.clone().unwrap_or_else(|| "tasks".to_string());
                let function = task
                    .function
                    .clone()
                    .unwrap_or_else(|| task_id.to_string());
                Ok(TaskType::Python { module, function })
            }

            "shell" | "bash" => {
                let command = task.command.clone().ok_or_else(|| ConduitError::ParseError {
                    file: source_path.display().to_string(),
                    message: format!("Task '{}': shell task requires 'command' field", task_id),
                })?;
                Ok(TaskType::Bash { command })
            }

            "sql" => {
                let connection = task
                    .connection
                    .clone()
                    .unwrap_or_else(|| "default".to_string());
                let query = task.query.clone().ok_or_else(|| ConduitError::ParseError {
                    file: source_path.display().to_string(),
                    message: format!("Task '{}': sql task requires 'query' field", task_id),
                })?;
                Ok(TaskType::Sql { connection, query })
            }

            "sensor" => {
                let sensor_type = task
                    .sensor_type
                    .clone()
                    .unwrap_or_else(|| "file".to_string());
                Ok(TaskType::Sensor {
                    sensor_type,
                    poke_interval: task.poke_interval.clone(),
                })
            }

            "executable" | "exec" => {
                let command = task.command.clone().ok_or_else(|| ConduitError::ParseError {
                    file: source_path.display().to_string(),
                    message: format!(
                        "Task '{}': executable task requires 'command' field",
                        task_id
                    ),
                })?;
                Ok(TaskType::Executable {
                    command,
                    args: task.args.clone(),
                })
            }

            other => Err(ConduitError::ParseError {
                file: source_path.display().to_string(),
                message: format!(
                    "Task '{}': unknown task type '{}'. Expected: python, shell, sql, sensor, executable",
                    task_id, other
                ),
            }),
        }
    }

    /// Convert a YAML incremental config into the common type.
    /// This is used by the resolver after the DAG has been parsed.
    pub fn resolve_incremental_config(
        yaml: &YamlIncrementalConfig,
    ) -> ConduitResult<IncrementalConfig> {
        let strategy = match yaml.strategy.to_lowercase().as_str() {
            "full_refresh" | "full" => IncrementalStrategy::FullRefresh,

            "append" => {
                let time_column = yaml
                    .time_column
                    .clone()
                    .unwrap_or_else(|| "created_at".to_string());
                IncrementalStrategy::Append {
                    time_column,
                    lookback: yaml.lookback.clone(),
                }
            }

            "merge" | "merge_on_key" | "upsert" => IncrementalStrategy::MergeOnKey {
                unique_key: yaml.unique_key.clone(),
                time_column: yaml.time_column.clone(),
                invalidate_hard_deletes: yaml.invalidate_hard_deletes,
            },

            "delete_insert" | "delete+insert" => {
                let partition_column = yaml
                    .partition_column
                    .clone()
                    .unwrap_or_else(|| "dt".to_string());
                let granularity = match yaml
                    .partition_granularity
                    .as_deref()
                    .unwrap_or("day")
                    .to_lowercase()
                    .as_str()
                {
                    "hour" => PartitionGranularity::Hour,
                    "day" => PartitionGranularity::Day,
                    "week" => PartitionGranularity::Week,
                    "month" => PartitionGranularity::Month,
                    "year" => PartitionGranularity::Year,
                    _ => PartitionGranularity::Day,
                };
                IncrementalStrategy::DeleteInsert {
                    partition_column,
                    partition_granularity: granularity,
                    time_column: yaml.time_column.clone(),
                }
            }

            "snapshot" | "snapshot_diff" | "scd" => IncrementalStrategy::SnapshotDiff {
                unique_key: yaml.unique_key.clone(),
                check_columns: yaml.check_columns.clone(),
                scd_type_2: yaml.scd_type_2,
                valid_from_column: None,
                valid_to_column: None,
            },

            other => {
                return Err(ConduitError::ConfigError(format!(
                    "Unknown incremental strategy: '{}'. Expected: full_refresh, append, merge, delete_insert, snapshot",
                    other
                )));
            }
        };

        Ok(IncrementalConfig {
            strategy,
            allow_full_refresh: true,
            force_full_refresh: false,
            batch_size: yaml.batch_size,
            max_partitions_per_run: yaml.max_partitions_per_run,
            emit_watermark: true,
        })
    }

    /// Convert YAML contract declarations into TaskContracts.
    ///
    /// Evidence-based: each contract type maps to specific metrics that the
    /// task must emit via `CONDUIT::METRIC::name::value`.
    fn resolve_contracts(
        task_id: &str,
        dag_id: &str,
        yaml_contracts: &[YamlContract],
    ) -> ConduitResult<TaskContracts> {
        let mut tc = TaskContracts::new(task_id).with_dag(dag_id);

        for yc in yaml_contracts {
            let severity = match yc.severity.as_deref() {
                Some("warning") | Some("warn") => Severity::Warning,
                _ => Severity::Error,
            };

            let check = match yc.check_type.to_lowercase().as_str() {
                "row_count" | "rowcount" => ContractCheck::RowCount {
                    min: yc.min.map(|v| v as u64),
                    max: yc.max.map(|v| v as u64),
                    exact: yc.exact,
                },

                "freshness" => {
                    let max_age = yc.max_age.clone().ok_or_else(|| {
                        ConduitError::ConfigError(format!(
                            "Task '{}': freshness contract requires 'max_age'",
                            task_id
                        ))
                    })?;
                    ContractCheck::Freshness { max_age }
                }

                "unique" | "uniqueness" => {
                    let columns = if yc.columns.is_empty() {
                        yc.column.iter().cloned().collect()
                    } else {
                        yc.columns.clone()
                    };
                    ContractCheck::Unique { columns }
                }

                "not_null" | "notnull" | "not_null_rate" => {
                    let column = yc.column.clone().ok_or_else(|| {
                        ConduitError::ConfigError(format!(
                            "Task '{}': not_null contract requires 'column'",
                            task_id
                        ))
                    })?;
                    ContractCheck::NotNullRate {
                        column,
                        min_rate: yc.min_rate.unwrap_or(1.0),
                    }
                }

                "accepted_values" | "values" => {
                    let column = yc.column.clone().ok_or_else(|| {
                        ConduitError::ConfigError(format!(
                            "Task '{}': accepted_values contract requires 'column'",
                            task_id
                        ))
                    })?;
                    ContractCheck::AcceptedValues {
                        column,
                        values: yc.values.clone(),
                        allow_null: yc.allow_null,
                    }
                }

                "value_range" | "range" => {
                    let column = yc.column.clone().ok_or_else(|| {
                        ConduitError::ConfigError(format!(
                            "Task '{}': value_range contract requires 'column'",
                            task_id
                        ))
                    })?;
                    ContractCheck::ValueRange {
                        column,
                        min: yc.min,
                        max: yc.max,
                    }
                }

                "references" | "referential_integrity" | "fk" => {
                    let column = yc.column.clone().ok_or_else(|| {
                        ConduitError::ConfigError(format!(
                            "Task '{}': references contract requires 'column'",
                            task_id
                        ))
                    })?;
                    let ref_task = yc.ref_task.clone().ok_or_else(|| {
                        ConduitError::ConfigError(format!(
                            "Task '{}': references contract requires 'ref_task'",
                            task_id
                        ))
                    })?;
                    let ref_column = yc.ref_column.clone().ok_or_else(|| {
                        ConduitError::ConfigError(format!(
                            "Task '{}': references contract requires 'ref_column'",
                            task_id
                        ))
                    })?;
                    ContractCheck::ReferentialIntegrity {
                        column,
                        ref_task,
                        ref_column,
                    }
                }

                "row_count_delta" | "delta" => ContractCheck::RowCountDelta {
                    max_percent_change: yc.max_percent_change.unwrap_or(0.5),
                    allow_decrease: yc.allow_decrease,
                },

                "metric" => {
                    let metric_name = yc.metric_name.clone().ok_or_else(|| {
                        ConduitError::ConfigError(format!(
                            "Task '{}': metric contract requires 'metric_name'",
                            task_id
                        ))
                    })?;
                    ContractCheck::Metric {
                        metric_name,
                        min: yc.min,
                        max: yc.max,
                        exact: yc.exact_value,
                    }
                }

                "custom" => {
                    let assertion_name = yc.assertion_name.clone().ok_or_else(|| {
                        ConduitError::ConfigError(format!(
                            "Task '{}': custom contract requires 'assertion_name'",
                            task_id
                        ))
                    })?;
                    ContractCheck::Custom { assertion_name }
                }

                other => {
                    return Err(ConduitError::ConfigError(format!(
                        "Task '{}': unknown contract type '{}'. Expected: row_count, freshness, unique, \
                         not_null, accepted_values, value_range, references, \
                         row_count_delta, metric, custom",
                        task_id, other
                    )));
                }
            };

            let name = yc
                .name
                .clone()
                .unwrap_or_else(|| format!("{}:{}", yc.check_type, task_id));

            tc.checks.push(DataContract {
                name,
                check,
                severity,
                description: yc.description.clone(),
                tags: vec![],
            });
        }

        Ok(tc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_yaml_dag() {
        let yaml = r#"
id: test_pipeline
description: A test pipeline
schedule: "0 6 * * *"
tags: [test, example]

tasks:
  extract:
    type: sql
    connection: warehouse
    query: "SELECT * FROM source.orders"
    retries: 3
    timeout: 30m

  transform:
    type: python
    module: transforms.etl
    function: run_transform
    depends_on: [extract]
    pool: transform_pool

  load:
    type: shell
    command: "python load.py"
    depends_on: [transform]
"#;

        let dag = YamlDagParser::parse_string(yaml, Path::new("test.yaml")).unwrap();
        assert_eq!(dag.id, "test_pipeline");
        assert_eq!(dag.tasks.len(), 3);
        assert_eq!(dag.schedule, Some("0 6 * * *".to_string()));
        assert_eq!(dag.tags, vec!["test", "example"]);

        // Check task types
        let extract = dag.tasks.iter().find(|t| t.id == "extract").unwrap();
        assert!(matches!(extract.task_type, TaskType::Sql { .. }));
        assert_eq!(extract.retries, 3);

        let transform = dag.tasks.iter().find(|t| t.id == "transform").unwrap();
        assert!(matches!(transform.task_type, TaskType::Python { .. }));
        assert_eq!(transform.raw_dependencies, vec!["extract"]);
    }

    #[test]
    fn parse_all_task_types() {
        let yaml = r#"
id: multi_type
tasks:
  py_task:
    type: python
    module: my_module
    function: my_func
  shell_task:
    type: shell
    command: "echo hello"
  sql_task:
    type: sql
    query: "SELECT 1"
  sensor_task:
    type: sensor
    sensor_type: file
    poke_interval: "30s"
  exec_task:
    type: executable
    command: "/usr/bin/process"
    args: ["--flag", "value"]
"#;

        let dag = YamlDagParser::parse_string(yaml, Path::new("test.yaml")).unwrap();
        assert_eq!(dag.tasks.len(), 5);
    }

    #[test]
    fn unknown_task_type_returns_error() {
        let yaml = r#"
id: bad_dag
tasks:
  broken:
    type: nonexistent
"#;

        let result = YamlDagParser::parse_string(yaml, Path::new("test.yaml"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unknown task type"));
    }

    #[test]
    fn missing_required_field_returns_error() {
        let yaml = r#"
id: bad_dag
tasks:
  no_command:
    type: shell
"#;

        let result = YamlDagParser::parse_string(yaml, Path::new("test.yaml"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires 'command'"));
    }

    #[test]
    fn parse_incremental_config() {
        let yaml_inc = YamlIncrementalConfig {
            strategy: "append".to_string(),
            time_column: Some("updated_at".to_string()),
            lookback: Some("2h".to_string()),
            unique_key: vec![],
            partition_column: None,
            partition_granularity: None,
            check_columns: vec![],
            scd_type_2: false,
            batch_size: Some(5000),
            max_partitions_per_run: None,
            invalidate_hard_deletes: false,
        };

        let config = YamlDagParser::resolve_incremental_config(&yaml_inc).unwrap();
        assert!(matches!(
            config.strategy,
            IncrementalStrategy::Append { .. }
        ));
        assert_eq!(config.batch_size, Some(5000));
    }

    #[test]
    fn parse_merge_on_key_incremental() {
        let yaml_inc = YamlIncrementalConfig {
            strategy: "merge_on_key".to_string(),
            time_column: Some("updated_at".to_string()),
            unique_key: vec!["user_id".to_string()],
            invalidate_hard_deletes: true,
            lookback: None,
            partition_column: None,
            partition_granularity: None,
            check_columns: vec![],
            scd_type_2: false,
            batch_size: None,
            max_partitions_per_run: None,
        };

        let config = YamlDagParser::resolve_incremental_config(&yaml_inc).unwrap();
        match config.strategy {
            IncrementalStrategy::MergeOnKey {
                unique_key,
                invalidate_hard_deletes,
                ..
            } => {
                assert_eq!(unique_key, vec!["user_id"]);
                assert!(invalidate_hard_deletes);
            }
            _ => panic!("Expected MergeOnKey"),
        }
    }

    #[test]
    fn parse_delete_insert_incremental() {
        let yaml_inc = YamlIncrementalConfig {
            strategy: "delete_insert".to_string(),
            partition_column: Some("dt".to_string()),
            partition_granularity: Some("month".to_string()),
            time_column: None,
            unique_key: vec![],
            lookback: None,
            check_columns: vec![],
            scd_type_2: false,
            batch_size: None,
            max_partitions_per_run: Some(12),
            invalidate_hard_deletes: false,
        };

        let config = YamlDagParser::resolve_incremental_config(&yaml_inc).unwrap();
        match config.strategy {
            IncrementalStrategy::DeleteInsert {
                partition_granularity,
                ..
            } => {
                assert_eq!(partition_granularity, PartitionGranularity::Month);
            }
            _ => panic!("Expected DeleteInsert"),
        }
        assert_eq!(config.max_partitions_per_run, Some(12));
    }

    #[test]
    fn parse_yaml_with_incremental_task() {
        let yaml = r#"
id: incremental_pipeline
tasks:
  extract:
    type: sql
    query: "SELECT * FROM orders"
    incremental:
      strategy: append
      time_column: created_at
      lookback: 1h
      batch_size: 10000

  load:
    type: shell
    command: "python load.py"
    depends_on: [extract]
"#;

        let dag = YamlDagParser::parse_string(yaml, Path::new("test.yaml")).unwrap();
        assert_eq!(dag.tasks.len(), 2);
    }

    #[test]
    fn parse_from_directory() {
        let dir = tempfile::tempdir().unwrap();
        let dag_file = dir.path().join("pipeline.yaml");
        std::fs::write(
            &dag_file,
            r#"
id: dir_pipeline
tasks:
  hello:
    type: shell
    command: "echo hello"
"#,
        )
        .unwrap();

        // Also write a conduit.yaml that should be skipped
        std::fs::write(dir.path().join("conduit.yaml"), "project: test").unwrap();

        let dags = YamlDagParser::parse_directory(dir.path()).unwrap();
        assert_eq!(dags.len(), 1);
        assert_eq!(dags[0].id, "dir_pipeline");
    }

    #[test]
    fn defaults_applied_correctly() {
        let yaml = r#"
id: minimal
tasks:
  task1:
    type: python
"#;

        let dag = YamlDagParser::parse_string(yaml, Path::new("test.yaml")).unwrap();
        assert_eq!(dag.max_active_runs, 1);
        assert!(dag.schedule.is_none());
        assert!(dag.tags.is_empty());

        let task = &dag.tasks[0];
        assert_eq!(task.retries, 0);
        assert_eq!(task.priority, 0);
    }
}
