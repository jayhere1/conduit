//! Python wrapper for conduit-lineage
//!
//! Exposes column-level lineage extraction and schema change detection.

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use serde_json::{json, Value};
use conduit_lineage::sql_parser::SqlLineage;
use conduit_lineage::{
    parse_sql_type, CatalogColumn, ColumnType, SqlDialect, SqlLineageExtractor, TableCatalog,
};

/// Extract SQL lineage from a query
///
/// Parses SQL and extracts source tables, output columns, and column-level dependencies.
///
/// Args:
///     sql: SQL query string
///
/// Returns:
///     JSON string with lineage information: {
///       "input_tables": ["table1", "table2"],
///       "output_columns": [...],
///       "column_dependencies": {...}
///     }
#[pyfunction]
pub fn extract_sql_lineage(sql: &str) -> PyResult<String> {
    let lineage = SqlLineageExtractor::extract(sql);
    Ok(lineage_to_json(sql, &lineage).to_string())
}

/// Extract SQL lineage with a table catalog and dialect for precise resolution.
///
/// Unlike [`extract_sql_lineage`], a catalog lets the extractor resolve bare
/// (unqualified) columns to the correct source table, expand `SELECT *`, and
/// propagate lineage through CTEs. The dialect string selects warehouse-specific
/// parsing (e.g. BigQuery `UNNEST`, Snowflake `QUALIFY`).
///
/// Args:
///     sql: SQL query string.
///     catalog_json: JSON object mapping a table name (optionally
///         `"schema.table"`) to its columns. Each column is either a string
///         (the column name) or an object `{"name": ..., "type": ...}`. e.g.
///         `{"orders": ["id", "customer_id"],
///           "public.customers": [{"name": "active", "type": "boolean"}]}`.
///         An empty string, `"null"`, or `"{}"` means "no catalog" — behaves
///         like [`extract_sql_lineage`] but still dialect-aware.
///     dialect: connection/dialect string (e.g. "bigquery", "postgresql",
///         "clickhouse"); unknown values fall back to the generic dialect.
///
/// Returns:
///     The same JSON shape as [`extract_sql_lineage`].
#[pyfunction]
pub fn extract_sql_lineage_with_catalog(
    sql: &str,
    catalog_json: &str,
    dialect: &str,
) -> PyResult<String> {
    let catalog = build_catalog(catalog_json)
        .map_err(|e| PyValueError::new_err(format!("Invalid catalog JSON: {}", e)))?;
    let sql_dialect = SqlDialect::from_connection_type(dialect);
    let lineage =
        SqlLineageExtractor::extract_with_catalog_and_dialect(sql, &catalog, sql_dialect);
    Ok(lineage_to_json(sql, &lineage).to_string())
}

/// Serialise a [`SqlLineage`] to the JSON contract shared by both extract
/// entry points: `{sql, input_tables, output_columns[], column_dependencies[]}`.
fn lineage_to_json(sql: &str, lineage: &SqlLineage) -> Value {
    // output column name → its source column references ("table.column")
    let source_map: HashMap<&str, Vec<String>> = lineage
        .column_mappings
        .iter()
        .map(|mapping| {
            let sources: Vec<String> = mapping
                .inputs
                .iter()
                .map(|col_ref| col_ref.to_string())
                .collect();
            (mapping.output.as_str(), sources)
        })
        .collect();

    json!({
        "sql": sql,
        "input_tables": lineage.source_tables.iter().map(|t| &t.name).collect::<Vec<_>>(),
        "output_columns": lineage.output_columns.iter().map(|col| {
            let sources = source_map.get(col.name.as_str()).cloned().unwrap_or_default();
            json!({
                "name": col.name,
                "expression": col.expression,
                "is_computed": col.is_computed,
                "sources": sources
            })
        }).collect::<Vec<_>>(),
        "column_dependencies": lineage.column_mappings.iter().map(|mapping| {
            json!({
                "output": mapping.output,
                "sources": mapping.inputs.iter().map(|col_ref| col_ref.to_string()).collect::<Vec<_>>()
            })
        }).collect::<Vec<_>>()
    })
}

/// Build a [`TableCatalog`] from the JSON passed by Python callers. Tolerant by
/// design: an empty/`null` payload yields an empty catalog, non-object payloads
/// and unparseable column entries are skipped, and `"schema.table"` keys are
/// split into `(schema, table)`.
fn build_catalog(catalog_json: &str) -> Result<TableCatalog, serde_json::Error> {
    let mut catalog = TableCatalog::new();
    let trimmed = catalog_json.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Ok(catalog);
    }

    let value: Value = serde_json::from_str(trimmed)?;
    let Some(obj) = value.as_object() else {
        return Ok(catalog);
    };

    for (table_key, cols_value) in obj {
        let (schema, table) = split_table_key(table_key);
        if table.is_empty() {
            continue;
        }
        let columns = parse_catalog_columns(cols_value);
        catalog.register_table(schema.as_deref(), &table, columns);
    }
    Ok(catalog)
}

/// Parse a column-list value into [`CatalogColumn`]s. Accepts both
/// `["id", "name"]` and `[{"name": "id", "type": "int"}]` forms; unrecognised
/// entries are skipped. Types are best-effort — lineage resolution only needs
/// column names, so a missing/unknown type maps to [`ColumnType::Unknown`].
fn parse_catalog_columns(value: &Value) -> Vec<CatalogColumn> {
    let Some(arr) = value.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|col| match col {
            Value::String(name) => Some(CatalogColumn::new(name, ColumnType::Unknown)),
            Value::Object(map) => {
                let name = map
                    .get("name")
                    .or_else(|| map.get("column"))
                    .or_else(|| map.get("column_name"))
                    .and_then(|v| v.as_str())?;
                let data_type = map
                    .get("type")
                    .or_else(|| map.get("data_type"))
                    .and_then(|v| v.as_str())
                    .map(parse_sql_type)
                    .unwrap_or(ColumnType::Unknown);
                Some(CatalogColumn::new(name, data_type))
            }
            _ => None,
        })
        .collect()
}

/// Split a catalog key (`"schema.table"`, `"db.schema.table"`, or `"table"`)
/// into `(schema, table)`, mirroring the crate's two-tier catalog keying.
fn split_table_key(key: &str) -> (Option<String>, String) {
    let parts: Vec<&str> = key.split('.').filter(|p| !p.is_empty()).collect();
    match parts.as_slice() {
        [] => (None, String::new()),
        [t] => (None, (*t).to_string()),
        [.., s, t] => (Some((*s).to_string()), (*t).to_string()),
    }
}

/// Trace column lineage in a direction (upstream or downstream)
///
/// Args:
///     direction: "upstream" or "downstream"
///     task_id: ID of the task to trace from
///     column_name: Name of the column to trace
///     edges_json: JSON representation of the lineage graph edges
///
/// Returns:
///     JSON string with tracing results: {
///       "direction": "upstream" | "downstream",
///       "start_column": "col_name",
///       "trace_path": [...]
///     }
#[pyfunction]
pub fn trace_column(direction: &str, task_id: &str, column_name: &str, edges_json: &str) -> PyResult<String> {
    // Parse the edges JSON
    let edges: Value = serde_json::from_str(edges_json)
        .map_err(|e| PyValueError::new_err(format!("Invalid edges JSON: {}", e)))?;

    // Perform breadth-first traversal
    let mut visited = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::new();
    let mut trace_path = Vec::new();

    queue.push_back((task_id.to_string(), column_name.to_string()));
    visited.insert((task_id.to_string(), column_name.to_string()));

    let is_upstream = direction == "upstream";

    while let Some((current_task, current_col)) = queue.pop_front() {
        trace_path.push(json!({
            "task_id": current_task,
            "column_name": current_col
        }));

        // Search edges for related columns
        if let Some(edges_arr) = edges.as_array() {
            for edge in edges_arr {
                let should_follow = if is_upstream {
                    // For upstream, we're looking for edges where current is the destination
                    edge.get("to_task").and_then(|v| v.as_str()) == Some(&current_task) &&
                    edge.get("to_column").and_then(|v| v.as_str()) == Some(&current_col)
                } else {
                    // For downstream, current is the source
                    edge.get("from_task").and_then(|v| v.as_str()) == Some(&current_task) &&
                    edge.get("from_column").and_then(|v| v.as_str()) == Some(&current_col)
                };

                if should_follow {
                    let next_task = if is_upstream {
                        edge.get("from_task").and_then(|v| v.as_str()).unwrap_or("").to_string()
                    } else {
                        edge.get("to_task").and_then(|v| v.as_str()).unwrap_or("").to_string()
                    };

                    let next_col = if is_upstream {
                        edge.get("from_column").and_then(|v| v.as_str()).unwrap_or("").to_string()
                    } else {
                        edge.get("to_column").and_then(|v| v.as_str()).unwrap_or("").to_string()
                    };

                    let key = (next_task.clone(), next_col.clone());
                    if !visited.contains(&key) {
                        visited.insert(key);
                        queue.push_back((next_task, next_col));
                    }
                }
            }
        }
    }

    let result = json!({
        "direction": direction,
        "start_column": format!("{}.{}", task_id, column_name),
        "trace_path": trace_path,
        "path_length": trace_path.len()
    });

    Ok(result.to_string())
}

/// Detect and describe schema differences between two schemas
///
/// Args:
///     old_json: Previous schema as JSON
///     new_json: Current schema as JSON
///
/// Returns:
///     JSON string with schema changes: {
///       "added_columns": [...],
///       "removed_columns": [...],
///       "modified_columns": [...],
///       "breaking_changes": [...]
///     }
#[pyfunction]
pub fn diff_schemas(old_json: &str, new_json: &str) -> PyResult<String> {
    let old_schema: Value = serde_json::from_str(old_json)
        .map_err(|e| PyValueError::new_err(format!("Invalid old schema JSON: {}", e)))?;

    let new_schema: Value = serde_json::from_str(new_json)
        .map_err(|e| PyValueError::new_err(format!("Invalid new schema JSON: {}", e)))?;

    let empty_vec = vec![];
    let old_cols = old_schema.get("columns")
        .and_then(|v| v.as_array())
        .unwrap_or(&empty_vec);

    let empty_vec2 = vec![];
    let new_cols = new_schema.get("columns")
        .and_then(|v| v.as_array())
        .unwrap_or(&empty_vec2);

    let mut added_columns = Vec::new();
    let mut removed_columns = Vec::new();
    let mut modified_columns = Vec::new();
    let mut breaking_changes = Vec::new();

    // Build old column map
    let mut old_col_map: std::collections::HashMap<String, &Value> = std::collections::HashMap::new();
    for col in old_cols {
        if let Some(name) = col.get("name").and_then(|v| v.as_str()) {
            old_col_map.insert(name.to_string(), col);
        }
    }

    // Check for added and modified columns
    for new_col in new_cols {
        if let Some(col_name) = new_col.get("name").and_then(|v| v.as_str()) {
            if let Some(old_col) = old_col_map.get(col_name) {
                // Column exists in both - check for type changes
                let old_type = old_col.get("type").and_then(|v| v.as_str()).unwrap_or("");
                let new_type = new_col.get("type").and_then(|v| v.as_str()).unwrap_or("");

                if old_type != new_type {
                    // Type change is a breaking change
                    breaking_changes.push(json!({
                        "column": col_name,
                        "change": "type_changed",
                        "old_type": old_type,
                        "new_type": new_type
                    }));

                    modified_columns.push(json!({
                        "name": col_name,
                        "old_type": old_type,
                        "new_type": new_type
                    }));
                }

                // Check for nullable changes
                let old_nullable = old_col.get("nullable").and_then(|v| v.as_bool()).unwrap_or(false);
                let new_nullable = new_col.get("nullable").and_then(|v| v.as_bool()).unwrap_or(false);

                if !old_nullable && new_nullable {
                    // Making non-null column nullable is safe
                } else if old_nullable && !new_nullable {
                    // Making nullable column non-null is breaking
                    breaking_changes.push(json!({
                        "column": col_name,
                        "change": "made_non_nullable",
                        "reason": "may contain null values"
                    }));
                }
            } else {
                // New column
                added_columns.push(json!({
                    "name": col_name,
                    "type": new_col.get("type").and_then(|v| v.as_str()).unwrap_or("unknown"),
                    "nullable": new_col.get("nullable").and_then(|v| v.as_bool()).unwrap_or(false)
                }));
            }
        }
    }

    // Check for removed columns
    for col in old_cols {
        if let Some(col_name) = col.get("name").and_then(|v| v.as_str()) {
            let exists_in_new = new_cols.iter()
                .any(|c| c.get("name").and_then(|v| v.as_str()) == Some(col_name));

            if !exists_in_new {
                removed_columns.push(json!({
                    "name": col_name,
                    "type": col.get("type").and_then(|v| v.as_str()).unwrap_or("unknown")
                }));

                // Removed columns are breaking changes
                breaking_changes.push(json!({
                    "column": col_name,
                    "change": "removed",
                    "reason": "downstream tasks may depend on this column"
                }));
            }
        }
    }

    let result = json!({
        "added_columns": added_columns,
        "removed_columns": removed_columns,
        "modified_columns": modified_columns,
        "breaking_changes": breaking_changes,
        "is_breaking": !breaking_changes.is_empty()
    });

    Ok(result.to_string())
}

/// Create the lineage submodule for Python
pub fn create_module(py: Python) -> PyResult<Bound<PyModule>> {
    let module = PyModule::new_bound(py, "lineage")?;
    module.add_function(wrap_pyfunction!(extract_sql_lineage, &module)?)?;
    module.add_function(wrap_pyfunction!(extract_sql_lineage_with_catalog, &module)?)?;
    module.add_function(wrap_pyfunction!(trace_column, &module)?)?;
    module.add_function(wrap_pyfunction!(diff_schemas, &module)?)?;
    module.add("__doc__", "Column-level lineage and schema change detection module")?;
    Ok(module)
}
