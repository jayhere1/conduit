//! DuckDB provider — embedded analytical database.
//!
//! Uses the `duckdb` crate (C API bindings) for high-performance local SQL
//! execution, Parquet/CSV/JSON file querying, and SQL preview.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   local_analytics:
//!     type: duckdb
//!     database: /data/analytics.duckdb   # file path, or ":memory:"
//!     threads: 4                         # optional
//!     memory_limit: 4GB                  # optional
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use tokio::sync::OnceCell;

use super::extra_str;
use crate::errors::ProviderError;
use crate::traits::*;

pub struct DuckDbProvider {
    name: String,
    database_path: String,
    threads: Option<u64>,
    memory_limit: Option<String>,
    conn: OnceCell<Arc<Mutex<duckdb::Connection>>>,
}

impl DuckDbProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let database_path = config
            .database
            .clone()
            .unwrap_or_else(|| ":memory:".to_string());
        let threads = super::extra_u64(config, "threads");
        let memory_limit = extra_str(config, "memory_limit");

        Ok(Self {
            name: name.to_string(),
            database_path,
            threads,
            memory_limit,
            conn: OnceCell::new(),
        })
    }

    /// Create an ephemeral in-memory DuckDB provider (no config needed).
    pub fn ephemeral() -> Self {
        Self {
            name: "_ephemeral".to_string(),
            database_path: ":memory:".to_string(),
            threads: None,
            memory_limit: None,
            conn: OnceCell::new(),
        }
    }

    /// Lazily initialize the DuckDB connection on first use.
    async fn ensure_connection(&self) -> Result<Arc<Mutex<duckdb::Connection>>, ProviderError> {
        let conn = self
            .conn
            .get_or_try_init(|| {
                let path = self.database_path.clone();
                let threads = self.threads;
                let memory_limit = self.memory_limit.clone();
                let name = self.name.clone();

                async move {
                    tokio::task::spawn_blocking(move || {
                        let conn = duckdb::Connection::open(&path).map_err(|e| {
                            ProviderError::ConnectionFailed {
                                name: name.clone(),
                                reason: e.to_string(),
                            }
                        })?;

                        if let Some(t) = threads {
                            conn.execute_batch(&format!("SET threads = {t}"))
                                .map_err(|e| ProviderError::ConnectionFailed {
                                    name: name.clone(),
                                    reason: format!("failed to set threads: {e}"),
                                })?;
                        }
                        if let Some(ref mem) = memory_limit {
                            conn.execute_batch(&format!("SET memory_limit = '{mem}'"))
                                .map_err(|e| ProviderError::ConnectionFailed {
                                    name: name.clone(),
                                    reason: format!("failed to set memory_limit: {e}"),
                                })?;
                        }

                        Ok::<_, ProviderError>(Arc::new(Mutex::new(conn)))
                    })
                    .await
                    .map_err(|e| ProviderError::ConnectionFailed {
                        name: String::new(),
                        reason: format!("spawn_blocking failed: {e}"),
                    })?
                }
            })
            .await?;

        Ok(Arc::clone(conn))
    }

    /// Execute a SQL statement directly on the connection (for setup like CREATE VIEW).
    pub async fn execute_raw(&self, sql: &str) -> Result<(), ProviderError> {
        let conn = self.ensure_connection().await?;
        let sql = sql.to_string();
        let name = self.name.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| ProviderError::QueryFailed {
                connection: name.clone(),
                reason: format!("lock poisoned: {e}"),
            })?;
            conn.execute_batch(&sql)
                .map_err(|e| ProviderError::QueryFailed {
                    connection: name,
                    reason: e.to_string(),
                })
        })
        .await
        .map_err(|e| ProviderError::QueryFailed {
            connection: self.name.clone(),
            reason: format!("spawn_blocking failed: {e}"),
        })?
    }
}

/// Convert a DuckDB value to serde_json::Value.
fn duckdb_value_to_json(val: duckdb::types::Value) -> serde_json::Value {
    use duckdb::types::Value;
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(b),
        Value::TinyInt(n) => serde_json::json!(n),
        Value::SmallInt(n) => serde_json::json!(n),
        Value::Int(n) => serde_json::json!(n),
        Value::BigInt(n) => serde_json::json!(n),
        Value::HugeInt(n) => serde_json::json!(n.to_string()),
        Value::UTinyInt(n) => serde_json::json!(n),
        Value::USmallInt(n) => serde_json::json!(n),
        Value::UInt(n) => serde_json::json!(n),
        Value::UBigInt(n) => serde_json::json!(n),
        Value::Float(f) => serde_json::Number::from_f64(f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Double(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Decimal(d) => {
            // Try to represent as a number; fall back to string
            let s = format!("{d}");
            if let Ok(f) = s.parse::<f64>() {
                serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::String(s))
            } else {
                serde_json::Value::String(s)
            }
        }
        Value::Text(s) => serde_json::Value::String(s),
        Value::Blob(b) => serde_json::Value::String(format!("<blob {} bytes>", b.len())),
        Value::Date32(days) => {
            // Days since epoch
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163);
            serde_json::Value::String(
                date.map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| format!("{days}")),
            )
        }
        Value::Timestamp(unit, val) => {
            let micros = match unit {
                duckdb::types::TimeUnit::Second => val * 1_000_000,
                duckdb::types::TimeUnit::Millisecond => val * 1_000,
                duckdb::types::TimeUnit::Microsecond => val,
                duckdb::types::TimeUnit::Nanosecond => val / 1_000,
            };
            let dt = chrono::DateTime::from_timestamp_micros(micros);
            serde_json::Value::String(
                dt.map(|d| d.format("%Y-%m-%d %H:%M:%S%.f").to_string())
                    .unwrap_or_else(|| format!("{val}")),
            )
        }
        Value::Time64(..) | Value::Interval { .. } => serde_json::Value::String(format!("{val:?}")),
        Value::Enum(s) => serde_json::Value::String(s),
        Value::List(items) | Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(duckdb_value_to_json).collect())
        }
        Value::Struct(map) => serde_json::Value::String(format!("{map:?}")),
        _ => serde_json::Value::String(format!("{val:?}")),
    }
}

#[async_trait]
impl Provider for DuckDbProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "duckdb".to_string(),
            display_name: format!(
                "DuckDB ({})",
                if self.database_path == ":memory:" {
                    "in-memory"
                } else {
                    &self.database_path
                }
            ),
            version: None,
            capabilities: vec![
                Capability::SqlQuery,
                Capability::SqlDdl,
                Capability::BulkLoad,
                Capability::IncrementalRead,
                Capability::Transactions,
            ],
            is_stub: false,
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        let start = std::time::Instant::now();
        let conn = self.ensure_connection().await?;
        let name = self.name.clone();

        let version = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| ProviderError::ConnectionFailed {
                name: name.clone(),
                reason: format!("lock poisoned: {e}"),
            })?;
            let mut stmt =
                conn.prepare("SELECT version()")
                    .map_err(|e| ProviderError::ConnectionFailed {
                        name: name.clone(),
                        reason: e.to_string(),
                    })?;
            let version: String = stmt.query_row([], |row| row.get(0)).map_err(|e| {
                ProviderError::ConnectionFailed {
                    name: name.clone(),
                    reason: e.to_string(),
                }
            })?;
            Ok::<_, ProviderError>(version)
        })
        .await
        .map_err(|e| ProviderError::ConnectionFailed {
            name: self.name.clone(),
            reason: format!("spawn_blocking failed: {e}"),
        })??;

        let latency = start.elapsed().as_millis() as u64;

        Ok(ConnectionTestResult {
            success: true,
            message: format!("DuckDB connection OK: {}", self.database_path),
            latency_ms: latency,
            server_version: Some(version),
        })
    }

    async fn close(&self) -> Result<(), ProviderError> {
        // Connection is dropped when the provider is dropped.
        Ok(())
    }
}

#[async_trait]
impl SqlProvider for DuckDbProvider {
    async fn execute(
        &self,
        query: &str,
        _params: &HashMap<String, String>,
    ) -> Result<SqlResult, ProviderError> {
        let start = std::time::Instant::now();
        let conn = self.ensure_connection().await?;
        let query = query.to_string();
        let name = self.name.clone();

        let result = tokio::task::spawn_blocking(move || -> Result<SqlResult, ProviderError> {
            let conn = conn.lock().map_err(|e| ProviderError::QueryFailed {
                connection: name.clone(),
                reason: format!("lock poisoned: {e}"),
            })?;

            let query_upper = query.trim().to_uppercase();
            let is_select = query_upper.starts_with("SELECT") || query_upper.starts_with("WITH");

            if is_select {
                let mut stmt = conn
                    .prepare(&query)
                    .map_err(|e| ProviderError::QueryFailed {
                        connection: name.clone(),
                        reason: e.to_string(),
                    })?;

                // Execute the query first, then read column metadata
                let mut rows = stmt.query([]).map_err(|e| ProviderError::QueryFailed {
                    connection: name.clone(),
                    reason: e.to_string(),
                })?;

                let column_count = rows.as_ref().unwrap().column_count();
                let columns: Vec<String> = (0..column_count)
                    .map(|i| {
                        rows.as_ref()
                            .unwrap()
                            .column_name(i)
                            .map_or("?", |v| v)
                            .to_string()
                    })
                    .collect();

                let mut all_rows = Vec::new();
                while let Some(row) = rows.next().map_err(|e| ProviderError::QueryFailed {
                    connection: name.clone(),
                    reason: e.to_string(),
                })? {
                    let vals: Vec<serde_json::Value> = (0..column_count)
                        .map(|i| {
                            let val: duckdb::types::Value = row.get_unwrap(i);
                            duckdb_value_to_json(val)
                        })
                        .collect();
                    all_rows.push(vals);
                }

                let total_rows = all_rows.len() as u64;
                let sample_limit = 100.min(all_rows.len());
                let sample_rows = all_rows[..sample_limit].to_vec();

                Ok(SqlResult {
                    rows_affected: 0,
                    rows_returned: Some(total_rows),
                    execution_time_ms: 0, // filled after spawn_blocking
                    columns,
                    sample_rows,
                    metrics: HashMap::new(),
                })
            } else {
                let rows_affected = conn.execute_batch(&query).map(|_| 0u64).map_err(|e| {
                    ProviderError::QueryFailed {
                        connection: name.clone(),
                        reason: e.to_string(),
                    }
                })?;

                Ok(SqlResult {
                    rows_affected,
                    rows_returned: None,
                    execution_time_ms: 0,
                    columns: Vec::new(),
                    sample_rows: Vec::new(),
                    metrics: HashMap::new(),
                })
            }
        })
        .await
        .map_err(|e| ProviderError::QueryFailed {
            connection: self.name.clone(),
            reason: format!("spawn_blocking failed: {e}"),
        })??;

        let execution_time = start.elapsed().as_millis() as u64;
        let mut metrics = HashMap::new();
        metrics.insert("execution_time_ms".to_string(), execution_time as f64);

        Ok(SqlResult {
            execution_time_ms: execution_time,
            metrics,
            ..result
        })
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        let conn = self.ensure_connection().await?;
        let name = self.name.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| ProviderError::QueryFailed {
                connection: name.clone(),
                reason: format!("lock poisoned: {e}"),
            })?;
            let mut stmt = conn
                .prepare("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
                .map_err(|e| ProviderError::QueryFailed {
                    connection: name.clone(),
                    reason: e.to_string(),
                })?;
            let schemas: Vec<String> = stmt
                .query_map([], |row| row.get(0))
                .map_err(|e| ProviderError::QueryFailed {
                    connection: name.clone(),
                    reason: e.to_string(),
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(schemas)
        })
        .await
        .map_err(|e| ProviderError::QueryFailed {
            connection: self.name.clone(),
            reason: format!("spawn_blocking failed: {e}"),
        })?
    }

    async fn describe_table(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>, ProviderError> {
        // Sanitize inputs to prevent SQL injection.
        let valid = |s: &str| s.chars().all(|c| c.is_alphanumeric() || c == '_');
        if !valid(schema) || !valid(table) {
            return Err(ProviderError::InvalidConfig {
                connection: self.name.clone(),
                reason: format!("Invalid schema/table name: {schema}.{table}"),
            });
        }

        let conn = self.ensure_connection().await?;
        let name = self.name.clone();
        let schema = schema.to_string();
        let table = table.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| ProviderError::QueryFailed {
                connection: name.clone(),
                reason: format!("lock poisoned: {e}"),
            })?;
            let sql = format!(
                "SELECT column_name, data_type, is_nullable, column_default \
                 FROM information_schema.columns \
                 WHERE table_schema = '{}' AND table_name = '{}' \
                 ORDER BY ordinal_position",
                schema, table
            );
            let mut stmt = conn.prepare(&sql).map_err(|e| ProviderError::QueryFailed {
                connection: name.clone(),
                reason: e.to_string(),
            })?;
            let columns: Vec<ColumnInfo> = stmt
                .query_map([], |row| {
                    let col_name: String = row.get(0)?;
                    let data_type: String = row.get(1)?;
                    let is_nullable: String = row.get(2)?;
                    let default_value: Option<String> = row.get(3)?;
                    Ok(ColumnInfo {
                        name: col_name,
                        data_type,
                        is_nullable: is_nullable == "YES",
                        is_primary_key: false,
                        default_value,
                    })
                })
                .map_err(|e| ProviderError::QueryFailed {
                    connection: name.clone(),
                    reason: e.to_string(),
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(columns)
        })
        .await
        .map_err(|e| ProviderError::QueryFailed {
            connection: self.name.clone(),
            reason: format!("spawn_blocking failed: {e}"),
        })?
    }
}
