//! DuckDB provider — embedded analytical database.
//!
//! Uses the `duckdb` crate for in-process query execution. DuckDB runs
//! embedded (no separate server), making it ideal for local analytics,
//! testing, and lightweight ETL workloads.
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
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use tokio::sync::Mutex;

use crate::errors::ProviderError;
use crate::traits::*;
use super::extra_str;

pub struct DuckDbProvider {
    name: String,
    database_path: String,
    threads: Option<u64>,
    memory_limit: Option<String>,
    conn: Arc<Mutex<Option<duckdb::Connection>>>,
}

impl DuckDbProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let database_path = config.database.clone().unwrap_or_else(|| ":memory:".to_string());
        let threads = super::extra_u64(config, "threads");
        let memory_limit = extra_str(config, "memory_limit");

        Ok(Self {
            name: name.to_string(),
            database_path,
            threads,
            memory_limit,
            conn: Arc::new(Mutex::new(None)),
        })
    }

    /// Lazily open or return the existing DuckDB connection.
    async fn ensure_conn(&self) -> Result<(), ProviderError> {
        let mut guard = self.conn.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        let path = self.database_path.clone();
        let threads = self.threads;
        let memory_limit = self.memory_limit.clone();
        let name = self.name.clone();

        // DuckDB operations are synchronous — run on blocking thread
        let conn = tokio::task::spawn_blocking(move || {
            let conn = if path == ":memory:" {
                duckdb::Connection::open_in_memory()
            } else {
                duckdb::Connection::open(&path)
            }
            .map_err(|e| ProviderError::ConnectionFailed {
                name: name.clone(),
                reason: format!("Failed to open DuckDB: {}", e),
            })?;

            // Apply settings
            if let Some(t) = threads {
                let _ = conn.execute(&format!("SET threads = {}", t), []);
            }
            if let Some(ref ml) = memory_limit {
                let _ = conn.execute(&format!("SET memory_limit = '{}'", ml), []);
            }

            Ok::<_, ProviderError>(conn)
        })
        .await
        .map_err(|e| ProviderError::ConnectionFailed {
            name: self.name.clone(),
            reason: format!("Spawn blocking failed: {}", e),
        })??;

        *guard = Some(conn);
        Ok(())
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
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        let start = Instant::now();

        match self.ensure_conn().await {
            Ok(()) => Ok(ConnectionTestResult {
                success: true,
                message: format!("DuckDB connection OK: {}", self.database_path),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            }),
            Err(e) => Ok(ConnectionTestResult {
                success: false,
                message: format!("DuckDB connection failed: {}", e),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            }),
        }
    }

    async fn close(&self) -> Result<(), ProviderError> {
        let mut guard = self.conn.lock().await;
        *guard = None;
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
        let query = super::sanitize::sanitize_query(query, &self.name)?;
        self.ensure_conn().await?;

        let start = Instant::now();
        let conn = self.conn.clone();
        let name = self.name.clone();
        let query_owned = query.to_string();

        let result = tokio::task::spawn_blocking(move || {
            let guard = conn.blocking_lock();
            let conn = guard.as_ref().ok_or_else(|| ProviderError::ConnectionFailed {
                name: name.clone(),
                reason: "Connection not open".to_string(),
            })?;

            let query_upper = query_owned.trim().to_uppercase();
            let is_select = query_upper.starts_with("SELECT")
                || query_upper.starts_with("WITH")
                || query_upper.starts_with("SHOW")
                || query_upper.starts_with("DESCRIBE")
                || query_upper.starts_with("PRAGMA");

            if is_select {
                // Use DuckDB's arrow API to get results with column names
                let mut stmt = conn.prepare(&query_owned).map_err(|e| {
                    ProviderError::QueryFailed {
                        connection: name.clone(),
                        reason: format!("{}", e),
                    }
                })?;

                // query_arrow returns Arrow RecordBatches with column metadata
                let arrow_result: Vec<duckdb::arrow::record_batch::RecordBatch> = stmt
                    .query_arrow([])
                    .map_err(|e| ProviderError::QueryFailed {
                        connection: name.clone(),
                        reason: format!("{}", e),
                    })?
                    .collect();

                // Extract column names from schema
                let columns: Vec<String> = if let Some(batch) = arrow_result.first() {
                    batch.schema().fields().iter().map(|f| f.name().clone()).collect()
                } else {
                    vec![]
                };

                // Convert arrow data to JSON sample rows
                let mut sample_rows = Vec::new();
                let mut total_rows: u64 = 0;
                let column_count = columns.len();

                for batch in &arrow_result {
                    for row_idx in 0..batch.num_rows() {
                        total_rows += 1;
                        if sample_rows.len() >= 100 {
                            continue;
                        }
                        let mut vals = Vec::new();
                        for col_idx in 0..column_count {
                            let col = batch.column(col_idx);
                            let val = if col.is_null(row_idx) {
                                serde_json::Value::Null
                            } else {
                                // Convert arrow array value to JSON string representation
                                let s = duckdb::arrow::util::display::array_value_to_string(col, row_idx)
                                    .unwrap_or_else(|_| "null".to_string());
                                // Try to parse as number
                                if let Ok(n) = s.parse::<i64>() {
                                    serde_json::json!(n)
                                } else if let Ok(f) = s.parse::<f64>() {
                                    serde_json::json!(f)
                                } else {
                                    serde_json::Value::String(s)
                                }
                            };
                            vals.push(val);
                        }
                        sample_rows.push(vals);
                    }
                }

                Ok(SqlResult {
                    columns,
                    sample_rows,
                    rows_affected: 0,
                    rows_returned: Some(total_rows),
                    execution_time_ms: 0,
                    metrics: HashMap::new(),
                })
            } else {
                let affected = conn.execute(&query_owned, []).map_err(|e| {
                    ProviderError::QueryFailed {
                        connection: name.clone(),
                        reason: format!("{}", e),
                    }
                })?;

                Ok::<SqlResult, ProviderError>(SqlResult {
                    columns: vec![],
                    sample_rows: vec![],
                    rows_affected: affected as u64,
                    rows_returned: None,
                    execution_time_ms: 0,
                    metrics: HashMap::new(),
                })
            }
        })
        .await
        .map_err(|e| ProviderError::QueryFailed {
            connection: self.name.clone(),
            reason: format!("Spawn blocking failed: {}", e),
        })??;

        Ok(SqlResult {
            execution_time_ms: start.elapsed().as_millis() as u64,
            ..result
        })
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        let result = self
            .execute("SELECT schema_name FROM information_schema.schemata", &HashMap::new())
            .await?;

        let schemas: Vec<String> = result
            .sample_rows
            .iter()
            .filter_map(|row| row.first().and_then(|v| v.as_str()).map(String::from))
            .collect();

        if schemas.is_empty() {
            Ok(vec!["main".to_string(), "information_schema".to_string()])
        } else {
            Ok(schemas)
        }
    }

    async fn describe_table(
        &self,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>, ProviderError> {
        let query = format!(
            "SELECT column_name, data_type, is_nullable, column_default \
             FROM information_schema.columns WHERE table_name = '{}'  \
             ORDER BY ordinal_position",
            table.replace('\'', "''")
        );

        let result = self.execute(&query, &HashMap::new()).await?;

        Ok(result
            .sample_rows
            .iter()
            .map(|row| ColumnInfo {
                name: row
                    .first()
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                data_type: row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("VARCHAR")
                    .to_string(),
                is_nullable: row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .map(|s| s == "YES")
                    .unwrap_or(true),
                is_primary_key: false,
                default_value: row.get(3).and_then(|v| v.as_str()).map(String::from),
            })
            .collect())
    }
}
