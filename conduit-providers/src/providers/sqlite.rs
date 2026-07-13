//! SQLite provider
//!
//! Provides connectivity to SQLite databases (embedded, file-based).
//!
//! # Configuration
//!
//! ```yaml
//! type: sqlite
//! config:
//!   database: ./mydb.db
//!   journal_mode: wal
//!   busy_timeout: 5000
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Column, Row, SqlitePool};
use tokio::sync::OnceCell;

use super::{extra_str, extra_u64};
use crate::errors::ProviderError;
use crate::traits::*;

/// SQLite provider
pub struct SqliteProvider {
    name: String,
    database: String,
    journal_mode: String,
    busy_timeout: u64,
    max_connections: u32,
    pool: OnceCell<SqlitePool>,
}

impl SqliteProvider {
    /// Create a new SQLite provider from configuration
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let database = config
            .database
            .clone()
            .or_else(|| extra_str(config, "database"))
            .unwrap_or_else(|| ":memory:".to_string());
        let journal_mode = extra_str(config, "journal_mode").unwrap_or_else(|| "wal".to_string());
        let busy_timeout = extra_u64(config, "busy_timeout").unwrap_or(5000);
        let max_connections = extra_u64(config, "max_connections").unwrap_or(5) as u32;

        Ok(Self {
            name: name.to_string(),
            database,
            journal_mode,
            busy_timeout,
            max_connections,
            pool: OnceCell::new(),
        })
    }

    /// Lazily initialize the connection pool on first use.
    async fn ensure_pool(&self) -> Result<&SqlitePool, ProviderError> {
        self.pool
            .get_or_try_init(|| async {
                let url = if self.database == ":memory:" {
                    "sqlite::memory:".to_string()
                } else {
                    format!("sqlite:{}?mode=rwc", self.database)
                };

                let pool = SqlitePoolOptions::new()
                    .max_connections(self.max_connections)
                    .connect(&url)
                    .await
                    .map_err(|e| ProviderError::ConnectionFailed {
                        name: self.name.clone(),
                        reason: super::sanitize::sanitize_error(&e.to_string()),
                    })?;

                let pragma_journal = format!("PRAGMA journal_mode = {}", self.journal_mode);
                sqlx::query(&pragma_journal)
                    .execute(&pool)
                    .await
                    .map_err(|e| ProviderError::ConnectionFailed {
                        name: self.name.clone(),
                        reason: super::sanitize::sanitize_error(&format!(
                            "failed to set journal_mode: {}",
                            e
                        )),
                    })?;

                let pragma_timeout = format!("PRAGMA busy_timeout = {}", self.busy_timeout);
                sqlx::query(&pragma_timeout)
                    .execute(&pool)
                    .await
                    .map_err(|e| ProviderError::ConnectionFailed {
                        name: self.name.clone(),
                        reason: super::sanitize::sanitize_error(&format!(
                            "failed to set busy_timeout: {}",
                            e
                        )),
                    })?;

                Ok(pool)
            })
            .await
    }
}

/// Convert a sqlx SqliteRow column value to serde_json::Value.
fn sqlite_value_to_json(row: &sqlx::sqlite::SqliteRow, idx: usize) -> serde_json::Value {
    if let Ok(v) = row.try_get::<i64, _>(idx) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<i32, _>(idx) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<f64, _>(idx) {
        return serde_json::Number::from_f64(v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<bool, _>(idx) {
        return serde_json::Value::Bool(v);
    }
    if let Ok(v) = row.try_get::<String, _>(idx) {
        return serde_json::Value::String(v);
    }
    if let Ok(None) = row.try_get::<Option<String>, _>(idx) {
        return serde_json::Value::Null;
    }
    serde_json::Value::Null
}

#[async_trait]
impl Provider for SqliteProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "sqlite".to_string(),
            display_name: format!("SQLite ({})", self.database),
            version: None,
            capabilities: vec![
                Capability::SqlQuery,
                Capability::SqlDdl,
                Capability::Transactions,
            ],
            is_stub: false,
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        let start = std::time::Instant::now();
        let pool = self.ensure_pool().await?;

        let _: (i32,) = sqlx::query_as("SELECT 1")
            .fetch_one(pool)
            .await
            .map_err(|e| ProviderError::ConnectionFailed {
                name: self.name.clone(),
                reason: super::sanitize::sanitize_error(&e.to_string()),
            })?;

        let latency = start.elapsed().as_millis() as u64;

        let server_version: Option<String> = sqlx::query_as("SELECT sqlite_version()")
            .fetch_one(pool)
            .await
            .map(|(v,): (String,)| Some(v))
            .unwrap_or(None);

        Ok(ConnectionTestResult {
            success: true,
            message: format!("SQLite connection OK: {}", self.database),
            latency_ms: latency,
            server_version,
        })
    }

    async fn close(&self) -> Result<(), ProviderError> {
        if let Some(pool) = self.pool.get() {
            pool.close().await;
        }
        Ok(())
    }
}

#[async_trait]
impl SqlProvider for SqliteProvider {
    async fn execute(
        &self,
        query: &str,
        params: &HashMap<String, String>,
    ) -> Result<SqlResult, ProviderError> {
        let query = super::sanitize::sanitize_query(query, &self.name)?;
        let (query, bind_values) = super::params::bind_named_params(
            &query,
            params,
            super::params::PlaceholderStyle::Question,
        )
        .map_err(|reason| ProviderError::QueryFailed {
            connection: self.name.clone(),
            reason,
        })?;
        let start = std::time::Instant::now();
        let pool = self.ensure_pool().await?;

        let query_upper = query.trim().to_uppercase();
        let is_select = query_upper.starts_with("SELECT") || query_upper.starts_with("WITH");

        if is_select {
            let mut q = sqlx::query(&query);
            for v in &bind_values {
                q = super::params::bind_inferred_sqlite(q, v);
            }
            let rows = q
                .fetch_all(pool)
                .await
                .map_err(|e| ProviderError::QueryFailed {
                    connection: self.name.clone(),
                    reason: super::sanitize::sanitize_error(&e.to_string()),
                })?;

            let execution_time = start.elapsed().as_millis() as u64;

            let columns: Vec<String> = if let Some(first) = rows.first() {
                first
                    .columns()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect()
            } else {
                Vec::new()
            };

            let total_rows = rows.len() as u64;
            let sample_limit = 100.min(rows.len());
            let sample_rows: Vec<Vec<serde_json::Value>> = rows[..sample_limit]
                .iter()
                .map(|row| {
                    (0..row.columns().len())
                        .map(|idx| sqlite_value_to_json(row, idx))
                        .collect()
                })
                .collect();

            let mut metrics = HashMap::new();
            metrics.insert("query_type".to_string(), 0.0);
            metrics.insert("execution_time_ms".to_string(), execution_time as f64);

            Ok(SqlResult {
                rows_affected: 0,
                rows_returned: Some(total_rows),
                execution_time_ms: execution_time,
                columns,
                sample_rows,
                metrics,
            })
        } else {
            let mut q = sqlx::query(&query);
            for v in &bind_values {
                q = super::params::bind_inferred_sqlite(q, v);
            }
            let result = q
                .execute(pool)
                .await
                .map_err(|e| ProviderError::QueryFailed {
                    connection: self.name.clone(),
                    reason: super::sanitize::sanitize_error(&e.to_string()),
                })?;

            let execution_time = start.elapsed().as_millis() as u64;

            let mut metrics = HashMap::new();
            if query_upper.starts_with("INSERT") {
                metrics.insert("query_type".to_string(), 1.0);
            } else if query_upper.starts_with("UPDATE") {
                metrics.insert("query_type".to_string(), 2.0);
            } else if query_upper.starts_with("DELETE") {
                metrics.insert("query_type".to_string(), 3.0);
            }
            metrics.insert("execution_time_ms".to_string(), execution_time as f64);

            Ok(SqlResult {
                rows_affected: result.rows_affected(),
                rows_returned: None,
                execution_time_ms: execution_time,
                columns: Vec::new(),
                sample_rows: Vec::new(),
                metrics,
            })
        }
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        let pool = self.ensure_pool().await?;

        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT name FROM pragma_database_list ORDER BY name")
                .fetch_all(pool)
                .await
                .map_err(|e| ProviderError::QueryFailed {
                    connection: self.name.clone(),
                    reason: super::sanitize::sanitize_error(&e.to_string()),
                })?;

        Ok(rows.into_iter().map(|(s,)| s).collect())
    }

    async fn describe_table(
        &self,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>, ProviderError> {
        let pool = self.ensure_pool().await?;

        // PRAGMA doesn't support parameterized queries, so sanitize the
        // table name to prevent SQL injection. Only allow alphanumeric,
        // underscores, and dots (for schema.table).
        if !table
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '.')
        {
            return Err(ProviderError::InvalidConfig {
                connection: self.name.clone(),
                reason: format!("Invalid table name: {}", table),
            });
        }

        let rows: Vec<(i32, String, String, bool, Option<String>, i32)> =
            sqlx::query_as(&format!("PRAGMA table_info(\"{}\")", table))
                .fetch_all(pool)
                .await
                .map_err(|e| ProviderError::QueryFailed {
                    connection: self.name.clone(),
                    reason: super::sanitize::sanitize_error(&e.to_string()),
                })?;

        Ok(rows
            .into_iter()
            .map(
                |(_cid, name, data_type, notnull, dflt_value, pk)| ColumnInfo {
                    name,
                    data_type,
                    is_nullable: !notnull,
                    is_primary_key: pk > 0,
                    default_value: dflt_value,
                },
            )
            .collect())
    }
}

#[cfg(test)]
mod param_binding_tests {
    use super::*;
    use crate::traits::SqlProvider;

    fn memory_provider() -> SqliteProvider {
        let config = ConnectionConfig {
            conn_type: "sqlite".to_string(),
            host: None,
            port: None,
            database: Some(":memory:".to_string()),
            credentials: None,
            extra: HashMap::new(),
        };
        SqliteProvider::from_config("test_sqlite", &config).unwrap()
    }

    /// `execute` must bind named `:params` as real bind parameters —
    /// the params map is not decorative.
    #[tokio::test]
    async fn execute_binds_named_params() {
        let p = memory_provider();
        p.execute_statement("CREATE TABLE t (id INTEGER, name TEXT)")
            .await
            .unwrap();
        p.execute_statement("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
            .await
            .unwrap();

        let mut params = HashMap::new();
        params.insert("id".to_string(), "2".to_string());
        let result = p
            .execute("SELECT name FROM t WHERE id = :id", &params)
            .await
            .unwrap();

        assert_eq!(
            result.sample_rows.len(),
            1,
            "bound :id must filter to one row"
        );
        assert_eq!(result.sample_rows[0][0], serde_json::json!("bob"));
    }

    /// A string param must not open an injection hole: the bound value is
    /// data, never SQL.
    #[tokio::test]
    async fn bound_param_is_not_interpreted_as_sql() {
        let p = memory_provider();
        p.execute_statement("CREATE TABLE t (id INTEGER, name TEXT)")
            .await
            .unwrap();
        p.execute_statement("INSERT INTO t VALUES (1, 'alice')")
            .await
            .unwrap();

        let mut params = HashMap::new();
        params.insert("name".to_string(), "alice' OR '1'='1".to_string());
        let result = p
            .execute("SELECT id FROM t WHERE name = :name", &params)
            .await
            .unwrap();

        assert_eq!(
            result.sample_rows.len(),
            0,
            "injection payload must be treated as a literal value"
        );
    }

    /// Referencing a param that wasn't supplied is a clear error, not a
    /// silently-unbound query.
    #[tokio::test]
    async fn missing_param_is_an_error() {
        let p = memory_provider();
        p.execute_statement("CREATE TABLE t (id INTEGER)")
            .await
            .unwrap();

        let params = HashMap::new();
        let err = p
            .execute("SELECT * FROM t WHERE id = :missing", &params)
            .await;
        assert!(err.is_err(), "missing param must error, got {:?}", err);
    }
}
