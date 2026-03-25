//! TimescaleDB provider — TimescaleDB with time-series extension.
//!
//! Connects to TimescaleDB instances (TimescaleDB with TimescaleDB extension).
//!
//! # Configuration
//! ```yaml
//! connections:
//!   my_pg:
//!     type: timescaledb
//!     host: localhost
//!     port: 5432
//!     database: mydb
//!     credentials: ${POSTGRES_PASSWORD}
//!     schema: public           # default schema (optional)
//!     user: conduit            # username (optional, defaults to "conduit")
//!     ssl_mode: prefer         # disable, allow, prefer, require, verify-ca, verify-full
//!     connection_timeout: 30   # seconds
//!     statement_timeout: 3600  # seconds
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, PgPool, Row};
use tokio::sync::OnceCell;

use crate::errors::ProviderError;
use crate::traits::*;
use super::{extra_str, extra_u64};

#[allow(dead_code)]
pub struct TimescaleDbProvider {
    name: String,
    host: String,
    port: u16,
    database: String,
    user: String,
    schema: String,
    ssl_mode: String,
    connection_timeout: u64,
    statement_timeout: u64,
    connection_string: String,
    pool: OnceCell<PgPool>,
}

impl TimescaleDbProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let host = config.host.clone().unwrap_or_else(|| "localhost".to_string());
        let port = config.port.unwrap_or(5432);
        let database = config.database.clone().unwrap_or_else(|| "postgres".to_string());
        let user = extra_str(config, "user").unwrap_or_else(|| "conduit".to_string());
        let schema = extra_str(config, "schema").unwrap_or_else(|| "public".to_string());
        let ssl_mode = extra_str(config, "ssl_mode").unwrap_or_else(|| "prefer".to_string());
        let connection_timeout = extra_u64(config, "connection_timeout").unwrap_or(30);
        let statement_timeout = extra_u64(config, "statement_timeout").unwrap_or(3600);

        let password = config
            .credentials
            .as_deref()
            .map(super::resolve_credential)
            .transpose()?
            .unwrap_or_default();

        let connection_string = format!(
            "postgresql://{}:{}@{}:{}/{}?sslmode={}&connect_timeout={}&options=-c statement_timeout={}",
            user, password, host, port, database, ssl_mode, connection_timeout, statement_timeout * 1000
        );

        Ok(Self {
            name: name.to_string(),
            host,
            port,
            database,
            user,
            schema,
            ssl_mode,
            connection_timeout,
            statement_timeout,
            connection_string,
            pool: OnceCell::new(),
        })
    }

    /// Get the full connection string (for use by external tools).
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Get the default schema.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Lazily initialize the connection pool on first use.
    async fn ensure_pool(&self) -> Result<&PgPool, ProviderError> {
        self.pool
            .get_or_try_init(|| async {
                PgPoolOptions::new()
                    .max_connections(5)
                    .acquire_timeout(std::time::Duration::from_secs(self.connection_timeout))
                    .connect(&self.connection_string)
                    .await
                    .map_err(|e| ProviderError::ConnectionFailed {
                        name: self.name.clone(),
                        reason: e.to_string(),
                    })
            })
            .await
    }
}

/// Convert a sqlx PgRow column value to serde_json::Value.
fn ts_value_to_json(row: &sqlx::postgres::PgRow, idx: usize) -> serde_json::Value {
    if let Ok(v) = row.try_get::<i64, _>(idx) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<i32, _>(idx) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<i16, _>(idx) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<f64, _>(idx) {
        return serde_json::Number::from_f64(v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<f32, _>(idx) {
        return serde_json::Number::from_f64(v as f64)
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
impl Provider for TimescaleDbProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "timescaledb".to_string(),
            display_name: format!("TimescaleDB ({}:{})", self.host, self.port),
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
        let start = std::time::Instant::now();
        let pool = self.ensure_pool().await?;

        let _: (i32,) = sqlx::query_as("SELECT 1")
            .fetch_one(pool)
            .await
            .map_err(|e| ProviderError::ConnectionFailed {
                name: self.name.clone(),
                reason: e.to_string(),
            })?;

        let latency = start.elapsed().as_millis() as u64;

        let server_version: Option<String> = sqlx::query_as("SHOW server_version")
            .fetch_one(pool)
            .await
            .map(|(v,): (String,)| Some(v))
            .unwrap_or(None);

        Ok(ConnectionTestResult {
            success: true,
            message: format!(
                "TimescaleDB connection OK: {}@{}:{}/{}",
                self.user, self.host, self.port, self.database
            ),
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
impl SqlProvider for TimescaleDbProvider {
    async fn execute(
        &self,
        query: &str,
        _params: &HashMap<String, String>,
    ) -> Result<SqlResult, ProviderError> {
        let start = std::time::Instant::now();
        let pool = self.ensure_pool().await?;

        let query_upper = query.trim().to_uppercase();
        let is_select = query_upper.starts_with("SELECT") || query_upper.starts_with("WITH");

        if is_select {
            let rows = sqlx::query(query)
                .fetch_all(pool)
                .await
                .map_err(|e| ProviderError::QueryFailed {
                    connection: self.name.clone(),
                    reason: e.to_string(),
                })?;

            let execution_time = start.elapsed().as_millis() as u64;

            let columns: Vec<String> = if let Some(first) = rows.first() {
                first.columns().iter().map(|c| c.name().to_string()).collect()
            } else {
                Vec::new()
            };

            let total_rows = rows.len() as u64;
            let sample_limit = 100.min(rows.len());
            let sample_rows: Vec<Vec<serde_json::Value>> = rows[..sample_limit]
                .iter()
                .map(|row| {
                    (0..row.columns().len())
                        .map(|idx| ts_value_to_json(row, idx))
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
            let result = sqlx::query(query)
                .execute(pool)
                .await
                .map_err(|e| ProviderError::QueryFailed {
                    connection: self.name.clone(),
                    reason: e.to_string(),
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
            sqlx::query_as("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
                .fetch_all(pool)
                .await
                .map_err(|e| ProviderError::QueryFailed {
                    connection: self.name.clone(),
                    reason: e.to_string(),
                })?;

        Ok(rows.into_iter().map(|(s,)| s).collect())
    }

    async fn describe_table(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>, ProviderError> {
        let pool = self.ensure_pool().await?;

        let rows: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
            r#"
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default
            FROM information_schema.columns c
            WHERE c.table_schema = $1
              AND c.table_name = $2
            ORDER BY c.ordinal_position
            "#,
        )
        .bind(schema)
        .bind(table)
        .fetch_all(pool)
        .await
        .map_err(|e| ProviderError::QueryFailed {
            connection: self.name.clone(),
            reason: e.to_string(),
        })?;

        let pk_rows: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = $1
              AND tc.table_name = $2
            "#,
        )
        .bind(schema)
        .bind(table)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        let pk_columns: std::collections::HashSet<String> =
            pk_rows.into_iter().map(|(s,)| s).collect();

        Ok(rows
            .into_iter()
            .map(|(name, data_type, is_nullable, default_value)| ColumnInfo {
                is_primary_key: pk_columns.contains(&name),
                name,
                data_type,
                is_nullable: is_nullable == "YES",
                default_value,
            })
            .collect())
    }
}
