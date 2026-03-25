//! MySQL / MariaDB provider
//!
//! Provides connectivity to MySQL and MariaDB database servers via sqlx.
//!
//! # Configuration
//!
//! ```yaml
//! type: mysql
//! config:
//!   host: localhost
//!   port: 3306
//!   database: mydb
//!   user: conduit
//!   credentials: secret
//!   charset: utf8mb4
//!   ssl_mode: prefer
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{Column, MySqlPool, Row};
use tokio::sync::OnceCell;

use crate::errors::ProviderError;
use crate::traits::*;
use super::{extra_str, extra_u64};

/// MySQL / MariaDB provider backed by sqlx.
#[allow(dead_code)]
pub struct MySqlProvider {
    name: String,
    host: String,
    port: u16,
    database: String,
    user: String,
    charset: String,
    ssl_mode: String,
    connection_timeout: u64,
    connection_string: String,
    pool: OnceCell<MySqlPool>,
}

impl MySqlProvider {
    /// Create a new MySQL provider from configuration.
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let host = config.host.clone().unwrap_or_else(|| "localhost".to_string());
        let port = config.port.unwrap_or(3306);
        let database = config.database.clone().unwrap_or_default();
        let user = extra_str(config, "user").unwrap_or_else(|| "conduit".to_string());
        let charset = extra_str(config, "charset").unwrap_or_else(|| "utf8mb4".to_string());
        let ssl_mode = extra_str(config, "ssl_mode").unwrap_or_else(|| "preferred".to_string());
        let connection_timeout = extra_u64(config, "connection_timeout").unwrap_or(30);

        let password = config
            .credentials
            .as_deref()
            .map(super::resolve_credential)
            .transpose()?
            .unwrap_or_default();

        let connection_string = format!(
            "mysql://{}:{}@{}:{}/{}?charset={}&ssl-mode={}",
            super::url_encode_credential(&user), super::url_encode_credential(&password),
            host, port,
            database, charset, ssl_mode
        );

        Ok(Self {
            name: name.to_string(),
            host,
            port,
            database,
            user,
            charset,
            ssl_mode,
            connection_timeout,
            connection_string,
            pool: OnceCell::new(),
        })
    }

    /// Lazily initialize the connection pool on first use.
    async fn ensure_pool(&self) -> Result<&MySqlPool, ProviderError> {
        self.pool
            .get_or_try_init(|| async {
                MySqlPoolOptions::new()
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

/// Convert a sqlx MySqlRow column value to serde_json::Value.
fn mysql_value_to_json(row: &sqlx::mysql::MySqlRow, idx: usize) -> serde_json::Value {
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
    serde_json::Value::Null
}

#[async_trait]
impl Provider for MySqlProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "mysql".to_string(),
            display_name: format!("MySQL ({}:{})", self.host, self.port),
            version: None,
            capabilities: vec![
                Capability::SqlQuery,
                Capability::SqlDdl,
                Capability::Transactions,
                Capability::BulkLoad,
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

        let server_version: Option<String> = sqlx::query_as("SELECT VERSION()")
            .fetch_one(pool)
            .await
            .map(|(v,): (String,)| Some(v))
            .unwrap_or(None);

        Ok(ConnectionTestResult {
            success: true,
            message: format!(
                "MySQL connection OK: {}@{}:{}/{}",
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
impl SqlProvider for MySqlProvider {
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
                        .map(|idx| mysql_value_to_json(row, idx))
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
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT
            FROM information_schema.COLUMNS c
            WHERE c.TABLE_SCHEMA = ?
              AND c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
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
            SELECT kcu.COLUMN_NAME
            FROM information_schema.TABLE_CONSTRAINTS tc
            JOIN information_schema.KEY_COLUMN_USAGE kcu
              ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
             AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND tc.TABLE_SCHEMA = ?
              AND tc.TABLE_NAME = ?
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
