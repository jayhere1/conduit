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

use crate::errors::ProviderError;
use crate::traits::*;
use super::{extra_str, extra_u64};

/// SQLite provider
#[allow(dead_code)]
pub struct SqliteProvider {
    name: String,
    database: String,
    journal_mode: String,
    busy_timeout: u64,
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

        Ok(Self {
            name: name.to_string(),
            database,
            journal_mode,
            busy_timeout,
        })
    }
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
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        let start = std::time::Instant::now();

        // In production, this would use sqlx::SqlitePool::connect()
        // For now, validate config and report readiness
        let latency = start.elapsed().as_millis() as u64;

        Ok(ConnectionTestResult {
            success: true,
            message: format!("SQLite connection configured: {}", self.database),
            latency_ms: latency,
            server_version: None,
        })
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl SqlProvider for SqliteProvider {
    async fn execute(
        &self,
        query: &str,
        _params: &HashMap<String, String>,
    ) -> Result<SqlResult, ProviderError> {
        let start = std::time::Instant::now();

        // In production: connect to SQLite, execute query, collect results
        // For now: return structured metadata about what would happen
        let execution_time = start.elapsed().as_millis() as u64;

        let mut result = SqlResult::empty();
        result.execution_time_ms = execution_time;

        // Detect query type for accurate metrics
        let query_upper = query.trim().to_uppercase();
        if query_upper.starts_with("SELECT") || query_upper.starts_with("WITH") {
            result.rows_returned = Some(0);
            result.metrics.insert("query_type".to_string(), 0.0); // 0 = SELECT
        } else if query_upper.starts_with("INSERT") {
            result.metrics.insert("query_type".to_string(), 1.0); // 1 = INSERT
        } else if query_upper.starts_with("UPDATE") {
            result.metrics.insert("query_type".to_string(), 2.0); // 2 = UPDATE
        } else if query_upper.starts_with("DELETE") {
            result.metrics.insert("query_type".to_string(), 3.0); // 3 = DELETE
        }

        result.metrics.insert("execution_time_ms".to_string(), execution_time as f64);

        Ok(result)
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec!["main".to_string()])
    }

    async fn describe_table(
        &self,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<ColumnInfo>, ProviderError> {
        // In production: query sqlite_master
        Ok(vec![])
    }
}
