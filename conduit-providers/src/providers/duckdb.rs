//! DuckDB provider — embedded analytical database.
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

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

use crate::errors::ProviderError;
use crate::traits::*;
use super::extra_str;

#[allow(dead_code)]
pub struct DuckDbProvider {
    name: String,
    database_path: String,
    threads: Option<u64>,
    memory_limit: Option<String>,
}

impl DuckDbProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let database_path = config.database.clone().unwrap_or_else(|| ":memory:".to_string());
        let threads = super::extra_u64(config, "threads");
        let memory_limit = extra_str(config, "memory_limit");

        Ok(Self { name: name.to_string(), database_path, threads, memory_limit })
    }
}

#[async_trait]
impl Provider for DuckDbProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "duckdb".to_string(),
            display_name: format!("DuckDB ({})", if self.database_path == ":memory:" { "in-memory" } else { &self.database_path }),
            version: None,
            capabilities: vec![
                Capability::SqlQuery, Capability::SqlDdl, Capability::BulkLoad,
                Capability::IncrementalRead, Capability::Transactions,
            ],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        use std::time::Instant;

        let start = Instant::now();

        if self.database_path == ":memory:" {
            return Ok(ConnectionTestResult {
                success: true,
                message: "DuckDB in-memory mode ready".to_string(),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            });
        }

        match tokio::fs::metadata(&self.database_path).await {
            Ok(_) => Ok(ConnectionTestResult {
                success: true,
                message: format!("DuckDB database file exists: {}", self.database_path),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            }),
            Err(e) => Ok(ConnectionTestResult {
                success: false,
                message: format!("Database file not accessible: {}", e),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            }),
        }
    }

    async fn close(&self) -> Result<(), ProviderError> { Ok(()) }
}

#[async_trait]
impl SqlProvider for DuckDbProvider {
    async fn execute(&self, _query: &str, _params: &HashMap<String, String>) -> Result<SqlResult, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "duckdb".into(), operation: "execute".into() })
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec!["main".to_string(), "information_schema".to_string()])
    }

    async fn describe_table(&self, _schema: &str, _table: &str) -> Result<Vec<ColumnInfo>, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "duckdb".into(), operation: "describe_table".into() })
    }
}
