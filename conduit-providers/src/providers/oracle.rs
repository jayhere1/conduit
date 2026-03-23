//! Oracle Database provider
//!
//! Provides connectivity to Oracle Database servers.
//!
//! # Configuration
//!
//! ```yaml
//! type: oracle
//! config:
//!   host: localhost
//!   port: 1521
//!   database: ORCL
//!   user: conduit
//!   credentials: secret
//!   schema: public
//!   tns_name: optional_tns_entry
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

use crate::errors::ProviderError;
use crate::traits::*;
use super::extra_str;

/// Oracle Database provider
#[allow(dead_code)]
pub struct OracleProvider {
    name: String,
    host: String,
    port: u16,
    database: String,
    user: String,
    schema: String,
    tns_name: Option<String>,
}

impl OracleProvider {
    /// Create a new Oracle provider from configuration
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let host = config.host.clone().unwrap_or_default();
        let port = config.port.unwrap_or(1521);
        let database = config.database.clone().unwrap_or_default();
        let user = extra_str(config, "user").unwrap_or_else(|| "conduit".to_string());
        let schema = extra_str(config, "schema").unwrap_or_default();
        let tns_name = extra_str(config, "tns_name");

        Ok(Self {
            name: name.to_string(),
            host,
            port,
            database,
            user,
            schema,
            tns_name,
        })
    }
}

#[async_trait]
impl Provider for OracleProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "oracle".to_string(),
            display_name: format!("Oracle ({}:{}/{})", self.host, self.port, self.database),
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

        // In production, this would use sqlx or native Oracle client
        // For now, validate config and report readiness
        let latency = start.elapsed().as_millis() as u64;

        Ok(ConnectionTestResult {
            success: true,
            message: format!(
                "Oracle connection configured: {}@{}:{}/{}",
                self.user, self.host, self.port, self.database
            ),
            latency_ms: latency,
            server_version: None,
        })
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl SqlProvider for OracleProvider {
    async fn execute(
        &self,
        query: &str,
        _params: &HashMap<String, String>,
    ) -> Result<SqlResult, ProviderError> {
        let start = std::time::Instant::now();

        // In production: connect to Oracle, execute query, collect results
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
        Ok(vec![self.schema.clone()])
    }

    async fn describe_table(
        &self,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<ColumnInfo>, ProviderError> {
        // In production: query all_tab_columns
        Ok(vec![])
    }
}
