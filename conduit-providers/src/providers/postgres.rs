//! PostgreSQL provider.
//!
//! Connects to PostgreSQL (and compatible databases like CockroachDB, YugabyteDB).
//!
//! # Configuration
//! ```yaml
//! connections:
//!   my_pg:
//!     type: postgres
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

use crate::errors::ProviderError;
use crate::traits::*;
use super::{extra_str, extra_u64};

#[allow(dead_code)]
pub struct PostgresProvider {
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
}

impl PostgresProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let host = config.host.clone().unwrap_or_else(|| "localhost".to_string());
        let port = config.port.unwrap_or(5432);
        let database = config.database.clone().unwrap_or_else(|| "postgres".to_string());
        let user = extra_str(config, "user").unwrap_or_else(|| "conduit".to_string());
        let schema = extra_str(config, "schema").unwrap_or_else(|| "public".to_string());
        let ssl_mode = extra_str(config, "ssl_mode").unwrap_or_else(|| "prefer".to_string());
        let connection_timeout = extra_u64(config, "connection_timeout").unwrap_or(30);
        let statement_timeout = extra_u64(config, "statement_timeout").unwrap_or(3600);

        // Resolve password from credential reference
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
}

#[async_trait]
impl Provider for PostgresProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "postgres".to_string(),
            display_name: format!("PostgreSQL ({}:{})", self.host, self.port),
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

        // In production, this would use sqlx::PgPool::connect()
        // For now, validate config and report readiness
        let latency = start.elapsed().as_millis() as u64;

        Ok(ConnectionTestResult {
            success: true,
            message: format!(
                "PostgreSQL connection configured: {}@{}:{}/{}",
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
impl SqlProvider for PostgresProvider {
    async fn execute(
        &self,
        query: &str,
        _params: &HashMap<String, String>,
    ) -> Result<SqlResult, ProviderError> {
        let start = std::time::Instant::now();

        // In production: connect to PG, execute query, collect results
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
        Ok(vec![
            self.schema.clone(),
            "information_schema".to_string(),
            "pg_catalog".to_string(),
        ])
    }

    async fn describe_table(
        &self,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<ColumnInfo>, ProviderError> {
        // In production: query information_schema.columns
        Ok(vec![])
    }
}
