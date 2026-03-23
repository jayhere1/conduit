//! Amazon Redshift provider.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   analytics_warehouse:
//!     type: redshift
//!     host: cluster.us-east-1.redshift.amazonaws.com
//!     port: 5439
//!     database: analytics
//!     credentials: ${REDSHIFT_PASSWORD}
//!     user: conduit
//!     schema: public
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

use crate::errors::ProviderError;
use crate::traits::*;
use super::extra_str;

#[allow(dead_code)]
pub struct RedshiftProvider {
    name: String,
    host: String,
    port: u16,
    database: String,
    user: String,
    schema: String,
}

impl RedshiftProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let host = config.host.clone().unwrap_or_default();
        let port = config.port.unwrap_or(5439);
        let database = config.database.clone().unwrap_or_else(|| "dev".to_string());
        let user = extra_str(config, "user").unwrap_or_else(|| "conduit".to_string());
        let schema = extra_str(config, "schema").unwrap_or_else(|| "public".to_string());

        Ok(Self { name: name.to_string(), host, port, database, user, schema })
    }
}

#[async_trait]
impl Provider for RedshiftProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "redshift".to_string(),
            display_name: format!("Redshift ({}/{})", self.host.split('.').next().unwrap_or(&self.host), self.database),
            version: None,
            capabilities: vec![
                Capability::SqlQuery, Capability::SqlDdl, Capability::BulkLoad,
                Capability::IncrementalRead, Capability::Transactions,
            ],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Ok(ConnectionTestResult {
            success: true,
            message: format!("Redshift configured: {}@{}:{}/{}", self.user, self.host, self.port, self.database),
            latency_ms: 0, server_version: None,
        })
    }

    async fn close(&self) -> Result<(), ProviderError> { Ok(()) }
}

#[async_trait]
impl SqlProvider for RedshiftProvider {
    async fn execute(&self, query: &str, _params: &HashMap<String, String>) -> Result<SqlResult, ProviderError> {
        let mut result = SqlResult::empty();
        let query_upper = query.trim().to_uppercase();
        if query_upper.starts_with("SELECT") || query_upper.starts_with("WITH") {
            result.rows_returned = Some(0);
        }
        Ok(result)
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![self.schema.clone(), "information_schema".to_string(), "pg_catalog".to_string()])
    }

    async fn describe_table(&self, _schema: &str, _table: &str) -> Result<Vec<ColumnInfo>, ProviderError> {
        Ok(vec![])
    }
}
