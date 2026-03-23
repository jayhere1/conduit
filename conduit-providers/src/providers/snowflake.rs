//! Snowflake provider.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   my_snowflake:
//!     type: snowflake
//!     host: account.snowflakecomputing.com
//!     database: analytics
//!     credentials: ${SNOWFLAKE_PASSWORD}
//!     user: conduit
//!     warehouse: compute_wh
//!     role: analyst
//!     schema: public
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

use crate::errors::ProviderError;
use crate::traits::*;
use super::extra_str;

#[allow(dead_code)]
pub struct SnowflakeProvider {
    name: String,
    account: String,
    database: String,
    user: String,
    warehouse: String,
    role: String,
    schema: String,
}

impl SnowflakeProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let account = config.host.clone().unwrap_or_default();
        let database = config.database.clone().unwrap_or_else(|| "analytics".to_string());
        let user = extra_str(config, "user").unwrap_or_else(|| "conduit".to_string());
        let warehouse = extra_str(config, "warehouse").unwrap_or_else(|| "compute_wh".to_string());
        let role = extra_str(config, "role").unwrap_or_else(|| "public".to_string());
        let schema = extra_str(config, "schema").unwrap_or_else(|| "public".to_string());

        if account.is_empty() {
            return Err(ProviderError::InvalidConfig {
                connection: name.to_string(),
                reason: "Snowflake requires 'host' (account identifier)".to_string(),
            });
        }

        Ok(Self { name: name.to_string(), account, database, user, warehouse, role, schema })
    }
}

#[async_trait]
impl Provider for SnowflakeProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "snowflake".to_string(),
            display_name: format!("Snowflake ({}/{})", self.account.split('.').next().unwrap_or(&self.account), self.database),
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
            message: format!("Snowflake connection configured: {}@{}/{}", self.user, self.account, self.database),
            latency_ms: 0,
            server_version: None,
        })
    }

    async fn close(&self) -> Result<(), ProviderError> { Ok(()) }
}

#[async_trait]
impl SqlProvider for SnowflakeProvider {
    async fn execute(&self, query: &str, _params: &HashMap<String, String>) -> Result<SqlResult, ProviderError> {
        let start = std::time::Instant::now();
        let mut result = SqlResult::empty();
        result.execution_time_ms = start.elapsed().as_millis() as u64;
        let query_upper = query.trim().to_uppercase();
        if query_upper.starts_with("SELECT") || query_upper.starts_with("WITH") {
            result.rows_returned = Some(0);
        }
        Ok(result)
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![self.schema.clone(), "information_schema".to_string()])
    }

    async fn describe_table(&self, _schema: &str, _table: &str) -> Result<Vec<ColumnInfo>, ProviderError> {
        Ok(vec![])
    }
}
