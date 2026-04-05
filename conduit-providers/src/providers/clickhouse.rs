//! ClickHouse provider.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   clickhouse_events:
//!     type: clickhouse
//!     host: clickhouse.internal
//!     port: 8123
//!     database: events
//!     credentials: ${CLICKHOUSE_PASSWORD}
//!     user: conduit
//!     protocol: http          # http (default) or native
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

use crate::errors::ProviderError;
use crate::traits::*;
use super::extra_str;

#[allow(dead_code)]
pub struct ClickHouseProvider {
    name: String,
    host: String,
    port: u16,
    database: String,
    user: String,
    protocol: String,
}

impl ClickHouseProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let host = config.host.clone().unwrap_or_else(|| "localhost".to_string());
        let port = config.port.unwrap_or(8123);
        let database = config.database.clone().unwrap_or_else(|| "default".to_string());
        let user = extra_str(config, "user").unwrap_or_else(|| "default".to_string());
        let protocol = extra_str(config, "protocol").unwrap_or_else(|| "http".to_string());

        Ok(Self { name: name.to_string(), host, port, database, user, protocol })
    }
}

#[async_trait]
impl Provider for ClickHouseProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "clickhouse".to_string(),
            display_name: format!("ClickHouse ({}:{})", self.host, self.port),
            version: None,
            capabilities: vec![
                Capability::SqlQuery, Capability::SqlDdl, Capability::BulkLoad,
                Capability::IncrementalRead,
            ],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "clickhouse".into(), operation: "test_connection".into() })
    }

    async fn close(&self) -> Result<(), ProviderError> { Ok(()) }
}

#[async_trait]
impl SqlProvider for ClickHouseProvider {
    async fn execute(&self, _query: &str, _params: &HashMap<String, String>) -> Result<SqlResult, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "clickhouse".into(), operation: "execute".into() })
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![self.database.clone(), "system".to_string()])
    }

    async fn describe_table(&self, _schema: &str, _table: &str) -> Result<Vec<ColumnInfo>, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "clickhouse".into(), operation: "describe_table".into() })
    }
}
