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
        Err(ProviderError::NotImplemented { provider_type: "oracle".into(), operation: "test_connection".into() })
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl SqlProvider for OracleProvider {
    async fn execute(
        &self,
        _query: &str,
        _params: &HashMap<String, String>,
    ) -> Result<SqlResult, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "oracle".into(), operation: "execute".into() })
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![self.schema.clone()])
    }

    async fn describe_table(
        &self,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<ColumnInfo>, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "oracle".into(), operation: "describe_table".into() })
    }
}
