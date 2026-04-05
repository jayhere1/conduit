//! Microsoft SQL Server provider
//!
//! Provides connectivity to Microsoft SQL Server databases.
//!
//! # Configuration
//!
//! ```yaml
//! type: sqlserver
//! config:
//!   host: localhost
//!   port: 1433
//!   database: mydb
//!   user: conduit
//!   credentials: secret
//!   schema: dbo
//!   encrypt: true
//!   trust_server_cert: false
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

use crate::errors::ProviderError;
use crate::traits::*;
use super::{extra_str, extra_bool};

/// Microsoft SQL Server provider
#[allow(dead_code)]
pub struct SqlServerProvider {
    name: String,
    host: String,
    port: u16,
    database: String,
    user: String,
    schema: String,
    encrypt: bool,
    trust_server_cert: bool,
}

impl SqlServerProvider {
    /// Create a new SQL Server provider from configuration
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let host = config.host.clone().unwrap_or_default();
        let port = config.port.unwrap_or(1433);
        let database = config.database.clone().unwrap_or_default();
        let user = extra_str(config, "user").unwrap_or_else(|| "conduit".to_string());
        let schema = extra_str(config, "schema").unwrap_or_else(|| "dbo".to_string());
        let encrypt = extra_bool(config, "encrypt").unwrap_or(true);
        let trust_server_cert = extra_bool(config, "trust_server_cert").unwrap_or(false);

        Ok(Self {
            name: name.to_string(),
            host,
            port,
            database,
            user,
            schema,
            encrypt,
            trust_server_cert,
        })
    }
}

#[async_trait]
impl Provider for SqlServerProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "sqlserver".to_string(),
            display_name: format!("SQL Server ({}:{})", self.host, self.port),
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
        Err(ProviderError::NotImplemented { provider_type: "sqlserver".into(), operation: "test_connection".into() })
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl SqlProvider for SqlServerProvider {
    async fn execute(
        &self,
        _query: &str,
        _params: &HashMap<String, String>,
    ) -> Result<SqlResult, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "sqlserver".into(), operation: "execute".into() })
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![self.schema.clone()])
    }

    async fn describe_table(
        &self,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<ColumnInfo>, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "sqlserver".into(), operation: "describe_table".into() })
    }
}
