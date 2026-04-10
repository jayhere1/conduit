//! Google BigQuery provider.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   my_bq:
//!     type: bigquery
//!     database: my-gcp-project     # GCP project ID
//!     credentials: file:///path/to/service-account.json
//!     dataset: analytics           # default dataset
//!     location: US                 # dataset location
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

use crate::errors::ProviderError;
use crate::traits::*;
use super::extra_str;

#[allow(dead_code)]
pub struct BigQueryProvider {
    name: String,
    project: String,
    dataset: String,
    location: String,
}

impl BigQueryProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let project = config.database.clone().unwrap_or_default();
        let dataset = extra_str(config, "dataset").unwrap_or_else(|| "default".to_string());
        let location = extra_str(config, "location").unwrap_or_else(|| "US".to_string());

        if project.is_empty() {
            return Err(ProviderError::InvalidConfig {
                connection: name.to_string(),
                reason: "BigQuery requires 'database' (GCP project ID)".to_string(),
            });
        }

        Ok(Self { name: name.to_string(), project, dataset, location })
    }
}

#[async_trait]
impl Provider for BigQueryProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "bigquery".to_string(),
            display_name: format!("BigQuery ({}/{})", self.project, self.dataset),
            version: None,
            capabilities: vec![
                Capability::SqlQuery, Capability::SqlDdl, Capability::BulkLoad,
                Capability::IncrementalRead,
            ],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        use tokio::net::TcpStream;
        use tokio::time::{timeout, Duration};
        use std::time::Instant;

        let start = Instant::now();
        let addr = "www.googleapis.com:443";

        match timeout(Duration::from_secs(5), TcpStream::connect(addr)).await {
            Ok(Ok(_)) => Ok(ConnectionTestResult {
                success: true,
                message: format!("TCP connection to {} successful", addr),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            }),
            Ok(Err(e)) => Ok(ConnectionTestResult {
                success: false,
                message: format!("Connection failed: {}", e),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            }),
            Err(_) => Ok(ConnectionTestResult {
                success: false,
                message: format!("Connection timed out after 5s to {}", addr),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            }),
        }
    }

    async fn close(&self) -> Result<(), ProviderError> { Ok(()) }
}

#[async_trait]
impl SqlProvider for BigQueryProvider {
    async fn execute(&self, _query: &str, _params: &HashMap<String, String>) -> Result<SqlResult, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "bigquery".into(), operation: "execute".into() })
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![self.dataset.clone(), "INFORMATION_SCHEMA".to_string()])
    }

    async fn describe_table(&self, _schema: &str, _table: &str) -> Result<Vec<ColumnInfo>, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "bigquery".into(), operation: "describe_table".into() })
    }
}
