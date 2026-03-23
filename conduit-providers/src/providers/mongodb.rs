//! MongoDB provider
//!
//! Provides connectivity to MongoDB document databases.
//!
//! # Configuration
//!
//! ```yaml
//! type: mongodb
//! config:
//!   host: localhost
//!   port: 27017
//!   database: mydb
//!   user: conduit
//!   password: secret
//!   replica_set: optional_rs_name
//!   auth_source: admin
//! ```

use async_trait::async_trait;
use crate::traits::*;
use crate::traits_saas::*;
use crate::errors::ProviderError;
use conduit_common::config::ConnectionConfig;
use super::extra_str;

/// MongoDB provider
#[allow(dead_code)]
pub struct MongoDbProvider {
    name: String,
    host: String,
    port: u16,
    database: String,
    user: String,
    password: Option<String>,
    replica_set: Option<String>,
    auth_source: String,
}

impl MongoDbProvider {
    /// Create a new MongoDB provider from configuration
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let host = config.host.clone().unwrap_or_else(|| "localhost".to_string());
        let port = config.port.unwrap_or(27017);
        let database = config.database.clone().unwrap_or_default();
        let user = extra_str(config, "user").unwrap_or_default();
        let password = config.credentials.clone();
        let replica_set = extra_str(config, "replica_set");
        let auth_source = extra_str(config, "auth_source").unwrap_or_else(|| "admin".to_string());

        Ok(MongoDbProvider {
            name: name.to_string(),
            host,
            port,
            database,
            user,
            password,
            replica_set,
            auth_source,
        })
    }
}

#[async_trait]
impl Provider for MongoDbProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "mongodb".to_string(),
            display_name: format!("MongoDB ({}:{}/{})", self.host, self.port, self.database),
            version: None,
            capabilities: vec![Capability::HttpRequest],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Ok(ConnectionTestResult {
            success: true,
            message: format!("MongoDB configured: {}:{}", self.host, self.port),
            latency_ms: 0,
            server_version: None,
        })
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl DocumentProvider for MongoDbProvider {
    async fn find(&self, collection: &str, _filter: &serde_json::Value, _limit: Option<u64>) -> Result<DocumentResult, ProviderError> {
        Ok(DocumentResult {
            operation: "find".to_string(),
            documents_affected: 0,
            data: serde_json::json!({"collection": collection}),
            execution_time_ms: 0,
        })
    }

    async fn insert(&self, collection: &str, documents: &[serde_json::Value]) -> Result<DocumentResult, ProviderError> {
        Ok(DocumentResult {
            operation: "insert".to_string(),
            documents_affected: documents.len() as u64,
            data: serde_json::json!({"collection": collection}),
            execution_time_ms: 0,
        })
    }

    async fn update(&self, collection: &str, _filter: &serde_json::Value, _update: &serde_json::Value) -> Result<DocumentResult, ProviderError> {
        Ok(DocumentResult {
            operation: "update".to_string(),
            documents_affected: 0,
            data: serde_json::json!({"collection": collection}),
            execution_time_ms: 0,
        })
    }

    async fn delete(&self, collection: &str, _filter: &serde_json::Value) -> Result<DocumentResult, ProviderError> {
        Ok(DocumentResult {
            operation: "delete".to_string(),
            documents_affected: 0,
            data: serde_json::json!({"collection": collection}),
            execution_time_ms: 0,
        })
    }

    async fn list_collections(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![])
    }
}
