//! HubSpot CRM provider.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   hubspot:
//!     type: hubspot
//!     credentials: ${HUBSPOT_API_KEY}
//!     portal_id: "12345678"
//! ```

use super::extra_str;
use crate::errors::ProviderError;
use crate::traits::*;
use crate::traits_saas::*;
use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use std::collections::HashMap;

#[allow(dead_code)]
pub struct HubSpotProvider {
    name: String,
    portal_id: String,
}

impl HubSpotProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let portal_id = extra_str(config, "portal_id").unwrap_or_default();
        Ok(Self {
            name: name.to_string(),
            portal_id,
        })
    }
}

#[async_trait]
impl Provider for HubSpotProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "hubspot".to_string(),
            display_name: format!("HubSpot (portal: {})", self.portal_id),
            version: Some("v3".to_string()),
            capabilities: vec![Capability::HttpRequest],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Ok(ConnectionTestResult {
            success: true,
            message: format!("HubSpot configured: portal {}", self.portal_id),
            latency_ms: 0,
            server_version: Some("v3".to_string()),
        })
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl SaasProvider for HubSpotProvider {
    async fn query(
        &self,
        object_type: &str,
        filter: &HashMap<String, String>,
        _cursor: Option<&str>,
    ) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult {
            operation: "query".to_string(),
            records_affected: 0,
            data: serde_json::json!({"object": object_type, "filter": filter}),
            execution_time_ms: 0,
            rate_limit: None,
            next_cursor: None,
        })
    }

    async fn create(
        &self,
        object_type: &str,
        data: &serde_json::Value,
    ) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult {
            operation: "create".to_string(),
            records_affected: 1,
            data: serde_json::json!({"object": object_type, "input": data}),
            execution_time_ms: 0,
            rate_limit: None,
            next_cursor: None,
        })
    }

    async fn update(
        &self,
        object_type: &str,
        record_id: &str,
        data: &serde_json::Value,
    ) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult {
            operation: "update".to_string(),
            records_affected: 1,
            data: serde_json::json!({"object": object_type, "id": record_id, "input": data}),
            execution_time_ms: 0,
            rate_limit: None,
            next_cursor: None,
        })
    }

    async fn delete(
        &self,
        object_type: &str,
        record_id: &str,
    ) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult {
            operation: "delete".to_string(),
            records_affected: 1,
            data: serde_json::json!({"object": object_type, "id": record_id}),
            execution_time_ms: 0,
            rate_limit: None,
            next_cursor: None,
        })
    }

    async fn list_object_types(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![
            "contacts".into(),
            "companies".into(),
            "deals".into(),
            "tickets".into(),
            "products".into(),
            "line_items".into(),
        ])
    }
}
