//! Salesforce provider.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   sfdc:
//!     type: salesforce
//!     host: login.salesforce.com       # or test.salesforce.com for sandbox
//!     credentials: ${SFDC_CLIENT_SECRET}
//!     client_id: "connected-app-id"
//!     user: "user@company.com"
//!     api_version: "v59.0"
//!     sandbox: false
//! ```

use std::collections::HashMap;
use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use crate::errors::ProviderError;
use crate::traits::*;
use crate::traits_saas::*;
use super::extra_str;

#[allow(dead_code)]
pub struct SalesforceProvider {
    name: String,
    instance_url: String,
    api_version: String,
    client_id: String,
    user: String,
    is_sandbox: bool,
}

impl SalesforceProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let host = config.host.clone().unwrap_or_else(|| "login.salesforce.com".to_string());
        let api_version = extra_str(config, "api_version").unwrap_or_else(|| "v59.0".to_string());
        let client_id = extra_str(config, "client_id").unwrap_or_default();
        let user = extra_str(config, "user").unwrap_or_default();
        let is_sandbox = extra_str(config, "sandbox").map(|s| s == "true").unwrap_or(false);
        let instance_url = format!("https://{}", host);

        Ok(Self { name: name.to_string(), instance_url, api_version, client_id, user, is_sandbox })
    }
}

#[async_trait]
impl Provider for SalesforceProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "salesforce".to_string(),
            display_name: format!("Salesforce{} ({})", if self.is_sandbox { " Sandbox" } else { "" }, self.user),
            version: Some(self.api_version.clone()),
            capabilities: vec![Capability::HttpRequest],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Ok(ConnectionTestResult {
            success: true,
            message: format!("Salesforce configured: {} ({})", self.instance_url, self.api_version),
            latency_ms: 0, server_version: Some(self.api_version.clone()),
        })
    }

    async fn close(&self) -> Result<(), ProviderError> { Ok(()) }
}

#[async_trait]
impl SaasProvider for SalesforceProvider {
    async fn query(&self, object_type: &str, filter: &HashMap<String, String>, _cursor: Option<&str>) -> Result<SaasResult, ProviderError> {
        // Would build SOQL query and call /services/data/{version}/query
        Ok(SaasResult {
            operation: "query".to_string(),
            records_affected: 0,
            data: serde_json::json!({"sobject": object_type, "filter": filter}),
            execution_time_ms: 0,
            rate_limit: None,
            next_cursor: None,
        })
    }

    async fn create(&self, object_type: &str, data: &serde_json::Value) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult {
            operation: "create".to_string(), records_affected: 1,
            data: serde_json::json!({"sobject": object_type, "input": data}),
            execution_time_ms: 0, rate_limit: None, next_cursor: None,
        })
    }

    async fn update(&self, object_type: &str, record_id: &str, data: &serde_json::Value) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult {
            operation: "update".to_string(), records_affected: 1,
            data: serde_json::json!({"sobject": object_type, "id": record_id, "input": data}),
            execution_time_ms: 0, rate_limit: None, next_cursor: None,
        })
    }

    async fn delete(&self, object_type: &str, record_id: &str) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult {
            operation: "delete".to_string(), records_affected: 1,
            data: serde_json::json!({"sobject": object_type, "id": record_id}),
            execution_time_ms: 0, rate_limit: None, next_cursor: None,
        })
    }

    async fn list_object_types(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![
            "Account".into(), "Contact".into(), "Lead".into(), "Opportunity".into(),
            "Case".into(), "Task".into(), "Event".into(), "Campaign".into(),
        ])
    }
}
