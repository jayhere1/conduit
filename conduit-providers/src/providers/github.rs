//! GitHub provider.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   github:
//!     type: github
//!     credentials: ${GITHUB_TOKEN}   # Personal access token or GitHub App token
//!     org: "my-org"                  # default organization
//!     api_url: "https://api.github.com"  # or GHE URL
//! ```

use std::collections::HashMap;
use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use crate::errors::ProviderError;
use crate::traits::*;
use crate::traits_saas::*;
use super::extra_str;

#[allow(dead_code)]
pub struct GitHubProvider {
    name: String,
    api_url: String,
    org: Option<String>,
}

impl GitHubProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let api_url = extra_str(config, "api_url").unwrap_or_else(|| "https://api.github.com".to_string());
        let org = extra_str(config, "org");
        Ok(Self { name: name.to_string(), api_url, org })
    }
}

#[async_trait]
impl Provider for GitHubProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "github".to_string(),
            display_name: format!("GitHub{}", self.org.as_ref().map(|o| format!(" ({})", o)).unwrap_or_default()),
            version: None,
            capabilities: vec![Capability::HttpRequest],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Ok(ConnectionTestResult {
            success: true,
            message: format!("GitHub configured: {}", self.api_url),
            latency_ms: 0, server_version: None,
        })
    }

    async fn close(&self) -> Result<(), ProviderError> { Ok(()) }
}

#[async_trait]
impl SaasProvider for GitHubProvider {
    async fn query(&self, object_type: &str, filter: &HashMap<String, String>, _cursor: Option<&str>) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult {
            operation: "list".to_string(), records_affected: 0,
            data: serde_json::json!({"resource": object_type, "filter": filter}),
            execution_time_ms: 0, rate_limit: None, next_cursor: None,
        })
    }

    async fn create(&self, object_type: &str, data: &serde_json::Value) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult { operation: "create".to_string(), records_affected: 1, data: serde_json::json!({"resource": object_type, "input": data}), execution_time_ms: 0, rate_limit: None, next_cursor: None })
    }

    async fn update(&self, object_type: &str, record_id: &str, data: &serde_json::Value) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult { operation: "update".to_string(), records_affected: 1, data: serde_json::json!({"resource": object_type, "id": record_id, "input": data}), execution_time_ms: 0, rate_limit: None, next_cursor: None })
    }

    async fn delete(&self, object_type: &str, record_id: &str) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult { operation: "delete".to_string(), records_affected: 1, data: serde_json::json!({"resource": object_type, "id": record_id}), execution_time_ms: 0, rate_limit: None, next_cursor: None })
    }

    async fn list_object_types(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec!["repos".into(), "issues".into(), "pull_requests".into(), "commits".into(), "releases".into(), "actions/workflows".into(), "actions/runs".into()])
    }
}
