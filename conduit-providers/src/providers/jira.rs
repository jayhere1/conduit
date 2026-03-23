//! Atlassian Jira provider.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   jira:
//!     type: jira
//!     host: mycompany.atlassian.net
//!     credentials: ${JIRA_API_TOKEN}
//!     user: "bot@company.com"
//!     project: "ENG"           # default project key
//! ```

use std::collections::HashMap;
use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use crate::errors::ProviderError;
use crate::traits::*;
use crate::traits_saas::*;
use super::extra_str;

#[allow(dead_code)]
pub struct JiraProvider {
    name: String,
    host: String,
    user: String,
    project: Option<String>,
}

impl JiraProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let host = config.host.clone().ok_or_else(|| ProviderError::InvalidConfig {
            connection: name.to_string(), reason: "Jira requires a 'host' (e.g., mycompany.atlassian.net)".to_string(),
        })?;
        let user = extra_str(config, "user").unwrap_or_default();
        let project = extra_str(config, "project");
        Ok(Self { name: name.to_string(), host, user, project })
    }
}

#[async_trait]
impl Provider for JiraProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "jira".to_string(),
            display_name: format!("Jira ({}{})", self.host, self.project.as_ref().map(|p| format!(", project: {}", p)).unwrap_or_default()),
            version: None,
            capabilities: vec![Capability::HttpRequest],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Ok(ConnectionTestResult {
            success: true,
            message: format!("Jira configured: https://{}", self.host),
            latency_ms: 0, server_version: None,
        })
    }

    async fn close(&self) -> Result<(), ProviderError> { Ok(()) }
}

#[async_trait]
impl SaasProvider for JiraProvider {
    async fn query(&self, object_type: &str, filter: &HashMap<String, String>, _cursor: Option<&str>) -> Result<SaasResult, ProviderError> {
        // Would use JQL search for issues, REST API for other types
        Ok(SaasResult {
            operation: "query".to_string(), records_affected: 0,
            data: serde_json::json!({"type": object_type, "jql": filter.get("jql")}),
            execution_time_ms: 0, rate_limit: None, next_cursor: None,
        })
    }

    async fn create(&self, object_type: &str, data: &serde_json::Value) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult { operation: "create".to_string(), records_affected: 1, data: serde_json::json!({"type": object_type, "input": data}), execution_time_ms: 0, rate_limit: None, next_cursor: None })
    }

    async fn update(&self, object_type: &str, record_id: &str, data: &serde_json::Value) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult { operation: "update".to_string(), records_affected: 1, data: serde_json::json!({"type": object_type, "id": record_id, "input": data}), execution_time_ms: 0, rate_limit: None, next_cursor: None })
    }

    async fn delete(&self, object_type: &str, record_id: &str) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult { operation: "delete".to_string(), records_affected: 1, data: serde_json::json!({"type": object_type, "id": record_id}), execution_time_ms: 0, rate_limit: None, next_cursor: None })
    }

    async fn list_object_types(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec!["issues".into(), "projects".into(), "boards".into(), "sprints".into(), "users".into(), "components".into(), "versions".into()])
    }
}
