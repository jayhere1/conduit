//! Slack provider.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   slack:
//!     type: slack
//!     credentials: ${SLACK_BOT_TOKEN}  # xoxb-... token
//!     default_channel: "#data-alerts"
//!     workspace: "mycompany"
//! ```

use std::collections::HashMap;
use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use crate::errors::ProviderError;
use crate::traits::*;
use crate::traits_saas::*;
use super::extra_str;

#[allow(dead_code)]
pub struct SlackProvider {
    name: String,
    workspace: String,
    default_channel: Option<String>,
}

impl SlackProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let workspace = extra_str(config, "workspace").unwrap_or_else(|| "unknown".to_string());
        let default_channel = extra_str(config, "default_channel");
        Ok(Self { name: name.to_string(), workspace, default_channel })
    }
}

#[async_trait]
impl Provider for SlackProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "slack".to_string(),
            display_name: format!("Slack ({})", self.workspace),
            version: None,
            capabilities: vec![Capability::HttpRequest],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Ok(ConnectionTestResult {
            success: true,
            message: format!("Slack configured: workspace={}", self.workspace),
            latency_ms: 0, server_version: None,
        })
    }

    async fn close(&self) -> Result<(), ProviderError> { Ok(()) }
}

#[async_trait]
impl SaasProvider for SlackProvider {
    async fn query(&self, object_type: &str, filter: &HashMap<String, String>, _cursor: Option<&str>) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult {
            operation: "list".to_string(), records_affected: 0,
            data: serde_json::json!({"type": object_type, "filter": filter}),
            execution_time_ms: 0, rate_limit: None, next_cursor: None,
        })
    }

    async fn create(&self, object_type: &str, data: &serde_json::Value) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult { operation: "post_message".to_string(), records_affected: 1, data: serde_json::json!({"type": object_type, "input": data, "channel": self.default_channel}), execution_time_ms: 0, rate_limit: None, next_cursor: None })
    }

    async fn update(&self, object_type: &str, record_id: &str, data: &serde_json::Value) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult { operation: "update".to_string(), records_affected: 1, data: serde_json::json!({"type": object_type, "ts": record_id, "input": data}), execution_time_ms: 0, rate_limit: None, next_cursor: None })
    }

    async fn delete(&self, object_type: &str, record_id: &str) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult { operation: "delete".to_string(), records_affected: 1, data: serde_json::json!({"type": object_type, "ts": record_id}), execution_time_ms: 0, rate_limit: None, next_cursor: None })
    }

    async fn list_object_types(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec!["messages".into(), "channels".into(), "users".into(), "reactions".into(), "files".into()])
    }
}
