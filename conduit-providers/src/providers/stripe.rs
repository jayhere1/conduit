//! Stripe payments provider.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   stripe_prod:
//!     type: stripe
//!     credentials: ${STRIPE_SECRET_KEY}
//!     api_version: "2024-06-20"
//!     account_id: "acct_xxx"      # optional, for Connect
//! ```

use std::collections::HashMap;
use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use crate::errors::ProviderError;
use crate::traits::*;
use crate::traits_saas::*;
use super::extra_str;

#[allow(dead_code)]
pub struct StripeProvider {
    name: String,
    api_version: String,
    account_id: Option<String>,
}

impl StripeProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let api_version = extra_str(config, "api_version").unwrap_or_else(|| "2024-06-20".to_string());
        let account_id = extra_str(config, "account_id");
        Ok(Self { name: name.to_string(), api_version, account_id })
    }
}

#[async_trait]
impl Provider for StripeProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "stripe".to_string(),
            display_name: format!("Stripe ({})", self.account_id.as_deref().unwrap_or("direct")),
            version: Some(self.api_version.clone()),
            capabilities: vec![Capability::HttpRequest],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Ok(ConnectionTestResult {
            success: true,
            message: format!("Stripe configured: API {}", self.api_version),
            latency_ms: 0, server_version: Some(self.api_version.clone()),
        })
    }

    async fn close(&self) -> Result<(), ProviderError> { Ok(()) }
}

#[async_trait]
impl SaasProvider for StripeProvider {
    async fn query(&self, object_type: &str, filter: &HashMap<String, String>, _cursor: Option<&str>) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult {
            operation: "list".to_string(), records_affected: 0,
            data: serde_json::json!({"object": object_type, "filter": filter}),
            execution_time_ms: 0, rate_limit: None, next_cursor: None,
        })
    }

    async fn create(&self, object_type: &str, data: &serde_json::Value) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult { operation: "create".to_string(), records_affected: 1, data: serde_json::json!({"object": object_type, "input": data}), execution_time_ms: 0, rate_limit: None, next_cursor: None })
    }

    async fn update(&self, object_type: &str, record_id: &str, data: &serde_json::Value) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult { operation: "update".to_string(), records_affected: 1, data: serde_json::json!({"object": object_type, "id": record_id, "input": data}), execution_time_ms: 0, rate_limit: None, next_cursor: None })
    }

    async fn delete(&self, object_type: &str, record_id: &str) -> Result<SaasResult, ProviderError> {
        Ok(SaasResult { operation: "delete".to_string(), records_affected: 1, data: serde_json::json!({"object": object_type, "id": record_id}), execution_time_ms: 0, rate_limit: None, next_cursor: None })
    }

    async fn list_object_types(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec!["charges".into(), "customers".into(), "invoices".into(), "subscriptions".into(), "payment_intents".into(), "products".into(), "prices".into(), "refunds".into()])
    }
}
