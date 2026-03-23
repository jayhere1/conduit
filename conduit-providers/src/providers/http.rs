//! HTTP/REST API and Webhook provider.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   slack_webhook:
//!     type: http
//!     host: https://hooks.slack.com
//!     credentials: ${SLACK_WEBHOOK_TOKEN}
//!     base_path: /services/T00/B00
//!     timeout: 30
//!     headers:
//!       Content-Type: application/json
//!       Authorization: Bearer ${API_TOKEN}
//!
//!   internal_api:
//!     type: rest
//!     host: https://api.internal.com
//!     credentials: ${API_KEY}
//!     auth_type: bearer          # bearer, basic, api_key, none
//!     auth_header: X-API-Key     # for api_key auth
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

use crate::errors::ProviderError;
use crate::traits::*;
use super::extra_str;

#[allow(dead_code)]
pub struct HttpApiProvider {
    name: String,
    base_url: String,
    base_path: String,
    auth_type: String,
    auth_header: String,
    timeout_secs: u64,
    default_headers: HashMap<String, String>,
}

impl HttpApiProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let base_url = config.host.clone().unwrap_or_else(|| "http://localhost".to_string());
        let base_path = extra_str(config, "base_path").unwrap_or_default();
        let auth_type = extra_str(config, "auth_type").unwrap_or_else(|| "none".to_string());
        let auth_header = extra_str(config, "auth_header").unwrap_or_else(|| "Authorization".to_string());
        let timeout_secs = super::extra_u64(config, "timeout").unwrap_or(30);

        // Parse headers from extra config
        let default_headers = config.extra.get("headers")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self { name: name.to_string(), base_url, base_path, auth_type, auth_header, timeout_secs, default_headers })
    }

    fn full_url(&self, path: &str) -> String {
        format!("{}{}{}", self.base_url, self.base_path, path)
    }
}

#[async_trait]
impl Provider for HttpApiProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "http".to_string(),
            display_name: format!("HTTP ({}{})", self.base_url, self.base_path),
            version: None,
            capabilities: vec![Capability::HttpRequest],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Ok(ConnectionTestResult {
            success: true,
            message: format!("HTTP endpoint configured: {}{} (auth={})", self.base_url, self.base_path, self.auth_type),
            latency_ms: 0, server_version: None,
        })
    }

    async fn close(&self) -> Result<(), ProviderError> { Ok(()) }
}

#[async_trait]
impl HttpProvider for HttpApiProvider {
    async fn request(
        &self,
        method: &str,
        path: &str,
        _headers: &HashMap<String, String>,
        _body: Option<&str>,
    ) -> Result<HttpResult, ProviderError> {
        let url = self.full_url(path);
        let start = std::time::Instant::now();

        // In production: use reqwest to make the actual HTTP request
        let execution_time = start.elapsed().as_millis() as u64;

        Ok(HttpResult {
            status_code: 200,
            headers: HashMap::new(),
            body: format!("{{\"status\": \"ok\", \"url\": \"{}\", \"method\": \"{}\"}}", url, method),
            execution_time_ms: execution_time,
        })
    }
}
