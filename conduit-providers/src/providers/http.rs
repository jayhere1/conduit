//! HTTP/REST API and Webhook provider.
//!
//! Uses `reqwest` for actual HTTP requests. Supports Bearer, Basic, and API key
//! authentication, custom headers, configurable timeouts, and all HTTP methods.
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
    auth_value: Option<String>,
    timeout_secs: u64,
    default_headers: HashMap<String, String>,
    client: reqwest::Client,
}

impl HttpApiProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let base_url = config.host.clone().unwrap_or_else(|| "http://localhost".to_string());
        let base_path = extra_str(config, "base_path").unwrap_or_default();
        let auth_type = extra_str(config, "auth_type").unwrap_or_else(|| "none".to_string());
        let auth_header = extra_str(config, "auth_header")
            .unwrap_or_else(|| "Authorization".to_string());
        let timeout_secs = super::extra_u64(config, "timeout").unwrap_or(30);

        let auth_value = config
            .credentials
            .as_deref()
            .map(super::resolve_credential)
            .transpose()?;

        // Parse headers from extra config
        let default_headers = config
            .extra
            .get("headers")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .build()
            .unwrap_or_default();

        Ok(Self {
            name: name.to_string(),
            base_url,
            base_path,
            auth_type,
            auth_header,
            auth_value,
            timeout_secs,
            default_headers,
            client,
        })
    }

    fn full_url(&self, path: &str) -> String {
        format!("{}{}{}", self.base_url, self.base_path, path)
    }

    fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Apply authentication to a request builder.
    fn apply_auth(&self, mut builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref cred) = self.auth_value {
            match self.auth_type.as_str() {
                "bearer" => {
                    builder = builder.bearer_auth(cred);
                }
                "basic" => {
                    // credentials format: "user:password"
                    let parts: Vec<&str> = cred.splitn(2, ':').collect();
                    let user = parts.first().unwrap_or(&"");
                    let pass = parts.get(1).map(|s| *s);
                    builder = builder.basic_auth(user, pass);
                }
                "api_key" => {
                    builder = builder.header(&self.auth_header, cred);
                }
                _ => {}
            }
        }
        builder
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
        let start = std::time::Instant::now();
        let client = self.client();
        let url = self.full_url("/");

        let mut builder = client.head(&url);
        builder = self.apply_auth(builder);

        match builder.send().await {
            Ok(resp) => {
                let latency = start.elapsed().as_millis() as u64;
                Ok(ConnectionTestResult {
                    success: resp.status().is_success() || resp.status().is_redirection(),
                    message: format!("HTTP {} — {}", resp.status(), url),
                    latency_ms: latency,
                    server_version: resp
                        .headers()
                        .get("server")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string()),
                })
            }
            Err(e) => {
                let latency = start.elapsed().as_millis() as u64;
                Ok(ConnectionTestResult {
                    success: false,
                    message: format!("HTTP connection failed: {}", e),
                    latency_ms: latency,
                    server_version: None,
                })
            }
        }
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl HttpProvider for HttpApiProvider {
    async fn request(
        &self,
        method: &str,
        path: &str,
        headers: &HashMap<String, String>,
        body: Option<&str>,
    ) -> Result<HttpResult, ProviderError> {
        let url = self.full_url(path);
        let start = std::time::Instant::now();
        let client = self.client();

        let reqwest_method = method.parse::<reqwest::Method>().map_err(|_| {
            ProviderError::InvalidConfig {
                connection: self.name.clone(),
                reason: format!("Invalid HTTP method: {}", method),
            }
        })?;

        let mut builder = client.request(reqwest_method, &url);

        // Apply default headers
        for (k, v) in &self.default_headers {
            builder = builder.header(k, v);
        }
        // Apply request-specific headers
        for (k, v) in headers {
            builder = builder.header(k, v);
        }
        // Apply auth
        builder = self.apply_auth(builder);
        // Apply body
        if let Some(body_str) = body {
            builder = builder.body(body_str.to_string());
        }

        let response = builder.send().await.map_err(|e| {
            ProviderError::QueryFailed {
                connection: self.name.clone(),
                reason: format!("HTTP request failed: {}", e),
            }
        })?;

        let status_code = response.status().as_u16();
        let resp_headers: HashMap<String, String> = response
            .headers()
            .iter()
            .filter_map(|(k, v)| {
                v.to_str().ok().map(|s| (k.to_string(), s.to_string()))
            })
            .collect();

        let body_text = response.text().await.map_err(|e| {
            ProviderError::QueryFailed {
                connection: self.name.clone(),
                reason: format!("Failed to read response body: {}", e),
            }
        })?;

        let execution_time = start.elapsed().as_millis() as u64;

        Ok(HttpResult {
            status_code,
            headers: resp_headers,
            body: body_text,
            execution_time_ms: execution_time,
        })
    }
}
