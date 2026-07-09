//! Google BigQuery provider.
//!
//! Uses the BigQuery REST API v2 for query execution. Authenticates via
//! GCP service account JSON (JWT -> OAuth2 access token exchange).
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
use std::time::Instant;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

use super::{extra_str, resolve_credential};
use crate::errors::ProviderError;
use crate::traits::*;

pub struct BigQueryProvider {
    name: String,
    project: String,
    dataset: String,
    location: String,
    credentials_json: Option<String>,
    client: reqwest::Client,
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

        // Resolve credentials — may be a file:// path or env var reference
        let credentials_json = config
            .credentials
            .as_deref()
            .map(resolve_credential)
            .transpose()
            .ok()
            .flatten();

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .unwrap_or_default();

        Ok(Self {
            name: name.to_string(),
            project,
            dataset,
            location,
            credentials_json,
            client,
        })
    }

    /// Parse a GCP service account JSON and obtain an OAuth2 access token.
    async fn get_access_token(&self) -> Result<String, ProviderError> {
        let creds_json = self.credentials_json.as_deref().ok_or_else(|| {
            ProviderError::AuthenticationFailed {
                connection: self.name.clone(),
                reason: "No credentials provided — set 'credentials' to a service account JSON file path".to_string(),
            }
        })?;

        // Parse service account JSON
        let sa: serde_json::Value =
            serde_json::from_str(creds_json).map_err(|e| ProviderError::AuthenticationFailed {
                connection: self.name.clone(),
                reason: format!("Invalid service account JSON: {}", e),
            })?;

        let client_email =
            sa["client_email"]
                .as_str()
                .ok_or_else(|| ProviderError::AuthenticationFailed {
                    connection: self.name.clone(),
                    reason: "Missing 'client_email' in service account JSON".to_string(),
                })?;

        let private_key_pem =
            sa["private_key"]
                .as_str()
                .ok_or_else(|| ProviderError::AuthenticationFailed {
                    connection: self.name.clone(),
                    reason: "Missing 'private_key' in service account JSON".to_string(),
                })?;

        // Build JWT
        let now = chrono::Utc::now().timestamp();
        let claims = serde_json::json!({
            "iss": client_email,
            "scope": "https://www.googleapis.com/auth/bigquery",
            "aud": "https://oauth2.googleapis.com/token",
            "iat": now,
            "exp": now + 3600,
        });

        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        let key =
            jsonwebtoken::EncodingKey::from_rsa_pem(private_key_pem.as_bytes()).map_err(|e| {
                ProviderError::AuthenticationFailed {
                    connection: self.name.clone(),
                    reason: format!("Invalid RSA private key: {}", e),
                }
            })?;

        let jwt = jsonwebtoken::encode(&header, &claims, &key).map_err(|e| {
            ProviderError::AuthenticationFailed {
                connection: self.name.clone(),
                reason: format!("Failed to encode JWT: {}", e),
            }
        })?;

        // Exchange JWT for access token
        let resp = self
            .client
            .post("https://oauth2.googleapis.com/token")
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &jwt),
            ])
            .send()
            .await
            .map_err(|e| ProviderError::AuthenticationFailed {
                connection: self.name.clone(),
                reason: format!("Token exchange request failed: {}", e),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(ProviderError::AuthenticationFailed {
                connection: self.name.clone(),
                reason: format!("Token exchange failed (HTTP {}): {}", status, text),
            });
        }

        let token_json: serde_json::Value =
            resp.json()
                .await
                .map_err(|e| ProviderError::AuthenticationFailed {
                    connection: self.name.clone(),
                    reason: format!("Failed to parse token response: {}", e),
                })?;

        token_json["access_token"]
            .as_str()
            .map(String::from)
            .ok_or_else(|| ProviderError::AuthenticationFailed {
                connection: self.name.clone(),
                reason: "No access_token in token response".to_string(),
            })
    }

    /// Execute a BigQuery SQL query via the REST API.
    async fn execute_query(&self, query: &str) -> Result<serde_json::Value, ProviderError> {
        let token = self.get_access_token().await?;

        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries",
            self.project
        );

        let body = serde_json::json!({
            "query": query,
            "defaultDataset": {
                "projectId": self.project,
                "datasetId": self.dataset,
            },
            "location": self.location,
            "useLegacySql": false,
            "timeoutMs": 60_000,
            "maxResults": 10_000,
        });

        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderError::QueryFailed {
                connection: self.name.clone(),
                reason: format!("Query request failed: {}", e),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(ProviderError::QueryFailed {
                connection: self.name.clone(),
                reason: format!("Query failed (HTTP {}): {}", status, text),
            });
        }

        resp.json().await.map_err(|e| ProviderError::QueryFailed {
            connection: self.name.clone(),
            reason: format!("Failed to parse query response: {}", e),
        })
    }

    /// Parse BigQuery query response into SqlResult.
    fn parse_result(
        &self,
        json: &serde_json::Value,
        start: Instant,
    ) -> Result<SqlResult, ProviderError> {
        // Extract column names from schema
        let columns: Vec<String> = json["schema"]["fields"]
            .as_array()
            .map(|fields| {
                fields
                    .iter()
                    .filter_map(|f| f["name"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        // Extract row data — BigQuery uses { "f": [{"v": "value"}, ...] } format
        let sample_rows: Vec<Vec<serde_json::Value>> = json["rows"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .map(|row| {
                        row["f"]
                            .as_array()
                            .map(|cells| cells.iter().map(|cell| cell["v"].clone()).collect())
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .unwrap_or_default();

        let total_rows = json["totalRows"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(sample_rows.len() as u64);

        let num_dml_affected = json["numDmlAffectedRows"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok());

        let is_select = !columns.is_empty() && num_dml_affected.is_none();

        Ok(SqlResult {
            columns,
            sample_rows,
            rows_affected: num_dml_affected.unwrap_or(total_rows),
            rows_returned: if is_select { Some(total_rows) } else { None },
            execution_time_ms: start.elapsed().as_millis() as u64,
            metrics: HashMap::new(),
        })
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
                Capability::SqlQuery,
                Capability::SqlDdl,
                Capability::BulkLoad,
                Capability::IncrementalRead,
            ],
            is_stub: false,
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        // A real authenticated round-trip: mint an OAuth token from the
        // service account, then run `SELECT 1` via the jobs.query API. This
        // validates credentials and project access, not just TCP reachability
        // to www.googleapis.com.
        let start = Instant::now();
        match self.execute_query("SELECT 1").await {
            Ok(_) => Ok(ConnectionTestResult {
                success: true,
                message: format!("Authenticated to BigQuery project '{}'", self.project),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            }),
            Err(e) => Ok(ConnectionTestResult {
                success: false,
                message: format!("BigQuery connection failed: {}", e),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            }),
        }
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl SqlProvider for BigQueryProvider {
    async fn execute(
        &self,
        query: &str,
        _params: &HashMap<String, String>,
    ) -> Result<SqlResult, ProviderError> {
        if self.credentials_json.is_none() {
            return Err(ProviderError::AuthenticationFailed {
                connection: self.name.clone(),
                reason: "No credentials provided — set 'credentials' to a service account JSON file path".to_string(),
            });
        }

        let start = Instant::now();
        let json = self.execute_query(query).await?;
        self.parse_result(&json, start)
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        if self.credentials_json.is_none() {
            return Ok(vec![self.dataset.clone(), "INFORMATION_SCHEMA".to_string()]);
        }

        // Use datasets.list API
        match self.get_access_token().await {
            Ok(token) => {
                let url = format!(
                    "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets",
                    self.project
                );

                match self
                    .client
                    .get(&url)
                    .header("Authorization", format!("Bearer {}", token))
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        let json: serde_json::Value = resp.json().await.unwrap_or_default();
                        let datasets: Vec<String> = json["datasets"]
                            .as_array()
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|ds| {
                                        ds["datasetReference"]["datasetId"]
                                            .as_str()
                                            .map(String::from)
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();

                        if datasets.is_empty() {
                            Ok(vec![self.dataset.clone(), "INFORMATION_SCHEMA".to_string()])
                        } else {
                            Ok(datasets)
                        }
                    }
                    _ => Ok(vec![self.dataset.clone(), "INFORMATION_SCHEMA".to_string()]),
                }
            }
            Err(_) => Ok(vec![self.dataset.clone(), "INFORMATION_SCHEMA".to_string()]),
        }
    }

    async fn describe_table(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>, ProviderError> {
        let query = format!(
            "SELECT column_name, data_type, is_nullable \
             FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` \
             WHERE table_name = '{}' \
             ORDER BY ordinal_position",
            self.project,
            schema.replace('\'', "\\'"),
            table.replace('\'', "\\'"),
        );

        let result = self.execute(&query, &HashMap::new()).await?;

        Ok(result
            .sample_rows
            .iter()
            .map(|row| {
                let col_name = row
                    .first()
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let data_type = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("STRING")
                    .to_string();
                let is_nullable = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .map(|s| s == "YES")
                    .unwrap_or(true);

                ColumnInfo {
                    name: col_name,
                    data_type,
                    is_nullable,
                    default_value: None,
                    is_primary_key: false,
                }
            })
            .collect())
    }
}
