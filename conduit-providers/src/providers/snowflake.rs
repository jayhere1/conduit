//! Snowflake provider.
//!
//! Uses the Snowflake SQL REST API v2 for query execution. Authenticates
//! via username/password to obtain a session token, then submits SQL
//! statements as HTTP POST requests.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   my_snowflake:
//!     type: snowflake
//!     host: account_identifier          # e.g., xy12345.us-east-1
//!     database: analytics
//!     credentials: ${SNOWFLAKE_PASSWORD}
//!     user: conduit
//!     warehouse: compute_wh
//!     role: analyst
//!     schema: public
//! ```

use std::collections::HashMap;
use std::time::Instant;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

use super::{extra_str, resolve_credential};
use crate::errors::ProviderError;
use crate::traits::*;

pub struct SnowflakeProvider {
    name: String,
    account: String,
    database: String,
    user: String,
    password: Option<String>,
    warehouse: String,
    role: String,
    schema: String,
    client: reqwest::Client,
}

impl SnowflakeProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let account = config.host.clone().unwrap_or_default();
        let database = config
            .database
            .clone()
            .unwrap_or_else(|| "analytics".to_string());
        let user = extra_str(config, "user").unwrap_or_else(|| "conduit".to_string());
        let warehouse = extra_str(config, "warehouse").unwrap_or_else(|| "compute_wh".to_string());
        let role = extra_str(config, "role").unwrap_or_else(|| "public".to_string());
        let schema = extra_str(config, "schema").unwrap_or_else(|| "public".to_string());

        if account.is_empty() {
            return Err(ProviderError::InvalidConfig {
                connection: name.to_string(),
                reason: "Snowflake requires 'host' (account identifier)".to_string(),
            });
        }

        // Resolve password from credentials (may be env var reference)
        let password = config
            .credentials
            .as_deref()
            .map(resolve_credential)
            .transpose()
            .ok()
            .flatten();

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .unwrap_or_default();

        Ok(Self {
            name: name.to_string(),
            account,
            database,
            user,
            password,
            warehouse,
            role,
            schema,
            client,
        })
    }

    /// Base URL for the Snowflake SQL REST API.
    fn api_url(&self) -> String {
        // Strip trailing .snowflakecomputing.com if the user included it
        let account = self.account.trim_end_matches(".snowflakecomputing.com");
        format!("https://{}.snowflakecomputing.com", account)
    }

    /// Authenticate via username/password and return a session token.
    async fn login(&self) -> Result<String, ProviderError> {
        let password =
            self.password
                .as_deref()
                .ok_or_else(|| ProviderError::AuthenticationFailed {
                    connection: self.name.clone(),
                    reason: "No password provided in credentials".to_string(),
                })?;

        let url = format!(
            "{}/session/v1/login-request?warehouse={}&databaseName={}&roleName={}",
            self.api_url(),
            self.warehouse,
            self.database,
            self.role,
        );

        let body = serde_json::json!({
            "data": {
                "LOGIN_NAME": self.user,
                "PASSWORD": password,
                "ACCOUNT_NAME": self.account.split('.').next().unwrap_or(&self.account),
                "CLIENT_APP_ID": "Conduit",
                "CLIENT_APP_VERSION": "0.2.0",
            }
        });

        let resp = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderError::ConnectionFailed {
                name: self.name.clone(),
                reason: format!("Login request failed: {}", e),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(ProviderError::AuthenticationFailed {
                connection: self.name.clone(),
                reason: format!("Login failed (HTTP {}): {}", status, text),
            });
        }

        let json: serde_json::Value =
            resp.json()
                .await
                .map_err(|e| ProviderError::AuthenticationFailed {
                    connection: self.name.clone(),
                    reason: format!("Failed to parse login response: {}", e),
                })?;

        json["data"]["token"]
            .as_str()
            .map(String::from)
            .ok_or_else(|| ProviderError::AuthenticationFailed {
                connection: self.name.clone(),
                reason: "No token in login response".to_string(),
            })
    }

    /// Execute a SQL statement via the REST API.
    async fn execute_sql(&self, query: &str) -> Result<serde_json::Value, ProviderError> {
        let token = self.login().await?;

        let url = format!("{}/api/v2/statements", self.api_url());

        let body = serde_json::json!({
            "statement": query,
            "timeout": 60,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
            "role": self.role,
        });

        let resp = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("Authorization", format!("Snowflake Token=\"{}\"", token))
            .header("X-Snowflake-Authorization-Token-Type", "SNOWFLAKE")
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderError::QueryFailed {
                connection: self.name.clone(),
                reason: format!("Statement request failed: {}", e),
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

    /// Parse Snowflake SQL API response into SqlResult.
    fn parse_result(
        &self,
        json: &serde_json::Value,
        start: Instant,
    ) -> Result<SqlResult, ProviderError> {
        let metadata = &json["resultSetMetaData"];

        // Extract column info
        let columns: Vec<String> = metadata["rowType"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|col| col["name"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        // Extract row data
        let sample_rows: Vec<Vec<serde_json::Value>> = json["data"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .map(|row| row.as_array().cloned().unwrap_or_default())
                    .collect()
            })
            .unwrap_or_default();

        let num_rows = metadata["numRows"]
            .as_u64()
            .unwrap_or(sample_rows.len() as u64);

        // Determine if this was a SELECT-like query
        let is_select = !sample_rows.is_empty() || !columns.is_empty();

        Ok(SqlResult {
            columns,
            sample_rows,
            rows_affected: num_rows,
            rows_returned: if is_select { Some(num_rows) } else { None },
            execution_time_ms: start.elapsed().as_millis() as u64,
            metrics: HashMap::new(),
        })
    }
}

#[async_trait]
impl Provider for SnowflakeProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "snowflake".to_string(),
            display_name: format!(
                "Snowflake ({}/{})",
                self.account.split('.').next().unwrap_or(&self.account),
                self.database
            ),
            version: None,
            capabilities: vec![
                Capability::SqlQuery,
                Capability::SqlDdl,
                Capability::BulkLoad,
                Capability::IncrementalRead,
                Capability::Transactions,
            ],
            is_stub: false,
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        use std::time::Instant;

        // A real authenticated round-trip: login() obtains a session token,
        // then `SELECT 1` runs it. This validates account, credentials, and
        // warehouse — not just TCP reachability to port 443.
        let start = Instant::now();
        match self.execute_sql("SELECT 1").await {
            Ok(_) => {
                let version = self
                    .execute_sql("SELECT CURRENT_VERSION()")
                    .await
                    .ok()
                    .and_then(|v| {
                        v.pointer("/data/0/0")
                            .and_then(|x| x.as_str())
                            .map(str::to_string)
                    });
                Ok(ConnectionTestResult {
                    success: true,
                    message: format!("Authenticated to Snowflake account '{}'", self.account),
                    latency_ms: start.elapsed().as_millis() as u64,
                    server_version: version,
                })
            }
            Err(e) => Ok(ConnectionTestResult {
                success: false,
                message: format!("Snowflake connection failed: {}", e),
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
impl SqlProvider for SnowflakeProvider {
    async fn execute(
        &self,
        query: &str,
        _params: &HashMap<String, String>,
    ) -> Result<SqlResult, ProviderError> {
        if self.password.is_none() {
            return Err(ProviderError::AuthenticationFailed {
                connection: self.name.clone(),
                reason: "No password provided — set 'credentials' in connection config".to_string(),
            });
        }

        let start = Instant::now();
        let json = self.execute_sql(query).await?;
        self.parse_result(&json, start)
    }

    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError> {
        if self.password.is_none() {
            return Ok(vec![self.schema.clone(), "information_schema".to_string()]);
        }

        match self
            .execute(
                &format!("SHOW SCHEMAS IN DATABASE {}", self.database),
                &HashMap::new(),
            )
            .await
        {
            Ok(result) => {
                // SHOW SCHEMAS returns schema names in the first column
                let schemas: Vec<String> = result
                    .sample_rows
                    .iter()
                    .filter_map(|row| {
                        row.get(1)
                            .and_then(|v: &serde_json::Value| v.as_str().map(String::from))
                    })
                    .collect();
                if schemas.is_empty() {
                    Ok(vec![self.schema.clone(), "information_schema".to_string()])
                } else {
                    Ok(schemas)
                }
            }
            Err(_) => Ok(vec![self.schema.clone(), "information_schema".to_string()]),
        }
    }

    async fn describe_table(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>, ProviderError> {
        let query = format!(
            "SELECT column_name, data_type, is_nullable, column_default \
             FROM {}.information_schema.columns \
             WHERE table_schema = '{}' AND table_name = '{}' \
             ORDER BY ordinal_position",
            self.database,
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
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
                    .unwrap_or("TEXT")
                    .to_string();
                let is_nullable = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .map(|s| s == "YES")
                    .unwrap_or(true);
                let default_value = row.get(3).and_then(|v| v.as_str()).map(String::from);

                ColumnInfo {
                    name: col_name,
                    data_type,
                    is_nullable,
                    default_value,
                    is_primary_key: false,
                }
            })
            .collect())
    }
}
