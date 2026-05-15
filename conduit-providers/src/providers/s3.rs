//! Amazon S3 provider.
//!
//! Uses the `rust-s3` crate for real S3 operations. Supports S3-compatible
//! services (MinIO, DigitalOcean Spaces, etc.) via endpoint_url override.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   data_lake:
//!     type: s3
//!     database: my-data-bucket           # bucket name (required)
//!     region: us-east-1                  # AWS region (optional, defaults to us-east-1)
//!     prefix: raw/                       # default key prefix (optional)
//!     access_key_id: ${AWS_ACCESS_KEY_ID}
//!     credentials: ${AWS_SECRET_ACCESS_KEY}
//!     endpoint_url: http://minio:9000    # S3-compatible override
//! ```

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use std::time::Instant;

use super::{extra_str, resolve_credential};
use crate::errors::ProviderError;
use crate::traits::*;

pub struct S3Provider {
    name: String,
    bucket: String,
    region: String,
    prefix: String,
    endpoint_url: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
}

impl S3Provider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let bucket = config.database.clone().unwrap_or_default();
        let region = extra_str(config, "region").unwrap_or_else(|| "us-east-1".to_string());
        let prefix = extra_str(config, "prefix").unwrap_or_default();
        let endpoint_url = extra_str(config, "endpoint_url").or_else(|| config.host.clone());

        if bucket.is_empty() {
            return Err(ProviderError::InvalidConfig {
                connection: name.to_string(),
                reason: "S3 requires 'database' (bucket name)".to_string(),
            });
        }

        let access_key_id = extra_str(config, "access_key_id")
            .map(|key_ref| resolve_credential(&key_ref))
            .transpose()?;

        let secret_access_key = config
            .credentials
            .as_deref()
            .map(resolve_credential)
            .transpose()?;

        Ok(Self {
            name: name.to_string(),
            bucket,
            region,
            prefix,
            endpoint_url,
            access_key_id,
            secret_access_key,
        })
    }

    /// Build the S3 bucket handle.
    fn build_bucket(&self) -> Result<s3::Bucket, ProviderError> {
        let region = if let Some(ref endpoint) = self.endpoint_url {
            s3::Region::Custom {
                region: self.region.clone(),
                endpoint: endpoint.clone(),
            }
        } else {
            self.region.parse().unwrap_or(s3::Region::UsEast1)
        };

        let credentials = if let (Some(ref key), Some(ref secret)) =
            (&self.access_key_id, &self.secret_access_key)
        {
            s3::creds::Credentials::new(Some(key), Some(secret), None, None, None).map_err(|e| {
                ProviderError::AuthenticationFailed {
                    connection: self.name.clone(),
                    reason: format!("Invalid S3 credentials: {}", e),
                }
            })?
        } else {
            // Try default credential chain
            s3::creds::Credentials::default().map_err(|e| ProviderError::AuthenticationFailed {
                connection: self.name.clone(),
                reason: format!("No S3 credentials available: {}", e),
            })?
        };

        let mut bucket = s3::Bucket::new(&self.bucket, region, credentials).map_err(|e| {
            ProviderError::ConnectionFailed {
                name: self.name.clone(),
                reason: format!("Failed to create S3 bucket handle: {}", e),
            }
        })?;

        // For path-style addressing (MinIO, local dev)
        if self.endpoint_url.is_some() {
            bucket.set_path_style();
        }

        Ok(*bucket)
    }

    fn full_key(&self, path: &str) -> String {
        format!("{}{}", self.prefix, path)
    }
}

#[async_trait]
impl Provider for S3Provider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "s3".to_string(),
            display_name: format!("S3 (s3://{}/{})", self.bucket, self.prefix),
            version: None,
            capabilities: vec![
                Capability::StorageRead,
                Capability::StorageWrite,
                Capability::StorageList,
                Capability::BulkLoad,
            ],
            is_stub: false,
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        let start = Instant::now();

        match self.build_bucket() {
            Ok(bucket) => {
                // Try to list with max 1 result to verify access
                match bucket
                    .list(self.prefix.clone(), Some("/".to_string()))
                    .await
                {
                    Ok(_) => Ok(ConnectionTestResult {
                        success: true,
                        message: format!(
                            "S3 bucket '{}' accessible (region={})",
                            self.bucket, self.region
                        ),
                        latency_ms: start.elapsed().as_millis() as u64,
                        server_version: None,
                    }),
                    Err(e) => Ok(ConnectionTestResult {
                        success: false,
                        message: format!("S3 access failed: {}", e),
                        latency_ms: start.elapsed().as_millis() as u64,
                        server_version: None,
                    }),
                }
            }
            Err(e) => Ok(ConnectionTestResult {
                success: false,
                message: format!("S3 configuration error: {}", e),
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
impl StorageProvider for S3Provider {
    async fn read_object(&self, path: &str) -> Result<Vec<u8>, ProviderError> {
        let bucket = self.build_bucket()?;
        let key = self.full_key(path);

        let response = bucket
            .get_object(&key)
            .await
            .map_err(|e| ProviderError::StorageFailed {
                connection: self.name.clone(),
                reason: format!("S3 GetObject failed for '{}': {}", key, e),
            })?;

        if response.status_code() != 200 {
            return Err(ProviderError::StorageFailed {
                connection: self.name.clone(),
                reason: format!(
                    "S3 GetObject returned status {} for '{}'",
                    response.status_code(),
                    key
                ),
            });
        }

        Ok(response.to_vec())
    }

    async fn write_object(&self, path: &str, data: &[u8]) -> Result<StorageResult, ProviderError> {
        let start = Instant::now();
        let bucket = self.build_bucket()?;
        let key = self.full_key(path);
        let bytes = data.len() as u64;

        let response =
            bucket
                .put_object(&key, data)
                .await
                .map_err(|e| ProviderError::StorageFailed {
                    connection: self.name.clone(),
                    reason: format!("S3 PutObject failed for '{}': {}", key, e),
                })?;

        if response.status_code() != 200 {
            return Err(ProviderError::StorageFailed {
                connection: self.name.clone(),
                reason: format!(
                    "S3 PutObject returned status {} for '{}'",
                    response.status_code(),
                    key
                ),
            });
        }

        Ok(StorageResult {
            operation: "PutObject".to_string(),
            objects_affected: 1,
            bytes_transferred: bytes,
            execution_time_ms: start.elapsed().as_millis() as u64,
            uris: vec![format!("s3://{}/{}", self.bucket, key)],
        })
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, ProviderError> {
        let bucket = self.build_bucket()?;
        let full_prefix = format!("{}{}", self.prefix, prefix);

        let results =
            bucket
                .list(full_prefix, None)
                .await
                .map_err(|e| ProviderError::StorageFailed {
                    connection: self.name.clone(),
                    reason: format!("S3 ListObjects failed: {}", e),
                })?;

        let keys: Vec<String> = results
            .into_iter()
            .flat_map(|page| page.contents.into_iter().map(|obj| obj.key))
            .collect();

        Ok(keys)
    }

    async fn delete_object(&self, path: &str) -> Result<(), ProviderError> {
        let bucket = self.build_bucket()?;
        let key = self.full_key(path);

        let response =
            bucket
                .delete_object(&key)
                .await
                .map_err(|e| ProviderError::StorageFailed {
                    connection: self.name.clone(),
                    reason: format!("S3 DeleteObject failed for '{}': {}", key, e),
                })?;

        if response.status_code() != 204 && response.status_code() != 200 {
            return Err(ProviderError::StorageFailed {
                connection: self.name.clone(),
                reason: format!(
                    "S3 DeleteObject returned status {} for '{}'",
                    response.status_code(),
                    key
                ),
            });
        }

        Ok(())
    }

    async fn copy_object(&self, source: &str, dest: &str) -> Result<StorageResult, ProviderError> {
        let start = Instant::now();

        // S3 copy: read source, write to dest
        let data = self.read_object(source).await?;
        let bytes = data.len() as u64;
        self.write_object(dest, &data).await?;

        Ok(StorageResult {
            operation: "CopyObject".to_string(),
            objects_affected: 1,
            bytes_transferred: bytes,
            execution_time_ms: start.elapsed().as_millis() as u64,
            uris: vec![
                format!("s3://{}/{}", self.bucket, self.full_key(source)),
                format!("s3://{}/{}", self.bucket, self.full_key(dest)),
            ],
        })
    }
}
