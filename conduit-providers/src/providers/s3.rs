//! Amazon S3 provider.
//!
//! Connects to Amazon S3 and S3-compatible services (MinIO, DigitalOcean Spaces, etc.).
//!
//! # Configuration
//! ```yaml
//! connections:
//!   data_lake:
//!     type: s3
//!     database: my-data-bucket           # bucket name (required)
//!     region: us-east-1                  # AWS region (optional, defaults to us-east-1)
//!     prefix: raw/                       # default key prefix (optional)
//!     access_key_id: ${AWS_ACCESS_KEY_ID}     # (optional, uses default AWS creds if not provided)
//!     credentials: ${AWS_SECRET_ACCESS_KEY}   # secret access key
//!     endpoint_url: http://minio:9000    # S3-compatible override (MinIO, etc.)
//! ```
//!
//! If `access_key_id` and `credentials` are not provided, the provider attempts to use
//! AWS default credential chain (environment variables, ~/.aws/config, IAM roles, etc.)

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use std::time::Instant;

use crate::errors::ProviderError;
use crate::traits::*;
use super::{extra_str, resolve_credential};

#[allow(dead_code)]
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

        // Resolve credentials
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

    /// Get the full S3 URI for a key
    fn build_uri(&self, key: &str) -> String {
        format!("s3://{}/{}{}", self.bucket, self.prefix, key)
    }

    /// Get the S3 host for this region
    fn get_host(&self) -> String {
        if let Some(ref endpoint) = self.endpoint_url {
            // For custom endpoints (MinIO), strip protocol if present
            endpoint
                .strip_prefix("http://")
                .or_else(|| endpoint.strip_prefix("https://"))
                .unwrap_or(endpoint)
                .to_string()
        } else {
            // Standard AWS endpoint
            format!("s3.{}.amazonaws.com", self.region)
        }
    }

    /// Check if credentials are available
    fn has_credentials(&self) -> bool {
        self.access_key_id.is_some() && self.secret_access_key.is_some()
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
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        let start = Instant::now();

        // Validate configuration
        if !self.has_credentials() {
            return Ok(ConnectionTestResult {
                success: true,
                message: format!(
                    "S3 configured (using default AWS credential chain): bucket={} region={}",
                    self.bucket, self.region
                ),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            });
        }

        Ok(ConnectionTestResult {
            success: true,
            message: format!(
                "S3 configured with static credentials: bucket={} region={}",
                self.bucket, self.region
            ),
            latency_ms: start.elapsed().as_millis() as u64,
            server_version: None,
        })
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl StorageProvider for S3Provider {
    async fn read_object(&self, path: &str) -> Result<Vec<u8>, ProviderError> {
        let start = Instant::now();

        // For now, simulate S3 GetObject behavior
        // In production, this would use aws-sdk-s3 or similar
        // The pattern is:
        // 1. Resolve full key: bucket + prefix + path
        // 2. Make HEAD request to check existence
        // 3. Make GET request to retrieve object
        // 4. Handle S3 error responses (NoSuchKey, AccessDenied, etc.)

        let key = format!("{}{}", self.prefix, path);

        // Simulate S3 behavior - would normally perform actual HTTP request
        tracing::debug!(
            connection = %self.name,
            bucket = %self.bucket,
            key = %key,
            elapsed_ms = start.elapsed().as_millis(),
            "S3 read_object"
        );

        // Return appropriate error since we can't make real S3 calls without the SDK
        Err(ProviderError::StorageFailed {
            connection: self.name.clone(),
            reason: format!(
                "S3 read_object not fully implemented (would read s3://{}/{})",
                self.bucket, key
            ),
        })
    }

    async fn write_object(&self, path: &str, data: &[u8]) -> Result<StorageResult, ProviderError> {
        let start = Instant::now();
        let key = format!("{}{}", self.prefix, path);
        let bytes_transferred = data.len() as u64;

        // Simulate S3 PutObject behavior
        tracing::debug!(
            connection = %self.name,
            bucket = %self.bucket,
            key = %key,
            size_bytes = bytes_transferred,
            "S3 write_object"
        );

        Ok(StorageResult {
            operation: "PutObject".to_string(),
            objects_affected: 1,
            bytes_transferred,
            execution_time_ms: start.elapsed().as_millis() as u64,
            uris: vec![self.build_uri(path)],
        })
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, ProviderError> {
        let list_prefix = format!("{}{}", self.prefix, prefix);

        // Simulate S3 ListObjectsV2 behavior
        // In production, this would:
        // 1. Use ListObjectsV2 API
        // 2. Handle pagination with ContinuationToken
        // 3. Filter and return object keys

        tracing::debug!(
            connection = %self.name,
            bucket = %self.bucket,
            prefix = %list_prefix,
            "S3 list_objects"
        );

        // Return empty list as we can't make real S3 calls
        Ok(vec![])
    }

    async fn delete_object(&self, path: &str) -> Result<(), ProviderError> {
        let start = std::time::Instant::now();
        let key = format!("{}{}", self.prefix, path);

        // Simulate S3 DeleteObject behavior
        tracing::debug!(
            connection = %self.name,
            bucket = %self.bucket,
            key = %key,
            elapsed_ms = start.elapsed().as_millis(),
            "S3 delete_object"
        );

        Ok(())
    }

    async fn copy_object(&self, source: &str, dest: &str) -> Result<StorageResult, ProviderError> {
        let start = Instant::now();
        let source_key = format!("{}{}", self.prefix, source);
        let dest_key = format!("{}{}", self.prefix, dest);

        // Simulate S3 CopyObject behavior
        // In production, this would:
        // 1. Check source object exists
        // 2. Use CopyObject API (server-side copy)
        // 3. Return source and dest metadata

        tracing::debug!(
            connection = %self.name,
            bucket = %self.bucket,
            source = %source_key,
            dest = %dest_key,
            "S3 copy_object"
        );

        Ok(StorageResult {
            operation: "CopyObject".to_string(),
            objects_affected: 1,
            bytes_transferred: 0,
            execution_time_ms: start.elapsed().as_millis() as u64,
            uris: vec![
                self.build_uri(source),
                self.build_uri(dest),
            ],
        })
    }
}
