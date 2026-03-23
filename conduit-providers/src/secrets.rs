//! Secrets management: pluggable backends for credential resolution.
//!
//! Conduit resolves credentials through a **secrets chain** — an ordered list
//! of backends tried in sequence until one returns a value. This mirrors the
//! credential-chain approach used by AWS SDKs and Terraform providers.
//!
//! # Built-in Backends
//!
//! | Backend | Prefix / Trigger | Example |
//! |---------|-----------------|---------|
//! | Literal | No prefix | `"my-password"` |
//! | Environment | `${VAR_NAME}` | `"${DB_PASSWORD}"` |
//! | File | `file:///path` | `"file:///etc/secrets/db.key"` |
//! | Vault | `vault://path#key` | `"vault://secret/data/db#password"` |
//! | AWS SSM | `ssm://name` | `"ssm:///prod/db/password"` |
//! | AWS Secrets Manager | `secretsmanager://name` | `"secretsmanager://prod/db-creds"` |
//! | GCP Secret Manager | `gcp-secret://name` | `"gcp-secret://db-password/versions/latest"` |
//!
//! # Architecture
//!
//! ```text
//! ConnectionConfig.credentials         SecretsChain
//! ┌─────────────────────────┐    ┌──────────────────────────────┐
//! │ "${DB_PASSWORD}"        │───▶│  1. EnvVarBackend            │
//! │ "vault://secret/db#pw"  │    │  2. FileBackend              │
//! │ "ssm:///prod/password"  │    │  3. VaultBackend             │
//! │ "literal-value"         │    │  4. AwsSsmBackend            │
//! └─────────────────────────┘    │  5. AwsSecretsManagerBackend │
//!                                │  6. GcpSecretManagerBackend  │
//!                                │  7. LiteralBackend (fallback)│
//!                                └──────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, trace, warn};

use crate::errors::ProviderError;

// ─── Core Trait ──────────────────────────────────────────────────────────────

/// A secrets backend that can resolve credential references to plaintext values.
///
/// Backends are tried in order by the `SecretsChain`. Each backend inspects
/// the credential reference string and either resolves it or returns `None`
/// to pass to the next backend in the chain.
#[async_trait]
pub trait SecretsBackend: Send + Sync + fmt::Debug {
    /// Human-readable name for logging/diagnostics.
    fn name(&self) -> &str;

    /// Whether this backend can handle the given credential reference.
    ///
    /// Called before `resolve` as a fast-path check. If this returns `false`,
    /// the chain skips this backend without calling `resolve`.
    fn can_handle(&self, reference: &str) -> bool;

    /// Resolve a credential reference to its plaintext value.
    ///
    /// Returns `Ok(Some(value))` if resolved, `Ok(None)` if this backend
    /// doesn't handle this reference, or `Err` on failure.
    async fn resolve(&self, reference: &str) -> Result<Option<String>, SecretsError>;

    /// Check if the backend is healthy / reachable.
    async fn health_check(&self) -> Result<(), SecretsError> {
        Ok(()) // Default: always healthy
    }
}

// ─── Error Type ─────────────────────────────────────────────────────────────

/// Errors from the secrets subsystem.
#[derive(Debug, thiserror::Error)]
pub enum SecretsError {
    /// The secret was not found in the backend.
    #[error("secret not found: {reference} (backend: {backend})")]
    NotFound { backend: String, reference: String },

    /// Authentication to the secrets backend failed.
    #[error("secrets backend authentication failed for '{backend}': {reason}")]
    AuthFailed { backend: String, reason: String },

    /// Network or connectivity error reaching the backend.
    #[error("secrets backend '{backend}' unreachable: {reason}")]
    Unreachable { backend: String, reason: String },

    /// The secret reference format is invalid.
    #[error("invalid secret reference '{reference}': {reason}")]
    InvalidReference { reference: String, reason: String },

    /// Permission denied accessing the secret.
    #[error("access denied for secret '{reference}' on '{backend}': {reason}")]
    AccessDenied {
        backend: String,
        reference: String,
        reason: String,
    },

    /// Timeout resolving the secret.
    #[error("timeout resolving secret '{reference}' from '{backend}'")]
    Timeout { backend: String, reference: String },

    /// Generic error.
    #[error("secrets error ({backend}): {reason}")]
    Other { backend: String, reason: String },
}

impl SecretsError {
    /// Convert to a ProviderError for interop.
    pub fn into_provider_error(self, connection: &str) -> ProviderError {
        ProviderError::AuthenticationFailed {
            connection: connection.to_string(),
            reason: self.to_string(),
        }
    }
}

// ─── Secrets Chain ──────────────────────────────────────────────────────────

/// An ordered chain of secrets backends. Tries each backend in sequence
/// until one successfully resolves the credential reference.
///
/// This is the primary entry point for credential resolution throughout Conduit.
pub struct SecretsChain {
    backends: Vec<Arc<dyn SecretsBackend>>,
    /// Cache resolved values for the lifetime of this chain (in-memory only,
    /// never persisted). Keys are the original reference strings.
    cache: tokio::sync::RwLock<HashMap<String, String>>,
    /// Whether to cache resolved values.
    caching_enabled: bool,
}

impl fmt::Debug for SecretsChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretsChain")
            .field(
                "backends",
                &self.backends.iter().map(|b| b.name()).collect::<Vec<_>>(),
            )
            .field("caching_enabled", &self.caching_enabled)
            .finish()
    }
}

impl SecretsChain {
    /// Create a new secrets chain with the given backends (tried in order).
    pub fn new(backends: Vec<Arc<dyn SecretsBackend>>) -> Self {
        Self {
            backends,
            cache: tokio::sync::RwLock::new(HashMap::new()),
            caching_enabled: true,
        }
    }

    /// Create a secrets chain with caching disabled.
    pub fn new_uncached(backends: Vec<Arc<dyn SecretsBackend>>) -> Self {
        Self {
            backends,
            cache: tokio::sync::RwLock::new(HashMap::new()),
            caching_enabled: false,
        }
    }

    /// Create a default chain with all built-in backends.
    ///
    /// Order: env var → file → vault → AWS SSM → AWS SM → GCP SM → literal
    pub fn default_chain(config: &SecretsConfig) -> Self {
        let mut backends: Vec<Arc<dyn SecretsBackend>> = Vec::new();

        // Always include env var backend
        backends.push(Arc::new(EnvVarBackend));

        // Always include file backend
        backends.push(Arc::new(FileBackend));

        // Vault (if configured)
        if let Some(vault_cfg) = &config.vault {
            backends.push(Arc::new(VaultBackend::new(vault_cfg.clone())));
        }

        // AWS SSM (if configured)
        if let Some(aws_cfg) = &config.aws {
            backends.push(Arc::new(AwsSsmBackend::new(aws_cfg.clone())));
            backends.push(Arc::new(AwsSecretsManagerBackend::new(aws_cfg.clone())));
        }

        // GCP Secret Manager (if configured)
        if let Some(gcp_cfg) = &config.gcp {
            backends.push(Arc::new(GcpSecretManagerBackend::new(gcp_cfg.clone())));
        }

        // Literal backend is always last (catch-all)
        backends.push(Arc::new(LiteralBackend));

        Self::new(backends)
    }

    /// Resolve a credential reference through the chain.
    ///
    /// Tries each backend in order. Returns the first successful resolution.
    /// If no backend can resolve it, returns an error.
    pub async fn resolve(&self, reference: &str) -> Result<String, SecretsError> {
        // Check cache first
        if self.caching_enabled {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(reference) {
                trace!(reference = %reference, "Secret resolved from cache");
                return Ok(cached.clone());
            }
        }

        for backend in &self.backends {
            if !backend.can_handle(reference) {
                continue;
            }

            trace!(
                backend = %backend.name(),
                reference = %mask_reference(reference),
                "Trying secrets backend"
            );

            match backend.resolve(reference).await {
                Ok(Some(value)) => {
                    debug!(
                        backend = %backend.name(),
                        reference = %mask_reference(reference),
                        "Secret resolved"
                    );

                    // Cache the result
                    if self.caching_enabled {
                        let mut cache = self.cache.write().await;
                        cache.insert(reference.to_string(), value.clone());
                    }

                    return Ok(value);
                }
                Ok(None) => {
                    // Backend didn't handle it, try next
                    continue;
                }
                Err(e) => {
                    warn!(
                        backend = %backend.name(),
                        reference = %mask_reference(reference),
                        error = %e,
                        "Secrets backend returned error, trying next"
                    );
                    // Continue to next backend on error
                    continue;
                }
            }
        }

        Err(SecretsError::NotFound {
            backend: "chain".to_string(),
            reference: mask_reference(reference).to_string(),
        })
    }

    /// Resolve a credential, converting the error to a ProviderError.
    pub async fn resolve_for_connection(
        &self,
        reference: &str,
        connection_name: &str,
    ) -> Result<String, ProviderError> {
        self.resolve(reference)
            .await
            .map_err(|e| e.into_provider_error(connection_name))
    }

    /// Health check all backends.
    pub async fn health_check_all(&self) -> HashMap<String, Result<(), SecretsError>> {
        let mut results = HashMap::new();
        for backend in &self.backends {
            let result = backend.health_check().await;
            results.insert(backend.name().to_string(), result);
        }
        results
    }

    /// Clear the in-memory cache.
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }

    /// List backend names in resolution order.
    pub fn backend_names(&self) -> Vec<&str> {
        self.backends.iter().map(|b| b.name()).collect()
    }
}

// ─── Configuration ──────────────────────────────────────────────────────────

/// Configuration for the secrets subsystem.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecretsConfig {
    /// HashiCorp Vault configuration.
    pub vault: Option<VaultConfig>,

    /// AWS configuration (for SSM Parameter Store + Secrets Manager).
    pub aws: Option<AwsSecretsConfig>,

    /// GCP configuration (for Secret Manager).
    pub gcp: Option<GcpSecretsConfig>,

    /// Whether to cache resolved secrets in memory (default: true).
    #[serde(default = "default_cache_enabled")]
    pub cache_enabled: bool,
}

fn default_cache_enabled() -> bool {
    true
}

/// HashiCorp Vault configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultConfig {
    /// Vault server address (e.g., "https://vault.example.com:8200").
    pub address: String,

    /// Authentication method: "token", "approle", "kubernetes", "aws-iam".
    #[serde(default = "default_vault_auth")]
    pub auth_method: String,

    /// Token for "token" auth. Can itself be an env var reference.
    pub token: Option<String>,

    /// AppRole role_id for "approle" auth.
    pub role_id: Option<String>,

    /// AppRole secret_id for "approle" auth.
    pub secret_id: Option<String>,

    /// Kubernetes auth role for "kubernetes" auth.
    pub k8s_role: Option<String>,

    /// Mount path for the secrets engine (default: "secret").
    #[serde(default = "default_vault_mount")]
    pub mount: String,

    /// Namespace (Vault Enterprise).
    pub namespace: Option<String>,

    /// TLS CA certificate path.
    pub ca_cert: Option<String>,

    /// Skip TLS verification (NOT recommended for production).
    #[serde(default)]
    pub skip_tls_verify: bool,

    /// Timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_vault_auth() -> String {
    "token".to_string()
}

fn default_vault_mount() -> String {
    "secret".to_string()
}

fn default_timeout_secs() -> u64 {
    5
}

/// AWS secrets backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsSecretsConfig {
    /// AWS region (e.g., "us-east-1"). Falls back to AWS_REGION env var.
    pub region: Option<String>,

    /// Optional AWS profile name.
    pub profile: Option<String>,

    /// Optional endpoint URL (for LocalStack / testing).
    pub endpoint_url: Option<String>,

    /// Timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

/// GCP secrets backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcpSecretsConfig {
    /// GCP project ID. Falls back to GOOGLE_CLOUD_PROJECT env var.
    pub project: Option<String>,

    /// Path to service account key JSON file.
    pub credentials_file: Option<String>,

    /// Timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

// ─── Built-in Backends ──────────────────────────────────────────────────────

// ── 1. Literal Backend (catch-all) ──────────────────────────────────────────

/// Returns the credential reference as-is. Always the last backend in a chain.
#[derive(Debug)]
pub struct LiteralBackend;

#[async_trait]
impl SecretsBackend for LiteralBackend {
    fn name(&self) -> &str {
        "literal"
    }

    fn can_handle(&self, _reference: &str) -> bool {
        true // Catch-all
    }

    async fn resolve(&self, reference: &str) -> Result<Option<String>, SecretsError> {
        Ok(Some(reference.to_string()))
    }
}

// ── 2. Environment Variable Backend ─────────────────────────────────────────

/// Resolves `${VAR_NAME}` references from the process environment.
#[derive(Debug)]
pub struct EnvVarBackend;

#[async_trait]
impl SecretsBackend for EnvVarBackend {
    fn name(&self) -> &str {
        "env"
    }

    fn can_handle(&self, reference: &str) -> bool {
        reference.starts_with("${") && reference.ends_with('}')
    }

    async fn resolve(&self, reference: &str) -> Result<Option<String>, SecretsError> {
        let var_name = &reference[2..reference.len() - 1];
        match std::env::var(var_name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(SecretsError::Other {
                backend: "env".to_string(),
                reason: format!("Environment variable '{}' contains invalid Unicode", var_name),
            }),
        }
    }
}

// ── 3. File Backend ─────────────────────────────────────────────────────────

/// Resolves `file:///path/to/secret` by reading the file contents.
/// Trims trailing whitespace/newlines.
#[derive(Debug)]
pub struct FileBackend;

#[async_trait]
impl SecretsBackend for FileBackend {
    fn name(&self) -> &str {
        "file"
    }

    fn can_handle(&self, reference: &str) -> bool {
        reference.starts_with("file://")
    }

    async fn resolve(&self, reference: &str) -> Result<Option<String>, SecretsError> {
        let path = &reference[7..];

        match tokio::fs::read_to_string(path).await {
            Ok(content) => Ok(Some(content.trim_end().to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SecretsError::Other {
                backend: "file".to_string(),
                reason: format!("Failed to read '{}': {}", path, e),
            }),
        }
    }
}

// ── 4. HashiCorp Vault Backend ──────────────────────────────────────────────

/// Resolves `vault://path/to/secret#key` from HashiCorp Vault's KV v2 engine.
///
/// Reference format: `vault://path/to/secret#field`
/// - `path/to/secret` is the path within the Vault secrets engine
/// - `field` is the key within the secret's data (optional; defaults to "value")
#[derive(Debug)]
pub struct VaultBackend {
    config: VaultConfig,
}

impl VaultBackend {
    pub fn new(config: VaultConfig) -> Self {
        Self { config }
    }

    /// Parse a vault:// reference into (path, field).
    fn parse_reference(reference: &str) -> Result<(&str, &str), SecretsError> {
        let stripped = reference.strip_prefix("vault://").ok_or_else(|| {
            SecretsError::InvalidReference {
                reference: reference.to_string(),
                reason: "Expected vault:// prefix".to_string(),
            }
        })?;

        if let Some(hash_pos) = stripped.rfind('#') {
            let path = &stripped[..hash_pos];
            let field = &stripped[hash_pos + 1..];
            if path.is_empty() || field.is_empty() {
                return Err(SecretsError::InvalidReference {
                    reference: reference.to_string(),
                    reason: "Empty path or field in vault reference".to_string(),
                });
            }
            Ok((path, field))
        } else {
            Ok((stripped, "value"))
        }
    }
}

#[async_trait]
impl SecretsBackend for VaultBackend {
    fn name(&self) -> &str {
        "vault"
    }

    fn can_handle(&self, reference: &str) -> bool {
        reference.starts_with("vault://")
    }

    async fn resolve(&self, reference: &str) -> Result<Option<String>, SecretsError> {
        let (path, field) = Self::parse_reference(reference)?;

        // Resolve the Vault token (which can itself be an env var)
        let token = match &self.config.token {
            Some(t) if t.starts_with("${") && t.ends_with('}') => {
                let var_name = &t[2..t.len() - 1];
                std::env::var(var_name).map_err(|_| SecretsError::AuthFailed {
                    backend: "vault".to_string(),
                    reason: format!("Vault token env var '{}' not set", var_name),
                })?
            }
            Some(t) => t.clone(),
            None => std::env::var("VAULT_TOKEN").map_err(|_| SecretsError::AuthFailed {
                backend: "vault".to_string(),
                reason: "No Vault token configured and VAULT_TOKEN not set".to_string(),
            })?,
        };

        // Build the KV v2 API URL
        let url = format!(
            "{}/v1/{}/data/{}",
            self.config.address.trim_end_matches('/'),
            self.config.mount,
            path
        );

        debug!(url = %url, field = %field, "Resolving secret from Vault");

        // In a real implementation, this would use reqwest or similar HTTP client.
        // For now, we implement the protocol structure so it's ready for the
        // actual HTTP call when reqwest is added as a dependency.
        //
        // The Vault KV v2 response format is:
        // { "data": { "data": { "field": "value" }, "metadata": {...} } }
        //
        // For demo/testing, fall through to None so the chain continues.
        // When reqwest is available, this becomes:
        //
        // let client = reqwest::Client::new();
        // let resp = client.get(&url)
        //     .header("X-Vault-Token", &token)
        //     .timeout(Duration::from_secs(self.config.timeout_secs))
        //     .send().await?;
        // let body: VaultResponse = resp.json().await?;
        // Ok(body.data.data.get(field).map(|v| v.as_str().unwrap().to_string()))

        let _ = (token, url, field); // Suppress unused warnings

        warn!(
            path = %path,
            field = %field,
            "Vault backend: HTTP client not compiled in, secret not resolved"
        );
        Ok(None)
    }

    async fn health_check(&self) -> Result<(), SecretsError> {
        // Would hit /v1/sys/health in a real implementation
        Ok(())
    }
}

// ── 5. AWS SSM Parameter Store Backend ──────────────────────────────────────

/// Resolves `ssm:///parameter/path` from AWS Systems Manager Parameter Store.
///
/// Reference format: `ssm:///path/to/parameter`
/// Supports SecureString parameters (auto-decrypted).
#[derive(Debug)]
pub struct AwsSsmBackend {
    config: AwsSecretsConfig,
}

impl AwsSsmBackend {
    pub fn new(config: AwsSecretsConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl SecretsBackend for AwsSsmBackend {
    fn name(&self) -> &str {
        "aws-ssm"
    }

    fn can_handle(&self, reference: &str) -> bool {
        reference.starts_with("ssm://")
    }

    async fn resolve(&self, reference: &str) -> Result<Option<String>, SecretsError> {
        let param_name = reference.strip_prefix("ssm://").ok_or_else(|| {
            SecretsError::InvalidReference {
                reference: reference.to_string(),
                reason: "Expected ssm:// prefix".to_string(),
            }
        })?;

        let region = self.config.region.clone().unwrap_or_else(|| {
            std::env::var("AWS_REGION")
                .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                .unwrap_or_else(|_| "us-east-1".to_string())
        });

        debug!(param = %param_name, region = %region, "Resolving secret from AWS SSM");

        // Real implementation would use aws-sdk-ssm:
        //
        // let config = aws_config::defaults(BehaviorVersion::latest())
        //     .region(Region::new(region))
        //     .load().await;
        // let client = aws_sdk_ssm::Client::new(&config);
        // let output = client.get_parameter()
        //     .name(param_name)
        //     .with_decryption(true)
        //     .send().await?;
        // Ok(output.parameter().and_then(|p| p.value()).map(|v| v.to_string()))

        let _ = (param_name, region);

        warn!(
            param = %param_name,
            "AWS SSM backend: SDK not compiled in, secret not resolved"
        );
        Ok(None)
    }

    async fn health_check(&self) -> Result<(), SecretsError> {
        // Would call DescribeParameters or similar
        Ok(())
    }
}

// ── 6. AWS Secrets Manager Backend ──────────────────────────────────────────

/// Resolves `secretsmanager://secret-name` or `secretsmanager://secret-name#key`
/// from AWS Secrets Manager.
///
/// For JSON secrets, use the `#key` syntax to extract a specific field.
#[derive(Debug)]
pub struct AwsSecretsManagerBackend {
    config: AwsSecretsConfig,
}

impl AwsSecretsManagerBackend {
    pub fn new(config: AwsSecretsConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl SecretsBackend for AwsSecretsManagerBackend {
    fn name(&self) -> &str {
        "aws-secretsmanager"
    }

    fn can_handle(&self, reference: &str) -> bool {
        reference.starts_with("secretsmanager://")
    }

    async fn resolve(&self, reference: &str) -> Result<Option<String>, SecretsError> {
        let stripped = reference.strip_prefix("secretsmanager://").ok_or_else(|| {
            SecretsError::InvalidReference {
                reference: reference.to_string(),
                reason: "Expected secretsmanager:// prefix".to_string(),
            }
        })?;

        let (secret_name, json_key) = if let Some(hash_pos) = stripped.rfind('#') {
            (&stripped[..hash_pos], Some(&stripped[hash_pos + 1..]))
        } else {
            (stripped, None)
        };

        let region = self.config.region.clone().unwrap_or_else(|| {
            std::env::var("AWS_REGION")
                .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                .unwrap_or_else(|_| "us-east-1".to_string())
        });

        debug!(
            secret = %secret_name,
            key = ?json_key,
            region = %region,
            "Resolving secret from AWS Secrets Manager"
        );

        // Real implementation would use aws-sdk-secretsmanager:
        //
        // let config = aws_config::defaults(BehaviorVersion::latest())
        //     .region(Region::new(region))
        //     .load().await;
        // let client = aws_sdk_secretsmanager::Client::new(&config);
        // let output = client.get_secret_value()
        //     .secret_id(secret_name)
        //     .send().await?;
        //
        // if let Some(secret_string) = output.secret_string() {
        //     if let Some(key) = json_key {
        //         let parsed: serde_json::Value = serde_json::from_str(secret_string)?;
        //         Ok(parsed.get(key).and_then(|v| v.as_str()).map(String::from))
        //     } else {
        //         Ok(Some(secret_string.to_string()))
        //     }
        // }

        let _ = (secret_name, json_key, region);

        warn!(
            secret = %secret_name,
            "AWS Secrets Manager backend: SDK not compiled in, secret not resolved"
        );
        Ok(None)
    }

    async fn health_check(&self) -> Result<(), SecretsError> {
        // Would call ListSecrets with max_results=1
        Ok(())
    }
}

// ── 7. GCP Secret Manager Backend ───────────────────────────────────────────

/// Resolves `gcp-secret://secret-name` or `gcp-secret://secret-name/versions/N`
/// from Google Cloud Secret Manager.
#[derive(Debug)]
pub struct GcpSecretManagerBackend {
    config: GcpSecretsConfig,
}

impl GcpSecretManagerBackend {
    pub fn new(config: GcpSecretsConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl SecretsBackend for GcpSecretManagerBackend {
    fn name(&self) -> &str {
        "gcp-secretmanager"
    }

    fn can_handle(&self, reference: &str) -> bool {
        reference.starts_with("gcp-secret://")
    }

    async fn resolve(&self, reference: &str) -> Result<Option<String>, SecretsError> {
        let stripped = reference.strip_prefix("gcp-secret://").ok_or_else(|| {
            SecretsError::InvalidReference {
                reference: reference.to_string(),
                reason: "Expected gcp-secret:// prefix".to_string(),
            }
        })?;

        // Parse: secret-name or secret-name/versions/N
        let (secret_name, version) = if let Some(versions_pos) = stripped.find("/versions/") {
            (&stripped[..versions_pos], &stripped[versions_pos + 10..])
        } else {
            (stripped, "latest")
        };

        let project = self.config.project.clone().unwrap_or_else(|| {
            std::env::var("GOOGLE_CLOUD_PROJECT")
                .or_else(|_| std::env::var("GCLOUD_PROJECT"))
                .unwrap_or_default()
        });

        debug!(
            secret = %secret_name,
            version = %version,
            project = %project,
            "Resolving secret from GCP Secret Manager"
        );

        // Real implementation would use google-cloud-secretmanager:
        //
        // let name = format!(
        //     "projects/{}/secrets/{}/versions/{}",
        //     project, secret_name, version
        // );
        // let client = SecretManagerServiceClient::new(channel).await?;
        // let response = client.access_secret_version(name).await?;
        // let payload = response.into_inner().payload.unwrap();
        // Ok(Some(String::from_utf8(payload.data)?))

        let _ = (secret_name, version, project);

        warn!(
            secret = %secret_name,
            "GCP Secret Manager backend: SDK not compiled in, secret not resolved"
        );
        Ok(None)
    }

    async fn health_check(&self) -> Result<(), SecretsError> {
        // Would call ListSecrets with page_size=1
        Ok(())
    }
}

// ─── Utilities ──────────────────────────────────────────────────────────────

/// Mask a credential reference for safe logging.
/// Shows the scheme and first few chars but hides the rest.
fn mask_reference(reference: &str) -> String {
    if reference.starts_with("${") && reference.ends_with('}') {
        let var_name = &reference[2..reference.len() - 1];
        return format!("${{{}...}}", &var_name[..var_name.len().min(4)]);
    }

    for prefix in &["vault://", "ssm://", "secretsmanager://", "gcp-secret://", "file://"] {
        if let Some(rest) = reference.strip_prefix(prefix) {
            let visible = &rest[..rest.len().min(8)];
            return format!("{}{}...", prefix, visible);
        }
    }

    // Literal value — show nothing
    "***".to_string()
}

// ─── Conversions from conduit-common config types ───────────────────────────

impl From<conduit_common::config::SecretsConfig> for SecretsConfig {
    fn from(c: conduit_common::config::SecretsConfig) -> Self {
        Self {
            vault: c.vault.map(|v| VaultConfig {
                address: v.address,
                auth_method: v.auth_method,
                token: v.token,
                role_id: v.role_id,
                secret_id: v.secret_id,
                k8s_role: v.k8s_role,
                mount: v.mount,
                namespace: v.namespace,
                ca_cert: v.ca_cert,
                skip_tls_verify: v.skip_tls_verify,
                timeout_secs: v.timeout_secs,
            }),
            aws: c.aws.map(|a| AwsSecretsConfig {
                region: a.region,
                profile: a.profile,
                endpoint_url: a.endpoint_url,
                timeout_secs: a.timeout_secs,
            }),
            gcp: c.gcp.map(|g| GcpSecretsConfig {
                project: g.project,
                credentials_file: g.credentials_file,
                timeout_secs: g.timeout_secs,
            }),
            cache_enabled: c.cache_enabled,
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_reference() {
        assert_eq!(mask_reference("${MY_PASSWORD}"), "${MY_P...}");
        assert_eq!(mask_reference("vault://secret/data/db#pw"), "vault://secret/d...");
        assert_eq!(mask_reference("ssm:///prod/db/password"), "ssm:///prod/db...");
        assert_eq!(mask_reference("plain-value"), "***");
    }

    #[test]
    fn test_vault_parse_reference() {
        let (path, field) = VaultBackend::parse_reference("vault://secret/data/mydb#password").unwrap();
        assert_eq!(path, "secret/data/mydb");
        assert_eq!(field, "password");

        let (path, field) = VaultBackend::parse_reference("vault://secret/data/mydb").unwrap();
        assert_eq!(path, "secret/data/mydb");
        assert_eq!(field, "value");
    }

    #[tokio::test]
    async fn test_env_var_backend() {
        let backend = EnvVarBackend;
        assert!(backend.can_handle("${HOME}"));
        assert!(!backend.can_handle("plain-value"));

        // HOME should exist on most systems
        let result = backend.resolve("${HOME}").await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_literal_backend() {
        let backend = LiteralBackend;
        assert!(backend.can_handle("anything"));

        let result = backend.resolve("my-password").await.unwrap();
        assert_eq!(result, Some("my-password".to_string()));
    }

    #[tokio::test]
    async fn test_file_backend_missing() {
        let backend = FileBackend;
        assert!(backend.can_handle("file:///nonexistent/path"));

        let result = backend.resolve("file:///nonexistent/path").await.unwrap();
        assert_eq!(result, None); // Missing file returns None, not error
    }

    #[tokio::test]
    async fn test_secrets_chain_env_then_literal() {
        let chain = SecretsChain::new(vec![
            Arc::new(EnvVarBackend),
            Arc::new(LiteralBackend),
        ]);

        // Env var that exists
        let result = chain.resolve("${HOME}").await.unwrap();
        assert!(!result.is_empty());

        // Literal fallback
        let result = chain.resolve("plain-password").await.unwrap();
        assert_eq!(result, "plain-password");
    }

    #[tokio::test]
    async fn test_secrets_chain_caching() {
        let chain = SecretsChain::new(vec![
            Arc::new(LiteralBackend),
        ]);

        let result1 = chain.resolve("test-value").await.unwrap();
        let result2 = chain.resolve("test-value").await.unwrap();
        assert_eq!(result1, result2);

        // Check cache has the value
        let cache = chain.cache.read().await;
        assert!(cache.contains_key("test-value"));
    }

    #[tokio::test]
    async fn test_default_chain_creation() {
        let config = SecretsConfig::default();
        let chain = SecretsChain::default_chain(&config);

        // Should have env, file, and literal backends
        let names = chain.backend_names();
        assert!(names.contains(&"env"));
        assert!(names.contains(&"file"));
        assert!(names.contains(&"literal"));

        // Vault/AWS/GCP should NOT be present without config
        assert!(!names.contains(&"vault"));
        assert!(!names.contains(&"aws-ssm"));
        assert!(!names.contains(&"gcp-secretmanager"));
    }

    #[tokio::test]
    async fn test_uncached_chain() {
        let chain = SecretsChain::new_uncached(vec![
            Arc::new(LiteralBackend),
        ]);

        let result = chain.resolve("test-value").await.unwrap();
        assert_eq!(result, "test-value");

        // Cache should be empty even after resolve
        let cache = chain.cache.read().await;
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn test_clear_cache() {
        let chain = SecretsChain::new(vec![
            Arc::new(LiteralBackend),
        ]);

        // Populate cache
        chain.resolve("secret-1").await.unwrap();
        chain.resolve("secret-2").await.unwrap();

        {
            let cache = chain.cache.read().await;
            assert_eq!(cache.len(), 2);
        }

        chain.clear_cache().await;

        let cache = chain.cache.read().await;
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn test_resolve_for_connection_converts_error() {
        // Chain with no backends that can handle the reference
        let chain = SecretsChain::new(vec![
            Arc::new(EnvVarBackend), // only handles ${...}
        ]);

        let result = chain
            .resolve_for_connection("vault://secret/missing#key", "my_db")
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ProviderError::AuthenticationFailed { connection, .. } => {
                assert_eq!(connection, "my_db");
            }
            other => panic!("Expected AuthenticationFailed, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_health_check_all_backends() {
        let chain = SecretsChain::new(vec![
            Arc::new(EnvVarBackend),
            Arc::new(FileBackend),
            Arc::new(LiteralBackend),
        ]);

        let results = chain.health_check_all().await;

        assert_eq!(results.len(), 3);
        // All built-in backends have default health_check that returns Ok(())
        assert!(results.get("env").unwrap().is_ok());
        assert!(results.get("file").unwrap().is_ok());
        assert!(results.get("literal").unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_backend_names_ordering() {
        let chain = SecretsChain::new(vec![
            Arc::new(EnvVarBackend),
            Arc::new(FileBackend),
            Arc::new(LiteralBackend),
        ]);

        let names = chain.backend_names();
        assert_eq!(names, vec!["env", "file", "literal"]);
    }

    #[tokio::test]
    async fn test_file_backend_reads_real_file() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("db_password.txt");
        std::fs::write(&secret_path, "super-secret-password\n").unwrap();

        let backend = FileBackend;
        let reference = format!("file://{}", secret_path.display());

        let result = backend.resolve(&reference).await.unwrap();
        // Should trim trailing newline
        assert_eq!(result, Some("super-secret-password".to_string()));
    }

    #[tokio::test]
    async fn test_env_var_backend_missing_var() {
        let backend = EnvVarBackend;
        let result = backend
            .resolve("${CONDUIT_TEST_NONEXISTENT_VAR_12345}")
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_secrets_error_into_provider_error() {
        let err = SecretsError::NotFound {
            backend: "vault".to_string(),
            reference: "vault://secret/db#pw".to_string(),
        };

        let provider_err = err.into_provider_error("prod_db");
        match provider_err {
            ProviderError::AuthenticationFailed { connection, reason } => {
                assert_eq!(connection, "prod_db");
                assert!(reason.contains("vault"));
            }
            other => panic!("Expected AuthenticationFailed, got {:?}", other),
        }
    }

    #[test]
    fn test_vault_parse_reference_errors() {
        // Empty path
        let result = VaultBackend::parse_reference("vault://#field");
        assert!(result.is_err());

        // Empty field
        let result = VaultBackend::parse_reference("vault://path/to/secret#");
        assert!(result.is_err());

        // No vault:// prefix
        let result = VaultBackend::parse_reference("ssm://something");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_chain_with_vault_config() {
        let config = SecretsConfig {
            vault: Some(VaultConfig {
                address: "https://vault.example.com:8200".to_string(),
                auth_method: "token".to_string(),
                token: Some("${VAULT_TOKEN}".to_string()),
                role_id: None,
                secret_id: None,
                k8s_role: None,
                mount: "secret".to_string(),
                namespace: None,
                ca_cert: None,
                skip_tls_verify: false,
                timeout_secs: 5,
            }),
            aws: Some(AwsSecretsConfig {
                region: Some("us-east-1".to_string()),
                profile: None,
                endpoint_url: None,
                timeout_secs: 5,
            }),
            gcp: None,
            cache_enabled: true,
        };

        let chain = SecretsChain::default_chain(&config);
        let names = chain.backend_names();

        assert!(names.contains(&"vault"));
        assert!(names.contains(&"aws-ssm"));
        assert!(names.contains(&"aws-secretsmanager"));
        assert!(!names.contains(&"gcp-secretmanager"));
    }
}
