//! Conduit project configuration (conduit.yaml).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use crate::error::{ConduitError, ConduitResult};

/// Top-level Conduit project configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConduitConfig {
    /// Project name.
    pub name: String,

    /// Path to DAG definitions (default: "dags/").
    #[serde(default = "default_dags_path")]
    pub dags_path: String,

    /// Named connections.
    #[serde(default)]
    pub connections: HashMap<String, ConnectionConfig>,

    /// Named resource pools.
    #[serde(default)]
    pub pools: HashMap<String, PoolConfig>,

    /// Global defaults.
    #[serde(default)]
    pub defaults: DefaultsConfig,

    /// Secrets backend configuration.
    ///
    /// Configures how Conduit resolves credential references in connection
    /// configs. Supports Vault, AWS SSM, AWS Secrets Manager, GCP Secret
    /// Manager, environment variables, files, and literal values.
    #[serde(default)]
    pub secrets: SecretsConfig,
}

/// Configuration for external secrets backends.
///
/// This mirrors `conduit_providers::secrets::SecretsConfig` but lives in
/// conduit-common so the config can be parsed before providers are loaded.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecretsConfig {
    /// HashiCorp Vault configuration.
    pub vault: Option<VaultSecretsConfig>,

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

/// HashiCorp Vault secrets backend config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultSecretsConfig {
    /// Vault server address.
    pub address: String,
    /// Auth method: "token", "approle", "kubernetes", "aws-iam".
    #[serde(default = "default_vault_auth")]
    pub auth_method: String,
    /// Vault token (can be `${ENV_VAR}` reference).
    pub token: Option<String>,
    /// AppRole role_id.
    pub role_id: Option<String>,
    /// AppRole secret_id.
    pub secret_id: Option<String>,
    /// Kubernetes auth role.
    pub k8s_role: Option<String>,
    /// Secrets engine mount path (default: "secret").
    #[serde(default = "default_vault_mount")]
    pub mount: String,
    /// Vault Enterprise namespace.
    pub namespace: Option<String>,
    /// TLS CA certificate path.
    pub ca_cert: Option<String>,
    /// Skip TLS verification.
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

/// AWS secrets backend config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsSecretsConfig {
    /// AWS region.
    pub region: Option<String>,
    /// AWS profile name.
    pub profile: Option<String>,
    /// Endpoint URL (for LocalStack / testing).
    pub endpoint_url: Option<String>,
    /// Timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

/// GCP secrets backend config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcpSecretsConfig {
    /// GCP project ID.
    pub project: Option<String>,
    /// Service account credentials file.
    pub credentials_file: Option<String>,
    /// Timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_dags_path() -> String {
    "dags".to_string()
}

/// A named connection to an external system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    #[serde(rename = "type")]
    pub conn_type: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub credentials: Option<String>,

    /// Additional connection-specific parameters.
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// A named resource pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    pub slots: u32,
    pub description: Option<String>,
}

/// Global default settings.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DefaultsConfig {
    /// Default number of retries for tasks.
    #[serde(default)]
    pub retries: u32,

    /// Default retry delay.
    pub retry_delay: Option<String>,

    /// Default task timeout.
    pub timeout: Option<String>,

    /// Default resource pool.
    pub pool: Option<String>,

    /// Maximum active tasks globally.
    #[serde(default = "default_max_active_tasks")]
    pub max_active_tasks: u32,
}

fn default_max_active_tasks() -> u32 {
    256
}

impl ConduitConfig {
    /// Load configuration from a YAML file.
    pub fn load(path: &Path) -> ConduitResult<Self> {
        if !path.exists() {
            return Err(ConduitError::FileNotFound(path.display().to_string()));
        }
        let content = std::fs::read_to_string(path)?;
        // Use serde_json for now; swap to serde_yaml when added as dependency
        serde_json::from_str(&content).map_err(|e| {
            ConduitError::ConfigError(format!("Failed to parse {}: {}", path.display(), e))
        })
    }

    /// Create a default config for `conduit init`.
    pub fn default_for_project(name: &str) -> Self {
        Self {
            name: name.to_string(),
            dags_path: "dags".to_string(),
            connections: HashMap::new(),
            pools: HashMap::new(),
            defaults: DefaultsConfig::default(),
            secrets: SecretsConfig::default(),
        }
    }
}
