//! Built-in provider implementations.
//!
//! Each provider module implements the relevant trait ([`SqlProvider`],
//! [`StorageProvider`], etc.) and can be instantiated from a
//! [`ConnectionConfig`].

// ── SQL Providers ─────────────────────────────────────────────────────────
pub mod postgres;
pub mod snowflake;
pub mod clickhouse;
pub mod redshift;
pub mod bigquery;
pub mod duckdb;
pub mod mysql;
pub mod sqlite;
pub mod oracle;
pub mod sqlserver;
pub mod cockroachdb;
pub mod timescaledb;

// ── Storage Providers ────────────────────────────────────────────────────
pub mod s3;
pub mod gcs;

// ── HTTP Providers ───────────────────────────────────────────────────────
pub mod http;

// ── Stream Providers ─────────────────────────────────────────────────────
pub mod kafka;
pub mod rabbitmq;
pub mod kinesis;
pub mod pubsub;
pub mod redis_stream;

// ── SaaS Providers ───────────────────────────────────────────────────────
pub mod salesforce;
pub mod hubspot;
pub mod stripe;
pub mod github;
pub mod jira;
pub mod slack;

// ── Document / NoSQL Providers ───────────────────────────────────────────
pub mod mongodb;
pub mod dynamodb;
pub mod cassandra;
pub mod elasticsearch;
pub mod redis_doc;
pub mod neo4j;

/// Helper: resolve a credential string (synchronous fallback).
///
/// This is the legacy synchronous resolver for backward compatibility.
/// Supports: literal values, `${ENV_VAR}`, and `file:///path` references.
///
/// For full secrets chain support (Vault, AWS SSM, GCP Secret Manager),
/// use [`resolve_credential_async`] with a [`SecretsChain`] instead.
pub fn resolve_credential(cred: &str) -> Result<String, crate::errors::ProviderError> {
    if cred.starts_with("${") && cred.ends_with('}') {
        // Environment variable reference
        let var_name = &cred[2..cred.len() - 1];
        std::env::var(var_name).map_err(|_| crate::errors::ProviderError::AuthenticationFailed {
            connection: String::new(),
            reason: format!("Environment variable '{}' not set", var_name),
        })
    } else if cred.starts_with("file://") {
        // File reference
        let path = &cred[7..];
        std::fs::read_to_string(path).map_err(|e| crate::errors::ProviderError::AuthenticationFailed {
            connection: String::new(),
            reason: format!("Failed to read credentials file '{}': {}", path, e),
        })
    } else {
        // Literal value
        Ok(cred.to_string())
    }
}

/// Resolve a credential string through the full secrets chain (async).
///
/// Supports all backend types: env vars, files, Vault, AWS SSM,
/// AWS Secrets Manager, GCP Secret Manager, and literal fallback.
pub async fn resolve_credential_async(
    cred: &str,
    connection_name: &str,
    chain: &crate::secrets::SecretsChain,
) -> Result<String, crate::errors::ProviderError> {
    chain.resolve_for_connection(cred, connection_name).await
}

/// Helper: extract a string from the extra config map.
pub fn extra_str(config: &conduit_common::config::ConnectionConfig, key: &str) -> Option<String> {
    config.extra.get(key).and_then(|v| v.as_str().map(String::from))
}

/// Helper: extract a u64 from the extra config map.
pub fn extra_u64(config: &conduit_common::config::ConnectionConfig, key: &str) -> Option<u64> {
    config.extra.get(key).and_then(|v| v.as_u64())
}

/// Percent-encode user credentials (user, password) for connection URLs.
/// Uses USERINFO encoding which encodes @, :, /, ?, #, etc. but preserves
/// unreserved characters (alphanumeric, -, ., _, ~).
pub fn url_encode_credential(s: &str) -> String {
    /// Encodes everything except RFC 3986 unreserved characters and sub-delims
    /// that are safe in the userinfo component. This encodes @, :, /, ?, #, &
    /// which would break URL parsing, while preserving -, ., _, ~ which are safe.
    const USERINFO: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
        .add(b' ')
        .add(b'"')
        .add(b'#')
        .add(b'%')
        .add(b'/')
        .add(b':')
        .add(b'?')
        .add(b'@')
        .add(b'[')
        .add(b']')
        .add(b'&')
        .add(b'=')
        .add(b'+');
    percent_encoding::utf8_percent_encode(s, USERINFO).to_string()
}

/// Helper: extract a bool from the extra config map.
pub fn extra_bool(config: &conduit_common::config::ConnectionConfig, key: &str) -> Option<bool> {
    config.extra.get(key).and_then(|v| v.as_bool())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use conduit_common::config::ConnectionConfig;

    fn make_config() -> ConnectionConfig {
        let mut extra = HashMap::new();
        extra.insert("user".to_string(), serde_json::json!("testuser"));
        extra.insert("port_num".to_string(), serde_json::json!(9999));
        extra.insert("ssl".to_string(), serde_json::json!(true));

        ConnectionConfig {
            conn_type: "test".to_string(),
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: Some("testdb".to_string()),
            credentials: None,
            extra,
        }
    }

    #[test]
    fn test_extra_str() {
        let config = make_config();
        assert_eq!(extra_str(&config, "user"), Some("testuser".to_string()));
        assert_eq!(extra_str(&config, "nonexistent"), None);
    }

    #[test]
    fn test_extra_u64() {
        let config = make_config();
        assert_eq!(extra_u64(&config, "port_num"), Some(9999));
        assert_eq!(extra_u64(&config, "nonexistent"), None);
        // String value should return None for u64
        assert_eq!(extra_u64(&config, "user"), None);
    }

    #[test]
    fn test_extra_bool() {
        let config = make_config();
        assert_eq!(extra_bool(&config, "ssl"), Some(true));
        assert_eq!(extra_bool(&config, "nonexistent"), None);
    }

    #[test]
    fn test_resolve_credential_literal() {
        let result = resolve_credential("my-secret-password");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "my-secret-password");
    }

    #[test]
    fn test_resolve_credential_env_var() {
        std::env::set_var("CONDUIT_TEST_CRED_123", "secret-from-env");
        let result = resolve_credential("${CONDUIT_TEST_CRED_123}");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "secret-from-env");
        std::env::remove_var("CONDUIT_TEST_CRED_123");
    }

    #[test]
    fn test_resolve_credential_env_var_missing() {
        let result = resolve_credential("${CONDUIT_NONEXISTENT_VAR_XYZ}");
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_credential_file_missing() {
        let result = resolve_credential("file:///nonexistent/path/to/cred");
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_credential_file() {
        use std::io::Write;
        let dir = std::env::temp_dir().join("conduit_test_cred");
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("test_secret.txt");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"file-secret").unwrap();

        let result = resolve_credential(&format!("file://{}", path.display()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "file-secret");

        std::fs::remove_file(&path).ok();
    }
}
