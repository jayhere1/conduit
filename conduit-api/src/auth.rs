//! Authentication and authorization for the Conduit API.
//!
//! Provides API key authentication with role-based access control (RBAC).
//!
//! ## Roles
//!
//! - **Admin**: Full access to all endpoints, plus key management.
//! - **Operator**: Read/write access — can trigger runs, manage environments,
//!   plan/apply deployments, manage connections, and drain workers.
//! - **Viewer**: Read-only access to all data endpoints.
//!
//! ## Authentication
//!
//! API keys are passed via the `Authorization: Bearer <key>` header.
//! Keys are stored as SHA-256 hashes; the plaintext is only returned once
//! at creation time.
//!
//! ## Bypassing Auth
//!
//! When the server is started without `--auth-enabled`, all endpoints are
//! accessible without authentication (backward compatible).

use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ── Roles ────────────────────────────────────────────────────────────────────

/// Access roles ordered by increasing privilege.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Viewer,
    Operator,
    Admin,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::Viewer => write!(f, "viewer"),
            Role::Operator => write!(f, "operator"),
            Role::Admin => write!(f, "admin"),
        }
    }
}

impl Role {
    /// Parse a role from a string, case-insensitive.
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "viewer" | "read" | "readonly" => Some(Role::Viewer),
            "operator" | "write" | "readwrite" => Some(Role::Operator),
            "admin" | "superadmin" | "root" => Some(Role::Admin),
            _ => None,
        }
    }
}

// ── Permissions ──────────────────────────────────────────────────────────────

/// Granular permission that can be checked against a role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    // Read operations
    ViewDags,
    ViewRuns,
    ViewEnvironments,
    ViewEvents,
    ViewLineage,
    ViewContracts,
    ViewMetrics,
    ViewConnections,
    ViewCluster,
    ViewHealth,

    // Write operations
    TriggerRun,
    CompileDags,
    CreateEnvironment,
    DeleteEnvironment,
    PromoteEnvironment,
    GeneratePlan,
    ApplyPlan,
    CreateBackfill,
    DrainWorker,
    ExtractLineage,
    ValidateContract,

    // Admin operations
    ManageApiKeys,
}

impl Permission {
    /// The minimum role required for this permission.
    pub fn required_role(&self) -> Role {
        use Permission::*;
        match self {
            // Anyone can view
            ViewDags | ViewRuns | ViewEnvironments | ViewEvents | ViewLineage | ViewContracts
            | ViewMetrics | ViewConnections | ViewCluster | ViewHealth => Role::Viewer,

            // Operators can write
            TriggerRun | CompileDags | CreateEnvironment | DeleteEnvironment
            | PromoteEnvironment | GeneratePlan | ApplyPlan | CreateBackfill | DrainWorker
            | ExtractLineage | ValidateContract => Role::Operator,

            // Admin-only
            ManageApiKeys => Role::Admin,
        }
    }

    /// Check if a role has this permission.
    pub fn allowed_for(&self, role: Role) -> bool {
        role >= self.required_role()
    }
}

// ── API Key ──────────────────────────────────────────────────────────────────

/// A stored API key record (the hash, never the plaintext).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKey {
    /// Unique key identifier (UUID).
    pub id: String,
    /// Human-readable name for the key.
    pub name: String,
    /// SHA-256 hash of the key (hex-encoded).
    pub key_hash: String,
    /// Random salt used when hashing this key.
    pub salt: String,
    /// First 8 characters of the key for identification.
    pub key_prefix: String,
    /// Role assigned to this key.
    pub role: Role,
    /// When the key was created.
    pub created_at: DateTime<Utc>,
    /// Optional expiry date.
    pub expires_at: Option<DateTime<Utc>>,
    /// Whether this key has been revoked.
    pub revoked: bool,
    /// Who created this key.
    pub created_by: String,
    /// Optional description.
    pub description: Option<String>,
    /// Last time this key was used.
    pub last_used_at: Option<DateTime<Utc>>,
}

impl ApiKey {
    /// Check if the key is currently valid (not revoked, not expired).
    pub fn is_valid(&self) -> bool {
        if self.revoked {
            return false;
        }
        if let Some(expires) = self.expires_at {
            if Utc::now() > expires {
                return false;
            }
        }
        true
    }
}

/// The result of successfully authenticating a request.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// The API key ID that was used.
    pub key_id: String,
    /// The name of the key.
    pub key_name: String,
    /// The role granted by this key.
    pub role: Role,
}

impl AuthContext {
    /// Check if this context has the given permission.
    pub fn has_permission(&self, perm: Permission) -> bool {
        perm.allowed_for(self.role)
    }

    /// Require a permission, returning an error if denied.
    pub fn require(&self, perm: Permission) -> Result<(), AuthError> {
        if self.has_permission(perm) {
            Ok(())
        } else {
            Err(AuthError::Forbidden {
                required_role: perm.required_role(),
                actual_role: self.role,
            })
        }
    }
}

// ── Auth Errors ──────────────────────────────────────────────────────────────

/// Authentication/authorization errors.
#[derive(Debug, Clone)]
pub enum AuthError {
    /// No Authorization header provided.
    MissingToken,
    /// The token format is invalid (not "Bearer <key>").
    InvalidFormat,
    /// The key does not match any stored key.
    InvalidKey,
    /// The key has been revoked.
    KeyRevoked,
    /// The key has expired.
    KeyExpired,
    /// The key's role does not have the required permission.
    Forbidden {
        required_role: Role,
        actual_role: Role,
    },
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::MissingToken => write!(f, "Missing authorization token"),
            AuthError::InvalidFormat => {
                write!(f, "Invalid authorization format. Use: Bearer <api-key>")
            }
            AuthError::InvalidKey => write!(f, "Invalid API key"),
            AuthError::KeyRevoked => write!(f, "API key has been revoked"),
            AuthError::KeyExpired => write!(f, "API key has expired"),
            AuthError::Forbidden {
                required_role,
                actual_role,
            } => {
                write!(
                    f,
                    "Insufficient permissions: requires {} role, you have {}",
                    required_role, actual_role
                )
            }
        }
    }
}

impl AuthError {
    /// HTTP status code for this error.
    pub fn status_code(&self) -> u16 {
        match self {
            AuthError::MissingToken
            | AuthError::InvalidFormat
            | AuthError::InvalidKey
            | AuthError::KeyRevoked
            | AuthError::KeyExpired => 401,
            AuthError::Forbidden { .. } => 403,
        }
    }
}

// ── Key Store ────────────────────────────────────────────────────────────────

/// Generate a cryptographically random API key.
pub fn generate_api_key() -> String {
    use uuid::Uuid;
    // Format: cdt_<32 hex chars> — easy to identify as a Conduit key
    let raw = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    format!("cdt_{}", &raw[..32])
}

/// Hash an API key for storage, using a salt to prevent rainbow-table attacks.
pub fn hash_key(salt: &str, key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(salt.as_bytes());
    hasher.update(key.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Thread-safe store for API keys.
pub struct AuthStore {
    /// Hash → ApiKey mapping (the hash is the lookup key for O(1) auth).
    keys_by_hash: RwLock<HashMap<String, ApiKey>>,
    /// ID → hash mapping (for management operations: revoke, list).
    id_to_hash: RwLock<HashMap<String, String>>,
    /// Whether authentication is enforced.
    pub auth_enabled: bool,
}

impl AuthStore {
    /// Create a new auth store.
    pub fn new(auth_enabled: bool) -> Self {
        Self {
            keys_by_hash: RwLock::new(HashMap::new()),
            id_to_hash: RwLock::new(HashMap::new()),
            auth_enabled,
        }
    }

    /// Create a new API key. Returns the plaintext key (only shown once).
    pub fn create_key(
        &self,
        name: &str,
        role: Role,
        created_by: &str,
        description: Option<String>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<(String, ApiKey), AuthError> {
        let plaintext = generate_api_key();
        let salt = uuid::Uuid::new_v4().to_string();
        let key_hash = hash_key(&salt, &plaintext);
        let key_prefix = plaintext[..12].to_string(); // "cdt_" + first 8 hex

        let key = ApiKey {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.to_string(),
            key_hash: key_hash.clone(),
            salt,
            key_prefix,
            role,
            created_at: Utc::now(),
            expires_at,
            revoked: false,
            created_by: created_by.to_string(),
            description,
            last_used_at: None,
        };

        // Acquire BOTH write locks before making any mutations to prevent
        // inconsistent state if a panic occurs between insertions.
        let mut keys = self
            .keys_by_hash
            .write()
            .map_err(|_| AuthError::InvalidKey)?;
        let mut ids = self
            .id_to_hash
            .write()
            .map_err(|_| AuthError::InvalidKey)?;

        ids.insert(key.id.clone(), key_hash.clone());
        keys.insert(key_hash, key.clone());
        Ok((plaintext, key))
    }

    /// Authenticate a request using a plaintext API key.
    ///
    /// Because each key has its own unique salt, we must iterate over all stored
    /// keys, re-hash the plaintext with each key's salt, and compare.
    pub fn authenticate(&self, plaintext_key: &str) -> Result<AuthContext, AuthError> {
        let mut keys = self
            .keys_by_hash
            .write()
            .map_err(|_| AuthError::InvalidKey)?;

        // Find the matching key by recomputing the salted hash for each stored key.
        let matched_hash = keys.iter().find_map(|(hash, stored_key)| {
            let candidate = hash_key(&stored_key.salt, plaintext_key);
            if candidate == *hash {
                Some(hash.clone())
            } else {
                None
            }
        });

        let hash = matched_hash.ok_or(AuthError::InvalidKey)?;
        let key = keys.get_mut(&hash).ok_or(AuthError::InvalidKey)?;

        if key.revoked {
            return Err(AuthError::KeyRevoked);
        }
        if let Some(expires) = key.expires_at {
            if Utc::now() > expires {
                return Err(AuthError::KeyExpired);
            }
        }

        // Update last_used_at
        key.last_used_at = Some(Utc::now());

        Ok(AuthContext {
            key_id: key.id.clone(),
            key_name: key.name.clone(),
            role: key.role,
        })
    }

    /// Extract the Bearer token from an Authorization header value.
    pub fn extract_bearer(header_value: &str) -> Result<&str, AuthError> {
        let trimmed = header_value.trim();
        if let Some(token) = trimmed.strip_prefix("Bearer ") {
            let token = token.trim();
            if token.is_empty() {
                return Err(AuthError::InvalidFormat);
            }
            Ok(token)
        } else {
            Err(AuthError::InvalidFormat)
        }
    }

    /// List all API keys (without exposing hashes in the response).
    pub fn list_keys(&self) -> Vec<ApiKey> {
        self.keys_by_hash
            .read()
            .map(|keys| {
                let mut list: Vec<ApiKey> = keys.values().cloned().collect();
                list.sort_by(|a, b| b.created_at.cmp(&a.created_at));
                list
            })
            .unwrap_or_default()
    }

    /// Get a specific key by ID.
    pub fn get_key(&self, key_id: &str) -> Option<ApiKey> {
        let ids = self.id_to_hash.read().ok()?;
        let hash = ids.get(key_id)?;
        let keys = self.keys_by_hash.read().ok()?;
        keys.get(hash).cloned()
    }

    /// Revoke a key by ID.
    pub fn revoke_key(&self, key_id: &str) -> Result<ApiKey, AuthError> {
        let ids = self.id_to_hash.read().map_err(|_| AuthError::InvalidKey)?;
        let hash = ids.get(key_id).ok_or(AuthError::InvalidKey)?.clone();
        drop(ids);

        let mut keys = self
            .keys_by_hash
            .write()
            .map_err(|_| AuthError::InvalidKey)?;
        let key = keys.get_mut(&hash).ok_or(AuthError::InvalidKey)?;
        key.revoked = true;
        Ok(key.clone())
    }

    /// Create a bootstrap admin key (for initial setup).
    /// Returns the plaintext key.
    pub fn create_bootstrap_key(&self) -> String {
        let (plaintext, _key) = self
            .create_key(
                "bootstrap-admin",
                Role::Admin,
                "system",
                Some("Initial admin key created at startup".to_string()),
                None,
            )
            .expect("Failed to create bootstrap key");
        plaintext
    }

    /// Serialize all keys to JSON for persistence.
    pub fn export_keys(&self) -> serde_json::Value {
        let keys = self.list_keys();
        serde_json::to_value(&keys).unwrap_or(serde_json::Value::Array(vec![]))
    }

    /// Import keys from JSON (for loading from disk).
    pub fn import_keys(&self, keys: &[ApiKey]) {
        if let (Ok(mut key_map), Ok(mut id_map)) =
            (self.keys_by_hash.write(), self.id_to_hash.write())
        {
            for key in keys {
                id_map.insert(key.id.clone(), key.key_hash.clone());
                key_map.insert(key.key_hash.clone(), key.clone());
            }
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_ordering() {
        assert!(Role::Admin > Role::Operator);
        assert!(Role::Operator > Role::Viewer);
        assert!(Role::Viewer < Role::Admin);
    }

    #[test]
    fn test_role_from_str() {
        assert_eq!(Role::from_str_loose("viewer"), Some(Role::Viewer));
        assert_eq!(Role::from_str_loose("ADMIN"), Some(Role::Admin));
        assert_eq!(Role::from_str_loose("write"), Some(Role::Operator));
        assert_eq!(Role::from_str_loose("readonly"), Some(Role::Viewer));
        assert_eq!(Role::from_str_loose("nonsense"), None);
    }

    #[test]
    fn test_permission_roles() {
        assert!(Permission::ViewDags.allowed_for(Role::Viewer));
        assert!(Permission::ViewDags.allowed_for(Role::Operator));
        assert!(Permission::ViewDags.allowed_for(Role::Admin));

        assert!(!Permission::TriggerRun.allowed_for(Role::Viewer));
        assert!(Permission::TriggerRun.allowed_for(Role::Operator));
        assert!(Permission::TriggerRun.allowed_for(Role::Admin));

        assert!(!Permission::ManageApiKeys.allowed_for(Role::Viewer));
        assert!(!Permission::ManageApiKeys.allowed_for(Role::Operator));
        assert!(Permission::ManageApiKeys.allowed_for(Role::Admin));
    }

    #[test]
    fn test_key_generation() {
        let key = generate_api_key();
        assert!(key.starts_with("cdt_"));
        assert_eq!(key.len(), 36); // "cdt_" + 32 hex chars
    }

    #[test]
    fn test_key_hashing() {
        let salt = "test-salt";
        let key = "cdt_abc123def456abc123def456abc123";
        let hash1 = hash_key(salt, key);
        let hash2 = hash_key(salt, key);
        assert_eq!(hash1, hash2);
        // Different key produces different hash
        assert_ne!(hash1, hash_key(salt, "cdt_different_key_0000000000000000"));
        // Different salt produces different hash
        assert_ne!(hash1, hash_key("other-salt", key));
    }

    #[test]
    fn test_create_and_authenticate() {
        let store = AuthStore::new(true);
        let (plaintext, key) = store
            .create_key("test-key", Role::Operator, "test", None, None)
            .unwrap();

        assert!(plaintext.starts_with("cdt_"));
        assert_eq!(key.role, Role::Operator);
        assert!(!key.revoked);

        let ctx = store.authenticate(&plaintext).unwrap();
        assert_eq!(ctx.role, Role::Operator);
        assert_eq!(ctx.key_name, "test-key");
    }

    #[test]
    fn test_authenticate_invalid_key() {
        let store = AuthStore::new(true);
        let result = store.authenticate("cdt_nonexistent_key_000000000000");
        assert!(matches!(result, Err(AuthError::InvalidKey)));
    }

    #[test]
    fn test_revoke_key() {
        let store = AuthStore::new(true);
        let (plaintext, key) = store
            .create_key("revoke-me", Role::Admin, "test", None, None)
            .unwrap();

        // Revoke it
        let revoked = store.revoke_key(&key.id).unwrap();
        assert!(revoked.revoked);

        // Can't authenticate anymore
        let result = store.authenticate(&plaintext);
        assert!(matches!(result, Err(AuthError::KeyRevoked)));
    }

    #[test]
    fn test_expired_key() {
        let store = AuthStore::new(true);
        let expired = Utc::now() - chrono::Duration::hours(1);
        let (plaintext, _key) = store
            .create_key("expired-key", Role::Viewer, "test", None, Some(expired))
            .unwrap();

        let result = store.authenticate(&plaintext);
        assert!(matches!(result, Err(AuthError::KeyExpired)));
    }

    #[test]
    fn test_list_keys() {
        let store = AuthStore::new(true);
        store
            .create_key("key-1", Role::Viewer, "test", None, None)
            .unwrap();
        store
            .create_key("key-2", Role::Admin, "test", None, None)
            .unwrap();

        let keys = store.list_keys();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_extract_bearer() {
        assert_eq!(
            AuthStore::extract_bearer("Bearer abc123").unwrap(),
            "abc123"
        );
        assert_eq!(
            AuthStore::extract_bearer("Bearer  abc123 ").unwrap(),
            "abc123"
        );
        assert!(AuthStore::extract_bearer("Basic abc123").is_err());
        assert!(AuthStore::extract_bearer("Bearer ").is_err());
        assert!(AuthStore::extract_bearer("Bearer").is_err());
    }

    #[test]
    fn test_auth_context_permissions() {
        let ctx = AuthContext {
            key_id: "test".to_string(),
            key_name: "test-key".to_string(),
            role: Role::Operator,
        };

        assert!(ctx.has_permission(Permission::ViewDags));
        assert!(ctx.has_permission(Permission::TriggerRun));
        assert!(!ctx.has_permission(Permission::ManageApiKeys));
        assert!(ctx.require(Permission::ViewDags).is_ok());
        assert!(ctx.require(Permission::ManageApiKeys).is_err());
    }

    #[test]
    fn test_bootstrap_key() {
        let store = AuthStore::new(true);
        let plaintext = store.create_bootstrap_key();
        assert!(plaintext.starts_with("cdt_"));

        let ctx = store.authenticate(&plaintext).unwrap();
        assert_eq!(ctx.role, Role::Admin);
        assert_eq!(ctx.key_name, "bootstrap-admin");
    }

    #[test]
    fn test_export_import_roundtrip() {
        let store1 = AuthStore::new(true);
        store1
            .create_key(
                "key-a",
                Role::Viewer,
                "test",
                Some("first key".to_string()),
                None,
            )
            .unwrap();
        store1
            .create_key("key-b", Role::Admin, "test", None, None)
            .unwrap();

        let exported = store1.export_keys();
        let keys: Vec<ApiKey> = serde_json::from_value(exported).unwrap();

        let store2 = AuthStore::new(true);
        store2.import_keys(&keys);

        let imported = store2.list_keys();
        assert_eq!(imported.len(), 2);
    }

    #[test]
    fn test_api_key_validity() {
        let mut key = ApiKey {
            id: "test".to_string(),
            name: "test".to_string(),
            key_hash: "hash".to_string(),
            salt: "test-salt".to_string(),
            key_prefix: "cdt_test".to_string(),
            role: Role::Viewer,
            created_at: Utc::now(),
            expires_at: None,
            revoked: false,
            created_by: "test".to_string(),
            description: None,
            last_used_at: None,
        };
        assert!(key.is_valid());

        key.revoked = true;
        assert!(!key.is_valid());

        key.revoked = false;
        key.expires_at = Some(Utc::now() - chrono::Duration::hours(1));
        assert!(!key.is_valid());

        key.expires_at = Some(Utc::now() + chrono::Duration::hours(1));
        assert!(key.is_valid());
    }

    #[test]
    fn test_error_status_codes() {
        assert_eq!(AuthError::MissingToken.status_code(), 401);
        assert_eq!(AuthError::InvalidKey.status_code(), 401);
        assert_eq!(AuthError::KeyRevoked.status_code(), 401);
        assert_eq!(AuthError::KeyExpired.status_code(), 401);
        assert_eq!(
            AuthError::Forbidden {
                required_role: Role::Admin,
                actual_role: Role::Viewer,
            }
            .status_code(),
            403
        );
    }
}
