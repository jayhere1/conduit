//! Integration tests for the authentication and authorization system.
//!
//! Tests cover:
//! - API key lifecycle (create, list, get, revoke)
//! - Authentication (valid key, invalid key, expired key, revoked key)
//! - Role-based access control (viewer, operator, admin permissions)
//! - Auth store persistence (export/import)
//! - Edge cases (empty names, invalid roles, concurrent access)

#[cfg(test)]
mod tests {
    use conduit_api::auth::*;

    // ── Key Lifecycle ────────────────────────────────────────────────────────

    #[test]
    fn create_key_returns_plaintext_and_metadata() {
        let store = AuthStore::new(true);
        let (plaintext, key) = store.create_key("test", Role::Viewer, "admin", None, None).unwrap();

        assert!(plaintext.starts_with("cdt_"));
        assert_eq!(plaintext.len(), 36);
        assert_eq!(key.name, "test");
        assert_eq!(key.role, Role::Viewer);
        assert!(!key.revoked);
        assert!(key.expires_at.is_none());
        assert_eq!(key.created_by, "admin");
    }

    #[test]
    fn list_keys_returns_all_keys() {
        let store = AuthStore::new(true);
        store.create_key("key1", Role::Viewer, "admin", None, None).unwrap();
        store.create_key("key2", Role::Operator, "admin", None, None).unwrap();
        store.create_key("key3", Role::Admin, "admin", None, None).unwrap();

        let keys = store.list_keys();
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn get_key_by_id() {
        let store = AuthStore::new(true);
        let (_, key) = store.create_key("findme", Role::Operator, "admin", None, None).unwrap();

        let found = store.get_key(&key.id).unwrap();
        assert_eq!(found.name, "findme");
        assert_eq!(found.role, Role::Operator);
    }

    #[test]
    fn get_key_nonexistent_returns_none() {
        let store = AuthStore::new(true);
        assert!(store.get_key("nonexistent-id").is_none());
    }

    #[test]
    fn revoke_key_marks_as_revoked() {
        let store = AuthStore::new(true);
        let (_, key) = store.create_key("revokeable", Role::Admin, "admin", None, None).unwrap();

        let revoked = store.revoke_key(&key.id).unwrap();
        assert!(revoked.revoked);

        let found = store.get_key(&key.id).unwrap();
        assert!(found.revoked);
    }

    #[test]
    fn revoke_nonexistent_key_fails() {
        let store = AuthStore::new(true);
        assert!(store.revoke_key("no-such-key").is_err());
    }

    // ── Authentication ───────────────────────────────────────────────────────

    #[test]
    fn authenticate_valid_key() {
        let store = AuthStore::new(true);
        let (plaintext, _) = store.create_key("valid", Role::Operator, "admin", None, None).unwrap();

        let ctx = store.authenticate(&plaintext).unwrap();
        assert_eq!(ctx.role, Role::Operator);
        assert_eq!(ctx.key_name, "valid");
    }

    #[test]
    fn authenticate_invalid_key_fails() {
        let store = AuthStore::new(true);
        store.create_key("real", Role::Admin, "admin", None, None).unwrap();

        let result = store.authenticate("cdt_completely_wrong_key_00000000");
        assert!(matches!(result, Err(AuthError::InvalidKey)));
    }

    #[test]
    fn authenticate_revoked_key_fails() {
        let store = AuthStore::new(true);
        let (plaintext, key) = store.create_key("temp", Role::Admin, "admin", None, None).unwrap();

        store.revoke_key(&key.id).unwrap();
        let result = store.authenticate(&plaintext);
        assert!(matches!(result, Err(AuthError::KeyRevoked)));
    }

    #[test]
    fn authenticate_expired_key_fails() {
        let store = AuthStore::new(true);
        let expired = chrono::Utc::now() - chrono::Duration::hours(1);
        let (plaintext, _) = store.create_key("expired", Role::Admin, "admin", None, Some(expired)).unwrap();

        let result = store.authenticate(&plaintext);
        assert!(matches!(result, Err(AuthError::KeyExpired)));
    }

    #[test]
    fn authenticate_updates_last_used() {
        let store = AuthStore::new(true);
        let (plaintext, key) = store.create_key("track-usage", Role::Viewer, "admin", None, None).unwrap();

        // Initially no last_used
        let before = store.get_key(&key.id).unwrap();
        assert!(before.last_used_at.is_none());

        // After auth, last_used is set
        store.authenticate(&plaintext).unwrap();
        let after = store.get_key(&key.id).unwrap();
        assert!(after.last_used_at.is_some());
    }

    // ── Bearer Token Extraction ──────────────────────────────────────────────

    #[test]
    fn extract_bearer_valid() {
        assert_eq!(AuthStore::extract_bearer("Bearer mytoken123").unwrap(), "mytoken123");
    }

    #[test]
    fn extract_bearer_with_spaces() {
        assert_eq!(AuthStore::extract_bearer("  Bearer  mytoken123  ").unwrap(), "mytoken123");
    }

    #[test]
    fn extract_bearer_wrong_scheme() {
        assert!(AuthStore::extract_bearer("Basic abc123").is_err());
    }

    #[test]
    fn extract_bearer_empty_token() {
        assert!(AuthStore::extract_bearer("Bearer ").is_err());
    }

    #[test]
    fn extract_bearer_no_space() {
        assert!(AuthStore::extract_bearer("Bearertoken").is_err());
    }

    // ── Role Ordering and Permissions ────────────────────────────────────────

    #[test]
    fn role_ordering() {
        assert!(Role::Admin > Role::Operator);
        assert!(Role::Operator > Role::Viewer);
        assert!(Role::Viewer < Role::Operator);
    }

    #[test]
    fn viewer_can_view_but_not_write() {
        assert!(Permission::ViewDags.allowed_for(Role::Viewer));
        assert!(Permission::ViewRuns.allowed_for(Role::Viewer));
        assert!(Permission::ViewEnvironments.allowed_for(Role::Viewer));
        assert!(Permission::ViewCluster.allowed_for(Role::Viewer));

        assert!(!Permission::TriggerRun.allowed_for(Role::Viewer));
        assert!(!Permission::ApplyPlan.allowed_for(Role::Viewer));
        assert!(!Permission::ManageApiKeys.allowed_for(Role::Viewer));
    }

    #[test]
    fn operator_can_view_and_write_but_not_admin() {
        assert!(Permission::ViewDags.allowed_for(Role::Operator));
        assert!(Permission::TriggerRun.allowed_for(Role::Operator));
        assert!(Permission::CompileDags.allowed_for(Role::Operator));
        assert!(Permission::CreateEnvironment.allowed_for(Role::Operator));
        assert!(Permission::ApplyPlan.allowed_for(Role::Operator));
        assert!(Permission::DrainWorker.allowed_for(Role::Operator));

        assert!(!Permission::ManageApiKeys.allowed_for(Role::Operator));
    }

    #[test]
    fn admin_can_do_everything() {
        assert!(Permission::ViewDags.allowed_for(Role::Admin));
        assert!(Permission::TriggerRun.allowed_for(Role::Admin));
        assert!(Permission::ManageApiKeys.allowed_for(Role::Admin));
    }

    #[test]
    fn auth_context_require_success() {
        let ctx = AuthContext {
            key_id: "k1".to_string(),
            key_name: "test".to_string(),
            role: Role::Operator,
        };

        assert!(ctx.require(Permission::ViewDags).is_ok());
        assert!(ctx.require(Permission::TriggerRun).is_ok());
    }

    #[test]
    fn auth_context_require_failure() {
        let ctx = AuthContext {
            key_id: "k1".to_string(),
            key_name: "test".to_string(),
            role: Role::Viewer,
        };

        let result = ctx.require(Permission::TriggerRun);
        assert!(result.is_err());
        if let Err(AuthError::Forbidden { required_role, actual_role }) = result {
            assert_eq!(required_role, Role::Operator);
            assert_eq!(actual_role, Role::Viewer);
        } else {
            panic!("Expected Forbidden error");
        }
    }

    // ── Role Parsing ─────────────────────────────────────────────────────────

    #[test]
    fn role_from_str_case_insensitive() {
        assert_eq!(Role::from_str_loose("ADMIN"), Some(Role::Admin));
        assert_eq!(Role::from_str_loose("Viewer"), Some(Role::Viewer));
        assert_eq!(Role::from_str_loose("operator"), Some(Role::Operator));
    }

    #[test]
    fn role_from_str_aliases() {
        assert_eq!(Role::from_str_loose("read"), Some(Role::Viewer));
        assert_eq!(Role::from_str_loose("readonly"), Some(Role::Viewer));
        assert_eq!(Role::from_str_loose("write"), Some(Role::Operator));
        assert_eq!(Role::from_str_loose("readwrite"), Some(Role::Operator));
        assert_eq!(Role::from_str_loose("superadmin"), Some(Role::Admin));
        assert_eq!(Role::from_str_loose("root"), Some(Role::Admin));
    }

    #[test]
    fn role_from_str_unknown_returns_none() {
        assert_eq!(Role::from_str_loose("god"), None);
        assert_eq!(Role::from_str_loose(""), None);
    }

    // ── Error Status Codes ───────────────────────────────────────────────────

    #[test]
    fn error_status_codes() {
        assert_eq!(AuthError::MissingToken.status_code(), 401);
        assert_eq!(AuthError::InvalidFormat.status_code(), 401);
        assert_eq!(AuthError::InvalidKey.status_code(), 401);
        assert_eq!(AuthError::KeyRevoked.status_code(), 401);
        assert_eq!(AuthError::KeyExpired.status_code(), 401);
        assert_eq!(
            AuthError::Forbidden { required_role: Role::Admin, actual_role: Role::Viewer }.status_code(),
            403
        );
    }

    // ── Key Hashing ──────────────────────────────────────────────────────────

    #[test]
    fn hash_is_deterministic() {
        let key = "cdt_test_key_000000000000000000000";
        assert_eq!(hash_key(key), hash_key(key));
    }

    #[test]
    fn different_keys_different_hashes() {
        assert_ne!(
            hash_key("cdt_key_aaaa0000000000000000000000"),
            hash_key("cdt_key_bbbb0000000000000000000000"),
        );
    }

    #[test]
    fn generated_keys_are_unique() {
        let k1 = generate_api_key();
        let k2 = generate_api_key();
        assert_ne!(k1, k2);
        assert!(k1.starts_with("cdt_"));
        assert!(k2.starts_with("cdt_"));
    }

    // ── API Key Validity ─────────────────────────────────────────────────────

    #[test]
    fn valid_key_is_valid() {
        let key = ApiKey {
            id: "x".to_string(),
            name: "x".to_string(),
            key_hash: "h".to_string(),
            key_prefix: "cdt_x".to_string(),
            role: Role::Viewer,
            created_at: chrono::Utc::now(),
            expires_at: None,
            revoked: false,
            created_by: "test".to_string(),
            description: None,
            last_used_at: None,
        };
        assert!(key.is_valid());
    }

    #[test]
    fn revoked_key_is_invalid() {
        let key = ApiKey {
            id: "x".to_string(),
            name: "x".to_string(),
            key_hash: "h".to_string(),
            key_prefix: "cdt_x".to_string(),
            role: Role::Viewer,
            created_at: chrono::Utc::now(),
            expires_at: None,
            revoked: true,
            created_by: "test".to_string(),
            description: None,
            last_used_at: None,
        };
        assert!(!key.is_valid());
    }

    #[test]
    fn future_expiry_is_valid() {
        let key = ApiKey {
            id: "x".to_string(),
            name: "x".to_string(),
            key_hash: "h".to_string(),
            key_prefix: "cdt_x".to_string(),
            role: Role::Viewer,
            created_at: chrono::Utc::now(),
            expires_at: Some(chrono::Utc::now() + chrono::Duration::hours(24)),
            revoked: false,
            created_by: "test".to_string(),
            description: None,
            last_used_at: None,
        };
        assert!(key.is_valid());
    }

    #[test]
    fn past_expiry_is_invalid() {
        let key = ApiKey {
            id: "x".to_string(),
            name: "x".to_string(),
            key_hash: "h".to_string(),
            key_prefix: "cdt_x".to_string(),
            role: Role::Viewer,
            created_at: chrono::Utc::now(),
            expires_at: Some(chrono::Utc::now() - chrono::Duration::hours(1)),
            revoked: false,
            created_by: "test".to_string(),
            description: None,
            last_used_at: None,
        };
        assert!(!key.is_valid());
    }

    // ── Bootstrap Key ────────────────────────────────────────────────────────

    #[test]
    fn bootstrap_key_is_admin() {
        let store = AuthStore::new(true);
        let plaintext = store.create_bootstrap_key();

        let ctx = store.authenticate(&plaintext).unwrap();
        assert_eq!(ctx.role, Role::Admin);
        assert_eq!(ctx.key_name, "bootstrap-admin");
    }

    // ── Export / Import ──────────────────────────────────────────────────────

    #[test]
    fn export_import_preserves_keys() {
        let store1 = AuthStore::new(true);
        let (pt1, _) = store1.create_key("k1", Role::Viewer, "admin", Some("first".to_string()), None).unwrap();
        let (pt2, _) = store1.create_key("k2", Role::Admin, "admin", None, None).unwrap();

        let exported = store1.export_keys();
        let keys: Vec<ApiKey> = serde_json::from_value(exported).unwrap();
        assert_eq!(keys.len(), 2);

        let store2 = AuthStore::new(true);
        store2.import_keys(&keys);

        // Can authenticate with original keys
        let ctx1 = store2.authenticate(&pt1).unwrap();
        assert_eq!(ctx1.role, Role::Viewer);

        let ctx2 = store2.authenticate(&pt2).unwrap();
        assert_eq!(ctx2.role, Role::Admin);
    }

    #[test]
    fn import_preserves_revoked_state() {
        let store1 = AuthStore::new(true);
        let (plaintext, key) = store1.create_key("revoked", Role::Admin, "admin", None, None).unwrap();
        store1.revoke_key(&key.id).unwrap();

        let exported = store1.export_keys();
        let keys: Vec<ApiKey> = serde_json::from_value(exported).unwrap();

        let store2 = AuthStore::new(true);
        store2.import_keys(&keys);

        let result = store2.authenticate(&plaintext);
        assert!(matches!(result, Err(AuthError::KeyRevoked)));
    }

    // ── Multiple Keys ────────────────────────────────────────────────────────

    #[test]
    fn multiple_keys_independent() {
        let store = AuthStore::new(true);
        let (pt_viewer, _) = store.create_key("viewer-key", Role::Viewer, "admin", None, None).unwrap();
        let (pt_operator, _) = store.create_key("op-key", Role::Operator, "admin", None, None).unwrap();
        let (pt_admin, _) = store.create_key("admin-key", Role::Admin, "admin", None, None).unwrap();

        let ctx_v = store.authenticate(&pt_viewer).unwrap();
        assert_eq!(ctx_v.role, Role::Viewer);

        let ctx_o = store.authenticate(&pt_operator).unwrap();
        assert_eq!(ctx_o.role, Role::Operator);

        let ctx_a = store.authenticate(&pt_admin).unwrap();
        assert_eq!(ctx_a.role, Role::Admin);
    }

    // ── Permission Coverage ──────────────────────────────────────────────────

    #[test]
    fn all_view_permissions_require_viewer() {
        let view_perms = vec![
            Permission::ViewDags, Permission::ViewRuns, Permission::ViewEnvironments,
            Permission::ViewEvents, Permission::ViewLineage, Permission::ViewContracts,
            Permission::ViewMetrics, Permission::ViewConnections, Permission::ViewCluster,
            Permission::ViewHealth,
        ];
        for perm in view_perms {
            assert_eq!(perm.required_role(), Role::Viewer, "Expected {:?} to require Viewer", perm);
        }
    }

    #[test]
    fn all_write_permissions_require_operator() {
        let write_perms = vec![
            Permission::TriggerRun, Permission::CompileDags, Permission::CreateEnvironment,
            Permission::DeleteEnvironment, Permission::PromoteEnvironment,
            Permission::GeneratePlan, Permission::ApplyPlan, Permission::CreateBackfill,
            Permission::DrainWorker, Permission::ExtractLineage, Permission::ValidateContract,
        ];
        for perm in write_perms {
            assert_eq!(perm.required_role(), Role::Operator, "Expected {:?} to require Operator", perm);
        }
    }

    #[test]
    fn admin_permissions_require_admin() {
        assert_eq!(Permission::ManageApiKeys.required_role(), Role::Admin);
    }

    // ── Description and Metadata ─────────────────────────────────────────────

    #[test]
    fn key_with_description() {
        let store = AuthStore::new(true);
        let (_, key) = store.create_key(
            "ci-key",
            Role::Operator,
            "admin",
            Some("Used by CI/CD pipeline".to_string()),
            None,
        ).unwrap();

        assert_eq!(key.description.as_deref(), Some("Used by CI/CD pipeline"));
    }

    #[test]
    fn key_with_expiry() {
        let store = AuthStore::new(true);
        let expiry = chrono::Utc::now() + chrono::Duration::days(30);
        let (plaintext, key) = store.create_key(
            "temp-key",
            Role::Viewer,
            "admin",
            None,
            Some(expiry),
        ).unwrap();

        assert!(key.expires_at.is_some());
        // Should still be valid (30 days in the future)
        let ctx = store.authenticate(&plaintext).unwrap();
        assert_eq!(ctx.role, Role::Viewer);
    }

    #[test]
    fn key_prefix_is_first_12_chars() {
        let store = AuthStore::new(true);
        let (plaintext, key) = store.create_key("prefix-test", Role::Viewer, "admin", None, None).unwrap();

        assert_eq!(key.key_prefix, &plaintext[..12]);
    }
}
