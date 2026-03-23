//! Integration tests for the Conduit REST API.
//!
//! These tests spin up a real Axum server (in-process, using `tower::ServiceExt`)
//! and make actual HTTP requests through the full middleware stack.
//!
//! Coverage:
//! - Health/info endpoints (public)
//! - Authentication flow (create key, authenticate, RBAC)
//! - DAG/run/environment CRUD
//! - Lineage, contracts, metrics endpoints
//! - Auth middleware (RequireAuth, OptionalAuth)

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt; // for `oneshot`

use conduit_api::routes::build_router;
use conduit_api::AppState;

// ─── Test helpers ────────────────────────────────────────────────────────

/// Create an AppState pointing at temp directories.
fn test_state(auth_enabled: bool) -> Arc<AppState> {
    let tmp = std::env::temp_dir().join(format!("conduit_test_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    let dags = tmp.join("dags");
    std::fs::create_dir_all(&dags).unwrap();
    AppState::with_options(dags, tmp, None, auth_enabled)
}

/// Build the router with a fresh state.
fn app(auth_enabled: bool) -> (axum::Router, Arc<AppState>) {
    let state = test_state(auth_enabled);
    let router = build_router(state.clone());
    (router, state)
}

/// Make a GET request and return (status, body_string).
async fn get(router: &axum::Router, path: &str) -> (StatusCode, String) {
    get_with_auth(router, path, None).await
}

/// Make a GET request with optional Bearer token.
async fn get_with_auth(
    router: &axum::Router,
    path: &str,
    token: Option<&str>,
) -> (StatusCode, String) {
    let mut req = Request::builder()
        .method("GET")
        .uri(path);

    if let Some(t) = token {
        req = req.header("Authorization", format!("Bearer {}", t));
    }

    let req = req.body(Body::empty()).unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&body).to_string())
}

/// Make a POST request with JSON body and optional auth.
async fn post_json(
    router: &axum::Router,
    path: &str,
    json: &str,
    token: Option<&str>,
) -> (StatusCode, String) {
    let mut req = Request::builder()
        .method("POST")
        .uri(path)
        .header("Content-Type", "application/json");

    if let Some(t) = token {
        req = req.header("Authorization", format!("Bearer {}", t));
    }

    let req = req.body(Body::from(json.to_string())).unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&body).to_string())
}

/// Make a DELETE request with optional auth.
async fn delete_with_auth(
    router: &axum::Router,
    path: &str,
    token: Option<&str>,
) -> (StatusCode, String) {
    let mut req = Request::builder()
        .method("DELETE")
        .uri(path);

    if let Some(t) = token {
        req = req.header("Authorization", format!("Bearer {}", t));
    }

    let req = req.body(Body::empty()).unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&body).to_string())
}

// ─── Public Endpoints ────────────────────────────────────────────────────

#[tokio::test]
async fn health_check_returns_200() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/health").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("ok") || body.contains("healthy"));
}

#[tokio::test]
async fn system_info_returns_200() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/info").await;
    assert_eq!(status, StatusCode::OK);
    let info: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(info.get("version").is_some() || info.get("name").is_some());
}

// ─── DAG Endpoints (no auth) ─────────────────────────────────────────────

#[tokio::test]
async fn list_dags_returns_empty_array() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/dags").await;
    assert_eq!(status, StatusCode::OK);
    let resp: serde_json::Value = serde_json::from_str(&body).unwrap();
    // Handler returns { "dags": [...], "total": N }
    let dags = &resp["dags"];
    assert!(dags.is_array(), "Expected dags array, got: {}", body);
}

#[tokio::test]
async fn get_nonexistent_dag_returns_404() {
    let (router, _) = app(false);
    let (status, _) = get(&router, "/api/v1/dags/nonexistent").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ─── Run Endpoints (no auth) ─────────────────────────────────────────────

#[tokio::test]
async fn list_all_runs_returns_empty() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/runs").await;
    assert_eq!(status, StatusCode::OK);
    let resp: serde_json::Value = serde_json::from_str(&body).unwrap();
    // Handler returns { "runs": [...], "total": N }
    let runs = &resp["runs"];
    assert!(runs.is_array(), "Expected runs array, got: {}", body);
    assert_eq!(runs.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn list_dag_runs_returns_empty() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/dags/test_dag/runs").await;
    assert_eq!(status, StatusCode::OK);
    let resp: serde_json::Value = serde_json::from_str(&body).unwrap();
    let runs = &resp["runs"];
    assert!(runs.is_array(), "Expected runs array, got: {}", body);
}

// ─── Environment Endpoints (no auth) ─────────────────────────────────────

#[tokio::test]
async fn list_environments_has_production() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/environments").await;
    assert_eq!(status, StatusCode::OK);
    let resp: serde_json::Value = serde_json::from_str(&body).unwrap();
    // Handler returns { "environments": [...], "total": N }
    let envs = resp["environments"].as_array().unwrap();
    assert!(envs.iter().any(|e| e.get("name").and_then(|n| n.as_str()) == Some("production")));
}

#[tokio::test]
async fn create_and_get_environment() {
    let (router, _) = app(false);

    // Create a staging environment
    let (status, body) = post_json(
        &router,
        "/api/v1/environments",
        r#"{"name": "staging", "based_on": "production"}"#,
        None,
    ).await;
    assert_eq!(status, StatusCode::OK, "Create env failed: {}", body);

    // Retrieve it
    let (status, body) = get(&router, "/api/v1/environments/staging").await;
    assert_eq!(status, StatusCode::OK);
    let env: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(env.get("name").and_then(|n| n.as_str()), Some("staging"));
}

#[tokio::test]
async fn delete_nonexistent_environment() {
    let (router, _) = app(false);
    let (status, _) = delete_with_auth(&router, "/api/v1/environments/nonexistent", None).await;
    // Should return 404 or 400
    assert!(status == StatusCode::NOT_FOUND || status == StatusCode::BAD_REQUEST);
}

// ─── Connection Endpoints (no auth) ──────────────────────────────────────

#[tokio::test]
async fn list_connections_returns_array() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/connections").await;
    assert_eq!(status, StatusCode::OK);
    let resp: serde_json::Value = serde_json::from_str(&body).unwrap();
    // Handler returns { "connections": [...], "total": N }
    let conns = &resp["connections"];
    assert!(conns.is_array(), "Expected connections array, got: {}", body);
}

#[tokio::test]
async fn list_providers_returns_array() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/connections/providers").await;
    assert_eq!(status, StatusCode::OK);
    let resp: serde_json::Value = serde_json::from_str(&body).unwrap();
    // Handler returns { "providers": [...], "total": N }
    let providers = &resp["providers"];
    assert!(providers.is_array(), "Expected providers array, got: {}", body);
}

// ─── Lineage Endpoints (no auth) ─────────────────────────────────────────

#[tokio::test]
async fn extract_sql_lineage() {
    let (router, _) = app(false);
    // SqlLineageRequest requires both `sql` and `source_task_id` fields
    let (status, body) = post_json(
        &router,
        "/api/v1/lineage/sql",
        r#"{"sql": "SELECT a, b FROM source_table WHERE a > 1", "source_task_id": "extract_task"}"#,
        None,
    ).await;
    assert_eq!(status, StatusCode::OK, "Lineage extraction failed: {}", body);
    let result: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(result.is_object());
}

// ─── Contract Endpoints (no auth) ────────────────────────────────────────

#[tokio::test]
async fn list_contracts_returns_array() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/contracts").await;
    assert_eq!(status, StatusCode::OK);
    let resp: serde_json::Value = serde_json::from_str(&body).unwrap();
    // Handler returns { "contracts": [...], ... }
    let contracts = &resp["contracts"];
    assert!(contracts.is_array(), "Expected contracts array, got: {}", body);
}

// ─── Metrics Endpoints (no auth) ─────────────────────────────────────────

#[tokio::test]
async fn list_metrics_returns_array_or_object() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let _: serde_json::Value = serde_json::from_str(&body).unwrap();
}

// ─── Cluster Endpoints (no auth) ─────────────────────────────────────────

#[tokio::test]
async fn cluster_status_returns_json() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/cluster/status").await;
    assert_eq!(status, StatusCode::OK);
    let status_obj: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(status_obj.is_object());
}

// ═════════════════════════════════════════════════════════════════════════
// Authentication Integration Tests
// ═════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn auth_disabled_allows_all_requests() {
    let (router, _) = app(false);

    // No token needed
    let (status, _) = get(&router, "/api/v1/dags").await;
    assert_eq!(status, StatusCode::OK);

    // whoami should return synthetic admin
    let (status, body) = get(&router, "/api/v1/auth/me").await;
    assert_eq!(status, StatusCode::OK);
    let me: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(me.get("role").is_some());
}

#[tokio::test]
async fn auth_enabled_rejects_unauthenticated_requests() {
    let (router, _) = app(true);

    // GET on an auth-protected endpoint (RequireAuth extractor) without token → 401
    let (status, _) = get(&router, "/api/v1/auth/me").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Health endpoint should still be public
    let (status, _) = get(&router, "/api/v1/health").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn auth_full_lifecycle() {
    let (router, state) = app(true);

    // Step 1: Create a bootstrap admin key directly on the store
    let (raw_key, _) = state.auth_store.create_key(
        "integration-test-admin",
        conduit_api::auth::Role::Admin,
        "test",
        Some("Integration test key".to_string()),
        None,
    ).unwrap();

    // Step 2: Authenticate and access protected endpoint
    let (status, body) = get_with_auth(&router, "/api/v1/auth/me", Some(&raw_key)).await;
    assert_eq!(status, StatusCode::OK, "Auth'd GET failed: {}", body);

    // Step 3: whoami should return admin role (lowercase due to serde rename_all)
    let me: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(me.get("role").and_then(|r| r.as_str()), Some("admin"));

    // Step 4: Create a viewer key via the API
    let (status, body) = post_json(
        &router,
        "/api/v1/auth/keys",
        r#"{"name": "viewer-key", "role": "viewer", "description": "Read-only"}"#,
        Some(&raw_key),
    ).await;
    assert_eq!(status, StatusCode::OK, "Create viewer key failed: {}", body);
    let created: serde_json::Value = serde_json::from_str(&body).unwrap();
    let viewer_key = created.get("key").and_then(|k| k.as_str()).unwrap();

    // Step 5: Viewer can read auth/me
    let (status, _) = get_with_auth(&router, "/api/v1/auth/me", Some(viewer_key)).await;
    assert_eq!(status, StatusCode::OK);

    // Step 6: Viewer cannot create API keys (Admin only)
    let (status, _) = post_json(
        &router,
        "/api/v1/auth/keys",
        r#"{"name": "hack", "role": "admin"}"#,
        Some(viewer_key),
    ).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Step 7: List keys shows both
    let (status, body) = get_with_auth(&router, "/api/v1/auth/keys", Some(&raw_key)).await;
    assert_eq!(status, StatusCode::OK);
    let keys_resp: serde_json::Value = serde_json::from_str(&body).unwrap();
    let keys = keys_resp.get("keys").and_then(|k| k.as_array()).or_else(|| keys_resp.as_array());
    assert!(keys.unwrap().len() >= 2);

    // Step 8: Revoke the viewer key
    let viewer_id = created.get("id").and_then(|i| i.as_str()).unwrap();
    let (status, _) = delete_with_auth(
        &router,
        &format!("/api/v1/auth/keys/{}", viewer_id),
        Some(&raw_key),
    ).await;
    assert_eq!(status, StatusCode::OK);

    // Step 9: Revoked key can no longer access
    let (status, _) = get_with_auth(&router, "/api/v1/auth/me", Some(viewer_key)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn auth_invalid_token_returns_401() {
    let (router, _) = app(true);
    // Use auth/me which enforces RequireAuth
    let (status, _) = get_with_auth(&router, "/api/v1/auth/me", Some("cdt_invalid_garbage_token_12345")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn auth_operator_can_trigger_run() {
    let (router, state) = app(true);

    // Create an operator key
    let (op_key, _) = state.auth_store.create_key(
        "operator-key",
        conduit_api::auth::Role::Operator,
        "test",
        None,
        None,
    ).unwrap();

    // Operators should be able to POST (trigger runs, etc.)
    let (status, _) = post_json(
        &router,
        "/api/v1/dags/test_dag/runs",
        r#"{"triggered_by": "test"}"#,
        Some(&op_key),
    ).await;
    // May return 404 (no such dag) or 200, but not 401/403
    assert!(status != StatusCode::UNAUTHORIZED && status != StatusCode::FORBIDDEN,
        "Operator should have permission, got: {}", status);
}

// ─── Events Endpoints (no auth) ──────────────────────────────────────────

#[tokio::test]
async fn list_events_returns_array() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/events").await;
    assert_eq!(status, StatusCode::OK);
    let resp: serde_json::Value = serde_json::from_str(&body).unwrap();
    // Handler returns { "events": [...], "total": N, ... }
    let events = &resp["events"];
    assert!(events.is_array(), "Expected events array, got: {}", body);
}

// ─── Plan/Apply Endpoints (no auth) ──────────────────────────────────────

#[tokio::test]
async fn plan_endpoint_accepts_json() {
    let (router, _) = app(false);
    let (status, body) = post_json(
        &router,
        "/api/v1/plan",
        r#"{"environment": "production"}"#,
        None,
    ).await;
    // Plan may return 200 or a specific status depending on state
    assert!(status.is_success() || status == StatusCode::BAD_REQUEST,
        "Plan returned unexpected status {}: {}", status, body);
}

// ─── Unknown Routes ──────────────────────────────────────────────────────

#[tokio::test]
async fn unknown_api_route_returns_404() {
    let (router, _) = app(false);
    let (status, _) = get(&router, "/api/v1/nonexistent").await;
    assert!(status == StatusCode::NOT_FOUND || status == StatusCode::METHOD_NOT_ALLOWED);
}

// ─── Backfill Endpoint ───────────────────────────────────────────────────

#[tokio::test]
async fn backfill_requires_body() {
    let (router, _) = app(false);
    let (status, _) = post_json(
        &router,
        "/api/v1/backfill",
        r#"{}"#,
        None,
    ).await;
    // Should fail with 400 (missing fields) or 422, not 500
    assert!(status != StatusCode::INTERNAL_SERVER_ERROR,
        "Backfill with empty body returned 500");
}
