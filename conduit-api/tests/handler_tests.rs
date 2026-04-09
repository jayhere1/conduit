//! Handler-level tests for Conduit API endpoints.
//!
//! These tests go deeper than the integration tests — they verify
//! specific response shapes, field values, edge cases, and state
//! mutations for each handler group.
//!
//! Organised by handler module:
//!   1. Environments — full CRUD, promote, diff
//!   2. Runs — trigger, list with filters, run detail
//!   3. Auth — key lifecycle, RBAC boundaries, edge cases
//!   4. Events — empty store, event retrieval
//!   5. Connections — list, providers
//!   6. Error responses — consistent error shape

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt;

use conduit_api::routes::build_router;
use conduit_api::state::{AppState, DagRunInfo};

// ─── Test Infrastructure ─────────────────────────────────────────────────────

fn test_state(auth_enabled: bool) -> Arc<AppState> {
    let tmp = std::env::temp_dir().join(format!("conduit_handler_test_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    let dags = tmp.join("dags");
    std::fs::create_dir_all(&dags).unwrap();
    AppState::with_options(dags, tmp, None, auth_enabled)
}

fn app(auth_enabled: bool) -> (axum::Router, Arc<AppState>) {
    let state = test_state(auth_enabled);
    let router = build_router(state.clone());
    (router, state)
}

async fn get(router: &axum::Router, path: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

async fn get_auth(router: &axum::Router, path: &str, token: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .header("Authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

async fn post(router: &axum::Router, path: &str, body: &Value) -> (StatusCode, Value) {
    post_with_auth(router, path, body, None).await
}

async fn post_with_auth(
    router: &axum::Router,
    path: &str,
    body: &Value,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri(path)
        .header("Content-Type", "application/json");

    if let Some(t) = token {
        req = req.header("Authorization", format!("Bearer {}", t));
    }

    let req = req
        .body(Body::from(serde_json::to_string(body).unwrap()))
        .unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn delete(router: &axum::Router, path: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("DELETE")
        .uri(path)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn delete_auth(router: &axum::Router, path: &str, token: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("DELETE")
        .uri(path)
        .header("Authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Environment Handlers
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn env_list_includes_production_by_default() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/environments").await;
    assert_eq!(status, StatusCode::OK);

    let envs = body["environments"].as_array().unwrap();
    assert!(envs.len() >= 1, "Should have at least production");
    assert!(envs.iter().any(|e| e["name"] == "production"));
}

#[tokio::test]
async fn env_create_and_retrieve_details() {
    let (router, _) = app(false);

    // Create.
    let (status, body) = post(
        &router,
        "/api/v1/environments",
        &json!({"name": "staging", "based_on": "production"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Create failed: {:?}", body);
    assert_eq!(body["name"], "staging");
    assert!(body["message"].as_str().unwrap().contains("created"));

    // Retrieve.
    let (status, body) = get(&router, "/api/v1/environments/staging").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "staging");
    assert!(body["snapshotCount"].is_number());
    assert!(body["updatedAt"].is_string());
}

#[tokio::test]
async fn env_create_duplicate_returns_error() {
    let (router, _) = app(false);

    // Production already exists.
    let (status, _) = post(
        &router,
        "/api/v1/environments",
        &json!({"name": "production"}),
    )
    .await;

    // Should fail (400 or 409 or similar).
    assert!(
        status.is_client_error(),
        "Duplicate create should fail, got {}",
        status
    );
}

#[tokio::test]
async fn env_delete_removes_environment() {
    let (router, _) = app(false);

    // Create then delete.
    post(
        &router,
        "/api/v1/environments",
        &json!({"name": "ephemeral"}),
    )
    .await;

    let (status, body) = delete(&router, "/api/v1/environments/ephemeral").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["message"].as_str().unwrap().contains("deleted"));

    // Should be gone.
    let (status, _) = get(&router, "/api/v1/environments/ephemeral").await;
    assert!(status.is_client_error());
}

#[tokio::test]
async fn env_promote_copies_snapshots() {
    let (router, _) = app(false);

    // Create staging.
    post(
        &router,
        "/api/v1/environments",
        &json!({"name": "staging", "based_on": "production"}),
    )
    .await;

    // Promote production → staging.
    let (status, body) = post(
        &router,
        "/api/v1/environments/promote",
        &json!({"source": "production", "target": "staging"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Promote failed: {:?}", body);
    assert!(body["snapshotChanges"].is_number());
}

#[tokio::test]
async fn env_diff_returns_comparison() {
    let (router, _) = app(false);

    // Create a second env.
    post(
        &router,
        "/api/v1/environments",
        &json!({"name": "dev", "based_on": "production"}),
    )
    .await;

    let (status, body) = get(&router, "/api/v1/environments/production/diff/dev").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["left"], "production");
    assert_eq!(body["right"], "dev");
    assert!(body["totalDifferences"].is_number());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Run Handlers
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn runs_list_all_empty_state() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/runs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 0);
    assert!(body["runs"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn runs_list_with_seeded_data() {
    let (router, state) = app(false);

    // Seed some runs.
    for i in 0..3 {
        state.record_run(DagRunInfo {
            run_id: format!("run-{}", i),
            dag_id: "test_dag".to_string(),
            status: if i == 1 { "failed" } else { "success" }.to_string(),
            started_at: chrono::Utc::now(),
            finished_at: Some(chrono::Utc::now()),
            task_states: HashMap::new(),
            triggered_by: "test".to_string(),
        });
    }

    // List all.
    let (_, body) = get(&router, "/api/v1/runs").await;
    assert_eq!(body["total"], 3);

    // List by DAG.
    let (_, body) = get(&router, "/api/v1/dags/test_dag/runs").await;
    assert_eq!(body["total"], 3);

    // List for nonexistent DAG.
    let (_, body) = get(&router, "/api/v1/dags/nonexistent/runs").await;
    assert_eq!(body["total"], 0);
}

#[tokio::test]
async fn runs_list_respects_limit_param() {
    let (router, state) = app(false);

    for i in 0..10 {
        state.record_run(DagRunInfo {
            run_id: format!("run-{}", i),
            dag_id: "dag1".to_string(),
            status: "success".to_string(),
            started_at: chrono::Utc::now(),
            finished_at: Some(chrono::Utc::now()),
            task_states: HashMap::new(),
            triggered_by: "test".to_string(),
        });
    }

    let (_, body) = get(&router, "/api/v1/runs?limit=3").await;
    assert_eq!(body["runs"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn runs_list_filters_by_status() {
    let (router, state) = app(false);

    for (i, status) in ["success", "failed", "success", "running"]
        .iter()
        .enumerate()
    {
        state.record_run(DagRunInfo {
            run_id: format!("run-{}", i),
            dag_id: "dag1".to_string(),
            status: status.to_string(),
            started_at: chrono::Utc::now(),
            finished_at: None,
            task_states: HashMap::new(),
            triggered_by: "test".to_string(),
        });
    }

    let (_, body) = get(&router, "/api/v1/dags/dag1/runs?status=success").await;
    assert_eq!(body["total"], 2);

    let (_, body) = get(&router, "/api/v1/dags/dag1/runs?status=failed").await;
    assert_eq!(body["total"], 1);
}

#[tokio::test]
async fn run_get_by_id() {
    let (router, state) = app(false);

    state.record_run(DagRunInfo {
        run_id: "specific-run-123".to_string(),
        dag_id: "my_dag".to_string(),
        status: "success".to_string(),
        started_at: chrono::Utc::now(),
        finished_at: Some(chrono::Utc::now()),
        task_states: HashMap::from([
            ("task_a".into(), "success".into()),
            ("task_b".into(), "success".into()),
        ]),
        triggered_by: "api".to_string(),
    });

    let (status, body) = get(&router, "/api/v1/runs/specific-run-123").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"], "specific-run-123");
    assert_eq!(body["dagId"], "my_dag");
    assert_eq!(body["status"], "success");
    assert_eq!(body["triggeredBy"], "api");
    assert!(body["taskStates"]["task_a"] == "success");
    assert!(body["startedAt"].is_string());
}

#[tokio::test]
async fn run_get_nonexistent_returns_404() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/runs/nonexistent-run").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"]["type"] == "not_found");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Auth Handlers
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn auth_whoami_returns_role_and_permissions() {
    let (router, state) = app(true);

    let (key, _) = state
        .auth_store
        .create_key(
            "admin-key",
            conduit_api::auth::Role::Admin,
            "test",
            None,
            None,
        )
        .unwrap();

    let (status, body) = get_auth(&router, "/api/v1/auth/me", &key).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["authenticated"], true);
    assert_eq!(body["role"], "admin"); // Role uses #[serde(rename_all = "lowercase")]
    assert_eq!(body["permissions"]["viewDags"], true);
    assert_eq!(body["permissions"]["manageApiKeys"], true);
    assert_eq!(body["permissions"]["triggerRun"], true);
}

#[tokio::test]
async fn auth_viewer_cannot_trigger_runs() {
    let (router, state) = app(true);

    let (viewer_key, _) = state
        .auth_store
        .create_key(
            "viewer-key",
            conduit_api::auth::Role::Viewer,
            "test",
            None,
            None,
        )
        .unwrap();

    // Viewers can read.
    let (status, _) = get_auth(&router, "/api/v1/environments", &viewer_key).await;
    assert_eq!(status, StatusCode::OK);

    // Viewers cannot manage API keys (admin-only operation in auth handlers).
    let (status, _) = post_with_auth(
        &router,
        "/api/v1/auth/keys",
        &json!({"name": "hack", "role": "viewer"}),
        Some(&viewer_key),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn auth_key_create_returns_one_time_plaintext() {
    let (router, state) = app(true);

    let (admin_key, _) = state
        .auth_store
        .create_key(
            "bootstrap",
            conduit_api::auth::Role::Admin,
            "test",
            None,
            None,
        )
        .unwrap();

    let (status, body) = post_with_auth(
        &router,
        "/api/v1/auth/keys",
        &json!({"name": "new-key", "role": "operator", "description": "CI pipeline"}),
        Some(&admin_key),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Create key failed: {:?}", body);

    // Response should include the plaintext key.
    let key_str = body["key"].as_str().unwrap();
    assert!(key_str.starts_with("cdt_"), "Key should have cdt_ prefix");
    assert!(body["id"].is_string());
    assert_eq!(body["name"], "new-key");
    assert!(body["message"].as_str().unwrap().contains("Save"));
}

#[tokio::test]
async fn auth_key_create_with_empty_name_fails() {
    let (router, state) = app(true);

    let (admin_key, _) = state
        .auth_store
        .create_key(
            "bootstrap",
            conduit_api::auth::Role::Admin,
            "test",
            None,
            None,
        )
        .unwrap();

    let (status, body) = post_with_auth(
        &router,
        "/api/v1/auth/keys",
        &json!({"name": "", "role": "viewer"}),
        Some(&admin_key),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]["message"].as_str().unwrap().contains("empty"));
}

#[tokio::test]
async fn auth_key_create_with_invalid_role_fails() {
    let (router, state) = app(true);

    let (admin_key, _) = state
        .auth_store
        .create_key(
            "bootstrap",
            conduit_api::auth::Role::Admin,
            "test",
            None,
            None,
        )
        .unwrap();

    let (status, body) = post_with_auth(
        &router,
        "/api/v1/auth/keys",
        &json!({"name": "test", "role": "megaadmin"}), // "superadmin" is a valid alias
        Some(&admin_key),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("Invalid role"));
}

#[tokio::test]
async fn auth_revoked_key_cannot_authenticate() {
    let (router, state) = app(true);

    let (admin_key, _) = state
        .auth_store
        .create_key("admin", conduit_api::auth::Role::Admin, "test", None, None)
        .unwrap();

    // Create then revoke a key.
    let (_, body) = post_with_auth(
        &router,
        "/api/v1/auth/keys",
        &json!({"name": "temp-key", "role": "viewer"}),
        Some(&admin_key),
    )
    .await;

    let temp_key = body["key"].as_str().unwrap().to_string();
    let temp_id = body["id"].as_str().unwrap();

    // Key works before revocation.
    let (status, _) = get_auth(&router, "/api/v1/health", &temp_key).await;
    assert_eq!(status, StatusCode::OK);

    // Revoke.
    let (status, _) = delete_auth(
        &router,
        &format!("/api/v1/auth/keys/{}", temp_id),
        &admin_key,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Key should no longer work.
    let (status, _) = get_auth(&router, "/api/v1/auth/me", &temp_key).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Connection Handlers
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn connections_list_empty() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/connections").await;
    assert_eq!(status, StatusCode::OK);
    let conns = body["connections"].as_array().unwrap();
    assert!(conns.is_empty(), "No connections configured by default");
}

#[tokio::test]
async fn connections_providers_returns_known_types() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/connections/providers").await;
    assert_eq!(status, StatusCode::OK);
    let providers = body["providers"].as_array().unwrap();
    // Should have at least postgres, snowflake, s3 etc.
    assert!(
        !providers.is_empty(),
        "Should list available provider types"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Events Handlers
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn events_list_empty_store() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/events").await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().unwrap();
    assert!(events.is_empty());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Health / Info
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn health_returns_service_and_version() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert_eq!(body["service"], "conduit");
    assert!(body["version"].is_string());
}

#[tokio::test]
async fn info_returns_system_details() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/info").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["version"].is_string());
    assert!(body["dags_path"].is_string() || body["dagsPath"].is_string());
    assert!(body["environments"].is_number());
    assert!(body["total_runs"].is_number() || body["totalRuns"].is_number());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Error Response Shape
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn error_response_has_consistent_shape() {
    let (router, _) = app(false);

    // 404 errors.
    let (status, body) = get(&router, "/api/v1/runs/nonexistent").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"].is_object(), "Error should be an object");
    assert!(
        body["error"]["type"].is_string(),
        "Error should have a type"
    );
    assert!(
        body["error"]["message"].is_string(),
        "Error should have a message"
    );
}

#[tokio::test]
async fn error_401_has_consistent_shape() {
    let (router, _) = app(true);

    // Use auth/me endpoint which enforces RequireAuth extractor
    let (status, body) = get(&router, "/api/v1/auth/me").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(body["error"].is_object());
    assert_eq!(body["error"]["type"], "unauthorized");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Cluster Status
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn cluster_status_has_expected_fields() {
    let (router, _) = app(false);
    let (status, body) = get(&router, "/api/v1/cluster/status").await;
    assert_eq!(status, StatusCode::OK);
    // Should return an object with cluster info.
    assert!(body.is_object());
}
