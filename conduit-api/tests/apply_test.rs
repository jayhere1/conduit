//! Tests for POST /api/v1/apply — plan store lookup, stale detection,
//! real execution, and environment update.
//!
//! Helpers (`app`, `post`) copied from `handler_tests.rs`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

use conduit_api::routes::build_router;
use conduit_api::state::AppState;

fn test_state(auth_enabled: bool) -> Arc<AppState> {
    let tmp = std::env::temp_dir().join(format!("conduit_apply_test_{}", uuid::Uuid::new_v4()));
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

async fn post(router: &axum::Router, path: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

#[tokio::test]
async fn apply_with_unknown_plan_id_is_404() {
    let (router, _state) = app(false);
    let (status, body) = post(
        &router,
        "/api/v1/apply",
        serde_json::json!({ "plan_id": "plan_nope", "environment": "production" }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND, "{body}");
}

#[tokio::test]
async fn apply_executes_stored_plan_and_updates_environment() {
    let (router, state) = app(false);
    std::fs::write(
        state.dags_path.join("api_apply.yaml"),
        "id: api_apply\ntasks:\n  hello:\n    type: bash\n    command: \"echo done\"\n",
    )
    .unwrap();

    let (status, plan_body) = post(
        &router,
        "/api/v1/plan",
        serde_json::json!({ "environment": "production" }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{plan_body}");
    let plan_id = plan_body["plan_id"].as_str().unwrap().to_string();

    let (status, body) = post(
        &router,
        "/api/v1/apply",
        serde_json::json!({ "plan_id": plan_id, "environment": "production" }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(body["status"], "applied");
    assert!(body["tasks_executed"].as_u64().unwrap() >= 1);

    // The environment now points at a real snapshot.
    let env = state.env_manager.get("production").unwrap();
    assert!(
        !env.snapshot_map.is_empty(),
        "env must gain snapshot pointers"
    );
    assert!(env.current_version >= 1);

    // Re-applying the same plan is now stale → 409.
    let (status, body) = post(
        &router,
        "/api/v1/apply",
        serde_json::json!({ "plan_id": body["plan_id"], "environment": "production" }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::CONFLICT, "{body}");
}

#[tokio::test]
async fn apply_without_plan_id_generates_and_applies_fresh_plan() {
    let (router, state) = app(false);
    std::fs::write(
        state.dags_path.join("fresh_apply.yaml"),
        "id: fresh_apply\ntasks:\n  hello:\n    type: bash\n    command: \"echo done\"\n",
    )
    .unwrap();

    let (status, body) = post(
        &router,
        "/api/v1/apply",
        serde_json::json!({ "environment": "production" }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(body["status"], "applied");
}
