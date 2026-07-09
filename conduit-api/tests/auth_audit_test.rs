//! Auth audit-log tests (PRD A3).
//!
//! Failed authentications, role denials, and key lifecycle changes must be
//! recorded as `AuthAudit` events in the event store and be queryable via
//! `GET /api/v1/events?event_type=authaudit`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

use conduit_api::auth::Role;
use conduit_api::routes::build_router;
use conduit_api::state::AppState;

/// State with a real (temp) event store attached — `with_options` only opens
/// the store when `state_dir/events` already exists.
fn audited_state() -> Arc<AppState> {
    let tmp = std::env::temp_dir().join(format!("conduit_audit_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(tmp.join("events")).unwrap();
    let dags = tmp.join("dags");
    std::fs::create_dir_all(&dags).unwrap();
    AppState::with_options(dags, tmp, None, true)
}

async fn send(
    router: &axum::Router,
    method: &str,
    path: &str,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(t) = token {
        builder = builder.header("Authorization", format!("Bearer {t}"));
    }
    let body = if method == "POST" {
        builder = builder.header("Content-Type", "application/json");
        Body::from("{}")
    } else {
        Body::empty()
    };
    let response = router
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn audit_events(router: &axum::Router, admin_key: &str) -> Vec<Value> {
    let (status, body) = send(
        router,
        "GET",
        "/api/v1/events?event_type=authaudit",
        Some(admin_key),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "events query failed: {body}");
    body["events"].as_array().cloned().unwrap_or_default()
}

#[tokio::test]
async fn failed_authentication_is_audited() {
    let state = audited_state();
    let (admin_key, _) = state
        .auth_store
        .create_key("admin", Role::Admin, "test", None, None)
        .unwrap();
    let router = build_router(state);

    let (status, _) = send(&router, "GET", "/api/v1/dags", Some("cdt_bogus_key")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let events = audit_events(&router, &admin_key).await;
    assert!(
        events.iter().any(|e| {
            e["kind"]["action"] == "auth_failed"
                && e["kind"]["detail"]
                    .as_str()
                    .is_some_and(|d| d.contains("/dags"))
        }),
        "expected an auth_failed audit event, got: {events:?}"
    );
}

#[tokio::test]
async fn role_denial_is_audited_with_key_identity() {
    let state = audited_state();
    let (admin_key, _) = state
        .auth_store
        .create_key("admin", Role::Admin, "test", None, None)
        .unwrap();
    let (viewer_key, viewer) = state
        .auth_store
        .create_key("viewer", Role::Viewer, "test", None, None)
        .unwrap();
    let router = build_router(state);

    let (status, _) = send(&router, "POST", "/api/v1/plan", Some(&viewer_key)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let events = audit_events(&router, &admin_key).await;
    assert!(
        events.iter().any(|e| {
            e["kind"]["action"] == "permission_denied"
                && e["kind"]["key_id"] == Value::String(viewer.id.clone())
        }),
        "expected a permission_denied audit event for the viewer key, got: {events:?}"
    );
}

#[tokio::test]
async fn key_lifecycle_is_audited() {
    let state = audited_state();
    let (admin_key, _) = state
        .auth_store
        .create_key("admin", Role::Admin, "test", None, None)
        .unwrap();
    let router = build_router(state.clone());

    // Create a key via the API…
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/auth/keys")
        .header("Authorization", format!("Bearer {admin_key}"))
        .header("Content-Type", "application/json")
        .body(Body::from(r#"{"name": "ci-bot", "role": "operator"}"#))
        .unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let created: Value = serde_json::from_slice(&bytes).unwrap();
    let new_key_id = created["id"].as_str().unwrap().to_string();

    // …then revoke it.
    let (status, _) = send(
        &router,
        "DELETE",
        &format!("/api/v1/auth/keys/{new_key_id}"),
        Some(&admin_key),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let events = audit_events(&router, &admin_key).await;
    let actions: Vec<&str> = events
        .iter()
        .filter(|e| e["kind"]["key_id"] == Value::String(new_key_id.clone()))
        .filter_map(|e| e["kind"]["action"].as_str())
        .collect();
    assert!(
        actions.contains(&"key_created") && actions.contains(&"key_revoked"),
        "expected key_created + key_revoked for {new_key_id}, got: {actions:?}"
    );
}
