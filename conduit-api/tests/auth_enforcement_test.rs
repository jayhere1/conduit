//! Global authentication enforcement tests (PRD A1).
//!
//! Table-driven sweep: when auth is enabled, every API route except the
//! public allowlist must reject anonymous requests with 401, and mutating
//! routes must reject read-only (Viewer) keys with 403. When auth is
//! disabled, everything behaves as before.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use conduit_api::auth::Role;
use conduit_api::routes::build_router;
use conduit_api::state::AppState;

// ─── Test infrastructure ─────────────────────────────────────────────────────

fn test_state(auth_enabled: bool) -> Arc<AppState> {
    let tmp = std::env::temp_dir().join(format!("conduit_auth_enf_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    let dags = tmp.join("dags");
    std::fs::create_dir_all(&dags).unwrap();
    AppState::with_options(dags, tmp, None, auth_enabled)
}

async fn request(
    router: &axum::Router,
    method: &Method,
    path: &str,
    token: Option<&str>,
) -> StatusCode {
    let mut builder = Request::builder().method(method.clone()).uri(path);
    if let Some(t) = token {
        builder = builder.header("Authorization", format!("Bearer {t}"));
    }
    let body = if matches!(*method, Method::POST | Method::PUT) {
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
    response.status()
}

/// Every mutating (POST/PUT/DELETE) route in `routes.rs`. New mutating routes
/// MUST be added here — this table is the enforcement contract.
fn mutating_routes() -> Vec<(Method, &'static str)> {
    vec![
        (Method::POST, "/api/v1/auth/keys"),
        (Method::DELETE, "/api/v1/auth/keys/some-key-id"),
        (Method::POST, "/api/v1/dags/compile"),
        (Method::POST, "/api/v1/dags/some_dag/runs"),
        (Method::POST, "/api/v1/environments"),
        (Method::DELETE, "/api/v1/environments/staging"),
        (Method::POST, "/api/v1/environments/promote"),
        (Method::POST, "/api/v1/environments/staging/rollback"),
        (Method::PUT, "/api/v1/environments/staging/policy"),
        (Method::POST, "/api/v1/plan"),
        (Method::POST, "/api/v1/apply"),
        (Method::POST, "/api/v1/lineage/sql"),
        (Method::POST, "/api/v1/lineage/trace/upstream"),
        (Method::POST, "/api/v1/lineage/trace/downstream"),
        (Method::POST, "/api/v1/lineage/graph"),
        (Method::POST, "/api/v1/lineage/schema/diff"),
        (Method::POST, "/api/v1/lineage/contracts/validate"),
        (Method::POST, "/api/v1/lineage/catalog/refresh"),
        (Method::POST, "/api/v1/lineage/cache/invalidate"),
        (Method::POST, "/api/v1/openlineage/v1/lineage"),
        (Method::POST, "/api/v1/backfill"),
        (Method::POST, "/api/v1/connections/somename/test"),
        (Method::POST, "/api/v1/cluster/workers/w1/drain"),
    ]
}

/// Representative read (GET) routes — also require auth, at Viewer level.
fn read_routes() -> Vec<&'static str> {
    vec![
        "/api/v1/dags",
        "/api/v1/runs",
        "/api/v1/environments",
        "/api/v1/events",
        "/api/v1/connections",
        "/api/v1/cluster/status",
        "/api/v1/contracts",
        "/api/v1/metrics",
    ]
}

// ─── Anonymous requests ──────────────────────────────────────────────────────

#[tokio::test]
async fn anonymous_mutating_requests_are_rejected_when_auth_enabled() {
    let state = test_state(true);
    let router = build_router(state);

    for (method, path) in mutating_routes() {
        let status = request(&router, &method, path, None).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{method} {path} must 401 for anonymous requests, got {status}"
        );
    }
}

#[tokio::test]
async fn anonymous_reads_are_rejected_when_auth_enabled() {
    let state = test_state(true);
    let router = build_router(state);

    for path in read_routes() {
        let status = request(&router, &Method::GET, path, None).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "GET {path} must 401 for anonymous requests, got {status}"
        );
    }
}

#[tokio::test]
async fn public_routes_stay_public_when_auth_enabled() {
    let state = test_state(true);
    let router = build_router(state);

    for path in [
        "/api/v1/health",
        "/api/v1/info",
        "/api/v1/docs/openapi.json",
    ] {
        let status = request(&router, &Method::GET, path, None).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "GET {path} must stay public, got {status}"
        );
    }
}

// ─── Role boundaries ─────────────────────────────────────────────────────────

#[tokio::test]
async fn viewer_key_is_forbidden_on_mutations_but_can_read() {
    let state = test_state(true);
    let (viewer_key, _) = state
        .auth_store
        .create_key("viewer", Role::Viewer, "test", None, None)
        .unwrap();
    let router = build_router(state);

    // Reads pass authorization (may still 4xx/5xx for other reasons, but
    // never 401/403).
    for path in read_routes() {
        let status = request(&router, &Method::GET, path, Some(&viewer_key)).await;
        assert_ne!(status, StatusCode::UNAUTHORIZED, "GET {path}: viewer 401");
        assert_ne!(status, StatusCode::FORBIDDEN, "GET {path}: viewer 403");
    }

    // Mutations are forbidden.
    for (method, path) in mutating_routes() {
        let status = request(&router, &method, path, Some(&viewer_key)).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{method} {path} must 403 for Viewer keys, got {status}"
        );
    }
}

#[tokio::test]
async fn operator_key_passes_authorization_on_non_admin_mutations() {
    let state = test_state(true);
    let (operator_key, _) = state
        .auth_store
        .create_key("operator", Role::Operator, "test", None, None)
        .unwrap();
    let router = build_router(state);

    for (method, path) in mutating_routes() {
        // Key management is Admin-only — Operator must be forbidden there.
        let expect_forbidden = path.starts_with("/api/v1/auth/keys");
        let status = request(&router, &method, path, Some(&operator_key)).await;
        if expect_forbidden {
            assert_eq!(
                status,
                StatusCode::FORBIDDEN,
                "{method} {path} must 403 for Operator keys, got {status}"
            );
        } else {
            assert_ne!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {path}: operator got 401"
            );
            assert_ne!(
                status,
                StatusCode::FORBIDDEN,
                "{method} {path}: operator got 403"
            );
        }
    }
}

// ─── Auth disabled (backward compatibility) ──────────────────────────────────

#[tokio::test]
async fn auth_disabled_leaves_routes_open() {
    let state = test_state(false);
    let router = build_router(state);

    for path in read_routes() {
        let status = request(&router, &Method::GET, path, None).await;
        assert_ne!(status, StatusCode::UNAUTHORIZED, "GET {path} 401ed");
        assert_ne!(status, StatusCode::FORBIDDEN, "GET {path} 403ed");
    }
    for (method, path) in mutating_routes() {
        let status = request(&router, &method, path, None).await;
        assert_ne!(status, StatusCode::UNAUTHORIZED, "{method} {path} 401ed");
        assert_ne!(status, StatusCode::FORBIDDEN, "{method} {path} 403ed");
    }
}
