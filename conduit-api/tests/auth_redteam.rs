//! Adversarial authentication tests (PRD A5).
//!
//! These probe the auth surface the way an attacker would: forged and
//! malformed tokens, revoked-key replay, privilege escalation, header
//! smuggling, and an anonymous-mutation sweep. They are the standing
//! regression guard for the security posture described in `SECURITY.md`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use conduit_api::auth::Role;
use conduit_api::routes::build_router;
use conduit_api::state::AppState;

fn state() -> Arc<AppState> {
    let tmp = std::env::temp_dir().join(format!("conduit_redteam_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    let dags = tmp.join("dags");
    std::fs::create_dir_all(&dags).unwrap();
    AppState::with_options(dags, tmp, None, true)
}

async fn status_with_header(
    router: &axum::Router,
    method: Method,
    path: &str,
    auth_header: Option<&str>,
) -> StatusCode {
    let mut builder = Request::builder().method(method.clone()).uri(path);
    if let Some(h) = auth_header {
        builder = builder.header("Authorization", h);
    }
    let body = if matches!(method, Method::POST | Method::PUT) {
        builder = builder.header("Content-Type", "application/json");
        Body::from("{}")
    } else {
        Body::empty()
    };
    router
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap()
        .status()
}

// ─── Forged / malformed tokens ───────────────────────────────────────────────

#[tokio::test]
async fn forged_tokens_are_rejected() {
    let router = build_router(state());
    for forged in [
        "Bearer cdt_0000000000000000000000000000000",
        "Bearer admin",
        "Bearer ' OR '1'='1",
        "Bearer ../../etc/passwd",
        // (a NUL-byte token can't be built as an HTTP header at all — the
        // `http` crate rejects it before auth sees it.)
    ] {
        let status = status_with_header(&router, Method::GET, "/api/v1/dags", Some(forged)).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "forged token accepted: {forged:?}"
        );
    }
}

#[tokio::test]
async fn malformed_authorization_headers_are_rejected() {
    let router = build_router(state());
    for header in [
        "",
        "cdt_nobearerprefix",
        "Basic dXNlcjpwYXNz",       // wrong scheme
        "Bearer",                   // no token
        "Bearer ",                  // empty token
        "bearer lowercase",         // scheme is case-sensitive here
        "Bearer tok1, Bearer tok2", // header smuggling attempt
    ] {
        let status = status_with_header(&router, Method::GET, "/api/v1/dags", Some(header)).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "malformed header accepted: {header:?}"
        );
    }
}

// ─── Revoked / expired replay ────────────────────────────────────────────────

#[tokio::test]
async fn revoked_key_cannot_be_replayed() {
    let s = state();
    let (key, meta) = s
        .auth_store
        .create_key("temp", Role::Operator, "test", None, None)
        .unwrap();
    let router = build_router(s.clone());

    // Works before revocation.
    let before = status_with_header(
        &router,
        Method::GET,
        "/api/v1/dags",
        Some(&format!("Bearer {key}")),
    )
    .await;
    assert_ne!(before, StatusCode::UNAUTHORIZED);

    // Revoke, then the same token must fail (replay defense).
    s.auth_store.revoke_key(&meta.id).unwrap();
    let after = status_with_header(
        &router,
        Method::GET,
        "/api/v1/dags",
        Some(&format!("Bearer {key}")),
    )
    .await;
    assert_eq!(after, StatusCode::UNAUTHORIZED, "revoked key still works");
}

#[tokio::test]
async fn expired_key_is_rejected() {
    let s = state();
    let past = chrono::Utc::now() - chrono::Duration::hours(1);
    let (key, _) = s
        .auth_store
        .create_key("expired", Role::Operator, "test", None, Some(past))
        .unwrap();
    let router = build_router(s);

    let status = status_with_header(
        &router,
        Method::GET,
        "/api/v1/dags",
        Some(&format!("Bearer {key}")),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "expired key accepted");
}

// ─── Privilege escalation ────────────────────────────────────────────────────

#[tokio::test]
async fn viewer_cannot_escalate_to_key_management() {
    let s = state();
    let (viewer, _) = s
        .auth_store
        .create_key("viewer", Role::Viewer, "test", None, None)
        .unwrap();
    let router = build_router(s);

    // A Viewer must not create keys (would be self-escalation to Admin power).
    let create = status_with_header(
        &router,
        Method::POST,
        "/api/v1/auth/keys",
        Some(&format!("Bearer {viewer}")),
    )
    .await;
    assert_eq!(create, StatusCode::FORBIDDEN, "viewer created a key");
}

#[tokio::test]
async fn operator_cannot_manage_keys() {
    let s = state();
    let (operator, _) = s
        .auth_store
        .create_key("operator", Role::Operator, "test", None, None)
        .unwrap();
    let router = build_router(s);

    // Operator can trigger runs but not manage keys (Admin-only).
    let manage = status_with_header(
        &router,
        Method::GET,
        "/api/v1/auth/keys",
        Some(&format!("Bearer {operator}")),
    )
    .await;
    assert_eq!(manage, StatusCode::FORBIDDEN, "operator managed keys");
}

// ─── Anonymous-mutation sweep ────────────────────────────────────────────────

/// No anonymous request may reach a mutating handler when auth is enabled.
#[tokio::test]
async fn anonymous_cannot_mutate_anything() {
    let router = build_router(state());
    let mutations = [
        (Method::POST, "/api/v1/dags/compile"),
        (Method::POST, "/api/v1/dags/x/runs"),
        (Method::POST, "/api/v1/environments"),
        (Method::DELETE, "/api/v1/environments/prod"),
        (Method::POST, "/api/v1/environments/promote"),
        (Method::POST, "/api/v1/plan"),
        (Method::POST, "/api/v1/apply"),
        (Method::POST, "/api/v1/backfill"),
        (Method::POST, "/api/v1/cluster/workers/w1/drain"),
        (Method::POST, "/api/v1/auth/keys"),
        (Method::DELETE, "/api/v1/auth/keys/some-id"),
    ];
    for (method, path) in mutations {
        let status = status_with_header(&router, method.clone(), path, None).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "anonymous reached {method} {path}"
        );
    }
}
