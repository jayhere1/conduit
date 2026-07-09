//! CORS configuration tests (PRD A4).
//!
//! Default is same-origin only: no `Access-Control-Allow-Origin` header is
//! emitted for cross-origin requests. Origins allowed via
//! `AppState::set_cors_origins` (CLI: `conduit serve --cors-origin`) are
//! echoed back exactly.

use std::sync::Arc;

use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;

use conduit_api::routes::build_router;
use conduit_api::state::AppState;

fn test_state() -> Arc<AppState> {
    let tmp = std::env::temp_dir().join(format!("conduit_cors_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    let dags = tmp.join("dags");
    std::fs::create_dir_all(&dags).unwrap();
    AppState::with_options(dags, tmp, None, false)
}

async fn allow_origin_header(router: &axum::Router, origin: &str) -> Option<String> {
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/health")
        .header("Origin", origin)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    response
        .headers()
        .get("access-control-allow-origin")
        .map(|v| v.to_str().unwrap().to_string())
}

#[tokio::test]
async fn default_is_same_origin_only() {
    let state = test_state();
    let router = build_router(state);

    let header = allow_origin_header(&router, "http://evil.example.com").await;
    assert_eq!(
        header, None,
        "no access-control-allow-origin should be emitted by default"
    );
}

#[tokio::test]
async fn configured_origin_is_echoed() {
    let state = test_state();
    state.set_cors_origins(vec!["http://localhost:3000".to_string()]);
    let router = build_router(state);

    let allowed = allow_origin_header(&router, "http://localhost:3000").await;
    assert_eq!(allowed.as_deref(), Some("http://localhost:3000"));

    let denied = allow_origin_header(&router, "http://evil.example.com").await;
    assert_eq!(denied, None, "unlisted origins must not be allowed");
}
