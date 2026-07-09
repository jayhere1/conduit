//! Route definitions for the Conduit API.
//!
//! All routes are grouped under /api/v1/ for versioning.
//! WebSocket connections are at /ws/events.
//!
//! ## Authentication
//!
//! When auth is enabled (`--auth-enabled`), the `auth_gate` middleware
//! rejects anonymous requests on every endpoint except the public
//! allowlist (`/health`, `/info`, `/docs*`) and enforces the coarse role
//! gate before bodies are parsed:
//! - GET endpoints require at least Viewer role
//! - POST/PUT/DELETE endpoints require Operator or Admin role
//! - Auth management endpoints (`/auth/keys*`) require Admin role
//!
//! Individual mutating handlers additionally check fine-grained
//! permissions via the `RequireAuth` extractor (defense in depth).

use std::sync::Arc;

use axum::http::{header, Method};
use axum::routing::{delete, get, post, put};
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::rate_limit;
use crate::websocket;
use crate::AppState;

/// Build the complete API router.
pub fn build_router(state: Arc<AppState>) -> Router {
    let api_routes = Router::new()
        // ── Health & Info (public) ───────────────────────────
        .route("/health", get(handlers::health_check))
        .route("/info", get(handlers::system_info))
        // ── Documentation (public) ──────────────────────────
        .route("/docs/openapi.json", get(handlers::docs::openapi_spec))
        .route("/docs", get(handlers::docs::swagger_ui))
        .route("/docs/redoc", get(handlers::docs::redoc_ui))
        // ── Authentication ───────────────────────────────────
        .route("/auth/keys", post(handlers::auth::create_key))
        .route("/auth/keys", get(handlers::auth::list_keys))
        .route("/auth/keys/:key_id", get(handlers::auth::get_key))
        .route("/auth/keys/:key_id", delete(handlers::auth::revoke_key))
        .route("/auth/me", get(handlers::auth::whoami))
        // ── DAGs ─────────────────────────────────────────────
        .route("/dags", get(handlers::dags::list_dags))
        .route("/dags/:dag_id", get(handlers::dags::get_dag))
        .route("/dags/:dag_id/graph", get(handlers::dags::get_dag_graph))
        .route("/dags/compile", post(handlers::dags::compile_dags))
        // ── DAG Runs ─────────────────────────────────────────
        .route("/dags/:dag_id/runs", get(handlers::runs::list_runs))
        .route("/dags/:dag_id/runs", post(handlers::runs::trigger_run))
        .route("/runs/:run_id", get(handlers::runs::get_run))
        .route("/runs", get(handlers::runs::list_all_runs))
        // ── Environments ─────────────────────────────────────
        .route("/environments", get(handlers::envs::list_environments))
        .route("/environments", post(handlers::envs::create_environment))
        .route(
            "/environments/:env_name",
            get(handlers::envs::get_environment),
        )
        .route(
            "/environments/:env_name",
            delete(handlers::envs::delete_environment),
        )
        .route(
            "/environments/promote",
            post(handlers::envs::promote_environment),
        )
        .route(
            "/environments/:env_name/diff/:other_env",
            get(handlers::envs::diff_environments),
        )
        .route(
            "/environments/:env_name/history",
            get(handlers::envs::list_env_history),
        )
        .route(
            "/environments/:env_name/history/:version",
            get(handlers::envs::get_env_history_version),
        )
        .route(
            "/environments/:env_name/rollback",
            post(handlers::envs::rollback_environment),
        )
        .route(
            "/environments/:env_name/policy",
            put(handlers::envs::update_env_policy),
        )
        // ── Plan/Apply ───────────────────────────────────────
        .route("/plan", post(handlers::plan::generate_plan))
        .route("/apply", post(handlers::plan::apply_plan))
        // ── Events / History ─────────────────────────────────
        .route("/events", get(handlers::events::list_events))
        .route("/events/:sequence", get(handlers::events::get_event))
        // ── Lineage ────────────────────────────────────────────
        .route("/lineage/sql", post(handlers::lineage::extract_sql_lineage))
        .route(
            "/lineage/trace/upstream",
            post(handlers::lineage::trace_upstream),
        )
        .route(
            "/lineage/trace/downstream",
            post(handlers::lineage::trace_downstream),
        )
        .route("/lineage/graph", post(handlers::lineage::lineage_graph))
        .route("/lineage/schema/diff", post(handlers::lineage::schema_diff))
        .route(
            "/lineage/contracts/validate",
            post(handlers::lineage::validate_contract),
        )
        .route(
            "/lineage/catalog/refresh",
            post(handlers::lineage::refresh_catalog),
        )
        // ── OpenLineage ingest ───────────────────────────────────
        // Spec-compliant path so external producers (Airflow/dbt/Spark
        // OpenLineage exporters) need only swap the base URL.
        .route(
            "/openlineage/v1/lineage",
            post(handlers::openlineage_ingest::ingest_event),
        )
        .route(
            "/openlineage/events",
            get(handlers::openlineage_ingest::list_events),
        )
        .route(
            "/openlineage/datasets/:namespace/:name",
            get(handlers::openlineage_ingest::get_dataset),
        )
        .route(
            "/openlineage/stats",
            get(handlers::openlineage_ingest::stats),
        )
        // Unified dataset view: fuses internal Conduit lineage with
        // ingested OpenLineage events into a single per-dataset
        // response. This is what the UI Datasets tab consumes.
        .route(
            "/lineage/datasets/:namespace/:name/unified",
            get(handlers::openlineage_ingest::unified_dataset_view),
        )
        // Plan + stitched-lineage cache, observed and manually flushed.
        .route(
            "/lineage/cache/stats",
            get(handlers::openlineage_ingest::cache_stats),
        )
        .route(
            "/lineage/cache/invalidate",
            post(handlers::openlineage_ingest::cache_invalidate),
        )
        // ── Contracts ────────────────────────────────────────────
        .route("/contracts", get(handlers::contracts::list_contracts))
        .route(
            "/contracts/:dag_id",
            get(handlers::contracts::dag_contracts),
        )
        .route(
            "/contracts/:dag_id/:task_id",
            get(handlers::contracts::task_contracts),
        )
        // ── Metrics ─────────────────────────────────────────────
        .route("/metrics", get(handlers::metrics::list_metrics))
        .route(
            "/metrics/:dag_id/:task_id",
            get(handlers::metrics::get_task_metrics),
        )
        // ── Connections ────────────────────────────────────────
        .route("/connections", get(handlers::connections::list_connections))
        .route(
            "/connections/providers",
            get(handlers::connections::list_providers),
        )
        .route(
            "/connections/:name",
            get(handlers::connections::get_connection),
        )
        .route(
            "/connections/:name/test",
            post(handlers::connections::test_connection),
        )
        // ── Backfill ────────────────────────────────────────────
        .route("/backfill", post(handlers::backfill::create_backfill))
        // ── Cluster ────────────────────────────────────────────
        .route("/cluster/status", get(handlers::cluster::cluster_status))
        .route(
            "/cluster/workers/:id/drain",
            post(handlers::cluster::drain_worker),
        );

    // Layer order (innermost first): auth_gate runs closest to the handlers,
    // so rate limiting and the body-size cap still apply to unauthenticated
    // requests. The Extension layer must be outermost so the limiter is
    // available when the rate_limit_middleware reads request extensions.
    let limiter = rate_limit::create_rate_limiter();
    let api_routes = api_routes
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::middleware::auth_gate,
        ))
        .layer(axum::middleware::from_fn(rate_limit::rate_limit_middleware))
        .layer(axum::Extension(limiter.clone()))
        .layer(RequestBodyLimitLayer::new(10 * 1024 * 1024)); // 10 MB

    let ws_routes = Router::new()
        .route("/events", get(websocket::ws_handler))
        .layer(axum::middleware::from_fn(rate_limit::rate_limit_middleware))
        .layer(axum::Extension(limiter.clone()));

    // Prometheus scrape endpoint with rate limiting
    let metrics_route = Router::new()
        .route("/metrics", get(handlers::prometheus::prometheus_metrics))
        .layer(axum::middleware::from_fn(rate_limit::rate_limit_middleware))
        .layer(axum::Extension(limiter));

    let mut router = Router::new()
        .merge(metrics_route)
        .nest("/api/v1", api_routes)
        .nest("/ws", ws_routes);

    // Serve the built React UI as a fallback for non-API routes
    if let Some(ui_dir) = &state.ui_dir {
        if ui_dir.exists() {
            let index_html = ui_dir.join("index.html");
            router =
                router.fallback_service(ServeDir::new(ui_dir).fallback(ServeFile::new(index_html)));
            tracing::info!(path = %ui_dir.display(), "Serving UI from static assets");
        }
    }

    if state.auth_store.auth_enabled {
        tracing::info!("Authentication enabled — API keys required for all endpoints");
    } else {
        tracing::info!("Authentication disabled — all endpoints are publicly accessible");
    }

    // CORS: same-origin only by default (no CORS headers emitted).
    // Cross-origin callers — e.g. a UI dev server on another port — must be
    // allowed explicitly via `conduit serve --cors-origin <URL>` (repeatable).
    let cors_origins: Vec<axum::http::HeaderValue> = state
        .cors_allowed_origins
        .read()
        .map(|v| v.iter().filter_map(|o| o.parse().ok()).collect())
        .unwrap_or_default();

    let router = if cors_origins.is_empty() {
        router
    } else {
        let cors = CorsLayer::new()
            .allow_origin(cors_origins)
            .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS])
            .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]);
        router.layer(cors)
    };

    router.layer(TraceLayer::new_for_http()).with_state(state)
}
