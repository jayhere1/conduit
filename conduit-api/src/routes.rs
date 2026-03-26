//! Route definitions for the Conduit API.
//!
//! All routes are grouped under /api/v1/ for versioning.
//! WebSocket connections are at /ws/events.
//!
//! ## Authentication
//!
//! When auth is enabled (`--auth-enabled`), all endpoints except `/health`
//! require a valid `Authorization: Bearer <api-key>` header.
//!
//! Individual handlers check permissions via the `RequireAuth` extractor:
//! - GET endpoints require at least Viewer role
//! - POST/DELETE endpoints require Operator or Admin role
//! - Auth management endpoints require Admin role

use std::sync::Arc;

use axum::routing::{get, post, delete};
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;

use crate::handlers;
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
        .route("/environments/:env_name", get(handlers::envs::get_environment))
        .route("/environments/:env_name", delete(handlers::envs::delete_environment))
        .route("/environments/promote", post(handlers::envs::promote_environment))
        .route("/environments/:env_name/diff/:other_env", get(handlers::envs::diff_environments))

        // ── Plan/Apply ───────────────────────────────────────
        .route("/plan", post(handlers::plan::generate_plan))
        .route("/apply", post(handlers::plan::apply_plan))

        // ── Events / History ─────────────────────────────────
        .route("/events", get(handlers::events::list_events))
        .route("/events/:sequence", get(handlers::events::get_event))

        // ── Lineage ────────────────────────────────────────────
        .route("/lineage/sql", post(handlers::lineage::extract_sql_lineage))
        .route("/lineage/trace/upstream", post(handlers::lineage::trace_upstream))
        .route("/lineage/trace/downstream", post(handlers::lineage::trace_downstream))
        .route("/lineage/graph", post(handlers::lineage::lineage_graph))
        .route("/lineage/schema/diff", post(handlers::lineage::schema_diff))
        .route("/lineage/contracts/validate", post(handlers::lineage::validate_contract))
        .route("/lineage/catalog/refresh", post(handlers::lineage::refresh_catalog))

        // ── Contracts ────────────────────────────────────────────
        .route("/contracts", get(handlers::contracts::list_contracts))
        .route("/contracts/:dag_id", get(handlers::contracts::dag_contracts))
        .route("/contracts/:dag_id/:task_id", get(handlers::contracts::task_contracts))

        // ── Metrics ─────────────────────────────────────────────
        .route("/metrics", get(handlers::metrics::list_metrics))
        .route("/metrics/:dag_id/:task_id", get(handlers::metrics::get_task_metrics))

        // ── Connections ────────────────────────────────────────
        .route("/connections", get(handlers::connections::list_connections))
        .route("/connections/providers", get(handlers::connections::list_providers))
        .route("/connections/:name", get(handlers::connections::get_connection))

        // ── Backfill ────────────────────────────────────────────
        .route("/backfill", post(handlers::backfill::create_backfill))

        // ── Cluster ────────────────────────────────────────────
        .route("/cluster/status", get(handlers::cluster::cluster_status))
        .route("/cluster/workers/:id/drain", post(handlers::cluster::drain_worker));

    let ws_routes = Router::new()
        .route("/events", get(websocket::ws_handler));

    // Prometheus scrape endpoint at top-level /metrics
    let metrics_route = Router::new()
        .route("/metrics", get(handlers::prometheus::prometheus_metrics));

    let mut router = Router::new()
        .merge(metrics_route)
        .nest("/api/v1", api_routes)
        .nest("/ws", ws_routes);

    // Serve the built React UI as a fallback for non-API routes
    if let Some(ui_dir) = &state.ui_dir {
        if ui_dir.exists() {
            let index_html = ui_dir.join("index.html");
            router = router.fallback_service(
                ServeDir::new(ui_dir).fallback(ServeFile::new(index_html)),
            );
            tracing::info!(path = %ui_dir.display(), "Serving UI from static assets");
        }
    }

    if state.auth_store.auth_enabled {
        tracing::info!("Authentication enabled — API keys required for all endpoints");
    } else {
        tracing::info!("Authentication disabled — all endpoints are publicly accessible");
    }

    router
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
