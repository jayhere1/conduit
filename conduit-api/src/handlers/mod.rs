//! Request handlers organized by resource type.

pub mod auth;
pub mod backfill;
pub mod connections;
pub mod contracts;
pub mod dags;
pub mod runs;
pub mod envs;
pub mod plan;
pub mod events;
pub mod lineage;
pub mod metrics;
pub mod cluster;
pub mod docs;
pub mod prometheus;

use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::AppState;

/// GET /api/v1/health — basic health check.
pub async fn health_check() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "service": "conduit",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// GET /api/v1/info — system information.
pub async fn system_info(State(state): State<Arc<AppState>>) -> Json<Value> {
    let env_count = state
        .env_manager
        .list()
        .map(|e| e.len())
        .unwrap_or(0);

    let run_count = state.get_runs(None).len();

    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "dags_path": state.dags_path.display().to_string(),
        "state_dir": state.state_dir.display().to_string(),
        "environments": env_count,
        "total_runs": run_count,
        "snapshots": state.snapshot_store.count(),
    }))
}
