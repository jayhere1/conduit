//! Connection management API endpoints.
//!
//! Provides endpoints for:
//! - Listing all configured connections
//! - Getting connection details
//! - Testing connection health
//! - Listing supported provider types

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};

use crate::AppState;

/// GET /api/v1/connections — list all configured connections.
pub async fn list_connections(
    State(state): State<Arc<AppState>>,
) -> Json<Value> {
    let connections = state.list_connections();

    Json(json!({
        "connections": connections,
        "total": connections.len(),
    }))
}

/// GET /api/v1/connections/:name — get details for a specific connection.
pub async fn get_connection(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<Value> {
    let connections = state.list_connections();

    match connections.iter().find(|c| c.name == name) {
        Some(conn) => Json(json!({
            "connection": conn,
        })),
        None => Json(json!({
            "error": format!("Connection '{}' not found", name),
        })),
    }
}

/// POST /api/v1/connections/:name/test — test a specific connection.
pub async fn test_connection(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<Value> {
    match state.test_connection(&name).await {
        Ok(result) => Json(json!({
            "name": name,
            "result": json!({
                "success": result.success,
                "message": result.message,
                "latencyMs": result.latency_ms,
                "serverVersion": result.server_version,
            }),
        })),
        Err(e) => Json(json!({
            "name": name,
            "result": json!({
                "success": false,
                "message": e.to_string(),
                "latencyMs": 0,
            }),
        })),
    }
}

/// GET /api/v1/connections/providers — list supported provider types.
pub async fn list_providers() -> Json<Value> {
    let providers: Vec<Value> = conduit_providers::registry::supported_provider_types()
        .into_iter()
        .map(|(id, name, aliases, category)| {
            json!({
                "id": id,
                "name": name,
                "aliases": aliases,
                "category": category,
            })
        })
        .collect();

    Json(json!({
        "providers": providers,
        "total": providers.len(),
    }))
}
