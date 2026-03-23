//! Environment management handlers.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize)]
pub struct CreateEnvRequest {
    pub name: String,
    pub based_on: Option<String>,
}

#[derive(Deserialize)]
pub struct PromoteRequest {
    pub source: String,
    pub target: String,
}

/// GET /api/v1/environments — list all virtual environments.
pub async fn list_environments(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, ApiError> {
    let envs = state.env_manager.list().map_err(ApiError::from)?;

    let env_list: Vec<Value> = envs
        .iter()
        .map(|env| {
            json!({
                "id": env.id,
                "name": env.id,
                "snapshotCount": env.snapshot_map.len(),
                "updatedAt": env.updated_at.to_rfc3339(),
                "basedOn": env.based_on,
            })
        })
        .collect();

    Ok(Json(json!({
        "environments": env_list,
        "total": env_list.len(),
    })))
}

/// POST /api/v1/environments — create a new virtual environment.
pub async fn create_environment(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateEnvRequest>,
) -> Result<Json<Value>, ApiError> {
    let env = state
        .env_manager
        .create(&body.name, body.based_on.as_deref())
        .map_err(ApiError::from)?;

    // Broadcast event
    let event = json!({
        "type": "environment_created",
        "name": env.id,
        "based_on": env.based_on,
        "snapshot_count": env.snapshot_map.len(),
        "timestamp": Utc::now().to_rfc3339(),
    });
    state.broadcast_event(&event.to_string());

    Ok(Json(json!({
        "id": env.id,
        "name": env.id,
        "snapshotCount": env.snapshot_map.len(),
        "basedOn": env.based_on,
        "message": format!("Environment '{}' created", env.id),
    })))
}

/// GET /api/v1/environments/:env_name — get environment details.
pub async fn get_environment(
    State(state): State<Arc<AppState>>,
    Path(env_name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let env = state.env_manager.get(&env_name).map_err(ApiError::from)?;

    let snapshots: Vec<Value> = env
        .snapshot_map
        .iter()
        .map(|((dag_id, task_id), snap_id)| {
            json!({
                "dagId": dag_id,
                "taskId": task_id,
                "snapshotId": snap_id,
            })
        })
        .collect();

    Ok(Json(json!({
        "id": env.id,
        "name": env.id,
        "snapshotCount": env.snapshot_map.len(),
        "updatedAt": env.updated_at.to_rfc3339(),
        "basedOn": env.based_on,
        "snapshots": snapshots,
    })))
}

/// DELETE /api/v1/environments/:env_name — delete an environment.
pub async fn delete_environment(
    State(state): State<Arc<AppState>>,
    Path(env_name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    state.env_manager.delete(&env_name).map_err(ApiError::from)?;

    Ok(Json(json!({
        "message": format!("Environment '{}' deleted", env_name),
    })))
}

/// POST /api/v1/environments/promote — promote one environment into another.
pub async fn promote_environment(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PromoteRequest>,
) -> Result<Json<Value>, ApiError> {
    let changes = state
        .env_manager
        .promote(&body.source, &body.target)
        .map_err(ApiError::from)?;

    // Broadcast event
    let event = json!({
        "type": "environment_promoted",
        "source": body.source,
        "target": body.target,
        "changes": changes,
        "timestamp": Utc::now().to_rfc3339(),
    });
    state.broadcast_event(&event.to_string());

    Ok(Json(json!({
        "source": body.source,
        "target": body.target,
        "snapshotChanges": changes,
        "message": format!(
            "Promoted '{}' -> '{}' ({} snapshot changes)",
            body.source, body.target, changes
        ),
    })))
}

/// GET /api/v1/environments/:env_name/diff/:other_env — diff two environments.
pub async fn diff_environments(
    State(state): State<Arc<AppState>>,
    Path((env_name, other_env)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let env1 = state.env_manager.get(&env_name).map_err(ApiError::from)?;
    let env2 = state.env_manager.get(&other_env).map_err(ApiError::from)?;

    let diff_count = env1.diff_count(&env2);

    // Find specific differences
    let mut only_in_first = Vec::new();
    let mut only_in_second = Vec::new();
    let mut different_snapshots = Vec::new();

    // Build a unified diff list the frontend can render as a table
    let mut items: Vec<Value> = Vec::new();

    for (key, snap_id) in &env1.snapshot_map {
        match env2.snapshot_map.get(key) {
            None => {
                only_in_first.push(json!({ "dagId": key.0, "taskId": key.1, "snapshotId": snap_id }));
                items.push(json!({
                    "task": format!("{}.{}", key.0, key.1),
                    "sourceSnapshot": snap_id,
                    "targetSnapshot": null,
                    "status": "Removed",
                }));
            }
            Some(other_snap) if other_snap != snap_id => {
                different_snapshots.push(json!({
                    "dagId": key.0, "taskId": key.1,
                    "leftSnapshot": snap_id, "rightSnapshot": other_snap,
                }));
                items.push(json!({
                    "task": format!("{}.{}", key.0, key.1),
                    "sourceSnapshot": snap_id,
                    "targetSnapshot": other_snap,
                    "status": "Changed",
                }));
            }
            _ => {}
        }
    }

    for (key, snap_id) in &env2.snapshot_map {
        if !env1.snapshot_map.contains_key(key) {
            only_in_second.push(json!({ "dagId": key.0, "taskId": key.1, "snapshotId": snap_id }));
            items.push(json!({
                "task": format!("{}.{}", key.0, key.1),
                "sourceSnapshot": null,
                "targetSnapshot": snap_id,
                "status": "Added",
            }));
        }
    }

    Ok(Json(json!({
        "left": env_name,
        "right": other_env,
        "totalDifferences": diff_count,
        "items": items,
        "onlyInLeft": only_in_first,
        "onlyInRight": only_in_second,
        "differentSnapshots": different_snapshots,
    })))
}
