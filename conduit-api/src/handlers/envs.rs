//! Environment management handlers.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::Permission;
use crate::error::ApiError;
use crate::middleware::RequireAuth;
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

#[derive(Deserialize)]
pub struct HistoryQuery {
    /// When true, response includes the full snapshot_map for each version.
    /// Defaults to false — listings only carry summary fields.
    #[serde(default)]
    pub include_snapshots: bool,
}

#[derive(Deserialize, Default)]
pub struct RollbackRequest {
    /// Specific history version to restore. When omitted, rolls back to the
    /// environment's `current_version` (i.e. undoes the most recent mutation).
    pub to_version: Option<u32>,
}

#[derive(Deserialize, Default)]
pub struct PolicyRequest {
    /// When set, only promotions whose source matches this name are allowed.
    pub require_source: Option<String>,
    /// When set, the newest snapshot in the source must be at least this many seconds old.
    pub min_age_secs: Option<u64>,
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
                "currentVersion": env.current_version,
                "promotionPolicy": {
                    "requireSource": env.promotion_policy.require_source,
                    "minAgeSecs": env.promotion_policy.min_age_secs,
                },
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
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateEnvRequest>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::CreateEnvironment)?;

    let env = state
        .env_manager
        .create(&body.name, body.based_on.as_deref())
        .map_err(ApiError::from)?;
    state.persist_environments();

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
        "currentVersion": env.current_version,
        "promotionPolicy": {
            "requireSource": env.promotion_policy.require_source,
            "minAgeSecs": env.promotion_policy.min_age_secs,
        },
        "snapshots": snapshots,
    })))
}

/// DELETE /api/v1/environments/:env_name — delete an environment.
pub async fn delete_environment(
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
    Path(env_name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::DeleteEnvironment)?;

    state
        .env_manager
        .delete(&env_name)
        .map_err(ApiError::from)?;
    state.persist_environments();

    Ok(Json(json!({
        "message": format!("Environment '{}' deleted", env_name),
    })))
}

/// POST /api/v1/environments/promote — promote one environment into another.
pub async fn promote_environment(
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
    Json(body): Json<PromoteRequest>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::PromoteEnvironment)?;

    let changes = state
        .env_manager
        .promote(&body.source, &body.target)
        .map_err(ApiError::from)?;
    state.persist_environments();

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

    let diff = env1.diff(&env2);

    let only_in_first: Vec<Value> = diff
        .removed
        .iter()
        .map(|e| json!({ "dagId": e.dag_id, "taskId": e.task_id, "snapshotId": e.snapshot_id }))
        .collect();
    let only_in_second: Vec<Value> = diff
        .added
        .iter()
        .map(|e| json!({ "dagId": e.dag_id, "taskId": e.task_id, "snapshotId": e.snapshot_id }))
        .collect();
    let different_snapshots: Vec<Value> = diff
        .changed
        .iter()
        .map(|c| {
            json!({
                "dagId": c.dag_id,
                "taskId": c.task_id,
                "leftSnapshot": c.old_snapshot_id,
                "rightSnapshot": c.new_snapshot_id,
            })
        })
        .collect();

    let mut items: Vec<Value> = Vec::with_capacity(diff.total());
    for e in &diff.removed {
        items.push(json!({
            "task": format!("{}.{}", e.dag_id, e.task_id),
            "sourceSnapshot": e.snapshot_id,
            "targetSnapshot": Value::Null,
            "status": "Removed",
        }));
    }
    for c in &diff.changed {
        items.push(json!({
            "task": format!("{}.{}", c.dag_id, c.task_id),
            "sourceSnapshot": c.old_snapshot_id,
            "targetSnapshot": c.new_snapshot_id,
            "status": "Changed",
        }));
    }
    for e in &diff.added {
        items.push(json!({
            "task": format!("{}.{}", e.dag_id, e.task_id),
            "sourceSnapshot": Value::Null,
            "targetSnapshot": e.snapshot_id,
            "status": "Added",
        }));
    }

    Ok(Json(json!({
        "left": env_name,
        "right": other_env,
        "totalDifferences": diff.total(),
        "items": items,
        "onlyInLeft": only_in_first,
        "onlyInRight": only_in_second,
        "differentSnapshots": different_snapshots,
    })))
}

/// GET /api/v1/environments/:env_name/history — list versioned history entries.
pub async fn list_env_history(
    State(state): State<Arc<AppState>>,
    Path(env_name): Path<String>,
    Query(params): Query<HistoryQuery>,
) -> Result<Json<Value>, ApiError> {
    // Confirm the env exists; surfaces a clean 404 instead of returning an empty list.
    let env = state.env_manager.get(&env_name).map_err(ApiError::from)?;

    let summaries = state
        .env_manager
        .history(&env_name)
        .map_err(ApiError::from)?;

    let versions: Vec<Value> = if params.include_snapshots {
        let mut out = Vec::with_capacity(summaries.len());
        for s in &summaries {
            // Fetch each full version. Tolerate read errors per-entry rather
            // than failing the whole listing.
            match state.env_manager.history_version(&env_name, s.version) {
                Ok(v) => out.push(json!({
                    "version": v.version,
                    "envId": v.env_id,
                    "capturedAt": v.captured_at.to_rfc3339(),
                    "reason": v.reason,
                    "snapshotCount": v.snapshot_map.len(),
                    "snapshotMap": v.snapshot_map
                        .iter()
                        .map(|((dag, task), snap)| json!({
                            "dagId": dag,
                            "taskId": task,
                            "snapshotId": snap,
                        }))
                        .collect::<Vec<_>>(),
                })),
                Err(_) => out.push(json!({
                    "version": s.version,
                    "envId": s.env_id,
                    "capturedAt": s.captured_at.to_rfc3339(),
                    "reason": s.reason,
                    "snapshotCount": s.snapshot_count,
                })),
            }
        }
        out
    } else {
        summaries
            .iter()
            .map(|s| {
                json!({
                    "version": s.version,
                    "envId": s.env_id,
                    "capturedAt": s.captured_at.to_rfc3339(),
                    "reason": s.reason,
                    "snapshotCount": s.snapshot_count,
                })
            })
            .collect()
    };

    Ok(Json(json!({
        "environment": env.id,
        "currentVersion": env.current_version,
        "total": versions.len(),
        "versions": versions,
    })))
}

/// GET /api/v1/environments/:env_name/history/:version — fetch a specific
/// history version (always includes its snapshot_map).
pub async fn get_env_history_version(
    State(state): State<Arc<AppState>>,
    Path((env_name, version)): Path<(String, u32)>,
) -> Result<Json<Value>, ApiError> {
    let v = state
        .env_manager
        .history_version(&env_name, version)
        .map_err(ApiError::from)?;

    Ok(Json(json!({
        "version": v.version,
        "envId": v.env_id,
        "capturedAt": v.captured_at.to_rfc3339(),
        "reason": v.reason,
        "snapshotCount": v.snapshot_map.len(),
        "snapshotMap": v.snapshot_map
            .iter()
            .map(|((dag, task), snap)| json!({
                "dagId": dag,
                "taskId": task,
                "snapshotId": snap,
            }))
            .collect::<Vec<_>>(),
    })))
}

/// PUT /api/v1/environments/:env_name/policy — update the env's promotion policy.
///
/// Body fields are optional; null/missing fields clear that constraint.
/// To clear the entire policy, POST `{}`.
pub async fn update_env_policy(
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
    Path(env_name): Path<String>,
    Json(body): Json<PolicyRequest>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::PromoteEnvironment)?;

    use conduit_common::snapshot::PromotionPolicy;

    let policy = PromotionPolicy {
        require_source: body.require_source.clone(),
        min_age_secs: body.min_age_secs,
    };

    let env = state
        .env_manager
        .set_promotion_policy(&env_name, policy)
        .map_err(ApiError::from)?;
    state.persist_environments();

    Ok(Json(json!({
        "environment": env.id,
        "promotionPolicy": {
            "requireSource": env.promotion_policy.require_source,
            "minAgeSecs": env.promotion_policy.min_age_secs,
        },
        "message": format!("Updated promotion policy for '{}'", env.id),
    })))
}

/// POST /api/v1/environments/:env_name/rollback — restore a prior snapshot map.
///
/// Body: `{ "to_version": <u32 | null> }`. When `to_version` is null/missing,
/// rolls back to the environment's `current_version` (undoes the most recent
/// mutation).
pub async fn rollback_environment(
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
    Path(env_name): Path<String>,
    Json(body): Json<RollbackRequest>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::PromoteEnvironment)?;

    let (new_version, changes) = state
        .env_manager
        .rollback(&env_name, body.to_version)
        .map_err(ApiError::from)?;
    state.persist_environments();

    let event = json!({
        "type": "environment_rolled_back",
        "env_name": env_name,
        "rolled_back_to": body.to_version,
        "new_version": new_version,
        "changes": changes,
        "timestamp": Utc::now().to_rfc3339(),
    });
    state.broadcast_event(&event.to_string());

    Ok(Json(json!({
        "environment": env_name,
        "rolledBackTo": body.to_version,
        "newVersion": new_version,
        "snapshotChanges": changes,
        "message": format!(
            "Rolled back '{}' (new version {}, {} snapshot changes)",
            env_name, new_version, changes
        ),
    })))
}
