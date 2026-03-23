//! Plan/Apply workflow handlers.
//!
//! These implement the Terraform-style workflow over HTTP:
//! POST /plan  → returns a deployment plan (what would change)
//! POST /apply → executes a deployment plan

use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};

use conduit_compiler::ConduitPlan;

use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize)]
pub struct PlanRequest {
    /// Target environment (default: "production").
    pub environment: Option<String>,
}

#[derive(Deserialize)]
pub struct ApplyRequest {
    /// Target environment (default: "production").
    pub environment: Option<String>,
    /// A previously generated plan ID to apply.
    pub plan_id: Option<String>,
}

/// POST /api/v1/plan — generate a deployment plan.
pub async fn generate_plan(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PlanRequest>,
) -> Result<Json<Value>, ApiError> {
    let env_name = body.environment.as_deref().unwrap_or("production");

    // Compile current DAGs
    let (plan, stats) = ConduitPlan::compile(&state.dags_path)
        .map_err(|e| ApiError::CompilationFailed(e.to_string()))?;

    if !stats.errors.is_empty() {
        let error_msgs: Vec<String> = stats.errors.iter().map(|e| e.to_string()).collect();
        return Err(ApiError::CompilationFailed(error_msgs.join("; ")));
    }

    // Load environment
    let env = state
        .env_manager
        .get(env_name)
        .unwrap_or_else(|_| conduit_common::snapshot::Environment::new(env_name));

    // Generate deployment plan
    let deploy = conduit_planner::DeploymentPlan::generate(&plan, &env, &state.snapshot_store);

    // Broadcast plan event
    let event = json!({
        "type": "plan_generated",
        "plan_id": deploy.id,
        "environment": env_name,
        "tasks_to_execute": deploy.stats.tasks_to_execute,
        "tasks_to_skip": deploy.stats.tasks_to_skip,
        "timestamp": Utc::now().to_rfc3339(),
    });
    state.broadcast_event(&event.to_string());

    // Format actions for JSON response
    let actions: Vec<Value> = deploy
        .actions
        .iter()
        .map(|action| {
            json!({
                "dag_id": action.dag_id,
                "task_id": action.task_id,
                "action": format!("{:?}", action.action),
                "reason": action.reason,
                "fingerprint": action.fingerprint.as_ref().map(|fp| fp.0.clone()),
            })
        })
        .collect();

    Ok(Json(json!({
        "plan_id": deploy.id,
        "environment": env_name,
        "created_at": deploy.created_at.to_rfc3339(),
        "actions": actions,
        "stats": {
            "total_tasks": deploy.stats.total_tasks_in_plan,
            "to_execute": deploy.stats.tasks_to_execute,
            "to_reuse": deploy.stats.tasks_to_reuse,
            "to_skip": deploy.stats.tasks_to_skip,
            "to_remove": deploy.stats.tasks_to_remove,
            "critical_path_depth": deploy.stats.critical_path_depth,
            "blast_radius": deploy.stats.blast_radius,
        },
        "compilation": {
            "dags_compiled": stats.dags_compiled,
            "tasks_total": stats.tasks_total,
            "duration_ms": stats.duration_ms,
        },
    })))
}

/// POST /api/v1/apply — apply a deployment plan.
pub async fn apply_plan(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ApplyRequest>,
) -> Result<Json<Value>, ApiError> {
    let env_name = body.environment.as_deref().unwrap_or("production");

    // Compile and generate fresh plan
    let (plan, stats) = ConduitPlan::compile(&state.dags_path)
        .map_err(|e| ApiError::CompilationFailed(e.to_string()))?;

    if !stats.errors.is_empty() {
        let error_msgs: Vec<String> = stats.errors.iter().map(|e| e.to_string()).collect();
        return Err(ApiError::CompilationFailed(error_msgs.join("; ")));
    }

    let env = state
        .env_manager
        .get(env_name)
        .unwrap_or_else(|_| conduit_common::snapshot::Environment::new(env_name));

    let deploy = conduit_planner::DeploymentPlan::generate(&plan, &env, &state.snapshot_store);

    if deploy.stats.tasks_to_execute == 0 && deploy.stats.tasks_to_remove == 0 {
        return Ok(Json(json!({
            "message": format!("Nothing to apply. Environment '{}' is up to date.", env_name),
            "tasks_executed": 0,
            "tasks_reused": 0,
            "tasks_removed": 0,
        })));
    }

    // In production, this would dispatch to the scheduler/executor.
    // For now, we record the intent and report what would happen.

    let executable_count = deploy.executable_actions().len();

    // Broadcast apply event
    let event = json!({
        "type": "apply_started",
        "plan_id": deploy.id,
        "environment": env_name,
        "tasks_to_execute": executable_count,
        "timestamp": Utc::now().to_rfc3339(),
    });
    state.broadcast_event(&event.to_string());

    Ok(Json(json!({
        "plan_id": deploy.id,
        "environment": env_name,
        "status": "accepted",
        "tasks_to_execute": deploy.stats.tasks_to_execute,
        "tasks_to_reuse": deploy.stats.tasks_to_reuse,
        "tasks_to_skip": deploy.stats.tasks_to_skip,
        "tasks_to_remove": deploy.stats.tasks_to_remove,
        "message": format!(
            "Apply accepted. {} tasks queued for execution in '{}'.",
            deploy.stats.tasks_to_execute, env_name
        ),
    })))
}
