//! DAG run handlers — trigger, monitor, and query run history.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};

use conduit_scheduler::SchedulerEvent;

use crate::auth::Permission;
use crate::error::ApiError;
use crate::middleware::RequireAuth;
use crate::state::DagRunInfo;
use crate::AppState;

/// Query parameters for listing runs.
#[derive(Deserialize)]
pub struct ListRunsQuery {
    pub limit: Option<usize>,
    pub status: Option<String>,
    /// Filter to runs that targeted this environment (e.g. "production", "staging").
    pub environment: Option<String>,
}

/// Request body for triggering a DAG run.
#[derive(Deserialize)]
pub struct TriggerRunRequest {
    /// Optional logical date override.
    pub logical_date: Option<String>,
    /// Optional configuration overrides.
    pub config: Option<HashMap<String, String>>,
    /// Target environment. Defaults to "production".
    pub environment: Option<String>,
}

/// POST /api/v1/dags/:dag_id/runs — trigger a new DAG run.
pub async fn trigger_run(
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
    Path(dag_id): Path<String>,
    Json(body): Json<TriggerRunRequest>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::TriggerRun)?;

    // Verify the DAG exists
    let (plan, _) = conduit_compiler::ConduitPlan::compile(&state.dags_path)
        .map_err(|e| ApiError::CompilationFailed(e.to_string()))?;

    let dag = plan
        .dags
        .get(&dag_id)
        .ok_or_else(|| ApiError::NotFound(format!("DAG '{}' not found", dag_id)))?;

    let now = Utc::now();
    let run_id = format!("run_{}_{}", dag_id, now.format("%Y%m%d_%H%M%S_%3f"));

    let logical_date = body
        .logical_date
        .as_deref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or(now);

    let config = body.config.unwrap_or_default();
    let environment = body
        .environment
        .unwrap_or_else(|| "production".to_string());

    let task_states: HashMap<String, String> = dag
        .tasks
        .keys()
        .map(|tid| (tid.clone(), "pending".to_string()))
        .collect();

    // Dispatch to the scheduler if the channel is available.
    let dispatched = if let Some(tx) = state.scheduler_tx.get() {
        let event = SchedulerEvent::DagRunRequested {
            dag_id: dag_id.clone(),
            run_id: run_id.clone(),
            logical_date,
            config: config.clone(),
        };
        tx.send(event).is_ok()
    } else {
        false
    };

    let status = if dispatched { "dispatched" } else { "queued" };

    let run_info = DagRunInfo {
        run_id: run_id.clone(),
        dag_id: dag_id.clone(),
        status: status.to_string(),
        started_at: now,
        finished_at: None,
        task_states: task_states.clone(),
        triggered_by: "api".to_string(),
        environment: environment.clone(),
    };

    state.record_run(run_info);

    // Broadcast the event via WebSocket
    let ws_event = json!({
        "type": "dag_run_created",
        "dagId": dag_id,
        "runId": run_id,
        "status": status,
        "timestamp": Utc::now().to_rfc3339(),
        "taskCount": dag.tasks.len(),
    });
    state.broadcast_event(&ws_event.to_string());

    let message = if dispatched {
        format!(
            "DAG run '{}' dispatched to scheduler ({} tasks)",
            run_id,
            dag.tasks.len()
        )
    } else {
        format!(
            "DAG run '{}' queued ({} tasks, no scheduler attached)",
            run_id,
            dag.tasks.len()
        )
    };

    Ok(Json(json!({
        "runId": run_id,
        "dagId": dag_id,
        "environment": environment,
        "status": status,
        "taskStates": task_states,
        "message": message,
    })))
}

/// GET /api/v1/dags/:dag_id/runs — list runs for a specific DAG.
pub async fn list_runs(
    State(state): State<Arc<AppState>>,
    Path(dag_id): Path<String>,
    Query(params): Query<ListRunsQuery>,
) -> Json<Value> {
    let limit = params.limit.unwrap_or(50);
    let mut runs = state.get_runs(Some(&dag_id));

    if let Some(ref status) = params.status {
        runs.retain(|r| r.status == *status);
    }
    if let Some(ref env) = params.environment {
        runs.retain(|r| r.environment == *env);
    }

    runs.sort_by_key(|r| std::cmp::Reverse(r.started_at));
    runs.truncate(limit);

    Json(json!({
        "dag_id": dag_id,
        "runs": runs,
        "total": runs.len(),
    }))
}

/// GET /api/v1/runs/:run_id — get details for a specific run.
pub async fn get_run(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let runs = state.get_runs(None);
    let run = runs
        .iter()
        .find(|r| r.run_id == run_id)
        .ok_or_else(|| ApiError::NotFound(format!("Run '{}' not found", run_id)))?;

    Ok(Json(json!({
        "id": run.run_id,
        "dagId": run.dag_id,
        "status": run.status,
        "startedAt": run.started_at.to_rfc3339(),
        "endedAt": run.finished_at.map(|t| t.to_rfc3339()),
        "taskStates": run.task_states,
        "triggeredBy": run.triggered_by,
        "environment": run.environment,
    })))
}

/// GET /api/v1/runs — list all runs across all DAGs.
pub async fn list_all_runs(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListRunsQuery>,
) -> Json<Value> {
    let limit = params.limit.unwrap_or(100);
    let mut runs = state.get_runs(None);

    if let Some(ref status) = params.status {
        runs.retain(|r| r.status == *status);
    }
    if let Some(ref env) = params.environment {
        runs.retain(|r| r.environment == *env);
    }

    runs.sort_by_key(|r| std::cmp::Reverse(r.started_at));
    runs.truncate(limit);

    Json(json!({
        "runs": runs,
        "total": runs.len(),
    }))
}
