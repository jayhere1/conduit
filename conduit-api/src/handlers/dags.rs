//! DAG-related API handlers.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use serde::Serialize;
use serde_json::{json, Value};

use conduit_compiler::ConduitPlan;

use crate::auth::Permission;
use crate::error::ApiError;
use crate::middleware::RequireAuth;
use crate::AppState;

/// Response type for a single DAG (reserved for typed responses).
#[derive(Serialize)]
#[allow(dead_code)]
struct DagResponse {
    id: String,
    description: Option<String>,
    schedule: Option<String>,
    tags: Vec<String>,
    max_active_runs: u32,
    task_count: usize,
    tasks: Vec<TaskSummary>,
    source_file: String,
}

#[derive(Serialize)]
#[allow(dead_code)]
struct TaskSummary {
    id: String,
    task_type: String,
    dependencies: Vec<String>,
    retries: u32,
    pool: Option<String>,
    trigger_rule: String,
}

/// GET /api/v1/dags — list all compiled DAGs.
pub async fn list_dags(State(state): State<Arc<AppState>>) -> Result<Json<Value>, ApiError> {
    let (plan, _stats) = ConduitPlan::compile(&state.dags_path)
        .map_err(|e| ApiError::CompilationFailed(e.to_string()))?;

    let dags: Vec<Value> = plan
        .dags
        .values()
        .map(|dag| {
            json!({
                "id": dag.id,
                "name": dag.id,
                "description": dag.description,
                "schedule": dag.schedule,
                "tags": dag.tags,
                "taskCount": dag.tasks.len(),
                "sourceFile": dag.source_file,
            })
        })
        .collect();

    Ok(Json(json!({
        "dags": dags,
        "total": dags.len(),
    })))
}

/// GET /api/v1/dags/:dag_id — get a specific DAG with full task details.
pub async fn get_dag(
    State(state): State<Arc<AppState>>,
    Path(dag_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let (plan, _) = ConduitPlan::compile(&state.dags_path)
        .map_err(|e| ApiError::CompilationFailed(e.to_string()))?;

    let dag = plan
        .dags
        .get(&dag_id)
        .ok_or_else(|| ApiError::NotFound(format!("DAG '{}' not found", dag_id)))?;

    let tasks: Vec<Value> = dag
        .execution_order
        .iter()
        .filter_map(|tid| dag.tasks.get(tid))
        .map(|task| {
            json!({
                "id": task.id,
                "name": task.id,
                "type": format!("{:?}", task.task_type).split('{').next().unwrap_or("Unknown").trim(),
                "dependencies": task.dependencies.iter().map(|d| &d.task_id).collect::<Vec<_>>(),
                "retries": task.retries,
                "retryDelay": task.retry_delay,
                "pool": task.pool,
                "timeout": task.timeout,
                "priority": task.priority,
                "triggerRule": format!("{:?}", task.trigger_rule),
            })
        })
        .collect();

    Ok(Json(json!({
        "id": dag.id,
        "name": dag.id,
        "description": dag.description,
        "schedule": dag.schedule,
        "tags": dag.tags,
        "maxActiveRuns": dag.max_active_runs,
        "taskCount": dag.tasks.len(),
        "sourceFile": dag.source_file,
        "executionOrder": dag.execution_order,
        "tasks": tasks,
    })))
}

/// GET /api/v1/dags/:dag_id/graph — get DAG as a graph structure for visualization.
///
/// Returns nodes and edges suitable for rendering with D3.js, dagre, or similar.
pub async fn get_dag_graph(
    State(state): State<Arc<AppState>>,
    Path(dag_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let (plan, _) = ConduitPlan::compile(&state.dags_path)
        .map_err(|e| ApiError::CompilationFailed(e.to_string()))?;

    let dag = plan
        .dags
        .get(&dag_id)
        .ok_or_else(|| ApiError::NotFound(format!("DAG '{}' not found", dag_id)))?;

    let nodes: Vec<Value> = dag
        .tasks
        .values()
        .map(|task| {
            json!({
                "id": task.id,
                "name": task.id,
                "type": format!("{:?}", task.task_type).split('{').next().unwrap_or("Unknown").trim(),
                "pool": task.pool,
                "retries": task.retries,
            })
        })
        .collect();

    let mut edges: Vec<Value> = Vec::new();
    for task in dag.tasks.values() {
        for dep in &task.dependencies {
            edges.push(json!({
                "from": dep.task_id,
                "to": task.id,
                "type": format!("{:?}", dep.dependency_type),
            }));
        }
    }

    // Compute depth levels for layout
    let mut depths: HashMap<&str, usize> = HashMap::new();
    for task_id in &dag.execution_order {
        let task = match dag.tasks.get(task_id) {
            Some(t) => t,
            None => continue,
        };
        let depth = task
            .dependencies
            .iter()
            .filter_map(|d| depths.get(d.task_id.as_str()))
            .max()
            .copied()
            .unwrap_or(0)
            + if task.dependencies.is_empty() { 0 } else { 1 };
        depths.insert(task_id, depth);
    }

    let levels: Vec<Value> = depths
        .iter()
        .map(|(id, depth)| json!({ "id": id, "depth": depth }))
        .collect();

    Ok(Json(json!({
        "dagId": dag.id,
        "nodes": nodes,
        "edges": edges,
        "levels": levels,
        "executionOrder": dag.execution_order,
    })))
}

/// POST /api/v1/dags/compile — recompile all DAGs and return results.
pub async fn compile_dags(
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::CompileDags)?;
    let (_plan, stats) = ConduitPlan::compile(&state.dags_path)
        .map_err(|e| ApiError::CompilationFailed(e.to_string()))?;

    let errors: Vec<String> = stats.errors.iter().map(|e| e.to_string()).collect();

    Ok(Json(json!({
        "success": errors.is_empty(),
        "dagsCompiled": stats.dags_compiled,
        "tasksTotal": stats.tasks_total,
        "errors": errors,
        "warnings": stats.warnings,
        "durationMs": stats.duration_ms,
    })))
}
