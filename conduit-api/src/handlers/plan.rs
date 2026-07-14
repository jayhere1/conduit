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

use crate::auth::Permission;
use crate::error::ApiError;
use crate::middleware::RequireAuth;
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
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
    Json(body): Json<PlanRequest>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::GeneratePlan)?;

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

    // Cache it so a later POST /apply can look it up by id and apply
    // exactly what was reviewed here.
    state.store_plan(&deploy);

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
///
/// Honors `plan_id` when provided: looks the plan up in the in-memory
/// cache populated by POST /plan, enforces that it targets the requested
/// environment and that the environment hasn't moved since the plan was
/// generated (409 on stale), then executes it for real — dispatching each
/// `Execute` action through the provider registry, validating contracts,
/// storing snapshots, and recording the environment update with history.
/// Without `plan_id`, generates a fresh plan against the current
/// environment state and applies it immediately.
pub async fn apply_plan(
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
    Json(body): Json<ApplyRequest>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::ApplyPlan)?;

    let env_name = body
        .environment
        .as_deref()
        .unwrap_or("production")
        .to_string();

    // Compile current DAGs — needed to look up task definitions for execution.
    let (plan, stats) = ConduitPlan::compile(&state.dags_path)
        .map_err(|e| ApiError::CompilationFailed(e.to_string()))?;
    if !stats.errors.is_empty() {
        let error_msgs: Vec<String> = stats.errors.iter().map(|e| e.to_string()).collect();
        return Err(ApiError::CompilationFailed(error_msgs.join("; ")));
    }

    let deploy = if let Some(plan_id) = body.plan_id.as_deref() {
        let stored = state.get_plan(plan_id).ok_or_else(|| {
            ApiError::NotFound(format!(
                "plan '{}' not found (plans are cached in-memory; regenerate via POST /api/v1/plan)",
                plan_id
            ))
        })?;
        if stored.target_environment != env_name {
            return Err(ApiError::BadRequest(format!(
                "plan '{}' targets environment '{}', not '{}'",
                plan_id, stored.target_environment, env_name
            )));
        }
        let current_version = state
            .env_manager
            .get(&env_name)
            .map(|e| e.current_version)
            .unwrap_or(0);
        if current_version != stored.base_environment_version {
            return Err(ApiError::Conflict(format!(
                "stale plan: environment '{}' is at version {} but plan '{}' was generated against version {} — regenerate the plan",
                env_name, current_version, plan_id, stored.base_environment_version
            )));
        }
        stored
    } else {
        let env = state
            .env_manager
            .get(&env_name)
            .unwrap_or_else(|_| conduit_common::snapshot::Environment::new(&env_name));
        let deploy = conduit_planner::DeploymentPlan::generate(&plan, &env, &state.snapshot_store);
        state.store_plan(&deploy);
        deploy
    };

    if deploy.stats.tasks_to_execute == 0 && deploy.stats.tasks_to_remove == 0 {
        return Ok(Json(json!({
            "plan_id": deploy.id,
            "message": format!("Nothing to apply. Environment '{}' is up to date.", env_name),
            "status": "noop",
            "tasks_executed": 0, "tasks_reused": 0, "tasks_removed": 0,
        })));
    }

    state.broadcast_event(
        &json!({
            "type": "apply_started",
            "plan_id": deploy.id,
            "environment": env_name,
            "tasks_to_execute": deploy.stats.tasks_to_execute,
            "timestamp": Utc::now().to_rfc3339(),
        })
        .to_string(),
    );

    // ── Execute the plan (mirrors CLI cmd_apply) ──
    use conduit_executor::process_runner::{ProcessRunner, TaskContext};
    use conduit_planner::ActionKind;

    let registry = state.provider_registry.read().ok().and_then(|g| g.clone());
    let contract_index: std::collections::HashMap<
        (String, String),
        &conduit_common::contracts::TaskContracts,
    > = deploy
        .pending_contracts
        .iter()
        .map(|tc| {
            (
                (tc.dag_id.clone().unwrap_or_default(), tc.task_id.clone()),
                tc,
            )
        })
        .collect();
    let mut contract_results: Vec<conduit_common::contracts::ValidationResult> = Vec::new();
    let mut new_snapshots: std::collections::HashMap<(String, String), String> =
        std::collections::HashMap::new();
    let (mut executed, mut reused, mut removed) = (0usize, 0usize, 0usize);
    let logical_date = Utc::now();
    let run_id = format!("apply_{}", Utc::now().format("%Y%m%d%H%M%S"));

    for action in &deploy.actions {
        match &action.action {
            ActionKind::Execute => {
                let task = plan
                    .dags
                    .get(&action.dag_id)
                    .and_then(|dag| dag.tasks.get(&action.task_id))
                    .ok_or_else(|| {
                        ApiError::ApplyFailed(format!(
                            "task {}.{} not found in compiled plan",
                            action.dag_id, action.task_id
                        ))
                    })?;

                let context = TaskContext {
                    dag_id: action.dag_id.clone(),
                    run_id: run_id.clone(),
                    task_id: action.task_id.clone(),
                    attempt: 1,
                    logical_date,
                    environment: env_name.clone(),
                    params: Default::default(),
                    extra_env: Vec::new(),
                };

                let output = ProcessRunner::run_with_providers(task, &context, registry.as_deref())
                    .await
                    .map_err(|e| {
                        ApiError::ApplyFailed(format!(
                            "task {}.{} execution error: {}",
                            action.dag_id, action.task_id, e
                        ))
                    })?;
                if output.exit_code != 0 {
                    return Err(ApiError::ApplyFailed(format!(
                        "task {}.{} failed with exit code {}: {}",
                        action.dag_id,
                        action.task_id,
                        output.exit_code,
                        output.stderr.trim()
                    )));
                }

                if let Some(tc) =
                    contract_index.get(&(action.dag_id.clone(), action.task_id.clone()))
                {
                    let result = conduit_common::contracts::ContractEvaluator::evaluate(
                        tc,
                        &output.evidence,
                    );
                    let blocked = !result.passed;
                    contract_results.push(result);
                    if blocked {
                        return Err(ApiError::ApplyFailed(format!(
                            "contract validation failed for {}.{} — environment not updated",
                            action.dag_id, action.task_id
                        )));
                    }
                }

                let snap_id = format!(
                    "snap_{}_{}",
                    action.task_id,
                    Utc::now().format("%Y%m%d%H%M%S%3f")
                );
                if let Some(ref fp) = action.fingerprint {
                    let snapshot = conduit_common::snapshot::Snapshot {
                        id: snap_id.clone(),
                        fingerprint: fp.clone(),
                        dag_id: action.dag_id.clone(),
                        task_id: action.task_id.clone(),
                        created_at: Utc::now(),
                        parent_fingerprints: vec![],
                        metadata: Default::default(),
                    };
                    let _ = state.snapshot_store.put(snapshot);
                }
                new_snapshots.insert((action.dag_id.clone(), action.task_id.clone()), snap_id);
                executed += 1;
            }
            ActionKind::ReuseSnapshot { .. } => reused += 1,
            ActionKind::Skip => {}
            ActionKind::Remove => removed += 1,
        }
    }

    // ── Update the environment (history-recorded, rollbackable) ──
    if state.env_manager.get(&env_name).is_err() {
        let _ = state.env_manager.create(&env_name, None);
    }
    let mut env_snapshot = state
        .env_manager
        .get(&env_name)
        .unwrap_or_else(|_| conduit_common::snapshot::Environment::new(&env_name));
    deploy.apply_to_environment(&mut env_snapshot, &new_snapshots);
    let recorded_version = state
        .env_manager
        .apply_snapshot_map(
            &env_name,
            env_snapshot.snapshot_map.clone(),
            deploy.id.clone(),
        )
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    state.persist_environments();

    if let Some(store) = &state.event_store {
        let _ = store.append(conduit_common::event::EventKind::PlanApplied {
            plan_id: deploy.id.clone(),
            environment: env_name.clone(),
            tasks_executed: executed as u32,
            tasks_skipped: reused as u32,
        });
    }
    state.broadcast_event(
        &json!({
            "type": "apply_completed",
            "plan_id": deploy.id,
            "environment": env_name,
            "tasks_executed": executed,
            "timestamp": Utc::now().to_rfc3339(),
        })
        .to_string(),
    );

    Ok(Json(json!({
        "plan_id": deploy.id,
        "environment": env_name,
        "status": "applied",
        "tasks_executed": executed,
        "tasks_reused": reused,
        "tasks_removed": removed,
        "environment_version": recorded_version,
    })))
}
