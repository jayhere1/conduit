//! Contract validation API endpoints.
//!
//! Provides endpoints for:
//! - Listing all contracts across DAGs
//! - Validating contracts for a specific task
//! - Running contract validation for an entire deployment plan

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use serde::Serialize;
use serde_json::{json, Value};

use conduit_common::contracts::Severity;
use conduit_compiler::ConduitPlan;

use crate::AppState;

/// Summary of a contract for API responses.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ContractSummary {
    dag_id: String,
    task_id: String,
    check_count: usize,
    checks: Vec<CheckSummary>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CheckSummary {
    name: String,
    /// Serialized as "checkType" (not "type" — reserved word in JS)
    check_type: String,
    severity: String,
    description: Option<String>,
}

/// GET /api/v1/contracts — list all contracts across all DAGs.
pub async fn list_contracts(State(state): State<Arc<AppState>>) -> Json<Value> {
    let plan = match ConduitPlan::compile(&state.dags_path) {
        Ok((plan, _)) => plan,
        Err(e) => {
            return Json(json!({
                "error": format!("Compilation error: {}", e),
                "contracts": [],
            }));
        }
    };

    let mut summaries = Vec::new();

    for (dag_id, dag) in &plan.dags {
        for (task_id, task) in &dag.tasks {
            if let Some(tc) = &task.contracts {
                summaries.push(ContractSummary {
                    dag_id: dag_id.clone(),
                    task_id: task_id.clone(),
                    check_count: tc.checks.len(),
                    checks: tc
                        .checks
                        .iter()
                        .map(|c| CheckSummary {
                            name: c.name.clone(),
                            check_type: format!("{:?}", c.check)
                                .split('{')
                                .next()
                                .unwrap_or("unknown")
                                .trim()
                                .to_string(),
                            severity: match c.severity {
                                Severity::Error => "error".to_string(),
                                Severity::Warning => "warning".to_string(),
                            },
                            description: c.description.clone(),
                        })
                        .collect(),
                });
            }
        }
    }

    let total_checks: usize = summaries.iter().map(|s| s.check_count).sum();

    Json(json!({
        "totalTasksWithContracts": summaries.len(),
        "totalChecks": total_checks,
        "contracts": summaries,
    }))
}

/// GET /api/v1/contracts/:dag_id — list contracts for a specific DAG.
pub async fn dag_contracts(
    State(state): State<Arc<AppState>>,
    Path(dag_id): Path<String>,
) -> Json<Value> {
    let plan = match ConduitPlan::compile(&state.dags_path) {
        Ok((plan, _)) => plan,
        Err(e) => {
            return Json(json!({ "error": format!("Compilation error: {}", e) }));
        }
    };

    let dag = match plan.dags.get(&dag_id) {
        Some(d) => d,
        None => {
            return Json(json!({ "error": format!("DAG '{}' not found", dag_id) }));
        }
    };

    let mut tasks_with_contracts = Vec::new();

    for (task_id, task) in &dag.tasks {
        if let Some(tc) = &task.contracts {
            tasks_with_contracts.push(json!({
                "taskId": task_id,
                "checkCount": tc.checks.len(),
                "checks": tc.checks.iter().map(|c| json!({
                    "name": c.name,
                    "severity": match c.severity {
                        Severity::Error => "error",
                        Severity::Warning => "warning",
                    },
                    "description": c.description,
                })).collect::<Vec<_>>(),
            }));
        }
    }

    Json(json!({
        "dagId": dag_id,
        "tasksWithContracts": tasks_with_contracts.len(),
        "tasks": tasks_with_contracts,
    }))
}

/// GET /api/v1/contracts/:dag_id/:task_id — get contracts for a specific task.
pub async fn task_contracts(
    State(state): State<Arc<AppState>>,
    Path((dag_id, task_id)): Path<(String, String)>,
) -> Json<Value> {
    let plan = match ConduitPlan::compile(&state.dags_path) {
        Ok((plan, _)) => plan,
        Err(e) => {
            return Json(json!({ "error": format!("Compilation error: {}", e) }));
        }
    };

    let dag = match plan.dags.get(&dag_id) {
        Some(d) => d,
        None => {
            return Json(json!({ "error": format!("DAG '{}' not found", dag_id) }));
        }
    };

    let task = match dag.tasks.get(&task_id) {
        Some(t) => t,
        None => {
            return Json(json!({
                "error": format!("Task '{}' not found in DAG '{}'", task_id, dag_id)
            }));
        }
    };

    match &task.contracts {
        Some(tc) => Json(json!({
            "dagId": dag_id,
            "taskId": task_id,
            "checkCount": tc.checks.len(),
            "expectedMetrics": tc.expected_metrics(),
            "checks": tc.checks.iter().map(|c| json!({
                "name": c.name,
                "severity": match c.severity {
                    Severity::Error => "error",
                    Severity::Warning => "warning",
                },
                "description": c.description,
            })).collect::<Vec<_>>(),
        })),
        None => Json(json!({
            "dagId": dag_id,
            "taskId": task_id,
            "checkCount": 0,
            "checks": [],
        })),
    }
}
