//! Python wrapper for conduit-planner
//!
//! Exposes fingerprinting, change detection, and deployment planning.

use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use serde_json::{json, Value};
use sha2::{Sha256, Digest};

/// Compute a SHA-256 fingerprint for a JSON value
fn compute_hash(value: &Value) -> String {
    let serialized = serde_json::to_string(value).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(serialized.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Compute fingerprints for all tasks in a compiled plan
///
/// Args:
///     plan_json: JSON string representation of the ConduitPlan from compile_dags
///
/// Returns:
///     JSON string mapping task IDs to their fingerprints: {
///       "fingerprints": {
///         "task_id": {
///           "hash": "sha256...",
///           "version": 1
///         }
///       }
///     }
#[pyfunction]
pub fn compute_fingerprints(plan_json: &str) -> PyResult<String> {
    let plan: Value = serde_json::from_str(plan_json)
        .map_err(|e| PyValueError::new_err(format!("Invalid plan JSON: {}", e)))?;

    let empty_dags = vec![];
    let dags = plan.get("dags")
        .and_then(|v| v.as_array())
        .unwrap_or(&empty_dags);

    let mut fingerprints = serde_json::Map::new();

    for dag_value in dags {
        let empty_tasks = vec![];
        let tasks = dag_value.get("tasks")
            .and_then(|v| v.as_array())
            .unwrap_or(&empty_tasks);

        for task_value in tasks {
            let task_id = task_value.get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            let hash = compute_hash(task_value);

            fingerprints.insert(
                task_id.to_string(),
                json!({
                    "hash": hash,
                    "version": 1,
                    "computed_at": chrono::Utc::now().to_rfc3339()
                }),
            );
        }
    }

    let result = json!({
        "fingerprints": fingerprints,
        "computed_at": chrono::Utc::now().to_rfc3339()
    });

    Ok(result.to_string())
}

/// Detect changes between a current plan and a previous environment state
///
/// Args:
///     plan_json: Current plan JSON from compile_dags
///     env_json: Environment/snapshot state JSON
///
/// Returns:
///     JSON string with detected changes: {
///       "changes": {
///         "added": [...],
///         "modified": [...],
///         "removed": [...],
///         "upstream_invalidated": [...]
///       }
///     }
#[pyfunction]
pub fn detect_changes(plan_json: &str, env_json: &str) -> PyResult<String> {
    let _plan: Value = serde_json::from_str(plan_json)
        .map_err(|e| PyValueError::new_err(format!("Invalid plan JSON: {}", e)))?;

    let env: Value = serde_json::from_str(env_json)
        .map_err(|e| PyValueError::new_err(format!("Invalid environment JSON: {}", e)))?;

    // Extract current fingerprints from plan
    let plan_fingerprints = compute_fingerprints(plan_json)?;
    let current_fps: Value = serde_json::from_str(&plan_fingerprints)
        .map_err(|e| PyValueError::new_err(format!("Failed to parse fingerprints: {}", e)))?;

    // Extract previous fingerprints from environment
    let previous_fps = env.get("fingerprints")
        .cloned()
        .unwrap_or(json!({}));

    let mut added = Vec::new();
    let mut modified = Vec::new();
    let mut removed = Vec::new();

    // Check for added and modified tasks
    if let Some(curr_fps) = current_fps.get("fingerprints").and_then(|v| v.as_object()) {
        for (task_id, curr_fp) in curr_fps.iter() {
            if let Some(prev_fp) = previous_fps.get(task_id) {
                // Task existed before - check if modified
                let curr_hash = curr_fp.get("hash").and_then(|v| v.as_str()).unwrap_or("");
                let prev_hash = prev_fp.get("hash").and_then(|v| v.as_str()).unwrap_or("");

                if curr_hash != prev_hash {
                    modified.push(json!({
                        "task_id": task_id,
                        "previous_hash": prev_hash,
                        "current_hash": curr_hash
                    }));
                }
            } else {
                // New task
                added.push(json!({
                    "task_id": task_id,
                    "hash": curr_fp.get("hash")
                }));
            }
        }
    }

    // Check for removed tasks
    if let Some(prev_fps) = previous_fps.as_object() {
        if let Some(curr_fps) = current_fps.get("fingerprints").and_then(|v| v.as_object()) {
            for task_id in prev_fps.keys() {
                if !curr_fps.contains_key(task_id) {
                    removed.push(json!({
                        "task_id": task_id
                    }));
                }
            }
        }
    }

    // Analyze upstream impact
    let mut upstream_invalidated = Vec::new();
    for modified_change in &modified {
        if let Some(task_id) = modified_change.get("task_id").and_then(|v| v.as_str()) {
            upstream_invalidated.push(json!({
                "task_id": task_id,
                "reason": "upstream_modified"
            }));
        }
    }

    let result = json!({
        "changes": {
            "added": added,
            "modified": modified,
            "removed": removed,
            "upstream_invalidated": upstream_invalidated
        },
        "summary": {
            "total_added": added.len(),
            "total_modified": modified.len(),
            "total_removed": removed.len()
        }
    });

    Ok(result.to_string())
}

/// Create the planner submodule for Python
pub fn create_module(py: Python) -> PyResult<Bound<PyModule>> {
    let module = PyModule::new_bound(py, "planner")?;
    module.add_function(wrap_pyfunction!(compute_fingerprints, &module)?)?;
    module.add_function(wrap_pyfunction!(detect_changes, &module)?)?;
    module.add("__doc__", "Change detection and deployment planning module")?;
    Ok(module)
}
