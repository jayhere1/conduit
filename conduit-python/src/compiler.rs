//! Python wrapper for conduit-compiler
//!
//! Exposes DAG parsing, validation, and compilation to a ConduitPlan.

use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use std::path::Path;
use conduit_compiler::{DagParser, DependencyResolver};
use serde_json::json;

/// Convert ConduitError to PyErr
fn error_to_pyerr(err: conduit_common::error::ConduitError) -> PyErr {
    PyValueError::new_err(err.to_string())
}

/// Compile DAGs from a file or directory into a ConduitPlan
///
/// Args:
///     path: File or directory path containing Python DAG definitions
///
/// Returns:
///     JSON string representation of the ConduitPlan
#[pyfunction]
pub fn compile_dags(path: &str) -> PyResult<String> {
    let path_obj = Path::new(path);

    // Parse the DAG(s) from the file or directory
    let mut parser = DagParser::new().map_err(error_to_pyerr)?;

    let parsed_dags = if path_obj.is_dir() {
        parser.parse_directory(path_obj).map_err(error_to_pyerr)?
    } else {
        parser.parse_file(path_obj).map_err(error_to_pyerr)?
    };

    // Resolve dependencies into Dag structures
    let (dags, errors) = DependencyResolver::resolve_all(parsed_dags);

    // If there were resolution errors, return them as a JSON error response
    if !errors.is_empty() {
        let error_messages: Vec<String> = errors.iter()
            .map(|e| e.to_string())
            .collect();
        return Err(PyValueError::new_err(format!(
            "Compilation failed with {} errors: {}",
            errors.len(),
            error_messages.join("; ")
        )));
    }

    // Convert Dags to JSON for return to Python
    let plan_json = json!({
        "dags": dags.iter().map(|dag| {
            json!({
                "id": dag.id.0,
                "name": dag.name,
                "tasks": dag.tasks.iter().map(|task| {
                    json!({
                        "id": task.id.0,
                        "type": task.task_type,
                        "dependencies": task.dependencies.iter().map(|dep| {
                            json!({
                                "task_id": dep.task_id.0,
                                "kind": format!("{:?}", dep.kind)
                            })
                        }).collect::<Vec<_>>(),
                        "config": task.config,
                        "trigger_rule": format!("{:?}", task.trigger_rule),
                        "pool": task.pool.as_ref().map(|p| json!({
                            "name": p.name,
                            "slots": p.slots
                        }))
                    })
                }).collect::<Vec<_>>(),
                "description": dag.description
            })
        }).collect::<Vec<_>>()
    });

    Ok(plan_json.to_string())
}

/// Validate DAGs from a file or directory
///
/// Args:
///     path: File or directory path containing Python DAG definitions
///
/// Returns:
///     JSON string with validation results: {
///       "valid": bool,
///       "errors": [error strings],
///       "warnings": [warning strings]
///     }
#[pyfunction]
pub fn validate_dag(path: &str) -> PyResult<String> {
    let path_obj = Path::new(path);

    let mut parser = DagParser::new().map_err(error_to_pyerr)?;

    let parsed_dags = if path_obj.is_dir() {
        parser.parse_directory(path_obj).map_err(error_to_pyerr)?
    } else {
        parser.parse_file(path_obj).map_err(error_to_pyerr)?
    };

    let (dags, errors) = DependencyResolver::resolve_all(parsed_dags);

    let mut validation_errors = Vec::new();
    let mut warnings = Vec::new();

    // Collect resolution errors
    for err in errors {
        validation_errors.push(err.to_string());
    }

    // Perform additional validation on successfully resolved DAGs
    for dag in &dags {
        // Check for empty DAGs
        if dag.tasks.is_empty() {
            warnings.push(format!("DAG '{}' contains no tasks", dag.id.0));
        }

        // Check for tasks with no dependencies (orphans in non-root DAGs)
        for task in &dag.tasks {
            if task.dependencies.is_empty() && dag.tasks.len() > 1 {
                warnings.push(format!(
                    "Task '{}' in DAG '{}' has no dependencies",
                    task.id.0, dag.id.0
                ));
            }
        }
    }

    let result = json!({
        "valid": validation_errors.is_empty(),
        "errors": validation_errors,
        "warnings": warnings,
        "dags_compiled": dags.len()
    });

    Ok(result.to_string())
}

/// Create the compiler submodule for Python
pub fn create_module(py: Python) -> PyResult<Bound<PyModule>> {
    let module = PyModule::new_bound(py, "compiler")?;
    module.add_function(wrap_pyfunction!(compile_dags, &module)?)?;
    module.add_function(wrap_pyfunction!(validate_dag, &module)?)?;
    module.add("__doc__", "DAG compilation and validation module")?;
    Ok(module)
}
