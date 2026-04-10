//! Python wrapper for conduit-state
//!
//! Exposes environment management, event store, and snapshot operations.

use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use serde_json::{json, Value};
use std::sync::Mutex;
use std::collections::HashMap;
use std::path::PathBuf;

/// Python-facing environment store wrapper
///
/// Manages virtual environments and their snapshots. Uses JSON for v0.1.
#[pyclass]
pub struct EnvironmentStore {
    root_path: PathBuf,
    environments: Mutex<HashMap<String, Value>>,
    loaded: Mutex<bool>,
}

#[pymethods]
impl EnvironmentStore {
    /// Create a new EnvironmentStore
    ///
    /// Args:
    ///     path: Root directory for storing environments
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let root_path = PathBuf::from(path);

        // Create directory if it doesn't exist
        std::fs::create_dir_all(&root_path)
            .map_err(|e| PyValueError::new_err(format!("Failed to create directory: {}", e)))?;

        Ok(EnvironmentStore {
            root_path,
            environments: Mutex::new(HashMap::new()),
            loaded: Mutex::new(false),
        })
    }

    /// Create a new environment
    ///
    /// Args:
    ///     name: Environment name (e.g., "dev", "staging", "prod")
    ///     based_on: Optional parent environment to inherit from
    #[pyo3(signature = (name, based_on=None))]
    fn create_env(&self, name: &str, based_on: Option<&str>) -> PyResult<()> {
        let mut envs = self.environments.lock().unwrap();

        let new_env = if let Some(parent_name) = based_on {
            // Clone from parent environment
            if let Some(parent) = envs.get(parent_name) {
                let mut cloned = parent.clone();
                cloned["name"] = json!(name);
                cloned["created_from"] = json!(parent_name);
                cloned["created_at"] = json!(chrono::Utc::now().to_rfc3339());
                cloned
            } else {
                return Err(PyValueError::new_err(format!(
                    "Parent environment '{}' not found",
                    parent_name
                )));
            }
        } else {
            // Create fresh environment
            json!({
                "name": name,
                "created_at": chrono::Utc::now().to_rfc3339(),
                "snapshots": {},
                "fingerprints": {},
                "metadata": {}
            })
        };

        envs.insert(name.to_string(), new_env);
        Ok(())
    }

    /// List all environments
    ///
    /// Returns:
    ///     JSON string with environment list and metadata
    fn list_envs(&self) -> PyResult<String> {
        let envs = self.environments.lock().unwrap();

        let env_list: Vec<Value> = envs.iter().map(|(name, env)| {
            json!({
                "name": name,
                "created_at": env.get("created_at"),
                "created_from": env.get("created_from"),
                "snapshot_count": env.get("snapshots")
                    .and_then(|s| s.as_object())
                    .map(|m| m.len())
                    .unwrap_or(0),
                "metadata": env.get("metadata")
            })
        }).collect();

        let result = json!({
            "environments": env_list,
            "total": env_list.len()
        });

        Ok(result.to_string())
    }

    /// Promote snapshots from source environment to target environment
    ///
    /// This is the "apply" operation - copies snapshots from one environment to another.
    ///
    /// Args:
    ///     source: Source environment name
    ///     target: Target environment name
    fn promote(&self, source: &str, target: &str) -> PyResult<()> {
        let mut envs = self.environments.lock().unwrap();

        // Get source environment
        let source_env = envs.get(source)
            .ok_or_else(|| PyValueError::new_err(format!(
                "Source environment '{}' not found",
                source
            )))?
            .clone();

        // Get or create target environment
        if !envs.contains_key(target) {
            envs.insert(target.to_string(), json!({
                "name": target,
                "created_at": chrono::Utc::now().to_rfc3339(),
                "snapshots": {},
                "fingerprints": {},
                "metadata": {}
            }));
        }

        let target_env = envs.get_mut(target).unwrap();

        // Copy snapshots from source to target
        if let Some(source_snapshots) = source_env.get("snapshots").and_then(|s| s.as_object()) {
            let target_snapshots = target_env.get_mut("snapshots")
                .and_then(|s| s.as_object_mut());

            if let Some(target_snaps) = target_snapshots {
                for (snap_id, snapshot) in source_snapshots.iter() {
                    target_snaps.insert(snap_id.clone(), snapshot.clone());
                }
            }
        }

        // Copy fingerprints from source to target
        if let Some(source_fps) = source_env.get("fingerprints").and_then(|s| s.as_object()) {
            let target_fps = target_env.get_mut("fingerprints")
                .and_then(|s| s.as_object_mut());

            if let Some(target_fps) = target_fps {
                for (fp_id, fp) in source_fps.iter() {
                    target_fps.insert(fp_id.clone(), fp.clone());
                }
            }
        }

        // Update promotion metadata
        target_env["promoted_from"] = json!(source);
        target_env["promoted_at"] = json!(chrono::Utc::now().to_rfc3339());

        Ok(())
    }

    /// Save all environments to disk
    fn save(&self) -> PyResult<()> {
        let envs = self.environments.lock().unwrap();

        for (name, env) in envs.iter() {
            let file_path = self.root_path.join(format!("{}.json", name));
            let json_str = serde_json::to_string_pretty(env)
                .map_err(|e| PyValueError::new_err(format!("Failed to serialize: {}", e)))?;

            std::fs::write(&file_path, json_str)
                .map_err(|e| PyValueError::new_err(format!(
                    "Failed to write environment '{}': {}",
                    name, e
                )))?;
        }

        Ok(())
    }

    /// Load all environments from disk
    fn load(&self) -> PyResult<()> {
        let mut envs = self.environments.lock().unwrap();
        envs.clear();

        // Read all .json files in the root directory
        for entry in std::fs::read_dir(&self.root_path)
            .map_err(|e| PyValueError::new_err(format!("Failed to read directory: {}", e)))? {

            let entry = entry
                .map_err(|e| PyValueError::new_err(format!("Failed to read entry: {}", e)))?;

            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let json_str = std::fs::read_to_string(&path)
                    .map_err(|e| PyValueError::new_err(format!(
                        "Failed to read file {}: {}",
                        path.display(), e
                    )))?;

                let env: Value = serde_json::from_str(&json_str)
                    .map_err(|e| PyValueError::new_err(format!(
                        "Failed to parse JSON in {}: {}",
                        path.display(), e
                    )))?;

                if let Some(name) = env.get("name").and_then(|v| v.as_str()) {
                    envs.insert(name.to_string(), env);
                }
            }
        }

        *self.loaded.lock().unwrap() = true;
        Ok(())
    }

    /// Add or update a snapshot in an environment
    ///
    /// Args:
    ///     env_name: Environment name
    ///     snapshot_id: Snapshot identifier
    ///     snapshot_data: JSON snapshot data
    fn add_snapshot(&self, env_name: &str, snapshot_id: &str, snapshot_data: &str) -> PyResult<()> {
        let snapshot: Value = serde_json::from_str(snapshot_data)
            .map_err(|e| PyValueError::new_err(format!("Invalid snapshot JSON: {}", e)))?;

        let mut envs = self.environments.lock().unwrap();
        let env = envs.get_mut(env_name)
            .ok_or_else(|| PyValueError::new_err(format!(
                "Environment '{}' not found",
                env_name
            )))?;

        let snapshots = env.get_mut("snapshots")
            .and_then(|s| s.as_object_mut())
            .ok_or_else(|| PyValueError::new_err("Invalid environment structure"))?;

        snapshots.insert(snapshot_id.to_string(), snapshot);
        Ok(())
    }

    /// Get a snapshot from an environment
    ///
    /// Args:
    ///     env_name: Environment name
    ///     snapshot_id: Snapshot identifier
    ///
    /// Returns:
    ///     JSON string of the snapshot, or None if not found
    fn get_snapshot(&self, env_name: &str, snapshot_id: &str) -> PyResult<Option<String>> {
        let envs = self.environments.lock().unwrap();
        let env = envs.get(env_name)
            .ok_or_else(|| PyValueError::new_err(format!(
                "Environment '{}' not found",
                env_name
            )))?;

        let snapshot = env.get("snapshots")
            .and_then(|s| s.get(snapshot_id));

        Ok(snapshot.map(|s| s.to_string()))
    }

    /// Get environment as JSON
    ///
    /// Args:
    ///     env_name: Environment name
    ///
    /// Returns:
    ///     JSON string representation of the environment
    fn get_env(&self, env_name: &str) -> PyResult<String> {
        let envs = self.environments.lock().unwrap();
        let env = envs.get(env_name)
            .ok_or_else(|| PyValueError::new_err(format!(
                "Environment '{}' not found",
                env_name
            )))?;

        Ok(env.to_string())
    }
}

/// Create the state submodule for Python
pub fn create_module(py: Python) -> PyResult<Bound<PyModule>> {
    let module = PyModule::new_bound(py, "state")?;
    module.add_class::<EnvironmentStore>()?;
    module.add("__doc__", "Environment state management and snapshot operations")?;
    Ok(module)
}
