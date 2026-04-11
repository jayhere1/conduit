//! Virtual environment management.
//!
//! Environments are named sets of snapshot pointers (inspired by SQLMesh).
//! Creating, promoting, and rolling back environments is O(1) — no data is copied.

use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

use conduit_common::error::{ConduitError, ConduitResult};
use conduit_common::snapshot::{Environment, EnvironmentId};
use tracing::info;

/// Manages virtual pipeline environments.
pub struct EnvironmentManager {
    environments: RwLock<HashMap<EnvironmentId, Environment>>,
}

impl EnvironmentManager {
    /// Create a new environment manager with a default "production" environment.
    pub fn new() -> Self {
        let mut envs = HashMap::new();
        envs.insert("production".to_string(), Environment::new("production"));

        Self {
            environments: RwLock::new(envs),
        }
    }

    /// Load environments from a JSON file on disk.
    ///
    /// The file should contain a JSON array of `Environment` objects (as produced
    /// by serializing `self.list()`). If the file contains no "production" env,
    /// one is created automatically.
    pub fn from_file(path: &Path) -> ConduitResult<Self> {
        let data = std::fs::read_to_string(path).map_err(|e| {
            ConduitError::ConfigError(format!("Failed to read environments file: {}", e))
        })?;

        let envs: Vec<Environment> = serde_json::from_str(&data).map_err(|e| {
            ConduitError::ConfigError(format!("Failed to parse environments file: {}", e))
        })?;

        let mut map = HashMap::new();
        for env in envs {
            map.insert(env.id.clone(), env);
        }

        // Ensure production always exists
        if !map.contains_key("production") {
            map.insert("production".to_string(), Environment::new("production"));
        }

        info!(
            count = map.len(),
            "Loaded environments from {}",
            path.display()
        );

        Ok(Self {
            environments: RwLock::new(map),
        })
    }

    /// Save all environments to a JSON file on disk.
    pub fn save_to_file(&self, path: &Path) -> ConduitResult<()> {
        let envs = self.list()?;
        let data = serde_json::to_string_pretty(&envs)?;
        std::fs::write(path, data).map_err(|e| {
            ConduitError::ConfigError(format!("Failed to write environments file: {}", e))
        })?;
        info!(
            count = envs.len(),
            "Saved environments to {}",
            path.display()
        );
        Ok(())
    }

    /// Create a new environment, optionally forked from an existing one.
    pub fn create(&self, name: &str, based_on: Option<&str>) -> ConduitResult<Environment> {
        let mut envs = self
            .environments
            .write()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?;

        if envs.contains_key(name) {
            return Err(ConduitError::ConfigError(format!(
                "Environment '{}' already exists",
                name
            )));
        }

        let env = if let Some(source) = based_on {
            let source_env = envs
                .get(source)
                .ok_or_else(|| ConduitError::EnvironmentNotFound(source.to_string()))?;
            source_env.fork(name)
        } else {
            Environment::new(name)
        };

        info!(
            env = name,
            based_on = based_on.unwrap_or("(empty)"),
            snapshots = env.snapshot_map.len(),
            "Environment created"
        );

        envs.insert(name.to_string(), env.clone());
        Ok(env)
    }

    /// Get an environment by name.
    pub fn get(&self, name: &str) -> ConduitResult<Environment> {
        self.environments
            .read()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?
            .get(name)
            .cloned()
            .ok_or_else(|| ConduitError::EnvironmentNotFound(name.to_string()))
    }

    /// Promote one environment into another (copy snapshot pointers).
    pub fn promote(&self, source: &str, target: &str) -> ConduitResult<u32> {
        let mut envs = self
            .environments
            .write()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?;

        let source_env = envs
            .get(source)
            .ok_or_else(|| ConduitError::EnvironmentNotFound(source.to_string()))?
            .clone();

        let target_env = envs
            .get_mut(target)
            .ok_or_else(|| ConduitError::EnvironmentNotFound(target.to_string()))?;

        let diff = source_env.diff_count(target_env) as u32;
        source_env.promote_into(target_env);

        info!(
            source = source,
            target = target,
            changes = diff,
            "Environment promoted"
        );

        Ok(diff)
    }

    /// List all environments.
    pub fn list(&self) -> ConduitResult<Vec<Environment>> {
        Ok(self
            .environments
            .read()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?
            .values()
            .cloned()
            .collect())
    }

    /// Delete an environment (cannot delete "production").
    pub fn delete(&self, name: &str) -> ConduitResult<()> {
        if name == "production" {
            return Err(ConduitError::ConfigError(
                "Cannot delete the 'production' environment".to_string(),
            ));
        }

        self.environments
            .write()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?
            .remove(name)
            .ok_or_else(|| ConduitError::EnvironmentNotFound(name.to_string()))?;

        Ok(())
    }
}

impl Default for EnvironmentManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_list_environments() {
        let mgr = EnvironmentManager::new();

        mgr.create("staging", Some("production")).unwrap();
        mgr.create("dev-jay", Some("production")).unwrap();

        let envs = mgr.list().unwrap();
        assert_eq!(envs.len(), 3);
    }

    #[test]
    fn promote_environment() {
        let mgr = EnvironmentManager::new();
        mgr.create("staging", Some("production")).unwrap();

        // Modify staging (in real usage, the planner would do this)
        {
            let mut envs = mgr.environments.write().unwrap();
            let staging = envs.get_mut("staging").unwrap();
            staging.snapshot_map.insert(
                ("dag1".to_string(), "task1".to_string()),
                "new_snap".to_string(),
            );
        }

        let changes = mgr.promote("staging", "production").unwrap();
        assert_eq!(changes, 1);

        let prod = mgr.get("production").unwrap();
        assert_eq!(
            prod.snapshot_map
                .get(&("dag1".to_string(), "task1".to_string())),
            Some(&"new_snap".to_string())
        );
    }

    #[test]
    fn save_and_load_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("environments.json");

        // Create and populate a manager
        let mgr = EnvironmentManager::new();
        mgr.create("staging", Some("production")).unwrap();
        mgr.create("dev", None).unwrap();

        // Modify staging to have snapshot pointers
        {
            let mut envs = mgr.environments.write().unwrap();
            let staging = envs.get_mut("staging").unwrap();
            staging.snapshot_map.insert(
                ("dag1".to_string(), "task1".to_string()),
                "snap_abc".to_string(),
            );
        }

        // Save to disk
        mgr.save_to_file(&path).unwrap();
        assert!(path.exists());

        // Load from disk
        let loaded = EnvironmentManager::from_file(&path).unwrap();
        let envs = loaded.list().unwrap();
        assert_eq!(envs.len(), 3); // production, staging, dev

        // Verify data integrity — staging has the snapshot pointer
        let staging = loaded.get("staging").unwrap();
        assert_eq!(
            staging
                .snapshot_map
                .get(&("dag1".to_string(), "task1".to_string())),
            Some(&"snap_abc".to_string())
        );
    }

    #[test]
    fn from_file_ensures_production_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("environments.json");

        // Write a file with no production env
        let envs = vec![conduit_common::snapshot::Environment::new("custom-only")];
        let data = serde_json::to_string_pretty(&envs).unwrap();
        std::fs::write(&path, data).unwrap();

        // Load — production should be auto-created
        let loaded = EnvironmentManager::from_file(&path).unwrap();
        let all = loaded.list().unwrap();
        assert!(all.iter().any(|e| e.id == "production"));
        assert!(all.iter().any(|e| e.id == "custom-only"));
    }

    #[test]
    fn cannot_delete_production() {
        let mgr = EnvironmentManager::new();
        assert!(mgr.delete("production").is_err());
    }
}
