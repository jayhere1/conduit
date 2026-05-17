//! Virtual environment management.
//!
//! Environments are named sets of snapshot pointers (inspired by SQLMesh).
//! Creating, promoting, and rolling back environments is O(1) — no data is copied.

use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

use chrono::Utc;
use conduit_common::error::{ConduitError, ConduitResult};
use conduit_common::snapshot::{
    EnvHistoryReason, EnvHistorySummary, EnvSnapshotMapVersion, Environment, EnvironmentId,
    PromotionPolicy,
};
use tracing::info;

use crate::env_history_store::EnvHistoryStore;
use crate::snapshot_store::SnapshotStore;

/// Manages virtual pipeline environments.
pub struct EnvironmentManager {
    environments: RwLock<HashMap<EnvironmentId, Environment>>,
    history: Option<EnvHistoryStore>,
    snapshots: Option<std::sync::Arc<SnapshotStore>>,
}

impl EnvironmentManager {
    /// Create a new environment manager with a default "production" environment.
    pub fn new() -> Self {
        let mut envs = HashMap::new();
        envs.insert("production".to_string(), Environment::new("production"));

        Self {
            environments: RwLock::new(envs),
            history: None,
            snapshots: None,
        }
    }

    /// Attach an on-disk history store. Promote and rollback will capture the
    /// prior `snapshot_map` of the affected env into this store before mutating.
    /// Without a history store, promote/rollback still work but are not
    /// recorded — and rollback requires history, so it will fail.
    pub fn with_history_store(mut self, store: EnvHistoryStore) -> Self {
        self.history = Some(store);
        self
    }

    /// Attach a snapshot store reference. Required for promotion policies that
    /// gate on snapshot age (`min_age_secs`); without it, `min_age_secs`
    /// policies are rejected at promote time with a clear error.
    pub fn with_snapshot_store(mut self, store: std::sync::Arc<SnapshotStore>) -> Self {
        self.snapshots = Some(store);
        self
    }

    /// Reference to the attached history store, if any.
    pub fn history_store(&self) -> Option<&EnvHistoryStore> {
        self.history.as_ref()
    }

    /// Update the promotion policy for an environment.
    pub fn set_promotion_policy(
        &self,
        env_name: &str,
        policy: PromotionPolicy,
    ) -> ConduitResult<Environment> {
        let mut envs = self
            .environments
            .write()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?;
        let env = envs
            .get_mut(env_name)
            .ok_or_else(|| ConduitError::EnvironmentNotFound(env_name.to_string()))?;
        env.promotion_policy = policy;
        env.updated_at = Utc::now();
        Ok(env.clone())
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
            history: None,
            snapshots: None,
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
    /// Returns the number of snapshot changes applied.
    ///
    /// If a history store is attached, the target's prior `snapshot_map` is
    /// captured as a new history version before being overwritten, and the
    /// target's `current_version` is bumped.
    ///
    /// Fails with `PromotionPolicyViolation` when the target's
    /// `promotion_policy` rejects the source or the source's snapshots are
    /// too fresh per `min_age_secs`.
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

        // Enforce the target's promotion policy before mutating anything.
        let policy = target_env.promotion_policy.clone();
        if let Some(required) = &policy.require_source {
            if required != source {
                return Err(ConduitError::PromotionPolicyViolation(format!(
                    "target '{}' requires source '{}', got '{}'",
                    target, required, source
                )));
            }
        }
        if let Some(min_age_secs) = policy.min_age_secs {
            let store = self.snapshots.as_ref().ok_or_else(|| {
                ConduitError::PromotionPolicyViolation(format!(
                    "target '{}' enforces min_age_secs={} but no snapshot store is attached \
                     to validate snapshot ages",
                    target, min_age_secs
                ))
            })?;

            let newest = newest_snapshot_age_secs(store, &source_env)?;
            // Empty source envs vacuously satisfy min-age: there is no fresh
            // change to soak. This matches the "bake time" mental model.
            if let Some(age) = newest {
                if age < min_age_secs {
                    return Err(ConduitError::PromotionPolicyViolation(format!(
                        "target '{}' requires source snapshots to be at least {}s old, but \
                         the newest snapshot in '{}' is {}s old",
                        target, min_age_secs, source, age
                    )));
                }
            }
        }

        let diff = source_env.diff_count(target_env) as u32;

        if let Some(store) = &self.history {
            let next = store.next_version(target)?;
            let entry = EnvSnapshotMapVersion {
                version: next,
                env_id: target.to_string(),
                captured_at: Utc::now(),
                reason: EnvHistoryReason::Promotion {
                    from: source.to_string(),
                },
                snapshot_map: target_env.snapshot_map.clone(),
            };
            store.record(&entry)?;
            target_env.current_version = next;
        }

        source_env.promote_into(target_env);

        info!(
            source = source,
            target = target,
            changes = diff,
            version = target_env.current_version,
            "Environment promoted"
        );

        Ok(diff)
    }

    /// Rewrite an environment's snapshot pointers as the result of a
    /// `conduit apply`. Captures the *prior* `snapshot_map` as a history
    /// entry tagged `EnvHistoryReason::Apply { plan_id }` before mutating,
    /// so the apply is reversible via `rollback`.
    ///
    /// `new_snapshot_map` is the post-apply map (typically built by
    /// `DeploymentPlan::apply_to_environment`). Returns the history version
    /// that captured the pre-apply state.
    ///
    /// If no history store is attached, the env is still mutated — apply
    /// must succeed even without history — but no version is recorded and
    /// `rollback` will refuse later. Callers that care about reversibility
    /// should ensure the manager was built with `with_history_store`.
    pub fn apply_snapshot_map(
        &self,
        env_name: &str,
        new_snapshot_map: HashMap<(conduit_common::dag::DagId, conduit_common::dag::TaskId), String>,
        plan_id: String,
    ) -> ConduitResult<Option<u32>> {
        let mut envs = self
            .environments
            .write()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?;

        let env = envs
            .get_mut(env_name)
            .ok_or_else(|| ConduitError::EnvironmentNotFound(env_name.to_string()))?;

        let captured_version = if let Some(store) = &self.history {
            let next = store.next_version(env_name)?;
            let entry = EnvSnapshotMapVersion {
                version: next,
                env_id: env_name.to_string(),
                captured_at: Utc::now(),
                reason: EnvHistoryReason::Apply {
                    plan_id: plan_id.clone(),
                },
                snapshot_map: env.snapshot_map.clone(),
            };
            store.record(&entry)?;
            env.current_version = next;
            Some(next)
        } else {
            None
        };

        env.snapshot_map = new_snapshot_map;
        env.updated_at = Utc::now();

        info!(
            env = env_name,
            plan_id = plan_id.as_str(),
            version = env.current_version,
            "Apply recorded to environment"
        );

        Ok(captured_version)
    }

    /// Roll back an environment to a prior version captured in the history
    /// store. If `to_version` is None, rolls back to the immediately previous
    /// version (the entry just before `current_version`).
    ///
    /// The current `snapshot_map` is itself captured as a new history version
    /// before being overwritten, so a rollback is reversible by another
    /// rollback to the version this call created.
    ///
    /// Returns `(new_current_version, snapshot_changes_applied)`.
    pub fn rollback(&self, env_name: &str, to_version: Option<u32>) -> ConduitResult<(u32, u32)> {
        let store = self.history.as_ref().ok_or_else(|| {
            ConduitError::ConfigError(
                "Rollback requires a history store. Reopen the environment manager with \
                 with_history_store(EnvHistoryStore::open(...))."
                    .to_string(),
            )
        })?;

        // Default target: the env's current_version. That entry holds the
        // snapshot_map captured *before* the current state, so rolling back to
        // it undoes the most recent mutation. current_version == 0 means no
        // mutations have been recorded — nothing to roll back to.
        let target_version = match to_version {
            Some(v) => v,
            None => {
                let current = self.get(env_name)?.current_version;
                if current == 0 {
                    return Err(ConduitError::ConfigError(format!(
                        "No prior history version to roll back to for env '{}'",
                        env_name
                    )));
                }
                current
            }
        };

        let restored = store.get(env_name, target_version)?;

        let mut envs = self
            .environments
            .write()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?;

        let env = envs
            .get_mut(env_name)
            .ok_or_else(|| ConduitError::EnvironmentNotFound(env_name.to_string()))?;

        // Capture the current map as a new history entry so the rollback is reversible.
        let next = store.next_version(env_name)?;
        let pre_rollback = EnvSnapshotMapVersion {
            version: next,
            env_id: env_name.to_string(),
            captured_at: Utc::now(),
            reason: EnvHistoryReason::Rollback {
                from_version: env.current_version,
            },
            snapshot_map: env.snapshot_map.clone(),
        };
        store.record(&pre_rollback)?;

        let changes = {
            // diff_count is symmetric, count differences between old and new map
            let dummy_other = Environment {
                id: env_name.to_string(),
                snapshot_map: restored.snapshot_map.clone(),
                updated_at: Utc::now(),
                based_on: None,
                current_version: 0,
                promotion_policy: Default::default(),
            };
            env.diff_count(&dummy_other) as u32
        };

        env.snapshot_map = restored.snapshot_map;
        env.updated_at = Utc::now();
        env.current_version = next;

        info!(
            env = env_name,
            restored_from = target_version,
            new_version = next,
            changes = changes,
            "Environment rolled back"
        );

        Ok((next, changes))
    }

    /// History summaries for `env_name`, newest first. Empty if no history
    /// store is attached or no history has been recorded.
    pub fn history(&self, env_name: &str) -> ConduitResult<Vec<EnvHistorySummary>> {
        match &self.history {
            Some(store) => store.list_summaries(env_name),
            None => Ok(Vec::new()),
        }
    }

    /// Fetch a specific history version (with full snapshot_map).
    pub fn history_version(
        &self,
        env_name: &str,
        version: u32,
    ) -> ConduitResult<EnvSnapshotMapVersion> {
        let store = self.history.as_ref().ok_or_else(|| {
            ConduitError::ConfigError("No history store attached".to_string())
        })?;
        store.get(env_name, version)
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

    /// Delete an environment (cannot delete "production"). Also clears any
    /// recorded history for the env so recreating it later starts clean.
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

        if let Some(store) = &self.history {
            store.delete_for_env(name)?;
        }

        Ok(())
    }
}

impl Default for EnvironmentManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Age (in seconds) of the most recently created snapshot referenced by `env`,
/// or `None` if the env has no snapshot pointers. Snapshot IDs that can't be
/// looked up are skipped — a missing snapshot can't establish a younger lower
/// bound, so omitting it errs on the side of allowing the promotion.
fn newest_snapshot_age_secs(
    store: &SnapshotStore,
    env: &Environment,
) -> ConduitResult<Option<u64>> {
    let now = Utc::now();
    let mut newest: Option<chrono::DateTime<Utc>> = None;

    for snap_id in env.snapshot_map.values() {
        if let Ok(Some(snap)) = store.get(snap_id) {
            newest = Some(match newest {
                Some(prev) if prev >= snap.created_at => prev,
                _ => snap.created_at,
            });
        }
    }

    Ok(newest.map(|ts| {
        let delta = now.signed_duration_since(ts).num_seconds();
        if delta < 0 {
            0
        } else {
            delta as u64
        }
    }))
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

    fn put_snap(env: &mut Environment, dag: &str, task: &str, snap: &str) {
        env.snapshot_map
            .insert((dag.to_string(), task.to_string()), snap.to_string());
    }

    fn mgr_with_history() -> (EnvironmentManager, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EnvHistoryStore::open(dir.path()).unwrap();
        (EnvironmentManager::new().with_history_store(store), dir)
    }

    #[test]
    fn promote_captures_prior_target_state() {
        let (mgr, _dir) = mgr_with_history();
        mgr.create("staging", Some("production")).unwrap();

        // Mutate production directly so it has snapshot pointers to capture.
        {
            let mut envs = mgr.environments.write().unwrap();
            put_snap(envs.get_mut("production").unwrap(), "d", "t", "prod_v1");
            put_snap(envs.get_mut("staging").unwrap(), "d", "t", "staging_v1");
        }

        mgr.promote("staging", "production").unwrap();

        let prod = mgr.get("production").unwrap();
        assert_eq!(
            prod.snapshot_map
                .get(&("d".to_string(), "t".to_string()))
                .unwrap(),
            "staging_v1"
        );
        assert_eq!(prod.current_version, 1);

        let history = mgr.history("production").unwrap();
        assert_eq!(history.len(), 1);
        assert!(matches!(
            history[0].reason,
            EnvHistoryReason::Promotion { ref from } if from == "staging"
        ));

        // The captured version must carry the *pre-promotion* map.
        let v1 = mgr.history_version("production", 1).unwrap();
        assert_eq!(
            v1.snapshot_map
                .get(&("d".to_string(), "t".to_string()))
                .unwrap(),
            "prod_v1"
        );
    }

    #[test]
    fn rollback_restores_pre_promotion_state() {
        // Bet 1.3 acceptance: promote A->B, rollback B, B's snapshot_map equals
        // its pre-promotion state.
        let (mgr, _dir) = mgr_with_history();
        mgr.create("staging", Some("production")).unwrap();

        {
            let mut envs = mgr.environments.write().unwrap();
            put_snap(envs.get_mut("production").unwrap(), "d", "t", "prod_v1");
            put_snap(envs.get_mut("staging").unwrap(), "d", "t", "staging_v1");
        }

        mgr.promote("staging", "production").unwrap();
        let (new_version, changes) = mgr.rollback("production", None).unwrap();
        assert_eq!(new_version, 2);
        assert_eq!(changes, 1);

        let prod = mgr.get("production").unwrap();
        assert_eq!(
            prod.snapshot_map
                .get(&("d".to_string(), "t".to_string()))
                .unwrap(),
            "prod_v1"
        );

        // Rollback itself was captured, so history now has two entries.
        let hist = mgr.history("production").unwrap();
        assert_eq!(hist.len(), 2);
        assert!(matches!(hist[0].reason, EnvHistoryReason::Rollback { .. }));
    }

    #[test]
    fn rollback_to_specific_version() {
        let (mgr, _dir) = mgr_with_history();
        mgr.create("staging", Some("production")).unwrap();

        {
            let mut envs = mgr.environments.write().unwrap();
            put_snap(envs.get_mut("production").unwrap(), "d", "t", "v_a");
            put_snap(envs.get_mut("staging").unwrap(), "d", "t", "v_b");
        }
        mgr.promote("staging", "production").unwrap();

        {
            let mut envs = mgr.environments.write().unwrap();
            put_snap(envs.get_mut("staging").unwrap(), "d", "t", "v_c");
        }
        mgr.promote("staging", "production").unwrap();

        assert_eq!(mgr.get("production").unwrap().current_version, 2);

        // Roll back to version 1 — should restore pre-first-promotion (v_a).
        mgr.rollback("production", Some(1)).unwrap();
        assert_eq!(
            mgr.get("production")
                .unwrap()
                .snapshot_map
                .get(&("d".to_string(), "t".to_string()))
                .unwrap(),
            "v_a"
        );
    }

    #[test]
    fn rollback_without_history_errors() {
        let (mgr, _dir) = mgr_with_history();
        // No promotions yet → no history → rollback has nothing to restore.
        assert!(mgr.rollback("production", None).is_err());
    }

    #[test]
    fn apply_snapshot_map_captures_prior_state() {
        // Bet 5: applies must record env history so they are revertible via
        // `conduit env rollback`.
        let (mgr, _dir) = mgr_with_history();

        // Seed production with a pre-apply pointer.
        {
            let mut envs = mgr.environments.write().unwrap();
            put_snap(envs.get_mut("production").unwrap(), "etl", "load", "pre_v1");
        }

        let mut new_map = HashMap::new();
        new_map.insert(
            ("etl".to_string(), "load".to_string()),
            "post_v1".to_string(),
        );

        let captured = mgr
            .apply_snapshot_map("production", new_map.clone(), "plan_abc".to_string())
            .unwrap();
        assert_eq!(captured, Some(1));

        let prod = mgr.get("production").unwrap();
        assert_eq!(
            prod.snapshot_map
                .get(&("etl".to_string(), "load".to_string()))
                .unwrap(),
            "post_v1"
        );
        assert_eq!(prod.current_version, 1);

        let v1 = mgr.history_version("production", 1).unwrap();
        assert_eq!(
            v1.snapshot_map
                .get(&("etl".to_string(), "load".to_string()))
                .unwrap(),
            "pre_v1",
            "history entry must hold the pre-apply map, not the post-apply map"
        );
        assert!(matches!(
            v1.reason,
            EnvHistoryReason::Apply { ref plan_id } if plan_id == "plan_abc"
        ));
    }

    #[test]
    fn apply_then_rollback_round_trips() {
        // The whole point of recording apply history: rollback must restore the
        // pre-apply snapshot map.
        let (mgr, _dir) = mgr_with_history();

        {
            let mut envs = mgr.environments.write().unwrap();
            put_snap(envs.get_mut("production").unwrap(), "etl", "load", "v1");
            put_snap(
                envs.get_mut("production").unwrap(),
                "etl",
                "transform",
                "v1",
            );
        }

        let mut new_map = HashMap::new();
        new_map.insert(("etl".to_string(), "load".to_string()), "v2".to_string());
        new_map.insert(
            ("etl".to_string(), "transform".to_string()),
            "v2".to_string(),
        );

        mgr.apply_snapshot_map("production", new_map, "plan_xyz".to_string())
            .unwrap();

        // Rolling back should revert to "v1" for both tasks.
        let (_new_version, changes) = mgr.rollback("production", None).unwrap();
        assert_eq!(changes, 2);

        let prod = mgr.get("production").unwrap();
        assert_eq!(
            prod.snapshot_map
                .get(&("etl".to_string(), "load".to_string()))
                .unwrap(),
            "v1"
        );
        assert_eq!(
            prod.snapshot_map
                .get(&("etl".to_string(), "transform".to_string()))
                .unwrap(),
            "v1"
        );
    }

    #[test]
    fn apply_without_history_store_still_mutates() {
        // Apply must succeed even when no history store is attached — we just
        // can't roll back later.
        let mgr = EnvironmentManager::new();

        let mut new_map = HashMap::new();
        new_map.insert(("d".to_string(), "t".to_string()), "snap".to_string());

        let captured = mgr
            .apply_snapshot_map("production", new_map, "plan_no_hist".to_string())
            .unwrap();
        assert_eq!(captured, None, "no history store ⇒ no version captured");

        let prod = mgr.get("production").unwrap();
        assert_eq!(
            prod.snapshot_map
                .get(&("d".to_string(), "t".to_string()))
                .unwrap(),
            "snap"
        );
    }

    #[test]
    fn rollback_requires_history_store() {
        let mgr = EnvironmentManager::new();
        assert!(mgr.rollback("production", None).is_err());
    }

    #[test]
    fn promotion_policy_require_source_blocks_wrong_source() {
        let (mgr, _dir) = mgr_with_history();
        mgr.create("staging", Some("production")).unwrap();
        mgr.create("dev", Some("production")).unwrap();

        let policy = PromotionPolicy {
            require_source: Some("staging".to_string()),
            min_age_secs: None,
        };
        mgr.set_promotion_policy("production", policy).unwrap();

        let err = mgr.promote("dev", "production").unwrap_err();
        assert!(matches!(
            err,
            ConduitError::PromotionPolicyViolation(_)
        ));

        // The correct source still works.
        assert!(mgr.promote("staging", "production").is_ok());
    }

    #[test]
    fn promotion_policy_min_age_blocks_fresh_snapshots() {
        use conduit_common::fingerprint::Fingerprint;
        use conduit_common::snapshot::Snapshot;
        use std::collections::HashMap as Map;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let history = EnvHistoryStore::open(dir.path().join("history")).unwrap();
        let snapshots = Arc::new(SnapshotStore::new());
        let mgr = EnvironmentManager::new()
            .with_history_store(history)
            .with_snapshot_store(snapshots.clone());

        mgr.create("staging", Some("production")).unwrap();

        // Plant a freshly-created snapshot and point staging at it.
        snapshots
            .put(Snapshot {
                id: "snap_fresh".to_string(),
                fingerprint: Fingerprint::from_hex(&"a".repeat(64)),
                dag_id: "d".to_string(),
                task_id: "t".to_string(),
                created_at: Utc::now(),
                parent_fingerprints: vec![],
                metadata: Map::new(),
            })
            .unwrap();
        {
            let mut envs = mgr.environments.write().unwrap();
            put_snap(envs.get_mut("staging").unwrap(), "d", "t", "snap_fresh");
        }

        // Require snapshots be at least an hour old.
        mgr.set_promotion_policy(
            "production",
            PromotionPolicy {
                require_source: None,
                min_age_secs: Some(3600),
            },
        )
        .unwrap();

        let err = mgr.promote("staging", "production").unwrap_err();
        assert!(matches!(
            err,
            ConduitError::PromotionPolicyViolation(_)
        ));

        // Loosen the policy: zero-second min satisfied immediately.
        mgr.set_promotion_policy(
            "production",
            PromotionPolicy {
                require_source: None,
                min_age_secs: Some(0),
            },
        )
        .unwrap();
        assert!(mgr.promote("staging", "production").is_ok());
    }

    #[test]
    fn promotion_policy_min_age_without_snapshot_store_errors() {
        let (mgr, _dir) = mgr_with_history();
        mgr.create("staging", Some("production")).unwrap();
        mgr.set_promotion_policy(
            "production",
            PromotionPolicy {
                require_source: None,
                min_age_secs: Some(60),
            },
        )
        .unwrap();

        let err = mgr.promote("staging", "production").unwrap_err();
        assert!(matches!(
            err,
            ConduitError::PromotionPolicyViolation(_)
        ));
    }

    #[test]
    fn promotion_policy_min_age_empty_source_is_vacuously_allowed() {
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let history = EnvHistoryStore::open(dir.path().join("history")).unwrap();
        let snapshots = Arc::new(SnapshotStore::new());
        let mgr = EnvironmentManager::new()
            .with_history_store(history)
            .with_snapshot_store(snapshots);

        mgr.create("staging", Some("production")).unwrap();
        mgr.set_promotion_policy(
            "production",
            PromotionPolicy {
                require_source: None,
                min_age_secs: Some(3600),
            },
        )
        .unwrap();

        // Staging has no snapshots → no "newest snapshot age" to violate.
        assert!(mgr.promote("staging", "production").is_ok());
    }

    #[test]
    fn delete_clears_history() {
        let (mgr, _dir) = mgr_with_history();
        mgr.create("staging", Some("production")).unwrap();
        {
            let mut envs = mgr.environments.write().unwrap();
            put_snap(envs.get_mut("staging").unwrap(), "d", "t", "s1");
        }
        mgr.promote("staging", "production").unwrap();
        assert_eq!(mgr.history("production").unwrap().len(), 1);

        // Re-create production conceptually by deleting staging won't affect prod,
        // so delete staging and assert its history is gone.
        mgr.create("scratch", Some("production")).unwrap();
        mgr.promote("scratch", "production").unwrap();
        mgr.delete("scratch").unwrap();

        // history dir for scratch should be cleared
        let hist = mgr.history("scratch").unwrap();
        assert!(hist.is_empty());
    }
}
