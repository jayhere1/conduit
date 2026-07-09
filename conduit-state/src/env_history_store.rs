//! Versioned history of environment snapshot maps.
//!
//! Each promote or rollback captures the prior `snapshot_map` of the target
//! env as a new version. Versions are written as one file per version under
//! `{root}/{env_id}/{version:06}.json`. Writes are atomic (temp + rename) so
//! a crash mid-write cannot corrupt the history log.
//!
//! See `docs/STRATEGIC_DIRECTION.md` §3 Bet 1.3 for the design rationale.

use std::path::{Path, PathBuf};

use conduit_common::error::{ConduitError, ConduitResult};
use conduit_common::snapshot::{EnvHistorySummary, EnvSnapshotMapVersion};

/// On-disk store of env history versions.
///
/// Cheap to clone — only holds the root path.
#[derive(Debug, Clone)]
pub struct EnvHistoryStore {
    root: PathBuf,
}

impl EnvHistoryStore {
    /// Open or create a history store rooted at `root`.
    /// The directory is created if it does not exist.
    pub fn open(root: impl Into<PathBuf>) -> ConduitResult<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(|e| {
            ConduitError::ConfigError(format!(
                "Failed to create env_history dir at {}: {}",
                root.display(),
                e
            ))
        })?;
        Ok(Self { root })
    }

    fn env_dir(&self, env_id: &str) -> PathBuf {
        self.root.join(env_id)
    }

    fn version_path(&self, env_id: &str, version: u32) -> PathBuf {
        self.env_dir(env_id).join(format!("{:06}.json", version))
    }

    /// The next version number to assign to a new history entry for `env_id`.
    /// Returns 1 for envs with no history.
    pub fn next_version(&self, env_id: &str) -> ConduitResult<u32> {
        let dir = self.env_dir(env_id);
        if !dir.exists() {
            return Ok(1);
        }
        let mut max: u32 = 0;
        for entry in std::fs::read_dir(&dir)
            .map_err(|e| ConduitError::ConfigError(format!("read_dir {}: {}", dir.display(), e)))?
        {
            let entry = entry.map_err(|e| ConduitError::ConfigError(e.to_string()))?;
            if let Some(stem) = entry.path().file_stem().and_then(|s| s.to_str()) {
                if let Ok(n) = stem.parse::<u32>() {
                    if n > max {
                        max = n;
                    }
                }
            }
        }
        Ok(max + 1)
    }

    /// Atomically write a version entry. The caller is responsible for setting
    /// `version` to the value returned by `next_version`.
    pub fn record(&self, entry: &EnvSnapshotMapVersion) -> ConduitResult<()> {
        let dir = self.env_dir(&entry.env_id);
        std::fs::create_dir_all(&dir).map_err(|e| {
            ConduitError::ConfigError(format!("create_dir_all {}: {}", dir.display(), e))
        })?;

        let final_path = self.version_path(&entry.env_id, entry.version);
        let tmp_path = final_path.with_extension("json.tmp");

        let data = serde_json::to_string_pretty(entry)
            .map_err(|e| ConduitError::ConfigError(format!("serialize history version: {}", e)))?;
        std::fs::write(&tmp_path, data).map_err(|e| {
            ConduitError::ConfigError(format!("write tmp {}: {}", tmp_path.display(), e))
        })?;
        std::fs::rename(&tmp_path, &final_path).map_err(|e| {
            ConduitError::ConfigError(format!(
                "rename {} -> {}: {}",
                tmp_path.display(),
                final_path.display(),
                e
            ))
        })?;
        Ok(())
    }

    /// Load a specific version. Errors if not found.
    pub fn get(&self, env_id: &str, version: u32) -> ConduitResult<EnvSnapshotMapVersion> {
        let path = self.version_path(env_id, version);
        let data = std::fs::read_to_string(&path).map_err(|_| {
            ConduitError::ConfigError(format!(
                "No history version {} for env '{}'",
                version, env_id
            ))
        })?;
        serde_json::from_str(&data)
            .map_err(|e| ConduitError::ConfigError(format!("parse {}: {}", path.display(), e)))
    }

    /// All history entries for `env_id`, newest first. Returns empty Vec if none.
    pub fn list(&self, env_id: &str) -> ConduitResult<Vec<EnvSnapshotMapVersion>> {
        let versions = self.collect_versions(env_id)?;
        let mut out = Vec::with_capacity(versions.len());
        for v in versions {
            out.push(self.get(env_id, v)?);
        }
        Ok(out)
    }

    /// Lightweight summaries for `env_id`, newest first.
    pub fn list_summaries(&self, env_id: &str) -> ConduitResult<Vec<EnvHistorySummary>> {
        Ok(self.list(env_id)?.iter().map(|v| v.summary()).collect())
    }

    /// Sorted (descending) list of known versions for `env_id`.
    fn collect_versions(&self, env_id: &str) -> ConduitResult<Vec<u32>> {
        let dir = self.env_dir(env_id);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&dir)
            .map_err(|e| ConduitError::ConfigError(format!("read_dir {}: {}", dir.display(), e)))?
        {
            let entry = entry.map_err(|e| ConduitError::ConfigError(e.to_string()))?;
            let path = entry.path();
            // Skip in-flight temp files written by `record`.
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if let Ok(n) = stem.parse::<u32>() {
                    out.push(n);
                }
            }
        }
        out.sort_unstable_by(|a, b| b.cmp(a));
        Ok(out)
    }

    /// Remove all history for `env_id`. Used when an env is deleted so
    /// recreating it later starts from a clean slate.
    pub fn delete_for_env(&self, env_id: &str) -> ConduitResult<()> {
        let dir = self.env_dir(env_id);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(|e| {
                ConduitError::ConfigError(format!("remove_dir_all {}: {}", dir.display(), e))
            })?;
        }
        Ok(())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use conduit_common::snapshot::{EnvHistoryReason, EnvSnapshotMapVersion};
    use std::collections::HashMap;

    fn make_entry(env_id: &str, version: u32) -> EnvSnapshotMapVersion {
        let mut snapshot_map = HashMap::new();
        snapshot_map.insert(("d".to_string(), "t".to_string()), "s1".to_string());
        EnvSnapshotMapVersion {
            version,
            env_id: env_id.to_string(),
            captured_at: Utc::now(),
            reason: EnvHistoryReason::Manual,
            snapshot_map,
        }
    }

    #[test]
    fn next_version_is_1_for_fresh_env() {
        let dir = tempfile::tempdir().unwrap();
        let store = EnvHistoryStore::open(dir.path()).unwrap();
        assert_eq!(store.next_version("staging").unwrap(), 1);
    }

    #[test]
    fn record_and_list_newest_first() {
        let dir = tempfile::tempdir().unwrap();
        let store = EnvHistoryStore::open(dir.path()).unwrap();

        for v in 1..=3 {
            store.record(&make_entry("staging", v)).unwrap();
        }

        let listed = store.list("staging").unwrap();
        let versions: Vec<u32> = listed.iter().map(|e| e.version).collect();
        assert_eq!(versions, vec![3, 2, 1]);

        assert_eq!(store.next_version("staging").unwrap(), 4);
    }

    #[test]
    fn list_summaries_omits_snapshot_map_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = EnvHistoryStore::open(dir.path()).unwrap();
        store.record(&make_entry("prod", 1)).unwrap();

        let summaries = store.list_summaries("prod").unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].snapshot_count, 1);
    }

    #[test]
    fn get_returns_specific_version() {
        let dir = tempfile::tempdir().unwrap();
        let store = EnvHistoryStore::open(dir.path()).unwrap();
        store.record(&make_entry("prod", 1)).unwrap();
        store.record(&make_entry("prod", 2)).unwrap();

        let v2 = store.get("prod", 2).unwrap();
        assert_eq!(v2.version, 2);
        assert!(store.get("prod", 99).is_err());
    }

    #[test]
    fn delete_for_env_clears_history() {
        let dir = tempfile::tempdir().unwrap();
        let store = EnvHistoryStore::open(dir.path()).unwrap();
        store.record(&make_entry("staging", 1)).unwrap();
        store.delete_for_env("staging").unwrap();
        assert!(store.list("staging").unwrap().is_empty());
        // next_version resets after deletion
        assert_eq!(store.next_version("staging").unwrap(), 1);
    }

    #[test]
    fn list_skips_tmp_files() {
        // Simulate a crashed write that left a .tmp behind.
        let dir = tempfile::tempdir().unwrap();
        let store = EnvHistoryStore::open(dir.path()).unwrap();
        store.record(&make_entry("staging", 1)).unwrap();

        let env_dir = store.env_dir("staging");
        std::fs::write(env_dir.join("000002.json.tmp"), b"garbage").unwrap();

        let listed = store.list("staging").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].version, 1);
    }
}
