//! Snapshot and environment types.
//!
//! Snapshots are immutable records of task execution results.
//! Environments are collections of snapshot pointers — enabling
//! instant creation, promotion, and rollback (inspired by SQLMesh).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::dag::{DagId, TaskId};
use crate::fingerprint::Fingerprint;

/// Unique identifier for a snapshot.
pub type SnapshotId = String;

/// Unique identifier for an environment.
pub type EnvironmentId = String;

/// An immutable snapshot of a task execution result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Unique snapshot ID.
    pub id: SnapshotId,

    /// Content-addressable fingerprint (derived from task code + config + upstream).
    pub fingerprint: Fingerprint,

    /// Which DAG and task produced this snapshot.
    pub dag_id: DagId,
    pub task_id: TaskId,

    /// When this snapshot was created.
    pub created_at: DateTime<Utc>,

    /// Fingerprints of upstream snapshots that this was computed from.
    pub parent_fingerprints: Vec<Fingerprint>,

    /// Optional metadata (e.g., row count, output schema hash).
    pub metadata: HashMap<String, String>,
}

/// A virtual environment — a named set of snapshot pointers.
///
/// Creating an environment is O(1): just copy the pointer map.
/// Promoting is O(1): swap the pointer map.
/// Rollback is O(1): restore a previous pointer map from the event log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    /// Environment name (e.g., "production", "staging", "dev-jay").
    pub id: EnvironmentId,

    /// Map of (dag_id, task_id) -> snapshot_id.
    /// This is the "view layer" — it points at immutable snapshots.
    #[serde(with = "tuple_key_map")]
    pub snapshot_map: HashMap<(DagId, TaskId), SnapshotId>,

    /// When this environment was last modified.
    pub updated_at: DateTime<Utc>,

    /// Which environment this was forked from (if any).
    pub based_on: Option<EnvironmentId>,

    /// Monotonic version counter. Bumped by promote/rollback after a history
    /// entry is captured. Defaults to 0 for environments persisted before
    /// versioning existed.
    #[serde(default)]
    pub current_version: u32,

    /// Optional gates on promotions targeting this environment. Defaults to an
    /// empty policy that allows any source and any age.
    #[serde(default)]
    pub promotion_policy: PromotionPolicy,
}

/// Constraints applied to promotions targeting an environment.
///
/// The policy is checked on the *target* env. An empty policy (the default)
/// imposes no constraints.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromotionPolicy {
    /// When set, only promotions whose source matches this name are allowed.
    /// Use this to enforce flows like "production may only be promoted from staging".
    #[serde(default)]
    pub require_source: Option<EnvironmentId>,

    /// When set, the most recent snapshot in the *source* env must be at
    /// least this many seconds old. Implements a "bake time" before promotion.
    #[serde(default)]
    pub min_age_secs: Option<u64>,
}

impl PromotionPolicy {
    pub fn is_empty(&self) -> bool {
        self.require_source.is_none() && self.min_age_secs.is_none()
    }
}

impl Environment {
    /// Create a new empty environment.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            snapshot_map: HashMap::new(),
            updated_at: Utc::now(),
            based_on: None,
            current_version: 0,
            promotion_policy: PromotionPolicy::default(),
        }
    }

    /// Fork this environment — create a new one with the same snapshot pointers.
    /// This is an O(n) clone of the HashMap, but snapshots are not copied.
    /// The fork starts with an empty promotion policy; policies don't inherit.
    pub fn fork(&self, new_id: impl Into<String>) -> Self {
        Self {
            id: new_id.into(),
            snapshot_map: self.snapshot_map.clone(),
            updated_at: Utc::now(),
            based_on: Some(self.id.clone()),
            current_version: 0,
            promotion_policy: PromotionPolicy::default(),
        }
    }

    /// Promote this environment into another — overwrite target's snapshot map.
    pub fn promote_into(&self, target: &mut Environment) {
        target.snapshot_map = self.snapshot_map.clone();
        target.updated_at = Utc::now();
    }

    /// Count how many snapshots differ between this and another environment.
    pub fn diff_count(&self, other: &Environment) -> usize {
        let mut diff = 0;
        for (key, snap_id) in &self.snapshot_map {
            match other.snapshot_map.get(key) {
                Some(other_snap_id) if other_snap_id == snap_id => {}
                _ => diff += 1,
            }
        }
        // Also count entries in other that aren't in self
        for key in other.snapshot_map.keys() {
            if !self.snapshot_map.contains_key(key) {
                diff += 1;
            }
        }
        diff
    }

    /// Compute a structured diff between this environment (left) and another (right).
    ///
    /// `added` is keys present in `other` but not in `self`.
    /// `removed` is keys present in `self` but not in `other`.
    /// `changed` is keys present in both with different snapshot IDs.
    pub fn diff(&self, other: &Environment) -> EnvironmentDiff {
        let mut diff = EnvironmentDiff::default();

        for (key, snap_id) in &self.snapshot_map {
            match other.snapshot_map.get(key) {
                None => diff.removed.push(EnvDiffEntry {
                    dag_id: key.0.clone(),
                    task_id: key.1.clone(),
                    snapshot_id: snap_id.clone(),
                }),
                Some(other_snap_id) if other_snap_id != snap_id => {
                    diff.changed.push(EnvDiffChange {
                        dag_id: key.0.clone(),
                        task_id: key.1.clone(),
                        old_snapshot_id: snap_id.clone(),
                        new_snapshot_id: other_snap_id.clone(),
                    });
                }
                _ => {}
            }
        }

        for (key, snap_id) in &other.snapshot_map {
            if !self.snapshot_map.contains_key(key) {
                diff.added.push(EnvDiffEntry {
                    dag_id: key.0.clone(),
                    task_id: key.1.clone(),
                    snapshot_id: snap_id.clone(),
                });
            }
        }

        diff
    }
}

/// Structured diff between two environments.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentDiff {
    pub added: Vec<EnvDiffEntry>,
    pub removed: Vec<EnvDiffEntry>,
    pub changed: Vec<EnvDiffChange>,
}

impl EnvironmentDiff {
    /// Total number of differing entries.
    pub fn total(&self) -> usize {
        self.added.len() + self.removed.len() + self.changed.len()
    }

    /// True when there are no differences.
    pub fn is_empty(&self) -> bool {
        self.total() == 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvDiffEntry {
    pub dag_id: DagId,
    pub task_id: TaskId,
    pub snapshot_id: SnapshotId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvDiffChange {
    pub dag_id: DagId,
    pub task_id: TaskId,
    pub old_snapshot_id: SnapshotId,
    pub new_snapshot_id: SnapshotId,
}

/// Reason a history version was captured.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EnvHistoryReason {
    /// Captured because some other env was promoted into this one.
    Promotion { from: EnvironmentId },
    /// Captured because this env was rolled back to a prior version.
    Rollback { from_version: u32 },
    /// Captured before `conduit apply` rewrote the snapshot pointers, so the
    /// apply can be reverted with `conduit env rollback`. `plan_id` is the
    /// id of the DeploymentPlan that was applied.
    Apply { plan_id: String },
    /// Captured manually (e.g. via an explicit checkpoint).
    Manual,
}

/// A snapshot-in-time of an environment's snapshot_map, recorded before a
/// mutation (promote / rollback) so the prior state can be restored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvSnapshotMapVersion {
    pub version: u32,
    pub env_id: EnvironmentId,
    pub captured_at: DateTime<Utc>,
    pub reason: EnvHistoryReason,
    #[serde(with = "tuple_key_map")]
    pub snapshot_map: HashMap<(DagId, TaskId), SnapshotId>,
}

/// Lightweight summary of a history version — used for listings that don't
/// need to ship every snapshot pointer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvHistorySummary {
    pub version: u32,
    pub env_id: EnvironmentId,
    pub captured_at: DateTime<Utc>,
    pub reason: EnvHistoryReason,
    pub snapshot_count: usize,
}

impl EnvSnapshotMapVersion {
    pub fn summary(&self) -> EnvHistorySummary {
        EnvHistorySummary {
            version: self.version,
            env_id: self.env_id.clone(),
            captured_at: self.captured_at,
            reason: self.reason.clone(),
            snapshot_count: self.snapshot_map.len(),
        }
    }
}

/// Serde helper to serialize HashMap<(String, String), V> as HashMap<"key1/key2", V>.
mod tuple_key_map {
    use super::*;
    use serde::{Deserializer, Serializer};
    use std::collections::HashMap;

    pub fn serialize<S>(
        map: &HashMap<(DagId, TaskId), SnapshotId>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let mut ser_map = serializer.serialize_map(Some(map.len()))?;
        for ((dag_id, task_id), snap_id) in map {
            ser_map.serialize_entry(&format!("{}/{}", dag_id, task_id), snap_id)?;
        }
        ser_map.end()
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<(DagId, TaskId), SnapshotId>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string_map: HashMap<String, SnapshotId> = HashMap::deserialize(deserializer)?;
        let mut result = HashMap::new();
        for (key, value) in string_map {
            let parts: Vec<&str> = key.splitn(2, '/').collect();
            if parts.len() == 2 {
                result.insert((parts[0].to_string(), parts[1].to_string()), value);
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_creates_independent_copy() {
        let mut prod = Environment::new("production");
        prod.snapshot_map.insert(
            ("dag1".to_string(), "task1".to_string()),
            "snap_abc".to_string(),
        );

        let mut staging = prod.fork("staging");
        staging.snapshot_map.insert(
            ("dag1".to_string(), "task1".to_string()),
            "snap_xyz".to_string(),
        );

        // Production unchanged
        assert_eq!(
            prod.snapshot_map
                .get(&("dag1".to_string(), "task1".to_string())),
            Some(&"snap_abc".to_string())
        );
        // Staging has new snapshot
        assert_eq!(
            staging
                .snapshot_map
                .get(&("dag1".to_string(), "task1".to_string())),
            Some(&"snap_xyz".to_string())
        );
    }

    #[test]
    fn diff_count_detects_changes() {
        let mut env1 = Environment::new("env1");
        env1.snapshot_map
            .insert(("d".into(), "t1".into()), "s1".into());
        env1.snapshot_map
            .insert(("d".into(), "t2".into()), "s2".into());

        let mut env2 = env1.fork("env2");
        env2.snapshot_map
            .insert(("d".into(), "t2".into()), "s3".into());

        assert_eq!(env1.diff_count(&env2), 1);
    }

    #[test]
    fn diff_round_trip_matches_mutations() {
        let mut left = Environment::new("left");
        left.snapshot_map
            .insert(("d".into(), "kept".into()), "s_kept".into());
        left.snapshot_map
            .insert(("d".into(), "removed".into()), "s_removed".into());
        left.snapshot_map
            .insert(("d".into(), "changed".into()), "s_old".into());

        let mut right = left.fork("right");
        right
            .snapshot_map
            .remove(&("d".to_string(), "removed".to_string()));
        right.snapshot_map.insert(
            ("d".to_string(), "changed".to_string()),
            "s_new".to_string(),
        );
        right
            .snapshot_map
            .insert(("d".into(), "added".into()), "s_added".into());

        let diff = left.diff(&right);

        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].task_id, "added");
        assert_eq!(diff.added[0].snapshot_id, "s_added");

        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].task_id, "removed");
        assert_eq!(diff.removed[0].snapshot_id, "s_removed");

        assert_eq!(diff.changed.len(), 1);
        assert_eq!(diff.changed[0].task_id, "changed");
        assert_eq!(diff.changed[0].old_snapshot_id, "s_old");
        assert_eq!(diff.changed[0].new_snapshot_id, "s_new");

        assert_eq!(diff.total(), 3);
        assert_eq!(diff.total(), left.diff_count(&right));
    }

    #[test]
    fn diff_empty_when_identical() {
        let mut env = Environment::new("env");
        env.snapshot_map
            .insert(("d".into(), "t".into()), "s".into());
        let fork = env.fork("fork");
        assert!(env.diff(&fork).is_empty());
    }
}
