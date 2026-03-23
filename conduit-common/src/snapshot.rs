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
}

impl Environment {
    /// Create a new empty environment.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            snapshot_map: HashMap::new(),
            updated_at: Utc::now(),
            based_on: None,
        }
    }

    /// Fork this environment — create a new one with the same snapshot pointers.
    /// This is an O(n) clone of the HashMap, but snapshots are not copied.
    pub fn fork(&self, new_id: impl Into<String>) -> Self {
        Self {
            id: new_id.into(),
            snapshot_map: self.snapshot_map.clone(),
            updated_at: Utc::now(),
            based_on: Some(self.id.clone()),
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
            prod.snapshot_map.get(&("dag1".to_string(), "task1".to_string())),
            Some(&"snap_abc".to_string())
        );
        // Staging has new snapshot
        assert_eq!(
            staging.snapshot_map.get(&("dag1".to_string(), "task1".to_string())),
            Some(&"snap_xyz".to_string())
        );
    }

    #[test]
    fn diff_count_detects_changes() {
        let mut env1 = Environment::new("env1");
        env1.snapshot_map.insert(("d".into(), "t1".into()), "s1".into());
        env1.snapshot_map.insert(("d".into(), "t2".into()), "s2".into());

        let mut env2 = env1.fork("env2");
        env2.snapshot_map.insert(("d".into(), "t2".into()), "s3".into());

        assert_eq!(env1.diff_count(&env2), 1);
    }
}
