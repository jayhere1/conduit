//! Snapshot storage and fingerprint-based lookup.
//!
//! Snapshots are the immutable outputs of task executions.
//! The snapshot store enables fingerprint-based reuse: if a task's
//! fingerprint matches an existing snapshot, execution is skipped.

use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

use conduit_common::error::{ConduitError, ConduitResult};
use conduit_common::fingerprint::Fingerprint;
use conduit_common::snapshot::{Snapshot, SnapshotId};

/// In-memory snapshot store (v0.1). Will be backed by RocksDB in v0.2.
pub struct SnapshotStore {
    /// Snapshots by ID.
    snapshots: RwLock<HashMap<SnapshotId, Snapshot>>,
    /// Index: fingerprint -> snapshot ID (for reuse lookups).
    fingerprint_index: RwLock<HashMap<Fingerprint, SnapshotId>>,
}

impl SnapshotStore {
    /// Create a new empty snapshot store.
    pub fn new() -> Self {
        Self {
            snapshots: RwLock::new(HashMap::new()),
            fingerprint_index: RwLock::new(HashMap::new()),
        }
    }

    /// Store a snapshot. Returns the snapshot ID.
    pub fn put(&self, snapshot: Snapshot) -> ConduitResult<SnapshotId> {
        let id = snapshot.id.clone();
        let fingerprint = snapshot.fingerprint.clone();

        self.snapshots
            .write()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?
            .insert(id.clone(), snapshot);

        self.fingerprint_index
            .write()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?
            .insert(fingerprint, id.clone());

        Ok(id)
    }

    /// Get a snapshot by ID.
    pub fn get(&self, id: &str) -> ConduitResult<Option<Snapshot>> {
        Ok(self
            .snapshots
            .read()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?
            .get(id)
            .cloned())
    }

    /// Look up a snapshot by fingerprint.
    /// This is the key operation for snapshot reuse: if a matching fingerprint exists,
    /// the task can be skipped entirely.
    pub fn find_by_fingerprint(&self, fingerprint: &Fingerprint) -> ConduitResult<Option<Snapshot>> {
        let index = self
            .fingerprint_index
            .read()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?;

        if let Some(id) = index.get(fingerprint) {
            return self.get(id);
        }

        Ok(None)
    }

    /// Check if a snapshot with this fingerprint exists (without loading it).
    pub fn has_fingerprint(&self, fingerprint: &Fingerprint) -> bool {
        self.fingerprint_index
            .read()
            .map(|idx| idx.contains_key(fingerprint))
            .unwrap_or(false)
    }

    /// Get the total number of stored snapshots.
    pub fn count(&self) -> usize {
        self.snapshots.read().map(|s| s.len()).unwrap_or(0)
    }

    /// Return all snapshots as a Vec (for serialization).
    pub fn list_all(&self) -> ConduitResult<Vec<Snapshot>> {
        Ok(self
            .snapshots
            .read()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?
            .values()
            .cloned()
            .collect())
    }

    /// Load snapshots from a JSON file on disk.
    ///
    /// The file should contain a JSON array of `Snapshot` objects
    /// (as produced by `save_to_file`). Both the snapshots map and
    /// the fingerprint index are rebuilt from the loaded data.
    pub fn from_file(path: &Path) -> ConduitResult<Self> {
        let data = std::fs::read_to_string(path).map_err(|e| {
            ConduitError::ConfigError(format!("Failed to read snapshots file: {}", e))
        })?;

        let snaps: Vec<Snapshot> = serde_json::from_str(&data).map_err(|e| {
            ConduitError::ConfigError(format!("Failed to parse snapshots file: {}", e))
        })?;

        let mut snapshots = HashMap::new();
        let mut fingerprint_index = HashMap::new();

        for snap in snaps {
            fingerprint_index.insert(snap.fingerprint.clone(), snap.id.clone());
            snapshots.insert(snap.id.clone(), snap);
        }

        tracing::info!(
            count = snapshots.len(),
            "Loaded snapshots from {}",
            path.display()
        );

        Ok(Self {
            snapshots: RwLock::new(snapshots),
            fingerprint_index: RwLock::new(fingerprint_index),
        })
    }

    /// Save all snapshots to a JSON file on disk.
    pub fn save_to_file(&self, path: &Path) -> ConduitResult<()> {
        let snaps = self.list_all()?;
        let data = serde_json::to_string_pretty(&snaps)?;
        std::fs::write(path, data).map_err(|e| {
            ConduitError::ConfigError(format!("Failed to write snapshots file: {}", e))
        })?;
        tracing::info!(
            count = snaps.len(),
            "Saved snapshots to {}",
            path.display()
        );
        Ok(())
    }
}

impl Default for SnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_common::fingerprint::Fingerprint;

    fn make_snapshot(id: &str, fp: &str) -> Snapshot {
        Snapshot {
            id: id.to_string(),
            fingerprint: Fingerprint::from_hex(fp),
            dag_id: "test_dag".to_string(),
            task_id: "test_task".to_string(),
            created_at: chrono::Utc::now(),
            parent_fingerprints: vec![],
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn store_and_retrieve() {
        let store = SnapshotStore::new();
        let snap = make_snapshot("snap_001", "abc123");

        store.put(snap).unwrap();

        let retrieved = store.get("snap_001").unwrap().unwrap();
        assert_eq!(retrieved.id, "snap_001");
    }

    #[test]
    fn list_all_snapshots() {
        let store = SnapshotStore::new();
        store.put(make_snapshot("snap_001", "abc123")).unwrap();
        store.put(make_snapshot("snap_002", "def456")).unwrap();

        let all = store.list_all().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn save_and_load_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshots.json");

        // Create and populate a store
        let store = SnapshotStore::new();
        store.put(make_snapshot("snap_001", "abc123")).unwrap();
        store.put(make_snapshot("snap_002", "def456")).unwrap();

        // Save to disk
        store.save_to_file(&path).unwrap();
        assert!(path.exists());

        // Load from disk
        let loaded = SnapshotStore::from_file(&path).unwrap();
        assert_eq!(loaded.count(), 2);

        // Verify data integrity
        let snap = loaded.get("snap_001").unwrap().unwrap();
        assert_eq!(snap.id, "snap_001");

        // Verify fingerprint index was rebuilt
        let found = loaded
            .find_by_fingerprint(&Fingerprint::from_hex("def456"))
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().id, "snap_002");
    }

    #[test]
    fn fingerprint_lookup() {
        let store = SnapshotStore::new();
        let snap = make_snapshot("snap_001", "abc123");

        store.put(snap).unwrap();

        let found = store
            .find_by_fingerprint(&Fingerprint::from_hex("abc123"))
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().id, "snap_001");

        let not_found = store
            .find_by_fingerprint(&Fingerprint::from_hex("xyz789"))
            .unwrap();
        assert!(not_found.is_none());
    }
}
