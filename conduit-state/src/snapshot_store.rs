//! Snapshot storage and fingerprint-based lookup backed by RocksDB.
//!
//! Snapshots are the immutable outputs of task executions.
//! The snapshot store enables fingerprint-based reuse: if a task's
//! fingerprint matches an existing snapshot, execution is skipped.
//!
//! Uses two RocksDB column families:
//! - `snapshots`: snapshot_id -> serialized Snapshot
//! - `fingerprint_idx`: fingerprint hex -> snapshot_id

use std::path::{Path, PathBuf};

use conduit_common::error::{ConduitError, ConduitResult};
use conduit_common::fingerprint::Fingerprint;
use conduit_common::snapshot::{Snapshot, SnapshotId};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use tracing::info;

const CF_SNAPSHOTS: &str = "snapshots";
const CF_FINGERPRINT_IDX: &str = "fingerprint_idx";

/// Snapshot store backed by RocksDB with fingerprint-based lookup.
pub struct SnapshotStore {
    db: DB,
    /// When set, this temporary directory is cleaned up on drop.
    temp_dir: Option<PathBuf>,
}

impl Drop for SnapshotStore {
    fn drop(&mut self) {
        if let Some(ref path) = self.temp_dir {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

impl SnapshotStore {
    /// Create a new snapshot store using a temporary directory.
    /// Useful for tests and ephemeral usage. The directory is cleaned up on drop.
    pub fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("conduit-snapshots-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("failed to create temp dir for SnapshotStore");
        let db = Self::open_db(&dir).expect("failed to open RocksDB in temp dir for SnapshotStore");
        Self {
            db,
            temp_dir: Some(dir),
        }
    }

    /// Open or create a snapshot store at the given directory path.
    pub fn open(path: &Path) -> ConduitResult<Self> {
        let db = Self::open_db(path)?;
        let store = Self { db, temp_dir: None };

        info!(
            path = %path.display(),
            count = store.count(),
            "Snapshot store opened"
        );

        Ok(store)
    }

    /// Store a snapshot. Returns the snapshot ID.
    pub fn put(&self, snapshot: Snapshot) -> ConduitResult<SnapshotId> {
        let id = snapshot.id.clone();
        let fingerprint = snapshot.fingerprint.clone();

        let value = serde_json::to_vec(&snapshot)?;

        let cf_snapshots = self.cf_snapshots();
        let cf_fp_idx = self.cf_fingerprint_idx();

        self.db
            .put_cf(cf_snapshots, id.as_bytes(), &value)
            .map_err(|e| {
                ConduitError::EventStoreError(format!("Failed to put snapshot {}: {}", id, e))
            })?;

        self.db
            .put_cf(cf_fp_idx, fingerprint.0.as_bytes(), id.as_bytes())
            .map_err(|e| {
                ConduitError::EventStoreError(format!(
                    "Failed to put fingerprint index for {}: {}",
                    id, e
                ))
            })?;

        Ok(id)
    }

    /// Get a snapshot by ID.
    pub fn get(&self, id: &str) -> ConduitResult<Option<Snapshot>> {
        let cf = self.cf_snapshots();
        match self.db.get_cf(cf, id.as_bytes()) {
            Ok(Some(value)) => {
                let snapshot: Snapshot = serde_json::from_slice(&value)?;
                Ok(Some(snapshot))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(ConduitError::EventStoreError(format!(
                "Failed to read snapshot {}: {}",
                id, e
            ))),
        }
    }

    /// Look up a snapshot by fingerprint.
    /// This is the key operation for snapshot reuse: if a matching fingerprint exists,
    /// the task can be skipped entirely.
    pub fn find_by_fingerprint(
        &self,
        fingerprint: &Fingerprint,
    ) -> ConduitResult<Option<Snapshot>> {
        let cf = self.cf_fingerprint_idx();
        match self.db.get_cf(cf, fingerprint.0.as_bytes()) {
            Ok(Some(id_bytes)) => {
                let id = String::from_utf8(id_bytes.to_vec()).map_err(|e| {
                    ConduitError::EventStoreError(format!(
                        "Invalid UTF-8 in fingerprint index: {}",
                        e
                    ))
                })?;
                self.get(&id)
            }
            Ok(None) => Ok(None),
            Err(e) => Err(ConduitError::EventStoreError(format!(
                "Failed to read fingerprint index: {}",
                e
            ))),
        }
    }

    /// Check if a snapshot with this fingerprint exists (without loading it).
    pub fn has_fingerprint(&self, fingerprint: &Fingerprint) -> bool {
        let cf = self.cf_fingerprint_idx();
        self.db
            .get_cf(cf, fingerprint.0.as_bytes())
            .ok()
            .flatten()
            .is_some()
    }

    /// Get the total number of stored snapshots.
    pub fn count(&self) -> usize {
        let cf = self.cf_snapshots();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        let mut count = 0usize;
        for item in iter {
            if item.is_ok() {
                count += 1;
            }
        }
        count
    }

    /// Return all snapshots as a Vec.
    pub fn list_all(&self) -> ConduitResult<Vec<Snapshot>> {
        let cf = self.cf_snapshots();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        let mut snapshots = Vec::new();

        for item in iter {
            let (_key, value) =
                item.map_err(|e| ConduitError::EventStoreError(format!("Iterator error: {}", e)))?;
            let snapshot: Snapshot = serde_json::from_slice(&value)?;
            snapshots.push(snapshot);
        }

        Ok(snapshots)
    }

    // ─── Internal helpers ───────────────────────────────────────────────

    fn open_db(path: &Path) -> ConduitResult<DB> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_SNAPSHOTS, Options::default()),
            ColumnFamilyDescriptor::new(CF_FINGERPRINT_IDX, Options::default()),
        ];

        DB::open_cf_descriptors(&opts, path, cf_descriptors).map_err(|e| {
            ConduitError::EventStoreError(format!(
                "Failed to open RocksDB at {}: {}",
                path.display(),
                e
            ))
        })
    }

    fn cf_snapshots(&self) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle(CF_SNAPSHOTS)
            .expect("snapshots column family missing")
    }

    fn cf_fingerprint_idx(&self) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle(CF_FINGERPRINT_IDX)
            .expect("fingerprint_idx column family missing")
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
    use std::collections::HashMap;

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
    fn persistence_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot_db");

        {
            let store = SnapshotStore::open(&path).unwrap();
            store.put(make_snapshot("snap_001", "abc123")).unwrap();
            store.put(make_snapshot("snap_002", "def456")).unwrap();
        }

        let loaded = SnapshotStore::open(&path).unwrap();
        assert_eq!(loaded.count(), 2);

        let snap = loaded.get("snap_001").unwrap().unwrap();
        assert_eq!(snap.id, "snap_001");

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

    #[test]
    fn has_fingerprint_check() {
        let store = SnapshotStore::new();
        store.put(make_snapshot("snap_001", "abc123")).unwrap();

        assert!(store.has_fingerprint(&Fingerprint::from_hex("abc123")));
        assert!(!store.has_fingerprint(&Fingerprint::from_hex("xyz789")));
    }

    #[test]
    fn count_tracks_entries() {
        let store = SnapshotStore::new();
        assert_eq!(store.count(), 0);

        store.put(make_snapshot("snap_001", "abc123")).unwrap();
        assert_eq!(store.count(), 1);

        store.put(make_snapshot("snap_002", "def456")).unwrap();
        assert_eq!(store.count(), 2);
    }

    #[test]
    fn default_creates_working_store() {
        let store = SnapshotStore::default();
        store.put(make_snapshot("snap_001", "abc123")).unwrap();
        assert_eq!(store.count(), 1);
    }
}
