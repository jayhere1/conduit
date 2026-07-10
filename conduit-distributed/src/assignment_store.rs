//! Durable store for the coordinator's in-flight task assignments (PRD E3).
//!
//! The coordinator keeps its authoritative dispatch state in memory
//! (`inflight`). If the coordinator process restarts, that state is lost and
//! every task a worker is running becomes an orphan the new coordinator knows
//! nothing about. This store persists each dispatched assignment so a
//! restarted coordinator can reconstruct and re-queue in-flight work.
//!
//! The store is trait-based: [`InMemoryAssignmentStore`] is the default
//! (no durability, zero setup — the coordinator behaves exactly as before),
//! and [`RocksAssignmentStore`] persists to RocksDB. Both are exercised by the
//! same conformance test.

use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::proto_types::TaskAssignment;

/// A dispatched assignment, persisted until the task reaches a terminal state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedAssignment {
    pub assignment: TaskAssignment,
    /// The worker it was dispatched to (stale after a coordinator restart —
    /// recovery re-queues rather than trusting it).
    pub worker_id: String,
    /// The pool the task was submitted to.
    pub pool: String,
}

/// Persistence for in-flight assignments. Implementations must be safe to call
/// from the coordinator's async paths (they hold no `.await` points) and are
/// keyed by `assignment_id`.
pub trait AssignmentStore: Send + Sync {
    /// Record (or overwrite) a dispatched assignment.
    fn record(&self, entry: &PersistedAssignment);
    /// Remove an assignment that has reached a terminal state.
    fn remove(&self, assignment_id: &str);
    /// Load every persisted assignment (used once at coordinator startup).
    fn load_all(&self) -> Vec<PersistedAssignment>;
}

// ─── In-memory (default) ─────────────────────────────────────────────────────

/// Non-durable store. The coordinator uses this by default, preserving the
/// original behaviour: nothing survives a restart.
#[derive(Default)]
pub struct InMemoryAssignmentStore {
    entries: RwLock<HashMap<String, PersistedAssignment>>,
}

impl InMemoryAssignmentStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AssignmentStore for InMemoryAssignmentStore {
    fn record(&self, entry: &PersistedAssignment) {
        if let Ok(mut e) = self.entries.write() {
            e.insert(entry.assignment.assignment_id.clone(), entry.clone());
        }
    }

    fn remove(&self, assignment_id: &str) {
        if let Ok(mut e) = self.entries.write() {
            e.remove(assignment_id);
        }
    }

    fn load_all(&self) -> Vec<PersistedAssignment> {
        self.entries
            .read()
            .map(|e| e.values().cloned().collect())
            .unwrap_or_default()
    }
}

// ─── RocksDB-backed ──────────────────────────────────────────────────────────

/// Durable store backed by RocksDB. Keys are `assignment_id`, values are the
/// JSON-serialised [`PersistedAssignment`]. Writes are best-effort: a failed
/// persist logs and continues rather than blocking dispatch, since the
/// in-memory `inflight` map remains the live source of truth within one
/// coordinator lifetime — persistence only matters across a restart.
pub struct RocksAssignmentStore {
    db: rocksdb::DB,
}

impl RocksAssignmentStore {
    /// Open (creating if needed) the assignment store at `path`, e.g.
    /// `{state_dir}/coordinator_assignments`.
    pub fn open(path: &Path) -> Result<Self, rocksdb::Error> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open(&opts, path)?;
        Ok(Self { db })
    }
}

impl AssignmentStore for RocksAssignmentStore {
    fn record(&self, entry: &PersistedAssignment) {
        match serde_json::to_vec(entry) {
            Ok(bytes) => {
                if let Err(e) = self
                    .db
                    .put(entry.assignment.assignment_id.as_bytes(), bytes)
                {
                    tracing::warn!(error = %e, "failed to persist assignment");
                }
            }
            Err(e) => tracing::warn!(error = %e, "failed to serialize assignment"),
        }
    }

    fn remove(&self, assignment_id: &str) {
        if let Err(e) = self.db.delete(assignment_id.as_bytes()) {
            tracing::warn!(error = %e, "failed to delete persisted assignment");
        }
    }

    fn load_all(&self) -> Vec<PersistedAssignment> {
        let mut out = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            match item {
                Ok((_key, value)) => match serde_json::from_slice::<PersistedAssignment>(&value) {
                    Ok(entry) => out.push(entry),
                    Err(e) => tracing::warn!(error = %e, "skipping unparseable assignment"),
                },
                Err(e) => tracing::warn!(error = %e, "assignment store iteration error"),
            }
        }
        out
    }
}

// ─── Conformance tests (both backends) ───────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto_types::{ResourceLimits, TaskContext, TaskSpec, TaskType};

    fn make_entry(id: &str, worker: &str) -> PersistedAssignment {
        PersistedAssignment {
            assignment: TaskAssignment {
                assignment_id: id.to_string(),
                dag_id: "dag".into(),
                run_id: "run".into(),
                task_id: format!("task-{id}"),
                attempt: 0,
                spec: TaskSpec {
                    task_type: TaskType::Bash,
                    script: "true".into(),
                    connection: String::new(),
                    query: String::new(),
                    command: String::new(),
                    args: vec![],
                    timeout_secs: 60,
                    resources: ResourceLimits::default(),
                },
                context: TaskContext {
                    dag_id: "dag".into(),
                    run_id: "run".into(),
                    task_id: format!("task-{id}"),
                    attempt: 0,
                    logical_date_epoch_ms: 0,
                    environment: "test".into(),
                    params: HashMap::new(),
                },
                deadline_epoch_ms: 0,
            },
            worker_id: worker.to_string(),
            pool: "default".into(),
        }
    }

    fn conformance(store: &dyn AssignmentStore) {
        assert!(store.load_all().is_empty());

        store.record(&make_entry("a1", "w1"));
        store.record(&make_entry("a2", "w2"));
        assert_eq!(store.load_all().len(), 2);

        // Overwrite is idempotent by assignment_id.
        store.record(&make_entry("a1", "w3"));
        assert_eq!(store.load_all().len(), 2);

        store.remove("a1");
        let remaining = store.load_all();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].assignment.assignment_id, "a2");

        // Removing a non-existent key is a no-op.
        store.remove("nope");
        assert_eq!(store.load_all().len(), 1);
    }

    #[test]
    fn in_memory_conformance() {
        conformance(&InMemoryAssignmentStore::new());
    }

    #[test]
    fn rocks_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksAssignmentStore::open(dir.path()).unwrap();
        conformance(&store);
    }

    #[test]
    fn rocks_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = RocksAssignmentStore::open(dir.path()).unwrap();
            store.record(&make_entry("a1", "w1"));
            store.record(&make_entry("a2", "w2"));
        }
        // Reopen: the assignments persist across the "restart".
        let store = RocksAssignmentStore::open(dir.path()).unwrap();
        let loaded = store.load_all();
        assert_eq!(loaded.len(), 2);
    }
}
