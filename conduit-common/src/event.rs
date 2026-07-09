//! Event types for the append-only event store.
//!
//! Every state change in Forge is recorded as an immutable event.
//! This enables time-travel debugging, instant rollback, and audit trails.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::dag::{DagId, TaskId};
use crate::snapshot::SnapshotId;

/// Unique identifier for an event.
pub type EventId = Uuid;

/// An immutable event in the Forge event log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Unique event ID.
    pub id: EventId,

    /// Monotonically increasing sequence number.
    pub sequence: u64,

    /// When this event occurred.
    pub timestamp: DateTime<Utc>,

    /// The actual event data.
    pub kind: EventKind,
}

impl Event {
    /// Create a new event with the given kind.
    pub fn new(sequence: u64, kind: EventKind) -> Self {
        Self {
            id: Uuid::new_v4(),
            sequence,
            timestamp: Utc::now(),
            kind,
        }
    }
}

/// All possible event types in the Forge system.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EventKind {
    // ── DAG Run lifecycle ───────────────────────────────────
    DagRunCreated {
        dag_id: DagId,
        run_id: String,
        logical_date: DateTime<Utc>,
        environment: String,
        triggered_by: String,
    },
    DagRunCompleted {
        dag_id: DagId,
        run_id: String,
        status: RunStatus,
        duration_ms: u64,
    },

    // ── Task lifecycle ──────────────────────────────────────
    TaskQueued {
        dag_id: DagId,
        run_id: String,
        task_id: TaskId,
        priority: i32,
        pool: Option<String>,
        snapshot_fingerprint: Option<String>,
    },
    TaskStarted {
        dag_id: DagId,
        run_id: String,
        task_id: TaskId,
        worker_id: String,
        attempt: u32,
        pid: Option<u32>,
    },
    TaskCompleted {
        dag_id: DagId,
        run_id: String,
        task_id: TaskId,
        duration_ms: u64,
        snapshot_id: Option<SnapshotId>,
    },
    TaskFailed {
        dag_id: DagId,
        run_id: String,
        task_id: TaskId,
        error: String,
        traceback: Option<String>,
        attempt: u32,
    },
    TaskRetrying {
        dag_id: DagId,
        run_id: String,
        task_id: TaskId,
        attempt: u32,
        next_retry_at: DateTime<Utc>,
    },
    TaskSkipped {
        dag_id: DagId,
        run_id: String,
        task_id: TaskId,
        reason: String,
    },

    // ── Authentication audit ─────────────────────────────────
    /// Security-relevant auth events: failed authentications, role
    /// denials, and API-key lifecycle changes. Successful per-request
    /// authentications are NOT logged (too noisy — `last_used_at` on the
    /// key covers usage); key identity is present whenever known.
    AuthAudit {
        /// "auth_failed" | "permission_denied" | "key_created" | "key_revoked".
        action: String,
        /// The key involved, when known (None for failed authentications).
        key_id: Option<String>,
        /// Human-readable context: failure reason, denied path, key name/role.
        detail: String,
    },

    // ── Snapshot & Environment ───────────────────────────────
    SnapshotCreated {
        snapshot_id: SnapshotId,
        fingerprint: String,
        dag_id: DagId,
        task_id: TaskId,
    },
    EnvironmentCreated {
        env_name: String,
        based_on: Option<String>,
    },
    EnvironmentPromoted {
        source_env: String,
        target_env: String,
        snapshot_changes: u32,
    },
    EnvironmentRolledBack {
        env_name: String,
        rolled_back_to: u64, // sequence number
    },

    // ── Plan/Apply ──────────────────────────────────────────
    PlanCreated {
        plan_id: String,
        environment: String,
        changes_count: u32,
        breaking_changes: u32,
    },
    PlanApplied {
        plan_id: String,
        environment: String,
        tasks_executed: u32,
        tasks_skipped: u32,
    },
}

/// Status of a completed DAG run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RunStatus {
    Success,
    Failed,
    Cancelled,
}
