//! Backfill and partition types for Conduit.
//!
//! A backfill runs a DAG across a range of dates or partitions, processing
//! each partition independently. This is the standard approach for:
//!
//! - **Historical reprocessing** — rebuild tables after a schema change
//! - **Late data catch-up** — process days that were missed due to outages
//! - **New pipeline onboarding** — populate the initial dataset
//!
//! Each partition gets its own logical date and environment variables,
//! so tasks can query the correct time window.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::incremental::PartitionGranularity;

// ─── Backfill request ───────────────────────────────────────────────────────

/// A backfill request — run a DAG across a range of dates/partitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillRequest {
    /// The DAG to backfill.
    pub dag_id: String,

    /// Start of the backfill range (inclusive).
    pub start_date: DateTime<Utc>,

    /// End of the backfill range (exclusive).
    pub end_date: DateTime<Utc>,

    /// How to split the range into partitions.
    pub granularity: PartitionGranularity,

    /// Target environment for execution.
    pub environment: String,

    /// Maximum number of partitions to run concurrently.
    pub max_concurrent_partitions: u32,

    /// If true, ignore watermarks and reprocess everything.
    pub full_refresh: bool,

    /// If true, just show what would run without executing.
    pub dry_run: bool,
}

// ─── Backfill partition ─────────────────────────────────────────────────────

/// A single partition in a backfill.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillPartition {
    /// Human-readable partition key (e.g., "2026-03-01", "2026-03-01T06").
    pub partition_key: String,

    /// Start of this partition's time window (inclusive).
    pub logical_start: DateTime<Utc>,

    /// End of this partition's time window (exclusive).
    pub logical_end: DateTime<Utc>,

    /// Current execution status.
    pub status: PartitionStatus,
}

/// Execution status of a single backfill partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStatus {
    /// Not yet started.
    Pending,

    /// Currently executing.
    Running,

    /// Completed successfully.
    Success {
        /// Wall-clock execution time in milliseconds.
        duration_ms: u64,
    },

    /// Execution failed.
    Failed {
        /// The error message.
        error: String,
        /// Which attempt this failure occurred on.
        attempt: u32,
    },

    /// Partition was skipped (e.g., already processed, or dry-run).
    Skipped {
        /// Why the partition was skipped.
        reason: String,
    },
}

// ─── Backfill result ────────────────────────────────────────────────────────

/// The result of a completed backfill operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillResult {
    /// The DAG that was backfilled.
    pub dag_id: String,

    /// Total number of partitions in the range.
    pub total_partitions: usize,

    /// Number of partitions that succeeded.
    pub succeeded: usize,

    /// Number of partitions that failed.
    pub failed: usize,

    /// Number of partitions that were skipped.
    pub skipped: usize,

    /// Total wall-clock duration of the backfill in milliseconds.
    pub total_duration_ms: u64,

    /// Detailed per-partition results.
    pub partitions: Vec<BackfillPartition>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backfill_request_round_trip() {
        let request = BackfillRequest {
            dag_id: "daily_etl".to_string(),
            start_date: "2026-01-01T00:00:00Z".parse().unwrap(),
            end_date: "2026-03-01T00:00:00Z".parse().unwrap(),
            granularity: PartitionGranularity::Day,
            environment: "production".to_string(),
            max_concurrent_partitions: 4,
            full_refresh: false,
            dry_run: true,
        };

        let json = serde_json::to_string(&request).unwrap();
        let parsed: BackfillRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.dag_id, "daily_etl");
        assert_eq!(parsed.max_concurrent_partitions, 4);
        assert!(parsed.dry_run);
    }

    #[test]
    fn partition_status_variants() {
        let pending = PartitionStatus::Pending;
        let json = serde_json::to_string(&pending).unwrap();
        assert!(json.contains("Pending"));

        let success = PartitionStatus::Success { duration_ms: 1234 };
        let json = serde_json::to_string(&success).unwrap();
        assert!(json.contains("1234"));

        let failed = PartitionStatus::Failed {
            error: "connection refused".to_string(),
            attempt: 3,
        };
        let json = serde_json::to_string(&failed).unwrap();
        assert!(json.contains("connection refused"));

        let skipped = PartitionStatus::Skipped {
            reason: "already processed".to_string(),
        };
        let json = serde_json::to_string(&skipped).unwrap();
        assert!(json.contains("already processed"));
    }

    #[test]
    fn backfill_result_summary() {
        let result = BackfillResult {
            dag_id: "etl".to_string(),
            total_partitions: 10,
            succeeded: 8,
            failed: 1,
            skipped: 1,
            total_duration_ms: 45000,
            partitions: vec![],
        };

        assert_eq!(
            result.succeeded + result.failed + result.skipped,
            result.total_partitions
        );
    }
}
