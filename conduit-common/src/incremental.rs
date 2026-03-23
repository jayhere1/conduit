//! Incremental computation model types.
//!
//! Incremental models process only new or changed data instead of
//! recomputing entire datasets. This is the difference between scanning
//! 500M rows every run vs. processing only the 50K that changed today.
//!
//! Conduit supports four incremental strategies:
//!
//! 1. **Append** — insert new rows, never update existing ones (event logs, metrics)
//! 2. **MergeOnKey** — upsert: insert new rows, update existing ones by key (dimensions)
//! 3. **DeleteInsert** — delete matching partition, then insert fresh (time-partitioned facts)
//! 4. **Snapshot** — capture full state, compare with previous snapshot (SCD Type 2)
//!
//! Each strategy needs a watermark (high-water mark) to know where the last run ended.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─── Incremental strategy ───────────────────────────────────────────────────

/// How a task processes data incrementally.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "strategy")]
pub enum IncrementalStrategy {
    /// Full refresh — reprocess all data every run. This is the default
    /// and serves as a baseline (no incremental logic needed).
    FullRefresh,

    /// Append only — process rows where `time_column > last_watermark`.
    /// Never updates or deletes existing output rows.
    /// Best for: event logs, immutable fact tables, metrics.
    Append {
        /// The column used to track what's been processed (e.g., "created_at").
        time_column: String,
        /// How far back to look as a safety overlap (e.g., "2h", "1d").
        /// Handles late-arriving data.
        lookback: Option<String>,
    },

    /// Merge on key — upsert rows by matching on unique key columns.
    /// Inserts new rows, updates existing rows where the key matches.
    /// Best for: dimension tables, user profiles, slowly changing data.
    MergeOnKey {
        /// Columns that uniquely identify a row (composite key).
        unique_key: Vec<String>,
        /// The column used to determine which rows are new/changed.
        time_column: Option<String>,
        /// If true, mark deleted rows (soft delete) instead of removing them.
        invalidate_hard_deletes: bool,
    },

    /// Delete + insert — delete all rows in the affected partition(s),
    /// then insert fresh data. Simpler than merge, handles deletes naturally.
    /// Best for: time-partitioned fact tables, daily snapshots.
    DeleteInsert {
        /// Column to partition by (usually a date).
        partition_column: String,
        /// The granularity of partitions ("day", "hour", "month").
        partition_granularity: PartitionGranularity,
        /// Optional time column for watermark tracking within a partition.
        time_column: Option<String>,
    },

    /// Full snapshot comparison — load the complete current state,
    /// diff against the previous snapshot, emit inserts/updates/deletes.
    /// Enables SCD Type 2 (slowly changing dimensions with history).
    /// Best for: source systems without reliable timestamps.
    SnapshotDiff {
        /// Columns that uniquely identify a row.
        unique_key: Vec<String>,
        /// Columns to check for changes (if empty, check all non-key columns).
        check_columns: Vec<String>,
        /// If true, add valid_from/valid_to columns for SCD Type 2.
        scd_type_2: bool,
        /// Column name for valid_from (default: "valid_from").
        valid_from_column: Option<String>,
        /// Column name for valid_to (default: "valid_to").
        valid_to_column: Option<String>,
    },
}

impl Default for IncrementalStrategy {
    fn default() -> Self {
        Self::FullRefresh
    }
}

/// Partition granularity for delete+insert strategy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionGranularity {
    Hour,
    Day,
    Week,
    Month,
    Year,
}

// ─── Watermark ──────────────────────────────────────────────────────────────

/// A watermark tracks the processing frontier for an incremental task.
/// After a successful run, the watermark advances to indicate what's been processed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Watermark {
    /// The task this watermark belongs to.
    pub dag_id: String,
    pub task_id: String,

    /// The type of watermark value.
    pub value: WatermarkValue,

    /// When this watermark was last updated.
    pub updated_at: DateTime<Utc>,

    /// The run_id that last advanced this watermark.
    pub last_run_id: Option<String>,

    /// Additional metadata (e.g., partition info, row counts).
    pub metadata: HashMap<String, String>,
}

/// The actual watermark value — what "position" have we processed up to.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WatermarkValue {
    /// A timestamp-based watermark (most common).
    Timestamp(DateTime<Utc>),
    /// An integer sequence number (e.g., auto-increment ID, Kafka offset).
    Sequence(i64),
    /// A string partition key (e.g., "2026-03-22").
    Partition(String),
    /// No watermark yet — first run, process everything.
    Initial,
}

impl Watermark {
    /// Create a new initial watermark for a task.
    pub fn new(dag_id: impl Into<String>, task_id: impl Into<String>) -> Self {
        Self {
            dag_id: dag_id.into(),
            task_id: task_id.into(),
            value: WatermarkValue::Initial,
            updated_at: Utc::now(),
            last_run_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Advance the watermark to a new timestamp.
    pub fn advance_timestamp(&mut self, ts: DateTime<Utc>, run_id: &str) {
        self.value = WatermarkValue::Timestamp(ts);
        self.updated_at = Utc::now();
        self.last_run_id = Some(run_id.to_string());
    }

    /// Advance the watermark to a new sequence number.
    pub fn advance_sequence(&mut self, seq: i64, run_id: &str) {
        self.value = WatermarkValue::Sequence(seq);
        self.updated_at = Utc::now();
        self.last_run_id = Some(run_id.to_string());
    }

    /// Advance the watermark to a new partition key.
    pub fn advance_partition(&mut self, partition: impl Into<String>, run_id: &str) {
        self.value = WatermarkValue::Partition(partition.into());
        self.updated_at = Utc::now();
        self.last_run_id = Some(run_id.to_string());
    }

    /// Whether this is the first run (no data processed yet).
    pub fn is_initial(&self) -> bool {
        self.value == WatermarkValue::Initial
    }
}

// ─── Incremental config on tasks ────────────────────────────────────────────

/// Incremental configuration for a task. Attached to the Task struct
/// when the task uses incremental processing.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IncrementalConfig {
    /// The incremental strategy to use.
    pub strategy: IncrementalStrategy,

    /// Whether to allow a full refresh even for incremental tasks.
    /// A full refresh is triggered by `conduit run --full-refresh`
    /// or when the fingerprint changes (schema change).
    pub allow_full_refresh: bool,

    /// Force full refresh on the next run (one-shot flag).
    pub force_full_refresh: bool,

    /// Batch size for incremental processing (number of rows/records per batch).
    pub batch_size: Option<u64>,

    /// Maximum number of partitions to process in a single run
    /// (prevents runaway catch-up jobs).
    pub max_partitions_per_run: Option<u32>,

    /// If true, the task should emit the new watermark value on stdout
    /// using the CONDUIT:: protocol (e.g., "CONDUIT::WATERMARK::2026-03-22T00:00:00Z").
    pub emit_watermark: bool,
}

// ─── Incremental context passed to tasks at runtime ─────────────────────────

/// Runtime context injected into incremental tasks via environment variables.
/// The task uses this to construct its incremental query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalContext {
    /// Whether this is a full refresh (ignore watermark, process everything).
    pub is_full_refresh: bool,

    /// The current watermark (where to resume from).
    pub watermark: WatermarkValue,

    /// The lookback-adjusted watermark (watermark - lookback duration).
    pub effective_start: Option<String>,

    /// For partition-based strategies: which partitions to process.
    pub target_partitions: Vec<String>,

    /// Batch size hint.
    pub batch_size: Option<u64>,
}

impl IncrementalContext {
    /// Serialize to environment variables for the child process.
    pub fn to_env_vars(&self) -> Vec<(String, String)> {
        let mut vars = vec![
            ("CONDUIT_INCREMENTAL".to_string(), "true".to_string()),
            (
                "CONDUIT_FULL_REFRESH".to_string(),
                self.is_full_refresh.to_string(),
            ),
        ];

        match &self.watermark {
            WatermarkValue::Timestamp(ts) => {
                vars.push(("CONDUIT_WATERMARK_TYPE".to_string(), "timestamp".to_string()));
                vars.push(("CONDUIT_WATERMARK_VALUE".to_string(), ts.to_rfc3339()));
            }
            WatermarkValue::Sequence(seq) => {
                vars.push(("CONDUIT_WATERMARK_TYPE".to_string(), "sequence".to_string()));
                vars.push(("CONDUIT_WATERMARK_VALUE".to_string(), seq.to_string()));
            }
            WatermarkValue::Partition(p) => {
                vars.push(("CONDUIT_WATERMARK_TYPE".to_string(), "partition".to_string()));
                vars.push(("CONDUIT_WATERMARK_VALUE".to_string(), p.clone()));
            }
            WatermarkValue::Initial => {
                vars.push(("CONDUIT_WATERMARK_TYPE".to_string(), "initial".to_string()));
                vars.push(("CONDUIT_WATERMARK_VALUE".to_string(), String::new()));
            }
        }

        if let Some(start) = &self.effective_start {
            vars.push(("CONDUIT_EFFECTIVE_START".to_string(), start.clone()));
        }

        if !self.target_partitions.is_empty() {
            vars.push((
                "CONDUIT_TARGET_PARTITIONS".to_string(),
                self.target_partitions.join(","),
            ));
        }

        if let Some(bs) = self.batch_size {
            vars.push(("CONDUIT_BATCH_SIZE".to_string(), bs.to_string()));
        }

        vars
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_lifecycle() {
        let mut wm = Watermark::new("etl", "extract_orders");
        assert!(wm.is_initial());

        wm.advance_timestamp(
            "2026-03-22T00:00:00Z".parse().unwrap(),
            "run_001",
        );
        assert!(!wm.is_initial());
        assert_eq!(wm.last_run_id, Some("run_001".to_string()));

        match &wm.value {
            WatermarkValue::Timestamp(ts) => {
                assert_eq!(ts.to_rfc3339(), "2026-03-22T00:00:00+00:00");
            }
            _ => panic!("Expected Timestamp"),
        }
    }

    #[test]
    fn incremental_context_env_vars() {
        let ctx = IncrementalContext {
            is_full_refresh: false,
            watermark: WatermarkValue::Timestamp(
                "2026-03-21T06:00:00Z".parse().unwrap(),
            ),
            effective_start: Some("2026-03-21T04:00:00Z".to_string()),
            target_partitions: vec![],
            batch_size: Some(10000),
        };

        let vars = ctx.to_env_vars();
        let map: HashMap<String, String> = vars.into_iter().collect();

        assert_eq!(map["CONDUIT_INCREMENTAL"], "true");
        assert_eq!(map["CONDUIT_FULL_REFRESH"], "false");
        assert_eq!(map["CONDUIT_WATERMARK_TYPE"], "timestamp");
        assert_eq!(map["CONDUIT_BATCH_SIZE"], "10000");
        assert!(map["CONDUIT_WATERMARK_VALUE"].starts_with("2026-03-21"));
    }

    #[test]
    fn partition_context_env_vars() {
        let ctx = IncrementalContext {
            is_full_refresh: false,
            watermark: WatermarkValue::Partition("2026-03-20".to_string()),
            effective_start: None,
            target_partitions: vec![
                "2026-03-21".to_string(),
                "2026-03-22".to_string(),
            ],
            batch_size: None,
        };

        let vars = ctx.to_env_vars();
        let map: HashMap<String, String> = vars.into_iter().collect();

        assert_eq!(map["CONDUIT_TARGET_PARTITIONS"], "2026-03-21,2026-03-22");
        assert_eq!(map["CONDUIT_WATERMARK_TYPE"], "partition");
    }

    #[test]
    fn default_strategy_is_full_refresh() {
        let cfg = IncrementalConfig::default();
        assert_eq!(cfg.strategy, IncrementalStrategy::FullRefresh);
    }

    #[test]
    fn serde_round_trip() {
        let strategy = IncrementalStrategy::MergeOnKey {
            unique_key: vec!["user_id".to_string()],
            time_column: Some("updated_at".to_string()),
            invalidate_hard_deletes: true,
        };

        let json = serde_json::to_string(&strategy).unwrap();
        let parsed: IncrementalStrategy = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, strategy);
    }
}
