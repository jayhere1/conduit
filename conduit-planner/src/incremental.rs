//! Incremental computation engine.
//!
//! Determines what incremental work a task needs to do based on its strategy,
//! current watermark, and the time window being processed. This module:
//!
//! 1. Resolves the effective watermark for a task
//! 2. Computes the incremental context (env vars injected into the task process)
//! 3. Determines whether a full refresh is needed (first run, schema change, forced)
//! 4. Advances watermarks after successful execution
//! 5. Computes partition ranges for delete+insert strategies
//! 6. Handles lookback windows for late-arriving data

use std::collections::HashMap;
use std::sync::RwLock;

use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};

use conduit_common::error::{ConduitError, ConduitResult};
use conduit_common::incremental::*;

// ─── Watermark Store ────────────────────────────────────────────────────────

/// Persistent store for watermarks. JSON-serializable for v0.1,
/// will be backed by the event store in v0.2.
pub struct WatermarkStore {
    watermarks: RwLock<HashMap<(String, String), Watermark>>,
}

impl WatermarkStore {
    pub fn new() -> Self {
        Self {
            watermarks: RwLock::new(HashMap::new()),
        }
    }

    /// Load watermarks from a JSON file.
    pub fn from_file(path: &std::path::Path) -> ConduitResult<Self> {
        let data = std::fs::read_to_string(path)
            .map_err(|e| ConduitError::ConfigError(format!("Failed to read watermarks: {}", e)))?;

        let list: Vec<Watermark> = serde_json::from_str(&data)
            .map_err(|e| ConduitError::ConfigError(format!("Failed to parse watermarks: {}", e)))?;

        let mut map = HashMap::new();
        for wm in list {
            map.insert((wm.dag_id.clone(), wm.task_id.clone()), wm);
        }

        Ok(Self {
            watermarks: RwLock::new(map),
        })
    }

    /// Save watermarks to a JSON file.
    pub fn save_to_file(&self, path: &std::path::Path) -> ConduitResult<()> {
        let wms = self
            .watermarks
            .read()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?;

        let list: Vec<&Watermark> = wms.values().collect();
        let data = serde_json::to_string_pretty(&list)?;
        std::fs::write(path, data)
            .map_err(|e| ConduitError::ConfigError(format!("Failed to write watermarks: {}", e)))?;
        Ok(())
    }

    /// Get the current watermark for a task.
    pub fn get(&self, dag_id: &str, task_id: &str) -> Option<Watermark> {
        self.watermarks
            .read()
            .ok()
            .and_then(|wms| wms.get(&(dag_id.to_string(), task_id.to_string())).cloned())
    }

    /// Set or update a watermark.
    pub fn set(&self, watermark: Watermark) -> ConduitResult<()> {
        self.watermarks
            .write()
            .map_err(|e| ConduitError::EventStoreError(format!("Lock error: {}", e)))?
            .insert(
                (watermark.dag_id.clone(), watermark.task_id.clone()),
                watermark,
            );
        Ok(())
    }
}

impl Default for WatermarkStore {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Incremental Engine ─────────────────────────────────────────────────────

/// The incremental engine resolves what work a task needs to do.
pub struct IncrementalEngine;

impl IncrementalEngine {
    /// Build the incremental context for a task run.
    ///
    /// This is the main entry point. Given a task's incremental config and
    /// its current watermark, it produces an `IncrementalContext` that gets
    /// serialized into environment variables for the child process.
    pub fn build_context(
        config: &IncrementalConfig,
        watermark: Option<&Watermark>,
        force_full_refresh: bool,
        run_time: DateTime<Utc>,
    ) -> IncrementalContext {
        // Determine if this should be a full refresh
        let is_full_refresh = force_full_refresh
            || config.force_full_refresh
            || watermark.is_none_or(|w| w.is_initial());

        // If full refresh, return simple context
        if is_full_refresh {
            return IncrementalContext {
                is_full_refresh: true,
                watermark: WatermarkValue::Initial,
                effective_start: None,
                target_partitions: vec![],
                batch_size: config.batch_size,
            };
        }

        let wm = watermark.unwrap(); // Safe: is_initial check above

        match &config.strategy {
            IncrementalStrategy::FullRefresh => IncrementalContext {
                is_full_refresh: true,
                watermark: WatermarkValue::Initial,
                effective_start: None,
                target_partitions: vec![],
                batch_size: config.batch_size,
            },

            IncrementalStrategy::Append { lookback, .. } => {
                let effective_start = Self::apply_lookback(&wm.value, lookback.as_deref());
                IncrementalContext {
                    is_full_refresh: false,
                    watermark: wm.value.clone(),
                    effective_start,
                    target_partitions: vec![],
                    batch_size: config.batch_size,
                }
            }

            IncrementalStrategy::MergeOnKey { time_column, .. } => {
                let effective_start = if time_column.is_some() {
                    match &wm.value {
                        WatermarkValue::Timestamp(ts) => Some(ts.to_rfc3339()),
                        WatermarkValue::Sequence(seq) => Some(seq.to_string()),
                        _ => None,
                    }
                } else {
                    None
                };

                IncrementalContext {
                    is_full_refresh: false,
                    watermark: wm.value.clone(),
                    effective_start,
                    target_partitions: vec![],
                    batch_size: config.batch_size,
                }
            }

            IncrementalStrategy::DeleteInsert {
                partition_granularity,
                ..
            } => {
                let partitions = Self::compute_partitions(
                    &wm.value,
                    run_time,
                    partition_granularity,
                    config.max_partitions_per_run,
                );

                IncrementalContext {
                    is_full_refresh: false,
                    watermark: wm.value.clone(),
                    effective_start: None,
                    target_partitions: partitions,
                    batch_size: config.batch_size,
                }
            }

            IncrementalStrategy::SnapshotDiff { .. } => {
                // Snapshot diff always reads the full current state,
                // but only processes rows that changed since last snapshot.
                IncrementalContext {
                    is_full_refresh: false,
                    watermark: wm.value.clone(),
                    effective_start: None,
                    target_partitions: vec![],
                    batch_size: config.batch_size,
                }
            }
        }
    }

    /// After a successful task run, compute the new watermark value.
    ///
    /// The task can emit a watermark via stdout protocol:
    ///   CONDUIT::WATERMARK::2026-03-22T06:00:00Z
    /// If not, we use the run_time as the new watermark.
    pub fn advance_watermark(
        current: &mut Watermark,
        emitted_value: Option<&str>,
        run_time: DateTime<Utc>,
        run_id: &str,
    ) {
        if let Some(val) = emitted_value {
            // Try to parse as timestamp
            if let Ok(ts) = val.parse::<DateTime<Utc>>() {
                current.advance_timestamp(ts, run_id);
                return;
            }
            // Try as integer sequence
            if let Ok(seq) = val.parse::<i64>() {
                current.advance_sequence(seq, run_id);
                return;
            }
            // Fall back to partition string
            current.advance_partition(val, run_id);
        } else {
            // Default: advance to run time
            current.advance_timestamp(run_time, run_id);
        }
    }

    /// Rewrite a SQL query to add incremental filtering.
    ///
    /// Given a base query like `SELECT * FROM orders`, this produces:
    /// - Append: `SELECT * FROM orders WHERE created_at > '2026-03-21T06:00:00Z'`
    /// - MergeOnKey with time: adds WHERE clause on time column
    /// - DeleteInsert: adds WHERE clause on partition column
    ///
    /// This is a convenience for SQL tasks. Python tasks use the env vars directly.
    pub fn rewrite_sql(
        base_query: &str,
        config: &IncrementalConfig,
        context: &IncrementalContext,
    ) -> String {
        if context.is_full_refresh {
            return base_query.to_string();
        }

        let trimmed = base_query.trim().trim_end_matches(';');

        match &config.strategy {
            IncrementalStrategy::Append { time_column, .. } => {
                if let Some(start) = &context.effective_start {
                    format!("{} WHERE {} > '{}'", trimmed, time_column, start)
                } else {
                    match &context.watermark {
                        WatermarkValue::Timestamp(ts) => {
                            format!("{} WHERE {} > '{}'", trimmed, time_column, ts.to_rfc3339())
                        }
                        WatermarkValue::Sequence(seq) => {
                            format!("{} WHERE {} > {}", trimmed, time_column, seq)
                        }
                        _ => base_query.to_string(),
                    }
                }
            }

            IncrementalStrategy::MergeOnKey {
                time_column: Some(tc),
                ..
            } => {
                if let Some(start) = &context.effective_start {
                    format!("{} WHERE {} > '{}'", trimmed, tc, start)
                } else {
                    base_query.to_string()
                }
            }

            IncrementalStrategy::DeleteInsert {
                partition_column, ..
            } => {
                if !context.target_partitions.is_empty() {
                    let parts: Vec<String> = context
                        .target_partitions
                        .iter()
                        .map(|p| format!("'{}'", p))
                        .collect();
                    format!(
                        "{} WHERE {} IN ({})",
                        trimmed,
                        partition_column,
                        parts.join(", ")
                    )
                } else {
                    base_query.to_string()
                }
            }

            _ => base_query.to_string(),
        }
    }

    /// Apply a lookback window to a watermark value.
    /// Returns the effective start time as a string.
    fn apply_lookback(value: &WatermarkValue, lookback: Option<&str>) -> Option<String> {
        match value {
            WatermarkValue::Timestamp(ts) => {
                let adjusted = if let Some(lb) = lookback {
                    let duration = Self::parse_duration(lb);
                    *ts - duration
                } else {
                    *ts
                };
                Some(adjusted.to_rfc3339())
            }
            WatermarkValue::Sequence(seq) => Some(seq.to_string()),
            WatermarkValue::Partition(p) => Some(p.clone()),
            WatermarkValue::Initial => None,
        }
    }

    /// Compute partition keys between the watermark and run_time.
    fn compute_partitions(
        watermark: &WatermarkValue,
        run_time: DateTime<Utc>,
        granularity: &PartitionGranularity,
        max_partitions: Option<u32>,
    ) -> Vec<String> {
        let start_date = match watermark {
            WatermarkValue::Partition(p) => {
                NaiveDate::parse_from_str(p, "%Y-%m-%d").unwrap_or(run_time.date_naive())
            }
            WatermarkValue::Timestamp(ts) => ts.date_naive(),
            _ => run_time.date_naive() - Duration::days(1),
        };

        let end_date = run_time.date_naive();
        let max = max_partitions.unwrap_or(365) as usize;
        let mut partitions = Vec::new();
        let mut current = start_date;

        while current <= end_date && partitions.len() < max {
            partitions.push(current.format("%Y-%m-%d").to_string());

            current = match granularity {
                PartitionGranularity::Hour => current, // For hour granularity, we'd need time
                PartitionGranularity::Day => current + Duration::days(1),
                PartitionGranularity::Week => current + Duration::weeks(1),
                PartitionGranularity::Month => {
                    // Advance to first day of next month
                    if current.month() == 12 {
                        NaiveDate::from_ymd_opt(current.year() + 1, 1, 1)
                            .unwrap_or(current + Duration::days(31))
                    } else {
                        NaiveDate::from_ymd_opt(current.year(), current.month() + 1, 1)
                            .unwrap_or(current + Duration::days(31))
                    }
                }
                PartitionGranularity::Year => NaiveDate::from_ymd_opt(current.year() + 1, 1, 1)
                    .unwrap_or(current + Duration::days(366)),
            };
        }

        partitions
    }

    /// Parse a duration string like "2h", "30m", "1d" into a chrono::Duration.
    fn parse_duration(s: &str) -> Duration {
        let s = s.trim();
        if s.len() < 2 {
            return Duration::zero();
        }

        // Check multi-char suffixes first
        if let Some(num_str) = s.strip_suffix("ms") {
            if let Ok(n) = num_str.parse::<i64>() {
                return Duration::milliseconds(n);
            }
        }

        let (num_str, unit) = s.split_at(s.len() - 1);
        let num: i64 = num_str.parse().unwrap_or(0);

        match unit {
            "s" => Duration::seconds(num),
            "m" => Duration::minutes(num),
            "h" => Duration::hours(num),
            "d" => Duration::days(num),
            "w" => Duration::weeks(num),
            _ => Duration::zero(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(strategy: IncrementalStrategy) -> IncrementalConfig {
        IncrementalConfig {
            strategy,
            allow_full_refresh: true,
            force_full_refresh: false,
            batch_size: Some(5000),
            max_partitions_per_run: None,
            emit_watermark: false,
        }
    }

    #[test]
    fn first_run_is_always_full_refresh() {
        let config = make_config(IncrementalStrategy::Append {
            time_column: "created_at".to_string(),
            lookback: None,
        });

        let ctx = IncrementalEngine::build_context(&config, None, false, Utc::now());
        assert!(ctx.is_full_refresh);
    }

    #[test]
    fn append_strategy_with_watermark() {
        let config = make_config(IncrementalStrategy::Append {
            time_column: "created_at".to_string(),
            lookback: Some("2h".to_string()),
        });

        let ts: DateTime<Utc> = "2026-03-21T06:00:00Z".parse().unwrap();
        let mut wm = Watermark::new("etl", "extract");
        wm.advance_timestamp(ts, "run_001");

        let ctx = IncrementalEngine::build_context(&config, Some(&wm), false, Utc::now());

        assert!(!ctx.is_full_refresh);
        assert_eq!(ctx.batch_size, Some(5000));
        // Effective start should be 2h before the watermark
        let start = ctx.effective_start.unwrap();
        assert!(start.contains("2026-03-21T04:00:00")); // 6am - 2h = 4am
    }

    #[test]
    fn forced_full_refresh_overrides_watermark() {
        let config = make_config(IncrementalStrategy::Append {
            time_column: "ts".to_string(),
            lookback: None,
        });

        let mut wm = Watermark::new("dag", "task");
        wm.advance_timestamp(Utc::now(), "run_001");

        let ctx = IncrementalEngine::build_context(&config, Some(&wm), true, Utc::now());
        assert!(ctx.is_full_refresh);
    }

    #[test]
    fn delete_insert_computes_partitions() {
        let config = make_config(IncrementalStrategy::DeleteInsert {
            partition_column: "dt".to_string(),
            partition_granularity: PartitionGranularity::Day,
            time_column: None,
        });

        let mut wm = Watermark::new("etl", "facts");
        wm.advance_partition("2026-03-19", "run_001");

        let run_time: DateTime<Utc> = "2026-03-22T12:00:00Z".parse().unwrap();
        let ctx = IncrementalEngine::build_context(&config, Some(&wm), false, run_time);

        assert!(!ctx.is_full_refresh);
        // Should have partitions from 2026-03-19 through 2026-03-22
        assert!(ctx.target_partitions.len() >= 4);
        assert!(ctx.target_partitions.contains(&"2026-03-19".to_string()));
        assert!(ctx.target_partitions.contains(&"2026-03-22".to_string()));
    }

    #[test]
    fn sql_rewrite_append() {
        let config = make_config(IncrementalStrategy::Append {
            time_column: "created_at".to_string(),
            lookback: None,
        });

        let ts: DateTime<Utc> = "2026-03-21T00:00:00Z".parse().unwrap();
        let ctx = IncrementalContext {
            is_full_refresh: false,
            watermark: WatermarkValue::Timestamp(ts),
            effective_start: Some(ts.to_rfc3339()),
            target_partitions: vec![],
            batch_size: None,
        };

        let result = IncrementalEngine::rewrite_sql("SELECT * FROM orders", &config, &ctx);

        assert!(result.contains("WHERE created_at >"));
        assert!(result.contains("2026-03-21"));
    }

    #[test]
    fn sql_rewrite_delete_insert_with_partitions() {
        let config = make_config(IncrementalStrategy::DeleteInsert {
            partition_column: "dt".to_string(),
            partition_granularity: PartitionGranularity::Day,
            time_column: None,
        });

        let ctx = IncrementalContext {
            is_full_refresh: false,
            watermark: WatermarkValue::Partition("2026-03-20".to_string()),
            effective_start: None,
            target_partitions: vec!["2026-03-21".to_string(), "2026-03-22".to_string()],
            batch_size: None,
        };

        let result = IncrementalEngine::rewrite_sql("SELECT * FROM events", &config, &ctx);

        assert!(result.contains("WHERE dt IN"));
        assert!(result.contains("'2026-03-21'"));
        assert!(result.contains("'2026-03-22'"));
    }

    #[test]
    fn sql_rewrite_full_refresh_passes_through() {
        let config = make_config(IncrementalStrategy::Append {
            time_column: "ts".to_string(),
            lookback: None,
        });

        let ctx = IncrementalContext {
            is_full_refresh: true,
            watermark: WatermarkValue::Initial,
            effective_start: None,
            target_partitions: vec![],
            batch_size: None,
        };

        let result = IncrementalEngine::rewrite_sql("SELECT * FROM orders", &config, &ctx);

        assert_eq!(result, "SELECT * FROM orders");
    }

    #[test]
    fn advance_watermark_from_emitted_timestamp() {
        let mut wm = Watermark::new("dag", "task");

        IncrementalEngine::advance_watermark(
            &mut wm,
            Some("2026-03-22T12:00:00Z"),
            Utc::now(),
            "run_002",
        );

        match &wm.value {
            WatermarkValue::Timestamp(ts) => {
                assert_eq!(ts.to_rfc3339(), "2026-03-22T12:00:00+00:00");
            }
            _ => panic!("Expected Timestamp"),
        }
    }

    #[test]
    fn advance_watermark_from_emitted_sequence() {
        let mut wm = Watermark::new("dag", "task");

        IncrementalEngine::advance_watermark(&mut wm, Some("42000"), Utc::now(), "run_003");

        match &wm.value {
            WatermarkValue::Sequence(seq) => assert_eq!(*seq, 42000),
            _ => panic!("Expected Sequence"),
        }
    }

    #[test]
    fn advance_watermark_fallback_to_run_time() {
        let mut wm = Watermark::new("dag", "task");
        let run_time: DateTime<Utc> = "2026-03-22T06:00:00Z".parse().unwrap();

        IncrementalEngine::advance_watermark(&mut wm, None, run_time, "run_004");

        match &wm.value {
            WatermarkValue::Timestamp(ts) => assert_eq!(*ts, run_time),
            _ => panic!("Expected Timestamp"),
        }
    }

    #[test]
    fn watermark_store_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("watermarks.json");

        let store = WatermarkStore::new();

        let mut wm = Watermark::new("etl", "extract");
        wm.advance_timestamp("2026-03-22T00:00:00Z".parse().unwrap(), "run_001");
        store.set(wm).unwrap();

        let mut wm2 = Watermark::new("etl", "transform");
        wm2.advance_sequence(50000, "run_001");
        store.set(wm2).unwrap();

        store.save_to_file(&path).unwrap();

        let loaded = WatermarkStore::from_file(&path).unwrap();
        let wm_loaded = loaded.get("etl", "extract").unwrap();
        match &wm_loaded.value {
            WatermarkValue::Timestamp(ts) => {
                assert_eq!(ts.to_rfc3339(), "2026-03-22T00:00:00+00:00");
            }
            _ => panic!("Expected Timestamp"),
        }

        let wm2_loaded = loaded.get("etl", "transform").unwrap();
        match &wm2_loaded.value {
            WatermarkValue::Sequence(seq) => assert_eq!(*seq, 50000),
            _ => panic!("Expected Sequence"),
        }
    }

    #[test]
    fn merge_on_key_with_time_column() {
        let config = make_config(IncrementalStrategy::MergeOnKey {
            unique_key: vec!["user_id".to_string()],
            time_column: Some("updated_at".to_string()),
            invalidate_hard_deletes: false,
        });

        let ts: DateTime<Utc> = "2026-03-20T00:00:00Z".parse().unwrap();
        let mut wm = Watermark::new("etl", "dim_users");
        wm.advance_timestamp(ts, "run_001");

        let ctx = IncrementalEngine::build_context(&config, Some(&wm), false, Utc::now());

        assert!(!ctx.is_full_refresh);
        let start = ctx.effective_start.unwrap();
        assert!(start.contains("2026-03-20"));
    }

    #[test]
    fn snapshot_diff_strategy() {
        let config = make_config(IncrementalStrategy::SnapshotDiff {
            unique_key: vec!["id".to_string()],
            check_columns: vec!["name".to_string(), "email".to_string()],
            scd_type_2: true,
            valid_from_column: None,
            valid_to_column: None,
        });

        let mut wm = Watermark::new("etl", "scd_customers");
        wm.advance_timestamp(Utc::now() - Duration::days(1), "run_001");

        let ctx = IncrementalEngine::build_context(&config, Some(&wm), false, Utc::now());

        assert!(!ctx.is_full_refresh);
        assert!(ctx.target_partitions.is_empty());
    }

    #[test]
    fn max_partitions_limits_output() {
        let config = IncrementalConfig {
            strategy: IncrementalStrategy::DeleteInsert {
                partition_column: "dt".to_string(),
                partition_granularity: PartitionGranularity::Day,
                time_column: None,
            },
            max_partitions_per_run: Some(3),
            ..IncrementalConfig::default()
        };

        let mut wm = Watermark::new("etl", "facts");
        wm.advance_partition("2026-01-01", "run_001");

        let run_time: DateTime<Utc> = "2026-03-22T12:00:00Z".parse().unwrap();
        let ctx = IncrementalEngine::build_context(&config, Some(&wm), false, run_time);

        // Should be capped at 3 partitions even though there are ~80 days
        assert_eq!(ctx.target_partitions.len(), 3);
    }

    #[test]
    fn parse_duration_variants() {
        assert_eq!(
            IncrementalEngine::parse_duration("30s"),
            Duration::seconds(30)
        );
        assert_eq!(
            IncrementalEngine::parse_duration("5m"),
            Duration::minutes(5)
        );
        assert_eq!(IncrementalEngine::parse_duration("2h"), Duration::hours(2));
        assert_eq!(IncrementalEngine::parse_duration("1d"), Duration::days(1));
        assert_eq!(IncrementalEngine::parse_duration("1w"), Duration::weeks(1));
        assert_eq!(
            IncrementalEngine::parse_duration("500ms"),
            Duration::milliseconds(500)
        );
    }
}
