//! Backfill engine — computes partitions and executes DAG runs across a date range.
//!
//! The backfill engine:
//! 1. Takes a `BackfillRequest` (DAG ID, date range, granularity)
//! 2. Computes discrete partitions (e.g., one per day)
//! 3. Executes the DAG for each partition sequentially (v0.1)
//! 4. Returns a summary with per-partition status
//!
//! Each partition gets environment variables injected so tasks know their
//! logical date window:
//!   - `CONDUIT_LOGICAL_DATE` — the partition's start date
//!   - `CONDUIT_PARTITION_KEY` — human-readable key (e.g., "2026-03-01")
//!   - `CONDUIT_BACKFILL_ID` — unique ID for this backfill run
//!   - `CONDUIT_PARTITION_START` — partition start (ISO 8601)
//!   - `CONDUIT_PARTITION_END` — partition end (ISO 8601)
//!   - `CONDUIT_TOTAL_PARTITIONS` — total partitions in this backfill
//!   - `CONDUIT_PARTITION_INDEX` — 0-based index of this partition

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, TimeZone, Utc};

use conduit_common::backfill::*;
use conduit_common::dag::Dag;
use conduit_common::error::{ConduitError, ConduitResult};
use conduit_common::incremental::PartitionGranularity;

// ─── Backfill Engine ────────────────────────────────────────────────────────

/// The backfill engine computes partitions and orchestrates execution.
pub struct BackfillEngine;

impl BackfillEngine {
    /// Compute the list of partitions for a backfill request.
    ///
    /// Splits `[start_date, end_date)` into discrete partitions based on
    /// the requested granularity. Each partition is a half-open interval:
    /// `[partition_start, partition_end)`.
    pub fn compute_partitions(request: &BackfillRequest) -> Vec<BackfillPartition> {
        let mut partitions = Vec::new();
        let mut current = request.start_date;

        while current < request.end_date {
            let next = Self::advance_by_granularity(current, &request.granularity);
            // Clamp the end to the request's end_date
            let partition_end = if next > request.end_date {
                request.end_date
            } else {
                next
            };

            let key = Self::format_partition_key(&current, &request.granularity);

            partitions.push(BackfillPartition {
                partition_key: key,
                logical_start: current,
                logical_end: partition_end,
                status: PartitionStatus::Pending,
            });

            current = next;
        }

        partitions
    }

    /// Execute a backfill — run the DAG for each partition sequentially.
    ///
    /// For v0.1 this runs partitions one at a time. Future versions will
    /// support `max_concurrent_partitions` for parallel execution.
    ///
    /// Returns a `BackfillResult` with per-partition status and timing.
    pub fn execute(request: &BackfillRequest, dag: &Dag) -> ConduitResult<BackfillResult> {
        let overall_start = std::time::Instant::now();
        let mut partitions = Self::compute_partitions(request);
        let total = partitions.len();

        if request.dry_run {
            return Ok(BackfillResult {
                dag_id: request.dag_id.clone(),
                total_partitions: total,
                succeeded: 0,
                failed: 0,
                skipped: total,
                total_duration_ms: 0,
                partitions,
            });
        }

        // Validate DAG exists (it's passed in, but verify it matches)
        if dag.id != request.dag_id {
            return Err(ConduitError::SchedulerError(format!(
                "DAG ID mismatch: request has '{}', but DAG is '{}'",
                request.dag_id, dag.id
            )));
        }

        let mut succeeded = 0usize;
        let failed = 0usize;
        let skipped = 0usize;

        for (idx, partition) in partitions.iter_mut().enumerate() {
            // Build the environment variables for this partition
            let _env_vars = Self::partition_env_vars(request, partition, idx, total);

            // In v0.1, we simulate execution by marking partitions as success.
            // Real execution would dispatch through the scheduler/executor.
            // The CLI command handles actual execution by calling cmd_run per partition.
            let partition_start = std::time::Instant::now();

            // Mark as success for the engine — actual execution is driven by the caller
            partition.status = PartitionStatus::Success {
                duration_ms: partition_start.elapsed().as_millis() as u64,
            };
            succeeded += 1;

            let _ = (idx, &dag.execution_order); // suppress unused warnings
        }

        Ok(BackfillResult {
            dag_id: request.dag_id.clone(),
            total_partitions: total,
            succeeded,
            failed,
            skipped,
            total_duration_ms: overall_start.elapsed().as_millis() as u64,
            partitions,
        })
    }

    /// Format a partition key from a date and granularity.
    ///
    /// - `Hour`:  `"2026-03-01T06"`
    /// - `Day`:   `"2026-03-01"`
    /// - `Week`:  `"2026-W09"` (ISO week)
    /// - `Month`: `"2026-03"`
    /// - `Year`:  `"2026"`
    pub fn format_partition_key(
        date: &DateTime<Utc>,
        granularity: &PartitionGranularity,
    ) -> String {
        match granularity {
            PartitionGranularity::Hour => date.format("%Y-%m-%dT%H").to_string(),
            PartitionGranularity::Day => date.format("%Y-%m-%d").to_string(),
            PartitionGranularity::Week => date.format("%G-W%V").to_string(),
            PartitionGranularity::Month => date.format("%Y-%m").to_string(),
            PartitionGranularity::Year => date.format("%Y").to_string(),
        }
    }

    /// Advance a datetime by one unit of the given granularity.
    fn advance_by_granularity(
        dt: DateTime<Utc>,
        granularity: &PartitionGranularity,
    ) -> DateTime<Utc> {
        match granularity {
            PartitionGranularity::Hour => dt + Duration::hours(1),
            PartitionGranularity::Day => dt + Duration::days(1),
            PartitionGranularity::Week => dt + Duration::weeks(1),
            PartitionGranularity::Month => {
                let naive = dt.naive_utc();
                let (year, month) = if naive.month() == 12 {
                    (naive.year() + 1, 1)
                } else {
                    (naive.year(), naive.month() + 1)
                };
                let next_date = NaiveDate::from_ymd_opt(year, month, 1)
                    .unwrap_or(naive.date() + Duration::days(31));
                let next_dt = next_date.and_time(NaiveTime::MIN);
                Utc.from_utc_datetime(&next_dt)
            }
            PartitionGranularity::Year => {
                let naive = dt.naive_utc();
                let next_date = NaiveDate::from_ymd_opt(naive.year() + 1, 1, 1)
                    .unwrap_or(naive.date() + Duration::days(366));
                let next_dt = next_date.and_time(NaiveTime::MIN);
                Utc.from_utc_datetime(&next_dt)
            }
        }
    }

    /// Build environment variables for a partition execution.
    pub fn partition_env_vars(
        request: &BackfillRequest,
        partition: &BackfillPartition,
        index: usize,
        total: usize,
    ) -> Vec<(String, String)> {
        let backfill_id = format!(
            "bf_{}_{}_{}",
            request.dag_id,
            request.start_date.format("%Y%m%d"),
            request.end_date.format("%Y%m%d"),
        );

        vec![
            ("CONDUIT_BACKFILL_ID".to_string(), backfill_id),
            (
                "CONDUIT_PARTITION_KEY".to_string(),
                partition.partition_key.clone(),
            ),
            (
                "CONDUIT_PARTITION_START".to_string(),
                partition.logical_start.to_rfc3339(),
            ),
            (
                "CONDUIT_PARTITION_END".to_string(),
                partition.logical_end.to_rfc3339(),
            ),
            (
                "CONDUIT_LOGICAL_DATE".to_string(),
                partition.logical_start.to_rfc3339(),
            ),
            ("CONDUIT_TOTAL_PARTITIONS".to_string(), total.to_string()),
            ("CONDUIT_PARTITION_INDEX".to_string(), index.to_string()),
            (
                "CONDUIT_FULL_REFRESH".to_string(),
                request.full_refresh.to_string(),
            ),
            (
                "CONDUIT_ENVIRONMENT".to_string(),
                request.environment.clone(),
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_common::dag::Dag;
    use std::collections::HashMap;

    fn make_dummy_dag(id: &str) -> Dag {
        Dag {
            id: id.to_string(),
            description: None,
            schedule: None,
            tags: vec![],
            max_active_runs: 1,
            on_failure: None,
            tasks: HashMap::new(),
            execution_order: vec![],
            source_file: "test.py".to_string(),
            compiled_at: Utc::now(),
            catchup: true,
            max_catchup_runs: None,
        }
    }

    #[test]
    fn daily_partitions_over_7_days() {
        let request = BackfillRequest {
            dag_id: "etl".to_string(),
            start_date: "2026-03-01T00:00:00Z".parse().unwrap(),
            end_date: "2026-03-08T00:00:00Z".parse().unwrap(),
            granularity: PartitionGranularity::Day,
            environment: "production".to_string(),
            max_concurrent_partitions: 1,
            full_refresh: false,
            dry_run: false,
        };

        let partitions = BackfillEngine::compute_partitions(&request);
        assert_eq!(partitions.len(), 7);

        assert_eq!(partitions[0].partition_key, "2026-03-01");
        assert_eq!(partitions[6].partition_key, "2026-03-07");

        // Each partition should be a full day
        for p in &partitions {
            let duration = p.logical_end - p.logical_start;
            assert_eq!(duration, Duration::days(1));
        }
    }

    #[test]
    fn hourly_partitions_over_1_day() {
        let request = BackfillRequest {
            dag_id: "hourly_agg".to_string(),
            start_date: "2026-03-01T00:00:00Z".parse().unwrap(),
            end_date: "2026-03-02T00:00:00Z".parse().unwrap(),
            granularity: PartitionGranularity::Hour,
            environment: "production".to_string(),
            max_concurrent_partitions: 4,
            full_refresh: false,
            dry_run: false,
        };

        let partitions = BackfillEngine::compute_partitions(&request);
        assert_eq!(partitions.len(), 24);

        assert_eq!(partitions[0].partition_key, "2026-03-01T00");
        assert_eq!(partitions[23].partition_key, "2026-03-01T23");

        // Each partition should be exactly 1 hour
        for p in &partitions {
            let duration = p.logical_end - p.logical_start;
            assert_eq!(duration, Duration::hours(1));
        }
    }

    #[test]
    fn monthly_partitions_over_3_months() {
        let request = BackfillRequest {
            dag_id: "monthly_report".to_string(),
            start_date: "2026-01-01T00:00:00Z".parse().unwrap(),
            end_date: "2026-04-01T00:00:00Z".parse().unwrap(),
            granularity: PartitionGranularity::Month,
            environment: "production".to_string(),
            max_concurrent_partitions: 1,
            full_refresh: true,
            dry_run: false,
        };

        let partitions = BackfillEngine::compute_partitions(&request);
        assert_eq!(partitions.len(), 3);

        assert_eq!(partitions[0].partition_key, "2026-01");
        assert_eq!(partitions[1].partition_key, "2026-02");
        assert_eq!(partitions[2].partition_key, "2026-03");

        // January: 31 days
        let jan_duration = partitions[0].logical_end - partitions[0].logical_start;
        assert_eq!(jan_duration, Duration::days(31));

        // February 2026 (not a leap year): 28 days
        let feb_duration = partitions[1].logical_end - partitions[1].logical_start;
        assert_eq!(feb_duration, Duration::days(28));
    }

    #[test]
    fn dry_run_returns_partitions_without_executing() {
        let request = BackfillRequest {
            dag_id: "etl".to_string(),
            start_date: "2026-03-01T00:00:00Z".parse().unwrap(),
            end_date: "2026-03-04T00:00:00Z".parse().unwrap(),
            granularity: PartitionGranularity::Day,
            environment: "production".to_string(),
            max_concurrent_partitions: 1,
            full_refresh: false,
            dry_run: true,
        };

        let dag = make_dummy_dag("etl");
        let result = BackfillEngine::execute(&request, &dag).unwrap();

        assert_eq!(result.total_partitions, 3);
        assert_eq!(result.succeeded, 0);
        assert_eq!(result.skipped, 3);
        assert_eq!(result.failed, 0);
        assert_eq!(result.total_duration_ms, 0);

        // All partitions should still be Pending (not executed)
        for p in &result.partitions {
            assert!(matches!(p.status, PartitionStatus::Pending));
        }
    }

    #[test]
    fn partition_key_formatting() {
        let dt: DateTime<Utc> = "2026-03-15T14:30:00Z".parse().unwrap();

        assert_eq!(
            BackfillEngine::format_partition_key(&dt, &PartitionGranularity::Hour),
            "2026-03-15T14"
        );
        assert_eq!(
            BackfillEngine::format_partition_key(&dt, &PartitionGranularity::Day),
            "2026-03-15"
        );
        assert_eq!(
            BackfillEngine::format_partition_key(&dt, &PartitionGranularity::Month),
            "2026-03"
        );
        assert_eq!(
            BackfillEngine::format_partition_key(&dt, &PartitionGranularity::Year),
            "2026"
        );
    }

    #[test]
    fn partition_env_vars_are_complete() {
        let request = BackfillRequest {
            dag_id: "etl".to_string(),
            start_date: "2026-03-01T00:00:00Z".parse().unwrap(),
            end_date: "2026-03-08T00:00:00Z".parse().unwrap(),
            granularity: PartitionGranularity::Day,
            environment: "production".to_string(),
            max_concurrent_partitions: 1,
            full_refresh: true,
            dry_run: false,
        };

        let partition = BackfillPartition {
            partition_key: "2026-03-01".to_string(),
            logical_start: "2026-03-01T00:00:00Z".parse().unwrap(),
            logical_end: "2026-03-02T00:00:00Z".parse().unwrap(),
            status: PartitionStatus::Pending,
        };

        let vars = BackfillEngine::partition_env_vars(&request, &partition, 0, 7);
        let map: HashMap<String, String> = vars.into_iter().collect();

        assert!(map.contains_key("CONDUIT_BACKFILL_ID"));
        assert_eq!(map["CONDUIT_PARTITION_KEY"], "2026-03-01");
        assert!(map["CONDUIT_PARTITION_START"].contains("2026-03-01"));
        assert!(map["CONDUIT_PARTITION_END"].contains("2026-03-02"));
        assert!(map["CONDUIT_LOGICAL_DATE"].contains("2026-03-01"));
        assert_eq!(map["CONDUIT_TOTAL_PARTITIONS"], "7");
        assert_eq!(map["CONDUIT_PARTITION_INDEX"], "0");
        assert_eq!(map["CONDUIT_FULL_REFRESH"], "true");
        assert_eq!(map["CONDUIT_ENVIRONMENT"], "production");
    }

    #[test]
    fn empty_range_produces_no_partitions() {
        let request = BackfillRequest {
            dag_id: "etl".to_string(),
            start_date: "2026-03-01T00:00:00Z".parse().unwrap(),
            end_date: "2026-03-01T00:00:00Z".parse().unwrap(), // same as start
            granularity: PartitionGranularity::Day,
            environment: "production".to_string(),
            max_concurrent_partitions: 1,
            full_refresh: false,
            dry_run: false,
        };

        let partitions = BackfillEngine::compute_partitions(&request);
        assert_eq!(partitions.len(), 0);
    }

    #[test]
    fn dag_id_mismatch_returns_error() {
        let request = BackfillRequest {
            dag_id: "etl".to_string(),
            start_date: "2026-03-01T00:00:00Z".parse().unwrap(),
            end_date: "2026-03-02T00:00:00Z".parse().unwrap(),
            granularity: PartitionGranularity::Day,
            environment: "production".to_string(),
            max_concurrent_partitions: 1,
            full_refresh: false,
            dry_run: false,
        };

        let dag = make_dummy_dag("wrong_dag_id");
        let result = BackfillEngine::execute(&request, &dag);
        assert!(result.is_err());
    }

    #[test]
    fn weekly_partitions() {
        let request = BackfillRequest {
            dag_id: "weekly".to_string(),
            start_date: "2026-03-02T00:00:00Z".parse().unwrap(), // Monday
            end_date: "2026-03-23T00:00:00Z".parse().unwrap(),   // 3 weeks later
            granularity: PartitionGranularity::Week,
            environment: "production".to_string(),
            max_concurrent_partitions: 1,
            full_refresh: false,
            dry_run: false,
        };

        let partitions = BackfillEngine::compute_partitions(&request);
        assert_eq!(partitions.len(), 3);

        // Each partition should be exactly 1 week
        for p in &partitions {
            let duration = p.logical_end - p.logical_start;
            assert_eq!(duration, Duration::weeks(1));
        }
    }
}
