//! Backfill API handlers — compute and preview backfill partitions.
//!
//! POST /api/v1/backfill — accepts a BackfillRequest, returns computed partitions.
//! Currently operates in dry-run mode only (async execution through the API is complex).

use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use conduit_common::incremental::PartitionGranularity;
use conduit_planner::BackfillEngine;

use crate::error::ApiError;
use crate::AppState;

/// Request body for the backfill endpoint.
#[derive(Debug, Deserialize)]
pub struct BackfillApiRequest {
    /// The DAG ID to backfill.
    pub dag_id: String,

    /// Start date (inclusive), ISO 8601 date string (e.g., "2026-01-01").
    pub start_date: String,

    /// End date (exclusive), ISO 8601 date string (e.g., "2026-03-01").
    pub end_date: String,

    /// Partition granularity: "hour", "day", "week", "month", "year".
    #[serde(default = "default_granularity")]
    pub granularity: String,

    /// Target environment.
    #[serde(default = "default_environment")]
    pub environment: String,

    /// Maximum concurrent partitions.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_partitions: u32,

    /// Whether to do a full refresh (ignore watermarks).
    #[serde(default)]
    pub full_refresh: bool,
}

fn default_granularity() -> String {
    "day".to_string()
}

fn default_environment() -> String {
    "production".to_string()
}

fn default_max_concurrent() -> u32 {
    1
}

/// Response partition info.
#[derive(Debug, Serialize)]
pub struct PartitionInfo {
    pub partition_key: String,
    pub logical_start: String,
    pub logical_end: String,
    pub status: String,
}

/// POST /api/v1/backfill — compute backfill partitions (dry-run preview).
pub async fn create_backfill(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BackfillApiRequest>,
) -> Result<Json<Value>, ApiError> {
    // Parse granularity
    let granularity = match body.granularity.to_lowercase().as_str() {
        "hour" | "hourly" => PartitionGranularity::Hour,
        "day" | "daily" => PartitionGranularity::Day,
        "week" | "weekly" => PartitionGranularity::Week,
        "month" | "monthly" => PartitionGranularity::Month,
        "year" | "yearly" => PartitionGranularity::Year,
        other => {
            return Err(ApiError::BadRequest(format!(
                "Unknown granularity '{}'. Use: hour, day, week, month, year",
                other
            )));
        }
    };

    // Parse dates
    let start_date = parse_date(&body.start_date)
        .map_err(|e| ApiError::BadRequest(format!("Invalid start_date: {}", e)))?;
    let end_date = parse_date(&body.end_date)
        .map_err(|e| ApiError::BadRequest(format!("Invalid end_date: {}", e)))?;

    if end_date <= start_date {
        return Err(ApiError::BadRequest(
            "end_date must be after start_date".to_string(),
        ));
    }

    // Verify the DAG exists
    let (plan, _) = conduit_compiler::ConduitPlan::compile(&state.dags_path)
        .map_err(|e| ApiError::CompilationFailed(e.to_string()))?;

    if !plan.dags.contains_key(&body.dag_id) {
        return Err(ApiError::NotFound(format!(
            "DAG '{}' not found",
            body.dag_id
        )));
    }

    // Compute partitions
    let request = conduit_common::backfill::BackfillRequest {
        dag_id: body.dag_id.clone(),
        start_date,
        end_date,
        granularity,
        environment: body.environment.clone(),
        max_concurrent_partitions: body.max_concurrent_partitions,
        full_refresh: body.full_refresh,
        dry_run: true, // API always returns dry-run for now
    };

    let partitions = BackfillEngine::compute_partitions(&request);

    let partition_infos: Vec<PartitionInfo> = partitions
        .iter()
        .map(|p| PartitionInfo {
            partition_key: p.partition_key.clone(),
            logical_start: p.logical_start.to_rfc3339(),
            logical_end: p.logical_end.to_rfc3339(),
            status: "pending".to_string(),
        })
        .collect();

    let total = partition_infos.len();

    // Rough estimate: assume each partition takes ~30 seconds
    let estimated_seconds = total as u64 * 30;
    let estimated_duration = if estimated_seconds < 60 {
        format!("~{}s", estimated_seconds)
    } else if estimated_seconds < 3600 {
        format!("~{}m", estimated_seconds / 60)
    } else {
        format!(
            "~{}h {}m",
            estimated_seconds / 3600,
            (estimated_seconds % 3600) / 60
        )
    };

    Ok(Json(json!({
        "dag_id": body.dag_id,
        "partitions": partition_infos,
        "total": total,
        "estimated_duration": estimated_duration,
        "granularity": body.granularity,
        "start_date": body.start_date,
        "end_date": body.end_date,
        "full_refresh": body.full_refresh,
        "mode": "dry_run",
        "note": "Use 'conduit backfill' CLI command for actual execution",
    })))
}

/// Parse a date string like "2026-03-01" into a DateTime<Utc>.
fn parse_date(s: &str) -> Result<DateTime<Utc>, String> {
    // Try full ISO 8601 first
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        return Ok(dt);
    }

    // Try YYYY-MM-DD
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map(|nd| {
            let ndt = nd.and_time(NaiveTime::MIN);
            Utc.from_utc_datetime(&ndt)
        })
        .map_err(|e| format!("Expected YYYY-MM-DD or ISO 8601, got '{}': {}", s, e))
}
