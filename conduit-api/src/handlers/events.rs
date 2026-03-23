//! Event history and time-travel query handlers.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize)]
pub struct EventsQuery {
    /// Start sequence number (inclusive).
    pub from: Option<u64>,
    /// End sequence number (inclusive).
    pub to: Option<u64>,
    /// Maximum number of events to return.
    pub limit: Option<usize>,
    /// Filter by event type (e.g., "TaskCompleted").
    pub event_type: Option<String>,
}

/// GET /api/v1/events — list events from the event store.
///
/// Supports range queries for time-travel debugging:
///   /events?from=100&to=200  → events in sequence range
///   /events?event_type=TaskFailed → only failures
///   /events?limit=50 → most recent 50 events
pub async fn list_events(
    State(_state): State<Arc<AppState>>,
    Query(params): Query<EventsQuery>,
) -> Json<Value> {
    // In production, this would query the RocksDB event store.
    // For now, return the query parameters to show the API contract.

    let from = params.from.unwrap_or(0);
    let to = params.to.unwrap_or(u64::MAX);
    let limit = params.limit.unwrap_or(100);

    // Placeholder: in real implementation, we'd call:
    //   state.event_store.range(from, to)
    //   .filter(|e| params.event_type.as_ref().map_or(true, |t| matches_type(e, t)))
    //   .take(limit)
    //   .collect()

    Json(json!({
        "events": [],
        "query": {
            "from_sequence": from,
            "to_sequence": to,
            "limit": limit,
            "event_type_filter": params.event_type,
        },
        "total": 0,
        "note": "Event store query will be backed by RocksDB. Connect event store instance to enable.",
    }))
}

/// GET /api/v1/events/:sequence — get a specific event by sequence number.
///
/// This is the primitive for time-travel debugging: you can query
/// any historical state change by its sequence number.
pub async fn get_event(
    State(_state): State<Arc<AppState>>,
    Path(sequence): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    // In production: state.event_store.get(sequence)

    Err(ApiError::NotFound(format!(
        "Event with sequence {} not found (event store not connected)",
        sequence
    )))
}
