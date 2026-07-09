//! Event history and time-travel query handlers.
//!
//! Backs the UI's "Events" page and the per-run timeline. Pulls from the
//! persistent `EventStore` opened in `AppState::with_options`. When the
//! store is unavailable (fresh install, missing dir, open error), the
//! endpoints return empty results with a clear note rather than failing
//! the request — keeps the UI usable on a stateless dev box.
//!
//! Closes the Bet 3 item from `docs/STRATEGIC_DIRECTION.md`:
//! "Structured run logs queryable from the UI."

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::Json;
use conduit_common::event::{Event, EventKind};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize)]
pub struct EventsQuery {
    /// Start sequence number (inclusive). Defaults to 1 (oldest).
    pub from: Option<u64>,
    /// End sequence number (inclusive). Defaults to the current sequence.
    pub to: Option<u64>,
    /// Maximum number of events to return after filters apply. Defaults to
    /// 100 to keep the default response shape small.
    pub limit: Option<usize>,
    /// Filter to a single event type (e.g. `TaskFailed`, `DagRunCompleted`).
    /// Matches the variant name of `EventKind` (the serde tag).
    pub event_type: Option<String>,
    /// Filter to events belonging to a specific DAG run id.
    pub run_id: Option<String>,
    /// Filter to events for a specific DAG.
    pub dag_id: Option<String>,
    /// Filter to events for a specific task within a DAG run.
    pub task_id: Option<String>,
}

/// GET /api/v1/events — list events from the event store.
///
/// Examples:
///   /events?from=100&to=200             — sequence range
///   /events?event_type=TaskFailed       — only failures
///   /events?run_id=run_42&task_id=load  — task timeline within a run
///   /events?dag_id=etl&limit=50         — most recent 50 events for one DAG
pub async fn list_events(
    State(state): State<Arc<AppState>>,
    Query(params): Query<EventsQuery>,
) -> Json<Value> {
    let store = match &state.event_store {
        Some(s) => s,
        None => {
            return Json(json!({
                "events": [],
                "total": 0,
                "note": "Event store not initialized (no events directory found at state_dir/events).",
            }));
        }
    };

    let current = store.current_sequence();
    let from = params.from.unwrap_or(1);
    let to = params.to.unwrap_or(current).min(current);
    let limit = params.limit.unwrap_or(100);

    if from > to {
        return Json(json!({
            "events": [],
            "total": 0,
            "note": format!("from ({}) > to ({})", from, to),
        }));
    }

    // Pull the range, then filter in-memory. The store has no secondary
    // indexes by dag_id / run_id today, so this is the right shape for v1;
    // when /events is hot enough to warrant it, drive filtering via a
    // dedicated index column family rather than scanning here.
    let raw = match store.range(from, to) {
        Ok(events) => events,
        Err(e) => {
            return Json(json!({
                "events": [],
                "total": 0,
                "error": format!("event store range query failed: {}", e),
            }));
        }
    };

    let filtered: Vec<&Event> = raw
        .iter()
        .filter(|e| match_event_type(&params.event_type, &e.kind))
        .filter(|e| match_run_id(&params.run_id, &e.kind))
        .filter(|e| match_dag_id(&params.dag_id, &e.kind))
        .filter(|e| match_task_id(&params.task_id, &e.kind))
        .collect();

    // Apply limit to the most recent N (filtered, then truncate from the end).
    let total_after_filter = filtered.len();
    let events: Vec<&Event> = if filtered.len() > limit {
        filtered.iter().rev().take(limit).rev().copied().collect()
    } else {
        filtered
    };

    Json(json!({
        "events": events,
        "total": total_after_filter,
        "returned": events.len(),
        "current_sequence": current,
    }))
}

/// GET /api/v1/events/:sequence — get a specific event by sequence number.
///
/// The primitive for time-travel debugging — any historical state change
/// can be re-fetched by its sequence number.
pub async fn get_event(
    State(state): State<Arc<AppState>>,
    Path(sequence): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    let store = state
        .event_store
        .as_ref()
        .ok_or_else(|| ApiError::NotFound("Event store not initialized".to_string()))?;

    match store.get(sequence) {
        Ok(Some(event)) => Ok(Json(serde_json::to_value(event).unwrap_or(Value::Null))),
        Ok(None) => Err(ApiError::NotFound(format!(
            "Event with sequence {} not found",
            sequence
        ))),
        Err(e) => Err(ApiError::Internal(format!(
            "Event store query failed: {}",
            e
        ))),
    }
}

// ─── filter helpers ──────────────────────────────────────────────────────────
//
// EventKind is a large enum; matching every variant in every filter is noisy.
// Group fields once per filter and use `..` on the rest.

fn match_event_type(want: &Option<String>, kind: &EventKind) -> bool {
    let want = match want {
        Some(s) => s,
        None => return true,
    };
    let got = event_type_name(kind);
    got.eq_ignore_ascii_case(want)
}

fn match_run_id(want: &Option<String>, kind: &EventKind) -> bool {
    let want = match want {
        Some(s) => s,
        None => return true,
    };
    match kind {
        EventKind::DagRunCreated { run_id, .. }
        | EventKind::DagRunCompleted { run_id, .. }
        | EventKind::TaskQueued { run_id, .. }
        | EventKind::TaskStarted { run_id, .. }
        | EventKind::TaskCompleted { run_id, .. }
        | EventKind::TaskFailed { run_id, .. }
        | EventKind::TaskRetrying { run_id, .. }
        | EventKind::TaskSkipped { run_id, .. } => run_id == want,
        _ => false,
    }
}

fn match_dag_id(want: &Option<String>, kind: &EventKind) -> bool {
    let want = match want {
        Some(s) => s,
        None => return true,
    };
    match kind {
        EventKind::DagRunCreated { dag_id, .. }
        | EventKind::DagRunCompleted { dag_id, .. }
        | EventKind::TaskQueued { dag_id, .. }
        | EventKind::TaskStarted { dag_id, .. }
        | EventKind::TaskCompleted { dag_id, .. }
        | EventKind::TaskFailed { dag_id, .. }
        | EventKind::TaskRetrying { dag_id, .. }
        | EventKind::TaskSkipped { dag_id, .. }
        | EventKind::SnapshotCreated { dag_id, .. } => dag_id == want,
        _ => false,
    }
}

fn match_task_id(want: &Option<String>, kind: &EventKind) -> bool {
    let want = match want {
        Some(s) => s,
        None => return true,
    };
    match kind {
        EventKind::TaskQueued { task_id, .. }
        | EventKind::TaskStarted { task_id, .. }
        | EventKind::TaskCompleted { task_id, .. }
        | EventKind::TaskFailed { task_id, .. }
        | EventKind::TaskRetrying { task_id, .. }
        | EventKind::TaskSkipped { task_id, .. }
        | EventKind::SnapshotCreated { task_id, .. } => task_id == want,
        _ => false,
    }
}

/// Stringify the `EventKind` variant tag for `event_type` filtering. Mirrors
/// what serde produces with `#[serde(tag = "type")]` so URL filters match
/// what the UI sees in the response body.
fn event_type_name(kind: &EventKind) -> &'static str {
    match kind {
        EventKind::DagRunCreated { .. } => "DagRunCreated",
        EventKind::DagRunCompleted { .. } => "DagRunCompleted",
        EventKind::TaskQueued { .. } => "TaskQueued",
        EventKind::TaskStarted { .. } => "TaskStarted",
        EventKind::TaskCompleted { .. } => "TaskCompleted",
        EventKind::TaskFailed { .. } => "TaskFailed",
        EventKind::TaskRetrying { .. } => "TaskRetrying",
        EventKind::TaskSkipped { .. } => "TaskSkipped",
        EventKind::SnapshotCreated { .. } => "SnapshotCreated",
        EventKind::EnvironmentCreated { .. } => "EnvironmentCreated",
        EventKind::EnvironmentPromoted { .. } => "EnvironmentPromoted",
        EventKind::EnvironmentRolledBack { .. } => "EnvironmentRolledBack",
        EventKind::PlanCreated { .. } => "PlanCreated",
        EventKind::PlanApplied { .. } => "PlanApplied",
        EventKind::AuthAudit { .. } => "AuthAudit",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use conduit_common::event::Event;

    fn task_failed_event(seq: u64, run_id: &str, dag_id: &str, task_id: &str) -> Event {
        Event {
            id: uuid::Uuid::new_v4(),
            sequence: seq,
            timestamp: Utc::now(),
            kind: EventKind::TaskFailed {
                dag_id: dag_id.to_string(),
                run_id: run_id.to_string(),
                task_id: task_id.to_string(),
                error: "boom".to_string(),
                traceback: None,
                attempt: 1,
            },
        }
    }

    fn dag_created_event(seq: u64, run_id: &str, dag_id: &str) -> Event {
        Event {
            id: uuid::Uuid::new_v4(),
            sequence: seq,
            timestamp: Utc::now(),
            kind: EventKind::DagRunCreated {
                dag_id: dag_id.to_string(),
                run_id: run_id.to_string(),
                logical_date: Utc::now(),
                environment: "production".to_string(),
                triggered_by: "test".to_string(),
            },
        }
    }

    #[test]
    fn run_id_filter_matches_task_and_dag_events() {
        let a = task_failed_event(1, "r1", "etl", "load");
        let b = task_failed_event(2, "r2", "etl", "load");
        let c = dag_created_event(3, "r1", "etl");
        let events = vec![&a, &b, &c];

        let want = Some("r1".to_string());
        let kept: Vec<u64> = events
            .iter()
            .filter(|e| match_run_id(&want, &e.kind))
            .map(|e| e.sequence)
            .collect();
        assert_eq!(
            kept,
            vec![1, 3],
            "run_id=r1 must match both r1 task and r1 dag-create"
        );
    }

    #[test]
    fn event_type_filter_is_case_insensitive() {
        let a = task_failed_event(1, "r1", "etl", "load");
        let b = dag_created_event(2, "r1", "etl");

        assert!(match_event_type(&Some("TaskFailed".into()), &a.kind));
        assert!(match_event_type(&Some("taskfailed".into()), &a.kind));
        assert!(!match_event_type(&Some("TaskFailed".into()), &b.kind));
    }

    #[test]
    fn task_id_filter_drops_non_task_events() {
        let a = task_failed_event(1, "r1", "etl", "load");
        let b = dag_created_event(2, "r1", "etl");

        let want = Some("load".to_string());
        assert!(match_task_id(&want, &a.kind));
        // DagRunCreated has no task_id — filter drops it.
        assert!(!match_task_id(&want, &b.kind));
    }

    #[test]
    fn event_type_name_matches_serde_tag() {
        let kind = EventKind::TaskCompleted {
            dag_id: "d".into(),
            run_id: "r".into(),
            task_id: "t".into(),
            duration_ms: 0,
            snapshot_id: None,
        };
        assert_eq!(event_type_name(&kind), "TaskCompleted");
        // Sanity: the name must match what serde would emit in the JSON
        // body so URL filters and response inspection agree.
        let v = serde_json::to_value(&kind).unwrap();
        assert_eq!(v["type"], "TaskCompleted");
    }
}
