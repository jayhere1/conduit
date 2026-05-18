//! HTTP handlers for the OpenLineage ingest surface.
//!
//! Three endpoints (see `docs/STRATEGIC_DIRECTION.md` §4 item 1):
//!
//! - `POST /api/v1/openlineage/v1/lineage` — record a RunEvent. The path
//!   is spec-compliant so Airflow / dbt / Spark exporters can be pointed
//!   at us with just a base URL change.
//! - `GET /api/v1/openlineage/events` — recent events with optional
//!   filter by `namespace` and/or `dataset`.
//! - `GET /api/v1/openlineage/datasets/:namespace/:name` — aggregated
//!   view of a dataset (latest columns, last producer, event counts).

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use conduit_lineage::OpenLineageRunEvent;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize)]
pub struct ListEventsQuery {
    pub namespace: Option<String>,
    pub dataset: Option<String>,
    pub limit: Option<usize>,
}

/// `POST /api/v1/openlineage/v1/lineage`
///
/// Accept and record an OpenLineage RunEvent. Returns `201 Created` with
/// a small summary the caller can use to confirm receipt. Validation is
/// limited to "the payload parses as a RunEvent" — we don't enforce
/// e.g. lifecycle ordering, because well-behaved producers handle that
/// and we want to be lossless for the round-trip.
pub async fn ingest_event(
    State(state): State<Arc<AppState>>,
    Json(event): Json<OpenLineageRunEvent>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let result = state.external_lineage.record(event);

    // Broadcast over websocket so live UI views can light up.
    let ws = json!({
        "type": "openlineage_event_ingested",
        "runId": result.run_id,
        "receivedAt": result.received_at.to_rfc3339(),
        "inputsRecorded": result.inputs_recorded,
        "outputsRecorded": result.outputs_recorded,
    });
    state.broadcast_event(&ws.to_string());

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "runId": result.run_id,
            "receivedAt": result.received_at.to_rfc3339(),
            "inputsRecorded": result.inputs_recorded,
            "outputsRecorded": result.outputs_recorded,
        })),
    ))
}

/// `GET /api/v1/openlineage/events?namespace=&dataset=&limit=`
pub async fn list_events(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListEventsQuery>,
) -> Json<Value> {
    let limit = params.limit.unwrap_or(50).min(1000);
    let events = state.external_lineage.recent_events(
        limit,
        params.namespace.as_deref(),
        params.dataset.as_deref(),
    );
    Json(json!({
        "total": events.len(),
        "events": events,
    }))
}

/// `GET /api/v1/openlineage/datasets/:namespace/:name`
pub async fn get_dataset(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let summary = state
        .external_lineage
        .dataset_summary(&namespace, &name)
        .ok_or_else(|| ApiError::NotFound(format!("Dataset '{}/{}' not seen", namespace, name)))?;
    let edges = state.external_lineage.edges_targeting(&namespace, &name);

    Ok(Json(json!({
        "summary": summary,
        "incomingEdges": edges,
    })))
}

/// `GET /api/v1/openlineage/stats`
pub async fn stats(State(state): State<Arc<AppState>>) -> Json<Value> {
    let s = state.external_lineage.stats();
    Json(json!({
        "eventCount": s.event_count,
        "datasetCount": s.dataset_count,
        "edgeCount": s.edge_count,
    }))
}

/// `GET /api/v1/lineage/datasets/:namespace/:name/unified`
///
/// One-stop dataset view: fuses the external lineage store (ingested
/// OpenLineage events from foreign systems) with Conduit's own
/// cross-task stitch over the compiled DAGs. The response answers all
/// of:
///   - "Who produces this dataset?" — internal Conduit task + external
///     job history.
///   - "What columns does it have?" — latest declared schema from
///     either side.
///   - "What flows into it?" — incoming column edges from both sources.
///   - "What consumes it?" — outgoing column edges from both sources.
///
/// Compile-then-stitch is per-request; for hot paths this can be backed
/// by `state.catalog_cache` and a future stitched-graph cache.
pub async fn unified_dataset_view(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    use conduit_lineage::{ColumnRef, TaskRef};

    let qualified = format!("{}/{}", namespace, name);

    // ── External side ──────────────────────────────────────────────
    let external_summary = state.external_lineage.dataset_summary(&namespace, &name);
    let external_incoming = state.external_lineage.edges_targeting(&namespace, &name);
    let external_outgoing = state.external_lineage.edges_sourced_by(&namespace, &name);
    let recent_events = state
        .external_lineage
        .recent_events(10, Some(&namespace), Some(&name));

    // ── Internal side ──────────────────────────────────────────────
    // Look up the internal producer task via the cached catalog. If no
    // catalog is loaded the internal side stays empty — that's fine,
    // the response just won't have internal producer info.
    let internal_producer: Option<TaskRef> = state.catalog_cache.read().ok().and_then(|guard| {
        guard
            .as_ref()
            .and_then(|cat| cat.lookup_producer(&qualified).cloned())
    });

    let mut internal_upstream: Vec<Value> = Vec::new();
    let mut internal_downstream: Vec<Value> = Vec::new();
    // One cache lookup serves both the upstream/downstream trace and the
    // schema-merge fallback below. The cache itself only recompiles when
    // the DAG source signature changes.
    let cached_plan = state.plan_cache.get().ok();

    if let Some(producer) = &internal_producer {
        if let Some(view) = &cached_plan {
            if let Some(dag) = view.plan.dags.get(&producer.dag_id) {
                if let Some(stitched) = view.stitched_for(&producer.dag_id) {
                    if let Some(task) = dag.tasks.get(&producer.task_id) {
                        let columns: Vec<String> = task
                            .outputs
                            .iter()
                            .filter(|ds| ds.name == qualified || ds.name == name)
                            .flat_map(|ds| ds.columns.iter().map(|c| c.name.clone()))
                            .collect();

                        for col_name in &columns {
                            let origin = ColumnRef::task(producer.clone(), col_name);
                            let up = stitched.graph.trace_upstream(&origin);
                            for col_ref in up.columns {
                                internal_upstream.push(serialize_column_ref(&col_ref, col_name));
                            }
                            let down = stitched.graph.trace_downstream(&origin);
                            for col_ref in down.columns {
                                internal_downstream.push(serialize_column_ref(&col_ref, col_name));
                            }
                        }
                    }
                }
            }
        }
    }

    // ── Schema merge ──────────────────────────────────────────────
    // Prefer the external schema if present (it usually has dtypes from
    // a real producer); fall back to internal task output columns.
    let columns = if let Some(s) = &external_summary {
        s.columns
            .iter()
            .map(|c| json!({"name": c.name, "dtype": c.dtype}))
            .collect::<Vec<_>>()
    } else if let (Some(producer), Some(view)) = (&internal_producer, &cached_plan) {
        view.plan
            .dags
            .get(&producer.dag_id)
            .and_then(|dag| {
                dag.tasks.get(&producer.task_id).map(|task| {
                    task.outputs
                        .iter()
                        .find(|ds| ds.name == qualified || ds.name == name)
                        .map(|ds| {
                            ds.columns
                                .iter()
                                .map(|c| json!({"name": c.name, "dtype": c.dtype}))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let schema_source = match (external_summary.is_some(), internal_producer.is_some()) {
        (true, true) => "both",
        (true, false) => "external",
        (false, true) => "internal",
        (false, false) => "unknown",
    };

    // ── Compose response ──────────────────────────────────────────
    if external_summary.is_none() && internal_producer.is_none() {
        return Err(ApiError::NotFound(format!(
            "No lineage information for dataset '{}'",
            qualified
        )));
    }

    Ok(Json(json!({
        "namespace": namespace,
        "name": name,
        "qualified": qualified,
        "schema": {
            "columns": columns,
            "source": schema_source,
        },
        "producers": {
            "internal": internal_producer.as_ref().map(|p| json!({
                "dagId": p.dag_id,
                "taskId": p.task_id,
            })),
            "external": external_summary.as_ref().and_then(|s| s.last_producer.as_ref().map(|p| json!({
                "jobNamespace": p.job_namespace,
                "jobName": p.job_name,
                "runId": p.run_id,
            }))),
            "externalEventCount": external_summary.as_ref().map(|s| s.event_count).unwrap_or(0),
            "externalFirstSeenAt": external_summary.as_ref().map(|s| s.first_seen_at.to_rfc3339()),
            "externalLastSeenAt": external_summary.as_ref().map(|s| s.last_seen_at.to_rfc3339()),
        },
        "upstream": {
            "internal": internal_upstream,
            "external": external_incoming,
        },
        "downstream": {
            "internal": internal_downstream,
            "external": external_outgoing,
        },
        "recentEvents": recent_events,
    })))
}

fn serialize_column_ref(col: &conduit_lineage::ColumnRef, relative_to_column: &str) -> Value {
    use conduit_lineage::ColumnSource;
    match &col.source {
        ColumnSource::Task(task) => json!({
            "kind": "task",
            "dagId": task.dag_id,
            "taskId": task.task_id,
            "column": col.column_name,
            "origin": relative_to_column,
        }),
        ColumnSource::Table(name) => json!({
            "kind": "table",
            "table": name,
            "column": col.column_name,
            "origin": relative_to_column,
        }),
    }
}

// Use `ColumnRef` from conduit_lineage to keep the import surface
// explicit at the use site.
#[allow(unused_imports)]
use conduit_lineage::ColumnRef as _;

/// `GET /api/v1/lineage/cache/stats` — visibility into the
/// compile/stitch cache that powers the unified dataset view.
pub async fn cache_stats(State(state): State<Arc<AppState>>) -> Json<Value> {
    let s = state.plan_cache.stats();
    Json(json!(s))
}

/// `POST /api/v1/lineage/cache/invalidate` — force a recompile on the
/// next request. Useful after an external DAG-source mutation that
/// somehow didn't change file mtimes (rare, but harmless to expose).
pub async fn cache_invalidate(State(state): State<Arc<AppState>>) -> Json<Value> {
    state.plan_cache.invalidate();
    Json(json!({
        "invalidated": true,
        "message": "Plan cache will recompile on the next request.",
    }))
}
