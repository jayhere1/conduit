//! OpenLineage ingest: receive RunEvents from foreign systems
//! (Airflow, dbt, Spark, any compliant producer) and surface them
//! alongside Conduit's own column-level lineage.
//!
//! ## Architecture
//!
//! [`ExternalLineageStore`] is a thin facade over the [`Backend`]
//! trait, so persistence and analysis are decoupled. Two
//! implementations:
//!
//! - [`InMemoryBackend`] — bounded ring buffer (10K events) for tests
//!   and the rare deployment that intentionally runs without a state
//!   directory.
//! - `conduit_state::RocksExternalLineageBackend` — the default for
//!   production deployments. Durable across restarts, unbounded,
//!   prefix-indexed for both incoming and outgoing column edges.
//!
//! Both backends pass the same conformance suite (exposed under the
//! `testing` feature). Keeping the trait in `conduit-lineage` leaves
//! the analysis crate free of any persistence dependency.
//!
//! ## Cross-system rendering
//!
//! Conduit's column-level graph (`ColumnSource::{Table | Task}`)
//! represents what *Conduit's own pipelines* produce. Foreign
//! producers live in this store. The unified view —
//! `GET /api/v1/lineage/datasets/:ns/:name/unified` and the **Datasets**
//! tab in the UI — joins the two at query time, so a single dataset
//! page shows "Airflow produced it, Conduit's `transform` task
//! consumes it, Spark reads downstream."
//!
//! A separate `Backend` per system is intentional: dropping foreign
//! producers into a third `ColumnSource` variant would force every
//! `match` site in Conduit's stitcher and impact analyzer to handle a
//! case that has no role in Conduit's compile-time stitching
//! algorithm. The join belongs at the render layer, not the model.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::openlineage::{
    ColumnLineageDatasetFacet, OpenLineageDataset, OpenLineageEventType, OpenLineageRunEvent,
};

/// Soft cap on the in-memory ring buffer. Persistent backends ignore it.
const IN_MEMORY_RING_CAPACITY: usize = 10_000;

/// One ingested RunEvent plus the metadata Conduit derived from it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestedEvent {
    pub received_at: DateTime<Utc>,
    pub run_id: String,
    pub job_namespace: String,
    pub job_name: String,
    pub producer: String,
    pub event_type: OpenLineageEventType,
    pub outputs: Vec<String>,
    pub inputs: Vec<String>,
    pub event: OpenLineageRunEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalDatasetSummary {
    pub namespace: String,
    pub name: String,
    pub columns: Vec<ExternalColumn>,
    pub last_producer: Option<ExternalProducerRef>,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub event_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalColumn {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dtype: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalProducerRef {
    pub job_namespace: String,
    pub job_name: String,
    pub run_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalColumnEdge {
    pub source_dataset: String,
    pub source_column: String,
    pub target_dataset: String,
    pub target_column: String,
    pub run_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestResult {
    pub run_id: String,
    pub received_at: DateTime<Utc>,
    pub inputs_recorded: usize,
    pub outputs_recorded: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalStoreStats {
    pub event_count: usize,
    pub dataset_count: usize,
    pub edge_count: usize,
}

/// Storage trait for ingested OpenLineage data. Implementations live
/// alongside their persistence concerns: [`InMemoryBackend`] is in this
/// crate; the RocksDB implementation is in `conduit-state`.
pub trait Backend: Send + Sync {
    fn record(&self, event: OpenLineageRunEvent) -> IngestResult;
    fn recent_events(
        &self,
        limit: usize,
        namespace_filter: Option<&str>,
        dataset_filter: Option<&str>,
    ) -> Vec<IngestedEvent>;
    fn dataset_summary(&self, namespace: &str, name: &str) -> Option<ExternalDatasetSummary>;
    fn edges_targeting(&self, namespace: &str, name: &str) -> Vec<ExternalColumnEdge>;
    fn edges_sourced_by(&self, namespace: &str, name: &str) -> Vec<ExternalColumnEdge>;
    fn stats(&self) -> ExternalStoreStats;
}

/// Public facade. Clones cheaply (it's an `Arc` under the hood).
#[derive(Clone)]
pub struct ExternalLineageStore {
    backend: Arc<dyn Backend>,
}

impl std::fmt::Debug for ExternalLineageStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalLineageStore")
            .field("stats", &self.backend.stats())
            .finish()
    }
}

impl ExternalLineageStore {
    /// Construct over an arbitrary backend.
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }

    /// In-memory backend — convenient for tests and ephemeral runs.
    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemoryBackend::new()))
    }

    pub fn record(&self, event: OpenLineageRunEvent) -> IngestResult {
        self.backend.record(event)
    }

    pub fn recent_events(
        &self,
        limit: usize,
        namespace_filter: Option<&str>,
        dataset_filter: Option<&str>,
    ) -> Vec<IngestedEvent> {
        self.backend
            .recent_events(limit, namespace_filter, dataset_filter)
    }

    pub fn dataset_summary(&self, namespace: &str, name: &str) -> Option<ExternalDatasetSummary> {
        self.backend.dataset_summary(namespace, name)
    }

    pub fn edges_targeting(&self, namespace: &str, name: &str) -> Vec<ExternalColumnEdge> {
        self.backend.edges_targeting(namespace, name)
    }

    pub fn edges_sourced_by(&self, namespace: &str, name: &str) -> Vec<ExternalColumnEdge> {
        self.backend.edges_sourced_by(namespace, name)
    }

    pub fn stats(&self) -> ExternalStoreStats {
        self.backend.stats()
    }
}

impl Default for ExternalLineageStore {
    fn default() -> Self {
        Self::in_memory()
    }
}

/// In-memory [`Backend`]. Used in tests and as a fallback when the
/// persistent store cannot be opened.
#[derive(Debug, Default)]
pub struct InMemoryBackend {
    inner: RwLock<InMemoryInner>,
}

#[derive(Debug, Default)]
struct InMemoryInner {
    events: VecDeque<IngestedEvent>,
    datasets: HashMap<String, ExternalDatasetSummary>,
    edges: Vec<ExternalColumnEdge>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Backend for InMemoryBackend {
    fn record(&self, event: OpenLineageRunEvent) -> IngestResult {
        let received_at = Utc::now();

        // Flatten + extract via the shared helpers (also used by the
        // RocksDB backend in `conduit_state`), so the in-memory and
        // durable backends stay byte-for-byte equivalent on the
        // observable surface.
        let ingested = flatten_event(&event, received_at);
        let edges = extract_column_edges(&event);

        let producer_ref = ExternalProducerRef {
            job_namespace: event.job.namespace.clone(),
            job_name: event.job.name.clone(),
            run_id: event.run.run_id.clone(),
        };

        let inputs_recorded = ingested.inputs.len();
        let outputs_recorded = ingested.outputs.len();

        let mut inner = self.inner.write().expect("InMemoryBackend lock poisoned");

        for ds in &event.inputs {
            update_dataset(&mut inner.datasets, ds, received_at, None);
        }
        for ds in &event.outputs {
            update_dataset(
                &mut inner.datasets,
                ds,
                received_at,
                Some(producer_ref.clone()),
            );
        }

        inner.edges.extend(edges);

        inner.events.push_back(ingested);
        while inner.events.len() > IN_MEMORY_RING_CAPACITY {
            inner.events.pop_front();
        }

        IngestResult {
            run_id: event.run.run_id,
            received_at,
            inputs_recorded,
            outputs_recorded,
        }
    }

    fn recent_events(
        &self,
        limit: usize,
        namespace_filter: Option<&str>,
        dataset_filter: Option<&str>,
    ) -> Vec<IngestedEvent> {
        let inner = self.inner.read().expect("InMemoryBackend lock poisoned");
        let qualified_filter = match (namespace_filter, dataset_filter) {
            (Some(n), Some(d)) => Some(format!("{}/{}", n, d)),
            _ => None,
        };

        inner
            .events
            .iter()
            .rev()
            .filter(
                |e| match (&qualified_filter, namespace_filter, dataset_filter) {
                    (Some(qn), _, _) => e.inputs.contains(qn) || e.outputs.contains(qn),
                    (None, Some(ns), None) => e.job_namespace == ns,
                    (None, None, Some(ds)) => {
                        e.outputs.iter().any(|q| q.ends_with(&format!("/{}", ds)))
                            || e.inputs.iter().any(|q| q.ends_with(&format!("/{}", ds)))
                    }
                    _ => true,
                },
            )
            .take(limit)
            .cloned()
            .collect()
    }

    fn dataset_summary(&self, namespace: &str, name: &str) -> Option<ExternalDatasetSummary> {
        let inner = self.inner.read().expect("InMemoryBackend lock poisoned");
        inner
            .datasets
            .get(&format!("{}/{}", namespace, name))
            .cloned()
    }

    fn edges_targeting(&self, namespace: &str, name: &str) -> Vec<ExternalColumnEdge> {
        let qualified = format!("{}/{}", namespace, name);
        let inner = self.inner.read().expect("InMemoryBackend lock poisoned");
        inner
            .edges
            .iter()
            .filter(|e| e.target_dataset == qualified)
            .cloned()
            .collect()
    }

    fn edges_sourced_by(&self, namespace: &str, name: &str) -> Vec<ExternalColumnEdge> {
        let qualified = format!("{}/{}", namespace, name);
        let inner = self.inner.read().expect("InMemoryBackend lock poisoned");
        inner
            .edges
            .iter()
            .filter(|e| e.source_dataset == qualified)
            .cloned()
            .collect()
    }

    fn stats(&self) -> ExternalStoreStats {
        let inner = self.inner.read().expect("InMemoryBackend lock poisoned");
        ExternalStoreStats {
            event_count: inner.events.len(),
            dataset_count: inner.datasets.len(),
            edge_count: inner.edges.len(),
        }
    }
}

// ── Shared helpers used by both backends ────────────────────────────

pub fn qualify_dataset(d: &OpenLineageDataset) -> String {
    format!("{}/{}", d.namespace, d.name)
}

pub fn update_dataset(
    map: &mut HashMap<String, ExternalDatasetSummary>,
    ds: &OpenLineageDataset,
    received_at: DateTime<Utc>,
    producer: Option<ExternalProducerRef>,
) {
    let key = qualify_dataset(ds);
    let columns = extract_columns_from_schema_facet(ds);

    map.entry(key)
        .and_modify(|s| {
            s.last_seen_at = received_at;
            s.event_count += 1;
            if !columns.is_empty() {
                s.columns = columns.clone();
            }
            if let Some(p) = producer.clone() {
                s.last_producer = Some(p);
            }
        })
        .or_insert_with(|| ExternalDatasetSummary {
            namespace: ds.namespace.clone(),
            name: ds.name.clone(),
            columns,
            last_producer: producer,
            first_seen_at: received_at,
            last_seen_at: received_at,
            event_count: 1,
        });
}

pub fn extract_columns_from_schema_facet(ds: &OpenLineageDataset) -> Vec<ExternalColumn> {
    ds.facets
        .get("schema")
        .and_then(|v| v.get("fields"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|f| {
                    let name = f.get("name")?.as_str()?.to_string();
                    let dtype = f
                        .get("type")
                        .and_then(|t| t.as_str())
                        .map(|s| s.to_string());
                    Some(ExternalColumn { name, dtype })
                })
                .collect()
        })
        .unwrap_or_default()
}

pub fn extract_column_edges(event: &OpenLineageRunEvent) -> Vec<ExternalColumnEdge> {
    let mut out = Vec::new();
    for output in &event.outputs {
        let target_dataset = qualify_dataset(output);
        let Some(facet_value) = output.facets.get("columnLineage") else {
            continue;
        };
        let Ok(facet) = serde_json::from_value::<ColumnLineageDatasetFacet>(facet_value.clone())
        else {
            continue;
        };
        for (target_col, field) in &facet.fields {
            for input in &field.input_fields {
                out.push(ExternalColumnEdge {
                    source_dataset: format!("{}/{}", input.namespace, input.name),
                    source_column: input.field.clone(),
                    target_dataset: target_dataset.clone(),
                    target_column: target_col.clone(),
                    run_id: event.run.run_id.clone(),
                });
            }
        }
    }
    out
}

pub fn flatten_event(
    event: &OpenLineageRunEvent,
    received_at: DateTime<Utc>,
) -> IngestedEvent {
    IngestedEvent {
        received_at,
        run_id: event.run.run_id.clone(),
        job_namespace: event.job.namespace.clone(),
        job_name: event.job.name.clone(),
        producer: event.producer.clone(),
        event_type: event.event_type,
        inputs: event.inputs.iter().map(qualify_dataset).collect(),
        outputs: event.outputs.iter().map(qualify_dataset).collect(),
        event: event.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openlineage::{
        OpenLineageEventType, OpenLineageJob, OpenLineageRun, OpenLineageRunEvent,
        OPENLINEAGE_RUN_EVENT_SCHEMA_URL,
    };
    use serde_json::json;
    use std::collections::BTreeMap;

    fn dataset(namespace: &str, name: &str, columns: &[(&str, &str)]) -> OpenLineageDataset {
        let mut facets = BTreeMap::new();
        if !columns.is_empty() {
            facets.insert(
                "schema".to_string(),
                json!({
                    "_producer": "test",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                    "fields": columns.iter().map(|(n, t)| json!({"name": n, "type": t})).collect::<Vec<_>>(),
                }),
            );
        }
        OpenLineageDataset {
            namespace: namespace.to_string(),
            name: name.to_string(),
            facets,
        }
    }

    fn event(
        run_id: &str,
        namespace: &str,
        job_name: &str,
        inputs: Vec<OpenLineageDataset>,
        outputs: Vec<OpenLineageDataset>,
    ) -> OpenLineageRunEvent {
        OpenLineageRunEvent {
            event_time: "2026-05-17T12:00:00Z".to_string(),
            producer: "https://test/integration/airflow".to_string(),
            schema_url: OPENLINEAGE_RUN_EVENT_SCHEMA_URL.to_string(),
            event_type: OpenLineageEventType::Complete,
            run: OpenLineageRun {
                run_id: run_id.to_string(),
            },
            job: OpenLineageJob {
                namespace: namespace.to_string(),
                name: job_name.to_string(),
            },
            inputs,
            outputs,
        }
    }

    /// Reusable cross-backend test suite (local). The cross-crate copy
    /// lives in the `testing` feature module.
    fn run_backend_conformance_suite(store: &ExternalLineageStore) {
        let r = store.record(event(
            "run-1",
            "airflow",
            "etl.extract_orders",
            vec![dataset("postgres", "raw.orders", &[("id", "int")])],
            vec![dataset(
                "warehouse",
                "staging.orders",
                &[("id", "int"), ("amount", "decimal")],
            )],
        ));
        assert_eq!(r.run_id, "run-1");
        assert_eq!(r.inputs_recorded, 1);
        assert_eq!(r.outputs_recorded, 1);

        let summary = store
            .dataset_summary("warehouse", "staging.orders")
            .expect("dataset recorded");
        assert_eq!(summary.columns.len(), 2);
        assert_eq!(summary.event_count, 1);
        let producer = summary.last_producer.expect("producer");
        assert_eq!(producer.run_id, "run-1");

        let s = store.stats();
        assert_eq!(s.event_count, 1);
        assert_eq!(s.dataset_count, 2);
    }

    #[test]
    fn in_memory_backend_passes_conformance_suite() {
        let store = ExternalLineageStore::in_memory();
        run_backend_conformance_suite(&store);
    }

    #[test]
    fn recent_events_filters_by_dataset() {
        let store = ExternalLineageStore::in_memory();
        store.record(event(
            "r1",
            "airflow",
            "j1",
            vec![],
            vec![dataset("ns", "ds_a", &[])],
        ));
        store.record(event(
            "r2",
            "airflow",
            "j2",
            vec![],
            vec![dataset("ns", "ds_b", &[])],
        ));
        store.record(event(
            "r3",
            "airflow",
            "j3",
            vec![],
            vec![dataset("ns", "ds_a", &[])],
        ));

        let only_a = store.recent_events(10, Some("ns"), Some("ds_a"));
        assert_eq!(only_a.len(), 2);
        assert_eq!(only_a[0].run_id, "r3");
        assert_eq!(only_a[1].run_id, "r1");
    }

    #[test]
    fn column_lineage_edges_flatten_from_facets() {
        let mut output = dataset("warehouse", "staging.orders", &[]);
        output.facets.insert(
            "columnLineage".to_string(),
            json!({
                "_producer": "test",
                "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json",
                "fields": {
                    "amount": {
                        "inputFields": [{
                            "namespace": "postgres",
                            "name": "raw.orders",
                            "field": "amount",
                        }]
                    }
                }
            }),
        );

        let store = ExternalLineageStore::in_memory();
        store.record(event(
            "r1",
            "airflow",
            "j1",
            vec![dataset("postgres", "raw.orders", &[])],
            vec![output],
        ));

        let edges = store.edges_targeting("warehouse", "staging.orders");
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].source_dataset, "postgres/raw.orders");
        assert_eq!(edges[0].source_column, "amount");

        let outgoing = store.edges_sourced_by("postgres", "raw.orders");
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].target_dataset, "warehouse/staging.orders");
    }

    #[test]
    fn raw_json_round_trips_through_record() {
        let payload = json!({
            "eventTime": "2026-05-17T12:00:00Z",
            "producer": "https://github.com/apache/airflow",
            "schemaURL": OPENLINEAGE_RUN_EVENT_SCHEMA_URL,
            "eventType": "COMPLETE",
            "run": { "runId": "550e8400-e29b-41d4-a716-446655440000" },
            "job": { "namespace": "airflow", "name": "etl.orders" },
            "inputs": [{ "namespace": "postgres", "name": "raw.orders" }],
            "outputs": [{ "namespace": "warehouse", "name": "staging.orders" }],
        });
        let event: OpenLineageRunEvent =
            serde_json::from_value(payload).expect("spec-shaped event must parse");
        let store = ExternalLineageStore::in_memory();
        let result = store.record(event);
        assert_eq!(result.run_id, "550e8400-e29b-41d4-a716-446655440000");
    }
}

/// Cross-crate test helpers. Hidden behind the `testing` feature so
/// production builds don't pay for it. The RocksDB backend in
/// `conduit-state` uses this to run the same conformance suite as the
/// in-memory backend.
#[cfg(feature = "testing")]
pub mod testing {
    use super::*;
    use crate::openlineage::{
        OpenLineageEventType, OpenLineageJob, OpenLineageRun, OpenLineageRunEvent,
        OPENLINEAGE_RUN_EVENT_SCHEMA_URL,
    };
    use serde_json::json;
    use std::collections::BTreeMap;

    pub fn dataset(namespace: &str, name: &str, columns: &[(&str, &str)]) -> OpenLineageDataset {
        let mut facets = BTreeMap::new();
        if !columns.is_empty() {
            facets.insert(
                "schema".to_string(),
                json!({
                    "_producer": "test",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                    "fields": columns.iter().map(|(n, t)| json!({"name": n, "type": t})).collect::<Vec<_>>(),
                }),
            );
        }
        OpenLineageDataset {
            namespace: namespace.to_string(),
            name: name.to_string(),
            facets,
        }
    }

    pub fn event(
        run_id: &str,
        namespace: &str,
        job_name: &str,
        inputs: Vec<OpenLineageDataset>,
        outputs: Vec<OpenLineageDataset>,
    ) -> OpenLineageRunEvent {
        OpenLineageRunEvent {
            event_time: "2026-05-17T12:00:00Z".to_string(),
            producer: "https://test/integration/airflow".to_string(),
            schema_url: OPENLINEAGE_RUN_EVENT_SCHEMA_URL.to_string(),
            event_type: OpenLineageEventType::Complete,
            run: OpenLineageRun {
                run_id: run_id.to_string(),
            },
            job: OpenLineageJob {
                namespace: namespace.to_string(),
                name: job_name.to_string(),
            },
            inputs,
            outputs,
        }
    }

    /// Asserts behavioural parity. Every [`Backend`] implementation
    /// should pass this.
    pub fn run_backend_conformance_suite(store: &ExternalLineageStore) {
        let r = store.record(event(
            "run-1",
            "airflow",
            "etl.extract_orders",
            vec![dataset("postgres", "raw.orders", &[("id", "int")])],
            vec![dataset(
                "warehouse",
                "staging.orders",
                &[("id", "int"), ("amount", "decimal")],
            )],
        ));
        assert_eq!(r.run_id, "run-1");
        assert_eq!(r.inputs_recorded, 1);
        assert_eq!(r.outputs_recorded, 1);

        let summary = store
            .dataset_summary("warehouse", "staging.orders")
            .expect("dataset recorded");
        assert_eq!(summary.columns.len(), 2);
        assert_eq!(summary.event_count, 1);
        let producer = summary.last_producer.expect("producer");
        assert_eq!(producer.run_id, "run-1");

        let s = store.stats();
        assert_eq!(s.event_count, 1);
        assert_eq!(s.dataset_count, 2);
    }
}
