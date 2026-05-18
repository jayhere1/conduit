//! Durable RocksDB backend for [`conduit_lineage::ExternalLineageStore`].
//!
//! Implements [`conduit_lineage::ExternalLineageBackend`] on top of three
//! column families:
//!
//! - `events` — keyed by `{u64_be_inverted_received_at_nanos}{run_id}`.
//!   Forward iteration walks events newest-first (the inverted timestamp
//!   makes the BE-encoded key sort that way naturally).
//! - `datasets` — keyed by qualified `namespace/name`. Value is the
//!   aggregated [`ExternalDatasetSummary`].
//! - `edges` — keyed by `{target_dataset}\0{source_dataset}\0{source_col}\0{target_col}\0{run_id}`.
//!   Prefix iteration by `target_dataset\0` returns every edge feeding a
//!   dataset; the symmetric prefix `{source_dataset}\0` is supported via
//!   a thin secondary index in CF `edges_by_source`.
//!
//! The store is process-thread-safe: `Arc<DB>` is internally locked by
//! RocksDB. Cloning the store handle is cheap.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use conduit_lineage::{
    extract_column_edges, extract_columns_from_schema_facet, flatten_event, qualify_dataset,
    ExternalColumnEdge, ExternalDatasetSummary, ExternalLineageBackend, ExternalStoreStats,
    IngestResult, IngestedEvent,
};
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::{info, warn};

const CF_EVENTS: &str = "events";
const CF_DATASETS: &str = "datasets";
const CF_EDGES_BY_TARGET: &str = "edges_by_target";
const CF_EDGES_BY_SOURCE: &str = "edges_by_source";
const CF_STATS: &str = "stats";

const STATS_KEY_EVENTS: &[u8] = b"event_count";
const STATS_KEY_EDGES: &[u8] = b"edge_count";

/// Durable RocksDB-backed external-lineage store.
pub struct RocksExternalLineageBackend {
    db: Arc<DB>,
    #[allow(dead_code)]
    path: PathBuf,
}

impl RocksExternalLineageBackend {
    /// Open or create a store at `path`. The directory is created if it
    /// doesn't exist.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_EVENTS, Options::default()),
            ColumnFamilyDescriptor::new(CF_DATASETS, Options::default()),
            ColumnFamilyDescriptor::new(CF_EDGES_BY_TARGET, Options::default()),
            ColumnFamilyDescriptor::new(CF_EDGES_BY_SOURCE, Options::default()),
            ColumnFamilyDescriptor::new(CF_STATS, Options::default()),
        ];
        let db = DB::open_cf_descriptors(&opts, &path, cfs)
            .map_err(|e| std::io::Error::other(format!("open external lineage db: {}", e)))?;
        info!(path = %path.display(), "Opened external lineage store");
        Ok(Self {
            db: Arc::new(db),
            path,
        })
    }

    fn cf(&self, name: &str) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle(name)
            .unwrap_or_else(|| panic!("column family '{}' missing", name))
    }

    fn put_json<T: Serialize>(
        &self,
        cf_name: &str,
        key: &[u8],
        value: &T,
    ) -> Result<(), rocksdb::Error> {
        let bytes = serde_json::to_vec(value).expect("serialize");
        self.db.put_cf(self.cf(cf_name), key, bytes)
    }

    fn get_json<T: DeserializeOwned>(&self, cf_name: &str, key: &[u8]) -> Option<T> {
        self.db
            .get_cf(self.cf(cf_name), key)
            .ok()
            .flatten()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
    }

    fn incr_counter(&self, key: &[u8], by: i64) {
        let cf = self.cf(CF_STATS);
        let cur = self
            .db
            .get_cf(cf, key)
            .ok()
            .flatten()
            .and_then(|b| b.try_into().ok().map(i64::from_be_bytes))
            .unwrap_or(0);
        let next = (cur + by).max(0);
        let _ = self.db.put_cf(cf, key, next.to_be_bytes());
    }

    fn read_counter(&self, key: &[u8]) -> i64 {
        let cf = self.cf(CF_STATS);
        self.db
            .get_cf(cf, key)
            .ok()
            .flatten()
            .and_then(|b| b.try_into().ok().map(i64::from_be_bytes))
            .unwrap_or(0)
    }
}

impl ExternalLineageBackend for RocksExternalLineageBackend {
    fn record(&self, event: conduit_lineage::OpenLineageRunEvent) -> IngestResult {
        let received_at = Utc::now();
        let flattened = flatten_event(&event, received_at);

        // Events CF: key sorts newest-first.
        let key = event_key(received_at, &event.run.run_id);
        if let Err(e) = self.put_json(CF_EVENTS, &key, &flattened) {
            warn!(error = %e, "failed to persist event");
        } else {
            self.incr_counter(STATS_KEY_EVENTS, 1);
        }

        // Datasets CF: upsert aggregates for every referenced dataset.
        let producer_ref = conduit_lineage::ExternalProducerRef {
            job_namespace: event.job.namespace.clone(),
            job_name: event.job.name.clone(),
            run_id: event.run.run_id.clone(),
        };
        for ds in &event.inputs {
            self.upsert_dataset(ds, received_at, None);
        }
        for ds in &event.outputs {
            self.upsert_dataset(ds, received_at, Some(producer_ref.clone()));
        }

        // Edges CFs: emit per-column edges from output facets, indexed
        // both by target (for downstream queries) and by source (for
        // upstream queries).
        let edges = extract_column_edges(&event);
        for edge in &edges {
            let val = serde_json::to_vec(edge).expect("serialize edge");
            let by_target = edge_key_by_target(edge);
            let by_source = edge_key_by_source(edge);
            let _ = self
                .db
                .put_cf(self.cf(CF_EDGES_BY_TARGET), &by_target, &val);
            let _ = self
                .db
                .put_cf(self.cf(CF_EDGES_BY_SOURCE), &by_source, &val);
        }
        if !edges.is_empty() {
            self.incr_counter(STATS_KEY_EDGES, edges.len() as i64);
        }

        IngestResult {
            run_id: event.run.run_id,
            received_at,
            inputs_recorded: event.inputs.len(),
            outputs_recorded: event.outputs.len(),
        }
    }

    fn recent_events(
        &self,
        limit: usize,
        namespace_filter: Option<&str>,
        dataset_filter: Option<&str>,
    ) -> Vec<IngestedEvent> {
        let qualified_filter = match (namespace_filter, dataset_filter) {
            (Some(n), Some(d)) => Some(format!("{}/{}", n, d)),
            _ => None,
        };

        let cf = self.cf(CF_EVENTS);
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);

        let mut out = Vec::with_capacity(limit.min(64));
        for entry in iter {
            let Ok((_, value)) = entry else { continue };
            let Ok(event) = serde_json::from_slice::<IngestedEvent>(&value) else {
                continue;
            };
            let matches = match (&qualified_filter, namespace_filter, dataset_filter) {
                (Some(qn), _, _) => event.inputs.contains(qn) || event.outputs.contains(qn),
                (None, Some(ns), None) => event.job_namespace == ns,
                (None, None, Some(ds)) => {
                    event
                        .outputs
                        .iter()
                        .any(|q| q.ends_with(&format!("/{}", ds)))
                        || event
                            .inputs
                            .iter()
                            .any(|q| q.ends_with(&format!("/{}", ds)))
                }
                _ => true,
            };
            if matches {
                out.push(event);
                if out.len() >= limit {
                    break;
                }
            }
        }
        out
    }

    fn dataset_summary(&self, namespace: &str, name: &str) -> Option<ExternalDatasetSummary> {
        let key = format!("{}/{}", namespace, name);
        self.get_json::<ExternalDatasetSummary>(CF_DATASETS, key.as_bytes())
    }

    fn edges_targeting(&self, namespace: &str, name: &str) -> Vec<ExternalColumnEdge> {
        let prefix = format!("{}/{}\0", namespace, name);
        self.scan_prefix(CF_EDGES_BY_TARGET, prefix.as_bytes())
    }

    fn edges_sourced_by(&self, namespace: &str, name: &str) -> Vec<ExternalColumnEdge> {
        let prefix = format!("{}/{}\0", namespace, name);
        self.scan_prefix(CF_EDGES_BY_SOURCE, prefix.as_bytes())
    }

    fn stats(&self) -> ExternalStoreStats {
        // Dataset count: a quick iterator count. Cheap because aggregates
        // are few relative to events.
        let dataset_count = self
            .db
            .iterator_cf(self.cf(CF_DATASETS), IteratorMode::Start)
            .count();
        ExternalStoreStats {
            event_count: self.read_counter(STATS_KEY_EVENTS).max(0) as usize,
            dataset_count,
            edge_count: self.read_counter(STATS_KEY_EDGES).max(0) as usize,
        }
    }
}

impl RocksExternalLineageBackend {
    fn scan_prefix(&self, cf_name: &str, prefix: &[u8]) -> Vec<ExternalColumnEdge> {
        let cf = self.cf(cf_name);
        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let iter = self.db.iterator_cf_opt(
            cf,
            opts,
            IteratorMode::From(prefix, rocksdb::Direction::Forward),
        );
        let mut out = Vec::new();
        for entry in iter {
            let Ok((k, v)) = entry else { continue };
            if !k.starts_with(prefix) {
                break;
            }
            if let Ok(edge) = serde_json::from_slice::<ExternalColumnEdge>(&v) {
                out.push(edge);
            }
        }
        out
    }

    fn upsert_dataset(
        &self,
        ds: &conduit_lineage::openlineage::OpenLineageDataset,
        received_at: DateTime<Utc>,
        producer: Option<conduit_lineage::ExternalProducerRef>,
    ) {
        let key = qualify_dataset(ds);
        let columns = extract_columns_from_schema_facet(ds);

        let mut summary = self
            .get_json::<ExternalDatasetSummary>(CF_DATASETS, key.as_bytes())
            .unwrap_or_else(|| ExternalDatasetSummary {
                namespace: ds.namespace.clone(),
                name: ds.name.clone(),
                columns: columns.clone(),
                last_producer: producer.clone(),
                first_seen_at: received_at,
                last_seen_at: received_at,
                event_count: 0,
            });

        summary.last_seen_at = received_at;
        summary.event_count += 1;
        if !columns.is_empty() {
            summary.columns = columns;
        }
        if let Some(p) = producer {
            summary.last_producer = Some(p);
        }

        let _ = self.put_json(CF_DATASETS, key.as_bytes(), &summary);
    }
}

// Helpers (`qualify_dataset`, `flatten_event`, `extract_column_edges`,
// `extract_columns_from_schema_facet`) live in `conduit_lineage` as the
// single source of truth. The previous local copies in this file went
// out of sync with the canonical versions at least once, so they're
// now imported rather than duplicated.

// Newest-first event key: invert the BE-encoded nanos so forward
// iteration walks newest → oldest. `run_id` is appended verbatim as a
// disambiguator for events with the same instant.
fn event_key(ts: DateTime<Utc>, run_id: &str) -> Vec<u8> {
    let nanos = ts.timestamp_nanos_opt().unwrap_or(0) as u64;
    let inverted = u64::MAX - nanos;
    let mut k = Vec::with_capacity(8 + run_id.len());
    k.extend_from_slice(&inverted.to_be_bytes());
    k.extend_from_slice(run_id.as_bytes());
    k
}

fn edge_key_by_target(edge: &ExternalColumnEdge) -> Vec<u8> {
    let mut k = Vec::new();
    k.extend_from_slice(edge.target_dataset.as_bytes());
    k.push(0);
    k.extend_from_slice(edge.source_dataset.as_bytes());
    k.push(0);
    k.extend_from_slice(edge.source_column.as_bytes());
    k.push(0);
    k.extend_from_slice(edge.target_column.as_bytes());
    k.push(0);
    k.extend_from_slice(edge.run_id.as_bytes());
    k
}

fn edge_key_by_source(edge: &ExternalColumnEdge) -> Vec<u8> {
    let mut k = Vec::new();
    k.extend_from_slice(edge.source_dataset.as_bytes());
    k.push(0);
    k.extend_from_slice(edge.target_dataset.as_bytes());
    k.push(0);
    k.extend_from_slice(edge.source_column.as_bytes());
    k.push(0);
    k.extend_from_slice(edge.target_column.as_bytes());
    k.push(0);
    k.extend_from_slice(edge.run_id.as_bytes());
    k
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_lineage::testing as ln_test;
    use conduit_lineage::ExternalLineageStore;
    use std::sync::Arc;

    fn open_temp_backend() -> (RocksExternalLineageBackend, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let backend = RocksExternalLineageBackend::open(dir.path().join("db")).unwrap();
        (backend, dir)
    }

    #[test]
    fn rocks_backend_passes_conformance_suite() {
        let (backend, _dir) = open_temp_backend();
        let store = ExternalLineageStore::new(Arc::new(backend));
        ln_test::run_backend_conformance_suite(&store);
    }

    #[test]
    fn rocks_backend_recent_events_newest_first() {
        let (backend, _dir) = open_temp_backend();
        let store = ExternalLineageStore::new(Arc::new(backend));
        store.record(ln_test::event(
            "r1",
            "airflow",
            "j",
            vec![],
            vec![ln_test::dataset("ns", "ds", &[])],
        ));
        std::thread::sleep(std::time::Duration::from_millis(2));
        store.record(ln_test::event(
            "r2",
            "airflow",
            "j",
            vec![],
            vec![ln_test::dataset("ns", "ds", &[])],
        ));
        let events = store.recent_events(10, None, None);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].run_id, "r2", "newest first");
        assert_eq!(events[1].run_id, "r1");
    }

    #[test]
    fn rocks_backend_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        {
            let backend = RocksExternalLineageBackend::open(&db_path).unwrap();
            let store = ExternalLineageStore::new(Arc::new(backend));
            store.record(ln_test::event(
                "persistent-run",
                "airflow",
                "j",
                vec![],
                vec![ln_test::dataset(
                    "warehouse",
                    "staging.orders",
                    &[("id", "int"), ("amount", "decimal")],
                )],
            ));
            // Drop the backend to flush and release RocksDB lock.
        }

        // Reopen the same path.
        let backend = RocksExternalLineageBackend::open(&db_path).unwrap();
        let store = ExternalLineageStore::new(Arc::new(backend));

        let summary = store
            .dataset_summary("warehouse", "staging.orders")
            .expect("dataset survived reopen");
        assert_eq!(summary.columns.len(), 2);
        assert_eq!(summary.event_count, 1);

        let events = store.recent_events(10, None, None);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].run_id, "persistent-run");

        let stats = store.stats();
        assert_eq!(stats.event_count, 1);
        assert_eq!(stats.dataset_count, 1);
    }

    #[test]
    fn rocks_backend_edge_prefix_scans_target_and_source() {
        use conduit_lineage::openlineage::OpenLineageDataset;
        use serde_json::json;
        use std::collections::BTreeMap;

        let (backend, _dir) = open_temp_backend();
        let store = ExternalLineageStore::new(Arc::new(backend));

        let mut output = OpenLineageDataset {
            namespace: "warehouse".to_string(),
            name: "staging.orders".to_string(),
            facets: BTreeMap::new(),
        };
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

        store.record(ln_test::event(
            "r",
            "airflow",
            "j",
            vec![ln_test::dataset("postgres", "raw.orders", &[])],
            vec![output],
        ));

        let inbound = store.edges_targeting("warehouse", "staging.orders");
        assert_eq!(inbound.len(), 1);
        assert_eq!(inbound[0].source_dataset, "postgres/raw.orders");

        let outbound = store.edges_sourced_by("postgres", "raw.orders");
        assert_eq!(outbound.len(), 1);
        assert_eq!(outbound[0].target_dataset, "warehouse/staging.orders");
    }
}
