//! Marquez round-trip integration test.
//!
//! Emits a Conduit OpenLineage `RunEvent` to a live Marquez server,
//! reads back the recorded dataset via Marquez's REST API, and asserts
//! the round trip preserves the spec-shaped fields and the
//! `conduit_task_lineage` facet (Bet 2.2 emit content).
//!
//! ## Running
//!
//! These tests are `#[ignore]` by default — they need a live Marquez.
//!
//! ```sh
//! # bring up the bundled compose file
//! docker compose -f docker-compose.marquez.yml up -d
//!
//! # run the tests (waits up to ~30s for Marquez to be ready)
//! MARQUEZ_URL=http://localhost:5000 \
//!   cargo test -p conduit-lineage --test marquez_roundtrip -- --ignored --nocapture
//! ```
//!
//! Default `MARQUEZ_URL` is `http://localhost:5000` — the value Marquez
//! ships with. Override only if you're running it somewhere else.

use std::collections::BTreeMap;
use std::time::Duration;

use conduit_lineage::lineage_graph::{ColumnRef, TaskRef, TransformType};
use conduit_lineage::{
    CatalogColumn, ColumnType, LineageGraph, OpenLineageEventType, OpenLineageRunEvent,
    OpenLineageSqlEventOptions, SqlLineageExtractor, TableCatalog,
};
use serde_json::Value;

const DEFAULT_MARQUEZ_URL: &str = "http://localhost:5000";
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

fn marquez_url() -> String {
    std::env::var("MARQUEZ_URL").unwrap_or_else(|_| DEFAULT_MARQUEZ_URL.to_string())
}

/// Block (async) until Marquez responds on `/api/v1/namespaces`, or fail
/// the test if it never comes up. We avoid relying on `/health` because
/// Marquez's health endpoint moved between releases.
async fn wait_until_ready(client: &reqwest::Client, base_url: &str) {
    let deadline = std::time::Instant::now() + READY_TIMEOUT;
    let probe = format!("{}/api/v1/namespaces", base_url);
    loop {
        match client.get(&probe).send().await {
            Ok(resp) if resp.status().is_success() => return,
            _ if std::time::Instant::now() < deadline => {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            other => panic!(
                "Marquez at {} did not become ready within {:?} (last status: {:?}). \
                 Set MARQUEZ_URL or run `docker compose -f docker-compose.marquez.yml up -d`.",
                base_url,
                READY_TIMEOUT,
                other.map(|r| r.status())
            ),
        }
    }
}

/// Post a RunEvent to Marquez and assert it accepted us. Marquez returns
/// 201 Created on success.
async fn post_event(client: &reqwest::Client, base_url: &str, event: &OpenLineageRunEvent) {
    let payload = serde_json::to_value(event).expect("event serializes");
    let resp = client
        .post(format!("{}/api/v1/lineage", base_url))
        .json(&payload)
        .send()
        .await
        .expect("POST /api/v1/lineage");
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "Marquez rejected event: {} - {}",
        status,
        body
    );
}

/// Read a dataset back from Marquez. Returns None if it never showed up.
async fn read_dataset(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
    name: &str,
) -> Option<Value> {
    let url = format!(
        "{}/api/v1/namespaces/{}/datasets/{}",
        base_url,
        urlencoding::encode(namespace),
        urlencoding::encode(name),
    );
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<Value>().await.ok()
}

#[tokio::test]
#[ignore = "requires a running Marquez — see file docs"]
async fn emit_and_roundtrip_basic_event() {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let base = marquez_url();
    wait_until_ready(&client, &base).await;

    // Build a minimal SQL lineage and emit a Conduit-shaped RunEvent.
    let lineage = SqlLineageExtractor::extract(
        "INSERT INTO analytics.daily_revenue SELECT id, amount FROM staging.orders",
    );
    let run_id = format!("conduit-roundtrip-{}", uuid_like_suffix());
    let options = OpenLineageSqlEventOptions {
        event_type: OpenLineageEventType::Complete,
        event_time: chrono::Utc::now().to_rfc3339(),
        run_id: run_id.clone(),
        job_namespace: "conduit-test".to_string(),
        job_name: "marquez-roundtrip-basic".to_string(),
        dataset_namespace: "conduit-test-warehouse".to_string(),
        output_dataset: "analytics.daily_revenue".to_string(),
        producer: conduit_lineage::CONDUIT_OPENLINEAGE_PRODUCER.to_string(),
    };
    let event = OpenLineageRunEvent::from_sql_lineage(&lineage, options);

    post_event(&client, &base, &event).await;

    // Poll Marquez for the dataset — it's recorded synchronously but
    // we leave room for slow CI containers.
    let ds = poll_for_dataset(
        &client,
        &base,
        "conduit-test-warehouse",
        "analytics.daily_revenue",
    )
    .await
    .expect("Marquez did not record output dataset");

    // Marquez echoes the namespace+name we posted, regardless of facets.
    assert_eq!(
        ds.get("namespace").and_then(|v| v.as_str()),
        Some("conduit-test-warehouse")
    );
    assert_eq!(
        ds.get("name").and_then(|v| v.as_str()),
        Some("analytics.daily_revenue")
    );
}

#[tokio::test]
#[ignore = "requires a running Marquez — see file docs"]
async fn emit_with_task_lineage_facet_survives_roundtrip() {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let base = marquez_url();
    wait_until_ready(&client, &base).await;

    // Build a catalog where `staging.orders` is produced by a Conduit
    // task so the emitted event includes the `conduit_task_lineage` facet.
    let mut catalog = TableCatalog::new();
    catalog.register_dataset(
        "staging.orders",
        vec![
            CatalogColumn::new("id", ColumnType::Integer),
            CatalogColumn::new("amount", ColumnType::Integer),
        ],
        TaskRef::new("warehouse", "extract_orders"),
    );

    let lineage = SqlLineageExtractor::extract_with_catalog(
        "INSERT INTO analytics.daily_revenue SELECT id, amount FROM staging.orders",
        &catalog,
    );
    let run_id = format!("conduit-roundtrip-facet-{}", uuid_like_suffix());
    let options = OpenLineageSqlEventOptions {
        event_type: OpenLineageEventType::Complete,
        event_time: chrono::Utc::now().to_rfc3339(),
        run_id: run_id.clone(),
        job_namespace: "conduit-test".to_string(),
        job_name: "marquez-roundtrip-facet".to_string(),
        dataset_namespace: "conduit-test-warehouse".to_string(),
        output_dataset: "analytics.daily_revenue".to_string(),
        producer: conduit_lineage::CONDUIT_OPENLINEAGE_PRODUCER.to_string(),
    };
    let event = OpenLineageRunEvent::from_sql_lineage_with_catalog(&lineage, options, &catalog);

    // Sanity: our emit must actually attach the facet.
    let outputs = &event.outputs;
    assert_eq!(outputs.len(), 1);
    assert!(
        outputs[0].facets.contains_key("conduit_task_lineage"),
        "facet missing from emitted event: {:?}",
        outputs[0].facets.keys().collect::<Vec<_>>()
    );

    post_event(&client, &base, &event).await;

    let ds = poll_for_dataset(
        &client,
        &base,
        "conduit-test-warehouse",
        "analytics.daily_revenue",
    )
    .await
    .expect("Marquez did not record output dataset");

    // Marquez merges custom dataset facets under `facets`. Our custom
    // facet should round-trip with its producer + schemaURL intact.
    let facets = ds
        .get("facets")
        .and_then(|v| v.as_object())
        .expect("Marquez response missing facets");
    let task_facet = facets
        .get("conduit_task_lineage")
        .expect("conduit_task_lineage facet not echoed by Marquez");
    assert_eq!(
        task_facet.get("_producer").and_then(|v| v.as_str()),
        Some(conduit_lineage::CONDUIT_OPENLINEAGE_PRODUCER),
        "task facet producer mangled: {:?}",
        task_facet,
    );

    // The facet's `fields` map should contain `amount` with a
    // producer task ref to extract_orders.
    let fields = task_facet
        .get("fields")
        .and_then(|v| v.as_object())
        .expect("task facet missing fields map");
    let amount_producers = fields
        .get("amount")
        .and_then(|v| v.as_array())
        .expect("amount field missing in facet");
    assert!(
        amount_producers
            .iter()
            .any(|p| p.get("taskId").and_then(|v| v.as_str()) == Some("extract_orders")),
        "amount producers don't include extract_orders: {:?}",
        amount_producers
    );
}

/// Poll until the dataset is queryable. Marquez can take a moment under
/// heavy load even though writes are synchronous.
async fn poll_for_dataset(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
    name: &str,
) -> Option<Value> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(v) = read_dataset(client, base_url, namespace, name).await {
            return Some(v);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn uuid_like_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{:x}", nanos)
}

// Suppress unused warnings on items only used in #[ignore]'d tests so
// `cargo test --no-run` stays clean.
#[allow(dead_code)]
fn _suppress_unused() {
    let _ = (LineageGraph::new(),);
    let _ = ColumnRef::table("x", "y");
    let _ = TransformType::Direct;
    let _: BTreeMap<String, Value> = BTreeMap::new();
}
