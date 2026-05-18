# conduit-lineage

Column-level lineage extraction, schema validation, and breaking-change
detection. The flagship differentiator in this crate is **cross-task
lineage**: column-level flow stitched *through* the task graph so a
single trace can walk from a downstream Python task's output column,
through an intermediate SQL transform, back to the upstream Python
task's declared input column.

SQL-to-SQL column lineage exists in SQLMesh, dbt-docs, and
OpenLineage. **Stitching column-level lineage through tasks of
different kinds (Python → SQL → Python) is what this crate adds.**

## Cross-task lineage

### The model
- **`Dataset`** (`conduit_common::dag`) — a schema-qualified named set
  of columns. The interop unit at every task boundary.
- **`@task(inputs=[Dataset(...)], outputs=[Dataset(...)])`** — what a
  Python task reads and writes. Inferred from the SQL AST for SQL
  tasks (`INSERT INTO …`, `CREATE TABLE … AS`, plus an explicit
  `target:` YAML field for plain `SELECT`).
- **`TableCatalog::register_dataset(name, columns, producer)`** —
  ties a dataset's qualified name back to a `TaskRef`. SQL `FROM`
  clauses resolve through this so the SQL parser's output gets
  promoted to task-rooted edges.
- **`stitch(dag) -> CrossTaskLineage`** — walks `dag.execution_order`,
  registers outputs, re-parses SQL with the populated catalog,
  emits a merged `LineageGraph` whose edges span task boundaries.
- **`@dag(lineage_strict=True)`** — opt-in compile error when a
  consumer column has no matching upstream declaration. Off by default
  to keep adoption frictionless.

### Walking the graph

```bash
conduit lineage trace \
    --dag cross_task_lineage_sql \
    --column load.total \
    --dags-path examples/dags
```

```
upstream trace for load.total in DAG 'cross_task_lineage_sql':
  [sql] cross_task_lineage_sql::transform.total
  [sql] cross_task_lineage_sql::seed.amount
  (2 columns / 2 edges traversed)
```

`[sql]` / `[py]` / `[bash]` / `[exec]` annotate each node by task kind
— that's the bit that's hard to get from spec-only OpenLineage tooling.
Add `--downstream` to walk dependents instead of ancestors and
`--format json` to pipe the result.

### OpenLineage emit

`OpenLineageRunEvent::from_sql_lineage_with_catalog(...)` produces
spec-shaped `RunEvent`s plus a Conduit-specific `conduit_task_lineage`
facet (`https://conduit.dev/schemas/conduit_task_lineage/v1`) that
names the upstream task(s) per output column. Inputs whose dataset is
task-produced get a `conduit://<dag_id>` namespace so downstream
consumers can tell apart a physical warehouse table from a Conduit
pipeline output.

### Examples
- `examples/dags/cross_task_lineage.yaml` — three SQL tasks. The
  middle task's `GROUP BY` lets the SQL extractor derive `total =
  SUM(amount)` so a trace from `load.total` reaches `seed.amount`.
- `examples/dags/cross_task_lineage.py` — declarative Python form,
  `lineage_strict=True`. Column edges span tasks where input and
  output names match; future work will add a column-mapping
  declaration to express intra-task derivations like `total =
  SUM(amount)` from Python.

## What else lives here

| Module | Purpose |
|---|---|
| `sql_parser` | `sqlparser-rs` AST walk → `SqlLineage` (output columns, source tables, column mappings, optional INSERT/CTAS target). Handles CTEs, UNION, window functions, wildcard expansion, single-level views, Jinja stripping. |
| `catalog` | `TableCatalog` — physical tables, views, *and* task-produced datasets. |
| `cross_task` | The stitcher described above. |
| `lineage_graph` | `LineageGraph` over `ColumnRef { source: ColumnSource::Table \| Task, column_name }`. Upstream/downstream traversal. |
| `openlineage` | OpenLineage spec event emission, plus the `conduit_task_lineage` facet. |
| `schema` / `contracts` / `impact` | Schema registry, contract validation, breaking-change detection (`SchemaChangeDetector`). |

## Honest limitations
- Single-DAG stitching only. Cross-DAG lineage isn't wired through
  `stitch`; if your Python tasks live in one file and the SQL transform
  lives in a sibling YAML, they're two separate DAGs and the trace
  command only walks one.
- Python column mappings (intra-task `output_col = f(input_cols)`)
  aren't declarable yet — outputs and inputs are dataset-level only.
  The SQL path *does* get column-level mappings because the parser
  derives them from the AST.
- The lineage graph is rebuilt on demand. No persistence yet — for
  large catalogs this will become the bottleneck.
- 80 unit tests + integration tests cover the headline paths; property
  testing (round-trip SQL → lineage → diff) is the next hardening step.

## OpenLineage ingest and the cross-system view

Conduit accepts OpenLineage RunEvents from foreign systems (Airflow,
dbt, Spark, anything that emits the spec) and renders them in the same
plane as its own lineage. Combined with the emit side and Bet 2.2's
stitcher, this is the full lineage surface:

```
Airflow ──┐                           ┌── dbt
          │                           │
          ├── ingest ── Conduit ── emit ──┤
          │              │                │
          │              ▼                ▼
          │       /lineage/datasets/      │
          │       :ns/:name/unified  ◄────┘
          ▼              ▲
       persisted in      │
       RocksDB           │
                Datasets tab in the UI
```

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/openlineage/v1/lineage` | Spec-compliant ingest. Any OpenLineage exporter points at this with no code changes. |
| `GET`  | `/api/v1/openlineage/events?namespace=&dataset=&limit=` | Recent ingested events. |
| `GET`  | `/api/v1/openlineage/datasets/{namespace}/{name}` | External-only aggregate: columns, last producer, edges. |
| `GET`  | `/api/v1/openlineage/stats` | Event / dataset / edge counters. |
| `GET`  | `/api/v1/lineage/datasets/{namespace}/{name}/unified` | **The unified view.** Fuses internal Conduit lineage (via `cross_task::stitch`) with ingested external events into one per-dataset response: producers, schema, upstream, downstream, recent events. This is what the UI **Datasets** tab renders. |

### Storage

Ingested events are persisted to RocksDB at
`{state_dir}/external_lineage_db` (the same state directory that holds
the snapshot store and env history). Three column families:

| CF | Key | Value |
|---|---|---|
| `events` | `{inverted_be_nanos}{run_id}` — forward iteration walks newest-first | `IngestedEvent` JSON |
| `datasets` | `{namespace}/{name}` | aggregated `ExternalDatasetSummary` |
| `edges_by_target` / `edges_by_source` | `{dataset}\0{other}\0{source_col}\0{target_col}\0{run_id}` | `ExternalColumnEdge` JSON |

Plus a `stats` CF for cheap event/edge counters. Prefix scans give
upstream/downstream queries on a dataset in a single iteration.

The store falls back to in-memory only if the on-disk store can't be
opened (e.g. another process holding the lock); a warning is logged and
the public API stays identical. For test code, `ExternalLineageStore::in_memory()`
constructs the in-memory backend directly.

### Trait-based backends

[`ExternalLineageStore`] is a thin facade over an
`ExternalLineageBackend` trait. The two implementations:

- `conduit_lineage::InMemoryExternalLineageBackend` — bounded ring
  buffer, used in tests.
- `conduit_state::RocksExternalLineageBackend` — the production
  backend, durable and unbounded.

Both pass the same `conduit_lineage::testing::run_backend_conformance_suite`,
so behavioural drift between them is caught at test time.

### Worked example

Post an event that looks like an Airflow ETL job writing `staging.orders`:

```bash
curl -sX POST http://localhost:8080/api/v1/openlineage/v1/lineage \
  -H 'Content-Type: application/json' \
  -d '{
    "eventTime": "2026-05-17T12:00:00Z",
    "producer": "https://github.com/apache/airflow",
    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
    "eventType": "COMPLETE",
    "run": {"runId": "550e8400-e29b-41d4-a716-446655440000"},
    "job": {"namespace": "airflow", "name": "etl.extract_orders"},
    "inputs": [{"namespace": "postgres", "name": "raw.orders"}],
    "outputs": [{
      "namespace": "warehouse",
      "name": "staging.orders",
      "facets": {
        "schema": {
          "_producer": "https://github.com/apache/airflow",
          "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
          "fields": [
            {"name": "id", "type": "INTEGER"},
            {"name": "amount", "type": "DECIMAL"}
          ]
        }
      }
    }]
  }'
```

Read back via the unified view:

```bash
curl -s http://localhost:8080/api/v1/lineage/datasets/warehouse/staging.orders/unified | jq
# {
#   "namespace": "warehouse",
#   "name": "staging.orders",
#   "schema": {
#     "columns": [{"name":"id","dtype":"INTEGER"},{"name":"amount","dtype":"DECIMAL"}],
#     "source": "external"
#   },
#   "producers": {
#     "internal": null,
#     "external": {"jobNamespace":"airflow","jobName":"etl.extract_orders","runId":"550e8400-..."}
#   },
#   "upstream":   {"internal": [], "external": []},
#   "downstream": {"internal": [], "external": []},
#   "recentEvents": [ ... ]
# }
```

When Conduit's own DAGs reference `warehouse.staging.orders` downstream,
the same query returns the internal consumer tasks alongside the
external producer — that's the cross-system join in action.

### Marquez round-trip test

`conduit-lineage/tests/marquez_roundtrip.rs` validates the wire format
end-to-end. `#[ignore]`'d by default; bring up Marquez and run:

```bash
docker compose -f docker-compose.marquez.yml up -d
MARQUEZ_URL=http://localhost:5000 \
  cargo test -p conduit-lineage --test marquez_roundtrip -- --ignored --nocapture
docker compose -f docker-compose.marquez.yml down -v
```

The tests emit a Conduit RunEvent (including the `conduit_task_lineage`
facet), POST it to Marquez, read the dataset back via Marquez's REST
API, and assert the facet survives the round trip.

### Pointing external producers at Conduit

Any OpenLineage-spec exporter works out of the box. For example, the
Airflow OpenLineage provider:

```bash
export OPENLINEAGE_URL=https://your-conduit.host/api/v1/openlineage
export OPENLINEAGE_ENDPOINT=v1/lineage
# (Airflow's OpenLineage provider posts to {URL}/{ENDPOINT}.)
```

dbt's `openlineage-dbt` integration and Spark's `openlineage-spark`
listener use the same pair.
