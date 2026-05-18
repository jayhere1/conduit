# Bet 2.2 — Cross-Task Lineage (Python→SQL→Python)

**Status:** Planning. Targets Bet 2.2 from `docs/STRATEGIC_DIRECTION.md` §4.
**Estimate:** ~1–2 weeks across 4 phases.
**Owner:** TBD.

---

## 1. Why this bet

SQL-to-SQL column lineage exists in SQLMesh, dbt-docs, and OpenLineage integrations. **Stitching column-level lineage *through* Python tasks via the task graph does not exist anywhere.** Conduit's compiler already knows every task's place in the DAG and (for SQL tasks) its column flow. Joining those signals turns column-level lineage into pipeline-level lineage and gives Conduit a defensible positioning wedge no current competitor occupies.

---

## 2. Design choices (locked)

| Decision | Choice | Rationale |
|---|---|---|
| Declaration model | **Explicit only**: `@task(outputs=[Dataset(...)], inputs=[...])` | Deterministic, no runtime probing, no fragile type-hint parsing. |
| MVP scope | **Graph + OpenLineage + CLI** | Ships the novel claim *and* the interop story together. |
| Strictness | **Per-DAG opt-in**: `@dag(lineage_strict=True)` | Adoption-friendly default; production DAGs can flip the gate on. |
| Cross-task naming | **Schema-qualified datasets** (`"staging.orders"`) | Matches OpenLineage's interop unit. SQL FROM clauses resolve naturally without Conduit-specific syntax. |

---

## 3. Current state (anchored to code)

- **SQL lineage** — `conduit-lineage/src/sql_parser.rs` (1,440 LOC). `SqlLineage { output_columns, source_tables, column_mappings }`. `ColumnRef.task_id` today holds **source table names**, not actual task IDs.
- **`LineageGraph`** — `conduit-lineage/src/lineage_graph.rs`. Forward/reverse adjacency over `ColumnRef`. Built manually by callers; no automatic DAG-level construction.
- **`TableCatalog`** — `conduit-lineage/src/catalog.rs`. Tracks SQL tables and single-level views. **Does not know Python task outputs.**
- **OpenLineage emit** — `conduit-lineage/src/openlineage.rs`. `OpenLineageRunEvent.inputs/outputs` are dataset refs. `ColumnLineageDatasetFacet` works for SQL only; does not reference upstream tasks.
- **`Task` model** — `conduit-common/src/dag.rs:68`. No `inputs`/`outputs` fields. `DependencyType::DataFlow` exists but is never populated.
- **Python parser** — `conduit-compiler/src/parser.rs`. Extracts `raw_dependencies` from function-arg names; no I/O metadata.
- **`@task` decorator** — `sdk/python/conduit_sdk/decorators.py:42`. No `outputs`/`inputs` kwargs.

**Nothing today bridges Python ↔ SQL at the column level.**

---

## 4. Phase 1 — Data model & declarations (2–3 days)

### 4.1 `conduit-common/src/dag.rs`
- Add types (all additive, `#[serde(default)]` so old DAGs round-trip):
  ```rust
  pub struct ColumnSpec {
      pub name: String,
      pub dtype: Option<String>,
  }

  pub struct Dataset {
      pub name: String,                 // schema-qualified, e.g. "staging.orders"
      pub columns: Vec<ColumnSpec>,
  }
  ```
- On `Task`:
  ```rust
  #[serde(default)] pub inputs:  Vec<Dataset>,
  #[serde(default)] pub outputs: Vec<Dataset>,
  ```
- On `Dag`:
  ```rust
  #[serde(default)] pub lineage_strict: bool,
  ```

### 4.2 Python SDK (`sdk/python/conduit_sdk/`)
- New module `conduit_sdk/lineage.py` exporting `Dataset` and `ColumnSpec` dataclasses.
- `decorators.py`:
  - Extend `@task` signature with `outputs: list[Dataset] | None = None`, `inputs: list[Dataset] | None = None`.
  - Extend `TaskDefinition` with `outputs` / `inputs` fields.
  - Extend `@dag` with `lineage_strict: bool = False`; thread onto `DagDefinition`.
- Re-export `Dataset`, `ColumnSpec` from `conduit_sdk/__init__.py`.

### 4.3 Python parser (`conduit-compiler/src/parser.rs`)
- Extract `outputs=`/`inputs=` literal lists from `@task` decorator AST → `ParsedTask.outputs`/`inputs`.
- Extract `lineage_strict=` from `@dag` → `ParsedDag`.
- Compile-time error if any `Dataset(...)` argument is not a statically resolvable literal (no runtime values).

### 4.4 SQL task I/O inference (`conduit-compiler/src/resolver.rs` or new pass)
- Run `SqlLineageExtractor` at compile time against each SQL task's query.
- Populate `Task.inputs` from `source_tables`.
- Populate `Task.outputs` from the write target + `output_columns`. Write-target resolution order (per R1 audit):
  1. **AST-derived** — `Statement::Insert.table_name` (INSERT INTO …) or `Statement::CreateTable.name` (CREATE TABLE … AS).
  2. **YAML-declared** — new optional `target:` field on `YamlTask` and `TaskType::Sql { connection, query, target: Option<String> }`.
  3. **Fallback** — task id, tagged as anonymous (`Dataset::anonymous(task_id)`). Downstream SQL `FROM` won't resolve to it, but task-graph consumers still can.

### 4.4.1 SQL parser extension (R1 unlock)
- `conduit-lineage/src/sql_parser.rs`: extend `SqlLineage` with `target_table: Option<TableRef>`. In `extract_from_statement`, capture `Insert.table_name` / `CreateTable.name` (currently discarded at lines 138–152).
- Single-commit prerequisite for the inference pass above. Add tests covering CTAS, INSERT INTO, and plain SELECT (target stays `None`).

### 4.5 Tests
- Round-trip a DAG with `outputs=`/`inputs=` declarations through compile + serialize + load; assert preserved.
- Parser test: malformed `Dataset(...)` literal (e.g. variable reference) errors at compile time.
- Inference test: simple `SELECT a, b FROM raw.x` SQL task populates `inputs` and `outputs` correctly.

---

## 5. Phase 2 — Catalog + stitching (3–4 days)

### 5.1 `conduit-lineage/src/catalog.rs`
- New type:
  ```rust
  pub struct TaskRef { pub dag_id: DagId, pub task_id: TaskId }
  ```
- New API:
  ```rust
  pub fn register_dataset(
      &mut self,
      qualified_name: &str,
      columns: &[ColumnSpec],
      producer: TaskRef,
  );
  ```
- Lookup returns `Option<(Vec<CatalogColumn>, Option<TaskRef>)>` — `producer` is `None` for physical tables, `Some` for task-produced datasets.
- **Collision policy:** within a single DAG, producer task wins over physical-table entry. Across DAGs, physical-table entry wins. Documented inline.

### 5.2 `ColumnRef` semantics — breaking change inside the crate
Current `ColumnRef { task_id: String, column_name: String }` overloads `task_id` to mean "source table or task." Replace with:
```rust
pub enum ColumnSource {
    Table(String),          // unqualified table name, as seen in SQL FROM
    Task(TaskRef),          // resolved task producer
}
pub struct ColumnRef {
    pub source: ColumnSource,
    pub column_name: String,
}
```
Update all callsites inside `conduit-lineage`. Public callers go through the new enum.

### 5.3 New file `conduit-lineage/src/cross_task.rs`
```rust
pub struct CrossTaskLineage {
    pub graph: LineageGraph,
    pub unresolved: Vec<UnresolvedRef>,   // consumer columns with no upstream match
}

pub struct UnresolvedRef {
    pub consumer: TaskRef,
    pub dataset: String,
    pub column: String,
    pub reason: UnresolvedReason,         // DatasetNotProduced | ColumnNotDeclared
}

pub fn stitch(dag: &Dag) -> Result<CrossTaskLineage, LineageError>;
```

**Algorithm:**
1. Walk tasks in topo order.
2. For each task:
   - SQL: parse with `SqlLineageExtractor::extract_with_catalog` against the current catalog state. Add per-statement column edges to the graph.
   - Python: read declared `inputs`/`outputs`.
   - Register the task's outputs into the catalog with `producer = TaskRef { dag.id, task.id }`.
3. For every `Task.input` dataset on every task: look up producer in the catalog. For each declared input column, add `LineageEdge { source: ColumnRef::Task(producer).col, target: ColumnRef::Task(self).col, transform_type: Direct }`.
4. If a consumer column has no declared producer column, push to `unresolved` (don't drop silently).
5. If `dag.lineage_strict == true` and `!unresolved.is_empty()`: return `Err(LineageStrictViolation { unresolved })`. Otherwise log a warning per unresolved entry.

**Cycle defense:** `dag.execution_order` already topo-sorts; `debug_assert!` confirms no edge points backward in topo order.

### 5.4 Tests
- 3-task DAG (`py_extract` → `sql_transform` → `py_load`): merged graph has edge from `py_extract.col` to `py_load.col` through `sql_transform`.
- Strict-mode test: column-name mismatch errors under `lineage_strict=True`, warns otherwise.
- Property test: edge set identical regardless of task iteration order (within a valid topo order).
- Collision test: physical table + Python output share a qualified name → producer wins; warning logged.

---

## 6. Phase 3 — OpenLineage emit + CLI (2–3 days)

### 6.1 `conduit-lineage/src/openlineage.rs`
- Per-task `RunEvent.inputs[]` reference upstream datasets by qualified name (works uniformly for physical tables and Python-produced).
- `ColumnLineageDatasetFacet.fields[col].input_fields[*]` is resolved through the catalog:
  - Physical-table producer → existing namespace.
  - Python-task producer → `namespace = "conduit://<dag_id>"`, `name = "<dataset>"`, `field = "<column>"`.
- Add custom facet `conduit_task_lineage`:
  ```json
  {
    "_producer": "https://conduit.dev",
    "_schemaURL": "https://conduit.dev/schemas/conduit_task_lineage/v1",
    "producerTasks": [
      { "dagId": "...", "taskId": "...", "column": "..." }
    ]
  }
  ```
- Test: emit a `RunEvent` for the demo DAG's middle SQL task; assert it includes producer task refs for the Python upstream.

### 6.2 CLI (`conduit-cli`)
- New subcommand:
  ```
  conduit lineage trace --dag <id> --column <task.col>
      [--upstream | --downstream]
      [--format text|json]
  ```
- Runs `stitch(dag)`, walks the merged graph in the requested direction, prints the chain with task IDs annotated by kind (`[py]` / `[sql]`).
- Exits non-zero if column not found in graph.

### 6.3 Tests
- CLI integration test against the example DAG, asserting text output contains all three task IDs in order.
- OpenLineage facet shape test (snapshot or schema-validate).

---

## 7. Phase 4 — Demo + docs (1–2 days)

### 7.1 Example DAG
- `examples/dags/cross_task_lineage.py`: 3 tasks demonstrating the bridge.
  ```python
  @dag(schedule="@daily", lineage_strict=True)
  def cross_task_demo():
      @task(outputs=[Dataset("staging.orders",
                             columns=[ColumnSpec("id"),
                                      ColumnSpec("customer_id"),
                                      ColumnSpec("amount")])])
      def extract_orders(): ...

      # Conventional SQL task in dags/ pointing at staging.orders
      # → reads producer columns through the catalog.

      @task(inputs=[Dataset("analytics.daily_revenue",
                            columns=[ColumnSpec("customer_id"),
                                     ColumnSpec("total")])])
      def push_to_warehouse(data=transform): ...
  ```

### 7.2 Docs
- Section in `conduit-lineage/README.md`: "Cross-task lineage" with before/after Mermaid diagram showing the Python→SQL→Python chain.
- Update `lib.rs` docstring per `STRATEGIC_DIRECTION.md` §8.4 (already a known cleanup).
- Add an entry to `docs/STRATEGIC_DIRECTION.md` §0 once shipped.

---

## 8. Risks & non-obvious points

| ID | Risk | Mitigation |
|---|---|---|
| R1 | **SQL task write target.** ✅ Audited. `TaskType::Sql` and `YamlTask` carry no target field; `sql_parser.rs::extract_from_statement` sees `Insert.table_name` / `CreateTable.name` in the AST but discards them. Real DAGs include all three shapes: CTAS, INSERT INTO, and plain SELECT (no target). | Resolved into Phase 1 (§4.4 + §4.4.1): extract AST targets (free), add optional YAML `target:` field, fall back to anonymous dataset keyed on task id. +½ day. |
| R2 | **Namespace collisions.** `staging.orders` could exist as both a physical table and a Python-produced dataset. | Documented policy (§5.1): producer task wins within DAG; physical-table entry wins across DAGs. Emit warning on collision. |
| R3 | **`ColumnRef` enum migration** touches many callsites inside `conduit-lineage`. | Land §5.2 as its own commit before Phase 2 algorithm work. Public API surfaces the enum from day one. |
| R4 | **Strict-mode adoption pain.** Existing DAGs that opt in will fail until they're annotated. | `lineage_strict` is opt-in (default `false`); ship warnings first, let users tighten when ready. |
| R5 | **OpenLineage namespace for Python tasks** (`conduit://<dag_id>`) is non-standard. | Keep it stable, document in `conduit-lineage/README.md`, ship a Marquez round-trip test in the §4 ingest bet to validate consumer behavior. |

---

## 9. Definition of done

- `cargo test -p conduit-lineage` passes including the new cross-task integration test.
- `conduit lineage trace --dag cross_task_demo --column push_to_warehouse.total` prints a chain that includes `extract_orders`, the SQL task, and `push_to_warehouse`.
- A `RunEvent` emitted for the SQL task in the demo includes `producerTasks` referencing `extract_orders`.
- `examples/dags/cross_task_lineage.py` compiles cleanly with `lineage_strict=True`.
- `conduit-lineage/README.md` documents the feature with a worked example.

---

## 10. Sequencing summary

```
Phase 1  (data model + R1 SQL target extraction)
                             │
                             ▼
                     Phase 2 (catalog + stitcher)
                             │
                ┌────────────┴────────────┐
                ▼                         ▼
        Phase 3a (OpenLineage)      Phase 3b (CLI)
                └────────────┬────────────┘
                             ▼
                     Phase 4 (demo + docs)
```

Phases 3a and 3b can run in parallel once Phase 2 lands. Phase 4 depends on both.
