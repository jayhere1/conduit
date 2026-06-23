# conduit-lineage vs SQLGlot — Positioning & Enhancement Roadmap

**Status:** Strategy / roadmap (2026-06-23)
**Scope:** `conduit-lineage`, `conduit-python`
**Consumer driving this:** mantrix-core (AXIS.AI), the first real external consumer of conduit-lineage.

---

## TL;DR — Positioning

When a downstream team reaches for SQLGlot instead of conduit-lineage, the
instinct to **enhance conduit rather than concede** is correct — *provided we
remember what conduit-lineage is*.

conduit-lineage **is not a SQL parser**. Its public surface
(`conduit-lineage/src/lib.rs:21-64`) is a **lineage platform**:

- `catalog` — table/column catalog for resolution
- `contracts` — schema contracts + validation
- `cross_task` — lineage stitched across tasks/queries
- `impact` + `impact_report` + `plan_impact` — breaking-change / blast-radius
- `openlineage` (emit) + `openlineage_ingest` (Marquez)
- `schema` registry
- `sql_parser` — the column-lineage extractor (built on `sqlparser-rs`)

SQLGlot is only the **`sql_parser`** slice of that — a parser + resolver. So
"just use SQLGlot" trades a platform for a function. We don't concede the
function; we make it good enough that the platform carries the rest.

---

## The competitor: what SQLGlot is, and isn't

**Is:** the most widely-used OSS SQL parser/transpiler — pure Python, 30+
dialects, and a mature `lineage()` + optimizer (`qualify`, `expand_stars`) that,
*given a schema*, resolves bare columns to the right table, expands `SELECT *`,
and qualifies across CTEs/subqueries/joins. It's what DataHub uses for
column-level lineage in production.

**Isn't:** a lineage platform. No schema-diff, no contracts, no impact reports,
no OpenLineage emission, no cross-task graph, no embedded (non-Python) runtime.

That asymmetry is the whole strategy: **don't lose the parser race; win on the
platform.**

---

## Why not concede lineage extraction to SQLGlot

1. **Rust + sqlparser-rs is the right foundation for an embedded orchestrator.**
   An Airflow-competitor that shells out to a Python library for lineage is
   architecturally wrong. sqlparser-rs is the same parser DataFusion (and
   WrenAI's engine) builds on — fast, embeddable, no Python on the hot path.
2. **A real consumer hardens the product for free.** mantrix-core feeds
   conduit-lineage arbitrary LLM-generated SQL across BigQuery / Postgres /
   ClickHouse — the hardest possible corpus. Every divergence it surfaces is a
   concrete test case.
3. **The platform features compound.** OpenLineage interop, contracts, and
   impact reports are what differentiate conduit from every "text-to-SQL with a
   parser" tool. Each consumer that wires into them widens the moat.

---

## The reframe: the binding is starving the consumer

The root cause of the consumer's degraded lineage is **ours, in the binding —
not a flaw in the crate**:

> `conduit-python/src/lineage.rs` exposes only `extract_sql_lineage(sql)` →
> `SqlLineageExtractor::extract` (the **no-catalog** path). There is **no Python
> entry point for `extract_with_catalog`** at all.

The crate already has the correctness machinery — `extract_with_catalog` +
`TableCatalog` (`conduit-lineage/src/catalog.rs`) do bare-column resolution,
`SELECT *` expansion, and CTE propagation, and they're tested
(`catalog_wildcard_expansion`, `catalog_bare_column_without_catalog_defaults_to_first`).
Python callers simply can't reach them. So the single highest-leverage fix is a
**binding change**, not a parser rewrite.

---

## Enhancement roadmap (honest cost)

1. **Binding + catalog (days) — do first, pure upside.**
   Add `extract_sql_lineage_with_catalog(sql, catalog_json, dialect)` to
   `conduit-python`. Accept a JSON catalog (`{schema, table → columns}`) and a
   dialect string. This alone lets the consumer fix join / `SELECT *`
   misattribution, because the Rust resolver already handles it with a catalog.

2. **Per-dialect parsing (~1 wk).**
   `sql_parser.rs:122` hardcodes `GenericDialect`. sqlparser-rs ships
   `BigQueryDialect` / `ClickHouseDialect` / `PostgreSqlDialect` / etc. — take a
   dialect arg and select the dialect. Also: on parse failure, **stop silently
   returning `empty()`** — surface a reason so consumers can fall back
   deliberately instead of getting silent table-level-only lineage.

3. **Multi-arch wheels (days) — cheapest high-visibility win.**
   The only published release (`v0.1.0`) ships a single
   `cp312 / manylinux_2_34 / x86_64` wheel; arm64/macOS requires building from
   source via `maturin`. Add cibuildwheel for arm64 + macOS + py3.13 so
   `pip install` works everywhere and the consumer's local dev / CI stop
   silently skipping lineage.

4. **Resolver hardening (the real investment).**
   Ambiguity resolution, multi-level views/CTEs, expression-column extraction.
   sqlparser-rs gives you the AST but **none** of the semantic resolution —
   this is precisely the part SQLGlot spent years on. Budget for it; don't
   assume the AST walk is "done."

---

## SQLGlot as a differential-testing oracle (not a runtime dep)

Borrow SQLGlot's correctness without depending on it: in conduit-lineage's test
suite, run a corpus of real consumer queries through **both** SQLGlot's
`lineage()` and conduit, assert the column maps agree, and file every divergence
as a bug. This turns SQLGlot's maturity into a free conformance harness for the
Rust resolver — and gives a defensible "we match SQLGlot, plus a platform" claim.

---

## The differentiators to lean into

These are why conduit-lineage exists; make sure consumers can reach them:

- **`diff_schemas` / `contracts` / `impact`** — breaking-change detection. The
  consumer's headline use case is "frozen KPI definitions silently break when an
  upstream column is dropped/retyped." SQLGlot cannot express this; conduit
  already has the primitives. Expose them in the binding next.
- **`openlineage` emit** — make consumers' lineage interoperable with
  DataHub / Marquez / OpenMetadata / Atlan out of the box.
- **`cross_task` / `plan_impact`** — graph-level impact that a per-query parser
  can't provide.

---

## References

- `conduit-python/src/lineage.rs` — Python binding (only `extract_sql_lineage` today)
- `conduit-lineage/src/sql_parser.rs` — extractor; `GenericDialect` at line 122; `extract` vs `extract_with_catalog`
- `conduit-lineage/src/catalog.rs` — `TableCatalog` (resolution machinery, already built)
- `conduit-lineage/src/lib.rs:21-64` — full platform surface
- `.github/workflows/publish-python.yml` — wheel build (extend for multi-arch)
