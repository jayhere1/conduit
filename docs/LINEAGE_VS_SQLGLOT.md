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
(`conduit-lineage/src/lib.rs`) is a **lineage platform**: `catalog`, `contracts`
(schema contracts + validation), `cross_task` lineage, `impact` / `impact_report`
/ `plan_impact`, `openlineage` (emit) + `openlineage_ingest` (Marquez), `schema`
registry, and `sql_parser` (the extractor, built on `sqlparser-rs`).

SQLGlot is only the **`sql_parser`** slice of that. "Just use SQLGlot" trades a
platform for a function. We don't concede the function; we make it good enough
that the platform carries the rest.

---

## Current state — most of the roadmap already shipped (locally)

> ⚠️ The GitHub `v0.1.0` release is **8 commits behind** local `main`. Judge the
> crate by local source, not the published wheel.

Recent local commits already did the hard parser work:

- `7228999 feat(lineage): dialect-aware SQL parsing (Snowflake, BigQuery, Redshift, …)`
- `1ad246b feat(lineage): dbt manifest-aware Jinja resolution`
- `fa196c2 feat(lineage): stitch_with_dbt_manifest + lineage trace --dbt-manifest`

So in `conduit-lineage/src/sql_parser.rs` we **already have**:

- `SqlDialect` enum — 13 dialects (BigQuery, ClickHouse, Snowflake, Redshift,
  Postgres, DuckDB, MySQL, MsSQL, Hive, Databricks, SQLite, ANSI, Generic).
- `SqlDialect::from_connection_type("bigquery" | "postgresql" | "clickhouse" | …)`
  — maps a provider/connection string straight to a dialect (case-insensitive,
  with aliases; unknown → `Generic`).
- `extract`, `extract_with_dialect`, `extract_with_catalog`,
  `extract_with_catalog_and_dialect`, and
  `extract_with_full_context(sql, catalog, dialect, manifest)`.
- Jinja stripping + dbt-manifest `ref()`/`source()` resolution.

**The one thing missing is the Python binding.** `conduit-python/src/lineage.rs`
registers only `extract_sql_lineage`, `trace_column`, `diff_schemas`. None of the
catalog/dialect/full-context entry points are reachable from Python — so the
consumer is stuck on the no-catalog, Generic-dialect path even though the crate
has everything it needs.

> The consumer's degraded lineage is not a crate flaw and no longer even a
> "missing feature" — it's a **binding that hasn't caught up to the crate**.

---

## The competitor: what SQLGlot is, and isn't

**Is:** the most widely-used OSS SQL parser/transpiler — pure Python, 30+
dialects, mature `lineage()` + optimizer (`qualify`, `expand_stars`) that, *given
a schema*, resolves bare columns, expands `SELECT *`, and qualifies across
CTEs/subqueries/joins. It's what DataHub uses in production.

**Isn't:** a lineage platform. No schema-diff, no contracts, no impact reports,
no OpenLineage emission, no cross-task graph, no embedded (non-Python) runtime.

That asymmetry is the strategy: **don't lose the parser race; win on the platform.**

---

## Why not concede lineage extraction to SQLGlot

1. **Rust + sqlparser-rs is the right foundation for an embedded orchestrator.**
   An Airflow-competitor that shells out to a Python lib for lineage is
   architecturally wrong. Same parser DataFusion (and WrenAI's engine) build on.
2. **A real consumer hardens the product for free.** mantrix feeds conduit
   arbitrary LLM-generated SQL across BigQuery / Postgres / ClickHouse — the
   hardest possible corpus. Every divergence is a concrete test case.
3. **The platform features compound.** OpenLineage interop, contracts, impact
   reports are what differentiate conduit from every "text-to-SQL with a parser"
   tool. Each consumer that wires in widens the moat.

---

## Enhancement roadmap (honest cost — revised to local reality)

1. **Binding (hours, not days) — the whole ballgame now.**
   Add a pyfunction wrapping `extract_with_catalog_and_dialect` (or
   `extract_with_full_context`):
   `extract_sql_lineage_with_catalog(sql, catalog_json, dialect: str)`.
   Build the `TableCatalog` from `catalog_json` (`{schema, table → columns}`),
   resolve the dialect via `SqlDialect::from_connection_type(dialect)`. Register
   it in `create_module`. This single change unlocks dialect-correct,
   catalog-resolved lineage for every Python consumer — the work is already done
   in Rust, it just isn't exported.

2. ~~Per-dialect parsing~~ — **shipped** (`7228999`). No new work beyond passing
   the connection-type string through the binding above.

3. **Multi-arch wheels (days) — cheapest high-visibility win.**
   The only published release ships a single `cp312 / manylinux / x86_64` wheel;
   arm64/macOS require building from source via `maturin`. Add cibuildwheel for
   arm64 + macOS + py3.13 so `pip install` works everywhere and consumers' local
   dev / CI stop silently skipping lineage. Cut a `v0.1.1` once the binding lands.

4. **Resolver hardening (the real, ongoing investment).**
   Ambiguity, multi-level views/CTEs, expression-column extraction. sqlparser-rs
   gives the AST but none of the semantic resolution — this is the part SQLGlot
   spent years on. Budget for it.

---

## SQLGlot as a differential-testing oracle (not a runtime dep)

Borrow SQLGlot's correctness without depending on it: in conduit-lineage's test
suite, run a corpus of real consumer queries through **both** SQLGlot's
`lineage()` and conduit (catalog + dialect), assert the column maps agree, and
file every divergence as a bug. SQLGlot's maturity becomes a free conformance
harness for the Rust resolver — and backs a defensible "we match SQLGlot, plus a
platform" claim.

---

## The differentiators to lean into

Why conduit-lineage exists — make sure consumers can reach these:

- **`diff_schemas` / `contracts` / `impact`** — breaking-change detection.
  `diff_schemas` is already bound; `contracts`/`impact` are not. The consumer's
  headline use case is "frozen KPI definitions silently break when an upstream
  column is dropped/retyped." SQLGlot cannot express this. Expose these next.
- **`openlineage` emit** — make consumers' lineage interoperable with
  DataHub / Marquez / OpenMetadata / Atlan out of the box.
- **dbt-manifest (`extract_with_full_context`)** — `ref()`/`source()` resolution
  is already implemented; surfacing it via the binding is a near-free
  differentiator for dbt shops.
- **`cross_task` / `plan_impact`** — graph-level impact a per-query parser can't give.

---

## References

- `conduit-python/src/lineage.rs` — Python binding (registers only
  `extract_sql_lineage` / `trace_column` / `diff_schemas` today)
- `conduit-lineage/src/sql_parser.rs` — `SqlDialect` enum + `from_connection_type`;
  `extract` / `extract_with_dialect` / `extract_with_catalog` /
  `extract_with_catalog_and_dialect` / `extract_with_full_context`
- `conduit-lineage/src/catalog.rs` — `TableCatalog` (resolution machinery)
- `conduit-lineage/src/lib.rs` — full platform surface
- `.github/workflows/publish-python.yml` — wheel build (extend for multi-arch)
- Local `main` is ahead of GitHub `origin/main` by 8 commits — push before
  judging the published wheel.
