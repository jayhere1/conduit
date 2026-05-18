# Conduit: Strategic Direction Review

**Date:** 2026-05-18 (refreshed)
**Status:** Working notes — premise review, feasibility assessment, and direction recommendations. Updated after a ~10-commit progress check, then again after the Bet 5 (plan/apply) and Bet 3 (observability finish) work landed.
**Assumptions for this version:** team expertise is available, internal users already depend on the platform, deletion is off the table.

---

## 0. Progress since prior review

Shipped between the first draft of this doc and this refresh:

- **WebhookAlertHook — `Dag.on_failure` finally fires** (`c069cbb`). The `on_failure: Option<String>` field has been parsed by the compiler since the first lineage commit but never wired to a transport — the textbook §8.7 "claim without code link" pattern. Closes it: `WebhookAlertHook { url, client }` in `conduit-scheduler/src/alerts.rs` POSTs the `AlertEvent` JSON via reqwest with a configurable timeout (default 10s); `ScopedHook<H: AlertHook>` filter adapter scopes a hook to one `dag_id`; `Scheduler::with_dag_failure_webhooks()` builder auto-registers one `ScopedHook<WebhookAlertHook>` per DAG whose `on_failure` is set. Webhook build failures (TLS / malformed URL) log + skip rather than crashing the scheduler. Three new tests in `conduit-scheduler/src/alerts.rs`: round-trip against an in-process `tokio::net::TcpListener` (no new test deps), scoped-filter drops non-matching dag_id, unreachable URL surfaces a useful error message. Internal teams that want PagerDuty / Slack / Opsgenie still write `impl AlertHook`; the lowest common denominator now works without Rust.
- **dbt manifest threaded through cross-task stitcher + CLI** (`fa196c2`). The follow-up to the `1ad246b` slice. New `cross_task::stitch_with_dbt_manifest(dag, Option<&DbtManifest>)` calls the extractor's `extract_with_full_context` so resolved refs flow into the catalog and produce real upstream column edges. `stitch(dag)` is now a thin wrapper passing `None`. CLI: `conduit lineage trace --dbt-manifest <path>` loads the manifest (load failures are loud — the operator asked for resolution) and threads it through. `stitch_with_dbt_manifest_resolves_ref_to_real_table` integration test in `conduit-lineage/tests/cross_task_e2e.rs` contrasts both paths — without manifest the upstream trace doesn't reach the producer (placeholder breaks the chain), with manifest it does. The asymmetric assertions keep the test load-bearing if a future cargo update accidentally fixes the placeholder path.
- **dbt template semantic awareness — shipped** (`1ad246b`). The §4.3 stretch item. Previous Jinja stripping replaced every `{{ ... }}` block with a `__conduit_jinja_N__` placeholder — lineage stopped at the template boundary semantically. Render-then-parse approach: new `conduit-lineage/src/dbt_manifest.rs` defines a minimal subset of dbt's `manifest.json` (`DbtManifest { nodes, sources }`, `DbtNode { name, resource_type, database, schema, alias, package_name }`, `DbtSource { source_name, name, database, schema, identifier }`) with `resolve_ref(package, name)` and `resolve_source(source, table)` lookups, preferring `resource_type == "model"` on name collisions (dbt's own order). The Jinja pre-processor in `sql_parser.rs` recognises `{{ ref('name') }}` / `{{ ref('package', 'name') }}` / `{{ source('s', 't') }}` shapes (single or double quotes, whitespace-tolerant; hand-rolled parser, no regex dep) and substitutes the resolved `database.schema.alias` identifier. Unresolved refs / no-manifest cases fall through to the existing placeholder, so partial dbt adoption keeps working. New `SqlLineageExtractor::extract_with_full_context(sql, catalog, dialect, manifest)` entry point. 12 new tests across `dbt_manifest::tests` and `sql_parser::tests::jinja_*`. Auto-loading the manifest in `cross_task::stitch` from a DAG-level config knob is the next slice — the plumbing is in place, the discovery is the follow-up.
- **Dialect-aware SQL parsing — shipped** (`7228999`). The §4.4 stretch item: SQL lineage was pinned to `GenericDialect`, which silently mis-parsed (or failed on) Snowflake `COPY INTO` / semi-structured `obj:path` access, BigQuery `UNNEST`, Redshift `DISTSTYLE`, MsSql `TOP`, MySQL backtick quoting. New `SqlDialect` enum in `conduit-lineage/src/sql_parser.rs` with 13 variants (Generic / Snowflake / BigQuery / Redshift / Postgres / MySql / SQLite / DuckDb / ClickHouse / MsSql / Hive / Databricks / Ansi), each mapping to one `sqlparser::dialect::*Dialect`. `SqlDialect::from_connection_type(&str)` maps DAG YAML connection-type strings to a dialect (case-insensitive, aliases like `gcp`/`bq`/`postgresql`/`sqlserver` honoured, unknown values fall through to `Generic` for backward compat). New extractor entry points `extract_with_dialect` / `extract_with_catalog_and_dialect` alongside the existing methods. `cross_task::stitch` reads each SQL task's `connection` field and picks the dialect automatically — a `connection: snowflake` task now parses with `SnowflakeDialect` instead of `Generic`. Tests `dialect_from_connection_type_handles_known_aliases`, `snowflake_dialect_parses_semi_structured_access`, `bigquery_dialect_parses_unnest`, `default_dialect_is_generic_and_preserves_existing_behavior` in `conduit-lineage/src/sql_parser.rs`.
- **Bet 5 — plan/apply workflow (rollback + partial apply + readable diff) — shipped.** Three coordinated slices that take plan/apply from demoable to CI-grade. **Rollback** (`5948960`, `2fb20b4`): `cmd_apply` no longer mutates a local `Environment` clone that never made it back through `env_manager`. The new `EnvironmentManager::apply_snapshot_map(env, new_map, plan_id)` write-through captures the prior `snapshot_map` as a history entry tagged `EnvHistoryReason::Apply { plan_id }` before mutating, so `conduit env rollback <env>` reverts the most recent apply with the same mechanism that already covered promotions. The CLI prints the captured version on success with the exact rollback command the operator can run; `conduit env history` and the UI's Environments history modal both render `apply (plan <plan_id>)` rows. Tests `apply_snapshot_map_captures_prior_state` + `apply_then_rollback_round_trips` + `apply_without_history_store_still_mutates` in `conduit-state/src/environment_manager.rs`. **Partial apply** (`15b7890`, `2fb20b4`): `DeploymentPlan::filtered_to(&plan, &selectors) -> Result<…, PartialApplyError>` narrows a plan to selected `(dag_id, task_id)` pairs plus the transitive upstream Execute / Reuse / Remove they depend on; Skip upstream are dropped (no-op, no point including). `PartialApplyError::EmptySelection` and `UnknownSelectors(Vec<(dag, task)>)` surface operator errors instead of silent no-ops. CLI: `conduit apply <env> --only DAG.TASK` (repeatable). Tests `filtered_to_selects_single_task_and_auto_includes_upstream` + `filtered_to_skips_unchanged_upstream` + `filtered_to_errors_on_unknown_selector` + `filtered_to_rejects_empty_selection` + `filtered_to_preserves_plan_id_and_target` in `conduit-planner/src/deployment_plan.rs`. **Readable diff** (`014aaa8`): rewrote `ChangeSet::Display` to group changes by DAG (BTreeMap for deterministic order), label each kind in words ("new task", "task changed", "upstream changed", "task removed"), and render fingerprint deltas per kind — Added shows `(fp <new>)`, Removed shows `(was fp <old>)`, Modified / UpstreamInvalidated shows `(fp <old> → <new>)`. When nothing differs, the diff says so explicitly. Tests `display_groups_changes_by_dag_with_fingerprints` + `display_renders_old_to_new_fingerprint_for_modified` + `display_reports_clean_when_nothing_changed` in `conduit-planner/src/change_detector.rs`.
- **Bet 3 — observability finish — shipped.** The three remaining Bet 3 items from the prior revision (OTel exporter, alert hook surface, structured run logs queryable from UI) all landed. **OTLP tracing exporter** (`e04afff`): new `conduit-cli/src/tracing_setup.rs` module with `pub fn init_tracing(verbose: bool)` replacing the previous 5-line `tracing_subscriber::fmt().init()` call. Gated behind a default-off `otel` cargo feature on `conduit-cli` so the standard build pulls no new deps. Under the feature, `OTEL_EXPORTER_OTLP_ENDPOINT` (env var, no CLI flag) activates the exporter with service name `"conduit"` on the OTel `Resource`, W3C `TraceContextPropagator` installed for distributed-trace context, and graceful fmt-only fallback if the exporter init fails (so a misconfigured collector can't kill the CLI). Crate versions: `opentelemetry 0.27` + `opentelemetry_sdk 0.27` + `opentelemetry-otlp 0.27` + `tracing-opentelemetry 0.28` (the matching counterpart for otel 0.27 — `tracing-opentelemetry`'s version lags by one minor). Both `cargo check --workspace` (no feature) and `cargo check -p conduit-cli --features otel` are clean. **Alert hook surface** (`1abe114`): `Dag.on_failure: Option<String>` was previously parsed by both the Python decorator AST extractor and the YAML parser but never fired. New `conduit-scheduler/src/alerts.rs` defines `AlertEvent` (dag_id, run_id, status, started_at, completed_at, failed_tasks: `Vec<(TaskId, String)>`, config), `AlertStatus` (Failed | Cancelled — Success has no mapping by design so impls have a closed set), and an `AlertHook` async trait with `fire(&self, &AlertEvent) -> Result<(), String>`. `Scheduler::with_alert_hook(Arc<dyn AlertHook>)` registers hooks at build time; `check_dag_run_complete` builds the event and spawns each hook on the tokio runtime so a slow PagerDuty / Slack call can't stall the scheduler. Hook errors are logged and swallowed — alert delivery is never load-bearing. No transport impl ships in-tree (internal teams plug their own); a `WebhookAlertHook` reading the `Dag.on_failure` URL is the natural next slice but deferred until a real internal user asks. Tests `alert_hook_fires_on_dag_failure` + `alert_hook_does_not_fire_on_success` in `conduit-scheduler/tests/scheduler_integration_test.rs` (capturing-hook fixture). **Structured run logs queryable from UI** (`612f3a6`): `GET /api/v1/events` and `GET /api/v1/events/:sequence` have been placeholders since the API crate landed — they returned empty arrays with a "Event store query will be backed by RocksDB" note. Meanwhile the executor / scheduler / plan-apply paths have been writing structured `EventKind` records into `state_dir/events` the whole time, and the UI's `Events.jsx` plus per-run views have been consuming those empty placeholders. Wired now: `AppState` gains `event_store: Option<Arc<EventStore>>` opened in `with_options` from `state_dir/events` (open failures log and degrade to `None`, so the endpoint reports an empty result rather than 500-ing on a fresh box); `list_events` reads from the real store via `range(from, to)` and accepts new filter params `run_id`, `dag_id`, `task_id` alongside the existing `event_type`. Event-type matching is case-insensitive (`?event_type=taskfailed` ≡ `TaskFailed`). Response shape adds `total` / `returned` / `current_sequence` so the UI can paginate properly. `get_event` queries `store.get(sequence)` and 404s on missing. Tests `run_id_filter_matches_task_and_dag_events` + `event_type_filter_is_case_insensitive` + `task_id_filter_drops_non_task_events` + `event_type_name_matches_serde_tag` in `conduit-api/src/handlers/events.rs`.
- **OpenLineage ingest (Bet 2.1) — shipped** (`02b74cb`). Conduit now both emits and ingests OpenLineage. `POST /api/v1/openlineage/v1/lineage` accepts spec-shaped `RunEvent`s — any compliant producer (Airflow, dbt, Spark) points at it with just a base URL change. Read surface: `GET /api/v1/openlineage/events`, `GET /api/v1/openlineage/datasets/:ns/:name`, `GET /api/v1/openlineage/stats`. **Persistence is the default**: ingested events are durable in a dedicated RocksDB instance at `{state_dir}/external_lineage_db` (column families: `events` keyed by inverted-nanos for newest-first iteration; `datasets`; `edges_by_target`/`edges_by_source` for symmetric prefix scans; `stats` for counters). The store is trait-based — `conduit_lineage::ExternalLineageBackend` has an in-memory implementation in `conduit-lineage` and the durable RocksDB implementation in `conduit-state::RocksExternalLineageBackend`; both pass the same conformance suite. **Unified dataset view**: `GET /api/v1/lineage/datasets/:ns/:name/unified` fuses internal lineage (via `cross_task::stitch` over the compiled DAGs) with ingested external events into one response — producers (internal + external), schema, upstream + downstream column edges from both planes, recent events. The **Lineage page's Datasets tab** in `conduit-ui/src/pages/Lineage.jsx` renders this as the canonical cross-system lineage UI: an operator can type `warehouse / staging.orders` and see "Airflow produces this, Conduit's `transform` task consumes it, Spark reads downstream" in one glance. **Plan + stitched-lineage cache**: `conduit_api::plan_cache::PlanCache` keeps `Arc<ConduitPlan>` and per-DAG `Arc<CrossTaskLineage>` in-process; invalidation is signature-keyed (per-file `(path, mtime_nanos, size)` hash filtering editor noise) with an optional TTL ceiling; double-checked locking prevents thundering-herd recompiles. The unified view's two former compile-on-every-request paths now share one cache lookup. Observability + manual flush: `GET /api/v1/lineage/cache/stats` (hits, misses, last_compile_ms, cached/stitched DAG counts), `POST /api/v1/lineage/cache/invalidate`, and a hits/misses badge + "flush cache" button on the UI Datasets tab. The architecture chose a side store over a third `ColumnSource::Foreign` variant so Bet 2.2's stitcher and the impact analyzer stay untouched. **Marquez round-trip** at `conduit-lineage/tests/marquez_roundtrip.rs` (env-gated) validates emit → Marquez ingest → Marquez read-back end-to-end, including the `conduit_task_lineage` facet survival. `docker-compose.marquez.yml` ships for local validation. Docs: `conduit-lineage/README.md` § "OpenLineage ingest and the cross-system view".
- **Bet 7 — impact analysis as a CI gate — shipped** (`02b74cb`). New `conduit impact` CLI: `--base-plan <p> --head-plan <p>` (plan-file mode, paths auto-detect compiled JSON vs DAGs directory) and `--base <ref> --head <ref|WORKING>` (git mode, uses `git worktree`; `WORKING` token compiles uncommitted working tree). Diffs `Task.outputs` per task across plans via `SchemaChangeDetector`, traces downstream blast via `cross_task::stitch`. Markdown + JSON output formats with `lineage_coverage` metric. GH Action at `.github/workflows/conduit-impact.yml` — sticky PR comment via `actions/github-script`, merge gate on `allow-breaking` label. Release-binary infra: `.github/workflows/release.yml` (tag-only `v*`, 4-target matrix musl + native darwins via pinned `cross`, `.sha256` sidecars), `scripts/install.sh` (POSIX, `$HOME/.local/bin`, no sudo). Tarball convention: `conduit-{version}-{platform}.tar.gz`.
- **Bet 1 — virtual environments experience layer — shipped (1.1–1.6).** `Environment::diff()` + `EnvironmentDiff` types; `conduit env diff <a> <b>` (git-style); `EnvHistoryStore` (file-per-version under `.conduit/env_history/{env}/{version:06}.json`, atomic write); `EnvironmentManager::promote()` captures prior `snapshot_map`; `rollback(env, to_version)` with `EnvironmentRolledBack` events; `conduit env history` + `conduit env rollback`; promotion policies (`require_source`, `min_age_secs` with "newest snapshot bake time" semantics) wired through CLI + API; `environment` filter on `/runs`; UI Environments page gains history modal + rollback button + policy editor + Runs link; per-run env column on Runs table with click-to-filter.
- **Cross-task lineage (Bet 2.2) — shipped** (`02b74cb`). Python → SQL → Python column-level lineage stitched through the task graph. New `Dataset` / `ColumnSpec` model on `Task`, `@task(inputs=…, outputs=…)` + `@dag(lineage_strict=…)` in the SDK, decorator-AST extraction in the Python parser, SQL I/O inference (`INSERT INTO` / `CREATE TABLE AS` AST + YAML `target:` + anonymous fallback), `TableCatalog::register_dataset` + `lookup_producer`, `cross_task::stitch(dag) -> CrossTaskLineage`. `ColumnRef` now wraps `ColumnSource::Table | Task`. New CLI `conduit lineage trace --dag X --column task.col [--upstream|--downstream] [--format text|json]`. OpenLineage emit gains a `conduit_task_lineage` facet referencing producer tasks; input dataset namespaces become `conduit://<dag_id>` when the source is a task-produced dataset. Examples: `examples/dags/cross_task_lineage.yaml` (full SQL chain — `load.total → transform.total → seed.amount`) and `examples/dags/cross_task_lineage.py` (declarative Python form). Plan: `docs/CROSS_TASK_LINEAGE_PLAN.md`.
- **Ingest helper dedup** (`02b74cb`). `conduit-state/src/external_lineage_store.rs`'s RocksDB backend previously inlined ~70 lines of `flatten_event` / `extract_column_edges` / `extract_columns_from_schema_facet` / `qualify_dataset` copies that duplicated `conduit-lineage/src/openlineage_ingest.rs` — and the lineage-crate copies were `pub(crate)` and unused because `InMemoryBackend::record` also inlined its own copy. Three implementations of the same five helpers, drifting independently. Resolved by promoting the conduit-lineage helpers to `pub`, re-exporting from `conduit_lineage::`, rewriting `InMemoryBackend::record` to call them, and deleting the conduit-state copies. Single source of truth across both backends.
- **OpenLineage emit** — `conduit-lineage/src/openlineage.rs` (414 LOC, 2 tests). Builds spec-shaped `RunEvent`s with columnLineage facets. CLI + API expose `--openlineage`. No transport layer yet — caller forwards to Marquez/DataHub.
- **Jinja stripping in SQL parser** — regex placeholder substitution so `sqlparser-rs` doesn't choke on dbt-style templated SQL. *Not* semantically template-aware: no `ref()`/`source()` resolution, no macro context, no variable binding.
- **View resolution in `TableCatalog`** — `register_view()` extracts output columns from a defining SQL statement and registers the view as a pseudo-table for downstream `SELECT *` expansion and bare-column resolution. Single-level (views cannot reference views).
- **Provider stub flag exposed** — `ProviderInfo.is_stub`. Current count: **12 real, 20 stub** (real = BigQuery, DuckDB, MySQL, Postgres, Redshift, Snowflake, SQLite, TimescaleDB, CockroachDB, GCS, S3, HTTP).
- **CLI compile-time warning** when DAGs route through stub providers.
- **Distributed reliability:**
  - Eager re-dispatch when a worker's gRPC stream drops (avoids 30s+ orphaning under load).
  - SIGTERM-aware graceful shutdown on workers (drains in-flight tasks before exit).
  - Chaos test for at-most-once result delivery (kill worker mid-task, verify no duplicate completions).
- **Scheduler dedup** — prevents duplicate dispatch under event storms.
- **End-to-end `conduit run`** now actually works.
- **Lineage benchmarks** — `conduit-bench/benches/lineage_bench.rs` (criterion, parameterized 10–500 tasks/columns).
- **Prometheus `/metrics`** endpoint live at `conduit-api/src/handlers/prometheus.rs`.
- **Bet 3 observability slice — shipped.** The existing Prometheus registry now exposes labeled task lifecycle throughput (`conduit_task_events_total{dag_id,task_id,status}`), per-task latency histograms (`conduit_task_duration_by_task_seconds{dag_id,task_id}`), and DAG run latency histograms (`conduit_dag_run_duration_seconds{dag_id}`). The scheduler records dispatch/completion/failure/retry/skip events plus DAG completion durations. Executor and `ProcessRunner` paths emit structured `tracing` spans around dispatch, task execution, subprocess execution, sensor polling, native SQL execution, success, failure, timeout, and retry request events; these are ready for an OpenTelemetry layer when the runtime wires one in.
- **Lineage size now ~3,200 LOC** across 8 files, 69 unit tests (sql_parser alone is 1,440 LOC).

The shape of the work is encouraging: most commits are *depth* (reliability, correctness, completeness in existing crates), not new surfaces.

---

## 1. Premise

Conduit pitches itself as a **Rust-native Airflow replacement** with virtual environments, fingerprint-based change detection, event-driven scheduling, and 32 providers. The stakeholder brief frames it as a "production-ready candidate" at v0.2.

### The genuinely interesting kernel
Tree-sitter compile-time DAG parsing + content-addressable fingerprinting + Terraform-style plan/apply over virtual environments, written in Rust. Two or three real ideas that don't exist in this combination anywhere else.

The virtual-environment primitives exist in code (`conduit_common::snapshot::Environment` with `fork()`/`promote_into()`/`diff_count()`, full `EnvironmentManager` with create/promote/list/delete, JSON persistence, REST handlers). The unfinished work is the experience layer on top — rollback, env-aware planner output, human-readable diffs, env-scoped run history. Plans for those are in §3 Bet 1 and §8.

### What's wrapped around the kernel
- React UI (17 pages, ~9.8k LOC)
- VS Code extension
- Distributed gRPC coordinator/worker
- RBAC with 22 permissions
- Python SDK with Airflow-shaped operators
- Lineage engine (column-level)
- WASM build
- mdBook docs site
- 32 providers (12 real, 20 stubs by `ProviderInfo.is_stub`)
- CLI (compile, run, plan, apply, backfill, worker, cluster)

---

## 2. Feasibility — honestly

### Competing head-on with Airflow / Dagster / SQLMesh is not feasible in the current shape
The competitors are deeply embedded ecosystems with years of production scar tissue and large catalogs of validated integrations. The brief lists "no production users" as the one remaining "critical" gap — but that's not a gap you close by adding more features. It exists *because* the product was built without a user in the loop.

### The "Resolved" checkmarks in the brief are suspicious
"Distributed execution: resolved" means gRPC wires compile and a chaos test passes. It does not mean a real workload runs reliably across nodes for a quarter. Same for RBAC, retention, backfill. These are scaffolds, not battle-tested systems. Claiming otherwise in a stakeholder brief is the kind of self-deception that kills projects.

### The scope-to-depth ratio is the core problem
Every component is at roughly 30% depth — too much surface area to maintain, not enough in any one place to trust. A UI no one external uses, providers that are mostly stubs, a distributed runtime with no production workload, RBAC with no auth audit. Breadth without depth signals "toy" to anyone who probes, while consuming all the maintenance budget.

### The kernel itself is feasible
Rust + tree-sitter + fingerprinting + plan/apply is a coherent, well-sized idea that can be brought to real quality without compounding scope.

---

## 3. Direction — with team capacity, internal users, no deletion

The goal flips to: **where do we have the most defensible depth-to-build, and which existing surface is the highest-leverage place to invest?**

Internal users are the unlock. Before picking a direction, the most valuable next step is a **one-page audit per crate**: *which internal team/use case depends on this, and what would break for them if we stopped touching it for 6 months?*

That audit redirects investment toward what's real and lets the rest sit at "stable, maintained" without guilt.

### Directional bets, in priority order (refreshed)

#### Bet 1 — Finish virtual environments as a first-class experience
The data layer is in place (`Environment`, `fork`, `promote_into`, `diff_count`, persistence, REST handlers). The gap is the operator-facing layer — what makes virtual envs feel like a SQLMesh-style workflow rather than a low-level primitive. Concrete plan:

**1.1 Env-aware planner output**
- Make `conduit plan` take `--env <name>` (it likely already does — verify in `conduit-cli`) and diff the new compile against `EnvironmentManager::get(env).snapshot_map` rather than against a single global state.
- Where: `conduit-planner/src/deployment_plan.rs::build_plan` — accept an `Environment` parameter; in `conduit-cli`, load the env via `EnvironmentManager` and pass through.
- Tests: per-env plan diff (same DAG yields different plans against staging vs production because their snapshot maps differ).

**1.2 Human-readable env diff in CLI + API**
- Add a `diff(&self, other: &Environment) -> EnvironmentDiff` method to `Environment` that returns `{added: Vec<(dag, task, snap)>, removed: Vec<…>, changed: Vec<(dag, task, old_snap, new_snap)>}`.
- CLI: `conduit env diff staging production` — render as a git-style diff with snapshot hashes truncated.
- API: `GET /api/v1/environments/{a}/diff/{b}`.
- Test: round-trip — fork, mutate a few snapshot pointers, diff must match the mutation set.

**1.3 Rollback as a first-class verb**
- Today: `promote_into` is one-way; there's no undo. Capture the *prior* snapshot map of the target before each promotion as a versioned ledger.
- Data model: extend `Environment` with `history: Vec<EnvSnapshotMapVersion>` (or store separately in the snapshot store as `env_history/{env_id}/{timestamp}.json`).
- CLI: `conduit env rollback production` reverts to the previous version; `conduit env history production` lists versions.
- Test: promote A→B, rollback B, B's `snapshot_map` equals its pre-promotion state.

**1.4 Promotion policies**
- Add an optional `PromotionPolicy` per environment: `require_source: Option<String>` (e.g. production requires staging as the source), `min_age: Option<Duration>` (snapshots must be N hours old before promotion).
- Where: `EnvironmentManager::promote` checks the target env's policy before applying.
- Test: production with `require_source: "staging"` rejects a `dev → production` promote.

**1.5 Env-scoped run history**
- Today runs are likely stored globally. Tag each `RunEvent` with the environment it was triggered against and add `environment_id` to the run-list filter on the API and UI.
- Where: `conduit-state/src/event_store.rs` schema (additive), `conduit-api/src/handlers/runs.rs` filter.
- Test: trigger same DAG in staging and production, list each env's runs and verify isolation.

**1.6 UI surface**
- Identify the existing "Environments" page (`conduit-ui/`). Add: an env-pair selector for diff, a rollback button gated by `EnvironmentManager::history`, the promotion-policy editor.
- Out of scope for v1: per-env permissions (lives in the RBAC bet).

**Why this is Bet 1:** virtual environments are the most-advertised feature in the brief. The primitives exist; closing the experience gap turns a buried capability into the headline workflow.

#### Bet 2 — Lineage depth (the compounding bet)
Detailed below in §4. The recalibration: OpenLineage *emit* has shipped, so the next leverage is **OpenLineage ingest + a Marquez round-trip integration test** and **cross-task lineage (Python→SQL→Python)**. Jinja semantic awareness and dialect support stay on the roadmap but drop in priority — stripping is "good enough" for now and the bigger wins are integration + cross-task stitching.

#### Bet 3 — Observability: finish what was started — **shipped**
All three items from the prior revision landed (see §0):
- Wire an actual OpenTelemetry exporter/layer in the runtime configuration → `e04afff` (default-off `otel` cargo feature, OTLP gRPC, env-var activated).
- Structured run logs queryable from the UI → `612f3a6` (`/api/v1/events` now backed by the real event store with `run_id` / `dag_id` / `task_id` / `event_type` filters; the existing UI pages were already wired to the placeholder endpoint and start working the moment the API returns real data).
- Alert hook surface so internal teams can wire to their existing alerting → `1abe114` (`AlertHook` trait + scheduler integration; no transport impl in-tree by design).

Was the smallest unit of work with the biggest credibility return — and it shipped, so the credibility is now realized.

#### Bet 4 — One provider category to deep maturity
The provider count is now honest (12 real / 20 stub, with a CLI warn at compile time when DAGs route through stubs — good). Next: pick the 2-3 internal teams actually use (likely Postgres + S3 + one warehouse) and drive them to "you'd bet a production workload on them":
- Real connection pooling
- Retries with backoff
- Schema drift handling
- Credential rotation
- Observability hooks (ties into Bet 3)

#### Bet 5 — Plan/apply workflow: partial apply, rollback, human-readable diff — **shipped**
All three additions landed (see §0):
- Human-readable diff in the plan output → `014aaa8` (`ChangeSet::Display` rewritten — grouped by DAG, kind labelled in words, fingerprint `old → new` slug per Modified / UpstreamInvalidated change).
- Partial apply → `15b7890` + `2fb20b4` (`DeploymentPlan::filtered_to` planner API + `conduit apply --only DAG.TASK` CLI flag with upstream auto-include).
- Rollback → `5948960` + `2fb20b4` (`EnvironmentManager::apply_snapshot_map` write-through captures pre-apply state as `EnvHistoryReason::Apply { plan_id }`, revertible via the existing `conduit env rollback`).

Policy checks / approval gates remain as the follow-up.

#### Bet 6 — Distributed runtime: soak testing, not new RPCs
Eager re-dispatch and SIGTERM-aware shutdown are real progress. The one remaining concern is whether days-long runs hold up. Build a soak harness against internal workloads. No new RPCs.

#### Bet 7 — Impact analysis as a CI gate
`SchemaChangeDetector` already exists. Wrap it as a GitHub Action that posts a PR comment: *"this PR drops column X, which 7 downstream tasks read."* Smallest scope on this list, highest evangelism per line of code.

---

## 4. conduit-lineage — deep dive

### Why this is the flagship
- **~3,200 LOC across 8 files**, **69 unit tests**, real `sqlparser-rs` AST walking; `sql_parser.rs` alone is 1,440 LOC
- Handles CTEs / CTAS / UNION / window functions / wildcard expansion / catalog-resolved bare columns
- **View resolution** in `TableCatalog` (single-level) shipped — extending to view-of-view is the natural next slice
- **OpenLineage emit** shipped — Conduit can already produce spec-shaped `RunEvent`s with columnLineage facets
- **Criterion benchmarks** exist — publish numbers against SQLMesh as a credibility lever
- The `lib.rs` claim ("architecturally impossible in Airflow") should be softened — Airflow core is task-centric, OpenLineage-backed lineage is integration-layer behavior; Conduit can credibly claim *compiler-integrated* column lineage. Dagster's primary model is asset-graph rather than column-graph.

### Investment priorities, ranked by leverage (refreshed)

#### 1. OpenLineage **ingest** + a Marquez round-trip integration test
Emit has shipped. The next leverage is the *other half* of interop:
- **Ingest** OpenLineage events from upstream systems (Airflow, dbt, Spark) so Conduit shows lineage *through* systems it doesn't run
- A **Marquez round-trip integration test** that emits → posts → re-reads via the Marquez API closes the loop on the emit work and proves the spec-shaped events actually work with the reference consumer

This turns lineage from "we generate the right JSON" into "we participate in the OpenLineage ecosystem end-to-end."

#### 2. Cross-task lineage — the unique angle
- SQL-to-SQL lineage exists in SQLMesh, dbt-docs, OpenLineage
- **Python-to-SQL-to-Python** lineage stitched through Conduit's task graph **does not exist anywhere**

The compiler already knows task I/O. The lineage graph already knows column flow. Joining them turns column-level lineage into pipeline-level lineage. This is the genuinely novel claim and the single most defensible bet in the project. With emit shipped, this is now the highest *new* depth investment.

#### 3. Jinja/template *semantic* awareness — **shipped** (`1ad246b` + `fa196c2`)
Render-then-parse path landed: `DbtManifest` resolves `{{ ref(...) }}` and `{{ source(...) }}` to qualified table identifiers before `sqlparser` sees them. `cross_task::stitch_with_dbt_manifest` threads the manifest through to the extractor so resolved refs produce real cross-task column edges. CLI: `conduit lineage trace --dbt-manifest <path>`. Macro / variable / config block resolution (`{{ this }}`, `{{ var(...) }}`) still falls through to placeholder — those need the dbt graph state, not just the manifest. Manifest auto-discovery (looking for `dags_path/../target/manifest.json` or a DAG-level config knob) is the next slice — currently the operator hands it in explicitly.

#### 4. Dialect support — **shipped** (`7228999`)
`SqlDialect` enum with 13 variants plus connection-type → dialect mapping; `cross_task::stitch` now picks per-task. Remaining work is widening the test corpus to BigQuery `STRUCT` / nested paths, Snowflake `STAGE` / `VARIANT`, Redshift `DISTSTYLE` extraction — the plumbing is in place, those are additive tests that protect against sqlparser regressions.

#### 5. Impact analysis as a CI gate
`SchemaChangeDetector` exists. Wrap it as a GitHub Action that posts a PR comment:

> "this PR drops column X, which 7 downstream tasks read"

Smallest scope, highest evangelism per line of code.

#### 6. View-of-view + extended catalog resolution
View resolution shipped at single-level depth. Extending to view-of-view (with cycle detection) and to materialized views with refresh semantics is the next slice on the catalog side.

#### 7. Time-travel lineage
Snapshot the lineage graph alongside the event store, so *"what did our lineage look like on March 1?"* is answerable. Data governance teams pay for this.

### Honest weaknesses to be aware of
- **69 tests** against `sqlparser-rs`'s feature surface is still light for a flagship piece. **Property-based testing** on round-trip *SQL → lineage → diff* would harden it fast.
- Criterion benchmarks exist locally; **publish comparison numbers against SQLMesh** to make the performance claim concrete.
- No persistence story yet — the lineage graph is rebuilt on demand. For larger catalogs this becomes the bottleneck.
- OpenLineage emit has only 2 tests — likely thin coverage relative to the spec surface.

### One-line recommendation for lineage
Lean in, treat it as the project's flagship differentiator, and prioritize **OpenLineage ingest + Marquez round-trip + cross-task lineage** above everything else — those three together turn lineage from a feature into a positioning wedge no current competitor occupies.

---

## 5. What to stop doing (without deleting)

Since deletion is off the table:

- **Freeze, don't grow.** WASM, VS Code extension, stub providers (20 of them) → mark as `stable, maintenance-only` in the README. No new features, only correctness fixes when internal users hit them.
- **Triage the UI.** 17 pages is a lot. Identify the 4-5 pages internal users actually open weekly. Keep those polished; let the rest fall to "functional, low priority."
- **Stop adding new surfaces.** Every new crate, page, or operator should fail a test: *"does this make an existing internal workflow better?"* If not, defer.
- **Rewrite the brief to match reality.** "Production-ready candidate" with no production users sets the wrong expectation. "Used internally for X, Y, Z — public API stable for the kernel, experimental for everything else" is both honest and stronger positioning.

---

## 6. 90-day execution sketch (refreshed)

| Weeks | Focus | Outcome | Status |
|------|-------|---------|--------|
| 1-2  | Crate-by-crate internal-usage audit | Clear map of load-bearing vs scaffolding | pending |
| 1-4  | Virtual envs experience layer: env-aware planner, diff, rollback, policies | Brief workflow demoable end-to-end | ✓ shipped (Bet 1.1–1.6, prior refresh) |
| 2-6  | OTel spans through executor + per-task latency histograms + OTel exporter wiring | Internal runs show up in existing dashboards | ✓ shipped (executor spans + histograms prior; OTel exporter `e04afff`) |
| 4-8  | Cross-task lineage MVP (Python→SQL→Python) | The novel claim becomes demoable | ✓ shipped (Bet 2.2) |
| 6-10 | OpenLineage **ingest** + Marquez round-trip integration test | End-to-end ecosystem participation | ✓ shipped (Bet 2.1) |
| 8-12 | Distributed soak-test harness | Confidence in days-long runs | pending (blocked on real internal workloads) |
| 8-12 | Plan/apply: human-readable diff + partial apply + rollback | Plan/apply is CI-grade | ✓ shipped (Bet 5 — `5948960`, `15b7890`, `014aaa8`, `2fb20b4`) |
| 8-12 | Alert hook surface + structured run logs queryable from UI | Bet 3 finished — operator credibility piece | ✓ shipped (`1abe114`, `612f3a6`) |
| Ongoing | One provider category hardened to production grade | First "you'd bet a workload on it" tier | pending (blocked on knowing which 2-3 providers internal teams actually use) |
| Stretch | Impact analysis as a GitHub Action; dbt-aware template resolution; dialect coverage | Each is small + high-evangelism | Impact GH Action ✓ (Bet 7); dialect-aware parsing ✓ (`7228999`); dbt template resolution ✓ (`1ad246b`) |

---

## 7. Summary

- **Premise:** kernel has a real idea; product wrapper is far more surface than depth. The virtual-env primitives now have their experience layer; plan/apply is CI-grade; lineage participates in the OpenLineage ecosystem; observability spans + alerts are wired.
- **Feasibility:** competing head-on with Airflow/Dagster/SQLMesh as a platform is not feasible; deepening specific defensible wedges is. Recent commits are the right shape — depth, not new surfaces.
- **Direction:** Bets 1, 2.1, 2.2, 3, 5, 7 are all shipped. The originally-listed lineage stretch items (impact GH Action, dialect coverage, dbt template resolution) are also all shipped. The two remaining strategic bets are blocked on external input: Bet 4 (one provider category to production grade) needs to know which 2-3 providers internal teams actually use, and Bet 6 (distributed soak harness) needs real internal workloads to soak against.
- **Lineage:** OpenLineage emit + ingest + Marquez round-trip + cross-task (Python→SQL→Python) lineage + dialect-aware parsing + dbt template resolution all shipped. The wedge is now real — Conduit's lineage participates end-to-end with the OpenLineage ecosystem, handles warehouse-specific syntax, and walks dbt manifests. Remaining stretch: auto-load the manifest from a DAG-level config knob (plumbing exists), and macro / variable / config-block resolution beyond `ref()` / `source()` (needs dbt graph state).

---

## 8. Brief claims → implementation plans

Each entry pairs an aspirational claim in the stakeholder brief or `lib.rs` with a concrete plan to make the code match.

### 8.1 "Production-ready candidate at v0.2"
**Plan to make this true:**
- Land Bet 3 (observability — OTel spans + per-task histograms) and Bet 6 (distributed soak harness running ≥72h against an internal workload).
- Write `docs/PRODUCTION_READINESS.md` listing per-subsystem readiness with evidence links (commit SHAs, test paths, dashboards).
- Gate the claim on: 1 internal team running production for ≥30 days, 0 P0 incidents, observability dashboards live.
- Until those gates pass, the brief reads "**internally piloted at v0.2** — production-ready candidate for [X] workload shapes."

### 8.2 "Distributed execution: resolved"
**Plan to upgrade from claim to reality:**
- Soak-test harness in `conduit-distributed/tests/soak/` that runs a synthetic 1000-task DAG for ≥24h with periodic worker kills, coordinator restarts, and network partitions (via `tc netem` in the test container).
- Track three SLOs: tasks-completed-exactly-once rate, p99 task latency, coordinator memory growth.
- Implement coordinator HA: persist the coordinator's authoritative task state to the existing RocksDB store so a restarted coordinator reconstructs in-flight assignments; add an `is_coordinator_recovering` state that gates new dispatches until reconciliation completes.
- Add backpressure: bounded per-worker dispatch queue; coordinator throttles when any worker exceeds its high-water mark.
- New artifacts: `conduit-distributed/src/coordinator_recovery.rs`, soak test harness, weekly soak run in CI (long-running job).

### 8.3 "Authentication & RBAC: resolved"
**Plan:** audit the existing 894 LOC (`conduit-api/src/auth.rs` + `handlers/auth.rs`) against this checklist, fix gaps:
- API key storage: hashed (not plaintext) at rest, with rotation API.
- Constant-time comparison on key validation (timing attacks).
- Per-key rate limiting + audit log of all auth events into the event store.
- RBAC permission checks wired on every mutating handler (sweep: `grep -L "require_permission" conduit-api/src/handlers/*.rs` should be empty for mutating handlers).
- Session/key revocation that takes effect within one cache-TTL.
- Test: red-team test suite (`conduit-api/tests/auth_redteam.rs`) — forge tokens, replay attacks, privilege escalation attempts.
- Output: `SECURITY.md` documenting the threat model and validated mitigations.

### 8.4 Lineage `lib.rs`: "architecturally impossible in Airflow"
**Plan:** rewrite the docstring to the defensible claim:
> Airflow's core model is task-centric; column lineage there lives in OpenLineage integrations layered on top. Conduit traces column flow as part of compilation, so lineage stays in sync with the DAG without a separate integration. Dagster's primary model is asset-graph rather than column-graph, which is a different (and complementary) framing.

Single-commit doc change; takes 5 minutes. Pair with publishing the lineage benchmark numbers so the "compiler-integrated" framing has a performance proof point.

### 8.5 "32 providers"
**Plan:** make the brief and the CLI tell the same story.
- Brief: every mention of "32 providers" becomes "**12 production providers + 20 experimental stubs**" with a link to a table that lists each by status.
- Generate the table from code: a `build.rs` (or a `cargo xtask docs:providers`) walks every `Provider::info()`, emits `docs/providers.md`. Brief embeds or links it. Single source of truth.
- README badge: `providers: 12 production / 20 experimental` auto-updated.

### 8.6 "Time-travel lineage" / "Time-travel debugging" (planned in brief)
**Plan:**
- Snapshot the `LineageGraph` after every compile into `lineage_snapshots/{compile_hash}.json` (use the existing snapshot store).
- API: `GET /api/v1/lineage?at=<commit_or_timestamp>` reconstructs the graph at that point.
- UI: a time slider on the lineage page.
- Storage: lineage snapshots are small (KBs per DAG); retention follows the event-store retention policy.
- Test: compile two versions of a DAG, query each by hash, verify edges differ as expected.

### 8.7 Underlying discipline
- **No claim in the brief that doesn't trace back to a file path or commit.** Future stakeholder-brief edits should fail review if they add a feature description without a code link.
- Add a `docs/CLAIMS.md` that is the inverse of this section — a living index of every notable feature claim with its evidence link. Generated where possible (provider table, test counts, LOC), hand-curated where not.
- **`cargo check --workspace` must pass on every commit.** If pre-existing errors block that, fix them in the same commit with a note in the body — don't ship additive work on top of an existing break. Evidence this matters: `TaskType::Sql` got a `target: Option<String>` field added in `030d676` but two destructuring patterns in `conduit-cli/src/main.rs` (`cmd_lineage`, `cmd_preview`) weren't updated; the workspace was non-buildable for ~10 commits because individual contributors ran `cargo check -p <their-crate>` instead of the workspace. Caught only when the OTel cherry-pick (`e04afff`) forced a workspace check; cleaned up in `9764d65`. The fix is a one-line `..` per pattern — the discipline is to never let it ship in the first place.
- **A commit that adds a field to a public type sweeps every destructure.** `cargo check --workspace` enforces this for free; rely on it. Field additions look additive at the type level but are breaking at every destructure site — and `serde(default)` doesn't save you from `E0027`.


