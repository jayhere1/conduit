# Conduit: Strategic Direction Review

**Date:** 2026-05-15 (refreshed)
**Status:** Working notes — premise review, feasibility assessment, and direction recommendations. Updated after a ~10-commit progress check.
**Assumptions for this version:** team expertise is available, internal users already depend on the platform, deletion is off the table.

---

## 0. Progress since prior review

Shipped between the first draft of this doc and this refresh:

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

#### Bet 3 — Observability: finish what was started
The Prometheus scrape endpoint exists; the rest does not. OpenTelemetry is in `Cargo.toml` but no spans are emitted, and there are no per-task latency histograms. Next slice:
- OTel spans through the executor task lifecycle (start / end / error / retry)
- Per-task and per-DAG latency + throughput histograms exported via the existing `/metrics` endpoint
- Structured run logs queryable from the UI
- Alert hook surface (so internal teams can wire to their existing alerting)

This is the smallest unit of work with the biggest credibility return.

#### Bet 4 — One provider category to deep maturity
The provider count is now honest (12 real / 20 stub, with a CLI warn at compile time when DAGs route through stubs — good). Next: pick the 2-3 internal teams actually use (likely Postgres + S3 + one warehouse) and drive them to "you'd bet a production workload on them":
- Real connection pooling
- Retries with backoff
- Schema drift handling
- Credential rotation
- Observability hooks (ties into Bet 3)

#### Bet 5 — Plan/apply workflow: partial apply, rollback, human-readable diff
The plan/apply core works; the operator-experience layer doesn't. Three additions take it from demoable to CI-grade:
- Human-readable diff in the plan output (the change kinds are tracked internally, not rendered)
- Partial apply (select a subset of actions to apply)
- Rollback (revert to a prior snapshot pointer)

Policy checks / approval gates are a follow-up after these three.

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

#### 3. Jinja/template *semantic* awareness
Stripping shipped (regex placeholder substitution lets `sqlparser-rs` parse dbt-style SQL cleanly). The remaining gap is semantic awareness — dbt `ref()`/`source()` resolution, macro context, variable binding. Without it, lineage stops at the template boundary semantically even though parsing succeeds.

Two paths:
- Render-then-parse (with stub variable bindings — pragmatic, integrates with dbt manifests)
- Template-aware AST (purer, more work)

#### 4. Dialect support
`GenericDialect` is fine for ANSI SQL but misses:
- Snowflake (`COPY INTO`, semi-structured access)
- BigQuery (`UNNEST`, struct paths)
- Redshift (`DISTSTYLE`)

Lineage that silently degrades on dialect-specific syntax is worse than lineage that errors loudly. With Snowflake/BigQuery/Redshift all in the "real provider" list, dialect-aware parsing is a natural pairing.

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

| Weeks | Focus | Outcome |
|------|-------|---------|
| 1-2  | Crate-by-crate internal-usage audit | Clear map of load-bearing vs scaffolding |
| 1-4  | Virtual envs experience layer: env-aware planner, diff, rollback, policies | Brief workflow demoable end-to-end |
| 2-6  | OTel spans through executor + per-task latency histograms | Internal runs show up in existing dashboards |
| 4-8  | Cross-task lineage MVP (Python→SQL→Python) | The novel claim becomes demoable |
| 6-10 | OpenLineage **ingest** + Marquez round-trip integration test | End-to-end ecosystem participation |
| 8-12 | Distributed soak-test harness | Confidence in days-long runs |
| 8-12 | Plan/apply: human-readable diff + partial apply + rollback | Plan/apply is CI-grade |
| Ongoing | One provider category hardened to production grade | First "you'd bet a workload on it" tier |
| Stretch | Impact analysis as a GitHub Action; dbt-aware template resolution; dialect coverage | Each is small + high-evangelism |

---

## 7. Summary

- **Premise:** kernel has a real idea; product wrapper is far more surface than depth. The virtual-env primitives exist; the experience layer on top doesn't yet.
- **Feasibility:** competing head-on with Airflow/Dagster/SQLMesh as a platform is not feasible; deepening specific defensible wedges is. Recent commits are the right shape — depth, not new surfaces.
- **Direction:** finish the virtual-env experience first (env-aware planner + diff + rollback + policies). Then lineage as the flagship (OpenLineage *ingest* + cross-task), observability finished (OTel spans + histograms), one provider category deep, plan/apply made CI-grade, distributed runtime soak-tested.
- **Lineage:** OpenLineage emit shipped — next compounding bets are **ingest + Marquez round-trip** and **cross-task (Python→SQL→Python) lineage**.

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



