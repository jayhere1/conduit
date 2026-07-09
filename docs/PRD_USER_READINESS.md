# PRD — Conduit User-Ready Release (v0.2)

**Date:** 2026-07-09
**Status:** Draft for review — M0–M3 implemented on `feat/user-ready-v0.2` (2026-07-09): M0 hygiene, M1 security gate, M2 first-run success (impact CLI, SDK vendoring), M3 conduit-native 0.2.0 (D1–D3 bindings, D5/D6 wheel+PyPI pipeline). M4 code items done: A3 auth audit log, A5 red-team suite + SECURITY.md, C3 real Snowflake/BigQuery/GCS probes, E1 lineage proptests, C4 sensors documented poll-only. M4 repo-settings items now have workflows written (B6 docs.yml → Pages, B7 docker.yml → GHCR, provider reference chapter generated from code); they need one-time repo enablement (Settings → Pages = GitHub Actions; package visibility after first push). Remaining: M5 (soak, coordinator recovery, SQLGlot oracle).
**Owner:** Jay
**Scope:** Everything required to (1) let real users adopt Conduit without hitting a wall, and (2) finish the functionality the project has already committed to building.
**Evidence discipline:** Per `STRATEGIC_DIRECTION.md` §8.7 — every claim below traces to a file path, line, or commit. All findings verified against source on 2026-07-09 (`cargo check --workspace` and `cargo test --workspace` both green on `main` @ `a04a2b6`).

---

## 1. Context

Conduit is a Rust-native pipeline orchestrator whose strategic bets (virtual environments, plan/apply, observability, cross-task lineage, OpenLineage emit+ingest) have all shipped per `docs/STRATEGIC_DIRECTION.md` §0. The engine is deep and healthy: the full workspace test suite passes, CI runs check/fmt/clippy/test/pytest/UI-test on every PR (`.github/workflows/ci.yml`), the UI has no mock data, and the mdBook has ~7,000 lines of real content.

Two user populations now exist or are imminent:

1. **mantrix-core (AXIS.AI)** — the first real external consumer of `conduit-lineage` via the `conduit-native` Python package (`docs/LINEAGE_VS_SQLGLOT.md`).
2. **Data engineers** evaluating Conduit as an orchestrator — the audience of the README, install script, docs site, and release binaries.

The audit for this PRD found that **neither population can currently succeed end-to-end.** The blocking issues are not engine gaps — they are seams between finished components: an installer pointing at a nonexistent GitHub org, a Python SDK that is only discoverable from a repo checkout, an auth layer that is enforced on 5 of ~50 endpoints, wheels for one platform only and no PyPI listing, and a CI impact gate calling a CLI subcommand that was never committed.

This PRD defines the work to close those seams and finish the committed-but-incomplete functionality.

---

## 2. Goals

- **G1 — A new user reaches a successful `conduit run` in under 10 minutes** from either `install.sh` or `cargo install`, on macOS (arm64/x86_64) and Linux, without cloning the repo.
- **G2 — `pip install conduit-native` works on every mainstream platform** (manylinux x86_64/aarch64, macOS arm64/x86_64, Python 3.9–3.13) and exposes the full lineage-platform surface (catalog, dialect, dbt manifest, contracts, impact, OpenLineage emit, cross-task).
- **G3 — The API is safe to expose**: authentication is actually enforced on every non-public endpoint before any user is told to run `conduit serve`.
- **G4 — Every advertised feature works as documented**: no dead CI gates, no 404ing UI buttons, no README commands that error.
- **G5 — Docs and website are live**: mdBook deployed, provider reference exists, LICENSE present.

### Non-goals

- Competing feature-for-feature with Airflow/Dagster/SQLMesh (per `STRATEGIC_DIRECTION.md` §2 — deepen wedges, don't chase breadth).
- Implementing the 20 stub providers (they correctly return `NotImplemented` and the CLI warns; that honesty stays).
- A managed cloud offering.
- Kubernetes-native execution.
- New UI pages (17 exist; polish only where a story below touches them).

---

## 3. Personas

| Persona | Entry point | Definition of success |
|---|---|---|
| **P1: Python lineage consumer** (mantrix-core) | `pip install conduit-native` | Catalog+dialect+dbt-manifest lineage, contracts, and impact reports callable from Python on macOS/Linux CI |
| **P2: Data engineer / evaluator** | README → install → `conduit init` → `conduit run` → `conduit serve` | First DAG runs end-to-end outside the repo checkout; UI reachable; auth on |
| **P3: Contributor** | Clone → `cargo test` → PR | CI checks meaningful (impact gate works), no stale artifacts misleading them |

---

## 4. Current state — verified findings

### 4.1 What is healthy (do not rework)

- Workspace: `cargo check` + `cargo test --workspace` green; 500+ Rust tests across crates incl. proptest suites in compiler/planner/scheduler/state/distributed.
- `conduit-lineage`: 116 unit tests + e2e suites (cross-task, Jinja, view, plan-impact); Marquez round-trip env-gated by design.
- CI (`.github/workflows/ci.yml`): check, fmt, clippy `-D warnings`, test, pytest (sdk), UI tests.
- mdBook content complete (installation, quick-start, concepts, CLI/API/SDK references, Airflow migration — no placeholder chapters).
- UI: 17 pages, no mock data, all but one API call map to registered routes.
- Release workflow (`.github/workflows/release.yml`): 4-target binary matrix, sha256 sidecars.
- Docker image builds and correctly pip-installs the SDK in-container (`Dockerfile:98-99`).

### 4.2 Blocking defects (the seams)

| # | Finding | Evidence |
|---|---|---|
| BUG-1 | **Auth is not enforced on any non-auth endpoint.** `RequireAuth`/permission checks exist only in `handlers/auth.rs`; no global auth middleware on `api_routes`. Trigger run, apply plan, env create/delete/promote/rollback, backfill, drain worker are all callable with no key while startup logs claim "API keys required for all endpoints". | `conduit-api/src/routes.rs:192-195,224`; `handlers/runs.rs:39`, `handlers/plan.rs:108`, `handlers/envs.rs`, `handlers/backfill.rs:71`, `handlers/cluster.rs:35` |
| BUG-2 | **API key check is not constant-time** (plain `==` on hash strings) and is O(n) over all keys per request; no audit log of auth events. | `conduit-api/src/auth.rs:347-349` |
| BUG-3 | **`conduit impact` CLI does not exist** — commit `02b74cb` landed `plan_impact.rs` (587 LOC + e2e tests) and the GH workflow, but never the clap subcommand. The PR impact gate errors on every run and the merge gate never gates. | `conduit-cli/src/main.rs:162-466` (no `Impact`); `.github/workflows/conduit-impact.yml:71,80` |
| BUG-4 | **`conduit run` fails outside a repo checkout** — `discover_sdk_path` only finds `conduit_sdk` by walking up from the binary to `sdk/python/conduit_sdk`; installed binaries → `ModuleNotFoundError`. | `conduit-executor/src/process_runner.rs:599-614` |
| BUG-5 | **`install.sh` and `book.toml` point at nonexistent org `conduit-orchestrator/conduit`** (real: `jayhere1/conduit`) — the curl installer 404s. | `scripts/install.sh:5,8,15,103`; `docs/book.toml:11` |
| BUG-6 | **No LICENSE file** despite README declaring Apache-2.0; `release.yml` copies LICENSE files that don't exist (`\|\| true` swallows it) — releases ship without license text. | `README.md:154`; `.github/workflows/release.yml:120-123` |
| BUG-7 | **Python wheels: single-arch (cp312/linux x86_64), GitHub-Release-only, no PyPI publish**, not built in a manylinux container. | `.github/workflows/publish-python.yml:11,19,31,53-56` |
| BUG-8 | **Version drift:** package metadata says 0.1.1, runtime `__version__` says 0.1.0. | `conduit-python/src/lib.rs:34`; `conduit-python/python/conduit_native/__init__.py:14`; vs `conduit-python/Cargo.toml:3` |
| BUG-9 | **UI "Test Connection" button 404s** — handler fully implemented but never routed. | handler `conduit-api/src/handlers/connections.rs:45`; missing route in `routes.rs:170-177`; caller `conduit-ui/src/api.js:297` |
| BUG-10 | **`conduit-sdk` build is broken and unpublished** — `pyproject.toml` points at a README that doesn't exist (hatchling build fails); no publish workflow. | `sdk/python/pyproject.toml:9` |
| BUG-11 | **README quick start fails**: no install/PATH step after `cargo build --release`; documents `conduit lineage <dag.task>` which is now `conduit lineage extract`; omits `backfill`/`worker`/`cluster`/`query`/`preview`/`replay`. | `README.md:13-38,109`; `conduit-cli/src/main.rs:342-346,491-566` |
| BUG-12 | **Docs site and marketing site are never deployed** — no Pages/deploy workflow references `docs/` or `site/`. | `.github/workflows/` (4 workflows, none deploy) |

### 4.3 Committed-but-unfinished functionality

| # | Item | Evidence |
|---|---|---|
| GAP-1 | Python bindings missing for `extract_with_full_context` (dbt manifest), `contracts`, `impact`/`impact_report`/`plan_impact`, `openlineage` emit, `cross_task` — all named as "the differentiators to lean into" in `docs/LINEAGE_VS_SQLGLOT.md`. | `conduit-python/src/lineage.rs:384-392` (4 functions bound) |
| GAP-2 | Sensor external-trigger path is a no-op in the scheduler (executor-side polling works; event-driven `handle_sensor_triggered` logs "not yet implemented"). | `conduit-scheduler/src/scheduler.rs:702-705` |
| GAP-3 | `test_connection` shallow for 3 of 12 real providers: snowflake/bigquery are bare TCP dials; gcs never touches the network and always succeeds. | `conduit-providers/src/providers/snowflake.rs:276`, `bigquery.rs:292`, `gcs.rs:146` |
| GAP-4 | SQLGlot differential-testing oracle — planned in `docs/LINEAGE_VS_SQLGLOT.md`, not built. | no corpus/test exists |
| GAP-5 | No property-based tests in `conduit-lineage` (proptest used in 5 other crates). | `conduit-lineage/Cargo.toml` dev-deps |
| GAP-6 | Distributed: no soak harness, no coordinator recovery/HA (worker-failure reassignment exists; queue is drop-on-full). | `conduit-distributed/tests/`; `coordinator.rs:166-173,284-304` |
| GAP-7 | Time-travel lineage (`STRATEGIC_DIRECTION.md` §8.6) — not started; no lineage snapshots, no `?at=` param. | `conduit-api/src/handlers/lineage.rs:169-457` |
| GAP-8 | CORS allows any origin (`TODO: restrict origins in production`). | `conduit-api/src/routes.rs:230` |
| GAP-9 | dbt manifest auto-discovery (operator must pass `--dbt-manifest` explicitly). | `docs/STRATEGIC_DIRECTION.md` §4.3 follow-up |
| GAP-10 | Plan duration estimates never populated (`estimated_duration_ms: None // TODO`). | `conduit-planner/src/deployment_plan.rs:279` |

### 4.4 Hygiene

- Stale root artifacts: `OPENAPI_IMPLEMENTATION.md`, `STORAGE_PROVIDERS_IMPLEMENTATION.md`, `IMPLEMENTATION_SUMMARY.txt` (session write-ups, two covering the same work). `TESTING.md` is the only keeper.
- Stale branches: `jayhere1/competitive-analysis` fully merged; `jayhere1/audit-gaps-hardening` (Mar 26) superseded — merging it now would revert newer main work; `feat/otel-exporter` content landed as `e04afff`.
- `docs/src/introduction.md:195` + `architecture.md:24` mislabel `conduit-python` as "Python SDK and tree-sitter bindings" (it is the PyO3 native bindings; the SDK is `sdk/python`).
- Three different Rust versions referenced: README "1.75+", `Dockerfile` 1.91, `Dockerfile.dev` 1.77.
- `docs/LINEAGE_VS_SQLGLOT.md:50-54` now stale — the catalog+dialect binding it calls "missing" shipped in `6a3e192`.

---

## 5. Release definition

Two coordinated releases ship from this PRD:

- **`conduit-native` 0.2.0 on PyPI** — the lineage platform for Python consumers (Epic D).
- **`conduit` CLI v0.2.0** — binaries + fixed installer + deployed docs + enforced auth (Epics A–C).

---

## 6. Epics and stories

Priorities: **P0** = blocks any user; **P1** = blocks a stated goal; **P2** = production confidence; **P3** = stretch/differentiator.

### Epic A — Security gate (P0)

*The API cannot be recommended to users while BUG-1 stands. This epic executes `STRATEGIC_DIRECTION.md` §8.3.*

**A1. Enforce authentication globally** — P0
Add auth middleware to `api_routes` in `conduit-api/src/routes.rs` so that when auth is enabled, every endpoint except `/health`, login, and static UI assets requires a valid key; wire `Permission` checks (from the existing 22-permission model in `auth.rs`) on every mutating handler: runs (trigger/cancel), plan/apply, envs (create/delete/promote/rollback/policy), backfill, cluster drain, connections test, cache invalidate, openlineage ingest.
*Acceptance:* with auth enabled, unauthenticated `POST /api/v1/runs` (and every mutating route) returns 401; a Viewer-role key gets 403 on mutations; existing UI login flow still works; a sweep test asserts every mutating route rejects anonymous requests (pattern: route-table-driven test, not per-handler copies).

**A2. Constant-time key validation with O(1) lookup** — P0
Replace `candidate == *hash` (`auth.rs:349`) with constant-time comparison; index keys by prefix/ID to avoid O(n) re-hash per request.
*Acceptance:* comparison uses a constant-time primitive; auth lookup does one hash, not one per stored key; existing `auth_test.rs` passes.

**A3. Audit log for auth events** — P1
Emit structured events (into the existing event store) for auth success/failure, key create/revoke, permission denial.
*Acceptance:* events queryable via `GET /api/v1/events?event_type=auth...`; covered by tests.

**A4. Restrict CORS in production** — P1
Replace allow-any-origin (`routes.rs:230`) with configurable allowed origins (default: same-origin when serving the bundled UI).
*Acceptance:* config knob in `conduit.yaml`; default no longer `Any`.

**A5. Red-team test suite + SECURITY.md** — P1
`conduit-api/tests/auth_redteam.rs`: forged tokens, revoked-key replay, privilege escalation, anonymous-mutation sweep. Write `SECURITY.md` (threat model, mitigations, reporting).
*Acceptance:* suite runs in CI; SECURITY.md at repo root.

### Epic B — Zero-to-first-run onboarding (P0)

**B1. Fix installer and docs org references** — P0, trivial
`scripts/install.sh:5,8,15` and `docs/book.toml:11` → `jayhere1/conduit` (or the org the project settles on — see Open Decision OD2).
*Acceptance:* `curl … | sh` installs the latest release binary on macOS + Linux.

**B2. Add LICENSE** — P0, trivial
Apache-2.0 text at repo root (matching `README.md:154` and crate metadata); confirm `release.yml:120-123` picks it up.
*Acceptance:* LICENSE exists; release tarballs contain it.

**B3. Make `conduit run` work outside a checkout** — P0
The core fix for BUG-4. Approach (see Open Decision OD1): `conduit init` writes a vendored copy of `conduit_sdk` into the scaffolded project (it is stdlib-only, `sdk/python/pyproject.toml:23`), and `discover_sdk_path` (`process_runner.rs:599-614`) additionally checks the project dir and an env override `CONDUIT_SDK_PATH`; `conduit run`/`compile` print an actionable hint when the SDK is missing. Publishing `conduit-sdk` to PyPI (B4) is the complementary long-term path.
*Acceptance:* on a machine with only the release binary + python3, `conduit init demo && cd demo && conduit run hello_world` succeeds; error message when SDK truly absent names the fix.

**B4. Fix and publish `conduit-sdk` to PyPI** — P1
Add the missing `sdk/python/README.md` (build currently fails per `pyproject.toml:9`), add a publish workflow (hatchling build + PyPI trusted publishing), version 0.2.0.
*Acceptance:* `pip install conduit-sdk` works; `python -m build` succeeds in CI.

**B5. README truth pass** — P0
Install section: release binaries + `install.sh` + `cargo install --path conduit-cli`; fix `conduit lineage extract` row; add missing commands (`backfill`, `worker`, `cluster`, `query`, `preview`, `replay`, `impact` once C1 lands); align Rust version claim with the pinned toolchain (single source: rust-toolchain.toml — new).
*Acceptance:* every command in the README table parses against the clap definitions (smoke test in `cli_smoke_test.rs` that runs `--help` for each documented subcommand).

**B6. Deploy the mdBook + provider reference** — P1
GitHub Pages workflow building `docs/`; new "Providers & Connections" chapter (generated table per `STRATEGIC_DIRECTION.md` §8.5: 12 production / 20 experimental, from `ProviderInfo`); fix the conduit-python mislabel in `introduction.md:195` / `architecture.md:24`.
*Acceptance:* docs URL live and linked from README; provider table generated from code, not hand-maintained.

**B7. Publish the Docker image** — P2
GHCR publish job on tag; compose gains an `image:` variant.
*Acceptance:* `docker run ghcr.io/<org>/conduit serve` works.

**B8. Repo hygiene** — P2
Delete `OPENAPI_IMPLEMENTATION.md`, `STORAGE_PROVIDERS_IMPLEMENTATION.md`, `IMPLEMENTATION_SUMMARY.txt` (git history preserves them); move `TESTING.md` content into the book or CONTRIBUTING.md; delete merged/superseded branches; update `docs/LINEAGE_VS_SQLGLOT.md:50-54` to reflect the shipped binding.
*Acceptance:* repo root contains only user-facing files; stale branches pruned.

### Epic C — Finish committed functionality (P0–P1)

**C1. Wire the `conduit impact` subcommand** — P0
The engine (`conduit-lineage/src/plan_impact.rs`, `impact_report.rs`) and workflow exist; add the clap subcommand exactly as specified in `02b74cb`'s message: `--base-plan/--head-plan` file mode and `--base/--head <git-ref|WORKING>` git-worktree mode, markdown+JSON output, `lineage_coverage` metric.
*Acceptance:* `.github/workflows/conduit-impact.yml` goes green on a real PR; a PR that drops a consumed column gets a sticky comment and the merge gate blocks without the `allow-breaking` label; smoke test added to `cli_smoke_test.rs`.

**C2. Register the connection-test route** — P0, trivial
`POST /api/v1/connections/:name/test` → existing `handlers::connections::test_connection` (`connections.rs:45`), with auth per A1.
*Acceptance:* UI "Test Connection" button returns a real result; route covered by an API test.

**C3. Real `test_connection` for Snowflake, BigQuery, GCS** — P1
Replace TCP-dial/no-op checks with authenticated round-trips (e.g. `SELECT 1` via the Snowflake SQL API, BigQuery `SELECT 1` job or datasets.list, GCS bucket metadata get). Env-gated integration tests (same pattern as the 15 ignored sqlx tests).
*Acceptance:* wrong credentials fail the test; `is_stub: false` claims match behavior.

**C4. Sensor external-trigger path** — P1 (or explicit de-scope)
Either implement `handle_sensor_triggered` (`scheduler.rs:702-705`) to unblock sensor-waiting tasks on external events, or remove the event surface and document sensors as poll-only. Decide via OD3; do not leave the silent no-op.
*Acceptance:* external trigger unblocks a waiting sensor task (test), or the API/docs no longer advertise external sensor triggers.

**C5. Plan duration estimates** — P3
Populate `estimated_duration_ms` (`deployment_plan.rs:279`) from event-store history (avg of last N successful runs per task).
*Acceptance:* `conduit plan` shows estimates for previously-run tasks.

### Epic D — `conduit-native` 0.2.0: the lineage platform in Python (P1)

*Executes the remainder of `docs/LINEAGE_VS_SQLGLOT.md`. P1 persona is blocked on BUG-7/BUG-8 for CI use today.*

**D1. Bind `extract_with_full_context` (dbt manifest)** — P1
`extract_sql_lineage_full(sql, catalog_json, dialect, manifest_path_or_json)` in `conduit-python/src/lineage.rs`, reusing the existing `build_catalog` + `SqlDialect::from_connection_type`.
*Acceptance:* Python test resolves `{{ ref('model') }}` to a real table with a manifest, placeholder without.

**D2. Bind contracts + impact** — P1
Expose `contracts` validation and `impact`/`impact_report`/`plan_impact` (the consumer's headline "frozen KPI silently breaks" use case, `LINEAGE_VS_SQLGLOT.md` "differentiators").
*Acceptance:* Python can validate a schema contract and produce an impact report for a column drop; parity tests against the Rust e2e fixtures.

**D3. Bind OpenLineage emit** — P1
`to_openlineage_event(...)` returning the spec-shaped RunEvent JSON with columnLineage facets.
*Acceptance:* emitted JSON passes the same assertions as `openlineage.rs` unit tests; documented example posting to Marquez.

**D4. Bind cross-task stitching** — P2
`stitch_dag(dag_json, dbt_manifest=None)` exposing `CrossTaskLineage` for graph-level impact from Python.
*Acceptance:* the `cross_task_e2e` fixture round-trips through Python.

**D5. Multi-arch wheels via maturin-action** — P1
Rework `publish-python.yml`: manylinux (container) x86_64 + aarch64, macOS arm64 + x86_64, CPython 3.9–3.13 (or abi3-py39 to collapse the matrix); auditwheel repair; sdist.
*Acceptance:* CI produces installable wheels for all targets; `pip install` on a mac arm64 laptop succeeds with no Rust toolchain.

**D6. PyPI trusted publishing + version single-sourcing** — P1
Publish `conduit-native` (and `conduit-sdk`, B4) to PyPI via OIDC trusted publishing on tag; fix the 0.1.0/0.1.1 drift by deriving `__version__` from the crate version (`env!("CARGO_PKG_VERSION")` in `lib.rs:34`; drop the hardcoded string in `__init__.py:14`); update `conduit-python/README.md` to document all bound functions (it currently omits `extract_sql_lineage_with_catalog`).
*Acceptance:* `pip install conduit-native==0.2.0` from PyPI; `conduit_native.__version__ == importlib.metadata.version("conduit-native")`; release checklist documented.

**D7. SQLGlot differential-testing oracle** — P2
Test-only harness (`conduit-lineage/tests/sqlglot_differential/` + Python script in CI): run a corpus of real queries (seed with the existing test SQL + consumer-shaped queries across BigQuery/Postgres/ClickHouse) through SQLGlot `lineage()` and conduit's extractor; assert column-map agreement; divergences become tracked fixtures.
*Acceptance:* corpus ≥100 queries running in CI (non-blocking job initially); divergence list checked in as known-issues fixtures.

**D8. Resolver hardening backlog intake** — P2 (ongoing)
Triage D7 divergences into `sql_parser.rs`/`catalog.rs` fixes: ambiguity resolution, multi-level views/CTEs (view-of-view with cycle detection per `STRATEGIC_DIRECTION.md` §4.6), expression-column extraction.
*Acceptance:* each closed divergence gets a regression test; view-of-view resolution ships with cycle detection.

### Epic E — Production confidence (P2)

**E1. Property-based tests for lineage** — P2
Add proptest to `conduit-lineage` (the only core crate without it): generated SELECT/CTE/JOIN trees → extractor must not panic, and lineage must be stable under formatting-preserving transforms.
*Acceptance:* proptest suite in CI alongside the other 5 crates' suites.

**E2. Distributed soak harness** — P2
`conduit-distributed/tests/soak/` per §8.2: synthetic 1000-task DAG, periodic worker kills + coordinator restarts; SLOs: exactly-once completions, p99 latency, coordinator memory growth. Weekly scheduled CI job (not per-PR).
*Acceptance:* 24h soak passes locally; weekly job wired.

**E3. Coordinator recovery** — P2
Persist authoritative assignment state to RocksDB so a restarted coordinator reconstructs in-flight work; `is_coordinator_recovering` gates new dispatch until reconciled (per §8.2). Upgrade drop-on-full (`coordinator.rs:166-173`) to bounded-queue backpressure with per-worker high-water marks.
*Acceptance:* kill-and-restart coordinator mid-run → no lost/duplicated tasks (chaos test extends `distributed_e2e_test.rs`).

**E4. Publish benchmark numbers** — P2
Run the existing criterion benches (compiler 10–1,000 DAGs; lineage 10–500 tasks/columns), publish results + methodology in the book; compare against SQLMesh parse times where honest.
*Acceptance:* docs chapter with reproducible commands; README claims link to it.

**E5. PRODUCTION_READINESS.md** — P2
Per-subsystem readiness with evidence links (per §8.1); rewrite stakeholder-brief claims to match ("internally piloted", "12 production providers + 20 experimental stubs").
*Acceptance:* every claim has a commit/test link; brief and README agree with the CLI's own stub warnings.

### Epic F — Differentiators (P3, post-0.2)

**F1. Time-travel lineage** — snapshot `LineageGraph` per compile into the snapshot store; `GET /api/v1/lineage?at=<ts|hash>`; UI time slider (§8.6).
**F2. dbt manifest auto-discovery** — DAG-level config knob + conventional `target/manifest.json` lookup (§4.3 follow-up).
**F3. Incremental-model strategies** — evaluate SQLMesh-style incremental materializations (competitive gap noted in `STAKEHOLDER_BRIEF.md`); scope in a separate design doc before commitment.

---

## 7. Milestones

| Milestone | Contents | Exit criterion |
|---|---|---|
| **M0 — Truth & hygiene** (days) | B1, B2, B5, B8, D6-version-fix | Installer works; LICENSE present; README commands all parse; no stale artifacts |
| **M1 — Security** (1–2 wk) | A1, A2, C2, A4 | Anonymous mutation impossible; UI test-connection works; redteam suite started |
| **M2 — First-run success** (1–2 wk, parallel with M1) | B3, B4, C1 | Fresh-machine `init → run` succeeds; impact CI gate green and gating |
| **M3 — `conduit-native` 0.2.0** (2–3 wk) | D1–D3, D5, D6 | PyPI release installable on mac arm64 + linux aarch64; mantrix unblocked on full surface |
| **M4 — Docs & confidence** (2–3 wk) | B6, B7, A3, A5, C3, C4, E1, E4, E5 | Docs site live; providers reference; benchmarks published; SECURITY.md |
| **M5 — Distributed hardening** (ongoing) | E2, E3, D7, D8 | Soak green weekly; differential oracle in CI |
| **Post-0.2** | C5, D4, F1–F3 | Scoped separately |

Suggested working branch: `feat/user-ready-v0.2`, with M0 landing directly as small PRs.

---

## 8. Success metrics

- Time-to-first-successful-run (fresh macOS machine, stopwatch): **< 10 min** (currently: ∞ — quick start cannot complete).
- `pip install conduit-native` succeeds on {manylinux x86_64, manylinux aarch64, macOS arm64, macOS x86_64} × {3.9–3.13}: **20/20** (currently 1/20).
- Mutating endpoints rejecting anonymous requests when auth enabled: **100%** (currently ~10%).
- README-documented commands that parse: **100%** (currently fails on `lineage`).
- Impact CI gate: comments + gates on a test PR that drops a consumed column (currently: errors on every run).
- mantrix-core consumes catalog+dialect+manifest lineage and contracts from PyPI wheels in their CI (qualitative — confirm with the consumer).

---

## 9. Risks

| Risk | Mitigation |
|---|---|
| A1 breaks the bundled UI or local dev flows | Auth-disabled-by-default for `conduit serve` local mode preserved; UI login already exists; sweep test in CI |
| Multi-arch wheel matrix flakiness (cross-compiled aarch64, abi3 subtleties) | Use `PyO3/maturin-action` (battle-tested); start with abi3-py39 to shrink the matrix |
| Vendored SDK copies drift from `sdk/python` | Vendor at `init` time from a version-stamped embedded copy; `conduit compile` warns on version mismatch; PyPI install is the canonical path |
| Snowflake/BigQuery test_connection needs real credentials in CI | Env-gated tests (existing pattern: 15 ignored sqlx tests); local verification documented |
| `conduit impact` git-worktree mode interacts badly with CI checkouts | Plan-file mode is the CI path (workflow already builds both plans); git mode is operator convenience |
| Scope creep back into breadth | Non-goals section; every new story must serve P1/P2/P3 personas |

---

## 10. Open decisions (owner: Jay)

- **OD1 — SDK bootstrap mechanism (B3):** vendor `conduit_sdk` into `conduit init` scaffolds (recommended: works offline, stdlib-only ~few files) vs. auto-`pip install conduit-sdk` on init vs. document-only. Recommendation: vendor + `CONDUIT_SDK_PATH` override + PyPI as canonical.
- **OD2 — Canonical GitHub org/name:** keep `jayhere1/conduit` or move to an org (`conduit-orchestrator`) before links are published widely. Note: "conduit" is a crowded name on PyPI/crates.io (`conduit-native` chosen for PyPI already); decide before B1/B6 bake URLs in.
- **OD3 — Sensors (C4):** implement external triggers or de-scope to poll-only for 0.2. Recommendation: de-scope + document; polling covers the shipped use cases, and the event path deserves its own design.
- **OD4 — Publish cadence:** should `v0.2.0` tag both the CLI release and the Python packages, or decouple (e.g. `native-v0.2.0` tags)? Recommendation: decouple, since wheel-only fixes shouldn't cut binary releases (0.1.1 was exactly this shape).
- **OD5 — Where the docs live:** GitHub Pages from this repo (recommended, free, no infra) vs. the `site/` landing page getting its own deploy. The `site/` page can embed or link the book later.

---

## Appendix — audit provenance

Findings verified 2026-07-09 against `main` @ `a04a2b6` via: direct source reads (`routes.rs`, `auth.rs`, `process_runner.rs`, `scheduler.rs`, `install.sh`, workflows), `cargo check --workspace` (clean, 1m40s), `cargo test --workspace` (all green), `git show 02b74cb --stat` (confirms `conduit impact` CLI never committed), and three structured sub-audits (Python packaging surface; onboarding/docs surface; stubs/auth/distributed gaps).
