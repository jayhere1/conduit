# User-Readiness M0–M2 Implementation Plan

> **STATUS: COMPLETE (2026-07-09).** All 13 tasks implemented on
> `feat/user-ready-v0.2`. Final gate: 1072 workspace tests passing,
> `cargo fmt --check` clean, `cargo clippy --workspace -- -D warnings`
> clean. Deviations from plan noted per task: rust-toolchain.toml skipped
> (CI tracks `stable`; a pin would diverge), docker-compose CORS change
> unnecessary (UI is same-origin via the vite proxy), branch pruning
> deferred to Jay (deletion is destructive).

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute milestones M0–M2 of `docs/PRD_USER_READINESS.md` — repo hygiene/truth fixes, API security enforcement, and a first-run path that works outside the repo checkout.

**Architecture:** Three tracks, sequenced. M0 is metadata/doc-only commits. M1 adds a router-level auth middleware plus per-handler permission checks in `conduit-api` (the `RequireAuth`/`Permission` infrastructure already exists and is reused, not rebuilt). M2 wires the already-tested `plan_impact` engine into a new clap subcommand, and makes the Python SDK discoverable outside the checkout by vendoring it into `conduit init` scaffolds with an env-var override.

**Tech Stack:** Rust (axum 0.7, clap, tower-http), `subtle` for constant-time comparison, `include_dir` for SDK vendoring, GitHub Actions.

## Global Constraints

- `cargo check --workspace` and `cargo test --workspace` must pass at every commit (STRATEGIC_DIRECTION.md §8.7).
- CI runs clippy with `-D warnings` and `cargo fmt --check` (`.github/workflows/ci.yml:11`) — run both before each commit.
- Commit messages follow the repo's conventional style (`feat(api): …`, `fix(cli): …`, `docs: …`, `chore: …`). No Co-Authored-By trailers.
- Do not push; local commits only until Jay reviews.
- GitHub org is `jayhere1/conduit` (PRD OD2 default; env-overridable in install.sh).
- PRD references: story IDs (A1, B3, C1…) from `docs/PRD_USER_READINESS.md` §6.

---

### Task 1: LICENSE file (B2)

**Files:**
- Create: `LICENSE` (Apache-2.0 full text, copyright "2026 Jayveer Singh")

**Steps:**
- [x] Write the canonical Apache-2.0 text to `LICENSE`.
- [x] Verify `release.yml:120-123` glob (`LICENSE*`) now matches: `ls LICENSE`.
- [x] Commit: `chore: add Apache-2.0 LICENSE (PRD B2)`

### Task 2: Fix installer + book repo references (B1)

**Files:**
- Modify: `scripts/install.sh:5,8,15` — `conduit-orchestrator/conduit` → `jayhere1/conduit`
- Modify: `docs/book.toml:11` — same

**Steps:**
- [x] Edit both files; grep repo-wide for any other `conduit-orchestrator` references and fix.
- [x] Test: `sh -n scripts/install.sh` (syntax) and `CONDUIT_REPO=jayhere1/conduit sh -c 'grep -c jayhere1 scripts/install.sh'` ≥ 3.
- [x] Commit: `fix(install): point installer and book at the real GitHub repo (PRD B1)`

### Task 3: Single-source conduit-native version (D6 version fix, BUG-8)

**Files:**
- Modify: `conduit-python/src/lib.rs:34` — `m.add("__version__", "0.1.0")` → `m.add("__version__", env!("CARGO_PKG_VERSION"))`
- Modify: `conduit-python/python/conduit_native/__init__.py:14` — derive from `importlib.metadata.version("conduit-native")` with fallback to the native module's `__version__`
- Modify: `conduit-python/MANIFEST.md:270` — 0.1.0 → note "version is single-sourced from Cargo.toml"

**Steps:**
- [x] Make the three edits.
- [x] Test: `cargo check -p conduit-python` passes.
- [x] Commit: `fix(python): single-source conduit-native version from Cargo.toml (PRD D6)`

### Task 4: conduit-sdk README + packaging fix (B4 part 1, BUG-10)

**Files:**
- Create: `sdk/python/README.md` — short: what the SDK is, install, decorator example, link to docs
- Verify: `sdk/python/pyproject.toml:9` readme reference resolves

**Steps:**
- [x] Write README (decorators, operators, hooks, contracts — one example each, ≤80 lines).
- [x] Test: `cd sdk/python && python3 -m pip wheel --no-deps -w /tmp/wheeltest . ` succeeds (or `python3 -m build` if available).
- [x] Commit: `fix(sdk): add missing README so the package builds (PRD B4)`

### Task 5: README truth pass (B5, BUG-11)

**Files:**
- Modify: `README.md` — install section (release binaries + `install.sh` + `cargo install --path conduit-cli`), fix `conduit lineage extract` row, add missing commands (`backfill`, `worker`, `cluster`, `query`, `preview`, `replay`, `impact` [after Task 11], `env diff/history/rollback`, `test-connection` if present), align Rust version claim
- Create: `rust-toolchain.toml` (channel = the version CI actually uses; check `ci.yml`)
- Modify: `conduit-cli/tests/cli_smoke_test.rs` — add a test that runs `--help` for every README-documented subcommand

**Steps:**
- [x] Extract the clap command list: `cargo run -p conduit-cli -- --help`.
- [x] Rewrite README install + command table to match reality.
- [x] Add smoke test `readme_documented_commands_parse` iterating `["init","compile","run","serve","plan","apply","env","lineage","migrate","status","backfill","worker","cluster","query","preview","replay"]` → `conduit <cmd> --help` exits 0.
- [x] Run: `cargo test -p conduit-cli --test cli_smoke_test` passes.
- [x] Commit: `docs: make README install + command table match the CLI (PRD B5)`

### Task 6: Repo hygiene (B8)

**Files:**
- Delete: `OPENAPI_IMPLEMENTATION.md`, `STORAGE_PROVIDERS_IMPLEMENTATION.md`, `IMPLEMENTATION_SUMMARY.txt`
- Modify: `docs/src/introduction.md:195`, `docs/src/architecture.md:24` — conduit-python is "PyO3 native bindings (compiler/planner/lineage/state)", the SDK is `sdk/python`
- Modify: `docs/LINEAGE_VS_SQLGLOT.md:50-54` — mark binding item shipped (`6a3e192`), and the "8 commits behind" warning resolved
- NOT in scope: deleting branches (surface list to Jay instead)

**Steps:**
- [x] Make deletions + edits.
- [x] Commit: `chore: remove stale session artifacts, fix conduit-python doc labels (PRD B8)`

### Task 7: Constant-time + O(1)-hash key auth (A2, BUG-2)

**Files:**
- Modify: `conduit-api/Cargo.toml` — add `subtle = "2"`
- Modify: `conduit-api/src/auth.rs:340-376` — `authenticate()`
- Test: `conduit-api/tests/auth_test.rs` (extend)

**Interfaces:**
- Produces: `AuthStore::authenticate(&self, plaintext_key: &str) -> Result<AuthContext, AuthError>` (unchanged signature; new behavior: filters candidates by stored `key_prefix` — first 8 chars of plaintext — so exactly the matching-prefix keys are hashed; comparison via `subtle::ConstantTimeEq` on hash bytes).

**Steps:**
- [x] Write failing test `authenticate_hashes_only_prefix_matched_keys` — create 3 keys, authenticate with one, assert success; and `authenticate_uses_constant_time_compare` is not directly testable — instead assert behavior parity: valid key OK, revoked → KeyRevoked, expired → KeyExpired, wrong key → InvalidKey (existing tests must stay green).
- [x] Implement: candidates = keys where `plaintext_key.starts_with(&stored.key_prefix)`; for each, `hash_key(salt, plaintext)`, compare `hash.as_bytes().ct_eq(stored_hash.as_bytes())`.
- [x] Run: `cargo test -p conduit-api auth` → pass.
- [x] Commit: `fix(api): constant-time API-key comparison, prefix-indexed lookup (PRD A2)`

### Task 8: Global auth enforcement + handler permissions (A1, BUG-1)

**Files:**
- Modify: `conduit-api/src/middleware.rs` — add router middleware
- Modify: `conduit-api/src/routes.rs` — apply middleware to `api_routes`; add public allowlist
- Modify: `conduit-api/src/auth.rs` — add `Permission::IngestLineage` (Operator group)
- Modify handlers (add `auth: RequireAuth` param + `auth.require(Permission::X)?` as FIRST statement):
  - `handlers/runs.rs::trigger_run` → TriggerRun
  - `handlers/dags.rs::compile_dags` → CompileDags
  - `handlers/envs.rs::{create_environment→CreateEnvironment, delete_environment→DeleteEnvironment, promote_environment→PromoteEnvironment, rollback_environment→PromoteEnvironment, update_env_policy→PromoteEnvironment}`
  - `handlers/plan.rs::{generate_plan→GeneratePlan, apply_plan→ApplyPlan}`
  - `handlers/backfill.rs::create_backfill` → CreateBackfill
  - `handlers/cluster.rs::drain_worker` → DrainWorker
  - `handlers/lineage.rs::{extract_sql_lineage, trace_upstream, trace_downstream, lineage_graph, refresh_catalog}` → ExtractLineage; `validate_contract` → ValidateContract; `schema_diff` → ExtractLineage
  - `handlers/openlineage_ingest.rs::{ingest_event→IngestLineage, cache_invalidate→ExtractLineage}`
- Test: create `conduit-api/tests/auth_enforcement_test.rs`

**Interfaces:**
- Produces: `middleware::auth_gate` — `pub async fn auth_gate(State<Arc<AppState>>, Request, Next) -> Response`; PUBLIC_PATHS allowlist = `/health`, `/info`, `/docs`, `/docs/openapi.json`, `/docs/redoc` (paths as seen inside the nest, no `/api/v1` prefix). When `auth_enabled` and path not public: extract bearer → `authenticate` → 401 JSON on failure (reuse `AuthApiError`); insert `AuthContext` into request extensions. When disabled: pass through.
- WebSocket route `/ws/events`: NOT gated in this task (browser WS cannot set headers; UI would break — document in SECURITY.md task later, PRD A5/M4).

**Steps:**
- [x] Write failing table-driven test: with auth enabled and no key, every route in a `MUTATING: &[(Method, &str)]` table (all POST/PUT/DELETE routes from routes.rs) returns 401; `GET /api/v1/dags` returns 401; `GET /api/v1/health` returns 200.
- [x] Second test: Viewer key → 403 on `POST /api/v1/dags/x/runs`; Operator key → non-401/403 status.
- [x] Third test: auth disabled → everything passes as today (existing integration tests cover; keep green).
- [x] Implement middleware + wire `.layer(middleware::from_fn_with_state(state.clone(), auth_gate))` on api_routes (note: `build_router` receives `state` before `.with_state` — capture the Arc).
- [x] Add handler permission checks (mechanical sweep above).
- [x] Fix `routes.rs:8-14` doc comment and startup log to match new reality.
- [x] Run: `cargo test -p conduit-api` → pass; `cargo clippy -p conduit-api -- -D warnings`.
- [x] Commit: `feat(api): enforce authentication globally + permission checks on mutating handlers (PRD A1)`

### Task 9: Register connection-test route (C2, BUG-9)

**Files:**
- Modify: `conduit-api/src/routes.rs:170-178` — add `.route("/connections/:name/test", post(handlers::connections::test_connection))`
- Modify: `conduit-api/src/handlers/connections.rs:45` — add `auth: RequireAuth` + `auth.require(Permission::ViewConnections)?`
- Test: extend `auth_enforcement_test.rs` table + a direct handler test if a fixture exists

**Steps:**
- [x] Add route + auth check; add `(Method::POST, "/api/v1/connections/x/test")` to the sweep table.
- [x] Run: `cargo test -p conduit-api` → pass.
- [x] Commit: `fix(api): register POST /connections/:name/test route (PRD C2)`

### Task 10: CORS restriction (A4, GAP-8)

**Files:**
- Modify: `conduit-api/src/state.rs` — add `pub cors_allowed_origins: Vec<String>` to `AppState` (default empty), add setter `with_cors_origins`
- Modify: `conduit-api/src/routes.rs:229-234` — empty list → `CorsLayer` with no cross-origin allowance (same-origin only); non-empty → parse each into `HeaderValue` allow-list
- Modify: `conduit-cli` serve command — repeatable `--cors-origin <URL>` flag threaded to AppState
- Docs: note in `docs/src/reference/cli-reference.md` serve section + `docker-compose.yml` ui-dev profile gets `--cors-origin http://localhost:3000` (check the dev port used by vite config first)

**Steps:**
- [x] Test: router built with empty origins does not include `access-control-allow-origin: *` on a preflight response; with `--cors-origin http://localhost:3000` it echoes exactly that origin.
- [x] Implement + thread the flag.
- [x] Run: `cargo test -p conduit-api`, `cargo check --workspace`.
- [x] Commit: `feat(api): configurable CORS allow-list, same-origin by default (PRD A4)`

### Task 11: `conduit impact` subcommand (C1, BUG-3)

**Files:**
- Modify: `conduit-cli/src/main.rs` — new clap variant + `cmd_impact`
- Test: `conduit-cli/tests/cli_smoke_test.rs` + fixture DAGs under `conduit-cli/tests/fixtures/impact/{base,head}/`

**Interfaces:**
- Consumes: `conduit_lineage::plan_impact::analyze(base: &DagSet, head: &DagSet) -> PlanImpact` (`DagSet = HashMap<String, Dag>`); `conduit_lineage::impact_report::{ImpactFormat, render}`; the existing DAG-compile helper in main.rs (reuse whatever `cmd_compile`/`cmd_lineage` use to produce `Vec<Dag>`).
- CLI contract (must match `.github/workflows/conduit-impact.yml:71-86` exactly):
  ```
  conduit impact --base <git-ref> --head <git-ref|WORKING> --dags-path <dir> --format <markdown|json> --output <path>
  conduit impact --base-plan <dags-dir-or-compiled-json> --head-plan <…> --format … [--output …]
  ```
  Git mode: `git worktree add <tmp> <ref>` (cleanup via `git worktree remove --force` in a drop guard), compile `<tmp>/<dags-path>`; `WORKING` compiles `<dags-path>` in place. Exit 0 on successful analysis (breaking changes do NOT fail the exit — gating is the workflow's label logic); exit ≠0 only on operational errors. JSON output must serialize `PlanImpact` so `.summary.total_breaking_changes` resolves.

**Steps:**
- [x] Fixtures: base DAG with SQL task producing columns (a,b); head drops column b consumed downstream (crib the shape from `conduit-lineage/tests/plan_impact_e2e.rs`).
- [x] Write failing smoke test `impact_plan_file_mode_reports_breaking`: run binary with `--base-plan fixtures/impact/base --head-plan fixtures/impact/head --format json`, assert exit 0 and `summary.total_breaking_changes >= 1`.
- [x] Implement clap variant + cmd_impact (plan-file mode first, then git mode).
- [x] Test git mode manually in a scratch repo: two commits, run `--base HEAD~1 --head WORKING`.
- [x] Add `impact` to the Task-5 smoke list and README table.
- [x] Run: `cargo test -p conduit-cli`, `cargo clippy -p conduit-cli -- -D warnings`.
- [x] Commit: `feat(cli): wire conduit impact subcommand — plan-file + git modes (PRD C1)`

### Task 12: SDK bootstrap outside the checkout (B3, BUG-4)

**Files:**
- Modify: `conduit-cli/Cargo.toml` — add `include_dir = "0.7"`
- Modify: `conduit-cli/src/main.rs` (`cmd_init`, `main.rs:851-941`) — vendor SDK into `<project>/.conduit/sdk/conduit_sdk/` via `static SDK_DIR: include_dir::Dir = include_dir!("$CARGO_MANIFEST_DIR/../sdk/python/conduit_sdk")` (skip `__pycache__`); write a `VERSION` stamp file
- Modify: `conduit-executor/src/process_runner.rs:599-614` — discovery order: (1) `CONDUIT_SDK_PATH` env var, (2) walk up from CWD looking for `.conduit/sdk` then `sdk/python` (repo case), (3) existing binary-relative walk; on total failure the Python error path should surface: extend the run error message when stderr contains `ModuleNotFoundError: No module named 'conduit_sdk'` with hint "set CONDUIT_SDK_PATH or re-run `conduit init` (vendored SDK missing), or pip install conduit-sdk"
- Test: `conduit-executor` unit test for discovery order; `conduit-cli` integration test for init-vendors-SDK

**Interfaces:**
- Produces: `discover_sdk_path() -> Option<String>` (same name/signature, new search order — document each tier in the doc comment).

**Steps:**
- [x] Failing test `init_vendors_python_sdk`: run `conduit init tmpproj`, assert `tmpproj/.conduit/sdk/conduit_sdk/__init__.py` exists.
- [x] Failing test `discover_sdk_prefers_env_override` (executor): set env, assert returned path.
- [x] Implement vendoring + discovery.
- [x] End-to-end proof (the PRD acceptance): copy `target/release/conduit` to `/tmp/bin-only/conduit`, `cd /tmp && ./bin-only/conduit init demo && cd demo && ../bin-only/conduit run hello_world` succeeds.
- [x] Run: `cargo test -p conduit-cli -p conduit-executor`.
- [x] Commit: `feat(cli): vendor Python SDK into init scaffolds; CONDUIT_SDK_PATH override (PRD B3)`

### Task 13: conduit-sdk publish workflow (B4 part 2)

**Files:**
- Create: `.github/workflows/publish-sdk.yml` — trigger `sdk-v*` tags; `hatchling`/`python -m build` sdist+wheel; PyPI via `pypa/gh-action-pypi-publish` with trusted publishing (`id-token: write`); also attach to GitHub Release
- Modify: `sdk/python/pyproject.toml` — version → `0.2.0`

**Steps:**
- [x] Write workflow (pure-python wheel, single build job).
- [x] Validate: `actionlint .github/workflows/publish-sdk.yml` if available, else YAML parse.
- [x] Note for Jay: PyPI trusted-publisher must be configured on pypi.org for `conduit-sdk` before first tag.
- [x] Commit: `ci(sdk): publish conduit-sdk wheels to PyPI on sdk-v* tags (PRD B4)`

---

## Self-review notes

- Spec coverage: M0 = Tasks 1–6 (B1 ✓, B2 ✓, B5 ✓, B8 ✓, D6-version ✓ + B4-readme pulled forward); M1 = Tasks 7–10 (A2 ✓, A1 ✓, C2 ✓, A4 ✓); M2 = Tasks 11–13 (C1 ✓, B3 ✓, B4 ✓). A3/A5 (audit log, redteam/SECURITY.md) are M4 per PRD — the anonymous-mutation sweep from A5 is delivered early inside Task 8's test.
- Type consistency: `auth_gate` name used in Tasks 8; `discover_sdk_path` keeps its existing name (Task 12); `Permission::IngestLineage` introduced in Task 8 and not referenced elsewhere.
- Branch pruning intentionally excluded (destructive; surfaced to Jay separately).
