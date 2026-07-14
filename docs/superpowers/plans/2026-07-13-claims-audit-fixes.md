# Claims-Audit Round 2 Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Conduit's product surface honest — SQL tasks really execute (or fail loudly), apply really applies (CLI and API), stale plans are rejected, contracts and incremental processing actually run, distributed commands really talk gRPC, and concurrency flags are honored — closing all 7 findings from the 2026-07-13 end-to-end claims audit.

**Architecture:** Almost every fix wires up machinery that already exists but is dead code: `ProviderRegistry`/`run_with_providers` (conduit-providers/conduit-executor), `ContractEvaluator` (conduit-common), `IncrementalEngine`/`WatermarkStore` (conduit-planner), `Environment.current_version` (conduit-common), persistent store constructors (conduit-state), and the full `conduit-distributed` library. The CLI (`conduit-cli/src/main.rs`, 4230 lines, all commands in one file) and API handlers (`conduit-api/src/handlers/`) are the integration points.

**Tech Stack:** Rust 2021 workspace; tokio, axum, tonic/prost (protoc required for Task 10/11 proto regen), clap, serde, RocksDB (conduit-state), DuckDB (bundled, in-process — the zero-dependency real SQL provider for tests).

## Global Constraints

- Every commit: `cargo fmt --all` first; commit with `git commit -s` (DCO sign-off — repo convention); conventional-commit subject (`fix:`, `feat:`, `docs:`, `test:`); **no `Co-Authored-By` trailer** (user preference).
- Never fabricate success output. If a code path cannot do the real work, it must return an error — that is the entire point of this plan.
- Tests: `cargo test -p <crate>` per touched crate; CLI black-box tests live in `conduit-cli/tests/cli_smoke_test.rs` using `assert_cmd` (`Command::cargo_bin("conduit")`) + `predicates` + `tempfile` (all already dev-deps).
- Do not break existing tests. If an existing test asserts stub behavior (e.g. SQL fake-success), update it to assert the new honest behavior — say so in the commit message.
- `conduit-cli/src/main.rs` is one big file by design — add code in the section matching its command (sections are marked with `// ─── conduit <cmd> ───` banners). Do not restructure it.
- Line numbers below are from the pre-plan tree; earlier tasks shift later anchors. Anchor by the quoted code, not the number.

**Task order note:** Tasks 1–3 are the criticals; 4–8 the highs; 9–12 medium + cleanup. Task 8 depends on 1, 3, 7 (and reuses patterns from 4). Task 11 depends on 10. Everything else is independent.

---

### Task 1: SQL tasks execute for real or fail loudly (provider registry wiring)

**Files:**
- Modify: `conduit-executor/src/process_runner.rs` (the `TaskType::Sql` arm of `build_command`, ~line 469)
- Modify: `conduit-providers/src/registry.rs` (add `register` method, ~line 143)
- Modify: `conduit-cli/src/main.rs` (helper + 4 call sites: cmd_run ~1373, cmd_apply ~1728, cmd_serve ~2091, cmd_backfill ~3450; serve provider init ~1930)
- Test: `conduit-executor/tests/sql_provider_test.rs` (new)
- Test: `conduit-cli/tests/cli_smoke_test.rs`

**Interfaces:**
- Consumes: `ProcessRunner::run_with_providers(task: &Task, context: &TaskContext, registry: Option<&ProviderRegistry>) -> ConduitResult<ProcessOutput>` (exists, process_runner.rs:104); `ProviderRegistry::from_configs_with_secrets(&HashMap<String, ConnectionConfig>, &SecretsConfig) -> ProviderRegistry` (async, registry.rs:195); `find_conduit_yaml(dags_path: &Path) -> Option<PathBuf>` (main.rs:1117); `AppState::init_providers(&self, connections)` (async, conduit-api/src/state.rs:221); `AppState.provider_registry: RwLock<Option<Arc<ProviderRegistry>>>`.
- Produces: `async fn build_provider_registry(dags_path: &Path) -> Arc<conduit_providers::ProviderRegistry>` in main.rs (used again by Tasks 5, 8, 9, 11); `ProviderRegistry::register(&mut self, name: impl Into<String>, instance: ProviderInstance)`; the invariant "SQL task with no registered provider ⇒ `Err`, never fake success".

- [ ] **Step 1: Write the failing executor tests**

Create `conduit-executor/tests/sql_provider_test.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use conduit_common::dag::{ResourceLimits, Task, TaskType, TriggerRule};
use conduit_executor::process_runner::{ProcessRunner, TaskContext};
use conduit_providers::providers::duckdb::DuckDbProvider;
use conduit_providers::registry::ProviderInstance;
use conduit_providers::ProviderRegistry;

fn make_sql_task(id: &str, connection: &str, query: &str) -> Task {
    Task {
        id: id.to_string(),
        task_type: TaskType::Sql {
            connection: connection.to_string(),
            query: query.to_string(),
            target: None,
        },
        dependencies: vec![],
        retries: 0,
        retry_delay: None,
        retry_backoff: None,
        source_hash: None,
        pool: None,
        timeout: None,
        priority: 0,
        resources: ResourceLimits::default(),
        trigger_rule: TriggerRule::default(),
        incremental: None,
        contracts: None,
        inputs: vec![],
        outputs: vec![],
    }
}

fn make_context(task_id: &str) -> TaskContext {
    TaskContext {
        dag_id: "sql_dag".to_string(),
        run_id: format!("test_run_{}", task_id),
        task_id: task_id.to_string(),
        attempt: 1,
        logical_date: chrono::Utc::now(),
        environment: "test".to_string(),
        params: HashMap::new(),
    }
}

#[tokio::test]
async fn sql_task_executes_via_registered_provider() {
    let mut registry = ProviderRegistry::new();
    registry.register(
        "analytics",
        ProviderInstance::Sql(Arc::new(DuckDbProvider::ephemeral())),
    );

    let task = make_sql_task("select_one", "analytics", "SELECT 1 AS n, 'hi' AS greeting");
    let ctx = make_context("select_one");

    let output = ProcessRunner::run_with_providers(&task, &ctx, Some(&registry))
        .await
        .expect("native SQL execution should succeed");

    assert_eq!(output.exit_code, 0);
    // The native path records real row counts — the old stub always said 0.
    let xcom = output.xcom.expect("SQL task should emit xcom");
    assert_eq!(xcom["rows_affected"], 1);
    assert!(
        !output.stdout.contains("SQL execution completed"),
        "must not go through the fake subprocess stub"
    );
}

#[tokio::test]
async fn sql_task_without_provider_fails_loudly() {
    let task = make_sql_task("orphan", "warehouse", "SELECT 1");
    let ctx = make_context("orphan");

    // No registry at all (the old ProcessRunner::run path).
    let err = ProcessRunner::run(&task, &ctx)
        .await
        .expect_err("SQL without a provider must be an error, not fake success");
    let msg = err.to_string();
    assert!(msg.contains("warehouse"), "error names the connection: {msg}");
    assert!(msg.contains("conduit.yaml"), "error tells the user the fix: {msg}");
}

#[tokio::test]
async fn sql_task_with_registry_missing_connection_fails_loudly() {
    let registry = ProviderRegistry::new(); // empty
    let task = make_sql_task("orphan2", "warehouse", "SELECT 1");
    let ctx = make_context("orphan2");

    let result = ProcessRunner::run_with_providers(&task, &ctx, Some(&registry)).await;
    assert!(result.is_err());
}
```

Note: `TaskContext` gains an `extra_env` field in Task 5 — if Task 5 landed first, add `extra_env: vec![]` here. Verify the exact `Task`/`TriggerRule` field set against `conduit-common/src/dag.rs` (`Task` at ~:139) and copy the construction style from `make_bash_task` in `conduit-executor/src/process_runner.rs:747` if fields differ.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p conduit-executor --test sql_provider_test`
Expected: `register` does not exist (compile error), and after stubbing that, `sql_task_without_provider_fails_loudly` FAILS because the stub returns fake success.

- [ ] **Step 3: Add `ProviderRegistry::register`**

In `conduit-providers/src/registry.rs`, after `pub fn new()` (~:143):

```rust
    /// Register a pre-built provider instance under a connection name.
    ///
    /// Config-driven setups should use `from_configs`; this is for tests and
    /// embedders that construct providers directly.
    pub fn register(&mut self, name: impl Into<String>, instance: ProviderInstance) {
        self.providers.insert(name.into(), instance);
    }
```

- [ ] **Step 4: Make the subprocess SQL fallback an error**

In `conduit-executor/src/process_runner.rs`, replace the whole `TaskType::Sql { query, .. }` arm of `build_command` (~469–488, the block that prints `SQL execution started/completed` and `rows_affected: 0`) with:

```rust
            TaskType::Sql { connection, .. } => Err(ConduitError::ExecutionError(format!(
                "SQL task '{}' requires a provider for connection '{}', but none is \
                 configured. Add the connection to conduit.yaml under `connections:` \
                 (e.g. type: duckdb / postgres / sqlite). Refusing to fake SQL execution.",
                task.id, connection
            ))),
```

Check the file's existing error imports — it uses `ConduitResult`; import `ConduitError` from `conduit_common` the same way sibling code does.

- [ ] **Step 5: Run the executor tests to verify they pass**

Run: `cargo test -p conduit-executor --test sql_provider_test`
Expected: 3 passed. Also run `cargo test -p conduit-executor` — the pre-existing suite must stay green (it only uses bash/python tasks).

- [ ] **Step 6: Add the CLI registry helper**

In `conduit-cli/src/main.rs`, after `warn_on_stub_connections` (~:1219):

```rust
/// Build a provider registry from the project's conduit.yaml `connections:`.
///
/// Returns an empty registry when no config file or no connections exist —
/// SQL tasks then fail loudly instead of pretending to run.
async fn build_provider_registry(
    dags_path: &Path,
) -> std::sync::Arc<conduit_providers::ProviderRegistry> {
    let registry = match find_conduit_yaml(dags_path) {
        Some(config_path) => match conduit_common::config::ConduitConfig::load(&config_path) {
            Ok(cfg) if !cfg.connections.is_empty() => {
                conduit_providers::ProviderRegistry::from_configs_with_secrets(
                    &cfg.connections,
                    &cfg.secrets,
                )
                .await
            }
            Ok(_) => conduit_providers::ProviderRegistry::new(),
            Err(e) => {
                eprintln!(
                    "Warning: failed to load {}: {} — SQL tasks will fail without providers",
                    config_path.display(),
                    e
                );
                conduit_providers::ProviderRegistry::new()
            }
        },
        None => conduit_providers::ProviderRegistry::new(),
    };
    std::sync::Arc::new(registry)
}
```

(Verify `ConduitConfig::load`'s exact signature at `conduit-common/src/config.rs:184` — adjust `&config_path` vs by-value accordingly.)

- [ ] **Step 7: Wire the registry into all four execution paths**

1. **cmd_run** (~:1284, before the scheduler channels): `let registry = build_provider_registry(dags_path).await;` then inside the executor task move a clone (`let registry_for_exec = std::sync::Arc::clone(&registry);` before `tokio::spawn`, use `registry_for_exec` inside) and change line ~1373 to:
   `match ProcessRunner::run_with_providers(task, &context, Some(&registry_for_exec)).await {`
2. **cmd_apply** (~:1590, after `let state = PersistentState::open(...)`): `let registry = build_provider_registry(dags_path).await;` and change ~1728 to `ProcessRunner::run_with_providers(task, &context, Some(&registry)).await`.
3. **cmd_backfill** (~:3345, before the partition loop): build once, clone the Arc into each partition's executor task, change ~3450 the same way.
4. **cmd_serve**: after `AppState::with_options(...)` (~:1930) add:

```rust
    // Initialize SQL providers from conduit.yaml so both the /connections API
    // and the server-side executor run SQL for real.
    if let Some(config_path) = find_conduit_yaml(dags_path) {
        match conduit_common::config::ConduitConfig::load(&config_path) {
            Ok(cfg) => {
                state.init_providers(&cfg.connections).await;
                println!("  Providers:   {} connection(s) registered", cfg.connections.len());
            }
            Err(e) => eprintln!("  Providers:   WARNING — failed to load conduit.yaml: {}", e),
        }
    }
```

   and in the executor loop's `DispatchTask` arm (before building `TaskContext`, ~:2075):

```rust
    let registry = exec_state
        .provider_registry
        .read()
        .ok()
        .and_then(|guard| guard.clone());
```

   then inside the per-task `tokio::spawn` change ~2091 to `ProcessRunner::run_with_providers(&task, &context, registry.as_deref()).await` (move `registry` into the spawn; clone it per dispatch iteration).

- [ ] **Step 8: Update/extend CLI smoke tests**

In `conduit-cli/tests/cli_smoke_test.rs`:

a) New helper — a project with a real DuckDB connection (file-backed so all tasks share state):

```rust
/// Write a conduit.yaml declaring a real (bundled) DuckDB connection named
/// `warehouse`, pointing at a file DB inside the temp project.
fn write_duckdb_project_config(dir: &TempDir) {
    let db_path = dir.path().join("warehouse.duckdb");
    let config = format!(
        r#"
name: smoke_project
dags_path: ./dags
connections:
  warehouse:
    type: duckdb
    database: "{}"
"#,
        db_path.display()
    );
    fs::write(dir.path().join("conduit.yaml"), config).unwrap();
}
```

b) New tests:

```rust
#[test]
fn cli_run_sql_dag_without_connection_fails_loudly() {
    let dir = TempDir::new().unwrap();
    let dags = write_sql_dag(&dir); // references connection `warehouse`, no conduit.yaml

    conduit()
        .args(["run", "sql_lineage", "--dags-path"])
        .arg(&dags)
        .assert()
        .failure()
        .stderr(predicate::str::contains("warehouse").and(predicate::str::contains("conduit.yaml")));
}

#[test]
fn cli_run_sql_dag_with_duckdb_executes_for_real() {
    let dir = TempDir::new().unwrap();
    write_duckdb_project_config(&dir);
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();
    // Self-contained query (no pre-existing tables needed).
    let dag = r#"
id: duck_smoke
tasks:
  select_two:
    type: sql
    connection: warehouse
    query: "SELECT 1 AS a UNION ALL SELECT 2"
"#;
    fs::write(dags.join("duck_smoke.yaml"), dag).unwrap();

    conduit()
        .args(["run", "duck_smoke", "--dags-path"])
        .arg(&dags)
        .assert()
        .success()
        .stdout(
            predicate::str::contains("row_count")
                .and(predicate::str::contains("SQL execution completed").not()),
        );
}
```

c) Sweep existing tests that `run` a SQL dag fixture (grep `write_sql_dag(` / `write_cross_task_sql_dag(` usages). Lineage-only tests (compile/lineage/impact — no execution) are unaffected. Any test that *runs* those DAGs and asserted success must now either add `write_duckdb_project_config` **and change the fixture's connection queries to valid DuckDB SQL**, or assert `.failure()`. Prefer keeping one honest-failure test and converting the rest to the DuckDB config.

- [ ] **Step 9: Run the full CLI test suite**

Run: `cargo test -p conduit-cli`
Expected: PASS (including the two new tests). If `duck_smoke` fails on the native path, debug with `--verbose` — do not weaken the assertion.

- [ ] **Step 10: Commit**

```bash
cargo fmt --all
git add -A
git commit -s -m "fix(executor,cli): execute SQL via provider registry; refuse to fake SQL success

SQL tasks now route through ProviderRegistry built from conduit.yaml
connections in run/apply/backfill/serve. The subprocess fallback that
printed 'SQL execution completed' with rows_affected=0 is now a hard
error naming the unconfigured connection. (Claims-audit finding 1)"
```

---

### Task 2: `conduit apply` exits non-zero when execution fails

**Files:**
- Modify: `conduit-cli/src/main.rs` (cmd_apply failure branches ~1780–1788)
- Test: `conduit-cli/tests/cli_smoke_test.rs`

**Interfaces:**
- Consumes: nothing new. Produces: apply's exit code is a valid CI gate (matches cmd_run's behavior at ~:1489).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn cli_apply_fails_with_nonzero_exit_when_task_fails() {
    let dir = TempDir::new().unwrap();
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();
    let dag = r#"
id: failing_apply
tasks:
  boom:
    type: bash
    command: "echo about-to-fail >&2; exit 1"
"#;
    fs::write(dags.join("failing_apply.yaml"), dag).unwrap();

    conduit()
        .args(["apply", "production", "-y", "--dags-path"])
        .arg(&dags)
        .assert()
        .failure()
        .stderr(predicate::str::contains("apply aborted"));
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p conduit-cli --test cli_smoke_test cli_apply_fails_with_nonzero_exit`
Expected: FAIL — the command currently exits 0.

- [ ] **Step 3: Replace the swallowed failures with bails**

In cmd_apply, the task-failure branch (~:1780–1782): replace

```rust
                            eprintln!("Apply aborted due to task failure.");
                            return Ok(());
```

with

```rust
                            anyhow::bail!(
                                "apply aborted: task {}.{} failed with exit code {}",
                                action.dag_id, action.task_id, output.exit_code
                            );
```

and the execution-error branch (~:1786–1788): replace

```rust
                        eprintln!("Apply aborted due to execution error.");
                        return Ok(());
```

with

```rust
                        anyhow::bail!(
                            "apply aborted: task {}.{} execution error: {}",
                            action.dag_id, action.task_id, e
                        );
```

(anyhow's error is printed by `main() -> Result<()>` as `Error: apply aborted: …` on stderr with exit code 1 — that's what the test asserts.)

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test -p conduit-cli --test cli_smoke_test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A
git commit -s -m "fix(cli): apply exits non-zero when a task fails or errors

Both failure branches returned Ok(()) so CI gating on conduit apply
was impossible. (Claims-audit finding 2, CLI half)"
```

---

### Task 3: Stale-plan protection (base environment version)

**Files:**
- Modify: `conduit-planner/src/deployment_plan.rs` (struct ~:89, `generate` ~:130, `filtered_to` ~:423 — propagate the field)
- Modify: `conduit-cli/src/main.rs` (cmd_apply plan-file branch ~:1606)
- Modify: `docs/src/concepts/plan-apply.md` (Conflict Detection section, ~:293–321)
- Test: `conduit-planner/src/deployment_plan.rs` (unit tests, mod at ~:850)
- Test: `conduit-cli/tests/cli_smoke_test.rs`

**Interfaces:**
- Consumes: `Environment.current_version: u32` (conduit-common/src/snapshot.rs:68 — persisted, bumped by `EnvironmentManager::apply_snapshot_map`/promote/rollback).
- Produces: `DeploymentPlan.base_environment_version: u32` (serde-default 0) — Task 8 reuses this for the API 409 check.

- [ ] **Step 1: Write the failing planner unit tests**

In the existing `#[cfg(test)] mod tests` of `deployment_plan.rs` (copy setup from the round-trip test at ~:861 that builds a plan against a `staging` env):

```rust
    #[test]
    fn plan_records_base_environment_version() {
        // Reuse the same fixture helpers as the existing round-trip test.
        let (plan, mut env, store) = make_test_fixture(); // adapt to actual helper names
        env.current_version = 3;

        let deploy = DeploymentPlan::generate(&plan, &env, &store);
        assert_eq!(deploy.base_environment_version, 3);

        let restored = DeploymentPlan::from_json(&deploy.to_json().unwrap()).unwrap();
        assert_eq!(restored.base_environment_version, 3);
    }

    #[test]
    fn old_plan_json_without_base_version_defaults_to_zero() {
        let (plan, env, store) = make_test_fixture();
        let deploy = DeploymentPlan::generate(&plan, &env, &store);
        let mut value: serde_json::Value = serde_json::from_str(&deploy.to_json().unwrap()).unwrap();
        value.as_object_mut().unwrap().remove("base_environment_version");
        let restored = DeploymentPlan::from_json(&value.to_string()).unwrap();
        assert_eq!(restored.base_environment_version, 0);
    }
```

(There is no literal `make_test_fixture` — inline whatever the neighboring tests at ~:861/:1001 do to build a `ConduitPlan`, `Environment`, and `SnapshotStore`.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p conduit-planner plan_records_base_environment_version`
Expected: compile FAIL (no such field).

- [ ] **Step 3: Add the field and capture it**

In the `DeploymentPlan` struct, right after `target_environment`:

```rust
    /// The environment revision (`Environment::current_version`) this plan
    /// was generated against. `apply` refuses the plan when the live
    /// environment has moved past it. Plans saved before this field existed
    /// deserialize as 0.
    #[serde(default)]
    pub base_environment_version: u32,
```

In `generate` (~:167, the struct literal): add `base_environment_version: environment.current_version,`. Fix every other `DeploymentPlan { … }` literal the compiler flags (`filtered_to` must copy `base_environment_version: self.base_environment_version`).

- [ ] **Step 4: Run planner tests**

Run: `cargo test -p conduit-planner`
Expected: PASS (new + existing).

- [ ] **Step 5: Enforce in cmd_apply**

In `conduit-cli/src/main.rs`, the plan-file branch (~:1606) becomes:

```rust
    let deploy = if let Some(pf) = plan_file {
        println!("Loading plan from {}...", pf.display());
        let deploy = DeploymentPlan::from_json(&std::fs::read_to_string(pf)?)?;

        if deploy.target_environment != environment {
            anyhow::bail!(
                "plan file targets environment '{}' but apply was invoked for '{}'.\n  \
                 Re-run as: conduit apply {} --plan-file {}",
                deploy.target_environment, environment,
                deploy.target_environment, pf.display()
            );
        }

        let current_version = state
            .env_manager
            .get(environment)
            .map(|e| e.current_version)
            .unwrap_or(0);
        if current_version != deploy.base_environment_version {
            eprintln!("Error: stale plan — environment '{}' changed since this plan was generated.", environment);
            eprintln!("  Current environment version: {}", current_version);
            eprintln!("  Plan was based on version:    {}", deploy.base_environment_version);
            eprintln!();
            eprintln!("Recommended action:");
            eprintln!("  conduit plan {} --output <plan.json>   # regenerate against current state", environment);
            eprintln!("  conduit apply {} --plan-file <plan.json> -y", environment);
            anyhow::bail!(
                "stale plan rejected (environment version {} != plan base version {})",
                current_version, deploy.base_environment_version
            );
        }
        deploy
    } else {
        // …existing generate branch unchanged…
```

- [ ] **Step 6: Write the failing CLI smoke test**

```rust
#[test]
fn cli_apply_rejects_stale_plan_file() {
    let dir = TempDir::new().unwrap();
    let dags = write_yaml_dag(&dir); // bash-only fixture
    let plan_path = dir.path().join("plan.json");

    // Save a plan against the empty environment (version 0).
    conduit()
        .args(["plan", "production", "--dags-path"])
        .arg(&dags)
        .arg("--output")
        .arg(&plan_path)
        .assert()
        .success();

    // Applying the saved plan while the env is still at version 0 works.
    conduit()
        .args(["apply", "production", "-y", "--plan-file"])
        .arg(&plan_path)
        .arg("--dags-path")
        .arg(&dags)
        .assert()
        .success();

    // The apply bumped the env to version 1 — the same plan is now stale.
    conduit()
        .args(["apply", "production", "-y", "--plan-file"])
        .arg(&plan_path)
        .arg("--dags-path")
        .arg(&dags)
        .assert()
        .failure()
        .stderr(predicate::str::contains("stale plan"));
}

#[test]
fn cli_apply_rejects_plan_for_different_environment() {
    let dir = TempDir::new().unwrap();
    let dags = write_yaml_dag(&dir);
    let plan_path = dir.path().join("plan.json");

    conduit()
        .args(["plan", "staging", "--dags-path"]).arg(&dags)
        .arg("--output").arg(&plan_path)
        .assert().success();

    conduit()
        .args(["apply", "production", "-y", "--plan-file"]).arg(&plan_path)
        .arg("--dags-path").arg(&dags)
        .assert()
        .failure()
        .stderr(predicate::str::contains("targets environment 'staging'"));
}
```

- [ ] **Step 7: Run, verify pass**

Run: `cargo test -p conduit-cli --test cli_smoke_test cli_apply_rejects`
Expected: PASS.

- [ ] **Step 8: Update the docs to the implemented behavior**

In `docs/src/concepts/plan-apply.md` "Conflict Detection" (~:293–321): replace the fictional `prod-snap-…` snapshot-id example with the real output:

```
Error: stale plan — environment 'production' changed since this plan was generated.
  Current environment version: 4
  Plan was based on version:    3

Recommended action:
  conduit plan production --output plan.json   # regenerate against current state
  conduit apply production --plan-file plan.json -y
```

Keep the closing line "Conduit prevents applying stale plans." — it is now true. Also state that plans record `base_environment_version` and that applying a plan to a different environment than its `target_environment` is rejected.

- [ ] **Step 9: Commit**

```bash
cargo fmt --all
git add -A
git commit -s -m "feat(planner,cli): reject stale plan files via base environment version

DeploymentPlan now records the environment revision it was generated
against; apply refuses a plan whose base version no longer matches the
live environment, and refuses plans targeting a different environment.
Docs updated to the real conflict output. (Claims-audit finding 3)"
```

---

### Task 4: Contracts are validated during apply and block deployment

**Files:**
- Modify: `conduit-cli/src/main.rs` (cmd_apply Execute arm ~:1697–1789 and post-loop ~:1810)
- Test: `conduit-cli/tests/cli_smoke_test.rs`

**Interfaces:**
- Consumes: `ContractEvaluator::evaluate(&TaskContracts, &Evidence) -> ValidationResult` (conduit-common/src/contracts.rs:257); `ProcessOutput.evidence: Evidence`; `DeploymentPlan.pending_contracts: Vec<TaskContracts>`; `DeploymentValidation::from_results(Vec<ValidationResult>) -> DeploymentValidation` (contracts.rs:976, `can_deploy = total_errors == 0`); `DeploymentPlan::set_validation` / `can_apply`.
- Produces: apply blocks (non-zero exit, env untouched) on Error-severity contract failures; prints per-task results and a `DeploymentValidation` summary. Task 8 mirrors this block in the API handler.

- [ ] **Step 1: Write the failing smoke tests**

YAML contract syntax (verified against `YamlContract`, conduit-compiler/src/yaml_parser.rs:249): `contracts:` list under a task with `type: row_count`, `min`/`max` etc. Evidence comes from `CONDUIT::METRIC::<name>::<value>` stdout lines.

```rust
#[test]
fn cli_apply_validates_contracts_and_passes() {
    let dir = TempDir::new().unwrap();
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();
    let dag = r#"
id: contract_ok
tasks:
  emit:
    type: bash
    command: "echo CONDUIT::METRIC::row_count::100"
    contracts:
      - type: row_count
        min: 1
"#;
    fs::write(dags.join("contract_ok.yaml"), dag).unwrap();

    conduit()
        .args(["apply", "production", "-y", "--dags-path"]).arg(&dags)
        .assert()
        .success()
        .stdout(predicate::str::contains("Contract"));
}

#[test]
fn cli_apply_blocks_on_contract_violation() {
    let dir = TempDir::new().unwrap();
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();
    let dag = r#"
id: contract_bad
tasks:
  emit:
    type: bash
    command: "echo CONDUIT::METRIC::row_count::5"
    contracts:
      - type: row_count
        min: 1000
"#;
    fs::write(dags.join("contract_bad.yaml"), dag).unwrap();

    conduit()
        .args(["apply", "production", "-y", "--dags-path"]).arg(&dags)
        .assert()
        .failure()
        .stderr(predicate::str::contains("contract"));

    // A blocked apply must not update the environment.
    conduit()
        .args(["status", "--dags-path"]).arg(&dags)
        .assert()
        .success()
        .stdout(predicate::str::contains("contract_bad.emit").not());
}
```

(Adjust the `status` assertion to whatever `cmd_status` actually prints for env snapshot pointers — check `cmd_status` at main.rs:2331; if it doesn't list pointers, assert instead that a second `plan` still shows the task as Execute-pending, e.g. `stdout(predicate::str::contains("1 to execute"))` — match cmd_plan's real wording.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p conduit-cli --test cli_smoke_test cli_apply_blocks_on_contract`
Expected: FAIL — apply currently succeeds and updates the env.

- [ ] **Step 3: Evaluate contracts in cmd_apply**

In cmd_apply, before the action loop (~:1689):

```rust
    // Contracts indexed by (dag_id, task_id) — evaluated against the evidence
    // each executed task emits. Error-severity failures block the deployment
    // (docs/src/concepts/contracts.md "Plan/Apply Integration").
    let contract_index: HashMap<(String, String), &conduit_common::contracts::TaskContracts> =
        deploy
            .pending_contracts
            .iter()
            .map(|tc| {
                (
                    (tc.dag_id.clone().unwrap_or_default(), tc.task_id.clone()),
                    tc,
                )
            })
            .collect();
    let mut contract_results: Vec<conduit_common::contracts::ValidationResult> = Vec::new();
```

In the Execute success branch (`output.exit_code == 0`), immediately after computing `duration_ms` and **before** the snapshot bookkeeping:

```rust
                        if let Some(tc) =
                            contract_index.get(&(action.dag_id.clone(), action.task_id.clone()))
                        {
                            let result = conduit_common::contracts::ContractEvaluator::evaluate(
                                tc,
                                &output.evidence,
                            );
                            let blocked = !result.passed;
                            println!(
                                "  [{}] {}.{} contracts: {}/{} checks passed",
                                if blocked { "CVIO" } else { "CHK " },
                                action.dag_id,
                                action.task_id,
                                result.passed_checks,
                                result.total_checks
                            );
                            for check in result.checks.iter().filter(|c| !c.passed) {
                                eprintln!("          ! {}: {}", check.contract_name, check.message);
                            }
                            contract_results.push(result);
                            if blocked {
                                let validation =
                                    conduit_common::contracts::DeploymentValidation::from_results(
                                        contract_results,
                                    );
                                eprintln!();
                                eprintln!("{}", validation);
                                anyhow::bail!(
                                    "apply blocked: contract validation failed for {}.{} — environment not updated",
                                    action.dag_id, action.task_id
                                );
                            }
                        }
```

After the loop, before the env update (~:1810):

```rust
    if !contract_results.is_empty() {
        let validation =
            conduit_common::contracts::DeploymentValidation::from_results(contract_results);
        println!();
        println!("{}", validation);
        deploy.set_validation(validation);
        if !deploy.can_apply() {
            anyhow::bail!("apply blocked: contract validation failed — environment not updated");
        }
    }
```

`deploy` must become `let mut deploy = …` at both binding sites (~:1606, ~:1627). Note: `ValidationResult.passed` is `error_count == 0` — Warning-severity failures print but do not block, matching the docs. Skip contracts for `ReuseSnapshot` actions (no fresh evidence; the index is only consulted in the Execute arm, which handles this naturally).

- [ ] **Step 4: Run tests, verify pass**

Run: `cargo test -p conduit-cli --test cli_smoke_test`
Expected: PASS. Also `cargo test -p conduit-common` (contract evaluator untouched, must stay green).

- [ ] **Step 5: Align contracts docs sample output**

In `docs/src/concepts/contracts.md` "Plan/Apply Integration" (~:291–305): keep the promise (now true); update the sample output block to the actual format printed by Step 3 (`Contract Validation Summary` comes from `DeploymentValidation`'s `Display` impl — run the failing fixture manually once and paste the real output).

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A
git commit -s -m "feat(cli): validate data contracts during apply; block on Error severity

cmd_apply now evaluates each executed task's contracts against its
emitted evidence via ContractEvaluator. Error-severity failures abort
before the environment is updated, exit non-zero, and print the
DeploymentValidation summary. (Claims-audit finding 5, contracts half)"
```

---

### Task 5: Incremental engine + watermarks wired into run/apply; `--full-refresh` honored

**Files:**
- Modify: `conduit-executor/src/process_runner.rs` (`TaskContext` +`extra_env`, `inject_context_env` ~:557)
- Modify: every `TaskContext { … }` literal (grep `TaskContext {` across the workspace: conduit-cli/src/main.rs ×4, conduit-executor/src/executor.rs, conduit-executor tests, conduit-executor/src/process_runner.rs tests, conduit-api/tests/pipeline_e2e_test.rs, conduit-api/tests/python_sdk_e2e_test.rs, plus the new sql_provider_test.rs from Task 1)
- Modify: `conduit-cli/src/main.rs` (`PersistentState` ~:61–164; cmd_run ~:1224; cmd_apply ~:1578)
- Modify: `docs/src/concepts/incremental.md` (only if wording drifts — behavior should now match it)
- Test: `conduit-executor/tests/sql_provider_test.rs` (extra_env injection), `conduit-cli/tests/cli_smoke_test.rs`

**Interfaces:**
- Consumes: `IncrementalEngine::build_context(&IncrementalConfig, Option<&Watermark>, force_full_refresh: bool, run_time) -> IncrementalContext`, `IncrementalEngine::advance_watermark(&mut Watermark, Option<&str>, run_time, run_id)`, `IncrementalEngine::rewrite_sql(&str, &IncrementalConfig, &IncrementalContext) -> String`, `WatermarkStore::{new, from_file, save_to_file, get, set}` (all conduit-planner/src/incremental.rs); `Watermark::new(dag_id, task_id)` (conduit-common/src/incremental.rs:99); `IncrementalContext::to_env_vars() -> Vec<(String,String)>` (emits `CONDUIT_INCREMENTAL`, `CONDUIT_FULL_REFRESH`, `CONDUIT_WATERMARK_TYPE/_VALUE`, …); `Task.incremental: Option<IncrementalConfig>`.
- Produces: `TaskContext.extra_env: Vec<(String, String)>` (env vars injected verbatim — used by Task 8's API apply); watermarks persisted at `<state_dir>/watermarks.json`; `cmd_run`/`cmd_apply` signatures use `full_refresh: bool` (no underscore).

- [ ] **Step 1: Failing test for extra_env injection**

Append to `conduit-executor/tests/sql_provider_test.rs` (or a new `extra_env_test.rs`), reusing `make_context` with the new field:

```rust
#[tokio::test]
async fn extra_env_vars_reach_the_child_process() {
    let task = /* bash task via the local make_bash_task-style helper */
        make_bash_task("env_echo", "echo watermark=$CONDUIT_WATERMARK_VALUE full=$CONDUIT_FULL_REFRESH");
    let mut ctx = make_context("env_echo");
    ctx.extra_env = vec![
        ("CONDUIT_WATERMARK_VALUE".to_string(), "2026-01-01T00:00:00Z".to_string()),
        ("CONDUIT_FULL_REFRESH".to_string(), "false".to_string()),
    ];

    let output = ProcessRunner::run(&task, &ctx).await.unwrap();
    assert_eq!(output.exit_code, 0);
    assert!(output.stdout.contains("watermark=2026-01-01T00:00:00Z"));
    assert!(output.stdout.contains("full=false"));
}
```

(Copy `make_bash_task` from `conduit-executor/src/process_runner.rs:747` into the test file, adding the new field.)

- [ ] **Step 2: Run to verify compile failure** — `cargo test -p conduit-executor --test sql_provider_test`: FAIL, no field `extra_env`.

- [ ] **Step 3: Add `extra_env` to TaskContext**

In `process_runner.rs` `TaskContext` (after `params`):

```rust
    /// Extra environment variables injected verbatim (no CONDUIT_PARAM_
    /// prefix). Used for incremental-processing context (CONDUIT_WATERMARK_*,
    /// CONDUIT_FULL_REFRESH, …).
    pub extra_env: Vec<(String, String)>,
```

In `inject_context_env` (~:557), after the params loop:

```rust
        for (key, value) in &context.extra_env {
            cmd.env(key, value);
        }
```

Fix every `TaskContext { … }` literal the compiler flags by adding `extra_env: Vec::new(),` (workspace-wide `cargo build --workspace` + `cargo test --workspace --no-run` to find them all, including test files).

- [ ] **Step 4: Run** — `cargo test -p conduit-executor && cargo build --workspace`: PASS/green.

- [ ] **Step 5: Add the watermark store to CLI state**

`PersistentState` (main.rs:61): add field + open/save:

```rust
    watermark_store: std::sync::Arc<conduit_planner::WatermarkStore>,
```

In `PersistentState::open` (~:71), alongside the other stores:

```rust
        let watermarks_path = state_dir.join("watermarks.json");
        let watermark_store = std::sync::Arc::new(
            conduit_planner::WatermarkStore::from_file(&watermarks_path)
                .unwrap_or_else(|_| conduit_planner::WatermarkStore::new()),
        );
```

In `PersistentState::save` (~:152):

```rust
        self.watermark_store
            .save_to_file(&self.state_dir.join("watermarks.json"))?;
```

(Confirm `WatermarkStore` is re-exported from `conduit_planner` — lib.rs:23 says yes.)

- [ ] **Step 6: Wire incremental context into cmd_run**

Signature: `_full_refresh: bool` → `full_refresh: bool` (~:1229). Before the scheduler setup (~:1284):

```rust
    let watermarks_path = resolve_state_dir(dags_path).join("watermarks.json");
    let watermarks = std::sync::Arc::new(
        conduit_planner::WatermarkStore::from_file(&watermarks_path)
            .unwrap_or_else(|_| conduit_planner::WatermarkStore::new()),
    );
```

Clone `watermarks` into the executor task. Inside the `DispatchTask` arm, replace the context build + `ProcessRunner` call (~:1360–1373) with:

```rust
                    let run_time = chrono::Utc::now();
                    let mut task_to_run = task.clone();
                    let mut extra_env: Vec<(String, String)> = Vec::new();
                    if let Some(inc_cfg) = &task.incremental {
                        let wm = watermarks.get(&dag_id, &task_id);
                        let inc_ctx = conduit_planner::IncrementalEngine::build_context(
                            inc_cfg,
                            wm.as_ref(),
                            full_refresh,
                            run_time,
                        );
                        if inc_ctx.is_full_refresh {
                            println!("  [INCR]  {} → full refresh", task_id);
                        } else {
                            println!("  [INCR]  {} → incremental (watermark {:?})", task_id, inc_ctx.watermark);
                        }
                        if let conduit_common::dag::TaskType::Sql { query, .. } =
                            &mut task_to_run.task_type
                        {
                            *query = conduit_planner::IncrementalEngine::rewrite_sql(
                                query, inc_cfg, &inc_ctx,
                            );
                        }
                        extra_env = inc_ctx.to_env_vars();
                    }

                    let context = TaskContext {
                        dag_id: dag_id.clone(),
                        run_id: run_id.clone(),
                        task_id: task_id.clone(),
                        attempt,
                        logical_date,
                        environment: "production".to_string(), // Task 6 threads the real env
                        params: HashMap::new(),
                        extra_env,
                    };

                    let task_start = Instant::now();
                    match ProcessRunner::run_with_providers(&task_to_run, &context, Some(&registry_for_exec)).await {
```

(`full_refresh` is a `bool` — `Copy`, so the `move` closure captures it fine.) In the `exit_code == 0` success branch, after printing stdout:

```rust
                                if let Some(inc_cfg) = &task.incremental {
                                    if inc_cfg.emit_watermark {
                                        let emitted = output.stdout.lines().rev().find_map(|l| {
                                            l.trim()
                                                .strip_prefix("CONDUIT::WATERMARK::")
                                                .map(|s| s.trim().to_string())
                                        });
                                        let mut wm = watermarks.get(&dag_id, &task_id).unwrap_or_else(|| {
                                            conduit_common::incremental::Watermark::new(&dag_id, &task_id)
                                        });
                                        conduit_planner::IncrementalEngine::advance_watermark(
                                            &mut wm,
                                            emitted.as_deref(),
                                            run_time,
                                            &run_id,
                                        );
                                        let _ = watermarks.set(wm);
                                    }
                                }
```

(`Watermark::new` — verify parameter types at conduit-common/src/incremental.rs:99 and adapt `&dag_id` vs `dag_id.clone()`.) After `tokio::join!` (~:1483):

```rust
    if let Err(e) = watermarks.save_to_file(&watermarks_path) {
        tracing::warn!(error = %e, "Failed to persist watermarks");
    }
```

- [ ] **Step 7: Same wiring in cmd_apply**

`_full_refresh` → `full_refresh` (~:1583). In the Execute arm use `state.watermark_store` (no separate open — PersistentState now carries it): build `task_to_run` + `extra_env` exactly as Step 6 (watermark key is `(action.dag_id, action.task_id)`, run_id is the apply run id from the context, `run_time = logical_date`), call `run_with_providers(&task_to_run, …)`, and advance the watermark in the success branch. `state.save()` (~:1837) now persists watermarks too. Blocked/failed applies bail before `state.save()` — watermarks advanced in memory are discarded with the process, which is correct.

- [ ] **Step 8: CLI smoke test**

```rust
#[test]
fn cli_run_incremental_watermarks_and_full_refresh() {
    let dir = TempDir::new().unwrap();
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();
    let dag = r#"
id: incr_demo
tasks:
  ingest:
    type: bash
    command: "echo refresh=$CONDUIT_FULL_REFRESH; echo CONDUIT::WATERMARK::2026-01-02T00:00:00Z"
    incremental:
      strategy: append
      time_column: created_at
"#;
    fs::write(dags.join("incr_demo.yaml"), dag).unwrap();

    // First run: no watermark → full refresh.
    conduit()
        .args(["run", "incr_demo", "--dags-path"]).arg(&dags)
        .assert().success()
        .stdout(predicate::str::contains("full refresh"));

    // Watermark file was persisted next to the project.
    let wm_file = dir.path().join(".conduit").join("watermarks.json");
    assert!(wm_file.exists(), "watermarks.json must be persisted");
    let wm_json = fs::read_to_string(&wm_file).unwrap();
    assert!(wm_json.contains("2026-01-02T00:00:00"), "emitted watermark stored: {wm_json}");

    // Second run: incremental, and the task sees CONDUIT_FULL_REFRESH=false.
    conduit()
        .args(["run", "incr_demo", "--dags-path"]).arg(&dags)
        .assert().success()
        .stdout(predicate::str::contains("incremental").and(predicate::str::contains("refresh=false")));

    // --full-refresh overrides the watermark.
    conduit()
        .args(["run", "incr_demo", "--full-refresh", "--dags-path"]).arg(&dags)
        .assert().success()
        .stdout(predicate::str::contains("full refresh"));
}
```

Check the YAML incremental field names against `YamlIncrementalConfig` (conduit-compiler/src/yaml_parser.rs:202) and `resolve_incremental_config` (:521) — `strategy: append` + `time_column` are the documented spelling. Note `resolve_state_dir` walks up from the dags path, so `.conduit/` lands in the temp project root.

- [ ] **Step 9: Run everything**

Run: `cargo test -p conduit-executor -p conduit-cli -p conduit-planner`
Expected: PASS. Verify `docs/src/concepts/incremental.md:77` ("Watermarks are persisted to the `.conduit/` state directory") and :91–101 (`--full-refresh`) are now accurate — adjust doc wording only if something differs (e.g. name the file `watermarks.json`).

- [ ] **Step 10: Commit**

```bash
cargo fmt --all
git add -A
git commit -s -m "feat(cli,executor): wire incremental engine + watermark persistence into run/apply

Tasks with incremental config now get a real IncrementalContext (env
vars via TaskContext.extra_env, SQL rewritten via rewrite_sql), emitted
watermarks advance a WatermarkStore persisted at .conduit/watermarks.json,
and --full-refresh actually forces a full refresh. (Claims-audit
finding 5, incremental half)"
```

---

### Task 6: Virtual environments thread through execution (API config, serve executor, run --env)

**Files:**
- Modify: `conduit-api/src/handlers/runs.rs` (trigger_run ~:71–87)
- Modify: `conduit-cli/src/main.rs` (Commands::Run ~:208; main dispatch ~:719; cmd_run; cmd_serve executor ~:2075; `ensure_run_recorded` ~:2241)
- Test: `conduit-api/tests/handler_tests.rs`
- Test: `conduit-cli/tests/cli_smoke_test.rs`

**Interfaces:**
- Consumes: scheduler reads `config["environment"]` / `config["triggered_by"]` (conduit-scheduler/src/scheduler.rs:560–580); `DagRunInfo.environment`; `state.runs: RwLock<Vec<DagRunInfo>>` (pub — main.rs helpers already read it).
- Produces: `conduit run --env <name>` flag; API-triggered runs carry the environment into the scheduler event log and into `TaskContext.environment`.

- [ ] **Step 1: Failing API test**

In `conduit-api/tests/handler_tests.rs` (reuse the file's `app`/`post` helpers):

```rust
#[tokio::test]
async fn trigger_run_inserts_environment_into_scheduler_config() {
    let (router, state) = app(false);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    state.with_scheduler(tx);

    // A dag must exist to trigger; write a minimal yaml dag into state.dags_path.
    std::fs::write(
        state.dags_path.join("env_dag.yaml"),
        "id: env_dag\ntasks:\n  t1:\n    type: bash\n    command: \"echo hi\"\n",
    )
    .unwrap();

    let (status, _body) = post(
        &router,
        "/api/v1/dags/env_dag/runs",
        serde_json::json!({ "environment": "staging" }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::CREATED); // match the handler's real status

    let event = rx.recv().await.expect("scheduler event dispatched");
    match event {
        conduit_scheduler::SchedulerEvent::DagRunRequested { config, .. } => {
            assert_eq!(config.get("environment").map(String::as_str), Some("staging"));
            assert_eq!(config.get("triggered_by").map(String::as_str), Some("api"));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}
```

(Match the handler's actual success status code and the test-helper signatures already used in that file.)

- [ ] **Step 2: Run to verify failure** — `cargo test -p conduit-api trigger_run_inserts_environment`: FAIL (config has no environment key).

- [ ] **Step 3: Fix trigger_run**

In `runs.rs` (~:71):

```rust
    let mut config = body.config.unwrap_or_default();
    let environment = body.environment.unwrap_or_else(|| "production".to_string());
    // The scheduler reads environment/triggered_by out of the run config
    // (scheduler.rs handle_dag_run_requested) — without these keys every
    // API-triggered run is logged as production/scheduler.
    config
        .entry("environment".to_string())
        .or_insert_with(|| environment.clone());
    config
        .entry("triggered_by".to_string())
        .or_insert_with(|| "api".to_string());
```

(the subsequent `config: config.clone()` into `SchedulerEvent::DagRunRequested` and `DagRunInfo` stay as-is).

- [ ] **Step 4: Serve executor uses the run's environment**

In cmd_serve's `DispatchTask` arm (main.rs ~:2075), before building `TaskContext`:

```rust
                        let environment = exec_state
                            .runs
                            .read()
                            .ok()
                            .and_then(|runs| {
                                runs.iter()
                                    .find(|r| r.run_id == run_id)
                                    .map(|r| r.environment.clone())
                            })
                            .unwrap_or_else(|| "production".to_string());
```

and use `environment` (note: `ensure_run_recorded(&exec_state, &dag_id, &run_id)` runs first, so cron-initiated runs exist with the default). `ensure_run_recorded` keeps `environment: "production"` for cron runs — that is the real default, not a lie.

- [ ] **Step 5: Add `--env` to `conduit run`**

`Commands::Run` (~:208): add

```rust
        /// Target environment recorded for this run (context only; snapshots
        /// are managed by plan/apply).
        #[arg(long, default_value = "production")]
        env: String,
```

Dispatch arm (~:719): pass `&env` through; `cmd_run` signature gains `environment: &str`; the run_config gains `run_config.insert("environment".to_string(), environment.to_string());` (~:1312) and `TaskContext.environment: environment.to_string()` replaces the hardcoded `"production"` from Task 5's block.

- [ ] **Step 6: CLI smoke test**

```rust
#[test]
fn cli_run_env_flag_reaches_task_context() {
    let dir = TempDir::new().unwrap();
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();
    fs::write(
        dags.join("envcheck.yaml"),
        "id: envcheck\ntasks:\n  show:\n    type: bash\n    command: \"echo env=$CONDUIT_ENVIRONMENT\"\n",
    ).unwrap();

    conduit()
        .args(["run", "envcheck", "--env", "staging", "--dags-path"]).arg(&dags)
        .assert().success()
        .stdout(predicate::str::contains("env=staging"));
}
```

- [ ] **Step 7: Run** — `cargo test -p conduit-api -p conduit-cli`: PASS.

- [ ] **Step 8: Commit**

```bash
cargo fmt --all
git add -A
git commit -s -m "fix(api,cli): thread the requested environment through scheduler config and task context

trigger_run now inserts environment/triggered_by into the scheduler run
config (the scheduler reads them from there); the serve executor and
conduit run use the run's environment instead of hardcoding production;
run gains --env. (Claims-audit finding 4, threading half)"
```

---

### Task 7: API server state survives restart (snapshots, environments, run history)

**Files:**
- Modify: `conduit-api/src/state.rs` (`with_options` ~:119–188; new `persist_environments` + `rehydrate_runs`)
- Modify: `conduit-api/src/handlers/envs.rs` (persist after each mutation: `create_environment`, `delete_environment`, `promote_environment`, `rollback_environment`, `update_env_policy`)
- Test: `conduit-api/tests/persistence_test.rs` (new)

**Interfaces:**
- Consumes: `SnapshotStore::open(&Path)` (conduit-state/src/snapshot_store.rs:51 — durable; `new()` is a temp dir deleted on drop); `EnvironmentManager::{from_file, save_to_file}` (environment_manager.rs:108/:142 — JSON array; builders `with_history_store`/`with_snapshot_store`/`with_event_store` must be re-chained after `from_file`); `EventStore::all_events() -> ConduitResult<Vec<Event>>` (event_store.rs:202); `EventKind::{DagRunCreated, TaskStarted, TaskCompleted, TaskFailed, TaskSkipped, DagRunCompleted}` (conduit-common/src/event.rs); `MAX_CACHED_RUNS = 10_000` (state.rs:24).
- Produces: `AppState::persist_environments(&self)` (Task 8 calls it after apply); runs rehydrated on startup; snapshot DB at `<state_dir>/snapshots_db` (same path the CLI uses, so serve sees CLI applies).

- [ ] **Step 1: Failing persistence tests**

Create `conduit-api/tests/persistence_test.rs` (copy the `test_state` temp-dir pattern from `handler_tests.rs`, but keep the `state_dir` outside the helper so it can be reused across two `AppState` constructions):

```rust
use std::sync::Arc;
use conduit_api::AppState;

fn temp_project() -> (std::path::PathBuf, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("conduit_persist_{}", uuid::Uuid::new_v4()));
    let dags = root.join("dags");
    std::fs::create_dir_all(&dags).unwrap();
    (dags, root)
}

#[tokio::test]
async fn environments_survive_restart() {
    let (dags, state_dir) = temp_project();

    {
        let state = AppState::with_options(dags.clone(), state_dir.clone(), None, false);
        state.env_manager.create("staging", None).unwrap();
        state.persist_environments();
        drop(state);
    } // RocksDB handles released here

    let state = AppState::with_options(dags, state_dir, None, false);
    assert!(state.env_manager.get("staging").is_ok(), "staging must survive restart");
}

#[tokio::test]
async fn snapshots_survive_restart() {
    let (dags, state_dir) = temp_project();

    {
        let state = AppState::with_options(dags.clone(), state_dir.clone(), None, false);
        let snap = conduit_common::snapshot::Snapshot {
            id: "snap_test_1".to_string(),
            fingerprint: conduit_common::snapshot::Fingerprint("abc123".to_string()),
            dag_id: "d".to_string(),
            task_id: "t".to_string(),
            created_at: chrono::Utc::now(),
            parent_fingerprints: vec![],
            metadata: Default::default(),
        };
        state.snapshot_store.put(snap).unwrap();
        drop(state);
    }

    let state = AppState::with_options(dags, state_dir, None, false);
    assert!(state.snapshot_store.count() >= 1, "snapshot must survive restart");
}

#[tokio::test]
async fn run_history_rehydrates_from_event_store() {
    let (dags, state_dir) = temp_project();

    {
        let state = AppState::with_options(dags.clone(), state_dir.clone(), None, false);
        let store = state.event_store.as_ref().expect("event store").clone();
        store.append(conduit_common::event::EventKind::DagRunCreated {
            dag_id: "d1".into(), run_id: "r1".into(),
            logical_date: chrono::Utc::now(),
            environment: "staging".into(), triggered_by: "api".into(),
        }).unwrap();
        store.append(conduit_common::event::EventKind::TaskCompleted {
            dag_id: "d1".into(), run_id: "r1".into(), task_id: "t1".into(),
            duration_ms: 10, snapshot_id: None,
        }).unwrap();
        store.append(conduit_common::event::EventKind::DagRunCompleted {
            dag_id: "d1".into(), run_id: "r1".into(),
            status: conduit_common::event::RunStatus::Success, duration_ms: 12,
        }).unwrap();
        drop(state);
    }

    let state = AppState::with_options(dags, state_dir, None, false);
    let runs = state.get_runs(None);
    let run = runs.iter().find(|r| r.run_id == "r1").expect("run rehydrated");
    assert_eq!(run.status, "success");
    assert_eq!(run.environment, "staging");
    assert_eq!(run.task_states.get("t1").map(String::as_str), Some("success"));
}
```

Verify `Fingerprint`'s constructor (tuple struct vs method), `EventStore::append`'s signature (takes `EventKind`? returns sequence?) and `RunStatus` path (`conduit_common::event::RunStatus`) against the actual code and adjust — cmd_apply (main.rs:1751-1846) and scheduler.rs:1048 show working `append` calls and the status enum in use.

- [ ] **Step 2: Run to verify failure** — `cargo test -p conduit-api --test persistence_test`: all three FAIL (in-memory stores).

- [ ] **Step 3: Make with_options persistent**

In `state.rs` `with_options`, replace the env-manager/snapshot-store block (~:155–162):

```rust
        // Live environment set: JSON file shared with the CLI (`conduit apply`
        // writes the same file), so serve and the CLI see one world.
        let env_file = state_dir.join("environments.json");
        let env_manager = if env_file.exists() {
            EnvironmentManager::from_file(&env_file).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to load environments.json; starting fresh");
                EnvironmentManager::new()
            })
        } else {
            EnvironmentManager::new()
        };
        let env_manager = match conduit_state::EnvHistoryStore::open(state_dir.join("env_history"))
        {
            Ok(store) => env_manager.with_history_store(store),
            Err(_) => env_manager,
        };
        // Durable snapshot store at the same path the CLI uses. Falls back to
        // the in-memory temp store only if the DB can't be opened (e.g. the
        // CLI holds the lock) — that fallback loses data on restart, so warn.
        let snapshot_store = match SnapshotStore::open(&state_dir.join("snapshots_db")) {
            Ok(store) => Arc::new(store),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to open snapshots_db; falling back to in-memory snapshots (lost on restart)"
                );
                Arc::new(SnapshotStore::new())
            }
        };
        let env_manager = env_manager.with_snapshot_store(Arc::clone(&snapshot_store));
```

and the runs init (~:177): `runs: RwLock::new(rehydrated_runs),` where, just before the `Arc::new(Self { … })`:

```rust
        let rehydrated_runs = event_store
            .as_ref()
            .map(|s| Self::rehydrate_runs(s))
            .unwrap_or_default();
```

- [ ] **Step 4: Implement rehydrate_runs + persist_environments**

In `impl AppState`:

```rust
    /// Rebuild the run cache by folding the durable event log. Keeps at most
    /// MAX_CACHED_RUNS most-recent runs (same cap as record_run).
    fn rehydrate_runs(event_store: &conduit_state::EventStore) -> Vec<DagRunInfo> {
        use conduit_common::event::{EventKind, RunStatus};
        use std::collections::HashMap;

        let Ok(events) = event_store.all_events() else {
            return Vec::new();
        };
        let mut runs: Vec<DagRunInfo> = Vec::new();
        let mut index: HashMap<String, usize> = HashMap::new();

        for event in events {
            match event.kind {
                EventKind::DagRunCreated {
                    dag_id,
                    run_id,
                    environment,
                    triggered_by,
                    ..
                } => {
                    index.insert(run_id.clone(), runs.len());
                    runs.push(DagRunInfo {
                        run_id,
                        dag_id,
                        status: "running".to_string(),
                        started_at: event.timestamp,
                        finished_at: None,
                        task_states: HashMap::new(),
                        task_logs: HashMap::new(),
                        triggered_by,
                        environment,
                    });
                }
                EventKind::TaskStarted { run_id, task_id, .. } => {
                    if let Some(&i) = index.get(&run_id) {
                        runs[i].task_states.insert(task_id, "running".to_string());
                    }
                }
                EventKind::TaskCompleted { run_id, task_id, .. } => {
                    if let Some(&i) = index.get(&run_id) {
                        runs[i].task_states.insert(task_id, "success".to_string());
                    }
                }
                EventKind::TaskFailed { run_id, task_id, .. } => {
                    if let Some(&i) = index.get(&run_id) {
                        runs[i].task_states.insert(task_id, "failed".to_string());
                    }
                }
                EventKind::TaskSkipped { run_id, task_id, .. } => {
                    if let Some(&i) = index.get(&run_id) {
                        runs[i].task_states.insert(task_id, "skipped".to_string());
                    }
                }
                EventKind::DagRunCompleted { run_id, status, .. } => {
                    if let Some(&i) = index.get(&run_id) {
                        runs[i].status = match status {
                            RunStatus::Success => "success".to_string(),
                            RunStatus::Failed => "failed".to_string(),
                            RunStatus::Cancelled => "cancelled".to_string(),
                        };
                        runs[i].finished_at = Some(event.timestamp);
                    }
                }
                _ => {}
            }
        }

        if runs.len() > MAX_CACHED_RUNS {
            let excess = runs.len() - MAX_CACHED_RUNS;
            runs.drain(0..excess);
        }
        runs
    }

    /// Persist the live environment set. Call after any env mutation
    /// (create/delete/promote/rollback/policy/apply).
    pub fn persist_environments(&self) {
        let path = self.state_dir.join("environments.json");
        if let Err(e) = self.env_manager.save_to_file(&path) {
            tracing::warn!(error = %e, path = %path.display(), "Failed to persist environments");
        }
    }
```

(Adjust `EventKind` field lists / `RunStatus` variants to the real definitions in conduit-common/src/event.rs:45–110; unused fields are matched with `..`.)

- [ ] **Step 5: Call persist_environments in the env handlers**

In `conduit-api/src/handlers/envs.rs`, after each successful mutation (before returning Ok) in `create_environment`, `delete_environment`, `promote_environment`, `rollback_environment`, `update_env_policy`: add `state.persist_environments();`.

- [ ] **Step 6: Run** — `cargo test -p conduit-api`: the three new tests PASS; existing suites stay green (they use throwaway temp state dirs, so the durable stores are equivalent from their perspective).

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add -A
git commit -s -m "fix(api): reopen persistent state on serve startup instead of starting blank

AppState now opens the durable snapshots_db (shared path with the CLI),
loads environments.json (persisting after every env mutation), and
rehydrates the run cache from the durable event log. Restarting serve
no longer loses the operational view. (Claims-audit finding 4,
persistence half)"
```

---

### Task 8: POST /apply actually applies (plan store, stale check, execution, env update)

**Files:**
- Modify: `conduit-api/Cargo.toml` (move `conduit-executor` from `[dev-dependencies]` (line ~45) into `[dependencies]`)
- Modify: `conduit-api/src/state.rs` (plan store field + methods)
- Modify: `conduit-api/src/error.rs` (add `Conflict`, `ApplyFailed`)
- Modify: `conduit-api/src/handlers/plan.rs` (store in generate_plan; rewrite apply_plan ~:112–175)
- Modify: `conduit-planner/src/deployment_plan.rs` (add `#[derive(Clone)]` to `DeploymentPlan` and any member types the compiler then requires — `DeploymentAction`, `DeploymentStats`; `ActionKind`, `TaskContracts`, `DeploymentValidation`, `Fingerprint` already have Clone or add it)
- Test: `conduit-api/tests/apply_test.rs` (new)

**Interfaces:**
- Consumes: Task 1 (`init_providers` in serve, `run_with_providers`), Task 3 (`base_environment_version`), Task 4's contract-evaluation pattern, Task 7 (`persist_environments`); `DeploymentPlan::apply_to_environment(&self, &mut Environment, &HashMap<(String,String),String>)` (deployment_plan.rs:579); `EnvironmentManager::apply_snapshot_map(env, map, plan_id) -> ConduitResult<Option<u32>>`; `ProcessRunner`/`TaskContext` from conduit-executor.
- Produces: `AppState::{store_plan(&self, &DeploymentPlan), get_plan(&self, id) -> Option<DeploymentPlan>}`; `ApiError::Conflict` → HTTP 409 `"conflict"`, `ApiError::ApplyFailed` → HTTP 422 `"apply_failed"`; POST /apply response `{plan_id, environment, status:"applied", tasks_executed, tasks_reused, tasks_removed, environment_version}`.

- [ ] **Step 1: Failing API tests**

Create `conduit-api/tests/apply_test.rs` (helpers copied from `handler_tests.rs` — `app`, `post`, `get`):

```rust
#[tokio::test]
async fn apply_with_unknown_plan_id_is_404() {
    let (router, _state) = app(false);
    let (status, body) = post(
        &router,
        "/api/v1/apply",
        serde_json::json!({ "plan_id": "plan_nope", "environment": "production" }),
    ).await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND, "{body}");
}

#[tokio::test]
async fn apply_executes_stored_plan_and_updates_environment() {
    let (router, state) = app(false);
    std::fs::write(
        state.dags_path.join("api_apply.yaml"),
        "id: api_apply\ntasks:\n  hello:\n    type: bash\n    command: \"echo done\"\n",
    ).unwrap();

    let (status, plan_body) = post(&router, "/api/v1/plan",
        serde_json::json!({ "environment": "production" })).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{plan_body}");
    let plan_id = plan_body["plan_id"].as_str().unwrap().to_string();

    let (status, body) = post(&router, "/api/v1/apply",
        serde_json::json!({ "plan_id": plan_id, "environment": "production" })).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(body["status"], "applied");
    assert!(body["tasks_executed"].as_u64().unwrap() >= 1);

    // The environment now points at a real snapshot.
    let env = state.env_manager.get("production").unwrap();
    assert!(!env.snapshot_map.is_empty(), "env must gain snapshot pointers");
    assert!(env.current_version >= 1);

    // Re-applying the same plan is now stale → 409.
    let (status, body) = post(&router, "/api/v1/apply",
        serde_json::json!({ "plan_id": body["plan_id"], "environment": "production" })).await;
    assert_eq!(status, axum::http::StatusCode::CONFLICT, "{body}");
}

#[tokio::test]
async fn apply_without_plan_id_generates_and_applies_fresh_plan() {
    let (router, state) = app(false);
    std::fs::write(
        state.dags_path.join("fresh_apply.yaml"),
        "id: fresh_apply\ntasks:\n  hello:\n    type: bash\n    command: \"echo done\"\n",
    ).unwrap();

    let (status, body) = post(&router, "/api/v1/apply",
        serde_json::json!({ "environment": "production" })).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(body["status"], "applied");
}
```

(POST /plan's real success status may be 200 — check `generate_plan`; adjust asserts to reality, and note the second-apply 409 uses the plan_id echoed by the first apply response.)

- [ ] **Step 2: Run to verify failure** — `cargo test -p conduit-api --test apply_test`: FAIL (apply returns "accepted", env untouched, unknown plan_id accepted).

- [ ] **Step 3: Plan store on AppState**

`Cargo.toml`: move `conduit-executor = { path = "../conduit-executor" }` into `[dependencies]`. In `deployment_plan.rs`: add `Clone` to the derives of `DeploymentPlan`, `DeploymentAction`, `DeploymentStats` (and any type the compiler then names). In `state.rs`:

```rust
    /// Recently generated deployment plans, newest last, capped. POST /apply
    /// looks plans up here by id so the client applies exactly what it reviewed.
    pub deployment_plans: RwLock<Vec<conduit_planner::DeploymentPlan>>,
```

(init with `deployment_plans: RwLock::new(Vec::new()),`), plus:

```rust
    const MAX_CACHED_PLANS: usize = 50;

    pub fn store_plan(&self, plan: &conduit_planner::DeploymentPlan) {
        if let Ok(mut plans) = self.deployment_plans.write() {
            plans.push(plan.clone());
            if plans.len() > Self::MAX_CACHED_PLANS {
                let excess = plans.len() - Self::MAX_CACHED_PLANS;
                plans.drain(0..excess);
            }
        }
    }

    pub fn get_plan(&self, plan_id: &str) -> Option<conduit_planner::DeploymentPlan> {
        self.deployment_plans
            .read()
            .ok()
            .and_then(|plans| plans.iter().rev().find(|p| p.id == plan_id).cloned())
    }
```

- [ ] **Step 4: Error variants**

`error.rs`: add `Conflict(String)` and `ApplyFailed(String)` to `ApiError` and to the match:

```rust
            ApiError::Conflict(msg) => (StatusCode::CONFLICT, "conflict", msg),
            ApiError::ApplyFailed(msg) => (StatusCode::UNPROCESSABLE_ENTITY, "apply_failed", msg),
```

- [ ] **Step 5: Store plans in generate_plan; rewrite apply_plan**

`generate_plan`: after `let deploy = DeploymentPlan::generate(…)`, add `state.store_plan(&deploy);`.

Replace `apply_plan`'s body after the auth check with:

```rust
    let env_name = body.environment.as_deref().unwrap_or("production").to_string();

    // Compile current DAGs — needed to look up task definitions for execution.
    let (plan, stats) = ConduitPlan::compile(&state.dags_path)
        .map_err(|e| ApiError::CompilationFailed(e.to_string()))?;
    if !stats.errors.is_empty() {
        let error_msgs: Vec<String> = stats.errors.iter().map(|e| e.to_string()).collect();
        return Err(ApiError::CompilationFailed(error_msgs.join("; ")));
    }

    let deploy = if let Some(plan_id) = body.plan_id.as_deref() {
        let stored = state.get_plan(plan_id).ok_or_else(|| {
            ApiError::NotFound(format!(
                "plan '{}' not found (plans are cached in-memory; regenerate via POST /api/v1/plan)",
                plan_id
            ))
        })?;
        if stored.target_environment != env_name {
            return Err(ApiError::BadRequest(format!(
                "plan '{}' targets environment '{}', not '{}'",
                plan_id, stored.target_environment, env_name
            )));
        }
        let current_version = state
            .env_manager
            .get(&env_name)
            .map(|e| e.current_version)
            .unwrap_or(0);
        if current_version != stored.base_environment_version {
            return Err(ApiError::Conflict(format!(
                "stale plan: environment '{}' is at version {} but plan '{}' was generated against version {} — regenerate the plan",
                env_name, current_version, plan_id, stored.base_environment_version
            )));
        }
        stored
    } else {
        let env = state
            .env_manager
            .get(&env_name)
            .unwrap_or_else(|_| conduit_common::snapshot::Environment::new(&env_name));
        let deploy = conduit_planner::DeploymentPlan::generate(&plan, &env, &state.snapshot_store);
        state.store_plan(&deploy);
        deploy
    };

    if deploy.stats.tasks_to_execute == 0 && deploy.stats.tasks_to_remove == 0 {
        return Ok(Json(json!({
            "plan_id": deploy.id,
            "message": format!("Nothing to apply. Environment '{}' is up to date.", env_name),
            "status": "noop",
            "tasks_executed": 0, "tasks_reused": 0, "tasks_removed": 0,
        })));
    }

    state.broadcast_event(
        &json!({
            "type": "apply_started",
            "plan_id": deploy.id,
            "environment": env_name,
            "tasks_to_execute": deploy.stats.tasks_to_execute,
            "timestamp": Utc::now().to_rfc3339(),
        })
        .to_string(),
    );

    // ── Execute the plan (mirrors CLI cmd_apply) ──
    use conduit_executor::process_runner::{ProcessRunner, TaskContext};
    use conduit_planner::ActionKind;

    let registry = state.provider_registry.read().ok().and_then(|g| g.clone());
    let contract_index: std::collections::HashMap<(String, String), &conduit_common::contracts::TaskContracts> =
        deploy
            .pending_contracts
            .iter()
            .map(|tc| ((tc.dag_id.clone().unwrap_or_default(), tc.task_id.clone()), tc))
            .collect();
    let mut contract_results: Vec<conduit_common::contracts::ValidationResult> = Vec::new();
    let mut new_snapshots: std::collections::HashMap<(String, String), String> =
        std::collections::HashMap::new();
    let (mut executed, mut reused, mut removed) = (0usize, 0usize, 0usize);
    let logical_date = Utc::now();
    let run_id = format!("apply_{}", Utc::now().format("%Y%m%d%H%M%S"));

    for action in &deploy.actions {
        match &action.action {
            ActionKind::Execute => {
                let task = plan
                    .dags
                    .get(&action.dag_id)
                    .and_then(|dag| dag.tasks.get(&action.task_id))
                    .ok_or_else(|| {
                        ApiError::ApplyFailed(format!(
                            "task {}.{} not found in compiled plan",
                            action.dag_id, action.task_id
                        ))
                    })?;

                let context = TaskContext {
                    dag_id: action.dag_id.clone(),
                    run_id: run_id.clone(),
                    task_id: action.task_id.clone(),
                    attempt: 1,
                    logical_date,
                    environment: env_name.clone(),
                    params: Default::default(),
                    extra_env: Vec::new(),
                };

                let output = ProcessRunner::run_with_providers(task, &context, registry.as_deref())
                    .await
                    .map_err(|e| {
                        ApiError::ApplyFailed(format!(
                            "task {}.{} execution error: {}",
                            action.dag_id, action.task_id, e
                        ))
                    })?;
                if output.exit_code != 0 {
                    return Err(ApiError::ApplyFailed(format!(
                        "task {}.{} failed with exit code {}: {}",
                        action.dag_id,
                        action.task_id,
                        output.exit_code,
                        output.stderr.trim()
                    )));
                }

                if let Some(tc) = contract_index.get(&(action.dag_id.clone(), action.task_id.clone())) {
                    let result =
                        conduit_common::contracts::ContractEvaluator::evaluate(tc, &output.evidence);
                    let blocked = !result.passed;
                    contract_results.push(result);
                    if blocked {
                        return Err(ApiError::ApplyFailed(format!(
                            "contract validation failed for {}.{} — environment not updated",
                            action.dag_id, action.task_id
                        )));
                    }
                }

                let snap_id = format!(
                    "snap_{}_{}",
                    action.task_id,
                    Utc::now().format("%Y%m%d%H%M%S%3f")
                );
                if let Some(ref fp) = action.fingerprint {
                    let snapshot = conduit_common::snapshot::Snapshot {
                        id: snap_id.clone(),
                        fingerprint: fp.clone(),
                        dag_id: action.dag_id.clone(),
                        task_id: action.task_id.clone(),
                        created_at: Utc::now(),
                        parent_fingerprints: vec![],
                        metadata: Default::default(),
                    };
                    let _ = state.snapshot_store.put(snapshot);
                }
                new_snapshots.insert((action.dag_id.clone(), action.task_id.clone()), snap_id);
                executed += 1;
            }
            ActionKind::ReuseSnapshot { .. } => reused += 1,
            ActionKind::Skip => {}
            ActionKind::Remove => removed += 1,
        }
    }

    // ── Update the environment (history-recorded, rollbackable) ──
    if state.env_manager.get(&env_name).is_err() {
        let _ = state.env_manager.create(&env_name, None);
    }
    let mut env_snapshot = state
        .env_manager
        .get(&env_name)
        .unwrap_or_else(|_| conduit_common::snapshot::Environment::new(&env_name));
    deploy.apply_to_environment(&mut env_snapshot, &new_snapshots);
    let recorded_version = state
        .env_manager
        .apply_snapshot_map(&env_name, env_snapshot.snapshot_map.clone(), deploy.id.clone())
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    state.persist_environments();

    if let Some(store) = &state.event_store {
        let _ = store.append(conduit_common::event::EventKind::PlanApplied {
            plan_id: deploy.id.clone(),
            environment: env_name.clone(),
            tasks_executed: executed as u32,
            tasks_skipped: reused as u32,
        });
    }
    state.broadcast_event(
        &json!({
            "type": "apply_completed",
            "plan_id": deploy.id,
            "environment": env_name,
            "tasks_executed": executed,
            "timestamp": Utc::now().to_rfc3339(),
        })
        .to_string(),
    );

    Ok(Json(json!({
        "plan_id": deploy.id,
        "environment": env_name,
        "status": "applied",
        "tasks_executed": executed,
        "tasks_reused": reused,
        "tasks_removed": removed,
        "environment_version": recorded_version,
    })))
```

(Check `EnvironmentManager::create`'s signature (environment_manager.rs) — the CLI calls `create(environment, None)`. Check `env_manager.get` return type for the `current_version` read. `apply_plan` executes synchronously — acceptable at this scale; documented in Task 12.)

- [ ] **Step 6: Run** — `cargo test -p conduit-api`: the new apply tests PASS; existing plan handler tests may need response-shape updates (`"status": "accepted"` → `"applied"`, message wording) — update them to the honest shape.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add -A
git commit -s -m "feat(api): POST /apply executes the stored plan and updates the environment

Plans generated via POST /plan are cached by id; apply looks up the
reviewed plan, enforces target-environment and base-version (409 on
stale), executes tasks through the provider registry, validates
contracts, stores snapshots, and records the env update with history.
(Claims-audit finding 2, API half)"
```

---

### Task 9: Honor `run --max-tasks` and `backfill --max-concurrent`

**Files:**
- Modify: `conduit-cli/src/main.rs` (cmd_run executor loop ~:1329–1481; cmd_backfill ~:3343–3520; help text of both args)
- Test: `conduit-cli/tests/cli_smoke_test.rs`

**Interfaces:**
- Consumes: `tokio::sync::Semaphore`, `tokio::task::JoinSet`; the spawn pattern from cmd_serve (~:2089).
- Produces: bounded-concurrent task execution in `conduit run`; bounded-concurrent partition execution in `conduit backfill`.

- [ ] **Step 1: Failing overlap test for run**

```rust
#[test]
fn cli_run_max_tasks_runs_independent_tasks_concurrently() {
    let dir = TempDir::new().unwrap();
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();
    let marker = dir.path().join("marks");
    // Two independent tasks; each records start+end epoch-ns. With
    // concurrency 2 their intervals must overlap (each sleeps 700ms).
    let dag = format!(
        r#"
id: par_demo
tasks:
  a:
    type: bash
    command: "date +%s%N >> {m}/a; sleep 0.7; date +%s%N >> {m}/a"
  b:
    type: bash
    command: "date +%s%N >> {m}/b; sleep 0.7; date +%s%N >> {m}/b"
"#,
        m = marker.display()
    );
    fs::create_dir_all(&marker).unwrap();
    fs::write(dags.join("par_demo.yaml"), dag).unwrap();

    conduit()
        .args(["run", "par_demo", "--max-tasks", "2", "--dags-path"]).arg(&dags)
        .assert().success();

    let read = |n: &str| -> (i128, i128) {
        let s = fs::read_to_string(marker.join(n)).unwrap();
        let v: Vec<i128> = s.lines().map(|l| l.trim().parse().unwrap()).collect();
        (v[0], v[1])
    };
    let (a0, a1) = read("a");
    let (b0, b1) = read("b");
    assert!(a0 < b1 && b0 < a1, "task intervals must overlap: a=({a0},{a1}) b=({b0},{b1})");
}
```

- [ ] **Step 2: Run to verify failure** — currently serial, intervals don't overlap → FAIL.

- [ ] **Step 3: Bounded-concurrent cmd_run executor**

Rework the executor loop: rename `_max_tasks` → `max_tasks`; wrap the whole `DispatchTask` handling body in a spawned task gated by a semaphore. Shared state changes: `completed` becomes `std::sync::Arc<std::sync::atomic::AtomicUsize>`; drop the unused `_failed` local (the `run_failed` flag already exists). Sketch of the arm:

```rust
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(max_tasks.max(1)));
        // …inside while let Some(cmd):
                SchedulerCommand::DispatchTask { dag_id, run_id, task_id, attempt } => {
                    // task lookup + incremental prep stay here (cheap, sync)…
                    let permit_sem = std::sync::Arc::clone(&semaphore);
                    let event_tx = executor_event_tx.clone();
                    let registry = std::sync::Arc::clone(&registry_for_exec);
                    let watermarks = std::sync::Arc::clone(&watermarks);
                    let completed = std::sync::Arc::clone(&completed);
                    tokio::spawn(async move {
                        let _permit = permit_sem
                            .acquire_owned()
                            .await
                            .expect("semaphore never closed");
                        // …existing execution + event-sending body, using
                        // completed.fetch_add(1, Ordering::SeqCst) + 1 for the
                        // [n/total] progress counter…
                    });
                }
```

`CompleteDagRun` still breaks the recv loop — the scheduler only sends it after all task events arrived, so no in-flight spawn is lost. The `RetryTask` counter adjustment (`completed.saturating_sub`) becomes `completed.fetch_sub(1, …)` guarded against underflow (use `fetch_update`).

- [ ] **Step 4: Concurrent backfill partitions**

Extract the body of the per-partition `for` loop (~:3366–3519) into:

```rust
#[allow(clippy::too_many_arguments)]
async fn run_backfill_partition(
    idx: usize,
    total: usize,
    partition: conduit_common::backfill::BackfillPartition,
    request: conduit_common::backfill::BackfillRequest,
    dag: conduit_common::dag::Dag,
    dag_id: String,
    pools: Vec<(String, u32)>, // or whatever load_pools returns — pass pre-loaded data, not the path
    event_store: Option<std::sync::Arc<conduit_state::EventStore>>,
    registry: std::sync::Arc<conduit_providers::ProviderRegistry>,
) -> Result<bool /* partition_failed */>
```

(match real types: `BackfillPartition` from `conduit_common::backfill`, `load_pools` return type from main.rs:1136 — call `load_pools` once before the loop and clone the result in). Then:

```rust
    let max_concurrent = max_concurrent.max(1) as usize;
    println!("Executing partitions ({} at a time)...", max_concurrent);
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let mut join_set = tokio::task::JoinSet::new();
    for (idx, partition) in partitions.iter().cloned().enumerate() {
        let sem = std::sync::Arc::clone(&semaphore);
        /* clone request/dag/dag_id/pools/event_store/registry */
        join_set.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore never closed");
            let started = Instant::now();
            let result = run_backfill_partition(/* … */).await;
            (idx, started.elapsed(), result)
        });
    }
    let (mut succeeded, mut failed) = (0usize, 0usize);
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok((idx, dur, Ok(false))) => { succeeded += 1; println!("  [{:>3}/{}] OK ({:.0}ms)", idx + 1, total, dur.as_secs_f64() * 1000.0); }
            Ok((idx, dur, Ok(true)))  => { failed += 1;    println!("  [{:>3}/{}] FAILED ({:.0}ms)", idx + 1, total, dur.as_secs_f64() * 1000.0); }
            Ok((idx, _, Err(e)))      => { failed += 1;    println!("  [{:>3}/{}] ERROR: {}", idx + 1, total, e); }
            Err(e)                    => { failed += 1;    println!("  [join] ERROR: {}", e); }
        }
    }
```

Rename `_max_concurrent` → `max_concurrent`; delete the "v0.1: sequential only" comment on the clap arg (~:438) and the "Execute partitions sequentially" phase comment.

- [ ] **Step 5: Backfill smoke test**

```rust
#[test]
fn cli_backfill_max_concurrent_completes_all_partitions() {
    let dir = TempDir::new().unwrap();
    let dags = write_yaml_dag(&dir);

    conduit()
        .args([
            "backfill", "smoke_test",
            "--start", "2026-01-01", "--end", "2026-01-04",
            "--max-concurrent", "3",
            "--dags-path",
        ]).arg(&dags)
        .assert().success()
        .stdout(predicate::str::contains("Succeeded:        3"));
}
```

(Match the summary's exact spacing from the existing "Backfill complete" block, or loosen to `contains("Succeeded:") .and(contains("3"))`.)

- [ ] **Step 6: Run** — `cargo test -p conduit-cli`: PASS (the Task 5 incremental test also re-exercises the reworked run loop).

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add -A
git commit -s -m "feat(cli): honor run --max-tasks and backfill --max-concurrent

conduit run executes dispatched tasks on a semaphore-bounded pool
instead of serially awaiting each; backfill runs partitions through a
JoinSet bounded by --max-concurrent. (Claims-audit finding 7,
concurrency half)"
```

---

### Task 10: Distributed CLI, part A — real worker, cluster status, drain

**Files:**
- Modify: `conduit-cli/Cargo.toml` (add `conduit-distributed = { path = "../conduit-distributed" }`)
- Modify: `conduit-cli/src/main.rs` (cmd_worker ~:3544, cmd_cluster_status ~:3614, cmd_cluster_drain ~:3647, dispatch arms ~:903–910 → `rt.block_on`)
- Modify: `conduit-distributed/proto/conduit.proto` (DrainWorker RPC), `src/coordinator.rs` (drain_worker + heartbeat directive), `src/worker_pool.rs` (`is_draining`), `src/grpc_server.rs` (RPC impl), `src/worker.rs` (execute_sql honesty)
- Test: `conduit-distributed/tests/grpc_integration_test.rs` (drain RPC), worker unit tests (SQL), `conduit-cli/tests/cli_smoke_test.rs` (unreachable-coordinator errors)

**Interfaces:**
- Consumes: `conduit_distributed::{run_worker, WorkerConfig}`; `conduit_distributed::generated_proto::{coordinator_client::CoordinatorClient, ClusterStatusRequest, ClusterStatusResponse, WorkerStatus, ClusterHealth, WorkerState}`; `WorkerPool::drain_worker(worker_id, reason)` (worker_pool.rs:293, sets `WorkerState::Draining`); local `CoordinatorDirective::Drain { reason, grace_period_secs }` (proto_types.rs:124 — the worker client already handles it, grpc_client.rs:275). `protoc` must be installed (build.rs regenerates `src/generated` on build; `brew install protobuf` if missing).
- Produces: `DrainWorker` RPC (`DrainRequest { worker_id, reason }` → `Ack`); `Coordinator::drain_worker(&self, worker_id, reason) -> bool`; heartbeat responses carry `Drain` when the pool marks a worker draining; honest `execute_sql` failure on workers.

- [ ] **Step 1: Failing drain-RPC integration test**

In `conduit-distributed/tests/grpc_integration_test.rs`, reuse the existing `start_server()` helper (line ~48):

```rust
#[tokio::test]
async fn drain_worker_rpc_marks_worker_draining() {
    let (addr, coordinator) = start_server().await; // adapt to the helper's real return shape

    let mut client = proto::coordinator_client::CoordinatorClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    // Register a worker (copy the register flow from the existing tests).
    register_test_worker(&mut client, "w-drain").await;

    let ack = client
        .drain_worker(proto::DrainRequest {
            worker_id: "w-drain".to_string(),
            reason: "maintenance".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(ack.success);

    // Cluster status reflects the draining state.
    let status = client
        .cluster_status(proto::ClusterStatusRequest {})
        .await
        .unwrap()
        .into_inner();
    let w = status.workers.iter().find(|w| w.worker_id == "w-drain").unwrap();
    assert_eq!(w.state, proto::WorkerState::Draining as i32);

    // Unknown worker → error.
    let err = client
        .drain_worker(proto::DrainRequest { worker_id: "nope".into(), reason: String::new() })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}
```

(Adapt imports/helper names to that file's actual conventions; it already drives Register/ClusterStatus.)

- [ ] **Step 2: Add the RPC**

`proto/conduit.proto`, inside `service Coordinator`:

```proto
    /// Administratively drain a worker: it finishes in-flight tasks and
    /// receives no new assignments. Invoked by `conduit cluster drain`.
    rpc DrainWorker(DrainRequest) returns (Ack);
```

and with the other messages:

```proto
message DrainRequest {
    string worker_id = 1;
    string reason = 2;
}
```

`cargo build -p conduit-distributed` regenerates `src/generated/conduit.distributed.rs` (commit the regenerated file — it is checked in).

- [ ] **Step 3: Coordinator + pool + server plumbing**

`worker_pool.rs` — add next to `drain_worker` (:293):

```rust
    /// Whether a worker is currently marked draining.
    pub fn is_draining(&self, worker_id: &str) -> bool {
        self.workers
            .get(worker_id)
            .map(|w| w.state == WorkerState::Draining)
            .unwrap_or(false)
    }

    /// Whether a worker is registered at all.
    pub fn contains(&self, worker_id: &str) -> bool {
        self.workers.contains_key(worker_id)
    }
```

(match the actual `workers` container — it may be a DashMap; mirror how `drain_worker` accesses it.)

`coordinator.rs` — public wrapper + heartbeat directive. In `impl Coordinator`:

```rust
    /// Mark a worker as draining. Returns false when the worker is unknown.
    /// The drain directive is delivered on the worker's next heartbeat.
    pub fn drain_worker(&self, worker_id: &str, reason: &str) -> bool {
        if !self.pool.contains(worker_id) {
            return false;
        }
        self.pool.drain_worker(worker_id, reason);
        true
    }
```

and change `handle_heartbeat` (:358):

```rust
    pub fn handle_heartbeat(&self, hb: &WorkerHeartbeat) -> CoordinatorDirective {
        self.pool.heartbeat(hb);

        if self.pool.is_draining(&hb.worker_id) {
            return CoordinatorDirective::Drain {
                reason: "drain requested by operator".to_string(),
                grace_period_secs: 30,
            };
        }

        CoordinatorDirective::HeartbeatAck {
            timestamp_ms: Utc::now().timestamp_millis(),
        }
    }
```

`grpc_server.rs` — implement the new trait method in the `Coordinator` service impl (the generated trait will demand it; follow the shape of the existing `cluster_status` method):

```rust
    async fn drain_worker(
        &self,
        request: Request<proto::DrainRequest>,
    ) -> Result<Response<proto::Ack>, Status> {
        let req = request.into_inner();
        if self.coordinator.drain_worker(&req.worker_id, &req.reason) {
            Ok(Response::new(proto::Ack {
                success: true,
                message: format!("worker '{}' draining", req.worker_id),
            }))
        } else {
            Err(Status::not_found(format!("worker '{}' is not registered", req.worker_id)))
        }
    }
```

(match the `proto::Ack` field names / `Ack::ok()` helper used elsewhere in the file.)

- [ ] **Step 4: Honest worker SQL**

`worker.rs` `execute_sql` (:507): replace the fake-success body with a failure:

```rust
        let msg = format!(
            "SQL task on connection '{}' cannot run on this worker: remote workers \
             have no provider registry yet. Run SQL DAGs locally (conduit run/apply) \
             or on a worker build with providers.",
            spec.connection
        );
        let _ = log_tx.send(TaskLogEntry {
            assignment_id: assignment_id.to_string(),
            worker_id: worker_id.to_string(),
            level: LogLevel::Error,
            message: msg.clone(),
            timestamp_ms: Utc::now().timestamp_millis(),
            metadata_json: String::new(),
        });
        (TaskOutcome::Failed, 1, msg, String::new(), HashMap::new())
```

Grep `conduit-distributed` tests for anything asserting SQL success on workers and flip those assertions to Failed (state why in the commit).

- [ ] **Step 5: Run distributed tests** — `cargo test -p conduit-distributed`: new drain test PASS, suite green.

- [ ] **Step 6: Rewire the three CLI commands**

`Cargo.toml`: add the dependency. Dispatch arms (~:903–910) become `rt.block_on(...)` since all three go async:

```rust
        Commands::Worker { coordinator, capacity, pools, id, labels } => rt.block_on(
            cmd_worker(&coordinator, capacity, &pools, id.as_deref(), &labels),
        ),
        Commands::Cluster { action } => match action {
            ClusterCommands::Status { coordinator, json } => {
                rt.block_on(cmd_cluster_status(&coordinator, json))
            }
            ClusterCommands::Drain { worker_id, coordinator } => {
                rt.block_on(cmd_cluster_drain(&coordinator, &worker_id))
            }
        },
```

`cmd_worker` (replace body from "Connecting to coordinator" down, keep the banner):

```rust
async fn cmd_worker(
    coordinator_addr: &str,
    capacity: u32,
    pools: &str,
    id: Option<&str>,
    label_strs: &[String],
) -> Result<()> {
    // …existing pool_list / labels / worker_id / banner code unchanged…

    let config = conduit_distributed::WorkerConfig {
        worker_id: worker_id.clone(),
        coordinator_addr: coordinator_addr.to_string(),
        capacity,
        pool_affinity: pool_list,
        labels,
        heartbeat_interval_secs: 5,
        graceful_shutdown: true,
        tls_ca_cert_path: None,
    };

    println!("Connecting to coordinator at {}...", coordinator_addr);
    conduit_distributed::run_worker(config)
        .await
        .map_err(|e| anyhow::anyhow!("worker failed: {} (is the coordinator running at {}?)", e, coordinator_addr))
}
```

`cmd_cluster_status`:

```rust
async fn cmd_cluster_status(coordinator_addr: &str, json: bool) -> Result<()> {
    use conduit_distributed::generated_proto::{
        coordinator_client::CoordinatorClient, ClusterHealth, ClusterStatusRequest, WorkerState,
    };

    let mut client = CoordinatorClient::connect(format!("http://{}", coordinator_addr))
        .await
        .map_err(|e| anyhow::anyhow!("cannot reach coordinator at {}: {}", coordinator_addr, e))?;
    let status = client
        .cluster_status(ClusterStatusRequest {})
        .await
        .map_err(|e| anyhow::anyhow!("ClusterStatus RPC failed: {}", e))?
        .into_inner();

    let health = ClusterHealth::try_from(status.health)
        .map(|h| format!("{:?}", h))
        .unwrap_or_else(|_| "Unknown".to_string());
    let worker_state = |s: i32| {
        WorkerState::try_from(s)
            .map(|w| format!("{:?}", w))
            .unwrap_or_else(|_| "Unknown".to_string())
    };

    if json {
        let value = serde_json::json!({
            "health": health,
            "coordinator": coordinator_addr,
            "uptime_secs": status.uptime_secs,
            "running_tasks": status.running_tasks,
            "queued_tasks": status.queued_tasks,
            "workers": status.workers.iter().map(|w| serde_json::json!({
                "worker_id": w.worker_id,
                "hostname": w.hostname,
                "state": worker_state(w.state),
                "capacity": w.capacity,
                "active_tasks": w.active_tasks,
                "pools": w.pool_affinity,
            })).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&value)?);
    } else {
        println!();
        println!("Cluster Status");
        println!("──────────────────────────────────────────");
        println!("  Coordinator:  {}", coordinator_addr);
        println!("  Health:       {}", health);
        println!("  Uptime:       {}s", status.uptime_secs);
        println!("  Workers:      {}", status.workers.len());
        println!("  Running:      {} tasks", status.running_tasks);
        println!("  Queued:       {} tasks", status.queued_tasks);
        for w in &status.workers {
            println!(
                "    - {} ({}) {} {}/{} tasks pools={:?}",
                w.worker_id, w.hostname, worker_state(w.state), w.active_tasks, w.capacity, w.pool_affinity
            );
        }
        if status.workers.is_empty() {
            println!();
            println!("No workers connected. Start one with:");
            println!("  conduit worker --coordinator {}", coordinator_addr);
        }
    }
    Ok(())
}
```

`cmd_cluster_drain`:

```rust
async fn cmd_cluster_drain(coordinator_addr: &str, worker_id: &str) -> Result<()> {
    use conduit_distributed::generated_proto::{coordinator_client::CoordinatorClient, DrainRequest};

    println!("Draining worker '{}' via {}...", worker_id, coordinator_addr);
    let mut client = CoordinatorClient::connect(format!("http://{}", coordinator_addr))
        .await
        .map_err(|e| anyhow::anyhow!("cannot reach coordinator at {}: {}", coordinator_addr, e))?;
    let ack = client
        .drain_worker(DrainRequest {
            worker_id: worker_id.to_string(),
            reason: "requested via conduit cluster drain".to_string(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("drain failed: {}", e))?
        .into_inner();
    println!("{}", ack.message);
    println!("Monitor with: conduit cluster status --coordinator {}", coordinator_addr);
    Ok(())
}
```

(If the generated client module path differs — e.g. re-exported at crate root via `proto_types` — adjust the `use` paths; `rg "pub mod coordinator_client" conduit-distributed/src/generated/` shows the truth. `gethostname` usage in cmd_worker: confirm the crate is a CLI dependency; add it if the build says otherwise.)

- [ ] **Step 7: CLI smoke tests (honest failure when nothing is listening)**

```rust
#[test]
fn cli_cluster_status_fails_honestly_when_unreachable() {
    conduit()
        .args(["cluster", "status", "--coordinator", "127.0.0.1:1"]) // nothing listens on port 1
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot reach coordinator"));
}

#[test]
fn cli_cluster_drain_fails_honestly_when_unreachable() {
    conduit()
        .args(["cluster", "drain", "w1", "--coordinator", "127.0.0.1:1"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot reach coordinator"));
}
```

(The old code printed fabricated success for both — these are the regression tests for the façade. If `readme_documented_commands_parse` asserted anything beyond `--help`, keep it green.)

- [ ] **Step 8: Run** — `cargo test -p conduit-distributed -p conduit-cli`: PASS.

- [ ] **Step 9: Commit**

```bash
cargo fmt --all
git add -A
git commit -s -m "feat(cli,distributed): real worker/cluster-status/drain over gRPC

conduit worker now runs the real gRPC worker runtime; cluster status
calls the ClusterStatus RPC; cluster drain uses a new DrainWorker RPC
whose directive is delivered on the worker's next heartbeat. Worker-side
SQL stub now fails honestly instead of reporting success. (Claims-audit
finding 6, part A)"
```

---

### Task 11: Distributed CLI, part B — `run --distributed` starts a coordinator and dispatches to workers

**Files:**
- Modify: `conduit-cli/src/main.rs` (Run dispatch arm ~:728; new `cmd_run_distributed` in the worker/cluster section)
- Test: `conduit-cli/tests/distributed_run_test.rs` (new; spawns the real binary twice)

**Interfaces:**
- Consumes: `DistributedExecutor::{with_persistence, dispatch, recv_result, coordinator, start_health_checker}` and `DispatchRequest`/`DispatchResult` (conduit-distributed/src/distributed_executor.rs); `serve_grpc(Arc<Coordinator>, SocketAddr, None, None)` (grpc_server.rs:190); `conduit_distributed::TaskType` (local mirror enum, proto_types); scheduler channel types as in cmd_run.
- Produces: `conduit run <dag> --distributed [--bind addr]` = start coordinator gRPC on `bind`, schedule the DAG, dispatch every task to connected workers, exit non-zero on failure. SQL tasks in distributed mode fail loudly (Task 10 Step 4) — documented.

- [ ] **Step 1: Failing end-to-end test (binary-level)**

Create `conduit-cli/tests/distributed_run_test.rs`:

```rust
use assert_cmd::cargo::CommandCargoExt;
use std::fs;
use std::process::{Command, Stdio};
use tempfile::TempDir;

/// Full distributed round trip: `run --distributed` starts a coordinator,
/// a separately spawned `worker` executes the bash task, run exits 0.
#[test]
fn distributed_run_executes_on_a_real_worker() {
    let dir = TempDir::new().unwrap();
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();
    let out_file = dir.path().join("touched");
    fs::write(
        dags.join("dist_demo.yaml"),
        format!(
            "id: dist_demo\ntasks:\n  touch:\n    type: bash\n    command: \"echo done > {}\"\n",
            out_file.display()
        ),
    )
    .unwrap();

    let port = 19477; // fixed high port; adjust if CI collides
    let bind = format!("127.0.0.1:{port}");

    let mut runner = Command::cargo_bin("conduit")
        .unwrap()
        .args(["run", "dist_demo", "--distributed", "--bind", &bind, "--dags-path"])
        .arg(&dags)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    std::thread::sleep(std::time::Duration::from_secs(2)); // coordinator up

    let mut worker = Command::cargo_bin("conduit")
        .unwrap()
        .args(["worker", "--coordinator", &bind, "--id", "w-test"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let status = runner.wait().unwrap(); // run exits once the DAG completes
    let _ = worker.kill();

    assert!(status.success(), "distributed run must exit 0");
    assert!(out_file.exists(), "task must have executed on the worker");
}
```

(Serial-port caveat: mark `#[ignore]`-not — keep it enabled but pick an uncommon port; if the suite runs tests in parallel with other net tests, that's fine, the port is unique to this test.)

- [ ] **Step 2: Run to verify failure** — the current banner-then-local path never opens the port; the worker can't connect; depending on timing the runner may still exit 0 via local execution — assert on worker connectability if needed. Expected before fix: worker exits with connection error / test FAILS on the "distributed" premise. After the fix both assertions hold.

- [ ] **Step 3: Implement cmd_run_distributed**

Dispatch arm (~:728): replace the banner-then-local block with

```rust
            if distributed {
                let bind_addr = bind.unwrap_or_else(|| "0.0.0.0:9400".to_string());
                return rt.block_on(cmd_run_distributed(
                    &dag_id, &dags_path, date.as_deref(), &bind_addr,
                ));
            }
```

New function (place after `cmd_worker`; mirrors cmd_run's scheduler wiring, replacing the local executor with the distributed bridge):

```rust
/// `conduit run --distributed`: run the coordinator in-process, serve the
/// worker gRPC endpoint on `bind_addr`, and dispatch every task of the DAG
/// run to connected workers.
async fn cmd_run_distributed(
    dag_id: &str,
    dags_path: &PathBuf,
    date: Option<&str>,
    bind_addr: &str,
) -> Result<()> {
    use conduit_distributed::{
        DispatchRequest, DistributedExecutor, DistributedExecutorConfig,
    };
    use conduit_scheduler::pool_manager::PoolManager;
    use conduit_scheduler::scheduler::{Scheduler, SchedulerCommand, SchedulerEvent};
    use std::collections::HashMap;

    // Phase 1: compile + resolve DAG (same as cmd_run — copy that block).
    let (plan, stats) = ConduitPlan::compile(dags_path)?;
    if !stats.errors.is_empty() {
        for err in &stats.errors { eprintln!("  {}", err); }
        std::process::exit(1);
    }
    let dag = plan.dags.get(dag_id)
        .ok_or_else(|| anyhow::anyhow!("DAG '{}' not found", dag_id))?
        .clone();

    // Phase 2: coordinator with durable assignment recovery.
    let state_dir = resolve_state_dir(dags_path);
    let mut dist_config = DistributedExecutorConfig::default();
    dist_config.coordinator.bind_addr = bind_addr.to_string(); // match the real field name
    let mut executor = DistributedExecutor::with_persistence(
        dist_config,
        &state_dir.join("coordinator_assignments"),
    )
    .await
    .map_err(|e| anyhow::anyhow!("failed to open coordinator assignment store: {}", e))?;
    let _health = executor.start_health_checker();

    let grpc_addr: std::net::SocketAddr = bind_addr.parse()?;
    let coordinator = std::sync::Arc::clone(executor.coordinator());
    tokio::spawn(async move {
        if let Err(e) = conduit_distributed::serve_grpc(coordinator, grpc_addr, None, None).await {
            eprintln!("coordinator gRPC server failed: {e}");
        }
    });
    println!("Coordinator listening on {bind_addr}");
    println!("Workers connect with: conduit worker --coordinator {bind_addr}");
    println!();

    // Phase 3: scheduler (copy cmd_run's channel + event-store + DagRunRequested setup).
    // …identical to cmd_run lines ~1284–1322, config triggered_by = "cli-distributed"…

    // Phase 4: bridge loop — SchedulerCommand -> DispatchRequest, DispatchResult -> SchedulerEvent.
    let logical_date = /* same date parsing as cmd_run */;
    let run_failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let run_failed_flag = std::sync::Arc::clone(&run_failed);
    let dag_for_exec = dag.clone();
    let bridge = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => match cmd {
                    SchedulerCommand::DispatchTask { dag_id, run_id, task_id, attempt } => {
                        let Some(task) = dag_for_exec.tasks.get(&task_id) else {
                            let _ = event_tx.send(SchedulerEvent::TaskFailed {
                                dag_id, run_id, task_id,
                                error: "Task definition not found".into(), attempt,
                            });
                            continue;
                        };
                        println!("  [DISPATCH] {} (attempt {})", task_id, attempt);
                        let req = dispatch_request_for(
                            task, &dag_id, &run_id, &task_id, attempt, logical_date,
                        );
                        executor.dispatch(req).await; // Queued is fine — waits for workers
                    }
                    SchedulerCommand::CompleteDagRun { dag_id, run_id, status } => {
                        println!();
                        println!("DAG '{}' run '{}' completed: {:?}", dag_id, run_id, status);
                        if !matches!(status, conduit_scheduler::scheduler::RunStatus::Success) {
                            run_failed_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        }
                        break;
                    }
                    SchedulerCommand::SkipTask { task_id, reason, .. } => {
                        println!("  [SKIP]  {} ({})", task_id, reason);
                    }
                    SchedulerCommand::RetryTask { task_id, delay, .. } => {
                        println!("  [RETRY] {} (retrying in {}s)", task_id, delay.num_seconds());
                    }
                },
                Some(result) = executor.recv_result() => {
                    if result.success {
                        println!("  [OK]    {} ({}ms, remote)", result.task_id, result.duration_ms);
                        let _ = event_tx.send(SchedulerEvent::TaskCompleted {
                            dag_id: result.dag_id, run_id: result.run_id,
                            task_id: result.task_id, snapshot_id: None,
                            duration_ms: result.duration_ms,
                        });
                    } else {
                        println!("  [FAIL]  {} — {}", result.task_id,
                            result.error.as_deref().unwrap_or("unknown error"));
                        let _ = event_tx.send(SchedulerEvent::TaskFailed {
                            dag_id: result.dag_id, run_id: result.run_id,
                            task_id: result.task_id,
                            error: result.error.unwrap_or_default(),
                            attempt: result.attempt,
                        });
                    }
                }
                else => break,
            }
        }
        let _ = event_tx.send(SchedulerEvent::Shutdown);
    });

    let _ = tokio::join!(scheduler_handle, bridge);
    if run_failed.load(std::sync::atomic::Ordering::SeqCst) {
        anyhow::bail!("distributed DAG run failed — see task output above");
    }
    Ok(())
}
```

Ownership note: `executor.dispatch(&self)` vs `recv_result(&mut self)` both inside the select — `recv_result` needs `&mut`; restructure by destructuring the executor (dispatch via `executor.coordinator()`-level API is not available, so instead: keep `let mut executor = …` owned by the bridge task and call methods sequentially inside the `select!` arms — `tokio::select!` on `cmd_rx.recv()` and `executor.recv_result()` borrows `executor` mutably in one arm and immutably in the other, which does not compile. Fix: split — spawn the dispatch handling inline (dispatch is `&self`), and poll results via `executor.recv_result()` — the clean structure is the one `run_distributed_loop` (distributed_executor.rs:274) already uses: two channels. Follow it: create `(dispatch_tx, dispatch_rx)` + `(result_tx, result_rx)` unbounded channels, spawn `conduit_distributed::distributed_executor::run_distributed_loop`-equivalent by moving `executor` into its own task fed by `dispatch_rx`, and have the bridge translate `SchedulerCommand→dispatch_tx` / `result_rx→SchedulerEvent`. If `run_distributed_loop` is public and takes `(dispatch_rx, result_tx, config)` — it is, but it constructs its own non-durable executor via `DistributedExecutor::new`; either add a `run_distributed_loop_with(executor, dispatch_rx, result_tx)` variant to conduit-distributed (preferred — 10 lines, reuses `with_persistence`) or accept `new()` for the CLI path. **Prefer adding the variant**:

```rust
/// Like `run_distributed_loop`, but drives a caller-constructed executor
/// (e.g. one created via `with_persistence`).
pub async fn run_distributed_loop_with(
    mut executor: DistributedExecutor,
    mut dispatch_rx: mpsc::UnboundedReceiver<DispatchRequest>,
    result_tx: mpsc::UnboundedSender<DispatchResult>,
) {
    let _health_handle = executor.start_health_checker();
    loop {
        tokio::select! {
            Some(req) = dispatch_rx.recv() => { executor.dispatch(req).await; }
            Some(result) = executor.recv_result() => {
                if result_tx.send(result).is_err() { break; }
            }
            else => break,
        }
    }
}
```

Export it from lib.rs next to the existing re-exports.)

And the Task→DispatchRequest mapping:

```rust
/// Map a compiled Task onto the distributed protocol's DispatchRequest.
/// Mirrors what the worker executes: Bash runs spec.script via bash -c,
/// Python runs spec.script via python3 -c, Executable runs command+args.
fn dispatch_request_for(
    task: &conduit_common::dag::Task,
    dag_id: &str,
    run_id: &str,
    task_id: &str,
    attempt: u32,
    logical_date: chrono::DateTime<chrono::Utc>,
) -> conduit_distributed::DispatchRequest {
    use conduit_common::dag::TaskType as T;
    use conduit_distributed::TaskType as DT;

    let (task_type, script, connection, query, command, args) = match &task.task_type {
        T::Bash { command } => (DT::Bash, command.clone(), String::new(), String::new(), String::new(), vec![]),
        T::Python { module, function } => (
            DT::Python,
            // Worker wraps spec.script in `python3 -c '…'`; import-and-call.
            format!("import {m}; {m}.{f}()", m = module, f = function),
            String::new(), String::new(), String::new(), vec![],
        ),
        T::Sql { connection, query, .. } => (
            DT::Sql, String::new(), connection.clone(), query.clone(), String::new(), vec![],
        ),
        T::Executable { command, args } => (
            DT::Executable, String::new(), String::new(), String::new(), command.clone(), args.clone(),
        ),
        T::Sensor { .. } => (DT::Sensor, String::new(), String::new(), String::new(), String::new(), vec![]),
    };

    conduit_distributed::DispatchRequest {
        dag_id: dag_id.to_string(),
        run_id: run_id.to_string(),
        task_id: task_id.to_string(),
        attempt,
        task_type,
        script,
        connection,
        query,
        command,
        args,
        timeout_secs: 0, // 0 = executor default
        pool: task.pool.clone().unwrap_or_default(),
        logical_date,
        environment: "production".to_string(),
        params: Default::default(),
        resources: Default::default(),
    }
}
```

(Check `conduit_distributed::TaskType` variant names in proto_types.rs and the `ResourceLimits` conversion — `task.resources` is the conduit-common type, `DispatchRequest.resources` the distributed one; map fields or use the distributed default. Check the `CoordinatorConfig` bind field's real name.)

- [ ] **Step 4: Run the e2e test** — `cargo test -p conduit-cli --test distributed_run_test`: PASS (allow ~30s; the runner exits when `CompleteDagRun` lands).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A
git commit -s -m "feat(cli,distributed): run --distributed starts a real coordinator and dispatches to workers

The banner-only path is gone: --distributed serves the coordinator gRPC
endpoint on --bind (durable assignment recovery under .conduit/), maps
scheduler dispatches onto the distributed protocol, and feeds worker
results back into the scheduler. Exit code reflects the run outcome.
(Claims-audit finding 6, part B)"
```

---

### Task 12: Documentation matches reality

**Files:**
- Modify: `docs/src/api-reference.md`
- Modify: `docs/src/concepts/plan-apply.md` (only if Task 3 left residue), `docs/src/concepts/contracts.md` (Task 4 sample output), `docs/src/concepts/incremental.md` (Task 5 file name)
- Modify: `conduit-cli/src/main.rs` clap doc-comments (`--max-concurrent` ~:438, `--max-tasks` ~:222, `--distributed` ~:231)
- Test: none (docs) — but `cargo test -p conduit-cli readme_documented_commands_parse` must stay green.

- [ ] **Step 1: Fix api-reference.md endpoint inventory**

Against the real router (`conduit-api/src/routes.rs:35–233`):

Remove (documented but not routed — do not leave them as promises):
- `POST /runs/{run_id}/cancel` (~:271)
- `GET /runs/{run_id}/logs` SSE (~:254) — note instead that task logs are returned inside `GET /runs/{run_id}` as `taskLogs`
- `GET /snapshots`, `GET /snapshots/{id}`, `DELETE /snapshots/{id}` (~:755–801)

Correct paths:
- `POST /dags/{dag_id}/compile` → `POST /dags/compile` (compiles the whole dags dir)
- `POST /dags/{dag_id}/run` → `POST /dags/{dag_id}/runs`
- `POST /environments/{env_name}/promote` → `POST /environments/promote` (source/target in body)
- `POST /lineage/upstream` / `POST /lineage/downstream` → `POST /lineage/trace/upstream` / `POST /lineage/trace/downstream`
- `WebSocket /api/v1/events/stream` → `GET /ws/events`

Update semantics:
- `POST /plan`: response includes `plan_id`; plans are cached server-side (in-memory, most recent 50) for use by apply.
- `POST /apply`: executes synchronously; honors `plan_id` (404 unknown, 400 environment mismatch, **409 `conflict` on stale plan**, 422 `apply_failed` on task/contract failure); response `{status: "applied", tasks_executed, environment_version, …}`. The old `"accepted"` fiction goes away.
- Error-code table (~:814–823): keep `CONFLICT` 409 — now real; drop or reword `INVALID_PLAN` to match the implemented 400/404/422 taxonomy.

- [ ] **Step 2: Sweep the concept docs**

- `plan-apply.md`: verify the Task 3 edits; also check the "Safety Guarantees" list still matches (audit trail claim was already true).
- `contracts.md`: sample "Contract Validation Summary" block matches the real `DeploymentValidation` Display output (Task 4 Step 5 pasted it).
- `incremental.md`: name the watermark file (`.conduit/watermarks.json`); confirm the `--full-refresh` examples list both `run` and `apply` (both now work). Add one line: SQL rewriting applies when the task has an incremental config; Python/Bash consume the `CONDUIT_*` env vars.
- Grep `docs/src` for `--max-concurrent`, `--max-tasks`, `distributed` claims; align wording (they are now honored; distributed run requires started workers; SQL on remote workers not yet supported — say so).

- [ ] **Step 3: Fix CLI help text**

- `--max-concurrent` (~:438): drop "(v0.1: sequential only)" → "Maximum partitions executed concurrently".
- `--max-tasks` (~:222): "Maximum tasks executed concurrently".
- `--distributed` (~:231): "Run via the distributed coordinator; workers must connect (see `conduit worker`)".

- [ ] **Step 4: Build the book if the toolchain exists**

Run: `mdbook build docs` (skip without failing the task if mdbook isn't installed — the CI owns that).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A
git commit -s -m "docs: align API reference and concept docs with implemented behavior

Remove unrouted endpoints (run cancel, SSE logs, snapshots CRUD), fix
path mismatches, document the real plan/apply semantics (plan_id, 409
stale-plan), watermark persistence location, and honored concurrency
flags. (Claims-audit finding 7, docs half + residue from findings 2-6)"
```

---

## Final verification (after all tasks)

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo test --workspace` (green)
- [ ] `cargo clippy --workspace --all-targets` — no new warnings vs. `main`
- [ ] Manual honesty sweep — each audit finding, exercised for real:
  1. `conduit run` on a SQL DAG without connections → non-zero exit, names the connection. With a DuckDB connection → real rows.
  2. `POST /apply` (serve running) → env version bumps, snapshots exist, restart `serve` → still there. CLI apply with a failing task → exit 1.
  3. Saved plan + intervening apply → `stale plan rejected`.
  4. `serve` restart keeps envs/runs; API-triggered staging run shows `environment: staging` in `GET /runs`.
  5. Contract violation blocks apply; watermark file advances across runs; `--full-refresh` forces refresh.
  6. `conduit worker` against a live `run --distributed` coordinator executes the DAG; `cluster status` shows the worker; `cluster drain` flips it to Draining; all three fail loudly when the coordinator is down.
  7. `--max-tasks`/`--max-concurrent` overlap verified by the timing tests.

## Self-review notes

- Spec coverage: finding 1 → Task 1; finding 2 → Tasks 2 + 8; finding 3 → Task 3 (+8 API); finding 4 → Tasks 6 + 7; finding 5 → Tasks 4 + 5; finding 6 → Tasks 10 + 11; finding 7 → Tasks 9 + 12.
- Known intentional scope cuts (stated, not hidden): remote workers can't run SQL (fail loudly, documented); run cancellation is removed from docs rather than implemented (no scheduler support); API plan cache is in-memory (documented), not persisted.
- Type-consistency: `build_provider_registry` (Task 1) reused in 5/8/9/11; `extra_env` (Task 5) consumed in 8; `base_environment_version` (Task 3) consumed in 8; `persist_environments` (Task 7) consumed in 8; `DrainWorker` RPC (Task 10) consumed by CLI drain.
