//! CLI smoke tests.
//!
//! Invoke the compiled `conduit` binary as a subprocess to verify
//! argument parsing, subcommand dispatch, and end-to-end behaviour.

use std::fs;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

// ─── Helpers ────────────────────────────────────────────────────────────────

fn conduit() -> Command {
    Command::cargo_bin("conduit").expect("conduit binary not found")
}

/// Write a minimal YAML DAG fixture into a dags/ directory.
fn write_yaml_dag(dir: &TempDir) -> std::path::PathBuf {
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();

    let dag = r#"
id: smoke_test
description: Smoke test DAG
schedule: "0 6 * * *"
tags: [test]

tasks:
  greet:
    type: bash
    command: "echo hello"
  farewell:
    type: bash
    command: "echo bye"
    depends_on: [greet]
"#;
    fs::write(dags.join("smoke_test.yaml"), dag).unwrap();
    dags
}

/// Write a Python DAG fixture.
fn write_python_dag(dir: &TempDir) -> std::path::PathBuf {
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();

    let dag = r#"from conduit_sdk import dag, task

@dag(schedule="0 6 * * *", tags=["smoke"])
def py_smoke():
    @task()
    def step_one():
        pass

    @task()
    def step_two(data):
        pass

    out = step_one()
    step_two(out)
"#;
    fs::write(dags.join("py_smoke.py"), dag).unwrap();
    dags
}

/// Write a SQL DAG fixture.
fn write_sql_dag(dir: &TempDir) -> std::path::PathBuf {
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();

    let dag = r#"
id: sql_lineage
description: SQL lineage fixture
tasks:
  summarize_orders:
    type: sql
    connection: warehouse
    query: "SELECT customer_id, SUM(amount) AS total FROM raw.orders WHERE status = 'paid' GROUP BY customer_id"
"#;
    fs::write(dags.join("sql_lineage.yaml"), dag).unwrap();
    dags
}

/// Three-task SQL pipeline that exercises cross-task lineage stitching:
/// `seed → transform → load`. Each task declares its target via `INSERT
/// INTO` / `CREATE TABLE AS`, which `infer_sql_io` lifts into Task I/O.
fn write_cross_task_sql_dag(dir: &TempDir) -> std::path::PathBuf {
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();

    let dag = r#"
id: cross_task_demo
description: Three-task SQL pipeline for cross-task lineage tests
tasks:
  seed:
    type: sql
    connection: warehouse
    query: "CREATE TABLE staging.orders AS SELECT 1 AS customer_id, 100 AS amount"
  transform:
    type: sql
    connection: warehouse
    query: "INSERT INTO analytics.daily_revenue SELECT customer_id, SUM(amount) AS total FROM staging.orders GROUP BY customer_id"
    depends_on: [seed]
  load:
    type: sql
    connection: warehouse
    query: "INSERT INTO reporting.summary SELECT customer_id, total FROM analytics.daily_revenue"
    depends_on: [transform]
"#;
    fs::write(dags.join("cross_task_demo.yaml"), dag).unwrap();
    dags
}

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

// ─── Tests ──────────────────────────────────────────────────────────────────

#[test]
fn cli_help_shows_usage() {
    conduit()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("conduit"))
        .stdout(predicate::str::contains("compile"));
}

#[test]
fn cli_version_works() {
    conduit()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("conduit"));
}

#[test]
fn cli_compile_yaml_dag() {
    let tmp = TempDir::new().unwrap();
    let dags = write_yaml_dag(&tmp);

    conduit()
        .arg("compile")
        .arg(dags.to_str().unwrap())
        .assert()
        .success()
        .stdout(predicate::str::contains("smoke_test").or(predicate::str::contains("compiled")));
}

#[test]
fn cli_compile_python_dag() {
    let tmp = TempDir::new().unwrap();
    let dags = write_python_dag(&tmp);

    conduit()
        .arg("compile")
        .arg(dags.to_str().unwrap())
        .assert()
        .success()
        .stdout(predicate::str::contains("py_smoke").or(predicate::str::contains("compiled")));
}

#[test]
fn cli_compile_to_output_file() {
    let tmp = TempDir::new().unwrap();
    let dags = write_yaml_dag(&tmp);
    let output = tmp.path().join("plan.json");

    conduit()
        .arg("compile")
        .arg(dags.to_str().unwrap())
        .arg("--output")
        .arg(output.to_str().unwrap())
        .assert()
        .success();

    assert!(output.exists(), "plan.json should be written");
    let contents = fs::read_to_string(&output).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&contents).expect("Output should be valid JSON");
    assert!(parsed.is_object(), "Plan should be a JSON object");
}

#[test]
fn cli_compile_nonexistent_path_fails() {
    conduit()
        .arg("compile")
        .arg("/tmp/nonexistent_dags_path_xyz")
        .assert()
        .failure();
}

#[test]
fn cli_run_yaml_dag() {
    let tmp = TempDir::new().unwrap();
    let dags = write_yaml_dag(&tmp);

    conduit()
        .arg("run")
        .arg("smoke_test")
        .arg("--dags-path")
        .arg(dags.to_str().unwrap())
        .assert()
        .success()
        .stdout(
            predicate::str::contains("greet")
                .or(predicate::str::contains("SUCCESS"))
                .or(predicate::str::contains("completed"))
                .or(predicate::str::contains("smoke_test")),
        );
}

#[test]
fn cli_run_env_flag_reaches_task_context() {
    let dir = TempDir::new().unwrap();
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();
    fs::write(
        dags.join("envcheck.yaml"),
        "id: envcheck\ntasks:\n  show:\n    type: bash\n    command: \"echo env=$CONDUIT_ENVIRONMENT\"\n",
    )
    .unwrap();

    conduit()
        .args(["run", "envcheck", "--env", "staging", "--dags-path"])
        .arg(&dags)
        .assert()
        .success()
        .stdout(predicate::str::contains("env=staging"));
}

#[test]
fn cli_run_nonexistent_dag_fails() {
    let tmp = TempDir::new().unwrap();
    let dags = write_yaml_dag(&tmp);

    conduit()
        .arg("run")
        .arg("nonexistent_dag_id")
        .arg("--dags-path")
        .arg(dags.to_str().unwrap())
        .assert()
        .failure();
}

#[test]
fn cli_run_sql_dag_without_connection_fails_loudly() {
    let dir = TempDir::new().unwrap();
    let dags = write_sql_dag(&dir); // references connection `warehouse`, no conduit.yaml

    conduit()
        .args(["run", "sql_lineage", "--dags-path"])
        .arg(&dags)
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("warehouse").and(predicate::str::contains("conduit.yaml")),
        );
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
        .args(["run", "par_demo", "--max-tasks", "2", "--dags-path"])
        .arg(&dags)
        .assert()
        .success();

    let read = |n: &str| -> (i128, i128) {
        let s = fs::read_to_string(marker.join(n)).unwrap();
        let v: Vec<i128> = s.lines().map(|l| l.trim().parse().unwrap()).collect();
        (v[0], v[1])
    };
    let (a0, a1) = read("a");
    let (b0, b1) = read("b");
    assert!(
        a0 < b1 && b0 < a1,
        "task intervals must overlap: a=({a0},{a1}) b=({b0},{b1})"
    );
}

#[test]
fn cli_backfill_max_concurrent_completes_all_partitions() {
    let dir = TempDir::new().unwrap();
    let dags = write_yaml_dag(&dir);

    conduit()
        .args([
            "backfill",
            "smoke_test",
            "--start",
            "2026-01-01",
            "--end",
            "2026-01-04",
            "--max-concurrent",
            "3",
            "--dags-path",
        ])
        .arg(&dags)
        .assert()
        .success()
        .stdout(predicate::str::contains("Succeeded:        3"));
}

#[test]
fn cli_plan_works() {
    let tmp = TempDir::new().unwrap();
    let dags = write_yaml_dag(&tmp);

    // Create .conduit/ state dir so plan has somewhere to read from
    fs::create_dir_all(tmp.path().join(".conduit")).unwrap();

    conduit()
        .arg("plan")
        .arg("production")
        .arg("--dags-path")
        .arg(dags.to_str().unwrap())
        .assert()
        .success();
}

#[test]
fn cli_init_creates_project() {
    let tmp = TempDir::new().unwrap();

    conduit()
        .arg("init")
        .arg("test_project")
        .current_dir(tmp.path())
        .assert()
        .success();

    let project_dir = tmp.path().join("test_project");
    assert!(project_dir.exists(), "Project directory should be created");
    assert!(
        project_dir.join("dags").exists(),
        "dags/ directory should exist"
    );
}

/// `conduit init` vendors the Python SDK so `conduit run` works outside a
/// repo checkout without `pip install conduit-sdk` (PRD B3).
#[test]
fn cli_init_vendors_python_sdk() {
    let tmp = TempDir::new().unwrap();

    conduit()
        .arg("init")
        .arg("vendored")
        .current_dir(tmp.path())
        .assert()
        .success();

    let sdk_root = tmp.path().join("vendored/.conduit/sdk");
    assert!(
        sdk_root.join("conduit_sdk/__init__.py").exists(),
        "vendored SDK package must exist"
    );
    assert!(
        sdk_root.join("conduit_sdk/_runtime.py").exists(),
        "runtime shim must be vendored (the executor imports it)"
    );
    assert!(
        sdk_root.join("VERSION").exists(),
        "vendored SDK must carry a version stamp"
    );
    // Bytecode caches must not be embedded in the binary or written out.
    assert!(
        !sdk_root.join("conduit_sdk/__pycache__").exists(),
        "__pycache__ must not be vendored"
    );
}

#[test]
fn cli_lineage_outputs_native_json_for_sql_task() {
    let tmp = TempDir::new().unwrap();
    let dags = write_sql_dag(&tmp);

    let output = conduit()
        .arg("lineage")
        .arg("extract")
        .arg("sql_lineage.summarize_orders")
        .arg("--dags-path")
        .arg(dags.to_str().unwrap())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let parsed: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(parsed["dag_id"], "sql_lineage");
    assert_eq!(parsed["task_id"], "summarize_orders");
    assert_eq!(parsed["source_tables"][0]["name"], "orders");
}

#[test]
fn cli_lineage_can_emit_openlineage_event() {
    let tmp = TempDir::new().unwrap();
    let dags = write_sql_dag(&tmp);

    let output = conduit()
        .arg("lineage")
        .arg("extract")
        .arg("sql_lineage.summarize_orders")
        .arg("--dags-path")
        .arg(dags.to_str().unwrap())
        .arg("--openlineage")
        .arg("--output-dataset")
        .arg("analytics.order_summary")
        .arg("--run-id")
        .arg("550e8400-e29b-41d4-a716-446655440000")
        .arg("--event-time")
        .arg("2026-05-17T12:00:00Z")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let parsed: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(parsed["eventType"], "COMPLETE");
    assert_eq!(parsed["outputs"][0]["name"], "analytics.order_summary");
    assert_eq!(
        parsed["outputs"][0]["facets"]["columnLineage"]["fields"]["total"]["inputFields"][0]
            ["transformations"][0]["subtype"],
        "AGGREGATION"
    );
}

#[test]
fn cli_lineage_trace_walks_cross_task_chain_text() {
    let tmp = TempDir::new().unwrap();
    let dags = write_cross_task_sql_dag(&tmp);

    let output = conduit()
        .arg("lineage")
        .arg("trace")
        .arg("--dag")
        .arg("cross_task_demo")
        .arg("--column")
        .arg("load.total")
        .arg("--dags-path")
        .arg(dags.to_str().unwrap())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let text = String::from_utf8_lossy(&output);
    // Default direction is upstream; the trace should walk back through
    // transform and reach seed via the catalog-resolved dataset chain.
    assert!(text.contains("upstream trace"), "got: {}", text);
    assert!(
        text.contains("cross_task_demo::transform"),
        "expected transform in trace, got: {}",
        text
    );
    assert!(
        text.contains("cross_task_demo::seed"),
        "expected seed in trace, got: {}",
        text
    );
    // Per-line task-kind annotation should mark SQL tasks.
    assert!(text.contains("[sql]"), "expected [sql] tag, got: {}", text);
}

#[test]
fn cli_lineage_trace_json_output() {
    let tmp = TempDir::new().unwrap();
    let dags = write_cross_task_sql_dag(&tmp);

    let output = conduit()
        .arg("lineage")
        .arg("trace")
        .arg("--dag")
        .arg("cross_task_demo")
        .arg("--column")
        .arg("load.total")
        .arg("--dags-path")
        .arg(dags.to_str().unwrap())
        .arg("--format")
        .arg("json")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let parsed: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(parsed["dag"], "cross_task_demo");
    assert_eq!(parsed["origin"]["task"], "load");
    assert_eq!(parsed["origin"]["column"], "total");
    assert_eq!(parsed["direction"], "upstream");
    assert!(parsed["columns"].as_array().unwrap().len() >= 2);
}

#[test]
fn cli_lineage_trace_unknown_column_fails() {
    let tmp = TempDir::new().unwrap();
    let dags = write_cross_task_sql_dag(&tmp);

    conduit()
        .arg("lineage")
        .arg("trace")
        .arg("--dag")
        .arg("cross_task_demo")
        .arg("--column")
        .arg("load.does_not_exist")
        .arg("--dags-path")
        .arg(dags.to_str().unwrap())
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "not found in the merged lineage graph",
        ));
}

// ─── conduit apply ──────────────────────────────────────────────────────────

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

#[test]
fn cli_apply_rejects_stale_plan_file() {
    let dir = TempDir::new().unwrap();
    let dags = write_yaml_dag(&dir);
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
        .args(["plan", "staging", "--dags-path"])
        .arg(&dags)
        .arg("--output")
        .arg(&plan_path)
        .assert()
        .success();

    conduit()
        .args(["apply", "production", "-y", "--plan-file"])
        .arg(&plan_path)
        .arg("--dags-path")
        .arg(&dags)
        .assert()
        .failure()
        .stderr(predicate::str::contains("targets environment 'staging'"));
}

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
        .args(["apply", "production", "-y", "--dags-path"])
        .arg(&dags)
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "contract_ok.emit contracts: 1/1 checks passed",
        ));
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
        .args(["apply", "production", "-y", "--dags-path"])
        .arg(&dags)
        .assert()
        .failure()
        .stderr(predicate::str::contains("contract"));

    // A blocked apply must not update the environment: a second `plan` must
    // still show the task as pending execution (cmd_status only prints
    // snapshot counts, not per-task pointers, so it can't distinguish this).
    conduit()
        .args(["plan", "production", "--dags-path"])
        .arg(&dags)
        .assert()
        .success()
        .stdout(predicate::str::contains("[EXEC ] contract_bad.emit"));
}

// ─── README contract ────────────────────────────────────────────────────────

/// Every command documented in the README's command table must parse.
/// If this fails, either the CLI changed (update the README) or the README
/// documents a command that doesn't exist (PRD B5).
#[test]
fn readme_documented_commands_parse() {
    let documented = [
        vec!["init"],
        vec!["compile"],
        vec!["run"],
        vec!["serve"],
        vec!["plan"],
        vec!["apply"],
        vec!["env", "create"],
        vec!["env", "list"],
        vec!["env", "promote"],
        vec!["env", "diff"],
        vec!["env", "history"],
        vec!["env", "rollback"],
        vec!["env", "set-policy"],
        vec!["lineage", "extract"],
        vec!["lineage", "trace"],
        vec!["impact"],
        vec!["backfill"],
        vec!["replay"],
        vec!["query"],
        vec!["preview"],
        vec!["worker"],
        vec!["cluster"],
        vec!["migrate"],
        vec!["status"],
    ];

    for cmd in documented {
        let mut c = conduit();
        for part in &cmd {
            c.arg(part);
        }
        c.arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Usage"));
    }
}

// ─── conduit impact (PRD C1) ─────────────────────────────────────────────────

/// Plan-file mode with DAGs directories: the head fixture drops a column the
/// downstream task reads — the JSON report must count it as breaking.
#[test]
fn impact_plan_file_mode_reports_breaking() {
    let out = conduit()
        .arg("impact")
        .arg("--base-plan")
        .arg("tests/fixtures/impact/base")
        .arg("--head-plan")
        .arg("tests/fixtures/impact/head")
        .arg("--format")
        .arg("json")
        .assert()
        .success();

    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    let report: serde_json::Value = serde_json::from_str(&stdout).expect("stdout must be JSON");
    let breaking = report["summary"]["total_breaking_changes"]
        .as_u64()
        .expect("summary.total_breaking_changes must exist");
    assert!(
        breaking >= 1,
        "expected at least one breaking change, got {breaking}"
    );
}

/// Markdown mode writes the report to --output and exits 0 even when
/// breaking changes exist (gating is the CI workflow's label logic).
#[test]
fn impact_markdown_mode_writes_output_file() {
    let tmp = TempDir::new().unwrap();
    let report_path = tmp.path().join("report.md");

    conduit()
        .arg("impact")
        .arg("--base-plan")
        .arg("tests/fixtures/impact/base")
        .arg("--head-plan")
        .arg("tests/fixtures/impact/head")
        .arg("--format")
        .arg("markdown")
        .arg("--output")
        .arg(report_path.to_str().unwrap())
        .assert()
        .success();

    let report = fs::read_to_string(&report_path).unwrap();
    assert!(
        report.contains("region"),
        "report must name the dropped column:\n{report}"
    );
}

/// Identical base and head → zero breaking changes.
#[test]
fn impact_identical_plans_report_clean() {
    let out = conduit()
        .arg("impact")
        .arg("--base-plan")
        .arg("tests/fixtures/impact/base")
        .arg("--head-plan")
        .arg("tests/fixtures/impact/base")
        .arg("--format")
        .arg("json")
        .assert()
        .success();

    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(
        report["summary"]["total_breaking_changes"].as_u64(),
        Some(0)
    );
}

/// End-to-end: first run of an incremental task has no watermark so it's a
/// full refresh; the emitted watermark is persisted to `.conduit/watermarks.json`;
/// the second run picks it up and runs incrementally; `--full-refresh`
/// forces a full refresh even with a watermark on file.
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
        .args(["run", "incr_demo", "--dags-path"])
        .arg(&dags)
        .assert()
        .success()
        .stdout(predicate::str::contains("full refresh"));

    // Watermark file was persisted next to the project.
    let wm_file = dir.path().join(".conduit").join("watermarks.json");
    assert!(wm_file.exists(), "watermarks.json must be persisted");
    let wm_json = fs::read_to_string(&wm_file).unwrap();
    assert!(
        wm_json.contains("2026-01-02T00:00:00"),
        "emitted watermark stored: {wm_json}"
    );

    // Second run: incremental, and the task sees CONDUIT_FULL_REFRESH=false.
    conduit()
        .args(["run", "incr_demo", "--dags-path"])
        .arg(&dags)
        .assert()
        .success()
        .stdout(
            predicate::str::contains("incremental").and(predicate::str::contains("refresh=false")),
        );

    // --full-refresh overrides the watermark.
    conduit()
        .args(["run", "incr_demo", "--full-refresh", "--dags-path"])
        .arg(&dags)
        .assert()
        .success()
        .stdout(predicate::str::contains("full refresh"));
}
