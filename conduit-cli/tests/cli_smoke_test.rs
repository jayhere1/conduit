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
    let parsed: serde_json::Value = serde_json::from_str(&contents)
        .expect("Output should be valid JSON");
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
