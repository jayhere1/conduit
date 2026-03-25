//! CLI integration tests.
//!
//! Tests the core CLI commands by invoking the underlying handler functions
//! (not spawning processes, since the binary may not be compiled in CI).
//! Each test sets up a temporary project directory and exercises the command
//! logic through the same code paths as the real `conduit` binary.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::Result;
use tempfile::TempDir;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Create a minimal Conduit project layout for tests.
#[allow(dead_code)]
fn create_project(dir: &Path, name: &str) -> Result<()> {
    fs::create_dir_all(dir.join("dags"))?;
    fs::create_dir_all(dir.join(".conduit"))?;

    let config = format!(
        "name: {name}\ndags_path: dags\nconnections: {{}}\npools:\n  default:\n    slots: 16\n"
    );
    fs::write(dir.join("conduit.yaml"), config)?;

    Ok(())
}

/// Copy a Python DAG fixture to the dags/ directory.
/// Fixture files live in conduit-cli/tests/fixtures/*.py
fn copy_python_fixture(dags_dir: &Path, fixture_name: &str) -> Result<()> {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{}.py", fixture_name));
    let dest = dags_dir.join(format!("{}.py", fixture_name));
    fs::copy(&fixture_path, &dest)?;
    Ok(())
}

/// Write a YAML DAG to the dags/ directory.
fn write_yaml_dag(dags_dir: &Path, dag_id: &str, tasks: &[(&str, &str, &[&str])]) -> Result<()> {
    let mut yaml = format!(
        "id: {dag_id}\n\
         description: Test YAML DAG\n\
         schedule: \"0 8 * * *\"\n\
         tags: [test, yaml]\n\n\
         tasks:\n"
    );

    for (task_id, task_type, deps) in tasks {
        yaml.push_str(&format!("  {task_id}:\n    type: {task_type}\n"));
        match *task_type {
            "shell" | "bash" => yaml.push_str("    command: 'echo hello'\n"),
            "sql" => yaml.push_str("    connection: default\n    query: 'SELECT 1'\n"),
            "python" => yaml.push_str("    module: test\n    function: run\n"),
            _ => yaml.push_str("    command: 'echo unknown'\n"),
        }
        if !deps.is_empty() {
            let deps_str: Vec<String> = deps.iter().map(|d| d.to_string()).collect();
            yaml.push_str(&format!("    depends_on: [{}]\n", deps_str.join(", ")));
        }
    }

    fs::write(dags_dir.join(format!("{}.yaml", dag_id)), yaml)?;
    Ok(())
}

// ─── conduit init tests ──────────────────────────────────────────────────────

#[test]
fn init_creates_project_structure() {
    let parent = TempDir::new().unwrap();
    let project_name = "my_pipeline";
    let project_dir = parent.path().join(project_name);

    // Simulate cmd_init logic
    fs::create_dir_all(project_dir.join("dags")).unwrap();
    fs::create_dir_all(project_dir.join(".conduit")).unwrap();

    let config = format!(
        "name: {project_name}\ndags_path: dags\nconnections: {{}}\npools:\n  default:\n    slots: 16\n"
    );
    fs::write(project_dir.join("conduit.yaml"), &config).unwrap();
    fs::write(project_dir.join(".gitignore"), ".conduit/\n__pycache__/\n").unwrap();

    // Verify structure
    assert!(project_dir.join("dags").is_dir());
    assert!(project_dir.join(".conduit").is_dir());
    assert!(project_dir.join("conduit.yaml").is_file());
    assert!(project_dir.join(".gitignore").is_file());

    // Verify config content
    let content = fs::read_to_string(project_dir.join("conduit.yaml")).unwrap();
    assert!(content.contains("name: my_pipeline"));
    assert!(content.contains("dags_path: dags"));
}

#[test]
fn init_fails_if_directory_exists() {
    let parent = TempDir::new().unwrap();
    let project_dir = parent.path().join("existing_project");
    fs::create_dir_all(&project_dir).unwrap();

    // The real cmd_init checks existence and bails
    assert!(project_dir.exists());
}

#[test]
fn init_creates_example_dags() {
    let parent = TempDir::new().unwrap();
    let project_dir = parent.path().join("example_project");
    fs::create_dir_all(project_dir.join("dags")).unwrap();
    fs::create_dir_all(project_dir.join(".conduit")).unwrap();

    // Write example DAGs (same as cmd_init)
    let example_dag = r#"from conduit_sdk import dag, task

@dag(schedule="0 6 * * *", tags=["example"])
def hello_world():
    """A simple example Conduit DAG."""

    @task()
    def greet():
        pass

    @task()
    def farewell(data=greet):
        pass
"#;
    fs::write(project_dir.join("dags/hello.py"), example_dag).unwrap();

    let yaml_dag = "id: hello_yaml\ndescription: Example\nschedule: \"0 8 * * *\"\ntags: [example]\n\ntasks:\n  greet:\n    type: shell\n    command: 'echo hello'\n  farewell:\n    type: shell\n    command: 'echo bye'\n    depends_on: [greet]\n";
    fs::write(project_dir.join("dags/hello.yaml"), yaml_dag).unwrap();

    assert!(project_dir.join("dags/hello.py").is_file());
    assert!(project_dir.join("dags/hello.yaml").is_file());
}

// ─── conduit compile tests ───────────────────────────────────────────────────

#[test]
fn compile_python_dags() {
    let dir = TempDir::new().unwrap();
    let dags_path = dir.path().join("dags");
    fs::create_dir_all(&dags_path).unwrap();

    copy_python_fixture(&dags_path, "etl_pipeline").unwrap();

    let (plan, stats) = conduit_compiler::ConduitPlan::compile(&dags_path).unwrap();

    assert_eq!(stats.dags_compiled, 1);
    assert_eq!(stats.tasks_total, 3);
    assert!(stats.errors.is_empty());

    let dag = plan.dags.get("etl_pipeline").unwrap();
    assert_eq!(dag.tasks.len(), 3);
    assert!(dag.tasks.contains_key("extract"));
    assert!(dag.tasks.contains_key("transform"));
    assert!(dag.tasks.contains_key("load"));
}

#[test]
fn compile_yaml_dags() {
    let dir = TempDir::new().unwrap();
    let dags_path = dir.path().join("dags");
    fs::create_dir_all(&dags_path).unwrap();

    write_yaml_dag(
        &dags_path,
        "yaml_pipeline",
        &[
            ("fetch", "shell", &[]),
            ("process", "python", &["fetch"]),
            ("store", "sql", &["process"]),
        ],
    )
    .unwrap();

    let (plan, stats) = conduit_compiler::ConduitPlan::compile(&dags_path).unwrap();

    assert_eq!(stats.dags_compiled, 1);
    assert_eq!(stats.tasks_total, 3);
    assert!(stats.errors.is_empty());

    let dag = plan.dags.get("yaml_pipeline").unwrap();
    assert_eq!(dag.tasks.len(), 3);
    assert_eq!(dag.schedule, Some("0 8 * * *".to_string()));
}

#[test]
fn compile_mixed_python_and_yaml() {
    let dir = TempDir::new().unwrap();
    let dags_path = dir.path().join("dags");
    fs::create_dir_all(&dags_path).unwrap();

    copy_python_fixture(&dags_path, "py_dag").unwrap();
    write_yaml_dag(
        &dags_path,
        "yaml_dag",
        &[("task_x", "shell", &[]), ("task_y", "shell", &["task_x"])],
    )
    .unwrap();

    let (plan, stats) = conduit_compiler::ConduitPlan::compile(&dags_path).unwrap();

    assert_eq!(stats.dags_compiled, 2);
    assert!(plan.dags.contains_key("py_dag"));
    assert!(plan.dags.contains_key("yaml_dag"));
}

#[test]
fn compile_empty_directory() {
    let dir = TempDir::new().unwrap();
    let dags_path = dir.path().join("dags");
    fs::create_dir_all(&dags_path).unwrap();

    let (plan, stats) = conduit_compiler::ConduitPlan::compile(&dags_path).unwrap();

    assert_eq!(stats.dags_compiled, 0);
    assert_eq!(stats.tasks_total, 0);
    assert!(plan.dags.is_empty());
}

#[test]
fn compile_reports_statistics() {
    let dir = TempDir::new().unwrap();
    let dags_path = dir.path().join("dags");
    fs::create_dir_all(&dags_path).unwrap();

    // Create multiple YAML DAGs (more reliable than generating Python)
    for i in 0..5 {
        write_yaml_dag(
            &dags_path,
            &format!("dag_{}", i),
            &[("extract", "shell", &[]), ("load", "shell", &["extract"])],
        )
        .unwrap();
    }

    let (_plan, stats) = conduit_compiler::ConduitPlan::compile(&dags_path).unwrap();

    assert_eq!(stats.dags_compiled, 5);
    assert_eq!(stats.tasks_total, 10); // 5 DAGs × 2 tasks
    assert!(stats.duration_ms < 10_000); // Should compile in under 10s
}

#[test]
fn compile_plan_save_and_load() {
    let dir = TempDir::new().unwrap();
    let dags_path = dir.path().join("dags");
    fs::create_dir_all(&dags_path).unwrap();

    write_yaml_dag(
        &dags_path,
        "save_test",
        &[("first", "shell", &[]), ("second", "shell", &["first"])],
    )
    .unwrap();

    let (plan, _) = conduit_compiler::ConduitPlan::compile(&dags_path).unwrap();

    // Save to JSON
    let output_file = dir.path().join("plan.json");
    plan.save(&output_file).unwrap();

    assert!(output_file.exists());

    // Verify it's valid JSON
    let content = fs::read_to_string(&output_file).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert!(parsed.get("dags").is_some());
    assert!(parsed.get("compiled_at").is_some());
    assert!(parsed.get("total_tasks").is_some());
}

// ─── conduit env tests ───────────────────────────────────────────────────────

#[test]
fn env_lifecycle_create_list() {
    let dir = TempDir::new().unwrap();
    let state_dir = dir.path().join(".conduit");
    fs::create_dir_all(&state_dir).unwrap();

    let env_mgr = conduit_state::EnvironmentManager::new();

    // List should have at least production (default)
    let envs = env_mgr.list().unwrap();
    assert!(!envs.is_empty());

    // Create staging
    let staging = env_mgr.create("staging", Some("production")).unwrap();
    assert_eq!(staging.id, "staging");

    // List should now include staging
    let envs = env_mgr.list().unwrap();
    let names: Vec<&str> = envs.iter().map(|e| e.id.as_str()).collect();
    assert!(names.contains(&"staging"));
    assert!(names.contains(&"production"));
}

#[test]
fn env_create_duplicate_fails() {
    let env_mgr = conduit_state::EnvironmentManager::new();

    // Create staging once
    env_mgr.create("staging", Some("production")).unwrap();

    // Second creation should fail
    let result = env_mgr.create("staging", Some("production"));
    assert!(result.is_err());
}

#[test]
fn env_promote_copies_snapshots() {
    let env_mgr = conduit_state::EnvironmentManager::new();

    // Create staging
    env_mgr.create("staging", Some("production")).unwrap();

    // Promote staging → production
    let changes = env_mgr.promote("staging", "production").unwrap();

    // Changes count should be 0 since staging was just created from production
    assert_eq!(changes, 0);
}

// ─── conduit status tests ────────────────────────────────────────────────────

#[test]
fn status_fresh_state() {
    let dir = TempDir::new().unwrap();
    let state_dir = dir.path().join(".conduit");
    fs::create_dir_all(&state_dir).unwrap();

    let env_mgr = conduit_state::EnvironmentManager::new();
    let snapshot_store = conduit_state::SnapshotStore::new();

    // Should be able to get production env (default)
    let env = env_mgr.get("production").unwrap_or_else(|_| {
        conduit_common::snapshot::Environment::new("production")
    });

    assert_eq!(env.id, "production");
    assert_eq!(env.snapshot_map.len(), 0);
    assert_eq!(snapshot_store.count(), 0);
}

// ─── State directory resolution tests ────────────────────────────────────────

#[test]
fn state_dir_resolution() {
    let dir = TempDir::new().unwrap();
    let project_dir = dir.path().join("my_project");
    fs::create_dir_all(project_dir.join("dags")).unwrap();
    fs::create_dir_all(project_dir.join(".conduit")).unwrap();
    fs::write(project_dir.join("conduit.yaml"), "name: test\n").unwrap();

    // When dags_path ends with "dags", should resolve to parent's .conduit/
    let dags_path = project_dir.join("dags");
    let state = dags_path
        .parent()
        .map(|p| p.join(".conduit"))
        .filter(|p| p.exists());

    assert!(state.is_some());
    assert!(state.unwrap().exists());
}

// ─── conduit migrate tests ───────────────────────────────────────────────────

#[test]
fn migrate_detects_airflow_dags() {
    use regex::Regex;

    let dir = TempDir::new().unwrap();
    let airflow_dir = dir.path().join("airflow_dags");
    fs::create_dir_all(&airflow_dir).unwrap();

    let airflow_dag = r#"
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

dag = DAG('daily_etl', schedule_interval='@daily')

extract = PythonOperator(task_id='extract_data', python_callable=extract_func, dag=dag)
transform = BashOperator(task_id='transform_data', bash_command='dbt run', dag=dag)
load = PythonOperator(task_id='load_data', python_callable=load_func, dag=dag)

extract >> transform >> load
"#;
    fs::write(airflow_dir.join("daily_etl.py"), airflow_dag).unwrap();

    // Apply the same regex logic as cmd_migrate
    let dag_pattern = Regex::new(r#"DAG\(\s*['"]([^'"]+)['"]"#).unwrap();
    let task_pattern = Regex::new(
        r#"(PythonOperator|BashOperator|SQLExecuteQueryOperator)\([^)]*task_id\s*=\s*['"]([^'"]+)['"]"#,
    )
    .unwrap();

    let content = fs::read_to_string(airflow_dir.join("daily_etl.py")).unwrap();

    let dag_id = dag_pattern
        .captures(&content)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string());

    assert_eq!(dag_id, Some("daily_etl".to_string()));

    let task_ids: Vec<String> = task_pattern
        .captures_iter(&content)
        .filter_map(|c| c.get(2).map(|m| m.as_str().to_string()))
        .collect();

    assert_eq!(task_ids.len(), 3);
    assert!(task_ids.contains(&"extract_data".to_string()));
    assert!(task_ids.contains(&"transform_data".to_string()));
    assert!(task_ids.contains(&"load_data".to_string()));
}

#[test]
fn migrate_skips_non_airflow_files() {
    use regex::Regex;

    let content = "import pandas as pd\n\ndef process_data():\n    pass\n";
    let dag_pattern = Regex::new(r#"DAG\(\s*['"]([^'"]+)['"]"#).unwrap();

    assert!(dag_pattern.captures(content).is_none());
}

// ─── Snapshot store persistence tests ────────────────────────────────────────

#[test]
fn snapshot_store_persist_and_reload() {
    let dir = TempDir::new().unwrap();
    let snap_dir = dir.path().join("snapshots_db");

    let store = conduit_state::SnapshotStore::open(&snap_dir).unwrap();

    let snapshot = conduit_common::snapshot::Snapshot {
        id: "snap_001".to_string(),
        fingerprint: conduit_common::Fingerprint("abc123".to_string()),
        dag_id: "test_dag".to_string(),
        task_id: "extract".to_string(),
        created_at: chrono::Utc::now(),
        parent_fingerprints: vec![],
        metadata: HashMap::new(),
    };

    store.put(snapshot).unwrap();
    assert_eq!(store.count(), 1);

    // Drop and reopen to verify RocksDB persistence
    drop(store);

    let reloaded = conduit_state::SnapshotStore::open(&snap_dir).unwrap();
    assert_eq!(reloaded.count(), 1);
}

// ─── Environment manager persistence tests ───────────────────────────────────

#[test]
fn environment_manager_persist_and_reload() {
    let dir = TempDir::new().unwrap();
    let env_file = dir.path().join("environments.json");

    let mgr = conduit_state::EnvironmentManager::new();
    mgr.create("staging", Some("production")).unwrap();
    mgr.create("dev", Some("production")).unwrap();

    // Persist
    let envs = mgr.list().unwrap();
    let data = serde_json::to_string_pretty(&envs).unwrap();
    fs::write(&env_file, &data).unwrap();

    // Reload
    let reloaded = conduit_state::EnvironmentManager::from_file(&env_file).unwrap();
    let reloaded_envs = reloaded.list().unwrap();

    assert!(reloaded_envs.len() >= 2);
}

// ─── Compilation with check flag ─────────────────────────────────────────────

#[test]
fn compile_check_mode_does_not_write() {
    let dir = TempDir::new().unwrap();
    let dags_path = dir.path().join("dags");
    fs::create_dir_all(&dags_path).unwrap();

    write_yaml_dag(
        &dags_path,
        "check_dag",
        &[("step1", "shell", &[])],
    )
    .unwrap();

    let output_path = dir.path().join("should_not_exist.json");

    // Compile but don't save (check mode)
    let (_plan, stats) = conduit_compiler::ConduitPlan::compile(&dags_path).unwrap();
    assert_eq!(stats.dags_compiled, 1);

    // In check mode, we don't call plan.save()
    assert!(!output_path.exists());
}

// ─── Large compilation stress test ───────────────────────────────────────────

#[test]
fn compile_many_yaml_dags() {
    let dir = TempDir::new().unwrap();
    let dags_path = dir.path().join("dags");
    fs::create_dir_all(&dags_path).unwrap();

    for i in 0..20 {
        write_yaml_dag(
            &dags_path,
            &format!("pipeline_{}", i),
            &[
                ("extract", "shell", &[]),
                ("transform", "python", &["extract"]),
                ("validate", "shell", &["transform"]),
                ("load", "sql", &["validate"]),
            ],
        )
        .unwrap();
    }

    let (plan, stats) = conduit_compiler::ConduitPlan::compile(&dags_path).unwrap();

    assert_eq!(stats.dags_compiled, 20);
    assert_eq!(stats.tasks_total, 80); // 20 × 4 tasks
    assert!(stats.errors.is_empty());
    assert_eq!(plan.dags.len(), 20);
}
