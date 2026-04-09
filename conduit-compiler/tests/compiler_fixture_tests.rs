//! Compiler integration tests using .py and .yaml fixture files.
//!
//! These tests exercise the full compilation pipeline (parse → resolve → plan)
//! against realistic DAG definitions stored in `tests/fixtures/`.

use std::path::{Path, PathBuf};

use conduit_common::dag::TaskType;
use conduit_compiler::{ConduitPlan, DagParser, DependencyResolver, YamlDagParser};

/// Resolve path to the fixtures directory.
fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

// ─── Full pipeline compilation from fixtures directory ───────────────────────

#[test]
fn compile_all_fixtures() {
    let fixtures = fixtures_dir();
    let (plan, stats) = ConduitPlan::compile(&fixtures).unwrap();

    // Should discover all DAGs from both .py and .yaml files
    assert!(
        stats.dags_compiled >= 6,
        "Expected at least 6 compiled DAGs, got {}",
        stats.dags_compiled
    );
    assert!(stats.tasks_total > 15);
    assert!(
        stats.errors.is_empty(),
        "Unexpected errors: {:?}",
        stats.errors
    );

    // Verify key DAGs exist
    assert!(plan.dags.contains_key("linear_etl"), "Missing linear_etl");
    assert!(plan.dags.contains_key("diamond_dag"), "Missing diamond_dag");
    assert!(plan.dags.contains_key("simple_etl"), "Missing simple_etl");
    assert!(
        plan.dags.contains_key("incremental_pipeline"),
        "Missing incremental_pipeline"
    );
    assert!(
        plan.dags.contains_key("all_task_types"),
        "Missing all_task_types"
    );
}

// ─── Python fixture: linear_etl.py ──────────────────────────────────────────

#[test]
fn fixture_linear_etl_structure() {
    let mut parser = DagParser::new().unwrap();
    let source = std::fs::read_to_string(fixtures_dir().join("linear_etl.py")).unwrap();
    let parsed = parser.parse_source(&source, "linear_etl.py").unwrap();

    assert_eq!(parsed.len(), 1);
    let dag = &parsed[0];

    assert_eq!(dag.id, "linear_etl");
    assert_eq!(dag.schedule, Some("0 6 * * *".to_string()));
    assert_eq!(dag.tags, vec!["etl", "warehouse"]);
    assert_eq!(dag.tasks.len(), 3);

    // Verify task ordering via dependencies
    let extract = dag.tasks.iter().find(|t| t.id == "extract_orders").unwrap();
    assert!(extract.raw_dependencies.is_empty());
    assert_eq!(extract.retries, 3);
    assert_eq!(extract.retry_delay, Some("5m".to_string()));
    assert_eq!(extract.pool, Some("source_pool".to_string()));

    let transform = dag
        .tasks
        .iter()
        .find(|t| t.id == "transform_orders")
        .unwrap();
    assert!(transform
        .raw_dependencies
        .contains(&"extract_orders".to_string()));
    assert_eq!(transform.timeout, Some("30m".to_string()));

    let load = dag.tasks.iter().find(|t| t.id == "load_orders").unwrap();
    assert!(load
        .raw_dependencies
        .contains(&"transform_orders".to_string()));
}

#[test]
fn fixture_linear_etl_resolves() {
    let mut parser = DagParser::new().unwrap();
    let source = std::fs::read_to_string(fixtures_dir().join("linear_etl.py")).unwrap();
    let parsed = parser.parse_source(&source, "linear_etl.py").unwrap();

    let dag = DependencyResolver::resolve(parsed.into_iter().next().unwrap()).unwrap();

    assert_eq!(dag.execution_order.len(), 3);
    // extract must come before transform, which must come before load
    let extract_pos = dag
        .execution_order
        .iter()
        .position(|t| t == "extract_orders")
        .unwrap();
    let transform_pos = dag
        .execution_order
        .iter()
        .position(|t| t == "transform_orders")
        .unwrap();
    let load_pos = dag
        .execution_order
        .iter()
        .position(|t| t == "load_orders")
        .unwrap();

    assert!(extract_pos < transform_pos);
    assert!(transform_pos < load_pos);
}

// ─── Python fixture: diamond_dag.py ─────────────────────────────────────────

#[test]
fn fixture_diamond_dag_structure() {
    let mut parser = DagParser::new().unwrap();
    let source = std::fs::read_to_string(fixtures_dir().join("diamond_dag.py")).unwrap();
    let parsed = parser.parse_source(&source, "diamond_dag.py").unwrap();

    let dag = &parsed[0];
    assert_eq!(dag.id, "diamond_dag");
    assert_eq!(dag.max_active_runs, 2);
    assert_eq!(dag.tasks.len(), 4);

    // join should depend on both branch_left and branch_right
    let join_task = dag.tasks.iter().find(|t| t.id == "join").unwrap();
    assert!(join_task
        .raw_dependencies
        .contains(&"branch_left".to_string()));
    assert!(join_task
        .raw_dependencies
        .contains(&"branch_right".to_string()));
}

#[test]
fn fixture_diamond_dag_resolves_with_parallelism() {
    let mut parser = DagParser::new().unwrap();
    let source = std::fs::read_to_string(fixtures_dir().join("diamond_dag.py")).unwrap();
    let parsed = parser.parse_source(&source, "diamond_dag.py").unwrap();

    let dag = DependencyResolver::resolve(parsed.into_iter().next().unwrap()).unwrap();

    // start must come first, join must come last
    let start_pos = dag
        .execution_order
        .iter()
        .position(|t| t == "start")
        .unwrap();
    let join_pos = dag
        .execution_order
        .iter()
        .position(|t| t == "join")
        .unwrap();

    assert_eq!(start_pos, 0);
    assert_eq!(join_pos, dag.execution_order.len() - 1);

    // branch_left and branch_right should be between start and join
    let left_pos = dag
        .execution_order
        .iter()
        .position(|t| t == "branch_left")
        .unwrap();
    let right_pos = dag
        .execution_order
        .iter()
        .position(|t| t == "branch_right")
        .unwrap();

    assert!(left_pos > start_pos && left_pos < join_pos);
    assert!(right_pos > start_pos && right_pos < join_pos);
}

// ─── Python fixture: multi_dag.py ───────────────────────────────────────────

#[test]
fn fixture_multi_dag_file() {
    let mut parser = DagParser::new().unwrap();
    let source = std::fs::read_to_string(fixtures_dir().join("multi_dag.py")).unwrap();
    let parsed = parser.parse_source(&source, "multi_dag.py").unwrap();

    // Should extract both DAGs from a single file
    assert_eq!(parsed.len(), 2);

    let ids: Vec<&str> = parsed.iter().map(|d| d.id.as_str()).collect();
    assert!(ids.contains(&"ingest_pipeline"));
    assert!(ids.contains(&"monitoring_pipeline"));

    let ingest = parsed.iter().find(|d| d.id == "ingest_pipeline").unwrap();
    assert_eq!(ingest.tasks.len(), 2);
    assert_eq!(ingest.schedule, Some("@daily".to_string()));

    let monitor = parsed
        .iter()
        .find(|d| d.id == "monitoring_pipeline")
        .unwrap();
    assert_eq!(monitor.tasks.len(), 2);
    assert_eq!(monitor.schedule, Some("@hourly".to_string()));
}

// ─── YAML fixture: simple_etl.yaml ──────────────────────────────────────────

#[test]
fn fixture_yaml_simple_etl() {
    let source = std::fs::read_to_string(fixtures_dir().join("simple_etl.yaml")).unwrap();
    let dag = YamlDagParser::parse_string(&source, Path::new("simple_etl.yaml")).unwrap();

    assert_eq!(dag.id, "simple_etl");
    assert_eq!(
        dag.description,
        Some("A simple ETL pipeline defined in YAML".to_string())
    );
    assert_eq!(dag.schedule, Some("0 6 * * *".to_string()));
    assert_eq!(dag.tags, vec!["etl", "yaml"]);
    assert_eq!(dag.max_active_runs, 1);
    assert_eq!(dag.tasks.len(), 3);

    // Verify task types
    let extract = dag.tasks.iter().find(|t| t.id == "extract").unwrap();
    assert_eq!(extract.retries, 2);
    assert_eq!(extract.timeout, Some("15m".to_string()));

    let transform = dag.tasks.iter().find(|t| t.id == "transform").unwrap();
    assert!(transform.raw_dependencies.contains(&"extract".to_string()));
    assert_eq!(transform.pool, Some("transform_pool".to_string()));

    let load = dag.tasks.iter().find(|t| t.id == "load").unwrap();
    assert!(load.raw_dependencies.contains(&"transform".to_string()));
}

#[test]
fn fixture_yaml_simple_etl_resolves() {
    let source = std::fs::read_to_string(fixtures_dir().join("simple_etl.yaml")).unwrap();
    let dag = YamlDagParser::parse_string(&source, Path::new("simple_etl.yaml")).unwrap();

    let resolved = DependencyResolver::resolve(dag).unwrap();

    assert_eq!(resolved.execution_order.len(), 3);
    let extract_pos = resolved
        .execution_order
        .iter()
        .position(|t| t == "extract")
        .unwrap();
    let load_pos = resolved
        .execution_order
        .iter()
        .position(|t| t == "load")
        .unwrap();
    assert!(extract_pos < load_pos);
}

// ─── YAML fixture: incremental.yaml ─────────────────────────────────────────

#[test]
fn fixture_yaml_incremental() {
    let source = std::fs::read_to_string(fixtures_dir().join("incremental.yaml")).unwrap();
    let dag = YamlDagParser::parse_string(&source, Path::new("incremental.yaml")).unwrap();

    assert_eq!(dag.id, "incremental_pipeline");
    assert_eq!(dag.schedule, Some("*/30 * * * *".to_string()));
    assert_eq!(dag.tasks.len(), 3);

    // Both merge_users and append_events depend on extract_events
    let merge = dag.tasks.iter().find(|t| t.id == "merge_users").unwrap();
    assert!(merge
        .raw_dependencies
        .contains(&"extract_events".to_string()));

    let append = dag.tasks.iter().find(|t| t.id == "append_events").unwrap();
    assert!(append
        .raw_dependencies
        .contains(&"extract_events".to_string()));
}

// ─── YAML fixture: all_task_types.yaml ───────────────────────────────────────

#[test]
fn fixture_yaml_all_task_types() {
    let source = std::fs::read_to_string(fixtures_dir().join("all_task_types.yaml")).unwrap();
    let dag = YamlDagParser::parse_string(&source, Path::new("all_task_types.yaml")).unwrap();

    assert_eq!(dag.id, "all_task_types");
    assert_eq!(dag.tasks.len(), 5);

    // Verify different task types
    let bash = dag.tasks.iter().find(|t| t.id == "bash_task").unwrap();
    assert!(matches!(bash.task_type, TaskType::Bash { .. }));

    let shell = dag.tasks.iter().find(|t| t.id == "shell_task").unwrap();
    assert!(matches!(shell.task_type, TaskType::Bash { .. })); // shell → Bash internally

    let python = dag.tasks.iter().find(|t| t.id == "python_task").unwrap();
    assert!(matches!(python.task_type, TaskType::Python { .. }));

    let sql = dag.tasks.iter().find(|t| t.id == "sql_task").unwrap();
    assert!(matches!(sql.task_type, TaskType::Sql { .. }));
    // sql_task depends on bash, shell, and python
    assert_eq!(sql.raw_dependencies.len(), 3);

    let sensor = dag.tasks.iter().find(|t| t.id == "sensor_task").unwrap();
    assert!(matches!(sensor.task_type, TaskType::Sensor { .. }));
    assert!(sensor.raw_dependencies.contains(&"sql_task".to_string()));
}

#[test]
fn fixture_yaml_all_task_types_resolves() {
    let source = std::fs::read_to_string(fixtures_dir().join("all_task_types.yaml")).unwrap();
    let dag = YamlDagParser::parse_string(&source, Path::new("all_task_types.yaml")).unwrap();

    let resolved = DependencyResolver::resolve(dag).unwrap();

    assert_eq!(resolved.execution_order.len(), 5);

    // sensor_task must be last (depends on sql_task which depends on 3 others)
    let sensor_pos = resolved
        .execution_order
        .iter()
        .position(|t| t == "sensor_task")
        .unwrap();
    assert_eq!(sensor_pos, 4);
}

// ─── YAML fixture: contracts.yaml ────────────────────────────────────────────

#[test]
fn fixture_yaml_contracts() {
    let source = std::fs::read_to_string(fixtures_dir().join("contracts.yaml")).unwrap();
    let dag = YamlDagParser::parse_string(&source, Path::new("contracts.yaml")).unwrap();

    assert_eq!(dag.id, "validated_pipeline");

    let transform = dag.tasks.iter().find(|t| t.id == "transform").unwrap();

    // Should have contracts
    assert!(transform.contracts.is_some());
    let contracts = transform.contracts.as_ref().unwrap();
    assert!(contracts.checks.len() >= 3);
}

// ─── Cycle detection ─────────────────────────────────────────────────────────

#[test]
fn cycle_detection_from_yaml_string() {
    let yaml = r#"
id: cyclic_dag
description: This DAG has a cycle
schedule: "@daily"
tasks:
  a:
    type: shell
    command: 'echo a'
    depends_on: [c]
  b:
    type: shell
    command: 'echo b'
    depends_on: [a]
  c:
    type: shell
    command: 'echo c'
    depends_on: [b]
"#;

    let parsed = YamlDagParser::parse_string(yaml, Path::new("cyclic.yaml")).unwrap();
    let result = DependencyResolver::resolve(parsed);

    assert!(result.is_err());
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("cycle") || msg.contains("Cycle"),
        "Error should mention cycle: {}",
        msg
    );
}

// ─── Unknown dependency detection ────────────────────────────────────────────

#[test]
fn unknown_dependency_from_yaml_string() {
    let yaml = r#"
id: bad_ref_dag
description: References a nonexistent task
schedule: "@daily"
tasks:
  step_a:
    type: shell
    command: 'echo a'
  step_b:
    type: shell
    command: 'echo b'
    depends_on: [step_a, nonexistent_task]
"#;

    let parsed = YamlDagParser::parse_string(yaml, Path::new("bad_ref.yaml")).unwrap();
    let result = DependencyResolver::resolve(parsed);

    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("nonexistent_task"),
        "Error should mention the missing task: {}",
        msg
    );
}

// ─── Directory scanning: skip conduit.yaml ───────────────────────────────────

#[test]
fn yaml_parser_skips_conduit_config() {
    let dir = tempfile::tempdir().unwrap();

    // Write a conduit.yaml config file (should be skipped)
    std::fs::write(
        dir.path().join("conduit.yaml"),
        "name: my_project\ndags_path: dags\n",
    )
    .unwrap();

    // Write a real DAG
    std::fs::write(
        dir.path().join("pipeline.yaml"),
        "id: real_dag\ntasks:\n  step:\n    type: shell\n    command: 'echo hi'\n",
    )
    .unwrap();

    let dags = YamlDagParser::parse_directory(dir.path()).unwrap();

    // Should find only the pipeline, not the config
    assert_eq!(dags.len(), 1);
    assert_eq!(dags[0].id, "real_dag");
}

// ─── Plan serialization roundtrip ────────────────────────────────────────────

#[test]
fn plan_serialization_roundtrip() {
    let (plan, _) = ConduitPlan::compile(&fixtures_dir()).unwrap();

    let json = plan.to_json().unwrap();
    let deserialized = ConduitPlan::from_json(&json).unwrap();

    assert_eq!(plan.dags.len(), deserialized.dags.len());
    assert_eq!(plan.total_tasks, deserialized.total_tasks);

    for (dag_id, dag) in &plan.dags {
        let d = deserialized.dags.get(dag_id).unwrap();
        assert_eq!(dag.tasks.len(), d.tasks.len());
        assert_eq!(dag.execution_order, d.execution_order);
    }
}

// ─── Python format equivalence with YAML ─────────────────────────────────────

#[test]
fn python_and_yaml_produce_equivalent_dags() {
    // Compile a directory with both formats
    let (plan, _) = ConduitPlan::compile(&fixtures_dir()).unwrap();

    // Both should produce DAGs with tasks, execution order, etc.
    for (dag_id, dag) in &plan.dags {
        assert!(!dag.tasks.is_empty(), "DAG '{}' should have tasks", dag_id);
        assert_eq!(
            dag.tasks.len(),
            dag.execution_order.len(),
            "DAG '{}' task count should match execution order length",
            dag_id
        );

        // Every task in execution order should exist in the tasks map
        for task_id in &dag.execution_order {
            assert!(
                dag.tasks.contains_key(task_id),
                "DAG '{}': task '{}' in execution_order but not in tasks map",
                dag_id,
                task_id
            );
        }
    }
}

// ─── Compilation performance assertion ───────────────────────────────────────

#[test]
fn fixtures_compile_under_5_seconds() {
    let start = std::time::Instant::now();
    let _ = ConduitPlan::compile(&fixtures_dir()).unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_secs() < 5,
        "Fixtures compilation took {:.1}s, expected < 5s",
        elapsed.as_secs_f64()
    );
}
