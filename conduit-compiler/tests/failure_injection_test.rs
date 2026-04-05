//! Failure injection tests for the compiler.
//!
//! These tests deliberately feed malformed, cyclic, and ambiguous DAG definitions
//! into the compiler pipeline and assert that it returns well-typed errors
//! rather than panicking.

use std::path::Path;

use conduit_compiler::{DependencyResolver, YamlDagParser};
use tempfile::TempDir;

// ─── Corrupt YAML ────────────────────────────────────────────────────────────

#[test]
fn test_corrupt_yaml_dag() {
    let dir = TempDir::new().unwrap();
    let bad_yaml_path = dir.path().join("bad.yaml");

    // Write syntactically invalid YAML (unmatched braces, bad indentation, etc.)
    let corrupt_content = r#"
id: broken_dag
tasks:
  a:
    type: shell
    command: echo hi
  - this is not valid here
  {{{{ totally broken yaml }}}}
  : :
"#;

    std::fs::write(&bad_yaml_path, corrupt_content).unwrap();

    // Parsing must return Err, not panic.
    let result = YamlDagParser::parse_file(&bad_yaml_path);
    assert!(
        result.is_err(),
        "Corrupt YAML should produce an error, got: {:?}",
        result
    );
}

// ─── Cyclic dependency ───────────────────────────────────────────────────────

#[test]
fn test_cyclic_dependency_detected() {
    let yaml = r#"
id: cyclic_dag
description: Task A depends on B, B depends on A
schedule: "@daily"
tasks:
  a:
    type: shell
    command: "echo a"
    depends_on: [b]
  b:
    type: shell
    command: "echo b"
    depends_on: [a]
"#;

    let parsed = YamlDagParser::parse_string(yaml, Path::new("cyclic.yaml")).unwrap();
    let result = DependencyResolver::resolve(parsed);

    assert!(result.is_err(), "Cyclic DAG must be rejected");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.to_lowercase().contains("cycle"),
        "Error should mention 'cycle', got: {msg}"
    );
}

// ─── Unknown dependency reference ────────────────────────────────────────────

#[test]
fn test_unknown_dependency_detected() {
    let yaml = r#"
id: bad_ref_dag
description: B depends on a task that does not exist
schedule: "@daily"
tasks:
  b:
    type: shell
    command: "echo b"
    depends_on: [nonexistent_task]
"#;

    let parsed = YamlDagParser::parse_string(yaml, Path::new("bad_ref.yaml")).unwrap();
    let result = DependencyResolver::resolve(parsed);

    assert!(result.is_err(), "Unknown dep must be rejected");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("nonexistent_task"),
        "Error should name the missing task, got: {msg}"
    );
}

// ─── Duplicate task IDs ──────────────────────────────────────────────────────

#[test]
fn test_duplicate_task_id() {
    // In YAML map syntax, duplicate keys silently take the last value,
    // so serde_yaml will not error on its own.
    //
    // The resolver catches duplicates when the ParsedDag contains two
    // ParsedTask entries with the same `id`. We construct such a dag
    // by parsing two single-task YAMLs and merging their task vecs.

    let yaml = r#"
id: dup_dag
description: testing duplicate detection
tasks:
  step_a:
    type: shell
    command: "echo a"
"#;

    let mut parsed = YamlDagParser::parse_string(yaml, Path::new("dup.yaml")).unwrap();

    // Parse the same YAML again to get a second ParsedTask with id "step_a".
    let parsed2 = YamlDagParser::parse_string(yaml, Path::new("dup2.yaml")).unwrap();
    parsed.tasks.extend(parsed2.tasks);

    let result = DependencyResolver::resolve(parsed);
    assert!(result.is_err(), "Duplicate task IDs must be rejected");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.to_lowercase().contains("duplicate"),
        "Error should mention 'duplicate', got: {msg}"
    );
}
