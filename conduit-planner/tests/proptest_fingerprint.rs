//! Property-based tests for fingerprint computation.
//!
//! Verifies:
//! - Determinism: same input always produces the same fingerprint
//! - Sensitivity: different inputs produce different fingerprints
//! - Cascade: changing an upstream task changes all downstream fingerprints

use conduit_common::dag::*;
use conduit_planner::PlanFingerprinter;
use proptest::prelude::*;

fn make_task(id: &str, deps: Vec<&str>, command: &str) -> (String, Task) {
    (
        id.to_string(),
        Task {
            id: id.to_string(),
            task_type: TaskType::Bash {
                command: command.to_string(),
            },
            dependencies: deps
                .iter()
                .map(|d| TaskDependency {
                    task_id: d.to_string(),
                    dependency_type: DependencyType::DataFlow,
                })
                .collect(),
            retries: 0,
            retry_delay: None,
            retry_backoff: None,
            source_hash: None,
            pool: None,
            timeout: None,
            priority: 0,
            resources: ResourceLimits::default(),
            trigger_rule: if deps.is_empty() {
                TriggerRule::NoDeps
            } else {
                TriggerRule::AllSuccess
            },
            incremental: None,
            contracts: None,
            inputs: Vec::new(),
            outputs: Vec::new(),
        },
    )
}

fn make_dag(tasks: Vec<(String, Task)>, order: Vec<&str>) -> Dag {
    Dag {
        id: "test_dag".to_string(),
        description: None,
        schedule: None,
        tags: vec![],
        max_active_runs: 1,
        on_failure: None,
        tasks: tasks.into_iter().collect(),
        execution_order: order.into_iter().map(String::from).collect(),
        source_file: "test.py".to_string(),
        compiled_at: chrono::Utc::now(),
        catchup: false,
        max_catchup_runs: None,
        lineage_strict: false,
    }
}

proptest! {
    /// Same task content always produces the same fingerprint.
    #[test]
    fn fingerprint_is_deterministic(
        command in "[a-zA-Z0-9 ]{1,50}",
    ) {
        let tasks = vec![make_task("a", vec![], &command)];
        let dag = make_dag(tasks, vec!["a"]);

        let fp1 = PlanFingerprinter::fingerprint_dag(&dag);
        let fp2 = PlanFingerprinter::fingerprint_dag(&dag);

        prop_assert_eq!(fp1.get("a"), fp2.get("a"));
    }

    /// Different commands produce different fingerprints.
    #[test]
    fn different_content_different_fingerprint(
        cmd1 in "[a-z]{1,20}",
        cmd2 in "[A-Z]{1,20}",
    ) {
        // Only test when commands are actually different
        prop_assume!(cmd1 != cmd2.to_lowercase());

        let dag1 = make_dag(vec![make_task("a", vec![], &cmd1)], vec!["a"]);
        let dag2 = make_dag(vec![make_task("a", vec![], &cmd2)], vec!["a"]);

        let fp1 = PlanFingerprinter::fingerprint_dag(&dag1);
        let fp2 = PlanFingerprinter::fingerprint_dag(&dag2);

        prop_assert_ne!(fp1.get("a"), fp2.get("a"));
    }

    /// Changing an upstream task's command changes all downstream fingerprints.
    #[test]
    fn upstream_change_cascades_to_downstream(
        cmd1 in "[a-z]{1,20}",
        cmd2 in "[A-Z]{1,20}",
    ) {
        prop_assume!(cmd1 != cmd2.to_lowercase());

        let dag1 = make_dag(
            vec![
                make_task("root", vec![], &cmd1),
                make_task("child", vec!["root"], "echo child"),
            ],
            vec!["root", "child"],
        );

        let dag2 = make_dag(
            vec![
                make_task("root", vec![], &cmd2),
                make_task("child", vec!["root"], "echo child"),
            ],
            vec!["root", "child"],
        );

        let fp1 = PlanFingerprinter::fingerprint_dag(&dag1);
        let fp2 = PlanFingerprinter::fingerprint_dag(&dag2);

        // Root fingerprints should differ
        prop_assert_ne!(fp1.get("root"), fp2.get("root"));
        // Child fingerprints should also differ (cascade)
        prop_assert_ne!(fp1.get("child"), fp2.get("child"));
    }
}

/// Non-proptest: verify that independent tasks don't affect each other.
#[test]
fn independent_tasks_dont_cross_contaminate() {
    let dag_with_b = make_dag(
        vec![
            make_task("a", vec![], "echo a"),
            make_task("b", vec![], "echo b"),
        ],
        vec!["a", "b"],
    );

    let dag_with_b_changed = make_dag(
        vec![
            make_task("a", vec![], "echo a"),
            make_task("b", vec![], "echo CHANGED"),
        ],
        vec!["a", "b"],
    );

    let fp1 = PlanFingerprinter::fingerprint_dag(&dag_with_b);
    let fp2 = PlanFingerprinter::fingerprint_dag(&dag_with_b_changed);

    // Task "a" should have the same fingerprint in both
    assert_eq!(fp1.get("a"), fp2.get("a"));
    // Task "b" should differ
    assert_ne!(fp1.get("b"), fp2.get("b"));
}
