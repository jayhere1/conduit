//! End-to-end snapshot tests for `analyze_plan_impact` + `render_markdown`.
//!
//! Builds two `DagSet`s (base + head) in-process, runs analysis, asserts the
//! rendered markdown contains the structural anchors a PR reviewer would
//! actually read. We assert *containment*, not byte-equality, so cosmetic
//! formatter tweaks don't break tests.

use std::collections::HashMap;

use chrono::Utc;
use conduit_common::dag::{
    ColumnSpec, Dag, Dataset, DependencyType, ResourceLimits, Task, TaskDependency, TaskType,
    TriggerRule,
};
use conduit_lineage::{analyze_plan_impact, render_impact_markdown, DagSet};

fn col(name: &str) -> ColumnSpec {
    ColumnSpec::new(name)
}

fn col_typed(name: &str, dtype: &str) -> ColumnSpec {
    let mut c = ColumnSpec::new(name);
    c.dtype = Some(dtype.to_string());
    c
}

fn py_task(id: &str, outputs: Vec<Dataset>, inputs: Vec<Dataset>, deps: Vec<&str>) -> Task {
    Task {
        id: id.to_string(),
        task_type: TaskType::Python {
            module: "m".to_string(),
            function: id.to_string(),
        },
        dependencies: deps
            .into_iter()
            .map(|d| TaskDependency {
                task_id: d.to_string(),
                dependency_type: DependencyType::DataFlow,
            })
            .collect(),
        retries: 0,
        retry_delay: None,
        pool: None,
        timeout: None,
        priority: 0,
        resources: ResourceLimits::default(),
        trigger_rule: TriggerRule::default(),
        incremental: None,
        contracts: None,
        inputs,
        outputs,
    }
}

fn sql_task(
    id: &str,
    query: &str,
    target: Option<&str>,
    outputs: Vec<Dataset>,
    deps: Vec<&str>,
) -> Task {
    Task {
        id: id.to_string(),
        task_type: TaskType::Sql {
            connection: "warehouse".to_string(),
            query: query.to_string(),
            target: target.map(String::from),
        },
        dependencies: deps
            .into_iter()
            .map(|d| TaskDependency {
                task_id: d.to_string(),
                dependency_type: DependencyType::DataFlow,
            })
            .collect(),
        retries: 0,
        retry_delay: None,
        pool: None,
        timeout: None,
        priority: 0,
        resources: ResourceLimits::default(),
        trigger_rule: TriggerRule::default(),
        incremental: None,
        contracts: None,
        inputs: Vec::new(),
        outputs,
    }
}

fn dag(id: &str, lineage_strict: bool, tasks: Vec<Task>, order: Vec<&str>) -> Dag {
    let mut map = HashMap::new();
    for t in tasks {
        map.insert(t.id.clone(), t);
    }
    Dag {
        id: id.to_string(),
        description: None,
        schedule: None,
        tags: vec![],
        max_active_runs: 1,
        on_failure: None,
        tasks: map,
        execution_order: order.into_iter().map(String::from).collect(),
        source_file: String::new(),
        compiled_at: Utc::now(),
        catchup: false,
        max_catchup_runs: None,
        lineage_strict,
    }
}

fn dag_set(dags: Vec<Dag>) -> DagSet {
    let mut map = HashMap::new();
    for d in dags {
        map.insert(d.id.clone(), d);
    }
    map
}

#[test]
fn snapshot_pure_sql_column_drop() {
    // Base: 3-task SQL chain, seed.amount → transform.amount → load.total.
    // Head: seed.amount is removed. Reviewer should see breaking change on
    // seed and a downstream blast that names transform.
    let base_seed = sql_task(
        "seed",
        "SELECT 1 AS id, 100 AS amount",
        Some("staging.orders"),
        vec![Dataset::new(
            "staging.orders",
            vec![col_typed("id", "int"), col_typed("amount", "int")],
        )],
        vec![],
    );
    let base_transform = sql_task(
        "transform",
        "INSERT INTO analytics.totals SELECT id, amount FROM staging.orders",
        None,
        vec![Dataset::new(
            "analytics.totals",
            vec![col_typed("id", "int"), col_typed("amount", "int")],
        )],
        vec!["seed"],
    );
    let base_load = sql_task(
        "load",
        "INSERT INTO analytics.daily SELECT amount AS total FROM analytics.totals",
        None,
        vec![Dataset::new(
            "analytics.daily",
            vec![col_typed("total", "int")],
        )],
        vec!["transform"],
    );

    let head_seed = sql_task(
        "seed",
        "SELECT 1 AS id",
        Some("staging.orders"),
        vec![Dataset::new("staging.orders", vec![col_typed("id", "int")])],
        vec![],
    );

    let base = dag_set(vec![dag(
        "wh",
        false,
        vec![base_seed, base_transform.clone(), base_load.clone()],
        vec!["seed", "transform", "load"],
    )]);
    let head = dag_set(vec![dag(
        "wh",
        false,
        vec![head_seed, base_transform, base_load],
        vec!["seed", "transform", "load"],
    )]);

    let impact = analyze_plan_impact(&base, &head);
    assert!(impact.has_breaking_changes(), "expected breaking change");
    assert!(
        impact.summary.total_breaking_changes >= 1,
        "summary: {:?}",
        impact.summary
    );

    let md = render_impact_markdown(&impact);
    assert!(md.contains("breaking"), "header: {}", md);
    assert!(md.contains("`seed`"), "task id: {}", md);
    assert!(md.contains("staging.orders"), "dataset name: {}", md);
    assert!(md.contains("💥"), "breaking marker: {}", md);
    assert!(md.contains("`amount`"), "column: {}", md);
    // The downstream trace section appears whenever the head graph has edges
    // from the affected origin. For a column that was dropped (no head edges
    // from that column), the formatter prints the "no consumers traced" line.
    // Both outcomes are acceptable; assert one of them is present.
    let has_trace =
        md.contains("Downstream blast radius") || md.contains("No downstream consumers traced");
    assert!(has_trace, "expected downstream section, got: {}", md);
    assert!(md.contains("Lineage coverage"), "coverage footer: {}", md);
}

#[test]
fn snapshot_python_declared_dataset_change() {
    // Base: Python producer declares Dataset("staging.orders", [id, amount]).
    // Head: drops the amount column.
    // Downstream: Python consumer reads staging.orders[id, amount].
    // Expected: breaking on producer, consumer's amount becomes unresolved,
    // markdown calls out partial coverage in the footer.
    let producer_base = py_task(
        "extract",
        vec![Dataset::new(
            "staging.orders",
            vec![col("id"), col("amount")],
        )],
        vec![],
        vec![],
    );
    let producer_head = py_task(
        "extract",
        vec![Dataset::new("staging.orders", vec![col("id")])],
        vec![],
        vec![],
    );
    let consumer = py_task(
        "load",
        vec![],
        vec![Dataset::new(
            "staging.orders",
            vec![col("id"), col("amount")],
        )],
        vec!["extract"],
    );

    let base = dag_set(vec![dag(
        "py_chain",
        false,
        vec![producer_base, consumer.clone()],
        vec!["extract", "load"],
    )]);
    let head = dag_set(vec![dag(
        "py_chain",
        false,
        vec![producer_head, consumer],
        vec!["extract", "load"],
    )]);

    let impact = analyze_plan_impact(&base, &head);
    assert_eq!(
        impact.summary.total_breaking_changes, 1,
        "summary: {:?}",
        impact.summary
    );
    assert!(
        impact.coverage.head_unresolved_refs >= 1,
        "expected unresolved refs since head consumer reads dropped column, got {:?}",
        impact.coverage
    );

    let md = render_impact_markdown(&impact);
    assert!(md.contains("py_chain"));
    assert!(md.contains("`extract`"));
    assert!(md.contains("Lineage coverage: partial"), "coverage: {}", md);
    assert!(
        md.contains("could not be resolved"),
        "unresolved msg: {}",
        md
    );
}

#[test]
fn snapshot_no_changes_message_when_plans_identical() {
    let only_task = py_task(
        "noop",
        vec![Dataset::new("ds", vec![col("x")])],
        vec![],
        vec![],
    );
    let plan = dag_set(vec![dag("d", false, vec![only_task], vec!["noop"])]);

    let impact = analyze_plan_impact(&plan, &plan);
    let md = render_impact_markdown(&impact);
    assert!(md.contains("no changes detected"), "{}", md);
}
