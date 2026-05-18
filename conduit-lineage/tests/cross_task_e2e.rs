//! End-to-end test for cross-task lineage stitching.
//!
//! Builds a 3-task DAG (Python → SQL → Python) directly via the
//! `conduit-common` model — bypassing the compiler so this test stays in
//! the lineage crate — then runs `stitch` and verifies that walking
//! `trace_upstream` from the final Python task's column reaches the
//! first Python task's declared output column.

use std::collections::HashMap;

use conduit_common::dag::{
    ColumnSpec, Dag, Dataset, DependencyType, ResourceLimits, Task, TaskDependency, TaskType,
    TriggerRule,
};
use conduit_lineage::{stitch, ColumnRef, ColumnSource, TaskRef};

fn make_task(
    id: &str,
    task_type: TaskType,
    deps: Vec<&str>,
    inputs: Vec<Dataset>,
    outputs: Vec<Dataset>,
) -> Task {
    Task {
        id: id.to_string(),
        task_type,
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

#[test]
fn python_to_sql_to_python_chain_is_stitched_end_to_end() {
    let extract = make_task(
        "extract_orders",
        TaskType::Python {
            module: "demo".to_string(),
            function: "extract_orders".to_string(),
        },
        vec![],
        vec![],
        vec![Dataset::new(
            "staging.orders",
            vec![
                ColumnSpec::new("id"),
                ColumnSpec::new("customer_id"),
                ColumnSpec::new("amount"),
            ],
        )],
    );

    let transform = make_task(
        "transform",
        TaskType::Sql {
            connection: "warehouse".to_string(),
            query: "INSERT INTO analytics.daily_revenue \
                 SELECT customer_id, SUM(amount) AS total \
                 FROM staging.orders \
                 GROUP BY customer_id"
                .to_string(),
            target: None,
        },
        vec!["extract_orders"],
        vec![],
        vec![Dataset::new(
            "analytics.daily_revenue",
            vec![ColumnSpec::new("customer_id"), ColumnSpec::new("total")],
        )],
    );

    let load = make_task(
        "push_to_warehouse",
        TaskType::Python {
            module: "demo".to_string(),
            function: "push_to_warehouse".to_string(),
        },
        vec!["transform"],
        vec![Dataset::new(
            "analytics.daily_revenue",
            vec![ColumnSpec::new("customer_id"), ColumnSpec::new("total")],
        )],
        vec![],
    );

    let mut tasks = HashMap::new();
    for t in [extract, transform, load] {
        tasks.insert(t.id.clone(), t);
    }
    let dag = Dag {
        id: "cross_task_demo".to_string(),
        description: None,
        schedule: None,
        tags: vec![],
        max_active_runs: 1,
        on_failure: None,
        tasks,
        execution_order: vec![
            "extract_orders".to_string(),
            "transform".to_string(),
            "push_to_warehouse".to_string(),
        ],
        source_file: "demo.py".to_string(),
        compiled_at: chrono::Utc::now(),
        catchup: false,
        max_catchup_runs: None,
        lineage_strict: false,
    };

    let result = stitch(&dag).expect("non-strict stitch should not error");
    assert!(
        result.unresolved.is_empty(),
        "expected zero unresolved refs, got {:?}",
        result.unresolved
    );

    // Walk upstream from push_to_warehouse.total — should reach
    // extract_orders.amount through the SQL transform's output column.
    let target = ColumnRef::task(
        TaskRef::new("cross_task_demo", "push_to_warehouse"),
        "total",
    );
    let trace = result.graph.trace_upstream(&target);

    let task_qualifiers: Vec<String> = trace
        .columns
        .iter()
        .filter_map(|c| match &c.source {
            ColumnSource::Task(t) => Some(t.to_string()),
            ColumnSource::Table(_) => None,
        })
        .collect();

    assert!(
        task_qualifiers
            .iter()
            .any(|q| q == "cross_task_demo::transform"),
        "expected chain to pass through transform; got {:?}",
        task_qualifiers
    );
    assert!(
        task_qualifiers
            .iter()
            .any(|q| q == "cross_task_demo::extract_orders"),
        "expected chain to reach extract_orders; got {:?}",
        task_qualifiers
    );
}
