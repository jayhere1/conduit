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

#[test]
fn stitch_with_dbt_manifest_resolves_ref_to_real_table() {
    // Demonstrates the full §4.3 round-trip: a DAG where the SQL task
    // references its upstream via `{{ ref('orders') }}` instead of the
    // physical name. Without a manifest, that block becomes a placeholder
    // and stitching cannot connect the consumer's columns to the producer.
    // With a manifest mapping `orders` → `staging.orders`, the SQL
    // parses against the catalog and cross-task lineage is recovered.
    use conduit_lineage::{stitch_with_dbt_manifest, DbtManifest, DbtNode};

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
                ColumnSpec::new("customer_id"),
                ColumnSpec::new("amount"),
            ],
        )],
    );

    // The SQL uses `{{ ref('orders') }}` — without the manifest the parser
    // sees `FROM __conduit_jinja_0__` and cross-task lineage falls apart.
    let transform = make_task(
        "transform",
        TaskType::Sql {
            connection: "warehouse".to_string(),
            query: "INSERT INTO analytics.daily_revenue \
                 SELECT customer_id, SUM(amount) AS total \
                 FROM {{ ref('orders') }} \
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

    let mut tasks = HashMap::new();
    for t in [extract, transform] {
        tasks.insert(t.id.clone(), t);
    }
    let dag = Dag {
        id: "dbt_demo".to_string(),
        description: None,
        schedule: None,
        tags: vec![],
        max_active_runs: 1,
        on_failure: None,
        tasks,
        execution_order: vec!["extract_orders".to_string(), "transform".to_string()],
        source_file: "demo.py".to_string(),
        compiled_at: chrono::Utc::now(),
        catchup: false,
        max_catchup_runs: None,
        lineage_strict: false,
    };

    // Manifest: ref('orders') → `staging.orders` (the schema+alias the
    // upstream task declared as its output dataset).
    let mut nodes = std::collections::HashMap::new();
    nodes.insert(
        "model.demo.orders".to_string(),
        DbtNode {
            name: "orders".to_string(),
            resource_type: "model".to_string(),
            database: None,
            schema: "staging".to_string(),
            alias: None,
            package_name: Some("demo".to_string()),
        },
    );
    let manifest = DbtManifest {
        nodes,
        sources: std::collections::HashMap::new(),
    };

    // 1. Without a manifest: the SQL placeholder breaks the upstream
    //    chain. The total column has no edges into extract_orders.
    let no_manifest =
        stitch_with_dbt_manifest(&dag, None).expect("non-strict stitch should not error");
    let target = ColumnRef::task(TaskRef::new("dbt_demo", "transform"), "total");
    let upstream_no_manifest = no_manifest.graph.trace_upstream(&target);
    let reaches_producer_no_manifest = upstream_no_manifest.columns.iter().any(|c| {
        matches!(&c.source, ColumnSource::Task(t) if t.task_id == "extract_orders")
    });
    assert!(
        !reaches_producer_no_manifest,
        "without manifest, ref('orders') should be a placeholder and lineage should NOT reach \
         extract_orders. If this assertion fails the test premise is broken — investigate."
    );

    // 2. With the manifest: the SQL resolves to FROM staging.orders, the
    //    catalog matches it to extract_orders' declared output, and the
    //    upstream trace finds the producer task.
    let with_manifest = stitch_with_dbt_manifest(&dag, Some(&manifest))
        .expect("non-strict stitch should not error");
    let upstream = with_manifest.graph.trace_upstream(&target);
    let reaches_producer = upstream.columns.iter().any(|c| {
        matches!(&c.source, ColumnSource::Task(t) if t.task_id == "extract_orders")
    });
    assert!(
        reaches_producer,
        "with manifest, ref('orders') should resolve to staging.orders and the upstream trace \
         should reach extract_orders. Got columns: {:?}",
        upstream.columns
    );
}
