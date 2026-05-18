//! Cross-task lineage stitching — the Python ↔ SQL ↔ Python bridge.
//!
//! Given a compiled [`Dag`] whose tasks already carry declared (or
//! inferred) [`Dataset`] I/O, `stitch` walks the task graph in topological
//! order and produces a merged [`LineageGraph`] whose edges span task
//! boundaries.
//!
//! Algorithm:
//! 1. Walk tasks in `dag.execution_order`.
//! 2. For each task, register its declared `outputs` into a fresh
//!    [`TableCatalog`] keyed on the dataset's qualified name. The
//!    producer is recorded so downstream consumers can resolve back to a
//!    [`TaskRef`].
//! 3. For SQL tasks, re-parse the query with `extract_with_catalog` so
//!    `FROM` clauses resolve columns against any upstream-registered
//!    datasets. The SQL extractor's column mappings are folded into the
//!    merged graph with `ColumnSource::Task` once we have a producer.
//! 4. For each task's `inputs`, look up the producer in the catalog. For
//!    every declared input column that the producer also declares, emit
//!    a cross-task edge. Columns the consumer claims to read but the
//!    producer didn't expose are collected as [`UnresolvedRef`]s.
//! 5. If `dag.lineage_strict` is true and there are unresolved refs,
//!    return [`LineageStrictError`]. Otherwise log a warning per
//!    unresolved column and return the graph.

use std::collections::HashMap;

use conduit_common::dag::{Dag, Dataset, Task, TaskType};
use tracing::warn;

use crate::catalog::{CatalogColumn, TableCatalog};
use crate::lineage_graph::{ColumnRef, ColumnSource, LineageGraph, TaskRef, TransformType};
use crate::schema::ColumnType;
use crate::sql_parser::SqlLineageExtractor;

/// Result of cross-task stitching: the merged graph plus any column
/// references that could not be resolved against an upstream declared
/// schema.
#[derive(Debug)]
pub struct CrossTaskLineage {
    pub graph: LineageGraph,
    pub unresolved: Vec<UnresolvedRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvedRef {
    pub consumer: TaskRef,
    pub dataset: String,
    pub column: String,
    pub reason: UnresolvedReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnresolvedReason {
    /// The consumer named a dataset that no task in this DAG produces.
    DatasetNotProduced,
    /// The producer exists but did not declare this column in its outputs.
    ColumnNotDeclared,
}

/// Returned in strict mode when at least one column reference cannot be
/// resolved against an upstream declaration.
#[derive(Debug, thiserror::Error)]
#[error(
    "lineage_strict: {} unresolved column reference(s) in DAG '{dag_id}'",
    unresolved.len()
)]
pub struct LineageStrictError {
    pub dag_id: String,
    pub unresolved: Vec<UnresolvedRef>,
}

/// Stitch a DAG's per-task lineage into a single column-level graph that
/// spans task boundaries.
///
/// Returns `Ok(CrossTaskLineage)` in lenient mode (warnings emitted for
/// unresolved refs) or `Err(LineageStrictError)` if the DAG declared
/// `lineage_strict = true` and any consumer column couldn't be resolved.
pub fn stitch(dag: &Dag) -> Result<CrossTaskLineage, LineageStrictError> {
    let mut graph = LineageGraph::new();
    let mut catalog = TableCatalog::new();
    let mut unresolved: Vec<UnresolvedRef> = Vec::new();

    // 1) Register declared outputs as we walk the topo order, so that by
    //    the time we process a consumer task, all of its upstream
    //    producers' datasets are already in the catalog.
    //
    //    Anonymous outputs (plain `SELECT` SQL tasks with no target) get
    //    registered under their *task id* — physical tables can collide,
    //    but the catalog's collision policy logs them.
    for task_id in &dag.execution_order {
        let Some(task) = dag.tasks.get(task_id) else {
            continue;
        };
        register_outputs(&dag.id, task, &mut catalog);
    }

    // 2) Walk again, this time wiring edges.
    for task_id in &dag.execution_order {
        let Some(task) = dag.tasks.get(task_id) else {
            continue;
        };
        let consumer = TaskRef::new(&dag.id, task_id);

        // SQL tasks: re-parse with the populated catalog so column
        // resolution can find upstream-registered datasets.
        if let TaskType::Sql { query, .. } = &task.task_type {
            let lineage = SqlLineageExtractor::extract_with_catalog(query, &catalog);
            for mapping in &lineage.column_mappings {
                // Skip the synthetic `__where__` sentinel — it tracks
                // filter dependencies, not output columns.
                if mapping.output == "__where__" {
                    continue;
                }
                let target = ColumnRef::task(consumer.clone(), &mapping.output);
                for input in &mapping.inputs {
                    let source = promote_to_task(input, &catalog);
                    graph.add_edge(source, target.clone(), TransformType::Direct);
                }
            }
        }

        // Declared `inputs`: stitch a direct edge per matched column.
        for input in &task.inputs {
            wire_input(&consumer, input, &catalog, &mut graph, &mut unresolved);
        }
    }

    if dag.lineage_strict && !unresolved.is_empty() {
        return Err(LineageStrictError {
            dag_id: dag.id.clone(),
            unresolved,
        });
    }

    for u in &unresolved {
        warn!(
            consumer = %u.consumer,
            dataset = %u.dataset,
            column = %u.column,
            reason = ?u.reason,
            "cross-task lineage: unresolved column reference (run with lineage_strict=true to fail compile)",
        );
    }

    Ok(CrossTaskLineage { graph, unresolved })
}

fn register_outputs(dag_id: &str, task: &Task, catalog: &mut TableCatalog) {
    let task_ref = TaskRef::new(dag_id, &task.id);
    for ds in &task.outputs {
        let columns: Vec<CatalogColumn> = ds
            .columns
            .iter()
            .map(|c| CatalogColumn::new(&c.name, ColumnType::Unknown))
            .collect();
        catalog.register_dataset(&ds.name, columns, task_ref.clone());
    }
}

/// Promote a `ColumnRef` emitted by the SQL extractor (always
/// `ColumnSource::Table`) into a task-scoped ref if the catalog knows
/// which task produces it. Leaves unknown tables untouched so the graph
/// still surfaces "this column came from table X" for physical tables.
fn promote_to_task(input: &ColumnRef, catalog: &TableCatalog) -> ColumnRef {
    if let ColumnSource::Table(name) = &input.source {
        if let Some(producer) = catalog.lookup_producer(name) {
            return ColumnRef::task(producer.clone(), &input.column_name);
        }
    }
    input.clone()
}

fn wire_input(
    consumer: &TaskRef,
    input: &Dataset,
    catalog: &TableCatalog,
    graph: &mut LineageGraph,
    unresolved: &mut Vec<UnresolvedRef>,
) {
    let Some(producer) = catalog.lookup_producer(&input.name) else {
        // Either a physical table (fine, leave it as a table-rooted edge)
        // or an unknown name (record).
        if catalog.lookup_via_qualified(&input.name).is_none() {
            for col in &input.columns {
                unresolved.push(UnresolvedRef {
                    consumer: consumer.clone(),
                    dataset: input.name.clone(),
                    column: col.name.clone(),
                    reason: UnresolvedReason::DatasetNotProduced,
                });
            }
        } else {
            // Physical table: emit a table → task edge per declared column.
            for col in &input.columns {
                let src = ColumnRef::table(&input.name, &col.name);
                let dst = ColumnRef::task(consumer.clone(), &col.name);
                graph.add_edge(src, dst, TransformType::Direct);
            }
        }
        return;
    };

    let producer_cols: std::collections::HashSet<String> = catalog
        .lookup_via_qualified(&input.name)
        .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
        .unwrap_or_default();

    for col in &input.columns {
        let name_lc = col.name.to_lowercase();
        if !producer_cols.is_empty() && !producer_cols.contains(&name_lc) {
            unresolved.push(UnresolvedRef {
                consumer: consumer.clone(),
                dataset: input.name.clone(),
                column: col.name.clone(),
                reason: UnresolvedReason::ColumnNotDeclared,
            });
            continue;
        }
        let src = ColumnRef::task(producer.clone(), &col.name);
        let dst = ColumnRef::task(consumer.clone(), &col.name);
        graph.add_edge(src, dst, TransformType::Direct);
    }

    // Suppress unused warning when no inputs.
    let _ = HashMap::<String, String>::new();
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_common::dag::{
        ColumnSpec, DependencyType, ResourceLimits, Task, TaskDependency, TaskType, TriggerRule,
    };
    use std::collections::HashMap;

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
                connection: "local".to_string(),
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

    fn dag_of(id: &str, lineage_strict: bool, tasks: Vec<Task>, order: Vec<&str>) -> Dag {
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
            compiled_at: chrono::Utc::now(),
            catchup: false,
            max_catchup_runs: None,
            lineage_strict,
        }
    }

    /// Py extract → SQL transform → Py load — the headline three-task chain.
    #[test]
    fn stitches_python_sql_python_chain() {
        let extract = py_task(
            "extract_orders",
            vec![Dataset::new(
                "staging.orders",
                vec![ColumnSpec::new("id"), ColumnSpec::new("amount")],
            )],
            vec![],
            vec![],
        );

        let transform = sql_task(
            "transform",
            "INSERT INTO analytics.daily_revenue SELECT id, amount FROM staging.orders",
            None,
            vec![Dataset::new(
                "analytics.daily_revenue",
                vec![ColumnSpec::new("id"), ColumnSpec::new("amount")],
            )],
            vec!["extract_orders"],
        );

        let load = py_task(
            "push_to_warehouse",
            vec![],
            vec![Dataset::new(
                "analytics.daily_revenue",
                vec![ColumnSpec::new("id"), ColumnSpec::new("amount")],
            )],
            vec!["transform"],
        );

        let dag = dag_of(
            "demo",
            false,
            vec![extract, transform, load],
            vec!["extract_orders", "transform", "push_to_warehouse"],
        );

        let result = stitch(&dag).expect("non-strict stitch should not error");
        assert!(
            result.unresolved.is_empty(),
            "unresolved: {:?}",
            result.unresolved
        );

        // Walk upstream from push_to_warehouse.amount and assert the chain.
        let target = ColumnRef::task(TaskRef::new("demo", "push_to_warehouse"), "amount");
        let trace = result.graph.trace_upstream(&target);
        let qualifiers: Vec<String> = trace.columns.iter().map(|c| c.qualifier()).collect();
        assert!(
            qualifiers.iter().any(|q| q == "demo::transform"),
            "missing transform: {:?}",
            qualifiers
        );
        assert!(
            qualifiers.iter().any(|q| q == "demo::extract_orders"),
            "missing extract: {:?}",
            qualifiers
        );
    }

    #[test]
    fn strict_mode_errors_on_undeclared_column() {
        let extract = py_task(
            "p",
            vec![Dataset::new(
                "staging.orders",
                vec![ColumnSpec::new("id")], // amount NOT declared
            )],
            vec![],
            vec![],
        );
        let consumer = py_task(
            "c",
            vec![],
            vec![Dataset::new(
                "staging.orders",
                vec![ColumnSpec::new("id"), ColumnSpec::new("amount")],
            )],
            vec!["p"],
        );

        let dag = dag_of("d", true, vec![extract, consumer], vec!["p", "c"]);
        let err = stitch(&dag).expect_err("strict mode should reject");
        assert_eq!(err.unresolved.len(), 1);
        assert_eq!(err.unresolved[0].column, "amount");
        assert_eq!(
            err.unresolved[0].reason,
            UnresolvedReason::ColumnNotDeclared
        );
    }

    #[test]
    fn non_strict_records_unresolved_without_error() {
        let consumer = py_task(
            "c",
            vec![],
            vec![Dataset::new("ghost", vec![ColumnSpec::new("nope")])],
            vec![],
        );
        let dag = dag_of("d", false, vec![consumer], vec!["c"]);
        let result = stitch(&dag).expect("non-strict should not error");
        assert_eq!(result.unresolved.len(), 1);
        assert_eq!(
            result.unresolved[0].reason,
            UnresolvedReason::DatasetNotProduced
        );
    }

    #[test]
    fn topo_order_independence() {
        // Same DAG, different topo orders that are both valid → identical
        // edge sets (modulo iteration order).
        let mk = || {
            let a = py_task(
                "a",
                vec![Dataset::new("ds.a", vec![ColumnSpec::new("x")])],
                vec![],
                vec![],
            );
            let b = py_task(
                "b",
                vec![Dataset::new("ds.b", vec![ColumnSpec::new("x")])],
                vec![],
                vec![],
            );
            let c = py_task(
                "c",
                vec![],
                vec![
                    Dataset::new("ds.a", vec![ColumnSpec::new("x")]),
                    Dataset::new("ds.b", vec![ColumnSpec::new("x")]),
                ],
                vec!["a", "b"],
            );
            (a, b, c)
        };

        let (a1, b1, c1) = mk();
        let (a2, b2, c2) = mk();

        let dag1 = dag_of("d", false, vec![a1, b1, c1], vec!["a", "b", "c"]);
        let dag2 = dag_of("d", false, vec![b2, a2, c2], vec!["b", "a", "c"]);

        let r1 = stitch(&dag1).unwrap();
        let r2 = stitch(&dag2).unwrap();
        assert_eq!(r1.graph.edge_count(), r2.graph.edge_count());
        assert_eq!(r1.graph.column_count(), r2.graph.column_count());
    }

    #[test]
    fn sql_extractor_promotes_table_to_task() {
        // SQL extractor sees `FROM staging.orders` and emits a
        // table-scoped ColumnRef. After stitching, the edge should be
        // task-scoped because the catalog knows extract_orders produces
        // staging.orders.
        let extract = py_task(
            "extract_orders",
            vec![Dataset::new(
                "staging.orders",
                vec![ColumnSpec::new("amount")],
            )],
            vec![],
            vec![],
        );
        let transform = sql_task(
            "transform",
            "SELECT amount FROM staging.orders",
            Some("analytics.x"),
            vec![],
            vec!["extract_orders"],
        );

        let dag = dag_of(
            "demo",
            false,
            vec![extract, transform],
            vec!["extract_orders", "transform"],
        );
        let result = stitch(&dag).unwrap();

        let any_task_edge = result.graph.all_edges().iter().any(
            |e| matches!(&e.source.source, ColumnSource::Task(t) if t.task_id == "extract_orders"),
        );
        assert!(
            any_task_edge,
            "expected an edge sourced at the producer task"
        );
    }
}
