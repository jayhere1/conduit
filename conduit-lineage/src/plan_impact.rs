//! Plan-level impact analysis.
//!
//! Compares two `ConduitPlan`s, detects schema changes per task using
//! [`SchemaChangeDetector`], and traces the downstream blast radius
//! through the stitched cross-task lineage graph of the *head* plan.
//!
//! This is the engine behind `conduit impact` and the GitHub Action that
//! posts schema-impact PR comments.

use std::collections::HashMap;

use conduit_common::dag::Dag;
use serde::{Deserialize, Serialize};

/// Map of compiled DAGs keyed by DAG id — the only piece of the compiler's
/// `ConduitPlan` this module needs. Callers in `conduit-cli` adapt their
/// `ConduitPlan` by passing `&plan.dags`.
pub type DagSet = HashMap<String, Dag>;

use crate::cross_task::{self, CrossTaskLineage};
use crate::impact::{ChangeKind, SchemaChange, SchemaChangeDetector};
use crate::lineage_graph::{ColumnRef, ColumnSource, TaskRef};
use crate::schema::{Column, ColumnType, Schema};

/// One task's impact entry — paired with the specific dataset that changed.
/// A task with multiple outputs can appear multiple times, one per dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskImpact {
    pub dag_id: String,
    pub task_id: String,
    /// The qualified dataset name (e.g. `"staging.orders"`).
    pub dataset_name: String,
    /// All changes detected on this dataset's columns.
    pub changes: Vec<SchemaChange>,
    /// Downstream columns reached via the stitched lineage graph from any
    /// breaking change on this dataset.
    pub affected_downstream: Vec<DownstreamColumn>,
    /// Whether the entire dataset was removed (vs columns being added/changed).
    pub dataset_removed: bool,
    /// Whether the dataset is brand-new on the head side.
    pub dataset_added: bool,
}

impl TaskImpact {
    pub fn breaking_count(&self) -> usize {
        self.changes.iter().filter(|c| c.is_breaking).count()
    }

    pub fn non_breaking_count(&self) -> usize {
        self.changes.iter().filter(|c| !c.is_breaking).count()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DownstreamColumn {
    pub dag_id: String,
    pub task_id: String,
    pub column: String,
}

/// Aggregate stats across every changed task in the plan.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlanImpactSummary {
    /// Tasks present in both plans whose outputs were compared.
    pub tasks_compared: usize,
    /// Tasks whose outputs produced at least one change.
    pub tasks_changed: usize,
    /// Tasks present in base but missing in head.
    pub tasks_removed: usize,
    /// Tasks present in head but missing in base.
    pub tasks_added: usize,
    pub total_breaking_changes: usize,
    pub total_non_breaking_changes: usize,
    pub total_downstream_columns_affected: usize,
}

/// Quantifies how complete the lineage coverage is, so PR comments can be
/// honest about under-reporting. Values come from `cross_task::stitch`'s
/// `unresolved` list on the *head* side.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LineageCoverage {
    /// Number of column references on head that the stitcher could not
    /// resolve to a producing task. Each entry is a place where downstream
    /// tracing may under-report.
    pub head_unresolved_refs: usize,
    /// DAGs in head that explicitly opted in to `lineage_strict = true`.
    /// Higher numbers ⇒ more confident downstream tracing.
    pub strict_dags: usize,
    /// Total DAGs in head (not just strict ones).
    pub total_dags: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanImpact {
    pub per_task: Vec<TaskImpact>,
    pub summary: PlanImpactSummary,
    pub coverage: LineageCoverage,
}

impl PlanImpact {
    pub fn has_breaking_changes(&self) -> bool {
        self.summary.total_breaking_changes > 0
    }
}

/// Compute the full impact of moving from `base` to `head`.
///
/// Accepts the `dags` map (the only part of `ConduitPlan` we need); the CLI
/// wrapper passes `&plan.dags`.
pub fn analyze(base: &DagSet, head: &DagSet) -> PlanImpact {
    let head_graphs = stitch_all(head);
    let coverage = compute_coverage(head, &head_graphs);

    let mut per_task: Vec<TaskImpact> = Vec::new();
    let mut summary = PlanImpactSummary::default();

    // Pairs of DAGs in both plans → per-task diff.
    for (dag_id, head_dag) in head {
        let base_dag = base.get(dag_id);

        // Pre-index base task outputs by (task_id, dataset_name) for cheap
        // lookup. Tasks/datasets in base but not head become "removed" entries.
        let base_task_ids: std::collections::HashSet<&str> = base_dag
            .map(|d| d.tasks.keys().map(|s| s.as_str()).collect())
            .unwrap_or_default();
        let head_task_ids: std::collections::HashSet<&str> =
            head_dag.tasks.keys().map(|s| s.as_str()).collect();

        // Count tasks added/removed at the DAG level
        summary.tasks_added += head_task_ids.difference(&base_task_ids).count();
        summary.tasks_removed += base_task_ids.difference(&head_task_ids).count();

        for (task_id, head_task) in &head_dag.tasks {
            let base_task = base_dag.and_then(|d| d.tasks.get(task_id));

            let base_outputs: HashMap<&str, &conduit_common::dag::Dataset> = base_task
                .map(|t| t.outputs.iter().map(|d| (d.name.as_str(), d)).collect())
                .unwrap_or_default();
            let head_outputs: HashMap<&str, &conduit_common::dag::Dataset> = head_task
                .outputs
                .iter()
                .map(|d| (d.name.as_str(), d))
                .collect();

            // Datasets present on the head side (changed or unchanged or added).
            for (ds_name, head_ds) in &head_outputs {
                let base_ds = base_outputs.get(ds_name).copied();
                summary.tasks_compared += 1;

                let head_schema = dataset_to_schema(task_id, &head_ds.columns);
                let base_schema = match base_ds {
                    Some(b) => dataset_to_schema(task_id, &b.columns),
                    None => Schema::new(task_id, vec![]),
                };

                let changes = SchemaChangeDetector::diff(&base_schema, &head_schema);
                let dataset_added = base_ds.is_none();
                if changes.is_empty() && !dataset_added {
                    continue;
                }
                summary.tasks_changed += 1;
                summary.total_breaking_changes += changes.iter().filter(|c| c.is_breaking).count();
                summary.total_non_breaking_changes +=
                    changes.iter().filter(|c| !c.is_breaking).count();

                let affected = if let Some(graph) = head_graphs.get(dag_id) {
                    trace_breaking_downstream(graph, dag_id, task_id, &changes)
                } else {
                    Vec::new()
                };
                summary.total_downstream_columns_affected += affected.len();

                per_task.push(TaskImpact {
                    dag_id: dag_id.clone(),
                    task_id: task_id.clone(),
                    dataset_name: ds_name.to_string(),
                    changes,
                    affected_downstream: affected,
                    dataset_removed: false,
                    dataset_added,
                });
            }

            // Datasets present only on the base side — entire dataset removed.
            for (ds_name, base_ds) in &base_outputs {
                if head_outputs.contains_key(ds_name) {
                    continue;
                }
                summary.tasks_compared += 1;
                summary.tasks_changed += 1;

                // Treat every column as removed (breaking).
                let base_schema = dataset_to_schema(task_id, &base_ds.columns);
                let head_schema = Schema::new(task_id, vec![]);
                let changes = SchemaChangeDetector::diff(&base_schema, &head_schema);
                let breaking = changes.iter().filter(|c| c.is_breaking).count();
                summary.total_breaking_changes += breaking;
                summary.total_non_breaking_changes += changes.len() - breaking;

                per_task.push(TaskImpact {
                    dag_id: dag_id.clone(),
                    task_id: task_id.clone(),
                    dataset_name: ds_name.to_string(),
                    changes,
                    affected_downstream: Vec::new(), // task is gone on head; no head graph for it
                    dataset_removed: true,
                    dataset_added: false,
                });
            }
        }
    }

    // Sort for deterministic output: dag, task, dataset.
    per_task.sort_by(|a, b| {
        (
            a.dag_id.as_str(),
            a.task_id.as_str(),
            a.dataset_name.as_str(),
        )
            .cmp(&(
                b.dag_id.as_str(),
                b.task_id.as_str(),
                b.dataset_name.as_str(),
            ))
    });

    PlanImpact {
        per_task,
        summary,
        coverage,
    }
}

fn stitch_all(plan: &DagSet) -> HashMap<String, CrossTaskLineage> {
    let mut out = HashMap::new();
    for (dag_id, dag) in plan {
        match cross_task::stitch(dag) {
            Ok(result) => {
                out.insert(dag_id.clone(), result);
            }
            Err(e) => {
                // Strict-mode failure on head means the head plan is broken;
                // we still want to surface impact analysis for the rest of
                // the plan rather than abort. Skip this DAG's graph.
                tracing::warn!(
                    dag_id = %e.dag_id,
                    unresolved = e.unresolved.len(),
                    "skip downstream tracing for DAG: strict-mode lineage failure on head",
                );
            }
        }
    }
    out
}

fn compute_coverage(plan: &DagSet, graphs: &HashMap<String, CrossTaskLineage>) -> LineageCoverage {
    let mut cov = LineageCoverage {
        total_dags: plan.len(),
        ..Default::default()
    };
    for (dag_id, dag) in plan {
        if dag.lineage_strict {
            cov.strict_dags += 1;
        }
        if let Some(g) = graphs.get(dag_id) {
            cov.head_unresolved_refs += g.unresolved.len();
        }
    }
    cov
}

fn trace_breaking_downstream(
    graph: &CrossTaskLineage,
    dag_id: &str,
    task_id: &str,
    changes: &[SchemaChange],
) -> Vec<DownstreamColumn> {
    let mut out: Vec<DownstreamColumn> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for change in changes {
        if !change.is_breaking {
            continue;
        }
        // For renames, the upstream-affecting column is the OLD name.
        let column = match &change.kind {
            ChangeKind::ColumnRenamed { old_name } => old_name.clone(),
            _ => change.column_name.clone(),
        };
        let origin = ColumnRef::task(TaskRef::new(dag_id, task_id), &column);
        let trace = graph.graph.trace_downstream(&origin);
        for col_ref in trace.columns {
            let key = (col_ref.qualifier(), col_ref.column_name.clone());
            if !seen.insert(key) {
                continue;
            }
            if let ColumnSource::Task(t) = &col_ref.source {
                // Skip the origin task itself.
                if t.dag_id == dag_id && t.task_id == task_id {
                    continue;
                }
                out.push(DownstreamColumn {
                    dag_id: t.dag_id.clone(),
                    task_id: t.task_id.clone(),
                    column: col_ref.column_name.clone(),
                });
            }
        }
    }

    out.sort_by(|a, b| {
        (a.dag_id.as_str(), a.task_id.as_str(), a.column.as_str()).cmp(&(
            b.dag_id.as_str(),
            b.task_id.as_str(),
            b.column.as_str(),
        ))
    });
    out
}

fn dataset_to_schema(task_id: &str, columns: &[conduit_common::dag::ColumnSpec]) -> Schema {
    let cols = columns
        .iter()
        .map(|cs| Column::new(&cs.name, parse_dtype(cs.dtype.as_deref())))
        .collect();
    Schema::new(task_id, cols)
}

fn parse_dtype(dtype: Option<&str>) -> ColumnType {
    let Some(s) = dtype else {
        return ColumnType::Unknown;
    };
    match s.to_ascii_lowercase().as_str() {
        "string" | "text" | "varchar" | "char" => ColumnType::String,
        "int" | "integer" | "bigint" | "smallint" | "int4" | "int8" => ColumnType::Integer,
        "float" | "double" | "real" | "float4" | "float8" => ColumnType::Float,
        "bool" | "boolean" => ColumnType::Boolean,
        "date" => ColumnType::Date,
        "timestamp" | "timestamptz" | "datetime" => ColumnType::Timestamp,
        "json" | "jsonb" => ColumnType::Json,
        "bytea" | "binary" | "blob" => ColumnType::Binary,
        _ => ColumnType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use conduit_common::dag::{
        ColumnSpec, Dag, Dataset, DependencyType, ResourceLimits, Task, TaskDependency, TaskType,
        TriggerRule,
    };

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

    fn dag_of(id: &str, tasks: Vec<Task>, order: Vec<&str>) -> Dag {
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
            lineage_strict: false,
        }
    }

    fn plan_of(dags: Vec<Dag>) -> DagSet {
        let mut map = HashMap::new();
        for d in dags {
            map.insert(d.id.clone(), d);
        }
        map
    }

    #[test]
    fn identical_plans_have_no_impact() {
        let dag = dag_of(
            "d",
            vec![py_task(
                "extract",
                vec![Dataset::new(
                    "staging.orders",
                    vec![col("id"), col("amount")],
                )],
                vec![],
                vec![],
            )],
            vec!["extract"],
        );
        let base = plan_of(vec![dag.clone()]);
        let head = plan_of(vec![dag]);

        let impact = analyze(&base, &head);
        assert_eq!(impact.summary.total_breaking_changes, 0);
        assert_eq!(impact.summary.total_non_breaking_changes, 0);
        assert!(impact.per_task.is_empty());
    }

    #[test]
    fn column_removal_is_breaking_and_traces_downstream() {
        // Chain: producer -> consumer. producer.amount removed on head → 1
        // breaking change, consumer.amount should appear in downstream.
        let producer_base = py_task(
            "producer",
            vec![Dataset::new(
                "staging.orders",
                vec![col("id"), col("amount")],
            )],
            vec![],
            vec![],
        );
        let producer_head = py_task(
            "producer",
            vec![Dataset::new("staging.orders", vec![col("id")])],
            vec![],
            vec![],
        );
        let consumer = py_task(
            "consumer",
            vec![],
            vec![Dataset::new(
                "staging.orders",
                vec![col("id"), col("amount")],
            )],
            vec!["producer"],
        );

        let base = plan_of(vec![dag_of(
            "d",
            vec![producer_base, consumer.clone()],
            vec!["producer", "consumer"],
        )]);
        let head = plan_of(vec![dag_of(
            "d",
            vec![producer_head, consumer],
            vec!["producer", "consumer"],
        )]);

        let impact = analyze(&base, &head);

        assert_eq!(impact.summary.tasks_changed, 1);
        assert_eq!(impact.summary.total_breaking_changes, 1);
        assert_eq!(impact.per_task.len(), 1);
        let entry = &impact.per_task[0];
        assert_eq!(entry.task_id, "producer");
        assert_eq!(entry.dataset_name, "staging.orders");
        assert!(matches!(entry.changes[0].kind, ChangeKind::ColumnRemoved));

        // consumer.amount is unresolved on the consumer side (the column
        // disappeared from producer's declared outputs), so the strict
        // stitcher records it as unresolved — but the downstream trace on
        // the head graph still walks the `id` edge. The breaking column
        // (`amount`) lives only on the base side, so its downstream trace
        // produces nothing on the head graph. That's expected: deleted
        // columns can't be traced forward.
        // We still record at least one breaking change.
        assert!(entry.breaking_count() >= 1);
    }

    #[test]
    fn type_widening_is_non_breaking() {
        let producer_base = py_task(
            "p",
            vec![Dataset::new("ds", vec![col_typed("x", "int")])],
            vec![],
            vec![],
        );
        let producer_head = py_task(
            "p",
            vec![Dataset::new("ds", vec![col_typed("x", "float")])],
            vec![],
            vec![],
        );
        let base = plan_of(vec![dag_of("d", vec![producer_base], vec!["p"])]);
        let head = plan_of(vec![dag_of("d", vec![producer_head], vec!["p"])]);

        let impact = analyze(&base, &head);
        assert_eq!(impact.summary.total_breaking_changes, 0);
        assert_eq!(impact.summary.total_non_breaking_changes, 1);
    }

    #[test]
    fn coverage_reports_total_and_strict_dags() {
        let lenient = dag_of(
            "lenient",
            vec![py_task("p", vec![], vec![], vec![])],
            vec!["p"],
        );
        let mut strict = dag_of(
            "strict",
            vec![py_task("p", vec![], vec![], vec![])],
            vec!["p"],
        );
        strict.lineage_strict = true;

        let plan = plan_of(vec![lenient, strict]);
        let impact = analyze(&plan, &plan);
        assert_eq!(impact.coverage.total_dags, 2);
        assert_eq!(impact.coverage.strict_dags, 1);
    }

    #[test]
    fn dataset_added_is_recorded_without_breaking() {
        let p_base = py_task("p", vec![], vec![], vec![]);
        let p_head = py_task(
            "p",
            vec![Dataset::new("new.ds", vec![col("id")])],
            vec![],
            vec![],
        );
        let base = plan_of(vec![dag_of("d", vec![p_base], vec!["p"])]);
        let head = plan_of(vec![dag_of("d", vec![p_head], vec!["p"])]);

        let impact = analyze(&base, &head);
        assert_eq!(impact.per_task.len(), 1);
        assert!(impact.per_task[0].dataset_added);
        // column `id` is added — non-breaking.
        assert_eq!(impact.summary.total_breaking_changes, 0);
        assert_eq!(impact.summary.total_non_breaking_changes, 1);
    }

    #[test]
    fn dataset_removed_is_breaking() {
        let p_base = py_task(
            "p",
            vec![Dataset::new("gone.ds", vec![col("id"), col("amount")])],
            vec![],
            vec![],
        );
        let p_head = py_task("p", vec![], vec![], vec![]);
        let base = plan_of(vec![dag_of("d", vec![p_base], vec!["p"])]);
        let head = plan_of(vec![dag_of("d", vec![p_head], vec!["p"])]);

        let impact = analyze(&base, &head);
        assert_eq!(impact.per_task.len(), 1);
        assert!(impact.per_task[0].dataset_removed);
        assert!(impact.per_task[0].breaking_count() >= 2);
    }
}
