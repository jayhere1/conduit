//! SQL task input/output inference.
//!
//! Runs after dependency resolution: for each [`TaskType::Sql`] task we
//! parse the query with [`SqlLineageExtractor`] and fill in
//! [`Task::inputs`] / [`Task::outputs`] so cross-task lineage stitching
//! has a uniform model to work with.
//!
//! Resolution order for the *output* dataset name (most → least specific):
//!   1. AST-derived target — `INSERT INTO …` or `CREATE TABLE … AS`.
//!   2. YAML / SDK-declared `target:` field on the SQL task.
//!   3. Fallback to the task id, marked as anonymous (downstream SQL
//!      `FROM` clauses won't resolve it, but task-graph consumers can).
//!
//! Sentinel columns emitted by the SQL extractor (`__where__`, `*`) are
//! filtered out of the materialised dataset.

use conduit_common::dag::{ColumnSpec, Dag, Dataset, Task, TaskType};
use conduit_lineage::sql_parser::{SqlLineage, SqlLineageExtractor, TableRef};

/// Walk every SQL task in `dag` and populate its `inputs` / `outputs`.
///
/// Tasks that already have non-empty `inputs` / `outputs` (e.g. Python
/// tasks with declarative decorators) are left alone — explicit
/// declarations win over inference.
pub fn infer_sql_io(dag: &mut Dag) {
    for task in dag.tasks.values_mut() {
        if !matches!(task.task_type, TaskType::Sql { .. }) {
            continue;
        }
        infer_for_task(task);
    }
}

fn infer_for_task(task: &mut Task) {
    let (query, declared_target) = match &task.task_type {
        TaskType::Sql { query, target, .. } => (query.as_str(), target.clone()),
        _ => return,
    };

    let lineage = SqlLineageExtractor::extract(query);

    if task.inputs.is_empty() {
        task.inputs = inputs_from_sources(&lineage);
    }
    if task.outputs.is_empty() {
        task.outputs = vec![output_dataset(
            &task.id,
            declared_target.as_deref(),
            &lineage,
        )];
    }
}

fn inputs_from_sources(lineage: &SqlLineage) -> Vec<Dataset> {
    let mut out = Vec::new();
    for tref in &lineage.source_tables {
        // Skip Jinja placeholder pseudo-tables.
        if tref.name.starts_with("__conduit_jinja_") {
            continue;
        }
        let name = qualified_name(tref);
        // Avoid duplicates when the same table appears under multiple aliases.
        if !out.iter().any(|d: &Dataset| d.name == name) {
            out.push(Dataset::new(name, Vec::new()));
        }
    }
    out
}

fn output_dataset(task_id: &str, declared: Option<&str>, lineage: &SqlLineage) -> Dataset {
    let columns = output_columns(lineage);

    if let Some(t) = declared {
        return Dataset::new(t.to_string(), columns);
    }
    if let Some(tref) = &lineage.target_table {
        return Dataset::new(qualified_name(tref), columns);
    }

    Dataset {
        name: task_id.to_string(),
        columns,
        anonymous: true,
    }
}

fn output_columns(lineage: &SqlLineage) -> Vec<ColumnSpec> {
    lineage
        .output_columns
        .iter()
        .filter(|c| c.name != "*" && c.name != "__where__")
        .map(|c| ColumnSpec::new(&c.name))
        .collect()
}

fn qualified_name(tref: &TableRef) -> String {
    match tref.schema.as_deref() {
        Some(s) if !s.is_empty() => format!("{}.{}", s, tref.name),
        _ => tref.name.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_common::dag::{
        DependencyType, ResourceLimits, Task, TaskDependency, TaskType, TriggerRule,
    };
    use std::collections::HashMap;

    fn sql_task(id: &str, query: &str, target: Option<&str>) -> Task {
        Task {
            id: id.to_string(),
            task_type: TaskType::Sql {
                connection: "local".to_string(),
                query: query.to_string(),
                target: target.map(String::from),
            },
            dependencies: Vec::new(),
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
            outputs: Vec::new(),
        }
    }

    fn dag_of(tasks: Vec<Task>) -> Dag {
        let mut map = HashMap::new();
        for t in tasks {
            map.insert(t.id.clone(), t);
        }
        Dag {
            id: "d".to_string(),
            description: None,
            schedule: None,
            tags: vec![],
            max_active_runs: 1,
            on_failure: None,
            tasks: map,
            execution_order: vec![],
            source_file: String::new(),
            compiled_at: chrono::Utc::now(),
            catchup: false,
            max_catchup_runs: None,
            lineage_strict: false,
        }
    }

    #[test]
    fn infers_from_insert_into() {
        let task = sql_task(
            "t",
            "INSERT INTO analytics.daily_revenue SELECT customer_id, amount FROM staging.orders",
            None,
        );
        let mut dag = dag_of(vec![task]);
        infer_sql_io(&mut dag);
        let t = &dag.tasks["t"];
        assert_eq!(t.inputs.len(), 1);
        assert_eq!(t.inputs[0].name, "staging.orders");
        assert_eq!(t.outputs.len(), 1);
        assert_eq!(t.outputs[0].name, "analytics.daily_revenue");
        assert!(!t.outputs[0].anonymous);
        let cols: Vec<&str> = t.outputs[0]
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(cols, vec!["customer_id", "amount"]);
    }

    #[test]
    fn infers_from_ctas() {
        let task = sql_task(
            "t",
            "CREATE OR REPLACE TABLE raw_events AS SELECT id, ts FROM source.events",
            None,
        );
        let mut dag = dag_of(vec![task]);
        infer_sql_io(&mut dag);
        let t = &dag.tasks["t"];
        assert_eq!(t.outputs[0].name, "raw_events");
        assert!(!t.outputs[0].anonymous);
    }

    #[test]
    fn declared_target_wins_over_inferred() {
        let task = sql_task(
            "t",
            "INSERT INTO analytics.daily_revenue SELECT a FROM x",
            Some("custom.override"),
        );
        let mut dag = dag_of(vec![task]);
        infer_sql_io(&mut dag);
        assert_eq!(dag.tasks["t"].outputs[0].name, "custom.override");
    }

    #[test]
    fn plain_select_falls_back_to_anonymous() {
        let task = sql_task("aggregate", "SELECT id, COUNT(*) AS n FROM orders", None);
        let mut dag = dag_of(vec![task]);
        infer_sql_io(&mut dag);
        let out = &dag.tasks["aggregate"].outputs[0];
        assert_eq!(out.name, "aggregate");
        assert!(out.anonymous);
        let cols: Vec<&str> = out.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(cols, vec!["id", "n"]);
    }

    #[test]
    fn explicit_declarations_are_preserved() {
        let mut task = sql_task("t", "SELECT a FROM x", None);
        task.outputs = vec![Dataset::new(
            "preset.dataset",
            vec![ColumnSpec::new("manual")],
        )];
        task.inputs = vec![Dataset::new("preset.input", Vec::new())];
        let mut dag = dag_of(vec![task]);
        infer_sql_io(&mut dag);
        let t = &dag.tasks["t"];
        assert_eq!(t.inputs[0].name, "preset.input");
        assert_eq!(t.outputs[0].name, "preset.dataset");
        assert_eq!(t.outputs[0].columns[0].name, "manual");
    }

    #[test]
    fn deduplicates_repeated_source_tables() {
        let task = sql_task(
            "t",
            "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id",
            None,
        );
        let mut dag = dag_of(vec![task]);
        infer_sql_io(&mut dag);
        assert_eq!(dag.tasks["t"].inputs.len(), 1);
        assert_eq!(dag.tasks["t"].inputs[0].name, "orders");
    }

    // Silence unused import warning when run in isolation.
    #[allow(dead_code)]
    fn _imports() -> (DependencyType, TaskDependency) {
        let d = TaskDependency {
            task_id: String::new(),
            dependency_type: DependencyType::ExecutionOrder,
        };
        (DependencyType::ExecutionOrder, d)
    }
}
