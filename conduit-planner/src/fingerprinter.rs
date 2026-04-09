//! Fingerprint computation for an entire compiled plan.
//!
//! Walks the DAG in topological order so each task's fingerprint
//! incorporates its upstream fingerprints — a change at the root
//! cascades through every downstream task automatically.

use std::collections::{BTreeMap, HashMap};

use conduit_common::dag::{Dag, DagId, TaskId};
use conduit_common::fingerprint::Fingerprint;
use conduit_compiler::ConduitPlan;

/// A map of (dag_id, task_id) -> computed fingerprint.
pub type FingerprintMap = HashMap<(DagId, TaskId), Fingerprint>;

/// Computes fingerprints for every task in a ConduitPlan.
pub struct PlanFingerprinter;

impl PlanFingerprinter {
    /// Compute fingerprints for every task across all DAGs in the plan.
    ///
    /// Walks each DAG in execution_order (topological order) so that
    /// upstream fingerprints are available when computing downstream ones.
    pub fn fingerprint_plan(plan: &ConduitPlan) -> FingerprintMap {
        let mut result = FingerprintMap::new();

        for (dag_id, dag) in &plan.dags {
            let dag_fps = Self::fingerprint_dag(dag);
            for (task_id, fp) in dag_fps {
                result.insert((dag_id.clone(), task_id), fp);
            }
        }

        result
    }

    /// Compute fingerprints for all tasks in a single DAG.
    ///
    /// Because we walk in execution_order (topological sort), every
    /// task's upstream fingerprints are already computed when we reach it.
    pub fn fingerprint_dag(dag: &Dag) -> HashMap<TaskId, Fingerprint> {
        let mut fingerprints: HashMap<TaskId, Fingerprint> = HashMap::new();

        for task_id in &dag.execution_order {
            let task = match dag.tasks.get(task_id) {
                Some(t) => t,
                None => continue,
            };

            // 1. Task content: the "what" — source code, query, command
            let task_content = Self::task_content_key(task);

            // 2. Task config: the "how" — retries, pool, timeout, trigger rule, resources
            let task_config = Self::task_config_key(task);

            // 3. Upstream fingerprints: sorted by task_id for determinism
            let mut upstream_fps = BTreeMap::new();
            for dep in &task.dependencies {
                if let Some(fp) = fingerprints.get(&dep.task_id) {
                    upstream_fps.insert(dep.task_id.clone(), fp.clone());
                }
            }

            let fp = Fingerprint::compute(&task_content, &task_config, &upstream_fps);
            fingerprints.insert(task_id.clone(), fp);
        }

        fingerprints
    }

    /// Extract a deterministic content key from a task's type.
    ///
    /// This captures "what the task does" — if the SQL query changes,
    /// the Python function changes, or the bash command changes,
    /// the fingerprint changes.
    fn task_content_key(task: &conduit_common::dag::Task) -> String {
        use conduit_common::dag::TaskType;
        match &task.task_type {
            TaskType::Python { module, function } => {
                format!("python:{}:{}", module, function)
            }
            TaskType::Bash { command } => {
                format!("bash:{}", command)
            }
            TaskType::Sql { connection, query } => {
                format!("sql:{}:{}", connection, query)
            }
            TaskType::Sensor {
                sensor_type,
                poke_interval,
            } => {
                format!(
                    "sensor:{}:{}",
                    sensor_type,
                    poke_interval.as_deref().unwrap_or("default")
                )
            }
            TaskType::Executable { command, args } => {
                format!("exec:{}:{}", command, args.join(","))
            }
        }
    }

    /// Extract a deterministic config key from a task's settings.
    fn task_config_key(task: &conduit_common::dag::Task) -> String {
        format!(
            "retries={},retry_delay={},pool={},timeout={},priority={},trigger={:?},cpu={},mem={}",
            task.retries,
            task.retry_delay.as_deref().unwrap_or("none"),
            task.pool.as_deref().unwrap_or("default"),
            task.timeout.as_deref().unwrap_or("none"),
            task.priority,
            task.trigger_rule,
            task.resources.cpu_millicores.unwrap_or(0),
            task.resources.memory_mb.unwrap_or(0),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use conduit_common::dag::*;
    use std::collections::HashMap;

    fn make_task(id: &str, deps: Vec<&str>) -> Task {
        Task {
            id: id.to_string(),
            task_type: TaskType::Python {
                module: "mod".to_string(),
                function: id.to_string(),
            },
            dependencies: deps
                .into_iter()
                .map(|d| TaskDependency {
                    task_id: d.to_string(),
                    dependency_type: DependencyType::ExecutionOrder,
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
        }
    }

    fn make_dag(id: &str, tasks: Vec<Task>, order: Vec<&str>) -> Dag {
        let mut task_map = HashMap::new();
        for t in tasks {
            task_map.insert(t.id.clone(), t);
        }
        Dag {
            id: id.to_string(),
            description: None,
            schedule: None,
            tags: vec![],
            max_active_runs: 1,
            on_failure: None,
            tasks: task_map,
            execution_order: order.into_iter().map(String::from).collect(),
            source_file: "test.py".to_string(),
            compiled_at: Utc::now(),
            catchup: true,
            max_catchup_runs: None,
        }
    }

    #[test]
    fn fingerprints_are_deterministic() {
        let dag = make_dag(
            "test",
            vec![make_task("a", vec![]), make_task("b", vec!["a"])],
            vec!["a", "b"],
        );

        let fps1 = PlanFingerprinter::fingerprint_dag(&dag);
        let fps2 = PlanFingerprinter::fingerprint_dag(&dag);

        assert_eq!(fps1["a"], fps2["a"]);
        assert_eq!(fps1["b"], fps2["b"]);
    }

    #[test]
    fn upstream_change_cascades() {
        // DAG: a -> b -> c
        let dag1 = make_dag(
            "test",
            vec![
                make_task("a", vec![]),
                make_task("b", vec!["a"]),
                make_task("c", vec!["b"]),
            ],
            vec!["a", "b", "c"],
        );

        let fps1 = PlanFingerprinter::fingerprint_dag(&dag1);

        // Now change task "a"
        let mut modified_a = make_task("a", vec![]);
        modified_a.task_type = TaskType::Bash {
            command: "echo changed".to_string(),
        };

        let dag2 = make_dag(
            "test",
            vec![
                modified_a,
                make_task("b", vec!["a"]),
                make_task("c", vec!["b"]),
            ],
            vec!["a", "b", "c"],
        );

        let fps2 = PlanFingerprinter::fingerprint_dag(&dag2);

        // a changed directly
        assert_ne!(fps1["a"], fps2["a"]);
        // b changed because upstream a changed
        assert_ne!(fps1["b"], fps2["b"]);
        // c changed because upstream b changed (transitive)
        assert_ne!(fps1["c"], fps2["c"]);
    }

    #[test]
    fn independent_tasks_dont_affect_each_other() {
        let dag1 = make_dag(
            "test",
            vec![make_task("a", vec![]), make_task("b", vec![])],
            vec!["a", "b"],
        );

        let fps1 = PlanFingerprinter::fingerprint_dag(&dag1);

        // Change task "a"
        let mut modified_a = make_task("a", vec![]);
        modified_a.task_type = TaskType::Bash {
            command: "echo changed".to_string(),
        };

        let dag2 = make_dag(
            "test",
            vec![modified_a, make_task("b", vec![])],
            vec!["a", "b"],
        );

        let fps2 = PlanFingerprinter::fingerprint_dag(&dag2);

        assert_ne!(fps1["a"], fps2["a"]); // a changed
        assert_eq!(fps1["b"], fps2["b"]); // b unaffected — no dependency on a
    }

    #[test]
    fn config_change_changes_fingerprint() {
        let mut task = make_task("a", vec![]);
        let dag1 = make_dag("test", vec![task.clone()], vec!["a"]);
        let fps1 = PlanFingerprinter::fingerprint_dag(&dag1);

        task.retries = 3;
        let dag2 = make_dag("test", vec![task], vec!["a"]);
        let fps2 = PlanFingerprinter::fingerprint_dag(&dag2);

        assert_ne!(fps1["a"], fps2["a"]);
    }
}
