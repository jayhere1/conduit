//! Downstream impact analysis.
//!
//! Given a set of directly-changed tasks, compute the full transitive closure
//! of affected downstream tasks. This answers "if I change this one query,
//! what else needs to re-run?"
//!
//! This is critical for safe deployments: you see the full blast radius
//! before applying any changes.

use std::collections::{HashMap, HashSet, VecDeque};

use conduit_common::dag::{Dag, DagId, TaskId};
use conduit_compiler::ConduitPlan;

/// Result of impact analysis for a set of changed tasks.
#[derive(Debug)]
pub struct ImpactReport {
    /// Tasks that changed directly (code/config modified).
    pub directly_changed: Vec<(DagId, TaskId)>,
    /// Tasks affected transitively (downstream of changed tasks).
    pub transitively_affected: Vec<(DagId, TaskId)>,
    /// Total blast radius (direct + transitive).
    pub total_affected: usize,
    /// Tasks that are safe (no upstream changes).
    pub unaffected: usize,
    /// Per-DAG breakdown.
    pub per_dag: HashMap<DagId, DagImpact>,
}

/// Impact breakdown for a single DAG.
#[derive(Debug)]
pub struct DagImpact {
    pub dag_id: DagId,
    pub total_tasks: usize,
    pub affected_tasks: usize,
    /// Topologically sorted list of affected tasks (execution order).
    pub affected_order: Vec<TaskId>,
    /// The "root causes" — directly changed tasks in this DAG.
    pub root_causes: Vec<TaskId>,
}

/// Analyzes the downstream impact of a set of changes.
pub struct ImpactAnalyzer;

impl ImpactAnalyzer {
    /// Given a set of directly-changed tasks, compute the full impact.
    ///
    /// The algorithm:
    /// 1. Build a reverse dependency graph (task -> downstream dependents)
    /// 2. BFS from each changed task to find all reachable downstream tasks
    /// 3. Intersect with execution order to produce a valid execution sequence
    pub fn analyze(plan: &ConduitPlan, changed_tasks: &[(DagId, TaskId)]) -> ImpactReport {
        let mut directly_changed_set: HashSet<(DagId, TaskId)> = HashSet::new();
        let mut transitively_affected: Vec<(DagId, TaskId)> = Vec::new();
        let mut per_dag: HashMap<DagId, DagImpact> = HashMap::new();
        let mut total_tasks = 0usize;

        for (dag_id, _) in changed_tasks {
            directly_changed_set.insert((
                dag_id.clone(),
                changed_tasks
                    .iter()
                    .find(|(d, _)| d == dag_id)
                    .map(|(_, t)| t.clone())
                    .unwrap_or_default(),
            ));
        }
        // Re-collect properly
        let directly_changed_set: HashSet<(DagId, TaskId)> =
            changed_tasks.iter().cloned().collect();

        for (dag_id, dag) in &plan.dags {
            total_tasks += dag.tasks.len();

            // Find which changed tasks belong to this DAG
            let dag_roots: Vec<TaskId> = changed_tasks
                .iter()
                .filter(|(d, _)| d == dag_id)
                .map(|(_, t)| t.clone())
                .collect();

            if dag_roots.is_empty() {
                // No changes in this DAG
                per_dag.insert(
                    dag_id.clone(),
                    DagImpact {
                        dag_id: dag_id.clone(),
                        total_tasks: dag.tasks.len(),
                        affected_tasks: 0,
                        affected_order: vec![],
                        root_causes: vec![],
                    },
                );
                continue;
            }

            // Build reverse dependency graph
            let reverse_deps = Self::build_reverse_deps(dag);

            // BFS to find all downstream affected tasks
            let all_affected = Self::bfs_downstream(&dag_roots, &reverse_deps);

            // Filter to only transitively affected (not directly changed)
            for task_id in &all_affected {
                let key = (dag_id.clone(), task_id.clone());
                if !directly_changed_set.contains(&key) {
                    transitively_affected.push(key);
                }
            }

            // Intersect with execution order to get valid ordering
            let affected_order: Vec<TaskId> = dag
                .execution_order
                .iter()
                .filter(|t| all_affected.contains(*t))
                .cloned()
                .collect();

            per_dag.insert(
                dag_id.clone(),
                DagImpact {
                    dag_id: dag_id.clone(),
                    total_tasks: dag.tasks.len(),
                    affected_tasks: all_affected.len(),
                    affected_order,
                    root_causes: dag_roots,
                },
            );
        }

        let total_affected = directly_changed_set.len() + transitively_affected.len();
        let unaffected = total_tasks.saturating_sub(total_affected);

        ImpactReport {
            directly_changed: changed_tasks.to_vec(),
            transitively_affected,
            total_affected,
            unaffected,
            per_dag,
        }
    }

    /// Build a reverse dependency map: task_id -> set of downstream dependents.
    fn build_reverse_deps(dag: &Dag) -> HashMap<TaskId, Vec<TaskId>> {
        let mut reverse: HashMap<TaskId, Vec<TaskId>> = HashMap::new();

        for (task_id, task) in &dag.tasks {
            for dep in &task.dependencies {
                reverse
                    .entry(dep.task_id.clone())
                    .or_default()
                    .push(task_id.clone());
            }
        }

        reverse
    }

    /// BFS from root tasks to find all transitively downstream tasks.
    fn bfs_downstream(
        roots: &[TaskId],
        reverse_deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> HashSet<TaskId> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        for root in roots {
            visited.insert(root.clone());
            queue.push_back(root.clone());
        }

        while let Some(task_id) = queue.pop_front() {
            if let Some(dependents) = reverse_deps.get(&task_id) {
                for dep in dependents {
                    if visited.insert(dep.clone()) {
                        queue.push_back(dep.clone());
                    }
                }
            }
        }

        visited
    }

    /// Compute the critical path length through affected tasks.
    /// This gives an estimate of minimum execution time assuming
    /// unlimited parallelism for independent tasks.
    pub fn critical_path_length(dag: &Dag, affected: &HashSet<TaskId>) -> usize {
        let mut depths: HashMap<&TaskId, usize> = HashMap::new();

        for task_id in &dag.execution_order {
            if !affected.contains(task_id) {
                continue;
            }

            let task = match dag.tasks.get(task_id) {
                Some(t) => t,
                None => continue,
            };

            let max_dep_depth = task
                .dependencies
                .iter()
                .filter(|d| affected.contains(&d.task_id))
                .filter_map(|d| depths.get(&d.task_id))
                .max()
                .copied()
                .unwrap_or(0);

            depths.insert(task_id, max_dep_depth + 1);
        }

        depths.values().max().copied().unwrap_or(0)
    }
}

impl std::fmt::Display for ImpactReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Impact Analysis")?;
        writeln!(
            f,
            "  Directly changed:        {}",
            self.directly_changed.len()
        )?;
        writeln!(
            f,
            "  Transitively affected:   {}",
            self.transitively_affected.len()
        )?;
        writeln!(f, "  Total blast radius:      {}", self.total_affected)?;
        writeln!(f, "  Unaffected:              {}", self.unaffected)?;

        for (dag_id, impact) in &self.per_dag {
            if impact.affected_tasks == 0 {
                continue;
            }
            writeln!(f)?;
            writeln!(
                f,
                "  DAG '{}': {}/{} tasks affected",
                dag_id, impact.affected_tasks, impact.total_tasks
            )?;
            writeln!(f, "    Root causes: {}", impact.root_causes.join(", "))?;
            writeln!(
                f,
                "    Execution order: {}",
                impact.affected_order.join(" -> ")
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use conduit_common::dag::*;

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

    fn make_plan(dag_id: &str, tasks: Vec<Task>, order: Vec<&str>) -> ConduitPlan {
        let mut task_map = HashMap::new();
        for t in &tasks {
            task_map.insert(t.id.clone(), t.clone());
        }
        let dag = Dag {
            id: dag_id.to_string(),
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
        };
        let mut dags = HashMap::new();
        dags.insert(dag_id.to_string(), dag);
        ConduitPlan {
            dags,
            compiled_at: Utc::now(),
            compilation_time_ms: 1,
            total_tasks: tasks.len(),
            warnings: vec![],
        }
    }

    #[test]
    fn change_at_root_affects_entire_chain() {
        // extract -> transform -> load
        let plan = make_plan(
            "etl",
            vec![
                make_task("extract", vec![]),
                make_task("transform", vec!["extract"]),
                make_task("load", vec!["transform"]),
            ],
            vec!["extract", "transform", "load"],
        );

        let report = ImpactAnalyzer::analyze(&plan, &[("etl".to_string(), "extract".to_string())]);

        assert_eq!(report.total_affected, 3); // extract + transform + load
        assert_eq!(report.transitively_affected.len(), 2); // transform + load
    }

    #[test]
    fn change_at_leaf_affects_only_itself() {
        // extract -> transform -> load
        let plan = make_plan(
            "etl",
            vec![
                make_task("extract", vec![]),
                make_task("transform", vec!["extract"]),
                make_task("load", vec!["transform"]),
            ],
            vec!["extract", "transform", "load"],
        );

        let report = ImpactAnalyzer::analyze(&plan, &[("etl".to_string(), "load".to_string())]);

        assert_eq!(report.total_affected, 1); // only load
        assert_eq!(report.transitively_affected.len(), 0);
    }

    #[test]
    fn diamond_dependency_counted_once() {
        //     a
        //    / \
        //   b   c
        //    \ /
        //     d
        let plan = make_plan(
            "diamond",
            vec![
                make_task("a", vec![]),
                make_task("b", vec!["a"]),
                make_task("c", vec!["a"]),
                make_task("d", vec!["b", "c"]),
            ],
            vec!["a", "b", "c", "d"],
        );

        let report = ImpactAnalyzer::analyze(&plan, &[("diamond".to_string(), "a".to_string())]);

        assert_eq!(report.total_affected, 4); // all tasks
                                              // d is only counted once despite two paths from a
        let dag_impact = report.per_dag.get("diamond").unwrap();
        assert_eq!(dag_impact.affected_tasks, 4);
    }

    #[test]
    fn parallel_branches_isolated() {
        // a -> b
        // c -> d
        let plan = make_plan(
            "parallel",
            vec![
                make_task("a", vec![]),
                make_task("b", vec!["a"]),
                make_task("c", vec![]),
                make_task("d", vec!["c"]),
            ],
            vec!["a", "c", "b", "d"],
        );

        let report = ImpactAnalyzer::analyze(&plan, &[("parallel".to_string(), "a".to_string())]);

        assert_eq!(report.total_affected, 2); // a + b
        assert_eq!(report.unaffected, 2); // c + d
    }

    #[test]
    fn critical_path_computation() {
        //     a
        //    / \
        //   b   c
        //    \ /
        //     d
        let mut task_map = HashMap::new();
        let tasks = vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["a"]),
            make_task("d", vec!["b", "c"]),
        ];
        for t in &tasks {
            task_map.insert(t.id.clone(), t.clone());
        }
        let dag = Dag {
            id: "test".to_string(),
            description: None,
            schedule: None,
            tags: vec![],
            max_active_runs: 1,
            on_failure: None,
            tasks: task_map,
            execution_order: vec!["a", "b", "c", "d"]
                .into_iter()
                .map(String::from)
                .collect(),
            source_file: "test.py".to_string(),
            compiled_at: Utc::now(),
            catchup: true,
            max_catchup_runs: None,
        };

        let all_tasks: HashSet<_> = vec!["a", "b", "c", "d"]
            .into_iter()
            .map(String::from)
            .collect();
        let cpl = ImpactAnalyzer::critical_path_length(&dag, &all_tasks);
        assert_eq!(cpl, 3); // a -> b|c -> d (3 levels)
    }
}
