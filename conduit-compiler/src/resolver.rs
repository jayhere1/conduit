//! Dependency resolution and cycle detection.
//!
//! Takes parsed DAGs and resolves raw dependency references into
//! a validated, topologically sorted execution order.
//! Detects cycles at compile time (not runtime like Airflow).

use std::collections::{HashMap, VecDeque};

use conduit_common::dag::*;
use conduit_common::error::{ConduitError, ConduitResult};
use tracing::debug;

use crate::parser::ParsedDag;

/// Resolves dependencies and produces fully validated DAGs.
pub struct DependencyResolver;

impl DependencyResolver {
    /// Resolve a parsed DAG into a fully validated DAG with execution order.
    pub fn resolve(parsed: ParsedDag) -> ConduitResult<Dag> {
        // Build task map and validate uniqueness
        let mut tasks = HashMap::new();
        for pt in &parsed.tasks {
            if tasks.contains_key(&pt.id) {
                return Err(ConduitError::DuplicateTaskId {
                    dag_id: parsed.id.clone(),
                    task_id: pt.id.clone(),
                });
            }
            tasks.insert(pt.id.clone(), pt);
        }

        // Validate all dependency references exist
        for pt in &parsed.tasks {
            for dep in &pt.raw_dependencies {
                if !tasks.contains_key(dep) {
                    return Err(ConduitError::UnknownTaskRef {
                        dag_id: parsed.id.clone(),
                        task_id: dep.clone(),
                    });
                }
            }
        }

        // Build adjacency list for topological sort
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut in_degree: HashMap<&str, usize> = HashMap::new();

        for pt in &parsed.tasks {
            adj.entry(pt.id.as_str()).or_default();
            in_degree.entry(pt.id.as_str()).or_insert(0);

            for dep in &pt.raw_dependencies {
                adj.entry(dep.as_str()).or_default().push(pt.id.as_str());
                *in_degree.entry(pt.id.as_str()).or_insert(0) += 1;
            }
        }

        // Kahn's algorithm for topological sort + cycle detection
        let execution_order = Self::topological_sort(&adj, &in_degree, &parsed.id)?;

        debug!(
            dag_id = %parsed.id,
            order = ?execution_order,
            "Resolved execution order"
        );

        // Derive module name from source file (e.g., "warehouse.py" -> "warehouse")
        let module_name = std::path::Path::new(&parsed.source_file)
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        // Convert parsed tasks to final Task structs
        let final_tasks: HashMap<TaskId, Task> = parsed
            .tasks
            .into_iter()
            .map(|pt| {
                // Fill in the module path for Python tasks
                let task_type = match pt.task_type {
                    TaskType::Python { module, function } => TaskType::Python {
                        module: if module.is_empty() { module_name.clone() } else { module },
                        function,
                    },
                    other => other,
                };
                let task = Task {
                    id: pt.id.clone(),
                    task_type,
                    dependencies: pt
                        .raw_dependencies
                        .into_iter()
                        .map(|dep| TaskDependency {
                            task_id: dep,
                            dependency_type: DependencyType::DataFlow,
                        })
                        .collect(),
                    retries: pt.retries,
                    retry_delay: pt.retry_delay,
                    pool: pt.pool,
                    timeout: pt.timeout,
                    priority: pt.priority,
                    resources: ResourceLimits::default(),
                    trigger_rule: TriggerRule::default(),
                    incremental: None,
                    contracts: pt.contracts,
                };
                (task.id.clone(), task)
            })
            .collect();

        Ok(Dag {
            id: parsed.id,
            description: parsed.description,
            schedule: parsed.schedule,
            tags: parsed.tags,
            max_active_runs: parsed.max_active_runs,
            on_failure: parsed.on_failure,
            tasks: final_tasks,
            execution_order,
            source_file: parsed.source_file,
            compiled_at: chrono::Utc::now(),
            catchup: true,
            max_catchup_runs: None,
        })
    }

    /// Topological sort using Kahn's algorithm.
    /// Returns an error if a cycle is detected.
    fn topological_sort(
        adj: &HashMap<&str, Vec<&str>>,
        in_degree: &HashMap<&str, usize>,
        dag_id: &str,
    ) -> ConduitResult<Vec<String>> {
        let mut in_deg = in_degree.clone();
        let mut queue: VecDeque<&str> = VecDeque::new();
        let mut order = Vec::new();

        // Start with nodes that have no incoming edges
        for (node, &deg) in &in_deg {
            if deg == 0 {
                queue.push_back(node);
            }
        }

        while let Some(node) = queue.pop_front() {
            order.push(node.to_string());

            if let Some(neighbors) = adj.get(node) {
                for &neighbor in neighbors {
                    let deg = in_deg.get_mut(neighbor).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        // If we didn't process all nodes, there's a cycle
        if order.len() != in_deg.len() {
            let cycle_nodes: Vec<String> = in_deg
                .iter()
                .filter(|(_, &deg)| deg > 0)
                .map(|(node, _)| node.to_string())
                .collect();

            return Err(ConduitError::CycleDetected {
                cycle: format!(
                    "DAG '{}' contains a cycle involving: {}",
                    dag_id,
                    cycle_nodes.join(" -> ")
                ),
            });
        }

        Ok(order)
    }

    /// Resolve multiple parsed DAGs, returning errors for any that fail.
    pub fn resolve_all(parsed_dags: Vec<ParsedDag>) -> (Vec<Dag>, Vec<ConduitError>) {
        let mut resolved = Vec::new();
        let mut errors = Vec::new();

        for parsed in parsed_dags {
            match Self::resolve(parsed) {
                Ok(dag) => resolved.push(dag),
                Err(e) => errors.push(e),
            }
        }

        (resolved, errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{ParsedDag, ParsedTask};

    fn make_task(id: &str, deps: Vec<&str>) -> ParsedTask {
        ParsedTask {
            id: id.to_string(),
            task_type: TaskType::Python {
                module: "test".to_string(),
                function: id.to_string(),
            },
            retries: 0,
            retry_delay: None,
            pool: None,
            timeout: None,
            priority: 0,
            raw_dependencies: deps.into_iter().map(String::from).collect(),
            contracts: None,
        }
    }

    fn make_parsed_dag(tasks: Vec<ParsedTask>) -> ParsedDag {
        ParsedDag {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            tags: vec![],
            max_active_runs: 1,
            on_failure: None,
            tasks,
            source_file: "test.py".to_string(),
        }
    }

    #[test]
    fn resolves_linear_chain() {
        let parsed = make_parsed_dag(vec![
            make_task("extract", vec![]),
            make_task("transform", vec!["extract"]),
            make_task("load", vec!["transform"]),
        ]);

        let dag = DependencyResolver::resolve(parsed).unwrap();
        assert_eq!(dag.execution_order, vec!["extract", "transform", "load"]);
    }

    #[test]
    fn resolves_diamond_dependency() {
        let parsed = make_parsed_dag(vec![
            make_task("start", vec![]),
            make_task("branch_a", vec!["start"]),
            make_task("branch_b", vec!["start"]),
            make_task("join", vec!["branch_a", "branch_b"]),
        ]);

        let dag = DependencyResolver::resolve(parsed).unwrap();
        // start must come first, join must come last
        assert_eq!(dag.execution_order[0], "start");
        assert_eq!(dag.execution_order[3], "join");
    }

    #[test]
    fn detects_cycle() {
        let parsed = make_parsed_dag(vec![
            make_task("a", vec!["c"]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["b"]),
        ]);

        let result = DependencyResolver::resolve(parsed);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ConduitError::CycleDetected { .. }));
    }

    #[test]
    fn detects_unknown_dependency() {
        let parsed = make_parsed_dag(vec![
            make_task("a", vec![]),
            make_task("b", vec!["nonexistent"]),
        ]);

        let result = DependencyResolver::resolve(parsed);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConduitError::UnknownTaskRef { .. }));
    }

    #[test]
    fn detects_duplicate_task_id() {
        let parsed = make_parsed_dag(vec![
            make_task("duplicate", vec![]),
            make_task("duplicate", vec![]),
        ]);

        let result = DependencyResolver::resolve(parsed);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConduitError::DuplicateTaskId { .. }));
    }
}
