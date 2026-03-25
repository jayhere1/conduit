//! Benchmark helpers for Conduit load testing.

use std::path::Path;
use std::collections::HashMap;

use chrono::Utc;
use conduit_common::dag::{
    Dag, Task, TaskType, TaskDependency, DependencyType, TriggerRule, ResourceLimits,
};
use conduit_common::error::ConduitResult;

/// Generate a DAG with n tasks in a diamond pattern (fan-out then fan-in).
///
/// Pattern:
/// ```
///   root
///   /  \
///  a    b
///  \  /
///  join
/// ```
///
/// For n_tasks, creates multiple diamonds stacked vertically.
pub fn generate_dag(dag_id: &str, n_tasks: usize) -> Dag {
    let mut tasks = HashMap::new();
    let mut execution_order = Vec::new();

    // Create root task
    let root_id = "root".to_string();
    tasks.insert(
        root_id.clone(),
        Task {
            id: root_id.clone(),
            task_type: TaskType::Bash {
                command: "echo 'root'".to_string(),
            },
            dependencies: Vec::new(),
            trigger_rule: TriggerRule::AllSuccess,
            retries: 0,
            retry_delay: None,
            pool: None,
            timeout: None,
            priority: 0,
            resources: ResourceLimits::default(),
        },
    );
    execution_order.push(root_id.clone());

    // Create tasks in diamond pattern
    let mut prev_join = root_id;
    let diamonds = (n_tasks.saturating_sub(1)) / 3; // 3 tasks per diamond (2 parallels + 1 join)

    for diamond_idx in 0..diamonds {
        let prefix = format!("d{}_", diamond_idx);

        // Fan-out tasks
        let task_a = format!("{}a", prefix);
        let task_b = format!("{}b", prefix);
        let task_join = format!("{}join", prefix);

        tasks.insert(
            task_a.clone(),
            Task {
                id: task_a.clone(),
                task_type: TaskType::Bash {
                    command: "echo 'task_a'".to_string(),
                },
                dependencies: vec![TaskDependency {
                    task_id: prev_join.clone(),
                    dependency_type: DependencyType::ExecutionOrder,
                }],
                trigger_rule: TriggerRule::AllSuccess,
                retries: 0,
                retry_delay: None,
                pool: None,
                timeout: None,
                priority: 0,
                resources: ResourceLimits::default(),
            },
        );
        execution_order.push(task_a.clone());

        tasks.insert(
            task_b.clone(),
            Task {
                id: task_b.clone(),
                task_type: TaskType::Bash {
                    command: "echo 'task_b'".to_string(),
                },
                dependencies: vec![TaskDependency {
                    task_id: prev_join.clone(),
                    dependency_type: DependencyType::ExecutionOrder,
                }],
                trigger_rule: TriggerRule::AllSuccess,
                retries: 0,
                retry_delay: None,
                pool: None,
                timeout: None,
                priority: 0,
                resources: ResourceLimits::default(),
            },
        );
        execution_order.push(task_b.clone());

        // Fan-in task
        tasks.insert(
            task_join.clone(),
            Task {
                id: task_join.clone(),
                task_type: TaskType::Bash {
                    command: "echo 'join'".to_string(),
                },
                dependencies: vec![
                    TaskDependency {
                        task_id: task_a.clone(),
                        dependency_type: DependencyType::ExecutionOrder,
                    },
                    TaskDependency {
                        task_id: task_b.clone(),
                        dependency_type: DependencyType::ExecutionOrder,
                    },
                ],
                trigger_rule: TriggerRule::AllSuccess,
                retries: 0,
                retry_delay: None,
                pool: None,
                timeout: None,
                priority: 0,
                resources: ResourceLimits::default(),
            },
        );
        execution_order.push(task_join.clone());

        prev_join = task_join;
    }

    // If n_tasks not divisible by 3, add remaining tasks
    let remainder = n_tasks % 3;
    for i in 0..remainder {
        let extra_id = format!("extra_{}", i);
        tasks.insert(
            extra_id.clone(),
            Task {
                id: extra_id.clone(),
                task_type: TaskType::Bash {
                    command: "echo 'extra'".to_string(),
                },
                dependencies: vec![TaskDependency {
                    task_id: prev_join.clone(),
                    dependency_type: DependencyType::ExecutionOrder,
                }],
                trigger_rule: TriggerRule::AllSuccess,
                retries: 0,
                retry_delay: None,
                pool: None,
                timeout: None,
                priority: 0,
                resources: ResourceLimits::default(),
            },
        );
        execution_order.push(extra_id);
    }

    Dag {
        id: dag_id.to_string(),
        description: Some(format!("Benchmark DAG with {} tasks", n_tasks)),
        schedule: Some("@daily".to_string()),
        tasks,
        execution_order,
        tags: Vec::new(),
        max_active_runs: 1,
        on_failure: None,
        source_file: "bench.py".to_string(),
        compiled_at: Utc::now(),
        catchup: true,
        max_catchup_runs: None,
    }
}

/// Generate DAG files to a temporary directory for compilation benchmarks.
pub fn generate_dag_files(dir: &Path, n_dags: usize, tasks_per_dag: usize) -> ConduitResult<()> {
    std::fs::create_dir_all(dir)?;

    for dag_idx in 0..n_dags {
        let dag_id = format!("bench_dag_{}", dag_idx);
        let dag = generate_dag(&dag_id, tasks_per_dag);

        // Convert DAG to a simple Python format that the compiler can parse
        let python_code = format!(
            r#"from conduit_sdk import dag, task

@dag(schedule="0 6 * * *", tags=["bench"])
def {}():
    """Benchmark DAG {}."""

{}

{}()
"#,
            dag.id.replace("-", "_"),
            dag_idx,
            generate_python_tasks(&dag),
            dag.id.replace("-", "_")
        );

        std::fs::write(dir.join(format!("{}.py", dag.id)), python_code)?;
    }

    Ok(())
}

fn generate_python_tasks(dag: &Dag) -> String {
    let mut code = String::new();

    for task_id in &dag.execution_order {
        let task = &dag.tasks[task_id];
        let deps_list: Vec<String> = task
            .dependencies
            .iter()
            .map(|d| d.task_id.clone())
            .collect();

        if deps_list.is_empty() {
            code.push_str(&format!(
                "    @task()\n    def {}():\n        pass\n\n",
                task_id
            ));
        } else {
            code.push_str(&format!(
                "    @task()\n    def {}({}):\n        pass\n\n",
                task_id,
                deps_list.join(", ")
            ));
        }
    }

    code
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_dag_100() {
        let dag = generate_dag("test_dag", 100);
        assert_eq!(dag.id, "test_dag");
        assert_eq!(dag.tasks.len(), 100);
        assert_eq!(dag.execution_order.len(), 100);
    }

    #[test]
    fn test_generate_dag_files() {
        let dir = tempfile::tempdir().unwrap();
        generate_dag_files(dir.path(), 10, 50).unwrap();

        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 10);
    }
}
