//! Trigger rule evaluation.
//!
//! Evaluates whether a task is ready to execute based on its upstream dependencies
//! and trigger rule. This is the core logic for determining task readiness in
//! event-driven scheduling.
//!
//! Trigger rules:
//! - **AllSuccess**: All upstream tasks succeeded (default, fails if any upstream fails)
//! - **AllDone**: All upstream tasks completed (success or failure)
//! - **OneSuccess**: At least one upstream succeeded
//! - **OneFailed**: At least one upstream failed
//! - **NoDeps**: Always ready (root task, no dependencies)

use conduit_common::dag::{Dag, TaskId, TriggerRule};

use crate::scheduler::{DagRunState, TaskState};

/// Evaluates trigger rules for tasks.
pub struct TriggerRuleEvaluator;

impl TriggerRuleEvaluator {
    /// Create a new evaluator.
    pub fn new() -> Self {
        Self
    }

    /// Evaluate if a task is ready to execute based on its trigger rule.
    ///
    /// Returns `true` if all conditions are met to run the task.
    pub fn evaluate(
        &self,
        trigger_rule: &TriggerRule,
        task_id: &TaskId,
        dag: &Dag,
        run_state: &DagRunState,
    ) -> bool {
        match trigger_rule {
            TriggerRule::AllSuccess => self.all_success(task_id, dag, run_state),
            TriggerRule::AllDone => self.all_done(task_id, dag, run_state),
            TriggerRule::OneSuccess => self.one_success(task_id, dag, run_state),
            TriggerRule::OneFailed => self.one_failed(task_id, dag, run_state),
            TriggerRule::NoDeps => self.no_deps(task_id, dag, run_state),
        }
    }

    /// AllSuccess: All upstream tasks succeeded. No upstream failures allowed.
    fn all_success(&self, task_id: &TaskId, dag: &Dag, run_state: &DagRunState) -> bool {
        let upstream = self.get_upstream_tasks(task_id, dag);

        if upstream.is_empty() {
            // No dependencies = ready
            return true;
        }

        // All upstream must be Success, none can be Failed or Skipped
        upstream.iter().all(|up_task_id| {
            matches!(
                run_state.task_states.get(up_task_id),
                Some(TaskState::Success { .. })
            )
        })
    }

    /// AllDone: All upstream tasks are terminal (success or failed).
    fn all_done(&self, task_id: &TaskId, dag: &Dag, run_state: &DagRunState) -> bool {
        let upstream = self.get_upstream_tasks(task_id, dag);

        if upstream.is_empty() {
            // No dependencies = ready
            return true;
        }

        // All upstream must be in a terminal state
        upstream.iter().all(|up_task_id| {
            matches!(
                run_state.task_states.get(up_task_id),
                Some(
                    TaskState::Success { .. }
                        | TaskState::Failed { .. }
                        | TaskState::Skipped { .. }
                )
            )
        })
    }

    /// OneSuccess: At least one upstream task succeeded.
    fn one_success(&self, task_id: &TaskId, dag: &Dag, run_state: &DagRunState) -> bool {
        let upstream = self.get_upstream_tasks(task_id, dag);

        if upstream.is_empty() {
            // No dependencies = ready
            return true;
        }

        // At least one upstream succeeded
        upstream.iter().any(|up_task_id| {
            matches!(
                run_state.task_states.get(up_task_id),
                Some(TaskState::Success { .. })
            )
        })
    }

    /// OneFailed: At least one upstream task failed.
    fn one_failed(&self, task_id: &TaskId, dag: &Dag, run_state: &DagRunState) -> bool {
        let upstream = self.get_upstream_tasks(task_id, dag);

        if upstream.is_empty() {
            // No dependencies = ready
            return true;
        }

        // At least one upstream failed
        upstream.iter().any(|up_task_id| {
            matches!(
                run_state.task_states.get(up_task_id),
                Some(TaskState::Failed { .. })
            )
        })
    }

    /// NoDeps: Always ready (task has no dependencies).
    fn no_deps(&self, _task_id: &TaskId, _dag: &Dag, _run_state: &DagRunState) -> bool {
        true
    }

    /// Get all upstream task IDs that this task depends on.
    fn get_upstream_tasks(&self, task_id: &TaskId, dag: &Dag) -> Vec<TaskId> {
        match dag.tasks.get(task_id) {
            None => vec![],
            Some(task) => task
                .dependencies
                .iter()
                .map(|dep| dep.task_id.clone())
                .collect(),
        }
    }
}

impl Default for TriggerRuleEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::HashMap;

    fn create_test_dag(tasks: Vec<(String, Vec<String>)>) -> Dag {
        use conduit_common::dag::{DependencyType, ResourceLimits, Task, TaskDependency, TaskType};

        let mut dag_tasks = HashMap::new();
        let mut execution_order = vec![];

        for (task_id, deps) in tasks {
            let dependencies = deps
                .into_iter()
                .map(|dep_id| TaskDependency {
                    task_id: dep_id,
                    dependency_type: DependencyType::ExecutionOrder,
                })
                .collect();

            dag_tasks.insert(
                task_id.clone(),
                Task {
                    id: task_id.clone(),
                    task_type: TaskType::Python {
                        module: "test".to_string(),
                        function: "test".to_string(),
                    },
                    dependencies,
                    retries: 0,
                    retry_delay: None,
                    pool: None,
                    timeout: None,
                    priority: 0,
                    resources: ResourceLimits::default(),
                    trigger_rule: TriggerRule::AllSuccess,
                    incremental: None,
                    contracts: None,
                },
            );
            execution_order.push(task_id);
        }

        Dag {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            tags: vec![],
            max_active_runs: 1,
            on_failure: None,
            tasks: dag_tasks,
            execution_order,
            source_file: "test.py".to_string(),
            compiled_at: Utc::now(),
            catchup: true,
            max_catchup_runs: None,
        }
    }

    fn create_run_state(task_states: HashMap<String, TaskState>) -> DagRunState {
        DagRunState {
            dag_id: "test_dag".to_string(),
            run_id: "run_1".to_string(),
            logical_date: Utc::now(),
            started_at: Utc::now(),
            task_states,
            config: HashMap::new(),
        }
    }

    #[test]
    fn test_all_success_no_deps() {
        let dag = create_test_dag(vec![("task_a".to_string(), vec![])]);
        let run_state = create_run_state(HashMap::new());

        let evaluator = TriggerRuleEvaluator::new();
        assert!(evaluator.all_success(&"task_a".to_string(), &dag, &run_state));
    }

    #[test]
    fn test_all_success_with_upstream() {
        let dag = create_test_dag(vec![
            ("task_a".to_string(), vec![]),
            ("task_b".to_string(), vec!["task_a".to_string()]),
        ]);

        let mut task_states = HashMap::new();
        task_states.insert(
            "task_a".to_string(),
            TaskState::Success {
                snapshot_id: None,
                duration_ms: 100,
                completed_at: Utc::now(),
            },
        );

        let run_state = create_run_state(task_states);
        let evaluator = TriggerRuleEvaluator::new();

        assert!(evaluator.all_success(&"task_b".to_string(), &dag, &run_state));
    }

    #[test]
    fn test_all_success_upstream_failed() {
        let dag = create_test_dag(vec![
            ("task_a".to_string(), vec![]),
            ("task_b".to_string(), vec!["task_a".to_string()]),
        ]);

        let mut task_states = HashMap::new();
        task_states.insert(
            "task_a".to_string(),
            TaskState::Failed {
                error: "test error".to_string(),
                attempt: 0,
                completed_at: Utc::now(),
            },
        );

        let run_state = create_run_state(task_states);
        let evaluator = TriggerRuleEvaluator::new();

        assert!(!evaluator.all_success(&"task_b".to_string(), &dag, &run_state));
    }

    #[test]
    fn test_all_done_both_success() {
        let dag = create_test_dag(vec![
            ("task_a".to_string(), vec![]),
            ("task_b".to_string(), vec!["task_a".to_string()]),
        ]);

        let mut task_states = HashMap::new();
        task_states.insert(
            "task_a".to_string(),
            TaskState::Success {
                snapshot_id: None,
                duration_ms: 100,
                completed_at: Utc::now(),
            },
        );

        let run_state = create_run_state(task_states);
        let evaluator = TriggerRuleEvaluator::new();

        assert!(evaluator.all_done(&"task_b".to_string(), &dag, &run_state));
    }

    #[test]
    fn test_all_done_with_failure() {
        let dag = create_test_dag(vec![
            ("task_a".to_string(), vec![]),
            ("task_b".to_string(), vec!["task_a".to_string()]),
        ]);

        let mut task_states = HashMap::new();
        task_states.insert(
            "task_a".to_string(),
            TaskState::Failed {
                error: "test error".to_string(),
                attempt: 0,
                completed_at: Utc::now(),
            },
        );

        let run_state = create_run_state(task_states);
        let evaluator = TriggerRuleEvaluator::new();

        assert!(evaluator.all_done(&"task_b".to_string(), &dag, &run_state));
    }

    #[test]
    fn test_one_success_satisfied() {
        let dag = create_test_dag(vec![
            ("task_a".to_string(), vec![]),
            ("task_b".to_string(), vec![]),
            (
                "task_c".to_string(),
                vec!["task_a".to_string(), "task_b".to_string()],
            ),
        ]);

        let mut task_states = HashMap::new();
        task_states.insert(
            "task_a".to_string(),
            TaskState::Success {
                snapshot_id: None,
                duration_ms: 100,
                completed_at: Utc::now(),
            },
        );
        task_states.insert(
            "task_b".to_string(),
            TaskState::Failed {
                error: "test error".to_string(),
                attempt: 0,
                completed_at: Utc::now(),
            },
        );

        let run_state = create_run_state(task_states);
        let evaluator = TriggerRuleEvaluator::new();

        assert!(evaluator.one_success(&"task_c".to_string(), &dag, &run_state));
    }

    #[test]
    fn test_one_failed_satisfied() {
        let dag = create_test_dag(vec![
            ("task_a".to_string(), vec![]),
            ("task_b".to_string(), vec![]),
            (
                "task_c".to_string(),
                vec!["task_a".to_string(), "task_b".to_string()],
            ),
        ]);

        let mut task_states = HashMap::new();
        task_states.insert(
            "task_a".to_string(),
            TaskState::Failed {
                error: "test error".to_string(),
                attempt: 0,
                completed_at: Utc::now(),
            },
        );
        task_states.insert(
            "task_b".to_string(),
            TaskState::Success {
                snapshot_id: None,
                duration_ms: 100,
                completed_at: Utc::now(),
            },
        );

        let run_state = create_run_state(task_states);
        let evaluator = TriggerRuleEvaluator::new();

        assert!(evaluator.one_failed(&"task_c".to_string(), &dag, &run_state));
    }
}
