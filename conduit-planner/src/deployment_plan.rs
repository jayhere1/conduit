//! Deployment plan generation and execution.
//!
//! A DeploymentPlan is the output of `conduit plan` and the input to
//! `conduit apply`. It contains exactly which tasks to execute, which
//! snapshots to reuse, and which environment pointers to update.
//!
//! This is the Terraform-style "plan file" — you can review it,
//! share it for approval, and apply it atomically.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use conduit_common::contracts::{DeploymentValidation, TaskContracts};
use conduit_common::dag::{DagId, TaskId};
use conduit_common::fingerprint::Fingerprint;
use conduit_common::snapshot::{Environment, EnvironmentId};
use conduit_compiler::ConduitPlan;

use crate::change_detector::{ChangeDetector, ChangeKind, ChangeSet};
use crate::impact_analyzer::ImpactAnalyzer;
use conduit_state::SnapshotStore;

/// What to do with a specific task during apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionKind {
    /// Execute this task (it's new or changed).
    Execute,
    /// Reuse an existing snapshot (fingerprint matches a previous execution).
    ReuseSnapshot { snapshot_id: String },
    /// Skip this task (unchanged, already has correct snapshot in environment).
    Skip,
    /// Remove this task's snapshot pointer from the environment.
    Remove,
}

/// A single action in the deployment plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentAction {
    pub dag_id: DagId,
    pub task_id: TaskId,
    pub action: ActionKind,
    pub fingerprint: Option<Fingerprint>,
    /// Why this action was chosen (human-readable).
    pub reason: String,
    /// Estimated execution time (if available from previous runs).
    pub estimated_duration_ms: Option<u64>,
}

/// A complete deployment plan — the minimum set of actions to
/// bring an environment up to date with the compiled plan.
#[derive(Debug, Serialize, Deserialize)]
pub struct DeploymentPlan {
    /// Unique plan ID.
    pub id: String,
    /// Target environment.
    pub target_environment: EnvironmentId,
    /// When the plan was generated.
    pub created_at: DateTime<Utc>,
    /// All actions, in a valid execution order.
    pub actions: Vec<DeploymentAction>,
    /// Summary statistics.
    pub stats: DeploymentStats,
    /// Contract validation results (populated after execution).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_validation: Option<DeploymentValidation>,
    /// All contracts that will be checked during/after apply.
    #[serde(default)]
    pub pending_contracts: Vec<TaskContracts>,
    /// The raw change set (for display / debugging).
    #[serde(skip)]
    pub change_set_display: Option<String>,
}

/// Statistics about the deployment plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentStats {
    pub total_tasks_in_plan: usize,
    pub tasks_to_execute: usize,
    pub tasks_to_reuse: usize,
    pub tasks_to_skip: usize,
    pub tasks_to_remove: usize,
    /// Estimated total execution time (ms) based on historical data.
    pub estimated_total_ms: Option<u64>,
    /// The critical path length (minimum sequential depth).
    pub critical_path_depth: usize,
    /// Blast radius: total tasks affected by changes.
    pub blast_radius: usize,
}

impl DeploymentPlan {
    /// Generate a deployment plan by comparing the compiled plan
    /// against the target environment.
    pub fn generate(
        plan: &ConduitPlan,
        environment: &Environment,
        snapshot_store: &SnapshotStore,
    ) -> Self {
        let plan_id = format!(
            "plan_{}_{}_{}",
            environment.id,
            Utc::now().format("%Y%m%d_%H%M%S"),
            &uuid::Uuid::new_v4().to_string()[..8]
        );

        // Step 1: Detect changes
        let detector = ChangeDetector::new(plan, environment, snapshot_store);
        let change_set = detector.detect();
        let change_display = format!("{}", change_set);

        // Step 2: Impact analysis for directly changed tasks
        let directly_changed: Vec<(DagId, TaskId)> = change_set
            .changes
            .iter()
            .filter(|c| matches!(c.kind, ChangeKind::Added | ChangeKind::Modified))
            .map(|c| (c.dag_id.clone(), c.task_id.clone()))
            .collect();

        let impact = ImpactAnalyzer::analyze(plan, &directly_changed);

        // Step 3: Convert changes to deployment actions
        let actions = Self::build_actions(&change_set, plan);

        // Step 4: Compute stats
        let stats = Self::compute_stats(&actions, &impact, plan);

        // Step 5: Collect contracts from tasks that will be executed
        let pending_contracts = Self::collect_contracts(&actions, plan);

        DeploymentPlan {
            id: plan_id,
            target_environment: environment.id.clone(),
            created_at: Utc::now(),
            actions,
            stats,
            contract_validation: None,
            pending_contracts,
            change_set_display: Some(change_display),
        }
    }

    /// Collect all data quality contracts from tasks that are part of this deployment.
    fn collect_contracts(
        actions: &[DeploymentAction],
        plan: &ConduitPlan,
    ) -> Vec<TaskContracts> {
        let mut contracts = Vec::new();

        for action in actions {
            // Collect contracts for tasks being executed or reused
            if matches!(action.action, ActionKind::Execute | ActionKind::ReuseSnapshot { .. }) {
                if let Some(dag) = plan.dags.get(&action.dag_id) {
                    if let Some(task) = dag.tasks.get(&action.task_id) {
                        if let Some(tc) = &task.contracts {
                            contracts.push(tc.clone());
                        }
                    }
                }
            }
        }

        contracts
    }

    /// Convert a ChangeSet into ordered deployment actions.
    fn build_actions(change_set: &ChangeSet, plan: &ConduitPlan) -> Vec<DeploymentAction> {
        let mut actions = Vec::new();

        // Process changes in execution order (per-DAG)
        for (dag_id, dag) in &plan.dags {
            for task_id in &dag.execution_order {
                let change = change_set
                    .changes
                    .iter()
                    .find(|c| c.dag_id == *dag_id && c.task_id == *task_id);

                let change = match change {
                    Some(c) => c,
                    None => continue,
                };

                let (action, reason) = match &change.kind {
                    ChangeKind::Added => {
                        if let Some(snap_id) = &change.reusable_snapshot_id {
                            (
                                ActionKind::ReuseSnapshot {
                                    snapshot_id: snap_id.clone(),
                                },
                                "New task, but matching snapshot found from previous run".to_string(),
                            )
                        } else {
                            (ActionKind::Execute, "New task — no prior execution".to_string())
                        }
                    }
                    ChangeKind::Modified => {
                        if let Some(snap_id) = &change.reusable_snapshot_id {
                            (
                                ActionKind::ReuseSnapshot {
                                    snapshot_id: snap_id.clone(),
                                },
                                "Modified, but identical fingerprint found in snapshot store".to_string(),
                            )
                        } else {
                            (ActionKind::Execute, "Task code or config changed".to_string())
                        }
                    }
                    ChangeKind::UpstreamInvalidated => {
                        if let Some(snap_id) = &change.reusable_snapshot_id {
                            (
                                ActionKind::ReuseSnapshot {
                                    snapshot_id: snap_id.clone(),
                                },
                                "Upstream changed but matching snapshot exists".to_string(),
                            )
                        } else {
                            (
                                ActionKind::Execute,
                                "Upstream task changed — must re-execute".to_string(),
                            )
                        }
                    }
                    ChangeKind::Unchanged => (
                        ActionKind::Skip,
                        "Fingerprint matches — snapshot reuse".to_string(),
                    ),
                    ChangeKind::Removed => (
                        ActionKind::Remove,
                        "Task no longer in DAG definition".to_string(),
                    ),
                };

                actions.push(DeploymentAction {
                    dag_id: dag_id.clone(),
                    task_id: task_id.clone(),
                    action,
                    fingerprint: change.current_fingerprint.clone(),
                    reason,
                    estimated_duration_ms: None, // TODO: historical lookup
                });
            }
        }

        // Add removed tasks (they're not in any DAG's execution_order)
        for change in &change_set.changes {
            if change.kind == ChangeKind::Removed {
                actions.push(DeploymentAction {
                    dag_id: change.dag_id.clone(),
                    task_id: change.task_id.clone(),
                    action: ActionKind::Remove,
                    fingerprint: None,
                    reason: "Task no longer in DAG definition".to_string(),
                    estimated_duration_ms: None,
                });
            }
        }

        actions
    }

    fn compute_stats(
        actions: &[DeploymentAction],
        impact: &crate::impact_analyzer::ImpactReport,
        plan: &ConduitPlan,
    ) -> DeploymentStats {
        let tasks_to_execute = actions
            .iter()
            .filter(|a| a.action == ActionKind::Execute)
            .count();
        let tasks_to_reuse = actions
            .iter()
            .filter(|a| matches!(a.action, ActionKind::ReuseSnapshot { .. }))
            .count();
        let tasks_to_skip = actions
            .iter()
            .filter(|a| a.action == ActionKind::Skip)
            .count();
        let tasks_to_remove = actions
            .iter()
            .filter(|a| a.action == ActionKind::Remove)
            .count();

        let estimated_total_ms = actions
            .iter()
            .filter_map(|a| a.estimated_duration_ms)
            .sum::<u64>();
        let estimated_total = if estimated_total_ms > 0 {
            Some(estimated_total_ms)
        } else {
            None
        };

        // Compute critical path across all DAGs
        let mut max_critical_path = 0;
        for (dag_id, dag_impact) in &impact.per_dag {
            if let Some(dag) = plan.dags.get(dag_id) {
                let affected: std::collections::HashSet<_> =
                    dag_impact.affected_order.iter().cloned().collect();
                let cp = crate::impact_analyzer::ImpactAnalyzer::critical_path_length(dag, &affected);
                max_critical_path = max_critical_path.max(cp);
            }
        }

        DeploymentStats {
            total_tasks_in_plan: plan.total_tasks,
            tasks_to_execute,
            tasks_to_reuse,
            tasks_to_skip,
            tasks_to_remove,
            estimated_total_ms: estimated_total,
            critical_path_depth: max_critical_path,
            blast_radius: impact.total_affected,
        }
    }

    /// Serialize the plan to JSON (for saving / sharing / approval).
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize a plan from JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Save to a file.
    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        let json = self.to_json().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        std::fs::write(path, json)
    }

    /// Get only the actions that require execution.
    pub fn executable_actions(&self) -> Vec<&DeploymentAction> {
        self.actions
            .iter()
            .filter(|a| a.action == ActionKind::Execute)
            .collect()
    }

    /// Check whether this plan can be applied. Returns false if any
    /// contract validation has Error-severity failures.
    pub fn can_apply(&self) -> bool {
        match &self.contract_validation {
            None => true, // No validation run yet — allow apply
            Some(v) => v.can_deploy,
        }
    }

    /// Returns true if this plan has pending contracts that need validation.
    pub fn has_contracts(&self) -> bool {
        !self.pending_contracts.is_empty()
    }

    /// Set the contract validation results (called after execution).
    pub fn set_validation(&mut self, validation: DeploymentValidation) {
        self.contract_validation = Some(validation);
    }

    /// Apply the plan to an environment: update snapshot pointers.
    ///
    /// This is the "apply" step — it takes the deployment plan and
    /// modifies the environment's snapshot map to reflect the new state.
    /// In practice, this would be called after all Execute actions have
    /// completed successfully.
    pub fn apply_to_environment(
        &self,
        environment: &mut Environment,
        new_snapshots: &HashMap<(DagId, TaskId), String>,
    ) {
        for action in &self.actions {
            let key = (action.dag_id.clone(), action.task_id.clone());
            match &action.action {
                ActionKind::Execute => {
                    // A new snapshot should have been created
                    if let Some(snap_id) = new_snapshots.get(&key) {
                        environment.snapshot_map.insert(key, snap_id.clone());
                    }
                }
                ActionKind::ReuseSnapshot { snapshot_id } => {
                    environment
                        .snapshot_map
                        .insert(key, snapshot_id.clone());
                }
                ActionKind::Skip => {
                    // No change needed — snapshot pointer stays the same
                }
                ActionKind::Remove => {
                    environment.snapshot_map.remove(&key);
                }
            }
        }

        environment.updated_at = Utc::now();
    }
}

impl std::fmt::Display for DeploymentPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Deployment Plan: {}", self.id)?;
        writeln!(f, "Target: {}", self.target_environment)?;
        writeln!(f, "Created: {}", self.created_at.format("%Y-%m-%d %H:%M:%S UTC"))?;
        writeln!(f)?;

        if let Some(ref cs) = self.change_set_display {
            write!(f, "{}", cs)?;
            writeln!(f)?;
        }

        writeln!(f, "Actions:")?;
        for action in &self.actions {
            let symbol = match &action.action {
                ActionKind::Execute => "EXEC",
                ActionKind::ReuseSnapshot { .. } => "REUSE",
                ActionKind::Skip => "SKIP",
                ActionKind::Remove => "DEL",
            };
            writeln!(
                f,
                "  [{:<5}] {}.{}: {}",
                symbol, action.dag_id, action.task_id, action.reason
            )?;
        }

        writeln!(f)?;
        writeln!(f, "Stats:")?;
        writeln!(f, "  Total tasks:     {}", self.stats.total_tasks_in_plan)?;
        writeln!(f, "  To execute:      {}", self.stats.tasks_to_execute)?;
        writeln!(f, "  To reuse:        {}", self.stats.tasks_to_reuse)?;
        writeln!(f, "  To skip:         {}", self.stats.tasks_to_skip)?;
        writeln!(f, "  To remove:       {}", self.stats.tasks_to_remove)?;
        writeln!(f, "  Critical path:   {} levels deep", self.stats.critical_path_depth)?;
        writeln!(f, "  Blast radius:    {} tasks", self.stats.blast_radius)?;

        if let Some(est) = self.stats.estimated_total_ms {
            writeln!(f, "  Est. duration:   {:.1}s", est as f64 / 1000.0)?;
        }

        // Show contract summary
        if !self.pending_contracts.is_empty() {
            let total_checks: usize = self.pending_contracts.iter().map(|c| c.checks.len()).sum();
            writeln!(f)?;
            writeln!(
                f,
                "Contracts: {} checks across {} tasks",
                total_checks,
                self.pending_contracts.len()
            )?;
            for tc in &self.pending_contracts {
                let scope = match &tc.dag_id {
                    Some(d) => format!("{}.{}", d, tc.task_id),
                    None => tc.task_id.clone(),
                };
                writeln!(f, "  {} — {} checks", scope, tc.checks.len())?;
            }
        }

        // Show validation results if available
        if let Some(ref validation) = self.contract_validation {
            writeln!(f)?;
            write!(f, "{}", validation)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_common::dag::*;
    use conduit_common::snapshot::{Environment, Snapshot};
    use chrono::Utc;

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
    fn fresh_deployment_executes_everything() {
        let plan = make_plan(
            "etl",
            vec![
                make_task("extract", vec![]),
                make_task("transform", vec!["extract"]),
                make_task("load", vec!["transform"]),
            ],
            vec!["extract", "transform", "load"],
        );
        let env = Environment::new("production");
        let store = SnapshotStore::new();

        let deploy = DeploymentPlan::generate(&plan, &env, &store);

        assert_eq!(deploy.stats.tasks_to_execute, 3);
        assert_eq!(deploy.stats.tasks_to_skip, 0);
        assert_eq!(deploy.stats.tasks_to_reuse, 0);
    }

    #[test]
    fn unchanged_tasks_are_skipped() {
        let plan = make_plan(
            "etl",
            vec![make_task("extract", vec![])],
            vec!["extract"],
        );

        // Compute fingerprint and set up matching environment
        let fps = crate::fingerprinter::PlanFingerprinter::fingerprint_plan(&plan);
        let fp = fps
            .get(&("etl".to_string(), "extract".to_string()))
            .unwrap();

        let store = SnapshotStore::new();
        let snap = Snapshot {
            id: "snap_1".to_string(),
            fingerprint: fp.clone(),
            dag_id: "etl".to_string(),
            task_id: "extract".to_string(),
            created_at: Utc::now(),
            parent_fingerprints: vec![],
            metadata: HashMap::new(),
        };
        store.put(snap).unwrap();

        let mut env = Environment::new("production");
        env.snapshot_map.insert(
            ("etl".to_string(), "extract".to_string()),
            "snap_1".to_string(),
        );

        let deploy = DeploymentPlan::generate(&plan, &env, &store);

        assert_eq!(deploy.stats.tasks_to_execute, 0);
        assert_eq!(deploy.stats.tasks_to_skip, 1);
    }

    #[test]
    fn apply_updates_environment() {
        let plan = make_plan(
            "etl",
            vec![
                make_task("extract", vec![]),
                make_task("load", vec!["extract"]),
            ],
            vec!["extract", "load"],
        );
        let mut env = Environment::new("production");
        let store = SnapshotStore::new();

        let deploy = DeploymentPlan::generate(&plan, &env, &store);

        // Simulate execution — new snapshots created
        let mut new_snaps = HashMap::new();
        new_snaps.insert(
            ("etl".to_string(), "extract".to_string()),
            "snap_new_1".to_string(),
        );
        new_snaps.insert(
            ("etl".to_string(), "load".to_string()),
            "snap_new_2".to_string(),
        );

        deploy.apply_to_environment(&mut env, &new_snaps);

        assert_eq!(
            env.snapshot_map.get(&("etl".to_string(), "extract".to_string())),
            Some(&"snap_new_1".to_string())
        );
        assert_eq!(
            env.snapshot_map.get(&("etl".to_string(), "load".to_string())),
            Some(&"snap_new_2".to_string())
        );
    }

    #[test]
    fn plan_serialization_roundtrip() {
        let plan = make_plan(
            "etl",
            vec![make_task("extract", vec![])],
            vec!["extract"],
        );
        let env = Environment::new("staging");
        let store = SnapshotStore::new();

        let deploy = DeploymentPlan::generate(&plan, &env, &store);

        let json = deploy.to_json().unwrap();
        let restored = DeploymentPlan::from_json(&json).unwrap();

        assert_eq!(restored.target_environment, "staging");
        assert_eq!(restored.actions.len(), deploy.actions.len());
    }

    #[test]
    fn display_format_is_readable() {
        let plan = make_plan(
            "etl",
            vec![
                make_task("extract", vec![]),
                make_task("load", vec!["extract"]),
            ],
            vec!["extract", "load"],
        );
        let env = Environment::new("production");
        let store = SnapshotStore::new();

        let deploy = DeploymentPlan::generate(&plan, &env, &store);
        let output = format!("{}", deploy);

        assert!(output.contains("Deployment Plan"));
        assert!(output.contains("EXEC"));
        assert!(output.contains("extract"));
        assert!(output.contains("load"));
    }
}
