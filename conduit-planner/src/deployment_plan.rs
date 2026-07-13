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

/// Errors that prevent a partial-apply selection from producing a valid
/// filtered plan. Returned by `DeploymentPlan::filtered_to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartialApplyError {
    /// `--only` was passed with no selectors. Distinguish from "nothing to do":
    /// empty selection is operator error, not a no-op apply.
    EmptySelection,
    /// One or more selectors don't match any action in this plan. The plan may
    /// have been generated against a different DAG layout, or the selector is
    /// a typo. Lists the offending `(dag, task)` pairs.
    UnknownSelectors(Vec<(DagId, TaskId)>),
}

impl std::fmt::Display for PartialApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptySelection => {
                write!(
                    f,
                    "partial apply: --only was passed but no selectors were given"
                )
            }
            Self::UnknownSelectors(items) => {
                write!(f, "partial apply: selectors not in plan:")?;
                for (d, t) in items {
                    write!(f, " {}.{}", d, t)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for PartialApplyError {}

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentPlan {
    /// Unique plan ID.
    pub id: String,
    /// Target environment.
    pub target_environment: EnvironmentId,
    /// The environment revision (`Environment::current_version`) this plan
    /// was generated against. `apply` refuses the plan when the live
    /// environment has moved past it. Plans saved before this field existed
    /// deserialize as 0.
    #[serde(default)]
    pub base_environment_version: u32,
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
            base_environment_version: environment.current_version,
            created_at: Utc::now(),
            actions,
            stats,
            contract_validation: None,
            pending_contracts,
            change_set_display: Some(change_display),
        }
    }

    /// Collect all data quality contracts from tasks that are part of this deployment.
    fn collect_contracts(actions: &[DeploymentAction], plan: &ConduitPlan) -> Vec<TaskContracts> {
        let mut contracts = Vec::new();

        for action in actions {
            // Collect contracts for tasks being executed or reused
            if matches!(
                action.action,
                ActionKind::Execute | ActionKind::ReuseSnapshot { .. }
            ) {
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
                                "New task, but matching snapshot found from previous run"
                                    .to_string(),
                            )
                        } else {
                            (
                                ActionKind::Execute,
                                "New task — no prior execution".to_string(),
                            )
                        }
                    }
                    ChangeKind::Modified => {
                        if let Some(snap_id) = &change.reusable_snapshot_id {
                            (
                                ActionKind::ReuseSnapshot {
                                    snapshot_id: snap_id.clone(),
                                },
                                "Modified, but identical fingerprint found in snapshot store"
                                    .to_string(),
                            )
                        } else {
                            (
                                ActionKind::Execute,
                                "Task code or config changed".to_string(),
                            )
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
                let cp =
                    crate::impact_analyzer::ImpactAnalyzer::critical_path_length(dag, &affected);
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
        let json = self
            .to_json()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
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

    /// Build a partial deployment plan containing only the selected tasks,
    /// plus the upstream Execute / ReuseSnapshot / Remove actions they depend
    /// on (transitively). Actions for tasks outside the selection are dropped;
    /// `apply_to_environment` then leaves those env pointers untouched.
    ///
    /// `selectors` are `(dag_id, task_id)` pairs. Each must match an action in
    /// this plan — selectors that don't are returned in
    /// `PartialApplyError::UnknownSelectors`.
    ///
    /// Upstream inclusion is based on the task `dependencies` declared in the
    /// `ConduitPlan`. Only upstream tasks that are themselves Execute / Reuse
    /// / Remove are auto-included — pure `Skip` upstream are no-ops and stay
    /// out, so the filtered plan stays minimal.
    ///
    /// The returned plan keeps the original execution order, has stats
    /// recomputed from the kept actions, and carries the same `id` and
    /// `target_environment` as the source — partial apply is still "this
    /// plan applied to this env", just narrower.
    pub fn filtered_to(
        &self,
        plan: &ConduitPlan,
        selectors: &[(DagId, TaskId)],
    ) -> Result<DeploymentPlan, PartialApplyError> {
        use std::collections::HashSet;

        if selectors.is_empty() {
            return Err(PartialApplyError::EmptySelection);
        }

        let action_index: HashSet<(DagId, TaskId)> = self
            .actions
            .iter()
            .map(|a| (a.dag_id.clone(), a.task_id.clone()))
            .collect();

        let unknown: Vec<(DagId, TaskId)> = selectors
            .iter()
            .filter(|s| !action_index.contains(s))
            .cloned()
            .collect();
        if !unknown.is_empty() {
            return Err(PartialApplyError::UnknownSelectors(unknown));
        }

        // BFS through ConduitPlan task deps to close over upstream that must
        // also run. An upstream is "must include" if its action in self is
        // anything other than Skip — Skip upstream are no-ops and can be
        // dropped from the partial plan without affecting correctness.
        let action_kind_for = |key: &(DagId, TaskId)| -> Option<&ActionKind> {
            self.actions
                .iter()
                .find(|a| &(a.dag_id.clone(), a.task_id.clone()) == key)
                .map(|a| &a.action)
        };

        let mut selected: HashSet<(DagId, TaskId)> = selectors.iter().cloned().collect();
        let mut queue: Vec<(DagId, TaskId)> = selectors.to_vec();
        while let Some((dag_id, task_id)) = queue.pop() {
            let dag = match plan.dags.get(&dag_id) {
                Some(d) => d,
                None => continue, // selector validated above; defensive
            };
            let task = match dag.tasks.get(&task_id) {
                Some(t) => t,
                None => continue,
            };
            for dep in &task.dependencies {
                let key = (dag_id.clone(), dep.task_id.clone());
                if selected.contains(&key) {
                    continue;
                }
                match action_kind_for(&key) {
                    Some(ActionKind::Skip) | None => {
                        // Skip upstream is a no-op — safe to leave out.
                        // None means the dep has no action (e.g. fully external),
                        // also safe to skip.
                    }
                    Some(_) => {
                        selected.insert(key.clone());
                        queue.push(key);
                    }
                }
            }
        }

        let kept_actions: Vec<DeploymentAction> = self
            .actions
            .iter()
            .filter(|a| selected.contains(&(a.dag_id.clone(), a.task_id.clone())))
            .cloned()
            .collect();

        let auto_included = selected.len() - selectors.len();

        let stats = DeploymentStats {
            total_tasks_in_plan: self.stats.total_tasks_in_plan,
            tasks_to_execute: kept_actions
                .iter()
                .filter(|a| a.action == ActionKind::Execute)
                .count(),
            tasks_to_reuse: kept_actions
                .iter()
                .filter(|a| matches!(a.action, ActionKind::ReuseSnapshot { .. }))
                .count(),
            tasks_to_skip: kept_actions
                .iter()
                .filter(|a| a.action == ActionKind::Skip)
                .count(),
            tasks_to_remove: kept_actions
                .iter()
                .filter(|a| a.action == ActionKind::Remove)
                .count(),
            estimated_total_ms: {
                let sum: u64 = kept_actions
                    .iter()
                    .filter_map(|a| a.estimated_duration_ms)
                    .sum();
                if sum > 0 {
                    Some(sum)
                } else {
                    None
                }
            },
            // Critical path and blast radius are properties of the unfiltered
            // plan; the filtered subset is a hand-picked slice that has no
            // meaningful "blast radius" of its own. Keep the original values
            // so the operator can still see the impact of the underlying plan.
            critical_path_depth: self.stats.critical_path_depth,
            blast_radius: self.stats.blast_radius,
        };

        // Filter contracts to only those for kept tasks.
        let kept_keys: HashSet<(DagId, TaskId)> = kept_actions
            .iter()
            .map(|a| (a.dag_id.clone(), a.task_id.clone()))
            .collect();
        let pending_contracts = self
            .pending_contracts
            .iter()
            .filter(|tc| match &tc.dag_id {
                Some(d) => kept_keys.contains(&(d.clone(), tc.task_id.clone())),
                None => false,
            })
            .cloned()
            .collect();

        let change_set_display = self.change_set_display.as_ref().map(|s| {
            format!(
                "{}\n[partial apply — {} selected, {} upstream auto-included, {} actions kept]",
                s,
                selectors.len(),
                auto_included,
                kept_actions.len()
            )
        });

        Ok(DeploymentPlan {
            id: self.id.clone(),
            target_environment: self.target_environment.clone(),
            base_environment_version: self.base_environment_version,
            created_at: self.created_at,
            actions: kept_actions,
            stats,
            contract_validation: None,
            pending_contracts,
            change_set_display,
        })
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
                    environment.snapshot_map.insert(key, snapshot_id.clone());
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
        writeln!(
            f,
            "Created: {}",
            self.created_at.format("%Y-%m-%d %H:%M:%S UTC")
        )?;
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
        writeln!(
            f,
            "  Critical path:   {} levels deep",
            self.stats.critical_path_depth
        )?;
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
    use chrono::Utc;
    use conduit_common::dag::*;
    use conduit_common::snapshot::{Environment, Snapshot};

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
            retry_backoff: None,
            source_hash: None,
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
            lineage_strict: false,
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
        let plan = make_plan("etl", vec![make_task("extract", vec![])], vec!["extract"]);

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
            env.snapshot_map
                .get(&("etl".to_string(), "extract".to_string())),
            Some(&"snap_new_1".to_string())
        );
        assert_eq!(
            env.snapshot_map
                .get(&("etl".to_string(), "load".to_string())),
            Some(&"snap_new_2".to_string())
        );
    }

    #[test]
    fn plan_serialization_roundtrip() {
        let plan = make_plan("etl", vec![make_task("extract", vec![])], vec!["extract"]);
        let env = Environment::new("staging");
        let store = SnapshotStore::new();

        let deploy = DeploymentPlan::generate(&plan, &env, &store);

        let json = deploy.to_json().unwrap();
        let restored = DeploymentPlan::from_json(&json).unwrap();

        assert_eq!(restored.target_environment, "staging");
        assert_eq!(restored.actions.len(), deploy.actions.len());
    }

    #[test]
    fn plan_records_base_environment_version() {
        let plan = make_plan("etl", vec![make_task("extract", vec![])], vec!["extract"]);
        let mut env = Environment::new("staging");
        env.current_version = 3;
        let store = SnapshotStore::new();

        let deploy = DeploymentPlan::generate(&plan, &env, &store);
        assert_eq!(deploy.base_environment_version, 3);

        let restored = DeploymentPlan::from_json(&deploy.to_json().unwrap()).unwrap();
        assert_eq!(restored.base_environment_version, 3);
    }

    #[test]
    fn old_plan_json_without_base_version_defaults_to_zero() {
        let plan = make_plan("etl", vec![make_task("extract", vec![])], vec!["extract"]);
        let env = Environment::new("staging");
        let store = SnapshotStore::new();

        let deploy = DeploymentPlan::generate(&plan, &env, &store);
        let mut value: serde_json::Value =
            serde_json::from_str(&deploy.to_json().unwrap()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("base_environment_version");
        let restored = DeploymentPlan::from_json(&value.to_string()).unwrap();
        assert_eq!(restored.base_environment_version, 0);
    }

    #[test]
    fn filtered_to_selects_single_task_and_auto_includes_upstream() {
        // extract -> transform -> load
        // Select only `load` — extract and transform must come along because
        // they're Execute upstream of load.
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
        assert_eq!(deploy.actions.len(), 3, "all three tasks should be Execute");

        let selectors = vec![("etl".to_string(), "load".to_string())];
        let filtered = deploy.filtered_to(&plan, &selectors).unwrap();

        let ids: Vec<&str> = filtered
            .actions
            .iter()
            .map(|a| a.task_id.as_str())
            .collect();
        assert_eq!(
            ids,
            vec!["extract", "transform", "load"],
            "upstream must be auto-included in execution order"
        );
        assert_eq!(filtered.stats.tasks_to_execute, 3);
    }

    #[test]
    fn filtered_to_skips_unchanged_upstream() {
        // extract is Unchanged (Skip action), transform is Modified (Execute).
        // Selecting `transform` should *not* drag the Skip extract along — Skip
        // is a no-op and including it just bloats the partial plan.
        let plan = make_plan(
            "etl",
            vec![
                make_task("extract", vec![]),
                make_task("transform", vec!["extract"]),
            ],
            vec!["extract", "transform"],
        );

        // Seed env with a matching snapshot for `extract` so it becomes Unchanged.
        let fps = crate::fingerprinter::PlanFingerprinter::fingerprint_plan(&plan);
        let extract_fp = fps
            .get(&("etl".to_string(), "extract".to_string()))
            .unwrap();
        let store = SnapshotStore::new();
        store
            .put(Snapshot {
                id: "snap_extract".to_string(),
                fingerprint: extract_fp.clone(),
                dag_id: "etl".to_string(),
                task_id: "extract".to_string(),
                created_at: chrono::Utc::now(),
                parent_fingerprints: vec![],
                metadata: HashMap::new(),
            })
            .unwrap();
        let mut env = Environment::new("production");
        env.snapshot_map.insert(
            ("etl".to_string(), "extract".to_string()),
            "snap_extract".to_string(),
        );

        let deploy = DeploymentPlan::generate(&plan, &env, &store);

        let selectors = vec![("etl".to_string(), "transform".to_string())];
        let filtered = deploy.filtered_to(&plan, &selectors).unwrap();

        let ids: Vec<&str> = filtered
            .actions
            .iter()
            .map(|a| a.task_id.as_str())
            .collect();
        assert_eq!(
            ids,
            vec!["transform"],
            "Skip upstream is a no-op and should not be dragged into the partial plan"
        );
    }

    #[test]
    fn filtered_to_errors_on_unknown_selector() {
        let plan = make_plan("etl", vec![make_task("extract", vec![])], vec!["extract"]);
        let env = Environment::new("production");
        let store = SnapshotStore::new();
        let deploy = DeploymentPlan::generate(&plan, &env, &store);

        let selectors = vec![("etl".to_string(), "no_such_task".to_string())];
        let err = deploy.filtered_to(&plan, &selectors).unwrap_err();
        match err {
            PartialApplyError::UnknownSelectors(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].1, "no_such_task");
            }
            other => panic!("expected UnknownSelectors, got {:?}", other),
        }
    }

    #[test]
    fn filtered_to_rejects_empty_selection() {
        let plan = make_plan("etl", vec![make_task("extract", vec![])], vec!["extract"]);
        let env = Environment::new("production");
        let store = SnapshotStore::new();
        let deploy = DeploymentPlan::generate(&plan, &env, &store);

        let err = deploy.filtered_to(&plan, &[]).unwrap_err();
        assert_eq!(err, PartialApplyError::EmptySelection);
    }

    #[test]
    fn filtered_to_preserves_plan_id_and_target() {
        // Partial apply is still "this plan applied to this env" — the filtered
        // plan must keep the same id and target so history accounting lines up.
        let plan = make_plan(
            "etl",
            vec![make_task("a", vec![]), make_task("b", vec!["a"])],
            vec!["a", "b"],
        );
        let env = Environment::new("production");
        let store = SnapshotStore::new();
        let deploy = DeploymentPlan::generate(&plan, &env, &store);

        let selectors = vec![("etl".to_string(), "a".to_string())];
        let filtered = deploy.filtered_to(&plan, &selectors).unwrap();

        assert_eq!(filtered.id, deploy.id);
        assert_eq!(filtered.target_environment, deploy.target_environment);
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
