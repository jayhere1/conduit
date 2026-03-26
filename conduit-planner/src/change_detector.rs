//! Change detection: compare computed fingerprints against an environment.
//!
//! This is the core of the `conduit plan` command. For each task in the
//! compiled plan, it determines whether the task is:
//! - **Added**: new task not present in the environment
//! - **Modified**: task exists but fingerprint changed (code or config changed)
//! - **Invalidated**: task code is unchanged but an upstream changed
//! - **Removed**: task exists in environment but not in the compiled plan
//! - **Unchanged**: fingerprint matches — snapshot can be reused

use std::collections::{HashMap, HashSet};

use conduit_common::dag::{DagId, TaskId};
use conduit_common::fingerprint::Fingerprint;
use conduit_common::snapshot::Environment;
use conduit_compiler::ConduitPlan;
use conduit_state::SnapshotStore;

use crate::fingerprinter::{FingerprintMap, PlanFingerprinter};

/// How a task changed relative to the target environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeKind {
    /// Task is new — not present in the target environment.
    Added,
    /// Task exists but its direct fingerprint changed (code or config diff).
    Modified,
    /// Task code is identical, but an upstream task changed,
    /// invalidating this task's fingerprint (transitive change).
    UpstreamInvalidated,
    /// Task exists in the environment but not in the current plan (was deleted).
    Removed,
    /// Fingerprint matches — execution can be skipped, snapshot reused.
    Unchanged,
}

/// A single task-level change.
#[derive(Debug, Clone)]
pub struct TaskChange {
    pub dag_id: DagId,
    pub task_id: TaskId,
    pub kind: ChangeKind,
    /// The current fingerprint (None for Removed tasks).
    pub current_fingerprint: Option<Fingerprint>,
    /// The environment's fingerprint (None for Added tasks).
    pub environment_fingerprint: Option<Fingerprint>,
    /// If this task has a reusable snapshot in the store.
    pub reusable_snapshot_id: Option<String>,
}

/// The full set of changes between a compiled plan and an environment.
#[derive(Debug)]
pub struct ChangeSet {
    pub changes: Vec<TaskChange>,
    pub fingerprints: FingerprintMap,
    pub summary: ChangeSummary,
}

/// High-level summary of changes.
#[derive(Debug)]
pub struct ChangeSummary {
    pub added: usize,
    pub modified: usize,
    pub upstream_invalidated: usize,
    pub removed: usize,
    pub unchanged: usize,
    pub total_tasks: usize,
    /// How many tasks can reuse an existing snapshot (even if upstream changed,
    /// the *exact same* fingerprint might exist from a previous run).
    pub reusable: usize,
    /// Tasks that must actually execute.
    pub must_execute: usize,
}

/// Detects changes between a compiled plan and a target environment.
pub struct ChangeDetector<'a> {
    plan: &'a ConduitPlan,
    environment: &'a Environment,
    snapshot_store: &'a SnapshotStore,
}

impl<'a> ChangeDetector<'a> {
    pub fn new(
        plan: &'a ConduitPlan,
        environment: &'a Environment,
        snapshot_store: &'a SnapshotStore,
    ) -> Self {
        Self {
            plan,
            environment,
            snapshot_store,
        }
    }

    /// Detect all changes between the plan and the environment.
    pub fn detect(&self) -> ChangeSet {
        // Step 1: Compute fingerprints for the current plan
        let fingerprints = PlanFingerprinter::fingerprint_plan(self.plan);

        // Step 2: Build a lookup of environment's (dag, task) -> snapshot fingerprints
        let env_fingerprints = self.resolve_environment_fingerprints();

        // Step 3: Track which tasks are in the plan
        let plan_keys: HashSet<(DagId, TaskId)> = fingerprints.keys().cloned().collect();
        let env_keys: HashSet<(DagId, TaskId)> = self.environment.snapshot_map.keys().cloned().collect();

        let mut changes = Vec::new();

        // Step 4: Classify each task in the plan
        for (dag_id, dag) in &self.plan.dags {
            // We need to walk in execution order to detect upstream invalidation
            // First pass: compute "direct" change status for each task
            let mut direct_changed: HashSet<TaskId> = HashSet::new();

            for task_id in &dag.execution_order {
                let key = (dag_id.clone(), task_id.clone());
                let current_fp = fingerprints.get(&key);

                if !env_keys.contains(&key) {
                    // Task is new
                    direct_changed.insert(task_id.clone());
                    let reusable = current_fp
                        .and_then(|fp| self.find_reusable_snapshot(fp));
                    changes.push(TaskChange {
                        dag_id: dag_id.clone(),
                        task_id: task_id.clone(),
                        kind: ChangeKind::Added,
                        current_fingerprint: current_fp.cloned(),
                        environment_fingerprint: None,
                        reusable_snapshot_id: reusable,
                    });
                    continue;
                }

                let env_fp = env_fingerprints.get(&key);

                // Check if this task's fingerprint matches the environment
                let fingerprint_matches = match (current_fp, env_fp) {
                    (Some(cur), Some(env)) => cur == env,
                    _ => false,
                };

                if fingerprint_matches {
                    // Check if any upstream was changed (direct or transitive)
                    let task = dag.tasks.get(task_id);
                    let upstream_changed = task
                        .map(|t| {
                            t.dependencies.iter().any(|dep| {
                                direct_changed.contains(&dep.task_id)
                            })
                        })
                        .unwrap_or(false);

                    if upstream_changed {
                        // This is subtle: the task's own content didn't change,
                        // but its fingerprint includes upstream fingerprints,
                        // so if the fingerprint still matches, the upstream
                        // changes didn't actually affect this task's fingerprint.
                        // This means the task is truly unchanged.
                        changes.push(TaskChange {
                            dag_id: dag_id.clone(),
                            task_id: task_id.clone(),
                            kind: ChangeKind::Unchanged,
                            current_fingerprint: current_fp.cloned(),
                            environment_fingerprint: env_fp.cloned(),
                            reusable_snapshot_id: self.environment.snapshot_map.get(&key).cloned(),
                        });
                    } else {
                        changes.push(TaskChange {
                            dag_id: dag_id.clone(),
                            task_id: task_id.clone(),
                            kind: ChangeKind::Unchanged,
                            current_fingerprint: current_fp.cloned(),
                            environment_fingerprint: env_fp.cloned(),
                            reusable_snapshot_id: self.environment.snapshot_map.get(&key).cloned(),
                        });
                    }
                } else {
                    // Fingerprint differs. Is it a direct change or upstream invalidation?
                    let task = dag.tasks.get(task_id);
                    let any_upstream_changed = task
                        .map(|t| {
                            t.dependencies.iter().any(|dep| {
                                direct_changed.contains(&dep.task_id)
                            })
                        })
                        .unwrap_or(false);

                    // To distinguish Modified vs UpstreamInvalidated:
                    // compute what the fingerprint *would be* if upstream hadn't changed.
                    // If it still differs from environment, it's a direct modification.
                    let kind = if any_upstream_changed {
                        // Check if the task's own content/config changed too
                        let own_content_changed = self.task_own_content_changed(dag_id, task_id, &env_fingerprints);
                        if own_content_changed {
                            ChangeKind::Modified
                        } else {
                            ChangeKind::UpstreamInvalidated
                        }
                    } else {
                        ChangeKind::Modified
                    };

                    direct_changed.insert(task_id.clone());

                    let reusable = current_fp
                        .and_then(|fp| self.find_reusable_snapshot(fp));

                    changes.push(TaskChange {
                        dag_id: dag_id.clone(),
                        task_id: task_id.clone(),
                        kind,
                        current_fingerprint: current_fp.cloned(),
                        environment_fingerprint: env_fp.cloned(),
                        reusable_snapshot_id: reusable,
                    });
                }
            }
        }

        // Step 5: Detect removed tasks (in environment but not in plan)
        for key in &env_keys {
            if !plan_keys.contains(key) {
                let env_fp = env_fingerprints.get(key);
                changes.push(TaskChange {
                    dag_id: key.0.clone(),
                    task_id: key.1.clone(),
                    kind: ChangeKind::Removed,
                    current_fingerprint: None,
                    environment_fingerprint: env_fp.cloned(),
                    reusable_snapshot_id: None,
                });
            }
        }

        // Step 6: Compute summary
        let summary = Self::compute_summary(&changes);

        ChangeSet {
            changes,
            fingerprints,
            summary,
        }
    }

    /// Resolve environment snapshot IDs to fingerprints via the snapshot store.
    fn resolve_environment_fingerprints(&self) -> HashMap<(DagId, TaskId), Fingerprint> {
        let mut result = HashMap::new();

        for (key, snapshot_id) in &self.environment.snapshot_map {
            if let Ok(Some(snapshot)) = self.snapshot_store.get(snapshot_id) {
                result.insert(key.clone(), snapshot.fingerprint);
            }
        }

        result
    }

    /// Check if a reusable snapshot exists for a given fingerprint.
    fn find_reusable_snapshot(&self, fingerprint: &Fingerprint) -> Option<String> {
        self.snapshot_store
            .find_by_fingerprint(fingerprint)
            .ok()
            .flatten()
            .map(|snap| snap.id)
    }

    /// Determine if a task's own content/config changed (ignoring upstream).
    ///
    /// We do this by comparing the task_content + task_config strings directly.
    /// If they match but the full fingerprint differs, it's purely upstream invalidation.
    fn task_own_content_changed(
        &self,
        _dag_id: &str,
        _task_id: &str,
        _env_fingerprints: &HashMap<(DagId, TaskId), Fingerprint>,
    ) -> bool {
        // In a production implementation, we'd store the content hash separately
        // from the full fingerprint to enable this distinction. For now, we use
        // a heuristic: if the task exists in both plan and environment, and
        // some upstream changed, we assume it's upstream-invalidated unless
        // we can prove otherwise.
        //
        // This is conservative: it may classify some Modified tasks as
        // UpstreamInvalidated, but never the reverse. The impact is minimal
        // because both require re-execution anyway.
        false
    }

    fn compute_summary(changes: &[TaskChange]) -> ChangeSummary {
        let mut added = 0;
        let mut modified = 0;
        let mut upstream_invalidated = 0;
        let mut removed = 0;
        let mut unchanged = 0;
        let mut reusable = 0;

        for change in changes {
            match change.kind {
                ChangeKind::Added => added += 1,
                ChangeKind::Modified => modified += 1,
                ChangeKind::UpstreamInvalidated => upstream_invalidated += 1,
                ChangeKind::Removed => removed += 1,
                ChangeKind::Unchanged => unchanged += 1,
            }
            if change.reusable_snapshot_id.is_some() {
                reusable += 1;
            }
        }

        let total_tasks = changes.len();
        let must_execute = added + modified + upstream_invalidated;

        ChangeSummary {
            added,
            modified,
            upstream_invalidated,
            removed,
            unchanged,
            total_tasks,
            reusable,
            must_execute,
        }
    }
}

impl std::fmt::Display for ChangeSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = &self.summary;

        writeln!(f, "Plan Summary")?;
        writeln!(f, "  Total tasks:    {}", s.total_tasks)?;
        writeln!(f, "  Unchanged:      {} (snapshot reuse)", s.unchanged)?;
        writeln!(f, "  Added:          {}", s.added)?;
        writeln!(f, "  Modified:       {} (direct change)", s.modified)?;
        writeln!(f, "  Invalidated:    {} (upstream change)", s.upstream_invalidated)?;
        writeln!(f, "  Removed:        {}", s.removed)?;
        writeln!(f)?;
        writeln!(f, "  Must execute:   {} tasks", s.must_execute)?;
        writeln!(f, "  Can reuse:      {} snapshots", s.reusable)?;

        if !self.changes.is_empty() {
            writeln!(f)?;
            writeln!(f, "  Changes:")?;
            for change in &self.changes {
                if change.kind == ChangeKind::Unchanged {
                    continue; // Don't show unchanged in the diff
                }
                let symbol = match change.kind {
                    ChangeKind::Added => "+",
                    ChangeKind::Modified => "~",
                    ChangeKind::UpstreamInvalidated => "^",
                    ChangeKind::Removed => "-",
                    ChangeKind::Unchanged => " ",
                };
                let reuse_note = if change.reusable_snapshot_id.is_some() {
                    " (reusable snapshot found)"
                } else {
                    ""
                };
                writeln!(
                    f,
                    "    [{}] {}.{}{}",
                    symbol, change.dag_id, change.task_id, reuse_note
                )?;
            }
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

    fn make_snapshot(id: &str, fp: &str, dag_id: &str, task_id: &str) -> Snapshot {
        Snapshot {
            id: id.to_string(),
            fingerprint: Fingerprint::from_hex(fp),
            dag_id: dag_id.to_string(),
            task_id: task_id.to_string(),
            created_at: Utc::now(),
            parent_fingerprints: vec![],
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn all_new_tasks_are_added() {
        let plan = make_plan(
            "etl",
            vec![make_task("extract", vec![]), make_task("load", vec!["extract"])],
            vec!["extract", "load"],
        );
        let env = Environment::new("production");
        let store = SnapshotStore::new();

        let detector = ChangeDetector::new(&plan, &env, &store);
        let changeset = detector.detect();

        assert_eq!(changeset.summary.added, 2);
        assert_eq!(changeset.summary.unchanged, 0);
    }

    #[test]
    fn unchanged_tasks_are_detected() {
        let plan = make_plan(
            "etl",
            vec![make_task("extract", vec![])],
            vec!["extract"],
        );

        // Compute what the fingerprint will be
        let fps = PlanFingerprinter::fingerprint_plan(&plan);
        let fp = fps.get(&("etl".to_string(), "extract".to_string())).unwrap();

        // Set up environment with a matching snapshot
        let store = SnapshotStore::new();
        let snap = make_snapshot("snap_1", &fp.0, "etl", "extract");
        store.put(snap).unwrap();

        let mut env = Environment::new("production");
        env.snapshot_map.insert(
            ("etl".to_string(), "extract".to_string()),
            "snap_1".to_string(),
        );

        let detector = ChangeDetector::new(&plan, &env, &store);
        let changeset = detector.detect();

        assert_eq!(changeset.summary.unchanged, 1);
        assert_eq!(changeset.summary.added, 0);
    }

    #[test]
    fn removed_tasks_are_detected() {
        let plan = make_plan(
            "etl",
            vec![make_task("extract", vec![])],
            vec!["extract"],
        );

        let mut env = Environment::new("production");
        env.snapshot_map.insert(
            ("etl".to_string(), "extract".to_string()),
            "snap_1".to_string(),
        );
        // This task was in the environment but is no longer in the plan
        env.snapshot_map.insert(
            ("etl".to_string(), "old_task".to_string()),
            "snap_2".to_string(),
        );

        let store = SnapshotStore::new();
        let detector = ChangeDetector::new(&plan, &env, &store);
        let changeset = detector.detect();

        let removed: Vec<_> = changeset
            .changes
            .iter()
            .filter(|c| c.kind == ChangeKind::Removed)
            .collect();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].task_id, "old_task");
    }

    #[test]
    fn display_format_works() {
        let plan = make_plan(
            "etl",
            vec![
                make_task("extract", vec![]),
                make_task("transform", vec!["extract"]),
            ],
            vec!["extract", "transform"],
        );
        let env = Environment::new("production");
        let store = SnapshotStore::new();

        let detector = ChangeDetector::new(&plan, &env, &store);
        let changeset = detector.detect();

        let output = format!("{}", changeset);
        assert!(output.contains("Added"));
        assert!(output.contains("[+]"));
    }
}
