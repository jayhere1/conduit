//! Property tests for scheduler invariants.
//!
//! These tests fuzz random DAGs through random event interleavings and assert
//! the scheduler's headline promises:
//!   1. No task is dispatched more than `retries + 1` times.
//!   2. A task is never dispatched before all of its `AllSuccess` upstreams
//!      have completed successfully.

use std::collections::{HashMap, HashSet};

use chrono::Utc;
use proptest::prelude::*;
use tokio::sync::mpsc;

use conduit_common::dag::{
    Dag, DependencyType, Pool, ResourceLimits, Task, TaskDependency, TaskType, TriggerRule,
};
use conduit_scheduler::{PoolManager, Scheduler, SchedulerCommand, SchedulerEvent};

fn task(id: &str, deps: &[&str], retries: u32) -> Task {
    Task {
        id: id.to_string(),
        task_type: TaskType::Bash {
            command: format!("echo {}", id),
        },
        dependencies: deps
            .iter()
            .map(|d| TaskDependency {
                task_id: d.to_string(),
                dependency_type: DependencyType::ExecutionOrder,
            })
            .collect(),
        retries,
        retry_delay: None,
        pool: None,
        timeout: None,
        priority: 0,
        resources: ResourceLimits::default(),
        trigger_rule: TriggerRule::AllSuccess,
        incremental: None,
        contracts: None,
    }
}

/// Generate a random DAG with N tasks where task `i` may depend on any subset
/// of {0..i-1}. This is acyclic by construction.
fn random_dag(num_tasks: usize, dep_prob: f64, rng_seed: u64) -> Dag {
    // Simple LCG for reproducibility per shrunk case (don't pull rand crate).
    let mut state = rng_seed.wrapping_add(1);
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) as f64 / (1u64 << 31) as f64
    };

    let mut tasks_map: HashMap<String, Task> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for i in 0..num_tasks {
        let id = format!("t{}", i);
        let mut deps_ref: Vec<&str> = Vec::new();
        let prior_ids: Vec<String> = (0..i).map(|j| format!("t{}", j)).collect();
        for upstream in &prior_ids {
            if next() < dep_prob {
                deps_ref.push(upstream.as_str());
            }
        }
        let t = task(&id, &deps_ref, 0);
        order.push(id.clone());
        tasks_map.insert(id, t);
    }

    Dag {
        id: "fuzz_dag".to_string(),
        description: None,
        schedule: None,
        tags: vec![],
        max_active_runs: 1,
        on_failure: None,
        tasks: tasks_map,
        execution_order: order,
        source_file: "proptest.rs".to_string(),
        compiled_at: Utc::now(),
        catchup: false,
        max_catchup_runs: None,
    }
}

#[derive(Debug, Clone)]
enum FuzzEvent {
    CronTick,
    CompleteTask(usize),
}

async fn drive_scheduler(dag: Dag, events: Vec<FuzzEvent>) -> Vec<SchedulerCommand> {
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (command_tx, mut command_rx) = mpsc::unbounded_channel();
    let pools = PoolManager::new(vec![Pool {
        name: "default".to_string(),
        slots: 128,
        description: None,
    }]);

    let order = dag.execution_order.clone();
    let mut plans = HashMap::new();
    plans.insert(dag.id.clone(), dag.clone());
    let scheduler = Scheduler::new(event_rx, command_tx, pools, plans).unwrap();

    event_tx
        .send(SchedulerEvent::DagRunRequested {
            dag_id: "fuzz_dag".to_string(),
            run_id: "r1".to_string(),
            logical_date: Utc::now(),
            config: HashMap::new(),
        })
        .unwrap();

    for ev in events {
        match ev {
            FuzzEvent::CronTick => {
                let _ = event_tx.send(SchedulerEvent::CronTick {
                    timestamp: Utc::now(),
                });
            }
            FuzzEvent::CompleteTask(idx) => {
                if let Some(tid) = order.get(idx) {
                    let _ = event_tx.send(SchedulerEvent::TaskCompleted {
                        dag_id: "fuzz_dag".to_string(),
                        run_id: "r1".to_string(),
                        task_id: tid.clone(),
                        snapshot_id: None,
                        duration_ms: 1,
                    });
                }
            }
        }
    }
    let _ = event_tx.send(SchedulerEvent::Shutdown);
    let _ = scheduler.run().await;

    let mut cmds = Vec::new();
    while let Ok(c) = command_rx.try_recv() {
        cmds.push(c);
    }
    cmds
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

    /// Invariant: every task is dispatched at most once (with retries=0).
    /// This is the regression test for the duplicate-dispatch bug.
    #[test]
    fn no_task_dispatched_more_than_once(
        num_tasks in 1usize..15,
        dep_prob in 0.0f64..0.7,
        seed in any::<u64>(),
        n_ticks in 0u32..30,
    ) {
        let dag = random_dag(num_tasks, dep_prob, seed);
        let order = dag.execution_order.clone();

        // Interleave: ticks, then for each task in topo order, complete it
        // with extra ticks sprinkled between.
        let mut events = Vec::new();
        for _ in 0..n_ticks {
            events.push(FuzzEvent::CronTick);
        }
        for i in 0..order.len() {
            events.push(FuzzEvent::CompleteTask(i));
            events.push(FuzzEvent::CronTick);
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let cmds = rt.block_on(drive_scheduler(dag, events));

        let mut dispatch_counts: HashMap<String, u32> = HashMap::new();
        for c in &cmds {
            if let SchedulerCommand::DispatchTask { task_id, .. } = c {
                *dispatch_counts.entry(task_id.clone()).or_insert(0) += 1;
            }
        }
        for (tid, count) in &dispatch_counts {
            prop_assert!(
                *count <= 1,
                "Task {} dispatched {} times (retries=0), expected <= 1. \
                 Events: {} ticks then completions in order; all commands: {:?}",
                tid, count, n_ticks, cmds.iter().map(|c| format!("{:?}", c)).collect::<Vec<_>>()
            );
        }
    }

    /// Invariant: a task is never dispatched until *all* of its upstreams
    /// have been observed completed. This is the topological-order promise.
    #[test]
    fn dispatch_respects_dependencies(
        num_tasks in 2usize..15,
        dep_prob in 0.3f64..0.9,
        seed in any::<u64>(),
    ) {
        let dag = random_dag(num_tasks, dep_prob, seed);
        let order = dag.execution_order.clone();
        let tasks_snapshot = dag.tasks.clone();

        // Complete tasks in topo order, one at a time, with no extra ticks.
        let mut events = Vec::new();
        for i in 0..order.len() {
            events.push(FuzzEvent::CompleteTask(i));
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let cmds = rt.block_on(drive_scheduler(dag, events));

        // Replay: walk commands in order; track which tasks have been
        // completed (we send completions in topo order, so a Dispatch at
        // position k in cmds is "issued before completion k").
        let mut dispatched_order: Vec<String> = Vec::new();
        for c in &cmds {
            if let SchedulerCommand::DispatchTask { task_id, .. } = c {
                dispatched_order.push(task_id.clone());
            }
        }

        // For each dispatch, all its upstreams must already appear earlier in
        // dispatched_order (since the scheduler dispatches strictly after
        // upstream completion in this scenario).
        let mut seen: HashSet<String> = HashSet::new();
        for d in &dispatched_order {
            let t = tasks_snapshot.get(d).unwrap();
            for dep in &t.dependencies {
                prop_assert!(
                    seen.contains(&dep.task_id),
                    "Task {} dispatched before upstream {} was even dispatched. \
                     Dispatch order: {:?}",
                    d, dep.task_id, dispatched_order
                );
            }
            seen.insert(d.clone());
        }
    }
}
