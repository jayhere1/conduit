//! Property tests for the DAG compiler/resolver invariants.
//!
//! Invariant: for any acyclic random DAG, the resolved `execution_order` is
//! a valid topological sort — every task appears after all of its raw
//! dependencies.

use proptest::prelude::*;

use conduit_common::dag::TaskType;
use conduit_compiler::{
    parser::{ParsedDag, ParsedTask},
    resolver::DependencyResolver,
};

fn make_task(id: &str, deps: &[String]) -> ParsedTask {
    ParsedTask {
        id: id.to_string(),
        task_type: TaskType::Python {
            module: "m".to_string(),
            function: id.to_string(),
        },
        retries: 0,
        retry_delay: None,
        retry_backoff: None,
        source_hash: None,
        pool: None,
        timeout: None,
        priority: 0,
        raw_dependencies: deps.to_vec(),
        contracts: None,
        incremental: None,
        parameters_text: String::new(),
        inputs: Vec::new(),
        outputs: Vec::new(),
    }
}

/// Build an acyclic ParsedDag with `num_tasks` tasks where task i may depend
/// on any subset of {0..i-1}. Seed-deterministic.
fn random_parsed_dag(num_tasks: usize, dep_prob: f64, seed: u64) -> ParsedDag {
    let mut state = seed.wrapping_add(1);
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) as f64 / (1u64 << 31) as f64
    };

    let mut tasks = Vec::new();
    for i in 0..num_tasks {
        let id = format!("t{}", i);
        let mut deps = Vec::new();
        for j in 0..i {
            if next() < dep_prob {
                deps.push(format!("t{}", j));
            }
        }
        tasks.push(make_task(&id, &deps));
    }

    ParsedDag {
        id: "fuzz_dag".to_string(),
        description: None,
        schedule: None,
        tags: vec![],
        max_active_runs: 1,
        on_failure: None,
        tasks,
        source_file: "proptest.rs".to_string(),
        lineage_strict: false,
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, .. ProptestConfig::default() })]

    /// Resolved execution_order is a valid topological sort: every task
    /// appears after all of its dependencies.
    #[test]
    fn execution_order_respects_dependencies(
        num_tasks in 1usize..30,
        dep_prob in 0.0f64..0.6,
        seed in any::<u64>(),
    ) {
        let parsed = random_parsed_dag(num_tasks, dep_prob, seed);
        let dag = DependencyResolver::resolve(parsed).map_err(|e| {
            TestCaseError::fail(format!("Resolver failed on acyclic input: {:?}", e))
        })?;

        // For each task in execution_order, every dep.task_id must already
        // appear earlier in the order.
        let mut seen = std::collections::HashSet::new();
        for task_id in &dag.execution_order {
            let t = dag.tasks.get(task_id)
                .ok_or_else(|| TestCaseError::fail(format!("missing task {}", task_id)))?;
            for dep in &t.dependencies {
                prop_assert!(
                    seen.contains(&dep.task_id),
                    "Task {} comes before its dep {} in execution_order {:?}",
                    task_id, dep.task_id, dag.execution_order
                );
            }
            seen.insert(task_id.clone());
        }
    }
}
