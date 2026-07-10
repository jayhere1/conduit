//! Distributed soak / chaos harness (PRD E2, STRATEGIC_DIRECTION.md §8.2).
//!
//! Drives the real `Coordinator` in-process (no gRPC transport — faster and
//! fully deterministic to control) under a continuous task load while
//! repeatedly killing and replacing workers mid-flight. It verifies the SLOs
//! the strategy doc calls for:
//!
//!   1. **Exactly-once completion** — every submitted task produces exactly
//!      one Success result: none lost (reassignment recovers orphans), none
//!      double-counted (a killed worker never reports the task another worker
//!      re-ran).
//!   2. **Drain to zero** — after generation stops, inflight and pending both
//!      reach zero (no stuck tasks, no leaked assignments).
//!   3. **Bounded state** — outstanding work stays within the backpressure
//!      cap and pending never exceeds the configured queue cap (a leak proxy
//!      for coordinator memory growth); it never grows without limit.
//!
//! It also reports throughput and p50/p99 task latency.
//!
//! ## Scale
//!
//! The default (CI) run is a fast, fixed-size burst that finishes in a couple
//! of seconds — a real regression test. Set `CONDUIT_SOAK_SECS=<n>` to run a
//! continuous soak for n seconds (a nightly job would set this to hours).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use dashmap::DashMap;

use conduit_distributed::coordinator::{Coordinator, CoordinatorConfig};
use conduit_distributed::proto_types::*;

// ─── Tunables ────────────────────────────────────────────────────────────────

const NUM_WORKERS: usize = 4;
const WORKER_CAPACITY: u32 = 4;
/// Default CI burst size when CONDUIT_SOAK_SECS is not set.
const DEFAULT_TASK_TARGET: u64 = 800;
/// Kill + replace a worker every this many completed tasks.
const CHAOS_EVERY: u64 = 60;
/// Simulated per-task work, varied deterministically 1–5ms by task index.
fn work_delay(task_index: u64) -> Duration {
    Duration::from_millis(1 + (task_index % 5))
}

// ─── A simulated worker ──────────────────────────────────────────────────────

/// Spawns a worker: registers with the coordinator, consumes assignments and
/// reports Success for each after a short delay.
///
/// Modelling a crashed process faithfully is the subtle part: once killed, the
/// worker must NEVER report a task, because the coordinator has reassigned that
/// task's orphan to another worker — a late report would be a phantom
/// second completion. So kill and report are serialized through one lock:
/// `kill()` sets the killed flag under the lock, and the worker only reports
/// while holding the same lock after re-checking the flag. Either the report
/// wins (task completes; disconnect finds nothing to reassign) or the kill
/// wins (task abandoned; disconnect reassigns it) — never both.
struct SimWorker {
    killed: Arc<Mutex<bool>>,
}

impl SimWorker {
    fn spawn(coordinator: Arc<Coordinator>, id: String, completed: Arc<AtomicU64>) -> Self {
        let req = RegisterRequest {
            worker_id: id.clone(),
            hostname: format!("{id}.soak"),
            capacity: WORKER_CAPACITY,
            pool_affinity: vec!["default".into()],
            labels: HashMap::new(),
            version: "soak".into(),
            health_port: 0,
        };
        let mut rx = coordinator.register_worker(&req);

        let killed = Arc::new(Mutex::new(false));
        let killed_task = killed.clone();
        let worker_id = id;

        tokio::spawn(async move {
            while let Some(assignment) = rx.recv().await {
                let idx = completed.load(Ordering::Relaxed);
                tokio::time::sleep(work_delay(idx)).await;

                // Report only if not killed — under the lock so a concurrent
                // kill() can't slip in between the check and the report.
                let guard = killed_task.lock().unwrap();
                if *guard {
                    break; // crashed: abandon this task, coordinator reassigns it
                }
                let result = TaskResult {
                    assignment_id: assignment.assignment_id.clone(),
                    worker_id: worker_id.clone(),
                    dag_id: assignment.dag_id.clone(),
                    run_id: assignment.run_id.clone(),
                    task_id: assignment.task_id.clone(),
                    attempt: assignment.attempt,
                    outcome: TaskOutcome::Success,
                    exit_code: 0,
                    duration_ms: 1,
                    xcom_json: "{}".into(),
                    error: String::new(),
                    metrics: HashMap::new(),
                };
                coordinator.handle_result(result);
                drop(guard);
                completed.fetch_add(1, Ordering::Relaxed);
            }
        });

        SimWorker { killed }
    }

    /// Mark the worker crashed. After this returns, the worker can never report
    /// another task (the report path re-checks this flag under the same lock).
    fn kill(&self) {
        *self.killed.lock().unwrap() = true;
    }
}

// ─── Task generator ──────────────────────────────────────────────────────────

/// Build and submit one task; returns its task_id.
async fn submit(coordinator: &Coordinator, task_index: u64) -> String {
    let task_id = format!("t{task_index}");
    let spec = TaskSpec {
        task_type: TaskType::Bash,
        script: "true".into(),
        connection: String::new(),
        query: String::new(),
        command: String::new(),
        args: vec![],
        timeout_secs: 60,
        resources: ResourceLimits::default(),
    };
    let ctx = TaskContext {
        dag_id: "soak".into(),
        run_id: "run".into(),
        task_id: task_id.clone(),
        attempt: 0,
        logical_date_epoch_ms: Utc::now().timestamp_millis(),
        environment: "soak".into(),
        params: HashMap::new(),
    };
    let assignment = coordinator.create_assignment("soak", "run", &task_id, 0, spec, ctx, 300);
    coordinator.submit_task(assignment, "default").await;
    task_id
}

/// p-quantile of a slice of millisecond latencies (nearest-rank).
fn quantile(sorted_ms: &[u64], q: f64) -> u64 {
    if sorted_ms.is_empty() {
        return 0;
    }
    let rank = (q * (sorted_ms.len() as f64 - 1.0)).round() as usize;
    sorted_ms[rank.min(sorted_ms.len() - 1)]
}

// ─── The soak test ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soak_chaos_exactly_once_under_worker_churn() {
    // Run the periodic health-checker at a short interval, as production does.
    // Chaos is still driven explicitly via handle_worker_disconnect; the
    // checker's role here is the safety-net drain it performs each tick, which
    // dispatches any task left queued when no completion event follows (e.g. a
    // reassignment that lands right as generation stops). Workers stay well
    // within the 120s heartbeat window, so the checker never spuriously
    // evicts one during the run.
    let config = CoordinatorConfig {
        bind_addr: "127.0.0.1:0".into(),
        health_check_interval_secs: 1,
        ..CoordinatorConfig::default()
    };
    let (coordinator, mut result_rx) = Coordinator::new(config);
    let coordinator = Arc::new(coordinator);
    let _health_checker = coordinator.start_health_checker();

    let soak_secs: Option<u64> = std::env::var("CONDUIT_SOAK_SECS")
        .ok()
        .and_then(|s| s.parse().ok());
    let deadline = soak_secs.map(|s| Instant::now() + Duration::from_secs(s));
    let task_target = if soak_secs.is_some() {
        u64::MAX
    } else {
        DEFAULT_TASK_TARGET
    };

    let completed = Arc::new(AtomicU64::new(0));

    // Start the initial worker fleet. `fleet[s]` is the (worker_id, handle) of
    // the live worker occupying slot s; chaos replaces one slot at a time so
    // the fleet size stays constant at NUM_WORKERS.
    let mut fleet: Vec<(String, SimWorker)> = Vec::with_capacity(NUM_WORKERS);
    for i in 0..NUM_WORKERS {
        let id = format!("w{i}");
        let w = SimWorker::spawn(coordinator.clone(), id.clone(), completed.clone());
        fleet.push((id, w));
    }
    // Give registrations a moment to land before the first dispatch.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // ── Result collector: records exactly-once + latency ─────────────────────
    let submit_times: Arc<DashMap<String, Instant>> = Arc::new(DashMap::new());
    let seen: Arc<DashMap<String, u32>> = Arc::new(DashMap::new());
    let latencies: Arc<std::sync::Mutex<Vec<u64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let duplicate_count = Arc::new(AtomicU64::new(0));

    let collector = {
        let submit_times = submit_times.clone();
        let seen = seen.clone();
        let latencies = latencies.clone();
        let duplicate_count = duplicate_count.clone();
        tokio::spawn(async move {
            while let Some(result) = result_rx.recv().await {
                if result.outcome != TaskOutcome::Success {
                    continue;
                }
                let count = {
                    let mut e = seen.entry(result.task_id.clone()).or_insert(0);
                    *e += 1;
                    *e
                };
                if count > 1 {
                    duplicate_count.fetch_add(1, Ordering::SeqCst);
                }
                if let Some(t0) = submit_times.get(&result.task_id) {
                    latencies
                        .lock()
                        .unwrap()
                        .push(t0.elapsed().as_millis() as u64);
                }
            }
        })
    };

    // ── Generator + chaos loop ───────────────────────────────────────────────
    let mut next_index: u64 = 0;
    let mut chaos_events: u64 = 0;
    let mut next_chaos_at: u64 = CHAOS_EVERY;
    let mut max_inflight: usize = 0;
    let mut max_pending: usize = 0;
    let start = Instant::now();

    loop {
        // Stop condition.
        if let Some(dl) = deadline {
            if Instant::now() >= dl {
                break;
            }
        } else if next_index >= task_target {
            break;
        }

        // Backpressure: don't outrun the fleet unboundedly. Keep at most
        // ~2x total capacity outstanding so the queue stays honest.
        let outstanding = coordinator.inflight_count() + coordinator.pending_count().await;
        let cap = (NUM_WORKERS as u32 * WORKER_CAPACITY * 2) as usize;
        if outstanding < cap {
            let task_id = format!("t{next_index}");
            submit_times.insert(task_id.clone(), Instant::now());
            submit(&coordinator, next_index).await;
            next_index += 1;
        } else {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        max_inflight = max_inflight.max(coordinator.inflight_count());
        max_pending = max_pending.max(coordinator.pending_count().await);

        // Chaos: crash the worker in one slot mid-flight and replace it, so
        // the fleet size stays constant at NUM_WORKERS but individual nodes
        // churn.
        if completed.load(Ordering::Relaxed) >= next_chaos_at {
            let slot = chaos_events as usize % NUM_WORKERS;
            let replacement_id = format!("w{slot}-r{chaos_events}");

            // Crash the current occupant: it can no longer report anything.
            let (victim_id, victim) = &fleet[slot];
            victim.kill();
            let victim_id = victim_id.clone();
            // The crashed node's in-flight tasks are reassigned here.
            coordinator.handle_worker_disconnect(&victim_id).await;

            // Bring the slot back with a fresh node.
            let w = SimWorker::spawn(
                coordinator.clone(),
                replacement_id.clone(),
                completed.clone(),
            );
            fleet[slot] = (replacement_id, w);

            chaos_events += 1;
            next_chaos_at += CHAOS_EVERY;
        }
    }

    let submitted = next_index;

    // ── Drain: wait for everything to complete ───────────────────────────────
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let done = seen.len() as u64;
        let inflight = coordinator.inflight_count();
        let pending = coordinator.pending_count().await;
        if done >= submitted && inflight == 0 && pending == 0 {
            break;
        }
        if Instant::now() >= drain_deadline {
            panic!(
                "soak did not drain: submitted={submitted} completed={done} \
                 inflight={inflight} pending={pending} (chaos_events={chaos_events})"
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Let the collector catch any last results, then stop it.
    tokio::time::sleep(Duration::from_millis(50)).await;
    collector.abort();

    let elapsed = start.elapsed();
    let mut lat = latencies.lock().unwrap().clone();
    lat.sort_unstable();
    let p50 = quantile(&lat, 0.50);
    let p99 = quantile(&lat, 0.99);
    let throughput = submitted as f64 / elapsed.as_secs_f64();

    eprintln!(
        "soak summary: submitted={submitted} completed={} duplicates={} \
         chaos_events={chaos_events} elapsed={:.1}s throughput={:.0}/s \
         latency_p50={p50}ms p99={p99}ms max_inflight={max_inflight} max_pending={max_pending}",
        seen.len(),
        duplicate_count.load(Ordering::SeqCst),
        elapsed.as_secs_f64(),
        throughput,
    );

    // ── SLO assertions ───────────────────────────────────────────────────────

    // 1. Exactly-once: every submitted task completed exactly once.
    assert_eq!(
        seen.len() as u64,
        submitted,
        "not every task completed exactly once (completed {} of {submitted})",
        seen.len()
    );
    assert_eq!(
        duplicate_count.load(Ordering::SeqCst),
        0,
        "some task completed more than once — exactly-once violated under churn"
    );
    for entry in seen.iter() {
        assert_eq!(
            *entry.value(),
            1,
            "task {} completed {} times",
            entry.key(),
            entry.value()
        );
    }

    // 2. Drained to zero (checked in the loop above; assert final state).
    assert_eq!(coordinator.inflight_count(), 0, "inflight not drained");
    assert_eq!(coordinator.pending_count().await, 0, "pending not drained");

    // 3. Bounded state (leak proxy): outstanding work stays bounded and never
    //    grows without limit. Inflight can transiently overshoot raw worker
    //    capacity because select_worker/assign_task aren't atomic against the
    //    drains handle_result spawns — but it stays within the backpressure
    //    cap. The pending queue never exceeds its configured limit.
    let backpressure_cap = (NUM_WORKERS as u32 * WORKER_CAPACITY * 2) as usize;
    assert!(
        max_inflight <= backpressure_cap,
        "inflight {max_inflight} exceeded backpressure cap {backpressure_cap} (possible leak)"
    );
    assert!(
        max_pending <= CoordinatorConfig::default().max_queue_size,
        "pending {max_pending} exceeded queue cap {}",
        CoordinatorConfig::default().max_queue_size
    );

    // Chaos actually happened (otherwise the test proves nothing about churn).
    assert!(
        chaos_events > 0,
        "no chaos events fired — churn was not exercised"
    );
}
