use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::collections::HashMap;
use conduit_bench::generate_dag;
use conduit_common::dag::Pool;
use conduit_scheduler::PoolManager;
use tokio::sync::mpsc;

fn scheduler_100_tasks(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("schedule_100_tasks", |b| {
        b.to_async(&rt).iter(|| async {
            let dag = black_box(generate_dag("dag_100", 100));
            let plans = {
                let mut m = HashMap::new();
                m.insert(dag.id.clone(), dag);
                m
            };

            let pools = PoolManager::new(vec![Pool {
                name: "default".to_string(),
                slots: 128,
                description: None,
            }]);
            let (_event_tx, event_rx) = mpsc::unbounded_channel();
            let (_cmd_tx, _cmd_rx) = mpsc::unbounded_channel();

            // Create scheduler
            let scheduler = conduit_scheduler::Scheduler::new(event_rx, _cmd_tx, pools, plans);

            // Verify scheduler was created
            assert!(scheduler.is_ok());
        });
    });
}

fn scheduler_1000_tasks(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("schedule_1000_tasks", |b| {
        b.to_async(&rt).iter(|| async {
            let dag = black_box(generate_dag("dag_1000", 1000));
            let plans = {
                let mut m = HashMap::new();
                m.insert(dag.id.clone(), dag);
                m
            };

            let pools = PoolManager::new(vec![Pool {
                name: "default".to_string(),
                slots: 128,
                description: None,
            }]);
            let (_event_tx, event_rx) = mpsc::unbounded_channel();
            let (_cmd_tx, _cmd_rx) = mpsc::unbounded_channel();

            let scheduler = conduit_scheduler::Scheduler::new(event_rx, _cmd_tx, pools, plans);
            assert!(scheduler.is_ok());
        });
    });
}

fn scheduler_10000_tasks(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("schedule_10000_tasks", |b| {
        b.to_async(&rt).iter(|| async {
            let dag = black_box(generate_dag("dag_10000", 10000));
            let plans = {
                let mut m = HashMap::new();
                m.insert(dag.id.clone(), dag);
                m
            };

            let pools = PoolManager::new(vec![Pool {
                name: "default".to_string(),
                slots: 128,
                description: None,
            }]);
            let (_event_tx, event_rx) = mpsc::unbounded_channel();
            let (_cmd_tx, _cmd_rx) = mpsc::unbounded_channel();

            let scheduler = conduit_scheduler::Scheduler::new(event_rx, _cmd_tx, pools, plans);
            assert!(scheduler.is_ok());
        });
    });
}

fn scheduler_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("scheduler_scaling");

    for n_tasks in [100, 500, 1000, 5000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_tasks),
            n_tasks,
            |b, &n_tasks| {
                b.to_async(&rt).iter(|| async {
                    let dag = black_box(generate_dag(&format!("dag_{}", n_tasks), n_tasks));
                    let plans = {
                        let mut m = HashMap::new();
                        m.insert(dag.id.clone(), dag);
                        m
                    };

                    let pools = PoolManager::new(vec![Pool {
                        name: "default".to_string(),
                        slots: 128,
                        description: None,
                    }]);
                    let (_event_tx, event_rx) = mpsc::unbounded_channel();
                    let (_cmd_tx, _cmd_rx) = mpsc::unbounded_channel();

                    let scheduler =
                        conduit_scheduler::Scheduler::new(event_rx, _cmd_tx, pools, plans);
                    assert!(scheduler.is_ok());
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    scheduler_100_tasks,
    scheduler_1000_tasks,
    scheduler_10000_tasks,
    scheduler_scaling
);
criterion_main!(benches);
