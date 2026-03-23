use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use conduit_state::EventStore;
use conduit_common::event::EventKind;
use tempfile::TempDir;

fn event_store_append(c: &mut Criterion) {
    c.bench_function("event_store_append_100", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                EventStore::open(dir.path()).unwrap()
            },
            |store| {
                for i in 0..100 {
                    let event = EventKind::TaskQueued {
                        dag_id: "bench_dag".to_string(),
                        run_id: format!("run_{}", i),
                        task_id: format!("task_{}", i),
                    };
                    black_box(store.append(event).unwrap());
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn event_store_append_1000(c: &mut Criterion) {
    c.bench_function("event_store_append_1000", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                EventStore::open(dir.path()).unwrap()
            },
            |store| {
                for i in 0..1000 {
                    let event = EventKind::TaskStarted {
                        dag_id: "bench_dag".to_string(),
                        run_id: format!("run_{}", i % 100),
                        task_id: format!("task_{}", i),
                        worker_id: "bench_worker".to_string(),
                        attempt: 1,
                        pid: None,
                    };
                    black_box(store.append(event).unwrap());
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn event_store_range_query(c: &mut Criterion) {
    c.bench_function("event_store_range_query", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let store = EventStore::open(dir.path()).unwrap();

                // Seed with 1000 events
                for i in 0..1000 {
                    let event = EventKind::TaskCompleted {
                        dag_id: "bench_dag".to_string(),
                        run_id: format!("run_{}", i % 50),
                        task_id: format!("task_{}", i),
                        duration_ms: 100,
                        snapshot_id: None,
                    };
                    store.append(event).unwrap();
                }

                (store, dir)
            },
            |(store, _dir)| {
                // Query middle 200 events
                let events = black_box(store.range(400, 600).unwrap());
                assert_eq!(events.len(), 201);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn event_store_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_store_append_scaling");

    for n_events in [100, 500, 1000, 5000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_events),
            n_events,
            |b, &n_events| {
                b.iter_batched(
                    || {
                        let dir = TempDir::new().unwrap();
                        EventStore::open(dir.path()).unwrap()
                    },
                    |store| {
                        for i in 0..n_events {
                            let event = EventKind::TaskQueued {
                                dag_id: "bench_dag".to_string(),
                                run_id: format!("run_{}", i % 100),
                                task_id: format!("task_{}", i),
                            };
                            black_box(store.append(event).unwrap());
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn event_store_mixed_event_types(c: &mut Criterion) {
    c.bench_function("event_store_mixed_events_500", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                EventStore::open(dir.path()).unwrap()
            },
            |store| {
                for i in 0..500 {
                    let event = match i % 5 {
                        0 => EventKind::DagRunCreated {
                            dag_id: format!("dag_{}", i % 10),
                            run_id: format!("run_{}", i),
                            logical_date: chrono::Utc::now(),
                            environment: "production".to_string(),
                            triggered_by: "benchmark".to_string(),
                        },
                        1 => EventKind::TaskQueued {
                            dag_id: format!("dag_{}", i % 10),
                            run_id: format!("run_{}", i),
                            task_id: format!("task_{}", i),
                        },
                        2 => EventKind::TaskStarted {
                            dag_id: format!("dag_{}", i % 10),
                            run_id: format!("run_{}", i),
                            task_id: format!("task_{}", i),
                            worker_id: "bench_worker".to_string(),
                            attempt: 1,
                            pid: None,
                        },
                        3 => EventKind::TaskCompleted {
                            dag_id: format!("dag_{}", i % 10),
                            run_id: format!("run_{}", i),
                            task_id: format!("task_{}", i),
                            duration_ms: 42,
                            snapshot_id: None,
                        },
                        _ => EventKind::TaskFailed {
                            dag_id: format!("dag_{}", i % 10),
                            run_id: format!("run_{}", i),
                            task_id: format!("task_{}", i),
                            error: "benchmark failure".to_string(),
                            traceback: None,
                            attempt: 1,
                        },
                    };
                    black_box(store.append(event).unwrap());
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    event_store_append,
    event_store_append_1000,
    event_store_range_query,
    event_store_scaling,
    event_store_mixed_event_types,
);
criterion_main!(benches);
