use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use conduit_bench::generate_dag_files;
use conduit_compiler::ConduitPlan;
use tempfile::TempDir;

fn compile_10_dags(c: &mut Criterion) {
    c.bench_function("compile_10_dags", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                generate_dag_files(dir.path(), 10, 100).unwrap();
                dir
            },
            |dir| {
                let (plan, _stats) = black_box(ConduitPlan::compile(dir.path())).unwrap();
                assert_eq!(plan.dags.len(), 10);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn compile_100_dags(c: &mut Criterion) {
    c.bench_function("compile_100_dags", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                generate_dag_files(dir.path(), 100, 50).unwrap();
                dir
            },
            |dir| {
                let (plan, _stats) = black_box(ConduitPlan::compile(dir.path())).unwrap();
                assert_eq!(plan.dags.len(), 100);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn compile_1000_dags(c: &mut Criterion) {
    c.bench_function("compile_1000_dags", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                generate_dag_files(dir.path(), 1000, 10).unwrap();
                dir
            },
            |dir| {
                let (plan, _stats) = black_box(ConduitPlan::compile(dir.path())).unwrap();
                assert_eq!(plan.dags.len(), 1000);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn compiler_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("compiler_scaling");

    for n_dags in [10, 50, 100, 500, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_dags),
            n_dags,
            |b, &n_dags| {
                b.iter_batched(
                    || {
                        let dir = TempDir::new().unwrap();
                        let tasks_per_dag = 100 / (n_dags / 10).max(1); // Adjust task count
                        generate_dag_files(dir.path(), n_dags, tasks_per_dag).unwrap();
                        dir
                    },
                    |dir| {
                        let (plan, _stats) =
                            black_box(ConduitPlan::compile(dir.path())).unwrap();
                        assert_eq!(plan.dags.len(), n_dags);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, compile_10_dags, compile_100_dags, compile_1000_dags, compiler_scaling);
criterion_main!(benches);
