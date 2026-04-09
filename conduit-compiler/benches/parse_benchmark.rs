//! Benchmark: DAG parsing and compilation performance.
//!
//! This is the core performance claim: Conduit compiles 1,000 DAGs in <2 seconds.
//! Run with: cargo bench -p conduit-compiler

use conduit_compiler::DagParser;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::fs;
use tempfile::tempdir;

/// Generate a synthetic DAG file with N tasks in a linear chain.
fn generate_dag(dag_id: &str, num_tasks: usize) -> String {
    let mut code = format!(
        r#"from conduit import dag, task, Param

@dag(schedule="0 6 * * *", tags=["bench"], max_active_runs=1)
def {dag_id}(date: Param[str] = "{{{{ ds }}}}"):
    """Benchmark DAG with {num_tasks} tasks."""

"#
    );

    // Generate task definitions
    for i in 0..num_tasks {
        code.push_str(&format!(
            r#"    @task(retries=1, pool="default", timeout="30m")
    def task_{i}({params}):
        """Task {i}."""
        pass

"#,
            params = if i == 0 {
                "date: str".to_string()
            } else {
                format!("input_{}", i - 1)
            }
        ));
    }

    // Generate call chain
    code.push_str(&format!("    result_0 = task_0(date)\n"));
    for i in 1..num_tasks {
        code.push_str(&format!("    result_{i} = task_{i}(result_{})\n", i - 1));
    }

    code
}

/// Generate N DAG files in a temporary directory.
fn generate_dag_files(num_dags: usize, tasks_per_dag: usize) -> tempfile::TempDir {
    let dir = tempdir().unwrap();
    for i in 0..num_dags {
        let code = generate_dag(&format!("dag_{}", i), tasks_per_dag);
        let path = dir.path().join(format!("dag_{}.py", i));
        fs::write(path, code).unwrap();
    }
    dir
}

fn bench_parse_single_dag(c: &mut Criterion) {
    let source = generate_dag("benchmark_dag", 10);

    c.bench_function("parse_single_dag_10_tasks", |b| {
        let mut parser = DagParser::new().unwrap();
        b.iter(|| parser.parse_source(black_box(&source), "bench.py").unwrap())
    });
}

fn bench_parse_directory(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_directory");

    for num_dags in [10, 100, 500, 1000] {
        let dir = generate_dag_files(num_dags, 10);

        group.bench_with_input(BenchmarkId::new("dags", num_dags), &num_dags, |b, _| {
            b.iter(|| {
                let mut parser = DagParser::new().unwrap();
                parser.parse_directory(black_box(dir.path())).unwrap()
            })
        });
    }

    group.finish();
}

fn bench_compile_full(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_compilation");

    for num_dags in [10, 100, 1000] {
        let dir = generate_dag_files(num_dags, 10);

        group.bench_with_input(BenchmarkId::new("dags", num_dags), &num_dags, |b, _| {
            b.iter(|| conduit_compiler::ConduitPlan::compile(black_box(dir.path())).unwrap())
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_parse_single_dag,
    bench_parse_directory,
    bench_compile_full,
);
criterion_main!(benches);
