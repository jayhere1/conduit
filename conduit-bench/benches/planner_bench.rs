use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use conduit_bench::generate_dag;
use conduit_compiler::ConduitPlan;
use conduit_planner::PlanFingerprinter;
use conduit_planner::ImpactAnalyzer;
use chrono::Utc;
use std::collections::HashMap;

fn make_plan(dag_id: &str, n_tasks: usize) -> ConduitPlan {
    let dag = generate_dag(dag_id, n_tasks);
    let total_tasks = dag.tasks.len();
    let mut dags = HashMap::new();
    dags.insert(dag.id.clone(), dag);
    ConduitPlan {
        dags,
        compiled_at: Utc::now(),
        compilation_time_ms: 0,
        total_tasks,
        warnings: vec![],
    }
}

fn fingerprint_100_tasks(c: &mut Criterion) {
    c.bench_function("fingerprint_100_tasks", |b| {
        b.iter(|| {
            let plan = black_box(make_plan("fp_dag_100", 100));
            let _fingerprints = PlanFingerprinter::fingerprint_plan(&plan);
        });
    });
}

fn fingerprint_1000_tasks(c: &mut Criterion) {
    c.bench_function("fingerprint_1000_tasks", |b| {
        b.iter(|| {
            let plan = black_box(make_plan("fp_dag_1000", 1000));
            let _fingerprints = PlanFingerprinter::fingerprint_plan(&plan);
        });
    });
}

fn fingerprint_10000_tasks(c: &mut Criterion) {
    c.bench_function("fingerprint_10000_tasks", |b| {
        b.iter(|| {
            let plan = black_box(make_plan("fp_dag_10000", 10000));
            let _fingerprints = PlanFingerprinter::fingerprint_plan(&plan);
        });
    });
}

fn fingerprint_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("fingerprint_scaling");

    for n_tasks in [100, 500, 1000, 5000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_tasks),
            n_tasks,
            |b, &n_tasks| {
                b.iter(|| {
                    let plan = black_box(make_plan(&format!("fp_dag_{}", n_tasks), n_tasks));
                    let _fingerprints = PlanFingerprinter::fingerprint_plan(&plan);
                });
            },
        );
    }
    group.finish();
}

fn impact_analysis_wide_dag(c: &mut Criterion) {
    c.bench_function("impact_analysis_wide_dag", |b| {
        b.iter(|| {
            let plan = black_box(make_plan("ia_wide", 1000));
            // Analyze impact of changing the root task
            let changed_tasks = vec![("ia_wide".to_string(), "root".to_string())];
            let _report = ImpactAnalyzer::analyze(&plan, &changed_tasks);
        });
    });
}

fn impact_analysis_deep_dag(c: &mut Criterion) {
    c.bench_function("impact_analysis_deep_dag", |b| {
        b.iter(|| {
            let plan = black_box(make_plan("ia_deep", 1000));
            // Analyze impact of changing the root task
            let changed_tasks = vec![("ia_deep".to_string(), "root".to_string())];
            let _report = ImpactAnalyzer::analyze(&plan, &changed_tasks);
        });
    });
}

criterion_group!(
    benches,
    fingerprint_100_tasks,
    fingerprint_1000_tasks,
    fingerprint_10000_tasks,
    fingerprint_scaling,
    impact_analysis_wide_dag,
    impact_analysis_deep_dag,
);
criterion_main!(benches);
