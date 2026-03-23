use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use conduit_lineage::{LineageGraph, ColumnRef, LineageEdge};
use conduit_lineage::lineage_graph::TransformType;

/// Generate a lineage graph with a chain of n_tasks, each with n_columns columns.
/// Each column passes through Direct transforms between adjacent tasks.
fn generate_chain_graph(n_tasks: usize, n_columns: usize) -> LineageGraph {
    let mut graph = LineageGraph::new();

    for task_idx in 0..n_tasks.saturating_sub(1) {
        let source_task = format!("task_{}", task_idx);
        let target_task = format!("task_{}", task_idx + 1);

        for col_idx in 0..n_columns {
            let col_name = format!("col_{}", col_idx);
            let source = ColumnRef::new(&source_task, &col_name);
            let target = ColumnRef::new(&target_task, &col_name);
            graph.add_edge(source, target, TransformType::Direct);
        }
    }

    graph
}

/// Generate a wide lineage graph: single task with fan-out to n_targets.
fn generate_fan_out_graph(n_targets: usize, n_columns: usize) -> LineageGraph {
    let mut graph = LineageGraph::new();

    let source_task = "source".to_string();
    for target_idx in 0..n_targets {
        let target_task = format!("target_{}", target_idx);
        for col_idx in 0..n_columns {
            let col_name = format!("col_{}", col_idx);
            let source = ColumnRef::new(&source_task, &col_name);
            let target = ColumnRef::new(&target_task, &col_name);
            let transform = if col_idx % 3 == 0 {
                TransformType::Aggregation("SUM".to_string())
            } else if col_idx % 3 == 1 {
                TransformType::Cast
            } else {
                TransformType::Direct
            };
            graph.add_edge(source, target, transform);
        }
    }

    graph
}

fn lineage_build_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("lineage_build_chain");

    for (n_tasks, n_cols) in [(10, 20), (50, 10), (100, 10), (200, 5)] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}tasks_{}cols", n_tasks, n_cols)),
            &(n_tasks, n_cols),
            |b, &(n_tasks, n_cols)| {
                b.iter(|| {
                    black_box(generate_chain_graph(n_tasks, n_cols));
                });
            },
        );
    }
    group.finish();
}

fn lineage_trace_upstream(c: &mut Criterion) {
    let mut group = c.benchmark_group("lineage_trace_upstream");

    for n_tasks in [10, 50, 100, 500] {
        let graph = generate_chain_graph(n_tasks, 10);
        // Trace from the last task's first column back to source
        let target = ColumnRef::new(format!("task_{}", n_tasks - 1), "col_0");

        group.bench_with_input(
            BenchmarkId::from_parameter(n_tasks),
            &n_tasks,
            |b, _| {
                b.iter(|| {
                    let trace = black_box(graph.trace_upstream(&target));
                    assert!(!trace.columns.is_empty());
                });
            },
        );
    }
    group.finish();
}

fn lineage_trace_downstream(c: &mut Criterion) {
    let mut group = c.benchmark_group("lineage_trace_downstream");

    for n_targets in [10, 50, 100, 500] {
        let graph = generate_fan_out_graph(n_targets, 10);
        let source = ColumnRef::new("source", "col_0");

        group.bench_with_input(
            BenchmarkId::from_parameter(n_targets),
            &n_targets,
            |b, _| {
                b.iter(|| {
                    let trace = black_box(graph.trace_downstream(&source));
                    assert!(!trace.columns.is_empty());
                });
            },
        );
    }
    group.finish();
}

fn lineage_add_edges(c: &mut Criterion) {
    let mut group = c.benchmark_group("lineage_add_edges");

    for n_edges in [100, 1000, 5000, 10000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_edges),
            &n_edges,
            |b, &n_edges| {
                b.iter(|| {
                    let mut graph = LineageGraph::new();
                    for i in 0..n_edges {
                        let source = ColumnRef::new(format!("t_{}", i / 10), format!("c_{}", i % 10));
                        let target = ColumnRef::new(format!("t_{}", i / 10 + 1), format!("c_{}", i % 10));
                        graph.add_edge(source, target, TransformType::Direct);
                    }
                    black_box(graph);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    lineage_build_chain,
    lineage_trace_upstream,
    lineage_trace_downstream,
    lineage_add_edges,
);
criterion_main!(benches);
