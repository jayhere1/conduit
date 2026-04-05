//! Prometheus metrics for Conduit.
//!
//! Provides a global metrics registry that can be initialized once at startup
//! and accessed from any crate. Metrics are exported in Prometheus text format
//! via the `/metrics` endpoint.

use std::sync::OnceLock;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

// ─── Label Sets ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DagStatusLabels {
    pub dag_id: String,
    pub status: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StatusLabels {
    pub status: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct EventLabels {
    pub event_type: String,
}

// ─── Duration Buckets ────────────────────────────────────────────────────

const TASK_DURATION_BUCKETS: [f64; 12] = [
    0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
];

// ─── Global Metrics ──────────────────────────────────────────────────────

static METRICS: OnceLock<ConduitMetrics> = OnceLock::new();

/// Initialize the global metrics registry. Safe to call multiple times;
/// only the first call takes effect.
pub fn init() -> &'static ConduitMetrics {
    METRICS.get_or_init(ConduitMetrics::new)
}

/// Get the global metrics registry. Initializes lazily if needed.
pub fn global() -> &'static ConduitMetrics {
    init()
}

/// Try to get the global metrics, returning None if not initialized.
pub fn try_global() -> Option<&'static ConduitMetrics> {
    METRICS.get()
}

// ─── Metrics Struct ──────────────────────────────────────────────────────

pub struct ConduitMetrics {
    registry: Registry,

    // ── DAG Runs ─────────────────────────────────────────────
    /// Total DAG runs by dag_id and status (created/completed/failed).
    pub dag_runs_total: Family<DagStatusLabels, Counter>,
    /// Currently active DAG runs.
    pub active_dag_runs: Gauge,

    // ── Tasks ────────────────────────────────────────────────
    /// Total tasks by status (dispatched/completed/failed/retried/skipped).
    pub tasks_total: Family<StatusLabels, Counter>,
    /// Currently running tasks.
    pub active_tasks: Gauge,
    /// Task execution duration in seconds.
    pub task_duration_seconds: Histogram,

    // ── Scheduler ────────────────────────────────────────────
    /// Scheduler events processed by type.
    pub scheduler_events_total: Family<EventLabels, Counter>,
    /// Errors sending commands to the executor.
    pub command_send_errors_total: Counter,
    /// Total cron ticks processed.
    pub cron_ticks_total: Counter,
    /// Catchup runs scheduled on startup.
    pub catchup_runs_total: Counter,

    // ── Executor ─────────────────────────────────────────────
    /// Tasks deferred due to concurrency limits.
    pub executor_deferred_total: Counter,
    /// Task timeouts.
    pub task_timeouts_total: Counter,
}

impl ConduitMetrics {
    fn new() -> Self {
        let mut registry = Registry::default();

        let dag_runs_total = Family::<DagStatusLabels, Counter>::default();
        registry.register(
            "conduit_dag_runs_total",
            "Total DAG runs by dag and status",
            dag_runs_total.clone(),
        );

        let active_dag_runs = Gauge::default();
        registry.register(
            "conduit_active_dag_runs",
            "Currently active DAG runs",
            active_dag_runs.clone(),
        );

        let tasks_total = Family::<StatusLabels, Counter>::default();
        registry.register(
            "conduit_tasks_total",
            "Total tasks by status",
            tasks_total.clone(),
        );

        let active_tasks = Gauge::default();
        registry.register(
            "conduit_active_tasks",
            "Currently running tasks",
            active_tasks.clone(),
        );

        let task_duration_seconds = Histogram::new(TASK_DURATION_BUCKETS.into_iter());
        registry.register(
            "conduit_task_duration_seconds",
            "Task execution duration in seconds",
            task_duration_seconds.clone(),
        );

        let scheduler_events_total = Family::<EventLabels, Counter>::default();
        registry.register(
            "conduit_scheduler_events_total",
            "Scheduler events processed by type",
            scheduler_events_total.clone(),
        );

        let command_send_errors_total = Counter::default();
        registry.register(
            "conduit_command_send_errors_total",
            "Errors sending commands to executor",
            command_send_errors_total.clone(),
        );

        let cron_ticks_total = Counter::default();
        registry.register(
            "conduit_cron_ticks_total",
            "Total cron ticks processed",
            cron_ticks_total.clone(),
        );

        let catchup_runs_total = Counter::default();
        registry.register(
            "conduit_catchup_runs_total",
            "Catchup runs scheduled on startup",
            catchup_runs_total.clone(),
        );

        let executor_deferred_total = Counter::default();
        registry.register(
            "conduit_executor_deferred_total",
            "Tasks deferred due to concurrency limits",
            executor_deferred_total.clone(),
        );

        let task_timeouts_total = Counter::default();
        registry.register(
            "conduit_task_timeouts_total",
            "Task execution timeouts",
            task_timeouts_total.clone(),
        );

        Self {
            registry,
            dag_runs_total,
            active_dag_runs,
            tasks_total,
            active_tasks,
            task_duration_seconds,
            scheduler_events_total,
            command_send_errors_total,
            cron_ticks_total,
            catchup_runs_total,
            executor_deferred_total,
            task_timeouts_total,
        }
    }

    /// Encode all metrics in Prometheus text exposition format.
    pub fn encode(&self) -> String {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry).unwrap_or_default();
        buffer
    }
}
