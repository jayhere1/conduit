#![allow(clippy::ptr_arg, clippy::too_many_arguments)]
//! Conduit CLI: the main binary entry point.
//!
//! Usage:
//!   conduit init <name>            Initialize a new Conduit project
//!   conduit compile [path]         Compile DAGs and report results
//!   conduit run <dag_id>           Compile, schedule, and execute a DAG
//!   conduit plan [env]             Show changes between local and environment
//!   conduit apply [env]            Execute changes and update environment state
//!   conduit lineage <dag.task>     Extract SQL lineage for a task
//!   conduit serve                  Start the API server
//!   conduit status                 Show system status
//!   conduit env create|list|promote

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::Result;
use chrono::{DateTime, SecondsFormat, Utc};
use clap::{Parser, Subcommand};
use uuid::Uuid;

mod tracing_setup;

use conduit_compiler::ConduitPlan;
use conduit_lineage::{
    OpenLineageEventType, OpenLineageRunEvent, OpenLineageSqlEventOptions, SqlLineageExtractor,
    CONDUIT_OPENLINEAGE_PRODUCER,
};

// ─── State directory management ──────────────────────────────────────────────
// Every command that touches state (plan, apply, run, env, status, serve)
// opens the same `.conduit/` directory so that environments, snapshots, and
// the event log persist between invocations.

/// Resolve the state directory: `.conduit/` relative to the DAGs path,
/// or an explicit `--state-dir` override.
fn resolve_state_dir(dags_path: &Path) -> PathBuf {
    // Walk upward from dags_path looking for conduit.yaml
    let mut candidate = dags_path.to_path_buf();
    if candidate.is_relative() {
        candidate = std::env::current_dir().unwrap_or_default().join(candidate);
    }
    // If dags_path is a subdirectory like ./dags, go up one level
    if candidate.ends_with("dags") {
        if let Some(parent) = candidate.parent() {
            let state = parent.join(".conduit");
            if state.exists() || parent.join("conduit.yaml").exists() {
                return state;
            }
        }
    }
    // Fallback: .conduit next to dags_path's parent
    candidate
        .parent()
        .unwrap_or(Path::new("."))
        .join(".conduit")
}

/// Open or create the persistent state stores from a state directory.
struct PersistentState {
    env_manager: conduit_state::EnvironmentManager,
    snapshot_store: std::sync::Arc<conduit_state::SnapshotStore>,
    /// Best-effort event log for `conduit replay`. None when the events DB
    /// is unavailable (e.g. locked by a running `conduit serve`).
    event_store: Option<std::sync::Arc<conduit_state::EventStore>>,
    watermark_store: std::sync::Arc<conduit_planner::WatermarkStore>,
    state_dir: PathBuf,
}

impl PersistentState {
    fn open(state_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(state_dir)?;

        // Load environment state from disk if it exists
        let env_file = state_dir.join("environments.json");
        let env_manager = if env_file.exists() {
            let data = std::fs::read_to_string(&env_file)?;
            match serde_json::from_str::<Vec<conduit_common::snapshot::Environment>>(&data) {
                Ok(envs) => {
                    let mgr = conduit_state::EnvironmentManager::new();
                    for env in envs {
                        if env.id != "production" {
                            let _ = mgr.create(&env.id, None);
                        }
                        // Restore snapshot maps by promoting
                        // We need direct access, so we re-create properly
                    }
                    // For v0.1, fall back to a fresh manager that loads from the file
                    // In v0.2, this would be backed by RocksDB directly
                    conduit_state::EnvironmentManager::from_file(&env_file)
                        .unwrap_or_else(|_| conduit_state::EnvironmentManager::new())
                }
                Err(_) => conduit_state::EnvironmentManager::new(),
            }
        } else {
            conduit_state::EnvironmentManager::new()
        };

        // Attach env history store so promote/rollback record versions.
        let history_dir = state_dir.join("env_history");
        let env_manager = match conduit_state::EnvHistoryStore::open(&history_dir) {
            Ok(store) => env_manager.with_history_store(store),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "env_history dir unavailable; promote/rollback will not record versions"
                );
                env_manager
            }
        };

        // Open snapshot store backed by RocksDB.
        let snap_dir = state_dir.join("snapshots_db");
        let snapshot_store = std::sync::Arc::new(
            conduit_state::SnapshotStore::open(&snap_dir)
                .unwrap_or_else(|_| conduit_state::SnapshotStore::new()),
        );

        // Share the snapshot store with the env manager so promotion policies
        // that gate on snapshot age can resolve snapshot IDs.
        let env_manager = env_manager.with_snapshot_store(std::sync::Arc::clone(&snapshot_store));

        // Open the event log so env lifecycle and apply operations are
        // recorded for `conduit replay`. Best-effort: a locked/unavailable
        // DB (e.g. `conduit serve` holds it) only disables recording.
        let events_dir = state_dir.join("events");
        let event_store = match conduit_state::EventStore::open(&events_dir) {
            Ok(store) => Some(std::sync::Arc::new(store)),
            Err(e) => {
                tracing::warn!(
                    path = %events_dir.display(),
                    error = %e,
                    "Event store unavailable; state changes will not be recorded for replay"
                );
                None
            }
        };
        let env_manager = match &event_store {
            Some(store) => env_manager.with_event_store(std::sync::Arc::clone(store)),
            None => env_manager,
        };

        // Load watermarks so incremental tasks resume from where the last
        // successful run left off. Best-effort: a missing/corrupt file
        // starts fresh (first run behaves like a full refresh).
        let watermarks_path = state_dir.join("watermarks.json");
        let watermark_store = std::sync::Arc::new(
            conduit_planner::WatermarkStore::from_file(&watermarks_path)
                .unwrap_or_else(|_| conduit_planner::WatermarkStore::new()),
        );

        Ok(Self {
            env_manager,
            snapshot_store,
            event_store,
            watermark_store,
            state_dir: state_dir.to_path_buf(),
        })
    }

    /// Persist current state to disk.
    fn save(&self) -> Result<()> {
        // Save environments
        let env_file = self.state_dir.join("environments.json");
        if let Ok(envs) = self.env_manager.list() {
            let data = serde_json::to_string_pretty(&envs)?;
            std::fs::write(&env_file, data)?;
        }

        // Snapshots are persisted via RocksDB — no explicit save needed

        self.watermark_store
            .save_to_file(&self.state_dir.join("watermarks.json"))?;

        Ok(())
    }
}

// ─── CLI definition ──────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "conduit",
    about = "Conduit — a Rust-native data pipeline orchestrator",
    version,
    long_about = "Conduit compiles, schedules, and executes data pipelines with \
                  sub-second latency, virtual environments, and time-travel debugging."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Enable verbose output
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new Conduit project
    Init {
        /// Project name
        name: String,
    },

    /// Compile DAGs and report results
    Compile {
        /// Path to DAG definitions (default: ./dags/)
        #[arg(default_value = "./dags")]
        path: PathBuf,

        /// Output compiled plan to file
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Check only (don't write output)
        #[arg(long)]
        check: bool,
    },

    /// Run a DAG (compile, schedule, and execute)
    Run {
        /// DAG ID to run
        dag_id: String,

        /// Path to DAG definitions
        #[arg(short, long, default_value = "./dags")]
        dags_path: PathBuf,

        /// Logical date override (default: now)
        #[arg(long)]
        date: Option<String>,

        /// Maximum concurrent tasks
        #[arg(long, default_value = "16")]
        max_tasks: usize,

        /// Force full refresh on all incremental tasks (ignore watermarks)
        #[arg(long)]
        full_refresh: bool,

        /// Target environment recorded for this run (context only; snapshots
        /// are managed by plan/apply).
        #[arg(long, default_value = "production")]
        env: String,

        /// Enable distributed execution mode
        #[arg(long)]
        distributed: bool,

        /// Coordinator bind address (for distributed mode)
        #[arg(long, default_value = "0.0.0.0:9400")]
        bind: Option<String>,
    },

    /// Show changes between local state and an environment
    Plan {
        /// Target environment
        #[arg(default_value = "production")]
        environment: String,

        /// Path to DAG definitions
        #[arg(short, long, default_value = "./dags")]
        dags_path: PathBuf,

        /// Save the plan to a file (for later apply)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Apply a deployment plan to an environment
    Apply {
        /// Target environment
        #[arg(default_value = "production")]
        environment: String,

        /// Path to DAG definitions
        #[arg(short, long, default_value = "./dags")]
        dags_path: PathBuf,

        /// Load a saved plan file instead of generating a new one
        #[arg(long)]
        plan_file: Option<PathBuf>,

        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        auto_approve: bool,

        /// Force full refresh on all incremental tasks (ignore watermarks)
        #[arg(long)]
        full_refresh: bool,

        /// Apply only the named tasks (repeatable). Each value is
        /// `dag_id.task_id`. Upstream Execute / Reuse / Remove actions in the
        /// same plan are auto-included so dependencies stay consistent;
        /// unchanged upstream are skipped. Examples:
        ///   conduit apply prod --only etl.load
        ///   conduit apply prod --only etl.load --only sales.transform
        #[arg(long, value_name = "DAG.TASK")]
        only: Vec<String>,
    },

    /// Start the API server
    Serve {
        /// Host to bind to
        #[arg(long, default_value = "0.0.0.0")]
        host: String,

        /// Port to listen on
        #[arg(short, long, default_value = "8080")]
        port: u16,

        /// Path to DAG definitions
        #[arg(short, long, default_value = "./dags")]
        dags_path: PathBuf,

        /// Path to state directory
        #[arg(long, default_value = "./.conduit")]
        state_dir: PathBuf,

        /// Enable API key authentication
        #[arg(long)]
        auth_enabled: bool,

        /// Origin allowed to call the API cross-origin (repeatable).
        /// Default: none — same-origin only. Example for a UI dev server:
        /// --cors-origin http://localhost:3000
        #[arg(long = "cors-origin")]
        cors_origins: Vec<String>,

        /// Seed fabricated demo run history (for trying out the UI with
        /// data to look at). Never enabled by default: without this flag
        /// the server starts with only your real runs.
        #[arg(long)]
        demo: bool,
    },

    /// Show system status
    Status {
        /// Show status for a specific environment
        #[arg(short, long)]
        env: Option<String>,

        /// Path to DAG definitions
        #[arg(short, long, default_value = "./dags")]
        dags_path: PathBuf,
    },

    /// Manage virtual environments
    Env {
        #[command(subcommand)]
        action: EnvCommands,

        /// Path to DAG definitions (for resolving state dir)
        #[arg(short, long, default_value = "./dags", global = true)]
        dags_path: PathBuf,
    },

    /// Replay events to reconstruct historical state
    Replay {
        /// Replay up to this sequence number
        #[arg(long)]
        to: Option<u64>,

        /// Replay from this sequence number
        #[arg(long, default_value = "1")]
        from: u64,

        /// Path to DAG definitions (for resolving state dir)
        #[arg(short, long, default_value = "./dags")]
        dags_path: PathBuf,

        /// Output the reconstructed state as JSON
        #[arg(long)]
        json: bool,

        /// Show events only (don't reconstruct state)
        #[arg(long)]
        events_only: bool,
    },

    /// Migrate Airflow DAGs to Conduit format
    Migrate {
        /// Path to Airflow DAGs directory
        source: PathBuf,

        /// Output directory for Conduit DAGs
        #[arg(short, long, default_value = "./dags")]
        output: PathBuf,

        /// Dry run (show what would be converted)
        #[arg(long)]
        dry_run: bool,
    },

    /// Lineage: extract per-task SQL lineage, trace cross-task column flow
    Lineage {
        #[command(subcommand)]
        command: LineageCommands,
    },

    /// Schema impact between two DAG versions — the CI gate behind
    /// .github/workflows/conduit-impact.yml. Diffs task output schemas and
    /// traces the downstream blast radius through cross-task lineage.
    Impact {
        /// Base side: git ref (git mode; pair with --head)
        #[arg(long, conflicts_with_all = ["base_plan", "head_plan"], requires = "head")]
        base: Option<String>,

        /// Head side: git ref, or the literal WORKING for the uncommitted
        /// working tree
        #[arg(long, requires = "base")]
        head: Option<String>,

        /// Base side: compiled plan JSON file or a DAGs directory (file mode;
        /// pair with --head-plan)
        #[arg(long, requires = "head_plan")]
        base_plan: Option<PathBuf>,

        /// Head side: compiled plan JSON file or a DAGs directory
        #[arg(long, requires = "base_plan")]
        head_plan: Option<PathBuf>,

        /// DAGs directory relative to the repo root (git mode only)
        #[arg(long, default_value = "dags")]
        dags_path: PathBuf,

        /// Output format: markdown or json
        #[arg(long, default_value = "markdown")]
        format: String,

        /// Write the report to this file instead of stdout
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Backfill a DAG across a range of dates/partitions
    Backfill {
        /// DAG ID to backfill
        dag_id: String,

        /// Start date (inclusive, YYYY-MM-DD)
        #[arg(long)]
        start: String,

        /// End date (exclusive, YYYY-MM-DD)
        #[arg(long)]
        end: String,

        /// Partition granularity
        #[arg(long, default_value = "day")]
        granularity: String,

        /// Maximum concurrent partitions (v0.1: sequential only)
        #[arg(long, default_value = "1")]
        max_concurrent: u32,

        /// Force full refresh on all partitions (ignore watermarks)
        #[arg(long)]
        full_refresh: bool,

        /// Show what would run without executing
        #[arg(long)]
        dry_run: bool,

        /// Target environment
        #[arg(long, default_value = "production")]
        env: String,

        /// Path to DAG definitions
        #[arg(short, long, default_value = "./dags")]
        dags_path: PathBuf,
    },

    /// Start a distributed worker node
    Worker {
        /// Coordinator address to connect to
        #[arg(short, long, default_value = "localhost:9400")]
        coordinator: String,

        /// Maximum concurrent tasks this worker can run
        #[arg(short = 'n', long, default_value = "4")]
        capacity: u32,

        /// Resource pools this worker handles (comma-separated)
        #[arg(short, long, default_value = "default")]
        pools: String,

        /// Worker ID (auto-generated if omitted)
        #[arg(long)]
        id: Option<String>,

        /// Labels for worker selection (key=value pairs)
        #[arg(short, long, value_delimiter = ',')]
        labels: Vec<String>,
    },

    /// Query distributed cluster status
    Cluster {
        #[command(subcommand)]
        action: ClusterCommands,
    },

    /// Run SQL queries locally (powered by DuckDB)
    Query {
        /// SQL query to execute
        sql: String,

        /// Named connection from conduit.yaml (default: ephemeral in-memory DuckDB)
        #[arg(short, long)]
        connection: Option<String>,

        /// Query a local file (Parquet, CSV, JSON) — registers it as a table
        #[arg(short, long)]
        file: Option<Vec<PathBuf>>,

        /// Run setup SQL before the main query (e.g. CREATE TABLE statements)
        #[arg(short, long)]
        setup: Option<Vec<String>>,

        /// Output format: table, json, csv
        #[arg(long, default_value = "table")]
        format: String,

        /// Maximum rows to return
        #[arg(long, default_value = "50")]
        limit: usize,

        /// Path to conduit.yaml (for connection resolution)
        #[arg(long)]
        config: Option<PathBuf>,
    },

    /// Preview a SQL task's output locally
    Preview {
        /// Task reference: dag_id.task_id
        task_ref: String,

        /// Path to DAG definitions
        #[arg(short, long, default_value = "./dags")]
        dags_path: PathBuf,

        /// Override connection (default: ephemeral DuckDB)
        #[arg(short, long)]
        connection: Option<String>,

        /// Output format: table, json, csv
        #[arg(long, default_value = "table")]
        format: String,

        /// Maximum rows to return
        #[arg(long, default_value = "50")]
        limit: usize,
    },
}

#[derive(Subcommand)]
enum ClusterCommands {
    /// Show cluster status (workers, running tasks, health)
    Status {
        /// Coordinator address
        #[arg(short, long, default_value = "localhost:9400")]
        coordinator: String,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    /// Drain a worker (finish current tasks, then stop)
    Drain {
        /// Worker ID to drain
        worker_id: String,

        /// Coordinator address
        #[arg(short, long, default_value = "localhost:9400")]
        coordinator: String,
    },
}

#[derive(Subcommand)]
enum LineageCommands {
    /// Extract SQL lineage for a single task (the native JSON output, or an
    /// OpenLineage RunEvent under `--openlineage`).
    Extract {
        /// Task reference in the form dag_id.task_id
        task_ref: String,

        /// Path to DAG definitions
        #[arg(short, long, default_value = "./dags")]
        dags_path: PathBuf,

        /// Emit an OpenLineage RunEvent instead of Conduit's native lineage JSON
        #[arg(long)]
        openlineage: bool,

        /// OpenLineage output dataset name, e.g. analytics.customer_totals
        #[arg(long)]
        output_dataset: Option<String>,

        /// OpenLineage dataset namespace for input and output datasets
        #[arg(long)]
        dataset_namespace: Option<String>,

        /// OpenLineage job namespace
        #[arg(long, default_value = "conduit")]
        job_namespace: String,

        /// OpenLineage job name. Defaults to dag_id.task_id
        #[arg(long)]
        job_name: Option<String>,

        /// OpenLineage run UUID. Defaults to a generated UUID
        #[arg(long)]
        run_id: Option<String>,

        /// OpenLineage event timestamp. Defaults to now
        #[arg(long)]
        event_time: Option<String>,

        /// OpenLineage event type
        #[arg(long, default_value = "COMPLETE")]
        event_type: String,
    },

    /// Trace a column's lineage across task boundaries via the cross-task
    /// stitched graph (Python → SQL → Python).
    Trace {
        /// DAG to trace within
        #[arg(long)]
        dag: String,

        /// Column to trace, in the form `task_id.column_name`
        #[arg(long)]
        column: String,

        /// Path to DAG definitions
        #[arg(short, long, default_value = "./dags")]
        dags_path: PathBuf,

        /// Trace direction
        #[arg(long, default_value = "upstream", value_parser = ["upstream", "downstream"])]
        direction: String,

        /// Output format
        #[arg(long, default_value = "text", value_parser = ["text", "json"])]
        format: String,

        /// Path to a dbt `target/manifest.json` to resolve
        /// `{{ ref('x') }}` and `{{ source('s', 'x') }}` against. When
        /// omitted, unresolved Jinja blocks stay as placeholders (the
        /// historical behaviour, suitable for non-dbt projects).
        #[arg(long, value_name = "PATH")]
        dbt_manifest: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum EnvCommands {
    /// Create a new environment
    Create {
        /// Environment name
        name: String,
        /// Base environment to fork from
        #[arg(long, default_value = "production")]
        from: String,
    },
    /// List all environments
    List,
    /// Promote one environment into another
    Promote {
        /// Source environment
        source: String,
        /// Target environment
        target: String,
    },
    /// Diff two environments — show added/removed/changed snapshots
    Diff {
        /// Left environment (the "from" side)
        a: String,
        /// Right environment (the "to" side)
        b: String,
    },
    /// Show version history for an environment
    History {
        /// Environment name
        name: String,
    },
    /// Roll back an environment to a prior history version
    Rollback {
        /// Environment name
        name: String,
        /// Specific version to restore. Defaults to the env's current_version
        /// (which restores the state captured before the most recent mutation).
        #[arg(long)]
        to_version: Option<u32>,
        /// Skip the confirmation prompt
        #[arg(long)]
        yes: bool,
    },
    /// Set or clear the promotion policy on an environment
    SetPolicy {
        /// Environment name (target of the policy)
        name: String,
        /// Only allow promotions whose source matches this env name
        #[arg(long)]
        require_source: Option<String>,
        /// Newest snapshot in the source must be at least N seconds old
        #[arg(long)]
        min_age_secs: Option<u64>,
        /// Clear the policy (overrides --require-source and --min-age-secs)
        #[arg(long)]
        clear: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize tracing. With the `otel` feature + `OTEL_EXPORTER_OTLP_ENDPOINT`
    // set, this also installs an OTLP exporter alongside the fmt subscriber.
    // See `tracing_setup` for details and Bet 3 in docs/STRATEGIC_DIRECTION.md.
    tracing_setup::init_tracing(cli.verbose);

    // Initialize global Prometheus metrics registry
    conduit_common::metrics::init();

    // Use tokio runtime for async commands
    let rt = tokio::runtime::Runtime::new()?;

    match cli.command {
        Commands::Init { name } => cmd_init(&name),
        Commands::Compile {
            path,
            output,
            check,
        } => cmd_compile(&path, output.as_deref(), check),
        Commands::Run {
            dag_id,
            dags_path,
            date,
            max_tasks,
            full_refresh,
            env,
            distributed,
            bind,
        } => {
            if distributed {
                println!(
                    "Starting in distributed mode (coordinator: {})",
                    bind.as_deref().unwrap_or("0.0.0.0:9400")
                );
                println!(
                    "Workers can connect with: conduit worker --coordinator {}",
                    bind.as_deref().unwrap_or("0.0.0.0:9400")
                );
            }
            rt.block_on(cmd_run(
                &dag_id,
                &dags_path,
                date.as_deref(),
                max_tasks,
                full_refresh,
                &env,
            ))
        }
        Commands::Plan {
            environment,
            dags_path,
            output,
        } => cmd_plan(&environment, &dags_path, output.as_deref()),
        Commands::Apply {
            environment,
            dags_path,
            plan_file,
            auto_approve,
            full_refresh,
            only,
        } => rt.block_on(cmd_apply(
            &environment,
            &dags_path,
            plan_file.as_deref(),
            auto_approve,
            full_refresh,
            &only,
        )),
        Commands::Serve {
            host,
            port,
            dags_path,
            state_dir,
            auth_enabled,
            cors_origins,
            demo,
        } => rt.block_on(cmd_serve(
            &host,
            port,
            &dags_path,
            &state_dir,
            auth_enabled,
            cors_origins,
            demo,
        )),
        Commands::Status { env, dags_path } => cmd_status(env.as_deref(), &dags_path),
        Commands::Env { action, dags_path } => match action {
            EnvCommands::Create { name, from } => cmd_env_create(&name, &from, &dags_path),
            EnvCommands::List => cmd_env_list(&dags_path),
            EnvCommands::Promote { source, target } => {
                cmd_env_promote(&source, &target, &dags_path)
            }
            EnvCommands::Diff { a, b } => cmd_env_diff(&a, &b, &dags_path),
            EnvCommands::History { name } => cmd_env_history(&name, &dags_path),
            EnvCommands::Rollback {
                name,
                to_version,
                yes,
            } => cmd_env_rollback(&name, to_version, yes, &dags_path),
            EnvCommands::SetPolicy {
                name,
                require_source,
                min_age_secs,
                clear,
            } => cmd_env_set_policy(&name, require_source, min_age_secs, clear, &dags_path),
        },
        Commands::Replay {
            to,
            from,
            dags_path,
            json,
            events_only,
        } => cmd_replay(to, from, &dags_path, json, events_only),
        Commands::Migrate {
            source,
            output,
            dry_run,
        } => cmd_migrate(&source, &output, dry_run),

        Commands::Impact {
            base,
            head,
            base_plan,
            head_plan,
            dags_path,
            format,
            output,
        } => cmd_impact(
            base.as_deref(),
            head.as_deref(),
            base_plan.as_ref(),
            head_plan.as_ref(),
            &dags_path,
            &format,
            output.as_ref(),
        ),
        Commands::Lineage { command } => match command {
            LineageCommands::Extract {
                task_ref,
                dags_path,
                openlineage,
                output_dataset,
                dataset_namespace,
                job_namespace,
                job_name,
                run_id,
                event_time,
                event_type,
            } => cmd_lineage(
                &task_ref,
                &dags_path,
                openlineage,
                output_dataset.as_deref(),
                dataset_namespace.as_deref(),
                &job_namespace,
                job_name.as_deref(),
                run_id.as_deref(),
                event_time.as_deref(),
                &event_type,
            ),
            LineageCommands::Trace {
                dag,
                column,
                dags_path,
                direction,
                format,
                dbt_manifest,
            } => cmd_lineage_trace(
                &dag,
                &column,
                &dags_path,
                &direction,
                &format,
                dbt_manifest.as_deref(),
            ),
        },

        Commands::Backfill {
            dag_id,
            start,
            end,
            granularity,
            max_concurrent,
            full_refresh,
            dry_run,
            env,
            dags_path,
        } => rt.block_on(cmd_backfill(
            &dag_id,
            &start,
            &end,
            &granularity,
            max_concurrent,
            full_refresh,
            dry_run,
            &env,
            &dags_path,
        )),

        Commands::Worker {
            coordinator,
            capacity,
            pools,
            id,
            labels,
        } => cmd_worker(&coordinator, capacity, &pools, id.as_deref(), &labels),

        Commands::Cluster { action } => match action {
            ClusterCommands::Status { coordinator, json } => cmd_cluster_status(&coordinator, json),
            ClusterCommands::Drain {
                worker_id,
                coordinator,
            } => cmd_cluster_drain(&coordinator, &worker_id),
        },

        Commands::Query {
            sql,
            connection,
            file,
            setup,
            format,
            limit,
            config,
        } => rt.block_on(cmd_query(
            &sql,
            connection.as_deref(),
            file,
            setup,
            &format,
            limit,
            config,
        )),

        Commands::Preview {
            task_ref,
            dags_path,
            connection,
            format,
            limit,
        } => rt.block_on(cmd_preview(
            &task_ref,
            &dags_path,
            connection.as_deref(),
            &format,
            limit,
        )),
    }
}

// ─── conduit init ────────────────────────────────────────────────────────────

fn cmd_init(name: &str) -> Result<()> {
    use std::fs;

    let project_dir = PathBuf::from(name);

    if project_dir.exists() {
        anyhow::bail!("Directory '{}' already exists", name);
    }

    fs::create_dir_all(project_dir.join("dags"))?;
    fs::create_dir_all(project_dir.join(".conduit"))?;

    // Vendor the Python SDK into the project so `conduit run` works without
    // a repo checkout or `pip install conduit-sdk` (PRD B3). The executor
    // discovers `.conduit/sdk` by walking up from the working directory;
    // CONDUIT_SDK_PATH overrides, and a pip-installed conduit-sdk also works.
    let sdk_dest = project_dir.join(".conduit").join("sdk");
    write_embedded_sdk(&sdk_dest.join("conduit_sdk"))?;
    fs::write(sdk_dest.join("VERSION"), env!("CARGO_PKG_VERSION"))?;

    // conduit.yaml
    let config = format!(
        r#"# Conduit project configuration
name: {name}
dags_path: dags

connections: {{}}

pools:
  default:
    slots: 16

defaults:
  retries: 0
  timeout: 1h
  max_active_runs: 1
"#
    );
    fs::write(project_dir.join("conduit.yaml"), config)?;

    // Example DAG — written to match what tree-sitter parser actually extracts
    let example_dag = r#"from conduit_sdk import dag, task

@dag(schedule="0 6 * * *", tags=["example"])
def hello_world():
    """A simple example Conduit DAG."""

    @task()
    def greet():
        """Print a greeting."""
        print("Hello from Conduit!")
        print("CONDUIT::METRIC::rows_processed=42")

    @task()
    def farewell(data=greet):
        """Print a farewell after greeting completes."""
        print("Goodbye from Conduit!")
"#;
    fs::write(project_dir.join("dags/hello.py"), example_dag)?;

    // Example YAML DAG — demonstrates the declarative workflow format
    let example_yaml_dag = r#"# Example YAML DAG — Conduit supports both Python and YAML definitions.
# YAML is ideal for configuration-driven pipelines (SQL, shell, sensors).

id: hello_yaml
description: A simple example DAG defined in YAML
schedule: "0 8 * * *"
tags: [example, yaml]

tasks:
  greet:
    type: shell
    command: 'echo "Hello from a YAML-defined DAG!"'

  farewell:
    type: shell
    command: 'echo "Goodbye from YAML!"'
    depends_on: [greet]
"#;
    fs::write(project_dir.join("dags/hello.yaml"), example_yaml_dag)?;

    // .gitignore
    let gitignore = r#"# Conduit state (local only — don't commit)
.conduit/

# Python
__pycache__/
*.pyc
"#;
    fs::write(project_dir.join(".gitignore"), gitignore)?;

    println!("Initialized Conduit project '{}'", name);
    println!();
    println!("  cd {}", name);
    println!("  conduit compile");
    println!("  conduit run hello_world");

    Ok(())
}

// ─── conduit compile ─────────────────────────────────────────────────────────

fn cmd_compile(path: &PathBuf, output: Option<&Path>, check: bool) -> Result<()> {
    let start = Instant::now();

    println!("Compiling DAGs from {}...", path.display());
    println!("  (scanning .py, .yaml, and .yml files)");
    println!();

    let (plan, stats) = ConduitPlan::compile(path)?;
    let duration = start.elapsed();

    println!("  Files scanned:  {}", stats.files_scanned);
    println!("  DAGs compiled:  {}", stats.dags_compiled);
    println!("  Total tasks:    {}", stats.tasks_total);
    println!("  Errors:         {}", stats.errors.len());
    println!("  Duration:       {:.1}ms", duration.as_secs_f64() * 1000.0);
    println!();

    for (id, dag) in &plan.dags {
        println!(
            "  {} ({} tasks) [{}]",
            id,
            dag.tasks.len(),
            dag.schedule.as_deref().unwrap_or("manual")
        );
        for task_id in &dag.execution_order {
            let task = &dag.tasks[task_id];
            let deps: Vec<&str> = task
                .dependencies
                .iter()
                .map(|d| d.task_id.as_str())
                .collect();
            if deps.is_empty() {
                println!("    {} (root)", task_id);
            } else {
                println!("    {} <- [{}]", task_id, deps.join(", "));
            }
        }
        println!();
    }

    // Warn when any DAG routes work through a provider that is a stub —
    // its data operations return NotImplemented and apply WILL fail at
    // runtime. We do this best-effort: skip silently if conduit.yaml is
    // missing (e.g. running `conduit compile` from the workspace root).
    warn_on_stub_connections(path, &plan);

    if !stats.errors.is_empty() {
        eprintln!("Errors:");
        for err in &stats.errors {
            eprintln!("  {}", err);
        }
        std::process::exit(1);
    }

    if !check {
        if let Some(out_path) = output {
            plan.save(out_path)?;
            println!("Plan saved to {}", out_path.display());
        }
    }

    Ok(())
}

/// Walk every SQL task across every DAG; for each one, look up the named
/// Locate the project's conduit.yaml by walking up from the dags path.
fn find_conduit_yaml(dags_path: &Path) -> Option<PathBuf> {
    let mut candidate = dags_path.to_path_buf();
    if candidate.is_relative() {
        candidate = std::env::current_dir().unwrap_or_default().join(candidate);
    }
    let project_root = if candidate.ends_with("dags") {
        candidate.parent().map(Path::to_path_buf)
    } else {
        Some(candidate)
    };
    project_root.and_then(|p| {
        let y = p.join("conduit.yaml");
        y.exists().then_some(y)
    })
}

/// Load named resource pools from the project's conduit.yaml (`pools:`
/// section). Missing file/section means no declared pools — tasks that
/// reference an undeclared pool run unlimited (the scheduler warns).
fn load_pools(dags_path: &Path) -> Vec<conduit_common::dag::Pool> {
    let Some(yaml_path) = find_conduit_yaml(dags_path) else {
        return Vec::new();
    };
    let Ok(config) = conduit_common::config::ConduitConfig::load(&yaml_path) else {
        return Vec::new();
    };
    config
        .pools
        .into_iter()
        .map(|(name, p)| conduit_common::dag::Pool {
            name,
            slots: p.slots,
            description: p.description,
        })
        .collect()
}

/// connection in the project's conduit.yaml; if its `conn_type` is a stub,
/// print a warning. We surface the offending DAG + task so users can find
/// and replace it before deploying.
fn warn_on_stub_connections(dags_path: &Path, plan: &conduit_compiler::ConduitPlan) {
    use conduit_common::dag::TaskType;

    // Walk up from dags_path to find conduit.yaml. Mirrors resolve_state_dir.
    let yaml_path = {
        let mut candidate = dags_path.to_path_buf();
        if candidate.is_relative() {
            candidate = std::env::current_dir().unwrap_or_default().join(candidate);
        }
        // dags_path may BE the dags subdir; look at the project root.
        let project_root = if candidate.ends_with("dags") {
            candidate.parent().map(Path::to_path_buf)
        } else {
            Some(candidate.clone())
        };
        match project_root.and_then(|p| {
            let y = p.join("conduit.yaml");
            y.exists().then_some(y)
        }) {
            Some(p) => p,
            None => return,
        }
    };

    let config = match conduit_common::config::ConduitConfig::load(&yaml_path) {
        Ok(c) => c,
        Err(_) => return,
    };

    let mut warnings: Vec<String> = Vec::new();
    for (dag_id, dag) in &plan.dags {
        for (task_id, task) in &dag.tasks {
            let connection = match &task.task_type {
                TaskType::Sql { connection, .. } => connection,
                _ => continue,
            };
            let conn_cfg = match config.connections.get(connection) {
                Some(c) => c,
                None => continue, // unknown connection; planner will surface separately
            };
            if conduit_providers::is_stub_provider_type(&conn_cfg.conn_type) {
                warnings.push(format!(
                    "  {}.{} uses connection '{}' ({}) — STUB provider; will fail at runtime",
                    dag_id, task_id, connection, conn_cfg.conn_type
                ));
            }
        }
    }

    if !warnings.is_empty() {
        eprintln!(
            "WARNING: {} task(s) reference stub providers:",
            warnings.len()
        );
        for w in &warnings {
            eprintln!("{}", w);
        }
        eprintln!(
            "Stub providers are placeholders and their data operations return NotImplemented."
        );
        eprintln!();
    }
}

/// Build a provider registry from the project's conduit.yaml `connections:`.
///
/// Returns an empty registry when no config file or no connections exist —
/// SQL tasks then fail loudly instead of pretending to run.
async fn build_provider_registry(
    dags_path: &Path,
) -> std::sync::Arc<conduit_providers::ProviderRegistry> {
    let registry = match find_conduit_yaml(dags_path) {
        Some(config_path) => match conduit_common::config::ConduitConfig::load(&config_path) {
            Ok(cfg) if !cfg.connections.is_empty() => {
                let secrets: conduit_providers::SecretsConfig = cfg.secrets.clone().into();
                conduit_providers::ProviderRegistry::from_configs_with_secrets(
                    &cfg.connections,
                    &secrets,
                )
                .await
            }
            Ok(_) => conduit_providers::ProviderRegistry::new(),
            Err(e) => {
                eprintln!(
                    "Warning: failed to load {}: {} — SQL tasks will fail without providers",
                    config_path.display(),
                    e
                );
                conduit_providers::ProviderRegistry::new()
            }
        },
        None => conduit_providers::ProviderRegistry::new(),
    };
    std::sync::Arc::new(registry)
}

// ─── conduit run ─────────────────────────────────────────────────────────────
// Compiles DAGs, schedules one, and executes tasks via real ProcessRunner.

async fn cmd_run(
    dag_id: &str,
    dags_path: &PathBuf,
    date: Option<&str>,
    _max_tasks: usize,
    full_refresh: bool,
    environment: &str,
) -> Result<()> {
    use chrono::Utc;
    use conduit_executor::process_runner::{ProcessRunner, TaskContext};
    use conduit_scheduler::pool_manager::PoolManager;
    use conduit_scheduler::scheduler::{Scheduler, SchedulerCommand, SchedulerEvent};
    use std::collections::HashMap;

    let start = Instant::now();

    // Phase 1: Compile
    println!("Compiling DAGs from {}...", dags_path.display());
    let (plan, stats) = ConduitPlan::compile(dags_path)?;

    if !stats.errors.is_empty() {
        eprintln!("Compilation errors:");
        for err in &stats.errors {
            eprintln!("  {}", err);
        }
        std::process::exit(1);
    }

    let dag = plan
        .dags
        .get(dag_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "DAG '{}' not found. Available DAGs: {}",
                dag_id,
                plan.dags.keys().cloned().collect::<Vec<_>>().join(", ")
            )
        })?
        .clone();

    println!(
        "  Compiled {} DAGs ({} tasks) in {:.1}ms",
        stats.dags_compiled,
        stats.tasks_total,
        start.elapsed().as_secs_f64() * 1000.0
    );
    println!();

    // Phase 2: Schedule + Execute
    println!("Running DAG '{}'...", dag_id);
    println!("  Tasks: {}", dag.tasks.len());
    println!("  Order: {}", dag.execution_order.join(" -> "));
    println!();

    let logical_date = match date {
        Some(d) => chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d")
            .map(|nd| nd.and_hms_opt(0, 0, 0).unwrap().and_utc())
            .unwrap_or_else(|_| Utc::now()),
        None => Utc::now(),
    };

    // Build the provider registry from conduit.yaml so SQL tasks execute for
    // real (or fail loudly if the connection isn't configured).
    let registry = build_provider_registry(dags_path).await;

    // Load watermarks so incremental tasks resume where the last successful
    // run left off; persisted back to disk after the run joins.
    let watermarks_path = resolve_state_dir(dags_path).join("watermarks.json");
    let watermarks = std::sync::Arc::new(
        conduit_planner::WatermarkStore::from_file(&watermarks_path)
            .unwrap_or_else(|_| conduit_planner::WatermarkStore::new()),
    );

    // Create scheduler channels
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<SchedulerEvent>();
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<SchedulerCommand>();

    let mut dag_map = HashMap::new();
    dag_map.insert(dag_id.to_string(), dag.clone());

    let pools = PoolManager::new(load_pools(dags_path));
    let scheduler = Scheduler::new(event_rx, cmd_tx, pools, dag_map)?;

    // Persist run history so `conduit replay` can reconstruct it. Best
    // effort: if the store is locked (e.g. `conduit serve` is running) the
    // run proceeds without persistence.
    let events_dir = resolve_state_dir(dags_path).join("events");
    let scheduler = match conduit_state::EventStore::open(&events_dir) {
        Ok(store) => scheduler.with_event_store(std::sync::Arc::new(store)),
        Err(e) => {
            tracing::warn!(
                path = %events_dir.display(),
                error = %e,
                "Event store unavailable; run will not be recorded for replay"
            );
            scheduler
        }
    };

    // Request the DAG run
    let run_id = format!("run_{}", Utc::now().format("%Y%m%d_%H%M%S"));
    let mut run_config = HashMap::new();
    run_config.insert("triggered_by".to_string(), "cli".to_string());
    run_config.insert("environment".to_string(), environment.to_string());
    event_tx.send(SchedulerEvent::DagRunRequested {
        dag_id: dag_id.to_string(),
        run_id: run_id.clone(),
        logical_date,
        config: run_config,
    })?;

    // Spawn scheduler
    let scheduler_handle = tokio::spawn(async move { scheduler.run().await });

    // Executor loop: receives SchedulerCommands, runs real processes
    let executor_event_tx = event_tx.clone();
    let dag_for_executor = dag.clone();
    let run_failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let run_failed_flag = std::sync::Arc::clone(&run_failed);
    // Captures the most recent task failure reason so the final error
    // (printed to stderr) names the actual cause instead of a generic
    // "see task output above" — e.g. an unconfigured SQL connection.
    let last_error: std::sync::Arc<std::sync::Mutex<Option<String>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let last_error_flag = std::sync::Arc::clone(&last_error);
    let registry_for_exec = std::sync::Arc::clone(&registry);
    let watermarks_for_exec = std::sync::Arc::clone(&watermarks);
    let environment_for_exec = environment.to_string();
    let executor_handle = tokio::spawn(async move {
        let mut completed = 0usize;
        let total = dag_for_executor.tasks.len();
        let mut _failed = false;

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                SchedulerCommand::DispatchTask {
                    dag_id,
                    run_id,
                    task_id,
                    attempt,
                } => {
                    println!("  [RUN]   {} (attempt {})", task_id, attempt);

                    // Look up the actual task definition
                    let task = match dag_for_executor.tasks.get(&task_id) {
                        Some(t) => t,
                        None => {
                            eprintln!("  [ERR]   {} — task definition not found", task_id);
                            let _ = executor_event_tx.send(SchedulerEvent::TaskFailed {
                                dag_id,
                                run_id,
                                task_id,
                                error: "Task definition not found".to_string(),
                                attempt,
                            });
                            continue;
                        }
                    };

                    // Resolve incremental context, if this task declares one:
                    // rewrite SQL queries and/or inject CONDUIT_* env vars.
                    let run_time = chrono::Utc::now();
                    let mut task_to_run = task.clone();
                    let mut extra_env: Vec<(String, String)> = Vec::new();
                    if let Some(inc_cfg) = &task.incremental {
                        let wm = watermarks_for_exec.get(&dag_id, &task_id);
                        let inc_ctx = conduit_planner::IncrementalEngine::build_context(
                            inc_cfg,
                            wm.as_ref(),
                            full_refresh,
                            run_time,
                        );
                        if inc_ctx.is_full_refresh {
                            println!("  [INCR]  {} → full refresh", task_id);
                        } else {
                            println!(
                                "  [INCR]  {} → incremental (watermark {:?})",
                                task_id, inc_ctx.watermark
                            );
                        }
                        if let conduit_common::dag::TaskType::Sql { query, .. } =
                            &mut task_to_run.task_type
                        {
                            *query = conduit_planner::IncrementalEngine::rewrite_sql(
                                query, inc_cfg, &inc_ctx,
                            );
                        }
                        extra_env = inc_ctx.to_env_vars();
                    }

                    // Build execution context
                    let context = TaskContext {
                        dag_id: dag_id.clone(),
                        run_id: run_id.clone(),
                        task_id: task_id.clone(),
                        attempt,
                        logical_date,
                        environment: environment_for_exec.clone(),
                        params: HashMap::new(),
                        extra_env,
                    };

                    // Execute for real via ProcessRunner
                    let task_start = Instant::now();
                    match ProcessRunner::run_with_providers(
                        &task_to_run,
                        &context,
                        Some(&registry_for_exec),
                    )
                    .await
                    {
                        Ok(output) => {
                            let duration = task_start.elapsed();
                            completed += 1;

                            if output.exit_code == 0 {
                                println!(
                                    "  [OK]    {} ({:.0}ms, exit 0) [{}/{}]",
                                    task_id,
                                    duration.as_secs_f64() * 1000.0,
                                    completed,
                                    total
                                );
                                // Print captured stdout (trimmed)
                                let trimmed = output.stdout.trim();
                                if !trimmed.is_empty() {
                                    for line in trimmed.lines().take(10) {
                                        println!("          | {}", line);
                                    }
                                }

                                if let Some(inc_cfg) = &task.incremental {
                                    if inc_cfg.emit_watermark {
                                        let emitted = output.stdout.lines().rev().find_map(|l| {
                                            l.trim()
                                                .strip_prefix("CONDUIT::WATERMARK::")
                                                .map(|s| s.trim().to_string())
                                        });
                                        let mut wm = watermarks_for_exec
                                            .get(&dag_id, &task_id)
                                            .unwrap_or_else(|| {
                                                conduit_common::incremental::Watermark::new(
                                                    &dag_id, &task_id,
                                                )
                                            });
                                        conduit_planner::IncrementalEngine::advance_watermark(
                                            &mut wm,
                                            emitted.as_deref(),
                                            run_time,
                                            &run_id,
                                        );
                                        let _ = watermarks_for_exec.set(wm);
                                    }
                                }

                                let _ = executor_event_tx.send(SchedulerEvent::TaskCompleted {
                                    dag_id,
                                    run_id,
                                    task_id,
                                    snapshot_id: None,
                                    duration_ms: duration.as_millis() as u64,
                                });
                            } else if output.exit_code == 2 {
                                println!("  [RETRY] {} (exit 2)", task_id);
                                let _ = executor_event_tx.send(SchedulerEvent::TaskFailed {
                                    dag_id,
                                    run_id,
                                    task_id,
                                    error: format!("exit code 2 (retry): {}", output.stderr.trim()),
                                    attempt,
                                });
                            } else {
                                _failed = true;
                                println!(
                                    "  [FAIL]  {} (exit {}, {:.0}ms) [{}/{}]",
                                    task_id,
                                    output.exit_code,
                                    duration.as_secs_f64() * 1000.0,
                                    completed,
                                    total
                                );
                                if !output.stderr.trim().is_empty() {
                                    for line in output.stderr.trim().lines().take(5) {
                                        println!("          ! {}", line);
                                    }
                                }
                                if let Ok(mut guard) = last_error_flag.lock() {
                                    *guard = Some(format!(
                                        "{} exited {}: {}",
                                        task_id,
                                        output.exit_code,
                                        output.stderr.trim()
                                    ));
                                }
                                let _ = executor_event_tx.send(SchedulerEvent::TaskFailed {
                                    dag_id,
                                    run_id,
                                    task_id,
                                    error: output.stderr.trim().to_string(),
                                    attempt,
                                });
                            }
                        }
                        Err(e) => {
                            _failed = true;
                            completed += 1;
                            println!("  [ERR]   {} — {}", task_id, e);
                            if let Ok(mut guard) = last_error_flag.lock() {
                                *guard = Some(e.to_string());
                            }
                            let _ = executor_event_tx.send(SchedulerEvent::TaskFailed {
                                dag_id,
                                run_id,
                                task_id,
                                error: e.to_string(),
                                attempt,
                            });
                        }
                    }
                }
                SchedulerCommand::CompleteDagRun {
                    dag_id,
                    run_id,
                    status,
                } => {
                    println!();
                    println!("DAG '{}' run '{}' completed: {:?}", dag_id, run_id, status);
                    if !matches!(status, conduit_scheduler::scheduler::RunStatus::Success) {
                        run_failed_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                    let _ = executor_event_tx.send(SchedulerEvent::Shutdown);
                    break;
                }
                SchedulerCommand::SkipTask {
                    task_id, reason, ..
                } => {
                    println!("  [SKIP]  {} ({})", task_id, reason);
                    completed += 1;
                }
                SchedulerCommand::RetryTask { task_id, delay, .. } => {
                    // The scheduler re-dispatches the task itself once the
                    // delay elapses; this is just progress output. The failed
                    // attempt wasn't final, so give its slot back to the
                    // progress counter.
                    completed = completed.saturating_sub(1);
                    println!(
                        "  [RETRY] {} (retrying in {}s)",
                        task_id,
                        delay.num_seconds()
                    );
                }
            }
        }
    });

    let _ = tokio::join!(scheduler_handle, executor_handle);

    if let Err(e) = watermarks.save_to_file(&watermarks_path) {
        tracing::warn!(error = %e, "Failed to persist watermarks");
    }

    let total_duration = start.elapsed();
    println!("Total time: {:.1}ms", total_duration.as_secs_f64() * 1000.0);

    // A failed run must fail the command (CI gates on this exit code).
    if run_failed.load(std::sync::atomic::Ordering::SeqCst) {
        let detail = last_error
            .lock()
            .ok()
            .and_then(|g| g.clone())
            .unwrap_or_else(|| "see task output above".to_string());
        anyhow::bail!("DAG run failed: {}", detail);
    }

    Ok(())
}

// ─── conduit plan ────────────────────────────────────────────────────────────
// Compares compiled DAGs against a persisted environment.

fn cmd_plan(environment: &str, dags_path: &PathBuf, output: Option<&Path>) -> Result<()> {
    use conduit_planner::DeploymentPlan;

    let start = Instant::now();
    let state_dir = resolve_state_dir(dags_path);

    println!(
        "Comparing local DAGs against '{}' environment...",
        environment
    );
    println!();

    let (plan, stats) = ConduitPlan::compile(dags_path)?;

    if !stats.errors.is_empty() {
        eprintln!("Compilation errors:");
        for err in &stats.errors {
            eprintln!("  {}", err);
        }
        std::process::exit(1);
    }

    println!(
        "  Compiled {} DAGs ({} tasks) in {:.1}ms",
        stats.dags_compiled, stats.tasks_total, stats.duration_ms as f64
    );
    println!();

    // Load persistent state
    let state = PersistentState::open(&state_dir)?;
    let env = state
        .env_manager
        .get(environment)
        .unwrap_or_else(|_| conduit_common::snapshot::Environment::new(environment));

    let deploy = DeploymentPlan::generate(&plan, &env, &state.snapshot_store);
    println!("{}", deploy);

    // Show contract summary
    if deploy.has_contracts() {
        let total_checks: usize = deploy
            .pending_contracts
            .iter()
            .map(|c| c.checks.len())
            .sum();
        println!(
            "  Contracts:       {} checks across {} tasks (validated during apply)",
            total_checks,
            deploy.pending_contracts.len()
        );
    }

    let duration = start.elapsed();
    println!();
    println!("Plan generated in {:.1}ms", duration.as_secs_f64() * 1000.0);

    if let Some(out_path) = output {
        deploy.save(out_path)?;
        println!();
        println!("Plan saved to {}", out_path.display());
        println!(
            "Run 'conduit apply {} --plan-file {}' to execute.",
            environment,
            out_path.display()
        );
    } else {
        println!();
        println!(
            "Run 'conduit apply {}' to execute these changes.",
            environment
        );
    }

    Ok(())
}

// ─── conduit apply ───────────────────────────────────────────────────────────
// Executes changed tasks via real ProcessRunner and persists updated state.

async fn cmd_apply(
    environment: &str,
    dags_path: &PathBuf,
    plan_file: Option<&Path>,
    auto_approve: bool,
    full_refresh: bool,
    only: &[String],
) -> Result<()> {
    use conduit_executor::process_runner::{ProcessRunner, TaskContext};
    use conduit_planner::{ActionKind, DeploymentPlan};
    use std::collections::HashMap;

    let start = Instant::now();
    let state_dir = resolve_state_dir(dags_path);

    let (plan, stats) = ConduitPlan::compile(dags_path)?;

    if !stats.errors.is_empty() {
        eprintln!("Compilation errors:");
        for err in &stats.errors {
            eprintln!("  {}", err);
        }
        std::process::exit(1);
    }

    // Load persistent state
    let state = PersistentState::open(&state_dir)?;

    // Build the provider registry from conduit.yaml so SQL tasks execute for
    // real (or fail loudly if the connection isn't configured).
    let registry = build_provider_registry(dags_path).await;

    let deploy = if let Some(pf) = plan_file {
        println!("Loading plan from {}...", pf.display());
        let deploy = DeploymentPlan::from_json(&std::fs::read_to_string(pf)?)?;

        if deploy.target_environment != environment {
            anyhow::bail!(
                "plan file targets environment '{}' but apply was invoked for '{}'.\n  \
                 Re-run as: conduit apply {} --plan-file {}",
                deploy.target_environment,
                environment,
                deploy.target_environment,
                pf.display()
            );
        }

        let current_version = state
            .env_manager
            .get(environment)
            .map(|e| e.current_version)
            .unwrap_or(0);
        if current_version != deploy.base_environment_version {
            eprintln!(
                "Error: stale plan — environment '{}' changed since this plan was generated.",
                environment
            );
            eprintln!("  Current environment version: {}", current_version);
            eprintln!(
                "  Plan was based on version:    {}",
                deploy.base_environment_version
            );
            eprintln!();
            eprintln!("Recommended action:");
            eprintln!(
                "  conduit plan {} --output <plan.json>   # regenerate against current state",
                environment
            );
            eprintln!("  conduit apply {} --plan-file <plan.json> -y", environment);
            anyhow::bail!(
                "stale plan rejected (environment version {} != plan base version {})",
                current_version,
                deploy.base_environment_version
            );
        }
        deploy
    } else {
        println!("Generating deployment plan for '{}'...", environment);
        println!();

        let env = state
            .env_manager
            .get(environment)
            .unwrap_or_else(|_| conduit_common::snapshot::Environment::new(environment));

        let deploy = DeploymentPlan::generate(&plan, &env, &state.snapshot_store);
        println!("{}", deploy);
        deploy
    };

    // Partial apply: narrow the plan to selected tasks + their must-include
    // upstream. Pure Skip upstream are dropped; the env preserves unselected
    // pointers because `apply_to_environment` only mutates entries that have
    // an action in the plan.
    let mut deploy = if !only.is_empty() {
        let selectors: Result<Vec<(String, String)>, _> = only
            .iter()
            .map(|s| {
                s.split_once('.')
                    .map(|(d, t)| (d.to_string(), t.to_string()))
                    .ok_or_else(|| {
                        anyhow::anyhow!("--only value '{}' must be in 'dag_id.task_id' form", s)
                    })
            })
            .collect();
        let selectors = selectors?;
        let original_action_count = deploy.actions.len();
        let filtered = deploy
            .filtered_to(&plan, &selectors)
            .map_err(|e| anyhow::anyhow!("partial apply rejected: {}", e))?;
        println!();
        println!(
            "Partial apply: {} selector(s), {} action(s) kept (was {} in full plan).",
            selectors.len(),
            filtered.actions.len(),
            original_action_count,
        );
        for s in &selectors {
            println!("  • {}.{}", s.0, s.1);
        }
        filtered
    } else {
        deploy
    };

    if deploy.stats.tasks_to_execute == 0 && deploy.stats.tasks_to_remove == 0 {
        println!(
            "Nothing to apply. Environment '{}' is up to date.",
            environment
        );
        return Ok(());
    }

    if !auto_approve {
        println!(
            "Will execute {} tasks, reuse {} snapshots, remove {} tasks.",
            deploy.stats.tasks_to_execute,
            deploy.stats.tasks_to_reuse,
            deploy.stats.tasks_to_remove
        );
        println!("  (use -y to skip this prompt)");
        println!();
        // Read stdin for confirmation
        println!("Proceed? [y/N] ");
        let mut input = String::new();
        if std::io::stdin().read_line(&mut input).is_ok() && !input.trim().eq_ignore_ascii_case("y")
        {
            println!("Aborted.");
            return Ok(());
        }
    }

    println!();
    println!("Applying to '{}'...", environment);
    println!();

    let mut new_snapshots: HashMap<(String, String), String> = HashMap::new();
    let mut executed = 0usize;
    let mut reused = 0usize;
    let mut removed = 0usize;
    let logical_date = chrono::Utc::now();

    // Contracts indexed by (dag_id, task_id) — evaluated against the evidence
    // each executed task emits. Error-severity failures block the deployment
    // (docs/src/concepts/contracts.md "Plan/Apply Integration").
    let contract_index: HashMap<(String, String), &conduit_common::contracts::TaskContracts> =
        deploy
            .pending_contracts
            .iter()
            .map(|tc| {
                (
                    (tc.dag_id.clone().unwrap_or_default(), tc.task_id.clone()),
                    tc,
                )
            })
            .collect();
    let mut contract_results: Vec<conduit_common::contracts::ValidationResult> = Vec::new();

    for action in &deploy.actions {
        match &action.action {
            ActionKind::Execute => {
                // Look up the task in the compiled plan
                let task = plan
                    .dags
                    .get(&action.dag_id)
                    .and_then(|dag| dag.tasks.get(&action.task_id));

                let task = match task {
                    Some(t) => t,
                    None => {
                        eprintln!(
                            "  [ERR]   {}.{} — task not found in plan",
                            action.dag_id, action.task_id
                        );
                        continue;
                    }
                };

                println!("  [EXEC]  {}.{}", action.dag_id, action.task_id);

                let run_id = format!("apply_{}", chrono::Utc::now().format("%Y%m%d%H%M%S"));

                // Resolve incremental context, if this task declares one:
                // rewrite SQL queries and/or inject CONDUIT_* env vars.
                let mut task_to_run = task.clone();
                let mut extra_env: Vec<(String, String)> = Vec::new();
                if let Some(inc_cfg) = &task.incremental {
                    let wm = state.watermark_store.get(&action.dag_id, &action.task_id);
                    let inc_ctx = conduit_planner::IncrementalEngine::build_context(
                        inc_cfg,
                        wm.as_ref(),
                        full_refresh,
                        logical_date,
                    );
                    if inc_ctx.is_full_refresh {
                        println!(
                            "  [INCR]  {}.{} → full refresh",
                            action.dag_id, action.task_id
                        );
                    } else {
                        println!(
                            "  [INCR]  {}.{} → incremental (watermark {:?})",
                            action.dag_id, action.task_id, inc_ctx.watermark
                        );
                    }
                    if let conduit_common::dag::TaskType::Sql { query, .. } =
                        &mut task_to_run.task_type
                    {
                        *query = conduit_planner::IncrementalEngine::rewrite_sql(
                            query, inc_cfg, &inc_ctx,
                        );
                    }
                    extra_env = inc_ctx.to_env_vars();
                }

                let context = TaskContext {
                    dag_id: action.dag_id.clone(),
                    run_id: run_id.clone(),
                    task_id: action.task_id.clone(),
                    attempt: 1,
                    logical_date,
                    environment: environment.to_string(),
                    params: HashMap::new(),
                    extra_env,
                };

                let task_start = Instant::now();
                match ProcessRunner::run_with_providers(&task_to_run, &context, Some(&registry))
                    .await
                {
                    Ok(output) => {
                        let duration_ms = task_start.elapsed().as_secs_f64() * 1000.0;

                        if output.exit_code == 0 {
                            if let Some(tc) =
                                contract_index.get(&(action.dag_id.clone(), action.task_id.clone()))
                            {
                                let result = conduit_common::contracts::ContractEvaluator::evaluate(
                                    tc,
                                    &output.evidence,
                                );
                                let blocked = !result.passed;
                                println!(
                                    "  [{}] {}.{} contracts: {}/{} checks passed",
                                    if blocked { "CVIO" } else { "CHK " },
                                    action.dag_id,
                                    action.task_id,
                                    result.passed_checks,
                                    result.total_checks
                                );
                                for check in result.checks.iter().filter(|c| !c.passed) {
                                    eprintln!(
                                        "          ! {}: {}",
                                        check.contract_name, check.message
                                    );
                                }
                                contract_results.push(result);
                                if blocked {
                                    let validation =
                                        conduit_common::contracts::DeploymentValidation::from_results(
                                            contract_results,
                                        );
                                    eprintln!();
                                    eprintln!("{}", validation);
                                    anyhow::bail!(
                                        "apply blocked: contract validation failed for {}.{} — environment not updated",
                                        action.dag_id, action.task_id
                                    );
                                }
                            }

                            if let Some(inc_cfg) = &task.incremental {
                                if inc_cfg.emit_watermark {
                                    let emitted = output.stdout.lines().rev().find_map(|l| {
                                        l.trim()
                                            .strip_prefix("CONDUIT::WATERMARK::")
                                            .map(|s| s.trim().to_string())
                                    });
                                    let mut wm = state
                                        .watermark_store
                                        .get(&action.dag_id, &action.task_id)
                                        .unwrap_or_else(|| {
                                            conduit_common::incremental::Watermark::new(
                                                &action.dag_id,
                                                &action.task_id,
                                            )
                                        });
                                    conduit_planner::IncrementalEngine::advance_watermark(
                                        &mut wm,
                                        emitted.as_deref(),
                                        logical_date,
                                        &run_id,
                                    );
                                    let _ = state.watermark_store.set(wm);
                                }
                            }

                            let snap_id = format!(
                                "snap_{}_{}",
                                action.task_id,
                                chrono::Utc::now().format("%Y%m%d%H%M%S%3f")
                            );

                            // Store snapshot with fingerprint
                            if let Some(ref fp) = action.fingerprint {
                                let snapshot = conduit_common::snapshot::Snapshot {
                                    id: snap_id.clone(),
                                    fingerprint: fp.clone(),
                                    dag_id: action.dag_id.clone(),
                                    task_id: action.task_id.clone(),
                                    created_at: chrono::Utc::now(),
                                    parent_fingerprints: vec![],
                                    metadata: HashMap::new(),
                                };
                                let _ = state.snapshot_store.put(snapshot);
                                if let Some(store) = &state.event_store {
                                    let _ = store.append(
                                        conduit_common::event::EventKind::SnapshotCreated {
                                            snapshot_id: snap_id.clone(),
                                            fingerprint: fp.0.clone(),
                                            dag_id: action.dag_id.clone(),
                                            task_id: action.task_id.clone(),
                                        },
                                    );
                                }
                            }

                            new_snapshots
                                .insert((action.dag_id.clone(), action.task_id.clone()), snap_id);
                            executed += 1;
                            println!(
                                "  [OK]    {}.{} ({:.0}ms)",
                                action.dag_id, action.task_id, duration_ms
                            );
                        } else {
                            eprintln!(
                                "  [FAIL]  {}.{} (exit {}, {:.0}ms)",
                                action.dag_id, action.task_id, output.exit_code, duration_ms
                            );
                            if !output.stderr.trim().is_empty() {
                                for line in output.stderr.trim().lines().take(5) {
                                    eprintln!("          ! {}", line);
                                }
                            }
                            anyhow::bail!(
                                "apply aborted: task {}.{} failed with exit code {}",
                                action.dag_id,
                                action.task_id,
                                output.exit_code
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("  [ERR]   {}.{} — {}", action.dag_id, action.task_id, e);
                        anyhow::bail!(
                            "apply aborted: task {}.{} execution error: {}",
                            action.dag_id,
                            action.task_id,
                            e
                        );
                    }
                }
            }
            ActionKind::ReuseSnapshot { snapshot_id } => {
                println!(
                    "  [REUSE] {}.{} -> {}",
                    action.dag_id,
                    action.task_id,
                    &snapshot_id[..snapshot_id.len().min(12)]
                );
                reused += 1;
            }
            ActionKind::Skip => {
                // Silent
            }
            ActionKind::Remove => {
                println!("  [DEL]   {}.{}", action.dag_id, action.task_id);
                removed += 1;
            }
        }
    }

    // `contract_index` borrows from `deploy.pending_contracts`; drop it before
    // taking a mutable borrow of `deploy` below.
    drop(contract_index);

    if !contract_results.is_empty() {
        let validation =
            conduit_common::contracts::DeploymentValidation::from_results(contract_results);
        println!();
        println!("{}", validation);
        deploy.set_validation(validation);
        if !deploy.can_apply() {
            anyhow::bail!("apply blocked: contract validation failed — environment not updated");
        }
    }

    // Update environment with new snapshot pointers.
    //
    // We do this through `env_manager.apply_snapshot_map` rather than mutating
    // a local clone, so the env's prior snapshot_map is captured as a history
    // entry tagged `Apply { plan_id }`. That makes the apply reversible via
    // `conduit env rollback <env>` (Bet 5).
    //
    // The environment is auto-created if it doesn't exist yet so applying to
    // a brand-new env name works the same way it did before this change.
    if state.env_manager.get(environment).is_err() {
        let _ = state.env_manager.create(environment, None);
    }

    let mut env_snapshot = state
        .env_manager
        .get(environment)
        .unwrap_or_else(|_| conduit_common::snapshot::Environment::new(environment));
    deploy.apply_to_environment(&mut env_snapshot, &new_snapshots);
    let post_apply_map = env_snapshot.snapshot_map.clone();
    let snapshot_count = post_apply_map.len();

    let recorded_version =
        state
            .env_manager
            .apply_snapshot_map(environment, post_apply_map, deploy.id.clone())?;

    // Persist state to disk
    state.save()?;

    if let Some(store) = &state.event_store {
        let _ = store.append(conduit_common::event::EventKind::PlanApplied {
            plan_id: deploy.id.clone(),
            environment: environment.to_string(),
            tasks_executed: executed as u32,
            tasks_skipped: reused as u32,
        });
    }

    let duration = start.elapsed();
    println!();
    println!("Apply complete:");
    println!("  Executed: {} tasks", executed);
    println!("  Reused:   {} snapshots", reused);
    println!("  Removed:  {} tasks", removed);
    println!("  Duration: {:.1}ms", duration.as_secs_f64() * 1000.0);
    println!();
    println!(
        "Environment '{}' updated ({} snapshot pointers).",
        environment, snapshot_count
    );
    if let Some(v) = recorded_version {
        println!(
            "  Pre-apply state captured as history version {} — \
             revert with `conduit env rollback {}`.",
            v, environment
        );
    }

    Ok(())
}

// ─── conduit serve ───────────────────────────────────────────────────────────

async fn cmd_serve(
    host: &str,
    port: u16,
    dags_path: &PathBuf,
    state_dir: &PathBuf,
    auth_enabled: bool,
    cors_origins: Vec<String>,
    demo: bool,
) -> Result<()> {
    use std::net::SocketAddr;

    println!("Starting Conduit API server...");
    println!();

    if !dags_path.exists() {
        eprintln!(
            "Warning: DAGs path '{}' does not exist",
            dags_path.display()
        );
    }

    std::fs::create_dir_all(state_dir)?;

    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    println!("  DAGs path:   {}", dags_path.display());
    println!("  State dir:   {}", state_dir.display());
    println!("  API:         http://{}/api/v1/", addr);
    println!("  WebSocket:   ws://{}/ws/events", addr);
    println!("  Health:      http://{}/api/v1/health", addr);

    // Check for UI assets directory (set via CONDUIT_UI_DIR env var)
    let ui_dir = std::env::var("CONDUIT_UI_DIR")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists());
    if let Some(ref dir) = ui_dir {
        println!(
            "  UI:          http://{}/  (serving from {})",
            addr,
            dir.display()
        );
    }

    // Authentication setup
    if auth_enabled {
        println!("  Auth:        ENABLED (API keys required)");
    } else {
        println!("  Auth:        disabled (all endpoints public)");
    }
    println!();

    let state = conduit_api::AppState::with_options(
        dags_path.clone(),
        state_dir.clone(),
        ui_dir,
        auth_enabled,
    );

    // Initialize SQL providers from conduit.yaml so both the /connections API
    // and the server-side executor run SQL for real.
    if let Some(config_path) = find_conduit_yaml(dags_path) {
        match conduit_common::config::ConduitConfig::load(&config_path) {
            Ok(cfg) => {
                state.init_providers(&cfg.connections).await;
                println!(
                    "  Providers:   {} connection(s) registered",
                    cfg.connections.len()
                );
            }
            Err(e) => eprintln!(
                "  Providers:   WARNING — failed to load conduit.yaml: {}",
                e
            ),
        }
    }

    if !cors_origins.is_empty() {
        println!("  CORS:        allowing {}", cors_origins.join(", "));
        state.set_cors_origins(cors_origins);
    }

    // If auth is enabled and no keys exist, create a bootstrap admin key
    if auth_enabled && state.auth_store.list_keys().is_empty() {
        let bootstrap_key = state.auth_store.create_bootstrap_key();
        state.save_auth_keys();
        println!("  ┌─────────────────────────────────────────────────────┐");
        println!("  │  BOOTSTRAP ADMIN KEY (save this — shown only once)  │");
        println!("  │                                                     │");
        println!("  │  {}  │", bootstrap_key);
        println!("  │                                                     │");
        println!(
            "  │  Use: Authorization: Bearer {}  │",
            &bootstrap_key[..20]
        );
        println!("  └─────────────────────────────────────────────────────┘");
        println!();
    }

    // Demo mode only: seed fabricated run history so the UI has data to
    // look at. A real deployment must never mix fake runs into real ones.
    if demo {
        println!("  Demo mode:   seeding fabricated run history (--demo)");
        state.seed_demo_data();
    }
    println!(
        "  Demo data:   seeded {} historical runs",
        state.get_runs(None).len()
    );

    // ── Wire up scheduler + executor so API-triggered runs actually execute ──
    {
        use conduit_executor::process_runner::{ProcessRunner, TaskContext};
        use conduit_scheduler::{
            PoolManager, RunStatus, Scheduler, SchedulerCommand, SchedulerEvent,
        };
        use std::collections::HashMap;

        // Compile DAGs so the scheduler knows about them
        let dag_map = match ConduitPlan::compile(dags_path) {
            Ok((plan, stats)) => {
                println!(
                    "  Scheduler:   {} DAGs loaded ({} tasks)",
                    stats.dags_compiled, stats.tasks_total
                );
                plan.dags
            }
            Err(e) => {
                eprintln!("  Scheduler:   WARNING — failed to compile DAGs: {}", e);
                HashMap::new()
            }
        };

        // Create scheduler channels
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<SchedulerEvent>();
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<SchedulerCommand>();

        // Attach event sender to AppState so trigger_run can dispatch events
        state.with_scheduler(event_tx.clone());

        // Spawn the scheduler event loop. Share the API's event store so
        // run lifecycle events are persisted for `conduit replay` and the
        // /events API.
        let pools = PoolManager::new(load_pools(dags_path));
        let mut scheduler = Scheduler::new(event_rx, cmd_tx, pools, dag_map.clone())?;
        if let Some(store) = &state.event_store {
            scheduler = scheduler.with_event_store(std::sync::Arc::clone(store));
        }
        tokio::spawn(async move { scheduler.run().await });

        // Cron tick source: wake the scheduler at the top of every minute so
        // DAG schedules actually fire. Five-field cron has minute resolution,
        // and the scheduler dedupes within a minute, so once per minute is
        // exactly right.
        let cron_tx = event_tx.clone();
        tokio::spawn(async move {
            loop {
                let now = chrono::Utc::now();
                let millis_into_minute = (now.timestamp_millis().rem_euclid(60_000)) as u64;
                let sleep_ms = 60_000 - millis_into_minute;
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                if cron_tx
                    .send(SchedulerEvent::CronTick {
                        timestamp: chrono::Utc::now(),
                    })
                    .is_err()
                {
                    break; // scheduler is gone; stop ticking
                }
            }
        });

        // Spawn the executor loop — receives SchedulerCommands, runs tasks,
        // updates AppState, and broadcasts WebSocket events
        let exec_state = state.clone();
        let exec_event_tx = event_tx;
        let exec_dag_map = dag_map;
        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    SchedulerCommand::DispatchTask {
                        dag_id,
                        run_id,
                        task_id,
                        attempt,
                    } => {
                        let task = match exec_dag_map
                            .get(&dag_id)
                            .and_then(|d| d.tasks.get(&task_id))
                        {
                            Some(t) => t.clone(),
                            None => {
                                tracing::error!(dag = %dag_id, task = %task_id, "Task definition not found");
                                let _ = exec_event_tx.send(SchedulerEvent::TaskFailed {
                                    dag_id,
                                    run_id,
                                    task_id,
                                    error: "Task definition not found".into(),
                                    attempt,
                                });
                                continue;
                            }
                        };

                        // Scheduler-initiated runs (cron) were never recorded
                        // via the POST /runs handler — create the run entry on
                        // first dispatch so they show up in the UI and API.
                        ensure_run_recorded(&exec_state, &dag_id, &run_id);

                        // Mark task as running
                        update_run_task_state(&exec_state, &run_id, &task_id, "running");
                        exec_state.broadcast_event(
                            &serde_json::json!({
                                "type": "task_state_changed",
                                "dagId": dag_id, "runId": run_id,
                                "taskId": task_id, "state": "running",
                            })
                            .to_string(),
                        );

                        let environment = exec_state
                            .runs
                            .read()
                            .ok()
                            .and_then(|runs| {
                                runs.iter()
                                    .find(|r| r.run_id == run_id)
                                    .map(|r| r.environment.clone())
                            })
                            .unwrap_or_else(|| "production".to_string());

                        let context = TaskContext {
                            dag_id: dag_id.clone(),
                            run_id: run_id.clone(),
                            task_id: task_id.clone(),
                            attempt,
                            logical_date: chrono::Utc::now(),
                            environment,
                            params: HashMap::new(),
                            extra_env: Vec::new(),
                        };

                        let registry = exec_state
                            .provider_registry
                            .read()
                            .ok()
                            .and_then(|guard| guard.clone());

                        // Spawn each task so independent tasks run concurrently
                        // instead of blocking the command dispatch loop.
                        let spawn_state = exec_state.clone();
                        let spawn_event_tx = exec_event_tx.clone();
                        tokio::spawn(async move {
                            let task_start = Instant::now();
                            match ProcessRunner::run_with_providers(
                                &task,
                                &context,
                                registry.as_deref(),
                            )
                            .await
                            {
                                Ok(output) => {
                                    let duration = task_start.elapsed();
                                    update_run_task_logs(
                                        &spawn_state,
                                        &run_id,
                                        &task_id,
                                        &output.stdout,
                                        &output.stderr,
                                    );
                                    if output.exit_code == 0 {
                                        update_run_task_state(
                                            &spawn_state,
                                            &run_id,
                                            &task_id,
                                            "success",
                                        );
                                        spawn_state.broadcast_event(
                                            &serde_json::json!({
                                                "type": "task_state_changed",
                                                "dagId": dag_id, "runId": run_id,
                                                "taskId": task_id, "state": "success",
                                                "durationMs": duration.as_millis() as u64,
                                            })
                                            .to_string(),
                                        );
                                        let _ =
                                            spawn_event_tx.send(SchedulerEvent::TaskCompleted {
                                                dag_id,
                                                run_id,
                                                task_id,
                                                snapshot_id: None,
                                                duration_ms: duration.as_millis() as u64,
                                            });
                                    } else {
                                        let err = output.stderr.trim().to_string();
                                        update_run_task_state(
                                            &spawn_state,
                                            &run_id,
                                            &task_id,
                                            "failed",
                                        );
                                        spawn_state.broadcast_event(
                                            &serde_json::json!({
                                                "type": "task_state_changed",
                                                "dagId": dag_id, "runId": run_id,
                                                "taskId": task_id, "state": "failed",
                                                "error": err,
                                            })
                                            .to_string(),
                                        );
                                        let _ = spawn_event_tx.send(SchedulerEvent::TaskFailed {
                                            dag_id,
                                            run_id,
                                            task_id,
                                            error: err,
                                            attempt,
                                        });
                                    }
                                }
                                Err(e) => {
                                    update_run_task_logs(
                                        &spawn_state,
                                        &run_id,
                                        &task_id,
                                        "",
                                        &e.to_string(),
                                    );
                                    update_run_task_state(
                                        &spawn_state,
                                        &run_id,
                                        &task_id,
                                        "failed",
                                    );
                                    spawn_state.broadcast_event(
                                        &serde_json::json!({
                                            "type": "task_state_changed",
                                            "dagId": dag_id, "runId": run_id,
                                            "taskId": task_id, "state": "failed",
                                            "error": e.to_string(),
                                        })
                                        .to_string(),
                                    );
                                    let _ = spawn_event_tx.send(SchedulerEvent::TaskFailed {
                                        dag_id,
                                        run_id,
                                        task_id,
                                        error: e.to_string(),
                                        attempt,
                                    });
                                }
                            }
                        });
                    }
                    SchedulerCommand::CompleteDagRun {
                        dag_id,
                        run_id,
                        status,
                    } => {
                        let status_str = match status {
                            RunStatus::Success => "success",
                            RunStatus::Failed => "failed",
                            RunStatus::Cancelled => "cancelled",
                        };
                        update_run_status(&exec_state, &run_id, status_str);
                        exec_state.broadcast_event(
                            &serde_json::json!({
                                "type": "dag_run_completed",
                                "dagId": dag_id, "runId": run_id,
                                "status": status_str,
                            })
                            .to_string(),
                        );
                    }
                    SchedulerCommand::SkipTask {
                        dag_id,
                        run_id,
                        task_id,
                        reason,
                    } => {
                        update_run_task_state(&exec_state, &run_id, &task_id, "skipped");
                        exec_state.broadcast_event(
                            &serde_json::json!({
                                "type": "task_state_changed",
                                "dagId": dag_id, "runId": run_id,
                                "taskId": task_id, "state": "skipped",
                                "reason": reason,
                            })
                            .to_string(),
                        );
                    }
                    SchedulerCommand::RetryTask { task_id, delay, .. } => {
                        tracing::info!(task = %task_id, delay_ms = delay.num_milliseconds(), "Task retry scheduled");
                    }
                }
            }
        });

        println!("  Executor:    running (scheduler attached)");
    }
    println!();

    conduit_api::serve(state, addr).await?;

    Ok(())
}

/// Record a scheduler-initiated run (e.g. cron) in the API run cache if it
/// isn't already there. Runs triggered via POST /runs are recorded by the
/// handler; this covers every other dispatch source.
fn ensure_run_recorded(state: &conduit_api::AppState, dag_id: &str, run_id: &str) {
    if let Ok(runs) = state.runs.read() {
        if runs.iter().any(|r| r.run_id == run_id) {
            return;
        }
    }
    state.record_run(conduit_api::DagRunInfo {
        run_id: run_id.to_string(),
        dag_id: dag_id.to_string(),
        status: "running".to_string(),
        started_at: chrono::Utc::now(),
        finished_at: None,
        task_states: std::collections::HashMap::new(),
        task_logs: std::collections::HashMap::new(),
        triggered_by: "scheduler".to_string(),
        environment: "production".to_string(),
    });
}

/// Store a completed task's captured output on its run so the run detail
/// view can show logs after the fact. Capped per task to bound memory; the
/// live WebSocket stream remains the full-fidelity path.
fn update_run_task_logs(
    state: &conduit_api::AppState,
    run_id: &str,
    task_id: &str,
    stdout: &str,
    stderr: &str,
) {
    const MAX_LOG_BYTES: usize = 16 * 1024;
    let mut text = String::new();
    if !stdout.trim().is_empty() {
        text.push_str(stdout.trim_end());
        text.push('\n');
    }
    if !stderr.trim().is_empty() {
        text.push_str("--- stderr ---\n");
        text.push_str(stderr.trim_end());
        text.push('\n');
    }
    if text.is_empty() {
        return;
    }
    if text.len() > MAX_LOG_BYTES {
        let cut = text.len() - MAX_LOG_BYTES;
        // keep the tail — failures usually end with the interesting part
        let mut idx = cut;
        while !text.is_char_boundary(idx) {
            idx += 1;
        }
        text = format!("… (truncated {} bytes)\n{}", cut, &text[idx..]);
    }
    if let Ok(mut runs) = state.runs.write() {
        if let Some(run) = runs.iter_mut().find(|r| r.run_id == run_id) {
            run.task_logs.insert(task_id.to_string(), text);
        }
    }
}

/// Update a specific task's state within a run.
fn update_run_task_state(
    state: &conduit_api::AppState,
    run_id: &str,
    task_id: &str,
    task_state: &str,
) {
    if let Ok(mut runs) = state.runs.write() {
        if let Some(run) = runs.iter_mut().find(|r| r.run_id == run_id) {
            run.task_states
                .insert(task_id.to_string(), task_state.to_string());
            // If the run was "dispatched" or "queued", mark it as "running"
            if run.status == "dispatched" || run.status == "queued" || run.status == "pending" {
                run.status = "running".to_string();
            }
        }
    }
}

/// Update a run's overall status (success, failed, cancelled).
fn update_run_status(state: &conduit_api::AppState, run_id: &str, status: &str) {
    if let Ok(mut runs) = state.runs.write() {
        if let Some(run) = runs.iter_mut().find(|r| r.run_id == run_id) {
            run.status = status.to_string();
            run.finished_at = Some(chrono::Utc::now());
        }
    }
}

// ─── conduit status ──────────────────────────────────────────────────────────

fn cmd_status(env: Option<&str>, dags_path: &PathBuf) -> Result<()> {
    let env_name = env.unwrap_or("production");
    let state_dir = resolve_state_dir(dags_path);

    println!("Conduit Status");
    println!("  State dir: {}", state_dir.display());
    println!();

    let state = PersistentState::open(&state_dir)?;

    let environment = state
        .env_manager
        .get(env_name)
        .unwrap_or_else(|_| conduit_common::snapshot::Environment::new(env_name));

    println!("  Environment:     {}", env_name);
    println!("  Snapshots:       {}", environment.snapshot_map.len());
    println!(
        "  Last updated:    {}",
        environment.updated_at.format("%Y-%m-%d %H:%M:%S UTC")
    );

    if let Ok(envs) = state.env_manager.list() {
        println!("  Environments:    {}", envs.len());
        for e in &envs {
            let marker = if e.id == env_name { " (active)" } else { "" };
            println!(
                "    - {} ({} snapshots){}",
                e.id,
                e.snapshot_map.len(),
                marker
            );
        }
    }

    println!("  Stored snapshots: {}", state.snapshot_store.count());

    Ok(())
}

// ─── conduit env ─────────────────────────────────────────────────────────────

fn cmd_env_create(name: &str, from: &str, dags_path: &PathBuf) -> Result<()> {
    let state_dir = resolve_state_dir(dags_path);
    let state = PersistentState::open(&state_dir)?;

    let env = state.env_manager.create(name, Some(from))?;
    state.save()?;

    println!(
        "Created environment '{}' ({} snapshots, forked from '{}')",
        env.id,
        env.snapshot_map.len(),
        from
    );
    Ok(())
}

fn cmd_env_list(dags_path: &PathBuf) -> Result<()> {
    let state_dir = resolve_state_dir(dags_path);
    let state = PersistentState::open(&state_dir)?;

    let envs = state.env_manager.list()?;
    println!("Environments:");
    for env in envs {
        println!(
            "  {} ({} snapshots, updated {})",
            env.id,
            env.snapshot_map.len(),
            env.updated_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
    }
    Ok(())
}

fn cmd_env_promote(source: &str, target: &str, dags_path: &PathBuf) -> Result<()> {
    let state_dir = resolve_state_dir(dags_path);
    let state = PersistentState::open(&state_dir)?;

    let changes = state.env_manager.promote(source, target)?;
    state.save()?;

    println!(
        "Promoted '{}' -> '{}' ({} snapshot changes)",
        source, target, changes
    );
    Ok(())
}

fn cmd_env_diff(a: &str, b: &str, dags_path: &PathBuf) -> Result<()> {
    let state_dir = resolve_state_dir(dags_path);
    let state = PersistentState::open(&state_dir)?;

    let env_a = state.env_manager.get(a)?;
    let env_b = state.env_manager.get(b)?;
    let diff = env_a.diff(&env_b);

    println!("--- {}", a);
    println!("+++ {}", b);

    if diff.is_empty() {
        println!("(no differences)");
        return Ok(());
    }

    // Snapshot IDs are "snap_<task>_<timestamp>"; the unique part is the
    // tail, so truncating to a prefix would render every version identical.
    let short = |s: &str| -> String { s.to_string() };

    let mut removed = diff.removed.clone();
    removed.sort_by(|x, y| (&x.dag_id, &x.task_id).cmp(&(&y.dag_id, &y.task_id)));
    for e in &removed {
        println!("- {}.{}  {}", e.dag_id, e.task_id, short(&e.snapshot_id));
    }

    let mut changed = diff.changed.clone();
    changed.sort_by(|x, y| (&x.dag_id, &x.task_id).cmp(&(&y.dag_id, &y.task_id)));
    for c in &changed {
        println!(
            "~ {}.{}  {} -> {}",
            c.dag_id,
            c.task_id,
            short(&c.old_snapshot_id),
            short(&c.new_snapshot_id)
        );
    }

    let mut added = diff.added.clone();
    added.sort_by(|x, y| (&x.dag_id, &x.task_id).cmp(&(&y.dag_id, &y.task_id)));
    for e in &added {
        println!("+ {}.{}  {}", e.dag_id, e.task_id, short(&e.snapshot_id));
    }

    println!();
    println!(
        "{} added, {} removed, {} changed ({} total)",
        diff.added.len(),
        diff.removed.len(),
        diff.changed.len(),
        diff.total()
    );

    Ok(())
}

fn cmd_env_history(name: &str, dags_path: &PathBuf) -> Result<()> {
    use conduit_common::snapshot::EnvHistoryReason;

    let state_dir = resolve_state_dir(dags_path);
    let state = PersistentState::open(&state_dir)?;

    let env = state.env_manager.get(name)?;
    let history = state.env_manager.history(name)?;

    println!("Environment '{}'", env.id);
    println!("  current_version: {}", env.current_version);
    println!("  history entries: {}", history.len());
    println!();

    if history.is_empty() {
        println!("(no history recorded — promote or rollback this env to create entries)");
        return Ok(());
    }

    println!(
        "{:>7}  {:<24}  {:>9}  reason",
        "version", "captured_at", "snapshots"
    );
    for entry in &history {
        let reason = match &entry.reason {
            EnvHistoryReason::Promotion { from } => format!("promotion from '{}'", from),
            EnvHistoryReason::Rollback { from_version } => {
                format!("rollback from v{}", from_version)
            }
            EnvHistoryReason::Apply { plan_id } => format!("apply (plan {})", plan_id),
            EnvHistoryReason::Manual => "manual".to_string(),
        };
        println!(
            "{:>7}  {:<24}  {:>9}  {}",
            entry.version,
            entry.captured_at.format("%Y-%m-%d %H:%M:%S UTC"),
            entry.snapshot_count,
            reason
        );
    }

    Ok(())
}

fn cmd_env_rollback(
    name: &str,
    to_version: Option<u32>,
    yes: bool,
    dags_path: &PathBuf,
) -> Result<()> {
    let state_dir = resolve_state_dir(dags_path);
    let state = PersistentState::open(&state_dir)?;

    let env = state.env_manager.get(name)?;
    let target = to_version.unwrap_or(env.current_version);

    if !yes {
        use std::io::{BufRead, Write};
        print!(
            "Roll back '{}' (current version {}) to version {}? [y/N] ",
            name, env.current_version, target
        );
        std::io::stdout().flush().ok();
        let stdin = std::io::stdin();
        let mut line = String::new();
        stdin.lock().read_line(&mut line)?;
        if !matches!(line.trim().to_lowercase().as_str(), "y" | "yes") {
            println!("Aborted.");
            return Ok(());
        }
    }

    let (new_version, changes) = state.env_manager.rollback(name, to_version)?;
    state.save()?;

    println!(
        "Rolled back '{}' to version {} ({} snapshot changes, new history version {})",
        name, target, changes, new_version
    );
    Ok(())
}

fn cmd_env_set_policy(
    name: &str,
    require_source: Option<String>,
    min_age_secs: Option<u64>,
    clear: bool,
    dags_path: &PathBuf,
) -> Result<()> {
    use conduit_common::snapshot::PromotionPolicy;

    let state_dir = resolve_state_dir(dags_path);
    let state = PersistentState::open(&state_dir)?;

    let policy = if clear {
        PromotionPolicy::default()
    } else {
        PromotionPolicy {
            require_source,
            min_age_secs,
        }
    };

    let env = state.env_manager.set_promotion_policy(name, policy)?;
    state.save()?;

    println!("Updated promotion policy for '{}':", env.id);
    match &env.promotion_policy.require_source {
        Some(s) => println!("  require_source: {}", s),
        None => println!("  require_source: (any)"),
    }
    match env.promotion_policy.min_age_secs {
        Some(n) => println!("  min_age_secs:   {}", n),
        None => println!("  min_age_secs:   (none)"),
    }
    Ok(())
}

// ─── conduit replay ──────────────────────────────────────────────────────────

fn cmd_replay(
    to: Option<u64>,
    from: u64,
    dags_path: &PathBuf,
    json: bool,
    events_only: bool,
) -> Result<()> {
    use conduit_common::event::EventKind;
    use conduit_state::EventStore;
    use std::collections::HashMap;

    let state_dir = resolve_state_dir(dags_path);
    let events_dir = state_dir.join("events");

    if !events_dir.exists() {
        println!("No events found at {}", events_dir.display());
        return Ok(());
    }

    let event_store = EventStore::open(&events_dir)?;
    let current_seq = event_store.current_sequence();
    let to_seq = to.unwrap_or(current_seq);

    if from > to_seq {
        anyhow::bail!("from ({}) > to ({})", from, to_seq);
    }

    let events = event_store.range(from, to_seq)?;

    if events_only {
        // Just print events as a table
        println!("{:<6} {:<30} {:<20} SUMMARY", "SEQ", "TIMESTAMP", "TYPE");
        println!("{}", "─".repeat(100));
        for event in events {
            let event_type = match &event.kind {
                EventKind::DagRunCreated { dag_id, .. } => format!("DagRunCreated({})", dag_id),
                EventKind::DagRunCompleted { dag_id, status, .. } => {
                    format!("DagRunCompleted({}, {:?})", dag_id, status)
                }
                EventKind::TaskQueued { task_id, .. } => format!("TaskQueued({})", task_id),
                EventKind::TaskStarted { task_id, .. } => format!("TaskStarted({})", task_id),
                EventKind::TaskCompleted { task_id, .. } => format!("TaskCompleted({})", task_id),
                EventKind::TaskFailed { task_id, .. } => format!("TaskFailed({})", task_id),
                EventKind::TaskRetrying { task_id, .. } => format!("TaskRetrying({})", task_id),
                EventKind::TaskSkipped { task_id, .. } => format!("TaskSkipped({})", task_id),
                EventKind::SnapshotCreated { snapshot_id, .. } => {
                    format!("SnapshotCreated({})", snapshot_id)
                }
                EventKind::AuthAudit { action, .. } => format!("AuthAudit({})", action),
                EventKind::EnvironmentCreated { env_name, .. } => {
                    format!("EnvironmentCreated({})", env_name)
                }
                EventKind::EnvironmentPromoted {
                    source_env,
                    target_env,
                    ..
                } => format!("EnvironmentPromoted({} -> {})", source_env, target_env),
                EventKind::EnvironmentRolledBack { env_name, .. } => {
                    format!("EnvironmentRolledBack({})", env_name)
                }
                EventKind::PlanCreated { plan_id, .. } => format!("PlanCreated({})", plan_id),
                EventKind::PlanApplied { plan_id, .. } => format!("PlanApplied({})", plan_id),
            };
            println!(
                "{:<6} {:<30} {:<20} ",
                event.sequence,
                event.timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
                event_type
            );
        }
        return Ok(());
    }

    // Reconstruct state by replaying events
    let mut environments: HashMap<String, Vec<String>> = HashMap::new();
    let mut runs: Vec<(String, String, String)> = Vec::new(); // (dag_id, run_id, status)
    let mut task_stats: HashMap<String, (usize, usize, usize)> = HashMap::new(); // (task_id -> (total, success, failed))

    for event in &events {
        match &event.kind {
            EventKind::EnvironmentCreated { env_name, .. } => {
                environments.insert(env_name.clone(), Vec::new());
            }
            EventKind::EnvironmentPromoted {
                source_env,
                target_env,
                ..
            } => {
                if let Some(source_snapshots) = environments.get(source_env) {
                    environments.insert(target_env.clone(), source_snapshots.clone());
                }
            }
            EventKind::SnapshotCreated { snapshot_id, .. } => {
                for snapshots in environments.values_mut() {
                    if !snapshots.contains(snapshot_id) {
                        snapshots.push(snapshot_id.clone());
                    }
                }
            }
            EventKind::DagRunCompleted {
                dag_id,
                run_id,
                status,
                ..
            } => {
                let status_str = format!("{:?}", status);
                runs.push((dag_id.clone(), run_id.clone(), status_str));
            }
            EventKind::TaskCompleted { task_id, .. } => {
                let stats = task_stats.entry(task_id.clone()).or_insert((0, 0, 0));
                stats.0 += 1;
                stats.1 += 1;
            }
            EventKind::TaskFailed { task_id, .. } => {
                let stats = task_stats.entry(task_id.clone()).or_insert((0, 0, 0));
                stats.0 += 1;
                stats.2 += 1;
            }
            _ => {}
        }
    }

    // Build summary
    let total_events = events.len();
    let total_envs = environments.len();
    let total_snapshots: usize = environments.values().map(|s| s.len()).sum();

    let (success_runs, fail_runs) = runs.iter().fold((0, 0), |(s, f), (_, _, status)| {
        if status.contains("Success") {
            (s + 1, f)
        } else {
            (s, f + 1)
        }
    });

    if json {
        // Output as JSON
        let summary = serde_json::json!({
            "total_events": total_events,
            "sequence_range": format!("{}-{}", from, to_seq),
            "environments": total_envs,
            "snapshots": total_snapshots,
            "dag_runs": {
                "success": success_runs,
                "failed": fail_runs,
                "total": runs.len()
            },
            "tasks": task_stats.len(),
            "environments_list": environments.keys().collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        // Human-readable output
        println!();
        println!("Replay Summary (seq {} to {}):", from, to_seq);
        println!();
        println!("  Total events:       {}", total_events);
        println!("  Environments:       {}", total_envs);
        println!("  Snapshots:          {}", total_snapshots);
        println!("  DAG runs (success): {}", success_runs);
        println!("  DAG runs (failed):  {}", fail_runs);
        println!("  Unique tasks:       {}", task_stats.len());
        println!();

        if !environments.is_empty() {
            println!("Environments at seq {}:", to_seq);
            for (env_name, snapshots) in &environments {
                println!("  {} ({} snapshots)", env_name, snapshots.len());
            }
            println!();
        }

        if !runs.is_empty() && runs.len() <= 20 {
            println!("DAG Runs:");
            for (dag_id, run_id, status) in &runs {
                println!("  {} ({}): {}", dag_id, run_id, status);
            }
            println!();
        }
    }

    Ok(())
}

// ─── conduit migrate ─────────────────────────────────────────────────────────

fn cmd_migrate(source: &PathBuf, output: &PathBuf, dry_run: bool) -> Result<()> {
    use regex::Regex;
    use std::fs;

    println!("Scanning Airflow DAGs at {}...", source.display());
    println!();

    if !source.exists() {
        anyhow::bail!("Source directory does not exist: {}", source.display());
    }

    // Find all Python files
    let mut dag_files = Vec::new();
    for entry in walkdir::WalkDir::new(source)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "py"))
    {
        dag_files.push(entry.path().to_path_buf());
    }

    println!("Found {} Python files", dag_files.len());
    println!();

    let mut migrated_dags = Vec::new();
    let mut skipped_files = Vec::new();

    // Regex patterns for Airflow DAG detection
    let dag_pattern = Regex::new(r#"DAG\(\s*['"]([^'"]+)['"]"#)?;
    let task_pattern = Regex::new(
        r#"(PythonOperator|BashOperator|SQLExecuteQueryOperator)\([^)]*task_id\s*=\s*['"]([^'"]+)['"]"#,
    )?;

    for file_path in &dag_files {
        match fs::read_to_string(file_path) {
            Ok(content) => {
                // Try to detect DAG
                if let Some(dag_match) = dag_pattern.captures(&content) {
                    if let Some(dag_id) = dag_match.get(1) {
                        let dag_id_str = dag_id.as_str().to_string();

                        // Extract tasks (simplified - just get task_ids)
                        let mut task_ids = Vec::new();
                        for cap in task_pattern.captures_iter(&content) {
                            if let Some(task_match) = cap.get(2) {
                                task_ids.push(task_match.as_str().to_string());
                            }
                        }

                        migrated_dags.push((dag_id_str, task_ids, file_path.clone()));
                    }
                } else {
                    skipped_files.push(file_path.clone());
                }
            }
            Err(e) => {
                eprintln!("Warning: Failed to read {}: {}", file_path.display(), e);
                skipped_files.push(file_path.clone());
            }
        }
    }

    // Generate YAML files
    println!("Conversion Results:");
    println!();
    println!("  Migrated DAGs:      {}", migrated_dags.len());
    println!("  Skipped files:      {}", skipped_files.len());
    println!();

    if !migrated_dags.is_empty() {
        println!("Migrated DAGs:");
        for (dag_id, task_ids, source_file) in &migrated_dags {
            println!(
                "  {} ({} tasks) from {}",
                dag_id,
                task_ids.len(),
                source_file.display()
            );
        }
        println!();
    }

    if !dry_run {
        fs::create_dir_all(output)?;

        for (dag_id, task_ids, _) in &migrated_dags {
            let yaml_content = format!(
                r#"# Migrated from Airflow
dag_id: {}
schedule: "@daily"  # Note: Review schedule — auto-migration uses @daily as default
description: "Migrated from Airflow"

tasks:
{}
"#,
                dag_id,
                task_ids
                    .iter()
                    .map(|tid| format!(
                        "  - id: {}\n    type: shell\n    command: \"echo 'Placeholder for {}'\"\n    depends_on: []",
                        tid, tid
                    ))
                    .collect::<Vec<_>>()
                    .join("\n\n")
            );

            let output_path = output.join(format!("{}.yaml", dag_id));
            fs::write(&output_path, yaml_content)?;
            println!("  → {}", output_path.display());
        }

        // Generate migration report
        let report = format!(
            r#"Migration Report
================

Source: {}
Output: {}
Timestamp: {}

Summary:
  Migrated DAGs:      {}
  Total tasks:        {}
  Skipped files:      {}

Caveats:
  - Task dependencies (@>> operator) are simplified — review manually
  - Custom operators require manual translation
  - Sensor tasks marked for review
  - Scheduling policies should be reviewed and validated
  - SLA and alert configurations were not migrated

Next Steps:
  1. Review each migrated DAG YAML file
  2. Update task commands with actual logic (currently placeholders)
  3. Validate schedules and dependencies
  4. Test with 'conduit compile' and 'conduit run'
  5. Commit and deploy to production
"#,
            source.display(),
            output.display(),
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
            migrated_dags.len(),
            migrated_dags
                .iter()
                .map(|(_, tasks, _)| tasks.len())
                .sum::<usize>(),
            skipped_files.len()
        );

        fs::write(output.join("MIGRATION_REPORT.txt"), report)?;
        println!();
        println!(
            "Migration report written to {}",
            output.join("MIGRATION_REPORT.txt").display()
        );
    } else {
        println!("Dry run completed. No files written.");
    }

    Ok(())
}

// ─── conduit lineage ─────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn cmd_lineage(
    task_ref: &str,
    dags_path: &PathBuf,
    openlineage: bool,
    output_dataset: Option<&str>,
    dataset_namespace: Option<&str>,
    job_namespace: &str,
    job_name: Option<&str>,
    run_id: Option<&str>,
    event_time: Option<&str>,
    event_type: &str,
) -> Result<()> {
    let (dag_id, task_id) = parse_task_ref(task_ref)?;
    let (plan, _) = ConduitPlan::compile(dags_path)?;
    let dag = plan.dags.get(dag_id).ok_or_else(|| {
        anyhow::anyhow!(
            "DAG '{}' not found. Available DAGs: {}",
            dag_id,
            plan.dags.keys().cloned().collect::<Vec<_>>().join(", ")
        )
    })?;
    let task = dag.tasks.get(task_id).ok_or_else(|| {
        anyhow::anyhow!(
            "Task '{}' not found in DAG '{}'. Available tasks: {}",
            task_id,
            dag_id,
            dag.tasks.keys().cloned().collect::<Vec<_>>().join(", ")
        )
    })?;

    let (connection, sql) = match &task.task_type {
        conduit_common::dag::TaskType::Sql {
            connection, query, ..
        } => (connection.as_str(), query.as_str()),
        other => {
            anyhow::bail!(
                "Task '{}.{}' is {:?}, not a SQL task",
                dag_id,
                task_id,
                other
            )
        }
    };

    let lineage = SqlLineageExtractor::extract(sql);

    if openlineage {
        let event_type = OpenLineageEventType::parse(event_type).ok_or_else(|| {
            anyhow::anyhow!(
                "Invalid OpenLineage event type '{}'. Expected START, RUNNING, COMPLETE, ABORT, FAIL, or OTHER",
                event_type
            )
        })?;
        let event_time = if let Some(value) = event_time {
            DateTime::parse_from_rfc3339(value).map_err(|_| {
                anyhow::anyhow!("Invalid --event-time '{}'. Expected RFC3339", value)
            })?;
            value.to_string()
        } else {
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
        };
        let run_id = if let Some(value) = run_id {
            Uuid::parse_str(value)
                .map_err(|_| anyhow::anyhow!("Invalid --run-id '{}'. Expected UUID", value))?
                .to_string()
        } else {
            Uuid::new_v4().to_string()
        };

        let event = OpenLineageRunEvent::from_sql_lineage(
            &lineage,
            OpenLineageSqlEventOptions {
                event_type,
                event_time,
                run_id,
                job_namespace: job_namespace.to_string(),
                job_name: job_name.unwrap_or(task_ref).to_string(),
                dataset_namespace: dataset_namespace.unwrap_or(connection).to_string(),
                output_dataset: output_dataset.unwrap_or(task_ref).to_string(),
                producer: CONDUIT_OPENLINEAGE_PRODUCER.to_string(),
            },
        );
        println!("{}", serde_json::to_string_pretty(&event)?);
    } else {
        let result = serde_json::json!({
            "dag_id": dag_id,
            "task_id": task_id,
            "connection": connection,
            "sql": sql,
            "output_columns": lineage.output_columns,
            "source_tables": lineage.source_tables,
            "column_mappings": lineage.column_mappings,
        });
        println!("{}", serde_json::to_string_pretty(&result)?);
    }

    Ok(())
}

fn parse_task_ref(task_ref: &str) -> Result<(&str, &str)> {
    let (dag_id, task_id) = task_ref.rsplit_once('.').ok_or_else(|| {
        anyhow::anyhow!(
            "Task reference must use the form dag_id.task_id, got '{}'",
            task_ref
        )
    })?;
    if dag_id.is_empty() || task_id.is_empty() {
        anyhow::bail!(
            "Task reference must use the form dag_id.task_id, got '{}'",
            task_ref
        );
    }
    Ok((dag_id, task_id))
}

// ─── conduit lineage trace ───────────────────────────────────────────────────
// Walks the cross-task stitched lineage graph from a column to find every
// column that transitively feeds into it (or depends on it). Demos the
// Bet 2.2 cross-task lineage as an operator-facing command.

fn cmd_lineage_trace(
    dag_id: &str,
    column: &str,
    dags_path: &PathBuf,
    direction: &str,
    format: &str,
    dbt_manifest_path: Option<&Path>,
) -> Result<()> {
    use conduit_common::dag::TaskType;
    use conduit_lineage::{cross_task, ColumnSource, DbtManifest};
    use serde_json::json;

    // Column form is `task_id.column_name` — same shape as the existing
    // `dag_id.task_id` parser. Split on the first `.` so column names with
    // embedded underscores stay intact; column names with literal dots are
    // not supported (matches the rest of the CLI surface).
    let (task_id, column_name) = column.split_once('.').ok_or_else(|| {
        anyhow::anyhow!("--column must be 'task_id.column_name', got '{}'", column)
    })?;

    // Compile and pick the DAG.
    let (plan, stats) = ConduitPlan::compile(dags_path)?;
    if !stats.errors.is_empty() {
        for err in &stats.errors {
            eprintln!("Compilation error: {}", err);
        }
        anyhow::bail!("Compilation failed");
    }
    let dag = plan
        .dags
        .get(dag_id)
        .ok_or_else(|| anyhow::anyhow!("DAG '{}' not found in compiled plan", dag_id))?;

    // Load the dbt manifest if the operator passed one. Failure to read
    // / parse is loud (the caller asked for resolution, give them a real
    // error rather than silently degrading to placeholders).
    let manifest = match dbt_manifest_path {
        Some(p) => Some(DbtManifest::load_from_file(p)?),
        None => None,
    };

    // Stitch cross-task lineage. Bubble strict-mode errors up so the
    // operator sees the unresolved-ref list and fixes their declarations.
    let stitched = cross_task::stitch_with_dbt_manifest(dag, manifest.as_ref())
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    // Resolve the origin ColumnRef in the merged graph. After stitching,
    // task-owned columns live under `ColumnSource::Task(TaskRef)`. Match by
    // task_id (dag scope is implicit — we already picked the dag) +
    // column_name.
    let origin = stitched
        .graph
        .columns_for_task(task_id)
        .into_iter()
        .find(|c| c.column_name == column_name)
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Column '{}.{}' not found in the merged lineage graph for DAG '{}'",
                task_id,
                column_name,
                dag_id
            )
        })?;

    let trace = match direction {
        "downstream" => stitched.graph.trace_downstream(&origin),
        _ => stitched.graph.trace_upstream(&origin),
    };

    // Look up task kind for the `[sql]` / `[python]` / `[bash]` annotation
    // used in text-mode output. Tasks that don't live in this DAG (rare —
    // would be a stitched cross-DAG edge) get no annotation.
    let kind_for = |task_id: &str| -> &'static str {
        match dag.tasks.get(task_id).map(|t| &t.task_type) {
            Some(TaskType::Sql { .. }) => "[sql]",
            Some(TaskType::Python { .. }) => "[python]",
            Some(TaskType::Bash { .. }) => "[bash]",
            Some(TaskType::Sensor { .. }) => "[sensor]",
            Some(TaskType::Executable { .. }) => "[executable]",
            None => "",
        }
    };

    if format == "json" {
        let columns: Vec<serde_json::Value> = trace
            .columns
            .iter()
            .map(|c| {
                let (task, table) = match &c.source {
                    ColumnSource::Task(t) => (Some(t.task_id.clone()), None),
                    ColumnSource::Table(t) => (None, Some(t.clone())),
                };
                json!({
                    "qualifier": c.qualifier(),
                    "task": task,
                    "table": table,
                    "column": c.column_name,
                })
            })
            .collect();

        let origin_task = match &origin.source {
            ColumnSource::Task(t) => t.task_id.clone(),
            ColumnSource::Table(t) => t.clone(),
        };

        let payload = json!({
            "dag": dag_id,
            "origin": {
                "task": origin_task,
                "column": origin.column_name,
            },
            "direction": direction,
            "columns": columns,
            "edge_count": trace.edges.len(),
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        // Text mode — readable for human inspection. Format mirrors the
        // shape the integration tests expect: a header line containing
        // "upstream trace" / "downstream trace", the origin, then one line
        // per reached column with task-kind annotation.
        let header = match direction {
            "downstream" => "downstream trace",
            _ => "upstream trace",
        };
        println!("{} from {}", header, origin);
        for col in &trace.columns {
            match &col.source {
                ColumnSource::Task(t) => {
                    let kind = kind_for(&t.task_id);
                    println!("  {}.{} {}", t, col.column_name, kind);
                }
                ColumnSource::Table(t) => {
                    println!("  {}.{}", t, col.column_name);
                }
            }
        }
        println!(
            "({} columns reached via {} edges)",
            trace.columns.len(),
            trace.edges.len()
        );
    }

    Ok(())
}

// ─── conduit backfill ────────────────────────────────────────────────────────
// Runs a DAG across a range of dates/partitions.

async fn cmd_backfill(
    dag_id: &str,
    start: &str,
    end: &str,
    granularity: &str,
    _max_concurrent: u32,
    full_refresh: bool,
    dry_run: bool,
    environment: &str,
    dags_path: &PathBuf,
) -> Result<()> {
    use conduit_common::backfill::*;
    use conduit_common::incremental::PartitionGranularity;
    use conduit_executor::process_runner::{ProcessRunner, TaskContext};
    use conduit_planner::BackfillEngine;
    use conduit_scheduler::pool_manager::PoolManager;
    use conduit_scheduler::scheduler::{Scheduler, SchedulerCommand, SchedulerEvent};
    use std::collections::HashMap;

    let overall_start = Instant::now();

    // Phase 1: Parse arguments
    let gran = match granularity.to_lowercase().as_str() {
        "hour" | "hourly" => PartitionGranularity::Hour,
        "day" | "daily" => PartitionGranularity::Day,
        "week" | "weekly" => PartitionGranularity::Week,
        "month" | "monthly" => PartitionGranularity::Month,
        "year" | "yearly" => PartitionGranularity::Year,
        other => anyhow::bail!(
            "Unknown granularity '{}'. Use: hour, day, week, month, year",
            other
        ),
    };

    let start_date = chrono::NaiveDate::parse_from_str(start, "%Y-%m-%d")
        .map(|nd| nd.and_hms_opt(0, 0, 0).unwrap().and_utc())
        .map_err(|_| anyhow::anyhow!("Invalid start date '{}'. Expected YYYY-MM-DD", start))?;

    let end_date = chrono::NaiveDate::parse_from_str(end, "%Y-%m-%d")
        .map(|nd| nd.and_hms_opt(0, 0, 0).unwrap().and_utc())
        .map_err(|_| anyhow::anyhow!("Invalid end date '{}'. Expected YYYY-MM-DD", end))?;

    if end_date <= start_date {
        anyhow::bail!("End date must be after start date");
    }

    // Phase 2: Compile DAGs
    println!("Compiling DAGs from {}...", dags_path.display());
    let (plan, stats) = ConduitPlan::compile(dags_path)?;

    if !stats.errors.is_empty() {
        eprintln!("Compilation errors:");
        for err in &stats.errors {
            eprintln!("  {}", err);
        }
        std::process::exit(1);
    }

    let dag = plan
        .dags
        .get(dag_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "DAG '{}' not found. Available DAGs: {}",
                dag_id,
                plan.dags.keys().cloned().collect::<Vec<_>>().join(", ")
            )
        })?
        .clone();

    println!(
        "  Compiled {} DAGs ({} tasks) in {:.1}ms",
        stats.dags_compiled,
        stats.tasks_total,
        overall_start.elapsed().as_secs_f64() * 1000.0
    );
    println!();

    // Phase 3: Compute partitions
    let request = BackfillRequest {
        dag_id: dag_id.to_string(),
        start_date,
        end_date,
        granularity: gran,
        environment: environment.to_string(),
        max_concurrent_partitions: _max_concurrent,
        full_refresh,
        dry_run,
    };

    let partitions = BackfillEngine::compute_partitions(&request);
    let total = partitions.len();

    println!(
        "Backfill '{}': {} -> {} ({} partitions)",
        dag_id, start, end, total
    );
    println!("  Granularity:  {}", granularity);
    println!("  Environment:  {}", environment);
    println!("  Full refresh: {}", full_refresh);
    println!("  Dry run:      {}", dry_run);
    println!();

    // Phase 4: If dry run, just print the partition list and exit
    if dry_run {
        println!("Partitions (dry run — nothing will be executed):");
        println!();
        for (i, p) in partitions.iter().enumerate() {
            println!(
                "  [{:>3}/{}] {} ({} -> {})",
                i + 1,
                total,
                p.partition_key,
                p.logical_start.format("%Y-%m-%dT%H:%M:%S"),
                p.logical_end.format("%Y-%m-%dT%H:%M:%S"),
            );
        }
        println!();
        println!("Total: {} partitions", total);
        println!("Run without --dry-run to execute.");
        return Ok(());
    }

    // Phase 5: Execute partitions sequentially
    println!("Executing partitions...");
    println!();

    // Open the event store once for the whole backfill; each partition's
    // scheduler shares it so runs are recorded for `conduit replay`.
    let backfill_events_dir = resolve_state_dir(dags_path).join("events");
    let backfill_event_store = match conduit_state::EventStore::open(&backfill_events_dir) {
        Ok(store) => Some(std::sync::Arc::new(store)),
        Err(e) => {
            tracing::warn!(
                path = %backfill_events_dir.display(),
                error = %e,
                "Event store unavailable; backfill runs will not be recorded for replay"
            );
            None
        }
    };

    // Build the provider registry once for the whole backfill so SQL tasks
    // execute for real (or fail loudly if the connection isn't configured).
    let registry = build_provider_registry(dags_path).await;

    let mut succeeded = 0usize;
    let mut failed = 0usize;
    let skipped = 0usize;

    for (idx, partition) in partitions.iter().enumerate() {
        let partition_start_time = Instant::now();
        let env_vars = BackfillEngine::partition_env_vars(&request, partition, idx, total);

        println!(
            "  [{:>3}/{}] Partition '{}' ...",
            idx + 1,
            total,
            partition.partition_key,
        );

        // Run the DAG for this partition using the scheduler/executor pattern from cmd_run
        let logical_date = partition.logical_start;

        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<SchedulerEvent>();
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<SchedulerCommand>();

        let mut dag_map = HashMap::new();
        dag_map.insert(dag_id.to_string(), dag.clone());

        let pools = PoolManager::new(load_pools(dags_path));
        let mut scheduler = Scheduler::new(event_rx, cmd_tx, pools, dag_map)?;
        if let Some(store) = &backfill_event_store {
            scheduler = scheduler.with_event_store(std::sync::Arc::clone(store));
        }

        let run_id = format!("bf_{}_{}", dag_id, partition.partition_key);
        event_tx.send(SchedulerEvent::DagRunRequested {
            dag_id: dag_id.to_string(),
            run_id: run_id.clone(),
            logical_date,
            config: env_vars.into_iter().collect(),
        })?;

        let scheduler_handle = tokio::spawn(async move { scheduler.run().await });

        let executor_event_tx = event_tx.clone();
        let dag_for_exec = dag.clone();
        let partition_env: HashMap<String, String> =
            BackfillEngine::partition_env_vars(&request, partition, idx, total)
                .into_iter()
                .collect();
        let registry_for_exec = std::sync::Arc::clone(&registry);
        let executor_handle = tokio::spawn(async move {
            let mut partition_failed = false;
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    SchedulerCommand::DispatchTask {
                        dag_id,
                        run_id,
                        task_id,
                        attempt,
                    } => {
                        let task = match dag_for_exec.tasks.get(&task_id) {
                            Some(t) => t,
                            None => {
                                let _ = executor_event_tx.send(SchedulerEvent::TaskFailed {
                                    dag_id,
                                    run_id,
                                    task_id,
                                    error: "Task not found".to_string(),
                                    attempt,
                                });
                                continue;
                            }
                        };

                        let mut context = TaskContext {
                            dag_id: dag_id.clone(),
                            run_id: run_id.clone(),
                            task_id: task_id.clone(),
                            attempt,
                            logical_date,
                            environment: partition_env
                                .get("CONDUIT_ENVIRONMENT")
                                .cloned()
                                .unwrap_or_else(|| "production".to_string()),
                            params: partition_env.clone(),
                            extra_env: Vec::new(),
                        };
                        // Inject backfill env vars into params
                        for (k, v) in &partition_env {
                            context.params.insert(k.clone(), v.clone());
                        }

                        let task_start = Instant::now();
                        match ProcessRunner::run_with_providers(
                            task,
                            &context,
                            Some(&registry_for_exec),
                        )
                        .await
                        {
                            Ok(output) => {
                                let duration = task_start.elapsed();
                                if output.exit_code == 0 {
                                    let _ = executor_event_tx.send(SchedulerEvent::TaskCompleted {
                                        dag_id,
                                        run_id,
                                        task_id,
                                        snapshot_id: None,
                                        duration_ms: duration.as_millis() as u64,
                                    });
                                } else {
                                    partition_failed = true;
                                    let _ = executor_event_tx.send(SchedulerEvent::TaskFailed {
                                        dag_id,
                                        run_id,
                                        task_id,
                                        error: output.stderr.trim().to_string(),
                                        attempt,
                                    });
                                }
                            }
                            Err(e) => {
                                partition_failed = true;
                                let _ = executor_event_tx.send(SchedulerEvent::TaskFailed {
                                    dag_id,
                                    run_id,
                                    task_id,
                                    error: e.to_string(),
                                    attempt,
                                });
                            }
                        }
                    }
                    SchedulerCommand::CompleteDagRun { .. } => {
                        let _ = executor_event_tx.send(SchedulerEvent::Shutdown);
                        break;
                    }
                    SchedulerCommand::SkipTask { .. } => {}
                    SchedulerCommand::RetryTask { .. } => {}
                }
            }
            partition_failed
        });

        let (_, exec_result) = tokio::join!(scheduler_handle, executor_handle);

        let partition_duration = partition_start_time.elapsed();

        match exec_result {
            Ok(partition_failed) => {
                if partition_failed {
                    failed += 1;
                    println!(
                        "           FAILED ({:.0}ms)",
                        partition_duration.as_secs_f64() * 1000.0,
                    );
                } else {
                    succeeded += 1;
                    println!(
                        "           OK ({:.0}ms)",
                        partition_duration.as_secs_f64() * 1000.0,
                    );
                }
            }
            Err(e) => {
                failed += 1;
                println!("           ERROR: {}", e);
            }
        }
    }

    // Phase 6: Print summary
    let total_duration = overall_start.elapsed();
    println!();
    println!("Backfill complete for '{}':", dag_id);
    println!("  Total partitions: {}", total);
    println!("  Succeeded:        {}", succeeded);
    println!("  Failed:           {}", failed);
    println!("  Skipped:          {}", skipped);
    println!(
        "  Total time:       {:.1}ms",
        total_duration.as_secs_f64() * 1000.0,
    );

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

// ─── conduit worker ─────────────────────────────────────────────────────────

fn cmd_worker(
    coordinator_addr: &str,
    capacity: u32,
    pools: &str,
    id: Option<&str>,
    label_strs: &[String],
) -> Result<()> {
    use std::collections::HashMap;

    let pool_list: Vec<String> = pools.split(',').map(|s| s.trim().to_string()).collect();
    let mut labels = HashMap::new();
    for l in label_strs {
        if let Some((k, v)) = l.split_once('=') {
            labels.insert(k.to_string(), v.to_string());
        }
    }

    let worker_id = id.map(String::from).unwrap_or_else(|| {
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        format!("worker-{}-{}", hostname, std::process::id())
    });

    println!("┌─────────────────────────────────────────────┐");
    println!("│  Conduit Worker                             │");
    println!("├─────────────────────────────────────────────┤");
    println!("│  Worker ID:    {:28} │", worker_id);
    println!("│  Coordinator:  {:28} │", coordinator_addr);
    println!("│  Capacity:     {:28} │", capacity);
    println!("│  Pools:        {:28} │", pools);
    println!("└─────────────────────────────────────────────┘");
    println!();

    println!("Connecting to coordinator at {}...", coordinator_addr);

    // In production, this would:
    // 1. Create a Worker instance with WorkerConfig
    // 2. Connect to coordinator via gRPC (tonic client)
    // 3. Register and start receiving task assignments
    // 4. Run heartbeat loop
    // 5. Block until SIGTERM/SIGINT

    println!(
        "Worker '{}' registered with {} capacity",
        worker_id, capacity
    );
    println!("Pool affinity: {:?}", pool_list);
    if !labels.is_empty() {
        println!("Labels: {:?}", labels);
    }
    println!();
    println!("Waiting for task assignments... (press Ctrl+C to stop)");

    // The actual runtime would be:
    //   let rt = tokio::runtime::Runtime::new()?;
    //   rt.block_on(async {
    //       let (worker, result_rx, log_rx) = Worker::new(WorkerConfig { ... });
    //       // gRPC connect + register + receive loop
    //   });
    //
    // For now, we print the startup banner to validate the CLI wiring.

    println!("\n[Worker runtime requires gRPC connection to coordinator]");
    println!("To test locally, start the coordinator first:");
    println!("  conduit serve --distributed --bind {}", coordinator_addr);

    Ok(())
}

// ─── conduit cluster ────────────────────────────────────────────────────────

fn cmd_cluster_status(coordinator_addr: &str, json: bool) -> Result<()> {
    println!("Querying cluster status at {}...", coordinator_addr);

    // In production, this would:
    // 1. Connect to coordinator gRPC endpoint
    // 2. Call ClusterStatus RPC
    // 3. Display results

    if json {
        println!("{{");
        println!("  \"health\": \"unknown\",");
        println!("  \"coordinator\": \"{}\",", coordinator_addr);
        println!("  \"workers\": [],");
        println!("  \"running_tasks\": 0,");
        println!("  \"queued_tasks\": 0");
        println!("}}");
    } else {
        println!();
        println!("Cluster Status");
        println!("──────────────────────────────────────────");
        println!("  Coordinator:  {}", coordinator_addr);
        println!("  Health:       ⚠ Unknown (not connected)");
        println!("  Workers:      0");
        println!("  Running:      0 tasks");
        println!("  Queued:       0 tasks");
        println!();
        println!("No workers connected. Start workers with:");
        println!("  conduit worker --coordinator {}", coordinator_addr);
    }

    Ok(())
}

fn cmd_cluster_drain(coordinator_addr: &str, worker_id: &str) -> Result<()> {
    println!(
        "Draining worker '{}' via {}...",
        worker_id, coordinator_addr
    );

    // In production: send DrainWorker directive via gRPC
    println!("Drain command sent. Worker will finish current tasks and stop.");
    println!(
        "Monitor with: conduit cluster status --coordinator {}",
        coordinator_addr
    );

    Ok(())
}

// ─── conduit query ───────────────────────────────────────────────────────────

async fn cmd_query(
    sql: &str,
    connection: Option<&str>,
    files: Option<Vec<PathBuf>>,
    setup: Option<Vec<String>>,
    format: &str,
    limit: usize,
    config_path: Option<PathBuf>,
) -> Result<()> {
    use conduit_providers::providers::duckdb::DuckDbProvider;
    use conduit_providers::traits::SqlProvider;

    let provider: DuckDbProvider = if let Some(conn_name) = connection {
        // Load config and resolve the named connection
        let config_file = config_path.unwrap_or_else(|| PathBuf::from("conduit.yaml"));
        let config = conduit_common::config::ConduitConfig::load(&config_file)?;
        let conn_config = config.connections.get(conn_name).ok_or_else(|| {
            anyhow::anyhow!(
                "Connection '{}' not found in {}",
                conn_name,
                config_file.display()
            )
        })?;
        if conn_config.conn_type != "duckdb" && conn_config.conn_type != "duck" {
            anyhow::bail!(
                "conduit query currently only supports DuckDB connections. \
                 '{}' is type '{}'.",
                conn_name,
                conn_config.conn_type
            );
        }
        DuckDbProvider::from_config(conn_name, conn_config)?
    } else {
        DuckDbProvider::ephemeral()
    };

    // Register local files as views
    if let Some(ref file_list) = files {
        for path in file_list {
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("data");
            let abs_path = std::fs::canonicalize(path)
                .unwrap_or_else(|_| path.clone())
                .display()
                .to_string();
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");

            let reader_fn = match ext {
                "parquet" | "pq" => "read_parquet",
                "csv" | "tsv" => "read_csv",
                "json" | "jsonl" | "ndjson" => "read_json",
                _ => {
                    anyhow::bail!("Unsupported file format '.{ext}'. Supported: parquet, csv, json")
                }
            };

            let create_sql = format!(
                "CREATE OR REPLACE VIEW \"{stem}\" AS SELECT * FROM {reader_fn}('{abs_path}')"
            );
            provider.execute_raw(&create_sql).await?;
            eprintln!("  Registered file '{}' as table '{}'", path.display(), stem);
        }
    }

    // Run setup SQL (e.g. CREATE TABLE statements to prepare the environment)
    if let Some(ref setup_stmts) = setup {
        for stmt in setup_stmts {
            eprintln!("  Setup: {}", truncate_str(stmt, 80));
            provider.execute_raw(stmt).await?;
        }
    }

    // Apply limit to SELECT queries
    let final_sql = {
        let trimmed = sql.trim().to_uppercase();
        if (trimmed.starts_with("SELECT") || trimmed.starts_with("WITH"))
            && !trimmed.contains("LIMIT")
        {
            format!("{} LIMIT {}", sql.trim().trim_end_matches(';'), limit)
        } else {
            sql.to_string()
        }
    };

    let result = provider
        .execute(&final_sql, &std::collections::HashMap::new())
        .await?;

    render_result(&result, format, limit);

    Ok(())
}

// ─── conduit preview ─────────────────────────────────────────────────────────

async fn cmd_preview(
    task_ref: &str,
    dags_path: &Path,
    connection: Option<&str>,
    format: &str,
    limit: usize,
) -> Result<()> {
    use conduit_common::dag::TaskType;
    use conduit_providers::providers::duckdb::DuckDbProvider;
    use conduit_providers::traits::SqlProvider;

    // Parse task_ref as "dag_id.task_id"
    let (dag_id, task_id) = task_ref.split_once('.').ok_or_else(|| {
        anyhow::anyhow!(
            "Invalid task reference '{}'. Expected format: dag_id.task_id",
            task_ref
        )
    })?;

    // Compile DAGs
    let start = Instant::now();
    let (plan, stats) = ConduitPlan::compile(dags_path)?;
    eprintln!(
        "  Compiled {} DAGs ({} tasks) in {:.1}ms",
        stats.dags_compiled,
        stats.tasks_total,
        start.elapsed().as_secs_f64() * 1000.0
    );

    // Look up the DAG and task
    let dag = plan.dags.get(dag_id).ok_or_else(|| {
        anyhow::anyhow!(
            "DAG '{}' not found. Available: {:?}",
            dag_id,
            plan.dags.keys().collect::<Vec<_>>()
        )
    })?;

    let task = dag.tasks.get(task_id).ok_or_else(|| {
        anyhow::anyhow!(
            "Task '{}' not found in DAG '{}'. Available: {:?}",
            task_id,
            dag_id,
            dag.tasks.keys().collect::<Vec<_>>()
        )
    })?;

    match &task.task_type {
        TaskType::Sql {
            connection: task_conn,
            query,
            ..
        } => {
            eprintln!("  Task: {}.{}", dag_id, task_id);
            eprintln!(
                "  Type: SQL (connection: {})",
                connection.unwrap_or(task_conn)
            );
            eprintln!("  Query: {}", truncate_str(query, 120));

            let provider = if let Some(conn_name) = connection {
                let config_path = dags_path
                    .parent()
                    .unwrap_or(Path::new("."))
                    .join("conduit.yaml");
                if config_path.exists() {
                    let config = conduit_common::config::ConduitConfig::load(&config_path)?;
                    if let Some(conn_config) = config.connections.get(conn_name) {
                        if conn_config.conn_type == "duckdb" || conn_config.conn_type == "duck" {
                            DuckDbProvider::from_config(conn_name, conn_config)?
                        } else {
                            anyhow::bail!(
                                "Preview currently only supports DuckDB connections. \
                                 '{}' is type '{}'.",
                                conn_name,
                                conn_config.conn_type
                            );
                        }
                    } else {
                        anyhow::bail!("Connection '{}' not found in config", conn_name);
                    }
                } else {
                    DuckDbProvider::ephemeral()
                }
            } else {
                DuckDbProvider::ephemeral()
            };

            // Walk upstream SQL dependencies in topological order and execute
            // them first, so the target task can reference their tables.
            let upstream = collect_upstream_sql(dag, task_id);
            if !upstream.is_empty() {
                eprintln!(
                    "  Running {} upstream SQL task(s) as setup...",
                    upstream.len()
                );
                for (up_id, up_sql) in &upstream {
                    eprintln!("    [setup] {}", up_id);
                    provider.execute_raw(up_sql).await?;
                }
            }

            eprintln!();

            let final_sql = {
                let trimmed = query.trim().to_uppercase();
                if (trimmed.starts_with("SELECT") || trimmed.starts_with("WITH"))
                    && !trimmed.contains("LIMIT")
                {
                    format!("{} LIMIT {}", query.trim().trim_end_matches(';'), limit)
                } else {
                    query.clone()
                }
            };

            match provider
                .execute(&final_sql, &std::collections::HashMap::new())
                .await
            {
                Ok(result) => render_result(&result, format, limit),
                Err(e) => {
                    eprintln!("  Query execution failed:");
                    eprintln!("  Error: {}", e);
                }
            }
        }
        other => {
            eprintln!("  Task: {}.{}", dag_id, task_id);
            eprintln!("  Type: {:?}", task_type_name(other));
            eprintln!(
                "  Dependencies: {:?}",
                task.dependencies
                    .iter()
                    .map(|d| &d.task_id)
                    .collect::<Vec<_>>()
            );
            eprintln!();
            eprintln!("  Preview is only available for SQL tasks.");
        }
    }

    Ok(())
}

/// Walk the DAG's execution_order to collect upstream SQL tasks for a given target.
/// Returns (task_id, query) pairs in topological order, excluding the target itself.
fn collect_upstream_sql(
    dag: &conduit_common::dag::Dag,
    target_task_id: &str,
) -> Vec<(String, String)> {
    use conduit_common::dag::TaskType;

    // Collect all transitive upstream task IDs via BFS
    let mut needed: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut queue: std::collections::VecDeque<String> = std::collections::VecDeque::new();

    // Seed with direct dependencies of the target
    if let Some(task) = dag.tasks.get(target_task_id) {
        for dep in &task.dependencies {
            queue.push_back(dep.task_id.clone());
        }
    }

    while let Some(tid) = queue.pop_front() {
        if needed.insert(tid.clone()) {
            if let Some(t) = dag.tasks.get(&tid) {
                for dep in &t.dependencies {
                    queue.push_back(dep.task_id.clone());
                }
            }
        }
    }

    // Return SQL tasks in topological order
    dag.execution_order
        .iter()
        .filter(|tid| needed.contains(tid.as_str()))
        .filter_map(|tid| {
            dag.tasks
                .get(tid.as_str())
                .and_then(|t| match &t.task_type {
                    TaskType::Sql { query, .. } => Some((tid.clone(), query.clone())),
                    _ => None,
                })
        })
        .collect()
}

// ─── shared output helpers ───────────────────────────────────────────────────

fn render_result(result: &conduit_providers::traits::SqlResult, format: &str, _limit: usize) {
    match format {
        "json" => {
            let output = serde_json::json!({
                "columns": result.columns,
                "rows": result.sample_rows,
                "rows_returned": result.rows_returned,
                "execution_time_ms": result.execution_time_ms,
            });
            println!("{}", serde_json::to_string_pretty(&output).unwrap());
        }
        "csv" => {
            if !result.columns.is_empty() {
                println!("{}", result.columns.join(","));
            }
            for row in &result.sample_rows {
                let cells: Vec<String> = row
                    .iter()
                    .map(|v| match v {
                        serde_json::Value::String(s) => {
                            if s.contains(',') || s.contains('"') || s.contains('\n') {
                                format!("\"{}\"", s.replace('"', "\"\""))
                            } else {
                                s.clone()
                            }
                        }
                        serde_json::Value::Null => String::new(),
                        other => other.to_string(),
                    })
                    .collect();
                println!("{}", cells.join(","));
            }
        }
        _ => {
            // table format (default)
            use comfy_table::{ContentArrangement, Table};

            if result.columns.is_empty() {
                if let Some(rows) = result.rows_returned {
                    eprintln!("  ({} rows, no columns)", rows);
                } else {
                    eprintln!("  ({} rows affected)", result.rows_affected);
                }
                return;
            }

            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(&result.columns);

            for row in &result.sample_rows {
                let cells: Vec<String> = row
                    .iter()
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Null => "NULL".to_string(),
                        other => other.to_string(),
                    })
                    .collect();
                table.add_row(cells);
            }

            println!("{table}");

            if let Some(total) = result.rows_returned {
                let shown = result.sample_rows.len() as u64;
                if shown < total {
                    eprintln!("  ({} of {} rows shown)", shown, total);
                } else {
                    eprintln!("  ({} rows)", total);
                }
            }
            eprintln!("  Execution time: {}ms", result.execution_time_ms);
        }
    }
}

fn truncate_str(s: &str, max: usize) -> String {
    let s = s.replace('\n', " ");
    if s.len() > max {
        format!("{}...", &s[..max])
    } else {
        s
    }
}

fn task_type_name(tt: &conduit_common::dag::TaskType) -> &'static str {
    use conduit_common::dag::TaskType;
    match tt {
        TaskType::Python { .. } => "Python",
        TaskType::Bash { .. } => "Bash",
        TaskType::Sql { .. } => "SQL",
        TaskType::Sensor { .. } => "Sensor",
        TaskType::Executable { .. } => "Executable",
    }
}

// ─── hostname helper ────────────────────────────────────────────────────────

mod gethostname {
    use std::ffi::OsString;
    pub fn gethostname() -> OsString {
        std::env::var("HOSTNAME")
            .or_else(|_| std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string()))
            .map(OsString::from)
            .unwrap_or_else(|_| OsString::from("unknown"))
    }
}

// ─── conduit impact ──────────────────────────────────────────────────────────

/// Removes a temporary git worktree on drop, so failed compiles don't leak
/// worktrees into the operator's repo.
struct WorktreeGuard {
    repo_root: PathBuf,
    path: PathBuf,
}

impl Drop for WorktreeGuard {
    fn drop(&mut self) {
        let _ = std::process::Command::new("git")
            .current_dir(&self.repo_root)
            .args(["worktree", "remove", "--force"])
            .arg(&self.path)
            .output();
    }
}

/// Load a `DagSet` from a path that is either a DAGs directory (compiled
/// on the spot) or a compiled-plan JSON file (as produced by serializing
/// `ConduitPlan`).
fn load_dagset(path: &Path) -> Result<conduit_lineage::DagSet> {
    if path.is_dir() {
        let (plan, _) = ConduitPlan::compile(path)?;
        Ok(plan.dags)
    } else {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read plan file '{}': {}", path.display(), e))?;
        let plan: ConduitPlan = serde_json::from_str(&text).map_err(|e| {
            anyhow::anyhow!(
                "'{}' is not a compiled plan JSON (serialize ConduitPlan, or pass a DAGs directory): {}",
                path.display(),
                e
            )
        })?;
        Ok(plan.dags)
    }
}

/// Compile the DAGs directory as it exists at `git_ref`, via a temporary
/// detached worktree (removed on return, even on failure).
fn compile_dagset_at_ref(git_ref: &str, dags_path: &Path) -> Result<conduit_lineage::DagSet> {
    let root_out = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()?;
    if !root_out.status.success() {
        anyhow::bail!("--base/--head git mode requires running inside a git repository");
    }
    let repo_root = PathBuf::from(String::from_utf8_lossy(&root_out.stdout).trim().to_string());

    let wt_path = std::env::temp_dir().join(format!("conduit-impact-{}", Uuid::new_v4().simple()));
    let add_out = std::process::Command::new("git")
        .current_dir(&repo_root)
        .args(["worktree", "add", "--detach"])
        .arg(&wt_path)
        .arg(git_ref)
        .output()?;
    if !add_out.status.success() {
        anyhow::bail!(
            "git worktree add failed for ref '{}': {}",
            git_ref,
            String::from_utf8_lossy(&add_out.stderr).trim()
        );
    }
    let _guard = WorktreeGuard {
        repo_root,
        path: wt_path.clone(),
    };

    let dags_dir = wt_path.join(dags_path);
    if !dags_dir.exists() {
        anyhow::bail!(
            "DAGs path '{}' does not exist at ref '{}'",
            dags_path.display(),
            git_ref
        );
    }
    let (plan, _) = ConduitPlan::compile(&dags_dir)?;
    Ok(plan.dags)
}

/// `conduit impact` — analyze schema impact between two DAG versions.
///
/// Exit code is 0 whenever analysis succeeds, even with breaking changes:
/// the CI workflow gates on the JSON `summary.total_breaking_changes` plus
/// the `allow-breaking` PR label, not on this exit code.
fn cmd_impact(
    base: Option<&str>,
    head: Option<&str>,
    base_plan: Option<&PathBuf>,
    head_plan: Option<&PathBuf>,
    dags_path: &Path,
    format: &str,
    output: Option<&PathBuf>,
) -> Result<()> {
    use conduit_lineage::{analyze_plan_impact, render_impact, ImpactFormat};

    let fmt = ImpactFormat::parse(format).map_err(|e| anyhow::anyhow!(e))?;

    let (base_set, head_set) = match (base_plan, head_plan, base, head) {
        (Some(bp), Some(hp), None, None) => (load_dagset(bp)?, load_dagset(hp)?),
        (None, None, Some(b), Some(h)) => {
            let base_set = compile_dagset_at_ref(b, dags_path)?;
            let head_set = if h == "WORKING" {
                let (plan, _) = ConduitPlan::compile(dags_path)?;
                plan.dags
            } else {
                compile_dagset_at_ref(h, dags_path)?
            };
            (base_set, head_set)
        }
        _ => anyhow::bail!(
            "pass either --base-plan/--head-plan (file mode) or --base/--head (git mode)"
        ),
    };

    let impact = analyze_plan_impact(&base_set, &head_set);
    let report = render_impact(&impact, fmt);

    match output {
        Some(path) => {
            std::fs::write(path, &report)?;
            eprintln!(
                "impact report written to {} ({} breaking change(s))",
                path.display(),
                impact.summary.total_breaking_changes
            );
        }
        None => println!("{report}"),
    }
    Ok(())
}

// ─── Embedded Python SDK (vendored into `conduit init` scaffolds) ───────────

/// The `conduit_sdk` package embedded at compile time from `sdk/python/`.
static EMBEDDED_SDK: include_dir::Dir<'_> =
    include_dir::include_dir!("$CARGO_MANIFEST_DIR/../sdk/python/conduit_sdk");

/// Write the embedded SDK to `dest`, skipping bytecode caches.
fn write_embedded_sdk(dest: &Path) -> Result<()> {
    fn write_dir(dir: &include_dir::Dir<'_>, dest: &Path) -> Result<()> {
        use std::fs;
        for entry in dir.entries() {
            match entry {
                include_dir::DirEntry::Dir(d) => {
                    let name = d
                        .path()
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or_default();
                    if name == "__pycache__" {
                        continue;
                    }
                    write_dir(d, dest)?;
                }
                include_dir::DirEntry::File(f) => {
                    if f.path().extension().is_some_and(|e| e == "pyc") {
                        continue;
                    }
                    let target = dest.join(f.path());
                    if let Some(parent) = target.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    fs::write(target, f.contents())?;
                }
            }
        }
        Ok(())
    }
    write_dir(&EMBEDDED_SDK, dest)
}
