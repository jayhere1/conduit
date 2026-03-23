# Conduit

**A Rust-native data pipeline orchestrator.**

Conduit is not "Airflow but faster." It solves problems that Airflow architecturally *cannot* solve — virtual pipeline environments, time-travel debugging, compile-time DAG validation, and plan/apply deployments — all in a single binary with zero external dependencies.

## Quick Start

```bash
# Build
cargo build --release

# Initialize a project
conduit init my-project
cd my-project

# Compile DAGs (tree-sitter parses Python without executing it)
conduit compile

# Run a DAG end-to-end
conduit run hello_world

# Show what would change in production
conduit plan production

# Apply the changes
conduit apply production -y

# Create a virtual environment (instant, zero data copy)
conduit env create staging --from production

# Promote staging to production when ready
conduit env promote staging production
```

## Project Structure

```
conduit/
  conduit-cli/          Binary entry point (conduit command)
  conduit-common/       Shared types: DAG model, errors, events, fingerprints, snapshots, config
  conduit-compiler/     Tree-sitter DAG parser + Kahn's algorithm dependency resolver + benchmarks
  conduit-state/        Event store (RocksDB) + snapshot store + environment manager
  conduit-scheduler/    Event-driven task scheduling with cron, trigger rules, and pool management
  conduit-executor/     Process-based task runtime with timeout enforcement and retry policies
  conduit-planner/      Fingerprint diffing, impact analysis, and plan/apply deployment workflow
  conduit-lineage/      Column-level lineage (Phase 4)
  conduit-api/          REST + WebSocket API (Phase 2)
  examples/dags/        Sample DAG definitions (ETL, marketing, ML pipeline)
```

## What's Implemented

### DAG Compiler (`conduit-compiler`)
Tree-sitter parses Python `@dag`/`@task` definitions **without executing Python**. Extracts schedules, tags, retry policies, pools, timeouts, and data-flow dependencies from call chains. Kahn's algorithm detects cycles, duplicates, and unknown references at compile time. Includes Criterion benchmarks for 10–1,000 DAG workloads.

### Event-Driven Scheduler (`conduit-scheduler`)
Fully async tokio-channel scheduler (no database polling). Manages DAG run state machines (Pending → Queued → Running → Success/Failed/Skipped/Retrying). Evaluates trigger rules (AllSuccess, AllDone, OneSuccess, OneFailed, NoDeps), enforces named resource pools, and parses 5-field cron expressions. ~630 lines of real scheduling logic.

### Task Executor (`conduit-executor`)
Process-isolated task execution with stdin/stdout protocol. Supports Python, Bash, SQL, Sensor, and generic Executable task types. Enforces timeouts via `tokio::time::timeout`, implements fixed and exponential backoff retry policies, and parses structured protocol messages (XCOM, LOG, PROGRESS, METRIC) from task output.

### Plan/Apply Workflow (`conduit-planner`)
Terraform-style change detection and deployment. Computes content-addressable fingerprints for every task in topological order (upstream changes cascade automatically). Compares against environment state, classifies changes as Added/Modified/UpstreamInvalidated/Removed/Unchanged. Impact analyzer computes full transitive blast radius via BFS. Generates serializable deployment plans with snapshot reuse optimization.

### State Layer (`conduit-state`)
- **Event Store**: Append-only RocksDB log with monotonic sequencing, range queries, and crash-safe recovery via `seek_to_last()`.
- **Snapshot Store**: Content-addressable fingerprint index for O(1) snapshot reuse lookups.
- **Environment Manager**: Virtual pipeline environments inspired by SQLMesh. Create/fork/promote/rollback as pointer operations over immutable snapshots.

### CLI Commands
| Command | Description |
|---|---|
| `conduit init <name>` | Scaffold a new project with example DAG |
| `conduit compile [path]` | Parse and validate DAGs, report results |
| `conduit run <dag_id>` | Compile, schedule, and execute a DAG end-to-end |
| `conduit plan [env]` | Show what would change in an environment |
| `conduit apply [env]` | Execute changes and update environment state |
| `conduit env create <name>` | Create a virtual environment (forked from production) |
| `conduit env list` | List all environments |
| `conduit env promote <src> <dst>` | Promote one environment into another |
| `conduit status` | Show system status |

## Build Requirements

- Rust 1.75+ (2021 edition)
- clang/llvm (for RocksDB and tree-sitter compilation)
- On Ubuntu: `apt install clang librocksdb-dev`
- On macOS: `brew install llvm rocksdb`

## Run Tests

```bash
cargo test --workspace
```

## Run Benchmarks

```bash
# Compile 10/100/500/1,000 DAGs and measure parse time
cargo bench -p conduit-compiler
```

## Architecture

**Core insight**: Orchestration is a systems problem being solved with a scripting language. Conduit moves the CPU-bound work (DAG parsing, dependency resolution, scheduling, state management) to Rust while keeping the user-facing SDK in Python.

**Key design decisions**:
1. **Event-sourced state** (not mutable database) — enables time-travel, rollback, zero lock contention
2. **Compile-time validation** (not runtime) — tree-sitter parses DAGs without executing Python
3. **Virtual environments** (not physical copies) — snapshot pointers, not data duplication
4. **Event-driven scheduling** (not polling) — react to state changes via tokio channels
5. **Content-addressable snapshots** — fingerprint-based reuse skips unchanged tasks automatically
6. **Process isolation** — tasks run as child processes with cgroup resource limits

**What makes this architecturally different from Airflow/Dagster/Prefect**:
- They poll a database to find ready tasks; Conduit reacts to events in microseconds.
- They re-execute entire pipelines on change; Conduit fingerprints each task and only re-executes what changed.
- They have no concept of pipeline environments; Conduit creates virtual environments as pointer swaps in O(1).
- They store mutable state; Conduit uses an append-only event log enabling time-travel and instant rollback.
- They parse Python by executing it; Conduit uses tree-sitter for zero-execution static analysis.

## License

Apache-2.0
