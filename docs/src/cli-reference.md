# CLI Reference

Complete reference for all Conduit CLI commands. Every command and flag on this page is taken from the CLI's own `--help` output — run `conduit <command> --help` for the authoritative version.

## Global Options

All commands accept these options:

```bash
--verbose, -v         Show detailed output
--help, -h            Show help for this command
--version, -V         Show Conduit version (top-level only)
```

## Project Commands

### init

Initialize a new Conduit project.

```bash
conduit init <NAME>

Arguments:
  <NAME>                Project name
```

Example:

```bash
conduit init analytics-pipeline
cd analytics-pipeline
```

### compile

Compile DAGs and report results.

```bash
conduit compile [PATH] [options]

Arguments:
  [PATH]                Path to DAG definitions (default: ./dags)

Options:
  --output, -o <path>   Output compiled plan to file
  --check               Check only (don't write output)
```

Example:

```bash
conduit compile                          # Compile ./dags
conduit compile ./my-dags                # Compile different directory
conduit compile --check                  # Validate only
conduit compile --output compiled.json   # Save compiled plan
```

## Execution Commands

### run

Run a DAG (compile, schedule, and execute).

```bash
conduit run <DAG_ID> [options]

Arguments:
  <DAG_ID>              DAG ID to run

Options:
  --dags-path, -d <path>   Path to DAG definitions (default: ./dags)
  --date <date>            Logical date override (default: now)
  --max-tasks <n>          Maximum tasks to execute concurrently (default: 16)
  --full-refresh           Force full refresh on all incremental tasks
                           (ignore watermarks)
  --env <name>             Target environment recorded for this run
                           (default: production; context only — snapshots
                           are managed by plan/apply)
  --distributed            Run via the distributed coordinator; workers
                           must connect (see conduit worker)
  --bind <addr>            Coordinator bind address for distributed mode
                           (default: 0.0.0.0:9400)
```

Example:

```bash
conduit run daily_etl                       # Run now
conduit run daily_etl --date 2024-03-01     # Run for specific date
conduit run daily_etl --env staging         # Record run against staging
conduit run daily_etl --max-tasks 4         # Cap concurrency
conduit run daily_etl --full-refresh        # Ignore watermarks
conduit run daily_etl --distributed         # Dispatch to connected workers
```

### status

Show system status.

```bash
conduit status [options]

Options:
  --env, -e <name>         Show status for a specific environment
  --dags-path, -d <path>   Path to DAG definitions (default: ./dags)
```

Example:

```bash
conduit status                      # Overall status
conduit status --env production     # Production status
```

### backfill

Backfill a DAG across a range of dates/partitions.

```bash
conduit backfill <DAG_ID> --start <START> --end <END> [options]

Arguments:
  <DAG_ID>              DAG ID to backfill

Options:
  --start <date>           Start date (inclusive, YYYY-MM-DD) [required]
  --end <date>             End date (exclusive, YYYY-MM-DD) [required]
  --granularity <g>        Partition granularity (default: day)
  --max-concurrent <n>     Maximum partitions to execute concurrently
                           (default: 1)
  --full-refresh           Force full refresh on all partitions
  --dry-run                Show what would run without executing
  --env <name>             Target environment (default: production)
  --dags-path, -d <path>   Path to DAG definitions (default: ./dags)
```

Example:

```bash
conduit backfill daily_etl --start 2024-03-01 --end 2024-03-08
conduit backfill daily_etl --start 2024-03-01 --end 2024-03-08 --max-concurrent 4
conduit backfill daily_etl --start 2024-03-01 --end 2024-03-08 --dry-run
```

### query

Run SQL queries locally (powered by DuckDB).

```bash
conduit query <SQL> [options]

Arguments:
  <SQL>                 SQL query to execute

Options:
  --connection, -c <name>  Named connection from conduit.yaml
                           (default: ephemeral in-memory DuckDB)
  --file, -f <path>        Query a local file (Parquet, CSV, JSON) —
                           registers it as a table
  --setup, -s <sql>        Run setup SQL before the main query
  --format <fmt>           Output format: table, json, csv (default: table)
  --limit <n>              Maximum rows to return (default: 50)
  --config <path>          Path to conduit.yaml (for connection resolution)
```

Example:

```bash
conduit query "SELECT 1 AS answer"
conduit query "SELECT * FROM data" --file events.parquet
conduit query "SELECT count(*) FROM orders" --connection warehouse
```

### preview

Preview a SQL task's output locally.

```bash
conduit preview <TASK_REF> [options]

Arguments:
  <TASK_REF>            Task reference: dag_id.task_id

Options:
  --dags-path, -d <path>   Path to DAG definitions (default: ./dags)
  --connection, -c <name>  Override connection (default: ephemeral DuckDB)
  --format <fmt>           Output format: table, json, csv (default: table)
  --limit <n>              Maximum rows to return (default: 50)
```

Example:

```bash
conduit preview daily_etl.transform
conduit preview daily_etl.transform --connection warehouse --limit 10
```

## Deployment Commands

### plan

Show changes between local state and an environment.

```bash
conduit plan [ENVIRONMENT] [options]

Arguments:
  [ENVIRONMENT]         Target environment (default: production)

Options:
  --dags-path, -d <path>   Path to DAG definitions (default: ./dags)
  --output, -o <path>      Save the plan to a file (for later apply)
```

Saved plans record the environment version they were generated against
(`base_environment_version`); `apply` rejects a saved plan if the
environment has changed since (see
[Plan & Apply — Conflict Detection](./concepts/plan-apply.md)).

Example:

```bash
conduit plan                             # Plan against production
conduit plan staging                     # Plan against staging
conduit plan production -o plan.json     # Save plan for later apply
```

### apply

Apply a deployment plan to an environment. Executes changed tasks for
real, validates their data contracts, and updates the environment's
snapshot pointers. Exits non-zero if any task fails, errors, or violates
a contract — safe to use as a CI gate.

```bash
conduit apply [ENVIRONMENT] [options]

Arguments:
  [ENVIRONMENT]         Target environment (default: production)

Options:
  --dags-path, -d <path>   Path to DAG definitions (default: ./dags)
  --plan-file <path>       Load a saved plan file instead of generating
                           a new one (stale plans are rejected)
  --auto-approve, -y       Skip confirmation prompt
  --full-refresh           Force full refresh on all incremental tasks
  --only <DAG.TASK>        Apply only the named tasks (repeatable).
                           Upstream Execute/Reuse/Remove actions in the
                           same plan are auto-included so dependencies
                           stay consistent
```

Example:

```bash
conduit apply production -y
conduit apply production --plan-file plan.json -y
conduit apply production --only etl.load -y
```

## Environment Commands

### env create

Create a new environment.

```bash
conduit env create <NAME> [options]

Arguments:
  <NAME>                Environment name

Options:
  --from <name>            Base environment to fork from (default: production)
  --dags-path, -d <path>   Path to DAG definitions (default: ./dags)
```

### env list

List all environments.

```bash
conduit env list [options]

Options:
  --dags-path, -d <path>   Path to DAG definitions (default: ./dags)
```

### env promote

Promote one environment into another.

```bash
conduit env promote <SOURCE> <TARGET> [options]

Arguments:
  <SOURCE>              Source environment
  <TARGET>              Target environment
```

### env diff

Diff two environments — show added/removed/changed snapshots.

```bash
conduit env diff <A> <B> [options]

Arguments:
  <A>                   Left environment (the "from" side)
  <B>                   Right environment (the "to" side)
```

### env history

Show version history for an environment.

```bash
conduit env history <NAME> [options]

Arguments:
  <NAME>                Environment name
```

### env rollback

Roll back an environment to a prior history version.

```bash
conduit env rollback <NAME> [options]

Arguments:
  <NAME>                Environment name

Options:
  --to-version <n>         Specific version to restore. Defaults to the
                           env's current_version (which restores the state
                           captured before the most recent mutation)
  --yes                    Skip the confirmation prompt
```

### env set-policy

Set or clear the promotion policy on an environment.

```bash
conduit env set-policy <NAME> [options]

Arguments:
  <NAME>                Environment name (target of the policy)

Options:
  --require-source <name>  Only allow promotions whose source matches
                           this env name
  --min-age-secs <n>       Newest snapshot in the source must be at
                           least N seconds old
  --clear                  Clear the policy (overrides the other flags)
```

Example workflow:

```bash
conduit env create staging --from production
conduit apply staging -y
conduit env diff staging production
conduit env promote staging production
conduit env history production
conduit env rollback production --yes
```

## Debugging Commands

### replay

Replay events to reconstruct historical state.

```bash
conduit replay [options]

Options:
  --from <n>               Replay from this sequence number (default: 1)
  --to <n>                 Replay up to this sequence number
  --dags-path, -d <path>   Path to DAG definitions (for resolving state dir)
  --json                   Output the reconstructed state as JSON
  --events-only            Show events only (don't reconstruct state)
```

Example:

```bash
conduit replay                        # Replay the full event log
conduit replay --to 50                # State as of event 50
conduit replay --events-only          # List events without reconstruction
```

## Lineage Commands

### lineage extract

Extract SQL lineage for a single task (native JSON output, or an
OpenLineage RunEvent under `--openlineage`).

```bash
conduit lineage extract <TASK_REF> [options]

Arguments:
  <TASK_REF>            Task reference in the form dag_id.task_id

Options:
  --dags-path, -d <path>       Path to DAG definitions (default: ./dags)
  --openlineage                Emit an OpenLineage RunEvent instead of
                               Conduit's native lineage JSON
  --output-dataset <name>      OpenLineage output dataset name
  --dataset-namespace <ns>     OpenLineage dataset namespace
  --job-namespace <ns>         OpenLineage job namespace (default: conduit)
  --job-name <name>            OpenLineage job name (default: dag_id.task_id)
  --run-id <uuid>              OpenLineage run UUID (default: generated)
  --event-time <ts>            OpenLineage event timestamp (default: now)
  --event-type <type>          OpenLineage event type (default: COMPLETE)
```

### lineage trace

Trace a column's lineage across task boundaries via the cross-task
stitched graph (Python → SQL → Python).

```bash
conduit lineage trace --dag <DAG> --column <COLUMN> [options]

Options:
  --dag <dag>              DAG to trace within [required]
  --column <col>           Column to trace, as task_id.column_name [required]
  --dags-path, -d <path>   Path to DAG definitions (default: ./dags)
  --direction <dir>        upstream or downstream (default: upstream)
  --format <fmt>           text or json (default: text)
  --dbt-manifest <path>    dbt target/manifest.json to resolve
                           {{ ref('x') }} / {{ source('s','x') }} against
```

Example:

```bash
conduit lineage extract daily_etl.transform
conduit lineage trace --dag daily_etl --column load.revenue --direction upstream
```

### impact

Schema impact between two DAG versions — diffs task output schemas and
traces the downstream blast radius through cross-task lineage. This is
the CI gate behind `.github/workflows/conduit-impact.yml`.

```bash
conduit impact [options]

Options:
  --base <ref>             Base side: git ref (git mode; pair with --head)
  --head <ref>             Head side: git ref, or the literal WORKING for
                           the uncommitted working tree
  --base-plan <path>       Base side: compiled plan JSON or DAGs directory
                           (file mode; pair with --head-plan)
  --head-plan <path>       Head side: compiled plan JSON or DAGs directory
  --dags-path <path>       DAGs directory relative to the repo root
                           (git mode only; default: dags)
  --format <fmt>           markdown or json (default: markdown)
  --output <path>          Write the report to this file instead of stdout
```

Example:

```bash
conduit impact --base main --head WORKING
conduit impact --base v1.0 --head v1.1 --format json
```

## Distributed Commands

Distributed execution uses a coordinator (started by `conduit run
--distributed`) and one or more workers connected over gRPC. Remote
workers currently execute bash/python tasks; SQL tasks require a local
provider registry and fail loudly on remote workers.

### worker

Start a distributed worker node.

```bash
conduit worker [options]

Options:
  --coordinator, -c <addr>  Coordinator address to connect to
                            (default: localhost:9400)
  --capacity, -n <n>        Maximum concurrent tasks this worker can run
                            (default: 4)
  --pools, -p <pools>       Resource pools this worker handles,
                            comma-separated (default: default)
  --id <id>                 Worker ID (auto-generated if omitted)
  --labels, -l <k=v>        Labels for worker selection (key=value pairs)
```

### cluster status

Show cluster status (workers, running tasks, health).

```bash
conduit cluster status [options]

Options:
  --coordinator, -c <addr>  Coordinator address (default: localhost:9400)
  --json                    Output as JSON
```

### cluster drain

Drain a worker (finish current tasks, then stop).

```bash
conduit cluster drain <WORKER_ID> [options]

Arguments:
  <WORKER_ID>           Worker ID to drain

Options:
  --coordinator, -c <addr>  Coordinator address (default: localhost:9400)
```

Example workflow:

```bash
# Terminal 1: start a long-lived coordinator run
conduit run daily_etl --distributed --bind 0.0.0.0:9400

# Terminal 2: connect a worker
conduit worker --coordinator localhost:9400 --capacity 8

# Inspect and manage
conduit cluster status
conduit cluster drain worker-1
```

All three commands fail with a clear error when no coordinator is
reachable — there is no simulated output.

## API Commands

### serve

Start the API server.

```bash
conduit serve [options]

Options:
  --host <host>            Host to bind to (default: 0.0.0.0)
  --port, -p <port>        Port to listen on (default: 8080)
  --dags-path, -d <path>   Path to DAG definitions (default: ./dags)
  --state-dir <path>       Path to state directory (default: ./.conduit)
  --auth-enabled           Enable API key authentication
  --cors-origin <origin>   Origin allowed to call the API cross-origin
                           (repeatable; default: same-origin only)
  --demo                   Seed fabricated demo run history (for trying
                           out the UI; never enabled by default)
```

Example:

```bash
conduit serve                            # Serve on 0.0.0.0:8080
conduit serve --port 9090 --auth-enabled
conduit serve --cors-origin http://localhost:3000
```

See the [REST API Reference](./api-reference.md) for the endpoints.

## Migration Commands

### migrate

Migrate Airflow DAGs to Conduit format.

```bash
conduit migrate <SOURCE> [options]

Arguments:
  <SOURCE>              Path to Airflow DAGs directory

Options:
  --output, -o <path>   Output directory for Conduit DAGs (default: ./dags)
  --dry-run             Dry run (show what would be converted)
```

Example:

```bash
conduit migrate ~/airflow/dags --dry-run
conduit migrate ~/airflow/dags --output ./dags
```

## Exit Codes

- `0`: Success
- `1`: Error (failed task, contract violation, stale plan, compilation
  error, unreachable coordinator, …) — the message on stderr says which
- `2`: Invalid command-line arguments

## Common Workflows

### Deploy to Production

```bash
# 1. Compile locally
conduit compile

# 2. Plan changes (optionally save the plan)
conduit plan production -o plan.json

# 3. Review plan output, then apply exactly what was reviewed
conduit apply production --plan-file plan.json -y

# 4. Verify status
conduit status --env production
```

### Create and Test Staging

```bash
# 1. Create staging environment
conduit env create staging --from production

# 2. Make changes
vim dags/etl.py
conduit compile

# 3. Plan and apply
conduit plan staging
conduit apply staging -y

# 4. Test
conduit run daily_etl --env staging

# 5. Promote to production
conduit env promote staging production
```

### Debug a Failed Run

```bash
# 1. View status
conduit status

# 2. Replay the event log around the failure
conduit replay --events-only
conduit replay --to 50 --json

# 3. Roll the environment back if a bad apply went out
conduit env history production
conduit env rollback production --yes
```
