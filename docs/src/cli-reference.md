# CLI Reference

Complete reference for all Conduit CLI commands.

## Global Options

All commands accept these options:

```bash
--verbose, -v         Show detailed output
--quiet, -q           Suppress non-error output
--config <path>       Path to .conduit.toml (default: .conduit.toml)
--help, -h            Show help for this command
--version             Show Conduit version
```

## Project Commands

### init

Initialize a new Conduit project.

```bash
conduit init <project-name> [options]

Options:
  --template <name>     Project template (default: etl)
                        Options: etl, ml-pipeline, sensors
  --python <version>    Python version to target (default: 3.9)
  --git                 Initialize git repository
```

Example:

```bash
conduit init analytics-pipeline --template etl --git
cd analytics-pipeline
```

### compile

Parse and validate DAGs.

```bash
conduit compile [path] [options]

Arguments:
  [path]                Path to DAGs directory (default: ./dags)

Options:
  --check               Dry-run, don't save compilation results
  --output <path>       Save compilation output to JSON file
  --parallel <n>        Number of parallel workers (default: CPU count)
```

Example:

```bash
conduit compile                          # Compile ./dags
conduit compile ./my-dags                # Compile different directory
conduit compile --check                  # Dry-run
conduit compile --output compiled.json   # Save output
```

## Execution Commands

### run

Execute a DAG end-to-end.

```bash
conduit run <dag-id> [options]

Arguments:
  <dag-id>              ID of the DAG to run

Options:
  --env <name>          Environment to run in (default: development)
  --wait                Wait for completion before returning
  --timeout <seconds>   Timeout for entire DAG
  --skip-tasks <list>   Comma-separated task IDs to skip
  --parallel            Enable parallel task execution
```

Example:

```bash
conduit run daily_etl                      # Run DAG
conduit run daily_etl --env production     # Run in production
conduit run daily_etl --timeout 3600       # 1 hour timeout
conduit run daily_etl --skip-tasks load    # Skip load task
```

### status

Show system status and recent runs.

```bash
conduit status [options]

Options:
  --env <name>          Show status for specific environment
  --limit <n>           Number of recent runs to show (default: 10)
  --format <type>       Output format: text, json (default: text)
```

Example:

```bash
conduit status                      # Show general status
conduit status --env production     # Production status
conduit status --limit 20           # Last 20 runs
conduit status --format json        # JSON output
```

## Deployment Commands

### plan

Preview changes before deploying.

```bash
conduit plan [env] [options]

Arguments:
  [env]                 Environment to plan for (default: production)

Options:
  --output <path>       Save plan to file
  --from <env>          Compare against different environment
  --format <type>       Output format: text, json (default: text)
```

Example:

```bash
conduit plan production                     # Plan for production
conduit plan staging --output plan.json     # Save plan
conduit plan production --from staging      # Compare to staging
```

### apply

Deploy changes to an environment.

```bash
conduit apply [env] [options]

Arguments:
  [env]                 Environment to apply to (default: production)

Options:
  --plan <path>         Path to saved plan (re-plan if omitted)
  -y, --yes             Skip confirmation prompt
  --atomic              Atomic deployment (all or nothing)
  --auto-rollback       Auto-rollback on failure
```

Example:

```bash
conduit apply production -y                  # Apply with confirmation
conduit apply production --plan plan.json    # Apply saved plan
conduit apply production --auto-rollback     # Auto-rollback on failure
```

## Environment Commands

### env create

Create a new environment.

```bash
conduit env create <name> [options]

Arguments:
  <name>                Name of new environment

Options:
  --from <env>          Fork from existing environment (default: production)
  --description <text>  Environment description
  --tags <list>         Comma-separated tags
```

Example:

```bash
conduit env create staging --from production
conduit env create feature-x --from production --tags feature,experimental
```

### env list

List all environments.

```bash
conduit env list [options]

Options:
  --format <type>       Output format: text, json (default: text)
  --include-archived    Show archived environments
```

Example:

```bash
conduit env list
conduit env list --include-archived
```

### env info

Show environment details.

```bash
conduit env info <name>

Arguments:
  <name>                Environment name
```

Example:

```bash
conduit env info production
```

### env promote

Promote one environment into another.

```bash
conduit env promote <source> <target> [options]

Arguments:
  <source>              Source environment
  <target>              Target environment

Options:
  -y, --yes             Skip confirmation
  --backup              Backup target before promotion (default: true)
```

Example:

```bash
conduit env promote staging production -y
```

### env rollback

Rollback environment to previous snapshot.

```bash
conduit env rollback <env> [options]

Options:
  --to <snapshot-id>    Specific snapshot to rollback to
  --steps <n>           Rollback n snapshots (default: 1)
  -y, --yes             Skip confirmation
```

Example:

```bash
conduit env rollback production              # Rollback 1 step
conduit env rollback production --steps 2    # Rollback 2 steps
conduit env rollback production --to prod-snap-v5
```

### env archive

Archive an environment.

```bash
conduit env archive <name> [options]

Options:
  -y, --yes             Skip confirmation
```

Example:

```bash
conduit env archive dev -y
```

## Scheduling Commands

### schedule set

Configure DAG schedule.

```bash
conduit schedule set <dag-id> <cron> [options]

Arguments:
  <dag-id>              DAG ID
  <cron>                Cron expression (or "" to disable)

Options:
  --env <name>          Environment (default: production)
```

Example:

```bash
conduit schedule set daily_etl "0 2 * * *"    # Daily at 2 AM
conduit schedule set hourly_sync "0 * * * *"  # Every hour
conduit schedule set experimental ""           # Disable
```

### schedule list

Show all scheduled DAGs.

```bash
conduit schedule list [options]

Options:
  --env <name>          Environment (default: production)
  --format <type>       Output format: text, json
```

Example:

```bash
conduit schedule list
conduit schedule list --env staging
```

## Debugging Commands

### replay

Replay a historical run.

```bash
conduit replay <run-id> [options]

Arguments:
  <run-id>              Run ID to replay

Options:
  --to <event>          Replay only to specific event
  --modify <changes>    Override task inputs (JSON)
  --env <name>          Environment (default: development)
```

Example:

```bash
conduit replay run123                                    # Full replay
conduit replay run123 --to event:50                      # Partial replay
conduit replay run123 --modify '{"extract": {"limit": 100}}'  # Modified replay
```

### events list

Query event log.

```bash
conduit events list [options]

Options:
  --filter <expr>       Filter expression
  --limit <n>           Number of events (default: 100)
  --since <time>        Events since time (e.g., "24h", "2024-01-01")
  --format <type>       Output format: text, json (default: text)
```

Example:

```bash
conduit events list --limit 50
conduit events list --filter 'type:TaskFailed'
conduit events list --since 24h --filter 'dag:daily_etl'
```

### events export

Export events to file.

```bash
conduit events export <path> [options]

Arguments:
  <path>                Output file path (JSON or CSV)

Options:
  --since <time>        Export events since time
  --filter <expr>       Filter expression
```

Example:

```bash
conduit events export events.json
conduit events export events.csv --since 7d
```

### audit-log

Show audit trail for environment.

```bash
conduit audit-log <env> [options]

Arguments:
  <env>                 Environment name

Options:
  --limit <n>           Number of entries (default: 50)
  --format <type>       Output format: text, json (default: text)
```

Example:

```bash
conduit audit-log production
conduit audit-log production --limit 100
```

## Snapshot Commands

### snapshot list

List all snapshots.

```bash
conduit snapshot list [options]

Options:
  --env <name>          Filter by environment
  --include-orphaned    Include unreferenced snapshots
  --format <type>       Output format: text, json
```

Example:

```bash
conduit snapshot list
conduit snapshot list --env production
```

### snapshot info

Show snapshot details.

```bash
conduit snapshot info <snapshot-id>

Arguments:
  <snapshot-id>         Snapshot ID
```

Example:

```bash
conduit snapshot info prod-snap-20240322-143215
```

### snapshot delete

Delete a snapshot.

```bash
conduit snapshot delete <snapshot-id> [options]

Arguments:
  <snapshot-id>         Snapshot ID

Options:
  -y, --yes             Skip confirmation
```

Example:

```bash
conduit snapshot delete prod-snap-20240322-143215 -y
```

### snapshot export

Export snapshot to file.

```bash
conduit snapshot export <snapshot-id> <path>

Arguments:
  <snapshot-id>         Snapshot ID
  <path>                Output file path
```

Example:

```bash
conduit snapshot export prod-snap-20240322-143215 backup.json
```

## Lineage Commands

### lineage

Extract column-level lineage for a SQL task. The task reference must use `dag_id.task_id`.

```bash
conduit lineage <dag.task> [options]

Arguments:
  <dag.task>            SQL task reference

Options:
  --dags-path <path>    Path to DAG definitions (default: ./dags)
  --openlineage         Emit an OpenLineage RunEvent instead of native lineage JSON
  --output-dataset <n>  OpenLineage output dataset name (default: dag.task)
  --dataset-namespace <n>
                        OpenLineage dataset namespace (default: SQL connection)
  --job-namespace <n>   OpenLineage job namespace (default: conduit)
  --job-name <n>        OpenLineage job name (default: dag.task)
  --run-id <uuid>       OpenLineage run UUID (default: generated UUID)
  --event-time <time>   OpenLineage event timestamp, RFC3339 (default: now)
  --event-type <type>   START, RUNNING, COMPLETE, ABORT, FAIL, or OTHER
                        (default: COMPLETE)
```

Example:

```bash
conduit lineage daily_etl.transform_orders
conduit lineage daily_etl.transform_orders \
  --openlineage \
  --output-dataset analytics.order_summary
```

## API Commands

### serve

Start the REST API and WebSocket server.

```bash
conduit serve [options]

Options:
  --host <addr>          Bind address (default: 0.0.0.0)
  -p, --port <n>         Port (default: 8080)
  -d, --dags-path <dir>  Path to DAG definitions (default: ./dags)
  --state-dir <dir>      State directory (default: ./.conduit)
  --auth-enabled         Require API keys on all endpoints (a bootstrap
                         admin key is printed on first start)
  --cors-origin <url>    Allow this origin to call the API cross-origin
                         (repeatable; default: same-origin only)
```

The bundled web UI is served from the same port when built UI assets are
available (`CONDUIT_UI_DIR`). Because the UI is same-origin, no
`--cors-origin` is needed for it — the flag exists for external browser
clients hosted elsewhere.

Example:

```bash
conduit serve                          # Start on 0.0.0.0:8080
conduit serve --port 9000             # Custom port
conduit serve --host 127.0.0.1        # Localhost only
conduit serve --auth-enabled          # Enforce API keys
```

### health

Check health status.

```bash
conduit health [options]

Options:
  --wait                Wait for healthy status
  --timeout <seconds>   Timeout (default: 30)
```

Example:

```bash
conduit health
conduit health --wait --timeout 60
```

## Maintenance Commands

### verify-snapshots

Verify snapshot coherency.

```bash
conduit verify-snapshots [options]

Options:
  --repair              Attempt repair if corruption found
  --detailed            Show detailed verification report
```

Example:

```bash
conduit verify-snapshots
conduit verify-snapshots --detailed
```

### cleanup

Clean up stale data.

```bash
conduit cleanup [options]

Options:
  --older-than <time>   Delete events older than (default: 90d)
  --dry-run             Show what would be deleted
  --yes, -y             Skip confirmation
```

Example:

```bash
conduit cleanup --older-than 180d --dry-run
conduit cleanup --older-than 180d -y
```

### migrate

Migrate from another orchestrator.

```bash
conduit migrate <source> [options]

Arguments:
  <source>              Source orchestrator: airflow, dagster, prefect

Options:
  --config <path>       Source configuration path
  --output <dir>        Output directory for migrated DAGs
  --skip-tests          Skip test generation
```

Example:

```bash
conduit migrate airflow --config ~/airflow.cfg
```

## Output Formats

Most commands support multiple output formats:

- **text**: Human-readable (default)
- **json**: Machine-readable JSON
- **csv**: Spreadsheet format (where applicable)

Example:

```bash
conduit status --format json | jq '.runs[0]'
conduit env list --format csv > environments.csv
```

## Exit Codes

- `0`: Success
- `1`: General error
- `2`: Invalid arguments
- `3`: Compilation error
- `4`: Deployment conflict
- `5`: Not found (DAG, environment, etc.)

## Common Workflows

### Deploy to Production

```bash
# 1. Compile locally
conduit compile

# 2. Plan changes
conduit plan production

# 3. Review plan output, then apply
conduit apply production -y

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

# 2. Check events
conduit events list --filter 'run:run123'

# 3. View audit trail
conduit audit-log production

# 4. Replay for debugging
conduit replay run123 --to event:50

# 5. Export events for analysis
conduit events export debug.json --filter 'run:run123'
```
