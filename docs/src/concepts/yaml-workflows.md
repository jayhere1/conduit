# YAML Workflow Definitions

Conduit supports defining DAGs in YAML as an alternative to Python. YAML workflows
are ideal for configuration-driven pipelines — SQL queries, shell commands, sensors —
where the full expressiveness of Python isn't needed.

Both formats produce the same compiled `ConduitPlan` and are treated identically by
the scheduler, executor, and environment system. You can freely mix `.py` and `.yaml`
DAGs in the same `dags/` directory.

## Basic Structure

```yaml
id: my_pipeline
description: A daily data pipeline
schedule: "0 6 * * *"
tags: [etl, warehouse]
max_active_runs: 1

tasks:
  extract:
    type: sql
    connection: warehouse
    query: "SELECT * FROM source.orders WHERE date = '{{ ds }}'"
    retries: 3
    timeout: 10m

  transform:
    type: python
    module: transforms.orders
    function: enrich
    depends_on: [extract]

  load:
    type: shell
    command: "python scripts/load.py --date {{ ds }}"
    depends_on: [transform]
```

## Task Types

### `python`
Runs a Python function. Fields: `module` (default: `"tasks"`), `function` (default: task ID).

```yaml
my_task:
  type: python
  module: my_module
  function: my_function
```

### `shell` / `bash`
Runs a shell command. Field: `command` (required).

```yaml
my_task:
  type: shell
  command: "echo hello world"
```

### `sql`
Runs a SQL query against a named connection. Fields: `connection` (default: `"default"`), `query` (required).

```yaml
my_task:
  type: sql
  connection: warehouse
  query: |
    INSERT INTO target.table
    SELECT * FROM staging.table
```

### `sensor`
Waits for an external condition. Fields: `sensor_type` (default: `"file"`), `poke_interval`.

```yaml
my_task:
  type: sensor
  sensor_type: file
  poke_interval: 60s
```

### `executable` / `exec`
Runs a binary with arguments. Fields: `command` (required), `args` (optional list).

```yaml
my_task:
  type: executable
  command: /usr/local/bin/processor
  args: ["--input", "data.csv", "--format", "parquet"]
```

## Common Task Fields

Every task type supports these optional fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `depends_on` | list | `[]` | Task IDs this task depends on |
| `retries` | int | `0` | Number of retry attempts |
| `retry_delay` | string | none | Delay between retries (e.g., `"30s"`, `"5m"`) |
| `pool` | string | none | Resource pool to run in |
| `timeout` | string | none | Execution timeout (e.g., `"1h"`, `"30m"`) |
| `priority` | int | `0` | Scheduling priority (higher = first) |
| `trigger_rule` | string | none | When to trigger (`all_success`, `one_success`, etc.) |
| `resources` | object | none | Resource limits (`cpu_millicores`, `memory_mb`) |
| `incremental` | object | none | Incremental computation config |

## Incremental Configuration

YAML tasks support the same incremental strategies as the Python SDK:

```yaml
my_task:
  type: sql
  query: "SELECT * FROM orders"
  incremental:
    strategy: append           # append, merge_on_key, delete_insert, snapshot_diff, full_refresh
    time_column: created_at
    lookback: 2h
    batch_size: 50000
```

### Strategies

- **`append`** — Append new rows based on a time column.
  Fields: `time_column`, `lookback`
- **`merge_on_key`** / `upsert` — Merge rows by unique key.
  Fields: `unique_key`, `time_column`, `invalidate_hard_deletes`
- **`delete_insert`** — Delete and reinsert by partition.
  Fields: `partition_column`, `partition_granularity` (`hour`/`day`/`week`/`month`/`year`)
- **`snapshot_diff`** / `scd` — SCD Type 2 snapshots.
  Fields: `unique_key`, `check_columns`, `scd_type_2`
- **`full_refresh`** — Always recompute everything.

## Template Variables

YAML workflows support Jinja-style template variables that are expanded at runtime:

- `{{ ds }}` — The logical execution date (YYYY-MM-DD)
- `{{ ds_nodash }}` — Execution date without dashes
- `{{ ts }}` — Full ISO timestamp

## Special Files

The YAML parser automatically skips `conduit.yaml` and `conduit.yml` files, which are
reserved for project configuration.

## Python vs YAML: When to Use Which

| Use YAML when... | Use Python when... |
|---|---|
| Pipeline is mostly SQL/shell | Complex branching logic |
| Configuration-driven | Dynamic DAG generation |
| Easy to review in PRs | Custom operators needed |
| Non-engineers need to edit | Heavy data transformations |
| Quick prototyping | Integration with Python libs |
