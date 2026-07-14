# Incremental Computation

Conduit includes a SQLMesh-inspired incremental computation engine that avoids
reprocessing data that hasn't changed. Instead of full refreshes every run, tasks
can process only new or modified rows using watermark-based change tracking.

## Strategies

Conduit supports five incremental strategies, each suited to different data patterns:

### Full Refresh
Reprocesses all data every run. Use when data is small or transformations are
non-deterministic.

```yaml
incremental:
  strategy: full_refresh
```

### Append
Processes only rows newer than the last watermark. Ideal for immutable, time-ordered
event streams (clicks, logs, transactions).

```yaml
incremental:
  strategy: append
  time_column: created_at
  lookback: 2h           # safety overlap
  batch_size: 100000
```

### Merge on Key
Upserts rows by a unique key — detects new and changed rows. Ideal for dimension
tables and mutable records.

```yaml
incremental:
  strategy: merge_on_key
  unique_key: [user_id]
  time_column: updated_at
  invalidate_hard_deletes: true
```

### Delete + Insert
Replaces entire partitions. Ideal for date-partitioned fact tables where late-arriving
data may update a whole day's partition.

```yaml
incremental:
  strategy: delete_insert
  partition_column: event_date
  partition_granularity: day
  max_partitions_per_run: 7
```

### Snapshot Diff (SCD Type 2)
Tracks historical changes by comparing current and previous snapshots. Creates new
versions when monitored columns change.

```yaml
incremental:
  strategy: snapshot_diff
  unique_key: [product_id]
  check_columns: [name, price, status]
  scd_type_2: true
```

## Watermarks

Conduit maintains watermarks that record how far each task has processed. Watermark
types include:

- **Timestamp** — a datetime high-water mark (used by `append`)
- **Sequence** — an integer offset (used by event streams)
- **Partition** — a set of processed partitions (used by `delete_insert`)

Watermarks are persisted to `.conduit/watermarks.json` in the state directory
and survive restarts. `conduit run` and `conduit apply` both load this file
before executing tasks. A task's watermark only advances once it exits
successfully and emits a new value; `conduit run` writes the file back once
the whole run finishes (even if some other task in the DAG failed), while
`conduit apply` only writes it back once the apply reaches its success path —
a blocked or failed apply discards any in-memory watermark advances instead
of persisting them.

## SQL Rewriting

For SQL tasks with incremental config, Conduit automatically rewrites queries to
filter only unprocessed data. For example, an append strategy adds:

```sql
WHERE created_at > '2024-01-15T10:30:00Z'
  AND created_at <= '2024-01-16T06:00:00Z'
```

This happens transparently — your SQL stays clean and the engine handles the filtering.

## Full Refresh Override

Force a complete reprocess of any task by passing `--full-refresh`:

```bash
conduit run my_dag --full-refresh
conduit apply production --full-refresh
```

This ignores all watermarks and reprocesses from scratch — useful after schema changes
or to fix data quality issues.

## Python SDK

The Python SDK provides helpers for reading incremental context in Python tasks:

```python
from conduit_sdk.incremental import get_incremental_context, emit_watermark

ctx = get_incremental_context()
if ctx.is_full_refresh:
    process_all()
else:
    process_since(ctx.low_watermark)
    emit_watermark(ctx.high_watermark)
```
