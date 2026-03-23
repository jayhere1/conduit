# Event-Sourced Architecture

Conduit uses **event sourcing** instead of mutable database state. Every action in the system is recorded as an immutable event in an append-only log. From this log, you can reconstruct any state, replay any execution, and time-travel through history.

## What Is Event Sourcing?

Traditional databases store the **current state**:

```
Database State (Mutable):
  DAG daily_analytics_etl
    status: running
    last_run: 2024-03-22 14:32
    next_run: 2024-03-22 15:32
```

If you want to know what the status was 1 hour ago, you're out of luck. The state has been mutated.

Event sourcing stores **every state change** as an immutable event:

```
Event Log (Append-only):
  1. DAGScheduled(dag_id: daily_analytics_etl, time: 2024-03-22 14:00)
  2. SnapshotDeployed(env: production, snapshot: prod-v5)
  3. DAGRunStarted(run_id: run123, dag: daily_analytics_etl, time: 2024-03-22 14:32)
  4. TaskStarted(run_id: run123, task: extract, time: 2024-03-22 14:32:10)
  5. TaskProgressEvent(run_id: run123, task: extract, progress: 50%)
  6. TaskProgressEvent(run_id: run123, task: extract, progress: 100%)
  7. TaskCompleted(run_id: run123, task: extract, time: 2024-03-22 14:32:45)
  8. TaskStarted(run_id: run123, task: transform, time: 2024-03-22 14:32:46)
  9. TaskCompleted(run_id: run123, task: transform, time: 2024-03-22 14:35:12)
  10. TaskStarted(run_id: run123, task: load, time: 2024-03-22 14:35:13)
  11. TaskCompleted(run_id: run123, task: load, time: 2024-03-22 14:37:20)
  12. DAGRunCompleted(run_id: run123, status: success, time: 2024-03-22 14:37:20)
```

From this log, you can:
- **Reconstruct state**: Apply all events to find current state at any point
- **Time-travel**: Query state at event 6 vs event 12
- **Replay**: Re-execute the same run using the same inputs
- **Debug**: See exactly what happened and when

## Event Types

Conduit defines structured event types:

### 1. Compilation Events

```python
class DAGCompiled(Event):
    dag_id: str
    fingerprint: str
    num_tasks: int
    compile_duration_ms: int
    timestamp: datetime
```

Fired when `conduit compile` succeeds.

### 2. Scheduling Events

```python
class DAGScheduled(Event):
    dag_id: str
    run_id: str
    scheduled_time: datetime
    trigger: str  # "cron" | "manual" | "dependency" | "event"
    timestamp: datetime

class DAGUnscheduled(Event):
    dag_id: str
    run_id: str
    reason: str
    timestamp: datetime
```

Fired when the scheduler determines a DAG should run.

### 3. Execution Events

```python
class DAGRunStarted(Event):
    run_id: str
    dag_id: str
    environment: str
    timestamp: datetime

class TaskStarted(Event):
    run_id: str
    task_id: str
    timestamp: datetime
    worker_id: str

class TaskProgressEvent(Event):
    run_id: str
    task_id: str
    progress_percent: int
    timestamp: datetime

class TaskCompleted(Event):
    run_id: str
    task_id: str
    status: str  # "success" | "failed" | "skipped"
    exit_code: int
    duration_ms: int
    xcom_output: Dict[str, Any]
    timestamp: datetime

class DAGRunCompleted(Event):
    run_id: str
    status: str
    total_duration_ms: int
    failed_tasks: List[str]
    timestamp: datetime
```

### 4. Deployment Events

```python
class SnapshotCompiled(Event):
    snapshot_id: str
    dag_id: str
    num_tasks: int
    snapshot_size_bytes: int
    timestamp: datetime

class SnapshotDeployed(Event):
    snapshot_id: str
    environment: str
    previous_snapshot: str
    num_reused_tasks: int
    timestamp: datetime

class EnvironmentCreated(Event):
    environment: str
    forked_from: Optional[str]
    snapshot_id: str
    timestamp: datetime

class EnvironmentPromoted(Event):
    source_env: str
    target_env: str
    snapshot_id: str
    previous_snapshot: str
    timestamp: datetime
```

### 5. Error Events

```python
class TaskFailed(Event):
    run_id: str
    task_id: str
    exit_code: int
    stderr: str
    timestamp: datetime

class DAGRunFailed(Event):
    run_id: str
    failed_task_id: str
    reason: str
    timestamp: datetime
```

## Event Storage

Events are stored in **RocksDB**, Conduit's embedded key-value store:

```
[Event Log in RocksDB]
  Key: event_sequence_000001
  Value: {serialized DAGCompiled event}

  Key: event_sequence_000002
  Value: {serialized DAGScheduled event}

  Key: event_sequence_000003
  Value: {serialized TaskStarted event}

  ... (100+ events)
```

Each event has a monotonic sequence number. Range queries are O(log n).

### Durability

RocksDB uses **write-ahead logging**, ensuring:
- **Crash safety**: Even if the process dies, events are persisted
- **No data loss**: Every event is durable before returning to caller
- **Fast writes**: Batch writes reduce I/O

## Time-Travel Debugging

Replay any run from the event log:

```bash
# Find an interesting run
conduit status

# Time-travel to specific point in a run
conduit replay run123 --to event:7  # After TaskCompleted(extract)
```

Output:

```
Replaying run123 from beginning to event 7

State at event 7:
  DAG: daily_analytics_etl
  Run ID: run123
  Completed tasks: extract
  In-progress tasks: (none)
  Pending tasks: transform, load
  Task outputs:
    - extract.users_count: 1000
    - extract.output: raw_users.csv

Time-travel debugging:
  - What were the exact XCom outputs at this point?
    Answer: extract returned raw_users.csv, emitted users_count=1000
  - Did the database state look correct here?
    Answer: Yes, 1000 users extracted
  - Why did transform fail later?
    Answer: (Check subsequent events)
```

### Replay with Modifications

Replay a run, but with different inputs:

```bash
conduit replay run123 --modify 'extract.users_limit=100'
```

This reruns extract with `users_limit=100`, then streams the new output through transform and load, letting you see if that would have avoided the failure.

## Event Streaming

Subscribe to events in real-time via WebSocket:

```python
from conduit.sdk import event_stream

async def watch_run(run_id):
    async with event_stream() as stream:
        async for event in stream.subscribe(f"run:{run_id}"):
            print(f"{event.timestamp} [{event.type}] {event}")

# Output:
# 2024-03-22 14:32:10 [DAGRunStarted] run123 started
# 2024-03-22 14:32:10 [TaskStarted] extract started
# 2024-03-22 14:32:12 [TaskCompleted] extract finished (1.67s)
# 2024-03-22 14:32:13 [TaskStarted] transform started
# 2024-03-22 14:35:12 [TaskCompleted] transform finished (2m59s)
# 2024-03-22 14:35:13 [TaskStarted] load started
# 2024-03-22 14:37:20 [TaskCompleted] load finished (2m07s)
# 2024-03-22 14:37:20 [DAGRunCompleted] run123 succeeded
```

## Event Queries

Query the event log:

```python
from conduit import event_store

# Get all events for a run
events = event_store.query(
    run_id='run123'
)
for event in events:
    print(f"{event.timestamp} {event.type} {event.task_id}")

# Get all failed tasks in last 24 hours
failures = event_store.query(
    event_type='TaskFailed',
    since='24h'
)
for event in failures:
    print(f"Task {event.task_id} failed: {event.stderr}")

# Get deployment history
deployments = event_store.query(
    event_type='SnapshotDeployed',
    environment='production'
)
for event in deployments:
    print(f"Deployed {event.snapshot_id} at {event.timestamp}")
```

## Snapshots

Snapshots are **derived state** computed from the event log:

```
Event Log:
  [1] DAGCompiled(fingerprint: f1a2b3...)
  [2] DAGCompiled(fingerprint: f1b3c4...)
  [3] SnapshotDeployed(snapshot: v2, uses fingerprints: f1b3c4...)
  [4-100] ... (other events)
  [101] DAGCompiled(fingerprint: f1c4d5...)
  [102] SnapshotDeployed(snapshot: v3, uses fingerprints: f1c4d5...)

Computed Snapshots:
  v2: snapshot_id → {dag_id → compiled_dag}
  v3: snapshot_id → {dag_id → compiled_dag}
```

Snapshots are **read-only caches** of compiled DAGs. They're computed from the event log on-demand.

### Snapshot Coherency

A snapshot is **coherent** if all events leading to it are present in the event log:

```bash
# Verify all snapshots are coherent
conduit verify-snapshots
```

Output:

```
Verifying snapshots...

✓ prod-snap-20240322-143215: 10 tasks, all source events present
✓ prod-snap-20240322-145456: 10 tasks (1 reused, 9 new), all source events present
✓ staging-snap-20240322-145123: 11 tasks (10 reused, 1 added), all source events present

All snapshots coherent.
```

## Retention Policies

Events are immutable but can be pruned after a retention period:

```toml
[retention]
# Keep events for 90 days
events_max_age = "90d"

# Or keep 10,000 most recent events
events_max_count = 10000

# Never prune deployment events
never_prune_types = ["SnapshotDeployed", "EnvironmentPromoted"]
```

Pruning is **safe** because snapshots can be reconstructed from remaining events.

## Event-Driven Triggers

Events can trigger external actions via webhooks:

```bash
conduit webhook add https://slack.com/hooks/abc123 \
  --event-type TaskFailed \
  --payload '{"text": "Task {{task_id}} failed"}'
```

When a TaskFailed event is logged, Conduit POSTs to the webhook with context.

## Audit Trail

Every change to every environment is logged:

```bash
# View audit trail for production
conduit audit-log production
```

Output:

```
Timestamp           Type                  User       Changes
──────────────────────────────────────────────────────────────
2024-03-22 14:51:23 SnapshotDeployed      (automated) prod-snap-v5 → v6
                                                      1 task added, 1 task modified
2024-03-22 13:12:45 EnvironmentPromoted   alice      staging → production
                                                      snapshot staging-v3
2024-03-21 08:00:00 SnapshotDeployed      bob        prod-snap-v4 → v5
                                                      3 tasks modified
2024-03-20 19:30:15 EnvironmentRolledBack alice      to prod-snap-v4
                                                      reason: high error rate detected
```

## Consistency Guarantees

Event sourcing provides strong consistency guarantees:

1. **Monotonic consistency**: Events are applied in order
2. **Durability**: Events are persisted before returning
3. **Completeness**: All state changes are captured
4. **Auditability**: Complete trace of who did what when

## Performance Implications

Event sourcing has a cost: **latency**. Reconstructing state from 100,000 events is slower than reading from a database.

Conduit mitigates this with:

1. **Snapshots**: Cache compiled DAGs to avoid replaying compile events
2. **Event indexing**: Index by run_id, task_id, timestamp for fast queries
3. **Batching**: Write multiple events in one RocksDB transaction

Typical latencies:
- **Query recent events**: < 1 ms (in-memory cache)
- **Reconstruct state**: < 10 ms (RocksDB range query)
- **Time-travel to event**: < 50 ms (index lookup + replay)

## Next Steps

- **[Plan/Apply Workflow](./plan-apply.md)**: How events enable safe deployments
- **[Architecture](../architecture.md)**: How the event store integrates with other crates
- **[API Reference](../api-reference.md)**: Query and subscribe to events via REST/WebSocket
