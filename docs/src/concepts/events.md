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

Replay the event log to reconstruct system state as of any point in
history:

```bash
# List the raw events (sequence, timestamp, type)
conduit replay --events-only

# Reconstruct state as of event 7
conduit replay --to 7

# Machine-readable reconstruction
conduit replay --to 7 --json
```

The reconstruction shows the environments that existed, every run and
its status, and per-task success/failure counts — all derived purely
from events up to the chosen sequence number. Because events are
immutable, the same replay always produces the same state.

## Event Streaming

Subscribe to events in real-time over the WebSocket endpoint
(`/ws/events`, outside the `/api/v1` prefix) with any WebSocket client:

```python
import asyncio, json, websockets

async def watch():
    async with websockets.connect("ws://localhost:8080/ws/events") as ws:
        async for message in ws:
            event = json.loads(message)
            print(f"[{event['type']}] {event}")

asyncio.run(watch())
```

## Event Queries

Query the event log over the API:

```bash
# All events for a run
curl 'localhost:8080/api/v1/events?run_id=run123'

# Failed tasks
curl 'localhost:8080/api/v1/events?event_type=TaskFailed&limit=50'

# Deployment history
curl 'localhost:8080/api/v1/events?event_type=PlanApplied'

# A sequence range
curl 'localhost:8080/api/v1/events?from=100&to=200'
```
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

A snapshot is **coherent** if all events leading to it are present in
the event log. You can verify what the log supports by reconstructing
state from it:

```bash
conduit replay --json
```

Whatever `replay` reconstructs is exactly what the event log can prove
happened; anything else was lost or never recorded.

## Retention Policies

The event store supports retention limits (maximum event age, maximum
event count, and a minimum number of events always kept). This is
currently a library-level policy — `RetentionPolicy` in `conduit-state`,
with `standard()` (7 days / 100k events) and `extended()` (30 days / 1M
events) presets — and the default keeps everything. There is no
user-facing configuration file setting for it yet.

## Event-Driven Triggers

There is no built-in webhook mechanism. To trigger external actions
(Slack alerts, PagerDuty, …), subscribe to the WebSocket stream at
`/ws/events` and forward the events you care about — see
[Event Streaming](#event-streaming) above.

## Audit Trail

Every apply is recorded in the event log, and every environment
mutation (apply, promote, rollback) is recorded in the environment's
version history:

```bash
# Environment-level audit: version history with reasons
conduit env history production

# Raw event log
conduit replay --events-only
```

Over the API: `GET /api/v1/events?event_type=PlanApplied`.

## Consistency Guarantees

Event sourcing provides strong consistency guarantees:

1. **Monotonic consistency**: Events are applied in order
2. **Durability**: Events are persisted before returning
3. **Completeness**: All state changes are captured
4. **Auditability**: Complete trace of what happened when

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
