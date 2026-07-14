# REST API Reference

Conduit provides a comprehensive REST API for all operations. Start the API server with `conduit serve`.

## Base URL

```
http://localhost:8080/api/v1
```

## Authentication

API-key authentication is built in but disabled by default. Enable it with:

```bash
conduit serve --auth-enabled
```

On first start with auth enabled, a bootstrap admin key is created and printed once. Send the key on every request:

```
Authorization: Bearer <api-key>
```

Keys are managed via `POST/GET /auth/keys`, `GET/DELETE /auth/keys/{key_id}`, and `GET /auth/me`. When auth is disabled, all endpoints are public — secure them at the network/infrastructure level.

## Response Format

Success responses are plain endpoint-specific JSON (no envelope) — see each endpoint below.

Errors share one shape:

```json
{
  "error": {
    "type": "not_found",
    "message": "DAG 'daily_etl' not found"
  }
}
```

## Health Endpoints

### GET /health

Health check.

**Response:**

```json
{
  "status": "ok",
  "service": "conduit",
  "version": "0.2.0"
}
```

### GET /metrics (root path, not under /api/v1)

Prometheus metrics, served at `http://localhost:8080/metrics`. (`GET /api/v1/metrics` is a different endpoint — it lists task metrics.)

**Response:**

```
# HELP conduit_dags_total Total DAGs compiled
# TYPE conduit_dags_total counter
conduit_dags_total 5
```

## DAG Endpoints

### GET /dags

List all compiled DAGs. Compiles the DAGs directory on each call; no query parameters.

**Response:**

```json
{
  "dags": [
    {
      "id": "daily_etl",
      "name": "daily_etl",
      "description": "Daily ETL pipeline",
      "schedule": "0 2 * * *",
      "tags": [],
      "taskCount": 3,
      "sourceFile": "dags/daily_etl.yaml"
    }
  ],
  "total": 5
}
```

### GET /dags/{dag_id}

Get DAG details.

**Response:**

```json
{
  "id": "daily_etl",
  "name": "daily_etl",
  "description": "Daily ETL pipeline",
  "schedule": "0 2 * * *",
  "tags": [],
  "maxActiveRuns": 1,
  "taskCount": 2,
  "sourceFile": "dags/daily_etl.yaml",
  "executionOrder": ["extract", "transform"],
  "tasks": [
    {
      "id": "extract",
      "name": "extract",
      "type": "Python",
      "dependencies": [],
      "retries": 1,
      "retryDelay": null,
      "pool": null,
      "timeout": 300,
      "priority": 0,
      "triggerRule": "AllSuccess"
    }
  ]
}
```

### POST /dags/compile

Recompile the whole DAGs directory. No request body.

**Response:**

```json
{
  "success": true,
  "dagsCompiled": 5,
  "tasksTotal": 32,
  "errors": [],
  "warnings": [],
  "durationMs": 45
}
```

## Run Endpoints

### POST /dags/{dag_id}/runs

Start a new DAG run. The run is dispatched to the scheduler which coordinates task execution via the executor. Task state changes are broadcast over WebSocket in real-time.

**Request** (all fields optional):

```json
{
  "logical_date": "2024-03-22T14:00:00Z",
  "config": { "batch_size": "1000" },
  "environment": "staging"
}
```

`environment` defaults to `"production"` and is recorded on the run and threaded into task execution context.

**Response:**

```json
{
  "runId": "run_daily_etl_20240322_143210_123",
  "dagId": "daily_etl",
  "environment": "staging",
  "status": "dispatched",
  "taskStates": {
    "extract": "pending",
    "transform": "pending",
    "load": "pending"
  },
  "message": "DAG run 'run_daily_etl_20240322_143210_123' dispatched to scheduler (3 tasks)"
}
```

If no scheduler is attached, `status` will be `"queued"` instead of `"dispatched"`.

### GET /runs

List recent runs across all DAGs. (`GET /dags/{dag_id}/runs` returns the same shape scoped to one DAG.)

**Query Parameters:**

- `dag_id` (optional): Filter by DAG
- `status` (optional): Filter by status (pending, running, success, failed)
- `environment` (optional): Filter by environment
- `limit` (optional): Max results (default: 100)

**Response:**

```json
{
  "runs": [
    {
      "id": "run_abc123def456",
      "dagId": "daily_etl",
      "status": "success",
      "startedAt": "2024-03-22T14:32:10Z",
      "endedAt": "2024-03-22T14:37:45Z",
      "taskStates": { "extract": "success" },
      "taskLogs": {},
      "triggeredBy": "api",
      "environment": "production"
    }
  ],
  "total": 142
}
```

### GET /runs/{run_id}

Get run details. Captured task output (truncated stdout/stderr per task) is returned in `taskLogs` — there is no separate log-streaming endpoint; live events stream over the WebSocket (see below).

**Response:**

```json
{
  "id": "run_abc123def456",
  "dagId": "daily_etl",
  "status": "success",
  "startedAt": "2024-03-22T14:32:10Z",
  "endedAt": "2024-03-22T14:37:45Z",
  "taskStates": {
    "extract": "success",
    "transform": "success"
  },
  "taskLogs": {
    "extract": "Extracting data from API...\n1000 rows written"
  },
  "triggeredBy": "api",
  "environment": "production"
}
```

## Environment Endpoints

### GET /environments

List all environments.

**Response:**

```json
{
  "environments": [
    {
      "id": "production",
      "name": "production",
      "snapshotCount": 32,
      "updatedAt": "2024-03-22T14:32:00Z",
      "basedOn": null,
      "currentVersion": 4,
      "promotionPolicy": {
        "requireSource": null,
        "minAgeSecs": null
      }
    }
  ]
}
```

### POST /environments

Create a new environment.

**Request:**

```json
{
  "name": "staging",
  "based_on": "production"
}
```

**Response:**

```json
{
  "id": "staging",
  "name": "staging",
  "snapshotCount": 32,
  "basedOn": "production",
  "message": "Environment 'staging' created"
}
```

### GET /environments/{env_name}

Get environment details, including its per-task snapshot pointers.

**Response:**

```json
{
  "id": "production",
  "name": "production",
  "snapshotCount": 32,
  "updatedAt": "2024-03-22T14:32:00Z",
  "basedOn": null,
  "currentVersion": 4,
  "promotionPolicy": {
    "requireSource": null,
    "minAgeSecs": null
  },
  "snapshots": [
    {
      "dagId": "daily_etl",
      "taskId": "extract",
      "snapshotId": "snap_extract_20240322143215123"
    }
  ]
}
```

### POST /environments/promote

Promote one environment's state to another. Source and target are given in the body, not the path.

**Request:**

```json
{
  "source": "staging",
  "target": "production"
}
```

**Response:**

```json
{
  "source": "staging",
  "target": "production",
  "snapshotChanges": 4,
  "message": "Promoted 'staging' -> 'production' (4 snapshot changes)"
}
```

### POST /environments/{env_name}/rollback

Roll an environment back to a previous recorded version (see `GET /environments/{env_name}/history`). Omit `to_version` to roll back one step.

**Request:**

```json
{
  "to_version": 3
}
```

**Response:**

```json
{
  "environment": "production",
  "rolledBackTo": 3,
  "newVersion": 5,
  "snapshotChanges": 2,
  "message": "Rolled back 'production' (new version 5, 2 snapshot changes)"
}
```

## Plan/Apply Endpoints

### POST /plan

Generate a deployment plan against an environment's current state. Generated plans are cached server-side (in-memory, most recent 50) so a later `POST /apply` can apply exactly the plan that was reviewed, by `plan_id`. Cached plans do not survive a server restart — regenerate if in doubt.

**Request:**

```json
{
  "environment": "production"
}
```

**Response:**

```json
{
  "plan_id": "plan_abc123",
  "environment": "production",
  "created_at": "2024-03-22T14:52:00Z",
  "actions": [
    {
      "dag_id": "daily_etl",
      "task_id": "extract",
      "action": "Execute",
      "reason": "fingerprint changed",
      "fingerprint": "f1a2b3c4d5e6"
    }
  ],
  "stats": {
    "total_tasks": 32,
    "to_execute": 2,
    "to_reuse": 30,
    "to_skip": 0,
    "to_remove": 0,
    "critical_path_depth": 3,
    "blast_radius": 2
  },
  "compilation": {
    "dags_compiled": 5,
    "tasks_total": 32,
    "duration_ms": 45
  }
}
```

### POST /apply

Execute a deployment plan synchronously: each `Execute` action runs for real (through the provider registry for SQL tasks), contracts are validated against the emitted evidence, snapshots are stored, and the environment is updated with a history-recorded, rollbackable version bump.

With `plan_id`, the cached plan is applied only if it still matches reality:

- **404 `not_found`** — unknown or expired `plan_id`
- **400 `bad_request`** — plan targets a different environment than requested
- **409 `conflict`** — stale plan: the environment's version has moved since the plan was generated; regenerate the plan
- **422 `apply_failed`** — a task failed, errored, or violated a contract; the environment is not updated

Without `plan_id`, a fresh plan is generated against current state and applied immediately.

**Request:**

```json
{
  "plan_id": "plan_abc123",
  "environment": "production"
}
```

**Response:**

```json
{
  "plan_id": "plan_abc123",
  "environment": "production",
  "status": "applied",
  "tasks_executed": 2,
  "tasks_reused": 30,
  "tasks_removed": 0,
  "environment_version": 4
}
```

If there is nothing to execute or remove, `status` is `"noop"` and the environment is left untouched.

## Lineage Endpoints

### POST /lineage/sql

Analyze SQL for column-level lineage. Optionally provide inline table schemas for bare column resolution and wildcard expansion. When `openlineage` metadata is provided, the response also includes an OpenLineage RunEvent with a `columnLineage` output dataset facet.

**Request:**

```json
{
  "sql": "SELECT c.id, c.name, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.id, c.name",
  "source_task_id": "daily_customer_totals",
  "tables": [
    {
      "table": "customers",
      "columns": [
        { "name": "id", "data_type": "integer" },
        { "name": "name", "data_type": "string" }
      ]
    },
    {
      "table": "orders",
      "columns": [
        { "name": "customer_id", "data_type": "integer" },
        { "name": "amount", "data_type": "float" }
      ]
    }
  ],
  "openlineage": {
    "output_dataset": "analytics.customer_totals",
    "dataset_namespace": "warehouse",
    "job_namespace": "conduit",
    "job_name": "daily_etl.daily_customer_totals",
    "run_id": "550e8400-e29b-41d4-a716-446655440000",
    "event_type": "COMPLETE"
  }
}
```

**Response:**

```json
{
  "source_task_id": "daily_customer_totals",
  "catalog_used": true,
  "output_columns": [
    {
      "name": "id",
      "expression": "c.id",
      "is_computed": false
    },
    {
      "name": "total",
      "expression": "sum(o.amount)",
      "is_computed": true
    }
  ],
  "source_tables": [
    { "name": "customers", "alias": "c", "schema": null },
    { "name": "orders", "alias": "o", "schema": null }
  ],
  "column_mappings": [
    {
      "output": "total",
      "inputs": [{ "task_id": "orders", "column_name": "amount" }]
    }
  ],
  "openlineage": {
    "eventType": "COMPLETE",
    "run": { "runId": "550e8400-e29b-41d4-a716-446655440000" },
    "job": { "namespace": "conduit", "name": "daily_etl.daily_customer_totals" },
    "inputs": [
      { "namespace": "warehouse", "name": "customers" },
      { "namespace": "warehouse", "name": "orders" }
    ],
    "outputs": [
      {
        "namespace": "warehouse",
        "name": "analytics.customer_totals",
        "facets": {
          "columnLineage": {
            "fields": {
              "total": {
                "inputFields": [
                  {
                    "namespace": "warehouse",
                    "name": "orders",
                    "field": "amount"
                  }
                ]
              }
            }
          }
        }
      }
    ]
  }
}
```

### POST /lineage/catalog/refresh

Refresh the schema catalog by introspecting connected providers.

**Response:**

```json
{
  "status": "refreshed",
  "tables_cataloged": 42,
  "providers_queried": 3
}
```

### POST /lineage/trace/upstream

Get upstream lineage.

**Request:**

```json
{
  "table": "analytics.metrics.customer_metrics",
  "column": "customer_ltv",
  "depth": 10
}
```

**Response:**

```json
{
  "column": "customer_ltv",
  "sources": [
    {
      "table": "warehouse.transformed.customer_summary",
      "column": "total_spent",
      "task": "daily_etl.aggregate_metrics",
      "dag": "daily_etl"
    }
  ]
}
```

### POST /lineage/trace/downstream

Get downstream lineage.

**Request:**

```json
{
  "table": "warehouse.raw.transactions",
  "column": "amount",
  "depth": 10
}
```

**Response:**

```json
{
  "column": "amount",
  "targets": [
    {
      "table": "analytics.metrics.customer_metrics",
      "column": "customer_ltv",
      "task": "daily_etl.create_metrics",
      "dag": "daily_etl"
    }
  ]
}
```

### POST /lineage/graph

Get complete lineage graph.

**Request:**

```json
{
  "start_table": "analytics.metrics.customer_metrics",
  "start_column": "customer_ltv",
  "direction": "both"
}
```

**Response:**

```json
{
  "nodes": [
    {
      "id": "analytics.metrics.customer_ltv",
      "type": "column",
      "table": "analytics.metrics.customer_metrics"
    }
  ],
  "edges": [
    {
      "source": "analytics.metrics.customer_ltv",
      "target": "warehouse.transformed.customer_summary.total_spent"
    }
  ]
}
```

## Event Endpoints

### GET /events

Query event log.

**Query Parameters:**

- `from` / `to` (optional): Sequence number range (inclusive)
- `event_type` (optional): Filter to one event type (e.g. `TaskFailed`, `DagRunCompleted`)
- `run_id`, `dag_id`, `task_id` (optional): Scope to a run, DAG, or task
- `limit` (optional): Max results (default: 100)

**Response:**

```json
{
  "events": [
    {
      "sequence": 12345,
      "type": "TaskCompleted",
      "run_id": "run_abc123",
      "task_id": "extract",
      "timestamp": "2024-03-22T14:32:45Z",
      "data": {
        "duration_ms": 35000,
        "exit_code": 0
      }
    }
  ],
  "total": 5432
}
```

### WebSocket GET /ws/events

Stream events in real-time. Note the path is `/ws/events`, outside the `/api/v1` prefix.

**Subscribe:**

```javascript
const ws = new WebSocket('ws://localhost:8080/ws/events');
ws.addEventListener('message', (event) => {
  const data = JSON.parse(event.data);
  console.log(`${data.type}: ${data.task_id}`);
});
```

**Messages:**

```json
{
  "type": "TaskStarted",
  "run_id": "run_abc123",
  "task_id": "extract",
  "timestamp": "2024-03-22T14:32:10Z"
}
```

## Error Codes

Errors are returned as `{"error": {"type": "...", "message": "..."}}`:

| Type | Status | Meaning |
|------|--------|---------|
| not_found | 404 | Resource (DAG, run, plan, …) doesn't exist |
| environment_not_found | 404 | Environment doesn't exist |
| bad_request | 400 | Malformed request (e.g. plan/environment mismatch) |
| unauthorized | 401 | Missing or invalid API key (when auth is enabled) |
| forbidden | 403 | API key lacks the required permission |
| conflict | 409 | Stale plan: environment changed since plan generation |
| compilation_failed | 422 | DAG compilation failed |
| apply_failed | 422 | Task failure or contract violation during apply |
| promotion_policy_violation | 422 | Environment promotion blocked by policy |
| internal_error | 500 | Server error (details logged server-side only) |

Rate-limited requests receive `429 Too Many Requests`.

## Rate Limiting

Requests are rate limited per client IP: 10 requests/second with a burst capacity of 50. Exceeding the limit returns `429 Too Many Requests`.

## Pagination

List endpoints accept a `limit` query parameter:

```
GET /runs?limit=50       # Most recent 50 runs
GET /events?limit=200    # Most recent 200 events
```

## Other Endpoints

Also routed but not detailed here (see `GET /api/v1/docs` for the live OpenAPI spec and Swagger UI):

- `GET /api/v1/info` — system info
- `POST/GET /api/v1/auth/keys`, `GET/DELETE /api/v1/auth/keys/{key_id}`, `GET /api/v1/auth/me` — API-key management
- `GET /api/v1/dags/{dag_id}/graph` — DAG graph for visualization
- `GET /api/v1/environments/{env_name}/diff/{other_env}`, `GET .../history`, `GET .../history/{version}`, `PUT .../policy` — environment diff, history, and promotion policy
- `POST /api/v1/lineage/schema/diff`, `POST /api/v1/lineage/contracts/validate` — schema diff and contract validation
- `POST /api/v1/openlineage/v1/lineage`, `GET /api/v1/openlineage/events`, `GET /api/v1/openlineage/datasets/{namespace}/{name}`, `GET /api/v1/openlineage/stats`, `GET /api/v1/lineage/datasets/{namespace}/{name}/unified`, `GET /api/v1/lineage/cache/stats`, `POST /api/v1/lineage/cache/invalidate` — OpenLineage ingest and unified dataset views
- `GET /api/v1/contracts`, `GET /api/v1/contracts/{dag_id}`, `GET /api/v1/contracts/{dag_id}/{task_id}` — contract inventory
- `GET /api/v1/metrics`, `GET /api/v1/metrics/{dag_id}/{task_id}` — task metrics
- `GET /api/v1/connections`, `GET /api/v1/connections/providers`, `GET /api/v1/connections/{name}`, `POST /api/v1/connections/{name}/test` — provider connections
- `POST /api/v1/backfill` — backfill runs
- `GET /api/v1/cluster/status`, `POST /api/v1/cluster/workers/{id}/drain` — distributed cluster operations

## Next Steps

- **[Python SDK](./python-sdk.md)**: Use Python client library
- **[CLI Reference](./cli-reference.md)**: Command-line access
- **[Architecture](./architecture.md)**: How API integrates with system
