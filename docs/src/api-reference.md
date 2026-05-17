# REST API Reference

Conduit provides a comprehensive REST API for all operations. Start the API server with `conduit serve`.

## Base URL

```
http://localhost:8080/api/v1
```

## Authentication

Currently, Conduit has no authentication (Phase 1). Phase 2 will add API keys and OAuth.

For now, assume all endpoints are public. Secure them at the network/infrastructure level.

## Response Format

All responses are JSON:

```json
{
  "success": true,
  "data": { /* response data */ },
  "error": null,
  "timestamp": "2024-03-22T14:32:10Z"
}
```

Errors:

```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "DAG_NOT_FOUND",
    "message": "DAG 'daily_etl' not found"
  },
  "timestamp": "2024-03-22T14:32:10Z"
}
```

## Health Endpoints

### GET /health

Health check.

**Response:**

```json
{
  "status": "healthy",
  "version": "0.2.0",
  "uptime_ms": 123456,
  "state_store": "connected",
  "scheduler": "running"
}
```

### GET /metrics

Prometheus metrics.

**Response:**

```
# HELP conduit_dags_total Total DAGs compiled
# TYPE conduit_dags_total counter
conduit_dags_total 5
```

## DAG Endpoints

### GET /dags

List all compiled DAGs.

**Query Parameters:**

- `env` (optional): Filter by environment (default: all)
- `limit` (optional): Max results (default: 100)
- `offset` (optional): Offset for pagination (default: 0)

**Response:**

```json
{
  "dags": [
    {
      "dag_id": "daily_etl",
      "description": "Daily ETL pipeline",
      "tasks": 3,
      "schedule": "0 2 * * *",
      "fingerprint": "f1a2b3c4d5e6",
      "created_at": "2024-03-22T14:00:00Z",
      "updated_at": "2024-03-22T14:32:00Z"
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
  "dag_id": "daily_etl",
  "description": "Daily ETL pipeline",
  "schedule": "0 2 * * *",
  "tasks": [
    {
      "task_id": "extract",
      "type": "python",
      "timeout": 300,
      "retries": 1
    },
    {
      "task_id": "transform",
      "type": "python",
      "timeout": 600,
      "retries": 2
    }
  ],
  "fingerprint": "f1a2b3c4d5e6",
  "created_at": "2024-03-22T14:00:00Z"
}
```

### POST /dags/{dag_id}/compile

Recompile a DAG.

**Request:**

```json
{
  "force": false
}
```

**Response:**

```json
{
  "dag_id": "daily_etl",
  "fingerprint": "f1a2b3c4d5e6",
  "num_tasks": 3,
  "compile_duration_ms": 45,
  "status": "success"
}
```

## Run Endpoints

### POST /dags/{dag_id}/run

Start a new DAG run. The run is dispatched to the scheduler which coordinates task execution via the executor. Task state changes are broadcast over WebSocket in real-time.

**Request:**

```json
{
  "logical_date": "2024-03-22T14:00:00Z",
  "config": { "batch_size": 1000 }
}
```

**Response:**

```json
{
  "runId": "run_daily_etl_20240322_143210_123",
  "dagId": "daily_etl",
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

List recent runs.

**Query Parameters:**

- `dag_id` (optional): Filter by DAG
- `status` (optional): Filter by status (pending, running, success, failed)
- `env` (optional): Filter by environment
- `limit` (optional): Max results (default: 20)
- `offset` (optional): Offset for pagination

**Response:**

```json
{
  "runs": [
    {
      "run_id": "run_abc123def456",
      "dag_id": "daily_etl",
      "status": "success",
      "started_at": "2024-03-22T14:32:10Z",
      "completed_at": "2024-03-22T14:37:45Z",
      "duration_ms": 335000,
      "failed_tasks": []
    }
  ],
  "total": 142
}
```

### GET /runs/{run_id}

Get run details.

**Response:**

```json
{
  "run_id": "run_abc123def456",
  "dag_id": "daily_etl",
  "status": "success",
  "env": "production",
  "started_at": "2024-03-22T14:32:10Z",
  "completed_at": "2024-03-22T14:37:45Z",
  "duration_ms": 335000,
  "tasks": [
    {
      "task_id": "extract",
      "status": "success",
      "started_at": "2024-03-22T14:32:10Z",
      "completed_at": "2024-03-22T14:32:45Z",
      "duration_ms": 35000,
      "exit_code": 0,
      "xcom": {
        "row_count": 1000
      }
    }
  ]
}
```

### GET /runs/{run_id}/logs

Stream task logs.

**Query Parameters:**

- `task_id` (optional): Filter by task

**Response:** Server-sent events (text/event-stream)

```
data: [2024-03-22 14:32:10] extract started
data: Extracting data from API...
data: [2024-03-22 14:32:15] extract completed
data: [2024-03-22 14:32:16] transform started
```

### POST /runs/{run_id}/cancel

Cancel a run.

**Response:**

```json
{
  "run_id": "run_abc123def456",
  "status": "cancelled",
  "cancelled_at": "2024-03-22T14:35:00Z"
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
      "name": "production",
      "status": "active",
      "snapshot_id": "prod-snap-20240322-143215",
      "created_at": "2024-03-15T00:00:00Z",
      "last_modified": "2024-03-22T14:32:00Z",
      "num_dags": 5,
      "num_tasks": 32
    },
    {
      "name": "staging",
      "status": "active",
      "snapshot_id": "staging-snap-20240322-145123",
      "created_at": "2024-03-22T14:51:00Z"
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
  "from_env": "production",
  "description": "Staging environment",
  "tags": ["testing", "pre-prod"]
}
```

**Response:**

```json
{
  "name": "staging",
  "snapshot_id": "staging-snap-20240322-145123",
  "forked_from": "production",
  "status": "active",
  "created_at": "2024-03-22T14:51:00Z"
}
```

### GET /environments/{env_name}

Get environment details.

**Response:**

```json
{
  "name": "production",
  "status": "active",
  "snapshot_id": "prod-snap-20240322-143215",
  "created_at": "2024-03-15T00:00:00Z",
  "last_modified": "2024-03-22T14:32:00Z",
  "num_runs_24h": 42,
  "dags": [
    {
      "dag_id": "daily_etl",
      "fingerprint": "f1a2b3c4d5e6",
      "tasks": 3
    }
  ]
}
```

### POST /environments/{env_name}/promote

Promote environment to another.

**Request:**

```json
{
  "target_env": "production",
  "backup": true
}
```

**Response:**

```json
{
  "source_env": "staging",
  "target_env": "production",
  "snapshot_id": "staging-snap-20240322-145123",
  "previous_snapshot": "prod-snap-20240322-143215",
  "promoted_at": "2024-03-22T14:55:00Z"
}
```

### POST /environments/{env_name}/rollback

Rollback environment.

**Request:**

```json
{
  "to_snapshot": "prod-snap-20240322-143215",
  "reason": "high error rate detected"
}
```

**Response:**

```json
{
  "environment": "production",
  "snapshot_id": "prod-snap-20240322-143215",
  "rolled_back_from": "staging-snap-20240322-145123",
  "rolled_back_at": "2024-03-22T14:55:00Z"
}
```

## Plan/Apply Endpoints

### POST /plan

Generate a deployment plan.

**Request:**

```json
{
  "env": "production",
  "from_env": "development"
}
```

**Response:**

```json
{
  "plan_id": "plan_abc123",
  "environment": "production",
  "modified": [
    {
      "dag_id": "daily_etl",
      "task_id": "extract",
      "change_type": "modified",
      "changes": {
        "timeout": "300 → 600"
      }
    }
  ],
  "upstream_invalidated": [
    {
      "dag_id": "daily_etl",
      "task_id": "transform",
      "reason": "upstream extract changed"
    }
  ],
  "impact_analysis": {
    "blast_radius": 2,
    "cascading_changes": 1
  },
  "created_at": "2024-03-22T14:52:00Z"
}
```

### POST /apply

Apply a deployment plan.

**Request:**

```json
{
  "plan_id": "plan_abc123",
  "skip_confirmation": false
}
```

**Response:**

```json
{
  "environment": "production",
  "snapshot_id": "prod-snap-20240322-145456",
  "previous_snapshot": "prod-snap-20240322-143215",
  "tasks_reused": 30,
  "tasks_compiled": 2,
  "applied_at": "2024-03-22T14:52:30Z"
}
```

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

### POST /lineage/upstream

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

### POST /lineage/downstream

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

- `filter` (optional): Filter expression (e.g., `type:TaskFailed`)
- `since` (optional): Time window (e.g., `24h`)
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

### WebSocket /events/stream

Stream events in real-time.

**Subscribe:**

```javascript
const ws = new WebSocket('ws://localhost:8080/api/v1/events/stream');
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

## Snapshot Endpoints

### GET /snapshots

List snapshots.

**Query Parameters:**

- `env` (optional): Filter by environment
- `limit` (optional): Max results

**Response:**

```json
{
  "snapshots": [
    {
      "snapshot_id": "prod-snap-20240322-143215",
      "num_tasks": 32,
      "size_bytes": 1229,
      "created_at": "2024-03-22T14:32:15Z",
      "referenced_by": ["production"]
    }
  ]
}
```

### GET /snapshots/{snapshot_id}

Get snapshot details.

**Response:**

```json
{
  "snapshot_id": "prod-snap-20240322-143215",
  "dags": [
    {
      "dag_id": "daily_etl",
      "fingerprint": "f1a2b3c4d5e6",
      "tasks": 3
    }
  ],
  "created_at": "2024-03-22T14:32:15Z",
  "size_bytes": 1229
}
```

### DELETE /snapshots/{snapshot_id}

Delete a snapshot.

**Response:**

```json
{
  "snapshot_id": "prod-snap-20240322-143215",
  "deleted_at": "2024-03-22T14:55:00Z"
}
```

## Error Codes

| Code | Status | Meaning |
|------|--------|---------|
| DAG_NOT_FOUND | 404 | DAG doesn't exist |
| RUN_NOT_FOUND | 404 | Run doesn't exist |
| ENV_NOT_FOUND | 404 | Environment doesn't exist |
| INVALID_PLAN | 400 | Plan is invalid or stale |
| COMPILATION_ERROR | 400 | DAG compilation failed |
| CONFLICT | 409 | Snapshot conflict (stale plan) |
| INTERNAL_ERROR | 500 | Server error |

## Rate Limiting

Currently no rate limiting. Phase 2 will add per-API-key limits.

## Pagination

Use `limit` and `offset` for pagination:

```
GET /dags?limit=10&offset=0    # First 10
GET /dags?limit=10&offset=10   # Next 10
GET /dags?limit=10&offset=20   # Next 10
```

## Next Steps

- **[Python SDK](./python-sdk.md)**: Use Python client library
- **[CLI Reference](./cli-reference.md)**: Command-line access
- **[Architecture](./architecture.md)**: How API integrates with system
