# Conduit API Documentation

This document describes how to access the API documentation and the endpoints available in the Conduit API.

## Quick Start

Once the Conduit API server is running (default: `http://localhost:9000`), you can access the API documentation at:

- **Swagger UI** (Interactive): http://localhost:9000/api/docs
- **OpenAPI JSON spec**: http://localhost:9000/api/docs/openapi.json
- **ReDoc** (Alternative UI): http://localhost:9000/api/docs/redoc

All documentation endpoints are **public** and do not require authentication.

## API Base Path

All API endpoints are under `/api/v1/`:

```
http://localhost:9000/api/v1
```

Example request:
```bash
curl http://localhost:9000/api/v1/health
```

## Authentication

When authentication is enabled (via `--auth-enabled` flag), most endpoints require a Bearer token in the `Authorization` header:

```bash
curl -H "Authorization: Bearer <api-key>" http://localhost:9000/api/v1/dags
```

### Public Endpoints (No Auth Required)
- `GET /health` - Health check
- `GET /docs` - Swagger UI
- `GET /docs/openapi.json` - OpenAPI specification
- `GET /docs/redoc` - ReDoc UI

### Authentication Endpoints
- `POST /auth/keys` - Create API key (Admin role required)
- `GET /auth/keys` - List API keys (Admin role required)
- `GET /auth/keys/{keyId}` - Get API key details (Admin role required)
- `DELETE /auth/keys/{keyId}` - Revoke API key (Admin role required)
- `GET /auth/me` - Get current authentication context

## API Structure

The API is organized into the following resource groups:

### System
- **Health & Info**: Basic health checks and system information
  - `GET /health`
  - `GET /info`

### DAGs (Directed Acyclic Graphs)
- **Listing & Details**:
  - `GET /dags` - List all compiled DAGs
  - `GET /dags/{dagId}` - Get DAG details with tasks
  - `GET /dags/{dagId}/graph` - Get task execution graph
  - `POST /dags/compile` - Compile DAGs and check for errors

### Runs (DAG Executions)
- **Triggering & Monitoring**:
  - `GET /dags/{dagId}/runs` - List runs for a DAG
  - `POST /dags/{dagId}/runs` - Trigger a new DAG run
  - `GET /runs/{runId}` - Get run details with task states
  - `GET /runs` - List all runs across all DAGs

### Environments
- **Virtual Environment Management**:
  - `GET /environments` - List all environments
  - `POST /environments` - Create a new environment
  - `GET /environments/{envName}` - Get environment details
  - `DELETE /environments/{envName}` - Delete environment
  - `GET /environments/{envName}/diff/{otherEnv}` - Compare two environments
  - `POST /environments/promote` - Promote from one environment to another

### Plan/Apply
- **Terraform-style Deployment**:
  - `POST /plan` - Generate deployment plan (what would change)
  - `POST /apply` - Apply a generated plan

### Events
- **Event History & Time-Travel**:
  - `GET /events` - Query events with range filtering
  - `GET /events/{sequence}` - Get a specific event

### Lineage
- **Column-Level Data Lineage**:
  - `POST /lineage/sql` - Extract lineage from SQL
  - `POST /lineage/trace/upstream` - Find upstream sources for a column
  - `POST /lineage/trace/downstream` - Find downstream consumers of a column
  - `POST /lineage/graph` - Build complete lineage graph
  - `POST /lineage/schema/diff` - Compare schemas for breaking changes
  - `POST /lineage/contracts/validate` - Validate schema against contract

### Contracts
- **Schema Contracts & Data Quality**:
  - `GET /contracts` - List all contracts across DAGs
  - `GET /contracts/{dagId}` - Get contracts for a DAG
  - `GET /contracts/{dagId}/{taskId}` - Get contracts for a specific task

### Metrics
- **Operational Metrics**:
  - `GET /metrics` - List all metrics
  - `GET /metrics/{dagId}/{taskId}` - Get metrics for a task

### Connections
- **External System Connections**:
  - `GET /connections` - List all configured connections
  - `GET /connections/providers` - List supported provider types
  - `GET /connections/{name}` - Get connection details

### Backfill
- **Historical Data Reprocessing**:
  - `POST /backfill` - Create a backfill job for a date range

### Cluster
- **Cluster Management**:
  - `GET /cluster/status` - Get cluster health and worker status
  - `POST /cluster/workers/{id}/drain` - Gracefully drain a worker

## Response Format

All API responses follow a consistent JSON format.

### Success Responses

List endpoints return a wrapper with `total` count:
```json
{
  "dags": [...],
  "total": 3
}
```

Single resource endpoints return the resource directly or in a wrapper:
```json
{
  "runId": "run_daily_etl_20250323_145030",
  "dagId": "daily_etl",
  "status": "queued",
  "taskStates": {...},
  "message": "DAG run queued..."
}
```

### Error Responses

All errors return a structured error object:
```json
{
  "error": {
    "type": "not_found",
    "message": "DAG 'unknown_dag' not found"
  }
}
```

HTTP status codes:
- `200 OK` - Success
- `204 No Content` - Success (for DELETE operations)
- `400 Bad Request` - Invalid request parameters
- `401 Unauthorized` - Missing or invalid authentication
- `403 Forbidden` - Insufficient permissions
- `404 Not Found` - Resource not found
- `422 Unprocessable Entity` - Request valid but unprocessable (e.g., compilation error)
- `500 Internal Server Error` - Server error

## Authentication Roles

API keys can have one of three roles:

| Role | Permissions |
|------|-------------|
| **viewer** | Read-only access to all GET endpoints |
| **operator** | viewer + ability to trigger runs, create backfills, drain workers |
| **admin** | Full access including auth management, environment promotion, applying plans |

## Example Workflows

### List DAGs and Trigger a Run

```bash
# List all DAGs
curl http://localhost:9000/api/v1/dags

# Trigger a run for daily_etl DAG
curl -X POST http://localhost:9000/api/v1/dags/daily_etl/runs \
  -H "Content-Type: application/json" \
  -d '{"logical_date": "2025-03-23"}'

# Get run status
curl http://localhost:9000/api/v1/runs/run_daily_etl_20250323_145030
```

### Environment Management

```bash
# List environments
curl http://localhost:9000/api/v1/environments

# Create staging from production
curl -X POST http://localhost:9000/api/v1/environments \
  -H "Content-Type: application/json" \
  -d '{"name": "staging", "based_on": "production"}'

# Compare staging and production
curl http://localhost:9000/api/v1/environments/staging/diff/production

# Generate plan for staging
curl -X POST http://localhost:9000/api/v1/plan \
  -H "Content-Type: application/json" \
  -d '{"environment": "staging"}'
```

### Data Lineage

```bash
# Extract lineage from SQL
curl -X POST http://localhost:9000/api/v1/lineage/sql \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
    "source_task_id": "extract_orders"
  }'

# Trace upstream for a column
curl -X POST http://localhost:9000/api/v1/lineage/trace/upstream \
  -H "Content-Type: application/json" \
  -d '{"task_id": "build_customer_360", "column_name": "customer_lifetime_value"}'
```

## WebSocket Connections

The API also supports WebSocket connections for live event streaming:

```
ws://localhost:9000/ws/events
```

WebSocket events are broadcast in real-time as DAGs are triggered, tasks complete, and environments are modified.

## Development

### Implementation Details

- **Framework**: Axum (async Rust web framework)
- **OpenAPI Version**: 3.0.0
- **Documentation Tool**: Swagger UI (via CDN)
- **Location**: `/conduit-api/openapi.json` and `/conduit-api/src/handlers/docs.rs`

### Adding New Endpoints

When adding a new endpoint:

1. Add the handler function to the appropriate module in `/handlers/`
2. Add the route to `/src/routes.rs`
3. **Update the OpenAPI spec** in `/openapi.json`:
   - Add the path under `paths`
   - Define request/response schemas under `components/schemas`
   - Include proper HTTP status codes and error responses
   - Document authentication requirements
   - Add operation ID and tags for organization

### Documentation Format

Each endpoint in the OpenAPI spec includes:
- `summary` - One-line description
- `description` - Detailed explanation
- `operationId` - Unique operation identifier
- `parameters` - Path, query, and header parameters
- `requestBody` - Request payload schema (for POST/PUT)
- `responses` - Response schemas for each status code
- `security` - Auth requirements (empty `[]` means public)
- `tags` - Grouping for UI organization

## Testing with curl

```bash
# Get OpenAPI spec
curl http://localhost:9000/api/v1/docs/openapi.json | jq .

# Access Swagger UI
open http://localhost:9000/api/v1/docs

# With authentication
curl -H "Authorization: Bearer your-api-key" http://localhost:9000/api/v1/dags
```

## Further Reading

- OpenAPI Specification: https://spec.openapis.org/oas/v3.0.0
- Swagger UI: https://swagger.io/tools/swagger-ui/
- ReDoc: https://redoc.ly/
