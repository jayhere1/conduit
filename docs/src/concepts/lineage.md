# Column-Level Lineage (Beta)

Conduit provides column-level SQL lineage powered by `sqlparser-rs` AST analysis. This feature is functional and tested (67 tests) but labeled **beta** due to known limitations with template SQL and view resolution.

## What Is Lineage?

Lineage answers the question: **"Where did this column come from?"**

For example, if a dashboard shows a metric `customer_lifetime_value`, lineage traces its source:

```
dashboard.revenue_dashboard.customer_ltv
  ← analytics.metrics.customer_metrics.customer_ltv
    ← warehouse.transformed.customer_summary.total_spent
      ← warehouse.raw.transactions.amount
        ← (origin: payment API)
```

This tracing works across multiple DAGs, systems, and technologies.

## Why Column-Level Lineage Matters

### 1. Data Governance

Understand data dependencies for compliance:
- GDPR: Find all places customer PII flows
- HIPAA: Audit medical data access
- CCPA: Identify where personal data is stored

### 2. Impact Analysis

When a source system changes, understand cascading effects:
- Source column deleted → which dashboards break?
- Data quality issue upstream → which reports are affected?

### 3. Debugging

Trace data quality issues to their root cause:
- Dashboard shows wrong numbers → which query is wrong?
- ETL pipeline failed → which upstream task caused it?

### 4. Documentation

Automatic documentation of how data flows:
- No more "I think this column comes from…"
- Executable source of truth

## How Conduit Tracks Lineage

### 1. SQL Parsing (sqlparser-rs AST)

For SQL tasks, Conduit parses the query AST to extract column dependencies. This uses `sqlparser-rs` for proper parsing (not regex), supporting:

- **SELECT** with aliases, expressions, and qualified references (`schema.table.column`)
- **JOINs** (INNER, LEFT, RIGHT, FULL, CROSS)
- **CTEs** (WITH clauses, including chained CTEs with column propagation)
- **UNIONs** / INTERSECT / EXCEPT
- **Subqueries** in FROM and scalar positions
- **Window functions** (PARTITION BY, ORDER BY)
- **INSERT INTO...SELECT** and **CREATE TABLE AS SELECT**
- **WHERE clause** dependency tracking

```python
@sql_task(dialect="postgres")
def create_customer_summary():
    CREATE TABLE analytics.customer_summary AS
    SELECT
        customer_id,
        name,           -- FROM raw.customers
        email,          -- FROM raw.customers
        SUM(amount),    -- FROM raw.transactions
        COUNT(*)        -- FROM raw.transactions
    FROM raw.customers c
    JOIN raw.transactions t ON c.id = t.customer_id
    GROUP BY customer_id, name, email
```

Conduit's SQL parser extracts:

```
Output: analytics.customer_summary
  - customer_id → FROM raw.customers.customer_id
  - name → FROM raw.customers.name
  - email → FROM raw.customers.email
  - sum → FROM raw.transactions.amount
  - count → FROM raw.transactions (COUNT(*))
```

### TableCatalog Integration

When a `TableCatalog` is provided (populated from connected providers via `information_schema`), lineage gains additional capabilities:

- **Bare column resolution**: `SELECT active FROM orders o JOIN customers c` correctly maps `active` to `customers` (instead of guessing the first table)
- **Wildcard expansion**: `SELECT *` is expanded to actual column names
- **CTE column propagation**: Output columns of CTEs are resolved through to their source tables

### 2. Python Task Lineage

For Python tasks, lineage is specified via annotations:

```python
from conduit.sdk import task, Lineage

@task
def transform_data(input_df):
    """
    Lineage annotations specify where outputs come from.
    """
    output_df = input_df.copy()
    output_df['revenue_category'] = pd.cut(
        output_df['amount'],
        bins=[0, 100, 1000, float('inf')],
        labels=['low', 'medium', 'high']
    )
    return output_df

# Specify lineage explicitly
transform_data.set_lineage({
    'revenue_category': ['amount']  # Column created from amount
})
```

Or with decorators:

```python
from conduit.sdk import task, lineage

@task
@lineage.input_columns(['customer_id', 'amount', 'date'])
@lineage.output_columns({'revenue_category': ['amount']})
def categorize_revenue(df):
    df['revenue_category'] = pd.cut(df['amount'], ...)
    return df[['customer_id', 'revenue_category', 'date']]
```

## Lineage Graph API

Query the lineage graph:

### Upstream Lineage

Find the origin of a column:

```python
from conduit import lineage_client

# What sources feed into this column?
upstream = lineage_client.upstream(
    table='analytics.metrics.customer_metrics',
    column='customer_ltv'
)

# Output:
# [
#   {
#     'source': 'warehouse.transformed.customer_summary',
#     'column': 'total_spent',
#     'task': 'daily_analytics_etl.aggregate_metrics',
#     'dag': 'daily_analytics_etl'
#   },
#   {
#     'source': 'warehouse.raw.transactions',
#     'column': 'amount',
#     'task': 'daily_analytics_etl.extract',
#     'dag': 'daily_analytics_etl'
#   }
# ]
```

### Downstream Lineage

Find all dependents of a column:

```python
# What downstream objects use this column?
downstream = lineage_client.downstream(
    table='warehouse.raw.transactions',
    column='amount'
)

# Output:
# [
#   {
#     'target': 'warehouse.transformed.customer_summary',
#     'column': 'total_spent',
#     'task': 'daily_analytics_etl.aggregate_metrics',
#     'dag': 'daily_analytics_etl'
#   },
#   {
#     'target': 'analytics.metrics.customer_metrics',
#     'column': 'customer_ltv',
#     'task': 'daily_analytics_etl.create_metrics',
#     'dag': 'daily_analytics_etl'
#   }
# ]
```

### Lineage Graph Visualization

```python
# Get complete lineage graph
graph = lineage_client.graph(
    start_table='analytics.metrics.customer_metrics',
    start_column='customer_ltv',
    direction='both'  # upstream and downstream
)

# Outputs:
# nodes: [
#   {'id': 'analytics.metrics.customer_ltv', 'type': 'column'},
#   {'id': 'warehouse.transformed.customer_summary.total_spent', 'type': 'column'},
#   {'id': 'warehouse.raw.transactions.amount', 'type': 'column'},
#   {'id': 'daily_analytics_etl.aggregate_metrics', 'type': 'task'},
#   {'id': 'daily_analytics_etl.extract', 'type': 'task'}
# ]
# edges: [
#   {'source': 'analytics.metrics.customer_ltv', 'target': 'warehouse.transformed.customer_summary.total_spent'},
#   ...
# ]
```

## Schema Contracts

Define expected schemas to detect breaking changes:

```python
from conduit.sdk import task, SchemaContract
from pydantic import BaseModel

class RawTransactionsSchema(BaseModel):
    transaction_id: str
    customer_id: int
    amount: float
    timestamp: str
    status: str  # NEW: This is a breaking change

@task
@SchemaContract.input('transactions', RawTransactionsSchema)
def process_transactions(df):
    # If upstream doesn't provide 'status', fail at compile time
    return df[df['status'] == 'completed']
```

Conduit verifies schema contracts at compile time:

```bash
conduit compile
```

Output:

```
Error: Schema contract violation
  Task: process_transactions
  Contract: RawTransactionsSchema
  Missing: status
  Provided by upstream: transaction_id, customer_id, amount, timestamp

The upstream task doesn't provide the 'status' column.
Update the upstream task to include 'status', or update the contract.
```

## Lineage Queries via REST API

Query lineage via HTTP:

```bash
# Get upstream lineage
curl http://localhost:8080/api/v1/lineage/upstream \
  -H "Content-Type: application/json" \
  -d '{
    "table": "analytics.metrics.customer_metrics",
    "column": "customer_ltv"
  }'

# Response:
# [
#   {
#     "source": "warehouse.transformed.customer_summary",
#     "column": "total_spent",
#     "task": "daily_analytics_etl.aggregate_metrics",
#     "dag": "daily_analytics_etl"
#   }
# ]
```

## Data Contracts and Breaking Changes

Detect breaking changes automatically:

```python
from conduit.sdk import task, DataContract

class CustomerMetricsContract(DataContract):
    """Schema for customer metrics table."""
    customer_id: int
    total_spent: float
    last_purchase: str
    segment: str

@task(contract=CustomerMetricsContract)
def create_metrics():
    # If output columns don't match contract, fail at runtime
    return {
        'customer_id': 123,
        'total_spent': 999.99,
        'last_purchase': '2024-03-22',
        'segment': 'high_value'
    }

# If you remove a column:
@task(contract=CustomerMetricsContract)
def create_metrics_v2():
    return {
        'customer_id': 123,
        'total_spent': 999.99,
        'last_purchase': '2024-03-22'
        # Missing: segment
    }
```

Conduit detects the contract violation:

```
Error: Contract violation
  Task: create_metrics_v2
  Contract: CustomerMetricsContract
  Missing columns: segment
  Impact: 3 downstream tasks depend on this column
    - dashboard_builder.segment_analysis
    - reporting_etl.customer_segments
    - ml_pipeline.customer_clustering
```

## Advanced Use Cases

### Finding Stale Columns

Identify columns that are produced but never consumed:

```python
stale = lineage_client.unused_columns(
    since='30d'  # Not used in past 30 days
)

# Output:
# [
#   {
#     'table': 'warehouse.transformed.legacy_metrics',
#     'column': 'old_metric',
#     'produced_by': 'daily_etl.legacy_aggregate',
#     'days_unused': 45
#   }
# ]
```

### Impact Analysis for Schema Changes

When you remove a column, see what breaks:

```python
impact = lineage_client.impact_of_removal(
    table='warehouse.raw.transactions',
    column='amount'
)

# Output:
# {
#   'direct_consumers': [
#     'daily_etl.aggregate_metrics'
#   ],
#   'indirect_consumers': [
#     'daily_etl.create_metrics',
#     'reporting_etl.dashboard_data'
#   ],
#   'total_affected_tasks': 3,
#   'total_affected_dashboards': 5
# }
```

### Compliance Audits

Find all places PII flows:

```python
pii_columns = ['email', 'phone', 'ssn', 'address']

for col in pii_columns:
    paths = lineage_client.downstream(
        table='raw.customers',
        column=col
    )
    print(f"Column {col} flows to:")
    for path in paths:
        print(f"  {path['target']}.{path['column']}")
```

## Current Status (Beta)

SQL lineage is functional and tested:

### Implemented
- SQL parsing via `sqlparser-rs` AST (PostgreSQL, MySQL, BigQuery, Snowflake, and other dialects)
- TableCatalog integration for bare column resolution and wildcard expansion
- CTE column propagation
- Explicit lineage annotations for Python tasks
- REST API endpoints (`/lineage/sql`, `/lineage/catalog/refresh`)
- Lineage graph traversal
- 67 test cases covering real ETL patterns

### Known Limitations
- Jinja/template SQL (`{{ ref('model') }}`) crashes the parser
- Views cannot be resolved to underlying tables without a recursive catalog
- `SELECT * EXCEPT(col)` (BigQuery syntax) not supported
- LATERAL joins, PIVOT/UNPIVOT, UNNEST not handled
- Python task lineage is annotation-based only (no static analysis)

### Planned
- Jinja pre-processing (strip templates before parsing)
- Runtime schema capture via `CONDUIT::SCHEMA::` protocol
- Schema evolution tracking
- Lineage visualization in the Web UI

## Next Steps

- **[Events Architecture](./events.md)**: How events enable lineage tracking
- **[REST API](../api-reference.md)**: Lineage API endpoints
- **[Architecture](../architecture.md)**: How lineage integrates with the system
