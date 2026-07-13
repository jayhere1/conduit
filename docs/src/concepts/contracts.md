# Data Quality Contracts

Contracts are declarative assertions about your pipeline's data. They live alongside
your DAG definitions — in YAML or Python — and are validated automatically during
`conduit plan` and `conduit apply`. Error-severity violations block deployment.
Warning-severity violations are reported but don't block.

## Evidence-Based Design

Unlike tools that assume SQL access, Conduit contracts are **evidence-based**.
Tasks emit **measurements** (evidence) via the stdout protocol, and contracts
assert against those measurements. This works uniformly across SQL, Python,
shell, API, and any other task type.

```
Task (any language)
  └─ emits CONDUIT::METRIC::row_count::5000
  └─ emits CONDUIT::METRIC::data_age_seconds::3600
  └─ emits CONDUIT::METRIC::null_rate.email::0.02

Executor
  └─ collects metrics into Evidence { metrics: HashMap<String, f64> }

ContractEvaluator
  └─ takes (Evidence, TaskContracts) → ValidationResult
  └─ each contract check knows which metric to look up
  └─ missing evidence = contract failure
```

The task is the only thing that knows its own output. The contract doesn't
know or care *how* the measurement was produced — only that it was emitted.

## Emitting Evidence

### Python Tasks

```python
from conduit_sdk import emit_metric, emit_row_count, emit_freshness

def extract_orders():
    rows = fetch_data()

    # Convenience helpers
    emit_row_count(len(rows))
    emit_freshness(rows[-1]["created_at"])

    # Generic metric emission
    emit_metric("duplicate_count", count_dupes(rows))
    emit_metric("null_rate.email", null_fraction(rows, "email"))
    emit_metric("accuracy", model.evaluate())

    return rows
```

### Shell Tasks

```bash
#!/bin/bash
ROW_COUNT=$(wc -l < output.csv)
echo "CONDUIT::METRIC::row_count::$ROW_COUNT"
echo "CONDUIT::METRIC::data_age_seconds::3600"
```

### SQL Tasks

Conduit's built-in SQL executor auto-emits common metrics (`row_count`,
`data_age_seconds`, `duplicate_count`, `null_rate.*`) for SQL tasks. You
don't need to emit them manually — contracts just work.

## Contract Types

### Row Count
Assert that the output has a minimum, maximum, or exact number of rows.
Expects metric: `row_count`

```yaml
contracts:
  - type: row_count
    min: 1
    max: 10000000
```

### Freshness
Assert that data is recent. Expects metric: `data_age_seconds`

```yaml
contracts:
  - type: freshness
    max_age: 24h
```

### Unique
Assert no duplicate values across columns. Expects metric: `duplicate_count`

```yaml
contracts:
  - type: unique
    columns: [id]
```

### Not Null
Assert that a column has a minimum fraction of non-null values.
Expects metric: `null_rate.{column}`

```yaml
contracts:
  - type: not_null
    column: customer_id
    min_rate: 0.99  # allow up to 1% nulls
```

### Accepted Values
Assert that a column's values are within a known set.
Expects metric: `invalid_value_count.{column}`

```yaml
contracts:
  - type: accepted_values
    column: status
    values: [pending, shipped, delivered, cancelled]
```

### Value Range
Assert that a numeric column falls within bounds.
Expects metric: `out_of_range_count.{column}`

```yaml
contracts:
  - type: value_range
    column: amount
    min: 0
    max: 1000000
```

### Referential Integrity
Assert that every value in a column exists in another task's output.
Expects metric: `orphan_count.{column}`

```yaml
contracts:
  - type: references
    column: customer_id
    ref_task: extract_customers
    ref_column: id
```

### Row Count Delta
Assert that the row count doesn't change too dramatically between runs.
Expects metric: `row_count_delta_pct`

```yaml
contracts:
  - type: row_count_delta
    max_percent_change: 0.1    # flag if >10% change
    allow_decrease: false       # any decrease is an error
```

### Metric (Generic)
The universal contract — assert any named metric against bounds.
This is the escape hatch for any custom measurement.

```yaml
contracts:
  - type: metric
    metric_name: accuracy
    min: 0.95
  - type: metric
    metric_name: latency_ms
    max: 500
  - type: metric
    metric_name: enrichment_rate
    min: 0.90
    max: 1.0
```

### Custom Assertion
A named pass/fail check. The task emits `pass.{name}::1` or `pass.{name}::0`.

```yaml
contracts:
  - type: custom
    assertion_name: no_orphan_orders
    description: "Every order must have a valid customer"
```

## Severity

Every contract defaults to `error` severity (blocks deployment). Set `severity: warning`
to report without blocking:

```yaml
contracts:
  - type: row_count_delta
    max_percent_change: 0.1
    severity: warning
    description: "Alert if customer count changes significantly"
```

## YAML Example

```yaml
id: daily_etl
tasks:
  extract_orders:
    type: sql
    query: "SELECT * FROM source.orders"
    contracts:
      - type: row_count
        min: 1
      - type: freshness
        max_age: 24h
      - type: unique
        columns: [id]
      - type: not_null
        column: customer_id
      - type: accepted_values
        column: status
        values: [pending, shipped, delivered]
      - type: value_range
        column: amount
        min: 0

  train_model:
    type: python
    module: ml.training
    function: train
    contracts:
      - type: metric
        metric_name: accuracy
        min: 0.90
      - type: metric
        metric_name: training_loss
        max: 0.1
      - type: custom
        assertion_name: model_convergence
```

## Python SDK

The Python SDK supports both decorator and imperative styles:

```python
from conduit_sdk import task, emit_metric, emit_row_count
from conduit_sdk.contracts import contract, check

# Decorator style — declare what to assert
@task(retries=3)
@contract(
    check.row_count(min=1),
    check.freshness(max_age="24h"),
    check.unique(["id"]),
    check.not_null("customer_id"),
    check.metric("accuracy", min=0.95),
)
def extract_orders():
    rows = do_work()
    # Emit evidence for the contracts to validate against
    emit_row_count(len(rows))
    emit_metric("data_age_seconds", compute_age(rows))
    emit_metric("duplicate_count", count_dupes(rows))
    emit_metric("null_rate.customer_id", null_frac(rows, "customer_id"))
    emit_metric("accuracy", evaluate())
    return rows

# Imperative style — contracts emitted at runtime
from conduit_sdk.contracts import Contracts

def transform():
    result = do_work()
    c = Contracts("transform")
    c.row_count(min=1, max=1_000_000)
    c.metric("accuracy", min=0.95)
    c.emit()  # sends to executor via stdout protocol
```

## Plan/Apply Integration

When you run `conduit plan`, the output shows which contracts will be validated
and what metrics each task is expected to emit:

```
Contracts: 11 checks across 3 tasks (validated during apply)
  daily_etl.extract_orders — 6 checks
    expected metrics: row_count, data_age_seconds, duplicate_count,
                      null_rate.customer_id, invalid_value_count.status,
                      out_of_range_count.amount
  daily_etl.train_model — 2 checks
    expected metrics: accuracy, pass.model_convergence
```

During `conduit apply`, immediately after each task executes, its evidence is
validated against that task's contracts. A passing task prints an inline
`[CHK ]` line; a violation prints `[CVIO]` plus the failing check(s) to
stderr:

```
  [EXEC]  contract_ok.emit
  [CHK ] contract_ok.emit contracts: 1/1 checks passed
  [OK]    contract_ok.emit (5ms)
```

```
  [EXEC]  contract_bad.emit
  [CVIO] contract_bad.emit contracts: 0/1 checks passed
          ! row_count:emit: 5 rows, expected at least 1000
```

If any Error-severity contract fails, the apply stops before the environment
is updated and prints the `DeploymentValidation` summary (this is the actual
output from `conduit apply` against a task whose `row_count` contract
requires `min: 1000` but the task only emitted `CONDUIT::METRIC::row_count::5`):

```
Contract Validation Summary
─────────────────────────────
Contracts for 'contract_bad.emit': FAILED (0/1 checks passed, 1 errors, 0 warnings)
  [ERROR] row_count:emit: 5 rows, expected at least 1000

Result: BLOCKED — 1 errors must be fixed before deployment (0 warnings)

Error: apply blocked: contract validation failed for contract_bad.emit — environment not updated
```

The environment's snapshot pointers are left untouched — `conduit status` /
`conduit env list` show no change, and a subsequent `conduit plan` still
reports the task as pending execution. Warning-severity failures print the
same way but do not block; `DeploymentValidation.can_deploy` (and
`DeploymentPlan::can_apply`) is only `false` when at least one Error-severity
check fails.

## API

The REST API exposes contract information:

- `GET /api/v1/contracts` — list all contracts across all DAGs
- `GET /api/v1/contracts/:dag_id` — contracts for a specific DAG
- `GET /api/v1/contracts/:dag_id/:task_id` — contracts for a specific task
