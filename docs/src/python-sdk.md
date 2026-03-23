# Python SDK Guide

The Conduit Python SDK provides decorators and utilities for defining DAGs and tasks. All code is statically analyzed via tree-sitter, not executed.

## Installation

The SDK is bundled with Conduit. Import it:

```python
from conduit.sdk import dag, task, Task, DAG
```

## Basic Usage

### Defining a DAG

```python
from conduit.sdk import dag, task

@task
def extract():
    print("Extracting data...")
    return "data.csv"

@task
def transform(data):
    print(f"Transforming {data}...")
    return "clean.csv"

@task
def load(data):
    print(f"Loading {data}...")
    return "success"

@dag(schedule="0 2 * * *")
def etl_pipeline():
    """Daily ETL pipeline."""
    raw = extract()
    clean = transform(raw)
    result = load(clean)
    return result
```

### Task Decorators

All functions decorated with `@task` or `@dag` must be defined at module level (not nested).

```python
# Valid
@task
def my_task():
    pass

@dag
def my_dag():
    my_task()

# Invalid - nested
def outer():
    @task
    def inner():  # ERROR: nested task
        pass
```

## Task Types

### Python Tasks

Default task type, executes Python code:

```python
@task(timeout=300)
def python_task():
    import pandas as pd
    df = pd.read_csv("data.csv")
    return len(df)
```

### Shell Tasks

Execute bash commands:

```python
from conduit.sdk import shell_task

@shell_task(timeout=600)
def bash_task():
    set -e  # Exit on error
    aws s3 ls s3://my-bucket/
    dbt run --profiles-dir /etc/dbt
```

### SQL Tasks

Execute SQL against configured data warehouse:

```python
from conduit.sdk import sql_task

@sql_task(dialect="postgres")
def create_table():
    CREATE TABLE staging_users AS
    SELECT * FROM raw_users
    WHERE created_at >= current_date - interval '1 day'

@sql_task(dialect="postgres")
def parametrized_query(start_date: str, limit: int):
    SELECT * FROM transactions
    WHERE date >= '{start_date}'
    LIMIT {limit}
```

### Sensor Tasks

Poll until condition is met:

```python
from conduit.sdk import sensor_task
import os
import time

@sensor_task(timeout=3600, poke_interval=60)
def wait_for_file():
    if os.path.exists("/data/export.csv"):
        return True  # Unblock
    return False  # Retry in 60s

@sensor_task(timeout=7200, poke_interval=300)
def wait_for_api():
    import requests
    response = requests.get("https://api.example.com/status")
    return response.json()["ready"] == True
```

## Task Configuration

```python
from conduit.sdk import task, Pool, TriggerRule

@task(
    timeout=300,                              # Timeout in seconds
    retries=2,                                # Retry count
    retry_delay=60,                           # Initial retry delay (seconds)
    retry_exponential_base=2,                 # Exponential backoff multiplier
    pool=Pool.name("api_calls", size=5),     # Concurrency pool
    tags=["production", "critical"],          # Metadata tags
    trigger_rule=TriggerRule.ALL_SUCCESS,    # When to run
)
def configured_task():
    print("Task with full configuration")
```

### Pools

Limit concurrent task execution:

```python
from conduit.sdk import Pool

api_pool = Pool.name("api_requests", size=3)

@task(pool=api_pool)
def api_call_1():
    requests.get("https://api.example.com/data")

@task(pool=api_pool)
def api_call_2():
    requests.get("https://api.example.com/users")

@task(pool=api_pool)
def api_call_3():
    requests.get("https://api.example.com/events")

# Only 3 of these run in parallel
```

### Trigger Rules

Control when downstream tasks execute:

```python
from conduit.sdk import TriggerRule

@task
def may_fail():
    import random
    if random.random() < 0.5:
        raise Exception("Random failure")
    return "success"

@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def run_on_success():
    # Runs only if may_fail succeeded
    print("May fail succeeded")

@task(trigger_rule=TriggerRule.ALL_DONE)
def run_always():
    # Runs regardless of may_fail status
    print("May fail completed (success or failure)")

@task(trigger_rule=TriggerRule.ONE_FAILED)
def run_on_failure():
    # Runs only if may_fail failed
    print("May fail failed")

@dag
def conditional_dag():
    result = may_fail()
    run_on_success(result)
    run_always(result)
    run_on_failure(result)
```

## Data Exchange (XCom)

### Implicit Returns

Return values flow automatically:

```python
@task
def extract():
    return {"count": 1000, "file": "data.csv"}

@task
def transform(data):
    # data = {"count": 1000, "file": "data.csv"}
    print(f"Processing {data['file']}")
    return {"clean_count": 950, "file": "clean.csv"}

@task
def load(data):
    # data = {"clean_count": 950, "file": "clean.csv"}
    print(f"Loading {data['count']} rows")

@dag
def etl():
    extracted = extract()
    transformed = transform(extracted)
    load(transformed)
```

### Explicit XCom Output

For structured logging:

```python
@task
def extract():
    print("xcom|row_count|1000")
    print("xcom|file_size|2.5GB")
    print("Starting extraction...")
    return "data.csv"

@dag
def my_dag():
    data = extract()
    # XCom values: row_count=1000, file_size="2.5GB"
```

## Dependency Resolution

Dependencies are inferred from function arguments:

```python
@task
def a(): return "a"
@task
def b(): return "b"
@task
def c(in_a, in_b): return f"{in_a}{in_b}"
@task
def d(in_c): return f"final: {in_c}"

@dag
def complex_dag():
    output_a = a()
    output_b = b()
    # c depends on both a and b
    output_c = c(output_a, output_b)
    # d depends on c
    output_d = d(output_c)
    return output_d
```

Tree-sitter parses the function calls and builds the dependency graph automatically.

## Task Context

Access information about the current run:

```python
from conduit.sdk import TaskContext

@task
def contextual_task():
    ctx = TaskContext()
    print(f"Run ID: {ctx.run_id}")
    print(f"Task ID: {ctx.task_id}")
    print(f"Environment: {ctx.environment}")
    print(f"Attempt: {ctx.attempt}")  # 1, 2, 3... on retries
    return "done"
```

Available context:

- `ctx.run_id` — Unique run ID
- `ctx.task_id` — Task ID within DAG
- `ctx.dag_id` — DAG ID
- `ctx.environment` — Environment name
- `ctx.attempt` — Attempt number (1-based)
- `ctx.xcom_pull(key)` — Retrieve XCom from previous task

## Advanced Patterns

### Shared Utilities

Keep utilities in `tasks/` directory:

```python
# tasks/api.py
import requests

def fetch_json(url):
    return requests.get(url).json()

def batch_api_calls(urls, batch_size=5):
    for i in range(0, len(urls), batch_size):
        batch = urls[i:i+batch_size]
        yield [fetch_json(url) for url in batch]
```

Import in DAGs:

```python
# dags/etl.py
from conduit.sdk import dag, task
from tasks.api import batch_api_calls

@task
def fetch_data():
    urls = ["https://api.example.com/page/1", ...]
    for batch in batch_api_calls(urls):
        print(f"Fetched {len(batch)} items")
    return "done"

@dag
def my_dag():
    fetch_data()
```

### Conditional DAGs

Use Python conditionals for dynamic DAGs:

```python
import os

@task
def check_env():
    return os.getenv("ENVIRONMENT", "dev")

@task
def prod_task():
    return "production path"

@task
def dev_task():
    return "development path"

@task
def finalize(result):
    return f"done: {result}"

@dag
def conditional_dag():
    env = check_env()

    # This is evaluated at COMPILE time
    if env == "production":
        result = prod_task()
    else:
        result = dev_task()

    finalize(result)
```

**Important**: This DAG is evaluated at compile time, not runtime. If you need runtime branching, use trigger rules instead.

### Parameterized DAGs

Pass parameters via environment variables:

```python
import os

@task
def extract():
    limit = int(os.getenv("EXTRACTION_LIMIT", "1000"))
    print(f"Extracting {limit} rows...")
    return "data.csv"

@dag
def configurable_etl():
    extract()
```

Run with parameters:

```bash
EXTRACTION_LIMIT=5000 conduit run configurable_etl
```

### Dynamic Task Graphs

Generate multiple tasks dynamically:

```python
@task
def extract(partition):
    print(f"Extracting partition {partition}")
    return f"data_{partition}.csv"

@task
def transform(data):
    return f"clean_{data}"

@dag
def multi_partition_etl():
    partitions = ["2024-01", "2024-02", "2024-03"]

    # This is evaluated at COMPILE time
    extracted = [extract(p) for p in partitions]
    transformed = [transform(e) for e in extracted]

    return transformed
```

This creates 6 tasks total (3 extract + 3 transform).

## Error Handling

### Task-Level Retries

Configured via task decorator:

```python
@task(retries=3, retry_delay=60)
def flaky_api():
    requests.get("https://flaky-api.example.com")
```

### DAG-Level Handling

Use trigger rules for error handling:

```python
@task
def critical_task():
    if something_wrong:
        raise Exception("Critical failure")

@task(trigger_rule=TriggerRule.ALL_DONE)
def notify_ops(result):
    # Runs regardless of critical_task status
    print(f"Task completed: {result}")
    # Check actual status and alert if needed
```

## Logging Best Practices

### Standard Output

Everything printed goes to logs:

```python
@task
def logged_task():
    print("INFO: Starting extraction")
    print("ERROR: Connection failed")  # Treated as regular output
    return "result"
```

### Structured Logging

Use XCom for metrics:

```python
@task
def extract():
    rows = 1000
    size_mb = 2.5

    print("xcom|rows_extracted|1000")
    print("xcom|size_mb|2.5")
    print(f"Extracted {rows} rows ({size_mb}MB)")

    return "data.csv"
```

## SDK Functions

### Import All

```python
from conduit.sdk import (
    dag,
    task,
    shell_task,
    sql_task,
    sensor_task,
    executable_task,
    DAG,
    Task,
    TaskContext,
    Pool,
    TriggerRule,
    Lineage,
    SchemaContract,
    DataContract,
)
```

## Type Hints (Optional)

Conduit doesn't enforce types, but hints are allowed:

```python
from typing import Dict, List

@task
def extract() -> Dict[str, int]:
    return {"count": 1000}

@task
def transform(data: Dict) -> List[str]:
    return ["a", "b", "c"]

@dag
def typed_dag():
    d = extract()
    transform(d)
```

Type hints are ignored during compilation and execution. They're purely for IDE assistance.

## Debugging

### Print Debugging

Standard Python print works:

```python
@task
def debug_task():
    x = 42
    print(f"DEBUG: x = {x}")
    return x
```

Output appears in logs.

### Introspection

Use TaskContext for debugging:

```python
@task
def introspect():
    ctx = TaskContext()
    print(f"Run: {ctx.run_id}")
    print(f"Attempt: {ctx.attempt}")
    return "done"
```

## Performance Notes

- **Compilation**: Tree-sitter parses in milliseconds
- **Task execution**: Standard Python execution, no overhead
- **Data transfer**: XCom is serialized as JSON
- **Memory**: Large return values are written to disk automatically

## Next Steps

- **[DAG Concepts](./concepts/dags.md)**: Advanced DAG patterns
- **[Python Task Examples](../quick-start.md)**: Real-world examples
- **[REST API](./api-reference.md)**: Programmatic API access
