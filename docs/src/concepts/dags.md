# DAG Definition and Task Types

A DAG (Directed Acyclic Graph) in Conduit is a Python function decorated with `@dag` that orchestrates a sequence of tasks. Each task is a unit of work decorated with `@task`.

## Basic DAG Structure

```python
from conduit.sdk import dag, task

@task
def task_a():
    print("Running task A")
    return "a_output"

@task
def task_b(input_a):
    print(f"Running task B with input: {input_a}")
    return "b_output"

@dag
def my_first_dag():
    """
    A simple two-task pipeline.

    This DAG runs task_a, passes its output to task_b, and completes.
    """
    output_a = task_a()
    output_b = task_b(output_a)
    return output_b
```

When you call a task function within a `@dag`, you're not executing the task immediately. Instead, you're declaring a dependency in the DAG graph.

## DAG Configuration

Use the `@dag` decorator to configure execution behavior:

```python
@dag(
    schedule="0 9 * * *",           # Cron: daily at 9 AM
    description="Sales ETL",        # Human-readable description
    retries=1,                       # Retry entire DAG on failure
    timeout=3600,                    # Timeout in seconds (1 hour)
    pool=Pool.name("etl", size=2),  # Named pool for concurrency control
    tags=["production", "daily"],    # Metadata tags
)
def sales_etl():
    raw = extract()
    clean = transform(raw)
    load(clean)
```

### Schedule (Cron Expressions)

Conduit uses standard **5-field cron syntax**:

```
┌─────────── minute (0 - 59)
│ ┌─────────── hour (0 - 23)
│ │ ┌─────────── day of month (1 - 31)
│ │ │ ┌─────────── month (1 - 12)
│ │ │ │ ┌─────────── day of week (0 - 6, 0=Sunday)
│ │ │ │ │
│ │ │ │ │
* * * * *
```

Common patterns:

- `0 9 * * *` — Daily at 9:00 AM
- `0 0 * * *` — Daily at midnight
- `*/15 * * * *` — Every 15 minutes
- `0 9 * * 1-5` — Weekdays at 9 AM
- `0 0 1 * *` — First of every month at midnight
- `0 */6 * * *` — Every 6 hours

### Pools (Concurrency Control)

Named pools limit how many tasks can run concurrently:

```python
from conduit.sdk import Pool

@task(pool=Pool.name("api_requests", size=5))
def call_api():
    # Max 5 tasks with this pool can run in parallel
    requests.get("https://api.example.com/data")

@task(pool=Pool.name("database", size=2))
def query_db():
    # Max 2 database queries at once
    conn.execute("SELECT * FROM users")
```

Pools are **global** across all DAGs. If you have two DAGs both using `pool_database`, they share the same concurrency limit.

## Task Types

Conduit supports five built-in task types. The type is inferred from the task implementation.

### 1. Python Tasks

Execute arbitrary Python code:

```python
@task
def process_data():
    import pandas as pd
    df = pd.read_csv("data.csv")
    df = df.dropna()
    print(f"Processed {len(df)} rows")
    return df.to_json()

@task(timeout=600, retries=2)
def api_call(url):
    import requests
    response = requests.get(url, timeout=10)
    return response.json()
```

Python tasks are executed via `python -c "<task_code>"`. The task can import any package available in the Python environment.

### 2. Shell Tasks

Execute bash commands:

```python
from conduit.sdk import shell_task

@shell_task
def backup_database():
    # This is executed as a bash script
    pg_dump my_database > backup.sql
    gzip backup.sql
    aws s3 cp backup.sql.gz s3://backups/$(date +%Y%m%d).sql.gz

@shell_task(timeout=1800)
def run_dbt():
    cd /projects/analytics
    dbt run --profiles-dir /etc/dbt
    dbt test
```

### 3. SQL Tasks

Execute SQL against a configured data warehouse:

```python
from conduit.sdk import sql_task

@sql_task(dialect="postgres")
def create_staging_table():
    CREATE TABLE staging_users AS
    SELECT * FROM raw_users
    WHERE created_at >= current_date - interval '1 day'

@sql_task(dialect="postgres")
def aggregate_metrics(table_name: str):
    INSERT INTO metrics (user_id, transaction_count, total_amount)
    SELECT
        user_id,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount
    FROM {table_name}
    GROUP BY user_id
```

SQL tasks are templated, allowing parameter substitution. The dialect (postgres, mysql, snowflake, bigquery) determines how the task parses and validates the SQL.

### 4. Sensor Tasks

Block until a condition is met:

```python
from conduit.sdk import sensor_task
import time

@sensor_task(timeout=3600, poke_interval=60)
def wait_for_file():
    # Check every 60 seconds if file exists
    if os.path.exists("/data/export.csv"):
        return True  # Unblock
    return False  # Retry

@sensor_task(timeout=7200, poke_interval=300)
def wait_for_api():
    import requests
    response = requests.get("https://api.example.com/status")
    if response.json()["status"] == "ready":
        return True
    return False
```

Sensors poll indefinitely until they return `True` or hit the timeout.

### 5. Generic Executables

Run any binary or script:

```python
from conduit.sdk import executable_task

@executable_task
def run_custom_binary():
    /opt/custom-tool/process data.txt > output.txt

@executable_task(timeout=600)
def run_ruby_script():
    ruby /scripts/aggregate.rb
```

## Task Configuration

All tasks accept common configuration parameters:

```python
@task(
    timeout=300,                          # Timeout in seconds
    retries=2,                            # Number of retries on failure
    retry_delay=60,                       # Delay between retries (seconds)
    retry_exponential_base=2,             # Exponential backoff multiplier
    pool=Pool.name("transforms", size=3), # Concurrency pool
    tags=["data", "critical"],            # Metadata tags
)
def my_task():
    print("Task with full config")
```

### Timeouts

If a task exceeds its timeout, it's terminated and marked as failed:

```python
@task(timeout=10)
def long_running():
    # This will be killed after 10 seconds
    import time
    time.sleep(30)  # Timeout!
```

### Retries

Failed tasks are retried with exponential backoff:

```python
@task(
    retries=3,                    # Retry up to 3 times
    retry_delay=5,                # Wait 5s before first retry
    retry_exponential_base=2,     # 5s, 10s, 20s, ...
)
def flaky_api_call():
    requests.get("https://flaky-api.example.com/data")
```

The retry sequence is: initial attempt, then 5s, 10s, 20s delays between retries.

### Trigger Rules

Control when downstream tasks start based on upstream status:

```python
from conduit.sdk import TriggerRule

@task
def extract():
    return "data"

@task(trigger_rule=TriggerRule.ALL_SUCCESS)  # Default
def transform(data):
    # Runs only if extract succeeded
    return f"transformed: {data}"

@task(trigger_rule=TriggerRule.ALL_DONE)
def log_result():
    # Runs whether extract succeeded or failed
    print("Extract finished")

@task(trigger_rule=TriggerRule.ONE_FAILED)
def alert_ops():
    # Runs only if extract failed
    print("Alert: extract failed")
```

Available trigger rules:
- `ALL_SUCCESS` (default) — All upstream tasks succeeded
- `ALL_DONE` — All upstream tasks finished (success or failure)
- `ONE_SUCCESS` — At least one upstream task succeeded
- `ONE_FAILED` — At least one upstream task failed

## Data Exchange (XCom)

Tasks communicate via **XCom** (Cross-Communication). There are two ways:

### Implicit Return Values

Return values are automatically captured:

```python
@task
def task_a():
    return {"count": 42, "status": "ok"}

@task
def task_b(data):
    # data = {"count": 42, "status": "ok"}
    print(f"Count: {data['count']}")
    return data["count"] * 2

@dag
def my_dag():
    output = task_a()
    result = task_b(output)
    return result
```

### Explicit XCom Output

For structured logging and metrics:

```python
@task
def extract():
    print("xcom|row_count|1000")
    print("xcom|file_size|2.5GB")
    return "data.csv"

@dag
def my_dag():
    data = extract()
    return data
```

The executor parses `xcom|key|value` messages and stores them in the event log for later retrieval.

## Task Dependencies

Dependencies are declared implicitly through function calls:

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
    output_c = c(output_a, output_b)  # Depends on both a and b
    output_d = d(output_c)              # Depends on c
    return output_d
```

Conduit builds the DAG by tracing these function call chains.

## Complex Workflows

### Branching

Tasks can have multiple downstream dependencies:

```python
@task
def split_data(data):
    return data

@task
def process_for_warehouse(data):
    return f"warehouse_ready: {data}"

@task
def process_for_analytics(data):
    return f"analytics_ready: {data}"

@task
def merge_results(warehouse, analytics):
    return f"merged: {warehouse} + {analytics}"

@dag
def parallel_processing():
    data = split_data("input.csv")
    w = process_for_warehouse(data)
    a = process_for_analytics(data)
    merged = merge_results(w, a)
    return merged
```

This creates a diamond-shaped DAG: split → {warehouse, analytics} → merge

### Conditional Execution

Use Python conditionals to create dynamic DAGs:

```python
@task
def check_condition():
    import random
    return random.choice([True, False])

@task
def process_path_a():
    return "A"

@task
def process_path_b():
    return "B"

@task
def join_paths(result):
    return f"final: {result}"

@dag
def conditional_dag():
    condition = check_condition()

    # This is Python conditional logic at DAG definition time
    # (not runtime, so it's evaluated once during compilation)
    if condition:
        result = process_path_a()
    else:
        result = process_path_b()

    final = join_paths(result)
    return final
```

**Important**: Conduit evaluates conditionals at compile time, not runtime. For runtime branching, use trigger rules instead.

## Best Practices

1. **Keep tasks small**: Each task should do one thing well.
2. **Use pools**: Prevent resource exhaustion by limiting concurrent tasks.
3. **Set meaningful timeouts**: Catch hung tasks early.
4. **Retry judiciously**: Don't retry non-idempotent operations.
5. **Document with tags**: Use tags for categorization and filtering.
6. **Return structured data**: Use dictionaries or JSON for multi-value returns.
7. **Log progress**: Use `xcom|metric|value` for monitoring.

## Next Steps

- **[Virtual Environments](./environments.md)**: Deploy DAGs to isolated environments
- **[Plan/Apply Workflow](./plan-apply.md)**: Understand change detection and deployment
- **[Python SDK](../python-sdk.md)**: Advanced SDK features and patterns
