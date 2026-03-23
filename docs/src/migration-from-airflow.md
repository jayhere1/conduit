# Migration Guide: Airflow to Conduit

This guide helps you migrate existing Airflow DAGs to Conduit. While the orchestration model is different, the core concepts map cleanly.

## Concept Mapping

| Airflow | Conduit | Notes |
|---------|---------|-------|
| DAG | DAG | Same concept, different definition syntax |
| Task | Task | Same concept, slightly different decorators |
| XCom | XCom | Same protocol, slightly different API |
| Connection | Pool or env var | Connection pooling; credentials via env |
| Variable | env var or task input | No mutable global variables |
| Operator | Task decorator | Conduit uses decorators, not classes |
| Sensor | sensor_task | Same concept, different syntax |
| BranchOperator | Conditional DAG | Conduit uses Python conditionals |
| Trigger rule | trigger_rule | AllSuccess, AllDone, OneSuccess, OneFailed |
| Execution context | TaskContext | Access via TaskContext() |

## High-Level Migration Path

1. **Audit Airflow DAGs** — Understand your existing setup
2. **Rewrite DAGs** — Convert to Conduit syntax
3. **Test locally** — Use `conduit run` to validate
4. **Parallel run** — Run both Airflow and Conduit for a period
5. **Switchover** — Point production to Conduit
6. **Decommission Airflow** — Clean up old infrastructure

Typical timeline: **2–8 weeks** depending on DAG complexity.

## Example: Rewriting an Airflow DAG

### Airflow Original

```python
# airflow/dags/sales_etl.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'analytics',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sales_etl',
    default_args=default_args,
    description='Daily sales ETL',
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['production', 'sales'],
)

def extract_data():
    import pandas as pd
    df = pd.read_sql("SELECT * FROM raw_sales", conn)
    print(f"Extracted {len(df)} rows")

def transform_data():
    import pandas as pd
    df = pd.read_sql("SELECT * FROM raw_sales", conn)
    df = df.dropna()
    print(f"Transformed {len(df)} rows")

def load_data():
    print("Loading to warehouse...")

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    op_kwargs={},
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    op_kwargs={},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    op_kwargs={},
    dag=dag,
)

extract_task >> transform_task >> load_task
```

### Conduit Converted

```python
# dags/sales_etl.py
from conduit.sdk import dag, task

@task(retries=2, retry_delay=300)
def extract():
    import pandas as pd
    # Credentials from environment
    conn_str = os.getenv("DATABASE_URL")
    df = pd.read_sql("SELECT * FROM raw_sales", conn_str)
    print(f"Extracted {len(df)} rows")
    print(f"xcom|row_count|{len(df)}")
    return "raw_sales.csv"

@task(retries=2, retry_delay=300)
def transform(raw_file):
    import pandas as pd
    conn_str = os.getenv("DATABASE_URL")
    df = pd.read_sql("SELECT * FROM raw_sales", conn_str)
    df = df.dropna()
    clean_count = len(df)
    print(f"Transformed {clean_count} rows")
    print(f"xcom|clean_count|{clean_count}")
    return "clean_sales.csv"

@task(retries=2, retry_delay=300)
def load(clean_file):
    print("Loading to warehouse...")
    return "success"

@dag(schedule="0 2 * * *", tags=["production", "sales"])
def sales_etl():
    """Daily sales ETL pipeline."""
    raw = extract()
    clean = transform(raw)
    load(clean)
```

**Key differences**:
- Decorators instead of operator classes
- No separate `default_args` dict
- Return values flow automatically (no explicit XCom sets)
- Schedule as string parameter, not `schedule_interval`
- Credentials from environment, not Airflow Connections

## Feature-by-Feature Migration

### Schedules

**Airflow:**
```python
dag = DAG(
    'my_dag',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
```

**Conduit:**
```python
@dag(schedule="0 2 * * *")
def my_dag():
    pass
```

Conduit uses standard 5-field cron syntax. No separate start_date or catchup behavior.

### Operators → Tasks

**Airflow:**
```python
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

task1 = PythonOperator(
    task_id='python_task',
    python_callable=my_function,
    provide_context=True,
    dag=dag,
)

task2 = BashOperator(
    task_id='bash_task',
    bash_command='ls -la /data',
    dag=dag,
)
```

**Conduit:**
```python
from conduit.sdk import task, shell_task

@task
def python_task():
    my_function()

@shell_task
def bash_task():
    ls -la /data
```

### Dependencies

**Airflow:**
```python
task1 >> task2 >> task3  # Sequential
[task1, task2] >> task3  # task1 and task2 → task3
```

**Conduit:**
```python
@dag
def my_dag():
    t1 = task1()
    t2 = task2(t1)  # task2 depends on task1 via input
    t3 = task3(t2)  # task3 depends on task2
```

Dependencies are inferred from function arguments.

### Retries and Timeouts

**Airflow:**
```python
task = PythonOperator(
    task_id='my_task',
    python_callable=my_func,
    retries=3,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(hours=1),
    dag=dag,
)
```

**Conduit:**
```python
@task(
    retries=3,
    retry_delay=300,      # seconds
    timeout=3600,         # seconds
)
def my_task():
    my_func()
```

### XCom

**Airflow:**
```python
def task1(context):
    context['task_instance'].xcom_push(
        key='my_key',
        value='my_value'
    )

def task2(context):
    value = context['task_instance'].xcom_pull(
        task_ids='task1',
        key='my_key'
    )
    print(value)

task1 = PythonOperator(task_id='task1', python_callable=task1, dag=dag)
task2 = PythonOperator(task_id='task2', python_callable=task2, dag=dag)
task1 >> task2
```

**Conduit:**
```python
@task
def task1():
    return {'my_key': 'my_value'}

@task
def task2(data):
    print(data['my_key'])

@dag
def my_dag():
    data = task1()
    task2(data)
```

Or explicit XCom:

```python
@task
def task1():
    print("xcom|my_key|my_value")
    return "result"
```

### Trigger Rules

**Airflow:**
```python
from airflow.utils.trigger_rule import TriggerRule

task = PythonOperator(
    task_id='my_task',
    python_callable=my_func,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)
```

**Conduit:**
```python
from conduit.sdk import TriggerRule

@task(trigger_rule=TriggerRule.ALL_DONE)
def my_task():
    my_func()
```

Available rules: `ALL_SUCCESS`, `ALL_DONE`, `ONE_SUCCESS`, `ONE_FAILED`

### Context Access

**Airflow:**
```python
def my_task(context):
    dag_run = context['dag_run']
    task_instance = context['task_instance']
    print(f"Run ID: {dag_run.run_id}")
    print(f"Attempt: {task_instance.try_number}")
```

**Conduit:**
```python
from conduit.sdk import TaskContext

@task
def my_task():
    ctx = TaskContext()
    print(f"Run ID: {ctx.run_id}")
    print(f"Attempt: {ctx.attempt}")
```

### Connections and Secrets

**Airflow:**
```python
from airflow.models import Variable
from airflow.hooks.base import BaseHook

def my_task():
    # Get connection
    conn = BaseHook.get_connection('my_db')
    user = conn.login
    password = conn.password

    # Get variable
    api_key = Variable.get('API_KEY')
```

**Conduit:**
Use environment variables:

```python
import os

@task
def my_task():
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    api_key = os.getenv('API_KEY')
```

Set environment variables in your deployment:

```bash
export DB_USER=admin
export DB_PASSWORD=secret
export API_KEY=xyz123

conduit run my_dag
```

Or in `.conduit.toml`:

```toml
[env]
DB_USER = "admin"
DB_PASSWORD = "secret"
API_KEY = "xyz123"
```

### Conditional Execution

**Airflow:**
```python
from airflow.operators.python import PythonOperator
from airflow.operators.branch import BranchPythonOperator

def choose_path(context):
    if context['execution_date'].day % 2 == 0:
        return 'even_task'
    else:
        return 'odd_task'

branch = BranchPythonOperator(
    task_id='branch',
    python_callable=choose_path,
    dag=dag,
)

even_task = PythonOperator(task_id='even_task', ...)
odd_task = PythonOperator(task_id='odd_task', ...)

branch >> [even_task, odd_task]
```

**Conduit:**
```python
import os

@task
def choose_path():
    day = int(os.getenv('DAY', '1'))
    return day % 2 == 0

@task
def even_task():
    print("Even day")

@task
def odd_task():
    print("Odd day")

@dag
def my_dag():
    is_even = choose_path()

    if is_even:
        even_task()
    else:
        odd_task()
```

**Note**: Conditionals are evaluated at compile time in Conduit. For runtime branching, use trigger rules instead.

### Sensors

**Airflow:**
```python
from airflow.sensors.filesystem import FileSensor

wait = FileSensor(
    task_id='wait_for_file',
    filepath='/data/export.csv',
    poke_interval=60,
    timeout=3600,
    dag=dag,
)
```

**Conduit:**
```python
from conduit.sdk import sensor_task
import os

@sensor_task(timeout=3600, poke_interval=60)
def wait_for_file():
    return os.path.exists('/data/export.csv')
```

## Automated Migration Tool

Conduit provides a migration tool for simple DAGs:

```bash
conduit migrate airflow --config ~/airflow.cfg --output dags/
```

**Capabilities**:
- Converts PythonOperator → @task
- Converts BashOperator → @shell_task
- Extracts schedules
- Converts trigger rules
- Generates basic XCom mappings

**Limitations**:
- Cannot convert complex operators (custom subclasses)
- Cannot infer context usage
- May require manual cleanup

## Step-by-Step Migration Plan

### Phase 1: Preparation (1 week)

1. **Audit Airflow DAGs**
   ```bash
   # List all DAGs
   airflow dags list

   # Find complex DAGs
   grep -r "BranchOperator\|SubDagOperator\|TriggerRuleSensor" dags/
   ```

2. **Categorize by complexity**
   - Tier 1: Simple linear DAGs (extract → transform → load)
   - Tier 2: Conditional DAGs (branching, dynamic tasks)
   - Tier 3: Complex DAGs (subDAGs, multiple pools, custom operators)

3. **Identify dependencies**
   - Which DAGs depend on which?
   - Which use shared connections, variables, or pools?

### Phase 2: Rewrite (2–4 weeks)

1. **Start with Tier 1 DAGs**
   ```bash
   # Rewrite simple DAGs
   # Run through migration tool for baseline
   conduit migrate airflow --output dags/

   # Manually cleanup and test
   cd dags
   vim extracted_dag.py
   ```

2. **Validate compilation**
   ```bash
   conduit compile
   ```

3. **Test locally**
   ```bash
   conduit run my_dag
   ```

4. **Handle Tier 2 DAGs**
   - Rewrite conditional logic as Python if/else
   - Convert custom operators to @task

5. **Handle Tier 3 DAGs**
   - Break subDAGs into multiple DAGs or inline them
   - Rewrite custom operators as @task + subprocess

### Phase 3: Testing (1–2 weeks)

1. **Create staging environment**
   ```bash
   conduit env create staging --from production
   ```

2. **Deploy Conduit DAGs**
   ```bash
   vim dags/*.py  # Make sure all DAGs are ready
   conduit compile
   conduit plan staging
   conduit apply staging -y
   ```

3. **Run test executions**
   ```bash
   # Run each DAG manually
   conduit run my_dag --env staging

   # Verify outputs match Airflow
   ```

4. **Monitor for 1 week**
   - Check logs
   - Verify XCom outputs
   - Validate downstream systems receive correct data

### Phase 4: Parallel Run (1–2 weeks)

1. **Keep Airflow running** for production DAGs
2. **Run Conduit in staging** for the same DAGs
3. **Compare outputs** — Are results identical?
4. **Build confidence** — Run both systems side-by-side

### Phase 5: Switchover (1 day)

1. **Disable Airflow DAGs**
   ```bash
   # In Airflow UI: Set DAGs to off
   ```

2. **Enable Conduit DAGs**
   ```bash
   conduit env promote staging production
   ```

3. **Monitor closely** for 24 hours
   - Watch scheduler logs
   - Check task execution times
   - Verify downstream systems

4. **Rollback plan** ready if needed
   ```bash
   conduit env rollback production --to previous-snapshot
   ```

### Phase 6: Decommission (ongoing)

1. **Delete old Airflow DAGs** after 30 days of successful Conduit runs
2. **Archive Airflow database** for audit trail
3. **Notify teams** of migration completion

## Common Gotchas

### 1. Credentials and Secrets

**Problem**: Airflow uses Connections, Conduit uses environment variables.

**Solution**:
- Export all Airflow connections to environment variables
- Use a secrets management tool (HashiCorp Vault, AWS Secrets Manager)
- Set in `.conduit.toml` or CI/CD system

### 2. Dynamic Task Generation

**Problem**: Airflow allows dynamic tasks via loops. Conduit's DAG structure is static at compile time.

**Solution**:
```python
# Airflow: Dynamic tasks
for i in range(10):
    task = PythonOperator(task_id=f'task_{i}', ...)

# Conduit: Static DAG, dynamic execution
@dag
def static_dag():
    tasks = [
        extract(i)
        for i in range(10)  # Generated at compile time
    ]
    return tasks
```

Both compile statically, but Conduit declares all tasks upfront.

### 3. Custom Operators

**Problem**: Your organization has custom operators that don't have Conduit equivalents.

**Solution**:
- Rewrite custom operator logic as a @task function
- Or use `@executable_task` to call the operator binary

### 4. Backfill and Catchup

**Problem**: Airflow supports backfill and catchup. Conduit does not.

**Solution**:
- Use Conduit's replay feature for historical debugging
- For backfill-like behavior, run DAG manually for past dates:
  ```bash
  conduit run my_dag --env production --date 2024-01-01
  ```

### 5. Pools and Resource Limits

**Problem**: Airflow has multiple pools. Conduit uses a single Pool abstraction.

**Solution**:
- Create a Pool for each resource type
- Map Airflow pools to Conduit pools 1:1

## Performance Expectations

### Compilation
- Airflow: Seconds (DAG parsing + full execution)
- Conduit: Milliseconds (tree-sitter parsing only)

### Scheduling
- Airflow: Polling every 5–30 seconds
- Conduit: Event-driven, microseconds latency

### Deployment
- Airflow: Restart webserver, parse all DAGs
- Conduit: Plan/apply, only recompile changed DAGs

### Typical improvements
- Faster feedback cycle: seconds → milliseconds
- Lower latency: polls → events
- Better visibility: immutable event log

## Next Steps

- **[Installation](./getting-started/installation.md)**: Set up Conduit
- **[Quick Start](./getting-started/quick-start.md)**: Write your first DAG
- **[DAG Concepts](./concepts/dags.md)**: Full DAG definition guide
- **[Migration Tool](./cli-reference.md#migrate)**: Automated conversion
