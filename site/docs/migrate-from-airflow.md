# Migrating from Airflow to Conduit

**You don't have to rewrite everything. Start with one DAG.**

---

## Why Migrate?

Let's be direct: Airflow was the right choice in 2016. It democratized DAG scheduling when there was no alternative. You probably have years of institutional knowledge baked into your Airflow setup—complex task groups, branch logic, retry policies tuned to perfection. That's not wasted effort. It's the baseline we're building on.

But 2026 is different. The operational burden of Airflow compounds over time. Here's what changes with Conduit:

| Dimension | Airflow | Conduit | Why It Matters |
|-----------|---------|---------|----------------|
| **Parsing** | `exec(compile(source))` | Tree-sitter AST | No code execution = 1000x faster, zero cold-start surprises |
| **State Management** | Mutable PostgreSQL rows | Event-sourced RocksDB | No scheduler deadlocks, time-travel debugging, local replication |
| **Deployment** | Restart scheduler, cross fingers | `plan` + `apply` + rollback | No cascading failures, instant rollback on bad DAG |
| **Infrastructure** | 5+ services (scheduler, webserver, worker, db, queue) | 1 binary | 80% less operational surface area |
| **Testing** | Full staging environment | Virtual environment fork | Test in milliseconds, not minutes |
| **Secrets** | Database, encryption at rest | YAML + env vars + sealed secrets | Simpler, audit-friendly, no DB schema drift |

The killer feature isn't any single one of these—it's the **friction reduction**. Airflow makes you think twice about every change. Conduit makes local iteration and validation automatic.

---

## Concept Mapping

Before diving into code, here's the mental translation table. Every Airflow concept has a Conduit equivalent:

| Airflow | Conduit | Notes |
|---------|---------|-------|
| **DAG** | DAG | Same thing—directed acyclic graph of tasks. Defined via `@dag` decorator instead of `DAG()` context manager. |
| **Operator** | Task Type | Shell, Python, SQL, Sensor, or Executable. No custom operator plugins (yet). |
| **Connection** | Connection | Stored in YAML or env config, not in the database. Same concept—credentials for external systems. |
| **Variable** | Environment Variable | `conduit env set key value`. Simpler than XCom for static config. |
| **XCom** | XCom | Protocol-based: tasks write `CONDUIT::XCOM::key::value` to stdout. Cleaner, no database pollution. |
| **DagBag** | ConduitPlan | A compiled artifact (not runtime bag import). Parsed once, reused forever. |
| **Sensor (polling)** | Sensor Task Type | Declarative, not a Python loop. No more brittle `poke()` methods. |
| **Pool** | Pool | Named resource pools with slot limits. Identical concept, simpler config. |
| **Trigger Rule** | Trigger Rule | `AllSuccess` (default), `AllDone`, `OneSuccess`, `OneFailed`, `NoDeps`. Same logic. |
| **SLA/Alerts** | Not yet | Roadmap item. For now, use webhooks via `on_failure`. |

---

## Automatic Migration

The `conduit migrate` command is your friend for greenfield projects and simple DAGs. It won't catch everything, but it's a solid starting point.

### The Command

```bash
conduit migrate ./airflow_dags --output ./conduit_dags
```

This command:
1. **Scans** for Python files with Airflow-style DAG definitions (looks for `DAG()` constructor and `@dag` decorators).
2. **Detects** `@dag` decorators and `DAG()` patterns (regex-based, not AST—quick and dirty).
3. **Maps** operators: `BashOperator` → shell task, `PythonOperator` → python task, etc.
4. **Generates** YAML DAG files (one YAML per migrated DAG).
5. **Reports** success/failures with a `MIGRATION_REPORT.txt` summary.

### What Gets Migrated Automatically

- **DAG ID, schedule, tags, description** — extracted from decorator args and docstrings
- **Task definitions** — operator type, task_id, retries, retry_delay, pool, timeout, priority
- **Task dependencies** — simple `>>` and `<<` operators
- **Basic task types** — BashOperator, PythonOperator, SQLExecuteQueryOperator

### What Doesn't Get Migrated

- Custom operators or plugins
- Jinja templating in SQL/bash commands
- Dynamic DAG factories (see below)
- Airflow-specific macros (like `{{ ds }}`, `{{ ti.xcom_pull() }}`)
- Complex branching logic
- SLA and alert configurations

### Example: Before and After

**Before (Airflow):**

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract_data(**context):
    """Extract data from source system."""
    print("Extracting...")
    # Logic here

def transform_data(**context):
    """Transform and validate."""
    print("Transforming...")
    # Logic here

with DAG(
    'daily_etl',
    description='Daily ETL pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'warehouse'],
) as dag:
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        pool='warehouse_pool',
    )

    load = BashOperator(
        task_id='load',
        bash_command='python /opt/scripts/load.py',
        timeout=1800,
    )

    extract >> transform >> load
```

**After (Conduit, Python):**

```python
from conduit_sdk import dag, task

@dag(schedule="0 0 * * *", tags=["etl", "warehouse"])
def daily_etl():
    """Daily ETL pipeline."""

    @task(retries=2, retry_delay="5m")
    def extract():
        """Extract data from source system."""
        # Your logic here
        pass

    @task(pool="warehouse_pool")
    def transform(data=extract):
        """Transform and validate."""
        # Your logic here
        pass

    @task(timeout="30m")
    def load(data=transform):
        """Load to warehouse."""
        import subprocess
        subprocess.run(['python', '/opt/scripts/load.py'], check=True)

    raw = extract()
    cleaned = transform(raw)
    load(cleaned)
```

**After (Conduit, YAML):**

```yaml
dag_id: daily_etl
description: "Daily ETL pipeline"
schedule: "0 0 * * *"
tags:
  - etl
  - warehouse

tasks:
  extract:
    type: python
    module: jobs.etl
    function: extract_data
    retries: 2
    retry_delay: "5m"

  transform:
    type: python
    module: jobs.etl
    function: transform_data
    pool: warehouse_pool
    dependencies:
      - task_id: extract
        type: data_flow

  load:
    type: bash
    command: "python /opt/scripts/load.py"
    timeout: "30m"
    dependencies:
      - task_id: transform
        type: execution_order
```

The key differences:
1. **Decorators instead of context managers** — cleaner Python.
2. **Function arguments as data flow** — `transform(data=extract)` is explicit dependency, not `>>` magic.
3. **No context object** — no `**context`, no `ti.xcom_pull()`, simpler function signatures.
4. **YAML is flat** — no task groups, no nested context managers. Conduit relies on task naming conventions.

---

## Manual Migration Patterns

Automatic migration is a head start, not a finish line. Here's how to handle the common Airflow patterns that don't translate directly.

### a. BashOperator → shell task

**Airflow:**
```python
run_query = BashOperator(
    task_id='run_query',
    bash_command='python /scripts/query.py {{ ds }}',
)
```

**Conduit (YAML):**
```yaml
tasks:
  run_query:
    type: shell
    command: "python /scripts/query.py"
    # Note: {{ ds }} execution_date won't work
    # Pass as env var or CLI arg instead
```

**Conduit (Python):**
```python
@task()
def run_query():
    import subprocess
    import os
    date = os.environ.get('EXECUTION_DATE', '2024-01-01')
    subprocess.run(['python', '/scripts/query.py', date], check=True)
```

The pattern: **move Jinja templating into your task code or environment setup**. Conduit doesn't template bash commands; it passes data via environment variables, stdout/XCom, or function arguments.

### b. PythonOperator → python task

**Airflow:**
```python
def process_orders(ds, **context):
    """Callable function for PythonOperator."""
    ti = context['task_instance']
    orders = ti.xcom_pull(task_ids=['extract'])
    print(f"Processing orders for {ds}")
    ti.xcom_push(key='result', value={'count': 100})

process = PythonOperator(
    task_id='process',
    python_callable=process_orders,
    provide_context=True,
    op_kwargs={'env': 'prod'},
)
```

**Conduit (Python):**
```python
@task()
def process():
    """Process orders."""
    from datetime import datetime
    ds = datetime.utcnow().date().isoformat()

    # XCom pull becomes function argument
    orders = extract()  # Call upstream task

    print(f"Processing orders for {ds}")
    result = {'count': 100}

    # XCom push: print to stdout
    print(f"CONDUIT::XCOM::result::{result}")

    return result

# Or with dependency injection in DAG:
@dag(schedule="@daily")
def my_pipeline():
    @task()
    def extract():
        return [1, 2, 3]

    @task()
    def process(orders=extract):
        # orders is injected as function arg
        print(f"Processing {len(orders)} orders")
        return {'count': len(orders)}

    result = process()
```

Key difference: **Conduit tasks are pure functions**. No context object, no `**kwargs` magic. Data flows through function arguments and return values.

### c. SQLOperator → sql task

**Airflow:**
```python
load_warehouse = SQLExecuteQueryOperator(
    task_id='load_warehouse',
    sql='SELECT * FROM raw.orders WHERE date = "{{ ds }}"',
    conn_id='snowflake',
    database='analytics',
)
```

**Conduit (YAML):**
```yaml
tasks:
  load_warehouse:
    type: sql
    connection: snowflake
    query: "SELECT * FROM raw.orders WHERE date = '2024-01-01'"
    # No Jinja—pass date via ENV or stored procedure
```

**Conduit (Python):**
```python
@task(type="sql")
def load_warehouse():
    return """
    SELECT * FROM raw.orders
    WHERE date = CURRENT_DATE()
    """
```

For SQL: **use stored procedures or database functions** (like `CURRENT_DATE()`) instead of templating. Cleaner, more portable.

### d. Sensor → sensor task type

**Airflow:**
```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id='wait_for_upstream',
    external_dag_id='upstream_dag',
    external_task_id='final_task',
    poke_interval=60,
    timeout=3600,
    allowed_states=['success'],
)
```

**Conduit:**
```yaml
tasks:
  wait_for_upstream:
    type: sensor
    sensor_type: "external_task"
    poke_interval: "60s"
    # Sensor configuration (future—roadmap)
```

**Conduit (Python):**
```python
@sensor()
def wait_for_upstream():
    """Poll for upstream DAG completion."""
    # Sensor logic: return True when condition met, False to retry
    import requests
    resp = requests.get('http://internal-api/dag/upstream_dag/status')
    return resp.json()['final_task'] == 'success'
```

Sensors are **declarative in Conduit**—you define the condition, not the polling loop. Conduit handles retries and backoff.

### e. TaskGroup → flat task naming

**Airflow:**
```python
with DAG('etl') as dag:
    with TaskGroup('extract') as extract_group:
        api = PythonOperator(task_id='api', ...)
        db = PythonOperator(task_id='db', ...)

    transform = PythonOperator(task_id='transform', ...)

    extract_group >> transform
    # Task IDs become: extract.api, extract.db, transform
```

**Conduit:**
```yaml
# Conduit doesn't have TaskGroup—use naming conventions
dag_id: etl

tasks:
  extract_api:
    type: python
    # ...

  extract_db:
    type: python
    # ...

  transform:
    type: python
    dependencies:
      - task_id: extract_api
      - task_id: extract_db
```

**Conduit (Python):**
```python
@dag()
def etl():
    @task()
    def extract_api():
        pass

    @task()
    def extract_db():
        pass

    @task()
    def transform(api_data=extract_api, db_data=extract_db):
        pass

    api = extract_api()
    db = extract_db()
    transform(api_data=api, db_data=db)
```

No TaskGroup equivalent yet. Instead: **use explicit naming** (`extract_api`, `extract_db`) and **pass data via function arguments**.

### f. Dynamic DAG Factories → Manual Expansion

This is the hardest pattern to migrate. Airflow lets you generate DAGs at runtime:

**Airflow (factory):**
```python
for dataset in ['orders', 'customers', 'products']:
    with DAG(f'etl_{dataset}', schedule_interval='@daily') as dag:
        extract = BashOperator(task_id='extract', bash_command=f'extract {dataset}.py')
        load = BashOperator(task_id='load', bash_command=f'load {dataset}.py')
        extract >> load
```

This generates 3 DAGs at runtime: `etl_orders`, `etl_customers`, `etl_products`.

**Conduit doesn't support this** because it parses the AST, not executes code. You have two options:

**Option 1: Write out individual DAG files (recommended)**

Create a Python script to generate the YAML:

```python
#!/usr/bin/env python3
"""Generate Conduit DAGs from a template."""

import yaml
from pathlib import Path

DATASETS = ['orders', 'customers', 'products']
OUTPUT_DIR = Path('./conduit_dags')
OUTPUT_DIR.mkdir(exist_ok=True)

for dataset in DATASETS:
    dag = {
        'dag_id': f'etl_{dataset}',
        'schedule': '0 0 * * *',
        'tasks': {
            'extract': {
                'type': 'shell',
                'command': f'python extract_{dataset}.py',
            },
            'load': {
                'type': 'shell',
                'command': f'python load_{dataset}.py',
                'dependencies': [{'task_id': 'extract', 'type': 'execution_order'}],
            },
        },
    }

    with open(OUTPUT_DIR / f'{dataset}.yaml', 'w') as f:
        yaml.dump(dag, f, default_flow_style=False)

print(f"Generated {len(DATASETS)} DAGs in {OUTPUT_DIR}")
```

Run this script once to create individual files. Then version-control them. This is **explicit and audit-friendly**.

**Option 2: Use a single parameterized DAG (future)**

Once Conduit supports parameterized DAG runs, you can do:

```python
@dag()
def etl(dataset: str):
    @task()
    def extract(ds=dataset):
        pass

    @task()
    def load(data=extract):
        pass

    load()

# Then: conduit run etl --param dataset=orders
```

But this doesn't exist yet. **Stick with Option 1 for now.**

### g. XComs → stdout protocol

**Airflow:**
```python
def upstream(**context):
    ti = context['task_instance']
    ti.xcom_push(key='user_count', value=12345)

def downstream(**context):
    ti = context['task_instance']
    count = ti.xcom_pull(key='user_count', task_ids=['upstream'])
    print(f"User count: {count}")
```

**Conduit:**

XCom is simpler. Output data to **stdout** with a special prefix:

```python
@task()
def upstream():
    """Produce data."""
    count = 12345
    print(f"CONDUIT::XCOM::user_count::{count}")
    return count

@task()
def downstream(count=upstream):
    """Consume data."""
    print(f"User count: {count}")
```

Or use **function return values** (cleaner):

```python
@task()
def upstream():
    return 12345

@task()
def downstream(count=upstream):
    print(f"User count: {count}")
```

**XCom protocol**: `CONDUIT::XCOM::key::value` printed to stdout. Conduit's executor parses this and injects it into downstream tasks. No database involved. Simpler, faster, less brittle.

### h. Connections → YAML config

**Airflow:**
```python
from airflow.models import Connection
from airflow.settings import Session

# Define in UI or via CLI:
# airflow connections add snowflake_conn --conn-type snowflake \
#   --conn-host acme.us-east-1.snowflakecomputing.com \
#   --conn-login user@domain.com \
#   --conn-password '...' \
#   --conn-extra '{"database": "prod"}'

from airflow.hooks.snowflake_hook import SnowflakeHook

def load():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    hook.run('INSERT INTO analytics ...')
```

**Conduit:**

Define connections in YAML (checked into version control, or load from env):

```yaml
# conduit/connections.yaml
snowflake:
  type: snowflake
  host: acme.us-east-1.snowflakecomputing.com
  user: user@domain.com
  password: ${SNOWFLAKE_PASSWORD}  # Sourced from env
  database: prod
  warehouse: compute_wh
```

Then reference in tasks:

```yaml
tasks:
  load:
    type: sql
    connection: snowflake
    query: "INSERT INTO analytics ..."
```

Or in Python:

```python
from conduit_sdk import connections

@task()
def load():
    conn = connections.get('snowflake')
    # Use conn.host, conn.user, etc.
    import snowflake.connector
    db = snowflake.connector.connect(
        user=conn.user,
        password=conn.password,
        account=conn.host,
        database=conn.database,
    )
    db.cursor().execute('INSERT INTO analytics ...')
```

**Best practice**: Store secrets in environment variables or a secret manager, reference in YAML via `${VAR_NAME}` interpolation. Simpler audit trail, less database state.

---

## Running Both Side by Side

You don't have to flip a switch on day one. Airflow and Conduit can coexist:

1. **Set up Conduit** in a separate directory (`./conduit_dags`).
2. **Migrate one DAG** (preferably the simplest one—daily batch job, no custom operators).
3. **Validate locally** with `conduit compile` and `conduit run`.
4. **Compare outputs** with Airflow's manual run (or dry-run).
5. **Deploy to staging** in a separate Conduit cluster.
6. **Monitor for 1-2 weeks**. Side-by-side execution, different alerting channels.
7. **Migrate the next DAG**.

### Step-by-Step

```bash
# 1. Migrate your entire Airflow DAG folder (generates templates)
conduit migrate ./airflow_dags --output ./conduit_dags --dry-run

# 2. Manually review and edit the generated YAML/Python files
# (Fix task commands, dependencies, connections, etc.)
ls ./conduit_dags/

# 3. Compile: parse all DAGs, check for errors
conduit compile --dags ./conduit_dags

# 4. Plan: see what will be scheduled
conduit plan --dags ./conduit_dags

# 5. Test a single DAG locally
conduit run daily_etl --dags ./conduit_dags

# 6. Deploy to Conduit (not Airflow)
conduit apply --dags ./conduit_dags --output-dir /etc/conduit/dags

# 7. Create a virtual environment for staging (isolated from prod)
conduit env create staging --dags ./conduit_dags

# 8. Test in staging
conduit run daily_etl --env staging

# 9. Promote to production
conduit env promote staging production
```

This **gradual migration** reduces risk and lets you gain confidence in Conduit's behavior before betting the farm.

---

## What Won't Migrate

Be honest with yourself about these limitations:

### Airflow Plugins & Custom Operators

Airflow lets you write custom `Operator` subclasses:

```python
class MyCustomOperator(BaseOperator):
    def execute(self, context):
        # Custom logic
        pass
```

**Conduit doesn't support plugins.** Task types are fixed: Python, Bash, SQL, Sensor, Executable. If you have custom operators, you'll need to:

- Rewrite them as **standalone Python scripts or executables**.
- Invoke them via `shell` or `executable` task types.
- Or wait for a plugin system (roadmap, not committed).

### Jinja Templating in SQL/Bash

Airflow renders Jinja:

```python
BashOperator(
    bash_command='python script.py {{ ds }} {{ ti.xcom_pull(...) }}'
)
```

**Conduit doesn't template command strings.** Instead:

- Use **stored procedures or database functions** for SQL (e.g., `CURRENT_DATE()`).
- Pass **environment variables** for bash scripts.
- Use **function arguments** in Python tasks.

### Dynamic DAG Factories

As discussed above: no runtime DAG generation. Use script to pre-generate files.

### Airflow REST API Integrations

If you have scripts that call `airflow dags list`, `airflow tasks test`, etc., those won't work with Conduit.

**Conduit's API** is different (and simpler). We publish a REST API for scheduling/triggering, but it's not feature-for-feature compatible with Airflow's.

### Custom Timetables

Airflow 2.2+ supports custom timetables (e.g., UTC-only, business days, fiscal calendars).

**Conduit uses standard cron expressions.** If you need non-standard scheduling, use a sensor or trigger rules.

### SLA & Alert Configurations

Not yet. We have `on_failure` webhooks, but no SLA definitions (time-based alerts).

---

## Validating the Migration

Once you've migrated a DAG, validate it before committing:

### 1. Compile

```bash
conduit compile --dags ./conduit_dags
```

This parses all Python/YAML files using tree-sitter, catches syntax errors, and detects cycles. If successful, you get:

```
✓ Parsed 3 DAGs (12 tasks total)
✓ No cycles detected
✓ All dependencies resolved
```

If it fails, you'll see:

```
✗ Parse error in ./conduit_dags/bad_dag.py (line 42)
  Expected task decorator, found unsupported syntax
```

### 2. Plan

```bash
conduit plan --dags ./conduit_dags
```

Shows what tasks will be scheduled, in what order, with what parameters:

```
DAG: daily_etl
Schedule: 0 0 * * * (daily at midnight UTC)
Max active runs: 1

Execution order:
  1. extract (shell) [no deps]
  2. transform (python) [depends on: extract]
  3. load (bash) [depends on: transform]

Resources:
  Pool: warehouse_pool (5 slots)
    - extract (slot usage: 1)
    - load (slot usage: 2)
```

### 3. Test a Single DAG

```bash
conduit run daily_etl --dags ./conduit_dags
```

Executes the DAG once, locally. You'll see:

```
[INFO] Starting DAG run: daily_etl (run_id: manual_2024-01-15T10:23:45Z)
[INFO] Task: extract
  Command: python /opt/scripts/extract.py
  Status: success (2.3s)
  XCom output:
    user_count: 12345

[INFO] Task: transform
  Inputs:
    user_count (from extract): 12345
  Command: python /opt/scripts/transform.py
  Status: success (5.1s)

[INFO] Task: load
  Command: python /opt/scripts/load.py
  Status: success (3.8s)

[SUCCESS] DAG run completed in 11.2s
```

If a task fails, you see the error immediately:

```
[ERROR] Task: load
  Status: failed
  Exit code: 1
  Stdout: ...
  Stderr: ERROR: connection timeout to warehouse
```

### 4. Compare with Airflow

If you're migrating an existing DAG, run the same tasks in Airflow and Conduit side-by-side:

```bash
# Airflow
airflow dags test daily_etl 2024-01-15

# Conduit
conduit run daily_etl --dags ./conduit_dags
```

Compare outputs (XCom values, logs, execution times). If they match, you're good.

### 5. Dry-Run Deployment

```bash
conduit apply --dags ./conduit_dags --dry-run
```

Shows what will change in the Conduit scheduler without actually applying:

```
DAGs to add:
  + daily_etl
  + weekly_report
  + monthly_close

Tasks to update:
  ~ daily_etl.transform (changed timeout from 20m to 30m)

Tasks to remove:
  - legacy_dag.old_task

Apply these changes? (y/n)
```

Then apply for real:

```bash
conduit apply --dags ./conduit_dags
```

---

## Troubleshooting Common Issues

### Issue: "Parse error in dag.py — tree-sitter failed"

**Cause**: Syntax error in your Python file (unclosed bracket, bad decorator, etc.).

**Fix**: Run `python -m py_compile dag.py` to find the exact line. Conduit's error message will point to the general area; Python's compiler pinpoints the issue.

### Issue: "Task 'transform' has missing dependency 'extract'"

**Cause**: You referenced a task that doesn't exist or typo'd the function name.

**Fix**: Double-check function names and argument names. Remember, in Conduit:
```python
@task()
def transform(data=extract):  # 'extract' is a function reference
    pass
```

If `extract` is not a task function in the same DAG, this fails.

### Issue: "Circular dependency detected: extract → transform → extract"

**Cause**: Your task graph has a cycle (impossible to schedule).

**Fix**: Draw the dependency graph on paper. Ensure tasks form a DAG, not a cycle.

### Issue: "SQL task failed: Unknown connection 'snowflake'"

**Cause**: Connection not defined in `connections.yaml` or not loaded.

**Fix**: Verify:
1. `connections.yaml` exists and contains the connection.
2. Environment variables are set (if using `${VAR}` interpolation).
3. File path is correct.

### Issue: Task output isn't being captured in downstream task

**Cause**: Upstream task didn't write to stdout or used wrong XCom protocol.

**Fix**: Ensure:
1. Python task returns a value or prints `CONDUIT::XCOM::key::value`.
2. Downstream task receives it as a function argument: `def process(data=upstream):`.
3. Upstream task completed successfully (no errors).

---

## Migration Checklist

Use this checklist for each migrated DAG:

- [ ] DAG ID and schedule correct
- [ ] All tasks present (count matches Airflow)
- [ ] Task dependencies accurate (no missing/extra edges)
- [ ] Retries and timeouts set
- [ ] Pool assignments correct
- [ ] Connections defined
- [ ] Secrets sourced from env vars, not hardcoded
- [ ] `conduit compile` passes with no errors
- [ ] `conduit plan` shows expected task order
- [ ] `conduit run` executes successfully (test once)
- [ ] XCom outputs match Airflow (if applicable)
- [ ] Logs are clear and traceable
- [ ] Team reviews DAG before deploying to prod
- [ ] Deployed to staging for 1-2 weeks before prod
- [ ] Monitoring and alerts configured
- [ ] Airflow DAG decommissioned (after success)

---

## Next Steps

1. **Start small**: Migrate a simple, low-risk DAG (no custom operators, no complex branching).
2. **Get feedback**: Have a team member review the migrated DAG definition.
3. **Deploy to staging**: Run in a staging Conduit cluster for 1-2 weeks.
4. **Monitor closely**: Watch logs, performance, and alerting.
5. **Iterate**: Update your migration scripts/templates based on what you learn.
6. **Scale**: Once confident, migrate more DAGs in batches.
7. **Decommission Airflow**: Once all critical DAGs are in Conduit, turn off Airflow.

**You invested years in Airflow. That knowledge doesn't disappear.** Conduit is the evolution—simpler, faster, less infrastructure. The migration is a sprint, not a marathon.

Welcome aboard.
