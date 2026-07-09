# conduit-sdk

The Python SDK for [Conduit](https://github.com/jayhere1/conduit), the Rust-native
data pipeline orchestrator. Zero runtime dependencies — everything is stdlib.

The SDK is how you *author* DAGs. Conduit's compiler parses these files with
tree-sitter (no Python execution), and the executor imports this package at run
time to execute your task functions.

## Install

```bash
pip install conduit-sdk
```

Projects scaffolded with `conduit init` also work without pip: the CLI vendors a
copy of this package into `.conduit/sdk/` and the executor discovers it
automatically (override with the `CONDUIT_SDK_PATH` environment variable).

## Define a DAG

```python
from conduit_sdk import dag, task

@dag(schedule="0 6 * * *", tags=["analytics"])
def daily_metrics():

    @task
    def extract():
        return {"rows": 42}

    @task
    def load(payload):
        print(f"loaded {payload['rows']} rows")

    load(extract())
```

## Operators and sensors

Airflow-shaped operators, stdlib-only:

```python
from conduit_sdk import BashOperator, SQLOperator, FileSensor

BashOperator(task_id="cleanup", bash_command="rm -f /tmp/staging-{{ ds }}.csv")
SQLOperator(task_id="rollup", sql="INSERT INTO daily ...", connection_id="warehouse")
FileSensor(task_id="wait_for_export", path="/data/export.done", poke_interval="10s")
```

Also available: `PythonOperator`, `HttpSensor`, `SqlSensor`,
`SlackNotifyOperator`, `EmailOperator`.

## Hooks and connections

Connections come from `CONDUIT_CONN_<ID>` environment variables (JSON or URI):

```python
from conduit_sdk import DatabaseHook

rows = DatabaseHook("warehouse").run("SELECT count(*) FROM users")
```

## Data contracts and lineage

```python
from conduit_sdk import task, contract, check, Dataset, ColumnSpec, emit_row_count

@task(
    outputs=[Dataset("staging.orders", columns=[ColumnSpec("order_id", "int64")])],
)
@contract(check.row_count(min=1), check.unique(["order_id"]))
def build_orders():
    emit_row_count(1042)
```

Declared inputs/outputs feed Conduit's cross-task column lineage
(`conduit lineage trace`), and contract evidence is validated by the scheduler.

## XCom and context

```python
from conduit_sdk import get_context, xcom_push, xcom_pull

ctx = get_context()          # run_id, ds, task_id, ...
xcom_push("key", {"a": 1})   # emitted via the Conduit stdout protocol
```

## License

Apache-2.0. Part of the [Conduit](https://github.com/jayhere1/conduit) project.
