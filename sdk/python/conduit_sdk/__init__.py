"""
Conduit Python SDK — define data pipelines that Conduit compiles without executing.

Usage:
    from conduit_sdk import dag, task

    @dag(schedule="0 6 * * *", tags=["etl", "warehouse"])
    def daily_etl():
        '''Daily ETL pipeline for the data warehouse.'''

        @task(retries=3, pool="extract_pool", timeout="30m")
        def extract_orders():
            '''Extract orders from source database.'''
            ...

        @task(retries=2)
        def extract_customers():
            '''Extract customer data.'''
            ...

        @task(pool="transform_pool", timeout="1h")
        def transform(raw_orders=extract_orders, raw_customers=extract_customers):
            '''Join and transform extracted data.'''
            ...

        @task(timeout="45m")
        def load(data=transform):
            '''Load transformed data into the warehouse.'''
            ...

Note:
    Conduit's compiler (tree-sitter) parses this file WITHOUT executing Python.
    The decorators exist for:
    1. IDE support (autocomplete, type checking)
    2. Local development and testing
    3. Documentation

    In production, Conduit reads the AST directly — your function bodies
    never run during compilation. This is what makes Conduit safe and fast.
"""

__version__ = "0.1.0"

from conduit_sdk.decorators import dag, task
from conduit_sdk.xcom import xcom_push, xcom_pull
from conduit_sdk.context import get_context, TaskContext
from conduit_sdk.incremental import get_incremental_context, emit_watermark, IncrementalContext
from conduit_sdk.backfill import get_backfill_context, BackfillContext
from conduit_sdk.contracts import (
    contract, check, Contracts,
    emit_metric, emit_evidence, emit_row_count,
    emit_freshness, emit_freshness_seconds,
    emit_duplicate_count, emit_null_rate, emit_custom,
)

# Operators
from conduit_sdk.operators import (
    PythonOperator,
    BashOperator,
    SQLOperator,
    Sensor,
    FileSensor,
    HttpSensor,
    SqlSensor,
    SlackNotifyOperator,
    EmailOperator,
)

# Hooks
from conduit_sdk.hooks import (
    BaseHook,
    Connection,
    DatabaseHook,
    HttpHook,
    FileSystemHook,
)

__all__ = [
    "dag",
    "task",
    "xcom_push",
    "xcom_pull",
    "get_context",
    "TaskContext",
    "get_incremental_context",
    "emit_watermark",
    "IncrementalContext",
    "get_backfill_context",
    "BackfillContext",
    "contract",
    "check",
    "Contracts",
    # Evidence emission
    "emit_metric",
    "emit_evidence",
    "emit_row_count",
    "emit_freshness",
    "emit_freshness_seconds",
    "emit_duplicate_count",
    "emit_null_rate",
    "emit_custom",
    # Operators
    "PythonOperator",
    "BashOperator",
    "SQLOperator",
    "Sensor",
    "FileSensor",
    "HttpSensor",
    "SqlSensor",
    "SlackNotifyOperator",
    "EmailOperator",
    # Hooks
    "BaseHook",
    "Connection",
    "DatabaseHook",
    "HttpHook",
    "FileSystemHook",
]
