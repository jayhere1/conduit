"""
SQLOperator — executes SQL queries as Conduit tasks.

Uses the connection hooks system to resolve database connections. Supports
parameterised queries and emits result rows as XCom plus a ``row_count``
metric.

Usage:
    import os
    os.environ["CONDUIT_CONN_DW"] = "postgres://etl:secret@db:5432/warehouse"

    from conduit_sdk.operators import SQLOperator

    op = SQLOperator(
        task_id="count_orders",
        sql="SELECT count(*) FROM orders WHERE date = %(ds)s",
        connection_id="dw",
        parameters={"ds": "2026-03-23"},
        retries=2,
    )
    rows = op.run()

    # Multi-statement SQL
    op = SQLOperator(
        task_id="refresh_view",
        sql="REFRESH MATERIALIZED VIEW order_summary;",
        connection_id="dw",
    )
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

from conduit_sdk.context import TaskContext
from conduit_sdk.hooks.database import DatabaseHook
from conduit_sdk.xcom import log, metric
from conduit_sdk.operators.base import BaseOperator


class SQLOperator(BaseOperator):
    """Operator that executes a SQL query against a database connection.

    The connection is resolved via :class:`~conduit_sdk.hooks.database.DatabaseHook`
    from the ``CONDUIT_CONN_{connection_id}`` environment variable.

    The query results (list of tuples) are returned and pushed as XCom.
    A ``row_count`` metric is emitted automatically.

    Args:
        task_id: Unique identifier for this task.
        sql: SQL query string. Use parameter placeholders appropriate for
            your database driver (``%s`` for psycopg2/pymysql, ``?`` for
            sqlite3).
        connection_id: The connection identifier to resolve from environment.
        parameters: Query parameters (tuple, list, or dict).
        **task_kwargs: Standard task kwargs (retries, timeout, pool, etc.).

    Example:
        op = SQLOperator(
            task_id="top_customers",
            sql="SELECT name, total FROM customers ORDER BY total DESC LIMIT 10",
            connection_id="warehouse",
        )
        top_ten = op.run()
    """

    def __init__(
        self,
        task_id: str,
        sql: str,
        connection_id: str,
        parameters: Optional[Any] = None,
        **task_kwargs: Any,
    ):
        super().__init__(task_id=task_id, **task_kwargs)
        self.sql = sql
        self.connection_id = connection_id
        self.parameters = parameters

    def execute(self, context: Optional[TaskContext] = None) -> List[Tuple]:
        """Execute the SQL query.

        Args:
            context: The task execution context.

        Returns:
            A list of tuples representing result rows. Returns an empty
            list for statements that don't produce results.
        """
        log(f"Executing SQL on connection '{self.connection_id}'", level="INFO")
        log(f"SQL: {self.sql[:200]}{'...' if len(self.sql) > 200 else ''}", level="DEBUG")

        hook = DatabaseHook(self.connection_id)
        rows = hook.run(self.sql, parameters=self.parameters)

        row_count = len(rows)
        metric("row_count", row_count, "rows")
        log(f"Query returned {row_count} rows", level="INFO")

        return rows
