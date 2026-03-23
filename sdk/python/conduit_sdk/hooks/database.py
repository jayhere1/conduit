"""
Database hook — wraps DB-API 2.0 connections for PostgreSQL, MySQL, and SQLite.

Usage:
    from conduit_sdk.hooks.database import DatabaseHook

    # With a configured connection (env: CONDUIT_CONN_MY_PG)
    hook = DatabaseHook("my_pg")
    rows = hook.run("SELECT id, name FROM users WHERE active = %s", parameters=(True,))

    # With SQLite (no external dependencies)
    import os
    os.environ["CONDUIT_CONN_LOCAL_DB"] = "sqlite:///my.db"
    hook = DatabaseHook("local_db")
    hook.run("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val TEXT)")

    # Get a pandas DataFrame (requires pandas)
    df = hook.get_pandas_df("SELECT * FROM users")
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple

from conduit_sdk.hooks.base import BaseHook, Connection


class DatabaseHook(BaseHook):
    """Hook for relational databases via DB-API 2.0.

    Supports PostgreSQL (psycopg2), MySQL (pymysql), and SQLite (builtin).
    Falls back gracefully if optional drivers are not installed.

    Args:
        connection_id: The connection identifier to resolve from environment.

    Example:
        hook = DatabaseHook("warehouse")
        rows = hook.run("SELECT count(*) FROM orders")
        print(f"Order count: {rows[0][0]}")
    """

    def get_conn(self) -> Any:
        """Return a DB-API 2.0 connection object.

        The connection type is determined by the ``conn_type`` field of the
        resolved :class:`Connection`.

        Returns:
            A DB-API 2.0 compliant connection object.

        Raises:
            ImportError: If the required driver package is not installed.
            ValueError: If the connection type is unsupported.
        """
        conn = self.get_connection()
        return self._create_dbapi_connection(conn)

    def run(self, sql: str, parameters: Optional[Any] = None) -> List[Tuple]:
        """Execute a SQL query and return all result rows.

        Args:
            sql: SQL query string. Use ``%s`` placeholders for parameters.
            parameters: Query parameters (tuple, list, or dict depending on driver).

        Returns:
            A list of tuples representing the result rows.
            Returns an empty list for non-SELECT statements (INSERT, UPDATE, etc.).

        Example:
            rows = hook.run(
                "SELECT * FROM orders WHERE status = %s AND total > %s",
                parameters=("shipped", 100),
            )
        """
        db_conn = self.get_conn()
        try:
            cursor = db_conn.cursor()
            try:
                if parameters is not None:
                    cursor.execute(sql, parameters)
                else:
                    cursor.execute(sql)

                # Check if this is a query that returns rows
                if cursor.description is not None:
                    return cursor.fetchall()

                db_conn.commit()
                return []
            finally:
                cursor.close()
        finally:
            db_conn.close()

    def get_pandas_df(self, sql: str, parameters: Optional[Any] = None) -> Any:
        """Execute a SQL query and return the result as a pandas DataFrame.

        Requires ``pandas`` to be installed.

        Args:
            sql: SQL query string.
            parameters: Query parameters.

        Returns:
            A ``pandas.DataFrame`` with the query results.

        Raises:
            ImportError: If pandas is not installed.

        Example:
            df = hook.get_pandas_df("SELECT * FROM users LIMIT 100")
            print(df.describe())
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for get_pandas_df(). "
                "Install it with: pip install pandas"
            )

        db_conn = self.get_conn()
        try:
            return pd.read_sql(sql, db_conn, params=parameters)
        finally:
            db_conn.close()

    # ── Internal helpers ──────────────────────────────────────────

    @staticmethod
    def _create_dbapi_connection(conn: Connection) -> Any:
        """Create a DB-API connection from a Connection dataclass."""
        conn_type = conn.conn_type.lower()

        if conn_type == "sqlite":
            import sqlite3

            db_path = conn.host or conn.schema or ":memory:"
            return sqlite3.connect(db_path)

        if conn_type == "postgres":
            try:
                import psycopg2
            except ImportError:
                raise ImportError(
                    "psycopg2 is required for PostgreSQL connections. "
                    "Install it with: pip install psycopg2-binary"
                )
            kwargs: dict[str, Any] = {
                "host": conn.host,
                "port": conn.port or 5432,
                "dbname": conn.schema,
                "user": conn.login,
                "password": conn.password,
            }
            kwargs.update(conn.extra)
            return psycopg2.connect(**kwargs)

        if conn_type == "mysql":
            try:
                import pymysql
            except ImportError:
                raise ImportError(
                    "pymysql is required for MySQL connections. "
                    "Install it with: pip install pymysql"
                )
            kwargs = {
                "host": conn.host,
                "port": conn.port or 3306,
                "database": conn.schema,
                "user": conn.login,
                "password": conn.password,
            }
            kwargs.update(conn.extra)
            return pymysql.connect(**kwargs)

        raise ValueError(
            f"Unsupported database connection type: '{conn_type}'. "
            f"Supported types: postgres, mysql, sqlite."
        )
