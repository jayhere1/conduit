"""
Sensor operators — poll until a condition is met.

Sensors repeatedly call :meth:`poke` at a configured interval until it
returns ``True`` or the timeout is exceeded.

Usage:
    from conduit_sdk.operators import FileSensor, HttpSensor, SqlSensor

    # Wait for a file to appear
    sensor = FileSensor(
        task_id="wait_for_export",
        path="/data/exports/daily.csv",
        poke_interval="30s",
        timeout="2h",
    )
    sensor.run()

    # Wait for an API endpoint to return 200
    sensor = HttpSensor(
        task_id="wait_for_api",
        url="https://api.example.com/health",
        poke_interval="10s",
        timeout="5m",
    )
    sensor.run()

    # Wait for a SQL query to return rows
    sensor = SqlSensor(
        task_id="wait_for_data",
        sql="SELECT 1 FROM orders WHERE date = '2026-03-23' LIMIT 1",
        connection_id="warehouse",
        poke_interval="1m",
        timeout="6h",
    )
    sensor.run()
"""

from __future__ import annotations

import os
import time
from typing import Any, Callable, Optional

from conduit_sdk.context import TaskContext
from conduit_sdk.xcom import log, progress
from conduit_sdk.operators.base import BaseOperator, _parse_duration


class Sensor(BaseOperator):
    """Base sensor that polls until a condition is met.

    Subclasses override :meth:`poke` to implement the actual check.

    Modes:
    - ``"poke"`` (default): sleeps between pokes in the same process.
    - ``"reschedule"``: hints to the executor to free the worker slot
      between pokes (only meaningful when running under Conduit).

    Args:
        task_id: Unique identifier for this task.
        poke_interval: How often to check (e.g., "30s", "5m"). Default "30s".
        timeout: Maximum total wait time (e.g., "6h"). Default "6h".
        mode: "poke" or "reschedule". Default "poke".
        **task_kwargs: Standard task kwargs (retries, retry_delay, pool, etc.).

    Example:
        class MyCustomSensor(Sensor):
            def __init__(self, task_id, check_value, **kwargs):
                super().__init__(task_id=task_id, **kwargs)
                self.check_value = check_value

            def poke(self, context):
                return some_external_check(self.check_value)
    """

    def __init__(
        self,
        task_id: str,
        poke_interval: str = "30s",
        timeout: str = "6h",
        mode: str = "poke",
        **task_kwargs: Any,
    ):
        # Use the sensor's own timeout, not the base operator's
        super().__init__(task_id=task_id, **task_kwargs)
        self.poke_interval = poke_interval
        self.sensor_timeout = timeout
        self.mode = mode

    def poke(self, context: Optional[TaskContext] = None) -> bool:
        """Check the condition.

        Override this in subclasses.

        Args:
            context: The task execution context.

        Returns:
            True if the condition is met, False otherwise.
        """
        raise NotImplementedError("Subclasses must implement poke()")

    def execute(self, context: Optional[TaskContext] = None) -> bool:
        """Poll :meth:`poke` until it returns True or timeout is reached.

        Args:
            context: The task execution context.

        Returns:
            True when the condition is met.

        Raises:
            TimeoutError: If the condition is not met before the timeout.
        """
        interval_seconds = _parse_duration(self.poke_interval)
        timeout_seconds = _parse_duration(self.sensor_timeout)

        start = time.monotonic()
        poke_count = 0

        while True:
            poke_count += 1
            elapsed = time.monotonic() - start

            if elapsed > timeout_seconds:
                log(
                    f"Sensor '{self.task_id}' timed out after {elapsed:.0f}s "
                    f"({poke_count} pokes)",
                    level="ERROR",
                )
                raise TimeoutError(
                    f"Sensor '{self.task_id}' timed out after "
                    f"{self.sensor_timeout} ({poke_count} pokes)"
                )

            log(
                f"Sensor '{self.task_id}' poke #{poke_count} "
                f"(elapsed: {elapsed:.0f}s/{timeout_seconds:.0f}s)",
                level="DEBUG",
            )

            try:
                result = self.poke(context)
            except Exception as exc:
                log(f"Sensor poke failed: {exc}", level="WARNING")
                result = False

            if result:
                log(
                    f"Sensor '{self.task_id}' succeeded after "
                    f"{poke_count} pokes ({elapsed:.0f}s)",
                    level="INFO",
                )
                return True

            time.sleep(interval_seconds)


class FileSensor(Sensor):
    """Sensor that waits for a file to exist on the filesystem.

    Args:
        task_id: Unique identifier for this task.
        path: The file path to watch for.
        **kwargs: Sensor and task kwargs (poke_interval, timeout, retries, etc.).

    Example:
        sensor = FileSensor(
            task_id="wait_for_data",
            path="/data/incoming/report.csv",
            poke_interval="10s",
            timeout="1h",
        )
        sensor.run()
    """

    def __init__(self, task_id: str, path: str, **kwargs: Any):
        super().__init__(task_id=task_id, **kwargs)
        self.path = path

    def poke(self, context: Optional[TaskContext] = None) -> bool:
        """Check if the file exists.

        Returns:
            True if the file exists at the configured path.
        """
        exists = os.path.exists(self.path)
        if not exists:
            log(f"File not found: {self.path}", level="DEBUG")
        return exists


class HttpSensor(Sensor):
    """Sensor that polls an HTTP endpoint until it returns a successful response.

    By default, a 2xx status code is considered success. Provide a custom
    ``response_check`` callable for more complex logic.

    Args:
        task_id: Unique identifier for this task.
        url: The URL to poll.
        method: HTTP method (default "GET").
        response_check: Optional callable that receives the response and returns
            True/False. If not provided, any 2xx status is considered success.
        headers: Additional HTTP headers.
        **kwargs: Sensor and task kwargs.

    Example:
        sensor = HttpSensor(
            task_id="wait_for_api",
            url="https://api.example.com/status",
            response_check=lambda r: r.json().get("ready") is True,
            poke_interval="15s",
            timeout="10m",
        )
    """

    def __init__(
        self,
        task_id: str,
        url: str,
        method: str = "GET",
        response_check: Optional[Callable] = None,
        headers: Optional[dict] = None,
        **kwargs: Any,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.url = url
        self.method = method
        self.response_check = response_check
        self.headers = headers or {}

    def poke(self, context: Optional[TaskContext] = None) -> bool:
        """Poll the HTTP endpoint.

        Uses the ``requests`` library if available, falls back to ``urllib``.

        Returns:
            True if the response passes the check.
        """
        try:
            response = self._make_request()
        except Exception as exc:
            log(f"HTTP request failed: {exc}", level="DEBUG")
            return False

        if self.response_check is not None:
            try:
                return bool(self.response_check(response))
            except Exception as exc:
                log(f"Response check failed: {exc}", level="DEBUG")
                return False

        # Default: check for 2xx status
        status = getattr(response, "status_code", getattr(response, "status", 0))
        return 200 <= status < 300

    def _make_request(self) -> Any:
        """Make the HTTP request using requests or urllib."""
        try:
            import requests
            return requests.request(
                self.method, self.url, headers=self.headers, timeout=30
            )
        except ImportError:
            from urllib.request import Request, urlopen

            req = Request(self.url, headers=self.headers, method=self.method)
            return urlopen(req, timeout=30)


class SqlSensor(Sensor):
    """Sensor that polls a SQL query until it returns rows.

    The query should return at least one row when the condition is met.
    An empty result set means the condition is not yet satisfied.

    Args:
        task_id: Unique identifier for this task.
        sql: The SQL query to execute.
        connection_id: The connection identifier for the database.
        parameters: Query parameters.
        **kwargs: Sensor and task kwargs.

    Example:
        sensor = SqlSensor(
            task_id="wait_for_partition",
            sql="SELECT 1 FROM partitions WHERE date = '2026-03-23'",
            connection_id="warehouse",
            poke_interval="5m",
            timeout="12h",
        )
    """

    def __init__(
        self,
        task_id: str,
        sql: str,
        connection_id: str,
        parameters: Optional[Any] = None,
        **kwargs: Any,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.sql = sql
        self.connection_id = connection_id
        self.parameters = parameters

    def poke(self, context: Optional[TaskContext] = None) -> bool:
        """Execute the SQL query and check if rows were returned.

        Returns:
            True if the query returns at least one row.
        """
        from conduit_sdk.hooks.database import DatabaseHook

        try:
            hook = DatabaseHook(self.connection_id)
            rows = hook.run(self.sql, parameters=self.parameters)
            has_rows = len(rows) > 0
            if has_rows:
                log(f"SQL sensor got {len(rows)} rows", level="DEBUG")
            return has_rows
        except Exception as exc:
            log(f"SQL sensor query failed: {exc}", level="WARNING")
            return False
