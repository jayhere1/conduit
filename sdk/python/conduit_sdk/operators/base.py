"""
Base operator — shared functionality for all Conduit operators.

All operators accept the same kwargs as ``@task`` (retries, retry_delay, pool,
timeout, priority) and emit Conduit protocol messages.
"""

from __future__ import annotations

import re
import sys
import time
from typing import Any, Dict, Optional

from conduit_sdk.context import get_context, TaskContext
from conduit_sdk.xcom import xcom_push, log, metric


def _parse_duration(value: str) -> float:
    """Parse a human-readable duration string into seconds.

    Supported formats: "30s", "5m", "1h", "2h30m", "1d", "6h".

    Args:
        value: Duration string.

    Returns:
        Total seconds as a float.

    Raises:
        ValueError: If the format is not recognised.
    """
    if not value:
        return 0.0

    # Try pure numeric (already seconds)
    try:
        return float(value)
    except ValueError:
        pass

    total = 0.0
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    parts = re.findall(r"(\d+(?:\.\d+)?)\s*([smhdw])", value.lower())
    if not parts:
        raise ValueError(f"Cannot parse duration: '{value}'")

    for num_str, unit in parts:
        total += float(num_str) * units[unit]

    return total


class BaseOperator:
    """Base class for all Conduit operators.

    Provides common functionality: task metadata, retry logic, timeout
    handling, context management, and protocol message emission.

    Args:
        task_id: Unique identifier for this task within the DAG.
        retries: Number of retry attempts on failure (default 0).
        retry_delay: Delay between retries (e.g., "30s", "5m").
        pool: Named resource pool for concurrency control.
        timeout: Maximum execution time (e.g., "1h", "30m").
        priority: Execution priority (higher = runs first).
        trigger_rule: When to run relative to upstreams.
        tags: Tags for filtering and organisation.
    """

    def __init__(
        self,
        task_id: str,
        retries: int = 0,
        retry_delay: Optional[str] = None,
        pool: Optional[str] = None,
        timeout: Optional[str] = None,
        priority: int = 0,
        trigger_rule: str = "all_success",
        tags: Optional[list] = None,
        **kwargs: Any,
    ):
        self.task_id = task_id
        self.retries = retries
        self.retry_delay = retry_delay or "30s"
        self.pool = pool
        self.timeout = timeout
        self.priority = priority
        self.trigger_rule = trigger_rule
        self.tags = tags or []
        self._extra_kwargs = kwargs

    def execute(self, context: Optional[TaskContext] = None) -> Any:
        """Execute the operator logic.

        Subclasses must override this method.

        Args:
            context: The task execution context. If None, a default context
                is obtained from environment variables.

        Returns:
            The operator's result value (pushed as XCom ``return_value``).
        """
        raise NotImplementedError("Subclasses must implement execute()")

    def run(self, context: Optional[TaskContext] = None) -> Any:
        """Run the operator with retry and timeout logic.

        This is the main entry point. It wraps :meth:`execute` with
        retry handling and timeout enforcement, and pushes the return
        value as XCom.

        Args:
            context: The task execution context.

        Returns:
            The result from :meth:`execute`.

        Raises:
            Exception: The last exception if all retries are exhausted.

        Example:
            op = BashOperator(task_id="greet", bash_command="echo hello")
            result = op.run()
        """
        if context is None:
            context = get_context()

        timeout_seconds = _parse_duration(self.timeout) if self.timeout else None
        delay_seconds = _parse_duration(self.retry_delay)

        last_exc: Optional[Exception] = None
        max_attempts = self.retries + 1

        for attempt in range(1, max_attempts + 1):
            log(f"[{self.task_id}] attempt {attempt}/{max_attempts}", level="INFO")

            try:
                start = time.monotonic()
                result = self.execute(context)
                elapsed = time.monotonic() - start

                # Check timeout
                if timeout_seconds and elapsed > timeout_seconds:
                    raise TimeoutError(
                        f"Task '{self.task_id}' exceeded timeout of {self.timeout} "
                        f"(ran for {elapsed:.1f}s)"
                    )

                # Push return value as XCom
                if result is not None:
                    xcom_push("return_value", result)

                metric("duration_seconds", elapsed, "seconds")
                log(f"[{self.task_id}] completed in {elapsed:.2f}s", level="INFO")
                return result

            except Exception as exc:
                last_exc = exc
                log(f"[{self.task_id}] attempt {attempt} failed: {exc}", level="ERROR")

                if attempt < max_attempts:
                    log(f"[{self.task_id}] retrying in {self.retry_delay}", level="INFO")
                    time.sleep(delay_seconds)

        raise last_exc  # type: ignore[misc]

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} task_id='{self.task_id}'>"
