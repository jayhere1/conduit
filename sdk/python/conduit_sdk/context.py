"""
Task execution context — runtime information available to tasks.

When Conduit's executor runs a task, it provides context via environment
variables (including upstream XCom as JSON in CONDUIT_XCOM_JSON). This
module reads that context and exposes it as a clean Python API.

Environment variables set by the executor:
    CONDUIT_DAG_ID        - The current DAG ID
    CONDUIT_TASK_ID       - The current task ID
    CONDUIT_RUN_ID        - The current run ID
    CONDUIT_ATTEMPT       - Current retry attempt (1-based)
    CONDUIT_LOGICAL_DATE  - The logical execution date (ISO 8601)
    CONDUIT_ENVIRONMENT   - The target environment name
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass
class TaskContext:
    """
    Runtime context for a Conduit task.

    Available during task execution, provides access to:
    - Task identity (dag_id, task_id, run_id)
    - Execution metadata (attempt number, logical date)
    - Environment information
    - Upstream XCom values (injected by the executor)
    """
    dag_id: str
    task_id: str
    run_id: str
    attempt: int
    logical_date: Optional[datetime]
    environment: str
    upstream_xcom: dict[str, Any]

    def get_upstream(self, task_id: str, key: str = "return_value") -> Any:
        """
        Get an XCom value from an upstream task.

        Args:
            task_id: The upstream task ID.
            key: The XCom key (default: "return_value").

        Returns:
            The value, or None if not available.
        """
        xcom_key = f"{task_id}.{key}"
        return self.upstream_xcom.get(xcom_key)

    @property
    def is_retry(self) -> bool:
        """True if this is a retry attempt (attempt > 1)."""
        return self.attempt > 1


def get_context() -> TaskContext:
    """
    Get the current task execution context.

    This reads from environment variables set by the Conduit executor.
    For local testing, it returns a dummy context.

    Returns:
        TaskContext with current execution information.

    Example:
        @task()
        def my_task():
            ctx = get_context()
            print(f"Running {ctx.dag_id}.{ctx.task_id} (attempt {ctx.attempt})")

            if ctx.is_retry:
                log("This is a retry — using incremental strategy")
    """
    dag_id = os.environ.get("CONDUIT_DAG_ID", "local_dag")
    task_id = os.environ.get("CONDUIT_TASK_ID", "local_task")
    run_id = os.environ.get("CONDUIT_RUN_ID", "local_run")
    attempt = int(os.environ.get("CONDUIT_ATTEMPT", "1"))
    environment = os.environ.get("CONDUIT_ENVIRONMENT", "development")

    logical_date_str = os.environ.get("CONDUIT_LOGICAL_DATE")
    logical_date = None
    if logical_date_str:
        try:
            logical_date = datetime.fromisoformat(logical_date_str)
        except ValueError:
            pass

    # Read upstream XCom values injected by the executor via environment
    upstream_xcom = _read_upstream_xcom()

    return TaskContext(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        attempt=attempt,
        logical_date=logical_date,
        environment=environment,
        upstream_xcom=upstream_xcom,
    )


def _read_upstream_xcom() -> dict[str, Any]:
    """
    Read upstream XCom values from the CONDUIT_XCOM_JSON environment
    variable, which the executor sets to a JSON object before the task
    starts: {"extract_orders.return_value": [...], "extract_orders.row_count": 1000}
    """
    xcom_json = os.environ.get("CONDUIT_XCOM_JSON")
    if xcom_json:
        try:
            return json.loads(xcom_json)
        except json.JSONDecodeError:
            return {}
    return {}
