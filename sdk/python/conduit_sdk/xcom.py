"""
XCom (cross-communication) — passing data between tasks.

In Conduit, tasks communicate via stdout protocol messages.
When a task writes to stdout:
    CONDUIT::XCOM::key=value

The executor captures this and makes it available to downstream tasks.

This module provides Python helpers that emit the correct protocol messages.
"""

from __future__ import annotations

import json
import sys
from typing import Any


def xcom_push(key: str, value: Any) -> None:
    """
    Push a value to XCom for downstream tasks to consume.

    The value is serialized to JSON and emitted as a stdout protocol message.
    The executor captures these messages and routes them to dependent tasks.

    Args:
        key: A unique key for this value (within the current task).
        value: Any JSON-serializable value.

    Example:
        @task()
        def extract_orders():
            orders = db.query("SELECT * FROM orders")
            xcom_push("row_count", len(orders))
            xcom_push("schema", orders.columns.tolist())
            return orders
    """
    serialized = json.dumps(value)
    # Emit the protocol message on stdout
    print(f"CONDUIT::XCOM::{key}={serialized}", flush=True)


def xcom_pull(task_id: str, key: str = "return_value") -> Any:
    """
    Pull a value from a completed upstream task's XCom.

    In Conduit's compiled execution model, xcom_pull is resolved at
    runtime by the executor, which injects upstream values via stdin.

    For local testing, this reads from an in-memory store.

    Args:
        task_id: The upstream task ID to pull from.
        key: The XCom key (default: "return_value").

    Returns:
        The deserialized value.
    """
    # In production, the executor injects XCom values via stdin JSON.
    # For local testing, check the local store.
    return _local_xcom_store.get((task_id, key))


# ── Protocol message helpers ──────────────────────────────────

def log(message: str, level: str = "INFO") -> None:
    """
    Emit a structured log message via the Conduit protocol.

    Args:
        message: The log message.
        level: Log level (DEBUG, INFO, WARNING, ERROR).

    Example:
        from conduit_sdk.xcom import log
        log("Processing 1,000,000 rows", level="INFO")
    """
    print(f"CONDUIT::LOG::{level}::{message}", flush=True)


def progress(current: int, total: int) -> None:
    """
    Report task progress via the Conduit protocol.

    The API and WebSocket subscribers will receive real-time
    progress updates for this task.

    Args:
        current: Current progress value.
        total: Total expected value.

    Example:
        for i, batch in enumerate(batches):
            process(batch)
            progress(i + 1, len(batches))
    """
    print(f"CONDUIT::PROGRESS::{current}/{total}", flush=True)


def metric(name: str, value: float, unit: str = "") -> None:
    """
    Report a custom metric via the Conduit protocol.

    Metrics are captured by the executor and stored with the
    task's snapshot for historical analysis.

    Args:
        name: Metric name (e.g., "rows_processed", "latency_ms").
        value: Numeric value.
        unit: Optional unit label (e.g., "ms", "rows", "MB").

    Example:
        metric("rows_processed", 1_500_000, "rows")
        metric("processing_time", 45.2, "seconds")
    """
    unit_suffix = f"::{unit}" if unit else ""
    print(f"CONDUIT::METRIC::{name}={value}{unit_suffix}", flush=True)


# ── Local testing support ─────────────────────────────────────

_local_xcom_store: dict[tuple[str, str], Any] = {}


def _local_xcom_set(task_id: str, key: str, value: Any) -> None:
    """Set a local XCom value (for testing only)."""
    _local_xcom_store[(task_id, key)] = value


def _local_xcom_clear() -> None:
    """Clear the local XCom store (for testing only)."""
    _local_xcom_store.clear()
