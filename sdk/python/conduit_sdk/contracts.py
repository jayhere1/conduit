"""
Evidence-based data quality contracts for Conduit pipelines.

Tasks emit **evidence** — structured measurements via the stdout protocol
(`CONDUIT::METRIC::name::value`). Contracts assert against this evidence.
The executor collects metrics during execution, then validates contracts.

This design is runtime-agnostic: SQL, Python, shell, API tasks all emit
evidence the same way. The contract doesn't know or care how the measurement
was produced.

Usage (decorator style — declare what to assert):
    from conduit_sdk import task
    from conduit_sdk.contracts import contract, check

    @task(retries=3)
    @contract(
        check.row_count(min=1),
        check.freshness(max_age="24h"),
        check.unique(["id"]),
        check.not_null("customer_id"),
        check.metric("accuracy", min=0.95),
    )
    def extract_orders():
        # Task emits evidence via emit_* helpers
        ...

Usage (evidence emission — tell Conduit what you measured):
    from conduit_sdk.contracts import emit_metric, emit_evidence

    def my_task():
        rows = do_work()
        emit_metric("row_count", len(rows))
        emit_metric("data_age_seconds", compute_freshness())
        emit_metric("accuracy", model.score())

Usage (inline builder style):
    from conduit_sdk.contracts import Contracts

    contracts = Contracts("extract_orders")
    contracts.row_count(min=1)
    contracts.freshness(max_age="24h")
    contracts.metric("accuracy", min=0.95)
    contracts.emit()

At runtime, contracts emit their definitions via the stdout protocol
so the Conduit executor can collect and validate them.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Union


# ─── Evidence Emission ──────────────────────────────────────────────────────
# These are the primary API for tasks to communicate measurements to Conduit.

def emit_metric(name: str, value: float) -> None:
    """Emit a single metric as evidence for contract validation.

    This is the primary way tasks communicate measurements to Conduit.
    The executor collects these and validates contracts against them.

    Args:
        name: Metric name (e.g., "row_count", "data_age_seconds", "accuracy")
        value: Numeric measurement

    Example:
        emit_metric("row_count", 5000)
        emit_metric("data_age_seconds", 3600)
        emit_metric("null_rate.email", 0.02)
        emit_metric("accuracy", 0.98)
    """
    print(f"CONDUIT::METRIC::{name}::{value}", flush=True)


def emit_evidence(metrics: Dict[str, float]) -> None:
    """Emit multiple metrics at once.

    Args:
        metrics: Dictionary of metric_name -> value

    Example:
        emit_evidence({
            "row_count": 5000,
            "data_age_seconds": 3600,
            "duplicate_count": 0,
            "null_rate.email": 0.02,
        })
    """
    for name, value in metrics.items():
        emit_metric(name, value)


def emit_row_count(count: int) -> None:
    """Convenience: emit the row_count metric."""
    emit_metric("row_count", count)


def emit_freshness_seconds(age_seconds: float) -> None:
    """Convenience: emit the data_age_seconds metric."""
    emit_metric("data_age_seconds", age_seconds)


def emit_freshness(latest_timestamp: str) -> None:
    """Convenience: compute and emit data_age_seconds from a timestamp.

    Args:
        latest_timestamp: ISO 8601 timestamp of the most recent data point.

    Example:
        emit_freshness("2024-01-15T10:30:00Z")
    """
    from datetime import datetime, timezone

    try:
        ts = datetime.fromisoformat(latest_timestamp.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        raise ValueError(f"Cannot parse timestamp: {latest_timestamp}")

    now = datetime.now(timezone.utc)
    age = (now - ts).total_seconds()
    emit_metric("data_age_seconds", age)


def emit_duplicate_count(count: int) -> None:
    """Convenience: emit the duplicate_count metric."""
    emit_metric("duplicate_count", count)


def emit_null_rate(column: str, rate: float) -> None:
    """Convenience: emit the null_rate.{column} metric.

    Args:
        column: Column name
        rate: Fraction of null values (0.0 to 1.0)
    """
    emit_metric(f"null_rate.{column}", rate)


def emit_custom(assertion_name: str, passed: bool) -> None:
    """Convenience: emit a custom pass/fail assertion.

    Args:
        assertion_name: Name of the assertion
        passed: Whether it passed
    """
    emit_metric(f"pass.{assertion_name}", 1.0 if passed else 0.0)


# ─── Check Builders ─────────────────────────────────────────────────────────

@dataclass
class Check:
    """A single data quality check definition."""
    type: str
    name: Optional[str] = None
    severity: str = "error"
    description: Optional[str] = None
    params: dict = field(default_factory=dict)

    def warning(self) -> "Check":
        """Downgrade this check to a warning (won't block deployment)."""
        self.severity = "warning"
        return self

    def named(self, name: str) -> "Check":
        """Set a custom name for this check."""
        self.name = name
        return self

    def described(self, desc: str) -> "Check":
        """Add a description explaining why this check exists."""
        self.description = desc
        return self

    def to_dict(self) -> dict:
        d = {"type": self.type, "severity": self.severity, **self.params}
        if self.name:
            d["name"] = self.name
        if self.description:
            d["description"] = self.description
        return d


class _CheckFactory:
    """Factory for creating data quality checks.

    These define what to assert. The task must emit the corresponding
    evidence metrics for validation to work.

    Usage:
        from conduit_sdk.contracts import check

        check.row_count(min=1)           # expects metric: row_count
        check.freshness(max_age="24h")   # expects metric: data_age_seconds
        check.unique(["id"])             # expects metric: duplicate_count
        check.metric("accuracy", min=0.95)  # expects metric: accuracy
    """

    @staticmethod
    def row_count(
        min: Optional[int] = None,
        max: Optional[int] = None,
        exact: Optional[int] = None,
    ) -> Check:
        """Row count must be within bounds.
        Expects metric: row_count
        """
        params = {}
        if min is not None:
            params["min"] = min
        if max is not None:
            params["max"] = max
        if exact is not None:
            params["exact"] = exact
        return Check(type="row_count", params=params)

    @staticmethod
    def freshness(max_age: str) -> Check:
        """Data must not be older than max_age.
        Expects metric: data_age_seconds
        """
        return Check(
            type="freshness",
            params={"max_age": max_age},
        )

    @staticmethod
    def unique(columns: Union[str, List[str]]) -> Check:
        """Columns must form a unique key (no duplicates).
        Expects metric: duplicate_count (must be 0)
        """
        if isinstance(columns, str):
            columns = [columns]
        return Check(type="unique", params={"columns": columns})

    @staticmethod
    def not_null(column: str, min_rate: float = 1.0) -> Check:
        """Column's null rate must not exceed threshold.
        Expects metric: null_rate.{column}
        """
        return Check(
            type="not_null",
            params={"column": column, "min_rate": min_rate},
        )

    @staticmethod
    def accepted_values(
        column: str,
        values: List[str],
        allow_null: bool = False,
    ) -> Check:
        """Column values must be in the accepted set.
        Expects metric: invalid_value_count.{column} (must be 0)
        """
        return Check(
            type="accepted_values",
            params={"column": column, "values": values, "allow_null": allow_null},
        )

    @staticmethod
    def value_range(
        column: str,
        min: Optional[float] = None,
        max: Optional[float] = None,
    ) -> Check:
        """Numeric column must be within range.
        Expects metric: out_of_range_count.{column} (must be 0)
        """
        params: dict = {"column": column}
        if min is not None:
            params["min"] = min
        if max is not None:
            params["max"] = max
        return Check(type="value_range", params=params)

    @staticmethod
    def references(
        column: str,
        ref_task: str,
        ref_column: str,
    ) -> Check:
        """Referential integrity: column values must exist in ref_task.ref_column.
        Expects metric: orphan_count.{column} (must be 0)
        """
        return Check(
            type="references",
            params={
                "column": column,
                "ref_task": ref_task,
                "ref_column": ref_column,
            },
        )

    @staticmethod
    def row_count_delta(
        max_percent_change: float = 0.5,
        allow_decrease: bool = False,
    ) -> Check:
        """Row count change between runs must not exceed threshold.
        Expects metric: row_count_delta_pct
        """
        return Check(
            type="row_count_delta",
            params={
                "max_percent_change": max_percent_change,
                "allow_decrease": allow_decrease,
            },
        )

    @staticmethod
    def metric(
        metric_name: str,
        min: Optional[float] = None,
        max: Optional[float] = None,
        exact: Optional[float] = None,
    ) -> Check:
        """Generic metric assertion — the universal contract.
        Expects metric: {metric_name}
        """
        params: dict = {"metric_name": metric_name}
        if min is not None:
            params["min"] = min
        if max is not None:
            params["max"] = max
        if exact is not None:
            params["exact"] = exact
        return Check(type="metric", params=params)

    @staticmethod
    def custom(assertion_name: str) -> Check:
        """Custom pass/fail assertion.
        Expects metric: pass.{assertion_name} (1.0 = pass, 0.0 = fail)
        """
        return Check(
            type="custom",
            params={"assertion_name": assertion_name},
        )


# Module-level singleton for easy imports
check = _CheckFactory()


# ─── Contract Decorator ─────────────────────────────────────────────────────

def contract(*checks: Check) -> Callable:
    """Decorator that attaches data quality contracts to a task.

    Usage:
        @task(retries=3)
        @contract(
            check.row_count(min=1),
            check.unique(["id"]),
            check.metric("accuracy", min=0.95),
        )
        def my_task():
            ...
    """
    def decorator(func: Callable) -> Callable:
        if not hasattr(func, "_conduit_contracts"):
            func._conduit_contracts = []
        func._conduit_contracts.extend(checks)
        return func
    return decorator


# ─── Imperative API ─────────────────────────────────────────────────────────

class Contracts:
    """Imperative contract builder for runtime use.

    Creates contracts and emits them via the stdout protocol
    so the executor can validate them after task completion.

    Usage:
        contracts = Contracts("extract_orders")
        contracts.row_count(min=1)
        contracts.freshness(max_age="24h")
        contracts.metric("accuracy", min=0.95)
        contracts.emit()
    """

    def __init__(self, task_id: str):
        self.task_id = task_id
        self._checks: List[Check] = []

    def row_count(self, **kwargs) -> "Contracts":
        self._checks.append(check.row_count(**kwargs))
        return self

    def freshness(self, max_age: str) -> "Contracts":
        self._checks.append(check.freshness(max_age))
        return self

    def unique(self, columns: Union[str, List[str]]) -> "Contracts":
        self._checks.append(check.unique(columns))
        return self

    def not_null(self, column: str, min_rate: float = 1.0) -> "Contracts":
        self._checks.append(check.not_null(column, min_rate))
        return self

    def accepted_values(self, column: str, values: List[str], **kwargs) -> "Contracts":
        self._checks.append(check.accepted_values(column, values, **kwargs))
        return self

    def value_range(self, column: str, **kwargs) -> "Contracts":
        self._checks.append(check.value_range(column, **kwargs))
        return self

    def references(self, column: str, ref_task: str, ref_column: str) -> "Contracts":
        self._checks.append(check.references(column, ref_task, ref_column))
        return self

    def row_count_delta(self, **kwargs) -> "Contracts":
        self._checks.append(check.row_count_delta(**kwargs))
        return self

    def metric(self, metric_name: str, **kwargs) -> "Contracts":
        self._checks.append(check.metric(metric_name, **kwargs))
        return self

    def custom(self, assertion_name: str) -> "Contracts":
        self._checks.append(check.custom(assertion_name))
        return self

    def emit(self) -> None:
        """Emit contract definitions via stdout protocol."""
        payload = {
            "task_id": self.task_id,
            "checks": [c.to_dict() for c in self._checks],
        }
        print(f"CONDUIT::CONTRACT::{json.dumps(payload)}", flush=True)

    def to_dict_list(self) -> List[dict]:
        return [c.to_dict() for c in self._checks]


# ─── Assertion Helpers (for runtime validation inside tasks) ─────────────────
# These both validate AND emit the evidence metric, so contracts work automatically.

def assert_row_count(
    actual: int,
    min: Optional[int] = None,
    max: Optional[int] = None,
    exact: Optional[int] = None,
) -> None:
    """Assert row count at runtime and emit evidence. Raises ValueError on failure."""
    if exact is not None and actual != exact:
        raise ValueError(f"Row count is {actual}, expected exactly {exact}")
    if min is not None and actual < min:
        raise ValueError(f"Row count is {actual}, expected at least {min}")
    if max is not None and actual > max:
        raise ValueError(f"Row count is {actual}, expected at most {max}")
    emit_metric("row_count", actual)


def assert_freshness(latest_timestamp: str, max_age: str) -> None:
    """Assert data freshness at runtime and emit evidence."""
    from datetime import datetime, timedelta, timezone
    import re

    match = re.match(r"(\d+)([mhdw])", max_age)
    if not match:
        raise ValueError(f"Invalid max_age format: {max_age}")

    value, unit = int(match.group(1)), match.group(2)
    delta = {
        "m": timedelta(minutes=value),
        "h": timedelta(hours=value),
        "d": timedelta(days=value),
        "w": timedelta(weeks=value),
    }[unit]

    try:
        ts = datetime.fromisoformat(latest_timestamp.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        raise ValueError(f"Cannot parse timestamp: {latest_timestamp}")

    now = datetime.now(timezone.utc)
    age = now - ts

    if age > delta:
        raise ValueError(
            f"Data is {age} old, max allowed is {delta} ({max_age})"
        )
    emit_metric("data_age_seconds", age.total_seconds())


def assert_unique(values: list, column_name: str = "column") -> None:
    """Assert no duplicate values and emit evidence. Raises ValueError if duplicates found."""
    seen = set()
    duplicates = set()
    for v in values:
        if v in seen:
            duplicates.add(v)
        seen.add(v)
    if duplicates:
        emit_metric("duplicate_count", len(duplicates))
        sample = list(duplicates)[:5]
        raise ValueError(
            f"Found {len(duplicates)} duplicate values in '{column_name}': {sample}"
        )
    emit_metric("duplicate_count", 0)
