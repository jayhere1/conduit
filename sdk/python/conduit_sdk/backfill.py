"""
Backfill context helpers for the Conduit Python SDK.

When a task runs as part of a backfill, the Conduit executor injects context
via environment variables:

    CONDUIT_BACKFILL_ID       - Unique identifier for this backfill run
    CONDUIT_PARTITION_KEY     - Human-readable partition key (e.g., "2026-03-01")
    CONDUIT_PARTITION_START   - Partition start time (ISO 8601)
    CONDUIT_PARTITION_END     - Partition end time (ISO 8601)
    CONDUIT_LOGICAL_DATE      - The partition's logical date (same as partition start)
    CONDUIT_TOTAL_PARTITIONS  - Total number of partitions in this backfill
    CONDUIT_PARTITION_INDEX   - 0-based index of this partition

Tasks can use this context to:
- Filter data to the correct time window
- Log progress relative to the total backfill
- Adjust behavior based on whether this is a backfill vs. normal run

Example:

    from conduit_sdk import task
    from conduit_sdk.backfill import get_backfill_context

    @task()
    def extract_orders():
        ctx = get_backfill_context()

        if ctx.is_backfill:
            print(f"Backfill partition {ctx.partition_index + 1}/{ctx.total_partitions}")
            df = db.query(
                f"SELECT * FROM orders "
                f"WHERE created_at >= '{ctx.partition_start}' "
                f"AND created_at < '{ctx.partition_end}'"
            )
        else:
            df = db.query("SELECT * FROM orders WHERE created_at > :last_watermark")

        return df
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class BackfillContext:
    """Runtime context for a task running as part of a backfill.

    Attributes:
        backfill_id: Unique identifier for this backfill run, or None if
            this is not a backfill.
        partition_key: Human-readable partition key (e.g., "2026-03-01"),
            or None if not a backfill.
        partition_start: Start of this partition's time window (inclusive),
            or None if not a backfill.
        partition_end: End of this partition's time window (exclusive),
            or None if not a backfill.
        is_backfill: True if this run is part of a backfill.
        total_partitions: Total number of partitions in the backfill,
            or None if not a backfill.
        partition_index: 0-based index of this partition within the backfill,
            or None if not a backfill.
    """

    backfill_id: Optional[str] = None
    partition_key: Optional[str] = None
    partition_start: Optional[datetime] = None
    partition_end: Optional[datetime] = None
    is_backfill: bool = False
    total_partitions: Optional[int] = None
    partition_index: Optional[int] = None

    @property
    def is_first_partition(self) -> bool:
        """True if this is the first partition in the backfill."""
        return self.is_backfill and self.partition_index == 0

    @property
    def is_last_partition(self) -> bool:
        """True if this is the last partition in the backfill."""
        if not self.is_backfill or self.partition_index is None or self.total_partitions is None:
            return False
        return self.partition_index == self.total_partitions - 1

    @property
    def progress(self) -> Optional[float]:
        """Backfill progress as a fraction [0.0, 1.0], or None if not a backfill."""
        if not self.is_backfill or self.partition_index is None or self.total_partitions is None:
            return None
        if self.total_partitions == 0:
            return 1.0
        return (self.partition_index + 1) / self.total_partitions

    def sql_between(self, column: str) -> str:
        """Generate a SQL WHERE clause for this partition's time window.

        Args:
            column: The column name to filter on.

        Returns:
            A SQL fragment like "created_at >= '2026-03-01T00:00:00' AND created_at < '2026-03-02T00:00:00'"
            or "1=1" if not a backfill.

        Example:
            ctx = get_backfill_context()
            query = f"SELECT * FROM orders WHERE {ctx.sql_between('created_at')}"
        """
        if not self.is_backfill or self.partition_start is None or self.partition_end is None:
            return "1=1"
        return (
            f"{column} >= '{self.partition_start.isoformat()}' "
            f"AND {column} < '{self.partition_end.isoformat()}'"
        )


def get_backfill_context() -> BackfillContext:
    """Read backfill context from environment variables.

    Returns:
        BackfillContext with the current partition's information.
        If this is not a backfill run, returns a context where
        is_backfill=False and all partition fields are None.
    """
    backfill_id = os.environ.get("CONDUIT_BACKFILL_ID")

    if not backfill_id:
        return BackfillContext()

    partition_start = _parse_datetime(os.environ.get("CONDUIT_PARTITION_START"))
    partition_end = _parse_datetime(os.environ.get("CONDUIT_PARTITION_END"))

    total_partitions = _parse_int(os.environ.get("CONDUIT_TOTAL_PARTITIONS"))
    partition_index = _parse_int(os.environ.get("CONDUIT_PARTITION_INDEX"))

    return BackfillContext(
        backfill_id=backfill_id,
        partition_key=os.environ.get("CONDUIT_PARTITION_KEY"),
        partition_start=partition_start,
        partition_end=partition_end,
        is_backfill=True,
        total_partitions=total_partitions,
        partition_index=partition_index,
    )


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO 8601 datetime string, returning None on failure."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def _parse_int(value: Optional[str]) -> Optional[int]:
    """Parse an integer string, returning None on failure."""
    if not value:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
