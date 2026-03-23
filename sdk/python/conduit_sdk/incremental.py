"""
Incremental processing helpers for the Conduit Python SDK.

When a task is configured for incremental processing, the Conduit executor
injects context via environment variables:

    CONDUIT_INCREMENTAL       - "true" if incremental is enabled
    CONDUIT_FULL_REFRESH      - "true" if this is a full refresh run
    CONDUIT_WATERMARK_TYPE    - "timestamp", "sequence", "partition", or "initial"
    CONDUIT_WATERMARK_VALUE   - The current watermark value
    CONDUIT_EFFECTIVE_START   - Lookback-adjusted start (for append strategy)
    CONDUIT_TARGET_PARTITIONS - Comma-separated partition keys (for delete+insert)
    CONDUIT_BATCH_SIZE        - Recommended batch size

Tasks emit their new watermark via the CONDUIT:: stdout protocol:
    CONDUIT::WATERMARK::2026-03-22T06:00:00Z

Example:

    from conduit_sdk import task
    from conduit_sdk.incremental import get_incremental_context, emit_watermark

    @task()
    def extract_orders():
        ctx = get_incremental_context()

        if ctx.is_full_refresh:
            df = db.query("SELECT * FROM orders")
        else:
            df = db.query(f"SELECT * FROM orders WHERE updated_at > '{ctx.watermark_value}'")

        # Emit the new high-water mark
        emit_watermark(df["updated_at"].max().isoformat())
        return df
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from .xcom import xcom_push as push_value


@dataclass
class IncrementalContext:
    """Runtime context for incremental task processing."""

    is_incremental: bool = False
    """Whether the task is running in incremental mode."""

    is_full_refresh: bool = True
    """Whether this is a full refresh (reprocess everything)."""

    watermark_type: str = "initial"
    """Type of watermark: 'timestamp', 'sequence', 'partition', or 'initial'."""

    watermark_value: Optional[str] = None
    """The current watermark value as a string."""

    effective_start: Optional[str] = None
    """Lookback-adjusted start time (only for append strategy)."""

    target_partitions: list[str] = field(default_factory=list)
    """Partition keys to process (only for delete+insert strategy)."""

    batch_size: Optional[int] = None
    """Recommended batch size for processing."""

    @property
    def watermark_timestamp(self) -> Optional[datetime]:
        """Parse watermark as a datetime (returns None if not a timestamp)."""
        if self.watermark_type != "timestamp" or not self.watermark_value:
            return None
        try:
            return datetime.fromisoformat(self.watermark_value)
        except ValueError:
            return None

    @property
    def watermark_sequence(self) -> Optional[int]:
        """Parse watermark as an integer sequence (returns None if not a sequence)."""
        if self.watermark_type != "sequence" or not self.watermark_value:
            return None
        try:
            return int(self.watermark_value)
        except ValueError:
            return None

    @property
    def is_first_run(self) -> bool:
        """True if this is the first run (no watermark yet)."""
        return self.watermark_type == "initial"

    def sql_filter(self, column: str) -> str:
        """
        Generate a SQL WHERE clause fragment for incremental filtering.

        Args:
            column: The column name to filter on.

        Returns:
            A SQL fragment like "updated_at > '2026-03-21T00:00:00Z'"
            or "1=1" for full refresh.

        Example:
            ctx = get_incremental_context()
            query = f"SELECT * FROM orders WHERE {ctx.sql_filter('updated_at')}"
        """
        if self.is_full_refresh or self.is_first_run:
            return "1=1"

        start = self.effective_start or self.watermark_value

        if self.target_partitions:
            parts = ", ".join(f"'{p}'" for p in self.target_partitions)
            return f"{column} IN ({parts})"

        if self.watermark_type == "sequence":
            return f"{column} > {start}"

        return f"{column} > '{start}'"


def get_incremental_context() -> IncrementalContext:
    """
    Read the incremental context from environment variables.

    Returns:
        IncrementalContext with the current run's incremental settings.
        If not running in incremental mode, returns a context where
        is_incremental=False and is_full_refresh=True.
    """
    is_incremental = os.environ.get("CONDUIT_INCREMENTAL", "false").lower() == "true"

    if not is_incremental:
        return IncrementalContext()

    target_partitions_str = os.environ.get("CONDUIT_TARGET_PARTITIONS", "")
    target_partitions = [p for p in target_partitions_str.split(",") if p]

    batch_size_str = os.environ.get("CONDUIT_BATCH_SIZE")
    batch_size = int(batch_size_str) if batch_size_str else None

    return IncrementalContext(
        is_incremental=True,
        is_full_refresh=os.environ.get("CONDUIT_FULL_REFRESH", "true").lower() == "true",
        watermark_type=os.environ.get("CONDUIT_WATERMARK_TYPE", "initial"),
        watermark_value=os.environ.get("CONDUIT_WATERMARK_VALUE") or None,
        effective_start=os.environ.get("CONDUIT_EFFECTIVE_START") or None,
        target_partitions=target_partitions,
        batch_size=batch_size,
    )


def emit_watermark(value: Any) -> None:
    """
    Emit a new watermark value after successful processing.

    The orchestrator reads this from stdout and advances the watermark
    for the next run.

    Args:
        value: The new watermark value. Can be:
            - A datetime (formatted as ISO 8601)
            - An integer (sequence number)
            - A string (partition key)

    Example:
        emit_watermark(datetime.now())        # Timestamp watermark
        emit_watermark(max_offset)            # Sequence watermark
        emit_watermark("2026-03-22")          # Partition watermark
    """
    if isinstance(value, datetime):
        str_value = value.isoformat()
    else:
        str_value = str(value)

    push_value("__watermark__", str_value)
    # Also emit via the CONDUIT:: protocol directly
    print(f"CONDUIT::WATERMARK::{str_value}")
