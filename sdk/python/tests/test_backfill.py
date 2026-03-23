"""Tests for the backfill context SDK."""

import os
import pytest
from unittest.mock import patch
from datetime import datetime, timezone

from conduit_sdk.backfill import (
    get_backfill_context,
    BackfillContext,
)


class TestBackfillContext:
    """Tests for BackfillContext dataclass."""

    def test_default_context_is_not_backfill(self):
        ctx = BackfillContext()
        assert not ctx.is_backfill
        assert ctx.backfill_id is None
        assert ctx.partition_key is None
        assert ctx.partition_start is None
        assert ctx.partition_end is None
        assert ctx.total_partitions is None
        assert ctx.partition_index is None

    def test_backfill_context_with_all_fields(self):
        ctx = BackfillContext(
            backfill_id="bf_etl_20260101_20260301",
            partition_key="2026-02-15",
            partition_start=datetime(2026, 2, 15, 0, 0, 0, tzinfo=timezone.utc),
            partition_end=datetime(2026, 2, 16, 0, 0, 0, tzinfo=timezone.utc),
            is_backfill=True,
            total_partitions=59,
            partition_index=45,
        )
        assert ctx.is_backfill
        assert ctx.backfill_id == "bf_etl_20260101_20260301"
        assert ctx.partition_key == "2026-02-15"
        assert ctx.partition_start.year == 2026
        assert ctx.partition_start.month == 2
        assert ctx.partition_start.day == 15
        assert ctx.total_partitions == 59
        assert ctx.partition_index == 45

    def test_is_first_partition(self):
        ctx = BackfillContext(
            is_backfill=True,
            partition_index=0,
            total_partitions=10,
        )
        assert ctx.is_first_partition
        assert not ctx.is_last_partition

    def test_is_last_partition(self):
        ctx = BackfillContext(
            is_backfill=True,
            partition_index=9,
            total_partitions=10,
        )
        assert not ctx.is_first_partition
        assert ctx.is_last_partition

    def test_is_first_and_last_when_single_partition(self):
        ctx = BackfillContext(
            is_backfill=True,
            partition_index=0,
            total_partitions=1,
        )
        assert ctx.is_first_partition
        assert ctx.is_last_partition

    def test_not_first_or_last_when_not_backfill(self):
        ctx = BackfillContext()
        assert not ctx.is_first_partition
        assert not ctx.is_last_partition

    def test_progress(self):
        # First partition of 10
        ctx = BackfillContext(is_backfill=True, partition_index=0, total_partitions=10)
        assert ctx.progress == pytest.approx(0.1)

        # Fifth partition of 10
        ctx = BackfillContext(is_backfill=True, partition_index=4, total_partitions=10)
        assert ctx.progress == pytest.approx(0.5)

        # Last partition of 10
        ctx = BackfillContext(is_backfill=True, partition_index=9, total_partitions=10)
        assert ctx.progress == pytest.approx(1.0)

    def test_progress_not_backfill(self):
        ctx = BackfillContext()
        assert ctx.progress is None

    def test_sql_between_with_backfill(self):
        ctx = BackfillContext(
            is_backfill=True,
            partition_start=datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc),
            partition_end=datetime(2026, 3, 2, 0, 0, 0, tzinfo=timezone.utc),
        )
        sql = ctx.sql_between("created_at")
        assert "created_at >=" in sql
        assert "created_at <" in sql
        assert "2026-03-01" in sql
        assert "2026-03-02" in sql

    def test_sql_between_not_backfill(self):
        ctx = BackfillContext()
        assert ctx.sql_between("created_at") == "1=1"

    def test_sql_between_missing_dates(self):
        ctx = BackfillContext(is_backfill=True, partition_start=None, partition_end=None)
        assert ctx.sql_between("dt") == "1=1"


class TestGetBackfillContext:
    """Tests for reading backfill context from environment variables."""

    def test_non_backfill_returns_default(self):
        with patch.dict(os.environ, {}, clear=True):
            ctx = get_backfill_context()
            assert not ctx.is_backfill
            assert ctx.backfill_id is None

    def test_reads_all_env_vars(self):
        env = {
            "CONDUIT_BACKFILL_ID": "bf_etl_20260101_20260301",
            "CONDUIT_PARTITION_KEY": "2026-02-15",
            "CONDUIT_PARTITION_START": "2026-02-15T00:00:00+00:00",
            "CONDUIT_PARTITION_END": "2026-02-16T00:00:00+00:00",
            "CONDUIT_TOTAL_PARTITIONS": "59",
            "CONDUIT_PARTITION_INDEX": "45",
        }
        with patch.dict(os.environ, env, clear=True):
            ctx = get_backfill_context()
            assert ctx.is_backfill
            assert ctx.backfill_id == "bf_etl_20260101_20260301"
            assert ctx.partition_key == "2026-02-15"
            assert ctx.partition_start is not None
            assert ctx.partition_start.day == 15
            assert ctx.partition_end is not None
            assert ctx.partition_end.day == 16
            assert ctx.total_partitions == 59
            assert ctx.partition_index == 45

    def test_partial_env_vars(self):
        """Even with just CONDUIT_BACKFILL_ID, it should be detected as a backfill."""
        env = {
            "CONDUIT_BACKFILL_ID": "bf_test",
        }
        with patch.dict(os.environ, env, clear=True):
            ctx = get_backfill_context()
            assert ctx.is_backfill
            assert ctx.backfill_id == "bf_test"
            assert ctx.partition_key is None
            assert ctx.partition_start is None
            assert ctx.total_partitions is None

    def test_invalid_datetime_returns_none(self):
        env = {
            "CONDUIT_BACKFILL_ID": "bf_test",
            "CONDUIT_PARTITION_START": "not-a-date",
            "CONDUIT_PARTITION_END": "also-not-a-date",
        }
        with patch.dict(os.environ, env, clear=True):
            ctx = get_backfill_context()
            assert ctx.is_backfill
            assert ctx.partition_start is None
            assert ctx.partition_end is None

    def test_invalid_integers_returns_none(self):
        env = {
            "CONDUIT_BACKFILL_ID": "bf_test",
            "CONDUIT_TOTAL_PARTITIONS": "not_a_number",
            "CONDUIT_PARTITION_INDEX": "also_not",
        }
        with patch.dict(os.environ, env, clear=True):
            ctx = get_backfill_context()
            assert ctx.is_backfill
            assert ctx.total_partitions is None
            assert ctx.partition_index is None


class TestBackfillContextImport:
    """Test that BackfillContext is importable from the main SDK."""

    def test_import_from_sdk(self):
        from conduit_sdk import get_backfill_context, BackfillContext
        ctx = get_backfill_context()
        assert isinstance(ctx, BackfillContext)
