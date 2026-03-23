"""Tests for the incremental processing SDK."""

import os
import pytest
from unittest.mock import patch
from datetime import datetime, timezone

from conduit_sdk.incremental import (
    get_incremental_context,
    emit_watermark,
    IncrementalContext,
)


class TestIncrementalContext:
    """Tests for IncrementalContext dataclass."""

    def test_default_context_is_full_refresh(self):
        ctx = IncrementalContext()
        assert not ctx.is_incremental
        assert ctx.is_full_refresh
        assert ctx.watermark_type == "initial"
        assert ctx.is_first_run

    def test_timestamp_watermark(self):
        ctx = IncrementalContext(
            is_incremental=True,
            is_full_refresh=False,
            watermark_type="timestamp",
            watermark_value="2026-03-21T06:00:00+00:00",
        )
        ts = ctx.watermark_timestamp
        assert ts is not None
        assert ts.year == 2026
        assert ts.month == 3
        assert ts.day == 21

    def test_sequence_watermark(self):
        ctx = IncrementalContext(
            is_incremental=True,
            is_full_refresh=False,
            watermark_type="sequence",
            watermark_value="42000",
        )
        assert ctx.watermark_sequence == 42000
        assert ctx.watermark_timestamp is None

    def test_sql_filter_full_refresh(self):
        ctx = IncrementalContext(is_full_refresh=True)
        assert ctx.sql_filter("ts") == "1=1"

    def test_sql_filter_timestamp(self):
        ctx = IncrementalContext(
            is_incremental=True,
            is_full_refresh=False,
            watermark_type="timestamp",
            watermark_value="2026-03-21T00:00:00Z",
        )
        filt = ctx.sql_filter("updated_at")
        assert "updated_at > '2026-03-21T00:00:00Z'" == filt

    def test_sql_filter_sequence(self):
        ctx = IncrementalContext(
            is_incremental=True,
            is_full_refresh=False,
            watermark_type="sequence",
            watermark_value="500",
        )
        assert ctx.sql_filter("id") == "id > 500"

    def test_sql_filter_partitions(self):
        ctx = IncrementalContext(
            is_incremental=True,
            is_full_refresh=False,
            watermark_type="partition",
            target_partitions=["2026-03-21", "2026-03-22"],
        )
        filt = ctx.sql_filter("dt")
        assert "IN" in filt
        assert "'2026-03-21'" in filt
        assert "'2026-03-22'" in filt

    def test_effective_start_used_over_watermark(self):
        ctx = IncrementalContext(
            is_incremental=True,
            is_full_refresh=False,
            watermark_type="timestamp",
            watermark_value="2026-03-21T06:00:00Z",
            effective_start="2026-03-21T04:00:00Z",  # lookback-adjusted
        )
        filt = ctx.sql_filter("created_at")
        assert "04:00:00" in filt  # Uses effective_start, not watermark


class TestGetIncrementalContext:
    """Tests for reading context from environment variables."""

    def test_non_incremental_returns_default(self):
        with patch.dict(os.environ, {}, clear=True):
            ctx = get_incremental_context()
            assert not ctx.is_incremental
            assert ctx.is_full_refresh

    def test_reads_all_env_vars(self):
        env = {
            "CONDUIT_INCREMENTAL": "true",
            "CONDUIT_FULL_REFRESH": "false",
            "CONDUIT_WATERMARK_TYPE": "timestamp",
            "CONDUIT_WATERMARK_VALUE": "2026-03-21T06:00:00Z",
            "CONDUIT_EFFECTIVE_START": "2026-03-21T04:00:00Z",
            "CONDUIT_TARGET_PARTITIONS": "",
            "CONDUIT_BATCH_SIZE": "5000",
        }
        with patch.dict(os.environ, env, clear=True):
            ctx = get_incremental_context()
            assert ctx.is_incremental
            assert not ctx.is_full_refresh
            assert ctx.watermark_type == "timestamp"
            assert ctx.watermark_value == "2026-03-21T06:00:00Z"
            assert ctx.effective_start == "2026-03-21T04:00:00Z"
            assert ctx.batch_size == 5000

    def test_reads_partitions(self):
        env = {
            "CONDUIT_INCREMENTAL": "true",
            "CONDUIT_FULL_REFRESH": "false",
            "CONDUIT_WATERMARK_TYPE": "partition",
            "CONDUIT_TARGET_PARTITIONS": "2026-03-21,2026-03-22,2026-03-23",
        }
        with patch.dict(os.environ, env, clear=True):
            ctx = get_incremental_context()
            assert len(ctx.target_partitions) == 3
            assert ctx.target_partitions[0] == "2026-03-21"


class TestEmitWatermark:
    """Tests for watermark emission."""

    def test_emit_datetime(self, capsys):
        dt = datetime(2026, 3, 22, 12, 0, 0, tzinfo=timezone.utc)
        emit_watermark(dt)
        captured = capsys.readouterr()
        assert "CONDUIT::WATERMARK::" in captured.out
        assert "2026-03-22" in captured.out

    def test_emit_integer(self, capsys):
        emit_watermark(42000)
        captured = capsys.readouterr()
        assert "CONDUIT::WATERMARK::42000" in captured.out

    def test_emit_string(self, capsys):
        emit_watermark("2026-03-22")
        captured = capsys.readouterr()
        assert "CONDUIT::WATERMARK::2026-03-22" in captured.out
