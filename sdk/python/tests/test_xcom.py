"""Tests for XCom and protocol message helpers."""

import io
import json
import sys

from conduit_sdk.xcom import (
    _local_xcom_clear,
    _local_xcom_set,
    metric,
    progress,
    xcom_pull,
    xcom_push,
    log,
)


def test_xcom_push_emits_protocol_message(capsys):
    """xcom_push writes the correct protocol message to stdout."""
    xcom_push("row_count", 1500)
    captured = capsys.readouterr()
    assert captured.out.strip() == "CONDUIT::XCOM::row_count=1500"


def test_xcom_push_json_serialization(capsys):
    """xcom_push correctly serializes complex values."""
    xcom_push("schema", ["id", "name", "email"])
    captured = capsys.readouterr()
    assert 'CONDUIT::XCOM::schema=["id", "name", "email"]' in captured.out


def test_xcom_pull_from_local_store():
    """xcom_pull reads from the local store for testing."""
    _local_xcom_clear()
    _local_xcom_set("extract", "row_count", 42)

    result = xcom_pull("extract", "row_count")
    assert result == 42
    _local_xcom_clear()


def test_xcom_pull_missing_returns_none():
    """xcom_pull returns None for missing keys."""
    _local_xcom_clear()
    assert xcom_pull("nonexistent", "key") is None


def test_log_emits_protocol(capsys):
    """log() emits the correct protocol message."""
    log("Processing started", level="INFO")
    captured = capsys.readouterr()
    assert captured.out.strip() == "CONDUIT::LOG::INFO::Processing started"


def test_progress_emits_protocol(capsys):
    """progress() emits the correct protocol message."""
    progress(50, 100)
    captured = capsys.readouterr()
    assert captured.out.strip() == "CONDUIT::PROGRESS::50/100"


def test_metric_emits_protocol(capsys):
    """metric() emits the correct protocol message."""
    metric("rows_processed", 1500000, "rows")
    captured = capsys.readouterr()
    assert captured.out.strip() == "CONDUIT::METRIC::rows_processed=1500000::rows"


def test_metric_without_unit(capsys):
    """metric() works without a unit."""
    metric("latency", 42.5)
    captured = capsys.readouterr()
    assert captured.out.strip() == "CONDUIT::METRIC::latency=42.5"
