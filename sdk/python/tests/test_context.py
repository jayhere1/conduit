"""Tests for the TaskContext class and get_context() function."""

import json
import os
from datetime import datetime

import pytest
from conduit_sdk.context import TaskContext, get_context


class TestTaskContextExplicit:
    """Tests for creating a TaskContext with explicit values."""

    def test_create_with_all_fields(self):
        """TaskContext can be constructed with all fields specified."""
        logical_date = datetime(2026, 4, 5, 12, 0, 0)
        ctx = TaskContext(
            dag_id="etl_daily",
            task_id="extract_orders",
            run_id="run-20260405-001",
            attempt=1,
            logical_date=logical_date,
            environment="production",
            upstream_xcom={"load.return_value": [1, 2, 3]},
        )

        assert ctx.dag_id == "etl_daily"
        assert ctx.task_id == "extract_orders"
        assert ctx.run_id == "run-20260405-001"
        assert ctx.attempt == 1
        assert ctx.logical_date == logical_date
        assert ctx.environment == "production"
        assert ctx.upstream_xcom == {"load.return_value": [1, 2, 3]}

    def test_get_upstream_existing_key(self):
        """get_upstream returns the value for a known task/key pair."""
        ctx = TaskContext(
            dag_id="dag1",
            task_id="t1",
            run_id="r1",
            attempt=1,
            logical_date=None,
            environment="dev",
            upstream_xcom={
                "extract.return_value": [10, 20],
                "extract.row_count": 2,
            },
        )

        assert ctx.get_upstream("extract") == [10, 20]
        assert ctx.get_upstream("extract", "row_count") == 2

    def test_get_upstream_missing_key(self):
        """get_upstream returns None when the task/key pair doesn't exist."""
        ctx = TaskContext(
            dag_id="dag1",
            task_id="t1",
            run_id="r1",
            attempt=1,
            logical_date=None,
            environment="dev",
            upstream_xcom={},
        )

        assert ctx.get_upstream("nonexistent") is None
        assert ctx.get_upstream("nonexistent", "some_key") is None

    def test_is_retry_first_attempt(self):
        """is_retry is False on attempt 1."""
        ctx = TaskContext(
            dag_id="d",
            task_id="t",
            run_id="r",
            attempt=1,
            logical_date=None,
            environment="dev",
            upstream_xcom={},
        )
        assert ctx.is_retry is False

    def test_is_retry_subsequent_attempt(self):
        """is_retry is True when attempt > 1."""
        ctx = TaskContext(
            dag_id="d",
            task_id="t",
            run_id="r",
            attempt=3,
            logical_date=None,
            environment="dev",
            upstream_xcom={},
        )
        assert ctx.is_retry is True


class TestGetContextDefaults:
    """Tests for get_context() when no environment variables are set."""

    @pytest.fixture(autouse=True)
    def clean_env(self, monkeypatch):
        """Remove all CONDUIT_* env vars before each test."""
        for key in list(os.environ):
            if key.startswith("CONDUIT_"):
                monkeypatch.delenv(key, raising=False)

    def test_default_dag_id(self):
        ctx = get_context()
        assert ctx.dag_id == "local_dag"

    def test_default_task_id(self):
        ctx = get_context()
        assert ctx.task_id == "local_task"

    def test_default_run_id(self):
        ctx = get_context()
        assert ctx.run_id == "local_run"

    def test_default_attempt(self):
        ctx = get_context()
        assert ctx.attempt == 1

    def test_default_environment(self):
        ctx = get_context()
        assert ctx.environment == "development"

    def test_default_logical_date_is_none(self):
        ctx = get_context()
        assert ctx.logical_date is None

    def test_default_upstream_xcom_is_empty(self):
        ctx = get_context()
        assert ctx.upstream_xcom == {}


class TestGetContextFromEnvironment:
    """Tests for get_context() reading from environment variables."""

    @pytest.fixture(autouse=True)
    def clean_env(self, monkeypatch):
        """Remove all CONDUIT_* env vars before each test."""
        for key in list(os.environ):
            if key.startswith("CONDUIT_"):
                monkeypatch.delenv(key, raising=False)

    def test_reads_dag_id(self, monkeypatch):
        monkeypatch.setenv("CONDUIT_DAG_ID", "my_dag")
        ctx = get_context()
        assert ctx.dag_id == "my_dag"

    def test_reads_task_id(self, monkeypatch):
        monkeypatch.setenv("CONDUIT_TASK_ID", "my_task")
        ctx = get_context()
        assert ctx.task_id == "my_task"

    def test_reads_run_id(self, monkeypatch):
        monkeypatch.setenv("CONDUIT_RUN_ID", "run-123")
        ctx = get_context()
        assert ctx.run_id == "run-123"

    def test_reads_attempt(self, monkeypatch):
        monkeypatch.setenv("CONDUIT_ATTEMPT", "3")
        ctx = get_context()
        assert ctx.attempt == 3
        assert ctx.is_retry is True

    def test_reads_environment(self, monkeypatch):
        monkeypatch.setenv("CONDUIT_ENVIRONMENT", "staging")
        ctx = get_context()
        assert ctx.environment == "staging"

    def test_reads_logical_date(self, monkeypatch):
        monkeypatch.setenv("CONDUIT_LOGICAL_DATE", "2026-04-05T06:00:00")
        ctx = get_context()
        assert ctx.logical_date == datetime(2026, 4, 5, 6, 0, 0)

    def test_invalid_logical_date_returns_none(self, monkeypatch):
        monkeypatch.setenv("CONDUIT_LOGICAL_DATE", "not-a-date")
        ctx = get_context()
        assert ctx.logical_date is None

    def test_reads_xcom_json(self, monkeypatch):
        xcom = {"extract.return_value": [1, 2, 3], "extract.row_count": 3}
        monkeypatch.setenv("CONDUIT_XCOM_JSON", json.dumps(xcom))
        ctx = get_context()
        assert ctx.upstream_xcom == xcom
        assert ctx.get_upstream("extract") == [1, 2, 3]

    def test_invalid_xcom_json_returns_empty(self, monkeypatch):
        monkeypatch.setenv("CONDUIT_XCOM_JSON", "{bad json")
        ctx = get_context()
        assert ctx.upstream_xcom == {}

    def test_full_context_from_env(self, monkeypatch):
        """All environment variables set together produce a complete context."""
        monkeypatch.setenv("CONDUIT_DAG_ID", "etl_pipeline")
        monkeypatch.setenv("CONDUIT_TASK_ID", "transform")
        monkeypatch.setenv("CONDUIT_RUN_ID", "run-456")
        monkeypatch.setenv("CONDUIT_ATTEMPT", "2")
        monkeypatch.setenv("CONDUIT_ENVIRONMENT", "production")
        monkeypatch.setenv("CONDUIT_LOGICAL_DATE", "2026-04-05T12:00:00")
        monkeypatch.setenv(
            "CONDUIT_XCOM_JSON",
            json.dumps({"extract.return_value": "data"}),
        )

        ctx = get_context()
        assert ctx.dag_id == "etl_pipeline"
        assert ctx.task_id == "transform"
        assert ctx.run_id == "run-456"
        assert ctx.attempt == 2
        assert ctx.is_retry is True
        assert ctx.environment == "production"
        assert ctx.logical_date == datetime(2026, 4, 5, 12, 0, 0)
        assert ctx.get_upstream("extract") == "data"
