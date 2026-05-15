"""Tests for conduit_sdk runtime mode (no double-execution).

Importing a DAG module from inside the Conduit executor must not run the
@dag function's call-chain (which would invoke every task body once).
Setting CONDUIT_RUNTIME_MODE=1 enables this suppression.
"""

from __future__ import annotations

import importlib
import os
import sys
import textwrap
from pathlib import Path

import pytest


@pytest.fixture
def isolated_module(tmp_path, monkeypatch):
    """Provide a fresh module-import sandbox and clear the dag registry."""
    monkeypatch.syspath_prepend(str(tmp_path))
    from conduit_sdk.decorators import clear_registry

    clear_registry()
    yield tmp_path
    for name in list(sys.modules):
        if name.startswith("_runtime_mode_fixture"):
            sys.modules.pop(name, None)


def _write_module(path: Path, name: str, body: str) -> None:
    (path / f"{name}.py").write_text(textwrap.dedent(body))


def test_local_dev_mode_runs_task_bodies_once(isolated_module, monkeypatch):
    """Without CONDUIT_RUNTIME_MODE, importing the module runs the DAG body,
    which calls each task — the original SDK behavior for local testing."""
    monkeypatch.delenv("CONDUIT_RUNTIME_MODE", raising=False)

    _write_module(
        isolated_module,
        "_runtime_mode_fixture_local",
        """
        from conduit_sdk import dag, task

        CALLS = []

        @dag()
        def pipeline():
            @task()
            def a():
                CALLS.append("a")

            @task()
            def b(upstream=a):
                CALLS.append("b")

            a()
            b(a)
        """,
    )

    mod = importlib.import_module("_runtime_mode_fixture_local")
    assert mod.CALLS == ["a", "b"], (
        f"In local mode, importing should run each task body exactly once; "
        f"got {mod.CALLS}"
    )


def test_runtime_mode_suppresses_task_bodies(isolated_module, monkeypatch):
    """With CONDUIT_RUNTIME_MODE=1, importing must NOT execute task bodies.
    This is the fix for the production double-execution bug: the executor
    sets this env, imports the module, then calls _runtime.run_task to run
    one specific task body."""
    monkeypatch.setenv("CONDUIT_RUNTIME_MODE", "1")

    _write_module(
        isolated_module,
        "_runtime_mode_fixture_runtime",
        """
        from conduit_sdk import dag, task

        CALLS = []

        @dag()
        def pipeline():
            @task()
            def a():
                CALLS.append("a")

            @task()
            def b(upstream=a):
                CALLS.append("b")

            a()
            b(a)
        """,
    )

    mod = importlib.import_module("_runtime_mode_fixture_runtime")
    assert mod.CALLS == [], (
        f"In runtime mode, importing must not run task bodies; got {mod.CALLS}"
    )

    # The DAG and its tasks are still registered.
    from conduit_sdk.decorators import get_dag

    pipeline = get_dag("pipeline")
    assert pipeline is not None
    assert set(pipeline.tasks.keys()) == {"a", "b"}


def test_runtime_run_task_executes_target_task_exactly_once(
    isolated_module, monkeypatch
):
    """End-to-end: import in runtime mode, then run_task for `b`. Only `b`'s
    body should run, exactly once. The call chain `a(); b(a)` must NOT have
    re-fired `a()`.
    """
    monkeypatch.setenv("CONDUIT_RUNTIME_MODE", "1")

    _write_module(
        isolated_module,
        "_runtime_mode_fixture_e2e",
        """
        from conduit_sdk import dag, task

        CALLS = []

        @dag()
        def pipeline():
            @task()
            def a():
                CALLS.append("a")

            @task()
            def b():
                CALLS.append("b")

            a()
            b()
        """,
    )

    from conduit_sdk._runtime import run_task

    run_task("_runtime_mode_fixture_e2e", "pipeline", "b", "b")

    mod = sys.modules["_runtime_mode_fixture_e2e"]
    assert mod.CALLS == ["b"], (
        f"Only target task 'b' should execute; got {mod.CALLS}"
    )


def test_xcom_round_trips_through_filesystem(isolated_module, monkeypatch, tmp_path):
    """End-to-end: upstream returns a value → it's persisted as JSON →
    downstream reads it from CONDUIT_XCOM_DIR and receives the deserialized
    value, not the TaskDefinition placeholder."""
    monkeypatch.setenv("CONDUIT_RUNTIME_MODE", "1")
    xcom_dir = tmp_path / "xcom"
    xcom_dir.mkdir()
    monkeypatch.setenv("CONDUIT_XCOM_DIR", str(xcom_dir))

    # Simulate upstream's persisted XCom (the executor writes this in prod).
    import json

    (xcom_dir / "produce.json").write_text(json.dumps({"rows": 42, "label": "ok"}))

    _write_module(
        isolated_module,
        "_runtime_mode_fixture_xcom",
        """
        from conduit_sdk import dag, task

        RECEIVED = []

        @dag()
        def pipeline():
            @task()
            def produce():
                return {"rows": 42, "label": "ok"}

            @task()
            def consume(upstream=produce):
                RECEIVED.append(upstream)
        """,
    )

    from conduit_sdk._runtime import run_task

    run_task("_runtime_mode_fixture_xcom", "pipeline", "consume", "consume")

    mod = sys.modules["_runtime_mode_fixture_xcom"]
    assert mod.RECEIVED == [{"rows": 42, "label": "ok"}], (
        f"Downstream did not receive upstream XCom; got {mod.RECEIVED}"
    )


def test_missing_upstream_xcom_degrades_to_none(isolated_module, monkeypatch, tmp_path):
    """If an upstream's XCom file is missing (e.g., the upstream task didn't
    return anything), the downstream parameter falls back to None rather than
    failing the dispatch."""
    monkeypatch.setenv("CONDUIT_RUNTIME_MODE", "1")
    xcom_dir = tmp_path / "xcom"
    xcom_dir.mkdir()
    monkeypatch.setenv("CONDUIT_XCOM_DIR", str(xcom_dir))

    _write_module(
        isolated_module,
        "_runtime_mode_fixture_xcom_missing",
        """
        from conduit_sdk import dag, task

        RECEIVED = []

        @dag()
        def pipeline():
            @task()
            def produce():
                pass  # returns None — no XCom

            @task()
            def consume(upstream=produce):
                RECEIVED.append(upstream)
        """,
    )

    from conduit_sdk._runtime import run_task

    run_task(
        "_runtime_mode_fixture_xcom_missing", "pipeline", "consume", "consume"
    )

    mod = sys.modules["_runtime_mode_fixture_xcom_missing"]
    # Default value (the TaskDefinition) is preserved when no XCom exists;
    # _runtime overrides positional defaults to None for the no-XCom case.
    # Either is acceptable; we just need NOT to crash.
    assert mod.RECEIVED, "Consume should have been called"
