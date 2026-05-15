"""
Runtime entry point invoked by the Rust executor when running a task.

The executor spawns:
    python3 -c "from conduit_sdk._runtime import run_task; run_task(module, dag_id, task_id, function)"

Side-effect of `import <module>`: the `@dag` decorator runs the DAG function,
which registers nested `@task` definitions in `_dag_registry`. We then look up
the task by (dag_id, task_id) and call it.

Falls back to a direct `from <module> import <function>` for non-SDK tasks
(e.g. plain Python files that don't use the `@dag` / `@task` decorators).
"""

from __future__ import annotations

import importlib
import inspect
import json
import os
import sys
from pathlib import Path
from typing import Any, Optional


_MISSING = object()


def _load_xcom(upstream_task_id: str) -> Any:
    """Read an upstream's XCom from CONDUIT_XCOM_DIR/<task_id>.json.

    Returns `_MISSING` if no XCom dir is configured or the file doesn't exist —
    the caller decides what to do (fall back to None, error, etc.).
    """
    dir_str = os.environ.get("CONDUIT_XCOM_DIR")
    if not dir_str:
        return _MISSING
    path = Path(dir_str) / f"{upstream_task_id}.json"
    if not path.exists():
        return _MISSING
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return _MISSING


def _call_with_xcom(fn, task_def=None) -> None:
    """Call `fn`, resolving each parameter to an upstream XCom value when
    possible. Resolution order, per parameter:
      1. Default value is a TaskDefinition  → load XCom for that upstream by id
      2. Parameter name matches an upstream  → load that upstream's XCom
      3. Positional fallback (no default)    → take next dep from task_def
      4. Otherwise                           → None
    Missing XCom files degrade to None so a task that doesn't actually use its
    declared input still runs.
    """
    from conduit_sdk.decorators import TaskDefinition

    sig = inspect.signature(fn)
    upstream_ids = list(task_def.dependencies) if task_def else []
    upstream_set = set(upstream_ids)
    positional_idx = 0
    kwargs: dict[str, Any] = {}

    for name, param in sig.parameters.items():
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue

        resolved: Any = _MISSING

        if isinstance(param.default, TaskDefinition):
            resolved = _load_xcom(param.default.id)
        elif name in upstream_set:
            resolved = _load_xcom(name)
        elif (
            param.default is inspect.Parameter.empty
            and positional_idx < len(upstream_ids)
        ):
            resolved = _load_xcom(upstream_ids[positional_idx])
            positional_idx += 1

        if resolved is _MISSING:
            if param.default is inspect.Parameter.empty:
                kwargs[name] = None
            # else: let Python apply the declared default
        else:
            kwargs[name] = resolved

    result = fn(**kwargs)
    # Auto-emit the task's return value as its XCom so downstream tasks can
    # consume it without explicit xcom_push calls. The executor parses this
    # stdout protocol message (format: CONDUIT::XCOM::{json_object}) and
    # persists the wrapped value to CONDUIT_XCOM_DIR/<task_id>.json.
    if result is not None:
        try:
            payload = json.dumps({"return_value": result})
        except (TypeError, ValueError):
            # Non-serializable return value — skip auto-emit, the user can
            # still emit explicitly via xcom_push.
            return
        print(f"CONDUIT::XCOM::{payload}", flush=True)


def run_task(module: str, dag_id: str, task_id: str, function: str) -> None:
    mod = importlib.import_module(module)

    from conduit_sdk.decorators import get_dag

    dag_def = get_dag(dag_id)
    if dag_def is not None and task_id in dag_def.tasks:
        td = dag_def.tasks[task_id]
        _call_with_xcom(td.function, td)
        return

    fn = getattr(mod, function, None)
    if fn is None:
        raise SystemExit(
            f"conduit_sdk._runtime: task '{task_id}' not registered in DAG "
            f"'{dag_id}' and no top-level function '{function}' in '{module}'"
        )
    _call_with_xcom(fn, None)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise SystemExit(
            "usage: python -m conduit_sdk._runtime <module> <dag_id> <task_id> <function>"
        )
    run_task(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
