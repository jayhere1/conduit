"""
@dag and @task decorators.

These decorators serve dual purposes:
1. At parse time (tree-sitter): Conduit reads decorator arguments from the AST
   without executing Python. The decorator names and keyword arguments are the
   "schema" that Conduit's compiler extracts.
2. At runtime (local dev/testing): The decorators capture metadata and build
   a local DAG representation, letting you test pipelines locally before
   deploying to Conduit.

The decorators are designed to be zero-dependency and zero-overhead when
used with Conduit's compiled execution model.
"""

from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class TaskDefinition:
    """Metadata extracted from a @task decorator."""
    id: str
    function: Callable
    retries: int = 0
    retry_delay: Optional[str] = None
    pool: Optional[str] = None
    timeout: Optional[str] = None
    priority: int = 0
    trigger_rule: str = "all_success"
    tags: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    doc: Optional[str] = None

    def __call__(self, *args, **kwargs):
        """Allow tasks to be called directly for local testing."""
        return self.function(*args, **kwargs)

    def __repr__(self):
        deps = f" <- [{', '.join(self.dependencies)}]" if self.dependencies else ""
        return f"<Task '{self.id}'{deps}>"


@dataclass
class DagDefinition:
    """Metadata extracted from a @dag decorator."""
    id: str
    function: Callable
    schedule: Optional[str] = None
    tags: list[str] = field(default_factory=list)
    max_active_runs: int = 1
    on_failure: Optional[str] = None
    tasks: dict[str, TaskDefinition] = field(default_factory=dict)
    doc: Optional[str] = None

    def __repr__(self):
        return f"<DAG '{self.id}' ({len(self.tasks)} tasks)>"


# Global registry of all defined DAGs (for local introspection/testing)
_dag_registry: dict[str, DagDefinition] = {}


def dag(
    schedule: Optional[str] = None,
    tags: Optional[list[str]] = None,
    max_active_runs: int = 1,
    on_failure: Optional[str] = None,
):
    """
    Decorator that marks a function as a Conduit DAG definition.

    Args:
        schedule: Cron expression (e.g., "0 6 * * *" for daily at 6am).
        tags: Tags for filtering and organization.
        max_active_runs: Maximum concurrent runs of this DAG.
        on_failure: Webhook URL for failure notifications.

    Example:
        @dag(schedule="0 6 * * *", tags=["etl", "warehouse"])
        def daily_etl():
            @task(retries=3)
            def extract():
                ...

            @task()
            def transform(data=extract):
                ...
    """
    def decorator(func: Callable) -> DagDefinition:
        dag_def = DagDefinition(
            id=func.__name__,
            function=func,
            schedule=schedule,
            tags=tags or [],
            max_active_runs=max_active_runs,
            on_failure=on_failure,
            doc=inspect.getdoc(func),
        )

        # Execute the function body to collect @task definitions
        # This is only for local dev — Conduit's compiler skips this
        _current_dag_context.append(dag_def)
        try:
            func()
        finally:
            _current_dag_context.pop()

        # Resolve dependencies from function signatures
        _resolve_dependencies(dag_def)

        # Register globally
        _dag_registry[dag_def.id] = dag_def

        return dag_def

    return decorator


def task(
    retries: int = 0,
    retry_delay: Optional[str] = None,
    pool: Optional[str] = None,
    timeout: Optional[str] = None,
    priority: int = 0,
    trigger_rule: str = "all_success",
    tags: Optional[list[str]] = None,
):
    """
    Decorator that marks a function as a Conduit task within a DAG.

    Args:
        retries: Number of retry attempts on failure.
        retry_delay: Delay between retries (e.g., "5m", "30s").
        pool: Named resource pool for concurrency control.
        timeout: Maximum execution time (e.g., "1h", "30m").
        priority: Execution priority (higher = runs first within pool).
        trigger_rule: When this task runs relative to upstreams.
            Options: "all_success", "all_done", "one_success", "one_failed".
        tags: Tags for this specific task.

    Example:
        @task(retries=3, pool="extract_pool", timeout="30m")
        def extract_orders():
            conn = get_connection("source_db")
            return conn.execute("SELECT * FROM orders WHERE date = :date")
    """
    def decorator(func: Callable) -> TaskDefinition:
        task_def = TaskDefinition(
            id=func.__name__,
            function=func,
            retries=retries,
            retry_delay=retry_delay,
            pool=pool,
            timeout=timeout,
            priority=priority,
            trigger_rule=trigger_rule,
            tags=tags or [],
            doc=inspect.getdoc(func),
        )

        # Register with current DAG context (if any)
        if _current_dag_context:
            current_dag = _current_dag_context[-1]
            current_dag.tasks[task_def.id] = task_def

        return task_def

    return decorator


# ── Internal helpers ──────────────────────────────────────────

# Stack of DAG definitions being built (supports nested DAGs, though unusual)
_current_dag_context: list[DagDefinition] = []


def _resolve_dependencies(dag_def: DagDefinition):
    """
    Resolve task dependencies from function signatures.

    Convention: if a task function has a parameter with a default value
    that is another TaskDefinition, that creates a data-flow dependency.

    Example:
        @task()
        def transform(raw_data=extract_orders):
            ...
        # transform depends on extract_orders
    """
    for task_def in dag_def.tasks.values():
        sig = inspect.signature(task_def.function)
        for param in sig.parameters.values():
            if isinstance(param.default, TaskDefinition):
                upstream_id = param.default.id
                if upstream_id in dag_def.tasks:
                    task_def.dependencies.append(upstream_id)


def get_dag(dag_id: str) -> Optional[DagDefinition]:
    """Get a registered DAG by ID."""
    return _dag_registry.get(dag_id)


def list_dags() -> list[DagDefinition]:
    """List all registered DAGs."""
    return list(_dag_registry.values())


def clear_registry():
    """Clear the DAG registry (useful for testing)."""
    _dag_registry.clear()
