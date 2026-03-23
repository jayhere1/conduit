"""
PythonOperator — wraps a Python callable as a Conduit task.

Integrates with the ``@task`` decorator system: captures context, runs the
callable, and pushes the return value as XCom.

Usage:
    from conduit_sdk.operators import PythonOperator

    def extract_data(**kwargs):
        context = kwargs["context"]
        print(f"Running in DAG: {context.dag_id}")
        return [1, 2, 3]

    op = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
        retries=3,
        timeout="30m",
    )
    result = op.run()

    # With positional and keyword arguments
    def add(a, b, multiplier=1):
        return (a + b) * multiplier

    op = PythonOperator(
        task_id="add",
        python_callable=add,
        op_args=[3, 4],
        op_kwargs={"multiplier": 2},
    )
    assert op.run() == 14
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from conduit_sdk.context import TaskContext
from conduit_sdk.operators.base import BaseOperator


class PythonOperator(BaseOperator):
    """Operator that executes a Python callable.

    The callable receives positional args from ``op_args``, keyword args
    from ``op_kwargs``, and an injected ``context`` keyword argument with
    the current :class:`~conduit_sdk.context.TaskContext`.

    Args:
        task_id: Unique identifier for this task.
        python_callable: The function to execute.
        op_args: Positional arguments to pass to the callable.
        op_kwargs: Keyword arguments to pass to the callable.
        **task_kwargs: Standard task kwargs (retries, timeout, pool, etc.).

    Example:
        def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        op = PythonOperator(
            task_id="greet",
            python_callable=greet,
            op_args=["World"],
            op_kwargs={"greeting": "Hi"},
            retries=2,
        )
        result = op.run()  # "Hi, World!"
    """

    def __init__(
        self,
        task_id: str,
        python_callable: Callable,
        op_args: Optional[List[Any]] = None,
        op_kwargs: Optional[Dict[str, Any]] = None,
        **task_kwargs: Any,
    ):
        super().__init__(task_id=task_id, **task_kwargs)
        if not callable(python_callable):
            raise TypeError(
                f"python_callable must be callable, got {type(python_callable).__name__}"
            )
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

    def execute(self, context: Optional[TaskContext] = None) -> Any:
        """Execute the Python callable.

        The callable is invoked with ``*op_args`` and ``**op_kwargs``.
        If the callable accepts a ``context`` keyword argument, the
        :class:`TaskContext` is injected automatically.

        Args:
            context: The task execution context.

        Returns:
            The return value of the callable.
        """
        import inspect

        kwargs = dict(self.op_kwargs)

        # Inject context if the callable accepts it
        sig = inspect.signature(self.python_callable)
        if "context" in sig.parameters:
            kwargs["context"] = context
        elif any(
            p.kind == inspect.Parameter.VAR_KEYWORD
            for p in sig.parameters.values()
        ):
            # Callable accepts **kwargs, inject context
            kwargs["context"] = context

        return self.python_callable(*self.op_args, **kwargs)
