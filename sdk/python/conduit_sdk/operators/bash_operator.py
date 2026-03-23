"""
BashOperator — runs shell commands as Conduit tasks.

Executes a bash command via ``subprocess``, captures stdout/stderr, and
emits Conduit protocol messages (LOG, PROGRESS). Supports Jinja-like
template variables that are resolved from the task context.

Usage:
    from conduit_sdk.operators import BashOperator

    op = BashOperator(
        task_id="list_files",
        bash_command="ls -la /data/{{ ds }}",
        retries=2,
        timeout="5m",
    )
    output = op.run()

    # With custom environment and working directory
    op = BashOperator(
        task_id="build",
        bash_command="make build",
        env={"BUILD_TYPE": "release"},
        cwd="/app",
    )
"""

from __future__ import annotations

import os
import re
import subprocess
from typing import Any, Dict, Optional

from conduit_sdk.context import TaskContext
from conduit_sdk.xcom import log, progress
from conduit_sdk.operators.base import BaseOperator, _parse_duration


class BashOperator(BaseOperator):
    """Operator that runs a shell command.

    Stdout is captured and returned as the XCom ``return_value``.
    Stderr is emitted as Conduit LOG messages.

    Template variables in the command are replaced with values from the
    execution context:

    - ``{{ ds }}`` - logical date as ``YYYY-MM-DD``
    - ``{{ run_id }}`` - the current run ID
    - ``{{ dag_id }}`` - the current DAG ID
    - ``{{ task_id }}`` - this task's ID
    - ``{{ attempt }}`` - current retry attempt number

    Args:
        task_id: Unique identifier for this task.
        bash_command: The shell command to execute.
        env: Additional environment variables (merged with current env).
        cwd: Working directory for the command.
        **task_kwargs: Standard task kwargs (retries, timeout, pool, etc.).

    Example:
        op = BashOperator(
            task_id="dump_db",
            bash_command="pg_dump mydb > /backups/{{ ds }}.sql",
            timeout="1h",
        )
    """

    def __init__(
        self,
        task_id: str,
        bash_command: str,
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        **task_kwargs: Any,
    ):
        super().__init__(task_id=task_id, **task_kwargs)
        self.bash_command = bash_command
        self.env = env
        self.cwd = cwd

    def execute(self, context: Optional[TaskContext] = None) -> str:
        """Execute the bash command.

        Args:
            context: The task execution context (used for template rendering).

        Returns:
            The command's stdout as a string (stripped of trailing whitespace).

        Raises:
            subprocess.CalledProcessError: If the command exits with a non-zero
                return code.
        """
        # Render template variables
        rendered_command = self._render_templates(self.bash_command, context)
        log(f"Executing: {rendered_command}", level="INFO")

        # Build environment
        run_env = os.environ.copy()
        if self.env:
            run_env.update(self.env)

        # Calculate timeout
        timeout_seconds = _parse_duration(self.timeout) if self.timeout else None

        # Run the command
        result = subprocess.run(
            rendered_command,
            shell=True,
            capture_output=True,
            text=True,
            env=run_env,
            cwd=self.cwd,
            timeout=timeout_seconds,
        )

        # Emit stderr as log messages
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                log(line, level="WARNING")

        # Check return code
        if result.returncode != 0:
            log(
                f"Command failed with return code {result.returncode}",
                level="ERROR",
            )
            result.check_returncode()

        stdout = result.stdout.rstrip()
        log(f"Command completed, output length: {len(stdout)} chars", level="INFO")
        return stdout

    @staticmethod
    def _render_templates(command: str, context: Optional[TaskContext]) -> str:
        """Replace ``{{ variable }}`` placeholders with context values.

        Args:
            command: The command string with template variables.
            context: The execution context to pull values from.

        Returns:
            The rendered command string.
        """
        if context is None:
            return command

        # Build template variable map
        template_vars: Dict[str, str] = {
            "dag_id": context.dag_id,
            "task_id": context.task_id,
            "run_id": context.run_id,
            "attempt": str(context.attempt),
        }

        # Add date-related variables
        if context.logical_date:
            template_vars["ds"] = context.logical_date.strftime("%Y-%m-%d")
            template_vars["ds_nodash"] = context.logical_date.strftime("%Y%m%d")
            template_vars["ts"] = context.logical_date.isoformat()
        else:
            template_vars["ds"] = "no-date"
            template_vars["ds_nodash"] = "nodate"
            template_vars["ts"] = "no-timestamp"

        # Replace {{ var }} patterns
        def _replace(match: re.Match) -> str:
            var_name = match.group(1).strip()
            return template_vars.get(var_name, match.group(0))

        return re.sub(r"\{\{\s*(\w+)\s*\}\}", _replace, command)
