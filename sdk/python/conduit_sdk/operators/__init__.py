"""
Conduit operators — reusable task implementations.

Operators provide pre-built task logic for common patterns: running Python
callables, executing shell commands, querying databases, polling for
conditions, and sending notifications.

Usage:
    from conduit_sdk.operators import (
        PythonOperator,
        BashOperator,
        SQLOperator,
        Sensor,
        FileSensor,
        HttpSensor,
        SqlSensor,
        SlackNotifyOperator,
        EmailOperator,
    )
"""

from conduit_sdk.operators.python_operator import PythonOperator
from conduit_sdk.operators.bash_operator import BashOperator
from conduit_sdk.operators.sql_operator import SQLOperator
from conduit_sdk.operators.sensor import Sensor, FileSensor, HttpSensor, SqlSensor
from conduit_sdk.operators.notifications import SlackNotifyOperator, EmailOperator

__all__ = [
    "PythonOperator",
    "BashOperator",
    "SQLOperator",
    "Sensor",
    "FileSensor",
    "HttpSensor",
    "SqlSensor",
    "SlackNotifyOperator",
    "EmailOperator",
]
