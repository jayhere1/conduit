"""
Conduit hooks — connection management for external systems.

Hooks manage connections to databases, HTTP APIs, filesystems, and other
external services. Connections are resolved from environment variables
using the naming convention ``CONDUIT_CONN_{CONN_ID}``.

Usage:
    from conduit_sdk.hooks import Connection, BaseHook, DatabaseHook, HttpHook

    # Resolve a connection from env
    hook = DatabaseHook("my_postgres")
    rows = hook.run("SELECT * FROM orders WHERE id = %s", parameters=(42,))

    # Use the HTTP hook
    http = HttpHook("my_api")
    response = http.run("/users", method="GET")
"""

from conduit_sdk.hooks.base import BaseHook, Connection
from conduit_sdk.hooks.database import DatabaseHook
from conduit_sdk.hooks.http import HttpHook
from conduit_sdk.hooks.filesystem import FileSystemHook

__all__ = [
    "BaseHook",
    "Connection",
    "DatabaseHook",
    "HttpHook",
    "FileSystemHook",
]
