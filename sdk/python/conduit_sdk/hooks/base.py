"""
Base hook and Connection dataclass for Conduit.

Connections are resolved from environment variables following this convention:

    CONDUIT_CONN_{CONN_ID}

The value can be either:
1. A JSON object::

    CONDUIT_CONN_MY_DB='{"conn_type":"postgres","host":"localhost","port":5432,
                         "schema":"mydb","login":"user","password":"secret"}'

2. A URI string::

    CONDUIT_CONN_MY_DB='postgres://user:secret@localhost:5432/mydb'

Usage:
    from conduit_sdk.hooks.base import BaseHook, Connection

    hook = BaseHook("my_db")
    conn = hook.get_connection()
    print(conn.host, conn.port)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import urlparse, parse_qs, unquote


@dataclass
class Connection:
    """Represents a connection to an external system.

    Attributes:
        conn_id: Unique identifier for this connection.
        conn_type: Type of the connection (e.g., "postgres", "mysql", "http").
        host: Hostname or IP address.
        port: Port number (0 means unset / use default).
        schema: Database or schema name.
        login: Username for authentication.
        password: Password for authentication.
        extra: Additional parameters as a dictionary.

    Example:
        conn = Connection(
            conn_id="my_pg",
            conn_type="postgres",
            host="db.example.com",
            port=5432,
            schema="analytics",
            login="etl_user",
            password="s3cret",
            extra={"sslmode": "require"},
        )
    """

    conn_id: str = ""
    conn_type: str = ""
    host: str = ""
    port: int = 0
    schema: str = ""
    login: str = ""
    password: str = ""
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_json(cls, conn_id: str, raw: str) -> "Connection":
        """Parse a Connection from a JSON string.

        Args:
            conn_id: The connection identifier.
            raw: JSON string with connection parameters.

        Returns:
            A populated Connection instance.

        Raises:
            ValueError: If the JSON cannot be parsed.
        """
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Cannot parse connection JSON for '{conn_id}': {exc}")

        extra = data.get("extra", {})
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except json.JSONDecodeError:
                extra = {}

        return cls(
            conn_id=conn_id,
            conn_type=data.get("conn_type", ""),
            host=data.get("host", ""),
            port=int(data.get("port", 0)),
            schema=data.get("schema", ""),
            login=data.get("login", ""),
            password=data.get("password", ""),
            extra=extra,
        )

    @classmethod
    def from_uri(cls, conn_id: str, uri: str) -> "Connection":
        """Parse a Connection from a URI string.

        Supported formats::

            postgres://user:pass@host:5432/dbname
            mysql://user:pass@host:3306/dbname?charset=utf8
            http://token@api.example.com/v1
            sqlite:///path/to/db.sqlite

        Args:
            conn_id: The connection identifier.
            uri: URI string.

        Returns:
            A populated Connection instance.
        """
        parsed = urlparse(uri)

        # Map common scheme names to conn_type
        scheme_map = {
            "postgresql": "postgres",
            "postgresql+psycopg2": "postgres",
            "postgres": "postgres",
            "mysql": "mysql",
            "mysql+pymysql": "mysql",
            "sqlite": "sqlite",
            "http": "http",
            "https": "http",
            "s3": "s3",
            "snowflake": "snowflake",
            "bigquery": "bigquery",
        }
        conn_type = scheme_map.get(parsed.scheme, parsed.scheme)

        # Parse query params into extra dict
        extra = {}
        if parsed.query:
            qs = parse_qs(parsed.query)
            extra = {k: v[0] if len(v) == 1 else v for k, v in qs.items()}

        return cls(
            conn_id=conn_id,
            conn_type=conn_type,
            host=parsed.hostname or "",
            port=parsed.port or 0,
            schema=parsed.path.lstrip("/") if parsed.path else "",
            login=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            extra=extra,
        )


class BaseHook:
    """Base class for all Conduit hooks.

    Resolves a :class:`Connection` from environment variables using the
    ``CONDUIT_CONN_{CONN_ID}`` convention. Subclasses add domain-specific
    behaviour (database queries, HTTP requests, etc.).

    Args:
        connection_id: The identifier for the connection to resolve.

    Example:
        hook = BaseHook("my_postgres")
        conn = hook.get_connection()
        print(f"Connecting to {conn.host}:{conn.port}/{conn.schema}")
    """

    def __init__(self, connection_id: str):
        self.connection_id = connection_id
        self._connection: Optional[Connection] = None

    def get_connection(self) -> Connection:
        """Resolve and return the Connection for this hook.

        The connection is cached after the first resolution.

        Returns:
            A :class:`Connection` instance.

        Raises:
            ValueError: If the environment variable is not set or cannot be parsed.
        """
        if self._connection is not None:
            return self._connection

        env_key = f"CONDUIT_CONN_{self.connection_id.upper()}"
        raw = os.environ.get(env_key)

        if raw is None:
            raise ValueError(
                f"Connection '{self.connection_id}' not found. "
                f"Set the environment variable {env_key} as JSON or URI."
            )

        raw = raw.strip()

        # Decide JSON vs URI by checking if it starts with '{'
        if raw.startswith("{"):
            self._connection = Connection.from_json(self.connection_id, raw)
        else:
            self._connection = Connection.from_uri(self.connection_id, raw)

        return self._connection
