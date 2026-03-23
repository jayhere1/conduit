"""
Filesystem hook — access local or mounted file paths.

The connection's ``host`` field (or ``extra.base_path``) specifies the root
directory. All operations are relative to that root.

Usage:
    import os
    os.environ["CONDUIT_CONN_DATA_DIR"] = '{"conn_type":"filesystem","host":"/data/warehouse"}'

    from conduit_sdk.hooks.filesystem import FileSystemHook

    hook = FileSystemHook("data_dir")
    print(hook.get_path())           # "/data/warehouse"
    files = hook.list_files("*.csv") # ["/data/warehouse/orders.csv", ...]
"""

from __future__ import annotations

import glob as glob_module
import os
from typing import List

from conduit_sdk.hooks.base import BaseHook


class FileSystemHook(BaseHook):
    """Hook for filesystem operations.

    Resolves a base path from the connection and provides helpers for
    listing and accessing files.

    Args:
        connection_id: The connection identifier to resolve from environment.

    Example:
        hook = FileSystemHook("data_lake")
        for csv_file in hook.list_files("**/*.csv"):
            print(f"Found: {csv_file}")
    """

    def get_path(self) -> str:
        """Return the base filesystem path from the connection.

        The path is read from (in order of priority):
        1. ``extra["base_path"]``
        2. ``host``
        3. ``schema``

        Returns:
            The resolved base path as a string.

        Raises:
            ValueError: If no path can be determined from the connection.
        """
        conn = self.get_connection()
        path = conn.extra.get("base_path") or conn.host or conn.schema
        if not path:
            raise ValueError(
                f"Connection '{self.connection_id}' has no path configured. "
                f"Set 'host', 'schema', or extra.base_path."
            )
        return path

    def list_files(self, glob_pattern: str = "*") -> List[str]:
        """List files matching a glob pattern relative to the base path.

        Args:
            glob_pattern: A glob pattern (e.g., ``"*.csv"``, ``"**/*.parquet"``).
                Uses Python's ``glob.glob`` with ``recursive=True``.

        Returns:
            A sorted list of absolute file paths matching the pattern.

        Example:
            files = hook.list_files("2026/**/*.parquet")
        """
        base = self.get_path()
        full_pattern = os.path.join(base, glob_pattern)
        return sorted(glob_module.glob(full_pattern, recursive=True))

    def exists(self, relative_path: str) -> bool:
        """Check if a file or directory exists relative to the base path.

        Args:
            relative_path: Path relative to the base directory.

        Returns:
            True if the path exists.
        """
        base = self.get_path()
        return os.path.exists(os.path.join(base, relative_path))
