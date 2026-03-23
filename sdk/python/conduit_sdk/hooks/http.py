"""
HTTP hook — makes HTTP requests using a Conduit connection for base URL and auth.

Usage:
    from conduit_sdk.hooks.http import HttpHook

    # Set up connection: CONDUIT_CONN_MY_API='http://token@api.example.com/v1'
    hook = HttpHook("my_api")
    response = hook.run("/users", method="GET")
    print(response.status_code, response.json())

    # POST with JSON body
    response = hook.run(
        "/users",
        method="POST",
        data={"name": "Alice"},
        headers={"Content-Type": "application/json"},
    )
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import json

from conduit_sdk.hooks.base import BaseHook


class HttpResponse:
    """Lightweight response wrapper that works without the ``requests`` library.

    When the ``requests`` library is available, :meth:`HttpHook.run` returns a
    real ``requests.Response``. Otherwise this stdlib-based wrapper is returned.

    Attributes:
        status_code: HTTP status code.
        text: Response body as a string.
        headers: Response headers as a dict.
    """

    def __init__(self, status_code: int, text: str, headers: Optional[Dict[str, str]] = None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}

    def json(self) -> Any:
        """Parse the response body as JSON.

        Returns:
            Parsed JSON data.

        Raises:
            json.JSONDecodeError: If the body is not valid JSON.
        """
        return json.loads(self.text)

    def raise_for_status(self) -> None:
        """Raise an exception if the status code indicates an error (>= 400)."""
        if self.status_code >= 400:
            raise RuntimeError(
                f"HTTP {self.status_code}: {self.text[:200]}"
            )

    def __repr__(self) -> str:
        return f"<HttpResponse [{self.status_code}]>"


class HttpHook(BaseHook):
    """Hook for making HTTP requests against a configured endpoint.

    The connection provides the base URL, and optionally authentication
    credentials. The ``login`` field is used as a bearer token if set.

    Args:
        connection_id: The connection identifier to resolve from environment.

    Example:
        hook = HttpHook("my_api")
        resp = hook.run("/health")
        assert resp.status_code == 200
    """

    def run(
        self,
        endpoint: str,
        method: str = "GET",
        data: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        """Make an HTTP request.

        Attempts to use the ``requests`` library if available; falls back to
        ``urllib`` from the standard library.

        Args:
            endpoint: URL path to append to the base URL (e.g., "/users").
            method: HTTP method (GET, POST, PUT, DELETE, PATCH).
            data: Request body. Dicts are JSON-encoded automatically.
            headers: Additional HTTP headers.

        Returns:
            A ``requests.Response`` if the requests library is installed,
            otherwise an :class:`HttpResponse` wrapper.

        Example:
            resp = hook.run("/api/data", method="POST", data={"key": "value"})
            print(resp.json())
        """
        conn = self.get_connection()

        # Build base URL
        scheme = "https" if conn.extra.get("ssl", "").lower() in ("true", "1") else "http"
        if conn.host.startswith("http://") or conn.host.startswith("https://"):
            base_url = conn.host.rstrip("/")
        else:
            port_part = f":{conn.port}" if conn.port else ""
            base_url = f"{scheme}://{conn.host}{port_part}"

        if conn.schema:
            base_url = f"{base_url}/{conn.schema.strip('/')}"

        url = f"{base_url}/{endpoint.lstrip('/')}" if endpoint else base_url

        # Build headers
        merged_headers = {}
        if conn.login:
            merged_headers["Authorization"] = f"Bearer {conn.login}"
        if conn.password and not conn.login:
            merged_headers["Authorization"] = f"Bearer {conn.password}"
        if headers:
            merged_headers.update(headers)

        # Try requests library first
        try:
            import requests as requests_lib
            return self._run_with_requests(
                requests_lib, url, method, data, merged_headers
            )
        except ImportError:
            return self._run_with_urllib(url, method, data, merged_headers)

    @staticmethod
    def _run_with_requests(
        requests_lib: Any,
        url: str,
        method: str,
        data: Optional[Any],
        headers: Dict[str, str],
    ) -> Any:
        """Execute request using the ``requests`` library."""
        kwargs: dict[str, Any] = {"headers": headers}
        if data is not None:
            if isinstance(data, (dict, list)):
                kwargs["json"] = data
            else:
                kwargs["data"] = data

        return requests_lib.request(method, url, **kwargs)

    @staticmethod
    def _run_with_urllib(
        url: str,
        method: str,
        data: Optional[Any],
        headers: Dict[str, str],
    ) -> HttpResponse:
        """Execute request using stdlib ``urllib``."""
        body = None
        if data is not None:
            if isinstance(data, (dict, list)):
                body = json.dumps(data).encode("utf-8")
                headers.setdefault("Content-Type", "application/json")
            elif isinstance(data, str):
                body = data.encode("utf-8")
            elif isinstance(data, bytes):
                body = data

        req = Request(url, data=body, headers=headers, method=method.upper())

        try:
            with urlopen(req) as resp:
                text = resp.read().decode("utf-8")
                resp_headers = {k: v for k, v in resp.getheaders()}
                return HttpResponse(
                    status_code=resp.status,
                    text=text,
                    headers=resp_headers,
                )
        except HTTPError as exc:
            text = exc.read().decode("utf-8") if exc.fp else ""
            return HttpResponse(
                status_code=exc.code,
                text=text,
                headers={},
            )
