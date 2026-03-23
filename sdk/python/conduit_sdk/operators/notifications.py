"""
Notification operators — send alerts via Slack and email.

Usage:
    from conduit_sdk.operators import SlackNotifyOperator, EmailOperator

    # Send a Slack message
    slack = SlackNotifyOperator(
        task_id="notify_success",
        channel="#data-alerts",
        message="Daily ETL completed successfully!",
        webhook_url="https://hooks.slack.com/services/...",
    )
    slack.run()

    # Send an email
    email = EmailOperator(
        task_id="send_report",
        to=["team@example.com"],
        subject="Daily Report",
        body="<h1>Report</h1><p>All checks passed.</p>",
        connection_id="smtp_default",
    )
    email.run()
"""

from __future__ import annotations

import json
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Union

from conduit_sdk.context import TaskContext
from conduit_sdk.xcom import log
from conduit_sdk.operators.base import BaseOperator


class SlackNotifyOperator(BaseOperator):
    """Operator that sends a message to a Slack channel via webhook.

    The webhook URL can be provided directly or resolved from a Conduit
    connection (``CONDUIT_CONN_{connection_id}``).

    Args:
        task_id: Unique identifier for this task.
        channel: Slack channel name (e.g., "#alerts").
        message: The message text to send.
        webhook_url: Slack webhook URL. If not provided, resolved from
            ``connection_id``.
        connection_id: Connection identifier for the Slack webhook.
            Default "slack_default".
        **task_kwargs: Standard task kwargs.

    Example:
        op = SlackNotifyOperator(
            task_id="alert",
            channel="#data-team",
            message="Pipeline failed!",
            webhook_url="https://hooks.slack.com/services/T00/B00/xxx",
        )
    """

    def __init__(
        self,
        task_id: str,
        channel: str,
        message: str,
        webhook_url: Optional[str] = None,
        connection_id: str = "slack_default",
        **task_kwargs: Any,
    ):
        super().__init__(task_id=task_id, **task_kwargs)
        self.channel = channel
        self.message = message
        self.webhook_url = webhook_url
        self.connection_id = connection_id

    def execute(self, context: Optional[TaskContext] = None) -> dict:
        """Send the Slack message.

        Args:
            context: The task execution context.

        Returns:
            A dict with ``status`` and ``channel`` keys.

        Raises:
            ValueError: If no webhook URL can be resolved.
        """
        url = self._resolve_webhook_url()
        payload = {
            "channel": self.channel,
            "text": self.message,
        }

        log(f"Sending Slack message to {self.channel}", level="INFO")

        self._post_webhook(url, payload)

        log(f"Slack message sent to {self.channel}", level="INFO")
        return {"status": "sent", "channel": self.channel}

    def _resolve_webhook_url(self) -> str:
        """Resolve the webhook URL from direct config or connection."""
        if self.webhook_url:
            return self.webhook_url

        # Try to resolve from connection
        from conduit_sdk.hooks.base import BaseHook

        try:
            hook = BaseHook(self.connection_id)
            conn = hook.get_connection()
            url = conn.extra.get("webhook_url") or conn.host
            if url:
                return url
        except ValueError:
            pass

        raise ValueError(
            f"No Slack webhook URL provided and connection "
            f"'{self.connection_id}' not found."
        )

    @staticmethod
    def _post_webhook(url: str, payload: dict) -> None:
        """POST JSON to a webhook URL."""
        try:
            import requests
            resp = requests.post(url, json=payload, timeout=30)
            resp.raise_for_status()
        except ImportError:
            from urllib.request import Request, urlopen

            data = json.dumps(payload).encode("utf-8")
            req = Request(
                url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urlopen(req, timeout=30)


class EmailOperator(BaseOperator):
    """Operator that sends an email via SMTP.

    The SMTP connection is resolved from the ``CONDUIT_CONN_{connection_id}``
    environment variable. The connection should have:
    - ``host``: SMTP server hostname
    - ``port``: SMTP port (587 for TLS, 465 for SSL, 25 for plain)
    - ``login``: SMTP username
    - ``password``: SMTP password
    - ``extra.use_tls``: "true" to use STARTTLS (default)

    Args:
        task_id: Unique identifier for this task.
        to: Recipient email address(es).
        subject: Email subject line.
        body: Email body (plain text or HTML).
        connection_id: Connection identifier. Default "smtp_default".
        html: Whether the body is HTML (default True if body contains HTML tags).
        from_email: Sender email. Defaults to the connection's login.
        **task_kwargs: Standard task kwargs.

    Example:
        op = EmailOperator(
            task_id="send_alert",
            to=["oncall@example.com"],
            subject="Pipeline Alert",
            body="The daily ETL has failed.",
            connection_id="smtp_default",
        )
    """

    def __init__(
        self,
        task_id: str,
        to: Union[str, List[str]],
        subject: str,
        body: str,
        connection_id: str = "smtp_default",
        html: Optional[bool] = None,
        from_email: Optional[str] = None,
        **task_kwargs: Any,
    ):
        super().__init__(task_id=task_id, **task_kwargs)
        self.to = [to] if isinstance(to, str) else to
        self.subject = subject
        self.body = body
        self.connection_id = connection_id
        self.from_email = from_email

        # Auto-detect HTML
        if html is None:
            self.html = "<" in body and ">" in body
        else:
            self.html = html

    def execute(self, context: Optional[TaskContext] = None) -> dict:
        """Send the email.

        Args:
            context: The task execution context.

        Returns:
            A dict with ``status``, ``to``, and ``subject`` keys.
        """
        from conduit_sdk.hooks.base import BaseHook

        hook = BaseHook(self.connection_id)
        conn = hook.get_connection()

        from_addr = self.from_email or conn.login
        port = conn.port or 587
        use_tls = conn.extra.get("use_tls", "true").lower() in ("true", "1", "yes")

        # Build MIME message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = self.subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(self.to)

        content_type = "html" if self.html else "plain"
        msg.attach(MIMEText(self.body, content_type))

        log(f"Sending email to {self.to} via {conn.host}:{port}", level="INFO")

        # Send
        if port == 465:
            server = smtplib.SMTP_SSL(conn.host, port)
        else:
            server = smtplib.SMTP(conn.host, port)
            if use_tls:
                server.starttls()

        try:
            if conn.login and conn.password:
                server.login(conn.login, conn.password)
            server.sendmail(from_addr, self.to, msg.as_string())
        finally:
            server.quit()

        log(f"Email sent to {self.to}", level="INFO")
        return {"status": "sent", "to": self.to, "subject": self.subject}
