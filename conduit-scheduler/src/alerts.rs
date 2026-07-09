//! Alert hook surface for internal teams to wire DAG-failure notifications
//! into their existing alerting (PagerDuty, Slack, Opsgenie, etc.).
//!
//! Closes the last Bet 3 item in `docs/STRATEGIC_DIRECTION.md` —
//! observability finished — by giving the scheduler an extension point for
//! "this DAG run failed" callbacks. The trait is intentionally small: one
//! method, fire-and-forget, async so impls can do network I/O without
//! blocking the scheduler event loop.
//!
//! No transport impl ships in-tree. Internal teams plug their own
//! `impl AlertHook` (webhook, Slack, internal queue, …) via
//! `Scheduler::with_alert_hook`. The strategic plan calls this out as the
//! intentional shape — "wire to your existing alerting" rather than ship a
//! one-size-fits-all notifier.

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use conduit_common::dag::{DagId, TaskId};
use serde::{Deserialize, Serialize};

use crate::scheduler::RunStatus;

/// One terminal-state snapshot of a DAG run, handed to every registered
/// `AlertHook` when a run reaches a non-success terminal state.
///
/// Carries everything an alert recipient typically wants without forcing the
/// hook impl to re-query state: dag id, run id, status, when it started /
/// ended, the failed tasks with their last error, and any free-form run
/// config the trigger included.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub dag_id: DagId,
    pub run_id: String,
    pub status: AlertStatus,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    /// `(task_id, last_error_message)` pairs for tasks that ended in
    /// `Failed`. Empty for `Cancelled` runs that had no task-level failure.
    pub failed_tasks: Vec<(TaskId, String)>,
    /// Free-form run config the trigger included (e.g. backfill window,
    /// triggering user). Pass-through; the hook impl decides what to surface.
    #[serde(default)]
    pub config: HashMap<String, String>,
}

/// Terminal status the alert fired for. Mirrors `RunStatus` minus `Success`
/// (which never fires alerts) so impls have a closed set without needing to
/// handle a "this shouldn't happen" variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertStatus {
    Failed,
    Cancelled,
}

impl AlertStatus {
    /// Map a `RunStatus` to an `AlertStatus`. Returns `None` for `Success`
    /// since alerts only fire on non-success terminal states; callers should
    /// skip firing rather than build an event the trait can't represent.
    pub fn from_run_status(s: RunStatus) -> Option<Self> {
        match s {
            RunStatus::Failed => Some(Self::Failed),
            RunStatus::Cancelled => Some(Self::Cancelled),
            RunStatus::Success => None,
        }
    }
}

/// Pluggable callback the scheduler invokes for each non-success DAG run.
///
/// Implementations are expected to be cheap to clone (the scheduler holds
/// them behind `Arc`) and the `fire` method must be non-blocking in the
/// "doesn't tie up the scheduler" sense — the scheduler spawns each hook on
/// the tokio runtime so a slow PagerDuty POST doesn't stall task dispatch.
///
/// Errors from `fire` are logged at the scheduler boundary and otherwise
/// swallowed — a failed alert must never propagate up and stop a healthy
/// scheduler. If you need delivery guarantees, layer a durable queue
/// inside your impl rather than returning an error and hoping a retry layer
/// exists.
#[async_trait]
pub trait AlertHook: Send + Sync + 'static {
    async fn fire(&self, event: &AlertEvent) -> Result<(), String>;

    /// Short human-readable label for log lines / metrics ("pagerduty",
    /// "slack-#oncall", "internal-queue"). Defaults to the type name.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// `AlertHook` that POSTs the `AlertEvent` JSON to a webhook URL. The
/// default transport for `Dag.on_failure` URLs configured on individual
/// DAGs — internal teams who want PagerDuty / Slack / Opsgenie wire
/// their own `impl AlertHook`, but a plain HTTP POST is the lowest
/// common denominator and finally makes the long-parsed
/// `Dag.on_failure` field do something.
///
/// Timeouts and retries: a single POST with the configured timeout. No
/// in-trait retry — alert delivery is fire-and-forget by design (the
/// trait contract is "swallowed on error"). If you need durable
/// delivery, wrap a queue in your own `impl AlertHook` rather than
/// piling retry logic into this struct.
pub struct WebhookAlertHook {
    url: String,
    client: reqwest::Client,
}

impl WebhookAlertHook {
    /// Default request timeout for the webhook POST. Picked to be
    /// short enough that a hung receiver can't queue up tokio tasks
    /// indefinitely, but long enough to survive a slow TLS handshake.
    pub const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    /// Build a webhook hook from a URL. Errors if the reqwest client
    /// can't be built — usually a TLS config issue at link time.
    pub fn new(url: impl Into<String>) -> Result<Self, reqwest::Error> {
        Self::with_timeout(url, Self::DEFAULT_TIMEOUT)
    }

    /// Like `new` but with an explicit timeout. Use for tests that
    /// shouldn't wait `DEFAULT_TIMEOUT` seconds for a fake server to
    /// hang.
    pub fn with_timeout(
        url: impl Into<String>,
        timeout: std::time::Duration,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder().timeout(timeout).build()?;
        Ok(Self {
            url: url.into(),
            client,
        })
    }
}

#[async_trait]
impl AlertHook for WebhookAlertHook {
    async fn fire(&self, event: &AlertEvent) -> Result<(), String> {
        // serde_json::to_value is infallible for `AlertEvent` since it's
        // entirely Serialize-derive — `.expect` is the documented use.
        let resp = self
            .client
            .post(&self.url)
            .json(event)
            .send()
            .await
            .map_err(|e| format!("POST {} failed: {}", self.url, e))?;
        if !resp.status().is_success() {
            return Err(format!(
                "POST {} returned non-2xx status {}",
                self.url,
                resp.status()
            ));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "webhook"
    }
}

/// Wraps another `AlertHook` so it only fires for events whose `dag_id`
/// matches. Used to scope a `WebhookAlertHook` built from a single
/// `Dag.on_failure` URL to that DAG only — without this, every webhook
/// would receive every DAG's failure.
pub struct ScopedHook<H: AlertHook> {
    dag_id: conduit_common::dag::DagId,
    inner: H,
}

impl<H: AlertHook> ScopedHook<H> {
    pub fn new(dag_id: impl Into<conduit_common::dag::DagId>, inner: H) -> Self {
        Self {
            dag_id: dag_id.into(),
            inner,
        }
    }
}

#[async_trait]
impl<H: AlertHook> AlertHook for ScopedHook<H> {
    async fn fire(&self, event: &AlertEvent) -> Result<(), String> {
        if event.dag_id != self.dag_id {
            return Ok(()); // Not our DAG; silently no-op.
        }
        self.inner.fire(event).await
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Test fixture that records every event it was fired with, plus a
    /// counter so tests can assert "fired N times" without re-locking.
    #[derive(Default, Clone)]
    pub struct RecordingHook {
        events: Arc<Mutex<Vec<AlertEvent>>>,
    }

    impl RecordingHook {
        pub fn new() -> Self {
            Self::default()
        }
        pub fn calls(&self) -> Vec<AlertEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl AlertHook for RecordingHook {
        async fn fire(&self, event: &AlertEvent) -> Result<(), String> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        fn name(&self) -> &'static str {
            "recording-hook"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alert_status_skips_success() {
        assert_eq!(
            AlertStatus::from_run_status(RunStatus::Failed),
            Some(AlertStatus::Failed)
        );
        assert_eq!(
            AlertStatus::from_run_status(RunStatus::Cancelled),
            Some(AlertStatus::Cancelled)
        );
        assert_eq!(AlertStatus::from_run_status(RunStatus::Success), None);
    }

    #[tokio::test]
    async fn recording_hook_captures_event() {
        let hook = test_helpers::RecordingHook::new();
        let event = AlertEvent {
            dag_id: "etl".to_string(),
            run_id: "r1".to_string(),
            status: AlertStatus::Failed,
            started_at: Utc::now(),
            completed_at: Utc::now(),
            failed_tasks: vec![("transform".to_string(), "boom".to_string())],
            config: HashMap::new(),
        };
        hook.fire(&event).await.unwrap();
        let calls = hook.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].failed_tasks[0].1, "boom");
    }

    fn sample_event(dag_id: &str) -> AlertEvent {
        AlertEvent {
            dag_id: dag_id.to_string(),
            run_id: "r1".to_string(),
            status: AlertStatus::Failed,
            started_at: Utc::now(),
            completed_at: Utc::now(),
            failed_tasks: vec![("t".to_string(), "boom".to_string())],
            config: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn scoped_hook_filters_by_dag_id() {
        // The scoped wrapper around a recording hook only fires for the
        // configured dag_id. Other DAGs' events flow past it without
        // being recorded.
        let inner = test_helpers::RecordingHook::new();
        let scoped = ScopedHook::new("etl".to_string(), inner.clone());

        scoped.fire(&sample_event("etl")).await.unwrap();
        scoped.fire(&sample_event("other")).await.unwrap();

        let calls = inner.calls();
        assert_eq!(calls.len(), 1, "only the matching dag_id should fire");
        assert_eq!(calls[0].dag_id, "etl");
    }

    // The webhook round-trip is an in-process listener so the test runs
    // anywhere without external infra. Binds to 127.0.0.1:0, accepts one
    // connection, parses the HTTP body, and returns 204. Captures the
    // request body back through a oneshot channel so the test asserts
    // on what the webhook actually sent.
    async fn one_shot_webhook_server() -> (String, tokio::sync::oneshot::Receiver<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{}/webhook", addr);
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();

        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::with_capacity(2048);
            // Read until we've consumed the request body. HTTP/1.1 with a
            // small POST body — we just slurp what's available; reqwest
            // sends the body in the same write as the headers for small
            // payloads, so a single read is enough in practice.
            let mut chunk = [0u8; 1024];
            loop {
                let n = sock.read(&mut chunk).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                // Naive end-of-body detection: stop once we see the
                // closing `}` after the headers. Good enough for a
                // JSON payload under a few KiB.
                if buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.last().copied() == Some(b'}') {
                    break;
                }
            }
            // Split off the body (after the first CRLFCRLF).
            let body = match buf.windows(4).position(|w| w == b"\r\n\r\n") {
                Some(p) => String::from_utf8_lossy(&buf[p + 4..]).into_owned(),
                None => String::new(),
            };
            let _ = tx.send(body);
            let _ = sock
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                .await;
        });

        (url, rx)
    }

    #[tokio::test]
    async fn webhook_hook_posts_event_as_json() {
        let (url, rx) = one_shot_webhook_server().await;
        let hook = WebhookAlertHook::with_timeout(url, std::time::Duration::from_secs(5)).unwrap();
        let event = sample_event("etl");

        hook.fire(&event).await.unwrap();

        let body = rx.await.expect("server should receive the POST body");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("body must be JSON");
        assert_eq!(parsed["dag_id"], "etl");
        assert_eq!(parsed["run_id"], "r1");
        assert_eq!(parsed["status"], "failed");
        assert_eq!(parsed["failed_tasks"][0][0], "t");
    }

    #[tokio::test]
    async fn webhook_hook_reports_error_on_unreachable_url() {
        // Bound but unreachable — we never accept the connection so the
        // POST times out (or refuses, depending on the platform).
        // Either way the hook returns Err, which is the contract.
        let hook = WebhookAlertHook::with_timeout(
            "http://127.0.0.1:1/never_listens".to_string(),
            std::time::Duration::from_millis(200),
        )
        .unwrap();
        let event = sample_event("etl");
        let err = hook.fire(&event).await.unwrap_err();
        assert!(
            err.contains("POST") && err.contains("never_listens"),
            "error message should mention the URL: {}",
            err
        );
    }
}
