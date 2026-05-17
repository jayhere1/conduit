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
        assert_eq!(AlertStatus::from_run_status(RunStatus::Failed), Some(AlertStatus::Failed));
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
}
