//! Property-based tests for the event store.
//!
//! Verifies:
//! - Append N events -> read back exactly N events
//! - Sequence numbers are always monotonically increasing
//! - Events survive close + reopen (persistence)

use conduit_common::event::{EventKind, RunStatus};
use conduit_state::EventStore;
use proptest::prelude::*;
use tempfile::TempDir;

fn make_event(i: u32) -> EventKind {
    if i.is_multiple_of(3) {
        EventKind::DagRunCreated {
            dag_id: format!("dag_{}", i),
            run_id: format!("run_{}", i),
            logical_date: chrono::Utc::now(),
            environment: "test".to_string(),
            triggered_by: "proptest".to_string(),
        }
    } else if i % 3 == 1 {
        EventKind::TaskCompleted {
            dag_id: format!("dag_{}", i),
            run_id: format!("run_{}", i),
            task_id: format!("task_{}", i),
            duration_ms: i as u64 * 100,
            snapshot_id: None,
        }
    } else {
        EventKind::DagRunCompleted {
            dag_id: format!("dag_{}", i),
            run_id: format!("run_{}", i),
            status: RunStatus::Success,
            duration_ms: i as u64 * 50,
        }
    }
}

proptest! {
    /// Appending N events and reading them back yields exactly N events.
    #[test]
    fn append_n_read_back_n(n in 1u32..50) {
        let tmp = TempDir::new().unwrap();
        let store = EventStore::open(tmp.path()).unwrap();

        for i in 0..n {
            store.append(make_event(i)).unwrap();
        }

        let events = store.range(1, n as u64).unwrap();
        prop_assert_eq!(events.len() as u32, n);
    }

    /// Sequence numbers are always monotonically increasing.
    #[test]
    fn sequences_are_monotonic(n in 2u32..30) {
        let tmp = TempDir::new().unwrap();
        let store = EventStore::open(tmp.path()).unwrap();

        let mut last_seq = 0u64;
        for i in 0..n {
            let event = store.append(make_event(i)).unwrap();
            prop_assert!(event.sequence > last_seq, "seq {} should be > {}", event.sequence, last_seq);
            last_seq = event.sequence;
        }
    }

    /// Events survive close + reopen.
    #[test]
    fn persistence_across_reopen(n in 1u32..20) {
        let tmp = TempDir::new().unwrap();

        // Write events
        {
            let store = EventStore::open(tmp.path()).unwrap();
            for i in 0..n {
                store.append(make_event(i)).unwrap();
            }
        }

        // Reopen and read
        {
            let store = EventStore::open(tmp.path()).unwrap();
            let events = store.range(1, n as u64).unwrap();
            prop_assert_eq!(events.len() as u32, n);

            // Verify sequence numbers are 1..=n
            for (idx, event) in events.iter().enumerate() {
                prop_assert_eq!(event.sequence, (idx + 1) as u64);
            }
        }
    }

    /// Appending after reopen continues from the correct sequence.
    #[test]
    fn sequence_continues_after_reopen(n1 in 1u32..15, n2 in 1u32..15) {
        let tmp = TempDir::new().unwrap();

        // Write first batch
        {
            let store = EventStore::open(tmp.path()).unwrap();
            for i in 0..n1 {
                store.append(make_event(i)).unwrap();
            }
        }

        // Reopen and write second batch
        {
            let store = EventStore::open(tmp.path()).unwrap();
            let first_new = store.append(make_event(n1)).unwrap();
            prop_assert_eq!(first_new.sequence, n1 as u64 + 1);

            for i in (n1 + 1)..(n1 + n2) {
                store.append(make_event(i)).unwrap();
            }
        }

        // Verify total count
        {
            let store = EventStore::open(tmp.path()).unwrap();
            let total = n1 + n2;
            let events = store.range(1, total as u64).unwrap();
            prop_assert_eq!(events.len() as u32, total);
        }
    }
}
