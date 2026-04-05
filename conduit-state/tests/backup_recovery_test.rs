//! Backup and recovery tests for the EventStore.
//!
//! These tests verify that events can be exported to JSON Lines format,
//! imported into a fresh store, and that sequence ordering and range
//! queries are preserved across the export/import cycle.

use std::io::{BufRead, BufReader, Write};

use chrono::Utc;
use conduit_common::event::{Event, EventKind, RunStatus};
use conduit_state::event_store::EventStore;
use tempfile::tempdir;

// ── Helpers ──────────────────────────────────────────────────────────────

/// Generate a mix of event kinds cycling through DagRunCreated, TaskStarted,
/// TaskCompleted, and DagRunCompleted.
fn make_mixed_event_kind(index: usize) -> EventKind {
    let dag_id = format!("dag_{}", index / 4);
    let run_id = format!("run_{}", index / 4);
    let task_id = format!("task_{}", index);

    match index % 4 {
        0 => EventKind::DagRunCreated {
            dag_id,
            run_id,
            logical_date: Utc::now(),
            environment: "production".to_string(),
            triggered_by: "scheduler".to_string(),
        },
        1 => EventKind::TaskStarted {
            dag_id,
            run_id,
            task_id,
            worker_id: "worker-1".to_string(),
            attempt: 1,
            pid: Some(12345),
        },
        2 => EventKind::TaskCompleted {
            dag_id,
            run_id,
            task_id,
            duration_ms: 500 + index as u64,
            snapshot_id: None,
        },
        3 => EventKind::DagRunCompleted {
            dag_id,
            run_id,
            status: RunStatus::Success,
            duration_ms: 2000 + index as u64,
        },
        _ => unreachable!(),
    }
}

/// Populate a store with `count` mixed events and return it.
fn populate_store(store: &EventStore, count: usize) {
    for i in 0..count {
        store.append(make_mixed_event_kind(i)).unwrap();
    }
}

/// Export events to a JSON Lines temp file and return its path.
fn export_events_to_file(events: &[Event]) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    for event in events {
        let line = serde_json::to_string(event).unwrap();
        writeln!(file, "{}", line).unwrap();
    }
    file.flush().unwrap();
    file
}

/// Import events from a JSON Lines file into the given store.
fn import_events_from_file(store: &EventStore, path: &std::path::Path) {
    let file = std::fs::File::open(path).unwrap();
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line.unwrap();
        let event: Event = serde_json::from_str(&line).unwrap();
        store.append(event.kind).unwrap();
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[test]
fn test_export_events_to_json_lines() {
    let dir = tempdir().unwrap();
    let store = EventStore::open(dir.path()).unwrap();

    // Append 10 mixed events
    populate_store(&store, 10);

    // Get all events from the store
    let events = store.all_events().unwrap();
    assert_eq!(events.len(), 10);

    // Export to JSON Lines file
    let export_file = export_events_to_file(&events);

    // Read lines back and verify count
    let file = std::fs::File::open(export_file.path()).unwrap();
    let reader = BufReader::new(file);
    let lines: Vec<String> = reader.lines().map(|l| l.unwrap()).collect();
    assert_eq!(lines.len(), 10);

    // Parse each line back and verify round-trip fidelity
    for (i, line) in lines.iter().enumerate() {
        let parsed: Event = serde_json::from_str(line).unwrap();
        assert_eq!(parsed.id, events[i].id);
        assert_eq!(parsed.sequence, events[i].sequence);
        assert_eq!(parsed.timestamp, events[i].timestamp);

        // Verify the serialized form matches
        let re_serialized = serde_json::to_string(&events[i]).unwrap();
        let original_value: serde_json::Value = serde_json::from_str(line).unwrap();
        let re_value: serde_json::Value = serde_json::from_str(&re_serialized).unwrap();
        assert_eq!(original_value, re_value);
    }
}

#[test]
fn test_import_events_from_json_lines() {
    // Create source store and export
    let source_dir = tempdir().unwrap();
    let source_store = EventStore::open(source_dir.path()).unwrap();
    populate_store(&source_store, 10);

    let events = source_store.all_events().unwrap();
    let export_file = export_events_to_file(&events);

    // Open a fresh store and import
    let target_dir = tempdir().unwrap();
    let target_store = EventStore::open(target_dir.path()).unwrap();

    import_events_from_file(&target_store, export_file.path());

    // Verify event count matches
    let imported_events = target_store.all_events().unwrap();
    assert_eq!(imported_events.len(), 10);

    // Verify event kinds match (IDs and timestamps will differ because
    // append() creates new Events, but the EventKind payloads must match)
    for (original, imported) in events.iter().zip(imported_events.iter()) {
        let orig_json = serde_json::to_value(&original.kind).unwrap();
        let imp_json = serde_json::to_value(&imported.kind).unwrap();
        assert_eq!(orig_json, imp_json);
    }
}

#[test]
fn test_recovery_preserves_sequence_order() {
    // Create source store with 20 events
    let source_dir = tempdir().unwrap();
    let source_store = EventStore::open(source_dir.path()).unwrap();
    populate_store(&source_store, 20);

    let events = source_store.all_events().unwrap();
    let export_file = export_events_to_file(&events);

    // Import into fresh store
    let target_dir = tempdir().unwrap();
    let target_store = EventStore::open(target_dir.path()).unwrap();
    import_events_from_file(&target_store, export_file.path());

    // Get all events from the target store
    let imported_events = target_store.all_events().unwrap();
    assert_eq!(imported_events.len(), 20);

    // Verify sequence numbers are monotonically increasing
    for i in 1..imported_events.len() {
        assert!(
            imported_events[i].sequence > imported_events[i - 1].sequence,
            "Sequence {} ({}) should be greater than sequence {} ({})",
            i,
            imported_events[i].sequence,
            i - 1,
            imported_events[i - 1].sequence,
        );
    }
}

#[test]
fn test_range_query_after_import() {
    // Create source store with 20 events
    let source_dir = tempdir().unwrap();
    let source_store = EventStore::open(source_dir.path()).unwrap();
    populate_store(&source_store, 20);

    let events = source_store.all_events().unwrap();
    let export_file = export_events_to_file(&events);

    // Import into fresh store
    let target_dir = tempdir().unwrap();
    let target_store = EventStore::open(target_dir.path()).unwrap();
    import_events_from_file(&target_store, export_file.path());

    // Query range 5-10 (inclusive)
    let range_events = target_store.range(5, 10).unwrap();
    assert_eq!(range_events.len(), 6); // 5, 6, 7, 8, 9, 10

    // Verify the range boundaries
    assert_eq!(range_events.first().unwrap().sequence, 5);
    assert_eq!(range_events.last().unwrap().sequence, 10);

    // Verify each sequence number in the range
    for (i, event) in range_events.iter().enumerate() {
        assert_eq!(event.sequence, 5 + i as u64);
    }
}

#[test]
fn test_incremental_export() {
    // Create source store with 20 events
    let source_dir = tempdir().unwrap();
    let source_store = EventStore::open(source_dir.path()).unwrap();
    populate_store(&source_store, 20);

    // Export only events 10-20 using range query
    let partial_events = source_store.range(10, 20).unwrap();
    assert_eq!(partial_events.len(), 11); // 10, 11, 12, ..., 20

    let export_file = export_events_to_file(&partial_events);

    // Verify the export file has 11 lines
    let file = std::fs::File::open(export_file.path()).unwrap();
    let reader = BufReader::new(file);
    let line_count = reader.lines().count();
    assert_eq!(line_count, 11);

    // Import into a fresh store
    let target_dir = tempdir().unwrap();
    let target_store = EventStore::open(target_dir.path()).unwrap();
    import_events_from_file(&target_store, export_file.path());

    // The fresh store should have 11 events (re-sequenced starting at 1)
    let imported = target_store.all_events().unwrap();
    assert_eq!(imported.len(), 11);

    // Verify the event kinds match the original partial export
    for (original, imported) in partial_events.iter().zip(imported.iter()) {
        let orig_json = serde_json::to_value(&original.kind).unwrap();
        let imp_json = serde_json::to_value(&imported.kind).unwrap();
        assert_eq!(orig_json, imp_json);
    }
}
