//! Append-only event store backed by RocksDB with retention policies.
//!
//! Events are stored with monotonically increasing sequence numbers as keys.
//! Retention policies support age-based (TTL), count-based, and combined
//! strategies to prevent unbounded growth.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use conduit_common::error::{ConduitError, ConduitResult};
use conduit_common::event::{Event, EventKind};
use tracing::{debug, info, warn};

// ─── Retention Configuration ──────────────────────────────────────────────

/// Policy for controlling how long events are kept in the store.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Maximum age of events. Events older than this are eligible for compaction.
    /// `None` means no age-based retention (keep forever).
    pub max_age: Option<Duration>,

    /// Maximum number of events to retain. When exceeded, the oldest events
    /// are pruned. `None` means no count-based limit.
    pub max_count: Option<u64>,

    /// Minimum number of events to always keep, even if they exceed max_age.
    /// This prevents the store from being completely emptied by TTL expiry.
    /// Defaults to 100.
    pub min_retain: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age: None,
            max_count: None,
            min_retain: 100,
        }
    }
}

impl RetentionPolicy {
    /// 7-day retention with 100k max events.
    pub fn standard() -> Self {
        Self {
            max_age: Some(Duration::from_secs(7 * 24 * 3600)),
            max_count: Some(100_000),
            min_retain: 100,
        }
    }

    /// 30-day retention with 1M max events.
    pub fn extended() -> Self {
        Self {
            max_age: Some(Duration::from_secs(30 * 24 * 3600)),
            max_count: Some(1_000_000),
            min_retain: 1000,
        }
    }

    /// Keep everything (no retention). Useful for development.
    pub fn unlimited() -> Self {
        Self::default()
    }

    /// Returns true if any retention limit is configured.
    pub fn is_active(&self) -> bool {
        self.max_age.is_some() || self.max_count.is_some()
    }
}

/// Result of a compaction run.
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// Number of events deleted.
    pub events_deleted: u64,
    /// Sequence number of the oldest surviving event (0 if store is empty).
    pub oldest_remaining_seq: u64,
    /// Sequence number of the newest event.
    pub newest_seq: u64,
    /// Duration the compaction took.
    pub duration: Duration,
}

// ─── Event Store ──────────────────────────────────────────────────────────

/// The append-only event store.
///
/// Events are stored with monotonically increasing sequence numbers as keys.
/// This ensures total ordering and enables efficient range queries.
pub struct EventStore {
    db: rocksdb::DB,
    sequence: AtomicU64,
    retention: RetentionPolicy,
}

impl EventStore {
    /// Open or create an event store at the given path with the default
    /// (unlimited) retention policy.
    pub fn open(path: &Path) -> ConduitResult<Self> {
        Self::open_with_retention(path, RetentionPolicy::default())
    }

    /// Open or create an event store with a specific retention policy.
    pub fn open_with_retention(path: &Path, retention: RetentionPolicy) -> ConduitResult<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB write buffer
        opts.set_max_write_buffer_number(3);

        let db = rocksdb::DB::open(&opts, path).map_err(|e| {
            ConduitError::EventStoreError(format!(
                "Failed to open RocksDB at {}: {}",
                path.display(),
                e
            ))
        })?;

        // Recover the latest sequence number
        let sequence = Self::recover_sequence(&db)?;

        info!(
            path = %path.display(),
            sequence = sequence,
            retention_age = ?retention.max_age,
            retention_count = ?retention.max_count,
            "Event store opened"
        );

        Ok(Self {
            db,
            sequence: AtomicU64::new(sequence),
            retention,
        })
    }

    /// Append a new event to the store. Returns the assigned sequence number.
    pub fn append(&self, kind: EventKind) -> ConduitResult<Event> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let event = Event::new(seq, kind);

        let key = seq.to_be_bytes();
        let value = serde_json::to_vec(&event)?;

        self.db.put(key, value).map_err(|e| {
            ConduitError::EventStoreError(format!("Failed to write event {}: {}", seq, e))
        })?;

        debug!(sequence = seq, event_type = ?std::mem::discriminant(&event.kind), "Event appended");

        Ok(event)
    }

    /// Read an event by sequence number.
    pub fn get(&self, sequence: u64) -> ConduitResult<Option<Event>> {
        let key = sequence.to_be_bytes();
        match self.db.get(key) {
            Ok(Some(value)) => {
                let event: Event = serde_json::from_slice(&value)?;
                Ok(Some(event))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(ConduitError::EventStoreError(format!(
                "Failed to read event {}: {}",
                sequence, e
            ))),
        }
    }

    /// Read all events in a sequence range (inclusive).
    pub fn range(&self, from: u64, to: u64) -> ConduitResult<Vec<Event>> {
        let mut events = Vec::new();

        let start = from.to_be_bytes();
        let end = (to + 1).to_be_bytes();

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &start,
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, value) = item.map_err(|e| {
                ConduitError::EventStoreError(format!("Iterator error: {}", e))
            })?;

            if key.as_ref() >= end.as_slice() {
                break;
            }

            let event: Event = serde_json::from_slice(&value)?;
            events.push(event);
        }

        Ok(events)
    }

    /// Read all events (for replay/debugging). Use with caution on large stores.
    pub fn all_events(&self) -> ConduitResult<Vec<Event>> {
        self.range(1, self.current_sequence())
    }

    /// Get the current (latest) sequence number.
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    /// Get the current retention policy.
    pub fn retention_policy(&self) -> &RetentionPolicy {
        &self.retention
    }

    /// Update the retention policy. Takes effect on next compaction run.
    pub fn set_retention_policy(&mut self, policy: RetentionPolicy) {
        info!(
            max_age = ?policy.max_age,
            max_count = ?policy.max_count,
            min_retain = policy.min_retain,
            "Retention policy updated"
        );
        self.retention = policy;
    }

    // ─── Compaction / Retention ──────────────────────────────────────────

    /// Run compaction according to the current retention policy.
    ///
    /// Deletes events that exceed the configured age or count limits,
    /// always preserving at least `min_retain` events.
    ///
    /// Returns the compaction result with statistics.
    pub fn compact(&self) -> ConduitResult<CompactionResult> {
        let started = std::time::Instant::now();
        let current_seq = self.current_sequence();

        if current_seq == 0 {
            return Ok(CompactionResult {
                events_deleted: 0,
                oldest_remaining_seq: 0,
                newest_seq: 0,
                duration: started.elapsed(),
            });
        }

        // Determine the cutoff sequence: events at or below this are deleted.
        let cutoff = self.compute_cutoff_sequence(current_seq)?;

        if cutoff == 0 {
            return Ok(CompactionResult {
                events_deleted: 0,
                oldest_remaining_seq: self.find_oldest_sequence()?,
                newest_seq: current_seq,
                duration: started.elapsed(),
            });
        }

        // Delete events from 1..=cutoff using batch writes.
        let deleted = self.delete_range(1, cutoff)?;

        // Trigger RocksDB compaction to reclaim disk space.
        let start_key = 1u64.to_be_bytes();
        let end_key = (cutoff + 1).to_be_bytes();
        self.db.compact_range(Some(&start_key), Some(&end_key));

        let result = CompactionResult {
            events_deleted: deleted,
            oldest_remaining_seq: self.find_oldest_sequence()?,
            newest_seq: current_seq,
            duration: started.elapsed(),
        };

        info!(
            deleted = result.events_deleted,
            oldest_remaining = result.oldest_remaining_seq,
            newest = result.newest_seq,
            duration_ms = result.duration.as_millis(),
            "Compaction completed"
        );

        Ok(result)
    }

    /// Delete all events with sequence numbers in [from, to] inclusive.
    /// Returns the count of deleted events.
    pub fn delete_range(&self, from: u64, to: u64) -> ConduitResult<u64> {
        let mut batch = rocksdb::WriteBatch::default();
        let mut count = 0u64;

        let start = from.to_be_bytes();
        let end = (to + 1).to_be_bytes();

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &start,
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, _) = item.map_err(|e| {
                ConduitError::EventStoreError(format!("Iterator error during delete: {}", e))
            })?;

            if key.as_ref() >= end.as_slice() {
                break;
            }

            batch.delete(&key);
            count += 1;

            // Flush batch periodically to avoid unbounded memory usage.
            if count % 10_000 == 0 {
                self.db.write(batch).map_err(|e| {
                    ConduitError::EventStoreError(format!("Batch delete error: {}", e))
                })?;
                batch = rocksdb::WriteBatch::default();
            }
        }

        if count % 10_000 != 0 {
            self.db.write(batch).map_err(|e| {
                ConduitError::EventStoreError(format!("Batch delete error: {}", e))
            })?;
        }

        Ok(count)
    }

    /// Delete events older than the given timestamp.
    /// Returns the count of deleted events.
    pub fn delete_before(&self, cutoff_time: DateTime<Utc>) -> ConduitResult<u64> {
        let current = self.current_sequence();
        if current == 0 {
            return Ok(0);
        }

        // Scan forward from sequence 1, finding the first event that's
        // newer than the cutoff. Everything before it gets deleted.
        let cutoff_seq = self.find_sequence_at_time(cutoff_time)?;

        if cutoff_seq == 0 {
            return Ok(0);
        }

        // Respect min_retain
        let protected = current.saturating_sub(self.retention.min_retain);
        let effective_cutoff = cutoff_seq.min(protected);

        if effective_cutoff == 0 {
            return Ok(0);
        }

        self.delete_range(1, effective_cutoff)
    }

    /// Get approximate event count by examining sequence range.
    /// This is O(1) but may overcount if events have been deleted.
    pub fn approximate_count(&self) -> u64 {
        let oldest = self.find_oldest_sequence().unwrap_or(0);
        let newest = self.current_sequence();
        if oldest == 0 || newest == 0 {
            return 0;
        }
        newest - oldest + 1
    }

    /// Get exact event count by scanning the database.
    /// This is O(n) and should be used sparingly.
    pub fn exact_count(&self) -> ConduitResult<u64> {
        let mut count = 0u64;
        let iter = self
            .db
            .iterator(rocksdb::IteratorMode::From(&[0u8; 8], rocksdb::Direction::Forward));

        for item in iter {
            let _ = item.map_err(|e| {
                ConduitError::EventStoreError(format!("Count iterator error: {}", e))
            })?;
            count += 1;
        }

        Ok(count)
    }

    // ─── DAG-specific Queries ────────────────────────────────────────────

    /// Find the logical_date of the most recent DagRunCreated event for a DAG.
    ///
    /// Scans backwards from the latest event to find the last run, which is
    /// more efficient than a forward scan when the event store is large.
    pub fn last_run_logical_date(&self, dag_id: &str) -> ConduitResult<Option<DateTime<Utc>>> {
        let current = self.current_sequence();
        if current == 0 {
            return Ok(None);
        }

        // Scan backward from latest
        let mut iter = self.db.raw_iterator();
        iter.seek_to_last();

        while iter.valid() {
            if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
                if key.len() == 8 {
                    if let Ok(event) = serde_json::from_slice::<Event>(value) {
                        if let EventKind::DagRunCreated {
                            dag_id: ref id,
                            logical_date,
                            ..
                        } = event.kind
                        {
                            if id == dag_id {
                                return Ok(Some(logical_date));
                            }
                        }
                    }
                }
            }
            iter.prev();
        }

        Ok(None)
    }

    /// Find the logical_date of the most recent successfully completed run for a DAG.
    pub fn last_successful_run_date(&self, dag_id: &str) -> ConduitResult<Option<DateTime<Utc>>> {
        let current = self.current_sequence();
        if current == 0 {
            return Ok(None);
        }

        // Collect completed run IDs scanning backward
        let mut iter = self.db.raw_iterator();
        iter.seek_to_last();

        // First find the most recent DagRunCompleted with Success for this DAG
        let mut success_run_id: Option<String> = None;
        while iter.valid() {
            if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
                if key.len() == 8 {
                    if let Ok(event) = serde_json::from_slice::<Event>(value) {
                        if let EventKind::DagRunCompleted {
                            dag_id: ref id,
                            ref run_id,
                            ref status,
                            ..
                        } = event.kind
                        {
                            if id == dag_id
                                && *status == conduit_common::event::RunStatus::Success
                            {
                                success_run_id = Some(run_id.clone());
                                break;
                            }
                        }
                    }
                }
            }
            iter.prev();
        }

        let run_id = match success_run_id {
            Some(id) => id,
            None => return Ok(None),
        };

        // Now find the DagRunCreated event for that run_id to get its logical_date
        iter.seek_to_first();
        while iter.valid() {
            if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
                if key.len() == 8 {
                    if let Ok(event) = serde_json::from_slice::<Event>(value) {
                        if let EventKind::DagRunCreated {
                            dag_id: ref id,
                            run_id: ref rid,
                            logical_date,
                            ..
                        } = event.kind
                        {
                            if id == dag_id && *rid == run_id {
                                return Ok(Some(logical_date));
                            }
                        }
                    }
                }
            }
            iter.next();
        }

        Ok(None)
    }

    // ─── Internal helpers ───────────────────────────────────────────────

    /// Compute the cutoff sequence based on retention policy.
    /// Events at or below this sequence should be deleted.
    fn compute_cutoff_sequence(&self, current_seq: u64) -> ConduitResult<u64> {
        let mut cutoff: u64 = 0;
        let now = Utc::now();

        // Age-based cutoff
        if let Some(max_age) = &self.retention.max_age {
            let cutoff_time = now - chrono::Duration::from_std(*max_age)
                .unwrap_or_else(|_| chrono::Duration::seconds(0));
            let age_cutoff = self.find_sequence_at_time(cutoff_time)?;
            if age_cutoff > cutoff {
                cutoff = age_cutoff;
            }
        }

        // Count-based cutoff
        if let Some(max_count) = self.retention.max_count {
            if current_seq > max_count {
                let count_cutoff = current_seq - max_count;
                if count_cutoff > cutoff {
                    cutoff = count_cutoff;
                }
            }
        }

        // Protect min_retain events
        let protected = current_seq.saturating_sub(self.retention.min_retain);
        cutoff = cutoff.min(protected);

        Ok(cutoff)
    }

    /// Find the highest sequence number whose event timestamp is <= cutoff_time.
    /// Returns 0 if no such event exists.
    fn find_sequence_at_time(&self, cutoff_time: DateTime<Utc>) -> ConduitResult<u64> {
        let mut last_eligible: u64 = 0;

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &1u64.to_be_bytes(),
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, value) = item.map_err(|e| {
                ConduitError::EventStoreError(format!("Time scan error: {}", e))
            })?;

            if key.len() != 8 {
                continue;
            }

            let event: Event = serde_json::from_slice(&value)?;

            if event.timestamp <= cutoff_time {
                let seq = u64::from_be_bytes(key.as_ref().try_into().unwrap());
                last_eligible = seq;
            } else {
                // Events are ordered by sequence (and approximately by time),
                // so once we see a newer event, we can stop.
                break;
            }
        }

        Ok(last_eligible)
    }

    /// Find the oldest (lowest) sequence number still in the database.
    fn find_oldest_sequence(&self) -> ConduitResult<u64> {
        let mut iter = self.db.raw_iterator();
        iter.seek_to_first();

        if iter.valid() {
            if let Some(key) = iter.key() {
                if key.len() == 8 {
                    return Ok(u64::from_be_bytes(key.try_into().unwrap()));
                }
            }
        }

        Ok(0)
    }

    /// Recover the latest sequence number from the database.
    fn recover_sequence(db: &rocksdb::DB) -> ConduitResult<u64> {
        let mut iter = db.raw_iterator();
        iter.seek_to_last();

        if iter.valid() {
            if let Some(key) = iter.key() {
                if key.len() == 8 {
                    let seq = u64::from_be_bytes(key.try_into().unwrap());
                    return Ok(seq);
                }
            }
        }

        Ok(0)
    }
}

// ─── Background Compaction Task ──────────────────────────────────────────

/// Spawn a background task that periodically runs compaction.
///
/// Returns a `JoinHandle` that can be used to cancel the task.
///
/// # Arguments
/// * `store` - Arc-wrapped event store to compact
/// * `interval` - How often to run compaction (e.g., every 15 minutes)
pub fn spawn_compaction_task(
    store: Arc<EventStore>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    info!(
        interval_secs = interval.as_secs(),
        "Starting background compaction task"
    );

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // Skip the first immediate tick
        ticker.tick().await;

        loop {
            ticker.tick().await;

            if !store.retention_policy().is_active() {
                debug!("Retention policy inactive, skipping compaction");
                continue;
            }

            match store.compact() {
                Ok(result) => {
                    if result.events_deleted > 0 {
                        info!(
                            deleted = result.events_deleted,
                            oldest_remaining = result.oldest_remaining_seq,
                            duration_ms = result.duration.as_millis(),
                            "Background compaction completed"
                        );
                    } else {
                        debug!("Background compaction: nothing to delete");
                    }
                }
                Err(e) => {
                    warn!("Background compaction failed: {}", e);
                }
            }
        }
    })
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_common::event::EventKind;
    use tempfile::tempdir;

    fn make_event_kind(dag_id: &str) -> EventKind {
        EventKind::DagRunCreated {
            dag_id: dag_id.to_string(),
            run_id: format!("run_{}", dag_id),
            logical_date: chrono::Utc::now(),
            environment: "production".to_string(),
            triggered_by: "test".to_string(),
        }
    }

    #[test]
    fn append_and_read_event() {
        let dir = tempdir().unwrap();
        let store = EventStore::open(dir.path()).unwrap();

        let event = store.append(make_event_kind("test_dag")).unwrap();
        assert_eq!(event.sequence, 1);

        let read = store.get(1).unwrap().unwrap();
        assert_eq!(read.sequence, 1);
    }

    #[test]
    fn sequence_increases_monotonically() {
        let dir = tempdir().unwrap();
        let store = EventStore::open(dir.path()).unwrap();

        for i in 1..=10 {
            let event = store.append(make_event_kind(&format!("dag_{}", i))).unwrap();
            assert_eq!(event.sequence, i);
        }

        assert_eq!(store.current_sequence(), 10);
    }

    #[test]
    fn range_query() {
        let dir = tempdir().unwrap();
        let store = EventStore::open(dir.path()).unwrap();

        for i in 1..=5 {
            store.append(make_event_kind(&format!("dag_{}", i))).unwrap();
        }

        let events = store.range(2, 4).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].sequence, 2);
        assert_eq!(events[2].sequence, 4);
    }

    #[test]
    fn sequence_recovery_on_reopen() {
        let dir = tempdir().unwrap();

        {
            let store = EventStore::open(dir.path()).unwrap();
            for _ in 0..5 {
                store.append(make_event_kind("dag")).unwrap();
            }
        }

        // Reopen and verify sequence continues
        let store = EventStore::open(dir.path()).unwrap();
        assert_eq!(store.current_sequence(), 5);

        let event = store.append(make_event_kind("dag")).unwrap();
        assert_eq!(event.sequence, 6);
    }

    // ─── Retention / Compaction Tests ────────────────────────────────────

    #[test]
    fn retention_policy_defaults() {
        let p = RetentionPolicy::default();
        assert!(p.max_age.is_none());
        assert!(p.max_count.is_none());
        assert_eq!(p.min_retain, 100);
        assert!(!p.is_active());
    }

    #[test]
    fn retention_policy_standard() {
        let p = RetentionPolicy::standard();
        assert!(p.max_age.is_some());
        assert_eq!(p.max_count, Some(100_000));
        assert!(p.is_active());
    }

    #[test]
    fn compact_with_no_policy_does_nothing() {
        let dir = tempdir().unwrap();
        let store = EventStore::open(dir.path()).unwrap();

        for i in 0..50 {
            store.append(make_event_kind(&format!("dag_{}", i))).unwrap();
        }

        let result = store.compact().unwrap();
        assert_eq!(result.events_deleted, 0);
        assert_eq!(result.newest_seq, 50);
    }

    #[test]
    fn compact_count_based() {
        let dir = tempdir().unwrap();
        let policy = RetentionPolicy {
            max_age: None,
            max_count: Some(10),
            min_retain: 5,
        };
        let store = EventStore::open_with_retention(dir.path(), policy).unwrap();

        // Insert 25 events
        for i in 0..25 {
            store.append(make_event_kind(&format!("dag_{}", i))).unwrap();
        }

        assert_eq!(store.current_sequence(), 25);

        // Compact: keep max 10, so delete 15
        let result = store.compact().unwrap();
        assert_eq!(result.events_deleted, 15);
        assert_eq!(result.oldest_remaining_seq, 16);
        assert_eq!(result.newest_seq, 25);

        // Verify deleted events are gone
        assert!(store.get(1).unwrap().is_none());
        assert!(store.get(15).unwrap().is_none());

        // Verify remaining events still exist
        assert!(store.get(16).unwrap().is_some());
        assert!(store.get(25).unwrap().is_some());
    }

    #[test]
    fn compact_respects_min_retain() {
        let dir = tempdir().unwrap();
        let policy = RetentionPolicy {
            max_age: None,
            max_count: Some(3), // Want only 3
            min_retain: 8,      // But must keep at least 8
        };
        let store = EventStore::open_with_retention(dir.path(), policy).unwrap();

        for i in 0..10 {
            store.append(make_event_kind(&format!("dag_{}", i))).unwrap();
        }

        let result = store.compact().unwrap();
        // max_count says delete 7 (keep 3), but min_retain says keep 8,
        // so we only delete 2
        assert_eq!(result.events_deleted, 2);
        assert_eq!(result.oldest_remaining_seq, 3);
    }

    #[test]
    fn compact_on_empty_store() {
        let dir = tempdir().unwrap();
        let policy = RetentionPolicy::standard();
        let store = EventStore::open_with_retention(dir.path(), policy).unwrap();

        let result = store.compact().unwrap();
        assert_eq!(result.events_deleted, 0);
        assert_eq!(result.oldest_remaining_seq, 0);
        assert_eq!(result.newest_seq, 0);
    }

    #[test]
    fn delete_range_works() {
        let dir = tempdir().unwrap();
        let store = EventStore::open(dir.path()).unwrap();

        for i in 0..20 {
            store.append(make_event_kind(&format!("dag_{}", i))).unwrap();
        }

        let deleted = store.delete_range(5, 15).unwrap();
        assert_eq!(deleted, 11); // 5,6,7,8,9,10,11,12,13,14,15

        // Verify edges
        assert!(store.get(4).unwrap().is_some());
        assert!(store.get(5).unwrap().is_none());
        assert!(store.get(15).unwrap().is_none());
        assert!(store.get(16).unwrap().is_some());
    }

    #[test]
    fn compact_age_based() {
        let dir = tempdir().unwrap();
        let policy = RetentionPolicy {
            max_age: Some(Duration::from_secs(1)), // 1 second TTL
            max_count: None,
            min_retain: 2,
        };
        let store = EventStore::open_with_retention(dir.path(), policy).unwrap();

        // Insert some events
        for i in 0..10 {
            store.append(make_event_kind(&format!("dag_{}", i))).unwrap();
        }

        // Sleep to let events age past the 1-second TTL
        std::thread::sleep(Duration::from_millis(1100));

        // Insert 2 more fresh events
        store.append(make_event_kind("fresh_1")).unwrap();
        store.append(make_event_kind("fresh_2")).unwrap();

        // Compact: the first 10 events are >1 second old
        let result = store.compact().unwrap();

        // Should delete the old events (10), keeping the 2 fresh ones + min_retain protection
        assert!(result.events_deleted >= 8); // At least 8 deleted (min_retain=2 protects last 2)
        assert!(store.get(11).unwrap().is_some()); // Fresh events survive
        assert!(store.get(12).unwrap().is_some());
    }

    #[test]
    fn approximate_count_after_compaction() {
        let dir = tempdir().unwrap();
        let policy = RetentionPolicy {
            max_age: None,
            max_count: Some(5),
            min_retain: 3,
        };
        let store = EventStore::open_with_retention(dir.path(), policy).unwrap();

        for i in 0..20 {
            store.append(make_event_kind(&format!("dag_{}", i))).unwrap();
        }

        store.compact().unwrap();

        // After compaction, approximate count should reflect remaining events
        let approx = store.approximate_count();
        assert!(approx <= 6); // 5 from max_count + some margin
    }

    #[test]
    fn exact_count() {
        let dir = tempdir().unwrap();
        let store = EventStore::open(dir.path()).unwrap();

        for i in 0..15 {
            store.append(make_event_kind(&format!("dag_{}", i))).unwrap();
        }

        assert_eq!(store.exact_count().unwrap(), 15);

        store.delete_range(3, 7).unwrap();
        assert_eq!(store.exact_count().unwrap(), 10);
    }

    #[test]
    fn repeated_compaction_is_idempotent() {
        let dir = tempdir().unwrap();
        let policy = RetentionPolicy {
            max_age: None,
            max_count: Some(5),
            min_retain: 3,
        };
        let store = EventStore::open_with_retention(dir.path(), policy).unwrap();

        for i in 0..20 {
            store.append(make_event_kind(&format!("dag_{}", i))).unwrap();
        }

        let r1 = store.compact().unwrap();
        assert!(r1.events_deleted > 0);

        // Second compaction should be a no-op
        let r2 = store.compact().unwrap();
        assert_eq!(r2.events_deleted, 0);
    }

    #[test]
    fn last_run_logical_date_finds_most_recent() {
        let dir = tempdir().unwrap();
        let store = EventStore::open(dir.path()).unwrap();

        let date1 = chrono::Utc::now() - chrono::Duration::hours(2);
        let date2 = chrono::Utc::now() - chrono::Duration::hours(1);

        store.append(EventKind::DagRunCreated {
            dag_id: "my_dag".to_string(),
            run_id: "run_1".to_string(),
            logical_date: date1,
            environment: "production".to_string(),
            triggered_by: "scheduler".to_string(),
        }).unwrap();

        store.append(EventKind::DagRunCreated {
            dag_id: "my_dag".to_string(),
            run_id: "run_2".to_string(),
            logical_date: date2,
            environment: "production".to_string(),
            triggered_by: "scheduler".to_string(),
        }).unwrap();

        // Should return the most recent (date2)
        let result = store.last_run_logical_date("my_dag").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), date2);
    }

    #[test]
    fn last_run_logical_date_returns_none_for_unknown_dag() {
        let dir = tempdir().unwrap();
        let store = EventStore::open(dir.path()).unwrap();

        store.append(make_event_kind("other_dag")).unwrap();

        let result = store.last_run_logical_date("unknown_dag").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn last_successful_run_date_skips_failures() {
        let dir = tempdir().unwrap();
        let store = EventStore::open(dir.path()).unwrap();

        let date1 = chrono::Utc::now() - chrono::Duration::hours(3);
        let date2 = chrono::Utc::now() - chrono::Duration::hours(2);

        // First run: success
        store.append(EventKind::DagRunCreated {
            dag_id: "my_dag".to_string(),
            run_id: "run_1".to_string(),
            logical_date: date1,
            environment: "production".to_string(),
            triggered_by: "scheduler".to_string(),
        }).unwrap();
        store.append(EventKind::DagRunCompleted {
            dag_id: "my_dag".to_string(),
            run_id: "run_1".to_string(),
            status: conduit_common::event::RunStatus::Success,
            duration_ms: 1000,
        }).unwrap();

        // Second run: failed
        store.append(EventKind::DagRunCreated {
            dag_id: "my_dag".to_string(),
            run_id: "run_2".to_string(),
            logical_date: date2,
            environment: "production".to_string(),
            triggered_by: "scheduler".to_string(),
        }).unwrap();
        store.append(EventKind::DagRunCompleted {
            dag_id: "my_dag".to_string(),
            run_id: "run_2".to_string(),
            status: conduit_common::event::RunStatus::Failed,
            duration_ms: 500,
        }).unwrap();

        // Should return date1 (last successful), not date2 (failed)
        let result = store.last_successful_run_date("my_dag").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), date1);
    }

    #[test]
    fn open_with_retention_preserves_existing_data() {
        let dir = tempdir().unwrap();

        // Open without retention, add data
        {
            let store = EventStore::open(dir.path()).unwrap();
            for i in 0..10 {
                store.append(make_event_kind(&format!("dag_{}", i))).unwrap();
            }
        }

        // Reopen with retention policy
        let policy = RetentionPolicy {
            max_age: None,
            max_count: Some(5),
            min_retain: 3,
        };
        let store = EventStore::open_with_retention(dir.path(), policy).unwrap();

        // Data should still be there
        assert_eq!(store.current_sequence(), 10);
        assert!(store.get(1).unwrap().is_some());

        // But compaction should trim it
        let result = store.compact().unwrap();
        assert_eq!(result.events_deleted, 5);
    }
}
