//! conduit-state: Event store, snapshot manager, and virtual environments.
//!
//! The state layer is the heart of Forge. Unlike Airflow's mutable PostgreSQL,
//! all state is an append-only event log backed by RocksDB. This enables:
//! - Time-travel debugging (replay any historical state)
//! - Instant rollback (restore previous snapshot pointers)
//! - Zero lock contention (writes are appends, not updates)

pub mod event_store;
pub mod snapshot_store;
pub mod environment_manager;

pub use event_store::{EventStore, RetentionPolicy, CompactionResult, spawn_compaction_task};
pub use snapshot_store::SnapshotStore;
pub use environment_manager::EnvironmentManager;
