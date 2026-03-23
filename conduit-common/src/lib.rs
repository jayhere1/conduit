//! conduit-common: Shared types, error handling, and configuration for Conduit.
//!
//! This crate defines the core data model used across all Conduit components:
//! DAG definitions, task definitions, events, snapshots, and environments.

pub mod backfill;
pub mod config;
pub mod contracts;
pub mod dag;
pub mod error;
pub mod event;
pub mod fingerprint;
pub mod incremental;
pub mod snapshot;

pub use backfill::{BackfillRequest, BackfillPartition, BackfillResult, PartitionStatus};
pub use config::ConduitConfig;
pub use contracts::{
    DataContract, ContractCheck, TaskContracts, Severity,
    CheckResult, ValidationResult, DeploymentValidation,
    Evidence, ContractEvaluator,
};
pub use dag::{Dag, DagId, Task, TaskId, TaskDependency, Pool, TriggerRule};
pub use error::{ConduitError, ConduitResult};
pub use event::{Event, EventId, EventKind};
pub use fingerprint::Fingerprint;
pub use incremental::{IncrementalConfig, IncrementalStrategy, Watermark, WatermarkValue, IncrementalContext};
pub use snapshot::{Snapshot, SnapshotId, Environment, EnvironmentId};
