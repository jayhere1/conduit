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
pub mod metrics;
pub mod snapshot;

pub use backfill::{BackfillPartition, BackfillRequest, BackfillResult, PartitionStatus};
pub use config::ConduitConfig;
pub use contracts::{
    CheckResult, ContractCheck, ContractEvaluator, DataContract, DeploymentValidation, Evidence,
    Severity, TaskContracts, ValidationResult,
};
pub use dag::{Dag, DagId, Pool, Task, TaskDependency, TaskId, TriggerRule};
pub use error::{ConduitError, ConduitResult};
pub use event::{Event, EventId, EventKind};
pub use fingerprint::Fingerprint;
pub use incremental::{
    IncrementalConfig, IncrementalContext, IncrementalStrategy, Watermark, WatermarkValue,
};
pub use snapshot::{Environment, EnvironmentId, Snapshot, SnapshotId};
