//! conduit-planner: Change detection and plan/apply workflow.
//!
//! Implements the Terraform-style plan/apply model:
//! 1. **Fingerprint** every task (code + config + upstream fingerprints)
//! 2. **Compare** against an environment's snapshot map
//! 3. **Classify** changes (Added, Modified, Removed, Upstream-Invalidated)
//! 4. **Impact analysis** — which downstream tasks are transitively affected
//! 5. **Generate a DeploymentPlan** — the minimum set of tasks to execute
//! 6. **Apply** — execute only changed tasks, reuse snapshots for unchanged ones

pub mod backfill_engine;
pub mod change_detector;
pub mod deployment_plan;
pub mod fingerprinter;
pub mod impact_analyzer;
pub mod incremental;

pub use backfill_engine::BackfillEngine;
pub use change_detector::{ChangeDetector, ChangeKind, ChangeSet, TaskChange};
pub use deployment_plan::{ActionKind, DeploymentAction, DeploymentPlan};
pub use fingerprinter::PlanFingerprinter;
pub use impact_analyzer::ImpactAnalyzer;
pub use incremental::{IncrementalEngine, WatermarkStore};
