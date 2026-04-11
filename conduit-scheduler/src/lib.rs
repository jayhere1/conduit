//! conduit-scheduler: Event-driven task scheduling.
//!
//! The scheduler reacts to events (task completions, cron ticks, sensor triggers)
//! rather than polling a database in a loop. This eliminates Airflow's #1 bottleneck.
//!
//! ## Architecture
//!
//! The scheduler is a single async event loop that processes:
//! - `DagRunRequested`: Creates a new DAG run and evaluates root tasks
//! - `TaskCompleted`: Updates task state and evaluates downstream tasks
//! - `TaskFailed`: Handles retries or marks the DAG run failed
//! - `CronTick`: Evaluates DAG schedules and creates new runs
//! - `SensorTriggered`: Unblocks sensor-waiting tasks
//!
//! Tasks transition through states based on trigger rules and dependency satisfaction.
//! All state is in-memory; persistence is delegated to the state module.
//!
//! Phase 1 implements single-node scheduling.
//! Phase 4 adds Raft-based distributed scheduling.

pub mod cron;
pub mod pool_manager;
pub mod scheduler;
pub mod trigger;

// Re-export key types
pub use cron::CronSchedule;
pub use pool_manager::PoolManager;
pub use scheduler::{
    DagRunState, RunStatus, Scheduler, SchedulerCommand, SchedulerEvent, TaskState,
};
pub use trigger::TriggerRuleEvaluator;
