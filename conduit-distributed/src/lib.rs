//! conduit-distributed: Distributed task execution for Conduit.
//!
//! This crate implements a leader-worker architecture for distributing
//! pipeline task execution across multiple nodes.
//!
//! # Architecture
//!
//! ```text
//!   ┌─────────────────────────────────────────┐
//!   │  Coordinator (leader node)              │
//!   │  ├── WorkerPool (tracks workers)        │
//!   │  ├── Task queue (pending assignments)   │
//!   │  └── gRPC server (:9400)                │
//!   └────────────┬───────────────┬────────────┘
//!                │ gRPC          │ gRPC
//!          ┌─────▼─────┐  ┌─────▼─────┐
//!          │ Worker-1  │  │ Worker-2  │
//!          │ cap: 4    │  │ cap: 8    │
//!          │ pool: gpu │  │ pool: *   │
//!          └───────────┘  └───────────┘
//! ```
//!
//! # Components
//!
//! - **[`proto_types`]**: Protobuf-equivalent Rust types for the gRPC protocol
//! - **[`worker_pool`]**: Worker registry, health tracking, and task routing
//! - **[`coordinator`]**: Leader node that manages workers and distributes tasks
//! - **[`worker`]**: Worker node that connects to the coordinator and executes tasks
//! - **[`distributed_executor`]**: Drop-in replacement for the local TaskExecutor
//!
//! # Usage
//!
//! Start a coordinator on the scheduler node:
//! ```bash
//! conduit coordinator --bind 0.0.0.0:9400
//! ```
//!
//! Start workers on execution nodes:
//! ```bash
//! conduit worker --coordinator scheduler:9400 --capacity 8 --pools default,gpu
//! ```
//!
//! Or use the integrated mode (scheduler + coordinator in one process):
//! ```bash
//! conduit run my_dag --distributed --bind 0.0.0.0:9400
//! ```

pub mod convert;
pub mod coordinator;
pub mod distributed_executor;
pub mod grpc_client;
pub mod grpc_server;
pub mod proto_types;
pub mod worker;
pub mod worker_pool;

/// Canonical location for generated protobuf types.
/// All modules should reference `crate::generated_proto` instead of
/// including the file independently.
pub mod generated_proto {
    include!("generated/conduit.distributed.rs");
}

pub use coordinator::{Coordinator, CoordinatorConfig};
pub use distributed_executor::{
    DispatchRequest, DispatchResult, DistributedExecutor, DistributedExecutorConfig, ExecutionMode,
};
pub use grpc_client::{run_worker, GrpcClientError, WorkerGrpcClient};
pub use grpc_server::{serve_grpc, CoordinatorGrpcService};
pub use proto_types::*;
pub use worker::{Worker, WorkerConfig};
pub use worker_pool::{RoutingStrategy, WorkerPool};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Verify all key types are accessible from the crate root.
        let _: CoordinatorConfig = CoordinatorConfig::default();
        let _: WorkerConfig = WorkerConfig::default();
        let _: DistributedExecutorConfig = DistributedExecutorConfig::default();
        let _: RoutingStrategy = RoutingStrategy::LeastLoaded;
        let _: ExecutionMode = ExecutionMode::Local;
    }

    #[test]
    fn test_ack_constructors() {
        let ok = Ack::ok();
        assert!(ok.success);

        let err = Ack::error("something broke");
        assert!(!err.success);
        assert_eq!(err.message, "something broke");
    }
}
