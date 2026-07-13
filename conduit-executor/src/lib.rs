//! Task executor module for Conduit.
//!
//! This crate provides the execution engine for Conduit DAG tasks. It handles:
//! - Task dispatch and lifecycle management
//! - Child process execution with isolation
//! - Protocol-based task communication (XCom, logs, metrics, progress)
//! - Retry policies and backoff strategies

pub mod executor;
pub mod process_runner;
pub mod protocol;
pub mod retry;

pub use conduit_providers::ProviderRegistry;
pub use executor::{ExecutorCommand, ExecutorEvent, TaskExecutor, TaskOutcome};
pub use process_runner::{ProcessOutput, ProcessRunner, TaskContext};
pub use protocol::{parse_stdout_line, ProtocolMessage};
pub use retry::parse_duration;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Ensure all public exports are accessible
        let _ = std::any::type_name::<TaskExecutor>();
        let _ = std::any::type_name::<ProcessRunner>();
        let _ = std::any::type_name::<ProtocolMessage>();
    }
}
