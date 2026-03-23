//! WASM plugin sandbox for Conduit pipeline orchestrator
//!
//! This crate provides a sandboxed execution environment for user-defined tasks
//! running inside WebAssembly via Wasmtime, with strict resource limits and
//! permission-based access control.

pub mod host_functions;
pub mod plugin;
pub mod registry;
pub mod runtime;
pub mod sandbox;

pub use host_functions::{HostState, register_host_functions};
pub use plugin::{Permission, PluginManifest, PluginSpec};
pub use registry::PluginRegistry;
pub use runtime::{LoadedPlugin, WasmRuntime, WasmRuntimeConfig};
pub use sandbox::{Sandbox, TaskInput, TaskOutput, TaskStatus};

use thiserror::Error;

/// Error type for WASM runtime operations
#[derive(Error, Debug)]
pub enum WasmError {
    #[error("Plugin not found: {0}")]
    PluginNotFound(String),

    #[error("Invalid plugin manifest: {0}")]
    InvalidManifest(String),

    #[error("WASM compilation error: {0}")]
    CompilationError(String),

    #[error("WASM instantiation error: {0}")]
    InstantiationError(String),

    #[error("WASM execution error: {0}")]
    ExecutionError(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Memory limit exceeded")]
    MemoryLimitExceeded,

    #[error("Execution timeout")]
    ExecutionTimeout,

    #[error("Fuel limit exceeded")]
    FuelLimitExceeded,

    #[error("Invalid entrypoint: {0}")]
    InvalidEntrypoint(String),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Wasmtime error: {0}")]
    WasmtimeError(String),

    #[error("WASI error: {0}")]
    WasiError(String),

    #[error("Host function error: {0}")]
    HostFunctionError(String),
}

/// Result type for WASM operations
pub type WasmResult<T> = Result<T, WasmError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = WasmError::PluginNotFound("test.wasm".to_string());
        assert!(err.to_string().contains("not found"));

        let err = WasmError::MemoryLimitExceeded;
        assert!(err.to_string().contains("Memory limit"));
    }

    #[test]
    fn test_wasm_result() {
        let result: WasmResult<i32> = Ok(42);
        assert!(result.is_ok());

        let result: WasmResult<i32> = Err(WasmError::ExecutionTimeout);
        assert!(result.is_err());
    }
}
