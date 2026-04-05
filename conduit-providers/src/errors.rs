//! Provider-specific error types.

use thiserror::Error;

/// Errors that can occur during provider operations.
#[derive(Error, Debug)]
pub enum ProviderError {
    /// Connection to the external system failed.
    #[error("connection failed for '{name}': {reason}")]
    ConnectionFailed { name: String, reason: String },

    /// The named connection was not found in the registry.
    #[error("connection '{name}' not found in registry")]
    ConnectionNotFound { name: String },

    /// The provider type is not supported.
    #[error("unsupported provider type '{provider_type}'")]
    UnsupportedProvider { provider_type: String },

    /// SQL query execution failed.
    #[error("query execution failed on '{connection}': {reason}")]
    QueryFailed { connection: String, reason: String },

    /// Authentication or credential error.
    #[error("authentication failed for '{connection}': {reason}")]
    AuthenticationFailed { connection: String, reason: String },

    /// Storage operation failed (read/write/list).
    #[error("storage operation failed on '{connection}': {reason}")]
    StorageFailed { connection: String, reason: String },

    /// HTTP request failed.
    #[error("HTTP request failed for '{connection}': {status} {reason}")]
    HttpFailed {
        connection: String,
        status: u16,
        reason: String,
    },

    /// Streaming operation failed.
    #[error("stream operation failed on '{connection}': {reason}")]
    StreamFailed { connection: String, reason: String },

    /// Configuration is invalid or missing required fields.
    #[error("invalid configuration for '{connection}': {reason}")]
    InvalidConfig { connection: String, reason: String },

    /// The provider operation is not yet implemented.
    #[error("{provider_type} provider: {operation} is not yet implemented")]
    NotImplemented { provider_type: String, operation: String },

    /// Timeout exceeded.
    #[error("operation timed out on '{connection}' after {timeout_secs}s")]
    Timeout { connection: String, timeout_secs: u64 },

    /// Generic wrapped error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl ProviderError {
    /// Convert to a `conduit_common::ConduitError` for interop.
    pub fn into_conduit_error(self) -> conduit_common::ConduitError {
        conduit_common::ConduitError::ExecutionError(self.to_string())
    }
}
