//! API error types and response formatting.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// API-level error that produces appropriate HTTP responses.
#[derive(Debug)]
pub enum ApiError {
    NotFound(String),
    BadRequest(String),
    Internal(String),
    CompilationFailed(String),
    EnvironmentNotFound(String),
    Unauthorized(String),
    Forbidden(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_type, message) = match self {
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, "not_found", msg),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "bad_request", msg),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", msg),
            ApiError::CompilationFailed(msg) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "compilation_failed", msg)
            }
            ApiError::EnvironmentNotFound(msg) => {
                (StatusCode::NOT_FOUND, "environment_not_found", msg)
            }
            ApiError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, "unauthorized", msg),
            ApiError::Forbidden(msg) => (StatusCode::FORBIDDEN, "forbidden", msg),
        };

        let body = json!({
            "error": {
                "type": error_type,
                "message": message,
            }
        });

        (status, Json(body)).into_response()
    }
}

impl From<conduit_common::error::ConduitError> for ApiError {
    fn from(err: conduit_common::error::ConduitError) -> Self {
        use conduit_common::error::ConduitError;
        match err {
            ConduitError::EnvironmentNotFound(name) => ApiError::EnvironmentNotFound(name),
            ConduitError::SnapshotNotFound(id) => {
                ApiError::NotFound(format!("Snapshot not found: {}", id))
            }
            ConduitError::FileNotFound(path) => {
                ApiError::NotFound(format!("File not found: {}", path))
            }
            ConduitError::ConfigError(msg) => ApiError::BadRequest(msg),
            ConduitError::ParseError { file, message } => {
                ApiError::CompilationFailed(format!("{}: {}", file, message))
            }
            ConduitError::CycleDetected { cycle } => {
                ApiError::CompilationFailed(format!("Cycle detected: {}", cycle))
            }
            other => ApiError::Internal(other.to_string()),
        }
    }
}
