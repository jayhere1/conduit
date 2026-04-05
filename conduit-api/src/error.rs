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
            ApiError::Internal(msg) => {
                // Log the full error internally but never expose it to clients
                tracing::error!(error = %msg, "Internal server error");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", "Internal server error".to_string())
            }
            ApiError::CompilationFailed(msg) => {
                // Strip absolute paths: keep only the portion starting at "dags/" or the
                // last path component so we never leak server directory structure.
                let sanitized = sanitize_paths(&msg);
                (StatusCode::UNPROCESSABLE_ENTITY, "compilation_failed", sanitized)
            }
            ApiError::EnvironmentNotFound(msg) => (StatusCode::NOT_FOUND, "environment_not_found", msg),
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

/// Strip absolute file paths from error messages.
///
/// If the path contains "dags/", truncate everything before it.
/// Otherwise, keep only the final path component (filename).
fn sanitize_paths(msg: &str) -> String {
    // Match sequences that look like absolute paths: /foo/bar/baz.py
    let mut result = msg.to_string();
    // Process each potential absolute path in the message
    while let Some(start) = result.find('/') {
        // Find the end of this path-like segment (space, colon, or end of string)
        let rest = &result[start..];
        let end_offset = rest
            .find(|c: char| c == ' ' || c == ':' || c == '\n')
            .unwrap_or(rest.len());
        let path_str = result[start..start + end_offset].to_string();

        // Only sanitize if it looks like an absolute path (starts with /)
        if !path_str.starts_with('/') || path_str.len() < 2 {
            break;
        }

        let replacement = if let Some(dags_pos) = path_str.find("dags/") {
            path_str[dags_pos..].to_string()
        } else {
            // Take the last path component
            path_str.rsplit('/').next().unwrap_or(&path_str).to_string()
        };

        result = format!("{}{}{}", &result[..start], replacement, &result[start + end_offset..]);

        // If we didn't change anything, stop to avoid infinite loop
        if result.contains(&path_str) && replacement == path_str {
            break;
        }
    }
    result
}

impl From<conduit_common::error::ConduitError> for ApiError {
    fn from(err: conduit_common::error::ConduitError) -> Self {
        use conduit_common::error::ConduitError;
        match err {
            ConduitError::EnvironmentNotFound(name) => ApiError::EnvironmentNotFound(name),
            ConduitError::SnapshotNotFound(id) => ApiError::NotFound(format!("Snapshot not found: {}", id)),
            ConduitError::FileNotFound(path) => ApiError::NotFound(format!("File not found: {}", path)),
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
