//! Authentication middleware for Axum.
//!
//! Provides two extractors:
//!
//! - `RequireAuth`: Rejects unauthenticated requests with 401.
//! - `OptionalAuth`: Passes through if no token provided; validates if present.
//!
//! Both are no-ops when `auth_enabled` is false on the AuthStore.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::FromRequestParts;
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::auth::{AuthContext, AuthError, AuthStore, Permission, Role};
use crate::AppState;

// ── RequireAuth extractor ────────────────────────────────────────────────────

/// Axum extractor that requires a valid API key.
///
/// When auth is disabled, creates a synthetic admin context.
///
/// Usage in handlers:
/// ```ignore
/// pub async fn my_handler(
///     auth: RequireAuth,
///     State(state): State<Arc<AppState>>,
/// ) -> Result<Json<Value>, ApiError> {
///     auth.require(Permission::TriggerRun)?;
///     // ... handler logic
/// }
/// ```
#[derive(Debug, Clone)]
pub struct RequireAuth(pub AuthContext);

impl RequireAuth {
    /// Convenience: check a permission, returning ApiError on failure.
    pub fn require(&self, perm: Permission) -> Result<(), AuthApiError> {
        self.0.require(perm).map_err(AuthApiError)
    }

    /// Get the underlying auth context.
    pub fn context(&self) -> &AuthContext {
        &self.0
    }

    /// Get the authenticated role.
    pub fn role(&self) -> Role {
        self.0.role
    }
}

#[async_trait]
impl FromRequestParts<Arc<AppState>> for RequireAuth
{
    type Rejection = AuthApiError;

    async fn from_request_parts(parts: &mut Parts, state: &Arc<AppState>) -> Result<Self, Self::Rejection> {
        let auth_store = &state.auth_store;

        // If auth is disabled, create a synthetic admin context
        if !auth_store.auth_enabled {
            return Ok(RequireAuth(AuthContext {
                key_id: "anonymous".to_string(),
                key_name: "anonymous".to_string(),
                role: Role::Admin,
            }));
        }

        // Extract the Authorization header
        let auth_header = parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok());

        let header_value = auth_header.ok_or(AuthApiError(AuthError::MissingToken))?;

        let token = AuthStore::extract_bearer(header_value)
            .map_err(AuthApiError)?;

        let context = auth_store.authenticate(token).map_err(AuthApiError)?;

        Ok(RequireAuth(context))
    }
}

// ── OptionalAuth extractor ───────────────────────────────────────────────────

/// Axum extractor that optionally authenticates a request.
///
/// Returns `Some(AuthContext)` if a valid token is provided,
/// `None` if no token is provided or auth is disabled.
/// Returns an error only if a token IS provided but is invalid.
#[derive(Debug, Clone)]
pub struct OptionalAuth(pub Option<AuthContext>);

#[async_trait]
impl FromRequestParts<Arc<AppState>> for OptionalAuth
{
    type Rejection = AuthApiError;

    async fn from_request_parts(parts: &mut Parts, state: &Arc<AppState>) -> Result<Self, Self::Rejection> {
        let auth_store = &state.auth_store;

        if !auth_store.auth_enabled {
            return Ok(OptionalAuth(None));
        }

        let auth_header = parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok());

        match auth_header {
            None => Ok(OptionalAuth(None)),
            Some(header_value) => {
                let token = AuthStore::extract_bearer(header_value)
                    .map_err(AuthApiError)?;
                let context = auth_store.authenticate(token).map_err(AuthApiError)?;
                Ok(OptionalAuth(Some(context)))
            }
        }
    }
}

// ── Error type ───────────────────────────────────────────────────────────────

/// Wrapper around AuthError that implements IntoResponse for Axum.
#[derive(Debug)]
pub struct AuthApiError(pub AuthError);

impl IntoResponse for AuthApiError {
    fn into_response(self) -> Response {
        let status = match self.0.status_code() {
            401 => StatusCode::UNAUTHORIZED,
            403 => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = json!({
            "error": {
                "type": if status == StatusCode::UNAUTHORIZED { "unauthorized" } else { "forbidden" },
                "message": self.0.to_string(),
            }
        });

        (status, Json(body)).into_response()
    }
}

impl From<AuthError> for AuthApiError {
    fn from(err: AuthError) -> Self {
        AuthApiError(err)
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_api_error_response_codes() {
        let err = AuthApiError(AuthError::MissingToken);
        assert_eq!(err.0.status_code(), 401);

        let err = AuthApiError(AuthError::Forbidden {
            required_role: Role::Admin,
            actual_role: Role::Viewer,
        });
        assert_eq!(err.0.status_code(), 403);
    }

    #[test]
    fn test_require_auth_permission_check() {
        let ctx = AuthContext {
            key_id: "k1".to_string(),
            key_name: "test".to_string(),
            role: Role::Operator,
        };
        let auth = RequireAuth(ctx);

        assert!(auth.require(Permission::ViewDags).is_ok());
        assert!(auth.require(Permission::TriggerRun).is_ok());
        assert!(auth.require(Permission::ManageApiKeys).is_err());
    }
}
