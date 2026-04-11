//! API key management handlers.
//!
//! All endpoints require Admin role.
//!
//! - POST   /api/v1/auth/keys          — Create a new API key
//! - GET    /api/v1/auth/keys          — List all API keys
//! - GET    /api/v1/auth/keys/:id      — Get a specific key
//! - DELETE /api/v1/auth/keys/:id      — Revoke a key
//! - GET    /api/v1/auth/me            — Get current auth context

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Permission, Role};
use crate::error::ApiError;
use crate::middleware::RequireAuth;
use crate::AppState;

/// Request body for creating an API key.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateKeyRequest {
    /// Human-readable name for the key.
    pub name: String,
    /// Role to assign: "viewer", "operator", or "admin".
    pub role: String,
    /// Optional description.
    pub description: Option<String>,
    /// Optional expiry date (ISO 8601).
    pub expires_at: Option<DateTime<Utc>>,
}

/// POST /api/v1/auth/keys — create a new API key.
///
/// Returns the plaintext key in the response. This is the ONLY time
/// the plaintext is available — it cannot be retrieved later.
pub async fn create_key(
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateKeyRequest>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::ManageApiKeys)
        .map_err(|e| ApiError::Forbidden(e.0.to_string()))?;

    let role = Role::from_str_loose(&body.role).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "Invalid role '{}'. Must be one of: viewer, operator, admin",
            body.role
        ))
    })?;

    if body.name.trim().is_empty() {
        return Err(ApiError::BadRequest("Key name cannot be empty".to_string()));
    }

    let (plaintext, key) = state
        .auth_store
        .create_key(
            &body.name,
            role,
            &auth.context().key_name,
            body.description,
            body.expires_at,
        )
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    // Persist keys to disk
    state.save_auth_keys();

    Ok(Json(json!({
        "key": plaintext,
        "id": key.id,
        "name": key.name,
        "role": key.role,
        "prefix": key.key_prefix,
        "createdAt": key.created_at.to_rfc3339(),
        "expiresAt": key.expires_at.map(|t| t.to_rfc3339()),
        "message": "Save this key now — it will not be shown again."
    })))
}

/// GET /api/v1/auth/keys — list all API keys.
///
/// Returns metadata only (no hashes or plaintext).
pub async fn list_keys(
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::ManageApiKeys)
        .map_err(|e| ApiError::Forbidden(e.0.to_string()))?;

    let keys: Vec<Value> = state
        .auth_store
        .list_keys()
        .iter()
        .map(|k| {
            json!({
                "id": k.id,
                "name": k.name,
                "prefix": k.key_prefix,
                "role": k.role,
                "createdAt": k.created_at.to_rfc3339(),
                "expiresAt": k.expires_at.map(|t| t.to_rfc3339()),
                "revoked": k.revoked,
                "createdBy": k.created_by,
                "description": k.description,
                "lastUsedAt": k.last_used_at.map(|t| t.to_rfc3339()),
            })
        })
        .collect();

    Ok(Json(json!({
        "keys": keys,
        "total": keys.len(),
    })))
}

/// GET /api/v1/auth/keys/:id — get a specific key's metadata.
pub async fn get_key(
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
    Path(key_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::ManageApiKeys)
        .map_err(|e| ApiError::Forbidden(e.0.to_string()))?;

    let key = state
        .auth_store
        .get_key(&key_id)
        .ok_or_else(|| ApiError::NotFound(format!("API key '{}' not found", key_id)))?;

    Ok(Json(json!({
        "id": key.id,
        "name": key.name,
        "prefix": key.key_prefix,
        "role": key.role,
        "createdAt": key.created_at.to_rfc3339(),
        "expiresAt": key.expires_at.map(|t| t.to_rfc3339()),
        "revoked": key.revoked,
        "createdBy": key.created_by,
        "description": key.description,
        "lastUsedAt": key.last_used_at.map(|t| t.to_rfc3339()),
    })))
}

/// DELETE /api/v1/auth/keys/:id — revoke an API key.
pub async fn revoke_key(
    auth: RequireAuth,
    State(state): State<Arc<AppState>>,
    Path(key_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Permission::ManageApiKeys)
        .map_err(|e| ApiError::Forbidden(e.0.to_string()))?;

    let key = state
        .auth_store
        .revoke_key(&key_id)
        .map_err(|_| ApiError::NotFound(format!("API key '{}' not found", key_id)))?;

    // Persist updated keys
    state.save_auth_keys();

    Ok(Json(json!({
        "id": key.id,
        "name": key.name,
        "revoked": true,
        "message": format!("API key '{}' has been revoked", key.name),
    })))
}

/// GET /api/v1/auth/me — get the current auth context.
///
/// This is a lightweight endpoint for the UI to check authentication status
/// and the current role.
pub async fn whoami(auth: RequireAuth) -> Json<Value> {
    let ctx = auth.context();
    Json(json!({
        "authenticated": true,
        "keyId": ctx.key_id,
        "keyName": ctx.key_name,
        "role": ctx.role,
        "permissions": {
            "viewDags": ctx.has_permission(Permission::ViewDags),
            "triggerRun": ctx.has_permission(Permission::TriggerRun),
            "manageEnvironments": ctx.has_permission(Permission::CreateEnvironment),
            "planApply": ctx.has_permission(Permission::ApplyPlan),
            "manageApiKeys": ctx.has_permission(Permission::ManageApiKeys),
            "drainWorker": ctx.has_permission(Permission::DrainWorker),
        }
    }))
}
