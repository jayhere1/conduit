//! conduit-api: REST + WebSocket API server for Conduit.
//!
//! Provides endpoints for:
//! - DAG listing, compilation, and graph visualization
//! - DAG run triggering and status monitoring
//! - Live task log streaming via WebSocket
//! - Environment management (create, list, promote, rollback)
//! - Plan/apply workflow (generate plan, review, apply)
//! - Event history and time-travel queries
//! - Health checks and metrics

pub mod auth;
pub mod middleware;
pub mod rate_limit;
pub mod routes;
pub mod state;
pub mod handlers;
pub mod websocket;
pub mod error;

pub use state::AppState;

use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

/// Start the Conduit API server.
pub async fn serve(
    state: Arc<AppState>,
    addr: SocketAddr,
) -> anyhow::Result<()> {
    let app = routes::build_router(state);

    info!(%addr, "Conduit API server starting");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
