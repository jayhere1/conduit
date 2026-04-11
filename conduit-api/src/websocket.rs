//! WebSocket handler for live event streaming.
//!
//! Clients connect to /ws/events and receive real-time JSON events
//! for every state change: task started, task completed, DAG run finished, etc.
//! This powers live-updating dashboards without polling.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use tracing::{info, warn};

use crate::auth::AuthStore;
use crate::AppState;

/// Max WebSocket message size: 1 MB.
const WS_MAX_MESSAGE_SIZE: usize = 1024 * 1024;
/// Max WebSocket frame size: 1 MB.
const WS_MAX_FRAME_SIZE: usize = 1024 * 1024;

/// Upgrade an HTTP connection to a WebSocket.
///
/// When authentication is enabled, the client must provide a valid
/// `Authorization: Bearer <token>` header. Unauthenticated requests
/// receive a 401 response instead of a WebSocket upgrade.
pub async fn ws_handler(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // Authenticate when auth is enabled
    if state.auth_store.auth_enabled {
        let token = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| AuthStore::extract_bearer(v).ok());

        match token {
            Some(t) => {
                if state.auth_store.authenticate(t).is_err() {
                    return StatusCode::UNAUTHORIZED.into_response();
                }
            }
            None => {
                return StatusCode::UNAUTHORIZED.into_response();
            }
        }
    }

    ws.max_message_size(WS_MAX_MESSAGE_SIZE)
        .max_frame_size(WS_MAX_FRAME_SIZE)
        .on_upgrade(move |socket| handle_socket(socket, state))
        .into_response()
}

/// Handle a single WebSocket connection.
///
/// Subscribes to the broadcast channel and forwards all events
/// to the connected client as JSON text frames.
async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.event_tx.subscribe();

    info!("WebSocket client connected");

    // Send a welcome message
    let welcome = serde_json::json!({
        "type": "connected",
        "message": "Conduit event stream",
        "version": "0.1.0",
    });

    if socket
        .send(Message::Text(welcome.to_string()))
        .await
        .is_err()
    {
        return;
    }

    // Forward events until the client disconnects
    loop {
        tokio::select! {
            // Receive an event from the broadcast channel
            result = rx.recv() => {
                match result {
                    Ok(event_json) => {
                        if socket
                            .send(Message::Text(event_json))
                            .await
                            .is_err()
                        {
                            // Client disconnected
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "WebSocket client lagged, dropped events");
                        let lag_msg = serde_json::json!({
                            "type": "warning",
                            "message": format!("Dropped {} events (client too slow)", n),
                        });
                        let _ = socket.send(Message::Text(lag_msg.to_string())).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }

            // Handle incoming messages from the client (ping/pong, close)
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = socket.send(Message::Pong(data)).await;
                    }
                    Some(Ok(_)) => {
                        // Ignore other messages (text commands could be added later)
                    }
                    Some(Err(_)) => {
                        break;
                    }
                }
            }
        }
    }

    info!("WebSocket client disconnected");
}
