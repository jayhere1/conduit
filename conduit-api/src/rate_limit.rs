//! Per-IP rate limiting middleware for the Conduit API.
//!
//! Uses a keyed token-bucket rate limiter (via `governor`) to enforce
//! per-client request limits. Each client is identified by their IP address.
//!
//! Default: 10 requests/second with a burst capacity of 50.

use axum::{
    extract::ConnectInfo,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use governor::{clock::DefaultClock, state::keyed::DefaultKeyedStateStore, Quota, RateLimiter};
use serde_json::json;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

/// A keyed rate limiter that tracks limits per client IP address.
pub type KeyedRateLimiter = RateLimiter<String, DefaultKeyedStateStore<String>, DefaultClock>;

/// Create a new per-IP rate limiter.
///
/// Allows 10 requests per second per IP, with a burst capacity of 50.
pub fn create_rate_limiter() -> Arc<KeyedRateLimiter> {
    Arc::new(RateLimiter::keyed(
        Quota::per_second(NonZeroU32::new(10).unwrap()).allow_burst(NonZeroU32::new(50).unwrap()),
    ))
}

/// Axum middleware that enforces per-IP rate limits.
///
/// Extracts the client IP from `ConnectInfo<SocketAddr>` and checks
/// the shared rate limiter. Returns 429 Too Many Requests if the
/// client has exceeded their quota.
///
/// Note: When `ConnectInfo` is not available (e.g., in tests without
/// a real TCP listener), the middleware passes requests through
/// without rate limiting.
pub async fn rate_limit_middleware(
    connect_info: Option<ConnectInfo<SocketAddr>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    // Extract rate limiter from request extensions
    let limiter = request.extensions().get::<Arc<KeyedRateLimiter>>().cloned();

    if let (Some(limiter), Some(ConnectInfo(addr))) = (limiter, connect_info) {
        let key = addr.ip().to_string();
        if limiter.check_key(&key).is_err() {
            tracing::warn!(client_ip = %key, "Rate limit exceeded");
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": {
                        "type": "rate_limited",
                        "message": "Too many requests. Please slow down.",
                    }
                })),
            )
                .into_response();
        }
    }

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_rate_limiter() {
        let limiter = create_rate_limiter();
        // Should allow initial requests
        let key = "127.0.0.1".to_string();
        assert!(limiter.check_key(&key).is_ok());
    }

    #[test]
    fn test_burst_capacity() {
        let limiter = create_rate_limiter();
        let key = "10.0.0.1".to_string();

        // Should allow up to burst capacity
        let mut allowed = 0;
        for _ in 0..60 {
            if limiter.check_key(&key).is_ok() {
                allowed += 1;
            }
        }

        // Burst of 50 should be allowed
        assert_eq!(allowed, 50, "Expected burst capacity of 50");
    }
}
