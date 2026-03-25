//! Prometheus metrics scrape endpoint.
//!
//! Serves metrics at `/metrics` in Prometheus text exposition format.
//! This is separate from the demo metrics at `/api/v1/metrics`.

use axum::http::header;
use axum::response::IntoResponse;

/// GET /metrics — Prometheus scrape endpoint.
pub async fn prometheus_metrics() -> impl IntoResponse {
    let body = conduit_common::metrics::try_global()
        .map(|m| m.encode())
        .unwrap_or_default();

    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}
