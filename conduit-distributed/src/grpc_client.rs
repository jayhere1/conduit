//! gRPC client that connects a Worker to a remote Coordinator.
//!
//! This module ties the existing [`crate::worker::Worker`] to the network by:
//! 1. Connecting to the coordinator's gRPC endpoint
//! 2. Sending a `Register` RPC and consuming the returned task-assignment stream
//! 3. Forwarding task results via `ReportResult`
//! 4. Running a bidirectional heartbeat stream
//! 5. Streaming log entries via `StreamLogs`
//!
//! # Usage
//!
//! ```rust,no_run
//! use conduit_distributed::grpc_client::WorkerGrpcClient;
//! use conduit_distributed::worker::{Worker, WorkerConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = WorkerConfig {
//!     coordinator_addr: "http://coordinator:9400".into(),
//!     ..WorkerConfig::default()
//! };
//! let (worker, result_rx, log_rx) = Worker::new(config.clone());
//! let client = WorkerGrpcClient::new(config, worker, result_rx, log_rx);
//! client.run().await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::Request;
use tracing::{info, warn, error};

use crate::convert;
use crate::proto_types as local;
use crate::worker::{Worker, WorkerConfig};

// Use the canonical generated types from the crate root.
use crate::generated_proto as proto;

use proto::coordinator_client::CoordinatorClient;

/// Error type for the gRPC client.
#[derive(Debug, thiserror::Error)]
pub enum GrpcClientError {
    #[error("Failed to connect to coordinator at {addr}: {source}")]
    ConnectionFailed {
        addr: String,
        source: tonic::transport::Error,
    },

    #[error("TLS configuration error: {0}")]
    TlsConfig(String),

    #[error("Registration failed: {0}")]
    RegistrationFailed(tonic::Status),

    #[error("Stream ended unexpectedly")]
    StreamEnded,

    #[error("gRPC error: {0}")]
    Rpc(#[from] tonic::Status),
}

/// A gRPC client that bridges a `Worker` to a remote `Coordinator`.
pub struct WorkerGrpcClient {
    config: WorkerConfig,
    worker: Arc<Worker>,
    result_rx: mpsc::UnboundedReceiver<local::TaskResult>,
    log_rx: mpsc::UnboundedReceiver<local::TaskLogEntry>,
}

impl WorkerGrpcClient {
    /// Create a new gRPC client for the given worker.
    pub fn new(
        config: WorkerConfig,
        worker: Worker,
        result_rx: mpsc::UnboundedReceiver<local::TaskResult>,
        log_rx: mpsc::UnboundedReceiver<local::TaskLogEntry>,
    ) -> Self {
        Self {
            config,
            worker: Arc::new(worker),
            result_rx,
            log_rx,
        }
    }

    /// Connect to the coordinator and run the worker loop.
    ///
    /// This method:
    /// 1. Establishes a gRPC connection to the coordinator
    /// 2. Registers the worker
    /// 3. Spawns background tasks for heartbeats, result forwarding, and log streaming
    /// 4. Processes incoming task assignments from the coordinator
    ///
    /// Returns when the connection is lost or the worker is drained.
    pub async fn run(self) -> Result<(), GrpcClientError> {
        let addr = &self.config.coordinator_addr;
        let use_tls = self.config.tls_ca_cert_path.is_some();
        let scheme = if use_tls { "https" } else { "http" };
        let endpoint_uri = format!("{}://{}", scheme, addr);

        info!(addr = %addr, tls = use_tls, worker_id = %self.config.worker_id, "Connecting to coordinator");

        let mut endpoint = tonic::transport::Endpoint::from_shared(endpoint_uri.clone())
            .map_err(|e| GrpcClientError::ConnectionFailed {
                addr: addr.clone(),
                source: e.into(),
            })?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(300));

        if let Some(ca_path) = &self.config.tls_ca_cert_path {
            let ca_cert = std::fs::read_to_string(ca_path).map_err(|e| {
                GrpcClientError::TlsConfig(format!(
                    "Failed to read CA certificate at {}: {}", ca_path, e
                ))
            })?;
            let ca = tonic::transport::Certificate::from_pem(ca_cert);
            let tls = tonic::transport::ClientTlsConfig::new().ca_certificate(ca);
            endpoint = endpoint.tls_config(tls).map_err(|e| {
                GrpcClientError::TlsConfig(format!("Failed to configure TLS: {}", e))
            })?;
        }

        let channel = endpoint.connect()
            .await
            .map_err(|e| GrpcClientError::ConnectionFailed {
                addr: addr.clone(),
                source: e,
            })?;

        let mut client = CoordinatorClient::new(channel.clone());

        // ── Step 1: Register ─────────────────────────────────────────────
        let registration = self.worker.registration();
        let proto_reg = convert::register_request_to_proto(&registration);

        info!(
            worker_id = %registration.worker_id,
            capacity = registration.capacity,
            pools = ?registration.pool_affinity,
            "Registering with coordinator"
        );

        let response = client
            .register(Request::new(proto_reg))
            .await
            .map_err(GrpcClientError::RegistrationFailed)?;

        let mut assignment_stream = response.into_inner();

        info!("Registration successful, listening for task assignments");

        // ── Step 2: Spawn heartbeat loop ─────────────────────────────────
        let heartbeat_handle = {
            let worker = self.worker.clone();
            let mut hb_client = CoordinatorClient::new(channel.clone());
            let interval_secs = self.config.heartbeat_interval_secs;

            tokio::spawn(async move {
                if let Err(e) = Self::heartbeat_loop(worker, &mut hb_client, interval_secs).await {
                    error!("Heartbeat loop ended: {}", e);
                }
            })
        };

        // ── Step 3: Spawn result forwarding ──────────────────────────────
        let result_handle = {
            let mut res_client = CoordinatorClient::new(channel.clone());
            let result_rx = self.result_rx;

            tokio::spawn(async move {
                if let Err(e) = Self::result_forwarder(result_rx, &mut res_client).await {
                    error!("Result forwarder ended: {}", e);
                }
            })
        };

        // ── Step 4: Spawn log streaming ──────────────────────────────────
        let log_handle = {
            let mut log_client = CoordinatorClient::new(channel.clone());
            let log_rx = self.log_rx;

            tokio::spawn(async move {
                if let Err(e) = Self::log_streamer(log_rx, &mut log_client).await {
                    error!("Log streamer ended: {}", e);
                }
            })
        };

        // ── Step 5: Process task assignments ─────────────────────────────
        let worker = self.worker.clone();
        while let Some(result) = assignment_stream.next().await {
            match result {
                Ok(proto_assignment) => {
                    let local_assignment =
                        convert::task_assignment_from_proto(&proto_assignment);

                    info!(
                        assignment_id = %local_assignment.assignment_id,
                        task_id = %local_assignment.task_id,
                        "Received task assignment from coordinator"
                    );

                    worker.handle_assignment(local_assignment).await;
                }
                Err(status) => {
                    error!("Assignment stream error: {}", status);
                    break;
                }
            }
        }

        warn!("Assignment stream ended, shutting down worker connection");

        // Clean up background tasks.
        heartbeat_handle.abort();
        result_handle.abort();
        log_handle.abort();

        Ok(())
    }

    /// Run the bidirectional heartbeat stream.
    async fn heartbeat_loop(
        worker: Arc<Worker>,
        client: &mut CoordinatorClient<tonic::transport::Channel>,
        interval_secs: u64,
    ) -> Result<(), GrpcClientError> {
        let (tx, rx) = mpsc::unbounded_channel();
        let _worker_id = worker.id().to_string();

        // Spawn a task that periodically sends heartbeats.
        let hb_sender = tokio::spawn({
            let worker = worker.clone();
            async move {
                let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
                loop {
                    interval.tick().await;
                    let local_hb = worker.heartbeat();
                    let proto_hb = convert::heartbeat_to_proto(&local_hb);
                    if tx.send(proto_hb).is_err() {
                        break;
                    }
                }
            }
        });

        let outbound = UnboundedReceiverStream::new(rx);
        let response = client.heartbeat(Request::new(outbound)).await?;
        let mut directives = response.into_inner();

        while let Some(result) = directives.next().await {
            match result {
                Ok(proto_directive) => {
                    let directive = convert::directive_from_proto(&proto_directive);
                    match &directive {
                        local::CoordinatorDirective::CancelTask {
                            assignment_id,
                            reason,
                        } => {
                            warn!(
                                assignment_id = %assignment_id,
                                reason = %reason,
                                "Received cancel directive"
                            );
                            worker.cancel_task(assignment_id).await;
                        }
                        local::CoordinatorDirective::Drain {
                            reason,
                            grace_period_secs,
                        } => {
                            warn!(
                                reason = %reason,
                                grace_period = grace_period_secs,
                                "Received drain directive"
                            );
                            worker.drain().await;
                            worker
                                .wait_for_drain(Duration::from_secs(*grace_period_secs as u64))
                                .await;
                        }
                        local::CoordinatorDirective::HeartbeatAck { .. } => {
                            // Normal ack, nothing to do.
                        }
                    }
                }
                Err(e) => {
                    warn!("Heartbeat directive error: {}", e);
                    break;
                }
            }
        }

        hb_sender.abort();
        Ok(())
    }

    /// Forward task results from the worker to the coordinator.
    async fn result_forwarder(
        mut result_rx: mpsc::UnboundedReceiver<local::TaskResult>,
        client: &mut CoordinatorClient<tonic::transport::Channel>,
    ) -> Result<(), GrpcClientError> {
        while let Some(local_result) = result_rx.recv().await {
            let proto_result = convert::task_result_to_proto(&local_result);

            info!(
                assignment_id = %local_result.assignment_id,
                outcome = ?local_result.outcome,
                "Forwarding task result to coordinator"
            );

            match client.report_result(Request::new(proto_result)).await {
                Ok(response) => {
                    let ack = response.into_inner();
                    if !ack.success {
                        warn!(
                            assignment = %local_result.assignment_id,
                            message = %ack.message,
                            "Coordinator rejected result"
                        );
                    }
                }
                Err(status) => {
                    error!(
                        assignment = %local_result.assignment_id,
                        "Failed to report result: {}",
                        status
                    );
                    // Don't break — try to send subsequent results.
                }
            }
        }

        Ok(())
    }

    /// Stream log entries from the worker to the coordinator.
    ///
    /// Batches log entries into client-streaming RPCs. Each RPC sends
    /// entries until the channel is empty for a short duration, then
    /// a new RPC is started for the next batch.
    async fn log_streamer(
        mut log_rx: mpsc::UnboundedReceiver<local::TaskLogEntry>,
        client: &mut CoordinatorClient<tonic::transport::Channel>,
    ) -> Result<(), GrpcClientError> {
        // We collect log entries and periodically flush them via a
        // client-streaming RPC. For simplicity, we open one long-lived
        // stream and keep sending on it.
        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn a task to bridge from the worker's log channel to
        // the proto stream.
        let bridge = tokio::spawn(async move {
            while let Some(local_entry) = log_rx.recv().await {
                let proto_entry = convert::log_entry_to_proto(&local_entry);
                if tx.send(proto_entry).is_err() {
                    break;
                }
            }
        });

        let outbound = UnboundedReceiverStream::new(rx);
        match client.stream_logs(Request::new(outbound)).await {
            Ok(response) => {
                let ack = response.into_inner();
                info!(message = %ack.message, "Log stream completed");
            }
            Err(status) => {
                warn!("Log stream error: {}", status);
            }
        }

        bridge.abort();
        Ok(())
    }
}

/// Convenience function to start a worker and connect it to a coordinator.
///
/// This is the main entry point for running a worker node.
pub async fn run_worker(config: WorkerConfig) -> Result<(), GrpcClientError> {
    let (worker, result_rx, log_rx) = Worker::new(config.clone());

    info!(
        worker_id = %config.worker_id,
        coordinator = %config.coordinator_addr,
        capacity = config.capacity,
        pools = ?config.pool_affinity,
        "Starting worker"
    );

    let client = WorkerGrpcClient::new(config, worker, result_rx, log_rx);
    client.run().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn client_can_be_constructed() {
        let config = WorkerConfig {
            worker_id: "test-worker".into(),
            coordinator_addr: "localhost:9400".into(),
            capacity: 4,
            pool_affinity: vec!["default".into()],
            labels: HashMap::new(),
            heartbeat_interval_secs: 5,
            graceful_shutdown: true,
            tls_ca_cert_path: None,
        };

        let (worker, result_rx, log_rx) = Worker::new(config.clone());
        let _client = WorkerGrpcClient::new(config, worker, result_rx, log_rx);
    }
}
