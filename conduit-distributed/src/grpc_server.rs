//! gRPC server implementation for the Coordinator.
//!
//! Implements the `Coordinator` tonic service trait by delegating to
//! the existing [`crate::coordinator::Coordinator`] struct. This bridges
//! the generated protobuf types and the hand-written Rust types.

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::{wrappers::UnboundedReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

use crate::coordinator::Coordinator;
use crate::convert;

// Use the canonical generated types from the crate root.
use crate::generated_proto as proto;

type TaskAssignmentStream = Pin<Box<dyn Stream<Item = Result<proto::TaskAssignment, Status>> + Send>>;
type DirectiveStream = Pin<Box<dyn Stream<Item = Result<proto::CoordinatorDirective, Status>> + Send>>;

/// gRPC service implementation that wraps the Coordinator.
pub struct CoordinatorGrpcService {
    coordinator: Arc<Coordinator>,
}

impl CoordinatorGrpcService {
    pub fn new(coordinator: Arc<Coordinator>) -> Self {
        Self { coordinator }
    }

    /// Create a tonic server from this service.
    pub fn into_server(self) -> proto::coordinator_server::CoordinatorServer<Self> {
        proto::coordinator_server::CoordinatorServer::new(self)
    }
}

#[tonic::async_trait]
impl proto::coordinator_server::Coordinator for CoordinatorGrpcService {
    type RegisterStream = TaskAssignmentStream;

    /// Worker registers and receives a stream of task assignments.
    async fn register(
        &self,
        request: Request<proto::RegisterRequest>,
    ) -> Result<Response<Self::RegisterStream>, Status> {
        let proto_req = request.into_inner();
        let local_req = convert::register_request_from_proto(&proto_req);

        info!(
            worker_id = %local_req.worker_id,
            hostname = %local_req.hostname,
            capacity = local_req.capacity,
            "Worker registering via gRPC"
        );

        // Register the worker and get back a channel of task assignments.
        // Coordinator::register_worker is synchronous.
        let task_rx = self.coordinator.register_worker(&local_req);

        // Convert the unbounded receiver into a gRPC stream,
        // translating local TaskAssignment → proto TaskAssignment.
        let stream = UnboundedReceiverStream::new(task_rx).map(|local_assignment| {
            Ok(convert::task_assignment_to_proto(&local_assignment))
        });

        Ok(Response::new(Box::pin(stream)))
    }

    /// Worker reports a task result.
    async fn report_result(
        &self,
        request: Request<proto::TaskResult>,
    ) -> Result<Response<proto::Ack>, Status> {
        let proto_result = request.into_inner();
        let local_result = convert::task_result_from_proto(&proto_result);

        info!(
            assignment_id = %local_result.assignment_id,
            worker_id = %local_result.worker_id,
            outcome = ?local_result.outcome,
            "Task result received via gRPC"
        );

        // Coordinator::handle_result is synchronous.
        self.coordinator.handle_result(local_result);

        Ok(Response::new(proto::Ack {
            success: true,
            message: "Result accepted".to_string(),
        }))
    }

    type HeartbeatStream = DirectiveStream;

    /// Bidirectional heartbeat stream.
    async fn heartbeat(
        &self,
        request: Request<Streaming<proto::WorkerHeartbeat>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let mut inbound = request.into_inner();
        let coordinator = self.coordinator.clone();
        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn a task to process inbound heartbeats and send directives.
        tokio::spawn(async move {
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(proto_hb) => {
                        let local_hb = convert::heartbeat_from_proto(&proto_hb);
                        // Coordinator::handle_heartbeat is synchronous.
                        let directive = coordinator.handle_heartbeat(&local_hb);
                        let proto_directive = convert::directive_to_proto(&directive);
                        if tx.send(Ok(proto_directive)).is_err() {
                            break; // Client disconnected
                        }
                    }
                    Err(e) => {
                        warn!("Heartbeat stream error: {}", e);
                        break;
                    }
                }
            }
        });

        let stream = UnboundedReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    /// Worker streams real-time log entries.
    async fn stream_logs(
        &self,
        request: Request<Streaming<proto::TaskLogEntry>>,
    ) -> Result<Response<proto::Ack>, Status> {
        let mut inbound = request.into_inner();
        let coordinator = self.coordinator.clone();
        let mut count = 0u64;

        while let Some(result) = inbound.next().await {
            match result {
                Ok(proto_entry) => {
                    let local_entry = convert::log_entry_from_proto(&proto_entry);
                    // Coordinator::handle_log_entry is synchronous.
                    coordinator.handle_log_entry(local_entry);
                    count += 1;
                }
                Err(e) => {
                    warn!("Log stream error: {}", e);
                    break;
                }
            }
        }

        Ok(Response::new(proto::Ack {
            success: true,
            message: format!("Received {} log entries", count),
        }))
    }

    /// Query cluster status.
    async fn cluster_status(
        &self,
        _request: Request<proto::ClusterStatusRequest>,
    ) -> Result<Response<proto::ClusterStatusResponse>, Status> {
        // Coordinator::cluster_status is synchronous.
        let local_status = self.coordinator.cluster_status();
        let proto_status = convert::cluster_status_to_proto(&local_status);
        Ok(Response::new(proto_status))
    }
}

/// Start the gRPC server for the coordinator.
///
/// When `tls_cert_path` and `tls_key_path` are both `Some`, the server
/// will require TLS. Otherwise it listens in plaintext.
pub async fn serve_grpc(
    coordinator: Arc<Coordinator>,
    addr: std::net::SocketAddr,
    tls_cert_path: Option<&str>,
    tls_key_path: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = CoordinatorGrpcService::new(coordinator);
    let server = service.into_server();

    let mut builder = tonic::transport::Server::builder();

    if let (Some(cert_path), Some(key_path)) = (tls_cert_path, tls_key_path) {
        let cert = std::fs::read_to_string(cert_path)?;
        let key = std::fs::read_to_string(key_path)?;
        let identity = tonic::transport::Identity::from_pem(cert, key);
        let tls = tonic::transport::ServerTlsConfig::new().identity(identity);
        builder = builder.tls_config(tls)?;
        info!(%addr, "Coordinator gRPC server starting with TLS");
    } else {
        info!(%addr, "Coordinator gRPC server starting (plaintext)");
    }

    builder.add_service(server).serve(addr).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::CoordinatorConfig;

    #[test]
    fn service_can_be_constructed() {
        let config = CoordinatorConfig::default();
        let (coordinator, _rx) = Coordinator::new(config);
        let _service = CoordinatorGrpcService::new(Arc::new(coordinator));
    }
}
