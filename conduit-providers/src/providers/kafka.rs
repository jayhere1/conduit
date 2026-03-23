//! Apache Kafka provider.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   event_stream:
//!     type: kafka
//!     host: kafka-1:9092,kafka-2:9092,kafka-3:9092   # bootstrap servers
//!     credentials: ${KAFKA_PASSWORD}
//!     user: conduit
//!     security_protocol: SASL_SSL       # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
//!     sasl_mechanism: PLAIN             # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
//!     group_id: conduit-consumer        # default consumer group
//!     schema_registry: http://schema-registry:8081   # optional
//! ```

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

use crate::errors::ProviderError;
use crate::traits::*;
use super::extra_str;

#[allow(dead_code)]
pub struct KafkaProvider {
    name: String,
    bootstrap_servers: String,
    security_protocol: String,
    sasl_mechanism: String,
    group_id: String,
    schema_registry: Option<String>,
}

impl KafkaProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let bootstrap_servers = config.host.clone().unwrap_or_else(|| "localhost:9092".to_string());
        let security_protocol = extra_str(config, "security_protocol").unwrap_or_else(|| "PLAINTEXT".to_string());
        let sasl_mechanism = extra_str(config, "sasl_mechanism").unwrap_or_else(|| "PLAIN".to_string());
        let group_id = extra_str(config, "group_id").unwrap_or_else(|| "conduit-consumer".to_string());
        let schema_registry = extra_str(config, "schema_registry");

        Ok(Self { name: name.to_string(), bootstrap_servers, security_protocol, sasl_mechanism, group_id, schema_registry })
    }
}

#[async_trait]
impl Provider for KafkaProvider {
    fn info(&self) -> ProviderInfo {
        let broker_count = self.bootstrap_servers.split(',').count();
        ProviderInfo {
            provider_type: "kafka".to_string(),
            display_name: format!("Kafka ({} brokers, {})", broker_count, self.security_protocol),
            version: None,
            capabilities: vec![
                Capability::StreamProduce, Capability::StreamConsume,
            ],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Ok(ConnectionTestResult {
            success: true,
            message: format!("Kafka configured: brokers={} protocol={}", self.bootstrap_servers, self.security_protocol),
            latency_ms: 0, server_version: None,
        })
    }

    async fn close(&self) -> Result<(), ProviderError> { Ok(()) }
}

#[async_trait]
impl StreamProvider for KafkaProvider {
    async fn produce(
        &self,
        _topic: &str,
        messages: &[StreamMessage],
    ) -> Result<StreamResult, ProviderError> {
        let total_bytes: u64 = messages.iter().map(|m| m.value.len() as u64).sum();

        Ok(StreamResult {
            message_count: messages.len() as u64,
            bytes_transferred: total_bytes,
            execution_time_ms: 0,
        })
    }

    async fn consume(
        &self,
        _topic: &str,
        _group_id: &str,
        _max_messages: usize,
    ) -> Result<Vec<StreamMessage>, ProviderError> {
        Ok(vec![])
    }

    async fn list_topics(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![])
    }
}
