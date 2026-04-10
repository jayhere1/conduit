//! Apache Kafka provider.
//!
//! Uses `rskafka` (pure-Rust Kafka client) for producing and consuming
//! messages. No C library dependencies required.
//!
//! # Configuration
//! ```yaml
//! connections:
//!   event_stream:
//!     type: kafka
//!     host: kafka-1:9092,kafka-2:9092,kafka-3:9092   # bootstrap servers
//!     credentials: ${KAFKA_PASSWORD}
//!     user: conduit
//!     security_protocol: PLAINTEXT       # PLAINTEXT or SASL_PLAINTEXT
//!     sasl_mechanism: PLAIN              # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
//!     group_id: conduit-consumer         # default consumer group
//!     schema_registry: http://schema-registry:8081   # optional
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use tokio::sync::OnceCell;

use crate::errors::ProviderError;
use crate::traits::*;
use super::extra_str;

pub struct KafkaProvider {
    name: String,
    bootstrap_servers: String,
    security_protocol: String,
    sasl_mechanism: String,
    group_id: String,
    schema_registry: Option<String>,
    user: Option<String>,
    password: Option<String>,
    client: OnceCell<Arc<rskafka::client::Client>>,
}

impl KafkaProvider {
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let bootstrap_servers = config
            .host
            .clone()
            .unwrap_or_else(|| "localhost:9092".to_string());
        let security_protocol = extra_str(config, "security_protocol")
            .unwrap_or_else(|| "PLAINTEXT".to_string());
        let sasl_mechanism =
            extra_str(config, "sasl_mechanism").unwrap_or_else(|| "PLAIN".to_string());
        let group_id =
            extra_str(config, "group_id").unwrap_or_else(|| "conduit-consumer".to_string());
        let schema_registry = extra_str(config, "schema_registry");
        let user = extra_str(config, "user");
        let password = config
            .credentials
            .as_deref()
            .map(super::resolve_credential)
            .transpose()
            .ok()
            .flatten();

        Ok(Self {
            name: name.to_string(),
            bootstrap_servers,
            security_protocol,
            sasl_mechanism,
            group_id,
            schema_registry,
            user,
            password,
            client: OnceCell::new(),
        })
    }

    /// Lazily connect to the Kafka cluster.
    async fn ensure_client(&self) -> Result<&Arc<rskafka::client::Client>, ProviderError> {
        self.client
            .get_or_try_init(|| async {
                let brokers: Vec<String> = self
                    .bootstrap_servers
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();

                let client = rskafka::client::ClientBuilder::new(brokers)
                    .build()
                    .await
                    .map_err(|e| ProviderError::ConnectionFailed {
                        name: self.name.clone(),
                        reason: format!("Kafka connection failed: {}", e),
                    })?;

                Ok(Arc::new(client))
            })
            .await
    }
}

#[async_trait]
impl Provider for KafkaProvider {
    fn info(&self) -> ProviderInfo {
        let broker_count = self.bootstrap_servers.split(',').count();
        ProviderInfo {
            provider_type: "kafka".to_string(),
            display_name: format!(
                "Kafka ({} brokers, {})",
                broker_count, self.security_protocol
            ),
            version: None,
            capabilities: vec![Capability::StreamProduce, Capability::StreamConsume],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        let start = Instant::now();

        match self.ensure_client().await {
            Ok(client) => {
                // List topics to verify the connection is live
                match client.list_topics().await {
                    Ok(topics) => Ok(ConnectionTestResult {
                        success: true,
                        message: format!(
                            "Kafka cluster reachable ({} topics)",
                            topics.len()
                        ),
                        latency_ms: start.elapsed().as_millis() as u64,
                        server_version: None,
                    }),
                    Err(e) => Ok(ConnectionTestResult {
                        success: false,
                        message: format!("Kafka metadata fetch failed: {}", e),
                        latency_ms: start.elapsed().as_millis() as u64,
                        server_version: None,
                    }),
                }
            }
            Err(e) => Ok(ConnectionTestResult {
                success: false,
                message: format!("Kafka connection failed: {}", e),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            }),
        }
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl StreamProvider for KafkaProvider {
    async fn produce(
        &self,
        topic: &str,
        messages: &[StreamMessage],
    ) -> Result<StreamResult, ProviderError> {
        let start = Instant::now();
        let client = self.ensure_client().await?;

        // Get partition client for partition 0
        let partition_client = client
            .partition_client(topic, 0, rskafka::client::partition::UnknownTopicHandling::Error)
            .await
            .map_err(|e| ProviderError::StreamFailed {
                connection: self.name.clone(),
                reason: format!("Failed to get partition client for '{}': {}", topic, e),
            })?;

        let mut total_bytes: u64 = 0;

        // Produce messages
        let records: Vec<rskafka::record::Record> = messages
            .iter()
            .map(|msg| {
                total_bytes += msg.value.len() as u64;
                rskafka::record::Record {
                    key: msg.key.as_ref().map(|k| k.as_bytes().to_vec()),
                    value: Some(msg.value.clone()),
                    headers: msg
                        .headers
                        .iter()
                        .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
                        .collect(),
                    timestamp: msg
                        .timestamp
                        .unwrap_or_else(chrono::Utc::now),
                }
            })
            .collect();

        partition_client
            .produce(records, rskafka::client::partition::Compression::NoCompression)
            .await
            .map_err(|e| ProviderError::StreamFailed {
                connection: self.name.clone(),
                reason: format!("Kafka produce failed: {}", e),
            })?;

        Ok(StreamResult {
            message_count: messages.len() as u64,
            bytes_transferred: total_bytes,
            execution_time_ms: start.elapsed().as_millis() as u64,
        })
    }

    async fn consume(
        &self,
        topic: &str,
        _group_id: &str,
        max_messages: usize,
    ) -> Result<Vec<StreamMessage>, ProviderError> {
        let client = self.ensure_client().await?;

        let partition_client = client
            .partition_client(topic, 0, rskafka::client::partition::UnknownTopicHandling::Error)
            .await
            .map_err(|e| ProviderError::StreamFailed {
                connection: self.name.clone(),
                reason: format!("Failed to get partition client for '{}': {}", topic, e),
            })?;

        // Fetch from the beginning
        let (records, _high_watermark) = partition_client
            .fetch_records(0, 1..1_048_576, 5_000)
            .await
            .map_err(|e| ProviderError::StreamFailed {
                connection: self.name.clone(),
                reason: format!("Kafka consume failed: {}", e),
            })?;

        let messages: Vec<StreamMessage> = records
            .into_iter()
            .take(max_messages)
            .map(|record| {
                let rec = record.record;
                StreamMessage {
                    key: rec.key.map(|k| String::from_utf8_lossy(&k).to_string()),
                    value: rec.value.unwrap_or_default(),
                    headers: rec
                        .headers
                        .into_iter()
                        .map(|(k, v)| (k, String::from_utf8_lossy(&v).to_string()))
                        .collect(),
                    timestamp: Some(rec.timestamp),
                }
            })
            .collect();

        Ok(messages)
    }

    async fn list_topics(&self) -> Result<Vec<String>, ProviderError> {
        let client = self.ensure_client().await?;

        let topics = client.list_topics().await.map_err(|e| {
            ProviderError::StreamFailed {
                connection: self.name.clone(),
                reason: format!("Failed to list topics: {}", e),
            }
        })?;

        Ok(topics.into_iter().map(|t| t.name).collect())
    }
}
