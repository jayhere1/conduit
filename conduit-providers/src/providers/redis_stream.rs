//! Redis Streams provider
//!
//! Provides connectivity to Redis Streams for message queuing.
//!
//! # Configuration
//!
//! ```yaml
//! type: redis
//! config:
//!   host: localhost
//!   port: 6379
//!   database: 0
//!   stream_key: my_stream
//!   consumer_group: my_group
//!   password: optional_password
//! ```

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use crate::errors::ProviderError;
use crate::traits::*;
use super::{extra_str, extra_u64};

/// Redis Streams provider
#[allow(dead_code)]
pub struct RedisStreamProvider {
    name: String,
    host: String,
    port: u64,
    database: u64,
    stream_key: String,
    consumer_group: String,
    password: Option<String>,
}

impl RedisStreamProvider {
    /// Create a new Redis Streams provider from configuration
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let host = extra_str(config, "host").unwrap_or_else(|| "localhost".to_string());
        let port = extra_u64(config, "port").unwrap_or(6379);
        let database = extra_u64(config, "database").unwrap_or(0);
        let stream_key = extra_str(config, "stream_key").unwrap_or_default();
        let consumer_group = extra_str(config, "consumer_group").unwrap_or_default();
        let password = extra_str(config, "password");

        Ok(RedisStreamProvider {
            name: name.to_string(),
            host,
            port,
            database,
            stream_key,
            consumer_group,
            password,
        })
    }
}

#[async_trait]
impl Provider for RedisStreamProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "redis".to_string(),
            display_name: format!("Redis ({}:{}/{})", self.host, self.port, self.database),
            version: None,
            capabilities: vec![
                Capability::StreamProduce,
                Capability::StreamConsume,
            ],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        use tokio::net::TcpStream;
        use tokio::time::{timeout, Duration};
        use std::time::Instant;

        let start = Instant::now();
        let addr = format!("{}:{}", self.host, self.port);

        match timeout(Duration::from_secs(5), TcpStream::connect(&addr)).await {
            Ok(Ok(_)) => Ok(ConnectionTestResult {
                success: true,
                message: format!("TCP connection to {} successful", addr),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            }),
            Ok(Err(e)) => Ok(ConnectionTestResult {
                success: false,
                message: format!("Connection failed: {}", e),
                latency_ms: start.elapsed().as_millis() as u64,
                server_version: None,
            }),
            Err(_) => Ok(ConnectionTestResult {
                success: false,
                message: format!("Connection timed out after 5s to {}", addr),
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
impl StreamProvider for RedisStreamProvider {
    async fn produce(&self, _topic: &str, _messages: &[StreamMessage]) -> Result<StreamResult, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "redis".into(), operation: "produce".into() })
    }

    async fn consume(&self, _topic: &str, _group_id: &str, _max_messages: usize) -> Result<Vec<StreamMessage>, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "redis".into(), operation: "consume".into() })
    }

    async fn list_topics(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![self.stream_key.clone()])
    }
}
