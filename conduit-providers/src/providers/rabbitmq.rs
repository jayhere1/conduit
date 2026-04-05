//! RabbitMQ provider
//!
//! Provides connectivity to RabbitMQ message brokers (AMQP).
//!
//! # Configuration
//!
//! ```yaml
//! type: rabbitmq
//! config:
//!   host: localhost
//!   port: 5672
//!   vhost: /
//!   user: guest
//!   password: guest
//!   exchange: my_exchange
//!   queue: my_queue
//!   routing_key: my.routing.key
//! ```

use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;
use crate::errors::ProviderError;
use crate::traits::*;
use super::{extra_str, extra_u64};

/// RabbitMQ provider
#[allow(dead_code)]
pub struct RabbitMqProvider {
    name: String,
    host: String,
    port: u64,
    vhost: String,
    user: String,
    password: Option<String>,
    exchange: String,
    queue: String,
    routing_key: String,
}

impl RabbitMqProvider {
    /// Create a new RabbitMQ provider from configuration
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let host = extra_str(config, "host").unwrap_or_else(|| "localhost".to_string());
        let port = extra_u64(config, "port").unwrap_or(5672);
        let vhost = extra_str(config, "vhost").unwrap_or_else(|| "/".to_string());
        let user = extra_str(config, "user").unwrap_or_else(|| "guest".to_string());
        let password = extra_str(config, "password");
        let exchange = extra_str(config, "exchange").unwrap_or_default();
        let queue = extra_str(config, "queue").unwrap_or_default();
        let routing_key = extra_str(config, "routing_key").unwrap_or_default();

        Ok(RabbitMqProvider {
            name: name.to_string(),
            host,
            port,
            vhost,
            user,
            password,
            exchange,
            queue,
            routing_key,
        })
    }
}

#[async_trait]
impl Provider for RabbitMqProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "rabbitmq".to_string(),
            display_name: format!("RabbitMQ ({}:{}{})", self.host, self.port, self.vhost),
            version: None,
            capabilities: vec![
                Capability::StreamProduce,
                Capability::StreamConsume,
            ],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "rabbitmq".into(), operation: "test_connection".into() })
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl StreamProvider for RabbitMqProvider {
    async fn produce(&self, _topic: &str, _messages: &[StreamMessage]) -> Result<StreamResult, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "rabbitmq".into(), operation: "produce".into() })
    }

    async fn consume(&self, _topic: &str, _group_id: &str, _max_messages: usize) -> Result<Vec<StreamMessage>, ProviderError> {
        Err(ProviderError::NotImplemented { provider_type: "rabbitmq".into(), operation: "consume".into() })
    }

    async fn list_topics(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![self.queue.clone()])
    }
}
