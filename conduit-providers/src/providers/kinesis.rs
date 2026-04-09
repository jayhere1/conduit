//! AWS Kinesis provider
//!
//! Provides connectivity to AWS Kinesis data streams.
//!
//! # Configuration
//!
//! ```yaml
//! type: kinesis
//! config:
//!   region: us-east-1
//!   stream_name: my_stream
//!   consumer_name: my_consumer
//!   endpoint_url: optional_localstack_url
//! ```

use super::extra_str;
use crate::errors::ProviderError;
use crate::traits::*;
use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

/// AWS Kinesis provider
#[allow(dead_code)]
pub struct KinesisProvider {
    name: String,
    region: String,
    stream_name: String,
    consumer_name: String,
    endpoint_url: Option<String>,
}

impl KinesisProvider {
    /// Create a new Kinesis provider from configuration
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let region = extra_str(config, "region").unwrap_or_else(|| "us-east-1".to_string());
        let stream_name = extra_str(config, "stream_name").unwrap_or_default();
        let consumer_name = extra_str(config, "consumer_name").unwrap_or_default();
        let endpoint_url = extra_str(config, "endpoint_url");

        Ok(KinesisProvider {
            name: name.to_string(),
            region,
            stream_name,
            consumer_name,
            endpoint_url,
        })
    }
}

#[async_trait]
impl Provider for KinesisProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "kinesis".to_string(),
            display_name: format!("Kinesis ({}, {})", self.stream_name, self.region),
            version: None,
            capabilities: vec![Capability::StreamProduce, Capability::StreamConsume],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Err(ProviderError::NotImplemented {
            provider_type: "kinesis".into(),
            operation: "test_connection".into(),
        })
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl StreamProvider for KinesisProvider {
    async fn produce(
        &self,
        _topic: &str,
        _messages: &[StreamMessage],
    ) -> Result<StreamResult, ProviderError> {
        Err(ProviderError::NotImplemented {
            provider_type: "kinesis".into(),
            operation: "produce".into(),
        })
    }

    async fn consume(
        &self,
        _topic: &str,
        _group_id: &str,
        _max_messages: usize,
    ) -> Result<Vec<StreamMessage>, ProviderError> {
        Err(ProviderError::NotImplemented {
            provider_type: "kinesis".into(),
            operation: "consume".into(),
        })
    }

    async fn list_topics(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![self.stream_name.clone()])
    }
}
