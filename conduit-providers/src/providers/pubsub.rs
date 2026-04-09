//! GCP Pub/Sub provider
//!
//! Provides connectivity to Google Cloud Pub/Sub message service.
//!
//! # Configuration
//!
//! ```yaml
//! type: pubsub
//! config:
//!   project: my_project
//!   topic: my_topic
//!   subscription: my_subscription
//!   credentials_file: optional_path_to_json_key
//! ```

use super::extra_str;
use crate::errors::ProviderError;
use crate::traits::*;
use async_trait::async_trait;
use conduit_common::config::ConnectionConfig;

/// GCP Pub/Sub provider
#[allow(dead_code)]
pub struct PubSubProvider {
    name: String,
    project: String,
    topic: String,
    subscription: String,
    credentials_file: Option<String>,
}

impl PubSubProvider {
    /// Create a new Pub/Sub provider from configuration
    pub fn from_config(name: &str, config: &ConnectionConfig) -> Result<Self, ProviderError> {
        let project = extra_str(config, "project").unwrap_or_default();
        let topic = extra_str(config, "topic").unwrap_or_default();
        let subscription = extra_str(config, "subscription").unwrap_or_default();
        let credentials_file = extra_str(config, "credentials_file");

        Ok(PubSubProvider {
            name: name.to_string(),
            project,
            topic,
            subscription,
            credentials_file,
        })
    }
}

#[async_trait]
impl Provider for PubSubProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            provider_type: "pubsub".to_string(),
            display_name: format!("Pub/Sub ({}/{})", self.project, self.topic),
            version: None,
            capabilities: vec![Capability::StreamProduce, Capability::StreamConsume],
        }
    }

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError> {
        Err(ProviderError::NotImplemented {
            provider_type: "pubsub".into(),
            operation: "test_connection".into(),
        })
    }

    async fn close(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[async_trait]
impl StreamProvider for PubSubProvider {
    async fn produce(
        &self,
        _topic: &str,
        _messages: &[StreamMessage],
    ) -> Result<StreamResult, ProviderError> {
        Err(ProviderError::NotImplemented {
            provider_type: "pubsub".into(),
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
            provider_type: "pubsub".into(),
            operation: "consume".into(),
        })
    }

    async fn list_topics(&self) -> Result<Vec<String>, ProviderError> {
        Ok(vec![self.topic.clone()])
    }
}
