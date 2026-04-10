//! Comprehensive integration tests for Stream, SaaS, and Document providers in Conduit.
//!
//! This test module verifies the public API of the conduit-providers crate
//! for all Stream, SaaS, Storage (S3/GCS), and Document/NoSQL providers.
//!
//! Test coverage includes:
//! - Stream providers (Kafka, RabbitMQ, Kinesis, Pub/Sub, Redis Streams)
//! - SaaS providers (Salesforce, HubSpot, Stripe, GitHub, Jira, Slack)
//! - Storage providers (S3, GCS)
//! - Document providers (MongoDB, DynamoDB, Cassandra, Elasticsearch, Redis Doc, Neo4j)

use std::collections::HashMap;

use conduit_common::config::ConnectionConfig;
use conduit_providers::traits::*;
use conduit_providers::traits_saas::*;
use conduit_providers::providers::*;

// ─── Helper Functions ───────────────────────────────────────────────────────

/// Create a minimal ConnectionConfig for testing a given provider type.
fn make_config(conn_type: &str) -> ConnectionConfig {
    ConnectionConfig {
        conn_type: conn_type.to_string(),
        host: Some("testhost".to_string()),
        port: Some(5432),
        database: Some("testdb".to_string()),
        credentials: None,
        extra: HashMap::new(),
    }
}

/// Create a ConnectionConfig with extra parameters.
fn make_config_with_extras(conn_type: &str, extras: Vec<(&str, serde_json::Value)>) -> ConnectionConfig {
    let mut extra = HashMap::new();
    for (k, v) in extras {
        extra.insert(k.to_string(), v);
    }
    ConnectionConfig {
        conn_type: conn_type.to_string(),
        host: Some("testhost".to_string()),
        port: Some(5432),
        database: Some("testdb".to_string()),
        credentials: None,
        extra,
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// STORAGE PROVIDER TESTS (2 providers: S3, GCS)
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_s3_from_config() {
    let config = make_config("s3");
    let provider = s3::S3Provider::from_config("test_s3", &config);
    assert!(provider.is_ok(), "S3 provider should initialize with bucket in database field");
}

#[tokio::test]
async fn test_s3_requires_bucket() {
    let mut config = make_config("s3");
    config.database = None; // Remove bucket
    let provider = s3::S3Provider::from_config("test_s3", &config);
    assert!(provider.is_err(), "S3 should error without bucket name");
    match provider {
        Err(conduit_providers::ProviderError::InvalidConfig { reason, .. }) => {
            assert!(reason.contains("bucket"), "Error should mention bucket requirement");
        }
        _ => panic!("Expected InvalidConfig error"),
    }
}

#[tokio::test]
async fn test_s3_provider_info() {
    let config = make_config("s3");
    let provider = s3::S3Provider::from_config("test_s3", &config)
        .expect("Failed to create S3 provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "s3");
    assert!(info.capabilities.contains(&Capability::StorageRead));
    assert!(info.capabilities.contains(&Capability::StorageWrite));
}

#[tokio::test]
#[ignore] // Requires real S3 credentials
async fn test_s3_write_object() {
    let config = make_config("s3");
    let provider = s3::S3Provider::from_config("test_s3", &config)
        .expect("Failed to create S3 provider");

    let test_data = b"test data";
    let result = provider.write_object("test/file.txt", test_data).await;
    assert!(result.is_ok());
    let storage_result = result.unwrap();
    assert_eq!(storage_result.objects_affected, 1);
    assert_eq!(storage_result.bytes_transferred, test_data.len() as u64);
    assert!(!storage_result.uris.is_empty());
    assert!(storage_result.uris[0].contains("s3://"));
}

#[tokio::test]
#[ignore] // Requires real S3 credentials
async fn test_s3_copy_object() {
    let config = make_config("s3");
    let provider = s3::S3Provider::from_config("test_s3", &config)
        .expect("Failed to create S3 provider");

    let result = provider.copy_object("source.txt", "dest.txt").await;
    assert!(result.is_ok());
    let storage_result = result.unwrap();
    assert_eq!(storage_result.objects_affected, 1);
    assert_eq!(storage_result.uris.len(), 2, "Copy should return source and dest URIs");
}

#[tokio::test]
async fn test_gcs_from_config() {
    let config = make_config("gcs");
    let provider = gcs::GcsProvider::from_config("test_gcs", &config);
    assert!(provider.is_ok(), "GCS provider should initialize from config");
}

#[tokio::test]
async fn test_gcs_provider_info() {
    let config = make_config("gcs");
    let provider = gcs::GcsProvider::from_config("test_gcs", &config)
        .expect("Failed to create GCS provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "gcs");
    assert!(info.capabilities.contains(&Capability::StorageRead));
    assert!(info.capabilities.contains(&Capability::StorageWrite));
}

// ══════════════════════════════════════════════════════════════════════════════
// STREAM PROVIDER TESTS (5 providers)
// ══════════════════════════════════════════════════════════════════════════════

// ─── Kafka Provider Tests ───────────────────────────────────────────────────

#[tokio::test]
async fn test_kafka_from_config() {
    let config = make_config("kafka");
    let provider = kafka::KafkaProvider::from_config("test_kafka", &config);
    assert!(provider.is_ok(), "Kafka provider should initialize from config");
}

#[tokio::test]
async fn test_kafka_provider_info() {
    let config = make_config("kafka");
    let provider = kafka::KafkaProvider::from_config("test_kafka", &config)
        .expect("Failed to create Kafka provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "kafka");
    assert!(info.capabilities.contains(&Capability::StreamProduce));
    assert!(info.capabilities.contains(&Capability::StreamConsume));
}

#[tokio::test]
#[ignore] // Requires live Kafka broker
async fn test_kafka_produce() {
    let config = make_config("kafka");
    let provider = kafka::KafkaProvider::from_config("test_kafka", &config)
        .expect("Failed to create Kafka provider");

    let messages = vec![
        StreamMessage {
            key: Some("key1".to_string()),
            value: b"msg1".to_vec(),
            headers: HashMap::new(),
            timestamp: None,
        },
        StreamMessage {
            key: Some("key2".to_string()),
            value: b"msg2".to_vec(),
            headers: HashMap::new(),
            timestamp: None,
        },
    ];

    let result = provider.produce("test_topic", &messages).await;
    assert!(result.is_err());
}

#[tokio::test]
#[ignore] // Requires live Kafka broker
async fn test_kafka_consume() {
    let config = make_config("kafka");
    let provider = kafka::KafkaProvider::from_config("test_kafka", &config)
        .expect("Failed to create Kafka provider");

    let result = provider.consume("test_topic", "test_group", 10).await;
    assert!(result.is_err());
}

#[tokio::test]
#[ignore] // Requires live Kafka broker
async fn test_kafka_list_topics() {
    let config = make_config("kafka");
    let provider = kafka::KafkaProvider::from_config("test_kafka", &config)
        .expect("Failed to create Kafka provider");

    let result = provider.list_topics().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_kafka_broker_count_in_display() {
    let config = make_config_with_extras("kafka", vec![]);
    let mut kafka_config = config;
    kafka_config.host = Some("broker1:9092,broker2:9092".to_string());

    let provider = kafka::KafkaProvider::from_config("test_kafka", &kafka_config)
        .expect("Failed to create Kafka provider");

    let info = provider.info();
    assert!(info.display_name.contains("2 brokers"), "Display should show broker count");
}

// ─── RabbitMQ Provider Tests ────────────────────────────────────────────────

#[tokio::test]
async fn test_rabbitmq_from_config() {
    let config = make_config("rabbitmq");
    let provider = rabbitmq::RabbitMqProvider::from_config("test_rabbitmq", &config);
    assert!(provider.is_ok(), "RabbitMQ provider should initialize from config");
}

#[tokio::test]
async fn test_rabbitmq_provider_info() {
    let config = make_config("rabbitmq");
    let provider = rabbitmq::RabbitMqProvider::from_config("test_rabbitmq", &config)
        .expect("Failed to create RabbitMQ provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "rabbitmq");
    assert!(info.capabilities.contains(&Capability::StreamProduce));
    assert!(info.capabilities.contains(&Capability::StreamConsume));
}

#[tokio::test]
async fn test_rabbitmq_produce() {
    let config = make_config("rabbitmq");
    let provider = rabbitmq::RabbitMqProvider::from_config("test_rabbitmq", &config)
        .expect("Failed to create RabbitMQ provider");

    let messages = vec![StreamMessage {
        key: None,
        value: b"test message".to_vec(),
        headers: HashMap::new(),
        timestamp: None,
    }];

    let result = provider.produce("test_queue", &messages).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_rabbitmq_consume() {
    let config = make_config("rabbitmq");
    let provider = rabbitmq::RabbitMqProvider::from_config("test_rabbitmq", &config)
        .expect("Failed to create RabbitMQ provider");

    let result = provider.consume("test_queue", "test_group", 5).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_rabbitmq_list_topics() {
    let config = make_config("rabbitmq");
    let provider = rabbitmq::RabbitMqProvider::from_config("test_rabbitmq", &config)
        .expect("Failed to create RabbitMQ provider");

    let result = provider.list_topics().await;
    assert!(result.is_ok());
}

// ─── Kinesis Provider Tests ─────────────────────────────────────────────────

#[tokio::test]
async fn test_kinesis_from_config() {
    let config = make_config("kinesis");
    let provider = kinesis::KinesisProvider::from_config("test_kinesis", &config);
    assert!(provider.is_ok(), "Kinesis provider should initialize from config");
}

#[tokio::test]
async fn test_kinesis_provider_info() {
    let config = make_config("kinesis");
    let provider = kinesis::KinesisProvider::from_config("test_kinesis", &config)
        .expect("Failed to create Kinesis provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "kinesis");
    assert!(info.capabilities.contains(&Capability::StreamProduce));
    assert!(info.capabilities.contains(&Capability::StreamConsume));
}

#[tokio::test]
async fn test_kinesis_produce() {
    let config = make_config("kinesis");
    let provider = kinesis::KinesisProvider::from_config("test_kinesis", &config)
        .expect("Failed to create Kinesis provider");

    let messages = vec![
        StreamMessage {
            key: Some("partition_key".to_string()),
            value: b"record1".to_vec(),
            headers: HashMap::new(),
            timestamp: None,
        },
    ];

    let result = provider.produce("test_stream", &messages).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_kinesis_consume() {
    let config = make_config("kinesis");
    let provider = kinesis::KinesisProvider::from_config("test_kinesis", &config)
        .expect("Failed to create Kinesis provider");

    let result = provider.consume("test_stream", "test_group", 100).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_kinesis_list_topics() {
    let config = make_config("kinesis");
    let provider = kinesis::KinesisProvider::from_config("test_kinesis", &config)
        .expect("Failed to create Kinesis provider");

    let result = provider.list_topics().await;
    assert!(result.is_ok());
}

// ─── Pub/Sub Provider Tests ─────────────────────────────────────────────────

#[tokio::test]
async fn test_pubsub_from_config() {
    let config = make_config("pubsub");
    let provider = pubsub::PubSubProvider::from_config("test_pubsub", &config);
    assert!(provider.is_ok(), "Pub/Sub provider should initialize from config");
}

#[tokio::test]
async fn test_pubsub_provider_info() {
    let config = make_config("pubsub");
    let provider = pubsub::PubSubProvider::from_config("test_pubsub", &config)
        .expect("Failed to create Pub/Sub provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "pubsub");
    assert!(info.capabilities.contains(&Capability::StreamProduce));
    assert!(info.capabilities.contains(&Capability::StreamConsume));
}

#[tokio::test]
async fn test_pubsub_produce() {
    let config = make_config("pubsub");
    let provider = pubsub::PubSubProvider::from_config("test_pubsub", &config)
        .expect("Failed to create Pub/Sub provider");

    let messages = vec![StreamMessage {
        key: None,
        value: b"gcp message".to_vec(),
        headers: HashMap::new(),
        timestamp: None,
    }];

    let result = provider.produce("test_topic", &messages).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_pubsub_consume() {
    let config = make_config("pubsub");
    let provider = pubsub::PubSubProvider::from_config("test_pubsub", &config)
        .expect("Failed to create Pub/Sub provider");

    let result = provider.consume("test_topic", "test_sub", 50).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_pubsub_list_topics() {
    let config = make_config("pubsub");
    let provider = pubsub::PubSubProvider::from_config("test_pubsub", &config)
        .expect("Failed to create Pub/Sub provider");

    let result = provider.list_topics().await;
    assert!(result.is_ok());
}

// ─── Redis Streams Provider Tests ───────────────────────────────────────────

#[tokio::test]
async fn test_redis_stream_from_config() {
    let config = make_config("redis_stream");
    let provider = redis_stream::RedisStreamProvider::from_config("test_redis_stream", &config);
    assert!(provider.is_ok(), "Redis Stream provider should initialize from config");
}

#[tokio::test]
async fn test_redis_stream_provider_info() {
    let config = make_config("redis_stream");
    let provider = redis_stream::RedisStreamProvider::from_config("test_redis_stream", &config)
        .expect("Failed to create Redis Stream provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "redis");
    assert!(info.capabilities.contains(&Capability::StreamProduce));
    assert!(info.capabilities.contains(&Capability::StreamConsume));
}

#[tokio::test]
async fn test_redis_stream_produce() {
    let config = make_config("redis_stream");
    let provider = redis_stream::RedisStreamProvider::from_config("test_redis_stream", &config)
        .expect("Failed to create Redis Stream provider");

    let messages = vec![StreamMessage {
        key: None,
        value: b"redis entry".to_vec(),
        headers: HashMap::new(),
        timestamp: None,
    }];

    let result = provider.produce("mystream", &messages).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_redis_stream_consume() {
    let config = make_config("redis_stream");
    let provider = redis_stream::RedisStreamProvider::from_config("test_redis_stream", &config)
        .expect("Failed to create Redis Stream provider");

    let result = provider.consume("mystream", "mygroup", 25).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_redis_stream_list_topics() {
    let config = make_config("redis_stream");
    let provider = redis_stream::RedisStreamProvider::from_config("test_redis_stream", &config)
        .expect("Failed to create Redis Stream provider");

    let result = provider.list_topics().await;
    assert!(result.is_ok());
}

// ══════════════════════════════════════════════════════════════════════════════
// SAAS PROVIDER TESTS (6 providers)
// ══════════════════════════════════════════════════════════════════════════════

// ─── Salesforce Provider Tests ──────────────────────────────────────────────

#[tokio::test]
async fn test_salesforce_from_config() {
    let config = make_config("salesforce");
    let provider = salesforce::SalesforceProvider::from_config("test_sfdc", &config);
    assert!(provider.is_ok(), "Salesforce provider should initialize from config");
}

#[tokio::test]
async fn test_salesforce_provider_info() {
    let config = make_config("salesforce");
    let provider = salesforce::SalesforceProvider::from_config("test_sfdc", &config)
        .expect("Failed to create Salesforce provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "salesforce");
    assert!(info.capabilities.contains(&Capability::HttpRequest));
}

#[tokio::test]
async fn test_salesforce_query() {
    let config = make_config("salesforce");
    let provider = salesforce::SalesforceProvider::from_config("test_sfdc", &config)
        .expect("Failed to create Salesforce provider");

    let filter = HashMap::new();
    let result = provider.query("Account", &filter, None).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.operation, "query");
}

#[tokio::test]
async fn test_salesforce_create() {
    let config = make_config("salesforce");
    let provider = salesforce::SalesforceProvider::from_config("test_sfdc", &config)
        .expect("Failed to create Salesforce provider");

    let data = serde_json::json!({"Name": "Test Account"});
    let result = provider.create("Account", &data).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_salesforce_update() {
    let config = make_config("salesforce");
    let provider = salesforce::SalesforceProvider::from_config("test_sfdc", &config)
        .expect("Failed to create Salesforce provider");

    let data = serde_json::json!({"Name": "Updated Account"});
    let result = provider.update("Account", "001xx000123", &data).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_salesforce_delete() {
    let config = make_config("salesforce");
    let provider = salesforce::SalesforceProvider::from_config("test_sfdc", &config)
        .expect("Failed to create Salesforce provider");

    let result = provider.delete("Account", "001xx000123").await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_salesforce_list_object_types() {
    let config = make_config("salesforce");
    let provider = salesforce::SalesforceProvider::from_config("test_sfdc", &config)
        .expect("Failed to create Salesforce provider");

    let result = provider.list_object_types().await;
    assert!(result.is_ok());
    let object_types = result.unwrap();
    assert!(!object_types.is_empty());
    assert!(object_types.contains(&"Account".to_string()));
}

// ─── HubSpot Provider Tests ─────────────────────────────────────────────────

#[tokio::test]
async fn test_hubspot_from_config() {
    let config = make_config("hubspot");
    let provider = hubspot::HubSpotProvider::from_config("test_hs", &config);
    assert!(provider.is_ok(), "HubSpot provider should initialize from config");
}

#[tokio::test]
async fn test_hubspot_provider_info() {
    let config = make_config("hubspot");
    let provider = hubspot::HubSpotProvider::from_config("test_hs", &config)
        .expect("Failed to create HubSpot provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "hubspot");
    assert!(info.capabilities.contains(&Capability::HttpRequest));
}

#[tokio::test]
async fn test_hubspot_query() {
    let config = make_config("hubspot");
    let provider = hubspot::HubSpotProvider::from_config("test_hs", &config)
        .expect("Failed to create HubSpot provider");

    let filter = HashMap::new();
    let result = provider.query("Contact", &filter, None).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert!(saas_result.operation == "query" || saas_result.operation == "list");
}

#[tokio::test]
async fn test_hubspot_create() {
    let config = make_config("hubspot");
    let provider = hubspot::HubSpotProvider::from_config("test_hs", &config)
        .expect("Failed to create HubSpot provider");

    let data = serde_json::json!({"properties": {"firstname": "Test"}});
    let result = provider.create("Contact", &data).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_hubspot_update() {
    let config = make_config("hubspot");
    let provider = hubspot::HubSpotProvider::from_config("test_hs", &config)
        .expect("Failed to create HubSpot provider");

    let data = serde_json::json!({"properties": {"firstname": "Updated"}});
    let result = provider.update("Contact", "123", &data).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_hubspot_delete() {
    let config = make_config("hubspot");
    let provider = hubspot::HubSpotProvider::from_config("test_hs", &config)
        .expect("Failed to create HubSpot provider");

    let result = provider.delete("Contact", "123").await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_hubspot_list_object_types() {
    let config = make_config("hubspot");
    let provider = hubspot::HubSpotProvider::from_config("test_hs", &config)
        .expect("Failed to create HubSpot provider");

    let result = provider.list_object_types().await;
    assert!(result.is_ok());
    let object_types = result.unwrap();
    assert!(!object_types.is_empty());
}

// ─── Stripe Provider Tests ──────────────────────────────────────────────────

#[tokio::test]
async fn test_stripe_from_config() {
    let config = make_config("stripe");
    let provider = stripe::StripeProvider::from_config("test_stripe", &config);
    assert!(provider.is_ok(), "Stripe provider should initialize from config");
}

#[tokio::test]
async fn test_stripe_provider_info() {
    let config = make_config("stripe");
    let provider = stripe::StripeProvider::from_config("test_stripe", &config)
        .expect("Failed to create Stripe provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "stripe");
    assert!(info.capabilities.contains(&Capability::HttpRequest));
}

#[tokio::test]
async fn test_stripe_query() {
    let config = make_config("stripe");
    let provider = stripe::StripeProvider::from_config("test_stripe", &config)
        .expect("Failed to create Stripe provider");

    let filter = HashMap::new();
    let result = provider.query("Invoice", &filter, None).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert!(saas_result.operation == "query" || saas_result.operation == "list");
}

#[tokio::test]
async fn test_stripe_create() {
    let config = make_config("stripe");
    let provider = stripe::StripeProvider::from_config("test_stripe", &config)
        .expect("Failed to create Stripe provider");

    let data = serde_json::json!({"amount": 1000});
    let result = provider.create("Charge", &data).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_stripe_update() {
    let config = make_config("stripe");
    let provider = stripe::StripeProvider::from_config("test_stripe", &config)
        .expect("Failed to create Stripe provider");

    let data = serde_json::json!({"metadata": {"order": "123"}});
    let result = provider.update("Invoice", "inv_123", &data).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_stripe_delete() {
    let config = make_config("stripe");
    let provider = stripe::StripeProvider::from_config("test_stripe", &config)
        .expect("Failed to create Stripe provider");

    let result = provider.delete("Plan", "plan_123").await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_stripe_list_object_types() {
    let config = make_config("stripe");
    let provider = stripe::StripeProvider::from_config("test_stripe", &config)
        .expect("Failed to create Stripe provider");

    let result = provider.list_object_types().await;
    assert!(result.is_ok());
    let object_types = result.unwrap();
    assert!(!object_types.is_empty());
}

// ─── GitHub Provider Tests ──────────────────────────────────────────────────

#[tokio::test]
async fn test_github_from_config() {
    let config = make_config("github");
    let provider = github::GitHubProvider::from_config("test_gh", &config);
    assert!(provider.is_ok(), "GitHub provider should initialize from config");
}

#[tokio::test]
async fn test_github_provider_info() {
    let config = make_config("github");
    let provider = github::GitHubProvider::from_config("test_gh", &config)
        .expect("Failed to create GitHub provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "github");
    assert!(info.capabilities.contains(&Capability::HttpRequest));
}

#[tokio::test]
async fn test_github_query() {
    let config = make_config("github");
    let provider = github::GitHubProvider::from_config("test_gh", &config)
        .expect("Failed to create GitHub provider");

    let filter = HashMap::new();
    let result = provider.query("Issue", &filter, None).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert!(saas_result.operation == "query" || saas_result.operation == "list");
}

#[tokio::test]
async fn test_github_create() {
    let config = make_config("github");
    let provider = github::GitHubProvider::from_config("test_gh", &config)
        .expect("Failed to create GitHub provider");

    let data = serde_json::json!({"title": "Test Issue"});
    let result = provider.create("Issue", &data).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_github_update() {
    let config = make_config("github");
    let provider = github::GitHubProvider::from_config("test_gh", &config)
        .expect("Failed to create GitHub provider");

    let data = serde_json::json!({"state": "closed"});
    let result = provider.update("Issue", "123", &data).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_github_delete() {
    let config = make_config("github");
    let provider = github::GitHubProvider::from_config("test_gh", &config)
        .expect("Failed to create GitHub provider");

    let result = provider.delete("Branch", "feature-branch").await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_github_list_object_types() {
    let config = make_config("github");
    let provider = github::GitHubProvider::from_config("test_gh", &config)
        .expect("Failed to create GitHub provider");

    let result = provider.list_object_types().await;
    assert!(result.is_ok());
    let object_types = result.unwrap();
    assert!(!object_types.is_empty());
}

// ─── Jira Provider Tests ────────────────────────────────────────────────────

#[tokio::test]
async fn test_jira_from_config() {
    let config = make_config("jira");
    let provider = jira::JiraProvider::from_config("test_jira", &config);
    assert!(provider.is_ok(), "Jira provider should initialize from config");
}

#[tokio::test]
async fn test_jira_provider_info() {
    let config = make_config("jira");
    let provider = jira::JiraProvider::from_config("test_jira", &config)
        .expect("Failed to create Jira provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "jira");
    assert!(info.capabilities.contains(&Capability::HttpRequest));
}

#[tokio::test]
async fn test_jira_query() {
    let config = make_config("jira");
    let provider = jira::JiraProvider::from_config("test_jira", &config)
        .expect("Failed to create Jira provider");

    let filter = HashMap::new();
    let result = provider.query("Issue", &filter, None).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert!(saas_result.operation == "query" || saas_result.operation == "list");
}

#[tokio::test]
async fn test_jira_create() {
    let config = make_config("jira");
    let provider = jira::JiraProvider::from_config("test_jira", &config)
        .expect("Failed to create Jira provider");

    let data = serde_json::json!({"fields": {"summary": "Test Task"}});
    let result = provider.create("Issue", &data).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_jira_update() {
    let config = make_config("jira");
    let provider = jira::JiraProvider::from_config("test_jira", &config)
        .expect("Failed to create Jira provider");

    let data = serde_json::json!({"fields": {"status": "Done"}});
    let result = provider.update("Issue", "PROJ-123", &data).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_jira_delete() {
    let config = make_config("jira");
    let provider = jira::JiraProvider::from_config("test_jira", &config)
        .expect("Failed to create Jira provider");

    let result = provider.delete("Issue", "PROJ-123").await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_jira_list_object_types() {
    let config = make_config("jira");
    let provider = jira::JiraProvider::from_config("test_jira", &config)
        .expect("Failed to create Jira provider");

    let result = provider.list_object_types().await;
    assert!(result.is_ok());
    let object_types = result.unwrap();
    assert!(!object_types.is_empty());
}

// ─── Slack Provider Tests ───────────────────────────────────────────────────

#[tokio::test]
async fn test_slack_from_config() {
    let config = make_config("slack");
    let provider = slack::SlackProvider::from_config("test_slack", &config);
    assert!(provider.is_ok(), "Slack provider should initialize from config");
}

#[tokio::test]
async fn test_slack_provider_info() {
    let config = make_config("slack");
    let provider = slack::SlackProvider::from_config("test_slack", &config)
        .expect("Failed to create Slack provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "slack");
    assert!(info.capabilities.contains(&Capability::HttpRequest));
}

#[tokio::test]
async fn test_slack_query() {
    let config = make_config("slack");
    let provider = slack::SlackProvider::from_config("test_slack", &config)
        .expect("Failed to create Slack provider");

    let filter = HashMap::new();
    let result = provider.query("Message", &filter, None).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert!(saas_result.operation == "query" || saas_result.operation == "list");
}

#[tokio::test]
async fn test_slack_create() {
    let config = make_config("slack");
    let provider = slack::SlackProvider::from_config("test_slack", &config)
        .expect("Failed to create Slack provider");

    let data = serde_json::json!({"text": "Hello Slack"});
    let result = provider.create("Message", &data).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_slack_update() {
    let config = make_config("slack");
    let provider = slack::SlackProvider::from_config("test_slack", &config)
        .expect("Failed to create Slack provider");

    let data = serde_json::json!({"text": "Updated message"});
    let result = provider.update("Message", "123", &data).await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_slack_delete() {
    let config = make_config("slack");
    let provider = slack::SlackProvider::from_config("test_slack", &config)
        .expect("Failed to create Slack provider");

    let result = provider.delete("Message", "123").await;
    assert!(result.is_ok());
    let saas_result = result.unwrap();
    assert_eq!(saas_result.records_affected, 1);
}

#[tokio::test]
async fn test_slack_list_object_types() {
    let config = make_config("slack");
    let provider = slack::SlackProvider::from_config("test_slack", &config)
        .expect("Failed to create Slack provider");

    let result = provider.list_object_types().await;
    assert!(result.is_ok());
    let object_types = result.unwrap();
    assert!(!object_types.is_empty());
}

// ══════════════════════════════════════════════════════════════════════════════
// DOCUMENT / NOSQL PROVIDER TESTS (6 providers)
// ══════════════════════════════════════════════════════════════════════════════

// ─── MongoDB Provider Tests ────────────────────────────────────────────────

#[tokio::test]
async fn test_mongodb_from_config() {
    let config = make_config("mongodb");
    let provider = mongodb::MongoDbProvider::from_config("test_mongo", &config);
    assert!(provider.is_ok(), "MongoDB provider should initialize from config");
}

#[tokio::test]
async fn test_mongodb_provider_info() {
    let config = make_config("mongodb");
    let provider = mongodb::MongoDbProvider::from_config("test_mongo", &config)
        .expect("Failed to create MongoDB provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "mongodb");
}

#[tokio::test]
async fn test_mongodb_find() {
    let config = make_config("mongodb");
    let provider = mongodb::MongoDbProvider::from_config("test_mongo", &config)
        .expect("Failed to create MongoDB provider");

    let filter = serde_json::json!({});
    let result = provider.find("users", &filter, None).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "find");
}

#[tokio::test]
async fn test_mongodb_insert() {
    let config = make_config("mongodb");
    let provider = mongodb::MongoDbProvider::from_config("test_mongo", &config)
        .expect("Failed to create MongoDB provider");

    let documents = vec![
        serde_json::json!({"name": "Alice"}),
        serde_json::json!({"name": "Bob"}),
    ];
    let result = provider.insert("users", &documents).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.documents_affected, 2);
}

#[tokio::test]
async fn test_mongodb_update() {
    let config = make_config("mongodb");
    let provider = mongodb::MongoDbProvider::from_config("test_mongo", &config)
        .expect("Failed to create MongoDB provider");

    let filter = serde_json::json!({"_id": "123"});
    let update = serde_json::json!({"name": "Alice Updated"});
    let result = provider.update("users", &filter, &update).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "update");
}

#[tokio::test]
async fn test_mongodb_delete() {
    let config = make_config("mongodb");
    let provider = mongodb::MongoDbProvider::from_config("test_mongo", &config)
        .expect("Failed to create MongoDB provider");

    let filter = serde_json::json!({"_id": "123"});
    let result = provider.delete("users", &filter).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "delete");
}

#[tokio::test]
async fn test_mongodb_list_collections() {
    let config = make_config("mongodb");
    let provider = mongodb::MongoDbProvider::from_config("test_mongo", &config)
        .expect("Failed to create MongoDB provider");

    let result = provider.list_collections().await;
    assert!(result.is_ok());
}

// ─── DynamoDB Provider Tests ────────────────────────────────────────────────

#[tokio::test]
async fn test_dynamodb_from_config() {
    let config = make_config("dynamodb");
    let provider = dynamodb::DynamoDbProvider::from_config("test_dynamo", &config);
    assert!(provider.is_ok(), "DynamoDB provider should initialize from config");
}

#[tokio::test]
async fn test_dynamodb_provider_info() {
    let config = make_config("dynamodb");
    let provider = dynamodb::DynamoDbProvider::from_config("test_dynamo", &config)
        .expect("Failed to create DynamoDB provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "dynamodb");
}

#[tokio::test]
async fn test_dynamodb_find() {
    let config = make_config("dynamodb");
    let provider = dynamodb::DynamoDbProvider::from_config("test_dynamo", &config)
        .expect("Failed to create DynamoDB provider");

    let filter = serde_json::json!({});
    let result = provider.find("items", &filter, Some(10)).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "find");
}

#[tokio::test]
async fn test_dynamodb_insert() {
    let config = make_config("dynamodb");
    let provider = dynamodb::DynamoDbProvider::from_config("test_dynamo", &config)
        .expect("Failed to create DynamoDB provider");

    let documents = vec![
        serde_json::json!({"id": "1", "data": "test"}),
    ];
    let result = provider.insert("items", &documents).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.documents_affected, 1);
}

#[tokio::test]
async fn test_dynamodb_update() {
    let config = make_config("dynamodb");
    let provider = dynamodb::DynamoDbProvider::from_config("test_dynamo", &config)
        .expect("Failed to create DynamoDB provider");

    let filter = serde_json::json!({"id": "1"});
    let update = serde_json::json!({"data": "updated"});
    let result = provider.update("items", &filter, &update).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "update");
}

#[tokio::test]
async fn test_dynamodb_delete() {
    let config = make_config("dynamodb");
    let provider = dynamodb::DynamoDbProvider::from_config("test_dynamo", &config)
        .expect("Failed to create DynamoDB provider");

    let filter = serde_json::json!({"id": "1"});
    let result = provider.delete("items", &filter).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "delete");
}

#[tokio::test]
async fn test_dynamodb_list_collections() {
    let config = make_config("dynamodb");
    let provider = dynamodb::DynamoDbProvider::from_config("test_dynamo", &config)
        .expect("Failed to create DynamoDB provider");

    let result = provider.list_collections().await;
    assert!(result.is_ok());
}

// ─── Cassandra Provider Tests ───────────────────────────────────────────────

#[tokio::test]
async fn test_cassandra_from_config() {
    let config = make_config("cassandra");
    let provider = cassandra::CassandraProvider::from_config("test_cassandra", &config);
    assert!(provider.is_ok(), "Cassandra provider should initialize from config");
}

#[tokio::test]
async fn test_cassandra_provider_info() {
    let config = make_config("cassandra");
    let provider = cassandra::CassandraProvider::from_config("test_cassandra", &config)
        .expect("Failed to create Cassandra provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "cassandra");
}

#[tokio::test]
async fn test_cassandra_find() {
    let config = make_config("cassandra");
    let provider = cassandra::CassandraProvider::from_config("test_cassandra", &config)
        .expect("Failed to create Cassandra provider");

    let filter = serde_json::json!({});
    let result = provider.find("events", &filter, None).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "find");
}

#[tokio::test]
async fn test_cassandra_insert() {
    let config = make_config("cassandra");
    let provider = cassandra::CassandraProvider::from_config("test_cassandra", &config)
        .expect("Failed to create Cassandra provider");

    let documents = vec![
        serde_json::json!({"event_id": "1", "timestamp": "2024-01-01"}),
    ];
    let result = provider.insert("events", &documents).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.documents_affected, 1);
}

#[tokio::test]
async fn test_cassandra_update() {
    let config = make_config("cassandra");
    let provider = cassandra::CassandraProvider::from_config("test_cassandra", &config)
        .expect("Failed to create Cassandra provider");

    let filter = serde_json::json!({"event_id": "1"});
    let update = serde_json::json!({"status": "processed"});
    let result = provider.update("events", &filter, &update).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "update");
}

#[tokio::test]
async fn test_cassandra_delete() {
    let config = make_config("cassandra");
    let provider = cassandra::CassandraProvider::from_config("test_cassandra", &config)
        .expect("Failed to create Cassandra provider");

    let filter = serde_json::json!({"event_id": "1"});
    let result = provider.delete("events", &filter).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "delete");
}

#[tokio::test]
async fn test_cassandra_list_collections() {
    let config = make_config("cassandra");
    let provider = cassandra::CassandraProvider::from_config("test_cassandra", &config)
        .expect("Failed to create Cassandra provider");

    let result = provider.list_collections().await;
    assert!(result.is_ok());
}

// ─── Elasticsearch Provider Tests ───────────────────────────────────────────

#[tokio::test]
async fn test_elasticsearch_from_config() {
    let config = make_config("elasticsearch");
    let provider = elasticsearch::ElasticsearchProvider::from_config("test_es", &config);
    assert!(provider.is_ok(), "Elasticsearch provider should initialize from config");
}

#[tokio::test]
async fn test_elasticsearch_provider_info() {
    let config = make_config("elasticsearch");
    let provider = elasticsearch::ElasticsearchProvider::from_config("test_es", &config)
        .expect("Failed to create Elasticsearch provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "elasticsearch");
}

#[tokio::test]
async fn test_elasticsearch_find() {
    let config = make_config("elasticsearch");
    let provider = elasticsearch::ElasticsearchProvider::from_config("test_es", &config)
        .expect("Failed to create Elasticsearch provider");

    let filter = serde_json::json!({"query": {"match_all": {}}});
    let result = provider.find("logs", &filter, None).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "find");
}

#[tokio::test]
async fn test_elasticsearch_insert() {
    let config = make_config("elasticsearch");
    let provider = elasticsearch::ElasticsearchProvider::from_config("test_es", &config)
        .expect("Failed to create Elasticsearch provider");

    let documents = vec![
        serde_json::json!({"message": "log entry"}),
    ];
    let result = provider.insert("logs", &documents).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.documents_affected, 1);
}

#[tokio::test]
async fn test_elasticsearch_update() {
    let config = make_config("elasticsearch");
    let provider = elasticsearch::ElasticsearchProvider::from_config("test_es", &config)
        .expect("Failed to create Elasticsearch provider");

    let filter = serde_json::json!({"query": {"match": {"_id": "1"}}});
    let update = serde_json::json!({"doc": {"processed": true}});
    let result = provider.update("logs", &filter, &update).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "update");
}

#[tokio::test]
async fn test_elasticsearch_delete() {
    let config = make_config("elasticsearch");
    let provider = elasticsearch::ElasticsearchProvider::from_config("test_es", &config)
        .expect("Failed to create Elasticsearch provider");

    let filter = serde_json::json!({"query": {"match": {"_id": "1"}}});
    let result = provider.delete("logs", &filter).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "delete");
}

#[tokio::test]
async fn test_elasticsearch_list_collections() {
    let config = make_config("elasticsearch");
    let provider = elasticsearch::ElasticsearchProvider::from_config("test_es", &config)
        .expect("Failed to create Elasticsearch provider");

    let result = provider.list_collections().await;
    assert!(result.is_ok());
}

// ─── Redis Document Provider Tests ──────────────────────────────────────────

#[tokio::test]
async fn test_redis_doc_from_config() {
    let config = make_config("redis_doc");
    let provider = redis_doc::RedisDocProvider::from_config("test_redis_doc", &config);
    assert!(provider.is_ok(), "Redis Doc provider should initialize from config");
}

#[tokio::test]
async fn test_redis_doc_provider_info() {
    let config = make_config("redis_doc");
    let provider = redis_doc::RedisDocProvider::from_config("test_redis_doc", &config)
        .expect("Failed to create Redis Doc provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "redis_kv");
}

#[tokio::test]
async fn test_redis_doc_find() {
    let config = make_config("redis_doc");
    let provider = redis_doc::RedisDocProvider::from_config("test_redis_doc", &config)
        .expect("Failed to create Redis Doc provider");

    let filter = serde_json::json!({});
    let result = provider.find("cache", &filter, None).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "find");
}

#[tokio::test]
async fn test_redis_doc_insert() {
    let config = make_config("redis_doc");
    let provider = redis_doc::RedisDocProvider::from_config("test_redis_doc", &config)
        .expect("Failed to create Redis Doc provider");

    let documents = vec![
        serde_json::json!({"key": "value"}),
    ];
    let result = provider.insert("cache", &documents).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.documents_affected, 1);
}

#[tokio::test]
async fn test_redis_doc_update() {
    let config = make_config("redis_doc");
    let provider = redis_doc::RedisDocProvider::from_config("test_redis_doc", &config)
        .expect("Failed to create Redis Doc provider");

    let filter = serde_json::json!({"key": "mykey"});
    let update = serde_json::json!({"value": "updated"});
    let result = provider.update("cache", &filter, &update).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "update");
}

#[tokio::test]
async fn test_redis_doc_delete() {
    let config = make_config("redis_doc");
    let provider = redis_doc::RedisDocProvider::from_config("test_redis_doc", &config)
        .expect("Failed to create Redis Doc provider");

    let filter = serde_json::json!({"key": "mykey"});
    let result = provider.delete("cache", &filter).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "delete");
}

#[tokio::test]
async fn test_redis_doc_list_collections() {
    let config = make_config("redis_doc");
    let provider = redis_doc::RedisDocProvider::from_config("test_redis_doc", &config)
        .expect("Failed to create Redis Doc provider");

    let result = provider.list_collections().await;
    assert!(result.is_ok());
}

// ─── Neo4j Provider Tests ───────────────────────────────────────────────────

#[tokio::test]
async fn test_neo4j_from_config() {
    let config = make_config("neo4j");
    let provider = neo4j::Neo4jProvider::from_config("test_neo4j", &config);
    assert!(provider.is_ok(), "Neo4j provider should initialize from config");
}

#[tokio::test]
async fn test_neo4j_provider_info() {
    let config = make_config("neo4j");
    let provider = neo4j::Neo4jProvider::from_config("test_neo4j", &config)
        .expect("Failed to create Neo4j provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "neo4j");
}

#[tokio::test]
async fn test_neo4j_find() {
    let config = make_config("neo4j");
    let provider = neo4j::Neo4jProvider::from_config("test_neo4j", &config)
        .expect("Failed to create Neo4j provider");

    let filter = serde_json::json!({"label": "Person"});
    let result = provider.find("nodes", &filter, None).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "find");
}

#[tokio::test]
async fn test_neo4j_insert() {
    let config = make_config("neo4j");
    let provider = neo4j::Neo4jProvider::from_config("test_neo4j", &config)
        .expect("Failed to create Neo4j provider");

    let documents = vec![
        serde_json::json!({"name": "Alice", "label": "Person"}),
    ];
    let result = provider.insert("nodes", &documents).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.documents_affected, 1);
}

#[tokio::test]
async fn test_neo4j_update() {
    let config = make_config("neo4j");
    let provider = neo4j::Neo4jProvider::from_config("test_neo4j", &config)
        .expect("Failed to create Neo4j provider");

    let filter = serde_json::json!({"id": "1"});
    let update = serde_json::json!({"name": "Alice Updated"});
    let result = provider.update("nodes", &filter, &update).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "update");
}

#[tokio::test]
async fn test_neo4j_delete() {
    let config = make_config("neo4j");
    let provider = neo4j::Neo4jProvider::from_config("test_neo4j", &config)
        .expect("Failed to create Neo4j provider");

    let filter = serde_json::json!({"id": "1"});
    let result = provider.delete("nodes", &filter).await;
    assert!(result.is_ok());
    let doc_result = result.unwrap();
    assert_eq!(doc_result.operation, "delete");
}

#[tokio::test]
async fn test_neo4j_list_collections() {
    let config = make_config("neo4j");
    let provider = neo4j::Neo4jProvider::from_config("test_neo4j", &config)
        .expect("Failed to create Neo4j provider");

    let result = provider.list_collections().await;
    assert!(result.is_ok());
}

// ══════════════════════════════════════════════════════════════════════════════
// CROSS-CUTTING TESTS
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_all_stream_providers_have_stream_capabilities() {
    let config = make_config("kafka");

    let kafka = kafka::KafkaProvider::from_config("k", &config).unwrap();
    assert!(kafka.info().capabilities.contains(&Capability::StreamProduce));
    assert!(kafka.info().capabilities.contains(&Capability::StreamConsume));

    let config = make_config("rabbitmq");
    let rabbitmq = rabbitmq::RabbitMqProvider::from_config("r", &config).unwrap();
    assert!(rabbitmq.info().capabilities.contains(&Capability::StreamProduce));
    assert!(rabbitmq.info().capabilities.contains(&Capability::StreamConsume));

    let config = make_config("kinesis");
    let kinesis = kinesis::KinesisProvider::from_config("ki", &config).unwrap();
    assert!(kinesis.info().capabilities.contains(&Capability::StreamProduce));
    assert!(kinesis.info().capabilities.contains(&Capability::StreamConsume));

    let config = make_config("pubsub");
    let pubsub = pubsub::PubSubProvider::from_config("ps", &config).unwrap();
    assert!(pubsub.info().capabilities.contains(&Capability::StreamProduce));
    assert!(pubsub.info().capabilities.contains(&Capability::StreamConsume));

    let config = make_config("redis_stream");
    let redis_stream = redis_stream::RedisStreamProvider::from_config("rs", &config).unwrap();
    assert!(redis_stream.info().capabilities.contains(&Capability::StreamProduce));
    assert!(redis_stream.info().capabilities.contains(&Capability::StreamConsume));
}

#[tokio::test]
async fn test_all_saas_providers_return_object_types() {
    let config = make_config("salesforce");
    let sf = salesforce::SalesforceProvider::from_config("sf", &config).unwrap();
    assert!(!sf.list_object_types().await.unwrap().is_empty());

    let config = make_config("hubspot");
    let hs = hubspot::HubSpotProvider::from_config("hs", &config).unwrap();
    assert!(!hs.list_object_types().await.unwrap().is_empty());

    let config = make_config("stripe");
    let stripe = stripe::StripeProvider::from_config("stripe", &config).unwrap();
    assert!(!stripe.list_object_types().await.unwrap().is_empty());

    let config = make_config("github");
    let gh = github::GitHubProvider::from_config("gh", &config).unwrap();
    assert!(!gh.list_object_types().await.unwrap().is_empty());

    let config = make_config("jira");
    let jira = jira::JiraProvider::from_config("jira", &config).unwrap();
    assert!(!jira.list_object_types().await.unwrap().is_empty());

    let config = make_config("slack");
    let slack = slack::SlackProvider::from_config("slack", &config).unwrap();
    assert!(!slack.list_object_types().await.unwrap().is_empty());
}
