//! Provider registry: maps connection names to live provider instances.
//!
//! The registry is initialized from `ConduitConfig::connections` at startup.
//! It resolves each connection's `type` field to the appropriate provider
//! implementation, initializes it, and stores it for lookup during task execution.

use std::collections::HashMap;
use std::sync::Arc;

use conduit_common::config::ConnectionConfig;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::errors::ProviderError;
use crate::providers;
use crate::secrets::{SecretsChain, SecretsConfig};
use crate::traits::*;
use crate::traits_saas::{DocumentProvider, SaasProvider};

/// Thread-safe handle to a provider instance.
pub type AnyProvider = Arc<dyn Provider>;
pub type AnySqlProvider = Arc<dyn SqlProvider>;
pub type AnyStorageProvider = Arc<dyn StorageProvider>;
pub type AnyHttpProvider = Arc<dyn HttpProvider>;
pub type AnyStreamProvider = Arc<dyn StreamProvider>;
pub type AnySaasProvider = Arc<dyn SaasProvider>;
pub type AnyDocumentProvider = Arc<dyn DocumentProvider>;

/// Categorized provider instance.
#[derive(Clone)]
pub enum ProviderInstance {
    Sql(AnySqlProvider),
    Storage(AnyStorageProvider),
    Http(AnyHttpProvider),
    Stream(AnyStreamProvider),
    Saas(AnySaasProvider),
    Document(AnyDocumentProvider),
}

impl ProviderInstance {
    /// Get the base provider (all variants implement Provider).
    pub fn as_provider(&self) -> &dyn Provider {
        match self {
            ProviderInstance::Sql(p) => p.as_ref(),
            ProviderInstance::Storage(p) => p.as_ref(),
            ProviderInstance::Http(p) => p.as_ref(),
            ProviderInstance::Stream(p) => p.as_ref(),
            ProviderInstance::Saas(p) => p.as_ref(),
            ProviderInstance::Document(p) => p.as_ref(),
        }
    }

    /// Try to get as a SQL provider.
    pub fn as_sql(&self) -> Option<&dyn SqlProvider> {
        match self {
            ProviderInstance::Sql(p) => Some(p.as_ref()),
            _ => None,
        }
    }

    /// Try to get as a storage provider.
    pub fn as_storage(&self) -> Option<&dyn StorageProvider> {
        match self {
            ProviderInstance::Storage(p) => Some(p.as_ref()),
            _ => None,
        }
    }

    /// Try to get as an HTTP provider.
    pub fn as_http(&self) -> Option<&dyn HttpProvider> {
        match self {
            ProviderInstance::Http(p) => Some(p.as_ref()),
            _ => None,
        }
    }

    /// Try to get as a stream provider.
    pub fn as_stream(&self) -> Option<&dyn StreamProvider> {
        match self {
            ProviderInstance::Stream(p) => Some(p.as_ref()),
            _ => None,
        }
    }

    /// Try to get as a SaaS provider.
    pub fn as_saas(&self) -> Option<&dyn SaasProvider> {
        match self {
            ProviderInstance::Saas(p) => Some(p.as_ref()),
            _ => None,
        }
    }

    /// Try to get as a document/NoSQL provider.
    pub fn as_document(&self) -> Option<&dyn DocumentProvider> {
        match self {
            ProviderInstance::Document(p) => Some(p.as_ref()),
            _ => None,
        }
    }
}

/// Summary information about a registered connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSummary {
    pub name: String,
    pub provider_type: String,
    pub display_name: String,
    pub host: Option<String>,
    pub database: Option<String>,
    pub capabilities: Vec<Capability>,
    pub status: ConnectionStatus,
}

/// Connection health status.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Error,
    Unknown,
}

/// Central registry of all configured provider connections.
pub struct ProviderRegistry {
    /// Named provider instances.
    providers: HashMap<String, ProviderInstance>,
    /// Raw connection configs (for API display).
    configs: HashMap<String, ConnectionConfig>,
    /// Secrets chain for credential resolution.
    secrets: Option<Arc<SecretsChain>>,
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ProviderRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
            configs: HashMap::new(),
            secrets: None,
        }
    }

    /// Register a pre-built provider instance under a connection name.
    ///
    /// Config-driven setups should use `from_configs`; this is for tests and
    /// embedders that construct providers directly.
    pub fn register(&mut self, name: impl Into<String>, instance: ProviderInstance) {
        self.providers.insert(name.into(), instance);
    }

    /// Initialize the registry from a map of connection configs.
    ///
    /// For each connection, resolves the provider type and creates an instance.
    /// Connections that fail to initialize are logged as warnings but don't block startup.
    pub async fn from_configs(connections: &HashMap<String, ConnectionConfig>) -> Self {
        let mut registry = Self::new();

        for (name, config) in connections {
            match Self::create_provider(name, config) {
                Ok(instance) => {
                    info!(
                        connection = %name,
                        provider_type = %config.conn_type,
                        "Registered connection"
                    );
                    registry.providers.insert(name.clone(), instance);
                    registry.configs.insert(name.clone(), config.clone());
                }
                Err(e) => {
                    warn!(
                        connection = %name,
                        provider_type = %config.conn_type,
                        error = %e,
                        "Failed to initialize connection (will retry on first use)"
                    );
                    // Still store the config so the API can show it
                    registry.configs.insert(name.clone(), config.clone());
                }
            }
        }

        info!(
            total = connections.len(),
            initialized = registry.providers.len(),
            "Provider registry initialized"
        );

        registry
    }

    /// Initialize the registry with a secrets chain for credential resolution.
    ///
    /// This is the preferred constructor when external secrets backends
    /// (Vault, AWS SSM, GCP Secret Manager) are configured.
    pub async fn from_configs_with_secrets(
        connections: &HashMap<String, ConnectionConfig>,
        secrets_config: &SecretsConfig,
    ) -> Self {
        let chain = Arc::new(SecretsChain::default_chain(secrets_config));

        let mut registry = Self {
            providers: HashMap::new(),
            configs: HashMap::new(),
            secrets: Some(chain.clone()),
        };

        for (name, config) in connections {
            match Self::create_provider(name, config) {
                Ok(instance) => {
                    info!(
                        connection = %name,
                        provider_type = %config.conn_type,
                        "Registered connection"
                    );
                    registry.providers.insert(name.clone(), instance);
                    registry.configs.insert(name.clone(), config.clone());
                }
                Err(e) => {
                    warn!(
                        connection = %name,
                        provider_type = %config.conn_type,
                        error = %e,
                        "Failed to initialize connection (will retry on first use)"
                    );
                    registry.configs.insert(name.clone(), config.clone());
                }
            }
        }

        info!(
            total = connections.len(),
            initialized = registry.providers.len(),
            secrets_backends = ?chain.backend_names(),
            "Provider registry initialized with secrets chain"
        );

        registry
    }

    /// Get the secrets chain (if configured).
    pub fn secrets(&self) -> Option<&SecretsChain> {
        self.secrets.as_ref().map(|s| s.as_ref())
    }

    /// Resolve a credential through the secrets chain, falling back to
    /// the synchronous resolver if no chain is configured.
    pub async fn resolve_credential(
        &self,
        reference: &str,
        connection_name: &str,
    ) -> Result<String, ProviderError> {
        if let Some(chain) = &self.secrets {
            chain
                .resolve_for_connection(reference, connection_name)
                .await
        } else {
            providers::resolve_credential(reference).map_err(|mut e| {
                // Patch the connection name into the error
                if let ProviderError::AuthenticationFailed { connection, .. } = &mut e {
                    if connection.is_empty() {
                        *connection = connection_name.to_string();
                    }
                }
                e
            })
        }
    }

    /// Create a provider instance from a connection config.
    fn create_provider(
        name: &str,
        config: &ConnectionConfig,
    ) -> Result<ProviderInstance, ProviderError> {
        let provider_type = config.conn_type.to_lowercase();

        match provider_type.as_str() {
            // ── SQL providers ───────────────────────────────────
            "postgres" | "postgresql" | "pg" => {
                let p = providers::postgres::PostgresProvider::from_config(name, config)?;
                Ok(ProviderInstance::Sql(Arc::new(p)))
            }
            "snowflake" | "sf" => {
                let p = providers::snowflake::SnowflakeProvider::from_config(name, config)?;
                Ok(ProviderInstance::Sql(Arc::new(p)))
            }
            "clickhouse" | "ch" => {
                let p = providers::clickhouse::ClickHouseProvider::from_config(name, config)?;
                Ok(ProviderInstance::Sql(Arc::new(p)))
            }
            "redshift" => {
                let p = providers::redshift::RedshiftProvider::from_config(name, config)?;
                Ok(ProviderInstance::Sql(Arc::new(p)))
            }
            "bigquery" | "bq" => {
                let p = providers::bigquery::BigQueryProvider::from_config(name, config)?;
                Ok(ProviderInstance::Sql(Arc::new(p)))
            }
            #[cfg(feature = "duckdb")]
            "duckdb" | "duck" => {
                let p = providers::duckdb::DuckDbProvider::from_config(name, config)?;
                Ok(ProviderInstance::Sql(Arc::new(p)))
            }
            "mysql" | "mariadb" => {
                let p = providers::mysql::MySqlProvider::from_config(name, config)?;
                Ok(ProviderInstance::Sql(Arc::new(p)))
            }
            "sqlite" => {
                let p = providers::sqlite::SqliteProvider::from_config(name, config)?;
                Ok(ProviderInstance::Sql(Arc::new(p)))
            }
            "oracle" => {
                let p = providers::oracle::OracleProvider::from_config(name, config)?;
                Ok(ProviderInstance::Sql(Arc::new(p)))
            }
            "sqlserver" | "mssql" => {
                let p = providers::sqlserver::SqlServerProvider::from_config(name, config)?;
                Ok(ProviderInstance::Sql(Arc::new(p)))
            }
            "cockroachdb" | "crdb" => {
                let p = providers::cockroachdb::CockroachDbProvider::from_config(name, config)?;
                Ok(ProviderInstance::Sql(Arc::new(p)))
            }
            "timescaledb" | "tsdb" => {
                let p = providers::timescaledb::TimescaleDbProvider::from_config(name, config)?;
                Ok(ProviderInstance::Sql(Arc::new(p)))
            }

            // ── Storage providers ───────────────────────────────
            "s3" | "aws_s3" => {
                let p = providers::s3::S3Provider::from_config(name, config)?;
                Ok(ProviderInstance::Storage(Arc::new(p)))
            }
            "gcs" | "google_cloud_storage" => {
                let p = providers::gcs::GcsProvider::from_config(name, config)?;
                Ok(ProviderInstance::Storage(Arc::new(p)))
            }

            // ── HTTP providers ──────────────────────────────────
            "http" | "https" | "rest" | "webhook" => {
                let p = providers::http::HttpApiProvider::from_config(name, config)?;
                Ok(ProviderInstance::Http(Arc::new(p)))
            }

            // ── Stream providers ────────────────────────────────
            "kafka" => {
                let p = providers::kafka::KafkaProvider::from_config(name, config)?;
                Ok(ProviderInstance::Stream(Arc::new(p)))
            }
            "rabbitmq" | "amqp" => {
                let p = providers::rabbitmq::RabbitMqProvider::from_config(name, config)?;
                Ok(ProviderInstance::Stream(Arc::new(p)))
            }
            "kinesis" => {
                let p = providers::kinesis::KinesisProvider::from_config(name, config)?;
                Ok(ProviderInstance::Stream(Arc::new(p)))
            }
            "pubsub" | "gcp_pubsub" => {
                let p = providers::pubsub::PubSubProvider::from_config(name, config)?;
                Ok(ProviderInstance::Stream(Arc::new(p)))
            }
            "redis" | "redis_stream" => {
                let p = providers::redis_stream::RedisStreamProvider::from_config(name, config)?;
                Ok(ProviderInstance::Stream(Arc::new(p)))
            }

            // ── SaaS providers ──────────────────────────────────
            "salesforce" | "sfdc" => {
                let p = providers::salesforce::SalesforceProvider::from_config(name, config)?;
                Ok(ProviderInstance::Saas(Arc::new(p)))
            }
            "hubspot" => {
                let p = providers::hubspot::HubSpotProvider::from_config(name, config)?;
                Ok(ProviderInstance::Saas(Arc::new(p)))
            }
            "stripe" => {
                let p = providers::stripe::StripeProvider::from_config(name, config)?;
                Ok(ProviderInstance::Saas(Arc::new(p)))
            }
            "github" | "gh" => {
                let p = providers::github::GitHubProvider::from_config(name, config)?;
                Ok(ProviderInstance::Saas(Arc::new(p)))
            }
            "jira" => {
                let p = providers::jira::JiraProvider::from_config(name, config)?;
                Ok(ProviderInstance::Saas(Arc::new(p)))
            }
            "slack" => {
                let p = providers::slack::SlackProvider::from_config(name, config)?;
                Ok(ProviderInstance::Saas(Arc::new(p)))
            }

            // ── Document / NoSQL providers ──────────────────────
            "mongodb" | "mongo" => {
                let p = providers::mongodb::MongoDbProvider::from_config(name, config)?;
                Ok(ProviderInstance::Document(Arc::new(p)))
            }
            "dynamodb" => {
                let p = providers::dynamodb::DynamoDbProvider::from_config(name, config)?;
                Ok(ProviderInstance::Document(Arc::new(p)))
            }
            "cassandra" | "scylladb" => {
                let p = providers::cassandra::CassandraProvider::from_config(name, config)?;
                Ok(ProviderInstance::Document(Arc::new(p)))
            }
            "elasticsearch" | "opensearch" | "es" => {
                let p = providers::elasticsearch::ElasticsearchProvider::from_config(name, config)?;
                Ok(ProviderInstance::Document(Arc::new(p)))
            }
            "redis_kv" => {
                let p = providers::redis_doc::RedisDocProvider::from_config(name, config)?;
                Ok(ProviderInstance::Document(Arc::new(p)))
            }
            "neo4j" => {
                let p = providers::neo4j::Neo4jProvider::from_config(name, config)?;
                Ok(ProviderInstance::Document(Arc::new(p)))
            }

            _ => Err(ProviderError::UnsupportedProvider {
                provider_type: config.conn_type.clone(),
            }),
        }
    }

    /// Whether a provider type is a stub (a placeholder whose operations
    /// return `NotImplemented`) rather than a real implementation.
    ///
    /// Authoritative: instantiates the provider with a throwaway config and
    /// reads `ProviderInfo.is_stub` — the same flag `test_connection`/`execute`
    /// honor. Falls back to the static [`crate::is_stub_provider_type`] list if
    /// the provider can't be constructed from the dummy config. This is the
    /// source of truth for the generated provider reference docs (PRD §8.5).
    pub fn provider_is_stub(conn_type: &str) -> bool {
        let cfg = ConnectionConfig {
            conn_type: conn_type.to_string(),
            host: Some("doc.invalid".to_string()),
            port: Some(0),
            database: Some("doc".to_string()),
            credentials: None,
            extra: std::collections::HashMap::new(),
        };
        match Self::create_provider("doc", &cfg) {
            Ok(instance) => instance.as_provider().info().is_stub,
            Err(_) => crate::is_stub_provider_type(conn_type),
        }
    }

    /// Get a provider by connection name.
    pub fn get(&self, name: &str) -> Option<&ProviderInstance> {
        self.providers.get(name)
    }

    /// Get a SQL provider by connection name.
    pub fn get_sql(&self, name: &str) -> Result<&dyn SqlProvider, ProviderError> {
        self.providers
            .get(name)
            .ok_or_else(|| ProviderError::ConnectionNotFound {
                name: name.to_string(),
            })?
            .as_sql()
            .ok_or_else(|| ProviderError::InvalidConfig {
                connection: name.to_string(),
                reason: "Connection is not a SQL provider".to_string(),
            })
    }

    /// Get a storage provider by connection name.
    pub fn get_storage(&self, name: &str) -> Result<&dyn StorageProvider, ProviderError> {
        self.providers
            .get(name)
            .ok_or_else(|| ProviderError::ConnectionNotFound {
                name: name.to_string(),
            })?
            .as_storage()
            .ok_or_else(|| ProviderError::InvalidConfig {
                connection: name.to_string(),
                reason: "Connection is not a storage provider".to_string(),
            })
    }

    /// Get a raw connection config.
    pub fn get_config(&self, name: &str) -> Option<&ConnectionConfig> {
        self.configs.get(name)
    }

    /// List all registered connection names.
    pub fn connection_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.configs.keys().cloned().collect();
        names.sort();
        names
    }

    /// Get summary info for all connections.
    pub fn list_connections(&self) -> Vec<ConnectionSummary> {
        self.configs
            .iter()
            .map(|(name, config)| {
                let (display_name, capabilities, status) =
                    if let Some(instance) = self.providers.get(name) {
                        let info = instance.as_provider().info();
                        (
                            info.display_name,
                            info.capabilities,
                            ConnectionStatus::Connected,
                        )
                    } else {
                        (
                            format_display_name(&config.conn_type),
                            vec![],
                            ConnectionStatus::Unknown,
                        )
                    };

                ConnectionSummary {
                    name: name.clone(),
                    provider_type: config.conn_type.clone(),
                    display_name,
                    host: config.host.clone(),
                    database: config.database.clone(),
                    capabilities,
                    status,
                }
            })
            .collect()
    }

    /// Test a specific connection and return the result.
    pub async fn test_connection(&self, name: &str) -> Result<ConnectionTestResult, ProviderError> {
        let instance =
            self.providers
                .get(name)
                .ok_or_else(|| ProviderError::ConnectionNotFound {
                    name: name.to_string(),
                })?;

        instance.as_provider().test_connection().await
    }

    /// Test all connections and return results.
    pub async fn test_all(&self) -> HashMap<String, Result<ConnectionTestResult, ProviderError>> {
        let mut results = HashMap::new();
        for name in self.providers.keys() {
            let result = self.test_connection(name).await;
            results.insert(name.clone(), result);
        }
        results
    }

    /// Number of registered connections.
    pub fn len(&self) -> usize {
        self.configs.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.configs.is_empty()
    }

    /// Number of successfully initialized providers.
    pub fn connected_count(&self) -> usize {
        self.providers.len()
    }
}

/// Format a provider type string into a human-readable display name.
fn format_display_name(provider_type: &str) -> String {
    match provider_type.to_lowercase().as_str() {
        "postgres" | "postgresql" | "pg" => "PostgreSQL".to_string(),
        "snowflake" | "sf" => "Snowflake".to_string(),
        "clickhouse" | "ch" => "ClickHouse".to_string(),
        "redshift" => "Amazon Redshift".to_string(),
        "bigquery" | "bq" => "Google BigQuery".to_string(),
        #[cfg(feature = "duckdb")]
        "duckdb" | "duck" => "DuckDB".to_string(),
        "mysql" | "mariadb" => "MySQL".to_string(),
        "sqlite" => "SQLite".to_string(),
        "oracle" => "Oracle Database".to_string(),
        "sqlserver" | "mssql" => "SQL Server".to_string(),
        "cockroachdb" | "crdb" => "CockroachDB".to_string(),
        "timescaledb" | "tsdb" => "TimescaleDB".to_string(),
        "s3" | "aws_s3" => "Amazon S3".to_string(),
        "gcs" | "google_cloud_storage" => "Google Cloud Storage".to_string(),
        "http" | "https" | "rest" => "HTTP/REST API".to_string(),
        "webhook" => "Webhook".to_string(),
        "kafka" => "Apache Kafka".to_string(),
        "rabbitmq" | "amqp" => "RabbitMQ".to_string(),
        "kinesis" => "AWS Kinesis".to_string(),
        "pubsub" | "gcp_pubsub" => "GCP Pub/Sub".to_string(),
        "redis" | "redis_stream" => "Redis Streams".to_string(),
        "salesforce" | "sfdc" => "Salesforce".to_string(),
        "hubspot" => "HubSpot".to_string(),
        "stripe" => "Stripe".to_string(),
        "github" | "gh" => "GitHub".to_string(),
        "jira" => "Jira".to_string(),
        "slack" => "Slack".to_string(),
        "mongodb" | "mongo" => "MongoDB".to_string(),
        "dynamodb" => "DynamoDB".to_string(),
        "cassandra" | "scylladb" => "Cassandra".to_string(),
        "elasticsearch" | "opensearch" | "es" => "Elasticsearch".to_string(),
        "redis_kv" => "Redis KV".to_string(),
        "neo4j" => "Neo4j".to_string(),
        _ => provider_type.to_string(),
    }
}

/// Supported provider type identifiers and their aliases.
pub fn supported_provider_types() -> Vec<(
    &'static str,
    &'static str,
    &'static [&'static str],
    &'static str,
)> {
    vec![
        // SQL
        ("postgres", "PostgreSQL", &["postgresql", "pg"], "sql"),
        ("snowflake", "Snowflake", &["sf"], "sql"),
        ("clickhouse", "ClickHouse", &["ch"], "sql"),
        ("redshift", "Amazon Redshift", &[], "sql"),
        ("bigquery", "Google BigQuery", &["bq"], "sql"),
        ("duckdb", "DuckDB", &["duck"], "sql"),
        ("mysql", "MySQL", &["mariadb"], "sql"),
        ("sqlite", "SQLite", &[], "sql"),
        ("oracle", "Oracle Database", &[], "sql"),
        ("sqlserver", "SQL Server", &["mssql"], "sql"),
        ("cockroachdb", "CockroachDB", &["crdb"], "sql"),
        ("timescaledb", "TimescaleDB", &["tsdb"], "sql"),
        // Storage
        ("s3", "Amazon S3", &["aws_s3"], "storage"),
        (
            "gcs",
            "Google Cloud Storage",
            &["google_cloud_storage"],
            "storage",
        ),
        // HTTP
        (
            "http",
            "HTTP/REST API",
            &["https", "rest", "webhook"],
            "http",
        ),
        // Streaming
        ("kafka", "Apache Kafka", &[], "stream"),
        ("rabbitmq", "RabbitMQ", &["amqp"], "stream"),
        ("kinesis", "AWS Kinesis", &[], "stream"),
        ("pubsub", "GCP Pub/Sub", &["gcp_pubsub"], "stream"),
        ("redis", "Redis Streams", &["redis_stream"], "stream"),
        // SaaS
        ("salesforce", "Salesforce", &["sfdc"], "saas"),
        ("hubspot", "HubSpot", &[], "saas"),
        ("stripe", "Stripe", &[], "saas"),
        ("github", "GitHub", &["gh"], "saas"),
        ("jira", "Jira", &[], "saas"),
        ("slack", "Slack", &[], "saas"),
        // Document / NoSQL
        ("mongodb", "MongoDB", &["mongo"], "document"),
        ("dynamodb", "DynamoDB", &[], "document"),
        ("cassandra", "Cassandra", &["scylladb"], "document"),
        (
            "elasticsearch",
            "Elasticsearch",
            &["opensearch", "es"],
            "document",
        ),
        ("redis_kv", "Redis KV", &[], "document"),
        ("neo4j", "Neo4j", &[], "document"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg_config(host: &str, db: &str) -> ConnectionConfig {
        ConnectionConfig {
            conn_type: "postgres".to_string(),
            host: Some(host.to_string()),
            port: Some(5432),
            database: Some(db.to_string()),
            credentials: None,
            extra: HashMap::new(),
        }
    }

    fn s3_config(bucket: &str) -> ConnectionConfig {
        ConnectionConfig {
            conn_type: "s3".to_string(),
            host: None,
            port: None,
            database: Some(bucket.to_string()),
            credentials: None,
            extra: HashMap::new(),
        }
    }

    fn webhook_config(host: &str) -> ConnectionConfig {
        ConnectionConfig {
            conn_type: "webhook".to_string(),
            host: Some(host.to_string()),
            port: None,
            database: None,
            credentials: None,
            extra: HashMap::new(),
        }
    }

    fn kafka_config(brokers: &str) -> ConnectionConfig {
        ConnectionConfig {
            conn_type: "kafka".to_string(),
            host: Some(brokers.to_string()),
            port: None,
            database: None,
            credentials: None,
            extra: HashMap::new(),
        }
    }

    #[test]
    fn empty_registry() {
        let registry = ProviderRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert_eq!(registry.connected_count(), 0);
        assert!(registry.connection_names().is_empty());
        assert!(registry.list_connections().is_empty());
    }

    #[tokio::test]
    async fn from_configs_registers_multiple_types() {
        let mut configs = HashMap::new();
        configs.insert("main_db".to_string(), pg_config("db.local", "analytics"));
        configs.insert("data_lake".to_string(), s3_config("my-bucket"));
        configs.insert(
            "alerts".to_string(),
            webhook_config("https://hooks.slack.com"),
        );
        configs.insert("events".to_string(), kafka_config("kafka-1:9092"));

        let registry = ProviderRegistry::from_configs(&configs).await;

        assert_eq!(registry.len(), 4);
        assert_eq!(registry.connected_count(), 4);

        let names = registry.connection_names();
        assert!(names.contains(&"main_db".to_string()));
        assert!(names.contains(&"data_lake".to_string()));
        assert!(names.contains(&"alerts".to_string()));
        assert!(names.contains(&"events".to_string()));
    }

    #[tokio::test]
    async fn get_provider_by_name() {
        let mut configs = HashMap::new();
        configs.insert("pg".to_string(), pg_config("localhost", "test"));

        let registry = ProviderRegistry::from_configs(&configs).await;

        assert!(registry.get("pg").is_some());
        assert!(registry.get("nonexistent").is_none());
    }

    #[tokio::test]
    async fn get_sql_provider_succeeds_for_sql_type() {
        let mut configs = HashMap::new();
        configs.insert("pg".to_string(), pg_config("localhost", "test"));

        let registry = ProviderRegistry::from_configs(&configs).await;

        assert!(registry.get_sql("pg").is_ok());
    }

    #[tokio::test]
    async fn get_sql_provider_fails_for_non_sql_type() {
        let mut configs = HashMap::new();
        configs.insert("bucket".to_string(), s3_config("my-bucket"));

        let registry = ProviderRegistry::from_configs(&configs).await;

        let result = registry.get_sql("bucket");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_sql_provider_fails_for_unknown_name() {
        let registry = ProviderRegistry::new();
        let result = registry.get_sql("nonexistent");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_storage_provider_succeeds_for_s3() {
        let mut configs = HashMap::new();
        configs.insert("bucket".to_string(), s3_config("my-bucket"));

        let registry = ProviderRegistry::from_configs(&configs).await;

        assert!(registry.get_storage("bucket").is_ok());
    }

    #[tokio::test]
    async fn get_storage_fails_for_sql_type() {
        let mut configs = HashMap::new();
        configs.insert("pg".to_string(), pg_config("localhost", "test"));

        let registry = ProviderRegistry::from_configs(&configs).await;

        assert!(registry.get_storage("pg").is_err());
    }

    #[tokio::test]
    async fn list_connections_returns_summaries() {
        let mut configs = HashMap::new();
        configs.insert("main_db".to_string(), pg_config("db.internal", "analytics"));

        let registry = ProviderRegistry::from_configs(&configs).await;
        let connections = registry.list_connections();

        assert_eq!(connections.len(), 1);
        let conn = &connections[0];
        assert_eq!(conn.name, "main_db");
        assert_eq!(conn.provider_type, "postgres");
        assert_eq!(conn.host.as_deref(), Some("db.internal"));
        assert_eq!(conn.database.as_deref(), Some("analytics"));
    }

    #[tokio::test]
    async fn provider_type_aliases_resolve() {
        // "pg" should resolve the same as "postgres"
        let mut configs = HashMap::new();
        configs.insert(
            "short".to_string(),
            ConnectionConfig {
                conn_type: "pg".to_string(),
                host: Some("localhost".to_string()),
                port: Some(5432),
                database: Some("test".to_string()),
                credentials: None,
                extra: HashMap::new(),
            },
        );

        let registry = ProviderRegistry::from_configs(&configs).await;
        assert!(registry.get("short").is_some());
        assert!(registry.get_sql("short").is_ok());
    }

    #[tokio::test]
    async fn unsupported_provider_type_skipped() {
        let mut configs = HashMap::new();
        configs.insert(
            "unknown".to_string(),
            ConnectionConfig {
                conn_type: "foobar_db".to_string(),
                host: Some("localhost".to_string()),
                port: None,
                database: None,
                credentials: None,
                extra: HashMap::new(),
            },
        );

        let registry = ProviderRegistry::from_configs(&configs).await;

        // Config is stored but provider instance is not.
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.connected_count(), 0);
        assert!(registry.get("unknown").is_none());
        assert!(registry.get_config("unknown").is_some());
    }

    #[tokio::test]
    async fn connection_summary_shows_unknown_for_failed_init() {
        let mut configs = HashMap::new();
        configs.insert(
            "broken".to_string(),
            ConnectionConfig {
                conn_type: "foobar".to_string(),
                host: None,
                port: None,
                database: None,
                credentials: None,
                extra: HashMap::new(),
            },
        );

        let registry = ProviderRegistry::from_configs(&configs).await;
        let connections = registry.list_connections();

        assert_eq!(connections.len(), 1);
        assert!(matches!(connections[0].status, ConnectionStatus::Unknown));
    }

    #[test]
    fn format_display_name_covers_all_types() {
        assert_eq!(format_display_name("postgres"), "PostgreSQL");
        assert_eq!(format_display_name("pg"), "PostgreSQL");
        assert_eq!(format_display_name("snowflake"), "Snowflake");
        assert_eq!(format_display_name("s3"), "Amazon S3");
        assert_eq!(format_display_name("kafka"), "Apache Kafka");
        assert_eq!(format_display_name("mongodb"), "MongoDB");
        assert_eq!(format_display_name("elasticsearch"), "Elasticsearch");
        assert_eq!(format_display_name("unknown_thing"), "unknown_thing");
    }

    #[tokio::test]
    async fn get_http_provider_succeeds() {
        let mut configs = HashMap::new();
        configs.insert("api".to_string(), webhook_config("https://api.example.com"));

        let registry = ProviderRegistry::from_configs(&configs).await;
        assert!(registry.get("api").is_some());

        // Should be accessible as HTTP but not as SQL or storage
        match registry.get("api").unwrap() {
            ProviderInstance::Http(_) => {}
            _ => panic!("Expected Http variant"),
        }
    }

    #[tokio::test]
    async fn get_stream_provider_succeeds_for_kafka() {
        let mut configs = HashMap::new();
        configs.insert("events".to_string(), kafka_config("broker-1:9092"));

        let registry = ProviderRegistry::from_configs(&configs).await;
        assert!(registry.get("events").is_some());

        match registry.get("events").unwrap() {
            ProviderInstance::Stream(_) => {}
            _ => panic!("Expected Stream variant"),
        }
    }

    #[tokio::test]
    async fn get_stream_fails_for_sql_type() {
        let mut configs = HashMap::new();
        configs.insert("db".to_string(), pg_config("localhost", "test"));

        let registry = ProviderRegistry::from_configs(&configs).await;

        // Stream accessor on a SQL connection should fail
        let instance = registry.get("db").unwrap();
        assert!(instance.as_stream().is_none());
    }

    #[tokio::test]
    async fn connection_names_are_sorted() {
        let mut configs = HashMap::new();
        configs.insert("zebra_db".to_string(), pg_config("z.local", "z"));
        configs.insert("alpha_db".to_string(), pg_config("a.local", "a"));
        configs.insert("middle_db".to_string(), pg_config("m.local", "m"));

        let registry = ProviderRegistry::from_configs(&configs).await;
        let names = registry.connection_names();

        assert_eq!(names, vec!["alpha_db", "middle_db", "zebra_db"]);
    }

    #[tokio::test]
    async fn test_connection_returns_error_for_missing_name() {
        let registry = ProviderRegistry::new();
        let result = registry.test_connection("nonexistent").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            ProviderError::ConnectionNotFound { name } => {
                assert_eq!(name, "nonexistent");
            }
            other => panic!("Expected ConnectionNotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn mixed_valid_and_invalid_configs() {
        let mut configs = HashMap::new();
        configs.insert("good".to_string(), pg_config("localhost", "test"));
        configs.insert(
            "bad".to_string(),
            ConnectionConfig {
                conn_type: "totally_unknown".to_string(),
                host: None,
                port: None,
                database: None,
                credentials: None,
                extra: HashMap::new(),
            },
        );

        let registry = ProviderRegistry::from_configs(&configs).await;

        // Both stored in configs
        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());

        // Only the valid one has a provider instance
        assert_eq!(registry.connected_count(), 1);
        assert!(registry.get("good").is_some());
        assert!(registry.get("bad").is_none());

        // But both show up in connection names
        let names = registry.connection_names();
        assert_eq!(names.len(), 2);

        // And both appear in list_connections
        let connections = registry.list_connections();
        assert_eq!(connections.len(), 2);

        let good_conn = connections.iter().find(|c| c.name == "good").unwrap();
        assert!(matches!(good_conn.status, ConnectionStatus::Connected));

        let bad_conn = connections.iter().find(|c| c.name == "bad").unwrap();
        assert!(matches!(bad_conn.status, ConnectionStatus::Unknown));
    }

    #[tokio::test]
    async fn multiple_connections_in_list() {
        let mut configs = HashMap::new();
        configs.insert(
            "pg_main".to_string(),
            pg_config("db-main.local", "analytics"),
        );
        configs.insert(
            "pg_replica".to_string(),
            pg_config("db-replica.local", "analytics"),
        );
        configs.insert("lake".to_string(), s3_config("data-lake"));
        configs.insert(
            "notifier".to_string(),
            webhook_config("https://hooks.slack.com/xxx"),
        );

        let registry = ProviderRegistry::from_configs(&configs).await;
        let connections = registry.list_connections();

        assert_eq!(connections.len(), 4);

        // Verify each connection has the right type
        let pg_main = connections.iter().find(|c| c.name == "pg_main").unwrap();
        assert_eq!(pg_main.provider_type, "postgres");
        assert_eq!(pg_main.host.as_deref(), Some("db-main.local"));
        assert_eq!(pg_main.database.as_deref(), Some("analytics"));

        let lake = connections.iter().find(|c| c.name == "lake").unwrap();
        assert_eq!(lake.provider_type, "s3");

        let notifier = connections.iter().find(|c| c.name == "notifier").unwrap();
        assert_eq!(notifier.provider_type, "webhook");
    }

    #[tokio::test]
    async fn get_config_returns_raw_connection_config() {
        let mut configs = HashMap::new();
        let original = pg_config("db.example.com", "production");
        configs.insert("prod".to_string(), original.clone());

        let registry = ProviderRegistry::from_configs(&configs).await;

        let retrieved = registry.get_config("prod").unwrap();
        assert_eq!(retrieved.conn_type, "postgres");
        assert_eq!(retrieved.host.as_deref(), Some("db.example.com"));
        assert_eq!(retrieved.database.as_deref(), Some("production"));
        assert_eq!(retrieved.port, Some(5432));

        assert!(registry.get_config("nonexistent").is_none());
    }

    #[tokio::test]
    async fn case_insensitive_provider_type() {
        // create_provider lowercases the type, so "POSTGRES" should work
        let mut configs = HashMap::new();
        configs.insert(
            "upper".to_string(),
            ConnectionConfig {
                conn_type: "POSTGRES".to_string(),
                host: Some("localhost".to_string()),
                port: Some(5432),
                database: Some("test".to_string()),
                credentials: None,
                extra: HashMap::new(),
            },
        );

        let registry = ProviderRegistry::from_configs(&configs).await;
        assert_eq!(registry.connected_count(), 1);
        assert!(registry.get_sql("upper").is_ok());
    }

    #[tokio::test]
    async fn all_sql_aliases_create_sql_providers() {
        let sql_aliases = vec![
            ("pg", "pg"),
            ("postgresql", "postgresql"),
            ("sf", "sf"),
            ("ch", "ch"),
            ("bq", "bq"),
            ("duck", "duck"),
            ("mariadb", "mariadb"),
            ("mssql", "mssql"),
            ("crdb", "crdb"),
            ("tsdb", "tsdb"),
        ];

        for (alias, name) in sql_aliases {
            let mut configs = HashMap::new();
            configs.insert(
                name.to_string(),
                ConnectionConfig {
                    conn_type: alias.to_string(),
                    host: Some("localhost".to_string()),
                    port: Some(5432),
                    database: Some("test".to_string()),
                    credentials: None,
                    extra: HashMap::new(),
                },
            );

            let registry = ProviderRegistry::from_configs(&configs).await;
            assert!(
                registry.get_sql(name).is_ok(),
                "Alias '{}' should create a SQL provider",
                alias
            );
        }
    }

    #[test]
    fn provider_error_into_conduit_error() {
        let err = ProviderError::ConnectionNotFound {
            name: "missing".to_string(),
        };
        let conduit_err = err.into_conduit_error();
        let msg = conduit_err.to_string();
        assert!(
            msg.contains("missing"),
            "Error message should contain connection name: {}",
            msg
        );
    }

    #[test]
    fn format_display_name_all_aliases() {
        // Verify all aliases map to the same display name as the canonical type
        assert_eq!(format_display_name("postgresql"), "PostgreSQL");
        assert_eq!(format_display_name("sf"), "Snowflake");
        assert_eq!(format_display_name("ch"), "ClickHouse");
        assert_eq!(format_display_name("bq"), "Google BigQuery");
        assert_eq!(format_display_name("duck"), "DuckDB");
        assert_eq!(format_display_name("mariadb"), "MySQL");
        assert_eq!(format_display_name("mssql"), "SQL Server");
        assert_eq!(format_display_name("crdb"), "CockroachDB");
        assert_eq!(format_display_name("tsdb"), "TimescaleDB");
        assert_eq!(format_display_name("aws_s3"), "Amazon S3");
        assert_eq!(
            format_display_name("google_cloud_storage"),
            "Google Cloud Storage"
        );
        assert_eq!(format_display_name("https"), "HTTP/REST API");
        assert_eq!(format_display_name("rest"), "HTTP/REST API");
        assert_eq!(format_display_name("amqp"), "RabbitMQ");
        assert_eq!(format_display_name("gcp_pubsub"), "GCP Pub/Sub");
        assert_eq!(format_display_name("redis_stream"), "Redis Streams");
        assert_eq!(format_display_name("sfdc"), "Salesforce");
        assert_eq!(format_display_name("gh"), "GitHub");
        assert_eq!(format_display_name("mongo"), "MongoDB");
        assert_eq!(format_display_name("scylladb"), "Cassandra");
        assert_eq!(format_display_name("opensearch"), "Elasticsearch");
        assert_eq!(format_display_name("es"), "Elasticsearch");
    }

    #[test]
    fn supported_provider_types_is_comprehensive() {
        let types = supported_provider_types();
        // Should have at least 30 entries (12 SQL + 2 storage + 1 HTTP + 5 stream + 6 SaaS + 6 document)
        assert!(
            types.len() >= 30,
            "Expected at least 30 provider types, got {}",
            types.len()
        );

        // Verify categories.
        let sql_count = types.iter().filter(|t| t.3 == "sql").count();
        let storage_count = types.iter().filter(|t| t.3 == "storage").count();
        let stream_count = types.iter().filter(|t| t.3 == "stream").count();
        let saas_count = types.iter().filter(|t| t.3 == "saas").count();
        let doc_count = types.iter().filter(|t| t.3 == "document").count();

        assert!(sql_count >= 10, "Should have at least 10 SQL providers");
        assert!(
            storage_count >= 2,
            "Should have at least 2 storage providers"
        );
        assert!(stream_count >= 4, "Should have at least 4 stream providers");
        assert!(saas_count >= 5, "Should have at least 5 SaaS providers");
        assert!(doc_count >= 5, "Should have at least 5 document providers");
    }
}
