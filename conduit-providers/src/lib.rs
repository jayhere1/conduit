//! conduit-providers: Connection registry and data source provider SDK.
//!
//! This crate provides:
//! - A trait-based provider plugin system for integrating with external data sources
//! - A connection registry that manages named connections from `conduit.yaml`
//! - Built-in providers for SQL databases, object storage, HTTP APIs, and streaming
//!
//! # Architecture
//!
//! ```text
//! conduit.yaml              Provider Registry             Task Execution
//! ┌──────────────┐    ┌────────────────────────┐    ┌──────────────────┐
//! │ connections:  │    │                        │    │  TaskType::Sql { │
//! │   my_pg:     │───▶│  ProviderRegistry      │◀───│    connection:   │
//! │     type: pg │    │    .get("my_pg")        │    │      "my_pg"    │
//! │     host: .. │    │    → PostgresProvider   │    │    query: "..."  │
//! └──────────────┘    └────────────────────────┘    └──────────────────┘
//! ```
//!
//! # Provider Types
//!
//! - [`SqlProvider`] — Relational databases (Postgres, Snowflake, ClickHouse, etc.)
//! - [`StorageProvider`] — Object stores (S3, GCS, Azure Blob)
//! - [`HttpProvider`] — REST APIs and webhooks
//! - [`StreamProvider`] — Message queues (Kafka, Kinesis, Pub/Sub)

pub mod errors;
pub mod plugin;
pub mod providers;
pub mod registry;
pub mod secrets;
pub mod traits;
pub mod traits_saas;

pub use errors::ProviderError;
pub use plugin::{PluginManager, PluginManifest};
pub use registry::ProviderRegistry;
pub use secrets::{SecretsBackend, SecretsChain, SecretsConfig, SecretsError};
pub use traits::*;
pub use traits_saas::{DocumentProvider, DocumentResult, RateLimitInfo, SaasProvider, SaasResult};

/// Connection-type strings whose providers are stubs — `info().is_stub` is
/// true and their data operations return `NotImplemented`. Used by callers
/// (e.g. `conduit compile`) to warn before runtime when a DAG would route
/// through one.
///
/// Kept in sync with the `is_stub: true` settings on each provider's
/// `ProviderInfo`; the test in `tests/stub_contract_test.rs` enforces
/// agreement at compile time.
pub fn is_stub_provider_type(conn_type: &str) -> bool {
    matches!(
        conn_type.to_lowercase().as_str(),
        "kinesis"
            | "neo4j"
            | "hubspot"
            | "dynamodb"
            | "salesforce"
            | "redis_doc"
            | "clickhouse"
            | "sqlserver"
            | "cassandra"
            | "elasticsearch"
            | "slack"
            | "stripe"
            | "github"
            | "oracle"
            | "redis_stream"
            | "jira"
            | "pubsub"
            | "rabbitmq"
            | "kafka"
            | "mongodb"
    )
}
