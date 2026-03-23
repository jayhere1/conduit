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

pub mod traits;
pub mod traits_saas;
pub mod registry;
pub mod errors;
pub mod providers;
pub mod secrets;
pub mod plugin;

pub use traits::*;
pub use traits_saas::{SaasProvider, SaasResult, RateLimitInfo, DocumentProvider, DocumentResult};
pub use registry::ProviderRegistry;
pub use errors::ProviderError;
pub use secrets::{SecretsChain, SecretsConfig, SecretsBackend, SecretsError};
pub use plugin::{PluginManager, PluginManifest};
