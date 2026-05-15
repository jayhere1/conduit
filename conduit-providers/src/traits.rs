//! Core provider traits.
//!
//! Every external system Conduit integrates with implements one of these traits.
//! The trait system is designed so that:
//! - Providers are `Send + Sync` (safe across async boundaries)
//! - Each provider type has a well-defined set of capabilities
//! - Connection testing is always available for health checks
//! - Results carry structured metadata for the Conduit protocol

use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::errors::ProviderError;

// ─── Common Types ───────────────────────────────────────────────────────────

/// Metadata about a provider instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderInfo {
    /// Provider type identifier (e.g., "postgres", "snowflake", "s3").
    pub provider_type: String,
    /// Human-readable display name.
    pub display_name: String,
    /// Provider version (e.g., "15.4" for PostgreSQL).
    pub version: Option<String>,
    /// Capabilities this provider supports.
    pub capabilities: Vec<Capability>,
    /// True when this provider is a stub: its `execute` / `test_connection`
    /// methods return `NotImplemented` and any task referencing it WILL fail
    /// at runtime. Surfaced at compile time and in `/connections` so callers
    /// don't unknowingly route real workloads through a placeholder.
    #[serde(default)]
    pub is_stub: bool,
}

/// What a provider can do.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Capability {
    /// Can execute SQL queries.
    SqlQuery,
    /// Can execute DDL statements (CREATE, ALTER, DROP).
    SqlDdl,
    /// Can perform bulk data loads.
    BulkLoad,
    /// Can stream data incrementally.
    IncrementalRead,
    /// Supports transactions.
    Transactions,
    /// Can read objects/files.
    StorageRead,
    /// Can write objects/files.
    StorageWrite,
    /// Can list objects/files.
    StorageList,
    /// Can send HTTP requests.
    HttpRequest,
    /// Can produce messages to a stream.
    StreamProduce,
    /// Can consume messages from a stream.
    StreamConsume,
}

/// Result from a SQL query execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlResult {
    /// Number of rows affected by the query.
    pub rows_affected: u64,
    /// Number of rows returned (for SELECT queries).
    pub rows_returned: Option<u64>,
    /// Wall-clock execution time in milliseconds.
    pub execution_time_ms: u64,
    /// Column names (for SELECT queries).
    pub columns: Vec<String>,
    /// Row data as JSON values (for SELECT queries, limited to first N rows).
    pub sample_rows: Vec<Vec<serde_json::Value>>,
    /// Auto-collected metrics for evidence/contracts.
    pub metrics: HashMap<String, f64>,
}

impl SqlResult {
    /// Create an empty result.
    pub fn empty() -> Self {
        Self {
            rows_affected: 0,
            rows_returned: None,
            execution_time_ms: 0,
            columns: Vec::new(),
            sample_rows: Vec::new(),
            metrics: HashMap::new(),
        }
    }

    /// Format this result as Conduit protocol messages for stdout.
    pub fn to_protocol_output(&self) -> String {
        let mut out = String::new();

        out.push_str(&format!(
            "CONDUIT::LOG::INFO::Query completed in {}ms\n",
            self.execution_time_ms
        ));

        if let Some(rows) = self.rows_returned {
            out.push_str(&format!("CONDUIT::LOG::INFO::Rows returned: {}\n", rows));
        }

        out.push_str(&format!(
            "CONDUIT::LOG::INFO::Rows affected: {}\n",
            self.rows_affected
        ));

        // Emit metrics for contract evaluation
        out.push_str(&format!(
            "CONDUIT::METRIC::row_count::{}\n",
            self.rows_returned.unwrap_or(self.rows_affected)
        ));

        for (name, value) in &self.metrics {
            out.push_str(&format!("CONDUIT::METRIC::{}::{}\n", name, value));
        }

        // Emit XCom result
        out.push_str(&format!(
            "CONDUIT::XCOM::{{\"rows_affected\": {}, \"rows_returned\": {}, \"execution_time_ms\": {}}}\n",
            self.rows_affected,
            self.rows_returned.map_or("null".to_string(), |r| r.to_string()),
            self.execution_time_ms,
        ));

        out
    }
}

/// Result from a storage operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageResult {
    /// Operation performed.
    pub operation: String,
    /// Number of objects/files affected.
    pub objects_affected: u64,
    /// Total bytes transferred.
    pub bytes_transferred: u64,
    /// Execution time in milliseconds.
    pub execution_time_ms: u64,
    /// Object URIs created/modified.
    pub uris: Vec<String>,
}

/// Result from an HTTP request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpResult {
    /// HTTP status code.
    pub status_code: u16,
    /// Response headers (selected).
    pub headers: HashMap<String, String>,
    /// Response body (truncated).
    pub body: String,
    /// Execution time in milliseconds.
    pub execution_time_ms: u64,
}

/// Result from a stream operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamResult {
    /// Number of messages produced/consumed.
    pub message_count: u64,
    /// Total bytes transferred.
    pub bytes_transferred: u64,
    /// Execution time in milliseconds.
    pub execution_time_ms: u64,
}

/// Result from testing a connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionTestResult {
    /// Whether the connection succeeded.
    pub success: bool,
    /// Human-readable status message.
    pub message: String,
    /// Time to connect in milliseconds.
    pub latency_ms: u64,
    /// Server version string, if available.
    pub server_version: Option<String>,
}

// ─── Provider Traits ────────────────────────────────────────────────────────

/// Base trait that all providers implement.
#[async_trait]
pub trait Provider: Send + Sync {
    /// Returns metadata about this provider.
    fn info(&self) -> ProviderInfo;

    /// Test the connection to the external system.
    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError>;

    /// Gracefully close the connection and release resources.
    async fn close(&self) -> Result<(), ProviderError>;
}

/// Provider for relational databases that execute SQL.
#[async_trait]
pub trait SqlProvider: Provider {
    /// Execute a SQL query and return results.
    ///
    /// The `context` map contains template variables (logical_date, etc.)
    /// that have already been rendered into the query string.
    async fn execute(
        &self,
        query: &str,
        params: &HashMap<String, String>,
    ) -> Result<SqlResult, ProviderError>;

    /// Execute a SQL query without returning rows (DDL, DML).
    async fn execute_statement(&self, statement: &str) -> Result<SqlResult, ProviderError> {
        // Default: delegates to execute with no params
        self.execute(statement, &HashMap::new()).await
    }

    /// Get the list of schemas/databases visible to this connection.
    async fn list_schemas(&self) -> Result<Vec<String>, ProviderError>;

    /// Get column metadata for a table.
    async fn describe_table(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>, ProviderError>;
}

/// Column metadata returned by `describe_table`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<String>,
}

/// Provider for object/file storage (S3, GCS, Azure Blob, local FS).
#[async_trait]
pub trait StorageProvider: Provider {
    /// Read an object and return its contents as bytes.
    async fn read_object(&self, path: &str) -> Result<Vec<u8>, ProviderError>;

    /// Write bytes to an object path.
    async fn write_object(&self, path: &str, data: &[u8]) -> Result<StorageResult, ProviderError>;

    /// List objects matching a prefix/glob.
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, ProviderError>;

    /// Delete an object.
    async fn delete_object(&self, path: &str) -> Result<(), ProviderError>;

    /// Copy an object to a new path.
    async fn copy_object(&self, source: &str, dest: &str) -> Result<StorageResult, ProviderError>;
}

/// Provider for HTTP/REST APIs and webhooks.
#[async_trait]
pub trait HttpProvider: Provider {
    /// Send an HTTP request and return the response.
    async fn request(
        &self,
        method: &str,
        path: &str,
        headers: &HashMap<String, String>,
        body: Option<&str>,
    ) -> Result<HttpResult, ProviderError>;

    /// Convenience: send a GET request.
    async fn get(&self, path: &str) -> Result<HttpResult, ProviderError> {
        self.request("GET", path, &HashMap::new(), None).await
    }

    /// Convenience: send a POST request with a JSON body.
    async fn post(&self, path: &str, body: &str) -> Result<HttpResult, ProviderError> {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        self.request("POST", path, &headers, Some(body)).await
    }
}

/// Provider for message streaming (Kafka, Kinesis, Pub/Sub).
#[async_trait]
pub trait StreamProvider: Provider {
    /// Produce a batch of messages to a topic/stream.
    async fn produce(
        &self,
        topic: &str,
        messages: &[StreamMessage],
    ) -> Result<StreamResult, ProviderError>;

    /// Consume messages from a topic/stream.
    async fn consume(
        &self,
        topic: &str,
        group_id: &str,
        max_messages: usize,
    ) -> Result<Vec<StreamMessage>, ProviderError>;

    /// List available topics/streams.
    async fn list_topics(&self) -> Result<Vec<String>, ProviderError>;
}

/// A message in a stream/queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMessage {
    /// Optional message key.
    pub key: Option<String>,
    /// Message payload.
    pub value: Vec<u8>,
    /// Message headers/attributes.
    pub headers: HashMap<String, String>,
    /// Timestamp (if available).
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_result_empty() {
        let result = SqlResult::empty();
        assert_eq!(result.rows_affected, 0);
        assert_eq!(result.rows_returned, None);
        assert_eq!(result.execution_time_ms, 0);
        assert!(result.columns.is_empty());
        assert!(result.sample_rows.is_empty());
        assert!(result.metrics.is_empty());
    }

    #[test]
    fn test_sql_result_to_protocol_output_select() {
        let mut result = SqlResult::empty();
        result.rows_returned = Some(42);
        result.rows_affected = 0;
        result.execution_time_ms = 150;

        let output = result.to_protocol_output();
        assert!(output.contains("CONDUIT::LOG::INFO::Query completed in 150ms"));
        assert!(output.contains("CONDUIT::LOG::INFO::Rows returned: 42"));
        assert!(output.contains("CONDUIT::METRIC::row_count::42"));
        assert!(output.contains("CONDUIT::XCOM::"));
    }

    #[test]
    fn test_sql_result_to_protocol_output_dml() {
        let mut result = SqlResult::empty();
        result.rows_affected = 10;
        result.execution_time_ms = 50;
        result.metrics.insert("custom_metric".to_string(), 99.5);

        let output = result.to_protocol_output();
        assert!(output.contains("CONDUIT::LOG::INFO::Rows affected: 10"));
        assert!(output.contains("CONDUIT::METRIC::row_count::10"));
        assert!(output.contains("CONDUIT::METRIC::custom_metric::99.5"));
    }

    #[test]
    fn test_capability_variants() {
        // Ensure all capability variants can be created and compared
        let caps = vec![
            Capability::SqlQuery,
            Capability::SqlDdl,
            Capability::BulkLoad,
            Capability::IncrementalRead,
            Capability::Transactions,
            Capability::StorageRead,
            Capability::StorageWrite,
            Capability::StorageList,
            Capability::HttpRequest,
            Capability::StreamProduce,
            Capability::StreamConsume,
        ];
        assert_eq!(caps.len(), 11);
        assert_eq!(caps[0], Capability::SqlQuery);
        assert_ne!(caps[0], Capability::SqlDdl);
    }

    #[test]
    fn test_provider_info_serialization() {
        let info = ProviderInfo {
            provider_type: "postgres".to_string(),
            display_name: "PostgreSQL (localhost:5432)".to_string(),
            version: Some("15.4".to_string()),
            capabilities: vec![Capability::SqlQuery, Capability::Transactions],
            is_stub: false,
        };

        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["provider_type"], "postgres");
        assert_eq!(json["display_name"], "PostgreSQL (localhost:5432)");
        assert_eq!(json["version"], "15.4");
        assert_eq!(json["capabilities"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_connection_test_result_serialization() {
        let result = ConnectionTestResult {
            success: true,
            message: "OK".to_string(),
            latency_ms: 42,
            server_version: Some("15.4".to_string()),
        };

        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["latency_ms"], 42);
    }

    #[test]
    fn test_storage_result_serialization() {
        let result = StorageResult {
            operation: "PutObject".to_string(),
            objects_affected: 1,
            bytes_transferred: 1024,
            execution_time_ms: 50,
            uris: vec!["s3://bucket/key".to_string()],
        };

        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["operation"], "PutObject");
        assert_eq!(json["objects_affected"], 1);
        assert_eq!(json["bytes_transferred"], 1024);
    }

    #[test]
    fn test_stream_message_creation() {
        let msg = StreamMessage {
            key: Some("key1".to_string()),
            value: b"hello world".to_vec(),
            headers: HashMap::new(),
            timestamp: None,
        };
        assert_eq!(msg.key, Some("key1".to_string()));
        assert_eq!(msg.value.len(), 11);
    }

    #[test]
    fn test_http_result_serialization() {
        let result = HttpResult {
            status_code: 200,
            headers: HashMap::new(),
            body: r#"{"ok": true}"#.to_string(),
            execution_time_ms: 100,
        };

        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["status_code"], 200);
    }

    #[test]
    fn test_stream_result_serialization() {
        let result = StreamResult {
            message_count: 100,
            bytes_transferred: 50000,
            execution_time_ms: 250,
        };

        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["message_count"], 100);
        assert_eq!(json["bytes_transferred"], 50000);
    }

    #[test]
    fn test_column_info_serialization() {
        let col = ColumnInfo {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            is_nullable: false,
            is_primary_key: true,
            default_value: None,
        };

        let json = serde_json::to_value(&col).unwrap();
        assert_eq!(json["name"], "id");
        assert_eq!(json["is_primary_key"], true);
    }
}
