//! SaaS provider trait for first-party API integrations.

use crate::errors::ProviderError;
use crate::traits::Provider;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Result from a SaaS API operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaasResult {
    /// Operation performed (e.g., "query", "create", "update", "webhook").
    pub operation: String,
    /// Number of records affected.
    pub records_affected: u64,
    /// Response payload (JSON).
    pub data: serde_json::Value,
    /// Execution time in milliseconds.
    pub execution_time_ms: u64,
    /// Rate limit info.
    pub rate_limit: Option<RateLimitInfo>,
    /// Pagination cursor for next page.
    pub next_cursor: Option<String>,
}

/// Rate limit information from a SaaS API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitInfo {
    pub limit: u64,
    pub remaining: u64,
    pub reset_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Provider for SaaS platform APIs (Salesforce, Stripe, GitHub, etc.).
///
/// SaaS providers work with records/objects rather than SQL or files.
/// They handle authentication, pagination, and rate limiting automatically.
#[async_trait]
pub trait SaasProvider: Provider {
    /// Query records from the SaaS platform.
    ///
    /// `object_type` is the entity kind (e.g., "Contact", "Invoice", "Issue").
    /// `filter` contains query parameters specific to the platform.
    /// `cursor` is the pagination cursor from a previous result.
    async fn query(
        &self,
        object_type: &str,
        filter: &HashMap<String, String>,
        cursor: Option<&str>,
    ) -> Result<SaasResult, ProviderError>;

    /// Create a record in the SaaS platform.
    async fn create(
        &self,
        object_type: &str,
        data: &serde_json::Value,
    ) -> Result<SaasResult, ProviderError>;

    /// Update a record by ID.
    async fn update(
        &self,
        object_type: &str,
        record_id: &str,
        data: &serde_json::Value,
    ) -> Result<SaasResult, ProviderError>;

    /// Delete a record by ID.
    async fn delete(&self, object_type: &str, record_id: &str)
        -> Result<SaasResult, ProviderError>;

    /// List available object types / entities.
    async fn list_object_types(&self) -> Result<Vec<String>, ProviderError>;
}

/// Result from a document/key-value operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentResult {
    /// Operation performed.
    pub operation: String,
    /// Number of documents affected.
    pub documents_affected: u64,
    /// Result data (JSON).
    pub data: serde_json::Value,
    /// Execution time in milliseconds.
    pub execution_time_ms: u64,
}

/// Provider for NoSQL / document databases (MongoDB, DynamoDB, etc.).
#[async_trait]
pub trait DocumentProvider: Provider {
    /// Find documents matching a query/filter.
    async fn find(
        &self,
        collection: &str,
        filter: &serde_json::Value,
        limit: Option<u64>,
    ) -> Result<DocumentResult, ProviderError>;

    /// Insert one or more documents.
    async fn insert(
        &self,
        collection: &str,
        documents: &[serde_json::Value],
    ) -> Result<DocumentResult, ProviderError>;

    /// Update documents matching a filter.
    async fn update(
        &self,
        collection: &str,
        filter: &serde_json::Value,
        update: &serde_json::Value,
    ) -> Result<DocumentResult, ProviderError>;

    /// Delete documents matching a filter.
    async fn delete(
        &self,
        collection: &str,
        filter: &serde_json::Value,
    ) -> Result<DocumentResult, ProviderError>;

    /// List collections/tables.
    async fn list_collections(&self) -> Result<Vec<String>, ProviderError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_saas_result_serialization() {
        let result = SaasResult {
            operation: "query".to_string(),
            records_affected: 10,
            data: serde_json::json!({"items": []}),
            execution_time_ms: 150,
            rate_limit: Some(RateLimitInfo {
                limit: 1000,
                remaining: 999,
                reset_at: None,
            }),
            next_cursor: Some("page2".to_string()),
        };

        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["operation"], "query");
        assert_eq!(json["records_affected"], 10);
        assert_eq!(json["next_cursor"], "page2");
        assert!(json["rate_limit"].is_object());
    }

    #[test]
    fn test_saas_result_without_rate_limit() {
        let result = SaasResult {
            operation: "create".to_string(),
            records_affected: 1,
            data: serde_json::json!({"id": "new-123"}),
            execution_time_ms: 50,
            rate_limit: None,
            next_cursor: None,
        };

        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["operation"], "create");
        assert!(json["rate_limit"].is_null());
        assert!(json["next_cursor"].is_null());
    }

    #[test]
    fn test_rate_limit_info_serialization() {
        let info = RateLimitInfo {
            limit: 5000,
            remaining: 4999,
            reset_at: None,
        };

        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["limit"], 5000);
        assert_eq!(json["remaining"], 4999);
    }

    #[test]
    fn test_document_result_serialization() {
        let result = DocumentResult {
            operation: "find".to_string(),
            documents_affected: 5,
            data: serde_json::json!({"docs": [{"_id": 1}, {"_id": 2}]}),
            execution_time_ms: 25,
        };

        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["operation"], "find");
        assert_eq!(json["documents_affected"], 5);
        assert!(json["data"]["docs"].is_array());
    }

    #[test]
    fn test_document_result_insert() {
        let result = DocumentResult {
            operation: "insert".to_string(),
            documents_affected: 3,
            data: serde_json::json!({"inserted_ids": ["a", "b", "c"]}),
            execution_time_ms: 10,
        };

        assert_eq!(result.documents_affected, 3);
        assert_eq!(result.operation, "insert");
    }
}
