//! Comprehensive integration tests for the Conduit provider registry.
//!
//! These tests verify:
//! - Registry initialization and emptiness checks
//! - Provider creation from config for all 32 provider types
//! - Alias resolution for common provider name variants
//! - Error handling for unsupported and missing connections
//! - Connection listing and management
//! - Test connection operations
//! - Supported provider types catalog

use std::collections::HashMap;

use conduit_common::config::ConnectionConfig;
use conduit_providers::registry::{ProviderRegistry, supported_provider_types};
use conduit_providers::errors::ProviderError;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Create a basic connection config for testing.
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

/// Create a connection config with a specific database name (used for S3 bucket).
fn make_config_with_db(conn_type: &str, database: &str) -> ConnectionConfig {
    ConnectionConfig {
        conn_type: conn_type.to_string(),
        host: Some("testhost".to_string()),
        port: Some(5432),
        database: Some(database.to_string()),
        credentials: None,
        extra: HashMap::new(),
    }
}

// ── Empty Registry Tests ────────────────────────────────────────────────────

#[test]
fn test_new_registry_is_empty() {
    let registry = ProviderRegistry::new();
    assert_eq!(registry.len(), 0);
    assert!(registry.is_empty());
    assert_eq!(registry.connected_count(), 0);
}

#[test]
fn test_empty_registry_len() {
    let registry = ProviderRegistry::new();
    assert!(registry.is_empty());
    assert_eq!(registry.len(), 0);
}

// ── From_configs Tests for All Provider Types ──────────────────────────────

#[tokio::test]
async fn test_from_configs_postgres() {
    let mut conns = HashMap::new();
    conns.insert("pg_conn".to_string(), make_config("postgres"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert_eq!(registry.len(), 1);
    assert!(registry.get("pg_conn").is_some());

    let instance = registry.get("pg_conn").unwrap();
    assert!(instance.as_sql().is_some());
    assert!(instance.as_storage().is_none());
}

#[tokio::test]
async fn test_from_configs_snowflake() {
    let mut conns = HashMap::new();
    conns.insert("snow_conn".to_string(), make_config("snowflake"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("snow_conn").is_some());
    assert!(registry.get("snow_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_from_configs_clickhouse() {
    let mut conns = HashMap::new();
    conns.insert("ch_conn".to_string(), make_config("clickhouse"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("ch_conn").is_some());
    assert!(registry.get("ch_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_from_configs_redshift() {
    let mut conns = HashMap::new();
    conns.insert("redshift_conn".to_string(), make_config("redshift"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("redshift_conn").is_some());
    assert!(registry.get("redshift_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_from_configs_bigquery() {
    let mut conns = HashMap::new();
    conns.insert("bq_conn".to_string(), make_config("bigquery"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("bq_conn").is_some());
    assert!(registry.get("bq_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_from_configs_duckdb() {
    let mut conns = HashMap::new();
    conns.insert("duck_conn".to_string(), make_config("duckdb"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("duck_conn").is_some());
    assert!(registry.get("duck_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_from_configs_mysql() {
    let mut conns = HashMap::new();
    conns.insert("mysql_conn".to_string(), make_config("mysql"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("mysql_conn").is_some());
    assert!(registry.get("mysql_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_from_configs_sqlite() {
    let mut conns = HashMap::new();
    conns.insert("sqlite_conn".to_string(), make_config("sqlite"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("sqlite_conn").is_some());
    assert!(registry.get("sqlite_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_from_configs_oracle() {
    let mut conns = HashMap::new();
    conns.insert("oracle_conn".to_string(), make_config("oracle"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("oracle_conn").is_some());
    assert!(registry.get("oracle_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_from_configs_sqlserver() {
    let mut conns = HashMap::new();
    conns.insert("sqlserver_conn".to_string(), make_config("sqlserver"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("sqlserver_conn").is_some());
    assert!(registry.get("sqlserver_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_from_configs_cockroachdb() {
    let mut conns = HashMap::new();
    conns.insert("crdb_conn".to_string(), make_config("cockroachdb"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("crdb_conn").is_some());
    assert!(registry.get("crdb_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_from_configs_timescaledb() {
    let mut conns = HashMap::new();
    conns.insert("tsdb_conn".to_string(), make_config("timescaledb"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("tsdb_conn").is_some());
    assert!(registry.get("tsdb_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_from_configs_s3() {
    let mut conns = HashMap::new();
    // S3 requires database field for bucket name
    conns.insert("s3_conn".to_string(), make_config_with_db("s3", "my-bucket"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("s3_conn").is_some());
    assert!(registry.get("s3_conn").unwrap().as_storage().is_some());
}

#[tokio::test]
async fn test_from_configs_gcs() {
    let mut conns = HashMap::new();
    conns.insert("gcs_conn".to_string(), make_config("gcs"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("gcs_conn").is_some());
    assert!(registry.get("gcs_conn").unwrap().as_storage().is_some());
}

#[tokio::test]
async fn test_from_configs_http() {
    let mut conns = HashMap::new();
    conns.insert("http_conn".to_string(), make_config("http"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("http_conn").is_some());
    assert!(registry.get("http_conn").unwrap().as_http().is_some());
}

#[tokio::test]
async fn test_from_configs_kafka() {
    let mut conns = HashMap::new();
    conns.insert("kafka_conn".to_string(), make_config("kafka"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("kafka_conn").is_some());
    assert!(registry.get("kafka_conn").unwrap().as_stream().is_some());
}

#[tokio::test]
async fn test_from_configs_rabbitmq() {
    let mut conns = HashMap::new();
    conns.insert("rmq_conn".to_string(), make_config("rabbitmq"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("rmq_conn").is_some());
    assert!(registry.get("rmq_conn").unwrap().as_stream().is_some());
}

#[tokio::test]
async fn test_from_configs_kinesis() {
    let mut conns = HashMap::new();
    conns.insert("kinesis_conn".to_string(), make_config("kinesis"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("kinesis_conn").is_some());
    assert!(registry.get("kinesis_conn").unwrap().as_stream().is_some());
}

#[tokio::test]
async fn test_from_configs_pubsub() {
    let mut conns = HashMap::new();
    conns.insert("pubsub_conn".to_string(), make_config("pubsub"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("pubsub_conn").is_some());
    assert!(registry.get("pubsub_conn").unwrap().as_stream().is_some());
}

#[tokio::test]
async fn test_from_configs_redis_stream() {
    let mut conns = HashMap::new();
    conns.insert("redis_stream_conn".to_string(), make_config("redis"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("redis_stream_conn").is_some());
    assert!(registry.get("redis_stream_conn").unwrap().as_stream().is_some());
}

#[tokio::test]
async fn test_from_configs_salesforce() {
    let mut conns = HashMap::new();
    conns.insert("sfdc_conn".to_string(), make_config("salesforce"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("sfdc_conn").is_some());
    assert!(registry.get("sfdc_conn").unwrap().as_saas().is_some());
}

#[tokio::test]
async fn test_from_configs_hubspot() {
    let mut conns = HashMap::new();
    conns.insert("hs_conn".to_string(), make_config("hubspot"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("hs_conn").is_some());
    assert!(registry.get("hs_conn").unwrap().as_saas().is_some());
}

#[tokio::test]
async fn test_from_configs_stripe() {
    let mut conns = HashMap::new();
    conns.insert("stripe_conn".to_string(), make_config("stripe"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("stripe_conn").is_some());
    assert!(registry.get("stripe_conn").unwrap().as_saas().is_some());
}

#[tokio::test]
async fn test_from_configs_github() {
    let mut conns = HashMap::new();
    conns.insert("github_conn".to_string(), make_config("github"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("github_conn").is_some());
    assert!(registry.get("github_conn").unwrap().as_saas().is_some());
}

#[tokio::test]
async fn test_from_configs_jira() {
    let mut conns = HashMap::new();
    conns.insert("jira_conn".to_string(), make_config("jira"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("jira_conn").is_some());
    assert!(registry.get("jira_conn").unwrap().as_saas().is_some());
}

#[tokio::test]
async fn test_from_configs_slack() {
    let mut conns = HashMap::new();
    conns.insert("slack_conn".to_string(), make_config("slack"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("slack_conn").is_some());
    assert!(registry.get("slack_conn").unwrap().as_saas().is_some());
}

#[tokio::test]
async fn test_from_configs_mongodb() {
    let mut conns = HashMap::new();
    conns.insert("mongo_conn".to_string(), make_config("mongodb"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("mongo_conn").is_some());
    assert!(registry.get("mongo_conn").unwrap().as_document().is_some());
}

#[tokio::test]
async fn test_from_configs_dynamodb() {
    let mut conns = HashMap::new();
    conns.insert("ddb_conn".to_string(), make_config("dynamodb"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("ddb_conn").is_some());
    assert!(registry.get("ddb_conn").unwrap().as_document().is_some());
}

#[tokio::test]
async fn test_from_configs_cassandra() {
    let mut conns = HashMap::new();
    conns.insert("cass_conn".to_string(), make_config("cassandra"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("cass_conn").is_some());
    assert!(registry.get("cass_conn").unwrap().as_document().is_some());
}

#[tokio::test]
async fn test_from_configs_elasticsearch() {
    let mut conns = HashMap::new();
    conns.insert("es_conn".to_string(), make_config("elasticsearch"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("es_conn").is_some());
    assert!(registry.get("es_conn").unwrap().as_document().is_some());
}

#[tokio::test]
async fn test_from_configs_redis_kv() {
    let mut conns = HashMap::new();
    conns.insert("redis_kv_conn".to_string(), make_config("redis_kv"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("redis_kv_conn").is_some());
    assert!(registry.get("redis_kv_conn").unwrap().as_document().is_some());
}

#[tokio::test]
async fn test_from_configs_neo4j() {
    let mut conns = HashMap::new();
    conns.insert("neo4j_conn".to_string(), make_config("neo4j"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("neo4j_conn").is_some());
    assert!(registry.get("neo4j_conn").unwrap().as_document().is_some());
}

// ── Alias Tests ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_alias_postgresql() {
    let mut conns = HashMap::new();
    conns.insert("pg_alias".to_string(), make_config("postgresql"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("pg_alias").is_some());
    assert!(registry.get("pg_alias").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_alias_pg() {
    let mut conns = HashMap::new();
    conns.insert("pg_short".to_string(), make_config("pg"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("pg_short").is_some());
    assert!(registry.get("pg_short").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_alias_mariadb() {
    let mut conns = HashMap::new();
    conns.insert("maria_conn".to_string(), make_config("mariadb"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("maria_conn").is_some());
    assert!(registry.get("maria_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_alias_mssql() {
    let mut conns = HashMap::new();
    conns.insert("mssql_conn".to_string(), make_config("mssql"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("mssql_conn").is_some());
    assert!(registry.get("mssql_conn").unwrap().as_sql().is_some());
}

#[tokio::test]
async fn test_alias_sfdc() {
    let mut conns = HashMap::new();
    conns.insert("sfdc_alias".to_string(), make_config("sfdc"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("sfdc_alias").is_some());
    assert!(registry.get("sfdc_alias").unwrap().as_saas().is_some());
}

#[tokio::test]
async fn test_alias_gh() {
    let mut conns = HashMap::new();
    conns.insert("gh_alias".to_string(), make_config("gh"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("gh_alias").is_some());
    assert!(registry.get("gh_alias").unwrap().as_saas().is_some());
}

#[tokio::test]
async fn test_alias_mongo() {
    let mut conns = HashMap::new();
    conns.insert("mongo_alias".to_string(), make_config("mongo"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("mongo_alias").is_some());
    assert!(registry.get("mongo_alias").unwrap().as_document().is_some());
}

#[tokio::test]
async fn test_alias_amqp() {
    let mut conns = HashMap::new();
    conns.insert("amqp_alias".to_string(), make_config("amqp"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    assert!(registry.get("amqp_alias").is_some());
    assert!(registry.get("amqp_alias").unwrap().as_stream().is_some());
}

// ── Error Handling Tests ────────────────────────────────────────────────────

#[tokio::test]
async fn test_unsupported_provider_type() {
    let mut conns = HashMap::new();
    conns.insert("bad_conn".to_string(), make_config("unsupported_db_type"));

    // Registry should still be created with the config but no provider
    let registry = ProviderRegistry::from_configs(&conns).await;
    assert_eq!(registry.len(), 1);
    assert_eq!(registry.connected_count(), 0);
    // Config should still be present
    assert!(registry.get_config("bad_conn").is_some());
    // But the provider should not be initialized
    assert!(registry.get("bad_conn").is_none());
}

#[test]
fn test_get_nonexistent_connection() {
    let registry = ProviderRegistry::new();
    assert!(registry.get("nonexistent").is_none());
}

#[tokio::test]
async fn test_get_sql_wrong_type() {
    let mut conns = HashMap::new();
    conns.insert("storage_conn".to_string(), make_config_with_db("s3", "bucket"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    let result = registry.get_sql("storage_conn");

    assert!(result.is_err());
    match result {
        Err(ProviderError::InvalidConfig { reason, .. }) => {
            assert!(reason.contains("not a SQL provider"));
        }
        _ => panic!("Expected InvalidConfig error"),
    }
}

// ── Connection Listing Tests ────────────────────────────────────────────────

#[tokio::test]
async fn test_list_connections_returns_all() {
    let mut conns = HashMap::new();
    conns.insert("conn1".to_string(), make_config("postgres"));
    conns.insert("conn2".to_string(), make_config("mysql"));
    conns.insert("conn3".to_string(), make_config_with_db("s3", "bucket"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    let summaries = registry.list_connections();

    assert_eq!(summaries.len(), 3);
    assert!(summaries.iter().any(|s| s.name == "conn1"));
    assert!(summaries.iter().any(|s| s.name == "conn2"));
    assert!(summaries.iter().any(|s| s.name == "conn3"));
}

#[tokio::test]
async fn test_connection_names_sorted() {
    let mut conns = HashMap::new();
    conns.insert("zebra".to_string(), make_config("postgres"));
    conns.insert("alpha".to_string(), make_config("mysql"));
    conns.insert("beta".to_string(), make_config("postgres"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    let names = registry.connection_names();

    assert_eq!(names, vec!["alpha", "beta", "zebra"]);
}

// ── Test Connection Tests ───────────────────────────────────────────────────

#[tokio::test]
async fn test_test_connection_not_found() {
    let registry = ProviderRegistry::new();
    let result = registry.test_connection("nonexistent").await;

    assert!(result.is_err());
    match result {
        Err(ProviderError::ConnectionNotFound { name }) => {
            assert_eq!(name, "nonexistent");
        }
        _ => panic!("Expected ConnectionNotFound error"),
    }
}

#[tokio::test]
async fn test_test_all_connections() {
    let mut conns = HashMap::new();
    conns.insert("conn1".to_string(), make_config("postgres"));
    conns.insert("conn2".to_string(), make_config("mysql"));

    let registry = ProviderRegistry::from_configs(&conns).await;
    let results = registry.test_all().await;

    // Should have results for all initialized providers
    assert_eq!(results.len(), 2);
    assert!(results.contains_key("conn1"));
    assert!(results.contains_key("conn2"));
}

// ── Supported Provider Types Tests ──────────────────────────────────────────

#[test]
fn test_supported_types_count() {
    let types = supported_provider_types();
    // 12 SQL + 2 Storage + 1 HTTP + 5 Stream + 6 SaaS + 6 Document = 32
    assert_eq!(types.len(), 32);
}

#[test]
fn test_supported_types_categories() {
    let types = supported_provider_types();

    let categories: std::collections::HashSet<_> =
        types.iter().map(|(_, _, _, cat)| *cat).collect();

    assert!(categories.contains("sql"));
    assert!(categories.contains("storage"));
    assert!(categories.contains("http"));
    assert!(categories.contains("stream"));
    assert!(categories.contains("saas"));
    assert!(categories.contains("document"));
}

#[test]
fn test_supported_types_no_duplicate_ids() {
    let types = supported_provider_types();
    let ids: Vec<_> = types.iter().map(|(id, _, _, _)| id).collect();

    // Check that all IDs are unique
    let mut seen = std::collections::HashSet::new();
    for id in ids {
        assert!(
            seen.insert(id),
            "Duplicate provider type ID: {}",
            id
        );
    }
}

#[test]
fn test_supported_types_sql_count() {
    let types = supported_provider_types();
    let sql_count = types.iter().filter(|(_, _, _, cat)| *cat == "sql").count();
    assert_eq!(sql_count, 12);
}

#[test]
fn test_supported_types_storage_count() {
    let types = supported_provider_types();
    let storage_count = types
        .iter()
        .filter(|(_, _, _, cat)| *cat == "storage")
        .count();
    assert_eq!(storage_count, 2);
}

#[test]
fn test_supported_types_stream_count() {
    let types = supported_provider_types();
    let stream_count = types
        .iter()
        .filter(|(_, _, _, cat)| *cat == "stream")
        .count();
    assert_eq!(stream_count, 5);
}

#[test]
fn test_supported_types_saas_count() {
    let types = supported_provider_types();
    let saas_count = types
        .iter()
        .filter(|(_, _, _, cat)| *cat == "saas")
        .count();
    assert_eq!(saas_count, 6);
}

#[test]
fn test_supported_types_document_count() {
    let types = supported_provider_types();
    let document_count = types
        .iter()
        .filter(|(_, _, _, cat)| *cat == "document")
        .count();
    assert_eq!(document_count, 6);
}

#[test]
fn test_supported_types_http_count() {
    let types = supported_provider_types();
    let http_count = types
        .iter()
        .filter(|(_, _, _, cat)| *cat == "http")
        .count();
    assert_eq!(http_count, 1);
}

#[test]
fn test_supported_types_all_have_display_names() {
    let types = supported_provider_types();

    for (id, display_name, _, _) in types {
        assert!(
            !display_name.is_empty(),
            "Provider {} has empty display name",
            id
        );
    }
}

#[test]
fn test_supported_types_postgres_aliases() {
    let types = supported_provider_types();
    let postgres = types
        .iter()
        .find(|(id, _, _, _)| *id == "postgres")
        .expect("postgres not found");

    let aliases = postgres.2;
    assert!(aliases.contains(&"postgresql"));
    assert!(aliases.contains(&"pg"));
}

#[test]
fn test_supported_types_mysql_aliases() {
    let types = supported_provider_types();
    let mysql = types
        .iter()
        .find(|(id, _, _, _)| *id == "mysql")
        .expect("mysql not found");

    let aliases = mysql.2;
    assert!(aliases.contains(&"mariadb"));
}

#[test]
fn test_supported_types_snowflake_aliases() {
    let types = supported_provider_types();
    let snowflake = types
        .iter()
        .find(|(id, _, _, _)| *id == "snowflake")
        .expect("snowflake not found");

    let aliases = snowflake.2;
    assert!(aliases.contains(&"sf"));
}

#[test]
fn test_supported_types_bigquery_aliases() {
    let types = supported_provider_types();
    let bq = types
        .iter()
        .find(|(id, _, _, _)| *id == "bigquery")
        .expect("bigquery not found");

    let aliases = bq.2;
    assert!(aliases.contains(&"bq"));
}

#[test]
fn test_supported_types_github_aliases() {
    let types = supported_provider_types();
    let github = types
        .iter()
        .find(|(id, _, _, _)| *id == "github")
        .expect("github not found");

    let aliases = github.2;
    assert!(aliases.contains(&"gh"));
}

#[test]
fn test_supported_types_salesforce_aliases() {
    let types = supported_provider_types();
    let sf = types
        .iter()
        .find(|(id, _, _, _)| *id == "salesforce")
        .expect("salesforce not found");

    let aliases = sf.2;
    assert!(aliases.contains(&"sfdc"));
}

#[test]
fn test_supported_types_mongodb_aliases() {
    let types = supported_provider_types();
    let mongo = types
        .iter()
        .find(|(id, _, _, _)| *id == "mongodb")
        .expect("mongodb not found");

    let aliases = mongo.2;
    assert!(aliases.contains(&"mongo"));
}

#[test]
fn test_supported_types_rabbitmq_aliases() {
    let types = supported_provider_types();
    let rabbit = types
        .iter()
        .find(|(id, _, _, _)| *id == "rabbitmq")
        .expect("rabbitmq not found");

    let aliases = rabbit.2;
    assert!(aliases.contains(&"amqp"));
}

// ── Integration Tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_mixed_provider_types() {
    let mut conns = HashMap::new();
    conns.insert("pg".to_string(), make_config("postgres"));
    conns.insert("s3".to_string(), make_config_with_db("s3", "bucket"));
    conns.insert("kafka".to_string(), make_config("kafka"));
    conns.insert("salesforce".to_string(), make_config("salesforce"));
    conns.insert("mongo".to_string(), make_config("mongodb"));

    let registry = ProviderRegistry::from_configs(&conns).await;

    assert_eq!(registry.len(), 5);
    assert_eq!(registry.connected_count(), 5);

    // Verify each type
    assert!(registry.get_sql("pg").is_ok());
    assert!(registry.get_storage("s3").is_ok());
    assert!(registry.get("kafka").unwrap().as_stream().is_some());
    assert!(registry.get("salesforce").unwrap().as_saas().is_some());
    assert!(registry.get("mongo").unwrap().as_document().is_some());
}

#[tokio::test]
async fn test_get_config_for_registered_connection() {
    let mut conns = HashMap::new();
    let config = make_config("postgres");
    conns.insert("my_pg".to_string(), config.clone());

    let registry = ProviderRegistry::from_configs(&conns).await;
    let retrieved = registry.get_config("my_pg");

    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().conn_type, "postgres");
    assert_eq!(retrieved.unwrap().database, Some("testdb".to_string()));
}

#[test]
fn test_get_config_for_nonexistent_connection() {
    let registry = ProviderRegistry::new();
    assert!(registry.get_config("nonexistent").is_none());
}
