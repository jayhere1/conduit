//! Comprehensive integration tests for all 12 SQL providers in Conduit.
//!
//! This test module verifies the public API of the conduit-providers crate
//! for all SQL database providers. Each provider is tested for:
//! - Configuration initialization (from_config)
//! - Provider metadata and capabilities
//! - Connection testing
//! - Query execution (SELECT and INSERT)
//! - Graceful shutdown (close)
//!
//! Tests that require live database connections are marked `#[ignore]`.
//! Run them with: `cargo test --package conduit-providers --test sql_providers_test -- --ignored`
//!
//! Stub providers (snowflake, bigquery, clickhouse, duckdb, oracle, sqlserver) are
//! expected to return `NotImplemented` errors from `test_connection()`, `execute()`,
//! and `describe_table()`.

use std::collections::HashMap;

use conduit_common::config::ConnectionConfig;
use conduit_providers::providers::*;
use conduit_providers::traits::*;

// ─── Helper Functions ───────────────────────────────────────────────────────

/// Create a minimal ConnectionConfig for testing a given provider type.
/// Uses dummy values — only for non-ignored tests that don't connect.
#[allow(dead_code)]
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
#[allow(dead_code)]
fn make_config_with_extras(
    conn_type: &str,
    extras: Vec<(&str, serde_json::Value)>,
) -> ConnectionConfig {
    let mut extra = HashMap::new();
    for (k, v) in extras {
        extra.insert(k.to_string(), v);
    }
    ConnectionConfig {
        conn_type: conn_type.to_string(),
        host: Some("testhost".to_string()),
        port: None,
        database: Some("testdb".to_string()),
        credentials: None,
        extra,
    }
}

// ─── Live Database Configs (for #[ignore] integration tests) ────────────────

/// Postgres config from env vars (defaults match docker-compose.integration.yml).
fn make_live_pg_config() -> ConnectionConfig {
    let mut extra = HashMap::new();
    let user = std::env::var("CONDUIT_TEST_PG_USER").unwrap_or_else(|_| "conduit".to_string());
    extra.insert("user".to_string(), serde_json::json!(user));
    extra.insert("sslmode".to_string(), serde_json::json!("disable"));

    ConnectionConfig {
        conn_type: "postgres".to_string(),
        host: Some(std::env::var("CONDUIT_TEST_PG_HOST").unwrap_or_else(|_| "localhost".to_string())),
        port: Some(
            std::env::var("CONDUIT_TEST_PG_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(5432),
        ),
        database: Some(std::env::var("CONDUIT_TEST_PG_DB").unwrap_or_else(|_| "testdb".to_string())),
        credentials: Some(
            std::env::var("CONDUIT_TEST_PG_PASSWORD").unwrap_or_else(|_| "conduit_test".to_string()),
        ),
        extra,
    }
}

/// MySQL config from env vars (defaults match docker-compose.integration.yml).
fn make_live_mysql_config() -> ConnectionConfig {
    let mut extra = HashMap::new();
    let user = std::env::var("CONDUIT_TEST_MYSQL_USER").unwrap_or_else(|_| "conduit".to_string());
    extra.insert("user".to_string(), serde_json::json!(user));

    ConnectionConfig {
        conn_type: "mysql".to_string(),
        host: Some(std::env::var("CONDUIT_TEST_MYSQL_HOST").unwrap_or_else(|_| "localhost".to_string())),
        port: Some(
            std::env::var("CONDUIT_TEST_MYSQL_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(3306),
        ),
        database: Some(
            std::env::var("CONDUIT_TEST_MYSQL_DB").unwrap_or_else(|_| "testdb".to_string()),
        ),
        credentials: Some(
            std::env::var("CONDUIT_TEST_MYSQL_PASSWORD").unwrap_or_else(|_| "conduit_test".to_string()),
        ),
        extra,
    }
}

/// SQLite in-memory config — no Docker needed.
fn make_live_sqlite_config() -> ConnectionConfig {
    ConnectionConfig {
        conn_type: "sqlite".to_string(),
        host: None,
        port: None,
        database: Some(":memory:".to_string()),
        credentials: None,
        extra: HashMap::new(),
    }
}

/// Postgres-wire-compatible config for CockroachDB/Redshift/TimescaleDB tests.
/// Uses the Docker Postgres since these share the PG wire protocol.
fn make_live_pgwire_config(conn_type: &str) -> ConnectionConfig {
    let mut config = make_live_pg_config();
    config.conn_type = conn_type.to_string();
    config
}

// ─── 1. PostgreSQL Provider Tests ──────────────────────────────────────────

#[tokio::test]
async fn test_postgres_from_config_defaults() {
    let config = make_config("postgres");
    let provider = postgres::PostgresProvider::from_config("test_pg", &config);
    assert!(
        provider.is_ok(),
        "PostgreSQL provider should initialize from config"
    );
}

#[tokio::test]
async fn test_postgres_provider_info() {
    let config = make_config("postgres");
    let provider = postgres::PostgresProvider::from_config("test_pg", &config)
        .expect("Failed to create PostgreSQL provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "postgres");
    assert!(!info.display_name.is_empty());
    assert!(info.capabilities.contains(&Capability::SqlQuery));
}

#[tokio::test]
#[ignore]
async fn test_postgres_test_connection() {
    let config = make_live_pg_config();
    let provider = postgres::PostgresProvider::from_config("test_pg", &config)
        .expect("Failed to create PostgreSQL provider");

    let result = provider.test_connection().await;
    assert!(result.is_ok());
    let test_result = result.unwrap();
    assert!(test_result.success);
}

#[tokio::test]
#[ignore]
async fn test_postgres_execute_select() {
    let config = make_live_pg_config();
    let provider = postgres::PostgresProvider::from_config("test_pg", &config)
        .expect("Failed to create PostgreSQL provider");

    let params = HashMap::new();
    let result = provider.execute("SELECT 1 AS val", &params).await;
    assert!(result.is_ok(), "Postgres SELECT failed: {:?}", result.err());
    let sql_result = result.unwrap();
    assert!(sql_result.rows_returned.is_some());
}

#[tokio::test]
#[ignore]
async fn test_postgres_execute_insert() {
    let config = make_live_pg_config();
    let provider = postgres::PostgresProvider::from_config("test_pg", &config)
        .expect("Failed to create PostgreSQL provider");

    let params = HashMap::new();
    provider
        .execute(
            "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)",
            &params,
        )
        .await
        .expect("Failed to create test table");
    let result = provider
        .execute(
            "INSERT INTO test_table VALUES (1, 'hello') ON CONFLICT DO NOTHING",
            &params,
        )
        .await;
    assert!(result.is_ok(), "Postgres INSERT failed: {:?}", result.err());
}

#[tokio::test]
async fn test_postgres_close() {
    let config = make_config("postgres");
    let provider = postgres::PostgresProvider::from_config("test_pg", &config)
        .expect("Failed to create PostgreSQL provider");

    let result = provider.close().await;
    assert!(result.is_ok());
}

// ─── 2. Snowflake Provider Tests ───────────────────────────────────────────

#[tokio::test]
async fn test_snowflake_from_config_defaults() {
    let config = make_config("snowflake");
    let provider = snowflake::SnowflakeProvider::from_config("test_snowflake", &config);
    assert!(
        provider.is_ok(),
        "Snowflake provider should initialize from config"
    );
}

#[tokio::test]
async fn test_snowflake_provider_info() {
    let config = make_config("snowflake");
    let provider = snowflake::SnowflakeProvider::from_config("test_snowflake", &config)
        .expect("Failed to create Snowflake provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "snowflake");
    assert!(!info.display_name.is_empty());
    assert!(info.capabilities.contains(&Capability::SqlQuery));
}

#[tokio::test]
async fn test_snowflake_test_connection() {
    let config = make_config("snowflake");
    let provider = snowflake::SnowflakeProvider::from_config("test_snowflake", &config)
        .expect("Failed to create Snowflake provider");

    let result = provider.test_connection().await;
    assert!(
        result.is_ok(),
        "Snowflake test_connection should succeed (returns Ok with success=false for unreachable hosts)"
    );
}

#[tokio::test]
async fn test_snowflake_execute_select() {
    let config = make_config("snowflake");
    let provider = snowflake::SnowflakeProvider::from_config("test_snowflake", &config)
        .expect("Failed to create Snowflake provider");

    let params = HashMap::new();
    let result = provider.execute("SELECT 1", &params).await;
    assert!(
        result.is_err(),
        "Snowflake execute without credentials should fail"
    );
}

#[tokio::test]
async fn test_snowflake_execute_insert() {
    let config = make_config("snowflake");
    let provider = snowflake::SnowflakeProvider::from_config("test_snowflake", &config)
        .expect("Failed to create Snowflake provider");

    let params = HashMap::new();
    let result = provider
        .execute("INSERT INTO test_table VALUES (1)", &params)
        .await;
    assert!(
        result.is_err(),
        "Snowflake execute without credentials should fail"
    );
}

#[tokio::test]
async fn test_snowflake_close() {
    let config = make_config("snowflake");
    let provider = snowflake::SnowflakeProvider::from_config("test_snowflake", &config)
        .expect("Failed to create Snowflake provider");

    let result = provider.close().await;
    assert!(result.is_ok());
}

// ─── 3. ClickHouse Provider Tests ──────────────────────────────────────────

#[tokio::test]
async fn test_clickhouse_from_config_defaults() {
    let config = make_config("clickhouse");
    let provider = clickhouse::ClickHouseProvider::from_config("test_clickhouse", &config);
    assert!(
        provider.is_ok(),
        "ClickHouse provider should initialize from config"
    );
}

#[tokio::test]
async fn test_clickhouse_provider_info() {
    let config = make_config("clickhouse");
    let provider = clickhouse::ClickHouseProvider::from_config("test_clickhouse", &config)
        .expect("Failed to create ClickHouse provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "clickhouse");
    assert!(!info.display_name.is_empty());
    assert!(info.capabilities.contains(&Capability::SqlQuery));
}

#[tokio::test]
async fn test_clickhouse_test_connection() {
    let config = make_config("clickhouse");
    let provider = clickhouse::ClickHouseProvider::from_config("test_clickhouse", &config)
        .expect("Failed to create ClickHouse provider");

    let result = provider.test_connection().await;
    assert!(
        result.is_ok(),
        "ClickHouse test_connection should succeed (returns Ok with success=false for unreachable hosts)"
    );
}

#[tokio::test]
async fn test_clickhouse_execute_select() {
    let config = make_config("clickhouse");
    let provider = clickhouse::ClickHouseProvider::from_config("test_clickhouse", &config)
        .expect("Failed to create ClickHouse provider");

    let params = HashMap::new();
    let result = provider.execute("SELECT 1", &params).await;
    assert!(
        result.is_err(),
        "ClickHouse execute should return NotImplemented"
    );
}

#[tokio::test]
async fn test_clickhouse_execute_insert() {
    let config = make_config("clickhouse");
    let provider = clickhouse::ClickHouseProvider::from_config("test_clickhouse", &config)
        .expect("Failed to create ClickHouse provider");

    let params = HashMap::new();
    let result = provider
        .execute("INSERT INTO test_table VALUES (1)", &params)
        .await;
    assert!(
        result.is_err(),
        "ClickHouse execute should return NotImplemented"
    );
}

#[tokio::test]
async fn test_clickhouse_close() {
    let config = make_config("clickhouse");
    let provider = clickhouse::ClickHouseProvider::from_config("test_clickhouse", &config)
        .expect("Failed to create ClickHouse provider");

    let result = provider.close().await;
    assert!(result.is_ok());
}

// ─── 4. Redshift Provider Tests ────────────────────────────────────────────

#[tokio::test]
async fn test_redshift_from_config_defaults() {
    let config = make_config("redshift");
    let provider = redshift::RedshiftProvider::from_config("test_redshift", &config);
    assert!(
        provider.is_ok(),
        "Redshift provider should initialize from config"
    );
}

#[tokio::test]
async fn test_redshift_provider_info() {
    let config = make_config("redshift");
    let provider = redshift::RedshiftProvider::from_config("test_redshift", &config)
        .expect("Failed to create Redshift provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "redshift");
    assert!(!info.display_name.is_empty());
    assert!(info.capabilities.contains(&Capability::SqlQuery));
}

#[tokio::test]
#[ignore]
async fn test_redshift_test_connection() {
    let config = make_live_pgwire_config("redshift");
    let provider = redshift::RedshiftProvider::from_config("test_redshift", &config)
        .expect("Failed to create Redshift provider");

    let result = provider.test_connection().await;
    assert!(result.is_ok());
    let test_result = result.unwrap();
    assert!(test_result.success);
}

#[tokio::test]
#[ignore]
async fn test_redshift_execute_select() {
    let config = make_live_pgwire_config("redshift");
    let provider = redshift::RedshiftProvider::from_config("test_redshift", &config)
        .expect("Failed to create Redshift provider");

    let params = HashMap::new();
    let result = provider.execute("SELECT 1 AS val", &params).await;
    assert!(result.is_ok(), "Redshift SELECT failed: {:?}", result.err());
    let sql_result = result.unwrap();
    assert!(sql_result.rows_returned.is_some());
}

#[tokio::test]
#[ignore]
async fn test_redshift_execute_insert() {
    let config = make_live_pgwire_config("redshift");
    let provider = redshift::RedshiftProvider::from_config("test_redshift", &config)
        .expect("Failed to create Redshift provider");

    let params = HashMap::new();
    provider
        .execute(
            "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)",
            &params,
        )
        .await
        .expect("Failed to create test table");
    let result = provider
        .execute(
            "INSERT INTO test_table VALUES (1, 'hello') ON CONFLICT DO NOTHING",
            &params,
        )
        .await;
    assert!(
        result.is_ok(),
        "Redshift INSERT failed: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_redshift_close() {
    let config = make_config("redshift");
    let provider = redshift::RedshiftProvider::from_config("test_redshift", &config)
        .expect("Failed to create Redshift provider");

    let result = provider.close().await;
    assert!(result.is_ok());
}

// ─── 5. BigQuery Provider Tests ────────────────────────────────────────────

#[tokio::test]
async fn test_bigquery_from_config_defaults() {
    let config = make_config("bigquery");
    let provider = bigquery::BigQueryProvider::from_config("test_bigquery", &config);
    assert!(
        provider.is_ok(),
        "BigQuery provider should initialize from config"
    );
}

#[tokio::test]
async fn test_bigquery_provider_info() {
    let config = make_config("bigquery");
    let provider = bigquery::BigQueryProvider::from_config("test_bigquery", &config)
        .expect("Failed to create BigQuery provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "bigquery");
    assert!(!info.display_name.is_empty());
    assert!(info.capabilities.contains(&Capability::SqlQuery));
}

#[tokio::test]
async fn test_bigquery_test_connection() {
    let config = make_config("bigquery");
    let provider = bigquery::BigQueryProvider::from_config("test_bigquery", &config)
        .expect("Failed to create BigQuery provider");

    let result = provider.test_connection().await;
    assert!(
        result.is_ok(),
        "BigQuery test_connection should succeed (returns Ok with success=false for unreachable hosts)"
    );
}

#[tokio::test]
async fn test_bigquery_execute_select() {
    let config = make_config("bigquery");
    let provider = bigquery::BigQueryProvider::from_config("test_bigquery", &config)
        .expect("Failed to create BigQuery provider");

    let params = HashMap::new();
    let result = provider.execute("SELECT 1", &params).await;
    assert!(
        result.is_err(),
        "BigQuery execute without credentials should fail"
    );
}

#[tokio::test]
async fn test_bigquery_execute_insert() {
    let config = make_config("bigquery");
    let provider = bigquery::BigQueryProvider::from_config("test_bigquery", &config)
        .expect("Failed to create BigQuery provider");

    let params = HashMap::new();
    let result = provider
        .execute("INSERT INTO test_table VALUES (1)", &params)
        .await;
    assert!(
        result.is_err(),
        "BigQuery execute without credentials should fail"
    );
}

#[tokio::test]
async fn test_bigquery_close() {
    let config = make_config("bigquery");
    let provider = bigquery::BigQueryProvider::from_config("test_bigquery", &config)
        .expect("Failed to create BigQuery provider");

    let result = provider.close().await;
    assert!(result.is_ok());
}

// ─── 6. DuckDB Provider Tests ──────────────────────────────────────────────

#[tokio::test]
async fn test_duckdb_from_config_defaults() {
    let config = make_config("duckdb");
    let provider = duckdb::DuckDbProvider::from_config("test_duckdb", &config);
    assert!(
        provider.is_ok(),
        "DuckDB provider should initialize from config"
    );
}

#[tokio::test]
async fn test_duckdb_provider_info() {
    let config = make_config("duckdb");
    let provider = duckdb::DuckDbProvider::from_config("test_duckdb", &config)
        .expect("Failed to create DuckDB provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "duckdb");
    assert!(!info.display_name.is_empty());
    assert!(info.capabilities.contains(&Capability::SqlQuery));
}

#[tokio::test]
async fn test_duckdb_test_connection() {
    let provider = duckdb::DuckDbProvider::ephemeral();

    let result = provider.test_connection().await;
    assert!(result.is_ok(), "DuckDB test_connection should succeed");
    let info = result.unwrap();
    assert!(info.success);
    assert!(info.server_version.is_some());
}

#[tokio::test]
async fn test_duckdb_execute_select() {
    let provider = duckdb::DuckDbProvider::ephemeral();

    let params = HashMap::new();
    let result = provider
        .execute("SELECT 1 AS n, 'hello' AS greeting", &params)
        .await;
    assert!(result.is_ok(), "DuckDB execute should succeed");
    let sql_result = result.unwrap();
    assert_eq!(sql_result.rows_returned, Some(1));
    assert_eq!(sql_result.columns, vec!["n", "greeting"]);
    assert_eq!(sql_result.sample_rows.len(), 1);
    assert_eq!(sql_result.sample_rows[0][0], serde_json::json!(1));
    assert_eq!(sql_result.sample_rows[0][1], serde_json::json!("hello"));
}

#[tokio::test]
async fn test_duckdb_execute_ddl_and_dml() {
    let provider = duckdb::DuckDbProvider::ephemeral();
    let params = HashMap::new();

    // Create table
    let result = provider
        .execute("CREATE TABLE test_t (id INTEGER, name TEXT)", &params)
        .await;
    assert!(result.is_ok(), "CREATE TABLE should succeed");

    // Insert data
    let result = provider
        .execute(
            "INSERT INTO test_t VALUES (1, 'Alice'), (2, 'Bob')",
            &params,
        )
        .await;
    assert!(result.is_ok(), "INSERT should succeed");

    // Query back
    let result = provider
        .execute("SELECT * FROM test_t ORDER BY id", &params)
        .await;
    assert!(result.is_ok());
    let sql_result = result.unwrap();
    assert_eq!(sql_result.rows_returned, Some(2));
    assert_eq!(sql_result.columns, vec!["id", "name"]);
    assert_eq!(sql_result.sample_rows[0][1], serde_json::json!("Alice"));
    assert_eq!(sql_result.sample_rows[1][1], serde_json::json!("Bob"));
}

#[tokio::test]
async fn test_duckdb_list_schemas() {
    let provider = duckdb::DuckDbProvider::ephemeral();

    let schemas = provider.list_schemas().await;
    assert!(schemas.is_ok());
    let schemas = schemas.unwrap();
    assert!(schemas.contains(&"main".to_string()));
    assert!(schemas.contains(&"information_schema".to_string()));
}

#[tokio::test]
async fn test_duckdb_describe_table() {
    let provider = duckdb::DuckDbProvider::ephemeral();
    let params = HashMap::new();

    provider
        .execute(
            "CREATE TABLE test_desc (id INTEGER NOT NULL, name TEXT, score DOUBLE)",
            &params,
        )
        .await
        .expect("CREATE should succeed");

    let columns = provider.describe_table("main", "test_desc").await;
    assert!(columns.is_ok());
    let columns = columns.unwrap();
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].name, "id");
    assert_eq!(columns[1].name, "name");
    assert_eq!(columns[2].name, "score");
}

#[tokio::test]
async fn test_duckdb_close() {
    let provider = duckdb::DuckDbProvider::ephemeral();

    let result = provider.close().await;
    assert!(result.is_ok());
}

// ─── 7. MySQL Provider Tests ───────────────────────────────────────────────

#[tokio::test]
async fn test_mysql_from_config_defaults() {
    let config = make_config("mysql");
    let provider = mysql::MySqlProvider::from_config("test_mysql", &config);
    assert!(
        provider.is_ok(),
        "MySQL provider should initialize from config"
    );
}

#[tokio::test]
async fn test_mysql_provider_info() {
    let config = make_config("mysql");
    let provider = mysql::MySqlProvider::from_config("test_mysql", &config)
        .expect("Failed to create MySQL provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "mysql");
    assert!(!info.display_name.is_empty());
    assert!(info.capabilities.contains(&Capability::SqlQuery));
}

#[tokio::test]
#[ignore]
async fn test_mysql_test_connection() {
    let config = make_live_mysql_config();
    let provider = mysql::MySqlProvider::from_config("test_mysql", &config)
        .expect("Failed to create MySQL provider");

    let result = provider.test_connection().await;
    assert!(result.is_ok());
    let test_result = result.unwrap();
    assert!(test_result.success);
}

#[tokio::test]
#[ignore]
async fn test_mysql_execute_select() {
    let config = make_live_mysql_config();
    let provider = mysql::MySqlProvider::from_config("test_mysql", &config)
        .expect("Failed to create MySQL provider");

    let params = HashMap::new();
    let result = provider.execute("SELECT 1 AS val", &params).await;
    assert!(result.is_ok(), "MySQL SELECT failed: {:?}", result.err());
    let sql_result = result.unwrap();
    assert!(sql_result.rows_returned.is_some());
}

#[tokio::test]
#[ignore]
async fn test_mysql_execute_insert() {
    let config = make_live_mysql_config();
    let provider = mysql::MySqlProvider::from_config("test_mysql", &config)
        .expect("Failed to create MySQL provider");

    let params = HashMap::new();
    provider
        .execute(
            "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)",
            &params,
        )
        .await
        .expect("Failed to create test table");
    let result = provider
        .execute(
            "INSERT IGNORE INTO test_table VALUES (1, 'hello')",
            &params,
        )
        .await;
    assert!(result.is_ok(), "MySQL INSERT failed: {:?}", result.err());
}

#[tokio::test]
async fn test_mysql_close() {
    let config = make_config("mysql");
    let provider = mysql::MySqlProvider::from_config("test_mysql", &config)
        .expect("Failed to create MySQL provider");

    let result = provider.close().await;
    assert!(result.is_ok());
}

// ─── 8. SQLite Provider Tests ──────────────────────────────────────────────

#[tokio::test]
async fn test_sqlite_from_config_defaults() {
    let config = make_config("sqlite");
    let provider = sqlite::SqliteProvider::from_config("test_sqlite", &config);
    assert!(
        provider.is_ok(),
        "SQLite provider should initialize from config"
    );
}

#[tokio::test]
async fn test_sqlite_provider_info() {
    let config = make_config("sqlite");
    let provider = sqlite::SqliteProvider::from_config("test_sqlite", &config)
        .expect("Failed to create SQLite provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "sqlite");
    assert!(!info.display_name.is_empty());
    assert!(info.capabilities.contains(&Capability::SqlQuery));
}

#[tokio::test]
async fn test_sqlite_test_connection() {
    let config = ConnectionConfig {
        conn_type: "sqlite".to_string(),
        host: None,
        port: None,
        database: Some(":memory:".to_string()),
        credentials: None,
        extra: HashMap::new(),
    };
    let provider = sqlite::SqliteProvider::from_config("test_sqlite", &config)
        .expect("Failed to create SQLite provider");

    let result = provider.test_connection().await;
    assert!(result.is_ok());
    let test_result = result.unwrap();
    assert!(test_result.success);
}

#[tokio::test]
async fn test_sqlite_execute_select() {
    let config = ConnectionConfig {
        conn_type: "sqlite".to_string(),
        host: None,
        port: None,
        database: Some(":memory:".to_string()),
        credentials: None,
        extra: HashMap::new(),
    };
    let provider = sqlite::SqliteProvider::from_config("test_sqlite", &config)
        .expect("Failed to create SQLite provider");

    let params = HashMap::new();
    let result = provider.execute("SELECT 1", &params).await;
    assert!(result.is_ok());
    let sql_result = result.unwrap();
    assert!(sql_result.rows_returned.is_some());
}

#[tokio::test]
async fn test_sqlite_execute_insert() {
    let config = ConnectionConfig {
        conn_type: "sqlite".to_string(),
        host: None,
        port: None,
        database: Some(":memory:".to_string()),
        credentials: None,
        extra: HashMap::new(),
    };
    let provider = sqlite::SqliteProvider::from_config("test_sqlite", &config)
        .expect("Failed to create SQLite provider");

    let params = HashMap::new();
    provider
        .execute("CREATE TABLE test_table (id INTEGER)", &params)
        .await
        .expect("Failed to create table");
    let result = provider
        .execute("INSERT INTO test_table VALUES (1)", &params)
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_sqlite_close() {
    let config = make_config("sqlite");
    let provider = sqlite::SqliteProvider::from_config("test_sqlite", &config)
        .expect("Failed to create SQLite provider");

    let result = provider.close().await;
    assert!(result.is_ok());
}

// ─── 9. Oracle Provider Tests ──────────────────────────────────────────────

#[tokio::test]
async fn test_oracle_from_config_defaults() {
    let config = make_config("oracle");
    let provider = oracle::OracleProvider::from_config("test_oracle", &config);
    assert!(
        provider.is_ok(),
        "Oracle provider should initialize from config"
    );
}

#[tokio::test]
async fn test_oracle_provider_info() {
    let config = make_config("oracle");
    let provider = oracle::OracleProvider::from_config("test_oracle", &config)
        .expect("Failed to create Oracle provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "oracle");
    assert!(!info.display_name.is_empty());
    assert!(info.capabilities.contains(&Capability::SqlQuery));
}

#[tokio::test]
async fn test_oracle_test_connection() {
    let config = make_config("oracle");
    let provider = oracle::OracleProvider::from_config("test_oracle", &config)
        .expect("Failed to create Oracle provider");

    let result = provider.test_connection().await;
    assert!(
        result.is_ok(),
        "Oracle test_connection should succeed (returns Ok with success=false for unreachable hosts)"
    );
}

#[tokio::test]
async fn test_oracle_execute_select() {
    let config = make_config("oracle");
    let provider = oracle::OracleProvider::from_config("test_oracle", &config)
        .expect("Failed to create Oracle provider");

    let params = HashMap::new();
    let result = provider.execute("SELECT 1 FROM DUAL", &params).await;
    assert!(
        result.is_err(),
        "Oracle execute should return NotImplemented"
    );
}

#[tokio::test]
async fn test_oracle_execute_insert() {
    let config = make_config("oracle");
    let provider = oracle::OracleProvider::from_config("test_oracle", &config)
        .expect("Failed to create Oracle provider");

    let params = HashMap::new();
    let result = provider
        .execute("INSERT INTO test_table VALUES (1)", &params)
        .await;
    assert!(
        result.is_err(),
        "Oracle execute should return NotImplemented"
    );
}

#[tokio::test]
async fn test_oracle_close() {
    let config = make_config("oracle");
    let provider = oracle::OracleProvider::from_config("test_oracle", &config)
        .expect("Failed to create Oracle provider");

    let result = provider.close().await;
    assert!(result.is_ok());
}

// ─── 10. SQL Server Provider Tests ────────────────────────────────────────

#[tokio::test]
async fn test_sqlserver_from_config_defaults() {
    let config = make_config("sqlserver");
    let provider = sqlserver::SqlServerProvider::from_config("test_sqlserver", &config);
    assert!(
        provider.is_ok(),
        "SQL Server provider should initialize from config"
    );
}

#[tokio::test]
async fn test_sqlserver_provider_info() {
    let config = make_config("sqlserver");
    let provider = sqlserver::SqlServerProvider::from_config("test_sqlserver", &config)
        .expect("Failed to create SQL Server provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "sqlserver");
    assert!(!info.display_name.is_empty());
    assert!(info.capabilities.contains(&Capability::SqlQuery));
}

#[tokio::test]
async fn test_sqlserver_test_connection() {
    let config = make_config("sqlserver");
    let provider = sqlserver::SqlServerProvider::from_config("test_sqlserver", &config)
        .expect("Failed to create SQL Server provider");

    let result = provider.test_connection().await;
    assert!(
        result.is_ok(),
        "SQL Server test_connection should succeed (returns Ok with success=false for unreachable hosts)"
    );
}

#[tokio::test]
async fn test_sqlserver_execute_select() {
    let config = make_config("sqlserver");
    let provider = sqlserver::SqlServerProvider::from_config("test_sqlserver", &config)
        .expect("Failed to create SQL Server provider");

    let params = HashMap::new();
    let result = provider.execute("SELECT 1", &params).await;
    assert!(
        result.is_err(),
        "SQL Server execute should return NotImplemented"
    );
}

#[tokio::test]
async fn test_sqlserver_execute_insert() {
    let config = make_config("sqlserver");
    let provider = sqlserver::SqlServerProvider::from_config("test_sqlserver", &config)
        .expect("Failed to create SQL Server provider");

    let params = HashMap::new();
    let result = provider
        .execute("INSERT INTO test_table VALUES (1)", &params)
        .await;
    assert!(
        result.is_err(),
        "SQL Server execute should return NotImplemented"
    );
}

#[tokio::test]
async fn test_sqlserver_close() {
    let config = make_config("sqlserver");
    let provider = sqlserver::SqlServerProvider::from_config("test_sqlserver", &config)
        .expect("Failed to create SQL Server provider");

    let result = provider.close().await;
    assert!(result.is_ok());
}

// ─── 11. CockroachDB Provider Tests ────────────────────────────────────────

#[tokio::test]
async fn test_cockroachdb_from_config_defaults() {
    let config = make_config("cockroachdb");
    let provider = cockroachdb::CockroachDbProvider::from_config("test_cockroachdb", &config);
    assert!(
        provider.is_ok(),
        "CockroachDB provider should initialize from config"
    );
}

#[tokio::test]
async fn test_cockroachdb_provider_info() {
    let config = make_config("cockroachdb");
    let provider = cockroachdb::CockroachDbProvider::from_config("test_cockroachdb", &config)
        .expect("Failed to create CockroachDB provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "cockroachdb");
    assert!(!info.display_name.is_empty());
    assert!(info.capabilities.contains(&Capability::SqlQuery));
}

#[tokio::test]
#[ignore]
async fn test_cockroachdb_test_connection() {
    let config = make_live_pgwire_config("cockroachdb");
    let provider = cockroachdb::CockroachDbProvider::from_config("test_cockroachdb", &config)
        .expect("Failed to create CockroachDB provider");

    let result = provider.test_connection().await;
    assert!(result.is_ok());
    let test_result = result.unwrap();
    assert!(test_result.success);
}

#[tokio::test]
#[ignore]
async fn test_cockroachdb_execute_select() {
    let config = make_live_pgwire_config("cockroachdb");
    let provider = cockroachdb::CockroachDbProvider::from_config("test_cockroachdb", &config)
        .expect("Failed to create CockroachDB provider");

    let params = HashMap::new();
    let result = provider.execute("SELECT 1 AS val", &params).await;
    assert!(result.is_ok(), "CockroachDB SELECT failed: {:?}", result.err());
    let sql_result = result.unwrap();
    assert!(sql_result.rows_returned.is_some());
}

#[tokio::test]
#[ignore]
async fn test_cockroachdb_execute_insert() {
    let config = make_live_pgwire_config("cockroachdb");
    let provider = cockroachdb::CockroachDbProvider::from_config("test_cockroachdb", &config)
        .expect("Failed to create CockroachDB provider");

    let params = HashMap::new();
    provider
        .execute(
            "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)",
            &params,
        )
        .await
        .expect("Failed to create test table");
    let result = provider
        .execute(
            "INSERT INTO test_table VALUES (1, 'hello') ON CONFLICT DO NOTHING",
            &params,
        )
        .await;
    assert!(
        result.is_ok(),
        "CockroachDB INSERT failed: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_cockroachdb_close() {
    let config = make_config("cockroachdb");
    let provider = cockroachdb::CockroachDbProvider::from_config("test_cockroachdb", &config)
        .expect("Failed to create CockroachDB provider");

    let result = provider.close().await;
    assert!(result.is_ok());
}

// ─── 12. TimescaleDB Provider Tests ────────────────────────────────────────

#[tokio::test]
async fn test_timescaledb_from_config_defaults() {
    let config = make_config("timescaledb");
    let provider = timescaledb::TimescaleDbProvider::from_config("test_timescaledb", &config);
    assert!(
        provider.is_ok(),
        "TimescaleDB provider should initialize from config"
    );
}

#[tokio::test]
async fn test_timescaledb_provider_info() {
    let config = make_config("timescaledb");
    let provider = timescaledb::TimescaleDbProvider::from_config("test_timescaledb", &config)
        .expect("Failed to create TimescaleDB provider");

    let info = provider.info();
    assert_eq!(info.provider_type, "timescaledb");
    assert!(!info.display_name.is_empty());
    assert!(info.capabilities.contains(&Capability::SqlQuery));
}

#[tokio::test]
#[ignore]
async fn test_timescaledb_test_connection() {
    let config = make_live_pgwire_config("timescaledb");
    let provider = timescaledb::TimescaleDbProvider::from_config("test_timescaledb", &config)
        .expect("Failed to create TimescaleDB provider");

    let result = provider.test_connection().await;
    assert!(result.is_ok());
    let test_result = result.unwrap();
    assert!(test_result.success);
}

#[tokio::test]
#[ignore]
async fn test_timescaledb_execute_select() {
    let config = make_live_pgwire_config("timescaledb");
    let provider = timescaledb::TimescaleDbProvider::from_config("test_timescaledb", &config)
        .expect("Failed to create TimescaleDB provider");

    let params = HashMap::new();
    let result = provider.execute("SELECT 1 AS val", &params).await;
    assert!(result.is_ok(), "TimescaleDB SELECT failed: {:?}", result.err());
    let sql_result = result.unwrap();
    assert!(sql_result.rows_returned.is_some());
}

#[tokio::test]
#[ignore]
async fn test_timescaledb_execute_insert() {
    let config = make_live_pgwire_config("timescaledb");
    let provider = timescaledb::TimescaleDbProvider::from_config("test_timescaledb", &config)
        .expect("Failed to create TimescaleDB provider");

    let params = HashMap::new();
    provider
        .execute(
            "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)",
            &params,
        )
        .await
        .expect("Failed to create test table");
    let result = provider
        .execute(
            "INSERT INTO test_table VALUES (1, 'hello') ON CONFLICT DO NOTHING",
            &params,
        )
        .await;
    assert!(
        result.is_ok(),
        "TimescaleDB INSERT failed: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_timescaledb_close() {
    let config = make_config("timescaledb");
    let provider = timescaledb::TimescaleDbProvider::from_config("test_timescaledb", &config)
        .expect("Failed to create TimescaleDB provider");

    let result = provider.close().await;
    assert!(result.is_ok());
}

// ─── Cross-Cutting Tests ───────────────────────────────────────────────────

#[tokio::test]
async fn test_all_sql_providers_have_sql_query_capability() {
    let provider_configs = vec![
        ("postgres", make_config("postgres")),
        ("snowflake", make_config("snowflake")),
        ("clickhouse", make_config("clickhouse")),
        ("redshift", make_config("redshift")),
        ("bigquery", make_config("bigquery")),
        ("duckdb", make_config("duckdb")),
        ("mysql", make_config("mysql")),
        ("sqlite", make_config("sqlite")),
        ("oracle", make_config("oracle")),
        ("sqlserver", make_config("sqlserver")),
        ("cockroachdb", make_config("cockroachdb")),
        ("timescaledb", make_config("timescaledb")),
    ];

    let providers_info = vec![
        (
            "postgres",
            postgres::PostgresProvider::from_config("test", &provider_configs[0].1)
                .expect("Failed to create postgres")
                .info(),
        ),
        (
            "snowflake",
            snowflake::SnowflakeProvider::from_config("test", &provider_configs[1].1)
                .expect("Failed to create snowflake")
                .info(),
        ),
        (
            "clickhouse",
            clickhouse::ClickHouseProvider::from_config("test", &provider_configs[2].1)
                .expect("Failed to create clickhouse")
                .info(),
        ),
        (
            "redshift",
            redshift::RedshiftProvider::from_config("test", &provider_configs[3].1)
                .expect("Failed to create redshift")
                .info(),
        ),
        (
            "bigquery",
            bigquery::BigQueryProvider::from_config("test", &provider_configs[4].1)
                .expect("Failed to create bigquery")
                .info(),
        ),
        (
            "duckdb",
            duckdb::DuckDbProvider::from_config("test", &provider_configs[5].1)
                .expect("Failed to create duckdb")
                .info(),
        ),
        (
            "mysql",
            mysql::MySqlProvider::from_config("test", &provider_configs[6].1)
                .expect("Failed to create mysql")
                .info(),
        ),
        (
            "sqlite",
            sqlite::SqliteProvider::from_config("test", &provider_configs[7].1)
                .expect("Failed to create sqlite")
                .info(),
        ),
        (
            "oracle",
            oracle::OracleProvider::from_config("test", &provider_configs[8].1)
                .expect("Failed to create oracle")
                .info(),
        ),
        (
            "sqlserver",
            sqlserver::SqlServerProvider::from_config("test", &provider_configs[9].1)
                .expect("Failed to create sqlserver")
                .info(),
        ),
        (
            "cockroachdb",
            cockroachdb::CockroachDbProvider::from_config("test", &provider_configs[10].1)
                .expect("Failed to create cockroachdb")
                .info(),
        ),
        (
            "timescaledb",
            timescaledb::TimescaleDbProvider::from_config("test", &provider_configs[11].1)
                .expect("Failed to create timescaledb")
                .info(),
        ),
    ];

    for (name, info) in &providers_info {
        assert!(
            info.capabilities.contains(&Capability::SqlQuery),
            "Provider {} should have SqlQuery capability",
            name
        );
    }
}

#[test]
fn test_sql_result_empty() {
    let result = SqlResult::empty();
    assert_eq!(result.rows_affected, 0);
    assert!(result.rows_returned.is_none());
    assert_eq!(result.execution_time_ms, 0);
    assert!(result.columns.is_empty());
    assert!(result.sample_rows.is_empty());
    assert!(result.metrics.is_empty());
}

#[test]
fn test_sql_result_to_protocol_output() {
    let mut result = SqlResult::empty();
    result.rows_affected = 5;
    result.rows_returned = Some(10);
    result.execution_time_ms = 150;
    result.metrics.insert("query_cost".to_string(), 1.5);

    let output = result.to_protocol_output();

    assert!(output.contains("CONDUIT::LOG::INFO::Query completed in 150ms"));
    assert!(output.contains("CONDUIT::LOG::INFO::Rows returned: 10"));
    assert!(output.contains("CONDUIT::LOG::INFO::Rows affected: 5"));
    assert!(output.contains("CONDUIT::METRIC::row_count::10"));
    assert!(output.contains("CONDUIT::METRIC::query_cost::1.5"));
    assert!(output.contains("CONDUIT::XCOM::"));
}
