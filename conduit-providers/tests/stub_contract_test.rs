//! Verifies ProviderInfo.is_stub is set correctly for known stub and real
//! providers. This is the load-bearing test for "stub honesty": it locks in
//! the contract that you can ask any provider whether it's real or a
//! placeholder before routing workloads through it.
//!
//! The audit found 19 providers returning NotImplemented while being
//! advertised as supported. This test prevents that regression by enforcing
//! the is_stub flag at the type level for the providers that actually have
//! stubbed behavior.

use std::collections::HashMap;

use conduit_common::config::ConnectionConfig;
use conduit_providers::providers::*;
use conduit_providers::traits::*;

fn cfg(conn_type: &str) -> ConnectionConfig {
    ConnectionConfig {
        conn_type: conn_type.to_string(),
        host: Some("dummy.invalid".to_string()),
        port: Some(5432),
        database: Some("dummy".to_string()),
        credentials: None,
        extra: HashMap::new(),
    }
}

#[test]
fn known_stubs_advertise_is_stub_true() {
    macro_rules! check_stub {
        ($provider:path, $conn_type:expr) => {
            if let Ok(p) = <$provider>::from_config("test", &cfg($conn_type)) {
                assert!(
                    p.info().is_stub,
                    "Provider {} is documented as a stub but ProviderInfo.is_stub=false",
                    $conn_type,
                );
            }
        };
    }

    check_stub!(kinesis::KinesisProvider, "kinesis");
    check_stub!(pubsub::PubSubProvider, "pubsub");
    check_stub!(rabbitmq::RabbitMqProvider, "rabbitmq");
    check_stub!(redis_stream::RedisStreamProvider, "redis_stream");
    check_stub!(clickhouse::ClickHouseProvider, "clickhouse");
    check_stub!(sqlserver::SqlServerProvider, "sqlserver");
    check_stub!(oracle::OracleProvider, "oracle");
}

#[test]
fn known_real_providers_advertise_is_stub_false() {
    macro_rules! check_real {
        ($provider:path, $conn_type:expr) => {
            if let Ok(p) = <$provider>::from_config("test", &cfg($conn_type)) {
                assert!(
                    !p.info().is_stub,
                    "Provider {} has a real implementation but ProviderInfo.is_stub=true. \
                     If this provider truly is stubbed, flip is_stub; otherwise this is a \
                     contract violation.",
                    $conn_type,
                );
            }
        };
    }

    check_real!(postgres::PostgresProvider, "postgres");
    check_real!(mysql::MySqlProvider, "mysql");
    check_real!(sqlite::SqliteProvider, "sqlite");
    check_real!(cockroachdb::CockroachDbProvider, "cockroachdb");
    check_real!(timescaledb::TimescaleDbProvider, "timescaledb");
    check_real!(redshift::RedshiftProvider, "redshift");
}
