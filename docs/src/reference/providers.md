# Providers & Connections

Conduit ships with a typed provider system: every connector implements a category trait (`SqlProvider`, `StorageProvider`, `HttpProvider`, `StreamProvider`, `SaasProvider`, `DocumentProvider`) on top of the base `Provider` trait. Configure connections in `conduit.yaml` and validate them with `conduit test-connection <name>`.

**32 provider types: 12 production, 20 experimental.** Experimental providers expose the trait interface but their operations return `NotImplemented`; `conduit compile` warns when a DAG routes through one. This table is generated from each provider's `ProviderInfo.is_stub` flag — it cannot drift from the code.

| Status legend | |
|---|---|
| ✅ Production | Real implementation with a live `test_connection` |
| 🧪 Experimental | Trait interface only; operations return `NotImplemented` |

## SQL Databases

| Status | Provider | Type ID | Aliases |
|---|---|---|---|
| ✅ | Google BigQuery | `bigquery` | `bq` |
| 🧪 | ClickHouse | `clickhouse` | `ch` |
| ✅ | CockroachDB | `cockroachdb` | `crdb` |
| ✅ | DuckDB | `duckdb` | `duck` |
| ✅ | MySQL | `mysql` | `mariadb` |
| 🧪 | Oracle Database | `oracle` | — |
| ✅ | PostgreSQL | `postgres` | `postgresql`, `pg` |
| ✅ | Amazon Redshift | `redshift` | — |
| ✅ | Snowflake | `snowflake` | `sf` |
| ✅ | SQLite | `sqlite` | — |
| 🧪 | SQL Server | `sqlserver` | `mssql` |
| ✅ | TimescaleDB | `timescaledb` | `tsdb` |

## Object Storage

| Status | Provider | Type ID | Aliases |
|---|---|---|---|
| ✅ | Google Cloud Storage | `gcs` | `google_cloud_storage` |
| ✅ | Amazon S3 | `s3` | `aws_s3` |

## HTTP / Webhooks

| Status | Provider | Type ID | Aliases |
|---|---|---|---|
| ✅ | HTTP/REST API | `http` | `https`, `rest`, `webhook` |

## Streaming

| Status | Provider | Type ID | Aliases |
|---|---|---|---|
| 🧪 | Apache Kafka | `kafka` | — |
| 🧪 | AWS Kinesis | `kinesis` | — |
| 🧪 | GCP Pub/Sub | `pubsub` | `gcp_pubsub` |
| 🧪 | RabbitMQ | `rabbitmq` | `amqp` |
| 🧪 | Redis Streams | `redis` | `redis_stream` |

## SaaS Platforms

| Status | Provider | Type ID | Aliases |
|---|---|---|---|
| 🧪 | GitHub | `github` | `gh` |
| 🧪 | HubSpot | `hubspot` | — |
| 🧪 | Jira | `jira` | — |
| 🧪 | Salesforce | `salesforce` | `sfdc` |
| 🧪 | Slack | `slack` | — |
| 🧪 | Stripe | `stripe` | — |

## Document / NoSQL

| Status | Provider | Type ID | Aliases |
|---|---|---|---|
| 🧪 | Cassandra | `cassandra` | `scylladb` |
| 🧪 | DynamoDB | `dynamodb` | — |
| 🧪 | Elasticsearch | `elasticsearch` | `opensearch`, `es` |
| 🧪 | MongoDB | `mongodb` | `mongo` |
| 🧪 | Neo4j | `neo4j` | — |
| 🧪 | Redis KV | `redis_kv` | — |

## Configuration

Connections live under `connections:` in `conduit.yaml`. Secrets can be injected from environment variables or a secrets backend rather than committed inline. Example:

```yaml
connections:
  warehouse:
    type: postgres
    host: db.internal
    port: 5432
    database: analytics
    # credentials resolved from CONDUIT_CONN_WAREHOUSE or a secrets backend
```

Validate connectivity before running pipelines:

```bash
conduit test-connection warehouse
```
