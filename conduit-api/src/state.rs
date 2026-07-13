//! Shared application state for the API server.
//!
//! All state is behind Arc so it can be shared across axum handlers.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, RwLock};

use chrono::{DateTime, Duration, Utc};
use conduit_lineage::{ExternalLineageStore, TableCatalog};

use crate::plan_cache::PlanCache;
use conduit_providers::registry::ConnectionSummary;
use conduit_providers::traits::ConnectionTestResult;
use conduit_providers::ProviderRegistry;
use conduit_scheduler::SchedulerEvent;
use conduit_state::{EnvironmentManager, SnapshotStore};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::auth::{ApiKey, AuthStore};

/// Maximum number of DAG runs to keep in the in-memory cache.
const MAX_CACHED_RUNS: usize = 10_000;

/// Run status tracking for the API.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DagRunInfo {
    /// Serializes as "id" for frontend convenience.
    #[serde(rename = "id")]
    pub run_id: String,
    pub dag_id: String,
    pub status: String,
    pub started_at: DateTime<Utc>,
    /// Serializes as "endedAt" to match frontend convention.
    #[serde(rename = "endedAt")]
    pub finished_at: Option<DateTime<Utc>>,
    pub task_states: HashMap<String, String>,
    /// Captured stdout/stderr per task (truncated), for post-hoc debugging
    /// of completed runs. Live streaming still happens over the WebSocket.
    #[serde(default)]
    pub task_logs: HashMap<String, String>,
    pub triggered_by: String,
    /// Virtual environment this run targets. Persisted JSON written before
    /// this field existed deserializes as "production".
    #[serde(default = "default_run_environment")]
    pub environment: String,
}

fn default_run_environment() -> String {
    "production".to_string()
}

/// Shared application state.
pub struct AppState {
    /// Path to DAG definitions.
    pub dags_path: PathBuf,
    /// State directory (for event store, snapshots).
    pub state_dir: PathBuf,
    /// Optional path to built UI assets (for serving the frontend).
    pub ui_dir: Option<PathBuf>,
    /// Environment manager.
    pub env_manager: EnvironmentManager,
    /// Snapshot store, shared with the env manager so promotion policies that
    /// gate on snapshot age see the same data.
    pub snapshot_store: Arc<SnapshotStore>,
    /// Active and historical DAG runs.
    pub runs: RwLock<Vec<DagRunInfo>>,
    /// WebSocket broadcast channel for live events.
    pub event_tx: tokio::sync::broadcast::Sender<String>,
    /// Provider registry: named connections to external data systems.
    pub provider_registry: RwLock<Option<Arc<ProviderRegistry>>>,
    /// Authentication store: API keys, roles, and permissions.
    pub auth_store: AuthStore,
    /// Optional channel to dispatch events to the scheduler.
    /// When set, triggered runs are sent to the scheduler for execution.
    /// When absent, runs are recorded as intent only (backward compatible).
    pub scheduler_tx: OnceLock<mpsc::UnboundedSender<SchedulerEvent>>,
    /// Cached table catalog for enhanced SQL lineage resolution.
    /// Populated via `/api/v1/lineage/catalog/refresh` or on-demand per query.
    pub catalog_cache: RwLock<Option<TableCatalog>>,
    /// Ingested OpenLineage events from upstream systems (Airflow, dbt, Spark).
    /// Durable via RocksDB by default; see `conduit-lineage::openlineage_ingest` docs.
    pub external_lineage: Arc<ExternalLineageStore>,
    /// Compiled-plan + per-DAG stitched-lineage cache, invalidated on
    /// DAG source signature change. Shared across all handlers that
    /// need a `ConduitPlan` or a `CrossTaskLineage`.
    pub plan_cache: Arc<PlanCache>,
    /// CORS origins allowed to call the API cross-origin. Empty (the
    /// default) means same-origin only — no CORS headers are emitted.
    /// Set at startup via `conduit serve --cors-origin` before the router
    /// is built.
    pub cors_allowed_origins: RwLock<Vec<String>>,
    /// Optional persistent event store backing the `/events` query API. When
    /// `state_dir/events` exists, it's opened on `AppState::with_options`; if
    /// the directory is missing or open fails (e.g. permissions), the field
    /// stays `None` and the events endpoint reports an empty result rather
    /// than failing the request.
    pub event_store: Option<Arc<conduit_state::EventStore>>,
}

impl AppState {
    /// Create a new AppState with defaults (auth disabled for backward compatibility).
    pub fn new(dags_path: PathBuf, state_dir: PathBuf) -> Arc<Self> {
        Self::with_options(dags_path, state_dir, None, false)
    }

    /// Create with UI directory (auth disabled).
    pub fn with_ui_dir(
        dags_path: PathBuf,
        state_dir: PathBuf,
        ui_dir: Option<PathBuf>,
    ) -> Arc<Self> {
        Self::with_options(dags_path, state_dir, ui_dir, false)
    }

    /// Create with all options including auth.
    pub fn with_options(
        dags_path: PathBuf,
        state_dir: PathBuf,
        ui_dir: Option<PathBuf>,
        auth_enabled: bool,
    ) -> Arc<Self> {
        let (event_tx, _) = tokio::sync::broadcast::channel(1024);
        let auth_store = AuthStore::new(auth_enabled);

        // Load persisted API keys if they exist
        let keys_file = state_dir.join("auth_keys.json");
        if keys_file.exists() {
            if let Ok(data) = std::fs::read_to_string(&keys_file) {
                if let Ok(keys) = serde_json::from_str::<Vec<ApiKey>>(&data) {
                    auth_store.import_keys(&keys);
                }
            }
        }

        // Open (creating if missing) the persistent event store. Failure is
        // non-fatal: the events API reports empty and lifecycle events are
        // simply not recorded. The CLI's replay command reads the same DB.
        let events_dir = state_dir.join("events");
        let event_store = match conduit_state::EventStore::open(&events_dir) {
            Ok(store) => Some(Arc::new(store)),
            Err(e) => {
                tracing::warn!(
                    path = %events_dir.display(),
                    error = %e,
                    "Failed to open event store; events will not be recorded",
                );
                None
            }
        };

        // Attach env history store so promote/rollback record versions on disk.
        let env_manager = EnvironmentManager::new();
        let env_manager = match conduit_state::EnvHistoryStore::open(state_dir.join("env_history"))
        {
            Ok(store) => env_manager.with_history_store(store),
            Err(_) => env_manager,
        };
        let snapshot_store = Arc::new(SnapshotStore::new());
        let env_manager = env_manager.with_snapshot_store(Arc::clone(&snapshot_store));
        let env_manager = match &event_store {
            Some(store) => env_manager.with_event_store(Arc::clone(store)),
            None => env_manager,
        };

        let external_lineage = Arc::new(open_external_lineage_store(&state_dir));
        let plan_cache = Arc::new(PlanCache::new(dags_path.clone()));

        Arc::new(Self {
            dags_path,
            state_dir,
            ui_dir,
            env_manager,
            snapshot_store,
            runs: RwLock::new(Vec::new()),
            event_tx,
            provider_registry: RwLock::new(None),
            auth_store,
            scheduler_tx: OnceLock::new(),
            catalog_cache: RwLock::new(None),
            external_lineage,
            plan_cache,
            cors_allowed_origins: RwLock::new(Vec::new()),
            event_store,
        })
    }

    /// Attach a scheduler event sender to enable run dispatching.
    ///
    /// Must be called before the server starts accepting requests.
    /// When this channel is set, `trigger_run` will send `SchedulerEvent::DagRunRequested`
    /// to the scheduler instead of merely recording intent.
    ///
    /// # Panics
    /// Panics if called more than once (scheduler channel can only be set once).
    pub fn with_scheduler(&self, sender: mpsc::UnboundedSender<SchedulerEvent>) {
        self.scheduler_tx
            .set(sender)
            .expect("scheduler channel already configured");
    }

    /// Persist API keys to disk with restricted file permissions (0600).
    pub fn save_auth_keys(&self) {
        let keys_file = self.state_dir.join("auth_keys.json");
        let keys = self.auth_store.export_keys();
        if let Ok(data) = serde_json::to_string_pretty(&keys) {
            if std::fs::write(&keys_file, data).is_ok() {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let perms = std::fs::Permissions::from_mode(0o600);
                    let _ = std::fs::set_permissions(&keys_file, perms);
                }
            }
        }
    }

    /// Initialize the provider registry from connection configs.
    pub async fn init_providers(
        &self,
        connections: &HashMap<String, conduit_common::config::ConnectionConfig>,
    ) {
        let registry = ProviderRegistry::from_configs(connections).await;
        if let Ok(mut guard) = self.provider_registry.write() {
            *guard = Some(Arc::new(registry));
        }
    }

    /// List all configured connections.
    pub fn list_connections(&self) -> Vec<ConnectionSummary> {
        self.provider_registry
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().map(|r| r.list_connections()))
            .unwrap_or_default()
    }

    /// Test a specific connection.
    ///
    /// The registry `Arc` is cloned and the lock released *before* awaiting,
    /// so the returned future stays `Send` (required by axum handlers).
    pub async fn test_connection(
        &self,
        name: &str,
    ) -> Result<ConnectionTestResult, conduit_providers::ProviderError> {
        let registry = {
            let guard = self.provider_registry.read().map_err(|_| {
                conduit_providers::ProviderError::ConnectionNotFound {
                    name: name.to_string(),
                }
            })?;
            guard.as_ref().cloned().ok_or_else(|| {
                conduit_providers::ProviderError::ConnectionNotFound {
                    name: name.to_string(),
                }
            })?
        };
        registry.test_connection(name).await
    }

    /// Configure the CORS allow-list. Call before `build_router`; the
    /// router snapshots this list at construction time.
    pub fn set_cors_origins(&self, origins: Vec<String>) {
        if let Ok(mut guard) = self.cors_allowed_origins.write() {
            *guard = origins;
        }
    }

    /// Record a security-relevant auth event in the event store (PRD A3).
    /// Best-effort: a missing or failing store never blocks the request.
    pub fn audit_auth(&self, action: &str, key_id: Option<String>, detail: impl Into<String>) {
        if let Some(store) = &self.event_store {
            let event = conduit_common::event::EventKind::AuthAudit {
                action: action.to_string(),
                key_id,
                detail: detail.into(),
            };
            if let Err(e) = store.append(event) {
                tracing::debug!(error = %e, "failed to append auth audit event");
            }
        }
    }

    /// Broadcast an event to all WebSocket subscribers.
    pub fn broadcast_event(&self, event_json: &str) {
        let _ = self.event_tx.send(event_json.to_string());
    }

    /// Record a new DAG run, evicting the oldest entries if the cache exceeds
    /// [`MAX_CACHED_RUNS`].
    pub fn record_run(&self, run: DagRunInfo) {
        if let Ok(mut runs) = self.runs.write() {
            runs.push(run);
            if runs.len() > MAX_CACHED_RUNS {
                let excess = runs.len() - MAX_CACHED_RUNS;
                runs.drain(..excess);
            }
        }
    }

    /// Get runs, optionally filtered by DAG ID.
    pub fn get_runs(&self, dag_id: Option<&str>) -> Vec<DagRunInfo> {
        self.runs
            .read()
            .map(|runs| {
                runs.iter()
                    .filter(|r| dag_id.is_none_or(|id| r.dag_id == id))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Seed the state with fabricated demo run history so the UI has data
    /// to display. Only called when `conduit serve --demo` is passed; all
    /// seeded runs carry `triggered_by: "demo"` so they are distinguishable
    /// from real runs.
    pub fn seed_demo_data(&self) {
        let now = Utc::now();

        let ecommerce_tasks = vec![
            "extract_orders",
            "extract_products",
            "extract_customers",
            "extract_inventory",
            "extract_clickstream",
            "validate_sources",
            "enrich_orders",
            "sessionize_clicks",
            "build_customer_360",
            "build_product_performance",
            "build_revenue_dashboard",
            "notify_stakeholders",
        ];

        for day in 0..7 {
            let started = now - Duration::days(day) - Duration::hours(19);
            let status = if day == 2 { "failed" } else { "success" };
            let ended = if status == "success" {
                Some(started + Duration::minutes(42) + Duration::seconds(day * 13))
            } else {
                Some(started + Duration::minutes(18))
            };

            let task_states: HashMap<String, String> = ecommerce_tasks
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    let s = if status == "failed" && i >= 6 {
                        if i == 6 {
                            "failed"
                        } else {
                            "skipped"
                        }
                    } else {
                        "success"
                    };
                    (t.to_string(), s.to_string())
                })
                .collect();

            self.record_run(DagRunInfo {
                run_id: format!(
                    "run_wh_{}",
                    (now - Duration::days(day)).format("%Y%m%d_050000")
                ),
                dag_id: "daily_warehouse_refresh".to_string(),
                status: status.to_string(),
                started_at: started,
                finished_at: ended,
                task_states,
                task_logs: HashMap::new(),
                triggered_by: "demo".to_string(),
                environment: "production".to_string(),
            });
        }

        let saas_tasks = vec![
            "wait_for_billing_close",
            "extract_subscriptions",
            "extract_billing_events",
            "extract_usage_data",
            "validate_billing",
            "compute_mrr",
            "compute_churn",
            "compute_cohort_retention",
            "compute_expansion_contraction",
            "aggregate_saas_dashboard",
            "update_investor_metrics",
            "alert_on_anomalies",
        ];

        for month in 0..3 {
            let started = now - Duration::days(month * 30 + 1) - Duration::hours(20);
            let task_states: HashMap<String, String> = saas_tasks
                .iter()
                .map(|t| (t.to_string(), "success".to_string()))
                .collect();

            self.record_run(DagRunInfo {
                run_id: format!(
                    "run_mkt_{}",
                    (now - Duration::days(month * 30 + 1)).format("%Y%m%d_040000")
                ),
                dag_id: "weekly_marketing_report".to_string(),
                status: "success".to_string(),
                started_at: started,
                finished_at: Some(started + Duration::hours(1) + Duration::minutes(23)),
                task_states,
                task_logs: HashMap::new(),
                triggered_by: "demo".to_string(),
                environment: "production".to_string(),
            });
        }

        let ml_tasks = vec![
            "extract_training_data",
            "validate_raw_data",
            "compute_user_features",
            "compute_product_features",
            "compute_behavioral_features",
            "compute_temporal_features",
            "assemble_feature_matrix",
            "train_model",
            "evaluate_holdout",
            "detect_drift",
            "check_fairness",
            "promotion_gate",
            "deploy_to_serving",
            "notify_ml_team",
        ];

        for day in 0..7 {
            let started = now - Duration::days(day) - Duration::hours(22);
            let (status, duration_min) = match day {
                1 => ("failed", 95),
                4 => ("running", 0),
                _ => ("success", 110 + (day * 7)),
            };

            let task_states: HashMap<String, String> = ml_tasks
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    let s = match (day, status, i) {
                        (4, "running", idx) if idx < 8 => "success",
                        (4, "running", 8) => "running",
                        (4, "running", _) => "pending",
                        (1, "failed", idx) if idx < 9 => "success",
                        (1, "failed", 9) => "failed",
                        (1, "failed", _) => "skipped",
                        _ => "success",
                    };
                    (t.to_string(), s.to_string())
                })
                .collect();

            let ended = if status == "running" {
                None
            } else {
                Some(started + Duration::minutes(duration_min))
            };

            self.record_run(DagRunInfo {
                run_id: format!(
                    "run_ml_{}",
                    (now - Duration::days(day)).format("%Y%m%d_020000")
                ),
                dag_id: "daily_model_training".to_string(),
                status: status.to_string(),
                started_at: started,
                finished_at: ended,
                task_states,
                task_logs: HashMap::new(),
                triggered_by: "demo".to_string(),
                environment: if day == 4 {
                    "staging".to_string()
                } else {
                    "production".to_string()
                },
            });
        }

        let etl_tasks = [
            "extract_orders",
            "extract_customers",
            "validate_extracts",
            "transform_orders",
            "build_customer_360",
            "notify_completion",
        ];

        for day in 0..5 {
            let started = now - Duration::days(day) - Duration::hours(18);
            let task_states: HashMap<String, String> = etl_tasks
                .iter()
                .map(|t| (t.to_string(), "success".to_string()))
                .collect();

            self.record_run(DagRunInfo {
                run_id: format!(
                    "run_demo_{}",
                    (now - Duration::days(day)).format("%Y%m%d_060000")
                ),
                dag_id: "demo_pipeline".to_string(),
                status: "success".to_string(),
                started_at: started,
                finished_at: Some(started + Duration::minutes(28 + day * 3)),
                task_states,
                task_logs: HashMap::new(),
                triggered_by: "demo".to_string(),
                environment: if day < 2 {
                    "staging".to_string()
                } else {
                    "production".to_string()
                },
            });
        }

        let _ = self.env_manager.create("staging", Some("production"));
        let _ = self.env_manager.create("dev", Some("production"));

        self.seed_demo_connections();
    }

    fn seed_demo_connections(&self) {
        use conduit_common::config::ConnectionConfig;
        use serde_json::json;

        let mut connections = HashMap::new();

        let sql_connections = vec![
            (
                "shopify_replica",
                "postgres",
                "shopify-replica.internal",
                5432,
                "shopify",
            ),
            (
                "warehouse_mgmt",
                "postgres",
                "warehouse-db.internal",
                5432,
                "warehouse",
            ),
            (
                "stripe_replica",
                "postgres",
                "stripe-replica.internal",
                5432,
                "stripe",
            ),
            (
                "product_db",
                "postgres",
                "product-db.internal",
                5432,
                "product",
            ),
            (
                "feature_store",
                "postgres",
                "feature-store.internal",
                5432,
                "features",
            ),
            (
                "source_db",
                "postgres",
                "source-primary.internal",
                5432,
                "production",
            ),
            (
                "clickhouse_events",
                "clickhouse",
                "clickhouse-cluster.internal",
                8123,
                "events",
            ),
        ];

        for (name, conn_type, host, port, db) in &sql_connections {
            connections.insert(
                name.to_string(),
                ConnectionConfig {
                    conn_type: conn_type.to_string(),
                    host: Some(host.to_string()),
                    port: Some(*port),
                    database: Some(db.to_string()),
                    credentials: Some("${DB_PASSWORD}".to_string()),
                    extra: [("user".to_string(), json!("conduit_reader"))]
                        .into_iter()
                        .collect(),
                },
            );
        }

        for (name, wh, schema) in [
            ("analytics_warehouse", "etl_wh_medium", "analytics"),
            ("warehouse", "transform_wh_large", "staging"),
            ("data_lake", "ingest_wh_small", "raw"),
        ] {
            connections.insert(
                name.to_string(),
                ConnectionConfig {
                    conn_type: "snowflake".to_string(),
                    host: Some("acme.us-east-1.snowflakecomputing.com".to_string()),
                    port: None,
                    database: Some("analytics".to_string()),
                    credentials: Some("${SNOWFLAKE_PASSWORD}".to_string()),
                    extra: [
                        ("user".to_string(), json!("conduit_etl")),
                        ("warehouse".to_string(), json!(wh)),
                        ("role".to_string(), json!("etl_role")),
                        ("schema".to_string(), json!(schema)),
                    ]
                    .into_iter()
                    .collect(),
                },
            );
        }

        connections.insert(
            "data_lake_s3".to_string(),
            ConnectionConfig {
                conn_type: "s3".to_string(),
                host: None,
                port: None,
                database: Some("acme-data-lake".to_string()),
                credentials: Some("${AWS_SECRET_ACCESS_KEY}".to_string()),
                extra: [
                    ("region".to_string(), json!("us-east-1")),
                    ("prefix".to_string(), json!("raw/")),
                ]
                .into_iter()
                .collect(),
            },
        );

        connections.insert(
            "model_artifacts".to_string(),
            ConnectionConfig {
                conn_type: "s3".to_string(),
                host: None,
                port: None,
                database: Some("acme-ml-artifacts".to_string()),
                credentials: Some("${AWS_SECRET_ACCESS_KEY}".to_string()),
                extra: [
                    ("region".to_string(), json!("us-east-1")),
                    ("prefix".to_string(), json!("models/")),
                ]
                .into_iter()
                .collect(),
            },
        );

        connections.insert(
            "slack_notifications".to_string(),
            ConnectionConfig {
                conn_type: "webhook".to_string(),
                host: Some("https://hooks.slack.com".to_string()),
                port: None,
                database: None,
                credentials: None,
                extra: [(
                    "base_path".to_string(),
                    json!("/services/T00/B00/pipeline-alerts"),
                )]
                .into_iter()
                .collect(),
            },
        );

        connections.insert(
            "event_bus".to_string(),
            ConnectionConfig {
                conn_type: "kafka".to_string(),
                host: Some(
                    "kafka-1.internal:9092,kafka-2.internal:9092,kafka-3.internal:9092".to_string(),
                ),
                port: None,
                database: None,
                credentials: Some("${KAFKA_PASSWORD}".to_string()),
                extra: [
                    ("security_protocol".to_string(), json!("SASL_SSL")),
                    ("sasl_mechanism".to_string(), json!("SCRAM-SHA-256")),
                    ("group_id".to_string(), json!("conduit-pipeline")),
                ]
                .into_iter()
                .collect(),
            },
        );

        let registry = tokio::runtime::Handle::try_current()
            .ok()
            .map(|handle| {
                tokio::task::block_in_place(|| {
                    handle.block_on(ProviderRegistry::from_configs(&connections))
                })
            })
            .unwrap_or_else(|| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(ProviderRegistry::from_configs(&connections))
            });

        if let Ok(mut guard) = self.provider_registry.write() {
            *guard = Some(Arc::new(registry));
        }
    }
}

/// Open the durable external-lineage store at `state_dir/external_lineage_db`.
///
/// Falls back to an in-memory store (with a warning) when the on-disk
/// store can't be opened — typically this means another process is
/// holding the RocksDB lock or the user supplied a state dir on
/// read-only media. The in-memory store still satisfies every public
/// API; it just loses ingested events on restart.
fn open_external_lineage_store(state_dir: &std::path::Path) -> ExternalLineageStore {
    let path = state_dir.join("external_lineage_db");
    match conduit_state::RocksExternalLineageBackend::open(&path) {
        Ok(backend) => {
            tracing::info!(
                path = %path.display(),
                "External lineage store opened (durable)"
            );
            ExternalLineageStore::new(Arc::new(backend))
        }
        Err(e) => {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "Falling back to in-memory external lineage store; ingested events will not persist across restart"
            );
            ExternalLineageStore::in_memory()
        }
    }
}
