//! Lineage API handlers.
//!
//! Provides endpoints for column-level lineage tracing, schema inspection,
//! impact analysis, and schema contract validation.
//!
//! The `/lineage/sql` endpoint supports enhanced resolution via:
//! - `connection`: introspect a SQL provider for table schemas
//! - `tables`: provide table schemas inline in the request
//! - Cached global catalog (populated via `/lineage/catalog/refresh`)

use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use conduit_lineage::{
    parse_sql_type, CatalogColumn, Column, ColumnRef, ColumnType, ContractValidator, LineageGraph,
    OpenLineageEventType, OpenLineageRunEvent, OpenLineageSqlEventOptions, Schema,
    SchemaChangeDetector, SchemaContract, SqlLineageExtractor, TableCatalog,
    CONDUIT_OPENLINEAGE_PRODUCER,
};
use conduit_providers::registry::ProviderInstance;

use crate::error::ApiError;
use crate::AppState;

// ─── Request / Response types ────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct TraceRequest {
    pub task_id: String,
    pub column_name: String,
}

#[derive(Deserialize)]
pub struct SqlLineageRequest {
    pub sql: String,
    pub source_task_id: String,
    /// Optional connection name — if provided, introspects table schemas
    /// from this SQL provider for enhanced column resolution.
    #[serde(default)]
    pub connection: Option<String>,
    /// Optional inline table schemas — provide column metadata directly
    /// without needing a live database connection.
    #[serde(default)]
    pub tables: Option<Vec<TableSchemaInput>>,
    /// Optional OpenLineage metadata. When present, `/lineage/sql` includes
    /// an OpenLineage RunEvent with a columnLineage output dataset facet.
    #[serde(default)]
    pub openlineage: Option<OpenLineageSqlRequest>,
}

#[derive(Deserialize)]
pub struct OpenLineageSqlRequest {
    /// Output dataset name for the event, e.g. `analytics.customer_daily`.
    pub output_dataset: String,
    /// Dataset namespace for both input and output datasets. Defaults to the
    /// requested connection name, then `conduit`.
    #[serde(default)]
    pub dataset_namespace: Option<String>,
    /// OpenLineage job namespace. Defaults to `conduit`.
    #[serde(default)]
    pub job_namespace: Option<String>,
    /// OpenLineage job name. Defaults to `source_task_id`.
    #[serde(default)]
    pub job_name: Option<String>,
    /// OpenLineage run UUID. Defaults to a generated UUID.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Event timestamp. Defaults to the current time.
    #[serde(default)]
    pub event_time: Option<String>,
    /// START, RUNNING, COMPLETE, ABORT, FAIL, or OTHER. Defaults to COMPLETE.
    #[serde(default)]
    pub event_type: Option<String>,
    /// Producer URI for the event and generated facets.
    #[serde(default)]
    pub producer: Option<String>,
}

#[derive(Deserialize)]
pub struct TableSchemaInput {
    #[serde(default)]
    pub schema: Option<String>,
    pub table: String,
    pub columns: Vec<TableColumnInput>,
}

#[derive(Deserialize)]
pub struct TableColumnInput {
    pub name: String,
    #[serde(default = "default_column_type")]
    pub data_type: String,
}

fn default_column_type() -> String {
    "unknown".to_string()
}

#[derive(Deserialize)]
pub struct SchemaInput {
    pub task_id: String,
    pub columns: Vec<ColumnInput>,
}

#[derive(Deserialize)]
pub struct ColumnInput {
    pub name: String,
    pub column_type: String,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    #[serde(default)]
    pub description: Option<String>,
}

fn default_nullable() -> bool {
    true
}

#[derive(Deserialize)]
pub struct SchemaDiffRequest {
    pub old_schema: SchemaInput,
    pub new_schema: SchemaInput,
}

#[derive(Deserialize)]
pub struct ContractValidateRequest {
    pub schema: SchemaInput,
    pub contract: ContractInput,
}

#[derive(Deserialize)]
pub struct ContractInput {
    pub task_id: String,
    #[serde(default)]
    pub required_columns: Vec<RequiredColumnInput>,
    #[serde(default)]
    pub forbidden_columns: Vec<String>,
    #[serde(default)]
    pub max_columns: Option<usize>,
    #[serde(default)]
    pub require_docs: bool,
    #[serde(default)]
    pub no_unknown_types: bool,
}

#[derive(Deserialize)]
pub struct RequiredColumnInput {
    pub name: String,
    #[serde(default)]
    pub expected_type: Option<String>,
}

// ─── Handlers ────────────────────────────────────────────────────────────────

/// POST /api/v1/lineage/sql — Extract column-level lineage from a SQL query.
///
/// Supports enhanced resolution via:
/// - `connection`: name of a configured SQL provider to introspect for table schemas
/// - `tables`: inline table schemas (array of {schema?, table, columns})
/// - Falls back to cached catalog if neither is provided
///
/// Without any catalog, bare columns default to the first table in FROM
/// and `SELECT *` produces a wildcard mapping.
pub async fn extract_sql_lineage(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlLineageRequest>,
) -> Result<Json<Value>, ApiError> {
    // Build catalog from request or cache
    let catalog = if let Some(ref tables) = req.tables {
        // Inline table schemas provided
        Some(build_catalog_from_inline(tables))
    } else if let Some(ref connection) = req.connection {
        // Introspect from a live SQL provider
        let initial = SqlLineageExtractor::extract(&req.sql);
        Some(build_catalog_from_provider(&state, connection, &initial.source_tables).await?)
    } else {
        // Fall back to cached catalog
        state
            .catalog_cache
            .read()
            .ok()
            .and_then(|guard| guard.clone())
    };

    // Parse SQL (with or without catalog)
    let lineage = if let Some(ref cat) = catalog {
        SqlLineageExtractor::extract_with_catalog(&req.sql, cat)
    } else {
        SqlLineageExtractor::extract(&req.sql)
    };

    let output_cols: Vec<Value> = lineage
        .output_columns
        .iter()
        .map(|c| {
            json!({
                "name": c.name,
                "expression": c.expression,
                "is_computed": c.is_computed,
            })
        })
        .collect();

    let source_tables: Vec<Value> = lineage
        .source_tables
        .iter()
        .map(|t| {
            json!({
                "name": t.name,
                "alias": t.alias,
                "schema": t.schema,
            })
        })
        .collect();

    let mappings: Vec<Value> = lineage
        .column_mappings
        .iter()
        .map(|m| {
            json!({
                "output": m.output,
                "inputs": m.inputs,
            })
        })
        .collect();

    let openlineage = if let Some(ref opts) = req.openlineage {
        Some(build_openlineage_event(&req, opts, &lineage)?)
    } else {
        None
    };

    let mut response = json!({
        "source_task_id": req.source_task_id,
        "sql": req.sql,
        "catalog_used": catalog.is_some(),
        "output_columns": output_cols,
        "source_tables": source_tables,
        "column_mappings": mappings,
    });

    if let Some(event) = openlineage {
        response["openlineage"] =
            serde_json::to_value(event).map_err(|e| ApiError::Internal(e.to_string()))?;
    }

    Ok(Json(response))
}

/// POST /api/v1/lineage/catalog/refresh — Rebuild the cached table catalog.
///
/// Accepts a list of connections and tables to introspect. The resulting
/// catalog is cached for subsequent `/lineage/sql` requests.
///
/// Request body:
/// ```json
/// {
///   "sources": [
///     { "connection": "warehouse", "schema": "public", "tables": ["orders", "customers"] }
///   ]
/// }
/// ```
pub async fn refresh_catalog(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CatalogRefreshRequest>,
) -> Result<Json<Value>, ApiError> {
    let mut catalog = TableCatalog::new();
    let mut tables_registered = 0u64;
    let mut errors = Vec::new();

    for source in &req.sources {
        // Get the SQL provider Arc (clone to drop lock before await)
        let provider = {
            let guard = state
                .provider_registry
                .read()
                .map_err(|_| ApiError::Internal("Failed to read provider registry".into()))?;
            let registry = guard
                .as_ref()
                .ok_or_else(|| ApiError::NotFound("Provider registry not initialized".into()))?;
            let instance = registry.get(&source.connection).ok_or_else(|| {
                ApiError::NotFound(format!("Connection '{}' not found", source.connection))
            })?;
            match instance {
                ProviderInstance::Sql(p) => p.clone(),
                _ => {
                    errors.push(format!(
                        "Connection '{}' is not a SQL provider",
                        source.connection
                    ));
                    continue;
                }
            }
        };

        let schema = source.schema.as_deref().unwrap_or("public");
        for table_name in &source.tables {
            match provider.describe_table(schema, table_name).await {
                Ok(columns) => {
                    let cat_columns = columns
                        .into_iter()
                        .map(|c| {
                            let mut col = CatalogColumn::new(&c.name, parse_sql_type(&c.data_type));
                            if !c.is_nullable {
                                col = col.not_null();
                            }
                            col
                        })
                        .collect();
                    catalog.register_table(Some(schema), table_name, cat_columns);
                    tables_registered += 1;
                }
                Err(e) => {
                    errors.push(format!("{}.{}: {}", schema, table_name, e));
                }
            }
        }
    }

    // Cache the catalog
    if let Ok(mut guard) = state.catalog_cache.write() {
        *guard = Some(catalog);
    }

    Ok(Json(json!({
        "tables_registered": tables_registered,
        "errors": errors,
    })))
}

#[derive(Deserialize)]
pub struct CatalogRefreshRequest {
    pub sources: Vec<CatalogSourceInput>,
}

#[derive(Deserialize)]
pub struct CatalogSourceInput {
    pub connection: String,
    #[serde(default)]
    pub schema: Option<String>,
    pub tables: Vec<String>,
}

/// POST /api/v1/lineage/trace/upstream — Trace upstream dependencies for a column.
///
/// Given a task_id and column_name, builds a lineage graph from the provided
/// edges and traces all upstream columns that feed into the target.
pub async fn trace_upstream(
    State(_state): State<Arc<AppState>>,
    Json(req): Json<TraceWithGraphRequest>,
) -> Result<Json<Value>, ApiError> {
    let graph = build_graph_from_edges(&req.edges);
    let column_ref = ColumnRef::new(&req.target.task_id, &req.target.column_name);
    let trace = graph.trace_upstream(&column_ref);

    Ok(Json(json!({
        "target": {
            "task_id": req.target.task_id,
            "column_name": req.target.column_name,
        },
        "direction": "upstream",
        "columns": trace.columns.iter().map(|c| json!({
            "task_id": c.qualifier(),
            "column_name": &c.column_name,
        })).collect::<Vec<_>>(),
        "depth": trace.columns.len(),
    })))
}

/// POST /api/v1/lineage/trace/downstream — Trace downstream dependents of a column.
pub async fn trace_downstream(
    State(_state): State<Arc<AppState>>,
    Json(req): Json<TraceWithGraphRequest>,
) -> Result<Json<Value>, ApiError> {
    let graph = build_graph_from_edges(&req.edges);
    let column_ref = ColumnRef::new(&req.target.task_id, &req.target.column_name);
    let trace = graph.trace_downstream(&column_ref);

    Ok(Json(json!({
        "target": {
            "task_id": req.target.task_id,
            "column_name": req.target.column_name,
        },
        "direction": "downstream",
        "columns": trace.columns.iter().map(|c| json!({
            "task_id": c.qualifier(),
            "column_name": &c.column_name,
        })).collect::<Vec<_>>(),
        "depth": trace.columns.len(),
    })))
}

/// POST /api/v1/lineage/graph — Build and return a full lineage graph for visualization.
///
/// Accepts a set of edges and returns D3.js-compatible visualization data
/// with nodes (columns) and links (lineage edges).
pub async fn lineage_graph(
    State(_state): State<Arc<AppState>>,
    Json(req): Json<GraphRequest>,
) -> Result<Json<Value>, ApiError> {
    let graph = build_graph_from_edges(&req.edges);
    let viz = graph.to_visualization_data();

    Ok(Json(json!({
        "stats": {
            "columns": graph.column_count(),
            "edges": graph.edge_count(),
            "tasks": graph.tasks().len(),
        },
        "visualization": viz,
    })))
}

/// POST /api/v1/lineage/schema/diff — Compare two schema versions and detect changes.
///
/// Returns classified changes: Added, Removed, TypeChanged, NullabilityChanged, Renamed.
pub async fn schema_diff(
    State(_state): State<Arc<AppState>>,
    Json(req): Json<SchemaDiffRequest>,
) -> Result<Json<Value>, ApiError> {
    let old_schema = schema_from_input(&req.old_schema);
    let new_schema = schema_from_input(&req.new_schema);

    let changes = SchemaChangeDetector::diff(&old_schema, &new_schema);

    let change_list: Vec<Value> = changes
        .iter()
        .map(|c| {
            json!({
                "column": c.column_name,
                "kind": format!("{:?}", c.kind),
                "breaking": c.is_breaking,
                "description": c.description,
            })
        })
        .collect();

    let breaking_count = changes.iter().filter(|c| c.is_breaking).count();

    Ok(Json(json!({
        "task_id": req.old_schema.task_id,
        "total_changes": changes.len(),
        "breaking_changes": breaking_count,
        "changes": change_list,
    })))
}

/// POST /api/v1/lineage/contracts/validate — Validate a schema against a contract.
///
/// Checks required columns, forbidden columns, documentation requirements,
/// max column limits, and unknown types. Returns violations with severity.
pub async fn validate_contract(
    State(_state): State<Arc<AppState>>,
    Json(req): Json<ContractValidateRequest>,
) -> Result<Json<Value>, ApiError> {
    let schema = schema_from_input(&req.schema);
    let contract = contract_from_input(&req.contract);

    let result = ContractValidator::validate(&schema, &contract);

    let error_count = result
        .violations
        .iter()
        .filter(|v| v.severity == conduit_lineage::contracts::ViolationSeverity::Error)
        .count();
    let warning_count = result.violations.len() - error_count;

    let violations: Vec<Value> = result
        .violations
        .iter()
        .map(|v| {
            json!({
                "rule": format!("{:?}", v.rule),
                "message": v.message,
                "severity": format!("{:?}", v.severity),
            })
        })
        .collect();

    Ok(Json(json!({
        "task_id": req.contract.task_id,
        "passed": result.passed,
        "violations": violations,
        "rules_checked": result.rules_checked,
        "rules_passed": result.rules_passed,
        "error_count": error_count,
        "warning_count": warning_count,
    })))
}

// ─── Internal helpers ────────────────────────────────────────────────────────

fn build_openlineage_event(
    req: &SqlLineageRequest,
    opts: &OpenLineageSqlRequest,
    lineage: &conduit_lineage::sql_parser::SqlLineage,
) -> Result<OpenLineageRunEvent, ApiError> {
    let event_type = opts
        .event_type
        .as_deref()
        .map(|s| {
            OpenLineageEventType::parse(s).ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "Invalid OpenLineage event_type '{}'. Expected START, RUNNING, COMPLETE, ABORT, FAIL, or OTHER",
                    s
                ))
            })
        })
        .transpose()?
        .unwrap_or(OpenLineageEventType::Complete);

    let event_time = if let Some(ref event_time) = opts.event_time {
        DateTime::parse_from_rfc3339(event_time).map_err(|_| {
            ApiError::BadRequest(format!(
                "Invalid OpenLineage event_time '{}'. Expected RFC3339 timestamp",
                event_time
            ))
        })?;
        event_time.clone()
    } else {
        Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
    };

    let run_id = if let Some(ref run_id) = opts.run_id {
        Uuid::parse_str(run_id)
            .map_err(|_| {
                ApiError::BadRequest(format!(
                    "Invalid OpenLineage run_id '{}'. Expected UUID",
                    run_id
                ))
            })?
            .to_string()
    } else {
        Uuid::new_v4().to_string()
    };

    let options = OpenLineageSqlEventOptions {
        event_type,
        event_time,
        run_id,
        job_namespace: opts
            .job_namespace
            .clone()
            .unwrap_or_else(|| "conduit".to_string()),
        job_name: opts
            .job_name
            .clone()
            .unwrap_or_else(|| req.source_task_id.clone()),
        dataset_namespace: opts
            .dataset_namespace
            .clone()
            .or_else(|| req.connection.clone())
            .unwrap_or_else(|| "conduit".to_string()),
        output_dataset: opts.output_dataset.clone(),
        producer: opts
            .producer
            .clone()
            .unwrap_or_else(|| CONDUIT_OPENLINEAGE_PRODUCER.to_string()),
    };

    Ok(OpenLineageRunEvent::from_sql_lineage(lineage, options))
}

/// Build a TableCatalog from inline table schemas in the request.
fn build_catalog_from_inline(tables: &[TableSchemaInput]) -> TableCatalog {
    let mut catalog = TableCatalog::new();
    for table in tables {
        let columns = table
            .columns
            .iter()
            .map(|c| CatalogColumn::new(&c.name, parse_sql_type(&c.data_type)))
            .collect();
        catalog.register_table(table.schema.as_deref(), &table.table, columns);
    }
    catalog
}

/// Build a TableCatalog by introspecting a SQL provider for specific tables.
async fn build_catalog_from_provider(
    state: &AppState,
    connection: &str,
    source_tables: &[conduit_lineage::sql_parser::TableRef],
) -> Result<TableCatalog, ApiError> {
    // Get the SQL provider Arc (clone to drop lock before await)
    let provider = {
        let guard = state
            .provider_registry
            .read()
            .map_err(|_| ApiError::Internal("Failed to read provider registry".into()))?;
        let registry = guard
            .as_ref()
            .ok_or_else(|| ApiError::NotFound("Provider registry not initialized".into()))?;
        let instance = registry
            .get(connection)
            .ok_or_else(|| ApiError::NotFound(format!("Connection '{}' not found", connection)))?;
        match instance {
            ProviderInstance::Sql(p) => p.clone(),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Connection '{}' is not a SQL provider",
                    connection
                )));
            }
        }
    };

    let mut catalog = TableCatalog::new();
    for table in source_tables {
        let schema = table.schema.as_deref().unwrap_or("public");
        match provider.describe_table(schema, &table.name).await {
            Ok(columns) => {
                let cat_columns = columns
                    .into_iter()
                    .map(|c| {
                        let mut col = CatalogColumn::new(&c.name, parse_sql_type(&c.data_type));
                        if !c.is_nullable {
                            col = col.not_null();
                        }
                        col
                    })
                    .collect();
                catalog.register_table(table.schema.as_deref(), &table.name, cat_columns);
            }
            Err(e) => {
                tracing::warn!(
                    connection = connection,
                    table = %table.name,
                    error = %e,
                    "Failed to describe table for catalog"
                );
            }
        }
    }

    Ok(catalog)
}

#[derive(Deserialize)]
pub struct TraceWithGraphRequest {
    pub target: TraceRequest,
    pub edges: Vec<EdgeInput>,
}

#[derive(Deserialize)]
pub struct GraphRequest {
    pub edges: Vec<EdgeInput>,
}

#[derive(Deserialize)]
pub struct EdgeInput {
    pub from_task: String,
    pub from_column: String,
    pub to_task: String,
    pub to_column: String,
    #[serde(default = "default_transform")]
    pub transform: String,
}

fn default_transform() -> String {
    "Direct".to_string()
}

fn build_graph_from_edges(edges: &[EdgeInput]) -> LineageGraph {
    let mut graph = LineageGraph::new();
    for edge in edges {
        let transform = match edge.transform.to_lowercase().as_str() {
            "aggregation" => {
                conduit_lineage::lineage_graph::TransformType::Aggregation("unknown".to_string())
            }
            "computation" => conduit_lineage::lineage_graph::TransformType::Computation,
            "cast" => conduit_lineage::lineage_graph::TransformType::Cast,
            "filter" => conduit_lineage::lineage_graph::TransformType::Filter,
            "joinkey" | "join_key" => conduit_lineage::lineage_graph::TransformType::JoinKey,
            _ => conduit_lineage::lineage_graph::TransformType::Direct,
        };
        graph.add_edge(
            ColumnRef::new(&edge.from_task, &edge.from_column),
            ColumnRef::new(&edge.to_task, &edge.to_column),
            transform,
        );
    }
    graph
}

fn parse_column_type(s: &str) -> ColumnType {
    match s.to_uppercase().as_str() {
        "STRING" | "TEXT" | "VARCHAR" => ColumnType::String,
        "INT" | "INTEGER" | "BIGINT" => ColumnType::Integer,
        "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" => ColumnType::Float,
        "BOOL" | "BOOLEAN" => ColumnType::Boolean,
        "DATE" => ColumnType::Date,
        "TIMESTAMP" | "DATETIME" => ColumnType::Timestamp,
        "JSON" | "JSONB" => ColumnType::Json,
        "BINARY" | "BLOB" | "BYTES" => ColumnType::Binary,
        "ARRAY" => ColumnType::Array(Box::new(ColumnType::String)),
        _ => ColumnType::Unknown,
    }
}

fn schema_from_input(input: &SchemaInput) -> Schema {
    let columns: Vec<Column> = input
        .columns
        .iter()
        .map(|c| {
            let mut col = Column::new(&c.name, parse_column_type(&c.column_type));
            if !c.nullable {
                col = col.not_null();
            }
            if let Some(desc) = &c.description {
                col = col.with_description(desc);
            }
            col
        })
        .collect();

    Schema::new(&input.task_id, columns)
}

fn contract_from_input(input: &ContractInput) -> SchemaContract {
    let mut contract = SchemaContract::new(&input.task_id);

    for rc in &input.required_columns {
        let col_type = rc.expected_type.as_deref().map(parse_column_type);
        contract = contract.require_column(&rc.name, col_type, false);
    }

    for fc in &input.forbidden_columns {
        contract = contract.forbid_column(fc, "Forbidden by contract");
    }

    if let Some(max) = input.max_columns {
        contract = contract.max_columns(max);
    }

    if input.require_docs {
        contract = contract.require_docs();
    }

    if input.no_unknown_types {
        contract = contract.no_unknown_types();
    }

    contract
}
