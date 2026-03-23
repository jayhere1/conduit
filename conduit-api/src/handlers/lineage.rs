//! Lineage API handlers.
//!
//! Provides endpoints for column-level lineage tracing, schema inspection,
//! impact analysis, and schema contract validation.

use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use conduit_lineage::{
    ColumnRef, ColumnType, Column, LineageGraph, Schema, SchemaChangeDetector,
    SchemaContract, ContractValidator, SqlLineageExtractor,
};

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
/// Takes a SQL query and returns the extracted column references,
/// source tables, joins, and derived column mappings.
pub async fn extract_sql_lineage(
    State(_state): State<Arc<AppState>>,
    Json(req): Json<SqlLineageRequest>,
) -> Result<Json<Value>, ApiError> {
    let lineage = SqlLineageExtractor::extract(&req.sql);

    let output_cols: Vec<Value> = lineage.output_columns.iter().map(|c| json!({
        "name": c.name,
        "expression": c.expression,
        "is_computed": c.is_computed,
    })).collect();

    let source_tables: Vec<Value> = lineage.source_tables.iter().map(|t| json!({
        "name": t.name,
        "alias": t.alias,
        "schema": t.schema,
    })).collect();

    let mappings: Vec<Value> = lineage.column_mappings.iter().map(|m| json!({
        "output": m.output,
        "inputs": m.inputs,
    })).collect();

    Ok(Json(json!({
        "source_task_id": req.source_task_id,
        "sql": req.sql,
        "output_columns": output_cols,
        "source_tables": source_tables,
        "column_mappings": mappings,
    })))
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
            "task_id": &c.task_id,
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
            "task_id": &c.task_id,
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
            "aggregation" => conduit_lineage::lineage_graph::TransformType::Aggregation("unknown".to_string()),
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
