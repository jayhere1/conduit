//! conduit-lineage: Column-level lineage and schema validation.
//!
//! Traces data flow at the column level through task schemas, enabling:
//! 1. **Column-level lineage**: "Which upstream columns feed into this output column?"
//! 2. **Impact analysis**: "If I rename column X, which downstream tasks break?"
//! 3. **Breaking change detection**: Column additions, removals, type changes
//! 4. **Schema contracts**: Validate that task outputs match declared schemas
//! 5. **Lineage visualization**: Graph data for rendering column-level flow
//!
//! This is architecturally impossible in Airflow — they operate at the
//! task level only. Conduit traces data flow through SQL queries, Python
//! transforms, and schema declarations to build a column-level dependency graph.

pub mod catalog;
pub mod schema;
pub mod sql_parser;
pub mod lineage_graph;
pub mod impact;
pub mod contracts;

pub use catalog::{CatalogColumn, TableCatalog, parse_sql_type};
pub use schema::{Schema, Column, ColumnType, SchemaRegistry};
pub use sql_parser::SqlLineageExtractor;
pub use lineage_graph::{LineageGraph, LineageEdge, ColumnRef};
pub use impact::{SchemaChangeDetector, SchemaChange, ChangeKind as SchemaChangeKind};
pub use contracts::{SchemaContract, ContractViolation, ContractValidator};
