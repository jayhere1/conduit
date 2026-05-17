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
pub mod contracts;
pub mod impact;
pub mod lineage_graph;
pub mod openlineage;
pub mod schema;
pub mod sql_parser;

pub use catalog::{parse_sql_type, CatalogColumn, TableCatalog};
pub use contracts::{ContractValidator, ContractViolation, SchemaContract};
pub use impact::{ChangeKind as SchemaChangeKind, SchemaChange, SchemaChangeDetector};
pub use lineage_graph::{ColumnRef, LineageEdge, LineageGraph};
pub use openlineage::{
    ColumnLineageDatasetFacet, OpenLineageEventType, OpenLineageRunEvent,
    OpenLineageSqlEventOptions, COLUMN_LINEAGE_FACET_SCHEMA_URL, CONDUIT_OPENLINEAGE_PRODUCER,
    OPENLINEAGE_RUN_EVENT_SCHEMA_URL,
};
pub use schema::{Column, ColumnType, Schema, SchemaRegistry};
pub use sql_parser::SqlLineageExtractor;
