//! conduit-lineage: Column-level lineage and schema validation.
//!
//! Traces data flow at the column level through task schemas, enabling:
//! 1. **Column-level lineage**: "Which upstream columns feed into this output column?"
//! 2. **Cross-task lineage**: stitches column flow *through* the task
//!    graph so a single trace can walk Python → SQL → Python, returning
//!    edges annotated by upstream task kind.
//! 3. **Impact analysis**: "If I rename column X, which downstream tasks break?"
//! 4. **Breaking change detection**: Column additions, removals, type changes
//! 5. **Schema contracts**: Validate that task outputs match declared schemas
//! 6. **OpenLineage emit** with a Conduit-specific
//!    `conduit_task_lineage` facet recording the producer task per
//!    output column.
//!
//! Airflow's core model is task-centric; column lineage there lives in
//! OpenLineage integrations layered on top. Conduit traces column flow
//! as part of compilation, so lineage stays in sync with the DAG without
//! a separate integration. Dagster's primary model is asset-graph rather
//! than column-graph, which is a different (and complementary) framing.

pub mod catalog;
pub mod contracts;
pub mod cross_task;
pub mod impact;
pub mod impact_report;
pub mod lineage_graph;
pub mod openlineage;
pub mod openlineage_ingest;
pub mod plan_impact;
pub mod schema;
pub mod dbt_manifest;
pub mod sql_parser;

pub use catalog::{parse_sql_type, CatalogColumn, TableCatalog};
pub use contracts::{ContractValidator, ContractViolation, SchemaContract};
pub use cross_task::{
    stitch, CrossTaskLineage, LineageStrictError, UnresolvedReason, UnresolvedRef,
};
pub use impact::{ChangeKind as SchemaChangeKind, SchemaChange, SchemaChangeDetector};
pub use impact_report::{
    render as render_impact, render_markdown as render_impact_markdown, ImpactFormat,
};
pub use lineage_graph::{ColumnRef, ColumnSource, LineageEdge, LineageGraph, TaskRef};
pub use openlineage::{
    ColumnLineageDatasetFacet, OpenLineageEventType, OpenLineageRunEvent,
    OpenLineageSqlEventOptions, COLUMN_LINEAGE_FACET_SCHEMA_URL, CONDUIT_OPENLINEAGE_PRODUCER,
    OPENLINEAGE_RUN_EVENT_SCHEMA_URL,
};
pub use openlineage_ingest::{
    extract_column_edges, extract_columns_from_schema_facet, flatten_event, qualify_dataset,
    update_dataset, Backend as ExternalLineageBackend, ExternalColumn, ExternalColumnEdge,
    ExternalDatasetSummary, ExternalLineageStore, ExternalProducerRef, ExternalStoreStats,
    InMemoryBackend as InMemoryExternalLineageBackend, IngestResult, IngestedEvent,
};
pub use plan_impact::{
    analyze as analyze_plan_impact, DagSet, DownstreamColumn, LineageCoverage, PlanImpact,
    PlanImpactSummary, TaskImpact,
};

/// Cross-crate test helpers for the [`ExternalLineageBackend`] trait.
/// Behind the `testing` feature so production builds don't pay for it.
#[cfg(feature = "testing")]
pub use openlineage_ingest::testing;
pub use schema::{Column, ColumnType, Schema, SchemaRegistry};
pub use dbt_manifest::{DbtManifest, DbtManifestError, DbtNode, DbtSource};
pub use sql_parser::{SqlDialect, SqlLineageExtractor};
