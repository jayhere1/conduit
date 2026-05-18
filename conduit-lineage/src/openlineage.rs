//! OpenLineage event generation for Conduit lineage.
//!
//! This module maps Conduit's SQL lineage extraction output into an
//! OpenLineage RunEvent with a columnLineage dataset facet. It deliberately
//! stops at JSON event construction; transport to Marquez/DataHub/etc. belongs
//! at the API/CLI integration layer.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::catalog::TableCatalog;
use crate::sql_parser::{OutputColumn, SqlLineage, TableRef};

/// OpenLineage RunEvent schema URL used by generated events.
pub const OPENLINEAGE_RUN_EVENT_SCHEMA_URL: &str =
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";

/// OpenLineage columnLineage dataset facet schema URL.
pub const COLUMN_LINEAGE_FACET_SCHEMA_URL: &str =
    "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json";

/// Schema URL for Conduit's custom `conduit_task_lineage` facet, which
/// names the upstream tasks that produced each output column. Not part
/// of the OpenLineage spec.
pub const CONDUIT_TASK_LINEAGE_FACET_SCHEMA_URL: &str =
    "https://conduit.dev/schemas/conduit_task_lineage/v1";

/// Default producer URI for Conduit-generated OpenLineage metadata.
pub const CONDUIT_OPENLINEAGE_PRODUCER: &str = "https://github.com/conduit-orchestrator/conduit";

/// Format the OpenLineage namespace used for task-produced datasets:
/// `conduit://<dag_id>`. Physical-table inputs keep the caller-supplied
/// namespace; only datasets resolved back to a task get this scheme.
pub fn conduit_task_namespace(dag_id: &str) -> String {
    format!("conduit://{}", dag_id)
}

/// Run state transition for an OpenLineage RunEvent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OpenLineageEventType {
    Start,
    Running,
    Complete,
    Abort,
    Fail,
    Other,
}

impl OpenLineageEventType {
    /// Parse an OpenLineage event type name, case-insensitive.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "START" => Some(Self::Start),
            "RUNNING" => Some(Self::Running),
            "COMPLETE" => Some(Self::Complete),
            "ABORT" => Some(Self::Abort),
            "FAIL" => Some(Self::Fail),
            "OTHER" => Some(Self::Other),
            _ => None,
        }
    }
}

/// Options required to build an OpenLineage RunEvent from SQL lineage.
#[derive(Debug, Clone)]
pub struct OpenLineageSqlEventOptions {
    /// Run state transition represented by this event.
    pub event_type: OpenLineageEventType,
    /// RFC3339 event timestamp.
    pub event_time: String,
    /// OpenLineage run ID. The spec expects a UUID string.
    pub run_id: String,
    /// Namespace containing the Conduit job.
    pub job_namespace: String,
    /// Unique job name within `job_namespace`.
    pub job_name: String,
    /// Namespace for input and output datasets.
    pub dataset_namespace: String,
    /// Output dataset name, for example `analytics.customer_daily`.
    pub output_dataset: String,
    /// Producer URI for the event and facets.
    pub producer: String,
}

/// OpenLineage RunEvent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageRunEvent {
    pub event_time: String,
    pub producer: String,
    #[serde(rename = "schemaURL")]
    pub schema_url: String,
    pub event_type: OpenLineageEventType,
    pub run: OpenLineageRun,
    pub job: OpenLineageJob,
    pub inputs: Vec<OpenLineageDataset>,
    pub outputs: Vec<OpenLineageDataset>,
}

/// OpenLineage Run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageRun {
    pub run_id: String,
}

/// OpenLineage Job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenLineageJob {
    pub namespace: String,
    pub name: String,
}

/// OpenLineage input or output dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenLineageDataset {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub facets: BTreeMap<String, Value>,
}

/// OpenLineage columnLineage dataset facet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLineageDatasetFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    pub fields: BTreeMap<String, ColumnLineageField>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub dataset: Vec<OpenLineageInputField>,
}

/// Lineage for one output field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnLineageField {
    pub input_fields: Vec<OpenLineageInputField>,
}

/// OpenLineage input field dependency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenLineageInputField {
    pub namespace: String,
    pub name: String,
    pub field: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub transformations: Vec<OpenLineageTransformation>,
}

/// OpenLineage column transformation descriptor.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OpenLineageTransformation {
    #[serde(rename = "type")]
    pub transformation_type: String,
    pub subtype: String,
    pub description: String,
    pub masking: bool,
}

/// Conduit-specific facet (not in the OpenLineage spec) recording which
/// upstream tasks produced each output column. This is the cross-task
/// lineage bit that OpenLineage's table-centric model can't express.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConduitTaskLineageFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    /// One entry per output column.
    pub fields: BTreeMap<String, Vec<ConduitProducerTask>>,
}

/// An upstream task that contributed a column to an output field.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConduitProducerTask {
    pub dag_id: String,
    pub task_id: String,
    pub column: String,
}

impl OpenLineageRunEvent {
    /// Build an OpenLineage RunEvent with an output columnLineage facet.
    pub fn from_sql_lineage(lineage: &SqlLineage, options: OpenLineageSqlEventOptions) -> Self {
        Self::from_sql_lineage_inner(lineage, options, None)
    }

    /// Like [`Self::from_sql_lineage`], but resolves SQL-extracted column
    /// references against `catalog` so that:
    /// 1. Input `namespace` becomes `conduit://<dag_id>` for columns whose
    ///    source table is actually a task-produced dataset.
    /// 2. A `conduit_task_lineage` facet is attached to the output
    ///    dataset, listing each producer task per column.
    ///
    /// Physical-table inputs are unaffected — they keep the
    /// caller-supplied `dataset_namespace`.
    pub fn from_sql_lineage_with_catalog(
        lineage: &SqlLineage,
        options: OpenLineageSqlEventOptions,
        catalog: &TableCatalog,
    ) -> Self {
        Self::from_sql_lineage_inner(lineage, options, Some(catalog))
    }

    fn from_sql_lineage_inner(
        lineage: &SqlLineage,
        options: OpenLineageSqlEventOptions,
        catalog: Option<&TableCatalog>,
    ) -> Self {
        let mut input_names = BTreeSet::new();
        for table in &lineage.source_tables {
            input_names.insert(dataset_name_for_table(table));
        }

        let inputs: Vec<OpenLineageDataset> = input_names
            .into_iter()
            .map(|name| {
                let producer = catalog.and_then(|c| c.lookup_producer(&name));
                let namespace = match producer {
                    Some(p) => conduit_task_namespace(&p.dag_id),
                    None => options.dataset_namespace.clone(),
                };
                OpenLineageDataset {
                    namespace,
                    name,
                    facets: BTreeMap::new(),
                }
            })
            .collect();

        let column_lineage = build_column_lineage_facet(lineage, &options, catalog);
        let mut output_facets = BTreeMap::new();
        output_facets.insert(
            "columnLineage".to_string(),
            serde_json::to_value(column_lineage).expect("column lineage facet serializes"),
        );

        if let Some(cat) = catalog {
            if let Some(facet) = build_conduit_task_lineage_facet(lineage, &options, cat) {
                output_facets.insert(
                    "conduit_task_lineage".to_string(),
                    serde_json::to_value(facet).expect("task lineage facet serializes"),
                );
            }
        }

        let outputs = vec![OpenLineageDataset {
            namespace: options.dataset_namespace.clone(),
            name: options.output_dataset.clone(),
            facets: output_facets,
        }];

        Self {
            event_time: options.event_time,
            producer: options.producer,
            schema_url: OPENLINEAGE_RUN_EVENT_SCHEMA_URL.to_string(),
            event_type: options.event_type,
            run: OpenLineageRun {
                run_id: options.run_id,
            },
            job: OpenLineageJob {
                namespace: options.job_namespace,
                name: options.job_name,
            },
            inputs,
            outputs,
        }
    }
}

fn build_column_lineage_facet(
    lineage: &SqlLineage,
    options: &OpenLineageSqlEventOptions,
    catalog: Option<&TableCatalog>,
) -> ColumnLineageDatasetFacet {
    let source_tables: HashMap<&str, &TableRef> = lineage
        .source_tables
        .iter()
        .map(|t| (t.name.as_str(), t))
        .collect();
    let output_columns: HashMap<&str, &OutputColumn> = lineage
        .output_columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    let mut fields: BTreeMap<String, ColumnLineageField> = lineage
        .output_columns
        .iter()
        .filter(|c| include_output_field(&c.name))
        .map(|c| {
            (
                c.name.clone(),
                ColumnLineageField {
                    input_fields: Vec::new(),
                },
            )
        })
        .collect();

    let mut dataset_inputs = Vec::new();

    for mapping in &lineage.column_mappings {
        if mapping.output == "__where__" {
            let transform = OpenLineageTransformation {
                transformation_type: "INDIRECT".to_string(),
                subtype: "FILTER".to_string(),
                description: "WHERE predicate".to_string(),
                masking: false,
            };
            dataset_inputs.extend(mapping.inputs.iter().map(|input| {
                input_field_for_ref(
                    input,
                    &source_tables,
                    &options.dataset_namespace,
                    transform.clone(),
                    catalog,
                )
            }));
            continue;
        }

        if !include_output_field(&mapping.output) {
            continue;
        }

        let output = output_columns.get(mapping.output.as_str()).copied();
        let transform = transformation_for_output(output);
        let input_fields: Vec<_> = mapping
            .inputs
            .iter()
            .map(|input| {
                input_field_for_ref(
                    input,
                    &source_tables,
                    &options.dataset_namespace,
                    transform.clone(),
                    catalog,
                )
            })
            .collect();

        fields
            .entry(mapping.output.clone())
            .or_insert_with(|| ColumnLineageField {
                input_fields: Vec::new(),
            })
            .input_fields
            .extend(input_fields);
    }

    for field in fields.values_mut() {
        dedupe_input_fields(&mut field.input_fields);
    }
    dedupe_input_fields(&mut dataset_inputs);

    ColumnLineageDatasetFacet {
        producer: options.producer.clone(),
        schema_url: COLUMN_LINEAGE_FACET_SCHEMA_URL.to_string(),
        fields,
        dataset: dataset_inputs,
    }
}

fn include_output_field(name: &str) -> bool {
    !name.starts_with("__") && name != "*"
}

fn dataset_name_for_table(table: &TableRef) -> String {
    match &table.schema {
        Some(schema) if !schema.is_empty() => format!("{}.{}", schema, table.name),
        _ => table.name.clone(),
    }
}

fn input_field_for_ref(
    input: &crate::lineage_graph::ColumnRef,
    source_tables: &HashMap<&str, &TableRef>,
    dataset_namespace: &str,
    transform: OpenLineageTransformation,
    catalog: Option<&TableCatalog>,
) -> OpenLineageInputField {
    let qualifier = input.qualifier();
    let dataset_name = source_tables
        .get(qualifier.as_str())
        .map(|table| dataset_name_for_table(table))
        .unwrap_or_else(|| qualifier.clone());

    // If the catalog identifies this dataset as task-produced, switch
    // the namespace to `conduit://<dag_id>` so downstream OL consumers
    // see this column came from a Conduit pipeline rather than a
    // physical warehouse table.
    let namespace = match catalog.and_then(|c| c.lookup_producer(&dataset_name)) {
        Some(p) => conduit_task_namespace(&p.dag_id),
        None => dataset_namespace.to_string(),
    };

    OpenLineageInputField {
        namespace,
        name: dataset_name,
        field: input.column_name.clone(),
        transformations: vec![transform],
    }
}

/// Build the Conduit-specific `conduit_task_lineage` facet: per output
/// column, the list of `{dagId, taskId, column}` producers. Returns
/// `None` when no input column resolves to a task producer (so the
/// facet stays absent for purely physical-table-driven SQL).
fn build_conduit_task_lineage_facet(
    lineage: &SqlLineage,
    options: &OpenLineageSqlEventOptions,
    catalog: &TableCatalog,
) -> Option<ConduitTaskLineageFacet> {
    let mut fields: BTreeMap<String, Vec<ConduitProducerTask>> = BTreeMap::new();

    for mapping in &lineage.column_mappings {
        if !include_output_field(&mapping.output) {
            continue;
        }
        for input in &mapping.inputs {
            let qualifier = input.qualifier();
            let Some(producer) = catalog.lookup_producer(&qualifier) else {
                continue;
            };
            fields
                .entry(mapping.output.clone())
                .or_default()
                .push(ConduitProducerTask {
                    dag_id: producer.dag_id.clone(),
                    task_id: producer.task_id.clone(),
                    column: input.column_name.clone(),
                });
        }
    }

    if fields.is_empty() {
        return None;
    }

    // Dedupe + stable order so the facet is deterministic across runs.
    for v in fields.values_mut() {
        v.sort();
        v.dedup();
    }

    Some(ConduitTaskLineageFacet {
        producer: options.producer.clone(),
        schema_url: CONDUIT_TASK_LINEAGE_FACET_SCHEMA_URL.to_string(),
        fields,
    })
}

fn transformation_for_output(output: Option<&OutputColumn>) -> OpenLineageTransformation {
    let subtype = match output {
        Some(col) if looks_like_aggregation(&col.expression) => "AGGREGATION",
        Some(col) if col.is_computed => "TRANSFORMATION",
        _ => "IDENTITY",
    };

    OpenLineageTransformation {
        transformation_type: "DIRECT".to_string(),
        subtype: subtype.to_string(),
        description: output.map(|c| c.expression.clone()).unwrap_or_default(),
        masking: false,
    }
}

fn looks_like_aggregation(expr: &str) -> bool {
    let expr = expr.to_ascii_lowercase();
    ["sum(", "count(", "avg(", "min(", "max("]
        .iter()
        .any(|needle| expr.contains(needle))
}

fn dedupe_input_fields(fields: &mut Vec<OpenLineageInputField>) {
    let mut seen = BTreeSet::new();
    fields.retain(|f| {
        let key = (
            f.namespace.clone(),
            f.name.clone(),
            f.field.clone(),
            f.transformations.clone(),
        );
        seen.insert(key)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_parser::SqlLineageExtractor;

    fn opts() -> OpenLineageSqlEventOptions {
        OpenLineageSqlEventOptions {
            event_type: OpenLineageEventType::Complete,
            event_time: "2026-05-17T12:00:00Z".to_string(),
            run_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            job_namespace: "conduit".to_string(),
            job_name: "daily_etl.transform_orders".to_string(),
            dataset_namespace: "warehouse".to_string(),
            output_dataset: "analytics.order_summary".to_string(),
            producer: CONDUIT_OPENLINEAGE_PRODUCER.to_string(),
        }
    }

    #[test]
    fn emits_run_event_with_column_lineage_facet() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT customer_id, SUM(amount) AS total FROM raw.orders WHERE status = 'paid'",
        );

        let event = OpenLineageRunEvent::from_sql_lineage(&lineage, opts());
        let json = serde_json::to_value(event).unwrap();

        assert_eq!(json["eventType"], "COMPLETE");
        assert_eq!(json["schemaURL"], OPENLINEAGE_RUN_EVENT_SCHEMA_URL);
        assert_eq!(json["run"]["runId"], "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(json["job"]["name"], "daily_etl.transform_orders");
        assert_eq!(json["inputs"][0]["namespace"], "warehouse");
        assert_eq!(json["inputs"][0]["name"], "raw.orders");
        assert_eq!(json["outputs"][0]["name"], "analytics.order_summary");

        let facet = &json["outputs"][0]["facets"]["columnLineage"];
        assert_eq!(facet["_schemaURL"], COLUMN_LINEAGE_FACET_SCHEMA_URL);
        assert_eq!(
            facet["fields"]["customer_id"]["inputFields"][0]["field"],
            "customer_id"
        );
        assert_eq!(
            facet["fields"]["total"]["inputFields"][0]["transformations"][0]["subtype"],
            "AGGREGATION"
        );
        assert_eq!(
            facet["dataset"][0]["transformations"][0]["subtype"],
            "FILTER"
        );
    }

    #[test]
    fn conduit_task_lineage_facet_lists_producer_tasks() {
        use crate::catalog::{CatalogColumn, TableCatalog};
        use crate::lineage_graph::TaskRef;
        use crate::schema::ColumnType;

        // Build a catalog where staging.orders is produced by a task.
        let mut catalog = TableCatalog::new();
        catalog.register_dataset(
            "staging.orders",
            vec![
                CatalogColumn::new("customer_id", ColumnType::Integer),
                CatalogColumn::new("amount", ColumnType::Float),
            ],
            TaskRef::new("warehouse", "extract_orders"),
        );

        let lineage = SqlLineageExtractor::extract_with_catalog(
            "SELECT customer_id, SUM(amount) AS total FROM staging.orders GROUP BY customer_id",
            &catalog,
        );

        let mut o = opts();
        o.output_dataset = "analytics.daily_revenue".to_string();
        o.job_name = "warehouse.transform".to_string();

        let event = OpenLineageRunEvent::from_sql_lineage_with_catalog(&lineage, o, &catalog);
        let json = serde_json::to_value(event).unwrap();

        // Input namespace promoted to conduit://<dag_id>.
        assert_eq!(json["inputs"][0]["namespace"], "conduit://warehouse");
        assert_eq!(json["inputs"][0]["name"], "staging.orders");

        // conduit_task_lineage facet is present and lists the producer.
        let facet = &json["outputs"][0]["facets"]["conduit_task_lineage"];
        assert_eq!(facet["_schemaURL"], CONDUIT_TASK_LINEAGE_FACET_SCHEMA_URL);
        let producers = &facet["fields"]["total"];
        assert!(producers.is_array(), "expected array of producers");
        let entry = &producers[0];
        assert_eq!(entry["dagId"], "warehouse");
        assert_eq!(entry["taskId"], "extract_orders");
        assert_eq!(entry["column"], "amount");

        // columnLineage input field's namespace was also promoted.
        let cl_input =
            &json["outputs"][0]["facets"]["columnLineage"]["fields"]["total"]["inputFields"][0];
        assert_eq!(cl_input["namespace"], "conduit://warehouse");
        assert_eq!(cl_input["name"], "staging.orders");
        assert_eq!(cl_input["field"], "amount");
    }

    #[test]
    fn task_lineage_facet_absent_when_no_producer() {
        use crate::catalog::TableCatalog;
        let catalog = TableCatalog::new();
        let lineage = SqlLineageExtractor::extract("SELECT a FROM public.t");
        let event = OpenLineageRunEvent::from_sql_lineage_with_catalog(&lineage, opts(), &catalog);
        let json = serde_json::to_value(event).unwrap();
        assert!(
            json["outputs"][0]["facets"]
                .get("conduit_task_lineage")
                .is_none(),
            "facet should be omitted when no input resolves to a task producer"
        );
    }

    #[test]
    fn parses_event_type_case_insensitively() {
        assert_eq!(
            OpenLineageEventType::parse("complete"),
            Some(OpenLineageEventType::Complete)
        );
        assert_eq!(OpenLineageEventType::parse("unknown"), None);
    }
}
