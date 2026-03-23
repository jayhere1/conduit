//! Schema types and registry.
//!
//! A Schema describes the columns produced by a task — their names, types,
//! nullability, and optional descriptions. Schemas can be declared explicitly
//! (via YAML or Python annotations) or inferred from SQL queries.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A column data type.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnType {
    String,
    Integer,
    Float,
    Boolean,
    Date,
    Timestamp,
    Decimal { precision: u8, scale: u8 },
    Array(Box<ColumnType>),
    Struct(Vec<Column>),
    Json,
    Binary,
    /// Unknown or not yet inferred.
    Unknown,
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnType::String => write!(f, "STRING"),
            ColumnType::Integer => write!(f, "INTEGER"),
            ColumnType::Float => write!(f, "FLOAT"),
            ColumnType::Boolean => write!(f, "BOOLEAN"),
            ColumnType::Date => write!(f, "DATE"),
            ColumnType::Timestamp => write!(f, "TIMESTAMP"),
            ColumnType::Decimal { precision, scale } => write!(f, "DECIMAL({},{})", precision, scale),
            ColumnType::Array(inner) => write!(f, "ARRAY<{}>", inner),
            ColumnType::Struct(fields) => {
                let names: Vec<_> = fields.iter().map(|c| c.name.as_str()).collect();
                write!(f, "STRUCT<{}>", names.join(", "))
            }
            ColumnType::Json => write!(f, "JSON"),
            ColumnType::Binary => write!(f, "BINARY"),
            ColumnType::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

/// A single column in a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// Column data type.
    pub column_type: ColumnType,
    /// Whether the column can be NULL.
    pub nullable: bool,
    /// Human-readable description.
    pub description: Option<String>,
    /// Tags for classification (e.g., "pii", "metric", "dimension").
    pub tags: Vec<String>,
}

impl Column {
    /// Create a new column with minimal fields.
    pub fn new(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: true,
            description: None,
            tags: vec![],
        }
    }

    /// Builder: set nullability.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Builder: add description.
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Builder: add a tag.
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }
}

/// A schema — the set of columns produced by a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    /// The task that produces this schema.
    pub task_id: String,
    /// Optional DAG ID for fully-qualified references.
    pub dag_id: Option<String>,
    /// The columns in this schema.
    pub columns: Vec<Column>,
    /// Schema version (bumped on breaking changes).
    pub version: u32,
}

impl Schema {
    /// Create a new schema.
    pub fn new(task_id: impl Into<String>, columns: Vec<Column>) -> Self {
        Self {
            task_id: task_id.into(),
            dag_id: None,
            columns,
            version: 1,
        }
    }

    /// Get a column by name.
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column names as a list.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Check if a column exists.
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }

    /// Find all columns with a given tag.
    pub fn columns_with_tag(&self, tag: &str) -> Vec<&Column> {
        self.columns
            .iter()
            .filter(|c| c.tags.iter().any(|t| t == tag))
            .collect()
    }
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Schema for '{}' (v{}):", self.task_id, self.version)?;
        for col in &self.columns {
            let null_marker = if col.nullable { "NULL" } else { "NOT NULL" };
            let tags = if col.tags.is_empty() {
                String::new()
            } else {
                format!(" [{}]", col.tags.join(", "))
            };
            writeln!(f, "  {} {} {}{}", col.name, col.column_type, null_marker, tags)?;
        }
        Ok(())
    }
}

/// Global schema registry — stores schemas for all tasks across all DAGs.
///
/// Key: (dag_id, task_id) → Schema.
/// This is populated during compilation from:
/// 1. Explicit schema declarations (YAML or Python annotations)
/// 2. SQL query analysis (inferred column types)
/// 3. Runtime schema capture (from previous executions)
pub struct SchemaRegistry {
    schemas: HashMap<(String, String), Schema>,
}

impl SchemaRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Register a schema for a task.
    pub fn register(&mut self, dag_id: &str, schema: Schema) {
        let key = (dag_id.to_string(), schema.task_id.clone());
        self.schemas.insert(key, schema);
    }

    /// Get a task's schema.
    pub fn get(&self, dag_id: &str, task_id: &str) -> Option<&Schema> {
        self.schemas.get(&(dag_id.to_string(), task_id.to_string()))
    }

    /// Get all schemas for a DAG.
    pub fn schemas_for_dag(&self, dag_id: &str) -> Vec<&Schema> {
        self.schemas
            .iter()
            .filter(|((d, _), _)| d == dag_id)
            .map(|(_, s)| s)
            .collect()
    }

    /// Get all registered schemas.
    pub fn all_schemas(&self) -> Vec<&Schema> {
        self.schemas.values().collect()
    }

    /// Total number of schemas.
    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    /// Is the registry empty?
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_column_lookup() {
        let schema = Schema::new("extract_orders", vec![
            Column::new("id", ColumnType::Integer).not_null(),
            Column::new("customer_name", ColumnType::String).with_tag("pii"),
            Column::new("total", ColumnType::Decimal { precision: 10, scale: 2 }),
        ]);

        assert!(schema.has_column("id"));
        assert!(!schema.has_column("nonexistent"));
        assert_eq!(schema.column_names(), vec!["id", "customer_name", "total"]);

        let pii_cols = schema.columns_with_tag("pii");
        assert_eq!(pii_cols.len(), 1);
        assert_eq!(pii_cols[0].name, "customer_name");
    }

    #[test]
    fn schema_registry_crud() {
        let mut registry = SchemaRegistry::new();

        let schema = Schema::new("extract", vec![
            Column::new("id", ColumnType::Integer),
            Column::new("name", ColumnType::String),
        ]);

        registry.register("etl", schema);
        assert_eq!(registry.len(), 1);

        let found = registry.get("etl", "extract").unwrap();
        assert_eq!(found.columns.len(), 2);

        let dag_schemas = registry.schemas_for_dag("etl");
        assert_eq!(dag_schemas.len(), 1);
    }

    #[test]
    fn column_type_display() {
        assert_eq!(format!("{}", ColumnType::String), "STRING");
        assert_eq!(format!("{}", ColumnType::Decimal { precision: 10, scale: 2 }), "DECIMAL(10,2)");
        assert_eq!(
            format!("{}", ColumnType::Array(Box::new(ColumnType::Integer))),
            "ARRAY<INTEGER>"
        );
    }
}
