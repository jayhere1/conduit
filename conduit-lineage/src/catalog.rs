//! Table catalog for enhanced SQL lineage extraction.
//!
//! The `TableCatalog` stores column metadata for database tables, enabling
//! the lineage extractor to:
//! - Resolve bare (unqualified) column references to the correct source table
//! - Expand `SELECT *` into actual column names
//! - Propagate column-level lineage through CTEs
//!
//! The catalog is populated from provider `describe_table()` calls and is
//! optional — lineage extraction works without it, just with reduced precision.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::schema::ColumnType;

/// A column as known from a database catalog (information_schema).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogColumn {
    /// Column name (stored lowercase).
    pub name: String,
    /// Column data type.
    pub data_type: ColumnType,
    /// Whether the column can be NULL.
    pub nullable: bool,
}

impl CatalogColumn {
    /// Create a new catalog column (name is lowercased).
    pub fn new(name: impl Into<String>, data_type: ColumnType) -> Self {
        Self {
            name: name.into().to_lowercase(),
            data_type,
            nullable: true,
        }
    }

    /// Builder: set as NOT NULL.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }
}

/// A catalog of known table schemas, used to enhance SQL lineage extraction.
///
/// Maps `(schema, table_name)` → columns. When provided to the lineage extractor,
/// enables precise column resolution that would otherwise require guessing.
///
/// The catalog is optional and additive — without it, lineage extraction works
/// exactly as before. With it, results are more precise.
#[derive(Debug, Clone, Default)]
pub struct TableCatalog {
    tables: HashMap<(Option<String>, String), Vec<CatalogColumn>>,
}

impl TableCatalog {
    /// Create a new empty catalog.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a table's columns. Schema and table name are lowercased.
    pub fn register_table(
        &mut self,
        schema: Option<&str>,
        table: &str,
        columns: Vec<CatalogColumn>,
    ) {
        let key = (schema.map(|s| s.to_lowercase()), table.to_lowercase());
        self.tables.insert(key, columns);
    }

    /// Look up a table's columns.
    ///
    /// Tries exact match first, then falls back to schema=None if a schema
    /// was specified but not found.
    pub fn lookup(&self, schema: Option<&str>, table: &str) -> Option<&[CatalogColumn]> {
        let key = (schema.map(|s| s.to_lowercase()), table.to_lowercase());
        self.tables
            .get(&key)
            .or_else(|| {
                if schema.is_some() {
                    self.tables.get(&(None, table.to_lowercase()))
                } else {
                    None
                }
            })
            .map(|v| v.as_slice())
    }

    /// Given a bare column name and candidate tables from the FROM clause,
    /// find which table owns the column.
    ///
    /// Returns the table name if exactly one candidate has the column.
    /// Returns `None` if zero or multiple candidates match (ambiguous).
    pub fn find_column_owner(
        &self,
        column_name: &str,
        candidates: &[(Option<&str>, &str)],
    ) -> Option<String> {
        let col_lower = column_name.to_lowercase();
        let mut owners = Vec::new();

        for &(schema, table) in candidates {
            if let Some(cols) = self.lookup(schema, table) {
                if cols.iter().any(|c| c.name == col_lower) {
                    owners.push(table.to_lowercase());
                }
            }
        }

        if owners.len() == 1 {
            Some(owners.into_iter().next().unwrap())
        } else {
            None
        }
    }

    /// Expand a wildcard for a table, returning column names in order.
    pub fn expand_wildcard(&self, schema: Option<&str>, table: &str) -> Option<Vec<String>> {
        self.lookup(schema, table)
            .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
    }

    /// Number of registered tables.
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Whether the catalog is empty.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

/// Parse a SQL type string (as returned by `information_schema.columns`)
/// into a [`ColumnType`].
///
/// Handles common type names across PostgreSQL, MySQL, SQLite, Snowflake,
/// BigQuery, ClickHouse, and other SQL dialects. Case-insensitive.
pub fn parse_sql_type(type_str: &str) -> ColumnType {
    let t = type_str.to_uppercase();
    let t = t.trim();

    // Strip precision/length suffixes: "varchar(255)" → "VARCHAR"
    let base = t.split('(').next().unwrap_or(t).trim();

    match base {
        // Integer types
        "INT" | "INTEGER" | "INT2" | "INT4" | "INT8" | "INT16" | "INT32" | "INT64" | "SMALLINT"
        | "BIGINT" | "TINYINT" | "MEDIUMINT" | "SERIAL" | "BIGSERIAL" | "SMALLSERIAL" | "UINT8"
        | "UINT16" | "UINT32" | "UINT64" | "NUMBER" => ColumnType::Integer,

        // Float types
        "FLOAT" | "FLOAT4" | "FLOAT8" | "FLOAT32" | "FLOAT64" | "DOUBLE" | "DOUBLE PRECISION"
        | "REAL" => ColumnType::Float,

        // Decimal types (precision lost in this mapping)
        "NUMERIC" | "DECIMAL" | "MONEY" => ColumnType::Decimal {
            precision: 38,
            scale: 9,
        },

        // String types
        "VARCHAR" | "TEXT" | "CHAR" | "CHARACTER" | "CHARACTER VARYING" | "BPCHAR" | "NVARCHAR"
        | "NCHAR" | "NTEXT" | "CLOB" | "LONGTEXT" | "MEDIUMTEXT" | "TINYTEXT" | "STRING"
        | "FIXEDSTRING" | "ENUM" | "SET" | "NAME" | "UUID" | "CITEXT" => ColumnType::String,

        // Boolean types
        "BOOL" | "BOOLEAN" | "BIT" => ColumnType::Boolean,

        // Date/time types
        "DATE" => ColumnType::Date,
        "TIMESTAMP"
        | "TIMESTAMPTZ"
        | "TIMESTAMP WITH TIME ZONE"
        | "TIMESTAMP WITHOUT TIME ZONE"
        | "DATETIME"
        | "DATETIME2"
        | "SMALLDATETIME"
        | "TIMESTAMP_NTZ"
        | "TIMESTAMP_LTZ"
        | "TIMESTAMP_TZ" => ColumnType::Timestamp,
        "TIME" | "TIMETZ" | "TIME WITH TIME ZONE" | "TIME WITHOUT TIME ZONE" | "INTERVAL" => {
            ColumnType::Timestamp
        }

        // JSON types
        "JSON" | "JSONB" | "VARIANT" | "OBJECT" | "MAP" => ColumnType::Json,

        // Binary types
        "BYTEA" | "BLOB" | "BINARY" | "VARBINARY" | "LONGBLOB" | "MEDIUMBLOB" | "TINYBLOB"
        | "RAW" | "IMAGE" | "BYTES" => ColumnType::Binary,

        // Array (can't know inner type from just the base name)
        "ARRAY" => ColumnType::Array(Box::new(ColumnType::Unknown)),

        _ => {
            // Handle compound types: "character varying" etc.
            if t.starts_with("CHARACTER VARYING") || t.starts_with("VARCHAR") {
                ColumnType::String
            } else if t.starts_with("NUMERIC") || t.starts_with("DECIMAL") {
                ColumnType::Decimal {
                    precision: 38,
                    scale: 9,
                }
            } else if t.starts_with("TIMESTAMP") {
                ColumnType::Timestamp
            } else if t.starts_with("DOUBLE") {
                ColumnType::Float
            } else if t.starts_with("ARRAY") {
                ColumnType::Array(Box::new(ColumnType::Unknown))
            } else {
                ColumnType::Unknown
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_lookup() {
        let mut cat = TableCatalog::new();
        cat.register_table(
            Some("public"),
            "orders",
            vec![
                CatalogColumn::new("id", ColumnType::Integer),
                CatalogColumn::new("amount", ColumnType::Float),
                CatalogColumn::new("customer_id", ColumnType::Integer),
            ],
        );

        let cols = cat.lookup(Some("public"), "orders").unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "id");
    }

    #[test]
    fn lookup_falls_back_to_no_schema() {
        let mut cat = TableCatalog::new();
        cat.register_table(
            None,
            "orders",
            vec![CatalogColumn::new("id", ColumnType::Integer)],
        );

        let cols = cat.lookup(Some("public"), "orders").unwrap();
        assert_eq!(cols.len(), 1);
    }

    #[test]
    fn find_column_owner_unique() {
        let mut cat = TableCatalog::new();
        cat.register_table(
            None,
            "orders",
            vec![
                CatalogColumn::new("id", ColumnType::Integer),
                CatalogColumn::new("amount", ColumnType::Float),
            ],
        );
        cat.register_table(
            None,
            "customers",
            vec![
                CatalogColumn::new("id", ColumnType::Integer),
                CatalogColumn::new("name", ColumnType::String),
                CatalogColumn::new("active", ColumnType::Boolean),
            ],
        );

        let candidates = vec![(None, "orders"), (None, "customers")];

        // "active" is only in customers
        assert_eq!(
            cat.find_column_owner("active", &candidates),
            Some("customers".to_string())
        );

        // "amount" is only in orders
        assert_eq!(
            cat.find_column_owner("amount", &candidates),
            Some("orders".to_string())
        );

        // "id" is in both — ambiguous
        assert_eq!(cat.find_column_owner("id", &candidates), None);

        // "nonexistent" is in neither
        assert_eq!(cat.find_column_owner("nonexistent", &candidates), None);
    }

    #[test]
    fn expand_wildcard() {
        let mut cat = TableCatalog::new();
        cat.register_table(
            None,
            "users",
            vec![
                CatalogColumn::new("id", ColumnType::Integer),
                CatalogColumn::new("name", ColumnType::String),
                CatalogColumn::new("email", ColumnType::String),
            ],
        );

        let cols = cat.expand_wildcard(None, "users").unwrap();
        assert_eq!(cols, vec!["id", "name", "email"]);
    }

    #[test]
    fn case_insensitive() {
        let mut cat = TableCatalog::new();
        cat.register_table(
            Some("PUBLIC"),
            "Orders",
            vec![CatalogColumn::new("ID", ColumnType::Integer)],
        );

        assert!(cat.lookup(Some("public"), "orders").is_some());
        assert!(cat.lookup(Some("PUBLIC"), "ORDERS").is_some());
    }

    #[test]
    fn parse_sql_type_integer_variants() {
        assert_eq!(parse_sql_type("integer"), ColumnType::Integer);
        assert_eq!(parse_sql_type("INT"), ColumnType::Integer);
        assert_eq!(parse_sql_type("int4"), ColumnType::Integer);
        assert_eq!(parse_sql_type("BIGINT"), ColumnType::Integer);
        assert_eq!(parse_sql_type("smallint"), ColumnType::Integer);
        assert_eq!(parse_sql_type("serial"), ColumnType::Integer);
    }

    #[test]
    fn parse_sql_type_string_variants() {
        assert_eq!(parse_sql_type("varchar"), ColumnType::String);
        assert_eq!(parse_sql_type("VARCHAR(255)"), ColumnType::String);
        assert_eq!(parse_sql_type("text"), ColumnType::String);
        assert_eq!(parse_sql_type("character varying"), ColumnType::String);
        assert_eq!(parse_sql_type("character varying(100)"), ColumnType::String);
        assert_eq!(parse_sql_type("uuid"), ColumnType::String);
    }

    #[test]
    fn parse_sql_type_other_types() {
        assert_eq!(parse_sql_type("boolean"), ColumnType::Boolean);
        assert_eq!(parse_sql_type("date"), ColumnType::Date);
        assert_eq!(parse_sql_type("timestamptz"), ColumnType::Timestamp);
        assert_eq!(
            parse_sql_type("timestamp without time zone"),
            ColumnType::Timestamp
        );
        assert_eq!(parse_sql_type("jsonb"), ColumnType::Json);
        assert_eq!(parse_sql_type("bytea"), ColumnType::Binary);
        assert_eq!(parse_sql_type("float8"), ColumnType::Float);
        matches!(parse_sql_type("numeric(10,2)"), ColumnType::Decimal { .. });
        assert_eq!(parse_sql_type("unknown_type"), ColumnType::Unknown);
    }

    #[test]
    fn virtual_table_for_cte() {
        let mut cat = TableCatalog::new();

        // Register a real table
        cat.register_table(
            None,
            "orders",
            vec![
                CatalogColumn::new("id", ColumnType::Integer),
                CatalogColumn::new("amount", ColumnType::Float),
                CatalogColumn::new("status", ColumnType::String),
            ],
        );

        // Register a CTE as a virtual table
        cat.register_table(
            None,
            "active_orders",
            vec![
                CatalogColumn::new("id", ColumnType::Integer),
                CatalogColumn::new("amount", ColumnType::Float),
            ],
        );

        // CTE lookup works
        let cols = cat.expand_wildcard(None, "active_orders").unwrap();
        assert_eq!(cols, vec!["id", "amount"]);

        // Column owner resolution works across real and virtual tables
        let candidates = vec![(None, "active_orders"), (None, "orders")];
        // "status" is only in orders
        assert_eq!(
            cat.find_column_owner("status", &candidates),
            Some("orders".to_string())
        );
    }
}
