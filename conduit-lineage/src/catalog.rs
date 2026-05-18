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
use tracing::warn;

use crate::lineage_graph::TaskRef;
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
///
/// **Views**: a view registered via `register_view` exposes columns derived
/// from its defining SQL. Downstream queries treat the view exactly like a
/// table: `SELECT *` expansion, bare-column resolution, and CTE-style lineage
/// propagation all work. View resolution is single-level — a view defined in
/// terms of another view falls back to the underlying view's stored columns,
/// so cycles are impossible by construction.
#[derive(Debug, Clone, Default)]
pub struct TableCatalog {
    tables: HashMap<(Option<String>, String), Vec<CatalogColumn>>,
    /// Tracks which task produced each registered dataset (if any). Keyed
    /// identically to `tables` so look-ups go through the same path.
    /// Physical tables registered via `register_table` have no producer
    /// entry; task-produced datasets registered via `register_dataset` do.
    producers: HashMap<(Option<String>, String), TaskRef>,
}

impl TableCatalog {
    /// Create a new empty catalog.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            producers: HashMap::new(),
        }
    }

    /// Register a dataset produced by a task. The dataset becomes
    /// resolvable by downstream SQL `FROM <qualified_name>` and
    /// `lookup_producer(qualified_name)` returns the originating task.
    ///
    /// **Collision policy (within a single DAG):** task-produced datasets
    /// win over previously registered physical tables. The clobber is
    /// logged at WARN level so collisions are visible. Across DAGs, the
    /// caller is responsible for keeping the catalog scoped — `stitch`
    /// builds a fresh catalog per DAG.
    pub fn register_dataset(
        &mut self,
        qualified_name: &str,
        columns: Vec<CatalogColumn>,
        producer: TaskRef,
    ) {
        let (schema, table) = split_qualified(qualified_name);
        let key = (
            schema.as_ref().map(|s| s.to_lowercase()),
            table.to_lowercase(),
        );

        if self.tables.contains_key(&key) && !self.producers.contains_key(&key) {
            warn!(
                qualified_name = %qualified_name,
                producer = %producer,
                "dataset name collides with a previously registered physical table; task producer wins",
            );
        }

        self.tables.insert(key.clone(), columns);
        self.producers.insert(key, producer);
    }

    /// Look up the producer task for a dataset by qualified name. Returns
    /// `None` for physical tables and unknown names.
    ///
    /// **Falls back to unqualified match** when the lookup name has no
    /// schema and exactly one registered dataset has a matching table
    /// segment. This is necessary because the SQL extractor strips
    /// schema prefixes when building per-column `ColumnRef`s — without
    /// the fallback, `FROM staging.orders` would fail to resolve back to
    /// a producer registered as `"staging.orders"`. If multiple datasets
    /// share the unqualified name, the lookup returns `None` (ambiguous)
    /// — callers should warn and treat the column as unresolved.
    pub fn lookup_producer(&self, qualified_name: &str) -> Option<&TaskRef> {
        let (schema, table) = split_qualified(qualified_name);
        let key = (
            schema.as_ref().map(|s| s.to_lowercase()),
            table.to_lowercase(),
        );
        if let Some(p) = self.producers.get(&key) {
            return Some(p);
        }
        if schema.is_none() {
            let table_lc = table.to_lowercase();
            let matches: Vec<&TaskRef> = self
                .producers
                .iter()
                .filter(|((_, t), _)| t == &table_lc)
                .map(|(_, v)| v)
                .collect();
            if matches.len() == 1 {
                return Some(matches[0]);
            }
        }
        None
    }

    /// Look up columns by a single qualified name (`"schema.table"` or
    /// `"table"`). Falls back to unqualified-match like
    /// [`Self::lookup_producer`] for the same SQL-extractor reason.
    pub fn lookup_via_qualified(&self, qualified_name: &str) -> Option<&[CatalogColumn]> {
        let (schema, table) = split_qualified(qualified_name);
        if let Some(cols) = self.lookup(schema.as_deref(), &table) {
            return Some(cols);
        }
        if schema.is_none() {
            let table_lc = table.to_lowercase();
            let matches: Vec<&Vec<CatalogColumn>> = self
                .tables
                .iter()
                .filter(|((_, t), _)| t == &table_lc)
                .map(|(_, v)| v)
                .collect();
            if matches.len() == 1 {
                return Some(matches[0].as_slice());
            }
        }
        None
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

    /// Register a view by extracting its output columns from its defining
    /// SQL and storing them under the view's name. Downstream queries that
    /// reference the view get the same column-resolution and `*`-expansion
    /// behavior as a real table.
    ///
    /// The view's SQL is parsed once at registration time using the *current*
    /// catalog as context — so register underlying tables before views that
    /// depend on them for the best resolution. Returns `true` if at least
    /// one output column was successfully extracted; `false` if the SQL was
    /// unparseable (the view is still registered with an empty column list).
    pub fn register_view(&mut self, schema: Option<&str>, view: &str, defining_sql: &str) -> bool {
        // Local import to avoid a top-of-file cycle on `sql_parser` (which
        // also imports from this module). Both modules are inside the same
        // crate so the local import is free.
        use crate::sql_parser::SqlLineageExtractor;
        let lineage = SqlLineageExtractor::extract_with_catalog(defining_sql, self);
        let columns: Vec<CatalogColumn> = lineage
            .output_columns
            .iter()
            // Filter out synthetic columns (e.g. WHERE-clause aggregations
            // surfaced as `__where__`) — they shouldn't appear in
            // `SELECT *` expansion against the view.
            .filter(|c| !c.name.starts_with("__"))
            .map(|c| CatalogColumn::new(&c.name, ColumnType::Unknown))
            .collect();
        let extracted = !columns.is_empty();
        self.register_table(schema, view, columns);
        extracted
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

/// Split `"schema.table"` into `(Some("schema"), "table")`, or `"table"`
/// into `(None, "table")`. The catalog is two-tier; multi-level paths like
/// `catalog.schema.table` are normalised to `(schema, table)`.
fn split_qualified(name: &str) -> (Option<String>, String) {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.as_slice() {
        [] | [""] => (None, String::new()),
        [t] => (None, (*t).to_string()),
        [s, t] => (Some((*s).to_string()), (*t).to_string()),
        rest => {
            let n = rest.len();
            (Some(rest[n - 2].to_string()), rest[n - 1].to_string())
        }
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
    fn register_dataset_resolves_producer() {
        let mut cat = TableCatalog::new();
        cat.register_dataset(
            "staging.orders",
            vec![
                CatalogColumn::new("id", ColumnType::Integer),
                CatalogColumn::new("amount", ColumnType::Float),
            ],
            TaskRef::new("warehouse", "extract_orders"),
        );

        let producer = cat.lookup_producer("staging.orders").unwrap();
        assert_eq!(producer.dag_id, "warehouse");
        assert_eq!(producer.task_id, "extract_orders");

        // Schema lookups go through the standard `lookup` path.
        let cols = cat.lookup(Some("staging"), "orders").unwrap();
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn physical_table_has_no_producer() {
        let mut cat = TableCatalog::new();
        cat.register_table(
            Some("public"),
            "orders",
            vec![CatalogColumn::new("id", ColumnType::Integer)],
        );
        assert!(cat.lookup_producer("public.orders").is_none());
    }

    #[test]
    fn dataset_clobbers_physical_table() {
        let mut cat = TableCatalog::new();
        cat.register_table(
            Some("staging"),
            "orders",
            vec![CatalogColumn::new("original", ColumnType::Integer)],
        );
        cat.register_dataset(
            "staging.orders",
            vec![CatalogColumn::new("new", ColumnType::Integer)],
            TaskRef::new("d", "t"),
        );

        let cols = cat.lookup(Some("staging"), "orders").unwrap();
        assert_eq!(cols[0].name, "new");
        assert!(cat.lookup_producer("staging.orders").is_some());
    }

    #[test]
    fn unqualified_dataset_name() {
        let mut cat = TableCatalog::new();
        cat.register_dataset(
            "scratch",
            vec![CatalogColumn::new("col", ColumnType::Integer)],
            TaskRef::new("d", "t"),
        );
        assert!(cat.lookup_producer("scratch").is_some());
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
