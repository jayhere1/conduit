//! Tests for view registration in TableCatalog.
//!
//! A view is just a query saved under a name. After `register_view`, the
//! lineage extractor treats it like any other table: `SELECT *` expands to
//! the view's output columns, bare column references resolve, and queries
//! against the view produce correct lineage as if the underlying SQL had
//! been substituted.

use conduit_lineage::{CatalogColumn, ColumnType, SqlLineageExtractor, TableCatalog};

fn cat_with_orders() -> TableCatalog {
    let mut c = TableCatalog::new();
    c.register_table(
        None,
        "orders",
        vec![
            CatalogColumn::new("id", ColumnType::Integer),
            CatalogColumn::new("user_id", ColumnType::Integer),
            CatalogColumn::new("amount", ColumnType::Float),
            CatalogColumn::new("status", ColumnType::String),
        ],
    );
    c
}

#[test]
fn register_view_extracts_output_columns() {
    let mut catalog = cat_with_orders();
    let extracted = catalog.register_view(
        None,
        "active_orders",
        "SELECT id, user_id, amount FROM orders WHERE status = 'active'",
    );
    assert!(extracted, "should have extracted at least one column");

    let cols = catalog.lookup(None, "active_orders").unwrap();
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"user_id"));
    assert!(names.contains(&"amount"));
}

#[test]
fn view_supports_wildcard_expansion() {
    let mut catalog = cat_with_orders();
    catalog.register_view(None, "order_totals", "SELECT id, amount FROM orders");
    let expanded = catalog.expand_wildcard(None, "order_totals").unwrap();
    assert_eq!(
        expanded.iter().collect::<std::collections::HashSet<_>>(),
        ["id", "amount"]
            .iter()
            .map(|s| s.to_string())
            .collect::<std::collections::HashSet<_>>()
            .iter()
            .collect()
    );
}

#[test]
fn querying_against_a_view_yields_lineage() {
    let mut catalog = cat_with_orders();
    catalog.register_view(
        None,
        "high_value_orders",
        "SELECT id, amount FROM orders WHERE amount > 1000",
    );

    let lineage = SqlLineageExtractor::extract_with_catalog(
        "SELECT id, amount FROM high_value_orders",
        &catalog,
    );

    let names: Vec<&str> = lineage
        .output_columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(names.contains(&"id") && names.contains(&"amount"));
    assert!(
        lineage
            .source_tables
            .iter()
            .any(|t| t.name == "high_value_orders"),
        "expected high_value_orders as source, got {:?}",
        lineage.source_tables
    );
}

#[test]
fn select_star_on_view_expands_to_view_columns() {
    let mut catalog = cat_with_orders();
    catalog.register_view(None, "order_summary", "SELECT id, amount FROM orders");

    let lineage =
        SqlLineageExtractor::extract_with_catalog("SELECT * FROM order_summary", &catalog);

    let names: Vec<&str> = lineage
        .output_columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(
        names.contains(&"id") && names.contains(&"amount"),
        "SELECT * against view should expand to its columns; got {:?}",
        names
    );
}

#[test]
fn view_of_view_resolves_via_underlying_columns() {
    let mut catalog = cat_with_orders();
    catalog.register_view(None, "level1", "SELECT id, amount FROM orders");
    catalog.register_view(None, "level2", "SELECT id FROM level1");

    let cols = catalog.lookup(None, "level2").unwrap();
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert!(
        names.contains(&"id"),
        "level2 should resolve through level1 to expose id; got {:?}",
        names
    );
}

#[test]
fn unparseable_view_registers_with_empty_columns() {
    let mut catalog = cat_with_orders();
    let extracted = catalog.register_view(None, "garbage", "not valid sql at all");
    assert!(!extracted, "unparseable SQL should report no extraction");
    // But the view is still registered (with empty columns) so downstream
    // lookups don't false-negative.
    let cols = catalog.lookup(None, "garbage").unwrap();
    assert!(cols.is_empty());
}
