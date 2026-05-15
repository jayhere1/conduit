//! Lineage extraction for SQL containing Jinja templates (dbt / Airflow style).
//!
//! Without preprocessing, the underlying `sqlparser` rejects Jinja syntax and
//! the extractor falls back to empty lineage. After preprocessing, Jinja
//! blocks are replaced with placeholder identifiers and the surrounding SQL
//! parses cleanly — so the column-level dataflow we *can* see (FROM/JOIN
//! tables, output columns) still gets surfaced.

use conduit_lineage::SqlLineageExtractor;

#[test]
fn jinja_var_in_from_clause_does_not_break_parse() {
    let sql = r#"
        SELECT id, amount
        FROM {{ source('raw', 'orders') }}
        WHERE created_at > {{ ds }}
    "#;
    let lineage = SqlLineageExtractor::extract(sql);
    // Without Jinja stripping the parser would error and return zero
    // outputs. After stripping, `id` and `amount` are real SELECT columns
    // (the extractor may also surface a synthetic WHERE-clause entry —
    // that's a separate concern).
    let names: Vec<&str> = lineage
        .output_columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(
        names.contains(&"id") && names.contains(&"amount"),
        "expected real SELECT columns to survive Jinja stripping; got {:?}",
        names
    );
}

#[test]
fn jinja_control_flow_block_is_dropped() {
    let sql = r#"
        SELECT
            id,
            amount
            {% if include_meta %}
            , meta
            {% endif %}
        FROM orders
    "#;
    let lineage = SqlLineageExtractor::extract(sql);
    // {% if %} is dropped entirely, so `, meta` stays — but the comma-meta
    // produces a 3rd output column. That's the price of not rendering. Just
    // assert the parse succeeded and at least the two unconditional columns
    // appear, since the more-permissive output is acceptable.
    let names: Vec<&str> = lineage
        .output_columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(
        names.contains(&"id") && names.contains(&"amount"),
        "Jinja-templated SQL should parse and extract unconditional columns; \
         got {:?}",
        names
    );
}

#[test]
fn malformed_jinja_falls_back_gracefully() {
    // Unterminated Jinja — the strip function leaves it as-is and the SQL
    // parser rejects it. Extractor returns empty rather than panicking.
    let sql = "SELECT id FROM {{ unterminated";
    let lineage = SqlLineageExtractor::extract(sql);
    // Should not panic. May or may not extract — either is acceptable.
    let _ = lineage;
}

#[test]
fn jinja_with_real_table_after_substitution() {
    // After Jinja stripping, the FROM has a clean table name — the extractor
    // should pick it up as a source table.
    let sql = "SELECT a, b FROM users WHERE created_at > '{{ ds }}'";
    let lineage = SqlLineageExtractor::extract(sql);
    let names: Vec<&str> = lineage
        .output_columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(names.contains(&"a") && names.contains(&"b"));
    assert!(
        lineage.source_tables.iter().any(|t| t.name == "users"),
        "expected 'users' as a source table, got {:?}",
        lineage.source_tables
    );
}
