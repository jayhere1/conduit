//! SQL lineage extraction — parse SQL queries to determine
//! which input columns flow into which output columns.
//!
//! This is a simplified SQL parser focused on lineage extraction,
//! not full SQL execution. It handles the most common patterns:
//! - SELECT column_name, alias
//! - SELECT table.column
//! - SELECT expression AS alias
//! - FROM / JOIN clauses (to resolve table references)
//! - Wildcard expansion (SELECT *)
//! - Subqueries in FROM
//!
//! For production use, this would be backed by a proper SQL parser
//! (e.g., sqlparser-rs). This implementation demonstrates the lineage
//! extraction architecture.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lineage_graph::ColumnRef;

/// A parsed SQL query with lineage information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlLineage {
    /// Output columns (what the query produces).
    pub output_columns: Vec<OutputColumn>,
    /// Input table references (FROM/JOIN).
    pub source_tables: Vec<TableRef>,
    /// Column-level mappings: output column → input columns it depends on.
    pub column_mappings: Vec<ColumnMapping>,
}

/// An output column from a SELECT clause.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputColumn {
    /// The column name (or alias).
    pub name: String,
    /// The raw expression (e.g., "COALESCE(a.name, b.name)").
    pub expression: String,
    /// Whether this is a simple column reference or a computed expression.
    pub is_computed: bool,
}

/// A table referenced in FROM/JOIN.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRef {
    /// Table or subquery name.
    pub name: String,
    /// Alias (if any).
    pub alias: Option<String>,
    /// The schema/database qualifier (if any).
    pub schema: Option<String>,
}

/// A mapping from one output column to its input column dependencies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMapping {
    /// The output column.
    pub output: String,
    /// The input columns this output depends on.
    pub inputs: Vec<ColumnRef>,
}

/// Extracts column-level lineage from SQL queries.
pub struct SqlLineageExtractor;

impl SqlLineageExtractor {
    /// Extract lineage from a SQL query string.
    ///
    /// This is a pattern-based extractor, not a full SQL parser.
    /// It handles the most common SELECT patterns in data pipelines.
    pub fn extract(sql: &str) -> SqlLineage {
        let normalized = Self::normalize_sql(sql);

        let source_tables = Self::extract_tables(&normalized);
        let (output_columns, column_mappings) = Self::extract_columns(&normalized, &source_tables);

        SqlLineage {
            output_columns,
            source_tables,
            column_mappings,
        }
    }

    /// Normalize SQL for easier parsing: lowercase, collapse whitespace.
    fn normalize_sql(sql: &str) -> String {
        let mut result = String::new();
        let mut in_string = false;
        let mut prev_was_space = false;

        for ch in sql.chars() {
            if ch == '\'' {
                in_string = !in_string;
                result.push(ch);
                prev_was_space = false;
                continue;
            }

            if in_string {
                result.push(ch);
                prev_was_space = false;
                continue;
            }

            if ch.is_whitespace() || ch == '\n' || ch == '\r' {
                if !prev_was_space {
                    result.push(' ');
                    prev_was_space = true;
                }
            } else {
                result.push(ch.to_ascii_lowercase());
                prev_was_space = false;
            }
        }

        result.trim().to_string()
    }

    /// Extract table references from FROM and JOIN clauses.
    fn extract_tables(sql: &str) -> Vec<TableRef> {
        let mut tables = Vec::new();

        // Find FROM clause
        if let Some(from_pos) = sql.find(" from ") {
            let after_from = &sql[from_pos + 6..];

            // Get the table list (everything until WHERE, GROUP BY, ORDER BY, LIMIT, JOIN, or end)
            let end_markers = [" where ", " group ", " order ", " limit ", " having ",
                               " join ", " inner join ", " left join ", " right join ",
                               " full join ", " cross join ", " left outer join ",
                               " right outer join ", " full outer join ", ";"];
            let table_section = Self::find_section(after_from, &end_markers);

            // Split on commas and JOINs
            for part in Self::split_table_refs(table_section) {
                if let Some(table) = Self::parse_table_ref(part.trim()) {
                    tables.push(table);
                }
            }
        }

        // Find JOIN clauses
        let join_keywords = [" join ", " inner join ", " left join ", " right join ",
                            " full join ", " cross join ", " left outer join ",
                            " right outer join ", " full outer join "];

        for keyword in &join_keywords {
            let mut search_from = 0;
            while let Some(pos) = sql[search_from..].find(keyword) {
                let abs_pos = search_from + pos + keyword.len();
                let after_join = &sql[abs_pos..];

                let end_markers = [" on ", " using ", " where ", " join ", " inner ",
                                   " left ", " right ", " full ", " cross ", " group ",
                                   " order ", " limit ", ";"];
                let table_section = Self::find_section(after_join, &end_markers);

                if let Some(table) = Self::parse_table_ref(table_section.trim()) {
                    // Avoid duplicates
                    if !tables.iter().any(|t| t.name == table.name) {
                        tables.push(table);
                    }
                }

                search_from = abs_pos;
            }
        }

        tables
    }

    /// Extract everything until one of the end markers.
    fn find_section<'a>(text: &'a str, end_markers: &[&str]) -> &'a str {
        let mut earliest_end = text.len();

        for marker in end_markers {
            if let Some(pos) = text.find(marker) {
                earliest_end = earliest_end.min(pos);
            }
        }

        &text[..earliest_end]
    }

    /// Split a table reference section on commas (but not inside parentheses).
    fn split_table_refs(section: &str) -> Vec<&str> {
        let mut parts = Vec::new();
        let mut depth = 0;
        let mut start = 0;

        for (i, ch) in section.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    parts.push(&section[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        parts.push(&section[start..]);
        parts
    }

    /// Parse a single table reference like "schema.table AS alias" or just "table".
    fn parse_table_ref(text: &str) -> Option<TableRef> {
        let text = text.trim();
        if text.is_empty() || text.starts_with('(') {
            return None;
        }

        // Split on " as " or whitespace for alias
        let (name_part, alias) = if let Some(as_pos) = text.find(" as ") {
            (&text[..as_pos], Some(text[as_pos + 4..].trim().to_string()))
        } else {
            // Check for implicit alias (table_name alias_name)
            let parts: Vec<&str> = text.split_whitespace().collect();
            if parts.len() == 2 {
                (parts[0], Some(parts[1].to_string()))
            } else {
                (text, None)
            }
        };

        // Split schema.table
        let (schema, name) = if let Some(dot_pos) = name_part.find('.') {
            (
                Some(name_part[..dot_pos].trim().to_string()),
                name_part[dot_pos + 1..].trim().to_string(),
            )
        } else {
            (None, name_part.trim().to_string())
        };

        Some(TableRef { name, alias, schema })
    }

    /// Extract output columns and their input mappings from the SELECT clause.
    fn extract_columns(sql: &str, tables: &[TableRef]) -> (Vec<OutputColumn>, Vec<ColumnMapping>) {
        let mut outputs = Vec::new();
        let mut mappings = Vec::new();

        // Find SELECT ... FROM
        let select_pos = match sql.find("select ") {
            Some(p) => p + 7,
            None => return (outputs, mappings),
        };

        // Handle SELECT DISTINCT
        let after_select = &sql[select_pos..];
        let col_start = if after_select.starts_with("distinct ") {
            select_pos + 9
        } else {
            select_pos
        };

        let from_pos = sql.find(" from ").unwrap_or(sql.len());
        let select_clause = &sql[col_start..from_pos];

        // Build table alias map for resolving qualified column references
        let alias_map: HashMap<&str, &str> = tables
            .iter()
            .filter_map(|t| t.alias.as_deref().map(|a| (a, t.name.as_str())))
            .collect();

        // Split columns on commas (respecting parentheses)
        for col_expr in Self::split_table_refs(select_clause) {
            let col_expr = col_expr.trim();
            if col_expr.is_empty() {
                continue;
            }

            // Handle SELECT *
            if col_expr == "*" {
                outputs.push(OutputColumn {
                    name: "*".to_string(),
                    expression: "*".to_string(),
                    is_computed: false,
                });
                // Wildcard expands to all columns from all source tables
                for table in tables {
                    mappings.push(ColumnMapping {
                        output: "*".to_string(),
                        inputs: vec![ColumnRef {
                            task_id: table.name.clone(),
                            column_name: "*".to_string(),
                        }],
                    });
                }
                continue;
            }

            // Handle table.* (e.g., "orders.*")
            if col_expr.ends_with(".*") {
                let table_ref = &col_expr[..col_expr.len() - 2];
                let resolved_table = alias_map.get(table_ref).unwrap_or(&table_ref);
                outputs.push(OutputColumn {
                    name: format!("{}.*", resolved_table),
                    expression: col_expr.to_string(),
                    is_computed: false,
                });
                mappings.push(ColumnMapping {
                    output: format!("{}.*", resolved_table),
                    inputs: vec![ColumnRef {
                        task_id: resolved_table.to_string(),
                        column_name: "*".to_string(),
                    }],
                });
                continue;
            }

            // Parse "expression AS alias" or just "column_name"
            let (expr, output_name) = if let Some(as_pos) = col_expr.rfind(" as ") {
                (&col_expr[..as_pos], col_expr[as_pos + 4..].trim())
            } else {
                // Use the column name itself (or last part after dot)
                let name = if let Some(dot_pos) = col_expr.rfind('.') {
                    &col_expr[dot_pos + 1..]
                } else {
                    col_expr
                };
                (col_expr, name)
            };

            let is_computed = expr.contains('(') || expr.contains('+') || expr.contains('-')
                || expr.contains("case ") || expr.contains("coalesce");

            outputs.push(OutputColumn {
                name: output_name.to_string(),
                expression: expr.to_string(),
                is_computed,
            });

            // Extract input column references from the expression
            let input_refs = Self::extract_column_refs(expr, &alias_map, tables);
            if !input_refs.is_empty() {
                mappings.push(ColumnMapping {
                    output: output_name.to_string(),
                    inputs: input_refs,
                });
            }
        }

        (outputs, mappings)
    }

    /// Extract column references from an expression.
    ///
    /// Handles:
    /// - Simple: `column_name` → resolves to first table
    /// - Qualified: `table.column` → resolves alias
    /// - Functions: `COALESCE(a.col1, b.col2)` → both references
    fn extract_column_refs(
        expr: &str,
        alias_map: &HashMap<&str, &str>,
        tables: &[TableRef],
    ) -> Vec<ColumnRef> {
        let mut refs = Vec::new();

        // Tokenize: split on non-identifier characters
        let tokens: Vec<&str> = expr
            .split(|c: char| !c.is_alphanumeric() && c != '_' && c != '.')
            .filter(|t| !t.is_empty())
            .collect();

        // SQL keywords to ignore
        let keywords = [
            "select", "from", "where", "and", "or", "not", "in", "is", "null",
            "as", "case", "when", "then", "else", "end", "coalesce", "count",
            "sum", "avg", "min", "max", "cast", "trim", "upper", "lower",
            "concat", "substring", "extract", "date", "timestamp", "interval",
            "true", "false", "asc", "desc", "distinct", "between", "like",
            "ilike", "exists", "having", "group", "order", "limit", "offset",
            "union", "intersect", "except", "all", "any", "some",
        ];

        for token in tokens {
            // Skip pure numbers
            if token.chars().all(|c| c.is_ascii_digit()) {
                continue;
            }

            // Skip SQL keywords
            if keywords.contains(&token) {
                continue;
            }

            if let Some(dot_pos) = token.find('.') {
                // Qualified reference: table.column
                let table_part = &token[..dot_pos];
                let col_part = &token[dot_pos + 1..];

                let resolved = alias_map.get(table_part).unwrap_or(&table_part);
                refs.push(ColumnRef {
                    task_id: resolved.to_string(),
                    column_name: col_part.to_string(),
                });
            } else {
                // Unqualified: try to resolve to first table
                let table_name = tables
                    .first()
                    .map(|t| t.name.as_str())
                    .unwrap_or("unknown");
                refs.push(ColumnRef {
                    task_id: table_name.to_string(),
                    column_name: token.to_string(),
                });
            }
        }

        // Deduplicate
        refs.sort_by(|a, b| (&a.task_id, &a.column_name).cmp(&(&b.task_id, &b.column_name)));
        refs.dedup_by(|a, b| a.task_id == b.task_id && a.column_name == b.column_name);

        refs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_select() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT id, name, email FROM customers"
        );
        assert_eq!(lineage.output_columns.len(), 3);
        assert_eq!(lineage.source_tables.len(), 1);
        assert_eq!(lineage.source_tables[0].name, "customers");
    }

    #[test]
    fn select_with_alias() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT c.id, c.name AS customer_name FROM customers AS c"
        );
        assert_eq!(lineage.output_columns.len(), 2);
        assert_eq!(lineage.output_columns[1].name, "customer_name");
        assert_eq!(lineage.source_tables[0].name, "customers");
        assert_eq!(lineage.source_tables[0].alias.as_deref(), Some("c"));
    }

    #[test]
    fn select_with_join() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT o.id, c.name
             FROM orders o
             JOIN customers c ON o.customer_id = c.id"
        );
        assert_eq!(lineage.source_tables.len(), 2);

        let table_names: Vec<&str> = lineage.source_tables.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"customers"));
    }

    #[test]
    fn select_star() {
        let lineage = SqlLineageExtractor::extract("SELECT * FROM orders");
        assert_eq!(lineage.output_columns.len(), 1);
        assert_eq!(lineage.output_columns[0].name, "*");

        // Wildcard should map to all columns from source
        assert_eq!(lineage.column_mappings.len(), 1);
        assert_eq!(lineage.column_mappings[0].inputs[0].task_id, "orders");
    }

    #[test]
    fn computed_columns_detected() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT id, COALESCE(name, 'unknown') AS display_name FROM users"
        );
        assert!(!lineage.output_columns[0].is_computed); // id
        assert!(lineage.output_columns[1].is_computed);   // COALESCE(...)
    }

    #[test]
    fn qualified_column_references_resolved() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT o.total, c.name
             FROM orders o
             JOIN customers c ON o.customer_id = c.id"
        );

        // Find the mapping for "total"
        let total_mapping = lineage.column_mappings.iter()
            .find(|m| m.output == "total")
            .expect("should have mapping for total");

        // Should reference orders.total (resolved from alias "o")
        assert!(total_mapping.inputs.iter().any(|r| r.task_id == "orders" && r.column_name == "total"));
    }

    #[test]
    fn schema_qualified_table() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT id FROM warehouse.orders"
        );
        assert_eq!(lineage.source_tables[0].schema.as_deref(), Some("warehouse"));
        assert_eq!(lineage.source_tables[0].name, "orders");
    }

    #[test]
    fn multiple_from_tables() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT a.id, b.name FROM orders a, customers b WHERE a.customer_id = b.id"
        );
        assert_eq!(lineage.source_tables.len(), 2);
    }
}
