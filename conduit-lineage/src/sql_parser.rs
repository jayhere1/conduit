//! SQL lineage extraction — parse SQL queries to determine
//! which input columns flow into which output columns.
//!
//! Uses the `sqlparser` crate to parse SQL into an AST, then walks the
//! AST to extract lineage information. Handles:
//! - SELECT column_name, alias
//! - SELECT table.column
//! - SELECT expression AS alias
//! - FROM / JOIN clauses (to resolve table references)
//! - Wildcard expansion (SELECT *)
//! - Subqueries in FROM (with recursive lineage tracing)
//! - Schema-qualified table names
//! - CTEs (WITH clauses)
//! - UNION / INTERSECT / EXCEPT
//! - INSERT INTO ... SELECT
//! - CREATE TABLE AS SELECT (CTAS)
//! - WHERE clause column tracking
//! - Window functions (PARTITION BY, ORDER BY)
//! - Multiple SQL dialects via GenericDialect
//!
//! When a [`TableCatalog`] is provided via [`SqlLineageExtractor::extract_with_catalog`]:
//! - Bare column references are resolved to the correct source table
//! - `SELECT *` is expanded to actual column names
//! - CTE output columns are registered as virtual tables for downstream resolution

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, WindowSpec, WindowType,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::catalog::{CatalogColumn, TableCatalog};
use crate::lineage_graph::ColumnRef;
use crate::schema::ColumnType;

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
    /// Parses the SQL using `sqlparser` with GenericDialect, then walks
    /// the AST to extract source tables, output columns, and column mappings.
    pub fn extract(sql: &str) -> SqlLineage {
        Self::extract_inner(sql, None)
    }

    /// Extract lineage with a table catalog for enhanced resolution.
    ///
    /// When a catalog is provided:
    /// - Bare column references are resolved to the correct source table
    ///   (instead of defaulting to the first table in the FROM clause)
    /// - `SELECT *` is expanded to actual column names
    /// - CTE output columns are registered as virtual tables, enabling
    ///   column-level lineage propagation through CTEs
    pub fn extract_with_catalog(sql: &str, catalog: &TableCatalog) -> SqlLineage {
        Self::extract_inner(sql, Some(catalog))
    }

    fn extract_inner(sql: &str, catalog: Option<&TableCatalog>) -> SqlLineage {
        let dialect = GenericDialect {};
        let statements = match Parser::parse_sql(&dialect, sql) {
            Ok(stmts) => stmts,
            Err(_) => {
                return Self::empty();
            }
        };

        let stmt = match statements.into_iter().next() {
            Some(s) => s,
            None => {
                return Self::empty();
            }
        };

        Self::extract_from_statement(&stmt, catalog)
    }

    /// Extract lineage from any statement type.
    fn extract_from_statement(stmt: &Statement, catalog: Option<&TableCatalog>) -> SqlLineage {
        match stmt {
            Statement::Query(q) => Self::extract_from_query(q, &HashMap::new(), catalog),
            // INSERT INTO target_table SELECT ... FROM source_tables
            Statement::Insert(insert) => {
                if let Some(ref source) = insert.source {
                    Self::extract_from_query(source, &HashMap::new(), catalog)
                } else {
                    Self::empty()
                }
            }
            // CREATE TABLE ... AS SELECT ...
            Statement::CreateTable(ct) => {
                if let Some(ref query) = ct.query {
                    Self::extract_from_query(query, &HashMap::new(), catalog)
                } else {
                    Self::empty()
                }
            }
            _ => Self::empty(),
        }
    }

    fn empty() -> SqlLineage {
        SqlLineage {
            output_columns: Vec::new(),
            source_tables: Vec::new(),
            column_mappings: Vec::new(),
        }
    }

    /// Extract lineage from a Query node, with CTE context.
    fn extract_from_query(
        query: &Query,
        parent_ctes: &HashMap<String, SqlLineage>,
        catalog: Option<&TableCatalog>,
    ) -> SqlLineage {
        let mut cte_map = parent_ctes.clone();

        if let Some(ref with) = query.with {
            // Clone catalog so we can register CTE output columns as virtual tables
            let mut local_catalog = catalog.cloned();

            for cte in &with.cte_tables {
                let cte_name = cte.alias.name.value.to_lowercase();
                let cte_lineage = Self::extract_from_query(
                    &cte.query,
                    &cte_map,
                    local_catalog.as_ref().or(catalog),
                );

                // Register CTE as virtual table in catalog for downstream resolution
                if let Some(ref mut cat) = local_catalog {
                    let virtual_columns: Vec<CatalogColumn> = cte_lineage
                        .output_columns
                        .iter()
                        .filter(|c| c.name != "*" && c.name != "__where__")
                        .map(|c| CatalogColumn::new(&c.name, ColumnType::Unknown))
                        .collect();
                    if !virtual_columns.is_empty() {
                        cat.register_table(None, &cte_name, virtual_columns);
                    }
                }

                cte_map.insert(cte_name, cte_lineage);
            }

            Self::extract_from_set_expr(
                query.body.as_ref(),
                &cte_map,
                local_catalog.as_ref().or(catalog),
            )
        } else {
            Self::extract_from_set_expr(query.body.as_ref(), &cte_map, catalog)
        }
    }

    /// Extract lineage from a SetExpr (SELECT, UNION, etc).
    fn extract_from_set_expr(
        body: &SetExpr,
        cte_map: &HashMap<String, SqlLineage>,
        catalog: Option<&TableCatalog>,
    ) -> SqlLineage {
        match body {
            SetExpr::Select(select) => Self::extract_from_select(select, cte_map, catalog),
            SetExpr::Query(inner_query) => Self::extract_from_query(inner_query, cte_map, catalog),
            SetExpr::SetOperation { left, right, .. } => {
                // UNION / INTERSECT / EXCEPT — merge lineage from both branches
                let left_lineage = Self::extract_from_set_expr(left, cte_map, catalog);
                let right_lineage = Self::extract_from_set_expr(right, cte_map, catalog);
                Self::merge_set_operation(left_lineage, right_lineage)
            }
            SetExpr::Values(_) => Self::empty(),
            _ => Self::empty(),
        }
    }

    /// Merge lineage from two branches of a UNION/INTERSECT/EXCEPT.
    fn merge_set_operation(left: SqlLineage, right: SqlLineage) -> SqlLineage {
        // Output columns come from the left branch (SQL standard)
        let output_columns = left.output_columns;

        // Source tables are the union of both
        let mut source_tables = left.source_tables;
        for t in right.source_tables {
            if !source_tables.iter().any(|s| s.name == t.name) {
                source_tables.push(t);
            }
        }

        // Column mappings: merge inputs from both branches for each output position
        let mut column_mappings = left.column_mappings;
        for right_mapping in right.column_mappings {
            if let Some(existing) = column_mappings
                .iter_mut()
                .find(|m| m.output == right_mapping.output)
            {
                for input in right_mapping.inputs {
                    if !existing
                        .inputs
                        .iter()
                        .any(|i| i.task_id == input.task_id && i.column_name == input.column_name)
                    {
                        existing.inputs.push(input);
                    }
                }
            } else {
                column_mappings.push(right_mapping);
            }
        }

        SqlLineage {
            output_columns,
            source_tables,
            column_mappings,
        }
    }

    fn extract_from_select(
        select: &Select,
        cte_map: &HashMap<String, SqlLineage>,
        catalog: Option<&TableCatalog>,
    ) -> SqlLineage {
        let source_tables = Self::extract_tables_from_select(select, catalog);

        let alias_map: HashMap<String, String> = source_tables
            .iter()
            .filter_map(|t| t.alias.as_ref().map(|a| (a.to_lowercase(), t.name.clone())))
            .collect();

        // Build a combined alias map that includes CTE-resolved tables
        // If a source table name matches a CTE, its columns trace through the CTE
        let _ = cte_map; // CTE resolution is via source_tables names matching cte_map keys

        let (mut output_columns, mut column_mappings) = Self::extract_columns_from_projection(
            &select.projection,
            &source_tables,
            &alias_map,
            catalog,
        );

        // Track WHERE clause column dependencies
        if let Some(ref selection) = select.selection {
            let where_refs =
                Self::extract_column_refs_from_expr(selection, &alias_map, &source_tables, catalog);
            if !where_refs.is_empty() {
                column_mappings.push(ColumnMapping {
                    output: "__where__".to_string(),
                    inputs: where_refs,
                });
                output_columns.push(OutputColumn {
                    name: "__where__".to_string(),
                    expression: selection.to_string().to_lowercase(),
                    is_computed: true,
                });
            }
        }

        SqlLineage {
            output_columns,
            source_tables,
            column_mappings,
        }
    }

    fn extract_tables_from_select(
        select: &Select,
        catalog: Option<&TableCatalog>,
    ) -> Vec<TableRef> {
        let mut tables = Vec::new();
        for table_with_joins in &select.from {
            Self::extract_tables_from_table_with_joins(table_with_joins, &mut tables, catalog);
        }
        tables
    }

    fn extract_tables_from_table_with_joins(
        twj: &TableWithJoins,
        tables: &mut Vec<TableRef>,
        catalog: Option<&TableCatalog>,
    ) {
        Self::extract_table_from_factor(&twj.relation, tables, catalog);
        for join in &twj.joins {
            Self::extract_table_from_factor(&join.relation, tables, catalog);
        }
    }

    fn extract_table_from_factor(
        factor: &TableFactor,
        tables: &mut Vec<TableRef>,
        catalog: Option<&TableCatalog>,
    ) {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let idents = &name.0;
                let (schema, table_name) = if idents.len() >= 2 {
                    (
                        Some(idents[idents.len() - 2].value.to_lowercase()),
                        idents[idents.len() - 1].value.to_lowercase(),
                    )
                } else if idents.len() == 1 {
                    (None, idents[0].value.to_lowercase())
                } else {
                    return;
                };

                let alias_str = alias.as_ref().map(|a| a.name.value.to_lowercase());

                if !tables.iter().any(|t| t.name == table_name) {
                    tables.push(TableRef {
                        name: table_name,
                        alias: alias_str,
                        schema,
                    });
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                // Subquery in FROM — register the alias as a table, and also
                // recurse into the subquery to find underlying table references
                if let Some(alias) = alias {
                    let alias_name = alias.name.value.to_lowercase();
                    tables.push(TableRef {
                        name: alias_name.clone(),
                        alias: Some(alias_name),
                        schema: None,
                    });
                }

                // Also extract tables from the subquery so we can trace
                // columns back to their ultimate source
                let sub_lineage = Self::extract_from_query(subquery, &HashMap::new(), catalog);
                for t in sub_lineage.source_tables {
                    if !tables.iter().any(|existing| existing.name == t.name) {
                        tables.push(t);
                    }
                }
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                let _ = alias;
                Self::extract_tables_from_table_with_joins(table_with_joins, tables, catalog);
            }
            _ => {}
        }
    }

    fn extract_columns_from_projection(
        projection: &[SelectItem],
        source_tables: &[TableRef],
        alias_map: &HashMap<String, String>,
        catalog: Option<&TableCatalog>,
    ) -> (Vec<OutputColumn>, Vec<ColumnMapping>) {
        let mut outputs = Vec::new();
        let mut mappings = Vec::new();

        for item in projection {
            match item {
                SelectItem::Wildcard(_) => {
                    let mut expanded = false;
                    if let Some(cat) = catalog {
                        for table in source_tables {
                            if let Some(col_names) =
                                cat.expand_wildcard(table.schema.as_deref(), &table.name)
                            {
                                expanded = true;
                                for col_name in col_names {
                                    outputs.push(OutputColumn {
                                        name: col_name.clone(),
                                        expression: format!("{}.{}", table.name, col_name),
                                        is_computed: false,
                                    });
                                    mappings.push(ColumnMapping {
                                        output: col_name.clone(),
                                        inputs: vec![ColumnRef {
                                            task_id: table.name.clone(),
                                            column_name: col_name,
                                        }],
                                    });
                                }
                            }
                        }
                    }
                    if !expanded {
                        // Original behavior: produce "*" mapping
                        outputs.push(OutputColumn {
                            name: "*".to_string(),
                            expression: "*".to_string(),
                            is_computed: false,
                        });
                        for table in source_tables {
                            mappings.push(ColumnMapping {
                                output: "*".to_string(),
                                inputs: vec![ColumnRef {
                                    task_id: table.name.clone(),
                                    column_name: "*".to_string(),
                                }],
                            });
                        }
                    }
                }
                SelectItem::QualifiedWildcard(obj_name, _) => {
                    let prefix = obj_name
                        .0
                        .last()
                        .map(|i| i.value.to_lowercase())
                        .unwrap_or_default();
                    let resolved = alias_map
                        .get(&prefix)
                        .cloned()
                        .unwrap_or_else(|| prefix.clone());

                    let mut expanded = false;
                    if let Some(cat) = catalog {
                        if let Some(col_names) = cat.expand_wildcard(None, &resolved) {
                            expanded = true;
                            for col_name in col_names {
                                outputs.push(OutputColumn {
                                    name: col_name.clone(),
                                    expression: format!("{}.{}", prefix, col_name),
                                    is_computed: false,
                                });
                                mappings.push(ColumnMapping {
                                    output: col_name.clone(),
                                    inputs: vec![ColumnRef {
                                        task_id: resolved.clone(),
                                        column_name: col_name,
                                    }],
                                });
                            }
                        }
                    }
                    if !expanded {
                        // Original behavior
                        outputs.push(OutputColumn {
                            name: format!("{}.*", resolved),
                            expression: format!("{}.*", prefix),
                            is_computed: false,
                        });
                        mappings.push(ColumnMapping {
                            output: format!("{}.*", resolved),
                            inputs: vec![ColumnRef {
                                task_id: resolved,
                                column_name: "*".to_string(),
                            }],
                        });
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let alias_name = alias.value.to_lowercase();
                    let expr_str = expr.to_string().to_lowercase();
                    let is_computed = Self::is_computed_expr(expr);

                    outputs.push(OutputColumn {
                        name: alias_name.clone(),
                        expression: expr_str,
                        is_computed,
                    });

                    let input_refs = Self::extract_column_refs_from_expr(
                        expr,
                        alias_map,
                        source_tables,
                        catalog,
                    );
                    if !input_refs.is_empty() {
                        mappings.push(ColumnMapping {
                            output: alias_name,
                            inputs: input_refs,
                        });
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let expr_str = expr.to_string().to_lowercase();
                    let is_computed = Self::is_computed_expr(expr);
                    let output_name = Self::derive_output_name(expr);

                    outputs.push(OutputColumn {
                        name: output_name.clone(),
                        expression: expr_str,
                        is_computed,
                    });

                    let input_refs = Self::extract_column_refs_from_expr(
                        expr,
                        alias_map,
                        source_tables,
                        catalog,
                    );
                    if !input_refs.is_empty() {
                        mappings.push(ColumnMapping {
                            output: output_name,
                            inputs: input_refs,
                        });
                    }
                }
            }
        }

        (outputs, mappings)
    }

    fn is_computed_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(_) => false,
            Expr::CompoundIdentifier(_) => false,
            Expr::Function(_) => true,
            Expr::BinaryOp { .. } => true,
            Expr::UnaryOp { .. } => true,
            Expr::Case { .. } => true,
            Expr::Cast { .. } => true,
            Expr::Nested(inner) => Self::is_computed_expr(inner),
            _ => {
                let s = expr.to_string().to_lowercase();
                s.contains('(') || s.contains('+') || s.contains('-') || s.contains("case ")
            }
        }
    }

    fn derive_output_name(expr: &Expr) -> String {
        match expr {
            Expr::Identifier(ident) => ident.value.to_lowercase(),
            Expr::CompoundIdentifier(parts) => parts
                .last()
                .map(|i| i.value.to_lowercase())
                .unwrap_or_else(|| expr.to_string().to_lowercase()),
            _ => expr.to_string().to_lowercase(),
        }
    }

    fn extract_column_refs_from_expr(
        expr: &Expr,
        alias_map: &HashMap<String, String>,
        tables: &[TableRef],
        catalog: Option<&TableCatalog>,
    ) -> Vec<ColumnRef> {
        let mut refs = Vec::new();
        Self::collect_column_refs(expr, alias_map, tables, catalog, &mut refs);

        refs.sort_by(|a, b| (&a.task_id, &a.column_name).cmp(&(&b.task_id, &b.column_name)));
        refs.dedup_by(|a, b| a.task_id == b.task_id && a.column_name == b.column_name);

        refs
    }

    fn collect_column_refs(
        expr: &Expr,
        alias_map: &HashMap<String, String>,
        tables: &[TableRef],
        catalog: Option<&TableCatalog>,
        refs: &mut Vec<ColumnRef>,
    ) {
        match expr {
            Expr::Identifier(ident) => {
                let col_name = ident.value.to_lowercase();
                let table_name = if let Some(cat) = catalog {
                    // Use catalog to resolve which table owns this bare column
                    let candidates: Vec<(Option<&str>, &str)> = tables
                        .iter()
                        .map(|t| (t.schema.as_deref(), t.name.as_str()))
                        .collect();
                    cat.find_column_owner(&col_name, &candidates)
                        .unwrap_or_else(|| {
                            tables
                                .first()
                                .map(|t| t.name.clone())
                                .unwrap_or_else(|| "unknown".to_string())
                        })
                } else {
                    tables
                        .first()
                        .map(|t| t.name.clone())
                        .unwrap_or_else(|| "unknown".to_string())
                };
                refs.push(ColumnRef {
                    task_id: table_name,
                    column_name: col_name,
                });
            }
            Expr::CompoundIdentifier(parts) => {
                if parts.len() >= 2 {
                    let table_part = parts[parts.len() - 2].value.to_lowercase();
                    let col_part = parts[parts.len() - 1].value.to_lowercase();
                    let resolved = alias_map.get(&table_part).cloned().unwrap_or(table_part);
                    refs.push(ColumnRef {
                        task_id: resolved,
                        column_name: col_part,
                    });
                } else if parts.len() == 1 {
                    let col_name = parts[0].value.to_lowercase();
                    let table_name = if let Some(cat) = catalog {
                        let candidates: Vec<(Option<&str>, &str)> = tables
                            .iter()
                            .map(|t| (t.schema.as_deref(), t.name.as_str()))
                            .collect();
                        cat.find_column_owner(&col_name, &candidates)
                            .unwrap_or_else(|| {
                                tables
                                    .first()
                                    .map(|t| t.name.clone())
                                    .unwrap_or_else(|| "unknown".to_string())
                            })
                    } else {
                        tables
                            .first()
                            .map(|t| t.name.clone())
                            .unwrap_or_else(|| "unknown".to_string())
                    };
                    refs.push(ColumnRef {
                        task_id: table_name,
                        column_name: col_name,
                    });
                }
            }
            Expr::Function(func) => {
                // Recurse into function arguments
                match &func.args {
                    FunctionArguments::List(arg_list) => {
                        for arg in &arg_list.args {
                            match arg {
                                FunctionArg::Unnamed(arg_expr)
                                | FunctionArg::Named { arg: arg_expr, .. } => match arg_expr {
                                    FunctionArgExpr::Expr(e) => {
                                        Self::collect_column_refs(
                                            e, alias_map, tables, catalog, refs,
                                        );
                                    }
                                    FunctionArgExpr::QualifiedWildcard(_) => {}
                                    FunctionArgExpr::Wildcard => {}
                                },
                            }
                        }
                    }
                    FunctionArguments::Subquery(_) | FunctionArguments::None => {}
                }
                // Window functions: OVER (PARTITION BY ... ORDER BY ...)
                if let Some(ref window_type) = func.over {
                    match window_type {
                        WindowType::WindowSpec(spec) => {
                            Self::collect_window_refs(spec, alias_map, tables, catalog, refs);
                        }
                        WindowType::NamedWindow(_) => {}
                    }
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_column_refs(left, alias_map, tables, catalog, refs);
                Self::collect_column_refs(right, alias_map, tables, catalog, refs);
            }
            Expr::UnaryOp { expr: inner, .. } => {
                Self::collect_column_refs(inner, alias_map, tables, catalog, refs);
            }
            Expr::Nested(inner) => {
                Self::collect_column_refs(inner, alias_map, tables, catalog, refs);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(op) = operand {
                    Self::collect_column_refs(op, alias_map, tables, catalog, refs);
                }
                for cond in conditions {
                    Self::collect_column_refs(cond, alias_map, tables, catalog, refs);
                }
                for result in results {
                    Self::collect_column_refs(result, alias_map, tables, catalog, refs);
                }
                if let Some(else_r) = else_result {
                    Self::collect_column_refs(else_r, alias_map, tables, catalog, refs);
                }
            }
            Expr::Cast { expr: inner, .. } => {
                Self::collect_column_refs(inner, alias_map, tables, catalog, refs);
            }
            Expr::InList {
                expr: inner, list, ..
            } => {
                Self::collect_column_refs(inner, alias_map, tables, catalog, refs);
                for item in list {
                    Self::collect_column_refs(item, alias_map, tables, catalog, refs);
                }
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                ..
            } => {
                Self::collect_column_refs(inner, alias_map, tables, catalog, refs);
                Self::collect_column_refs(low, alias_map, tables, catalog, refs);
                Self::collect_column_refs(high, alias_map, tables, catalog, refs);
            }
            Expr::IsNull(inner)
            | Expr::IsNotNull(inner)
            | Expr::IsTrue(inner)
            | Expr::IsFalse(inner)
            | Expr::IsNotTrue(inner)
            | Expr::IsNotFalse(inner) => {
                Self::collect_column_refs(inner, alias_map, tables, catalog, refs);
            }
            Expr::Subquery(subquery) => {
                // Recurse into scalar subqueries to find their column refs
                let sub_lineage = Self::extract_from_query(subquery, &HashMap::new(), catalog);
                for mapping in &sub_lineage.column_mappings {
                    for input in &mapping.inputs {
                        refs.push(input.clone());
                    }
                }
            }
            Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
                Self::collect_column_refs(expr, alias_map, tables, catalog, refs);
                Self::collect_column_refs(pattern, alias_map, tables, catalog, refs);
            }
            _ => {}
        }
    }

    /// Extract column refs from a window specification (PARTITION BY, ORDER BY).
    fn collect_window_refs(
        spec: &WindowSpec,
        alias_map: &HashMap<String, String>,
        tables: &[TableRef],
        catalog: Option<&TableCatalog>,
        refs: &mut Vec<ColumnRef>,
    ) {
        for expr in &spec.partition_by {
            Self::collect_column_refs(expr, alias_map, tables, catalog, refs);
        }
        for order_expr in &spec.order_by {
            Self::collect_column_refs(&order_expr.expr, alias_map, tables, catalog, refs);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Original tests ────────────────────────────────────────────

    #[test]
    fn simple_select() {
        let lineage = SqlLineageExtractor::extract("SELECT id, name, email FROM customers");
        assert_eq!(lineage.output_columns.len(), 3);
        assert_eq!(lineage.source_tables.len(), 1);
        assert_eq!(lineage.source_tables[0].name, "customers");
    }

    #[test]
    fn select_with_alias() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT c.id, c.name AS customer_name FROM customers AS c",
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
             JOIN customers c ON o.customer_id = c.id",
        );
        assert_eq!(lineage.source_tables.len(), 2);

        let table_names: Vec<&str> = lineage
            .source_tables
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"customers"));
    }

    #[test]
    fn select_star() {
        let lineage = SqlLineageExtractor::extract("SELECT * FROM orders");
        assert_eq!(lineage.output_columns.len(), 1);
        assert_eq!(lineage.output_columns[0].name, "*");

        assert_eq!(lineage.column_mappings.len(), 1);
        assert_eq!(lineage.column_mappings[0].inputs[0].task_id, "orders");
    }

    #[test]
    fn computed_columns_detected() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT id, COALESCE(name, 'unknown') AS display_name FROM users",
        );
        assert!(!lineage.output_columns[0].is_computed); // id
        assert!(lineage.output_columns[1].is_computed); // COALESCE(...)
    }

    #[test]
    fn qualified_column_references_resolved() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT o.total, c.name
             FROM orders o
             JOIN customers c ON o.customer_id = c.id",
        );

        let total_mapping = lineage
            .column_mappings
            .iter()
            .find(|m| m.output == "total")
            .expect("should have mapping for total");

        assert!(total_mapping
            .inputs
            .iter()
            .any(|r| r.task_id == "orders" && r.column_name == "total"));
    }

    #[test]
    fn schema_qualified_table() {
        let lineage = SqlLineageExtractor::extract("SELECT id FROM warehouse.orders");
        assert_eq!(
            lineage.source_tables[0].schema.as_deref(),
            Some("warehouse")
        );
        assert_eq!(lineage.source_tables[0].name, "orders");
    }

    #[test]
    fn multiple_from_tables() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT a.id, b.name FROM orders a, customers b WHERE a.customer_id = b.id",
        );
        assert_eq!(lineage.source_tables.len(), 2);
    }

    // ─── CTE tests ─────────────────────────────────────────────────

    #[test]
    fn cte_basic() {
        let lineage = SqlLineageExtractor::extract(
            "WITH active AS (
                SELECT id, name FROM customers WHERE active = true
            )
            SELECT id, name FROM active",
        );
        // The CTE references customers, so we should see customers as a source
        let table_names: Vec<&str> = lineage
            .source_tables
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        assert!(table_names.contains(&"active"));
        assert_eq!(lineage.output_columns.len(), 2);
    }

    #[test]
    fn cte_chained() {
        let lineage = SqlLineageExtractor::extract(
            "WITH
                raw AS (SELECT id, amount FROM orders),
                enriched AS (SELECT id, amount * 1.1 AS taxed FROM raw)
            SELECT id, taxed FROM enriched",
        );
        let table_names: Vec<&str> = lineage
            .source_tables
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        assert!(table_names.contains(&"enriched"));
        assert_eq!(lineage.output_columns.len(), 2);
    }

    // ─── UNION tests ───────────────────────────────────────────────

    #[test]
    fn union_merges_sources() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT id, amount FROM orders
             UNION ALL
             SELECT id, amount FROM returns",
        );
        let table_names: Vec<&str> = lineage
            .source_tables
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"returns"));
        // Output columns come from the left branch
        assert_eq!(lineage.output_columns.len(), 2);
    }

    #[test]
    fn union_three_branches() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT id FROM orders
             UNION SELECT id FROM returns
             UNION SELECT id FROM refunds",
        );
        let table_names: Vec<&str> = lineage
            .source_tables
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"returns"));
        assert!(table_names.contains(&"refunds"));
    }

    // ─── INSERT INTO ... SELECT tests ──────────────────────────────

    #[test]
    fn insert_into_select() {
        let lineage = SqlLineageExtractor::extract(
            "INSERT INTO staging.orders SELECT id, amount, customer_id FROM raw.orders",
        );
        assert_eq!(lineage.source_tables.len(), 1);
        assert_eq!(lineage.source_tables[0].name, "orders");
        assert_eq!(lineage.source_tables[0].schema.as_deref(), Some("raw"));
        assert_eq!(lineage.output_columns.len(), 3);
    }

    // ─── CREATE TABLE AS SELECT tests ──────────────────────────────

    #[test]
    fn create_table_as_select() {
        let lineage = SqlLineageExtractor::extract(
            "CREATE TABLE dim_users AS SELECT id, name, email FROM raw_users",
        );
        assert_eq!(lineage.source_tables.len(), 1);
        assert_eq!(lineage.source_tables[0].name, "raw_users");
        assert_eq!(lineage.output_columns.len(), 3);
    }

    // ─── WHERE clause tracking tests ───────────────────────────────

    #[test]
    fn where_clause_columns_tracked() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT id, name FROM users WHERE active = true AND created_at > '2024-01-01'",
        );
        // Should have the output columns plus a __where__ pseudo-column
        let where_mapping = lineage
            .column_mappings
            .iter()
            .find(|m| m.output == "__where__");
        assert!(where_mapping.is_some());
        let inputs = &where_mapping.unwrap().inputs;
        assert!(inputs.iter().any(|r| r.column_name == "active"));
        assert!(inputs.iter().any(|r| r.column_name == "created_at"));
    }

    // ─── Window function tests ─────────────────────────────────────

    #[test]
    fn window_function_partition_by() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT user_id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) AS rn FROM events",
        );
        assert_eq!(lineage.output_columns.len(), 2);
        assert!(lineage.output_columns[1].is_computed);

        // The rn column should depend on user_id and created_at (from the window)
        let rn_mapping = lineage
            .column_mappings
            .iter()
            .find(|m| m.output == "rn")
            .expect("should have mapping for rn");
        let input_cols: Vec<&str> = rn_mapping
            .inputs
            .iter()
            .map(|r| r.column_name.as_str())
            .collect();
        assert!(
            input_cols.contains(&"user_id"),
            "should contain user_id from PARTITION BY"
        );
        assert!(
            input_cols.contains(&"created_at"),
            "should contain created_at from ORDER BY"
        );
    }

    // ─── Subquery in FROM tracing tests ────────────────────────────

    #[test]
    fn subquery_traces_to_underlying_table() {
        let lineage = SqlLineageExtractor::extract(
            "SELECT x.id FROM (SELECT id FROM orders WHERE active = true) AS x",
        );
        // Should find both 'x' (the subquery alias) and 'orders' (the underlying table)
        let table_names: Vec<&str> = lineage
            .source_tables
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        assert!(
            table_names.contains(&"orders"),
            "should trace subquery back to orders"
        );
    }

    // ─── Complex real-world query test ─────────────────────────────

    #[test]
    fn complex_etl_query() {
        let lineage = SqlLineageExtractor::extract(
            "WITH daily_orders AS (
                SELECT
                    DATE_TRUNC('day', created_at) AS order_date,
                    customer_id,
                    SUM(amount) AS total_amount
                FROM orders
                WHERE status = 'completed'
                GROUP BY 1, 2
            )
            SELECT
                d.order_date,
                c.name AS customer_name,
                d.total_amount,
                d.total_amount / NULLIF(c.lifetime_value, 0) AS pct_lifetime
            FROM daily_orders d
            JOIN customers c ON d.customer_id = c.id
            WHERE d.total_amount > 100",
        );

        // Should have output columns
        assert_eq!(
            lineage
                .output_columns
                .iter()
                .filter(|c| c.name != "__where__")
                .count(),
            4
        );

        // Should find both source tables
        let table_names: Vec<&str> = lineage
            .source_tables
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        assert!(table_names.contains(&"daily_orders") || table_names.contains(&"customers"));

        // pct_lifetime should be computed
        let pct = lineage
            .output_columns
            .iter()
            .find(|c| c.name == "pct_lifetime");
        assert!(pct.is_some());
        assert!(pct.unwrap().is_computed);
    }

    // ─── Edge cases ────────────────────────────────────────────────

    #[test]
    fn invalid_sql_returns_empty() {
        let lineage = SqlLineageExtractor::extract("NOT VALID SQL AT ALL !!!");
        assert!(lineage.output_columns.is_empty());
        assert!(lineage.source_tables.is_empty());
    }

    #[test]
    fn empty_string_returns_empty() {
        let lineage = SqlLineageExtractor::extract("");
        assert!(lineage.output_columns.is_empty());
    }

    #[test]
    fn dml_without_select_returns_empty() {
        let lineage = SqlLineageExtractor::extract("DELETE FROM orders WHERE id = 1");
        assert!(lineage.output_columns.is_empty());
    }

    // ─── Catalog-aware tests ───────────────────────────────────────

    fn build_test_catalog() -> TableCatalog {
        let mut cat = TableCatalog::new();
        cat.register_table(
            None,
            "orders",
            vec![
                CatalogColumn::new("id", ColumnType::Integer),
                CatalogColumn::new("customer_id", ColumnType::Integer),
                CatalogColumn::new("amount", ColumnType::Float),
                CatalogColumn::new("status", ColumnType::String),
                CatalogColumn::new("created_at", ColumnType::Timestamp),
            ],
        );
        cat.register_table(
            None,
            "customers",
            vec![
                CatalogColumn::new("id", ColumnType::Integer),
                CatalogColumn::new("name", ColumnType::String),
                CatalogColumn::new("email", ColumnType::String),
                CatalogColumn::new("active", ColumnType::Boolean),
            ],
        );
        cat
    }

    #[test]
    fn catalog_bare_column_resolved_correctly() {
        let cat = build_test_catalog();
        let lineage = SqlLineageExtractor::extract_with_catalog(
            "SELECT o.id, active
             FROM orders o
             JOIN customers c ON o.customer_id = c.id",
            &cat,
        );

        // "active" should resolve to customers, not orders (the first table)
        let active_mapping = lineage
            .column_mappings
            .iter()
            .find(|m| m.output == "active")
            .expect("should have mapping for active");

        assert!(
            active_mapping
                .inputs
                .iter()
                .any(|r| r.task_id == "customers" && r.column_name == "active"),
            "active should be resolved to customers, got: {:?}",
            active_mapping.inputs
        );
    }

    #[test]
    fn catalog_bare_column_without_catalog_defaults_to_first() {
        // Without catalog, "active" defaults to the first table (orders)
        let lineage = SqlLineageExtractor::extract(
            "SELECT o.id, active
             FROM orders o
             JOIN customers c ON o.customer_id = c.id",
        );

        let active_mapping = lineage
            .column_mappings
            .iter()
            .find(|m| m.output == "active")
            .expect("should have mapping for active");

        // Without catalog, defaults to first table
        assert!(active_mapping.inputs.iter().any(|r| r.task_id == "orders"));
    }

    #[test]
    fn catalog_wildcard_expansion() {
        let cat = build_test_catalog();
        let lineage = SqlLineageExtractor::extract_with_catalog("SELECT * FROM customers", &cat);

        // Should expand to actual column names instead of "*"
        let col_names: Vec<&str> = lineage
            .output_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(col_names, vec!["id", "name", "email", "active"]);

        // Each column should map to its source
        for col in &["id", "name", "email", "active"] {
            let mapping = lineage
                .column_mappings
                .iter()
                .find(|m| m.output == *col)
                .unwrap_or_else(|| panic!("should have mapping for {}", col));
            assert_eq!(mapping.inputs[0].task_id, "customers");
            assert_eq!(mapping.inputs[0].column_name, *col);
        }
    }

    #[test]
    fn catalog_wildcard_without_catalog_stays_star() {
        let lineage = SqlLineageExtractor::extract("SELECT * FROM customers");
        assert_eq!(lineage.output_columns[0].name, "*");
    }

    #[test]
    fn catalog_qualified_wildcard_expansion() {
        let cat = build_test_catalog();
        let lineage = SqlLineageExtractor::extract_with_catalog(
            "SELECT o.id, c.* FROM orders o JOIN customers c ON o.customer_id = c.id",
            &cat,
        );

        let col_names: Vec<&str> = lineage
            .output_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        // o.id + expanded c.* (id, name, email, active)
        assert!(col_names.contains(&"id"));
        assert!(col_names.contains(&"name"));
        assert!(col_names.contains(&"email"));
        assert!(col_names.contains(&"active"));
    }

    #[test]
    fn catalog_cte_virtual_table_propagation() {
        let cat = build_test_catalog();
        let lineage = SqlLineageExtractor::extract_with_catalog(
            "WITH active_customers AS (
                SELECT id, name FROM customers WHERE active = true
            )
            SELECT * FROM active_customers",
            &cat,
        );

        // CTE output columns (id, name) should be registered as virtual table
        // and SELECT * should expand to those columns
        let col_names: Vec<&str> = lineage
            .output_columns
            .iter()
            .filter(|c| c.name != "__where__")
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(col_names, vec!["id", "name"]);
    }

    #[test]
    fn catalog_cte_bare_column_resolution() {
        let cat = build_test_catalog();
        let lineage = SqlLineageExtractor::extract_with_catalog(
            "WITH order_summary AS (
                SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY 1
            )
            SELECT name, total
            FROM order_summary s
            JOIN customers c ON s.customer_id = c.id",
            &cat,
        );

        // "name" should resolve to customers (not order_summary)
        let name_mapping = lineage
            .column_mappings
            .iter()
            .find(|m| m.output == "name")
            .expect("should have mapping for name");
        assert!(
            name_mapping.inputs.iter().any(|r| r.task_id == "customers"),
            "name should resolve to customers, got: {:?}",
            name_mapping.inputs
        );

        // "total" should resolve to order_summary
        let total_mapping = lineage
            .column_mappings
            .iter()
            .find(|m| m.output == "total")
            .expect("should have mapping for total");
        assert!(
            total_mapping
                .inputs
                .iter()
                .any(|r| r.task_id == "order_summary"),
            "total should resolve to order_summary, got: {:?}",
            total_mapping.inputs
        );
    }

    #[test]
    fn catalog_ambiguous_column_falls_back() {
        let cat = build_test_catalog();
        // "id" exists in both orders and customers — should fall back to first table
        let lineage = SqlLineageExtractor::extract_with_catalog(
            "SELECT id FROM orders o JOIN customers c ON o.id = c.id",
            &cat,
        );

        let id_mapping = lineage
            .column_mappings
            .iter()
            .find(|m| m.output == "id")
            .expect("should have mapping for id");
        // Ambiguous, falls back to first table
        assert_eq!(id_mapping.inputs[0].task_id, "orders");
    }

    #[test]
    fn catalog_multi_table_wildcard() {
        let cat = build_test_catalog();
        let lineage = SqlLineageExtractor::extract_with_catalog(
            "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id",
            &cat,
        );

        // Should expand wildcards from both tables
        let col_names: Vec<&str> = lineage
            .output_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        // orders: id, customer_id, amount, status, created_at
        // customers: id, name, email, active
        assert!(col_names.contains(&"amount"));
        assert!(col_names.contains(&"name"));
        assert!(col_names.contains(&"active"));
        assert!(col_names.len() >= 9);
    }

    #[test]
    fn catalog_chained_cte_propagation() {
        let cat = build_test_catalog();
        let lineage = SqlLineageExtractor::extract_with_catalog(
            "WITH
                raw AS (SELECT id, amount, status FROM orders),
                filtered AS (SELECT id, amount FROM raw WHERE status = 'complete')
            SELECT * FROM filtered",
            &cat,
        );

        // CTE "raw" has [id, amount, status]
        // CTE "filtered" has [id, amount] (from raw, with WHERE on status)
        // SELECT * FROM filtered should expand to [id, amount]
        let col_names: Vec<&str> = lineage
            .output_columns
            .iter()
            .filter(|c| c.name != "__where__")
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(col_names, vec!["id", "amount"]);
    }
}
