//! Property-based tests for the SQL lineage extractor (PRD E1).
//!
//! `conduit-lineage` is the flagship crate but was the only core crate
//! without proptest coverage. These properties assert two things the
//! example-based unit tests can't cover across a generated corpus:
//!
//! 1. **No panic.** The extractor must never panic on any generated SQL,
//!    across all dialects — it returns an (possibly empty) `SqlLineage`.
//! 2. **Formatting invariance.** Column-level lineage must not change under
//!    formatting-only rewrites (extra whitespace, comments), because those
//!    do not change query semantics.

use std::collections::BTreeSet;

use conduit_lineage::{SqlDialect, SqlLineageExtractor};
use proptest::prelude::*;

/// A small alphabet of valid SQL identifiers.
fn ident() -> impl Strategy<Value = String> {
    prop::sample::select(vec![
        "id", "name", "email", "amount", "customer_id", "region", "total", "created_at", "status",
        "qty", "price", "orders", "customers", "staging", "analytics", "t", "a", "b",
    ])
    .prop_map(String::from)
}

/// Generate a syntactically plausible SELECT over 1-4 columns from a table,
/// optionally with a WHERE and a JOIN. Kept in the grammar sqlparser accepts
/// so we exercise the resolver, not the error path — though the no-panic
/// property below also feeds it arbitrary strings.
fn select_query() -> impl Strategy<Value = String> {
    (
        prop::collection::vec(ident(), 1..4),
        ident(),
        prop::option::of(ident()),
        any::<bool>(),
    )
        .prop_map(|(cols, table, filter_col, with_join)| {
            let col_list = cols.join(", ");
            let mut q = format!("SELECT {col_list} FROM {table}");
            if with_join {
                q.push_str(&format!(" JOIN customers ON {table}.id = customers.id"));
            }
            if let Some(fc) = filter_col {
                q.push_str(&format!(" WHERE {fc} IS NOT NULL"));
            }
            q
        })
}

/// Sorted set of "output -> [inputs]" mapping strings, for order-independent
/// comparison of two `SqlLineage` results.
fn mapping_fingerprint(lineage: &conduit_lineage::sql_parser::SqlLineage) -> BTreeSet<String> {
    lineage
        .column_mappings
        .iter()
        .map(|m| {
            let mut inputs: Vec<String> = m.inputs.iter().map(|c| c.to_string()).collect();
            inputs.sort();
            format!("{}=>{}", m.output, inputs.join(","))
        })
        .collect()
}

const DIALECTS: &[SqlDialect] = &[
    SqlDialect::Generic,
    SqlDialect::Postgres,
    SqlDialect::Snowflake,
    SqlDialect::BigQuery,
    SqlDialect::MySql,
];

proptest! {
    /// The extractor must never panic on arbitrary input, in any dialect.
    #[test]
    fn extract_never_panics_on_arbitrary_input(s in ".{0,120}") {
        for &dialect in DIALECTS {
            let _ = SqlLineageExtractor::extract_with_dialect(&s, dialect);
        }
    }

    /// The extractor must never panic on generated well-formed SELECTs.
    #[test]
    fn extract_never_panics_on_generated_selects(q in select_query()) {
        for &dialect in DIALECTS {
            let _ = SqlLineageExtractor::extract_with_dialect(&q, dialect);
        }
    }

    /// Extra whitespace is semantically irrelevant: the column-level lineage
    /// must be identical before and after collapsing/expanding spaces.
    #[test]
    fn lineage_is_invariant_under_whitespace(q in select_query()) {
        let base = SqlLineageExtractor::extract(&q);

        // Reformat: collapse runs of spaces, then pad every space to three.
        let collapsed: String = q.split_whitespace().collect::<Vec<_>>().join(" ");
        let padded = collapsed.replace(' ', "   ");
        let reformatted = SqlLineageExtractor::extract(&padded);

        prop_assert_eq!(
            mapping_fingerprint(&base),
            mapping_fingerprint(&reformatted),
            "whitespace changed lineage for query: {}",
            q
        );
    }

    /// A trailing line comment does not change query semantics, so it must
    /// not change the extracted lineage.
    #[test]
    fn lineage_is_invariant_under_trailing_comment(q in select_query()) {
        let base = SqlLineageExtractor::extract(&q);
        let commented = format!("{q} -- a trailing comment\n");
        let with_comment = SqlLineageExtractor::extract(&commented);

        prop_assert_eq!(
            mapping_fingerprint(&base),
            mapping_fingerprint(&with_comment),
            "a trailing comment changed lineage for query: {}",
            q
        );
    }
}
