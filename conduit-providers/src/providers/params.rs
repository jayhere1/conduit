//! Named-parameter rewriting for SQL providers.
//!
//! User task SQL uses `:name` placeholders (the style the SDK documents).
//! Each provider rewrites those to its native placeholder syntax and binds
//! the values through sqlx — parameters are data, never string-spliced SQL.

use std::collections::HashMap;

/// Native placeholder syntax of the target database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaceholderStyle {
    /// `$1, $2, …` — PostgreSQL family (Postgres, CockroachDB, Timescale, Redshift).
    Dollar,
    /// `?` — MySQL and SQLite.
    Question,
}

/// Rewrite `:name` placeholders to the native style and return the ordered
/// values to bind. Skips quoted strings, quoted identifiers, comments, and
/// Postgres `::type` casts. Referencing a param that isn't in `params` is an
/// error; a query with no `:name` placeholders passes through untouched.
pub fn bind_named_params(
    query: &str,
    params: &HashMap<String, String>,
    style: PlaceholderStyle,
) -> Result<(String, Vec<String>), String> {
    let bytes = query.as_bytes();
    let mut out = String::with_capacity(query.len());
    let mut values = Vec::new();
    let mut i = 0;
    let n = bytes.len();

    while i < n {
        let c = bytes[i] as char;

        // Single-quoted string literal (with '' escaping).
        if c == '\'' {
            let start = i;
            i += 1;
            while i < n {
                if bytes[i] == b'\'' {
                    if i + 1 < n && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(&query[start..i]);
            continue;
        }

        // Double-quoted identifier.
        if c == '"' {
            let start = i;
            i += 1;
            while i < n && bytes[i] != b'"' {
                i += 1;
            }
            i = (i + 1).min(n);
            out.push_str(&query[start..i]);
            continue;
        }

        // Line comment.
        if c == '-' && i + 1 < n && bytes[i + 1] == b'-' {
            let start = i;
            while i < n && bytes[i] != b'\n' {
                i += 1;
            }
            out.push_str(&query[start..i]);
            continue;
        }

        // Block comment.
        if c == '/' && i + 1 < n && bytes[i + 1] == b'*' {
            let start = i;
            i += 2;
            while i + 1 < n && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(n);
            out.push_str(&query[start..i]);
            continue;
        }

        // `::` cast — copy both colons verbatim, never a placeholder.
        if c == ':' && i + 1 < n && bytes[i + 1] == b':' {
            out.push_str("::");
            i += 2;
            continue;
        }

        // `:name` placeholder (must start with a letter or underscore).
        if c == ':'
            && i + 1 < n
            && ((bytes[i + 1] as char).is_ascii_alphabetic() || bytes[i + 1] == b'_')
        {
            let mut j = i + 1;
            while j < n
                && ((bytes[j] as char).is_ascii_alphanumeric() || bytes[j] == b'_')
            {
                j += 1;
            }
            let name = &query[i + 1..j];
            let value = params.get(name).ok_or_else(|| {
                format!(
                    "query references :{} but no such parameter was supplied \
                     (available: {:?})",
                    name,
                    params.keys().collect::<Vec<_>>()
                )
            })?;
            values.push(value.clone());
            match style {
                PlaceholderStyle::Dollar => {
                    out.push_str(&format!("${}", values.len()));
                }
                PlaceholderStyle::Question => out.push('?'),
            }
            i = j;
            continue;
        }

        out.push(c);
        i += c.len_utf8();
    }

    Ok((out, values))
}

/// Bind a string value with type inference: integers, floats, and booleans
/// bind as their native types (so `WHERE id = :id` works against numeric
/// columns in strictly-typed databases); everything else binds as text.
macro_rules! bind_inferred {
    ($fn_name:ident, $db:ty) => {
        pub fn $fn_name<'q>(
            q: sqlx::query::Query<'q, $db, <$db as sqlx::Database>::Arguments<'q>>,
            value: &'q str,
        ) -> sqlx::query::Query<'q, $db, <$db as sqlx::Database>::Arguments<'q>> {
            if let Ok(i) = value.parse::<i64>() {
                q.bind(i)
            } else if let Ok(f) = value.parse::<f64>() {
                q.bind(f)
            } else if value.eq_ignore_ascii_case("true") {
                q.bind(true)
            } else if value.eq_ignore_ascii_case("false") {
                q.bind(false)
            } else {
                q.bind(value)
            }
        }
    };
}

bind_inferred!(bind_inferred_sqlite, sqlx::Sqlite);
bind_inferred!(bind_inferred_postgres, sqlx::Postgres);
bind_inferred!(bind_inferred_mysql, sqlx::MySql);

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn rewrites_to_dollar_placeholders_in_order() {
        let (q, v) = bind_named_params(
            "SELECT * FROM t WHERE a = :a AND b = :b",
            &params(&[("a", "1"), ("b", "x")]),
            PlaceholderStyle::Dollar,
        )
        .unwrap();
        assert_eq!(q, "SELECT * FROM t WHERE a = $1 AND b = $2");
        assert_eq!(v, vec!["1", "x"]);
    }

    #[test]
    fn rewrites_to_question_placeholders() {
        let (q, v) = bind_named_params(
            "SELECT * FROM t WHERE a = :a",
            &params(&[("a", "1")]),
            PlaceholderStyle::Question,
        )
        .unwrap();
        assert_eq!(q, "SELECT * FROM t WHERE a = ?");
        assert_eq!(v, vec!["1"]);
    }

    #[test]
    fn repeated_name_binds_each_occurrence() {
        let (q, v) = bind_named_params(
            "SELECT :x AS a, :x AS b",
            &params(&[("x", "7")]),
            PlaceholderStyle::Dollar,
        )
        .unwrap();
        assert_eq!(q, "SELECT $1 AS a, $2 AS b");
        assert_eq!(v, vec!["7", "7"]);
    }

    #[test]
    fn postgres_cast_is_not_a_placeholder() {
        let (q, v) = bind_named_params(
            "SELECT '5'::int, x::text FROM t WHERE a = :a",
            &params(&[("a", "1")]),
            PlaceholderStyle::Dollar,
        )
        .unwrap();
        assert_eq!(q, "SELECT '5'::int, x::text FROM t WHERE a = $1");
        assert_eq!(v, vec!["1"]);
    }

    #[test]
    fn colons_inside_strings_comments_and_identifiers_are_untouched() {
        let src = "SELECT ':not_me', \":also_not\" -- :nope\n/* :nor_this */ FROM t WHERE a = :a";
        let (q, v) =
            bind_named_params(src, &params(&[("a", "1")]), PlaceholderStyle::Dollar).unwrap();
        assert!(q.contains("':not_me'"));
        assert!(q.contains("\":also_not\""));
        assert!(q.contains("-- :nope"));
        assert!(q.contains("/* :nor_this */"));
        assert!(q.ends_with("WHERE a = $1"));
        assert_eq!(v, vec!["1"]);
    }

    #[test]
    fn missing_param_is_an_error() {
        let err = bind_named_params(
            "SELECT * FROM t WHERE a = :ghost",
            &params(&[]),
            PlaceholderStyle::Dollar,
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().contains(":ghost"));
    }

    #[test]
    fn query_without_placeholders_passes_through() {
        let src = "SELECT * FROM t WHERE ts < now()";
        let (q, v) = bind_named_params(src, &params(&[]), PlaceholderStyle::Dollar).unwrap();
        assert_eq!(q, src);
        assert!(v.is_empty());
    }

    #[test]
    fn array_slice_syntax_is_untouched() {
        let (q, v) = bind_named_params(
            "SELECT arr[1:3] FROM t",
            &params(&[]),
            PlaceholderStyle::Dollar,
        )
        .unwrap();
        assert_eq!(q, "SELECT arr[1:3] FROM t");
        assert!(v.is_empty());
    }
}
