//! Security sanitization helpers for provider modules.
//!
//! Provides SQL injection prevention and credential scrubbing for error messages.

use crate::errors::ProviderError;

/// Sanitize a user-provided SQL query to prevent injection attacks.
///
/// Rejects queries containing:
/// - Multiple statements (semicolons outside of string literals)
/// - Comment-based injection (`--`, `/* ... */`)
///
/// Also strips trailing semicolons.
pub fn sanitize_query(query: &str, connection_name: &str) -> Result<String, ProviderError> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Err(ProviderError::QueryFailed {
            connection: connection_name.to_string(),
            reason: "empty query".to_string(),
        });
    }

    // Strip trailing semicolons (harmless but unnecessary)
    let trimmed = trimmed.trim_end_matches(';').trim();

    if trimmed.is_empty() {
        return Err(ProviderError::QueryFailed {
            connection: connection_name.to_string(),
            reason: "empty query".to_string(),
        });
    }

    // Check for comment-based injection and multi-statement patterns
    if contains_injection_pattern(trimmed) {
        return Err(ProviderError::QueryFailed {
            connection: connection_name.to_string(),
            reason: "query rejected: potentially unsafe pattern detected (comments or multiple statements)".to_string(),
        });
    }

    Ok(trimmed.to_string())
}

/// Walk through the query character-by-character, tracking whether we are
/// inside a string literal. Outside of literals, flag:
///
/// - `;` (multiple statements)
/// - `--` (line comment)
/// - `/*` (block comment)
fn contains_injection_pattern(query: &str) -> bool {
    let chars: Vec<char> = query.chars().collect();
    let len = chars.len();
    let mut i = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while i < len {
        let c = chars[i];

        // Track single-quoted strings (SQL standard), handling escaped quotes ('')
        if c == '\'' && !in_double_quote {
            if in_single_quote {
                // Check for escaped quote ''
                if i + 1 < len && chars[i + 1] == '\'' {
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            } else {
                in_single_quote = true;
            }
            i += 1;
            continue;
        }

        // Track double-quoted identifiers
        if c == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
            i += 1;
            continue;
        }

        // Only check for injection patterns outside of string literals
        if !in_single_quote && !in_double_quote {
            // Semicolons = multiple statements
            if c == ';' {
                return true;
            }

            // Line comment: --
            if c == '-' && i + 1 < len && chars[i + 1] == '-' {
                return true;
            }

            // Block comment: /*
            if c == '/' && i + 1 < len && chars[i + 1] == '*' {
                return true;
            }
        }

        i += 1;
    }

    false
}

/// Sanitize an error message to remove credentials and connection strings.
///
/// Strips:
/// - Full connection URIs like `postgres://user:pass@host`
/// - `mongodb://`, `mysql://`, `redis://` URIs with embedded credentials
/// - `password=<value>` patterns (common in libpq / JDBC errors)
/// - `pwd=<value>` patterns (ODBC style)
#[allow(clippy::manual_strip)]
pub fn sanitize_error(error: &str) -> String {
    let mut result = error.to_string();

    // Strip full connection URIs: scheme://...whitespace
    for scheme in &[
        "postgresql://",
        "postgres://",
        "mysql://",
        "mongodb+srv://",
        "mongodb://",
        "redis://",
        "redshift://",
    ] {
        let mut offset = 0;
        loop {
            let lower = result[offset..].to_lowercase();
            if let Some(rel_start) = lower.find(scheme) {
                let start = offset + rel_start;
                // Find the end of the URI (next whitespace or end of string)
                let after = start + scheme.len();
                let end = result[after..]
                    .find(|c: char| c.is_whitespace())
                    .map(|pos| after + pos)
                    .unwrap_or(result.len());
                let replacement = "[CONNECTION_URI_REDACTED]";
                result.replace_range(start..end, replacement);
                offset = start + replacement.len();
            } else {
                break;
            }
        }
    }

    // Strip password=... and pwd=... patterns
    for prefix in &["password", "pwd"] {
        let mut offset = 0;
        loop {
            if offset >= result.len() {
                break;
            }
            let lower = result[offset..].to_lowercase();
            if let Some(rel_start) = lower.find(prefix) {
                let start = offset + rel_start;
                let after_key = start + prefix.len();
                let rest = &result[after_key..];

                // Skip optional whitespace around '='
                let rest_trimmed = rest.trim_start();
                let ws_len = rest.len() - rest_trimmed.len();
                if !rest_trimmed.starts_with('=') {
                    // Not a key=value pattern; skip past this match
                    offset = after_key;
                    continue;
                }
                let after_eq = after_key + ws_len + 1; // skip '='
                let val_start_str = &result[after_eq..];
                let val_trimmed = val_start_str.trim_start();
                let ws2_len = val_start_str.len() - val_trimmed.len();
                let val_abs_start = after_eq + ws2_len;

                // Determine the end of the value
                let val_end = if val_trimmed.starts_with('\'') {
                    // Single-quoted value
                    val_trimmed[1..]
                        .find('\'')
                        .map(|p| val_abs_start + 1 + p + 1)
                        .unwrap_or(result.len())
                } else if val_trimmed.starts_with('"') {
                    // Double-quoted value
                    val_trimmed[1..]
                        .find('"')
                        .map(|p| val_abs_start + 1 + p + 1)
                        .unwrap_or(result.len())
                } else {
                    // Unquoted: up to whitespace, semicolon, ampersand, or comma
                    val_trimmed
                        .find(|c: char| c.is_whitespace() || c == ';' || c == '&' || c == ',')
                        .map(|p| val_abs_start + p)
                        .unwrap_or(result.len())
                };

                let replacement = format!("{}=[REDACTED]", prefix);
                result.replace_range(start..val_end, &replacement);
                offset = start + replacement.len();
            } else {
                break;
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── sanitize_query tests ────────────────────────────────────────────

    #[test]
    fn test_simple_select_passes() {
        let result = sanitize_query("SELECT * FROM users", "test");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "SELECT * FROM users");
    }

    #[test]
    fn test_trailing_semicolon_stripped() {
        let result = sanitize_query("SELECT 1;", "test");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "SELECT 1");
    }

    #[test]
    fn test_multiple_statements_rejected() {
        let result = sanitize_query("SELECT 1; DROP TABLE users", "test");
        assert!(result.is_err());
    }

    #[test]
    fn test_line_comment_rejected() {
        let result = sanitize_query("SELECT * FROM users -- this is a comment", "test");
        assert!(result.is_err());
    }

    #[test]
    fn test_block_comment_rejected() {
        let result = sanitize_query("SELECT * FROM users /* comment */", "test");
        assert!(result.is_err());
    }

    #[test]
    fn test_semicolon_inside_string_ok() {
        let result = sanitize_query("SELECT * FROM users WHERE name = 'foo;bar'", "test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_double_dash_inside_string_ok() {
        let result = sanitize_query("SELECT * FROM users WHERE name = 'foo--bar'", "test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_block_comment_inside_string_ok() {
        let result = sanitize_query(
            "SELECT * FROM users WHERE note = '/* not a comment */'",
            "test",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_escaped_quote_in_string() {
        let result = sanitize_query("SELECT * FROM users WHERE name = 'it''s fine'", "test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_empty_query_rejected() {
        let result = sanitize_query("", "test");
        assert!(result.is_err());
    }

    #[test]
    fn test_whitespace_only_rejected() {
        let result = sanitize_query("   ", "test");
        assert!(result.is_err());
    }

    #[test]
    fn test_with_cte_passes() {
        let result = sanitize_query("WITH cte AS (SELECT 1 AS id) SELECT * FROM cte", "test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_passes() {
        let result = sanitize_query("INSERT INTO t (a) VALUES (1)", "test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_passes() {
        let result = sanitize_query("UPDATE t SET a = 1 WHERE id = 2", "test");
        assert!(result.is_ok());
    }

    // ── sanitize_error tests ────────────────────────────────────────────

    #[test]
    fn test_sanitize_postgres_uri() {
        let msg = "connection failed: postgresql://admin:s3cret@db.host:5432/mydb";
        let sanitized = sanitize_error(msg);
        assert!(!sanitized.contains("s3cret"), "sanitized = {}", sanitized);
        assert!(!sanitized.contains("admin"), "sanitized = {}", sanitized);
        assert!(sanitized.contains("[CONNECTION_URI_REDACTED]"));
    }

    #[test]
    fn test_sanitize_mysql_uri() {
        let msg = "connection failed: mysql://root:pass@localhost:3306/db";
        let sanitized = sanitize_error(msg);
        assert!(!sanitized.contains("pass"), "sanitized = {}", sanitized);
        assert!(sanitized.contains("[CONNECTION_URI_REDACTED]"));
    }

    #[test]
    fn test_sanitize_password_equals() {
        let msg = "error connecting password=hunter2 timeout=30";
        let sanitized = sanitize_error(msg);
        assert!(!sanitized.contains("hunter2"), "sanitized = {}", sanitized);
        assert!(sanitized.contains("password=[REDACTED]"));
    }

    #[test]
    fn test_sanitize_pwd_equals() {
        let msg = "ODBC error pwd=s3cret host=localhost";
        let sanitized = sanitize_error(msg);
        assert!(!sanitized.contains("s3cret"), "sanitized = {}", sanitized);
        assert!(sanitized.contains("pwd=[REDACTED]"));
    }

    #[test]
    fn test_sanitize_no_credentials_untouched() {
        let msg = "table 'users' not found";
        let sanitized = sanitize_error(msg);
        assert_eq!(sanitized, msg);
    }

    #[test]
    fn test_sanitize_mongodb_uri() {
        let msg = "connection failed: mongodb://user:pw@host:27017/db";
        let sanitized = sanitize_error(msg);
        assert!(!sanitized.contains(":pw@"), "sanitized = {}", sanitized);
        assert!(sanitized.contains("[CONNECTION_URI_REDACTED]"));
    }
}
