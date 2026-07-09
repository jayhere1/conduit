//! Generates and pins the provider reference chapter (PRD B6 / §8.5).
//!
//! The `docs/src/reference/providers.md` chapter is generated FROM CODE so
//! the docs can never drift from reality: the table's real/experimental
//! status comes from each provider's `ProviderInfo.is_stub`, the same flag
//! `test_connection`/`execute` honor.
//!
//! To regenerate after adding or changing a provider:
//!   UPDATE_PROVIDER_DOCS=1 cargo test -p conduit-providers --test provider_docs_test
//!
//! CI runs it without the env var, so an out-of-date chapter fails the test.

use std::path::PathBuf;

use conduit_providers::registry::{supported_provider_types, ProviderRegistry};

fn category_label(cat: &str) -> &'static str {
    match cat {
        "sql" => "SQL Databases",
        "storage" => "Object Storage",
        "http" => "HTTP / Webhooks",
        "stream" => "Streaming",
        "saas" => "SaaS Platforms",
        "document" => "Document / NoSQL",
        _ => "Other",
    }
}

/// Deterministic category order for the rendered chapter.
const CATEGORY_ORDER: &[&str] = &["sql", "storage", "http", "stream", "saas", "document"];

fn render_markdown() -> String {
    let providers = supported_provider_types();
    let total = providers.len();
    let real = providers
        .iter()
        .filter(|(id, _, _, _)| !ProviderRegistry::provider_is_stub(id))
        .count();
    let experimental = total - real;

    let mut out = String::new();
    out.push_str("# Providers & Connections\n\n");
    out.push_str(
        "Conduit ships with a typed provider system: every connector implements a \
         category trait (`SqlProvider`, `StorageProvider`, `HttpProvider`, \
         `StreamProvider`, `SaasProvider`, `DocumentProvider`) on top of the base \
         `Provider` trait. Configure connections in `conduit.yaml` and validate them \
         with `conduit test-connection <name>`.\n\n",
    );
    out.push_str(&format!(
        "**{total} provider types: {real} production, {experimental} experimental.** \
         Experimental providers expose the trait interface but their operations return \
         `NotImplemented`; `conduit compile` warns when a DAG routes through one. This \
         table is generated from each provider's `ProviderInfo.is_stub` flag — it cannot \
         drift from the code.\n\n",
    ));
    out.push_str(
        "| Status legend | |\n|---|---|\n\
         | ✅ Production | Real implementation with a live `test_connection` |\n\
         | 🧪 Experimental | Trait interface only; operations return `NotImplemented` |\n\n",
    );

    for &cat in CATEGORY_ORDER {
        let mut rows: Vec<&(&str, &str, &[&str], &str)> =
            providers.iter().filter(|(_, _, _, c)| *c == cat).collect();
        if rows.is_empty() {
            continue;
        }
        rows.sort_by_key(|(id, _, _, _)| *id);

        out.push_str(&format!("## {}\n\n", category_label(cat)));
        out.push_str("| Status | Provider | Type ID | Aliases |\n");
        out.push_str("|---|---|---|---|\n");
        for (id, name, aliases, _) in rows {
            let status = if ProviderRegistry::provider_is_stub(id) {
                "🧪"
            } else {
                "✅"
            };
            let alias_str = if aliases.is_empty() {
                "—".to_string()
            } else {
                aliases
                    .iter()
                    .map(|a| format!("`{a}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            out.push_str(&format!("| {status} | {name} | `{id}` | {alias_str} |\n"));
        }
        out.push('\n');
    }

    out.push_str(
        "## Configuration\n\n\
         Connections live under `connections:` in `conduit.yaml`. Secrets can be \
         injected from environment variables or a secrets backend rather than \
         committed inline. Example:\n\n\
         ```yaml\n\
         connections:\n\
         \x20 warehouse:\n\
         \x20   type: postgres\n\
         \x20   host: db.internal\n\
         \x20   port: 5432\n\
         \x20   database: analytics\n\
         \x20   # credentials resolved from CONDUIT_CONN_WAREHOUSE or a secrets backend\n\
         ```\n\n\
         Validate connectivity before running pipelines:\n\n\
         ```bash\n\
         conduit test-connection warehouse\n\
         ```\n",
    );

    out
}

fn doc_path() -> PathBuf {
    // Tests run with the crate dir as CWD; the book lives at repo-root/docs.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("docs/src/reference/providers.md")
}

#[test]
fn provider_reference_chapter_is_in_sync() {
    let rendered = render_markdown();
    let path = doc_path();

    if std::env::var("UPDATE_PROVIDER_DOCS").is_ok() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &rendered).unwrap();
        eprintln!("wrote {}", path.display());
        return;
    }

    let committed = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "provider reference chapter missing at {}. Generate it with \
             UPDATE_PROVIDER_DOCS=1 cargo test -p conduit-providers --test provider_docs_test",
            path.display()
        )
    });

    assert_eq!(
        committed, rendered,
        "docs/src/reference/providers.md is out of date. Regenerate with \
         UPDATE_PROVIDER_DOCS=1 cargo test -p conduit-providers --test provider_docs_test"
    );
}
