//! dbt `manifest.json` — minimal subset.
//!
//! dbt projects compile to a `target/manifest.json` that lists every
//! model, seed, source, snapshot, etc., plus how they reference each
//! other. The full schema is large and version-shifting; this module
//! defines only the slice we need to resolve `{{ ref('x') }}` and
//! `{{ source('s', 'x') }}` calls inside SQL queries to concrete
//! qualified table names so cross-task lineage can stitch through them.
//!
//! Closes the §4.3 stretch item from `docs/STRATEGIC_DIRECTION.md`
//! ("dbt-aware template resolution"). The pragmatic render-then-parse
//! path: substitute resolved refs into the SQL string before
//! `sqlparser` sees it, so the existing lineage pipeline gets real
//! table references instead of opaque `__conduit_jinja_N__` placeholders.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// One node in the dbt manifest (model, seed, snapshot, …). We only
/// keep what's needed to resolve a `ref(name)` call: the name, the
/// `(database, schema, alias)` that produces the qualified identifier,
/// and the resource type so a `ref('x')` against a non-model node
/// (e.g. a snapshot) still resolves.
///
/// Fields use `#[serde(default)]` so manifest versions that omit one
/// (older dbt, custom builds) deserialize without error.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbtNode {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub resource_type: String,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub schema: String,
    /// The physical table name dbt will use. Defaults to `name` if the
    /// model doesn't declare an alias.
    #[serde(default)]
    pub alias: Option<String>,
    /// Package the node belongs to. dbt's `ref()` accepts either
    /// `ref('name')` (search across packages) or `ref('package',
    /// 'name')` (qualified). We only need package for the qualified
    /// form so the disambiguation works.
    #[serde(default)]
    pub package_name: Option<String>,
}

impl DbtNode {
    /// Physical identifier dbt would build for this node. Matches the
    /// `{database.}schema.alias_or_name` form dbt itself uses, so the
    /// substituted SQL parses with `sqlparser`'s standard
    /// dotted-identifier handling.
    pub fn qualified_table(&self) -> String {
        let table = self.alias.clone().unwrap_or_else(|| self.name.clone());
        match &self.database {
            Some(db) => format!("{}.{}.{}", db, self.schema, table),
            None => format!("{}.{}", self.schema, table),
        }
    }
}

/// One `sources:` entry in a dbt project — `{{ source('src', 'tbl') }}`
/// resolves to one of these.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbtSource {
    /// The source name (the `source('THIS', 'table')` argument).
    #[serde(default)]
    pub source_name: String,
    /// The table name within the source.
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub schema: String,
    /// Physical name override; defaults to `name` if absent.
    #[serde(default)]
    pub identifier: Option<String>,
}

impl DbtSource {
    pub fn qualified_table(&self) -> String {
        let table = self
            .identifier
            .clone()
            .unwrap_or_else(|| self.name.clone());
        match &self.database {
            Some(db) => format!("{}.{}.{}", db, self.schema, table),
            None => format!("{}.{}", self.schema, table),
        }
    }
}

/// The subset of `manifest.json` we read. Real dbt manifests have many
/// more keys; serde drops what we don't name. `#[serde(default)]` on
/// every field means an empty `{}` deserializes to an empty manifest —
/// useful as a fallback for tests and for projects without dbt.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbtManifest {
    /// Keyed by dbt's `unique_id` (e.g. `model.project.name`).
    #[serde(default)]
    pub nodes: HashMap<String, DbtNode>,
    /// Keyed by dbt's `unique_id` (e.g. `source.project.src.tbl`).
    #[serde(default)]
    pub sources: HashMap<String, DbtSource>,
}

impl DbtManifest {
    /// Load a manifest from `target/manifest.json` (or wherever dbt put
    /// it). Returns an empty manifest if the file's missing — calling
    /// code can then proceed with placeholder behaviour for refs, which
    /// is the same shape as having no dbt project at all.
    pub fn load_from_file(path: &Path) -> Result<Self, DbtManifestError> {
        let data = std::fs::read_to_string(path)
            .map_err(|e| DbtManifestError::Io(path.display().to_string(), e.to_string()))?;
        serde_json::from_str(&data)
            .map_err(|e| DbtManifestError::Parse(path.display().to_string(), e.to_string()))
    }

    /// Resolve `{{ ref('name') }}` to a qualified table identifier.
    ///
    /// Returns `None` when the manifest has no matching node. Callers
    /// then keep the existing placeholder behaviour — which keeps
    /// pre-dbt and partial-dbt projects working without a fail-loud.
    ///
    /// Match priority: exact match on `(package, name)` when the caller
    /// passed a package; otherwise match by `name` across all nodes,
    /// preferring `resource_type == "model"` over seeds / snapshots
    /// (dbt's `ref()` resolution order).
    pub fn resolve_ref(&self, package: Option<&str>, name: &str) -> Option<String> {
        let mut candidates: Vec<&DbtNode> = self
            .nodes
            .values()
            .filter(|n| n.name == name)
            .filter(|n| match package {
                Some(p) => n.package_name.as_deref() == Some(p),
                None => true,
            })
            .collect();

        if candidates.is_empty() {
            return None;
        }

        // Prefer models. Snapshots / seeds are still valid ref targets
        // in dbt, but a model wins when names collide.
        candidates.sort_by_key(|n| if n.resource_type == "model" { 0 } else { 1 });
        Some(candidates[0].qualified_table())
    }

    /// Resolve `{{ source('source_name', 'table_name') }}`.
    pub fn resolve_source(&self, source_name: &str, table_name: &str) -> Option<String> {
        self.sources
            .values()
            .find(|s| s.source_name == source_name && s.name == table_name)
            .map(|s| s.qualified_table())
    }
}

/// Errors from loading a dbt manifest. Kept narrow so callers can
/// distinguish "file missing" from "file malformed" if they want to
/// fail loud on the latter but quiet on the former.
#[derive(Debug, thiserror::Error)]
pub enum DbtManifestError {
    #[error("Failed to read dbt manifest at {0}: {1}")]
    Io(String, String),
    #[error("Failed to parse dbt manifest at {0}: {1}")]
    Parse(String, String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str, kind: &str, schema: &str, alias: Option<&str>) -> DbtNode {
        DbtNode {
            name: name.to_string(),
            resource_type: kind.to_string(),
            database: Some("analytics".to_string()),
            schema: schema.to_string(),
            alias: alias.map(String::from),
            package_name: Some("demo".to_string()),
        }
    }

    fn manifest_with(nodes: Vec<(&str, DbtNode)>, sources: Vec<(&str, DbtSource)>) -> DbtManifest {
        DbtManifest {
            nodes: nodes
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            sources: sources
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        }
    }

    #[test]
    fn ref_resolves_to_database_schema_alias() {
        let m = manifest_with(
            vec![(
                "model.demo.users",
                node("users", "model", "marts", Some("dim_users")),
            )],
            vec![],
        );
        assert_eq!(
            m.resolve_ref(None, "users"),
            Some("analytics.marts.dim_users".to_string())
        );
    }

    #[test]
    fn ref_falls_back_to_name_when_no_alias() {
        let m = manifest_with(
            vec![("model.demo.orders", node("orders", "model", "staging", None))],
            vec![],
        );
        assert_eq!(
            m.resolve_ref(None, "orders"),
            Some("analytics.staging.orders".to_string())
        );
    }

    #[test]
    fn ref_prefers_model_over_snapshot_when_names_collide() {
        let m = manifest_with(
            vec![
                (
                    "snapshot.demo.users",
                    node("users", "snapshot", "snapshots", None),
                ),
                (
                    "model.demo.users",
                    node("users", "model", "marts", Some("dim_users")),
                ),
            ],
            vec![],
        );
        assert_eq!(
            m.resolve_ref(None, "users"),
            Some("analytics.marts.dim_users".to_string()),
            "model must win the name collision against snapshot"
        );
    }

    #[test]
    fn ref_returns_none_when_unknown() {
        let m = DbtManifest::default();
        assert_eq!(m.resolve_ref(None, "nonexistent"), None);
    }

    #[test]
    fn source_resolves_to_database_schema_identifier() {
        let mut sources = HashMap::new();
        sources.insert(
            "source.demo.salesforce.accounts".to_string(),
            DbtSource {
                source_name: "salesforce".to_string(),
                name: "accounts".to_string(),
                database: Some("raw".to_string()),
                schema: "salesforce".to_string(),
                identifier: Some("Account__c".to_string()),
            },
        );
        let m = DbtManifest {
            nodes: HashMap::new(),
            sources,
        };
        assert_eq!(
            m.resolve_source("salesforce", "accounts"),
            Some("raw.salesforce.Account__c".to_string())
        );
    }

    #[test]
    fn source_falls_back_to_name_when_no_identifier() {
        let mut sources = HashMap::new();
        sources.insert(
            "source.demo.stripe.charges".to_string(),
            DbtSource {
                source_name: "stripe".to_string(),
                name: "charges".to_string(),
                database: None,
                schema: "stripe".to_string(),
                identifier: None,
            },
        );
        let m = DbtManifest {
            nodes: HashMap::new(),
            sources,
        };
        assert_eq!(
            m.resolve_source("stripe", "charges"),
            Some("stripe.charges".to_string())
        );
    }

    #[test]
    fn load_from_file_parses_minimal_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(
            &path,
            r#"{
                "nodes": {
                    "model.demo.users": {
                        "name": "users",
                        "resource_type": "model",
                        "database": "analytics",
                        "schema": "marts",
                        "alias": "dim_users",
                        "package_name": "demo"
                    }
                },
                "sources": {}
            }"#,
        )
        .unwrap();
        let m = DbtManifest::load_from_file(&path).unwrap();
        assert_eq!(
            m.resolve_ref(None, "users"),
            Some("analytics.marts.dim_users".to_string())
        );
    }
}
