//! Dynamic plugin system for loading external provider implementations.
//!
//! Conduit supports loading providers from external shared libraries (dylibs)
//! at runtime. This allows third-party developers to write custom providers
//! without forking the Conduit codebase.
//!
//! # Plugin Architecture
//!
//! ```text
//! ~/.conduit/plugins/
//! ├── conduit-plugin-snowpipe/
//! │   ├── manifest.yaml
//! │   └── libconduit_plugin_snowpipe.so
//! ├── conduit-plugin-delta-lake/
//! │   ├── manifest.yaml
//! │   └── libconduit_plugin_delta_lake.so
//! └── conduit-plugin-custom-api/
//!     ├── manifest.yaml
//!     └── libconduit_plugin_custom_api.so
//! ```
//!
//! # Plugin Manifest
//!
//! Each plugin directory contains a `manifest.yaml`:
//!
//! ```yaml
//! name: conduit-plugin-snowpipe
//! version: "1.0.0"
//! conduit_api_version: "0.1"
//! authors: ["Example Corp"]
//! description: "Snowflake Snowpipe integration for Conduit"
//! license: "Apache-2.0"
//! providers:
//!   - id: snowpipe
//!     display_name: "Snowflake Snowpipe"
//!     category: storage    # sql | storage | http | stream | saas | document
//!     aliases: ["snow_pipe"]
//! entry_point: libconduit_plugin_snowpipe
//! ```
//!
//! # Plugin SDK Contract
//!
//! A plugin shared library must export these C-ABI symbols:
//!
//! ```c
//! // Returns plugin metadata as JSON
//! const char* conduit_plugin_manifest();
//!
//! // Creates a provider instance; returns opaque pointer
//! void* conduit_plugin_create_provider(
//!     const char* provider_id,
//!     const char* name,
//!     const char* config_json
//! );
//!
//! // Plugin API version for compatibility checks
//! uint32_t conduit_plugin_api_version();
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::errors::ProviderError;

// ─── Plugin Manifest ────────────────────────────────────────────────────────

/// Plugin manifest parsed from `manifest.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    /// Plugin name (e.g., "conduit-plugin-snowpipe").
    pub name: String,

    /// Semantic version string.
    pub version: String,

    /// Conduit plugin API version this plugin targets.
    #[serde(default = "default_api_version")]
    pub conduit_api_version: String,

    /// Plugin authors.
    #[serde(default)]
    pub authors: Vec<String>,

    /// Human-readable description.
    #[serde(default)]
    pub description: String,

    /// License identifier.
    pub license: Option<String>,

    /// Provider types exposed by this plugin.
    #[serde(default)]
    pub providers: Vec<PluginProviderDef>,

    /// Shared library name (without lib prefix and platform extension).
    pub entry_point: String,

    /// Optional homepage URL.
    pub homepage: Option<String>,

    /// Optional repository URL.
    pub repository: Option<String>,
}

fn default_api_version() -> String {
    "0.1".to_string()
}

/// Definition of a provider type within a plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginProviderDef {
    /// Provider type ID (e.g., "snowpipe").
    pub id: String,

    /// Human-readable display name.
    pub display_name: String,

    /// Provider category.
    pub category: PluginCategory,

    /// Type aliases (e.g., ["snow_pipe"]).
    #[serde(default)]
    pub aliases: Vec<String>,
}

/// Provider category for plugin providers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PluginCategory {
    Sql,
    Storage,
    Http,
    Stream,
    Saas,
    Document,
}

impl std::fmt::Display for PluginCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginCategory::Sql => write!(f, "sql"),
            PluginCategory::Storage => write!(f, "storage"),
            PluginCategory::Http => write!(f, "http"),
            PluginCategory::Stream => write!(f, "stream"),
            PluginCategory::Saas => write!(f, "saas"),
            PluginCategory::Document => write!(f, "document"),
        }
    }
}

// ─── Loaded Plugin ──────────────────────────────────────────────────────────

/// A loaded plugin with its manifest and library handle.
#[derive(Debug)]
pub struct LoadedPlugin {
    /// Parsed manifest.
    pub manifest: PluginManifest,
    /// Path to the plugin directory.
    pub path: PathBuf,
    /// Status of the plugin.
    pub status: PluginStatus,
}

/// Current status of a plugin.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PluginStatus {
    /// Plugin discovered and manifest parsed.
    Discovered,
    /// Plugin loaded and ready to create providers.
    Loaded,
    /// Plugin failed to load.
    Error(String),
    /// Plugin disabled by user configuration.
    Disabled,
}

// ─── Plugin Manager ─────────────────────────────────────────────────────────

/// Manages discovery, loading, and lifecycle of provider plugins.
///
/// The plugin manager scans configured directories for plugin manifests,
/// validates them against the current API version, and makes their
/// provider types available to the registry.
pub struct PluginManager {
    /// Directories to scan for plugins.
    search_paths: Vec<PathBuf>,
    /// Discovered plugins indexed by name.
    plugins: HashMap<String, LoadedPlugin>,
    /// Mapping from provider type ID → plugin name.
    provider_index: HashMap<String, String>,
    /// Current API version for compatibility checking.
    api_version: String,
}

impl std::fmt::Debug for PluginManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginManager")
            .field("search_paths", &self.search_paths)
            .field("plugins", &self.plugins.keys().collect::<Vec<_>>())
            .field(
                "provider_types",
                &self.provider_index.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Default for PluginManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginManager {
    /// Current Conduit plugin API version.
    pub const API_VERSION: &'static str = "0.1";

    /// Create a new plugin manager with default search paths.
    ///
    /// Default paths:
    /// - `~/.conduit/plugins/`
    /// - `./plugins/`
    /// - `/etc/conduit/plugins/` (system-wide)
    pub fn new() -> Self {
        let mut search_paths = Vec::new();

        // User plugins directory
        if let Some(home) = std::env::var_os("HOME") {
            let user_plugins = PathBuf::from(home).join(".conduit").join("plugins");
            search_paths.push(user_plugins);
        }

        // Project-local plugins
        search_paths.push(PathBuf::from("plugins"));

        // System plugins
        search_paths.push(PathBuf::from("/etc/conduit/plugins"));

        Self {
            search_paths,
            plugins: HashMap::new(),
            provider_index: HashMap::new(),
            api_version: Self::API_VERSION.to_string(),
        }
    }

    /// Create a plugin manager with custom search paths.
    pub fn with_paths(paths: Vec<PathBuf>) -> Self {
        Self {
            search_paths: paths,
            plugins: HashMap::new(),
            provider_index: HashMap::new(),
            api_version: Self::API_VERSION.to_string(),
        }
    }

    /// Discover all plugins in the configured search paths.
    ///
    /// Scans each directory for subdirectories containing a `manifest.yaml`.
    /// Plugins are validated but not loaded until `load_all()` is called.
    pub fn discover(&mut self) -> usize {
        let mut discovered = 0;

        for search_path in &self.search_paths.clone() {
            if !search_path.exists() {
                debug!(path = %search_path.display(), "Plugin search path does not exist, skipping");
                continue;
            }

            let entries = match std::fs::read_dir(search_path) {
                Ok(entries) => entries,
                Err(e) => {
                    warn!(path = %search_path.display(), error = %e, "Failed to read plugin directory");
                    continue;
                }
            };

            for entry in entries.flatten() {
                let plugin_dir = entry.path();
                if !plugin_dir.is_dir() {
                    continue;
                }

                let manifest_path = plugin_dir.join("manifest.yaml");
                if !manifest_path.exists() {
                    // Also check manifest.yml
                    let alt_path = plugin_dir.join("manifest.yml");
                    if !alt_path.exists() {
                        continue;
                    }
                }

                match self.load_manifest(&plugin_dir) {
                    Ok(manifest) => {
                        let name = manifest.name.clone();

                        // API version compatibility check
                        if !self.is_compatible(&manifest.conduit_api_version) {
                            warn!(
                                plugin = %name,
                                plugin_api = %manifest.conduit_api_version,
                                conduit_api = %self.api_version,
                                "Plugin API version mismatch"
                            );
                            self.plugins.insert(
                                name.clone(),
                                LoadedPlugin {
                                    manifest,
                                    path: plugin_dir,
                                    status: PluginStatus::Error(format!(
                                        "API version mismatch: plugin={}, conduit={}",
                                        name, self.api_version
                                    )),
                                },
                            );
                            continue;
                        }

                        // Index provider types
                        for provider_def in &manifest.providers {
                            self.provider_index
                                .insert(provider_def.id.clone(), name.clone());
                            for alias in &provider_def.aliases {
                                self.provider_index.insert(alias.clone(), name.clone());
                            }
                        }

                        info!(
                            plugin = %name,
                            version = %manifest.version,
                            providers = manifest.providers.len(),
                            "Discovered plugin"
                        );

                        self.plugins.insert(
                            name,
                            LoadedPlugin {
                                manifest,
                                path: plugin_dir,
                                status: PluginStatus::Discovered,
                            },
                        );

                        discovered += 1;
                    }
                    Err(e) => {
                        warn!(
                            path = %plugin_dir.display(),
                            error = %e,
                            "Failed to parse plugin manifest"
                        );
                    }
                }
            }
        }

        info!(
            search_paths = self.search_paths.len(),
            discovered = discovered,
            total_provider_types = self.provider_index.len(),
            "Plugin discovery complete"
        );

        discovered
    }

    /// Load a plugin manifest from a directory.
    fn load_manifest(&self, plugin_dir: &Path) -> Result<PluginManifest, ProviderError> {
        let manifest_path = plugin_dir.join("manifest.yaml");
        let manifest_path = if manifest_path.exists() {
            manifest_path
        } else {
            plugin_dir.join("manifest.yml")
        };

        let content =
            std::fs::read_to_string(&manifest_path).map_err(|e| ProviderError::InvalidConfig {
                connection: plugin_dir.display().to_string(),
                reason: format!("Failed to read manifest: {}", e),
            })?;

        serde_yaml::from_str(&content).map_err(|e| ProviderError::InvalidConfig {
            connection: plugin_dir.display().to_string(),
            reason: format!("Invalid manifest YAML: {}", e),
        })
    }

    /// Check if a plugin API version is compatible with this Conduit version.
    fn is_compatible(&self, plugin_api_version: &str) -> bool {
        // Simple major version check for now
        let conduit_major = self.api_version.split('.').next().unwrap_or("0");
        let plugin_major = plugin_api_version.split('.').next().unwrap_or("0");
        conduit_major == plugin_major
    }

    /// Check if a provider type is provided by a plugin.
    pub fn has_provider(&self, provider_type: &str) -> bool {
        self.provider_index.contains_key(provider_type)
    }

    /// Get the plugin name that provides a given provider type.
    pub fn provider_plugin(&self, provider_type: &str) -> Option<&str> {
        self.provider_index.get(provider_type).map(|s| s.as_str())
    }

    /// Get all discovered plugins.
    pub fn plugins(&self) -> &HashMap<String, LoadedPlugin> {
        &self.plugins
    }

    /// Get a specific plugin by name.
    pub fn get_plugin(&self, name: &str) -> Option<&LoadedPlugin> {
        self.plugins.get(name)
    }

    /// Get all provider type IDs from all discovered plugins.
    pub fn plugin_provider_types(&self) -> Vec<(&str, &str, &PluginCategory)> {
        let mut types = Vec::new();
        for plugin in self.plugins.values() {
            if plugin.status == PluginStatus::Discovered || plugin.status == PluginStatus::Loaded {
                for def in &plugin.manifest.providers {
                    types.push((def.id.as_str(), def.display_name.as_str(), &def.category));
                }
            }
        }
        types
    }

    /// Number of discovered plugins.
    pub fn plugin_count(&self) -> usize {
        self.plugins.len()
    }

    /// Number of provider types available from plugins.
    pub fn provider_type_count(&self) -> usize {
        self.provider_index.len()
    }

    /// List search paths.
    pub fn search_paths(&self) -> &[PathBuf] {
        &self.search_paths
    }

    /// Generate a summary for the API.
    pub fn summary(&self) -> PluginManagerSummary {
        let plugins: Vec<PluginSummary> = self
            .plugins
            .values()
            .map(|p| PluginSummary {
                name: p.manifest.name.clone(),
                version: p.manifest.version.clone(),
                description: p.manifest.description.clone(),
                providers: p
                    .manifest
                    .providers
                    .iter()
                    .map(|pd| PluginProviderSummary {
                        id: pd.id.clone(),
                        display_name: pd.display_name.clone(),
                        category: pd.category.to_string(),
                        aliases: pd.aliases.clone(),
                    })
                    .collect(),
                status: p.status.clone(),
            })
            .collect();

        PluginManagerSummary {
            search_paths: self
                .search_paths
                .iter()
                .map(|p| p.display().to_string())
                .collect(),
            api_version: self.api_version.clone(),
            plugins,
            total_provider_types: self.provider_index.len(),
        }
    }
}

// ─── API Types ──────────────────────────────────────────────────────────────

/// Summary of the plugin manager state (for API responses).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginManagerSummary {
    pub search_paths: Vec<String>,
    pub api_version: String,
    pub plugins: Vec<PluginSummary>,
    pub total_provider_types: usize,
}

/// Summary of a single plugin (for API responses).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginSummary {
    pub name: String,
    pub version: String,
    pub description: String,
    pub providers: Vec<PluginProviderSummary>,
    pub status: PluginStatus,
}

/// Summary of a provider within a plugin (for API responses).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginProviderSummary {
    pub id: String,
    pub display_name: String,
    pub category: String,
    pub aliases: Vec<String>,
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_manager_creation() {
        let mgr = PluginManager::new();
        assert!(!mgr.search_paths().is_empty());
        assert_eq!(mgr.plugin_count(), 0);
        assert_eq!(mgr.provider_type_count(), 0);
    }

    #[test]
    fn test_custom_search_paths() {
        let mgr = PluginManager::with_paths(vec![PathBuf::from("/tmp/test-plugins")]);
        assert_eq!(mgr.search_paths().len(), 1);
    }

    #[test]
    fn test_api_compatibility() {
        let mgr = PluginManager::new();
        assert!(mgr.is_compatible("0.1"));
        assert!(mgr.is_compatible("0.2"));
        assert!(!mgr.is_compatible("1.0"));
    }

    #[test]
    fn test_manifest_parsing() {
        let yaml = r#"
name: conduit-plugin-test
version: "1.0.0"
conduit_api_version: "0.1"
description: "Test plugin"
providers:
  - id: test_db
    display_name: "Test Database"
    category: sql
    aliases: ["testdb"]
entry_point: libconduit_plugin_test
"#;

        let manifest: PluginManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.name, "conduit-plugin-test");
        assert_eq!(manifest.version, "1.0.0");
        assert_eq!(manifest.providers.len(), 1);
        assert_eq!(manifest.providers[0].id, "test_db");
        assert_eq!(manifest.providers[0].category, PluginCategory::Sql);
        assert_eq!(manifest.providers[0].aliases, vec!["testdb"]);
    }

    #[test]
    fn test_plugin_status_serialization() {
        let status = PluginStatus::Loaded;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"loaded\"");

        let error_status = PluginStatus::Error("test error".to_string());
        let json = serde_json::to_string(&error_status).unwrap();
        assert!(json.contains("test error"));
    }

    #[test]
    fn test_discover_empty_paths() {
        let mut mgr = PluginManager::with_paths(vec![PathBuf::from("/nonexistent/path")]);
        let count = mgr.discover();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_summary_generation() {
        let mgr = PluginManager::new();
        let summary = mgr.summary();
        assert!(!summary.search_paths.is_empty());
        assert_eq!(summary.api_version, "0.1");
        assert!(summary.plugins.is_empty());
    }

    #[test]
    fn test_discover_with_real_manifest_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let plugin_dir = dir.path().join("conduit-plugin-testdb");
        std::fs::create_dir_all(&plugin_dir).unwrap();

        let manifest = r#"
name: conduit-plugin-testdb
version: "2.1.0"
conduit_api_version: "0.1"
description: "A test database plugin"
license: "MIT"
providers:
  - id: testdb
    display_name: "Test Database"
    category: sql
    aliases: ["tdb", "test_database"]
  - id: teststore
    display_name: "Test Object Store"
    category: storage
    aliases: []
entry_point: libconduit_plugin_testdb
"#;
        std::fs::write(plugin_dir.join("manifest.yaml"), manifest).unwrap();

        let mut mgr = PluginManager::with_paths(vec![dir.path().to_path_buf()]);
        let count = mgr.discover();

        assert_eq!(count, 1);
        assert_eq!(mgr.plugin_count(), 1);
        // 2 provider IDs + 2 aliases = 4 entries in the index
        assert_eq!(mgr.provider_type_count(), 4);

        assert!(mgr.has_provider("testdb"));
        assert!(mgr.has_provider("tdb"));
        assert!(mgr.has_provider("test_database"));
        assert!(mgr.has_provider("teststore"));
        assert!(!mgr.has_provider("nonexistent"));

        assert_eq!(mgr.provider_plugin("testdb"), Some("conduit-plugin-testdb"));
        assert_eq!(mgr.provider_plugin("tdb"), Some("conduit-plugin-testdb"));

        let plugin = mgr.get_plugin("conduit-plugin-testdb").unwrap();
        assert_eq!(plugin.manifest.version, "2.1.0");
        assert_eq!(plugin.manifest.license, Some("MIT".to_string()));
        assert_eq!(plugin.status, PluginStatus::Discovered);
    }

    #[test]
    fn test_discover_yml_extension() {
        let dir = tempfile::tempdir().unwrap();
        let plugin_dir = dir.path().join("conduit-plugin-alt");
        std::fs::create_dir_all(&plugin_dir).unwrap();

        let manifest = r#"
name: conduit-plugin-alt
version: "1.0.0"
conduit_api_version: "0.1"
providers:
  - id: altdb
    display_name: "Alt Database"
    category: document
entry_point: libconduit_plugin_alt
"#;
        // Use .yml instead of .yaml
        std::fs::write(plugin_dir.join("manifest.yml"), manifest).unwrap();

        let mut mgr = PluginManager::with_paths(vec![dir.path().to_path_buf()]);
        let count = mgr.discover();

        assert_eq!(count, 1);
        assert!(mgr.has_provider("altdb"));
    }

    #[test]
    fn test_discover_skips_incompatible_api_version() {
        let dir = tempfile::tempdir().unwrap();
        let plugin_dir = dir.path().join("conduit-plugin-future");
        std::fs::create_dir_all(&plugin_dir).unwrap();

        let manifest = r#"
name: conduit-plugin-future
version: "1.0.0"
conduit_api_version: "2.0"
providers:
  - id: futuredb
    display_name: "Future Database"
    category: sql
entry_point: libconduit_plugin_future
"#;
        std::fs::write(plugin_dir.join("manifest.yaml"), manifest).unwrap();

        let mut mgr = PluginManager::with_paths(vec![dir.path().to_path_buf()]);
        let count = mgr.discover();

        // Discovery returns 0 (incompatible plugins are not counted as successful)
        assert_eq!(count, 0);
        // But the plugin is still stored with error status
        assert_eq!(mgr.plugin_count(), 1);
        let plugin = mgr.get_plugin("conduit-plugin-future").unwrap();
        assert!(matches!(plugin.status, PluginStatus::Error(_)));
        // Provider types from incompatible plugins are NOT indexed
        assert!(!mgr.has_provider("futuredb"));
    }

    #[test]
    fn test_discover_skips_directories_without_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let plugin_dir = dir.path().join("not-a-plugin");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        std::fs::write(plugin_dir.join("README.md"), "Not a plugin").unwrap();

        let mut mgr = PluginManager::with_paths(vec![dir.path().to_path_buf()]);
        let count = mgr.discover();

        assert_eq!(count, 0);
        assert_eq!(mgr.plugin_count(), 0);
    }

    #[test]
    fn test_discover_skips_malformed_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let plugin_dir = dir.path().join("conduit-plugin-broken");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        std::fs::write(plugin_dir.join("manifest.yaml"), "not: valid: yaml: [[[").unwrap();

        let mut mgr = PluginManager::with_paths(vec![dir.path().to_path_buf()]);
        let count = mgr.discover();

        assert_eq!(count, 0);
        assert_eq!(mgr.plugin_count(), 0);
    }

    #[test]
    fn test_discover_multiple_plugins() {
        let dir = tempfile::tempdir().unwrap();

        for (name, id) in [("plugin-a", "provider_a"), ("plugin-b", "provider_b")] {
            let plugin_dir = dir.path().join(name);
            std::fs::create_dir_all(&plugin_dir).unwrap();
            let manifest = format!(
                r#"
name: {name}
version: "1.0.0"
conduit_api_version: "0.1"
providers:
  - id: {id}
    display_name: "Provider"
    category: sql
entry_point: lib{name}
"#
            );
            std::fs::write(plugin_dir.join("manifest.yaml"), manifest).unwrap();
        }

        let mut mgr = PluginManager::with_paths(vec![dir.path().to_path_buf()]);
        let count = mgr.discover();

        assert_eq!(count, 2);
        assert_eq!(mgr.plugin_count(), 2);
        assert!(mgr.has_provider("provider_a"));
        assert!(mgr.has_provider("provider_b"));

        let types = mgr.plugin_provider_types();
        assert_eq!(types.len(), 2);
    }

    #[test]
    fn test_manifest_defaults() {
        let yaml = r#"
name: minimal-plugin
version: "0.1.0"
providers: []
entry_point: libminimal
"#;
        let manifest: PluginManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.conduit_api_version, "0.1"); // default
        assert!(manifest.authors.is_empty()); // default
        assert_eq!(manifest.description, ""); // default
        assert!(manifest.license.is_none());
        assert!(manifest.homepage.is_none());
        assert!(manifest.repository.is_none());
    }

    #[test]
    fn test_plugin_category_display() {
        assert_eq!(PluginCategory::Sql.to_string(), "sql");
        assert_eq!(PluginCategory::Storage.to_string(), "storage");
        assert_eq!(PluginCategory::Http.to_string(), "http");
        assert_eq!(PluginCategory::Stream.to_string(), "stream");
        assert_eq!(PluginCategory::Saas.to_string(), "saas");
        assert_eq!(PluginCategory::Document.to_string(), "document");
    }

    #[test]
    fn test_summary_with_discovered_plugins() {
        let dir = tempfile::tempdir().unwrap();
        let plugin_dir = dir.path().join("conduit-plugin-demo");
        std::fs::create_dir_all(&plugin_dir).unwrap();

        let manifest = r#"
name: conduit-plugin-demo
version: "3.0.0"
conduit_api_version: "0.1"
description: "Demo plugin for testing"
providers:
  - id: demo
    display_name: "Demo Provider"
    category: saas
    aliases: ["demo_v2"]
entry_point: libconduit_plugin_demo
"#;
        std::fs::write(plugin_dir.join("manifest.yaml"), manifest).unwrap();

        let mut mgr = PluginManager::with_paths(vec![dir.path().to_path_buf()]);
        mgr.discover();

        let summary = mgr.summary();
        assert_eq!(summary.plugins.len(), 1);
        assert_eq!(summary.total_provider_types, 2); // "demo" + "demo_v2"

        let plugin_summary = &summary.plugins[0];
        assert_eq!(plugin_summary.name, "conduit-plugin-demo");
        assert_eq!(plugin_summary.version, "3.0.0");
        assert_eq!(plugin_summary.description, "Demo plugin for testing");
        assert_eq!(plugin_summary.providers.len(), 1);
        assert_eq!(plugin_summary.providers[0].id, "demo");
        assert_eq!(plugin_summary.providers[0].category, "saas");
        assert_eq!(plugin_summary.providers[0].aliases, vec!["demo_v2"]);
    }
}
