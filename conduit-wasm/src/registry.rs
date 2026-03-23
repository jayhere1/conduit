//! Plugin registry for managing loaded plugins

use crate::{LoadedPlugin, PluginManifest, PluginSpec, WasmError, WasmResult, WasmRuntime};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};
use tracing::{debug, info, warn};

/// Registry for managing loaded WASM plugins
pub struct PluginRegistry {
    plugins: HashMap<String, LoadedPlugin>,
    runtime: WasmRuntime,
}

impl PluginRegistry {
    /// Create a new empty plugin registry
    pub fn new(runtime: WasmRuntime) -> Self {
        PluginRegistry {
            plugins: HashMap::new(),
            runtime,
        }
    }

    /// Load all plugins from a directory
    /// Expected structure:
    /// - plugin_name/
    ///   - manifest.json (contains PluginManifest)
    ///   - plugin.wasm (the compiled WASM module)
    pub fn load_from_directory(&mut self, path: &Path) -> WasmResult<usize> {
        debug!(path = path.display().to_string(), "Scanning for plugins");

        if !path.is_dir() {
            return Err(WasmError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Plugin directory not found: {}", path.display()),
            )));
        }

        let mut loaded_count = 0;

        // Read all entries in the directory
        for entry in fs::read_dir(path).map_err(|e| {
            WasmError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to read plugin directory: {}", e),
            ))
        })? {
            let entry = entry.map_err(|e| {
                WasmError::IoError(std::io::Error::new(
                    e.kind(),
                    format!("Failed to read directory entry: {}", e),
                ))
            })?;

            let entry_path = entry.path();

            // Check for manifest.json in directory or at root
            if entry_path.is_dir() {
                let manifest_path = entry_path.join("manifest.json");
                if manifest_path.exists() {
                    match self.load_plugin_from_dir(&entry_path) {
                        Ok(plugin) => {
                            let plugin_name = plugin.manifest.name.clone();
                            self.plugins.insert(plugin_name.clone(), plugin);
                            loaded_count += 1;
                            info!(plugin = plugin_name, "Plugin loaded from directory");
                        }
                        Err(e) => {
                            warn!(
                                path = entry_path.display().to_string(),
                                error = e.to_string(),
                                "Failed to load plugin"
                            );
                        }
                    }
                }
            } else if entry_path.extension().map(|ext| ext == "json") == Some(true) {
                // Try loading from manifest.json at root
                match self.load_plugin(&entry_path) {
                    Ok(plugin) => {
                        let plugin_name = plugin.manifest.name.clone();
                        self.plugins.insert(plugin_name.clone(), plugin);
                        loaded_count += 1;
                        info!(plugin = plugin_name, "Plugin loaded");
                    }
                    Err(e) => {
                        warn!(
                            path = entry_path.display().to_string(),
                            error = e.to_string(),
                            "Failed to load plugin from manifest"
                        );
                    }
                }
            }
        }

        info!(
            path = path.display().to_string(),
            count = loaded_count,
            "Completed loading plugins"
        );

        Ok(loaded_count)
    }

    /// Load a plugin from a directory containing manifest.json and plugin.wasm
    fn load_plugin_from_dir(&self, dir_path: &Path) -> WasmResult<LoadedPlugin> {
        let manifest_path = dir_path.join("manifest.json");
        let wasm_path = dir_path.join("plugin.wasm");

        // Try alternative names
        let wasm_path = if !wasm_path.exists() {
            let alt_path = dir_path.with_extension("wasm");
            if alt_path.exists() {
                alt_path
            } else {
                return Err(WasmError::IoError(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "WASM module not found in directory: {}",
                        dir_path.display()
                    ),
                )));
            }
        } else {
            wasm_path
        };

        self.load_plugin(&manifest_path)
    }

    /// Load a single plugin from a manifest.json file
    /// The WASM module should be in the same directory named plugin.wasm
    pub fn load_plugin(&self, manifest_path: &Path) -> WasmResult<LoadedPlugin> {
        debug!(
            path = manifest_path.display().to_string(),
            "Loading plugin from manifest"
        );

        // Read and parse manifest
        let manifest_json =
            fs::read_to_string(manifest_path).map_err(|e| {
                WasmError::IoError(std::io::Error::new(
                    e.kind(),
                    format!("Failed to read manifest: {}", e),
                ))
            })?;

        let manifest: PluginManifest =
            serde_json::from_str(&manifest_json).map_err(|e| {
                WasmError::InvalidManifest(format!(
                    "Failed to parse manifest JSON: {}",
                    e
                ))
            })?;

        // Find the WASM module
        let manifest_dir = manifest_path
            .parent()
            .ok_or_else(|| {
                WasmError::IoError(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid manifest path",
                ))
            })?;

        let wasm_path = manifest_dir.join("plugin.wasm");

        if !wasm_path.exists() {
            return Err(WasmError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "WASM module not found: {}",
                    wasm_path.display()
                ),
            )));
        }

        // Read WASM bytes
        let wasm_bytes = fs::read(&wasm_path).map_err(|e| {
            WasmError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to read WASM module: {}", e),
            ))
        })?;

        // Create plugin spec
        let spec = PluginSpec::new(manifest, wasm_bytes)?;

        // Load through runtime
        self.runtime.load_plugin(&spec)
    }

    /// Get a loaded plugin by name
    pub fn get(&self, name: &str) -> Option<&LoadedPlugin> {
        self.plugins.get(name)
    }

    /// List all loaded plugins
    pub fn list(&self) -> Vec<&PluginManifest> {
        self.plugins.values().map(|p| &p.manifest).collect()
    }

    /// Get the number of loaded plugins
    pub fn count(&self) -> usize {
        self.plugins.len()
    }

    /// Check if a plugin is loaded
    pub fn contains(&self, name: &str) -> bool {
        self.plugins.contains_key(name)
    }

    /// Clear all loaded plugins
    pub fn clear(&mut self) {
        self.plugins.clear();
    }

    /// Get all plugin names
    pub fn plugin_names(&self) -> Vec<String> {
        self.plugins.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PluginManifest, WasmRuntimeConfig};

    fn create_test_registry() -> WasmResult<PluginRegistry> {
        let config = WasmRuntimeConfig::default();
        let runtime = WasmRuntime::new(config)?;
        Ok(PluginRegistry::new(runtime))
    }

    #[test]
    fn test_plugin_registry_new() -> WasmResult<()> {
        let registry = create_test_registry()?;
        assert_eq!(registry.count(), 0);
        Ok(())
    }

    #[test]
    fn test_plugin_registry_list_empty() -> WasmResult<()> {
        let registry = create_test_registry()?;
        let list = registry.list();
        assert_eq!(list.len(), 0);
        Ok(())
    }

    #[test]
    fn test_plugin_registry_contains_false() -> WasmResult<()> {
        let registry = create_test_registry()?;
        assert!(!registry.contains("nonexistent"));
        Ok(())
    }

    #[test]
    fn test_plugin_registry_plugin_names_empty() -> WasmResult<()> {
        let registry = create_test_registry()?;
        let names = registry.plugin_names();
        assert_eq!(names.len(), 0);
        Ok(())
    }

    #[test]
    fn test_plugin_registry_clear() -> WasmResult<()> {
        let mut registry = create_test_registry()?;
        registry.clear();
        assert_eq!(registry.count(), 0);
        Ok(())
    }

    #[test]
    fn test_plugin_registry_get_nonexistent() -> WasmResult<()> {
        let registry = create_test_registry()?;
        assert!(registry.get("nonexistent").is_none());
        Ok(())
    }

    #[test]
    fn test_plugin_registry_load_directory_not_found() -> WasmResult<()> {
        let mut registry = create_test_registry()?;
        let result = registry.load_from_directory(Path::new("/nonexistent/path"));
        assert!(result.is_err());
        match result {
            Err(WasmError::IoError(_)) => {}
            _ => panic!("Expected IoError"),
        }
        Ok(())
    }

    #[test]
    fn test_plugin_registry_load_directory_empty() -> WasmResult<()> {
        // Create a temporary empty directory
        let temp_dir = std::env::temp_dir().join("test_plugin_registry");
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir).ok();
        }
        fs::create_dir_all(&temp_dir)?;

        let mut registry = create_test_registry()?;
        let result = registry.load_from_directory(&temp_dir)?;
        assert_eq!(result, 0);

        // Cleanup
        fs::remove_dir_all(&temp_dir).ok();

        Ok(())
    }

    #[test]
    fn test_plugin_registry_load_manifest_not_found() -> WasmResult<()> {
        let mut registry = create_test_registry()?;
        let result = registry.load_plugin(Path::new("/nonexistent/manifest.json"));
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_plugin_registry_count() -> WasmResult<()> {
        let registry = create_test_registry()?;
        assert_eq!(registry.count(), 0);
        Ok(())
    }

    #[test]
    fn test_plugin_registry_get_none() -> WasmResult<()> {
        let registry = create_test_registry()?;
        let plugin = registry.get("test");
        assert!(plugin.is_none());
        Ok(())
    }

    #[test]
    fn test_plugin_registry_names() -> WasmResult<()> {
        let registry = create_test_registry()?;
        let names = registry.plugin_names();
        assert!(names.is_empty());
        Ok(())
    }
}
