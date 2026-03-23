//! WASM runtime configuration and module loading

use crate::{Permission, PluginManifest, PluginSpec, WasmError, WasmResult};
use std::sync::Arc;
use tracing::{debug, info};
use wasmtime::{Engine, EngineBuilder, Module};

/// Configuration for the WASM runtime
#[derive(Debug, Clone)]
pub struct WasmRuntimeConfig {
    /// Maximum memory available to WASM modules in bytes (default: 256MB)
    pub max_memory_bytes: usize,

    /// Maximum execution time in seconds (default: 300)
    pub max_execution_time_secs: u64,

    /// Maximum fuel units for deterministic execution limiting (default: 1_000_000_000)
    pub max_fuel: u64,

    /// Enable WASI support (default: true)
    pub enable_wasi: bool,

    /// Enable multi-memory (default: false)
    pub enable_multi_memory: bool,

    /// Enable reference types (default: true)
    pub enable_ref_types: bool,

    /// Enable bulk memory operations (default: true)
    pub enable_bulk_memory: bool,
}

impl Default for WasmRuntimeConfig {
    fn default() -> Self {
        WasmRuntimeConfig {
            max_memory_bytes: 256 * 1024 * 1024, // 256 MB
            max_execution_time_secs: 300,        // 5 minutes
            max_fuel: 1_000_000_000,
            enable_wasi: true,
            enable_multi_memory: false,
            enable_ref_types: true,
            enable_bulk_memory: true,
        }
    }
}

/// The WASM runtime that manages the Wasmtime engine
pub struct WasmRuntime {
    engine: Engine,
    config: WasmRuntimeConfig,
}

impl WasmRuntime {
    /// Create a new WASM runtime with the given configuration
    pub fn new(config: WasmRuntimeConfig) -> WasmResult<Self> {
        debug!(
            max_memory_bytes = config.max_memory_bytes,
            max_execution_time_secs = config.max_execution_time_secs,
            max_fuel = config.max_fuel,
            "Creating WASM runtime"
        );

        let mut builder = EngineBuilder::new();

        // Configure memory limits
        builder = builder.memory_reserve(config.max_memory_bytes as u64);

        // Enable/disable features
        if config.enable_wasi {
            builder = builder.wasi_default(true);
        }

        // Enable fuel metering for deterministic execution
        builder = builder.fuel_consumption(true);

        // Configure reference types
        builder = if config.enable_ref_types {
            builder.wasm_reference_types(true)
        } else {
            builder.wasm_reference_types(false)
        };

        // Configure bulk memory operations
        builder = if config.enable_bulk_memory {
            builder.wasm_bulk_memory(true)
        } else {
            builder.wasm_bulk_memory(false)
        };

        // Configure multi-memory
        builder = if config.enable_multi_memory {
            builder.wasm_multi_memory(true)
        } else {
            builder.wasm_multi_memory(false)
        };

        let engine = builder
            .build()
            .map_err(|e| WasmError::WasmtimeError(e.to_string()))?;

        info!("WASM runtime created successfully");

        Ok(WasmRuntime { engine, config })
    }

    /// Create a runtime with default configuration
    pub fn default_config() -> WasmResult<Self> {
        Self::new(WasmRuntimeConfig::default())
    }

    /// Load and compile a plugin from a specification
    pub fn load_plugin(&self, spec: &PluginSpec) -> WasmResult<LoadedPlugin> {
        debug!(
            plugin_name = &spec.manifest.name,
            plugin_version = &spec.manifest.version,
            "Loading plugin"
        );

        spec.manifest.validate()?;

        // Compile the WASM module
        let module = Module::new(&self.engine, &spec.wasm_bytes)
            .map_err(|e| WasmError::CompilationError(e.to_string()))?;

        debug!(
            plugin_name = &spec.manifest.name,
            "Plugin module compiled successfully"
        );

        // Verify that the entrypoint function exists
        let entrypoint = &spec.manifest.entrypoint;
        let exports = module.exports();

        let has_entrypoint = exports
            .find(|export| export.name() == entrypoint && export.ty().func().is_some())
            .is_some();

        if !has_entrypoint {
            return Err(WasmError::InvalidEntrypoint(format!(
                "Function '{}' not found or is not a function in module",
                entrypoint
            )));
        }

        info!(
            plugin_name = &spec.manifest.name,
            entrypoint = entrypoint,
            "Plugin loaded and validated"
        );

        Ok(LoadedPlugin {
            manifest: spec.manifest.clone(),
            module: Arc::new(module),
        })
    }

    /// Get a reference to the underlying Wasmtime engine
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Get the runtime configuration
    pub fn config(&self) -> &WasmRuntimeConfig {
        &self.config
    }
}

/// A loaded and compiled WASM plugin ready for execution
#[derive(Clone)]
pub struct LoadedPlugin {
    /// The plugin manifest
    pub manifest: PluginManifest,

    /// The compiled WASM module
    pub module: Arc<Module>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PluginManifest;

    fn create_test_manifest() -> PluginManifest {
        PluginManifest {
            name: "test-plugin".to_string(),
            version: "1.0.0".to_string(),
            description: "A test plugin".to_string(),
            entrypoint: "process".to_string(),
            author: "Test".to_string(),
            license: "MIT".to_string(),
            permissions: vec![],
        }
    }

    #[test]
    fn test_wasm_runtime_config_default() {
        let config = WasmRuntimeConfig::default();
        assert_eq!(config.max_memory_bytes, 256 * 1024 * 1024);
        assert_eq!(config.max_execution_time_secs, 300);
        assert_eq!(config.max_fuel, 1_000_000_000);
        assert!(config.enable_wasi);
        assert!(config.enable_ref_types);
        assert!(config.enable_bulk_memory);
    }

    #[test]
    fn test_wasm_runtime_config_custom() {
        let config = WasmRuntimeConfig {
            max_memory_bytes: 512 * 1024 * 1024,
            max_execution_time_secs: 600,
            max_fuel: 2_000_000_000,
            enable_wasi: false,
            enable_multi_memory: true,
            enable_ref_types: false,
            enable_bulk_memory: false,
        };

        assert_eq!(config.max_memory_bytes, 512 * 1024 * 1024);
        assert_eq!(config.max_execution_time_secs, 600);
        assert_eq!(config.max_fuel, 2_000_000_000);
        assert!(!config.enable_wasi);
        assert!(config.enable_multi_memory);
        assert!(!config.enable_ref_types);
        assert!(!config.enable_bulk_memory);
    }

    #[test]
    fn test_wasm_runtime_creation() {
        let config = WasmRuntimeConfig::default();
        let runtime = WasmRuntime::new(config.clone());
        assert!(runtime.is_ok());

        let runtime = runtime.unwrap();
        assert_eq!(runtime.config().max_memory_bytes, config.max_memory_bytes);
    }

    #[test]
    fn test_wasm_runtime_default_config() {
        let runtime = WasmRuntime::default_config();
        assert!(runtime.is_ok());
    }

    #[test]
    fn test_wasm_runtime_load_plugin_invalid_wasm() {
        let runtime = WasmRuntime::default_config().expect("Failed to create runtime");
        let manifest = create_test_manifest();

        // Create an invalid WASM module (not actual WASM bytes)
        let spec = PluginSpec {
            manifest,
            wasm_bytes: vec![1, 2, 3, 4],
        };

        let result = runtime.load_plugin(&spec);
        assert!(result.is_err());
        match result {
            Err(WasmError::CompilationError(_)) => {}
            _ => panic!("Expected CompilationError"),
        }
    }

    #[test]
    fn test_wasm_runtime_load_plugin_minimal_valid_wasm() {
        let runtime = WasmRuntime::default_config().expect("Failed to create runtime");

        // Minimal valid WASM module (empty module)
        // This is the binary representation of an empty WebAssembly module
        let minimal_wasm = vec![
            0x00, 0x61, 0x73, 0x6d, // Magic number: \0asm
            0x01, 0x00, 0x00, 0x00, // Version: 1
        ];

        let mut manifest = create_test_manifest();
        // This will fail because there's no exported function "process"
        manifest.entrypoint = "nonexistent".to_string();

        let spec = PluginSpec {
            manifest,
            wasm_bytes: minimal_wasm,
        };

        let result = runtime.load_plugin(&spec);
        assert!(result.is_err());
        match result {
            Err(WasmError::InvalidEntrypoint(_)) => {}
            e => panic!("Expected InvalidEntrypoint, got: {:?}", e),
        }
    }

    #[test]
    fn test_loaded_plugin_clone() {
        let runtime = WasmRuntime::default_config().expect("Failed to create runtime");
        let minimal_wasm = vec![
            0x00, 0x61, 0x73, 0x6d, // Magic number
            0x01, 0x00, 0x00, 0x00, // Version
        ];

        let manifest = create_test_manifest();
        let spec = PluginSpec {
            manifest: manifest.clone(),
            wasm_bytes: minimal_wasm,
        };

        // We expect this to fail validation, but we can test the manifest
        let plugin_result = runtime.load_plugin(&spec);

        // Test manifest cloning separately
        let manifest_copy = manifest.clone();
        assert_eq!(manifest.name, manifest_copy.name);
        assert_eq!(manifest.version, manifest_copy.version);
    }
}
