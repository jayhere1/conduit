//! Plugin specification and manifest types

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Permissions that a plugin can request
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "value")]
pub enum Permission {
    /// Read access to a specific file or directory
    #[serde(rename = "file_read")]
    FileRead(PathBuf),

    /// Write access to a specific file or directory
    #[serde(rename = "file_write")]
    FileWrite(PathBuf),

    /// Network access to a specific host (CIDR notation or hostname)
    #[serde(rename = "network")]
    Network(String),

    /// Read access to an environment variable
    #[serde(rename = "env_var")]
    EnvVar(String),

    /// Access to stdout
    #[serde(rename = "stdout")]
    Stdout,

    /// Access to stderr
    #[serde(rename = "stderr")]
    Stderr,

    /// Access to system clock
    #[serde(rename = "clock")]
    Clock,
}

/// Plugin manifest describing metadata and permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    /// Plugin name (must be lowercase alphanumeric with hyphens)
    pub name: String,

    /// Semantic version
    pub version: String,

    /// Short description
    pub description: String,

    /// Name of the exported WASM function to call as entrypoint
    pub entrypoint: String,

    /// Author/organization
    pub author: String,

    /// License identifier (e.g., "MIT", "Apache-2.0")
    pub license: String,

    /// List of requested permissions
    #[serde(default)]
    pub permissions: Vec<Permission>,
}

impl PluginManifest {
    /// Validate the manifest for correctness
    pub fn validate(&self) -> crate::WasmResult<()> {
        if self.name.is_empty() {
            return Err(crate::WasmError::InvalidManifest(
                "Plugin name cannot be empty".to_string(),
            ));
        }

        if !self
            .name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(crate::WasmError::InvalidManifest(
                "Plugin name must be alphanumeric with hyphens/underscores".to_string(),
            ));
        }

        if self.version.is_empty() {
            return Err(crate::WasmError::InvalidManifest(
                "Plugin version cannot be empty".to_string(),
            ));
        }

        if self.entrypoint.is_empty() {
            return Err(crate::WasmError::InvalidManifest(
                "Entrypoint function name cannot be empty".to_string(),
            ));
        }

        Ok(())
    }
}

/// Complete plugin specification with compiled WASM bytes
pub struct PluginSpec {
    /// Plugin manifest
    pub manifest: PluginManifest,

    /// Compiled WebAssembly module bytes
    pub wasm_bytes: Vec<u8>,
}

impl PluginSpec {
    /// Create a new plugin specification
    pub fn new(manifest: PluginManifest, wasm_bytes: Vec<u8>) -> crate::WasmResult<Self> {
        manifest.validate()?;

        if wasm_bytes.is_empty() {
            return Err(crate::WasmError::InvalidManifest(
                "WASM bytes cannot be empty".to_string(),
            ));
        }

        Ok(PluginSpec {
            manifest,
            wasm_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_manifest() -> PluginManifest {
        PluginManifest {
            name: "test-plugin".to_string(),
            version: "1.0.0".to_string(),
            description: "A test plugin".to_string(),
            entrypoint: "process_task".to_string(),
            author: "Test Author".to_string(),
            license: "MIT".to_string(),
            permissions: vec![
                Permission::Stdout,
                Permission::Clock,
                Permission::FileRead("/data".into()),
            ],
        }
    }

    #[test]
    fn test_manifest_serialize_deserialize() {
        let manifest = create_test_manifest();
        let json = serde_json::to_string(&manifest).expect("Failed to serialize");
        let deserialized: PluginManifest =
            serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(manifest.name, deserialized.name);
        assert_eq!(manifest.version, deserialized.version);
        assert_eq!(manifest.entrypoint, deserialized.entrypoint);
        assert_eq!(manifest.permissions.len(), deserialized.permissions.len());
    }

    #[test]
    fn test_manifest_validate_valid() {
        let manifest = create_test_manifest();
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn test_manifest_validate_empty_name() {
        let mut manifest = create_test_manifest();
        manifest.name = String::new();
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn test_manifest_validate_invalid_name() {
        let mut manifest = create_test_manifest();
        manifest.name = "test@plugin".to_string();
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn test_manifest_validate_empty_entrypoint() {
        let mut manifest = create_test_manifest();
        manifest.entrypoint = String::new();
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn test_permission_variants() {
        let perms = vec![
            Permission::FileRead("/tmp".into()),
            Permission::FileWrite("/output".into()),
            Permission::Network("192.168.1.0/24".to_string()),
            Permission::EnvVar("API_KEY".to_string()),
            Permission::Stdout,
            Permission::Stderr,
            Permission::Clock,
        ];

        for perm in perms {
            let json = serde_json::to_string(&perm).expect("Failed to serialize");
            let deserialized: Permission =
                serde_json::from_str(&json).expect("Failed to deserialize");
            assert_eq!(perm, deserialized);
        }
    }

    #[test]
    fn test_plugin_spec_new_valid() {
        let manifest = create_test_manifest();
        let wasm_bytes = vec![0, 97, 115, 109]; // Valid WASM magic number
        let spec = PluginSpec::new(manifest, wasm_bytes);
        assert!(spec.is_ok());
    }

    #[test]
    fn test_plugin_spec_new_empty_wasm() {
        let manifest = create_test_manifest();
        let spec = PluginSpec::new(manifest, vec![]);
        assert!(spec.is_err());
    }

    #[test]
    fn test_plugin_spec_new_invalid_manifest() {
        let mut manifest = create_test_manifest();
        manifest.name = String::new();
        let wasm_bytes = vec![0, 97, 115, 109];
        let spec = PluginSpec::new(manifest, wasm_bytes);
        assert!(spec.is_err());
    }

    #[test]
    fn test_manifest_permissions_empty() {
        let manifest = PluginManifest {
            name: "minimal".to_string(),
            version: "0.1.0".to_string(),
            description: "Minimal plugin".to_string(),
            entrypoint: "run".to_string(),
            author: "Nobody".to_string(),
            license: "Unlicense".to_string(),
            permissions: vec![],
        };

        assert!(manifest.validate().is_ok());
        assert!(manifest.permissions.is_empty());
    }

    #[test]
    fn test_permission_json_roundtrip() {
        let manifest = create_test_manifest();
        let json = serde_json::to_string(&manifest).unwrap();

        // Verify the JSON structure
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(value["permissions"].is_array());

        // Deserialize back
        let deserialized: PluginManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.permissions.len(), 3);
    }
}
