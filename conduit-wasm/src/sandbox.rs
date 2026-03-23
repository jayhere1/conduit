//! Isolated execution sandbox with permission-based resource access

use crate::{
    host_functions::{register_host_functions, HostState},
    LoadedPlugin, Permission, WasmError, WasmResult, WasmRuntime,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tracing::{debug, info, warn};
use wasmtime::{Caller, Linker, Memory, Store};
use wasmtime_wasi::{ambient_authority, WasiCtxBuilder};

/// Status of task execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Task completed successfully
    #[serde(rename = "success")]
    Success,

    /// Task failed with an error
    #[serde(rename = "failed")]
    Failed,

    /// Task was killed/timed out
    #[serde(rename = "killed")]
    Killed,
}

/// Input to the sandboxed task execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInput {
    /// DAG identifier
    pub dag_id: String,

    /// Task identifier within the DAG
    pub task_id: String,

    /// Run identifier
    pub run_id: String,

    /// Task parameters as JSON
    pub params: Value,

    /// Optional incremental context from previous execution
    pub incremental_context: Option<Value>,
}

impl TaskInput {
    /// Convert to a Value for passing to host state
    pub fn to_value(&self) -> Value {
        json!({
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "run_id": self.run_id,
            "params": self.params,
            "context": self.incremental_context.as_ref().unwrap_or(&json!(null))
        })
    }
}

/// Output from sandboxed task execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskOutput {
    /// Whether the task succeeded or failed
    pub status: TaskStatus,

    /// Task output as JSON
    pub output: Value,

    /// Logs collected during execution
    pub logs: Vec<String>,

    /// Execution time in milliseconds
    pub duration_ms: u128,

    /// Fuel consumed (if available)
    pub fuel_consumed: Option<u64>,

    /// Optional watermark value (for tracking progress)
    pub watermark: Option<String>,

    /// XCom values emitted by the task
    pub xcom: HashMap<String, Value>,
}

/// Isolated sandbox for executing a WASM plugin
pub struct Sandbox {
    plugin: LoadedPlugin,
    runtime: WasmRuntime,
    permissions: Vec<Permission>,
}

impl Sandbox {
    /// Create a new sandbox for a loaded plugin
    pub fn new(plugin: &LoadedPlugin, runtime: &WasmRuntime) -> Self {
        let permissions = plugin.manifest.permissions.clone();
        debug!(
            plugin_name = &plugin.manifest.name,
            permission_count = permissions.len(),
            "Creating sandbox"
        );

        Sandbox {
            plugin: plugin.clone(),
            runtime: runtime.clone(),
            permissions,
        }
    }

    /// Check if a permission is granted
    fn has_permission(&self, required: &Permission) -> bool {
        self.permissions.iter().any(|p| match (p, required) {
            (Permission::Stdout, Permission::Stdout) => true,
            (Permission::Stderr, Permission::Stderr) => true,
            (Permission::Clock, Permission::Clock) => true,
            (Permission::FileRead(p1), Permission::FileRead(p2)) => {
                Self::path_matches(p1, p2)
            }
            (Permission::FileWrite(p1), Permission::FileWrite(p2)) => {
                Self::path_matches(p1, p2)
            }
            (Permission::Network(n1), Permission::Network(n2)) => n1 == n2,
            (Permission::EnvVar(v1), Permission::EnvVar(v2)) => v1 == v2,
            _ => false,
        })
    }

    /// Check if one path matches or contains another
    fn path_matches(granted: &Path, required: &Path) -> bool {
        if granted == required {
            return true;
        }

        // Check if required is under granted directory
        required
            .ancestors()
            .any(|ancestor| ancestor == granted)
    }

    /// Execute the task in the sandbox
    pub fn execute(&self, input: &TaskInput) -> WasmResult<TaskOutput> {
        debug!(
            plugin_name = &self.plugin.manifest.name,
            dag_id = &input.dag_id,
            task_id = &input.task_id,
            "Executing task in sandbox"
        );

        let start_time = Instant::now();

        // Create store with fuel metering
        let mut store = Store::new(
            self.runtime.engine(),
            HostState::new(input.to_value()),
        );

        // Set fuel limit
        let fuel_limit = self.runtime.config().max_fuel;
        store
            .set_fuel(fuel_limit)
            .map_err(|e| WasmError::WasmtimeError(e.to_string()))?;

        // Create linker and register host functions
        let mut linker = Linker::new(self.runtime.engine());
        register_host_functions(&mut linker)?;

        // If WASI is enabled, set it up
        if self.runtime.config().enable_wasi {
            self.setup_wasi(&mut linker, &mut store)?;
        }

        // Instantiate the module
        let instance = linker
            .instantiate(&mut store, &self.plugin.module)
            .map_err(|e| {
                warn!("Plugin instantiation failed: {}", e);
                WasmError::InstantiationError(e.to_string())
            })?;

        // Call the entrypoint function
        let entrypoint_fn = instance
            .get_typed_func::<(), ()>(&mut store, &self.plugin.manifest.entrypoint)
            .map_err(|e| {
                WasmError::InvalidEntrypoint(format!(
                    "Failed to get entrypoint: {}",
                    e
                ))
            })?;

        let execution_result = entrypoint_fn.call(&mut store, ());

        let duration = start_time.elapsed();
        let fuel_consumed = fuel_limit
            .saturating_sub(
                store
                    .get_fuel()
                    .unwrap_or(0)
            );

        // Collect the results from host state
        let host_state = store.data();
        let logs = host_state.logs.clone();
        let xcom = host_state.xcom.clone();
        let watermark = host_state.watermark.clone();

        match execution_result {
            Ok(_) => {
                info!(
                    plugin_name = &self.plugin.manifest.name,
                    duration_ms = duration.as_millis(),
                    fuel_consumed = fuel_consumed,
                    "Task execution succeeded"
                );

                Ok(TaskOutput {
                    status: TaskStatus::Success,
                    output: json!({"completed": true}),
                    logs,
                    duration_ms: duration.as_millis(),
                    fuel_consumed: Some(fuel_consumed),
                    watermark,
                    xcom,
                })
            }
            Err(e) => {
                let error_msg = e.to_string();

                // Check for specific error types
                let status = if error_msg.contains("fuel") {
                    warn!(
                        plugin_name = &self.plugin.manifest.name,
                        "Task killed due to fuel exhaustion"
                    );
                    TaskStatus::Killed
                } else if error_msg.contains("timeout") {
                    warn!(
                        plugin_name = &self.plugin.manifest.name,
                        "Task timed out"
                    );
                    TaskStatus::Killed
                } else {
                    warn!(
                        plugin_name = &self.plugin.manifest.name,
                        error = &error_msg,
                        "Task execution failed"
                    );
                    TaskStatus::Failed
                };

                Ok(TaskOutput {
                    status,
                    output: json!({"error": error_msg}),
                    logs,
                    duration_ms: duration.as_millis(),
                    fuel_consumed: Some(fuel_consumed),
                    watermark,
                    xcom,
                })
            }
        }
    }

    /// Set up WASI context with permissions
    fn setup_wasi(
        &self,
        linker: &mut Linker<HostState>,
        store: &mut Store<HostState>,
    ) -> WasmResult<()> {
        // Start with minimal WASI context
        let mut wasi_builder = WasiCtxBuilder::new()
            .inherit_stdio()
            .inherit_stderr()
            .inherit_stdout();

        // Add permitted directories
        for perm in &self.permissions {
            if let Permission::FileRead(path) = perm {
                if let Ok(abs_path) = std::fs::canonicalize(path) {
                    debug!(
                        path = abs_path.display().to_string(),
                        "Adding read-only directory to WASI"
                    );

                    // In a real implementation, we would add the directory with proper permissions
                    // For now, we document the intent
                }
            }
            if let Permission::FileWrite(path) = perm {
                if let Ok(abs_path) = std::fs::canonicalize(path) {
                    debug!(
                        path = abs_path.display().to_string(),
                        "Adding writable directory to WASI"
                    );
                }
            }
        }

        // Add permitted environment variables
        for perm in &self.permissions {
            if let Permission::EnvVar(var_name) = perm {
                if let Ok(value) = std::env::var(var_name) {
                    debug!(var = var_name, "Adding environment variable to WASI");
                    wasi_builder = wasi_builder.env(var_name, &value)
                        .map_err(|e| {
                            WasmError::WasiError(format!(
                                "Failed to set env var {}: {}",
                                var_name, e
                            ))
                        })?;
                }
            }
        }

        let wasi_ctx = wasi_builder
            .build()
            .map_err(|e| WasmError::WasiError(e.to_string()))?;

        wasmtime_wasi::add_to_linker(linker, |s| &mut s)
            .map_err(|e| WasmError::WasiError(e.to_string()))?;

        // This would normally set the context on the store
        // In practice, this requires creating the right WASI provider
        Ok(())
    }
}

impl Clone for Sandbox {
    fn clone(&self) -> Self {
        Sandbox {
            plugin: self.plugin.clone(),
            runtime: self.runtime.clone(),
            permissions: self.permissions.clone(),
        }
    }
}

impl Clone for WasmRuntime {
    fn clone(&self) -> Self {
        // Note: In real usage, Engine should be shared via Arc
        // For testing purposes, we create a new one
        WasmRuntime::new(self.config().clone()).expect("Failed to clone runtime")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PluginManifest;

    fn create_test_plugin() -> LoadedPlugin {
        let minimal_wasm = vec![
            0x00, 0x61, 0x73, 0x6d, // Magic number
            0x01, 0x00, 0x00, 0x00, // Version
        ];

        let manifest = PluginManifest {
            name: "test-plugin".to_string(),
            version: "1.0.0".to_string(),
            description: "Test plugin".to_string(),
            entrypoint: "run".to_string(),
            author: "Test".to_string(),
            license: "MIT".to_string(),
            permissions: vec![
                Permission::Stdout,
                Permission::Clock,
            ],
        };

        LoadedPlugin {
            manifest,
            module: std::sync::Arc::new(
                wasmtime::Module::new(&wasmtime::Engine::default(), &minimal_wasm)
                    .expect("Failed to create module"),
            ),
        }
    }

    #[test]
    fn test_task_input_creation() {
        let input = TaskInput {
            dag_id: "my_dag".to_string(),
            task_id: "task_1".to_string(),
            run_id: "2024-03-22".to_string(),
            params: json!({"x": 42}),
            incremental_context: None,
        };

        assert_eq!(input.dag_id, "my_dag");
        assert_eq!(input.task_id, "task_1");
        assert_eq!(input.run_id, "2024-03-22");
    }

    #[test]
    fn test_task_input_to_value() {
        let input = TaskInput {
            dag_id: "dag1".to_string(),
            task_id: "task1".to_string(),
            run_id: "run1".to_string(),
            params: json!({"key": "value"}),
            incremental_context: Some(json!({"prev": "state"})),
        };

        let value = input.to_value();
        assert_eq!(value.get("dag_id"), Some(&json!("dag1")));
        assert_eq!(value.get("task_id"), Some(&json!("task1")));
        assert_eq!(value.get("params").and_then(|p| p.get("key")), Some(&json!("value")));
    }

    #[test]
    fn test_task_output_success() {
        let output = TaskOutput {
            status: TaskStatus::Success,
            output: json!({"result": "ok"}),
            logs: vec!["Log 1".to_string(), "Log 2".to_string()],
            duration_ms: 1234,
            fuel_consumed: Some(500000),
            watermark: None,
            xcom: HashMap::new(),
        };

        assert_eq!(output.logs.len(), 2);
        assert_eq!(output.duration_ms, 1234);
    }

    #[test]
    fn test_task_output_failed() {
        let output = TaskOutput {
            status: TaskStatus::Failed,
            output: json!({"error": "Something went wrong"}),
            logs: vec!["Error occurred".to_string()],
            duration_ms: 100,
            fuel_consumed: Some(10000),
            watermark: None,
            xcom: HashMap::new(),
        };

        match output.status {
            TaskStatus::Failed => {}
            _ => panic!("Expected Failed status"),
        }
    }

    #[test]
    fn test_task_output_serialization() {
        let mut xcom = HashMap::new();
        xcom.insert("key1".to_string(), json!(42));

        let output = TaskOutput {
            status: TaskStatus::Success,
            output: json!({}),
            logs: vec![],
            duration_ms: 100,
            fuel_consumed: Some(1000),
            watermark: Some("2024-03-22".to_string()),
            xcom,
        };

        let json_str = serde_json::to_string(&output).expect("Failed to serialize");
        let _deserialized: TaskOutput =
            serde_json::from_str(&json_str).expect("Failed to deserialize");

        assert!(json_str.contains("success"));
    }

    #[test]
    fn test_permission_file_read_match() {
        let perm1 = Permission::FileRead("/data".into());
        let perm2 = Permission::FileRead("/data".into());

        assert!(Sandbox::path_matches(&PathBuf::from("/data"), &PathBuf::from("/data")));
    }

    #[test]
    fn test_permission_file_read_not_match() {
        let perm1 = Permission::FileRead("/data".into());
        let perm2 = Permission::FileRead("/other".into());

        assert!(!Sandbox::path_matches(
            &PathBuf::from("/data"),
            &PathBuf::from("/other")
        ));
    }

    #[test]
    fn test_task_status_serialization() {
        let success = TaskStatus::Success;
        let json = serde_json::to_string(&success).unwrap();
        assert!(json.contains("success"));

        let failed = TaskStatus::Failed;
        let json = serde_json::to_string(&failed).unwrap();
        assert!(json.contains("failed"));

        let killed = TaskStatus::Killed;
        let json = serde_json::to_string(&killed).unwrap();
        assert!(json.contains("killed"));
    }

    #[test]
    fn test_task_input_with_complex_params() {
        let params = json!({
            "config": {
                "threads": 4,
                "timeout": 30
            },
            "items": [1, 2, 3]
        });

        let input = TaskInput {
            dag_id: "dag".to_string(),
            task_id: "task".to_string(),
            run_id: "run".to_string(),
            params,
            incremental_context: None,
        };

        let value = input.to_value();
        assert!(value.get("params").is_some());
    }
}
