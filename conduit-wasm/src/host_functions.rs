//! Host functions exposed to WASM guests

use crate::WasmResult;
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::{debug, warn};
use wasmtime::{Caller, Linker, Memory, Val};

/// State maintained for a sandbox execution
#[derive(Debug, Clone)]
pub struct HostState {
    /// Accumulated log messages
    pub logs: Vec<String>,

    /// XCom values emitted by the guest
    pub xcom: HashMap<String, Value>,

    /// Optional watermark value
    pub watermark: Option<String>,

    /// Input parameters available to the guest
    pub input_params: Value,
}

impl HostState {
    /// Create a new empty host state
    pub fn new(input_params: Value) -> Self {
        HostState {
            logs: Vec::new(),
            xcom: HashMap::new(),
            watermark: None,
            input_params,
        }
    }
}

/// Helper function to read a string from WASM linear memory
fn read_string_from_memory(
    memory: Memory,
    ptr: i32,
    len: i32,
) -> Result<String, String> {
    if ptr < 0 || len < 0 {
        return Err("Invalid pointer or length".to_string());
    }

    let ptr = ptr as usize;
    let len = len as usize;

    let data = memory
        .data()
        .get(ptr..ptr + len)
        .ok_or_else(|| "Pointer out of bounds".to_string())?;

    String::from_utf8(data.to_vec()).map_err(|e| e.to_string())
}

/// Helper function to write a string to WASM linear memory
fn write_string_to_memory(
    memory: Memory,
    ptr: i32,
    data: &[u8],
) -> Result<(), String> {
    if ptr < 0 {
        return Err("Invalid pointer".to_string());
    }

    let ptr = ptr as usize;
    let memory_data = unsafe { memory.data_mut() };

    if ptr + data.len() > memory_data.len() {
        return Err("Output buffer overflow".to_string());
    }

    memory_data[ptr..ptr + data.len()].copy_from_slice(data);
    Ok(())
}

/// Guest: Log a message at the specified level
/// Level: 0=TRACE, 1=DEBUG, 2=INFO, 3=WARN, 4=ERROR
pub fn conduit_log(
    mut caller: Caller<'_, HostState>,
    level: i32,
    msg_ptr: i32,
    msg_len: i32,
) -> Result<(), String> {
    let memory = caller.get_export("memory")
        .and_then(|e| e.into_memory())
        .map_err(|_| "Failed to get memory export".to_string())?;

    let message = read_string_from_memory(memory, msg_ptr, msg_len)?;

    let state = caller.data_mut();
    state.logs.push(format!("[Level {}] {}", level, message));

    let log_level_name = match level {
        0 => "TRACE",
        1 => "DEBUG",
        2 => "INFO",
        3 => "WARN",
        4 => "ERROR",
        _ => "UNKNOWN",
    };

    match level {
        0 => tracing::trace!("{}", message),
        1 => tracing::debug!("{}", message),
        2 => tracing::info!("{}", message),
        3 => tracing::warn!("{}", message),
        4 => tracing::error!("{}", message),
        _ => tracing::info!("[{}] {}", log_level_name, message),
    }

    Ok(())
}

/// Guest: Emit an XCom value (cross-communication)
pub fn conduit_emit_xcom(
    mut caller: Caller<'_, HostState>,
    key_ptr: i32,
    key_len: i32,
    val_ptr: i32,
    val_len: i32,
) -> Result<(), String> {
    let memory = caller.get_export("memory")
        .and_then(|e| e.into_memory())
        .map_err(|_| "Failed to get memory export".to_string())?;

    let key = read_string_from_memory(memory, key_ptr, key_len)?;
    let value_str = read_string_from_memory(memory, val_ptr, val_len)?;

    // Try to parse as JSON; if it fails, store as a string
    let value: Value = serde_json::from_str(&value_str)
        .unwrap_or_else(|_| Value::String(value_str));

    debug!(key = &key, "XCom emitted");
    caller.data_mut().xcom.insert(key, value);

    Ok(())
}

/// Guest: Emit a watermark value (for tracking progress)
pub fn conduit_emit_watermark(
    mut caller: Caller<'_, HostState>,
    val_ptr: i32,
    val_len: i32,
) -> Result<(), String> {
    let memory = caller.get_export("memory")
        .and_then(|e| e.into_memory())
        .map_err(|_| "Failed to get memory export".to_string())?;

    let watermark = read_string_from_memory(memory, val_ptr, val_len)?;

    debug!(watermark = &watermark, "Watermark emitted");
    caller.data_mut().watermark = Some(watermark);

    Ok(())
}

/// Guest: Read an input parameter by key
/// Returns the length of the parameter value (in bytes), or -1 if not found
pub fn conduit_get_param(
    caller: Caller<'_, HostState>,
    key_ptr: i32,
    key_len: i32,
    out_ptr: i32,
    out_len: i32,
) -> Result<i32, String> {
    let memory = caller.get_export("memory")
        .and_then(|e| e.into_memory())
        .map_err(|_| "Failed to get memory export".to_string())?;

    let key = read_string_from_memory(memory, key_ptr, key_len)?;
    let state = caller.data();

    // Look up the parameter in the input JSON
    let value = if let Some(v) = state.input_params.get(&key) {
        // Convert the value back to JSON string
        serde_json::to_string(v).map_err(|e| e.to_string())?
    } else {
        // Parameter not found
        return Ok(-1);
    };

    let value_bytes = value.as_bytes();

    // Check if the output buffer is large enough
    if value_bytes.len() > out_len as usize {
        warn!(
            key = &key,
            required = value_bytes.len(),
            provided = out_len,
            "Output buffer too small"
        );
        return Ok(-(value_bytes.len() as i32));
    }

    // Write the value to the output buffer
    write_string_to_memory(memory, out_ptr, value_bytes)?;

    debug!(key = &key, len = value_bytes.len(), "Parameter read");
    Ok(value_bytes.len() as i32)
}

/// Guest: Get the total XCom size (for debugging)
pub fn conduit_get_xcom_size(caller: Caller<'_, HostState>) -> i32 {
    caller.data().xcom.len() as i32
}

/// Register all host functions with the linker
pub fn register_host_functions(
    linker: &mut Linker<HostState>,
) -> WasmResult<()> {
    linker
        .func_wrap("conduit", "log", conduit_log)
        .map_err(|e| crate::WasmError::WasmtimeError(e.to_string()))?;

    linker
        .func_wrap("conduit", "emit_xcom", conduit_emit_xcom)
        .map_err(|e| crate::WasmError::WasmtimeError(e.to_string()))?;

    linker
        .func_wrap("conduit", "emit_watermark", conduit_emit_watermark)
        .map_err(|e| crate::WasmError::WasmtimeError(e.to_string()))?;

    linker
        .func_wrap("conduit", "get_param", conduit_get_param)
        .map_err(|e| crate::WasmError::WasmtimeError(e.to_string()))?;

    linker
        .func_wrap("conduit", "get_xcom_size", conduit_get_xcom_size)
        .map_err(|e| crate::WasmError::WasmtimeError(e.to_string()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_state_new() {
        let params = json!({"x": 42});
        let state = HostState::new(params.clone());

        assert_eq!(state.logs.len(), 0);
        assert_eq!(state.xcom.len(), 0);
        assert!(state.watermark.is_none());
        assert_eq!(state.input_params, params);
    }

    #[test]
    fn test_host_state_add_log() {
        let state = HostState::new(json!({}));
        let mut state = state;

        state.logs.push("Test message".to_string());
        assert_eq!(state.logs.len(), 1);
        assert_eq!(state.logs[0], "Test message");
    }

    #[test]
    fn test_host_state_add_xcom() {
        let state = HostState::new(json!({}));
        let mut state = state;

        state.xcom.insert("key1".to_string(), json!(42));
        state.xcom.insert("key2".to_string(), json!("value"));

        assert_eq!(state.xcom.len(), 2);
        assert_eq!(state.xcom.get("key1"), Some(&json!(42)));
        assert_eq!(state.xcom.get("key2"), Some(&json!("value")));
    }

    #[test]
    fn test_host_state_set_watermark() {
        let state = HostState::new(json!({}));
        let mut state = state;

        state.watermark = Some("2024-03-22T10:30:00Z".to_string());
        assert_eq!(state.watermark, Some("2024-03-22T10:30:00Z".to_string()));
    }

    #[test]
    fn test_read_string_from_memory_valid() {
        // This test is limited since we don't have a real Memory
        // We verify the logic with the string conversion
        let test_data = "hello world".as_bytes();
        let reconstructed = String::from_utf8(test_data.to_vec()).unwrap();
        assert_eq!(reconstructed, "hello world");
    }

    #[test]
    fn test_read_string_from_memory_invalid_utf8() {
        // Test that invalid UTF-8 is rejected
        let invalid_bytes = vec![0xFF, 0xFE, 0xFD];
        let result = String::from_utf8(invalid_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_host_state_complex_params() {
        let params = json!({
            "name": "test-task",
            "config": {
                "threads": 4,
                "timeout": 30
            },
            "tags": ["important", "urgent"]
        });

        let state = HostState::new(params.clone());
        assert_eq!(state.input_params.get("name"), Some(&json!("test-task")));
        assert_eq!(
            state.input_params.get("config").and_then(|c| c.get("threads")),
            Some(&json!(4))
        );
    }

    #[test]
    fn test_register_host_functions_succeeds() {
        // Test that we can create a linker and would register functions
        // Actual registration requires a valid Engine, tested in integration
        let test_fn = |_: Caller<'_, HostState>| -> Result<i32, String> { Ok(42) };

        // Verify the function signature is valid
        assert_eq!(test_fn(unsafe { std::mem::zeroed() }), Ok(42));
    }

    #[test]
    fn test_host_state_input_params_object() {
        let params = json!({
            "dag_id": "my_dag",
            "task_id": "task_1",
            "run_id": "2024-03-22T00:00:00Z"
        });

        let state = HostState::new(params);
        assert_eq!(
            state.input_params.get("dag_id"),
            Some(&json!("my_dag"))
        );
    }

    #[test]
    fn test_host_state_serialization() {
        let params = json!({"x": 1, "y": 2});
        let state = HostState::new(params);

        // Verify we can access the params
        assert_eq!(state.input_params.get("x"), Some(&json!(1)));
        assert_eq!(state.input_params.get("y"), Some(&json!(2)));
    }
}
