# WASM Plugin Sandbox

Conduit supports extending its functionality through WebAssembly (WASM) plugins.
Plugins run in a sandboxed Wasmtime runtime with fine-grained permission controls,
fuel-based execution limits, and memory caps — ensuring plugins can't crash the
orchestrator or access resources they shouldn't.

## Why WASM?

Traditional plugin systems use shared libraries (`.so`/`.dll`) or embedded scripting
(Lua, Python). Both have serious tradeoffs — shared libraries can crash the host
process and have full system access, while embedded scripting is slow and
language-specific.

WASM plugins offer:

- **Sandboxing** — plugins can't access files, network, or memory outside their sandbox
- **Performance** — near-native execution speed via Wasmtime's optimizing compiler
- **Language-agnostic** — write plugins in Rust, Go, C/C++, AssemblyScript, or anything that compiles to WASM
- **Deterministic** — fuel metering prevents infinite loops; memory limits prevent OOM

## Plugin Manifest

Every plugin ships as a `.wasm` binary alongside a `plugin.toml` manifest:

```toml
[plugin]
name = "my-custom-transform"
version = "1.0.0"
description = "A custom data transformation plugin"
author = "Your Name"
license = "Apache-2.0"

[permissions]
log = true
read_xcom = true
write_xcom = true
read_watermark = true
write_watermark = true
read_params = true

[limits]
max_fuel = 1000000000    # execution fuel (prevents infinite loops)
max_memory_bytes = 67108864  # 64 MB memory cap
```

## Permissions

Plugins must declare which host functions they need. The runtime enforces these at
call time — if a plugin tries to call a function it doesn't have permission for,
the call returns an error.

| Permission | Host Function | Description |
|---|---|---|
| `log` | `host_log(level, msg)` | Write to Conduit's structured log |
| `read_xcom` | `host_xcom_get(key)` | Read cross-task communication values |
| `write_xcom` | `host_xcom_set(key, val)` | Write cross-task communication values |
| `read_watermark` | `host_watermark_get(task)` | Read incremental watermark state |
| `write_watermark` | `host_watermark_set(task, val)` | Advance incremental watermarks |
| `read_params` | `host_param_get(key)` | Read pipeline parameters |

## Writing a Plugin (Rust)

Here's a minimal Rust plugin:

```rust
// lib.rs
extern "C" {
    fn host_log(level: i32, ptr: *const u8, len: i32);
    fn host_xcom_get(ptr: *const u8, len: i32) -> i64;
    fn host_xcom_set(key_ptr: *const u8, key_len: i32, val_ptr: *const u8, val_len: i32);
}

#[no_mangle]
pub extern "C" fn run() -> i32 {
    let msg = "Hello from WASM plugin!";
    unsafe {
        host_log(1, msg.as_ptr(), msg.len() as i32);
    }
    0  // return 0 for success
}
```

Compile with:

```bash
cargo build --target wasm32-wasi --release
```

## Plugin Registry

Conduit maintains a local plugin registry at `.conduit/plugins/`. Install plugins with:

```bash
# Install from a local .wasm file
conduit plugin install ./my-plugin.wasm --manifest ./plugin.toml

# List installed plugins
conduit plugin list

# Run a plugin directly
conduit plugin run my-custom-transform --params '{"key": "value"}'
```

## Runtime Configuration

Configure the WASM runtime globally in `conduit.yaml`:

```yaml
wasm:
  max_fuel: 1000000000       # default fuel limit per execution
  max_memory_mb: 64          # default memory limit per plugin
  enable_epoch_interruption: true
  plugin_dir: .conduit/plugins
```

## Security Model

The WASM sandbox provides defense-in-depth:

1. **Memory isolation** — each plugin gets its own linear memory space, completely
   separate from the host process
2. **Fuel metering** — every WASM instruction consumes fuel; when fuel runs out,
   execution halts with an error
3. **Permission gates** — host functions check the plugin's declared permissions
   before executing
4. **No filesystem access** — plugins cannot read or write files on the host
5. **No network access** — plugins cannot make network calls
6. **Deterministic execution** — given the same inputs and fuel, plugins produce
   the same outputs
