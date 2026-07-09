# PyO3 Bindings Crate Manifest

Complete PyO3 bindings for Conduit pipeline orchestrator. This manifest describes all created files.

## Project Structure

```
conduit-python/
├── Cargo.toml                          # Rust build configuration for maturin
├── pyproject.toml                      # Python packaging configuration
├── README.md                           # User-facing documentation
├── TECHNICAL.md                        # Technical deep-dive
├── MANIFEST.md                         # This file
├── src/                                # Rust source code
│   ├── lib.rs                          # Main PyO3 module initialization
│   ├── compiler.rs                     # DAG compilation wrapper (compiler module)
│   ├── planner.rs                      # Change detection wrapper (planner module)
│   ├── lineage.rs                      # Lineage extraction wrapper (lineage module)
│   └── state.rs                        # Environment management wrapper (state module)
├── python/
│   └── conduit_native/
│       └── __init__.py                 # Python package stub
└── examples/
    ├── plan_apply_workflow.py          # Complete workflow example
    └── test_fixtures.py                # Test data and fixtures
```

## File Descriptions

### Build & Configuration

#### `Cargo.toml`
Rust package manifest for maturin. Specifies:
- Package name, version, edition, license
- Library type as cdylib (native module)
- Dependencies on all Conduit crates
- PyO3 0.22 with extension-module feature
- Release profile with LTO and optimization

**Key Dependencies:**
- conduit-common, conduit-compiler, conduit-planner, conduit-lineage, conduit-state
- pyo3 0.22 (PyO3 bindings framework)
- serde_json 1.x (JSON serialization)

#### `pyproject.toml`
Python packaging manifest for maturin. Specifies:
- Build system (maturin>=1.7)
- Project metadata (name, version, description, Python requirements)
- Maturin configuration for building

**Supports:** Python 3.9+

### Documentation

#### `README.md`
User-facing documentation covering:
- Project overview and architecture
- Four functional modules (compiler, planner, lineage, state)
- Installation instructions
- API reference with JSON schema examples
- Complete usage examples
- Performance benchmarks
- Limitations and roadmap

#### `TECHNICAL.md`
Technical deep-dive for developers covering:
- Architecture overview and design decisions
- Module structure and code organization
- Detailed function descriptions with algorithms
- Error handling strategy
- JSON interchange rationale
- Performance considerations
- Building and distribution
- Testing strategy
- Debugging tips
- Limitations and future work

#### `MANIFEST.md`
This file - describes the complete project structure and file purposes.

### Rust Source Code

#### `src/lib.rs`
Main PyO3 module that:
- Defines error conversion function `conduit_error_to_pyerr()`
- Exports the `#[pymodule] fn conduit_native()` entry point
- Registers all four submodules: compiler, planner, lineage, state
- Adds module metadata (__version__, __doc__)

**Key Function:**
```rust
fn conduit_error_to_pyerr(err: ConduitError) -> PyErr
```
Converts Rust `ConduitError` to Python `ValueError`

#### `src/compiler.rs`
Exposes DAG compilation from `conduit-compiler` crate:

**Functions:**
- `compile_dags(path: &str) -> PyResult<String>`
  - Parses Python DAG definitions without execution
  - Returns JSON plan representation
  - Detects cycles and dependency issues

- `validate_dag(path: &str) -> PyResult<String>`
  - Validates DAGs without compilation
  - Returns validation results with errors and warnings
  - Checks for empty DAGs, orphaned tasks, etc.

**Creates submodule:** `conduit_native.compiler`

#### `src/planner.rs`
Exposes change detection from `conduit-planner` crate:

**Functions:**
- `compute_fingerprints(plan_json: &str) -> PyResult<String>`
  - Computes SHA256 fingerprints for all tasks
  - Returns JSON map of task_id → fingerprint hash
  - Enables change detection

- `detect_changes(plan_json: &str, env_json: &str) -> PyResult<String>`
  - Compares current plan against environment snapshots
  - Returns JSON with added/modified/removed/invalidated tasks
  - Implements Terraform-style plan/apply model

**Creates submodule:** `conduit_native.planner`

#### `src/lineage.rs`
Exposes SQL lineage from `conduit-lineage` crate:

**Functions:**
- `extract_sql_lineage(sql: &str) -> PyResult<String>`
  - Parses SQL and extracts source tables, output columns
  - Returns JSON with column-level dependencies
  - Enables lineage visualization

- `trace_column(direction: &str, task_id: &str, column_name: &str, edges_json: &str) -> PyResult<String>`
  - Traces column upstream or downstream through DAG
  - Uses BFS traversal on pre-computed edges
  - Returns path from start column through graph

- `diff_schemas(old_json: &str, new_json: &str) -> PyResult<String>`
  - Detects schema changes between two schemas
  - Identifies added, removed, modified columns
  - Flags breaking changes (removed columns, type changes)

**Creates submodule:** `conduit_native.lineage`

#### `src/state.rs`
Exposes environment management from `conduit-state` crate:

**Class: EnvironmentStore**
- `new(path: &str)` — Initialize at filesystem path
- `create_env(name: &str, based_on: Option<&str>)` — Create environment
- `list_envs() -> String` — List all environments with metadata
- `promote(source: &str, target: &str)` — Promote snapshots
- `save()` — Persist to disk
- `load()` — Load from disk
- `add_snapshot()` — Add snapshot to environment
- `get_snapshot()` — Retrieve snapshot
- `get_env()` — Get environment as JSON

**Creates submodule:** `conduit_native.state`

### Python Code

#### `python/conduit_native/__init__.py`
Python package initialization stub:
- Imports from compiled native module
- Exposes submodules: compiler, planner, lineage, state
- Provides helpful error message if module not compiled
- Sets __version__ and __doc__

### Examples

#### `examples/plan_apply_workflow.py`
Complete end-to-end example demonstrating:
1. DAG validation
2. DAG compilation
3. Fingerprint computation
4. Environment management
5. Change detection
6. Lineage analysis

Shows full workflow from compilation through promotion with error handling and progress output.

#### `examples/test_fixtures.py`
Comprehensive test data and fixtures including:
- Sample DAG definitions
- Compiled plans
- Fingerprints
- Change detection results
- Lineage extraction results
- Schema comparisons
- Environment structures

Can be imported and used in tests or as documentation of expected JSON formats.

## Module APIs

### `conduit_native.compiler`
```python
from conduit_native import compiler

plan_json = compiler.compile_dags("/path/to/dags")
result_json = compiler.validate_dag("/path/to/dags")
```

### `conduit_native.planner`
```python
from conduit_native import planner

fps_json = planner.compute_fingerprints(plan_json)
changes_json = planner.detect_changes(plan_json, env_json)
```

### `conduit_native.lineage`
```python
from conduit_native import lineage

lin_json = lineage.extract_sql_lineage("SELECT ...")
trace_json = lineage.trace_column("upstream", task_id, col, edges_json)
diff_json = lineage.diff_schemas(old_schema_json, new_schema_json)
```

### `conduit_native.state`
```python
from conduit_native.state import EnvironmentStore

store = EnvironmentStore("/path/to/state")
store.create_env("prod")
store.promote("dev", "prod")
store.save()
```

## Building

### Development
```bash
maturin develop
```

### Release
```bash
maturin build --release
```

### Tests
```bash
cargo test
python examples/plan_apply_workflow.py
```

## Dependencies

### Rust Crates
- **conduit-common** — Shared types and error handling
- **conduit-compiler** — DAG parsing and compilation
- **conduit-planner** — Fingerprinting and change detection
- **conduit-lineage** — SQL lineage and schema analysis
- **conduit-state** — Environment and snapshot management
- **pyo3** — Python bindings framework
- **serde_json** — JSON serialization

### Python
- Python 3.9+ (CPython)

## Version

- **Package Version:** single-sourced from conduit-python/Cargo.toml (currently 0.1.1)
- **Edition:** 2021 (Rust)
- **License:** Apache-2.0

## Design Principles (v0.1)

1. **JSON Interchange** — All complex types use JSON strings for pragmatic interchange
2. **Error Conversion** — All Rust errors become Python `ValueError` with messages
3. **Stateless Functions** — Most APIs are stateless; EnvironmentStore for state
4. **Module Organization** — Submodules mirror underlying Rust crates
5. **Completeness** — Bindings expose full functionality, not subset

## Performance Targets

- DAG compilation: ~500ms for 1000 DAGs
- Fingerprinting: ~10ms per 1000 tasks
- Change detection: ~50ms per comparison
- SQL lineage: ~2ms per query
- Schema diffing: <1ms per 100-column schema

## Future Enhancements (v0.2+)

- Native Python classes with `#[pyclass]`
- Async/await support with tokio
- Caching layer (Redis/SQLite)
- Python operator support
- Streaming for large DAGs
- Webhook notifications

## Testing

### Rust Unit Tests
```bash
cargo test
```

### Python Integration Tests
```bash
python -m pytest examples/
```

### Example Scripts
```bash
python examples/plan_apply_workflow.py
python examples/test_fixtures.py
```

## Contributing

When modifying:
1. Update both `src/` Rust code and `python/` stubs
2. Add JSON schema examples to TECHNICAL.md
3. Update test fixtures in examples/test_fixtures.py
4. Run cargo test and example scripts

## Support Resources

- **User Guide:** README.md
- **Technical Details:** TECHNICAL.md
- **Examples:** examples/plan_apply_workflow.py, examples/test_fixtures.py
- **Code:** src/{lib,compiler,planner,lineage,state}.rs
