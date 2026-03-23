# Quick Start Guide: conduit-python

Get up and running with the Conduit Python bindings in 5 minutes.

## Installation

### Prerequisites
- Python 3.9+
- Rust 1.70+
- Cargo (included with Rust)

### From Source

```bash
cd conduit/conduit-python
maturin develop
```

This compiles the Rust code and installs the module in development mode.

## Basic Usage

### 1. Compile DAGs

```python
from conduit_native import compiler
import json

# Compile DAGs from a directory
plan_json = compiler.compile_dags("/path/to/dags")
plan = json.loads(plan_json)

print(f"Compiled {len(plan['dags'])} DAG(s)")
for dag in plan['dags']:
    print(f"  - {dag['id']}: {len(dag['tasks'])} tasks")
```

### 2. Validate DAGs

```python
result_json = compiler.validate_dag("/path/to/dags")
result = json.loads(result_json)

if result['valid']:
    print("✓ All DAGs are valid")
else:
    print("✗ Validation errors:")
    for error in result['errors']:
        print(f"  - {error}")
```

### 3. Detect Changes

```python
from conduit_native import planner

# Compute current fingerprints
fps_json = planner.compute_fingerprints(plan_json)

# Compare against previous state
previous_env = json.dumps({
    "fingerprints": { ... }  # Previous fingerprints
})

changes_json = planner.detect_changes(plan_json, previous_env)
changes = json.loads(changes_json)

print(f"Added: {changes['summary']['total_added']}")
print(f"Modified: {changes['summary']['total_modified']}")
print(f"Removed: {changes['summary']['total_removed']}")
```

### 4. Manage Environments

```python
from conduit_native.state import EnvironmentStore

# Initialize environment store
store = EnvironmentStore("/path/to/state")

# Create environments
store.create_env("dev")
store.create_env("staging", based_on="dev")
store.create_env("prod", based_on="staging")

# Promote changes
store.promote("dev", "staging")
store.promote("staging", "prod")

# Save to disk
store.save()
```

### 5. Analyze Lineage

```python
from conduit_native import lineage

sql = "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id"

# Extract lineage
lin_json = lineage.extract_sql_lineage(sql)
lin = json.loads(lin_json)

print(f"Input tables: {', '.join(lin['input_tables'])}")
print(f"Output columns: {[c['name'] for c in lin['output_columns']]}")
```

## Complete Example

```python
import json
from conduit_native import compiler, planner, lineage
from conduit_native.state import EnvironmentStore

# Setup
dag_path = "/path/to/dags"
state_path = "/path/to/state"

# 1. Compile
plan_json = compiler.compile_dags(dag_path)
plan = json.loads(plan_json)
print(f"✓ Compiled {len(plan['dags'])} DAG(s)")

# 2. Fingerprint
fps_json = planner.compute_fingerprints(plan_json)
fps = json.loads(fps_json)
print(f"✓ Computed fingerprints for {len(fps['fingerprints'])} task(s)")

# 3. Manage state
store = EnvironmentStore(state_path)
store.load()
store.create_env("new_env")
store.save()
print(f"✓ Environments saved")

# 4. Analyze lineage
lin_json = lineage.extract_sql_lineage("SELECT * FROM table")
lin = json.loads(lin_json)
print(f"✓ Lineage extracted: {len(lin['output_columns'])} output columns")
```

## Error Handling

All errors are raised as `ValueError` with descriptive messages:

```python
from conduit_native import compiler

try:
    plan_json = compiler.compile_dags("/invalid/path")
except ValueError as e:
    print(f"Error: {e}")
```

## JSON Response Formats

### Compiled Plan
```json
{
  "dags": [
    {
      "id": "dag_id",
      "name": "dag_name",
      "tasks": [
        {
          "id": "task_id",
          "type": "sql_execute",
          "dependencies": [
            {"task_id": "upstream", "kind": "FinishOnSuccess"}
          ]
        }
      ]
    }
  ]
}
```

### Fingerprints
```json
{
  "fingerprints": {
    "task_id": {
      "hash": "sha256:...",
      "computed_at": "2025-03-22T10:00:00Z",
      "version": 1
    }
  }
}
```

### Changes
```json
{
  "changes": {
    "added": [...],
    "modified": [...],
    "removed": [...],
    "upstream_invalidated": [...]
  },
  "summary": {
    "total_added": 0,
    "total_modified": 1,
    "total_removed": 0
  }
}
```

### Lineage
```json
{
  "input_tables": ["orders", "customers"],
  "output_columns": [
    {"name": "col1", "type": "STRING", "sources": [...]}
  ],
  "column_dependencies": [...]
}
```

## Next Steps

1. **Read Full Documentation**: See [README.md](README.md) for complete API reference
2. **Explore Examples**: Check [examples/](examples/) for detailed examples
3. **Review Implementation**: See [TECHNICAL.md](TECHNICAL.md) for architecture details
4. **Run Tests**: Execute `cargo test` to run Rust unit tests

## Performance Tips

1. **Batch Operations**: Process multiple DAGs in single `compile_dags()` call
2. **Cache Fingerprints**: Reuse computed fingerprints across operations
3. **Lazy Evaluation**: Only diff schemas when schema changes detected
4. **Incremental Updates**: Use change detection to find affected tasks only

## Troubleshooting

### Module Not Found
```
ImportError: conduit_native module not found
```

**Solution**: Run `maturin develop` to compile and install the module.

### Build Errors
```
error: could not compile `conduit-python`
```

**Solution**: Ensure all Conduit crate dependencies are in the parent directory:
```bash
cd ../
ls  # Should see: conduit-common, conduit-compiler, conduit-planner, etc.
```

### Runtime Errors
All runtime errors are raised as `ValueError` with descriptive messages. Always wrap calls in try/except.

## Getting Help

- **API Documentation**: See [README.md](README.md)
- **Technical Details**: See [TECHNICAL.md](TECHNICAL.md)
- **Examples**: See [examples/](examples/)
- **File Manifest**: See [MANIFEST.md](MANIFEST.md)

## Building for Production

### Release Build
```bash
maturin build --release
# Produces wheels in target/wheels/
```

### Test Multiple Python Versions
```bash
pip install maturin
for PY in 3.9 3.10 3.11 3.12; do
  maturin build --release -i python$PY
done
```

### Publish to PyPI
```bash
pip install twine
twine upload target/wheels/*
```

---

**Ready to get started?** Run `maturin develop` and try the examples!
