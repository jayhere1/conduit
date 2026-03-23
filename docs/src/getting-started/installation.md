# Installation

This guide walks you through installing Conduit and setting up your first project.

## System Requirements

### Rust

Conduit requires **Rust 1.75 or later** (2021 edition).

Check your Rust version:

```bash
rustc --version
```

If you don't have Rust installed, install it via [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

### Python

Python is required for writing and running task definitions. Conduit supports **Python 3.9+**.

Check your Python version:

```bash
python3 --version
```

### System Libraries

Conduit uses RocksDB for the event store and tree-sitter for DAG parsing. You'll need development headers.

**Ubuntu/Debian:**

```bash
apt update
apt install -y build-essential clang librocksdb-dev
```

**macOS:**

```bash
brew install llvm rocksdb
```

**Fedora/RHEL:**

```bash
dnf groupinstall -y "Development Tools"
dnf install -y clang rocksdb-devel
```

## Installing Conduit

### Option 1: From Source (Recommended)

Clone the repository and build:

```bash
git clone https://github.com/conduit-orchestrator/conduit.git
cd conduit
cargo install --path conduit-cli
```

Verify the installation:

```bash
conduit --version
```

### Option 2: From Crates.io (When Available)

Once Conduit reaches 1.0, it will be available on crates.io:

```bash
cargo install conduit-cli
```

## Initializing Your First Project

Create a new Conduit project:

```bash
conduit init my-project
cd my-project
```

This scaffolds a project directory with the following structure:

```
my-project/
├── .conduit/
│   ├── state.db              # Event store (RocksDB)
│   ├── snapshots.json        # Snapshot index
│   └── environments.json      # Environment pointers
├── dags/
│   └── hello_world.py        # Example DAG
├── tasks/
│   └── common.py             # Shared task utilities
├── .gitignore
└── README.md
```

## Project Structure

### dags/

This directory contains your DAG definitions. Each file should define one or more `@dag` decorated functions:

```python
# dags/etl.py
from conduit.sdk import dag, task

@task
def extract():
    print("Extracting data...")
    return "data.csv"

@task
def transform(data):
    print(f"Transforming {data}...")
    return "clean.csv"

@dag(schedule="0 9 * * *")
def etl_pipeline():
    raw = extract()
    clean = transform(raw)
    return clean
```

### tasks/

Shared task logic and utilities. This is useful for DRY-ing up common patterns:

```python
# tasks/common.py
from conduit.sdk import task

@task
def log_status(message):
    print(f"Status: {message}")
```

### .conduit/

Internal state directory (version control with `.gitignore`):
- `state.db`: RocksDB event store containing all pipeline runs
- `snapshots.json`: Content-addressable index of compiled DAG snapshots
- `environments.json`: Pointers to production, staging, etc.

## Configuration

Optional: Create a `.conduit.toml` file in your project root to customize behavior:

```toml
[project]
name = "my-project"
version = "0.1.0"

[execution]
# Default timeout for all tasks (seconds)
timeout = 3600

# Max concurrent task pool size
max_concurrency = 10

# Event store path (relative to project root)
state_dir = ".conduit"

[scheduler]
# How often to check for scheduled DAGs (seconds)
check_interval = 5

[ui]
# Web UI port
port = 8080
```

## Verifying Installation

Compile the example DAG to verify everything works:

```bash
conduit compile
```

You should see output like:

```
Compiling DAGs in dags/...
✓ hello_world.py
  - Tasks: 2
  - Dependencies: 1
  - Schedule: None
  - Fingerprint: abc123def456

Compilation successful: 1 DAG compiled
```

## Next Steps

- **[Quick Start](../quick-start.md)**: Build a 3-task DAG in 5 minutes
- **[DAG Concepts](../concepts/dags.md)**: Learn the full DAG definition syntax
- **[Virtual Environments](../concepts/environments.md)**: Understand production, staging, and feature branches
