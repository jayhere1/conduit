# Conduit Test Guide

## Quick Start

```bash
# Run all tests
cargo test --workspace

# Run with output (for debugging)
cargo test --workspace -- --nocapture
```

## Test Tiers

### 1. Unit Tests (in-module `#[cfg(test)]`)

Each crate has inline unit tests. Run them individually:

```bash
cargo test --package conduit-common
cargo test --package conduit-compiler
cargo test --package conduit-scheduler
cargo test --package conduit-executor
cargo test --package conduit-state
cargo test --package conduit-distributed
cargo test --package conduit-planner
cargo test --package conduit-lineage
cargo test --package conduit-providers
```

Key modules with inline tests:

- `conduit-distributed/src/coordinator.rs` — Worker registration, task dispatch, queuing, heartbeat, result forwarding (10 tests)
- `conduit-distributed/src/worker.rs` — Task execution, cancellation, drain mode, CONDUIT protocol parsing (9 tests)
- `conduit-distributed/src/convert.rs` — Proto type roundtrip conversion (8 tests)
- `conduit-state/src/event_store.rs` — Append, range query, compaction, retention policies (12 tests)
- `conduit-providers/src/registry.rs` — Registry CRUD, typed accessors, aliases, display names, mixed configs (25+ tests)
- `conduit-providers/src/secrets.rs` — Secrets chain resolution, caching, backends, health checks, error conversion (20+ tests)
- `conduit-providers/src/plugin.rs` — Plugin discovery, manifest parsing, API compatibility, filesystem fixtures (15+ tests)

### 2. API Integration Tests

HTTP-level tests using `tower::ServiceExt::oneshot()` through the full Axum middleware stack. No TCP listener needed.

```bash
# All API integration tests
cargo test --package conduit-api --test integration_test

# Handler-specific tests (environments, runs, auth, connections, errors)
cargo test --package conduit-api --test handler_tests
```

**What they cover:**
- Health/info endpoints
- DAG listing and compilation
- Run triggering, listing, filtering by status/limit
- Environment CRUD, promote, diff
- Auth lifecycle (create key, authenticate, RBAC, revoke)
- Connection and provider listing
- Error response shape consistency (all errors return `{"error": {"type": "...", "message": "..."}}`)

### 3. gRPC Integration Tests

Spin up a real tonic gRPC server on localhost with ephemeral ports and connect a client.

```bash
cargo test --package conduit-distributed --test grpc_integration_test
```

**What they cover:**
- Worker registration and task assignment streaming
- Full task lifecycle over the wire (register, receive assignment, report result)
- Multi-worker dispatch with least-loaded routing
- Bidirectional heartbeat streaming with ack directives
- Client-streaming log delivery
- Cluster status queries
- Queued task dispatch on late worker arrival
- Proto type fidelity through serialization
- Concurrent client access

### 4. Pipeline E2E Tests

Wire up the full Conduit pipeline in-process: Scheduler → Executor → Event Store.

```bash
cargo test --package conduit-api --test pipeline_e2e_test
```

**What they cover:**
- Linear 3-task DAG (extract → transform → load) runs to completion
- Diamond DAG (start → left/right → join) with fan-out/fan-in
- Task failure propagation (failing task causes DAG run to fail, downstream tasks skipped)
- Event store captures correct sequences with monotonic ordering

### 5. CLI Integration Tests

Test the CLI command handlers (init, compile, env, status, migrate) using temp directories.

```bash
cargo test --package conduit-cli --test cli_integration_test
```

**What they cover:**
- `conduit init` project scaffolding (dirs, config, example DAGs, .gitignore)
- `conduit compile` for Python, YAML, and mixed DAG directories
- `conduit compile` statistics, plan save/load, check mode, empty directories
- `conduit env` lifecycle (create, list, promote, duplicate detection)
- `conduit status` fresh state inspection
- `conduit migrate` Airflow DAG detection via regex patterns
- Snapshot store and environment manager persistence/reload
- State directory resolution logic

### 6. Compiler Fixture Tests

Test the full compilation pipeline against real `.py` and `.yaml` fixture files.

```bash
cargo test --package conduit-compiler --test compiler_fixture_tests
```

**What they cover:**
- Full fixture directory compilation (all formats combined)
- Python DAGs: linear chains, diamond fan-out/fan-in, multi-DAG-per-file
- YAML DAGs: all 5 task types, incremental strategies, data quality contracts
- Dependency resolution with topological ordering verification
- Cycle and unknown-dependency detection
- `conduit.yaml` config file skipping during directory scan
- Plan JSON serialization roundtrip
- Format equivalence between Python and YAML outputs
- Compilation performance assertion (< 5 seconds for all fixtures)

### 7. Benchmarks

Performance benchmarks using Criterion:

```bash
# Run all benchmarks
cargo bench --package conduit-bench

# Run specific benchmark suites
cargo bench --package conduit-bench --bench compiler_bench
cargo bench --package conduit-bench --bench scheduler_bench
cargo bench --package conduit-bench --bench planner_bench
cargo bench --package conduit-bench --bench state_bench
cargo bench --package conduit-bench --bench lineage_bench
```

**Suites:**
- **compiler_bench** — Compilation scaling (10-1000 DAGs)
- **scheduler_bench** — Scheduler init scaling (100-10000 tasks)
- **planner_bench** — Fingerprinting, impact analysis (100-10000 tasks)
- **state_bench** — Event store append, range query, mixed events, scaling
- **lineage_bench** — Graph construction, upstream/downstream trace, edge scaling

## Running Specific Tests

```bash
# By test name
cargo test --package conduit-distributed --test grpc_integration_test full_task_lifecycle

# By pattern
cargo test --workspace heartbeat
```

## Test Infrastructure

**API tests** use `tower::ServiceExt::oneshot()` to send HTTP requests through the full Axum stack without binding a port. Each test creates a fresh `AppState` pointing at a temp directory.

**gRPC tests** bind to `127.0.0.1:0` (ephemeral port) and use `tonic::transport::Server::serve_with_incoming()` with a `TcpListenerStream`. Each test gets its own server instance.

**Pipeline E2E tests** use `tokio::sync::mpsc` channels to wire Scheduler ↔ Executor, with a mediator task that bridges commands/events and records to a RocksDB `EventStore` in a `TempDir`.

**Auth tests** create API keys directly on the `AuthStore` (bypassing HTTP) to bootstrap admin access, then test the full HTTP auth flow including key creation, RBAC enforcement, and revocation.

## Dependencies

Integration tests require no external services. Everything runs in-process:
- RocksDB for the event store (uses temp directories)
- tonic for gRPC (uses loopback)
- Axum for HTTP (uses tower oneshot, no TCP)
- Task execution uses real `bash` child processes

## Adding New Tests

1. **Handler tests** go in `conduit-api/tests/handler_tests.rs` (HTTP-level) or inline in the handler file (unit-level)
2. **gRPC tests** go in `conduit-distributed/tests/grpc_integration_test.rs`
3. **Pipeline tests** go in `conduit-api/tests/pipeline_e2e_test.rs`
4. **State/event tests** go inline in `conduit-state/src/event_store.rs`
5. **CLI tests** go in `conduit-cli/tests/cli_integration_test.rs`
6. **Compiler fixture tests** go in `conduit-compiler/tests/compiler_fixture_tests.rs` with fixtures in `tests/fixtures/`
7. **Provider tests** go inline in `conduit-providers/src/registry.rs`, `secrets.rs`, or `plugin.rs`
8. **Benchmarks** go in `conduit-bench/benches/` (one file per crate, register in `Cargo.toml`)
