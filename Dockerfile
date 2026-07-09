# ─────────────────────────────────────────────────────────────────────────────
# Conduit — Multi-stage Docker build
# Produces a single image with: Rust API server, compiled UI, Python runtime
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: Build the Rust binary ────────────────────────────────────────────
FROM rust:1.91-bookworm AS rust-builder

# Install RocksDB system deps + protoc (conduit-distributed's build script
# runs prost/tonic codegen).
RUN apt-get update && apt-get install -y \
    libclang-dev \
    librocksdb-dev \
    protobuf-compiler \
    cmake \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace manifests first for layer caching
COPY Cargo.toml Cargo.lock ./
COPY conduit-common/Cargo.toml conduit-common/Cargo.toml
COPY conduit-compiler/Cargo.toml conduit-compiler/Cargo.toml
COPY conduit-scheduler/Cargo.toml conduit-scheduler/Cargo.toml
COPY conduit-executor/Cargo.toml conduit-executor/Cargo.toml
COPY conduit-state/Cargo.toml conduit-state/Cargo.toml
COPY conduit-planner/Cargo.toml conduit-planner/Cargo.toml
COPY conduit-lineage/Cargo.toml conduit-lineage/Cargo.toml
COPY conduit-api/Cargo.toml conduit-api/Cargo.toml
COPY conduit-providers/Cargo.toml conduit-providers/Cargo.toml
COPY conduit-distributed/Cargo.toml conduit-distributed/Cargo.toml
COPY conduit-distributed/build.rs conduit-distributed/build.rs
COPY conduit-distributed/proto conduit-distributed/proto
COPY conduit-cli/Cargo.toml conduit-cli/Cargo.toml

# Create dummy source files so cargo can resolve the dependency graph. Every
# workspace member listed in the root Cargo.toml must be present or the
# workspace won't parse.
RUN mkdir -p conduit-common/src && echo "" > conduit-common/src/lib.rs && \
    mkdir -p conduit-compiler/src && echo "" > conduit-compiler/src/lib.rs && \
    mkdir -p conduit-scheduler/src && echo "" > conduit-scheduler/src/lib.rs && \
    mkdir -p conduit-executor/src && echo "" > conduit-executor/src/lib.rs && \
    mkdir -p conduit-state/src && echo "" > conduit-state/src/lib.rs && \
    mkdir -p conduit-planner/src && echo "" > conduit-planner/src/lib.rs && \
    mkdir -p conduit-lineage/src && echo "" > conduit-lineage/src/lib.rs && \
    mkdir -p conduit-api/src && echo "" > conduit-api/src/lib.rs && \
    mkdir -p conduit-providers/src && echo "" > conduit-providers/src/lib.rs && \
    mkdir -p conduit-distributed/src && echo "" > conduit-distributed/src/lib.rs && \
    mkdir -p conduit-cli/src && echo "fn main() {}" > conduit-cli/src/main.rs

# Pre-build dependencies (this layer gets cached unless Cargo.toml changes)
RUN cargo build --release 2>/dev/null || true

# Now copy real source code
COPY conduit-common/ conduit-common/
COPY conduit-compiler/ conduit-compiler/
COPY conduit-scheduler/ conduit-scheduler/
COPY conduit-executor/ conduit-executor/
COPY conduit-state/ conduit-state/
COPY conduit-planner/ conduit-planner/
COPY conduit-lineage/ conduit-lineage/
COPY conduit-api/ conduit-api/
COPY conduit-providers/ conduit-providers/
COPY conduit-distributed/ conduit-distributed/
COPY conduit-cli/ conduit-cli/

# conduit-cli embeds the Python SDK at compile time via include_dir!
# (vendored into `conduit init` scaffolds), so sdk/ must be present in the
# builder before the binary is compiled.
COPY sdk/ sdk/

# Touch the main files to invalidate the dummy builds
RUN find . -name "*.rs" -exec touch {} +

# Build the release binary
RUN cargo build --release --bin conduit

# ── Stage 2: Build the React UI ──────────────────────────────────────────────
FROM node:20-slim AS ui-builder

WORKDIR /build/conduit-ui

COPY conduit-ui/package.json conduit-ui/package-lock.json* ./
RUN npm ci --no-audit --no-fund 2>/dev/null || npm install --no-audit --no-fund

COPY conduit-ui/ ./
RUN npm run build

# ── Stage 3: Runtime image ───────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# Install runtime deps: Python (for task execution), ca-certs, RocksDB libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    librocksdb7.8 \
    ca-certificates \
    tini \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -s /bin/bash conduit
WORKDIR /home/conduit

# Copy the compiled binary
COPY --from=rust-builder /build/target/release/conduit /usr/local/bin/conduit

# Copy the built UI assets
COPY --from=ui-builder /build/conduit-ui/dist /opt/conduit/ui

# Copy Python SDK
COPY sdk/python /opt/conduit/sdk/python
RUN pip3 install --break-system-packages -e /opt/conduit/sdk/python 2>/dev/null || true

# Copy example DAGs
COPY examples /opt/conduit/examples

# Set up working directory structure
RUN mkdir -p /data/dags /data/.conduit && chown -R conduit:conduit /data

# Environment
ENV CONDUIT_DAGS_PATH=/data/dags \
    CONDUIT_STATE_DIR=/data/.conduit \
    CONDUIT_UI_DIR=/opt/conduit/ui \
    CONDUIT_HOST=0.0.0.0 \
    CONDUIT_PORT=9090 \
    RUST_LOG=conduit=info

EXPOSE 9090

# Health check
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:9090/api/v1/health || exit 1

USER conduit

ENTRYPOINT ["tini", "--"]
CMD ["conduit", "serve", "--host", "0.0.0.0", "--port", "9090", "--dags-path", "/data/dags", "--state-dir", "/data/.conduit"]
