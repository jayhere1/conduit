#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Conduit Docker Helper
#
# Usage:
#   ./scripts/docker-run.sh              → build & start server (production)
#   ./scripts/docker-run.sh dev          → dev mode (UI hot reload + cargo-watch)
#   ./scripts/docker-run.sh test         → run all tests in Docker
#   ./scripts/docker-run.sh bench        → run benchmarks in Docker
#   ./scripts/docker-run.sh docs         → serve documentation
#   ./scripts/docker-run.sh stop         → stop all containers
#   ./scripts/docker-run.sh clean        → remove containers, volumes, images
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Create dags directory if it doesn't exist
mkdir -p dags

case "${1:-start}" in
  start|up)
    echo "Building and starting Conduit..."
    docker compose up --build -d conduit
    echo ""
    echo "Conduit is running:"
    echo "  API:    http://localhost:${CONDUIT_PORT:-9090}"
    echo "  Health: http://localhost:${CONDUIT_PORT:-9090}/api/v1/health"
    echo ""
    echo "Place your DAG files in ./dags/ and they'll be picked up automatically."
    echo "Use 'docker compose logs -f conduit' to watch logs."
    ;;

  dev)
    echo "Starting Conduit in development mode..."
    docker compose --profile dev up --build -d
    echo ""
    echo "Dev mode running:"
    echo "  API:  http://localhost:${CONDUIT_PORT:-9090}"
    echo "  UI:   http://localhost:${UI_PORT:-3000}"
    echo ""
    echo "UI has hot reload. API rebuilds on source changes."
    ;;

  test)
    echo "Running tests in Docker..."
    docker compose --profile test run --rm test
    ;;

  bench)
    echo "Running benchmarks in Docker..."
    docker compose --profile bench run --rm bench
    echo "Results saved in 'bench-results' volume."
    ;;

  docs)
    echo "Starting documentation server..."
    docker compose --profile docs up -d docs
    echo ""
    echo "Docs: http://localhost:${DOCS_PORT:-3001}"
    ;;

  stop|down)
    echo "Stopping Conduit..."
    docker compose --profile dev --profile docs down
    ;;

  clean)
    echo "Cleaning up everything..."
    docker compose --profile dev --profile docs --profile test --profile bench down -v --rmi local
    echo "Done."
    ;;

  logs)
    docker compose logs -f conduit
    ;;

  shell)
    echo "Opening shell in Conduit container..."
    docker compose exec conduit bash
    ;;

  status)
    docker compose ps
    echo ""
    curl -s http://localhost:${CONDUIT_PORT:-9090}/api/v1/health 2>/dev/null | python3 -m json.tool || echo "Server not reachable"
    ;;

  *)
    echo "Usage: $0 {start|dev|test|bench|docs|stop|clean|logs|shell|status}"
    exit 1
    ;;
esac
