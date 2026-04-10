#!/usr/bin/env bash
# Run SQL provider integration tests against Docker databases.
#
# Usage: ./scripts/test-integration.sh
#
# Starts Postgres + MySQL via docker compose, runs the #[ignore] tests,
# and tears everything down on exit.

set -euo pipefail

COMPOSE_FILE="docker-compose.integration.yml"
cd "$(dirname "$0")/.."

cleanup() {
    echo ""
    echo "==> Stopping test databases..."
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
}
trap cleanup EXIT

echo "==> Starting test databases..."
docker compose -f "$COMPOSE_FILE" up -d --wait

# Postgres (also used for CockroachDB/Redshift/TimescaleDB wire-compat tests)
export CONDUIT_TEST_PG_HOST=localhost
export CONDUIT_TEST_PG_PORT=5432
export CONDUIT_TEST_PG_USER=conduit
export CONDUIT_TEST_PG_PASSWORD=conduit_test
export CONDUIT_TEST_PG_DB=testdb

# MySQL
export CONDUIT_TEST_MYSQL_HOST=localhost
export CONDUIT_TEST_MYSQL_PORT=3306
export CONDUIT_TEST_MYSQL_USER=conduit
export CONDUIT_TEST_MYSQL_PASSWORD=conduit_test
export CONDUIT_TEST_MYSQL_DB=testdb

echo "==> Running integration tests..."
cargo test --package conduit-providers --test sql_providers_test -- --ignored --test-threads=1

echo ""
echo "==> All integration tests passed!"
