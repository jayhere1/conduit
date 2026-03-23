#!/usr/bin/env bash
#
# Run the full Conduit test suite, writing results to a timestamped log file.
#
# Usage:
#   ./scripts/test.sh              # run all workspace tests
#   ./scripts/test.sh -p conduit-api  # pass extra args to cargo test
#
# Logs are written to logs/test_<timestamp>.log

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$REPO_ROOT/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/test_${TIMESTAMP}.log"

echo "╭──────────────────────────────────────────────╮"
echo "│  Conduit Test Suite                          │"
echo "│  Log: logs/test_${TIMESTAMP}.log             │"
echo "╰──────────────────────────────────────────────╯"

cd "$REPO_ROOT"

# Run tests, tee to both stdout and log file.
# Use -- --nocapture if you want to see println! output in tests.
cargo test --workspace "$@" 2>&1 | tee "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

echo ""
echo "───────────────────────────────────────────────"
echo "Log saved to: $LOG_FILE"
echo "Exit code: $EXIT_CODE"
echo "───────────────────────────────────────────────"

exit "$EXIT_CODE"
