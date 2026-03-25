#!/usr/bin/env bash
# ─── Seed Demo Data ─────────────────────────────────────────────────────────
# Trigger a few runs so the UI looks alive before recording a demo.
# Requires: conduit serve running on port 9000
#
# Usage: ./scripts/seed-demo.sh

set -euo pipefail

API="${CONDUIT_API_URL:-http://localhost:9091/api/v1}"
GREEN='\033[0;32m'
BLUE='\033[0;34m'
DIM='\033[2m'
RESET='\033[0m'

echo -e "${BLUE}Seeding demo data...${RESET}"
echo ""

# Get list of DAGs
DAGS=$(curl -s "$API/dags" | python3 -c "
import sys, json
data = json.load(sys.stdin)
dags = data.get('dags', data) if isinstance(data, dict) else data
for d in dags:
    print(d.get('id', ''))
" 2>/dev/null || echo "")

if [ -z "$DAGS" ]; then
    echo "No DAGs found. Make sure 'conduit serve' is running and DAGs are compiled."
    exit 1
fi

echo -e "Found DAGs:"
echo "$DAGS" | while read -r dag; do
    [ -n "$dag" ] && echo -e "  ${DIM}•${RESET} $dag"
done
echo ""

# Trigger a run for each DAG
echo "$DAGS" | while read -r dag_id; do
    [ -z "$dag_id" ] && continue

    echo -ne "  Triggering ${GREEN}${dag_id}${RESET}... "

    RESULT=$(curl -s -X POST "$API/dags/$dag_id/runs" \
        -H "Content-Type: application/json" \
        -d '{"environment": "production"}' 2>/dev/null || echo '{"error": "failed"}')

    RUN_ID=$(echo "$RESULT" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(data.get('id', data.get('run_id', 'unknown')))
" 2>/dev/null || echo "unknown")

    echo -e "run ${DIM}${RUN_ID}${RESET}"
done

echo ""
echo -e "${GREEN}Done!${RESET} Wait a few seconds for runs to complete, then record your demo."
echo ""
echo "Demo recording tips:"
echo "  1. Start on the Dashboard page"
echo "  2. Click into a DAG → Graph tab (show the dependency graph)"
echo "  3. Click Run → watch live execution"
echo "  4. Visit Plan/Apply → generate a plan"
echo "  5. Visit Environments → show staging fork"
echo ""
echo "Use a recording tool that hides tooltips (e.g., Kap, OBS, or macOS screen recording)."
