#!/bin/bash
# Demo recording script for Conduit — full UI + terminal version
#
# This script executes actual Conduit commands and provides voice-over cues
# for recording with OBS or macOS screen recording. Designed to show:
# - Terminal commands and their real output
# - Interactive dashboard at http://localhost:8080
# - API documentation at /api/docs
#
# Usage:
#   1. Set up a screen recorder (OBS or macOS Cmd+Shift+5)
#   2. Split screen: terminal on left (60%), browser on right (40%)
#   3. Run this script: ./scripts/demo-full.sh
#   4. Narrate over the recording or add voice-over in post-production
#
# Suggested resolution: 1920x1080 (or your monitor's native res)
# Suggested recording: 30fps, H.264 codec, 10 Mbps bitrate

set -e

# Colors
readonly CYAN='\033[1;36m'
readonly BLUE='\033[1;34m'
readonly GREEN='\033[0;32m'
readonly BOLD='\033[1m'
readonly RESET='\033[0m'

# Configuration
readonly PROJECT_NAME="conduit_demo_$$"
readonly DEMO_DIR="/tmp/${PROJECT_NAME}"
readonly PORT=8080
readonly SERVER_PID_FILE="${DEMO_DIR}/.server.pid"

# Cleanup on exit
cleanup() {
    echo ""
    printf "${CYAN}${BOLD}Cleaning up demo...${RESET}\n"

    # Kill the server if running
    if [[ -f "$SERVER_PID_FILE" ]]; then
        local pid
        pid=$(cat "$SERVER_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            printf "${CYAN}Stopping Conduit server (PID: ${pid})${RESET}\n"
            kill "$pid" 2>/dev/null || true
            sleep 1
        fi
        rm -f "$SERVER_PID_FILE"
    fi

    # Remove demo directory
    if [[ -d "$DEMO_DIR" ]]; then
        printf "${CYAN}Removing temporary project: ${DEMO_DIR}${RESET}\n"
        rm -rf "$DEMO_DIR"
    fi

    printf "${GREEN}✓ Cleanup complete${RESET}\n"
}

trap cleanup EXIT

# ─── Helper functions ────────────────────────────────────────────────────────

# Print a voice-over cue (grey, dimmed)
voice_cue() {
    local text="$1"
    printf "${CYAN}${BOLD}[VOICE CUE]${RESET} ${text}\n"
    sleep 0.5
}

# Print a section header
header() {
    local text="$1"
    printf "\n${CYAN}${BOLD}════ %s ════${RESET}\n" "$text"
    sleep 0.5
}

# Run a command with a pause after
run_cmd_with_cue() {
    local cmd="$1"
    local cue="$2"
    local wait_after="${3:-2}"

    voice_cue "$cue"
    printf "${CYAN}${BOLD}\$ ${cmd}${RESET}\n"
    sleep 1

    # Execute the command
    eval "$cmd"

    sleep "$wait_after"
}

# ─── Main demo flow ──────────────────────────────────────────────────────────

clear

printf "${CYAN}${BOLD}\n╔════════════════════════════════════════════════════════╗${RESET}\n"
printf "${CYAN}${BOLD}║   Conduit Demo — Terminal + UI (Full walkthrough)      ║${RESET}\n"
printf "${CYAN}${BOLD}║                                                        ║${RESET}\n"
printf "${CYAN}${BOLD}║     Recording instructions at top of this script       ║${RESET}\n"
printf "${CYAN}${BOLD}╚════════════════════════════════════════════════════════╝${RESET}\n\n"

voice_cue "Let's create a new data pipeline project from scratch."
sleep 2

# Step 1: Initialize project
header "Step 1: Project Initialization"

run_cmd_with_cue \
    "conduit init ${PROJECT_NAME} && cd ${DEMO_DIR}" \
    "Creating a new Conduit project..." \
    2

# Step 2: Show project structure
header "Step 2: Project Structure"

voice_cue "Here's what was created: DAGs directory, configuration, and example pipelines."
printf "${CYAN}\$ ls -la${RESET}\n"
sleep 0.5
ls -la "$DEMO_DIR"
sleep 2

# Step 3: Show example DAG
header "Step 3: Example Python DAG"

voice_cue "Let's look at one of the example DAGs to understand the SDK."
printf "${CYAN}\$ cat dags/hello.py${RESET}\n"
sleep 0.5
cat "$DEMO_DIR/dags/hello.py"
sleep 3

# Step 4: Create a realistic ETL pipeline
header "Step 4: Create Production ETL Pipeline"

voice_cue "Now let's create a realistic daily sales ETL pipeline: extract from database, transform, load to warehouse."

cat > "$DEMO_DIR/dags/daily_sales.py" << 'PYEOF'
from conduit_sdk import dag, task
import time

@dag(schedule="0 6 * * *", tags=["sales", "etl"])
def daily_sales():
    """Daily sales data pipeline.

    Extracts sales data from PostgreSQL, applies transformations,
    and loads aggregated results to the data warehouse.
    """

    @task()
    def extract():
        """Extract raw sales transactions from PostgreSQL."""
        print("Extracting sales data from PostgreSQL...")
        print("Found 18,432 transactions")
        return {"rows": 18432, "source": "sales.transactions"}

    @task()
    def validate(raw):
        """Validate data quality."""
        print(f"Validating {raw['rows']} records...")
        print("Quality checks: schema ✓, nulls ✓, duplicates ✓")
        return raw

    @task()
    def transform(data):
        """Clean, deduplicate, and aggregate by region."""
        print(f"Transforming {data['rows']} records...")
        print("Deduplication removed 32 duplicates")
        print("Aggregating by region...")
        return {"rows": 18400, "regions": 47, "aggregated": True}

    @task()
    def load(transformed):
        """Load aggregated data to BigQuery."""
        print(f"Loading {transformed['rows']} rows to BigQuery...")
        print(f"Regions: {transformed['regions']}")
        print("Load complete. Table updated.")
        return {"status": "success", "table": "warehouse.sales_daily"}

    @task()
    def notify(result):
        """Send Slack notification to data team."""
        print("Sending notification to #data-team on Slack...")
        print(f"Status: {result['status'].upper()}")
        print("✓ Team notified")

    raw = extract()
    validated = validate(raw)
    transformed = transform(validated)
    result = load(transformed)
    notify(result)

if __name__ == "__main__":
    daily_sales()
PYEOF

printf "${CYAN}\$ cat > dags/daily_sales.py << 'EOF'${RESET}\n"
sleep 0.5
cat "$DEMO_DIR/dags/daily_sales.py"
sleep 2
printf "${CYAN}EOF${RESET}\n"
sleep 2

# Step 5: Compile
header "Step 5: Compile DAGs (Parse, no execution)"

voice_cue "Compile the DAGs — Conduit parses the Python AST without actually executing any code."
sleep 1

run_cmd_with_cue \
    "cd ${DEMO_DIR} && conduit compile" \
    "Running compilation..." \
    3

# Step 6: Plan
header "Step 6: Plan Deployment"

voice_cue "Create a deployment plan, like Terraform. Shows exactly what will change."
sleep 1

run_cmd_with_cue \
    "cd ${DEMO_DIR} && conduit plan" \
    "Planning deployment to production..." \
    3

# Step 7: Apply
header "Step 7: Apply Plan"

voice_cue "Apply the plan to deploy the DAGs to production."
sleep 1

run_cmd_with_cue \
    "cd ${DEMO_DIR} && conduit apply --auto-approve" \
    "Deploying changes..." \
    3

# Step 8: Run a DAG
header "Step 8: Execute DAG"

voice_cue "Let's run the daily_sales DAG now. Watch the task execution in real-time."
sleep 1

run_cmd_with_cue \
    "cd ${DEMO_DIR} && conduit run daily_sales" \
    "Executing the daily_sales DAG..." \
    4

# Step 9: Start the server
header "Step 9: Start Dashboard & API"

voice_cue "Start the Conduit server to view the dashboard and API."
sleep 1

printf "${CYAN}${BOLD}\$ conduit serve &${RESET}\n"
sleep 0.5

cd "$DEMO_DIR"
conduit serve --port "$PORT" > /tmp/conduit_server.log 2>&1 &
local server_pid=$!
echo "$server_pid" > "$SERVER_PID_FILE"

printf "${GREEN}✓ Server started (PID: ${server_pid})${RESET}\n"
sleep 3

# Verify server is ready
voice_cue "Waiting for the server to be ready..."
local max_attempts=30
local attempt=0
while ! curl -s "http://localhost:${PORT}/api/health" > /dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [[ $attempt -gt $max_attempts ]]; then
        printf "${CYAN}Server not responding after ${max_attempts} seconds${RESET}\n"
        break
    fi
    printf "."
    sleep 1
done
printf "\n"

# Step 10: Test API
header "Step 10: API Integration"

voice_cue "Let's test the API — trigger a DAG programmatically."
sleep 1

printf "${CYAN}\$ curl -X POST http://localhost:${PORT}/api/dags/daily_sales/run \\${RESET}\n"
printf "${CYAN}  -H 'Content-Type: application/json' \\${RESET}\n"
printf "${CYAN}  -d '{\"logical_date\": \"2026-03-23\"}' | jq${RESET}\n"
sleep 1

curl -s -X POST "http://localhost:${PORT}/api/dags/daily_sales/run" \
    -H "Content-Type: application/json" \
    -d '{"logical_date": "2026-03-23"}' | jq . || true
sleep 3

# Step 11: Show dashboard URL
header "Step 11: Open Dashboard"

voice_cue "The dashboard is now running. Open http://localhost:${PORT} in your browser."
sleep 1

printf "${BOLD}Dashboard URL:${RESET} ${GREEN}http://localhost:${PORT}${RESET}\n"
printf "${BOLD}API Docs:${RESET}      ${GREEN}http://localhost:${PORT}/api/docs${RESET}\n"
printf "\n"

voice_cue "The server is running. Open these URLs in your browser to explore:"
sleep 1
printf "  1. Main dashboard: ${GREEN}http://localhost:${PORT}${RESET}\n"
printf "  2. API documentation: ${GREEN}http://localhost:${PORT}/api/docs${RESET}\n"
printf "  3. DAGs list: ${GREEN}http://localhost:${PORT}/api/dags${RESET}\n"
sleep 3

# Step 12: Auto-open browser if possible
if command -v xdg-open &> /dev/null; then
    # Linux
    xdg-open "http://localhost:${PORT}" &
elif command -v open &> /dev/null; then
    # macOS
    open "http://localhost:${PORT}" &
fi

# Step 13: Final summary
header "Summary"

printf "\n${CYAN}${BOLD}✓ Accomplishments:${RESET}\n"
printf "  1. Initialized a new Conduit project\n"
printf "  2. Created a production ETL pipeline\n"
printf "  3. Compiled DAGs (AST parsing, no execution)\n"
printf "  4. Planned and applied deployment changes\n"
printf "  5. Executed a DAG with real task outputs\n"
printf "  6. Started the dashboard and API server\n"
printf "  7. Tested API integration\n"
printf "\n"

printf "${BOLD}Key Points:${RESET}\n"
printf "  • One binary. No external dependencies.\n"
printf "  • Compilation is ${BLUE}fast${RESET} (no Python execution)\n"
printf "  • Plan-and-apply workflow (like Terraform)\n"
printf "  • Virtual environments for safe testing\n"
printf "  • Built-in dashboard and OpenAPI docs\n"
printf "\n"

printf "${CYAN}${BOLD}═══════════════════════════════════════════════════════${RESET}\n\n"

voice_cue "The server is running. Press Ctrl+C to stop this script. The server will shut down automatically."
sleep 2

printf "${BOLD}Server is running. Press Enter to continue, or Ctrl+C to stop.${RESET}\n"
read -r

# Server will be cleaned up by the trap handler
