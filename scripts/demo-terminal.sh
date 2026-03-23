#!/bin/bash
# Demo recording script for Conduit — terminal-only version
#
# This script simulates a realistic Conduit workflow with typed commands and pauses,
# designed to be recorded with asciinema for blog/GitHub documentation.
#
# To record this demo:
#   brew install asciinema
#   asciinema rec demo.cast -c ./scripts/demo-terminal.sh
#
# To convert to GIF:
#   npm install -g svg-term-cli
#   svg-term --in demo.cast --out demo.svg --window
#   # OR
#   pip install asciinema-agg
#   agg demo.cast demo.gif
#
# To convert to MP4 with ffmpeg:
#   ffmpeg -i demo.cast -y demo.mp4

set -e
trap 'echo' EXIT

# Colors and styling
readonly CYAN='\033[1;36m'
readonly BLUE='\033[1;34m'
readonly GREEN='\033[0;32m'
readonly GREY='\033[90m'
readonly RESET='\033[0m'
readonly BOLD='\033[1m'
readonly DIM='\033[2m'

# Set up the prompt
export PS1="${CYAN}conduit-demo${RESET}:${BLUE}~${RESET}$ "

# ─── Helper functions ────────────────────────────────────────────────────────

# Type a command character by character with a realistic delay
# Usage: type_cmd "conduit init my_project"
type_cmd() {
    local cmd="$1"
    local delay_min=30
    local delay_max=80

    # Print the command being typed
    for ((i = 0; i < ${#cmd}; i++)); do
        printf '%s' "${cmd:$i:1}"
        # Random delay between keystrokes (in milliseconds)
        local delay=$((RANDOM % (delay_max - delay_min + 1) + delay_min))
        sleep "0.$(printf '%03d' "$delay")"
    done

    printf '\n'
}

# Pause for dramatic effect
# Usage: pause [seconds]
pause() {
    local duration="${1:-2}"
    sleep "$duration"
}

# Print a dimmed narrator comment
# Usage: comment "This is a helpful comment"
comment() {
    printf "${GREY}# %s${RESET}\n" "$1"
    pause 1
}

# Print simulated output with color
# Usage: output "message" [color]
output() {
    local text="$1"
    local color="${2:-$RESET}"
    printf "${color}%s${RESET}\n" "$text"
}

# Clear screen and show title
clear_screen() {
    clear
    printf "${CYAN}${BOLD}╔════════════════════════════════════════════════════════╗${RESET}\n"
    printf "${CYAN}${BOLD}║          Conduit Data Pipeline Orchestrator            ║${RESET}\n"
    printf "${CYAN}${BOLD}║       From zero to running in under 60 seconds         ║${RESET}\n"
    printf "${CYAN}${BOLD}╚════════════════════════════════════════════════════════╝${RESET}\n"
    printf "\n"
    pause 1
}

# ─── Demo flow ───────────────────────────────────────────────────────────────

clear_screen

# Step 1: Initialize project
comment "Let's create a new project from scratch"
type_cmd "conduit init etl_demo"
pause 0.5

output "Creating project structure..."
pause 0.3
output "✓ dags/" "${GREEN}"
pause 0.2
output "✓ .conduit/" "${GREEN}"
pause 0.2
output "✓ conduit.yaml (project config)" "${GREEN}"
pause 0.2
output "✓ dags/hello.py (example DAG)" "${GREEN}"
pause 0.2
output "✓ dags/hello.yaml (example YAML DAG)" "${GREEN}"
pause 0.2
output "✓ .gitignore" "${GREEN}"
pause 0.3

output ""
output "Project initialized. Run 'conduit compile' to get started." "${GREEN}"
pause 2

# Step 2: Examine example
comment "Let's look at one of the example DAGs"
type_cmd "cat etl_demo/dags/hello.py"
pause 0.5

output "from conduit_sdk import dag, task"
output ""
output "@dag(schedule=\"0 9 * * *\", tags=[\"example\"])"
output "def hello_world():"
output "    \"\"\"A simple example DAG.\"\"\""
output ""
output "    @task()"
output "    def greet():"
output "        print(\"Hello, Conduit!\")"
output ""
output "    @task()"
output "    def farewell(greeting=greet()):"
output "        print(\"Goodbye!\")"
output ""
output "    greet()"
output "    farewell()"
pause 2

# Step 3: Navigate to project
comment "Now let's move into the project and add a real ETL pipeline"
type_cmd "cd etl_demo"
pause 1

# Step 4: Create a more realistic DAG
comment "Creating a sales ETL pipeline"
type_cmd "cat > dags/daily_sales.py << 'EOF'"
pause 0.3

output "from conduit_sdk import dag, task"
output ""
output "@dag(schedule=\"0 6 * * *\", tags=[\"sales\", \"warehouse\"])"
output "def daily_sales():"
output "    \"\"\"Daily sales pipeline — extract, transform, load.\"\"\""
output ""
output "    @task()"
output "    def extract():"
output "        \"\"\"Fetch raw data from Postgres.\"\"\""
output "        return {\"rows\": 15234}"
output ""
output "    @task()"
output "    def transform(raw):"
output "        \"\"\"Clean and aggregate by region.\"\"\""
output "        return {\"rows\": 15200, \"regions\": 42}"
output ""
output "    @task()"
output "    def load(data):"
output "        \"\"\"Write aggregates to warehouse.\"\"\""
output "        print(f\"Loaded {data['rows']} rows to BigQuery\")"
output ""
output "    @task()"
output "    def notify(status=load()):"
output "        \"\"\"Send Slack notification.\"\"\""
output "        print(\"✓ Pipeline complete. Notified data team.\")"
output ""
output "    raw = extract()"
output "    clean = transform(raw)"
output "    load(clean)"
output "    notify()"
output "EOF"
pause 1

output ""
output "Pipeline created." "${GREEN}"
pause 1.5

# Step 5: Compile
comment "Compile the DAGs — parse Python AST, no execution"
type_cmd "conduit compile"
pause 1

output "Compiling dags/..."
pause 0.5
output "✓ daily_sales (4 tasks: extract → transform → load → notify)" "${GREEN}"
pause 0.3
output "✓ hello_world (2 tasks: greet → farewell)" "${GREEN}"
pause 0.3

output ""
output "Compiled 2 DAGs, 6 total tasks in ${BOLD}28ms${RESET}" "${GREEN}"
pause 0.5
output "✓ No compilation errors" "${GREEN}"
pause 2

# Step 6: Plan
comment "Plan the deployment — Terraform-style diff"
type_cmd "conduit plan"
pause 0.8

output "Plan for environment: production"
output ""
output "New DAGs to deploy:"
output "  + daily_sales (4 tasks: extract → transform → load → notify)"
output "  + hello_world (2 tasks: greet → farewell)"
output ""
output "Changes: 2 added, 0 modified, 0 deleted"
output ""
output "Run 'conduit apply' to deploy these changes."
pause 2

# Step 7: Create staging environment
comment "Let's test in a staging environment first"
type_cmd "conduit env create staging"
pause 0.8

output "✓ Created environment: staging" "${GREEN}"
pause 2

# Step 8: Run in staging
comment "Execute one DAG in staging to verify correctness"
type_cmd "conduit run daily_sales --env staging"
pause 0.8

output "Executing daily_sales (logical date: 2026-03-23)..."
pause 0.3
output ""
output "extract [0/4]" "${BOLD}"
pause 0.4
output "  ✓ Output: {\"rows\": 15234} (${BOLD}12ms${RESET})" "${GREEN}"
pause 0.5
output ""
output "transform [1/4]" "${BOLD}"
pause 0.5
output "  ✓ Output: {\"rows\": 15200, \"regions\": 42} (${BOLD}18ms${RESET})" "${GREEN}"
pause 0.5
output ""
output "load [2/4]" "${BOLD}"
pause 0.4
output "  ✓ Loaded 15200 rows to BigQuery (${BOLD}8ms${RESET})" "${GREEN}"
pause 0.5
output ""
output "notify [3/4]" "${BOLD}"
pause 0.4
output "  ✓ ✓ Pipeline complete. Notified data team. (${BOLD}3ms${RESET})" "${GREEN}"
pause 0.3
output ""
output "Execution summary:" "${BOLD}"
pause 0.3
output "  Status: ${GREEN}SUCCESS${RESET}"
output "  Duration: 41ms"
output "  Tasks: 4/4 passed"
output "  Snapshot: snap_001_prod_2026-03-23"
pause 2

# Step 9: Promote to production
comment "Everything looks good. Promote staging to production"
type_cmd "conduit env promote staging production"
pause 1

output "✓ Promoted 2 DAGs from staging to production" "${GREEN}"
pause 1.5

# Step 10: Start the server
comment "Start the dashboard — view all pipelines and their execution history"
type_cmd "conduit serve &"
pause 1

output "Starting Conduit server..."
pause 0.4
output "✓ Bound to http://0.0.0.0:8080" "${GREEN}"
pause 0.3
output "✓ Loaded 2 DAGs (6 tasks)" "${GREEN}"
pause 0.3
output "✓ Loaded 2 environments (production, staging)" "${GREEN}"
pause 0.3
output ""
output "Open ${BOLD}http://localhost:8080${RESET} to view your pipelines."
pause 0.5
output "API docs: ${BOLD}http://localhost:8080/api/docs${RESET}"
pause 2

# Step 11: Summary
printf "\n"
comment "From zero to production in under 60 seconds"

output ""
output "${CYAN}✓ Initialized project" "${GREEN}"
output "${CYAN}✓ Created ETL pipeline" "${GREEN}"
output "${CYAN}✓ Compiled without executing Python" "${GREEN}"
output "${CYAN}✓ Planned changes (Terraform-style)" "${GREEN}"
output "${CYAN}✓ Tested in staging environment" "${GREEN}"
output "${CYAN}✓ Promoted to production" "${GREEN}"
output "${CYAN}✓ Started dashboard and API" "${GREEN}"

printf "\n"
output "${BOLD}One binary. No database. No message broker. No YAML hell.${RESET}"
pause 3

output ""
output "Next: conduit --help, or visit https://conduit.dev/docs"
pause 1
