#!/usr/bin/env bash
# Conduit Compilation Benchmark
# Usage: ./scripts/benchmark.sh [--quick] [--yaml]

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release/conduit"
GEN="$ROOT/scripts/gen_dags.py"

QUICK=false
YAML_FLAG=""
SIZES=(10 100 500 1000)

for arg in "$@"; do
    case $arg in
        --quick) QUICK=true; SIZES=(10 100) ;;
        --yaml)  YAML_FLAG="--yaml" ;;
    esac
done

# Build if needed
if [ ! -f "$BIN" ]; then
    echo "Building release binary..."
    (cd "$ROOT" && cargo build --release --bin conduit)
fi

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║  Conduit Compilation Benchmark                  ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

printf "%-8s  %-8s  %-8s  %-8s  %-8s  %-8s\n" "DAGs" "Tasks" "Min" "Mean" "Median" "Max"
printf "%-8s  %-8s  %-8s  %-8s  %-8s  %-8s\n" "────" "─────" "───" "────" "──────" "───"

RESULTS="["
FIRST=true

for count in "${SIZES[@]}"; do
    dir=$(mktemp -d)

    # Generate DAGs using Python (fast)
    python3 "$GEN" "$dir" "$count" $YAML_FLAG

    tasks=$((count * 4))
    times=()

    # Run 5 iterations
    for run in 1 2 3 4 5; do
        start_s=$(python3 -c "import time; print(int(time.time()*1000))")
        "$BIN" compile "$dir" > /dev/null 2>&1 || true
        end_s=$(python3 -c "import time; print(int(time.time()*1000))")
        ms=$((end_s - start_s))
        times+=($ms)
    done

    rm -rf "$dir"

    # Stats
    IFS=$'\n' sorted=($(sort -n <<< "${times[*]}")); unset IFS
    min=${sorted[0]}
    max=${sorted[${#sorted[@]}-1]}
    median=${sorted[2]}
    sum=0; for t in "${times[@]}"; do sum=$((sum + t)); done
    mean=$((sum / ${#times[@]}))

    printf "%-8d  %-8d  %5dms   %5dms   %5dms   %5dms\n" "$count" "$tasks" "$min" "$mean" "$median" "$max"

    # JSON
    $FIRST || RESULTS+=","
    FIRST=false
    RESULTS+="{\"dags\":$count,\"tasks\":$tasks,\"min_ms\":$min,\"mean_ms\":$mean,\"median_ms\":$median,\"max_ms\":$max}"
done

RESULTS+="]"

# Save results
mkdir -p "$ROOT/benchmarks"
echo "$RESULTS" | python3 -m json.tool > "$ROOT/benchmarks/results.json"

echo ""
echo "Results saved to benchmarks/results.json"
echo ""

# Summary
fastest_min=$(echo "$RESULTS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(min(r['min_ms'] for r in d))")
largest=$(echo "$RESULTS" | python3 -c "import sys,json; d=json.load(sys.stdin); r=d[-1]; print(f\"{r['dags']} DAGs ({r['tasks']} tasks) in {r['median_ms']}ms\")")
echo "Fastest: ${fastest_min}ms (smallest set)"
echo "Largest: $largest"
