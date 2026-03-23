#!/usr/bin/env python3
"""
Example: Complete plan/apply workflow using conduit-python bindings.

This demonstrates the full lifecycle of Conduit operations:
1. Compile DAGs from Python definitions
2. Compute fingerprints
3. Detect changes
4. Manage virtual environments
5. Promote changes through environments
"""

import json
import sys
from pathlib import Path

try:
    from conduit_native import compiler, planner, lineage
    from conduit_native.state import EnvironmentStore
except ImportError:
    print("Error: conduit_native module not found.")
    print("Build with: maturin develop")
    sys.exit(1)


def compile_dags(dag_path: str):
    """Compile DAGs from file or directory."""
    print(f"Compiling DAGs from: {dag_path}")
    try:
        plan_json = compiler.compile_dags(dag_path)
        plan = json.loads(plan_json)
        print(f"✓ Successfully compiled {len(plan['dags'])} DAG(s)")
        return plan_json, plan
    except ValueError as e:
        print(f"✗ Compilation failed: {e}")
        sys.exit(1)


def validate_dags(dag_path: str):
    """Validate DAGs and report issues."""
    print(f"Validating DAGs from: {dag_path}")
    try:
        result_json = compiler.validate_dag(dag_path)
        result = json.loads(result_json)

        if result['valid']:
            print(f"✓ All {result['dags_compiled']} DAG(s) are valid")
        else:
            print(f"✗ Validation failed with {len(result['errors'])} error(s)")
            for err in result['errors']:
                print(f"  - {err}")

        if result['warnings']:
            for warn in result['warnings']:
                print(f"  ⚠ {warn}")

        return result
    except ValueError as e:
        print(f"✗ Validation failed: {e}")
        sys.exit(1)


def compute_fingerprints(plan_json: str):
    """Compute fingerprints for all tasks."""
    print("Computing task fingerprints...")
    try:
        fps_json = planner.compute_fingerprints(plan_json)
        fps = json.loads(fps_json)
        print(f"✓ Computed fingerprints for {len(fps['fingerprints'])} task(s)")
        return fps_json, fps
    except ValueError as e:
        print(f"✗ Fingerprinting failed: {e}")
        sys.exit(1)


def detect_changes(plan_json: str, env_json: str):
    """Detect changes between plan and environment."""
    print("Detecting changes...")
    try:
        changes_json = planner.detect_changes(plan_json, env_json)
        changes = json.loads(changes_json)

        summary = changes['summary']
        print(f"✓ Change detection complete:")
        print(f"  - Added: {summary['total_added']}")
        print(f"  - Modified: {summary['total_modified']}")
        print(f"  - Removed: {summary['total_removed']}")

        return changes_json, changes
    except ValueError as e:
        print(f"✗ Change detection failed: {e}")
        sys.exit(1)


def manage_environments(state_path: str):
    """Demonstrate environment management."""
    print(f"Initializing environment store at: {state_path}")

    store = EnvironmentStore(state_path)

    # Create environments
    print("Creating environments...")
    store.create_env("dev")
    store.create_env("staging", based_on="dev")
    store.create_env("prod", based_on="staging")
    print("✓ Created dev, staging, prod environments")

    # List environments
    list_json = store.list_envs()
    envs = json.loads(list_json)
    print(f"✓ Total environments: {envs['total']}")

    # Add a snapshot to dev
    snapshot_data = json.dumps({
        "timestamp": "2025-03-22T10:00:00Z",
        "tasks": {
            "task1": {"status": "success", "duration": 45}
        }
    })
    store.add_snapshot("dev", "snapshot_v1", snapshot_data)
    print("✓ Added snapshot to dev")

    # Promote dev → staging
    print("Promoting dev → staging...")
    store.promote("dev", "staging")
    print("✓ Promotion complete")

    # Save state
    store.save()
    print(f"✓ State saved to disk")

    return store


def analyze_lineage(sql_query: str):
    """Demonstrate SQL lineage extraction."""
    print(f"Analyzing SQL lineage...")
    try:
        lineage_json = lineage.extract_sql_lineage(sql_query)
        lineage_info = json.loads(lineage_json)

        print(f"✓ Lineage extracted:")
        print(f"  - Input tables: {', '.join(lineage_info['input_tables'])}")
        print(f"  - Output columns: {len(lineage_info['output_columns'])}")

        return lineage_json, lineage_info
    except ValueError as e:
        print(f"✗ Lineage extraction failed: {e}")
        sys.exit(1)


def main():
    """Run the complete example workflow."""
    print("=" * 60)
    print("Conduit Python Bindings: Plan/Apply Workflow Example")
    print("=" * 60)
    print()

    # Configuration
    dag_path = "./dags"  # In real usage, point to actual DAG directory
    state_path = "./conduit_state"

    # Step 1: Validate DAGs
    print("STEP 1: Validate DAGs")
    print("-" * 60)
    validate_dags(dag_path)
    print()

    # Step 2: Compile DAGs
    print("STEP 2: Compile DAGs")
    print("-" * 60)
    plan_json, plan = compile_dags(dag_path)
    print()

    # Step 3: Compute fingerprints
    print("STEP 3: Compute Fingerprints")
    print("-" * 60)
    fps_json, fps = compute_fingerprints(plan_json)
    print()

    # Step 4: Manage environments
    print("STEP 4: Manage Environments")
    print("-" * 60)
    store = manage_environments(state_path)
    print()

    # Step 5: Detect changes (use current environment as baseline)
    print("STEP 5: Detect Changes")
    print("-" * 60)
    current_env_json = store.get_env("prod")
    changes_json, changes = detect_changes(plan_json, current_env_json)
    print()

    # Step 6: Analyze lineage (example with a sample query)
    print("STEP 6: Analyze Lineage")
    print("-" * 60)
    sample_sql = """
        SELECT
            customer_id,
            COUNT(*) as order_count,
            SUM(amount) as total_amount
        FROM orders
        WHERE created_date >= '2025-01-01'
        GROUP BY customer_id
    """
    lineage_json, lineage_info = analyze_lineage(sample_sql)
    print()

    # Summary
    print("=" * 60)
    print("Workflow Summary")
    print("=" * 60)
    print(f"DAGs compiled: {len(plan['dags'])}")
    print(f"Tasks fingerprinted: {len(fps['fingerprints'])}")
    print(f"Environments created: 3 (dev, staging, prod)")
    print(f"Changes detected: {changes['summary']['total_added'] + changes['summary']['total_modified'] + changes['summary']['total_removed']}")
    print()
    print("✓ Example workflow completed successfully!")


if __name__ == "__main__":
    main()
