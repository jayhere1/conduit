#!/usr/bin/env python3
"""
Test fixtures and example data for conduit-python bindings.

This module provides sample JSON payloads and data structures
that demonstrate the expected formats for the binding APIs.
"""

import json

# ============================================================================
# DAG Compilation Fixtures
# ============================================================================

SAMPLE_DAG_DEFINITION = '''
from conduit import DAG, Task

dag = DAG(
    dag_id="analytics_pipeline",
    description="Daily analytics processing"
)

# Extract task
extract = Task(
    task_id="extract_raw_data",
    task_type="sql_execute",
    sql="SELECT * FROM raw.events WHERE date = '{{ ds }}'"
)

# Transform task
transform = Task(
    task_id="transform_events",
    task_type="sql_execute",
    sql="SELECT event_id, user_id, COUNT(*) FROM raw.events GROUP BY event_id, user_id"
)
transform.depends_on(extract)

# Load task
load = Task(
    task_id="load_analytics",
    task_type="sql_execute",
    sql="INSERT INTO analytics.events_agg SELECT * FROM staging.events_agg"
)
load.depends_on(transform)

dag.add_task(extract)
dag.add_task(transform)
dag.add_task(load)
'''

COMPILED_PLAN = {
    "dags": [
        {
            "id": "analytics_pipeline",
            "name": "analytics_pipeline",
            "description": "Daily analytics processing",
            "tasks": [
                {
                    "id": "extract_raw_data",
                    "type": "sql_execute",
                    "dependencies": [],
                    "config": {
                        "sql": "SELECT * FROM raw.events WHERE date = '{{ ds }}'"
                    },
                    "trigger_rule": "AllSuccess",
                    "pool": {
                        "name": "default",
                        "slots": 5
                    }
                },
                {
                    "id": "transform_events",
                    "type": "sql_execute",
                    "dependencies": [
                        {
                            "task_id": "extract_raw_data",
                            "kind": "FinishOnSuccess"
                        }
                    ],
                    "config": {
                        "sql": "SELECT event_id, user_id, COUNT(*) FROM raw.events GROUP BY event_id, user_id"
                    },
                    "trigger_rule": "AllSuccess",
                    "pool": {
                        "name": "default",
                        "slots": 5
                    }
                },
                {
                    "id": "load_analytics",
                    "type": "sql_execute",
                    "dependencies": [
                        {
                            "task_id": "transform_events",
                            "kind": "FinishOnSuccess"
                        }
                    ],
                    "config": {
                        "sql": "INSERT INTO analytics.events_agg SELECT * FROM staging.events_agg"
                    },
                    "trigger_rule": "AllSuccess",
                    "pool": {
                        "name": "default",
                        "slots": 5
                    }
                }
            ]
        }
    ]
}

VALIDATION_RESULT_VALID = {
    "valid": True,
    "errors": [],
    "warnings": [],
    "dags_compiled": 1
}

VALIDATION_RESULT_WITH_ERRORS = {
    "valid": False,
    "errors": [
        "Cycle detected in DAG 'pipeline_dag': extract_raw_data -> transform -> extract_raw_data",
        "Unknown task reference 'nonexistent_task' in DAG 'pipeline_dag'"
    ],
    "warnings": [
        "Task 'load_data' in DAG 'pipeline_dag' has no dependencies"
    ],
    "dags_compiled": 0
}

# ============================================================================
# Fingerprinting Fixtures
# ============================================================================

FINGERPRINTS = {
    "fingerprints": {
        "extract_raw_data": {
            "hash": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "computed_at": "2025-03-22T10:00:00Z",
            "version": 1
        },
        "transform_events": {
            "hash": "sha256:da39a3ee5e6b4b0d3255bfef95601890afd80709f7ea5d0b8c1d5c8d8d1d5c8d",
            "computed_at": "2025-03-22T10:00:00Z",
            "version": 1
        },
        "load_analytics": {
            "hash": "sha256:2c26b46911185131006ba239b665d4614635335e5ac6dd94793e8df3b7e9e0bf",
            "computed_at": "2025-03-22T10:00:00Z",
            "version": 1
        }
    },
    "computed_at": "2025-03-22T10:00:00Z"
}

# ============================================================================
# Change Detection Fixtures
# ============================================================================

ENVIRONMENT_SNAPSHOT = {
    "name": "prod",
    "created_at": "2025-03-15T09:00:00Z",
    "fingerprints": {
        "extract_raw_data": {
            "hash": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        },
        "transform_events": {
            "hash": "sha256:da39a3ee5e6b4b0d3255bfef95601890afd80709f7ea5d0b8c1d5c8d8d1d5c8d"
        },
        "old_load_task": {
            "hash": "sha256:3c26b46911185131006ba239b665d4614635335e5ac6dd94793e8df3b7e9e0bf"
        }
    },
    "snapshots": {
        "snapshot_v1": {
            "timestamp": "2025-03-15T09:30:00Z",
            "tasks": {
                "extract_raw_data": {"status": "success", "duration": 120},
                "transform_events": {"status": "success", "duration": 300},
                "old_load_task": {"status": "success", "duration": 180}
            }
        }
    }
}

CHANGE_DETECTION_RESULT = {
    "changes": {
        "added": [
            {
                "task_id": "load_analytics",
                "hash": "sha256:2c26b46911185131006ba239b665d4614635335e5ac6dd94793e8df3b7e9e0bf"
            }
        ],
        "modified": [
            {
                "task_id": "transform_events",
                "previous_hash": "sha256:da39a3ee5e6b4b0d3255bfef95601890afd80709f7ea5d0b8c1d5c8d8d1d5c8d",
                "current_hash": "sha256:5c26b46911185131006ba239b665d4614635335e5ac6dd94793e8df3b7e9e0bf"
            }
        ],
        "removed": [
            {
                "task_id": "old_load_task"
            }
        ],
        "upstream_invalidated": [
            {
                "task_id": "transform_events",
                "reason": "upstream_modified"
            }
        ]
    },
    "summary": {
        "total_added": 1,
        "total_modified": 1,
        "total_removed": 1
    }
}

# ============================================================================
# Lineage Extraction Fixtures
# ============================================================================

SQL_QUERY = """
    SELECT
        c.customer_id,
        c.customer_name,
        COUNT(o.order_id) as order_count,
        SUM(o.order_amount) as total_amount,
        AVG(o.order_amount) as avg_order_value
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    WHERE c.created_date >= '2025-01-01'
    GROUP BY c.customer_id, c.customer_name
"""

LINEAGE_RESULT = {
    "sql": SQL_QUERY,
    "input_tables": ["customers", "orders"],
    "output_columns": [
        {
            "name": "customer_id",
            "type": "BIGINT",
            "sources": ["customers.customer_id"]
        },
        {
            "name": "customer_name",
            "type": "STRING",
            "sources": ["customers.customer_name"]
        },
        {
            "name": "order_count",
            "type": "INT",
            "sources": ["orders.order_id"]
        },
        {
            "name": "total_amount",
            "type": "DECIMAL",
            "sources": ["orders.order_amount"]
        },
        {
            "name": "avg_order_value",
            "type": "DECIMAL",
            "sources": ["orders.order_amount"]
        }
    ],
    "column_dependencies": [
        {"output": "customer_id", "sources": ["customers.customer_id"]},
        {"output": "customer_name", "sources": ["customers.customer_name"]},
        {"output": "order_count", "sources": ["orders.order_id"]},
        {"output": "total_amount", "sources": ["orders.order_amount"]},
        {"output": "avg_order_value", "sources": ["orders.order_amount"]}
    ]
}

LINEAGE_EDGES = [
    {
        "from_task": "extract_customers",
        "from_column": "customer_id",
        "to_task": "join_customer_orders",
        "to_column": "customer_id"
    },
    {
        "from_task": "extract_customers",
        "from_column": "customer_name",
        "to_task": "join_customer_orders",
        "to_column": "customer_name"
    },
    {
        "from_task": "extract_orders",
        "from_column": "order_id",
        "to_task": "join_customer_orders",
        "to_column": "order_id"
    },
    {
        "from_task": "extract_orders",
        "from_column": "order_amount",
        "to_task": "agg_orders",
        "to_column": "amount"
    },
    {
        "from_task": "join_customer_orders",
        "from_column": "customer_id",
        "to_task": "agg_orders",
        "to_column": "customer_id"
    }
]

COLUMN_TRACE_RESULT = {
    "direction": "upstream",
    "start_column": "agg_orders.customer_id",
    "trace_path": [
        {"task_id": "agg_orders", "column_name": "customer_id"},
        {"task_id": "join_customer_orders", "column_name": "customer_id"},
        {"task_id": "extract_customers", "column_name": "customer_id"}
    ],
    "path_length": 3
}

# ============================================================================
# Schema Change Detection Fixtures
# ============================================================================

OLD_SCHEMA = {
    "columns": [
        {
            "name": "id",
            "type": "BIGINT",
            "nullable": False
        },
        {
            "name": "name",
            "type": "STRING",
            "nullable": False
        },
        {
            "name": "email",
            "type": "STRING",
            "nullable": True
        },
        {
            "name": "age",
            "type": "INT",
            "nullable": True
        }
    ]
}

NEW_SCHEMA = {
    "columns": [
        {
            "name": "id",
            "type": "BIGINT",
            "nullable": False
        },
        {
            "name": "name",
            "type": "STRING",
            "nullable": False
        },
        {
            "name": "email",
            "type": "STRING",
            "nullable": False  # Changed from nullable: True
        },
        {
            "name": "phone",
            "type": "STRING",
            "nullable": True
        },
        {
            "name": "created_at",
            "type": "TIMESTAMP",
            "nullable": False
        }
        # age column removed
    ]
}

SCHEMA_DIFF_RESULT = {
    "added_columns": [
        {
            "name": "phone",
            "type": "STRING",
            "nullable": True
        },
        {
            "name": "created_at",
            "type": "TIMESTAMP",
            "nullable": False
        }
    ],
    "removed_columns": [
        {
            "name": "age",
            "type": "INT"
        }
    ],
    "modified_columns": [
        {
            "name": "email",
            "old_type": "STRING",
            "new_type": "STRING"
        }
    ],
    "breaking_changes": [
        {
            "column": "age",
            "change": "removed",
            "reason": "downstream tasks may depend on this column"
        },
        {
            "column": "email",
            "change": "made_non_nullable",
            "reason": "may contain null values"
        }
    ],
    "is_breaking": True
}

# ============================================================================
# Environment Management Fixtures
# ============================================================================

ENVIRONMENT_DEV = {
    "name": "dev",
    "created_at": "2025-03-20T08:00:00Z",
    "snapshots": {},
    "fingerprints": {},
    "metadata": {"owner": "data-team", "retention_days": 7}
}

ENVIRONMENT_STAGING = {
    "name": "staging",
    "created_at": "2025-03-20T08:30:00Z",
    "created_from": "dev",
    "snapshots": {},
    "fingerprints": {},
    "metadata": {"owner": "data-team", "retention_days": 14}
}

ENVIRONMENT_PROD = {
    "name": "prod",
    "created_at": "2025-03-20T09:00:00Z",
    "created_from": "staging",
    "promoted_from": "staging",
    "promoted_at": "2025-03-22T10:00:00Z",
    "snapshots": {
        "snapshot_v1": {
            "timestamp": "2025-03-22T10:00:00Z",
            "tasks": {
                "extract_raw_data": {"status": "success", "duration": 120},
                "transform_events": {"status": "success", "duration": 300},
                "load_analytics": {"status": "success", "duration": 180}
            }
        }
    },
    "fingerprints": FINGERPRINTS["fingerprints"],
    "metadata": {"owner": "data-team", "retention_days": 90}
}

ENVIRONMENT_LIST = {
    "environments": [
        {
            "name": "dev",
            "created_at": "2025-03-20T08:00:00Z",
            "created_from": None,
            "snapshot_count": 0,
            "metadata": {"owner": "data-team"}
        },
        {
            "name": "staging",
            "created_at": "2025-03-20T08:30:00Z",
            "created_from": "dev",
            "snapshot_count": 0,
            "metadata": {"owner": "data-team"}
        },
        {
            "name": "prod",
            "created_at": "2025-03-20T09:00:00Z",
            "created_from": "staging",
            "snapshot_count": 1,
            "metadata": {"owner": "data-team"}
        }
    ],
    "total": 3
}

# ============================================================================
# Helper Functions
# ============================================================================

def print_json(obj, title=None):
    """Pretty-print a JSON object."""
    if title:
        print(f"\n{title}")
        print("=" * len(title))
    print(json.dumps(obj, indent=2))


def load_fixture(fixture_dict):
    """Convert fixture to JSON string."""
    return json.dumps(fixture_dict)


if __name__ == "__main__":
    # Print all fixtures for reference
    print("=" * 70)
    print("CONDUIT-PYTHON TEST FIXTURES")
    print("=" * 70)

    print_json(COMPILED_PLAN, "Compiled Plan")
    print_json(VALIDATION_RESULT_VALID, "Validation Result (Valid)")
    print_json(FINGERPRINTS, "Fingerprints")
    print_json(CHANGE_DETECTION_RESULT, "Change Detection Result")
    print_json(LINEAGE_RESULT, "Lineage Result")
    print_json(COLUMN_TRACE_RESULT, "Column Trace Result")
    print_json(SCHEMA_DIFF_RESULT, "Schema Diff Result")
    print_json(ENVIRONMENT_LIST, "Environment List")

    print("\n" + "=" * 70)
    print("✓ All fixtures loaded successfully")
