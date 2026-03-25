"""
Demo pipeline — a working DAG that exercises the scheduler end-to-end.
"""

import os
import sys

# Add the SDK to the path relative to this file's location
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'sdk', 'python'))

from conduit_sdk import dag, task


@dag(
    schedule="0 */4 * * *",
    tags=["demo", "etl", "working"],
)
def demo_pipeline():
    """Demo ETL pipeline that executes end-to-end."""

    @task(retries=1, retry_delay="5s")
    def extract_data():
        """Generate sample CSV data."""
        pass

    @task()
    def validate_data(raw):
        """Validate the extracted data."""
        pass

    @task()
    def transform_data(validated):
        """Aggregate the data."""
        pass

    @task()
    def load_report(transformed):
        """Generate a final report."""
        pass

    raw = extract_data()
    validated = validate_data(raw)
    transformed = transform_data(validated)
    load_report(transformed)


# Make individual task functions importable at module level
def extract_data():
    import os, json
    print("CONDUIT::LOG::INFO::Generating sample data...")
    os.makedirs("/tmp/conduit-demo", exist_ok=True)
    data = [
        {"id": 1, "name": "Alice", "amount": 150.00},
        {"id": 2, "name": "Bob", "amount": 230.50},
        {"id": 3, "name": "Charlie", "amount": 89.99},
    ]
    with open("/tmp/conduit-demo/raw.json", "w") as f:
        json.dump(data, f)
    print(f"CONDUIT::LOG::INFO::Extracted {len(data)} rows")
    print(f"CONDUIT::METRIC::rows_extracted::{len(data)}")
    print("CONDUIT::PROGRESS::100")

def validate_data():
    import json
    print("CONDUIT::LOG::INFO::Validating data...")
    with open("/tmp/conduit-demo/raw.json") as f:
        data = json.load(f)
    assert all("id" in r and "amount" in r for r in data), "Schema mismatch"
    print(f"CONDUIT::LOG::INFO::Validated {len(data)} rows — schema OK")
    print("CONDUIT::PROGRESS::100")

def transform_data():
    import json
    print("CONDUIT::LOG::INFO::Transforming data...")
    with open("/tmp/conduit-demo/raw.json") as f:
        data = json.load(f)
    total = sum(r["amount"] for r in data)
    result = {"total_revenue": total, "num_records": len(data)}
    with open("/tmp/conduit-demo/summary.json", "w") as f:
        json.dump(result, f)
    print(f"CONDUIT::LOG::INFO::Aggregated: total=${total:.2f}")
    print(f'CONDUIT::XCOM::{{"total_revenue": {total}}}')
    print("CONDUIT::PROGRESS::100")

def load_report():
    import json
    print("CONDUIT::LOG::INFO::Loading report...")
    with open("/tmp/conduit-demo/summary.json") as f:
        summary = json.load(f)
    report = f"=== Pipeline Report ===\nTotal Revenue: ${summary['total_revenue']:.2f}\nRecords: {summary['num_records']}"
    with open("/tmp/conduit-demo/report.txt", "w") as f:
        f.write(report)
    print(report)
    print("CONDUIT::LOG::INFO::Report saved to /tmp/conduit-demo/report.txt")
    print(f"CONDUIT::METRIC::total_revenue::{summary['total_revenue']}")
    print("CONDUIT::PROGRESS::100")
