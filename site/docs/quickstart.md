# Getting Started in 60 Seconds

From zero to running pipeline in under a minute. No database. No message broker. No YAML hell.

## Step 1: Install

```bash
curl -sSL https://install.conduit.dev | sh
```

One binary. Nothing else to install.

## Step 2: Create a project

```bash
conduit init my_project
cd my_project
```

Expected output:
```
Creating project structure...
✓ dags/
✓ .conduit/
✓ conduit.yaml (project config)
✓ dags/hello.py (example DAG)
✓ dags/hello.yaml (example YAML DAG)
✓ .gitignore

Project initialized. Run 'conduit compile' to get started.
```

Your project structure:
```
my_project/
├── dags/
│   ├── hello.py          # Example Python DAG
│   └── hello.yaml        # Example YAML DAG
├── .conduit/             # State and environments (local)
├── conduit.yaml          # Project config
└── .gitignore
```

## Step 3: Write your first DAG

Replace `dags/hello.py` with this ETL pipeline:

```python
from conduit_sdk import dag, task

@dag(schedule="0 6 * * *", tags=["etl"])
def daily_etl():
    """Extract, transform, load daily data."""

    @task()
    def extract():
        """Fetch raw data from source."""
        return {"rows": 1000}

    @task()
    def transform(raw):
        """Clean and denormalize."""
        return {"rows": 950, "clean": True}

    @task()
    def load(data):
        """Write to warehouse."""
        print(f"Loaded {data['rows']} rows")

    raw = extract()
    clean = transform(raw)
    load(clean)
```

Or use YAML if you prefer declarative:

```yaml
id: daily_etl
description: Extract, transform, load daily data
schedule: "0 6 * * *"
tags: [etl]

tasks:
  extract:
    type: shell
    command: 'echo "extracting data" && echo "1000 rows"'

  transform:
    type: shell
    command: 'echo "transforming" && echo "950 rows"'
    depends_on: [extract]

  load:
    type: shell
    command: 'echo "loading 950 rows to warehouse"'
    depends_on: [transform]
```

## Step 4: Compile

```bash
conduit compile
```

Expected output:
```
Compiling dags/...
✓ daily_etl (3 tasks)
✓ hello_world (2 tasks)

Compiled 2 DAGs, 5 total tasks in 32ms
✓ No compilation errors
```

**What it means:** Parsed your Python DAGs without executing them. Generated task graph and resolved dependencies.

## Step 5: Plan

```bash
conduit plan
```

Expected output:
```
Plan for environment: production

New DAGs to deploy:
  + daily_etl (3 tasks: extract → transform → load)
  + hello_world (2 tasks: greet → farewell)

Changes: 2 added, 0 modified, 0 deleted

Run 'conduit apply' to deploy these changes.
```

**What it means:** Like `terraform plan` for your pipelines. Shows exactly what will change.

## Step 6: Run

```bash
conduit run daily_etl
```

Expected output:
```
Executing daily_etl (logical date: 2026-03-23)...

extract [0/1]
  ✓ Output: 1000 rows (12ms)

transform [1/1]
  ✓ Output: 950 rows (18ms)

load [2/1]
  ✓ Completed (5ms)

Execution summary:
  Status: SUCCESS
  Duration: 35ms
  Tasks: 3/3 passed
  Snapshot: snap_001_abc123

View logs: conduit logs daily_etl snap_001_abc123
```

## Step 7: Start the dashboard

```bash
conduit serve
```

Expected output:
```
Starting Conduit server...
✓ Bound to http://0.0.0.0:8080
✓ Loaded 2 DAGs (5 tasks)
✓ Loaded 1 environments (production)

Open http://localhost:8080 to view your pipelines.
Press Ctrl+C to stop.
```

Open your browser to:
- **http://localhost:8080** — DAGs, runs, and execution history
- **http://localhost:8080/api/docs** — OpenAPI documentation

## What's Next?

### Virtual environments

Develop safely before production:

```bash
conduit env create staging
conduit plan staging
conduit apply staging
```

Promote when ready:

```bash
conduit env promote staging production
```

### Migration from Airflow

Automatically convert Airflow DAGs:

```bash
conduit migrate ./airflow_dags --output ./dags --dry-run
conduit migrate ./airflow_dags --output ./dags
```

### Deploy changes safely

Use the plan-and-apply workflow:

```bash
conduit plan
conduit apply --auto-approve
```

### API integration

Trigger DAGs programmatically:

```bash
curl -X POST http://localhost:8080/api/dags/daily_etl/run \
  -H "Content-Type: application/json" \
  -d '{"logical_date": "2026-03-23"}'
```

### Time-travel debugging

Replay events to reconstruct state at any point:

```bash
conduit replay --from 1 --to 100 --json
```

---

**Need help?** Check the [full documentation](../README.md), or run `conduit --help`.
