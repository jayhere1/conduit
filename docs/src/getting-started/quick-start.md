# Quick Start: 5-Minute Tutorial

In this guide, you'll create a complete data pipeline, compile it, run it, deploy it to a virtual environment, make changes, plan the diff, and apply it.

## 1. Create a Project

```bash
conduit init analytics-pipeline
cd analytics-pipeline
```

## 2. Write Your First DAG

Replace the contents of `dags/etl.py` with a 3-task pipeline:

```python
from conduit.sdk import dag, task, Pool
from datetime import datetime

@task(timeout=300)
def extract():
    """Download raw data from the API."""
    print("Extracting user data from API...")
    # In real usage, this would call an API or read a database
    users_count = 1000
    print(f"xcom|users_count|{users_count}")
    return "raw_users.csv"

@task(timeout=300, pool=Pool.name("transforms", size=3))
def transform(raw_data):
    """Clean and deduplicate the data."""
    print(f"Transforming {raw_data}...")
    # Deduplicate, validate schemas, etc.
    users_count = 950  # 5% duplicates removed
    print(f"xcom|users_count|{users_count}")
    return "clean_users.csv"

@task(timeout=600, retries=2)
def load(clean_data):
    """Load cleaned data to the warehouse."""
    print(f"Loading {clean_data} to warehouse...")
    # This task has 2 retries in case of transient warehouse errors
    print("xcom|rows_loaded|950")
    return "success"

@dag(
    schedule="0 2 * * *",  # Daily at 2 AM UTC
    description="User analytics ETL pipeline"
)
def daily_analytics_etl():
    """
    Extract → Transform → Load pipeline.

    Typical run time: ~2 minutes
    Latest run: 2024-03-21 02:15 UTC
    """
    raw = extract()
    clean = transform(raw)
    result = load(clean)
    return result
```

## 3. Compile the DAG

Conduit uses tree-sitter to parse and validate your DAG without executing Python:

```bash
conduit compile
```

Expected output:

```
Compiling DAGs in dags/...

✓ etl.py
  - DAG: daily_analytics_etl
  - Tasks: 3
  - Dependencies: extract → transform → load
  - Schedule: 0 2 * * * (daily at 2 AM)
  - Fingerprint: f1e2d3c4b5a6

Compilation successful: 1 DAG, 3 tasks
```

The fingerprint is a content-addressable hash of the entire DAG. If you change even one character, the fingerprint changes, triggering a recompilation.

## 4. Run the DAG Locally

Test the complete pipeline end-to-end in your development environment:

```bash
conduit run daily_analytics_etl
```

Watch the output:

```
[2024-03-22 14:32:10.123] DAG run started: run_id=abc123def456
[2024-03-22 14:32:10.456] Task extract started
Extracting user data from API...
xcom|users_count|1000
[2024-03-22 14:32:12.123] Task extract completed (1.67s)
[2024-03-22 14:32:12.456] Task transform started
Transforming raw_users.csv...
xcom|users_count|950
[2024-03-22 14:32:15.789] Task transform completed (3.33s)
[2024-03-22 14:32:16.012] Task load started
Loading clean_users.csv to warehouse...
xcom|rows_loaded|950
[2024-03-22 14:32:18.567] Task load completed (2.56s)
[2024-03-22 14:32:18.890] DAG run completed successfully

Total time: 8.77s
XCom outputs:
  - extract.users_count: 1000
  - transform.users_count: 950
  - load.rows_loaded: 950
```

Conduit runs tasks sequentially or in parallel based on dependencies. In this case, extract → transform → load is a linear chain, so they run sequentially.

## 5. Check Status

View the history of all runs:

```bash
conduit status
```

Output:

```
Environment: development

Last 10 runs:
┌─────────┬────────────────┬──────────┬────────────┐
│ Run ID  │ DAG            │ Status   │ Started    │
├─────────┼────────────────┼──────────┼────────────┤
│ abc123  │ daily_analytics_etl │ success  │ 14:32:10   │
└─────────┴────────────────┴──────────┴────────────┘
```

## 6. Deploy to Production

Create a "production" environment and deploy:

```bash
# Create a production environment (forked from development)
conduit env create production --from development

# See what would change
conduit plan production
```

Expected output:

```
Planning changes for environment: production

Added:
  ✓ daily_analytics_etl (tasks: 3)
    - extract (fingerprint: f1a2b3c4d5e6)
    - transform (fingerprint: g2h3i4j5k6l7)
    - load (fingerprint: m3n4o5p6q7r8)

Summary: 1 DAG added, 3 tasks added
Snapshot would use 1.2 KB
```

Now apply the changes:

```bash
conduit apply production -y
```

Output:

```
Applying plan for environment: production

✓ Deployed daily_analytics_etl (3 tasks)
  - extract: new snapshot
  - transform: new snapshot
  - load: new snapshot

Environment updated: production
Snapshot ID: prod-snap-20240322-143215
```

Your DAG is now deployed to production!

## 7. Make a Change and Deploy Again

Imagine you want to increase the retry count for the load task. Edit `dags/etl.py`:

```python
@task(timeout=600, retries=3)  # Changed from 2 to 3
def load(clean_data):
    """Load cleaned data to the warehouse."""
    print(f"Loading {clean_data} to warehouse...")
    print("xcom|rows_loaded|950")
    return "success"
```

Recompile:

```bash
conduit compile
```

Now plan the changes for production:

```bash
conduit plan production
```

Output:

```
Planning changes for environment: production

Modified:
  ⚠ daily_analytics_etl
    - extract: unchanged
    - transform: unchanged
    - load: modified (retries 2 → 3)

Summary: 1 task modified
Impact analysis:
  - Blast radius: 1 task (load only)
  - Cascading changes: 0 tasks
  - Safe to apply immediately

Snapshot would reuse 2/3 tasks (extract, transform)
```

Only the `load` task fingerprint changed. The other tasks can be reused from the previous snapshot.

Apply the changes:

```bash
conduit apply production -y
```

Output:

```
Applying plan for environment: production

✓ Updated daily_analytics_etl
  - extract: reused (fingerprint: f1a2b3c4d5e6)
  - transform: reused (fingerprint: g2h3i4j5k6l7)
  - load: updated (retries 2 → 3)

Environment updated: production
Snapshot ID: prod-snap-20240322-143456
```

Notice that extract and transform were **reused** from the previous snapshot. Only load had to be recompiled. This is the power of fingerprint-based change detection.

## 8. Create a Feature Branch

Now create a staging environment to test new changes before production:

```bash
conduit env create staging --from production
```

Output:

```
Environment created: staging
Forked from: production
Snapshot: prod-snap-20240322-143456 (shared)
```

Make a bigger change in your DAG (e.g., add a validation task):

```python
@task(timeout=300)
def validate(clean_data):
    """Validate data quality before loading."""
    print(f"Validating {clean_data}...")
    # Check for null values, schema mismatches, etc.
    print("xcom|validation_passed|true")
    return True

@dag(
    schedule="0 2 * * *",
    description="User analytics ETL pipeline"
)
def daily_analytics_etl():
    raw = extract()
    clean = transform(raw)
    validated = validate(clean)
    result = load(validated)
    return result
```

Compile and plan:

```bash
conduit compile
conduit plan staging
```

Output:

```
Planning changes for environment: staging

Modified:
  ⚠ daily_analytics_etl
    - extract: unchanged
    - transform: unchanged
    - validate: added
    - load: modified (input changed)

Summary: 1 task added, 1 task modified
Impact analysis:
  - Blast radius: 2 tasks (validate, load)
  - Cascading changes: 1 task (load invalidated by new validate)

Snapshot would reuse 2/3 tasks (extract, transform)
```

Test in staging:

```bash
conduit run daily_analytics_etl --env staging
```

When ready, promote staging to production:

```bash
conduit env promote staging production
```

Output:

```
Promoting staging → production

Snapshot: staging-snap-20240322-144123 → production
Previous production snapshot archived: prod-snap-20240322-143456

Environment updated: production
```

Production now has the new validate task. Rollback is instant if needed:

```bash
conduit env rollback production --to prod-snap-20240322-143456
```

## Summary

You now understand the Conduit workflow:

1. **Define** DAGs in Python with `@dag` and `@task` decorators
2. **Compile** to validate syntax and extract dependencies (tree-sitter, no execution)
3. **Run** locally to test end-to-end
4. **Plan** changes against an environment (fingerprint diffing)
5. **Apply** to deploy (snapshot reuse optimization)
6. **Promote** between environments (staging → production)
7. **Rollback** instantly if needed

Next steps:

- **[DAG Concepts](../concepts/dags.md)**: Learn task types, schedules, retries, and pools
- **[Virtual Environments](../concepts/environments.md)**: Deep dive into fork/promote/rollback
- **[Plan/Apply Workflow](../concepts/plan-apply.md)**: Understand fingerprinting and change detection
