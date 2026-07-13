# Plan/Apply Workflow and Change Detection

Conduit uses a **Terraform-style plan/apply workflow** for deploying DAG changes. Instead of applying changes directly, you first generate a plan showing exactly what would change, then apply it after review.

## Overview

The workflow is:

1. **Compile** — Parse and validate DAG definitions (tree-sitter)
2. **Plan** — Compare compiled DAGs against environment state, compute fingerprints, detect changes
3. **Apply** — Execute the plan, update environment snapshots, reuse unchanged tasks

This ensures that deployments are **safe, predictable, and auditable**.

## Fingerprinting: The Core Innovation

A **fingerprint** is a content-addressable hash of a task and all its upstream dependencies.

### Fingerprint Computation

Conduit computes fingerprints in **topological order**:

```python
@task
def extract():
    return "data"

@task
def transform(data):
    return f"clean: {data}"

@task
def load(data):
    return f"loaded: {data}"

@dag
def etl():
    d = extract()
    c = transform(d)
    load(c)
```

Fingerprints are computed as:

```
extract fingerprint:
  hash(task_code, timeout, retries, pool, schedule)
  = "f1a2b3c4d5e6"

transform fingerprint:
  hash(task_code, timeout, retries, pool, upstream_fingerprints=[extract])
  = "g2h3i4j5k6l7"

load fingerprint:
  hash(task_code, timeout, retries, pool, upstream_fingerprints=[transform])
  = "m3n4o5p6q7r8"
```

**Key insight**: If `extract` changes, both `transform` and `load` fingerprints change automatically, even though their code didn't change. This is **upstream cascade invalidation**.

### What Goes Into a Fingerprint?

```
• Task code (the function body)
• Task configuration (timeout, retries, pool, tags)
• Schedule (cron expression)
• Trigger rules
• Data type annotations
• Upstream task fingerprints (recursive)
```

Fingerprints do **not** include:
- Comments or docstrings
- Variable names (unless they affect code)
- Comments in logs

### Why Content-Addressable?

Content-addressable means the hash is deterministic. The same DAG definition always produces the same fingerprint. This enables:

1. **Snapshot reuse**: If a task hasn't changed, reuse it from the previous snapshot
2. **Change detection**: Different fingerprint = something changed
3. **Deduplication**: Store only one copy of unchanged tasks

## Change Classification

When you plan a deployment, Conduit classifies each task as one of:

### Added
A new task that doesn't exist in the current environment:

```bash
conduit plan production
```

Output:

```
Added:
  + extract (fingerprint: f1a2b3c4d5e6)
  + transform (fingerprint: g2h3i4j5k6l7)
  + load (fingerprint: m3n4o5p6q7r8)
```

### Unchanged
Task code, config, and all upstream tasks are identical:

```
Unchanged:
  ✓ extract (fingerprint: f1a2b3c4d5e6 → f1a2b3c4d5e6)
  ✓ transform (fingerprint: g2h3i4j5k6l7 → g2h3i4j5k6l7)
  ✓ load (fingerprint: m3n4o5p6q7r8 → m3n4o5p6q7r8)
```

### Modified
Task code or config changed, but the DAG structure is the same:

```
Modified:
  ⚠ extract (timeout 300 → 600)
    Fingerprint: f1a2b3c4d5e6 → f1a2b3c4d5f7
```

### UpstreamInvalidated
An upstream task changed, so this task must be recompiled even if its code is unchanged:

```
Modified:
  ⚠ extract (timeout 300 → 600)
    Fingerprint: f1a2b3c4d5e6 → f1a2b3c4d5f7

UpstreamInvalidated:
  ⚠ transform (no code changes, but extract changed)
    Fingerprint: g2h3i4j5k6l7 → g2h3i4j5k7m8
  ⚠ load (no code changes, but transform changed)
    Fingerprint: m3n4o5p6q7r8 → m3n4o5q7r9s0
```

When extract changes, transform and load fingerprints cascade, even though their code is identical.

### Removed
A task was deleted from the DAG:

```
Removed:
  - old_task (fingerprint: x1y2z3a4b5c6)
```

## The Plan Output

```bash
conduit plan production
```

Complete plan example:

```
Planning changes for environment: production
Current snapshot: prod-snap-20240322-143215

Modified:
  ⚠ daily_analytics_etl.extract
    Timeout: 300 → 600
    Fingerprint: f1a2... → f1b3...

UpstreamInvalidated:
  ⚠ daily_analytics_etl.transform
    No code changes, but upstream extract changed
    Fingerprint: g2h3... → g2i4...

  ⚠ daily_analytics_etl.load
    No code changes, but upstream transform changed
    Fingerprint: m3n4... → m3o5...

Unchanged:
  ✓ hourly_metrics (all tasks)
  ✓ user_segmentation (all tasks)

Impact Analysis:
  Blast radius: 3 tasks (extract, transform, load)
  Cascading changes: 2 tasks (transform, load)
  Safe to apply: Yes

Snapshot Optimization:
  New tasks to compile: 3 (extract, transform, load)
  Reusable from snapshot: 7 (hourly_metrics, user_segmentation)
  Snapshot size savings: 35% (reusing 7 of 10 tasks)
  New snapshot size: 1.2 KB

Ready to apply?
  conduit apply production -y
```

## The Apply Process

```bash
conduit apply production -y
```

During apply:

1. **Validate**: Confirm no new conflicts since plan was generated
2. **Compile**: Recompile only modified and invalidated tasks
3. **Snapshot**: Create new snapshot with compiled tasks + reused tasks
4. **Update**: Move environment pointer to new snapshot
5. **Broadcast**: Notify scheduler of new snapshot

Output:

```
Applying plan for environment: production

Compiling modified tasks:
  ✓ daily_analytics_etl.extract (compiled from source)
  ✓ daily_analytics_etl.transform (compiled from source)
  ✓ daily_analytics_etl.load (compiled from source)

Reusing unchanged tasks:
  ✓ hourly_metrics.extract
  ✓ hourly_metrics.transform
  ✓ hourly_metrics.load
  ✓ user_segmentation.extract
  ✓ user_segmentation.transform
  ✓ user_segmentation.load
  ✓ user_segmentation.aggregate

Creating snapshot:
  ✓ Fingerprint index: 10 tasks
  ✓ Serialization: 1.2 KB
  ✓ Snapshot ID: prod-snap-20240322-145456

Updating environment:
  production → prod-snap-20240322-145456
  Previous snapshot: prod-snap-20240322-143215 (archived)

Deployment complete!
```

## Snapshot Reuse Optimization

The key insight: **Only compile what changed**. Everything else is reused from the previous snapshot.

### Example

Start with this DAG:

```python
@dag
def etl():
    d = extract()
    c = transform(d)
    load(c)
```

Initial deployment creates snapshot with 3 tasks.

Now change only the load task's timeout:

```python
@dag
def etl():
    d = extract()
    c = transform(d)
    load(c)  # No code change
```

Change config:

```python
@task(timeout=600)  # Was 300
def load(data):
    ...
```

Plan shows:

```
Modified:
  ⚠ load (timeout 300 → 600)
    Fingerprint: m3n4... → m3o5...

UpstreamInvalidated:
  (none, load has no downstream tasks)

Snapshot Optimization:
  New tasks to compile: 1 (load)
  Reusable from snapshot: 2 (extract, transform)
  Savings: 67% smaller snapshot
```

Only load is recompiled. Extract and transform are byte-for-byte identical, so they're reused. This is **content-addressable snapshots**.

## Conflict Detection

Every saved plan records `base_environment_version` — the environment's
revision counter (`Environment.current_version`) at the moment the plan was
generated. That counter is bumped by every apply, promote, and rollback. If
the environment changes while you're holding a saved plan, Conduit detects
the conflict at apply time and refuses to apply it:

```bash
# Generate a plan
conduit plan production --output plan.json

# Someone else applies a different change
# (in another shell)

# Try to apply your (now stale) plan
conduit apply production --plan-file plan.json -y
```

Output:

```
Error: stale plan — environment 'production' changed since this plan was generated.
  Current environment version: 4
  Plan was based on version:    3

Recommended action:
  conduit plan production --output plan.json   # regenerate against current state
  conduit apply production --plan-file plan.json -y
```

Conduit also rejects a plan file applied against the wrong environment: if a
plan's `target_environment` doesn't match the environment named on the
`apply` command line, the apply fails immediately with an error telling you
which environment the plan actually targets.

Conduit prevents applying stale plans.

## Rollback Is Just a Plan/Apply

Rollback is not a special operation—it's just a plan/apply to a previous snapshot:

```bash
# See previous snapshots
conduit snapshot list

# Plan rollback to specific snapshot
conduit plan production --to prod-snap-20240322-143215

# Apply rollback
conduit apply production --to prod-snap-20240322-143215 -y
```

This is functionally identical to promoting an old environment. Rollback is instant because snapshots are immutable pointers.

## Safety Guarantees

The plan/apply workflow provides several safety guarantees:

### 1. Read-Only Plans

Plans are read-only. Running `conduit plan` never modifies state:

```bash
conduit plan production  # Safe to run anytime
```

### 2. Deterministic Fingerprints

Same code always produces same fingerprint:

```bash
conduit plan production     # Fingerprint: f1a2b3c4d5e6
# Wait 1 hour
conduit plan production     # Fingerprint: f1a2b3c4d5e6 (identical)
```

### 3. Atomic Snapshots

Snapshots are created atomically. Partial uploads or corruption is impossible:

```bash
conduit apply production -y
# If killed mid-apply, environment remains unchanged
# (No half-applied snapshots)
```

### 4. Complete Audit Trail

Every plan and apply is logged to the event store:

```bash
conduit events list --filter type:plan_generated
conduit events list --filter type:snapshot_deployed
```

## Best Practices

1. **Always plan before apply**: Never skip the plan step
2. **Review plans in peer reviews**: Share plan output with team
3. **Apply during low-traffic windows**: Minimize impact if issues arise
4. **Keep snapshots around**: Don't immediately delete old snapshots
5. **Test in staging first**: Apply changes to staging before production
6. **Monitor after apply**: Check metrics for 1-2 hours post-deployment

## Complex Scenarios

### Zero-Downtime Deployments

```bash
# 1. Create canary environment
conduit env create canary --from production

# 2. Apply changes to canary
vim dags/etl.py
conduit compile
conduit plan canary
conduit apply canary -y

# 3. Verify canary works (run a few test executions)
conduit run daily_analytics_etl --env canary

# 4. Switch traffic gradually
# (This is application-level routing, not Conduit)

# 5. Promote canary to production when stable
conduit env promote canary production
```

### A/B Testing Different Implementations

```bash
# Branch A: New implementation
conduit env create impl-a --from production
vim dags/etl.py  # Implement new transform
conduit apply impl-a -y

# Branch B: Keep old implementation
conduit env create impl-b --from production
# (no changes, same as production)

# Run both for 1 week, compare metrics
# Winner becomes new production
conduit env promote impl-a production
```

## Next Steps

- **[Virtual Environments](./environments.md)**: Understand how environments and snapshots work
- **[Column-Level Lineage](./lineage.md)**: Track data flow through pipelines
- **[CLI Reference](../cli-reference.md)**: Plan and apply command details
