# We Compiled 1,000 DAGs in 1.8 Seconds Without Executing a Single Line of Python

If you've scaled Airflow to a few hundred DAGs, you've hit the wall. Your scheduler starts crawling. Your DAG parsing times balloon. You stare at the logs and see the same painful truth: `DagBag.process_file()` literally executes every Python file in your `dags/` folder. Every. Single. One.

Want to know what that means? If you import pandas at the top of one DAG file, your scheduler pays that import cost for *every* DAG parse cycle. If someone puts a network call at module scope, your parser blocks waiting for it. If you have 200 DAG files and one of them is slow, the entire parse stalls.

This is not a performance problem. It's a fundamental design flaw.

We asked ourselves: what if you could understand the structure of a Python DAG without running Python at all?

## How Airflow Parses DAGs (The Problem)

Here's what Airflow does:

```python
# Pseudocode from airflow/models/dagbag.py
def process_file(file_path):
    with open(file_path) as f:
        code = f.read()

    # This is the key line
    exec(compile(code, file_path, 'exec'), globals_dict)
```

This means:
- Every import statement at module scope runs
- Every class definition is evaluated
- Every decorator is executed
- Side effects happen *during parsing*
- You have no control over what runs before your DAG is actually instantiated

That serialization problem? Airflow has to pickle the entire DAG object to send it to workers. Complex DAG classes with stateful decorators or custom operators don't serialize cleanly.

And the fundamental issue: parsing time couples to *total code complexity*, not DAG count. Add a heavy library? Every parse pays the cost. Spin up a new developer machine? The scheduler parse time balloons until the first successful parse cycle.

## How Conduit Parses DAGs (The Solution)

We use [tree-sitter](https://tree-sitter.github.io/tree-sitter/), an incremental parsing library that builds concrete syntax trees. It parses Python to AST without executing anything.

Here's the core of our parser:

```rust
// Simplified from conduit-compiler/src/parser.rs
fn try_parse_dag(
    &self,
    node: &tree_sitter::Node,
    source: &[u8],
) -> Result<Option<ParsedDag>> {
    // Find the @dag decorator
    let mut has_dag_decorator = false;
    let mut dag_args = HashMap::new();

    for child in node.children(&mut cursor) {
        if child.kind() == "decorator" {
            let text = self.node_text(&child, source);
            if text.contains("@dag") {
                has_dag_decorator = true;
                dag_args = self.extract_decorator_args(&child, source);
            }
        }
    }

    if !has_dag_decorator {
        return Ok(None);
    }

    // Extract function name as DAG ID
    let dag_id = func_def
        .child_by_field_name("name")
        .map(|n| self.node_text(&n, source))
        .unwrap_or("unknown".to_string());

    // Extract docstring
    let description = self.extract_docstring(&func_def, source);

    // Parse tasks inside the function body
    let tasks = self.extract_tasks(&func_def, source)?;

    Ok(Some(ParsedDag {
        id: dag_id,
        description,
        schedule: dag_args.get("schedule").cloned(),
        tags: parse_tags(dag_args.get("tags")),
        tasks,
        // ... other metadata
    }))
}
```

What we extract:

- **DAG ID**: The function name (via `child_by_field_name("name")`)
- **Metadata**: Decorator arguments (`schedule`, `tags`, `max_active_runs`, `on_failure`)
- **Description**: The docstring (first string in the function body)
- **Tasks**: Nested `@task`-decorated functions
- **Dependencies**: Data-flow by parsing assignments like `cleaned = transform(raw)` and matching variable names to task calls
- **Task config**: Retries, pool, timeout, priority — all from decorators

What we **don't** do: import anything, execute anything, resolve runtime values.

It's pure syntax analysis. Your DAG could have a typo in a Python expression inside a task and Conduit would still parse it fine. Airflow would crash.

## Data-Flow Dependency Extraction

One thing that impressed us: you can extract task dependencies from *variable assignments*.

```python
@dag(schedule="0 6 * * *")
def daily_warehouse_refresh():
    @task(retries=3)
    def extract_orders():
        pass

    @task()
    def transform_orders(raw):
        pass

    @task()
    def load_to_warehouse(data):
        pass

    # This is how you define the pipeline
    raw = extract_orders()
    cleaned = transform_orders(raw)
    load_to_warehouse(cleaned)
```

Our parser walks the DAG function body, builds a map of variables to task results, then traces which tasks pass data to which. No execution required. It's all textual pattern matching on the AST.

## The Benchmark

We ran Criterion benchmarks with generated DAG files. Here are the real numbers:

| DAGs | Tasks/DAG | Conduit | Airflow (est.) |
|------|-----------|---------|----------------|
| 10   | 100       | 0.8ms   | ~2-5s          |
| 100  | 50        | 18ms    | ~15-30s        |
| 500  | 10        | 450ms   | ~60-90s        |
| 1000 | 10        | 1.8s    | ~120-150s      |

That 1.8-second number is from our actual Criterion output. We generated 1,000 Python files with 10 tasks each, wrote them to disk, and compiled them all. Single pass, cold cache.

Airflow numbers are estimates based on community benchmarks and our own testing on similar hardware. The variance is high because it depends heavily on your decorator implementation, whether you have expensive imports, whether you're using Pendulum or dateutil, etc.

The point: Conduit is 50-100x faster. Not because Rust is faster (though it is), but because we don't execute code.

## What This Unlocks

Speed alone isn't the goal. It's what you can do *because* parsing is fast:

### 1. CI/CD Feedback in Milliseconds

```bash
$ git push
# In your PR check:
$ conduit compile --dir dags/
Compiled 284 DAGs in 42ms.
All DAGs valid.
```

Sub-second feedback. No more waiting for a 60-second DAG parse just to find a typo.

### 2. Plan/Apply Workflow

Conduit has a Terraform-like plan/apply model. Because compilation is cheap, you can fingerprint the compiled DAGs and diff them against production:

```bash
$ conduit plan --dir dags/ --state production.db
Plan: 3 new DAGs, 2 modified, 0 deleted
  + daily_warehouse_refresh (100 new tasks)
  ~ customer_churn_model (5 tasks changed)
  - legacy_batch_job (removed)

Apply? (y/n)
```

You see the exact changes before they hit production. No surprises.

### 3. Virtual Environments

Conduit's state is event-sourced in RocksDB, not a mutable database. Snapshots are content-addressed. This means you can fork the entire state, compile your branch's DAGs, compare them to production, all with zero runtime cost:

```bash
$ conduit fork production my-feature-env
Forked production -> my-feature-env (0.3ms)
$ conduit compile --env my-feature-env
Compiled 284 DAGs in 38ms
```

No duplication. No wasteful resource provisioning. Just cheap snapshots.

## The Architecture

Conduit isn't just a fast parser. It's built differently from the ground up:

- **Event-sourced state**: Every mutation is an immutable event in RocksDB. No ACID database overhead. Replay-able. Auditable.
- **Async event loop**: The scheduler is a tokio-based event loop, not a polling scheduler. Tighter resource usage. Lower latency.
- **Single binary**: Compiler, scheduler, executor, API, and state management in one 11-crate Rust project (~32K lines). No external services. Deploy to a Lambda if you want.
- **Horizontal scaling**: Worker pool via gRPC. Spin up workers on demand. Tear them down when idle.

The parser lives in `conduit-compiler`, which is one crate. It takes a filesystem path, spawns a tree-sitter parser, and walks the tree. No runtime, no magic.

## The Trade-Offs (Be Honest)

We can't do everything Airflow does:

- **Dynamic DAG generation**: Factory patterns that build DAGs at runtime? We parse AST, not runtime. You need to commit your DAGs as concrete Python files. This actually feels like a feature to us (immutable DAGs, no late binding), but we admit it's a constraint.
- **Arbitrary operators**: Airflow's plug-in ecosystem is massive. Conduit has built-in providers (Snowflake, dbt, BigQuery, Postgres, S3, etc.) but not every operator ever written. We're adding them.
- **Python callbacks**: If your DAG logic depends on runtime Python (context managers, thread locals, etc.), we can't parse that. Conduit is for declarative pipelines.
- **Distributed execution is early**: Our distributed mode works, but the web UI is functional, not polished. We're not production-hardened like Airflow's Celery execution yet.

We're also honest: Conduit is younger. Airflow is battle-tested at Netflix scale. We're not there yet.

But for teams starting new, or teams that have outgrown Airflow and are tired of the parse-time tax, Conduit is worth the look.

## Compiler Benchmarks

If you want to dive into the numbers, we publish Criterion benchmarks in the repo:

```
test compile_10_dags     ... bench:      0.8 ms/iter
test compile_100_dags    ... bench:     18.2 ms/iter
test compile_1000_dags   ... bench:  1,847.5 ms/iter
```

These are wall-clock times, including filesystem I/O, tree-sitter parsing, AST walks, and serialization to our internal plan format. No hand-waving.

Run them yourself:

```bash
cargo bench --bench compiler_bench
```

It's all reproducible.

## Try It

Conduit is open source. If you're building data pipelines and tired of Airflow's parse-time pain, give it a shot:

```bash
curl -sSL https://install.conduit.dev | sh
conduit --help
```

The GitHub repo is [https://github.com/conduit-dev/conduit](https://github.com/conduit-dev/conduit).

Documentation is at [https://docs.conduit.dev](https://docs.conduit.dev).

## The Real Question

If your orchestrator needs to *execute* your code just to *understand* it, that's a design flaw, not a feature.

It's 2025. We have better tools. Use them.
