# Conduit Documentation

This is the complete mdBook documentation site for Conduit, a Rust-native data pipeline orchestrator.

## Building the Documentation

Install mdBook:

```bash
cargo install mdbook
```

Build the documentation:

```bash
mdbook build
```

Serve locally:

```bash
mdbook serve
```

The documentation will be available at `http://localhost:3000`.

## Structure

- **Introduction** — What Conduit is and why it exists
- **Getting Started** — Installation and quick start guide
- **Concepts** — Core concepts (DAGs, environments, plan/apply, lineage, events)
- **Reference** — CLI, REST API, Python SDK
- **Advanced** — System architecture and migration from Airflow

## Total Content

- 15 documentation files
- 6,300+ lines of content
- Includes code examples, diagrams, and practical guides
- Covers compilation, scheduling, execution, deployment, and debugging

## Key Topics Covered

1. **Installation & Setup** — Prerequisites, installation, project initialization
2. **Quick Start** — 5-minute walkthrough to create and deploy a DAG
3. **DAG Definition** — Task types, configuration, dependencies, data exchange
4. **Virtual Environments** — Fork/promote/rollback, snapshot pointers
5. **Plan/Apply Workflow** — Fingerprint-based change detection, deployment safety
6. **Event-Sourced Architecture** — Time-travel debugging, audit trails
7. **Column-Level Lineage** — Data tracing, impact analysis (Phase 4)
8. **CLI Reference** — Complete command reference with examples
9. **REST API Reference** — All 20+ endpoints documented
10. **Python SDK Guide** — Decorators, task types, advanced patterns
11. **System Architecture** — Crate layout, data flow, performance characteristics
12. **Migration from Airflow** — Concept mapping, automated conversion, step-by-step guide

## Design

The documentation is designed to be:

- **Comprehensive** — Covers all major features and use cases
- **Practical** — Includes real code examples and workflows
- **Visual** — Uses Mermaid diagrams for architecture and data flow
- **Searchable** — Full-text search via mdBook

## Next Steps

1. Run `mdbook build` to generate static HTML
2. Deploy to a web server or GitHub Pages
3. Share the link with your team
