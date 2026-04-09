//! conduit-compiler: DAG compilation engine.
//!
//! Parses DAG definitions from both Python (via tree-sitter) and YAML files,
//! resolves dependencies, detects cycles, and emits a ConduitPlan.
//!
//! Both formats produce identical `ParsedDag` structs — the scheduler and executor
//! don't know which format a DAG was defined in.
//!
//! This is Conduit's core performance advantage: compiling 1,000 DAGs in <2 seconds
//! vs. Airflow's 60-120 seconds of Python interpretation.

pub mod parser;
pub mod plan;
pub mod resolver;
pub mod yaml_parser;

pub use parser::DagParser;
pub use plan::ConduitPlan;
pub use resolver::DependencyResolver;
pub use yaml_parser::YamlDagParser;
