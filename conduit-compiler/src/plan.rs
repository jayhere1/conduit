//! ConduitPlan: the compiled, serialized execution plan.
//!
//! A ConduitPlan is the output of the compiler. It contains all resolved DAGs
//! ready for the scheduler to execute. The scheduler operates on ConduitPlans,
//! not raw Python files — this is what makes Forge fast.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use conduit_common::dag::Dag;
use conduit_common::error::{ConduitError, ConduitResult};
use tracing::info;

use crate::parser::DagParser;
use crate::resolver::DependencyResolver;
use crate::yaml_parser::YamlDagParser;

/// A compiled execution plan containing all resolved DAGs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConduitPlan {
    /// All compiled DAGs, keyed by DAG ID.
    pub dags: HashMap<String, Dag>,

    /// When this plan was compiled.
    pub compiled_at: DateTime<Utc>,

    /// How long compilation took (milliseconds).
    pub compilation_time_ms: u64,

    /// Total number of tasks across all DAGs.
    pub total_tasks: usize,

    /// Any non-fatal warnings from compilation.
    pub warnings: Vec<String>,
}

/// Compilation statistics for reporting.
#[derive(Debug)]
pub struct CompilationStats {
    pub files_scanned: usize,
    pub dags_compiled: usize,
    pub tasks_total: usize,
    pub errors: Vec<ConduitError>,
    pub warnings: Vec<String>,
    pub duration_ms: u64,
}

impl ConduitPlan {
    /// Compile all DAGs from a directory into a ConduitPlan.
    ///
    /// Scans for both Python (.py) and YAML (.yaml/.yml) DAG definitions.
    /// Both formats produce the same `ParsedDag` structs that feed into
    /// the dependency resolver — the scheduler doesn't know the difference.
    pub fn compile(dags_path: &Path) -> ConduitResult<(Self, CompilationStats)> {
        let start = Instant::now();

        // Phase 1a: Parse all Python files
        let mut parser = DagParser::new()?;
        let mut parsed_dags = parser.parse_directory(dags_path)?;
        let py_count = parsed_dags.len();

        // Phase 1b: Parse all YAML files from the same directory
        let yaml_dags = YamlDagParser::parse_directory(dags_path)?;
        let yaml_count = yaml_dags.len();
        parsed_dags.extend(yaml_dags);

        let files_scanned = py_count + yaml_count;

        info!(
            python_dags = py_count,
            yaml_dags = yaml_count,
            total = files_scanned,
            "Parsed DAG definitions"
        );

        // Phase 2: Resolve dependencies and detect cycles
        let (resolved, errors) = DependencyResolver::resolve_all(parsed_dags);

        let total_tasks: usize = resolved.iter().map(|d| d.tasks.len()).sum();
        let duration = start.elapsed();

        info!(
            dags = resolved.len(),
            tasks = total_tasks,
            errors = errors.len(),
            duration_ms = duration.as_millis() as u64,
            "Compilation complete"
        );

        let dags: HashMap<String, Dag> = resolved.into_iter().map(|d| (d.id.clone(), d)).collect();

        let plan = ConduitPlan {
            dags: dags.clone(),
            compiled_at: Utc::now(),
            compilation_time_ms: duration.as_millis() as u64,
            total_tasks,
            warnings: Vec::new(),
        };

        let stats = CompilationStats {
            files_scanned,
            dags_compiled: dags.len(),
            tasks_total: total_tasks,
            errors,
            warnings: Vec::new(),
            duration_ms: duration.as_millis() as u64,
        };

        Ok((plan, stats))
    }

    /// Serialize the plan to JSON (for caching and debugging).
    pub fn to_json(&self) -> ConduitResult<String> {
        serde_json::to_string_pretty(self).map_err(|e| e.into())
    }

    /// Deserialize a plan from JSON.
    pub fn from_json(json: &str) -> ConduitResult<Self> {
        serde_json::from_str(json).map_err(|e| e.into())
    }

    /// Save the plan to a file.
    pub fn save(&self, path: &Path) -> ConduitResult<()> {
        let json = self.to_json()?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Load a plan from a file.
    pub fn load(path: &Path) -> ConduitResult<Self> {
        let json = std::fs::read_to_string(path)?;
        Self::from_json(&json)
    }
}

impl std::fmt::Display for CompilationStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Compilation Results:")?;
        writeln!(f, "  DAGs compiled:  {}", self.dags_compiled)?;
        writeln!(f, "  Total tasks:    {}", self.tasks_total)?;
        writeln!(f, "  Errors:         {}", self.errors.len())?;
        writeln!(f, "  Duration:       {}ms", self.duration_ms)?;

        if !self.errors.is_empty() {
            writeln!(f, "\nErrors:")?;
            for err in &self.errors {
                writeln!(f, "  - {}", err)?;
            }
        }

        Ok(())
    }
}
