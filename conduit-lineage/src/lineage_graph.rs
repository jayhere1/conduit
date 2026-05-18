//! Column-level lineage graph.
//!
//! The lineage graph tracks which columns flow into which other columns,
//! across task boundaries. This enables:
//! - "Where does this column come from?" (upstream trace)
//! - "What would break if I change this column?" (downstream trace)
//! - Visualization of data flow at column granularity

use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};

/// Identifies a task by (dag, task) so cross-task lineage edges can be
/// emitted unambiguously across multiple DAGs sharing the same task id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskRef {
    pub dag_id: String,
    pub task_id: String,
}

impl TaskRef {
    pub fn new(dag_id: impl Into<String>, task_id: impl Into<String>) -> Self {
        Self {
            dag_id: dag_id.into(),
            task_id: task_id.into(),
        }
    }
}

impl std::fmt::Display for TaskRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::{}", self.dag_id, self.task_id)
    }
}

/// Where the column lives — either a (physical or virtual) table named in
/// SQL, or a specific task in the compiled DAG.
///
/// The SQL extractor only sees identifiers, so it always emits
/// `ColumnSource::Table`. The cross-task stitcher promotes those to
/// `ColumnSource::Task` once it has resolved a table name to a producer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ColumnSource {
    Table(String),
    Task(TaskRef),
}

impl ColumnSource {
    /// Display-form qualifier (`"table"` or `"dag::task"`). Useful for
    /// callers that just need a string key and don't care which variant.
    pub fn qualifier(&self) -> String {
        match self {
            ColumnSource::Table(t) => t.clone(),
            ColumnSource::Task(t) => t.to_string(),
        }
    }

    /// Returns `Some(&task_id)` only when the column lives on a task.
    pub fn task_id(&self) -> Option<&str> {
        match self {
            ColumnSource::Task(t) => Some(&t.task_id),
            ColumnSource::Table(_) => None,
        }
    }

    /// Returns `Some(&table_name)` only when the column lives on a table.
    pub fn table(&self) -> Option<&str> {
        match self {
            ColumnSource::Table(t) => Some(t),
            ColumnSource::Task(_) => None,
        }
    }
}

/// A reference to a specific column in a specific task or table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Where the column lives.
    pub source: ColumnSource,
    /// The column name.
    pub column_name: String,
}

impl ColumnRef {
    /// Construct a column ref against a *table* (the default for SQL
    /// extractor output). Most existing call sites want this — when the
    /// SQL parser sees `FROM orders`, it doesn't yet know which task
    /// produced `orders`.
    pub fn new(table_or_task_id: impl Into<String>, column_name: impl Into<String>) -> Self {
        Self {
            source: ColumnSource::Table(table_or_task_id.into()),
            column_name: column_name.into(),
        }
    }

    /// Explicit table-scoped column.
    pub fn table(table_name: impl Into<String>, column_name: impl Into<String>) -> Self {
        Self {
            source: ColumnSource::Table(table_name.into()),
            column_name: column_name.into(),
        }
    }

    /// Task-scoped column (used after cross-task stitching).
    pub fn task(task: TaskRef, column_name: impl Into<String>) -> Self {
        Self {
            source: ColumnSource::Task(task),
            column_name: column_name.into(),
        }
    }

    /// Display-form qualifier (table name or `dag::task`).
    pub fn qualifier(&self) -> String {
        self.source.qualifier()
    }
}

impl std::fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.qualifier(), self.column_name)
    }
}

/// An edge in the lineage graph: one column feeds into another.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    /// The source column (upstream).
    pub source: ColumnRef,
    /// The target column (downstream).
    pub target: ColumnRef,
    /// How the column is transformed.
    pub transform_type: TransformType,
}

/// How a column value is transformed along an edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransformType {
    /// Direct passthrough — no transformation.
    Direct,
    /// Aggregation (SUM, COUNT, AVG, etc.).
    Aggregation(String),
    /// Computation (expression involving multiple columns).
    Computation,
    /// Type cast.
    Cast,
    /// Filter (the column is used in a WHERE clause).
    Filter,
    /// Join key (the column is used in a JOIN condition).
    JoinKey,
}

/// The column-level lineage graph.
///
/// This is a directed graph where:
/// - Nodes are `ColumnRef` (task.column)
/// - Edges are `LineageEdge` (source → target with transform info)
#[derive(Debug)]
pub struct LineageGraph {
    /// Forward edges: source → list of targets.
    forward: HashMap<ColumnRef, Vec<LineageEdge>>,
    /// Reverse edges: target → list of sources.
    reverse: HashMap<ColumnRef, Vec<LineageEdge>>,
    /// All known columns (nodes in the graph).
    columns: HashSet<ColumnRef>,
}

impl LineageGraph {
    /// Create a new empty lineage graph.
    pub fn new() -> Self {
        Self {
            forward: HashMap::new(),
            reverse: HashMap::new(),
            columns: HashSet::new(),
        }
    }

    /// Add a lineage edge: source column feeds into target column.
    pub fn add_edge(
        &mut self,
        source: ColumnRef,
        target: ColumnRef,
        transform_type: TransformType,
    ) {
        self.columns.insert(source.clone());
        self.columns.insert(target.clone());

        let edge = LineageEdge {
            source: source.clone(),
            target: target.clone(),
            transform_type,
        };

        self.forward.entry(source).or_default().push(edge.clone());

        self.reverse.entry(target).or_default().push(edge);
    }

    /// Trace upstream: "Where does this column come from?"
    ///
    /// Returns all source columns that transitively feed into the given column,
    /// along with the edges traversed.
    pub fn trace_upstream(&self, column: &ColumnRef) -> LineageTrace {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut edges = Vec::new();

        visited.insert(column.clone());
        queue.push_back(column.clone());

        while let Some(current) = queue.pop_front() {
            if let Some(incoming) = self.reverse.get(&current) {
                for edge in incoming {
                    edges.push(edge.clone());
                    if visited.insert(edge.source.clone()) {
                        queue.push_back(edge.source.clone());
                    }
                }
            }
        }

        let sources: Vec<ColumnRef> = visited.into_iter().filter(|c| c != column).collect();

        LineageTrace {
            origin: column.clone(),
            direction: TraceDirection::Upstream,
            columns: sources,
            edges,
        }
    }

    /// Trace downstream: "What depends on this column?"
    ///
    /// Returns all columns that are transitively derived from the given column.
    pub fn trace_downstream(&self, column: &ColumnRef) -> LineageTrace {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut edges = Vec::new();

        visited.insert(column.clone());
        queue.push_back(column.clone());

        while let Some(current) = queue.pop_front() {
            if let Some(outgoing) = self.forward.get(&current) {
                for edge in outgoing {
                    edges.push(edge.clone());
                    if visited.insert(edge.target.clone()) {
                        queue.push_back(edge.target.clone());
                    }
                }
            }
        }

        let dependents: Vec<ColumnRef> = visited.into_iter().filter(|c| c != column).collect();

        LineageTrace {
            origin: column.clone(),
            direction: TraceDirection::Downstream,
            columns: dependents,
            edges,
        }
    }

    /// Get all columns for a given qualifier (table name or task id).
    ///
    /// Matches both `ColumnSource::Table(name)` and
    /// `ColumnSource::Task(task)` whose `task_id` equals the supplied
    /// value — convenient for callers that don't care about the
    /// distinction (visualisation, "all columns on X").
    pub fn columns_for_task(&self, qualifier: &str) -> Vec<&ColumnRef> {
        self.columns
            .iter()
            .filter(|c| match &c.source {
                ColumnSource::Table(t) => t == qualifier,
                ColumnSource::Task(t) => t.task_id == qualifier,
            })
            .collect()
    }

    /// Get all unique qualifier names (tables + tasks) in the graph.
    pub fn tasks(&self) -> HashSet<String> {
        self.columns.iter().map(|c| c.qualifier()).collect()
    }

    /// Get all edges in the graph.
    pub fn all_edges(&self) -> Vec<&LineageEdge> {
        self.forward.values().flat_map(|v| v.iter()).collect()
    }

    /// Total number of columns (nodes).
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Total number of edges.
    pub fn edge_count(&self) -> usize {
        self.forward.values().map(|v| v.len()).sum()
    }

    /// Export the graph as a serializable structure for visualization.
    pub fn to_visualization_data(&self) -> serde_json::Value {
        let nodes: Vec<serde_json::Value> = self
            .columns
            .iter()
            .map(|col| {
                serde_json::json!({
                    "id": col.to_string(),
                    "task": col.qualifier(),
                    "column": col.column_name,
                    "kind": match &col.source {
                        ColumnSource::Table(_) => "table",
                        ColumnSource::Task(_) => "task",
                    },
                })
            })
            .collect();

        let edges: Vec<serde_json::Value> = self
            .all_edges()
            .iter()
            .map(|edge| {
                serde_json::json!({
                    "source": edge.source.to_string(),
                    "target": edge.target.to_string(),
                    "transform": format!("{:?}", edge.transform_type),
                })
            })
            .collect();

        serde_json::json!({
            "nodes": nodes,
            "edges": edges,
            "tasks": self.tasks().into_iter().collect::<Vec<_>>(),
            "column_count": self.column_count(),
            "edge_count": self.edge_count(),
        })
    }
}

impl Default for LineageGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// The result of a lineage trace.
#[derive(Debug)]
pub struct LineageTrace {
    /// The column that was traced.
    pub origin: ColumnRef,
    /// Direction of the trace.
    pub direction: TraceDirection,
    /// All columns found in the trace (excluding origin).
    pub columns: Vec<ColumnRef>,
    /// All edges traversed.
    pub edges: Vec<LineageEdge>,
}

#[derive(Debug)]
pub enum TraceDirection {
    Upstream,
    Downstream,
}

impl std::fmt::Display for LineageTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let dir = match self.direction {
            TraceDirection::Upstream => "Upstream",
            TraceDirection::Downstream => "Downstream",
        };
        writeln!(f, "{} trace for {}:", dir, self.origin)?;
        writeln!(f, "  {} columns found:", self.columns.len())?;
        for col in &self.columns {
            writeln!(f, "    - {}", col)?;
        }
        writeln!(f, "  {} edges traversed:", self.edges.len())?;
        for edge in &self.edges {
            writeln!(
                f,
                "    {} -> {} ({:?})",
                edge.source, edge.target, edge.transform_type
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_etl_graph() -> LineageGraph {
        let mut g = LineageGraph::new();

        // extract_orders produces: id, customer_id, total, order_date
        // transform reads: orders.id, orders.customer_id, orders.total, customers.name
        //   and produces: order_id, customer_name, total_amount
        // aggregate reads: transform.total_amount
        //   and produces: daily_total

        // extract_orders.id → transform.order_id (direct)
        g.add_edge(
            ColumnRef::new("extract_orders", "id"),
            ColumnRef::new("transform", "order_id"),
            TransformType::Direct,
        );

        // extract_orders.total → transform.total_amount (direct)
        g.add_edge(
            ColumnRef::new("extract_orders", "total"),
            ColumnRef::new("transform", "total_amount"),
            TransformType::Direct,
        );

        // extract_customers.name → transform.customer_name (direct)
        g.add_edge(
            ColumnRef::new("extract_customers", "name"),
            ColumnRef::new("transform", "customer_name"),
            TransformType::Direct,
        );

        // extract_orders.customer_id → transform (join key)
        g.add_edge(
            ColumnRef::new("extract_orders", "customer_id"),
            ColumnRef::new("transform", "customer_name"),
            TransformType::JoinKey,
        );

        // transform.total_amount → aggregate.daily_total (aggregation)
        g.add_edge(
            ColumnRef::new("transform", "total_amount"),
            ColumnRef::new("aggregate", "daily_total"),
            TransformType::Aggregation("SUM".to_string()),
        );

        g
    }

    #[test]
    fn upstream_trace() {
        let g = build_etl_graph();

        let trace = g.trace_upstream(&ColumnRef::new("aggregate", "daily_total"));

        // daily_total ← transform.total_amount ← extract_orders.total
        assert!(trace
            .columns
            .iter()
            .any(|c| c.qualifier() == "transform" && c.column_name == "total_amount"));
        assert!(trace
            .columns
            .iter()
            .any(|c| c.qualifier() == "extract_orders" && c.column_name == "total"));
        assert_eq!(trace.edges.len(), 2);
    }

    #[test]
    fn downstream_trace() {
        let g = build_etl_graph();

        let trace = g.trace_downstream(&ColumnRef::new("extract_orders", "total"));

        // total → transform.total_amount → aggregate.daily_total
        assert!(trace
            .columns
            .iter()
            .any(|c| c.qualifier() == "transform" && c.column_name == "total_amount"));
        assert!(trace
            .columns
            .iter()
            .any(|c| c.qualifier() == "aggregate" && c.column_name == "daily_total"));
    }

    #[test]
    fn columns_for_task() {
        let g = build_etl_graph();
        let transform_cols = g.columns_for_task("transform");
        assert!(transform_cols.len() >= 3); // order_id, customer_name, total_amount
    }

    #[test]
    fn graph_stats() {
        let g = build_etl_graph();
        assert!(g.column_count() >= 7);
        assert_eq!(g.edge_count(), 5);
        assert!(g.tasks().len() >= 3);
    }

    #[test]
    fn visualization_data_is_valid_json() {
        let g = build_etl_graph();
        let viz = g.to_visualization_data();
        assert!(viz["nodes"].is_array());
        assert!(viz["edges"].is_array());
        assert!(viz["column_count"].as_u64().unwrap() >= 7);
    }

    #[test]
    fn isolated_column_has_no_lineage() {
        let g = build_etl_graph();

        // A column not in the graph
        let trace = g.trace_upstream(&ColumnRef::new("nonexistent", "col"));
        assert_eq!(trace.columns.len(), 0);
        assert_eq!(trace.edges.len(), 0);
    }
}
