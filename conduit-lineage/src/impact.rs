//! Schema change detection and impact analysis.
//!
//! Compares two versions of a schema (old vs new) and classifies
//! every change. Combined with the lineage graph, this answers
//! "If I change column X, what downstream tasks break?"

use serde::{Deserialize, Serialize};

use crate::lineage_graph::{ColumnRef, LineageGraph};
use crate::schema::Schema;

/// A detected change between two schema versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChange {
    /// What changed.
    pub kind: ChangeKind,
    /// The column affected.
    pub column_name: String,
    /// Human-readable description.
    pub description: String,
    /// Whether this is a breaking change.
    pub is_breaking: bool,
}

/// Types of schema changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeKind {
    /// A new column was added.
    ColumnAdded,
    /// An existing column was removed.
    ColumnRemoved,
    /// A column's type changed.
    TypeChanged { old_type: String, new_type: String },
    /// A column's nullability changed.
    NullabilityChanged { was_nullable: bool },
    /// A column was renamed (heuristic: same position + type, different name).
    ColumnRenamed { old_name: String },
    /// Column description or tags changed (non-breaking).
    MetadataChanged,
}

/// The full impact of schema changes.
#[derive(Debug)]
pub struct SchemaImpact {
    /// The task whose schema changed.
    pub task_id: String,
    /// All detected changes.
    pub changes: Vec<SchemaChange>,
    /// Downstream columns affected by breaking changes.
    pub affected_downstream: Vec<ColumnRef>,
    /// Number of breaking changes.
    pub breaking_count: usize,
    /// Number of non-breaking changes.
    pub non_breaking_count: usize,
}

/// Detects changes between two versions of a schema.
pub struct SchemaChangeDetector;

impl SchemaChangeDetector {
    /// Compare two schema versions and return all changes.
    pub fn diff(old: &Schema, new: &Schema) -> Vec<SchemaChange> {
        let mut changes = Vec::new();

        // Detect removed columns
        for old_col in &old.columns {
            if !new.has_column(&old_col.name) {
                // Check if it might be a rename (same position, same type)
                let old_pos = old.columns.iter().position(|c| c.name == old_col.name);
                let possible_rename = old_pos.and_then(|pos| {
                    new.columns.get(pos).filter(|new_col| {
                        new_col.column_type == old_col.column_type && !old.has_column(&new_col.name)
                    })
                });

                if let Some(renamed_to) = possible_rename {
                    changes.push(SchemaChange {
                        kind: ChangeKind::ColumnRenamed {
                            old_name: old_col.name.clone(),
                        },
                        column_name: renamed_to.name.clone(),
                        description: format!(
                            "Column '{}' renamed to '{}'",
                            old_col.name, renamed_to.name
                        ),
                        is_breaking: true, // Renames break downstream references
                    });
                } else {
                    changes.push(SchemaChange {
                        kind: ChangeKind::ColumnRemoved,
                        column_name: old_col.name.clone(),
                        description: format!("Column '{}' removed", old_col.name),
                        is_breaking: true,
                    });
                }
            }
        }

        // Detect added columns
        for new_col in &new.columns {
            if !old.has_column(&new_col.name) {
                // Skip if this was already captured as a rename
                let is_rename_target = changes.iter().any(|c| {
                    matches!(&c.kind, ChangeKind::ColumnRenamed { .. })
                        && c.column_name == new_col.name
                });

                if !is_rename_target {
                    changes.push(SchemaChange {
                        kind: ChangeKind::ColumnAdded,
                        column_name: new_col.name.clone(),
                        description: format!(
                            "Column '{}' added ({})",
                            new_col.name, new_col.column_type
                        ),
                        is_breaking: false, // Adding columns is generally safe
                    });
                }
            }
        }

        // Detect type and nullability changes
        for old_col in &old.columns {
            if let Some(new_col) = new.get_column(&old_col.name) {
                // Type change
                if old_col.column_type != new_col.column_type {
                    let is_breaking =
                        !Self::is_safe_type_widening(&old_col.column_type, &new_col.column_type);

                    changes.push(SchemaChange {
                        kind: ChangeKind::TypeChanged {
                            old_type: format!("{}", old_col.column_type),
                            new_type: format!("{}", new_col.column_type),
                        },
                        column_name: old_col.name.clone(),
                        description: format!(
                            "Column '{}' type changed: {} → {}",
                            old_col.name, old_col.column_type, new_col.column_type
                        ),
                        is_breaking,
                    });
                }

                // Nullability change
                if old_col.nullable != new_col.nullable {
                    let is_breaking = !new_col.nullable; // NOT NULL → NULL is safe; NULL → NOT NULL breaks
                    changes.push(SchemaChange {
                        kind: ChangeKind::NullabilityChanged {
                            was_nullable: old_col.nullable,
                        },
                        column_name: old_col.name.clone(),
                        description: format!(
                            "Column '{}' nullability changed: {} → {}",
                            old_col.name,
                            if old_col.nullable { "NULL" } else { "NOT NULL" },
                            if new_col.nullable { "NULL" } else { "NOT NULL" },
                        ),
                        is_breaking,
                    });
                }

                // Metadata changes (non-breaking)
                if old_col.description != new_col.description || old_col.tags != new_col.tags {
                    changes.push(SchemaChange {
                        kind: ChangeKind::MetadataChanged,
                        column_name: old_col.name.clone(),
                        description: format!("Column '{}' metadata updated", old_col.name),
                        is_breaking: false,
                    });
                }
            }
        }

        changes
    }

    /// Check if a type change is a safe widening (e.g., INT → BIGINT).
    fn is_safe_type_widening(
        old: &crate::schema::ColumnType,
        new: &crate::schema::ColumnType,
    ) -> bool {
        use crate::schema::ColumnType;
        matches!(
            (old, new),
            // Integer → Float is a widening
            (ColumnType::Integer, ColumnType::Float)
            // Float → Decimal is a widening
            | (ColumnType::Float, ColumnType::Decimal { .. })
            // Date → Timestamp is a widening
            | (ColumnType::Date, ColumnType::Timestamp)
            // String → Json is arguable, but generally safe
            | (ColumnType::String, ColumnType::Json)
        )
    }

    /// Analyze the full impact of schema changes using the lineage graph.
    ///
    /// For each breaking change, traces downstream through the lineage graph
    /// to find all affected columns across all tasks.
    pub fn analyze_impact(
        old_schema: &Schema,
        new_schema: &Schema,
        lineage: &LineageGraph,
    ) -> SchemaImpact {
        let changes = Self::diff(old_schema, new_schema);

        let mut affected_downstream = Vec::new();
        let task_id = &old_schema.task_id;

        for change in &changes {
            if !change.is_breaking {
                continue;
            }

            // For breaking changes, trace downstream from this column
            let col_name = match &change.kind {
                ChangeKind::ColumnRenamed { old_name } => old_name.clone(),
                _ => change.column_name.clone(),
            };

            let col_ref = ColumnRef::new(task_id.clone(), col_name);
            let trace = lineage.trace_downstream(&col_ref);

            for downstream_col in trace.columns {
                if !affected_downstream.contains(&downstream_col) {
                    affected_downstream.push(downstream_col);
                }
            }
        }

        let breaking_count = changes.iter().filter(|c| c.is_breaking).count();
        let non_breaking_count = changes.len() - breaking_count;

        SchemaImpact {
            task_id: task_id.clone(),
            changes,
            affected_downstream,
            breaking_count,
            non_breaking_count,
        }
    }
}

impl std::fmt::Display for SchemaImpact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Schema Impact for '{}':", self.task_id)?;
        writeln!(
            f,
            "  {} changes ({} breaking, {} non-breaking)",
            self.changes.len(),
            self.breaking_count,
            self.non_breaking_count,
        )?;

        if !self.changes.is_empty() {
            writeln!(f)?;
            writeln!(f, "  Changes:")?;
            for change in &self.changes {
                let marker = if change.is_breaking {
                    "BREAKING"
                } else {
                    "safe"
                };
                writeln!(f, "    [{}] {}", marker, change.description)?;
            }
        }

        if !self.affected_downstream.is_empty() {
            writeln!(f)?;
            writeln!(
                f,
                "  Downstream blast radius: {} columns affected",
                self.affected_downstream.len()
            )?;
            for col in &self.affected_downstream {
                writeln!(f, "    - {}", col)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lineage_graph::{LineageGraph, TransformType};
    use crate::schema::{Column, ColumnType, Schema};

    fn orders_schema_v1() -> Schema {
        Schema::new(
            "extract_orders",
            vec![
                Column::new("id", ColumnType::Integer).not_null(),
                Column::new("customer_id", ColumnType::Integer).not_null(),
                Column::new(
                    "total",
                    ColumnType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                ),
                Column::new("order_date", ColumnType::Date),
                Column::new("status", ColumnType::String),
            ],
        )
    }

    #[test]
    fn no_changes_detected_for_identical_schemas() {
        let schema = orders_schema_v1();
        let changes = SchemaChangeDetector::diff(&schema, &schema);
        assert!(changes.is_empty());
    }

    #[test]
    fn added_column_detected() {
        let old = orders_schema_v1();
        let mut new = old.clone();
        new.columns
            .push(Column::new("shipping_address", ColumnType::String));

        let changes = SchemaChangeDetector::diff(&old, &new);

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].kind, ChangeKind::ColumnAdded);
        assert!(!changes[0].is_breaking);
    }

    #[test]
    fn removed_column_is_breaking() {
        let old = orders_schema_v1();
        let mut new = old.clone();
        new.columns.retain(|c| c.name != "status");

        let changes = SchemaChangeDetector::diff(&old, &new);

        let removed = changes
            .iter()
            .find(|c| c.kind == ChangeKind::ColumnRemoved)
            .unwrap();
        assert_eq!(removed.column_name, "status");
        assert!(removed.is_breaking);
    }

    #[test]
    fn type_change_detected() {
        let old = orders_schema_v1();
        let mut new = old.clone();
        // Change total from Decimal to String — breaking
        if let Some(col) = new.columns.iter_mut().find(|c| c.name == "total") {
            col.column_type = ColumnType::String;
        }

        let changes = SchemaChangeDetector::diff(&old, &new);

        let type_change = changes
            .iter()
            .find(|c| matches!(c.kind, ChangeKind::TypeChanged { .. }))
            .unwrap();
        assert!(type_change.is_breaking);
    }

    #[test]
    fn safe_type_widening_is_not_breaking() {
        let old = Schema::new("task", vec![Column::new("count", ColumnType::Integer)]);
        let new = Schema::new("task", vec![Column::new("count", ColumnType::Float)]);

        let changes = SchemaChangeDetector::diff(&old, &new);

        assert_eq!(changes.len(), 1);
        assert!(!changes[0].is_breaking); // INT → FLOAT is safe
    }

    #[test]
    fn nullability_tightening_is_breaking() {
        let old = Schema::new(
            "task",
            vec![
                Column::new("name", ColumnType::String), // nullable by default
            ],
        );
        let new = Schema::new(
            "task",
            vec![Column::new("name", ColumnType::String).not_null()],
        );

        let changes = SchemaChangeDetector::diff(&old, &new);

        let null_change = changes
            .iter()
            .find(|c| matches!(c.kind, ChangeKind::NullabilityChanged { .. }))
            .unwrap();
        assert!(null_change.is_breaking); // NULL → NOT NULL breaks inserts
    }

    #[test]
    fn nullability_loosening_is_safe() {
        let old = Schema::new(
            "task",
            vec![Column::new("name", ColumnType::String).not_null()],
        );
        let new = Schema::new(
            "task",
            vec![
                Column::new("name", ColumnType::String), // nullable
            ],
        );

        let changes = SchemaChangeDetector::diff(&old, &new);

        let null_change = changes
            .iter()
            .find(|c| matches!(c.kind, ChangeKind::NullabilityChanged { .. }))
            .unwrap();
        assert!(!null_change.is_breaking); // NOT NULL → NULL is safe
    }

    #[test]
    fn rename_heuristic_detects_positional_match() {
        let old = Schema::new(
            "task",
            vec![
                Column::new("user_name", ColumnType::String),
                Column::new("age", ColumnType::Integer),
            ],
        );
        let new = Schema::new(
            "task",
            vec![
                Column::new("display_name", ColumnType::String), // same position + type
                Column::new("age", ColumnType::Integer),
            ],
        );

        let changes = SchemaChangeDetector::diff(&old, &new);

        let rename = changes
            .iter()
            .find(|c| matches!(&c.kind, ChangeKind::ColumnRenamed { .. }))
            .expect("should detect rename");
        assert!(rename.is_breaking);
        assert_eq!(rename.column_name, "display_name");
    }

    #[test]
    fn impact_analysis_traces_downstream() {
        let old = Schema::new(
            "extract_orders",
            vec![Column::new(
                "total",
                ColumnType::Decimal {
                    precision: 10,
                    scale: 2,
                },
            )],
        );
        let new = Schema::new(
            "extract_orders",
            vec![
                // Removed "total" column
            ],
        );

        // Build a lineage graph
        let mut lineage = LineageGraph::new();
        lineage.add_edge(
            ColumnRef::new("extract_orders", "total"),
            ColumnRef::new("transform", "total_amount"),
            TransformType::Direct,
        );
        lineage.add_edge(
            ColumnRef::new("transform", "total_amount"),
            ColumnRef::new("aggregate", "daily_total"),
            TransformType::Aggregation("SUM".to_string()),
        );

        let impact = SchemaChangeDetector::analyze_impact(&old, &new, &lineage);

        assert_eq!(impact.breaking_count, 1);
        assert_eq!(impact.affected_downstream.len(), 2); // transform.total_amount + aggregate.daily_total

        let affected_tasks: Vec<&str> = impact
            .affected_downstream
            .iter()
            .map(|c| c.task_id.as_str())
            .collect();
        assert!(affected_tasks.contains(&"transform"));
        assert!(affected_tasks.contains(&"aggregate"));
    }
}
