//! Render [`PlanImpact`] as PR-ready markdown or as a JSON string.
//!
//! Designed to be reused by both the `conduit impact` CLI and the
//! `/api/v1/impact` API handler, so the bot's PR comment and the API
//! consumer see the same wire format.

use std::fmt::Write;

use crate::impact::{ChangeKind, SchemaChange};
use crate::plan_impact::{PlanImpact, TaskImpact};

/// Output format selector for the impact report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImpactFormat {
    Markdown,
    Json,
}

impl ImpactFormat {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "markdown" | "md" => Ok(Self::Markdown),
            "json" => Ok(Self::Json),
            other => Err(format!(
                "unknown impact format '{}': expected 'markdown' or 'json'",
                other
            )),
        }
    }
}

/// Serialize a [`PlanImpact`] to a UTF-8 string in the requested format.
pub fn render(impact: &PlanImpact, format: ImpactFormat) -> String {
    match format {
        ImpactFormat::Markdown => render_markdown(impact),
        ImpactFormat::Json => {
            serde_json::to_string_pretty(impact).unwrap_or_else(|_| "{}".to_string())
        }
    }
}

/// Produces the markdown body suitable for a GitHub PR comment.
pub fn render_markdown(impact: &PlanImpact) -> String {
    let mut out = String::new();
    let s = &impact.summary;

    if s.tasks_changed == 0 && s.tasks_added == 0 && s.tasks_removed == 0 {
        let _ = writeln!(out, "## Schema impact: no changes detected");
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "_Compared {} task outputs across {} DAGs — every output is identical._",
            s.tasks_compared, impact.coverage.total_dags
        );
        return out;
    }

    // Header line summarising scale.
    let _ = writeln!(
        out,
        "## Schema impact: {} breaking, {} safe",
        s.total_breaking_changes, s.total_non_breaking_changes
    );
    let _ = writeln!(out);

    // High-level summary row.
    let _ =
        writeln!(
        out,
        "_{} task output{} changed across {} DAG{}. {} downstream column{} potentially affected._",
        s.tasks_changed,
        if s.tasks_changed == 1 { "" } else { "s" },
        impact.coverage.total_dags,
        if impact.coverage.total_dags == 1 { "" } else { "s" },
        s.total_downstream_columns_affected,
        if s.total_downstream_columns_affected == 1 { "" } else { "s" },
    );
    let _ = writeln!(out);

    // Tasks added / removed at the DAG level (separate from per-dataset changes).
    if s.tasks_added > 0 || s.tasks_removed > 0 {
        let _ = writeln!(
            out,
            "**Tasks:** {} added, {} removed.",
            s.tasks_added, s.tasks_removed
        );
        let _ = writeln!(out);
    }

    // Per-task sections.
    for entry in &impact.per_task {
        render_task_section(&mut out, entry);
    }

    // Coverage footer — honest about under-reporting.
    let _ = writeln!(out, "---");
    let _ = writeln!(out);
    render_coverage_footer(&mut out, impact);

    out
}

fn render_task_section(out: &mut String, entry: &TaskImpact) {
    let kind_tag = if entry.dataset_removed {
        " · removed"
    } else if entry.dataset_added {
        " · added"
    } else {
        ""
    };

    let _ = writeln!(
        out,
        "### `{}` → `{}`{}",
        entry.task_id, entry.dataset_name, kind_tag
    );
    let _ = writeln!(out, "_DAG: `{}`_", entry.dag_id);
    let _ = writeln!(out);

    if entry.changes.is_empty() {
        let _ = writeln!(out, "(no column changes)");
        let _ = writeln!(out);
        return;
    }

    for change in &entry.changes {
        let _ = writeln!(out, "- {}", render_change(change));
    }
    let _ = writeln!(out);

    if !entry.affected_downstream.is_empty() {
        let breaking = entry.breaking_count();
        let _ = writeln!(
            out,
            "**Downstream blast radius — {} column{} affected by {} breaking change{}:**",
            entry.affected_downstream.len(),
            if entry.affected_downstream.len() == 1 {
                ""
            } else {
                "s"
            },
            breaking,
            if breaking == 1 { "" } else { "s" },
        );
        // Cap to first 25 to keep the comment legible; mention overflow.
        const MAX_LISTED: usize = 25;
        for d in entry.affected_downstream.iter().take(MAX_LISTED) {
            let _ = writeln!(out, "- `{}.{}.{}`", d.dag_id, d.task_id, d.column);
        }
        if entry.affected_downstream.len() > MAX_LISTED {
            let _ = writeln!(
                out,
                "- _… and {} more_",
                entry.affected_downstream.len() - MAX_LISTED
            );
        }
        let _ = writeln!(out);
    } else if entry.breaking_count() > 0 && !entry.dataset_removed {
        let _ = writeln!(
            out,
            "_No downstream consumers traced — either this dataset has no declared consumers, or the change is on a removed column that can't be traced forward._"
        );
        let _ = writeln!(out);
    }
}

fn render_change(change: &SchemaChange) -> String {
    let marker = if change.is_breaking { "💥" } else { "✅" };
    let kind = match &change.kind {
        ChangeKind::ColumnAdded => "added".to_string(),
        ChangeKind::ColumnRemoved => "removed".to_string(),
        ChangeKind::TypeChanged { old_type, new_type } => {
            format!("type `{}` → `{}`", old_type, new_type)
        }
        ChangeKind::NullabilityChanged { was_nullable } => {
            if *was_nullable {
                "nullability NULL → NOT NULL".to_string()
            } else {
                "nullability NOT NULL → NULL".to_string()
            }
        }
        ChangeKind::ColumnRenamed { old_name } => {
            format!("renamed from `{}`", old_name)
        }
        ChangeKind::MetadataChanged => "metadata updated".to_string(),
    };
    format!("{} `{}` — {}", marker, change.column_name, kind)
}

fn render_coverage_footer(out: &mut String, impact: &PlanImpact) {
    let cov = &impact.coverage;
    let coverage_quality = if cov.head_unresolved_refs == 0 {
        "✅ Lineage coverage: fully resolved"
    } else {
        "⚠️ Lineage coverage: partial"
    };
    let _ = writeln!(out, "**{}**", coverage_quality);
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "- {}/{} DAG{} opted in to `lineage_strict = true`",
        cov.strict_dags,
        cov.total_dags,
        if cov.total_dags == 1 { "" } else { "s" }
    );
    if cov.head_unresolved_refs > 0 {
        let _ = writeln!(
            out,
            "- {} consumer column reference{} could not be resolved to a producer — downstream tracing may under-report",
            cov.head_unresolved_refs,
            if cov.head_unresolved_refs == 1 { "" } else { "s" }
        );
    }
    let _ = writeln!(
        out,
        "- Apply label `allow-breaking` on this PR if you intend to merge with breaking changes."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impact::SchemaChange;
    use crate::plan_impact::{
        DownstreamColumn, LineageCoverage, PlanImpact, PlanImpactSummary, TaskImpact,
    };

    fn breaking_change(name: &str) -> SchemaChange {
        SchemaChange {
            kind: ChangeKind::ColumnRemoved,
            column_name: name.to_string(),
            description: format!("Column '{}' removed", name),
            is_breaking: true,
        }
    }

    fn safe_change(name: &str) -> SchemaChange {
        SchemaChange {
            kind: ChangeKind::ColumnAdded,
            column_name: name.to_string(),
            description: format!("Column '{}' added", name),
            is_breaking: false,
        }
    }

    #[test]
    fn renders_clean_when_no_changes() {
        let empty = PlanImpact {
            per_task: vec![],
            summary: PlanImpactSummary {
                tasks_compared: 5,
                ..Default::default()
            },
            coverage: LineageCoverage {
                total_dags: 2,
                strict_dags: 1,
                head_unresolved_refs: 0,
            },
        };
        let md = render_markdown(&empty);
        assert!(md.contains("no changes detected"));
        assert!(md.contains("Compared 5 task outputs"));
    }

    #[test]
    fn renders_breaking_change_with_downstream() {
        let impact = PlanImpact {
            per_task: vec![TaskImpact {
                dag_id: "warehouse".to_string(),
                task_id: "extract_orders".to_string(),
                dataset_name: "staging.orders".to_string(),
                changes: vec![breaking_change("amount")],
                affected_downstream: vec![
                    DownstreamColumn {
                        dag_id: "warehouse".to_string(),
                        task_id: "transform".to_string(),
                        column: "amount".to_string(),
                    },
                    DownstreamColumn {
                        dag_id: "warehouse".to_string(),
                        task_id: "load".to_string(),
                        column: "total".to_string(),
                    },
                ],
                dataset_removed: false,
                dataset_added: false,
            }],
            summary: PlanImpactSummary {
                tasks_compared: 1,
                tasks_changed: 1,
                total_breaking_changes: 1,
                total_downstream_columns_affected: 2,
                ..Default::default()
            },
            coverage: LineageCoverage {
                total_dags: 1,
                strict_dags: 1,
                head_unresolved_refs: 0,
            },
        };

        let md = render_markdown(&impact);
        assert!(md.contains("1 breaking, 0 safe"));
        assert!(md.contains("staging.orders"));
        assert!(md.contains("💥"));
        assert!(md.contains("`amount`"));
        assert!(md.contains("Downstream blast radius"));
        assert!(md.contains("warehouse.transform.amount"));
        assert!(md.contains("warehouse.load.total"));
        assert!(md.contains("✅ Lineage coverage: fully resolved"));
    }

    #[test]
    fn flags_partial_coverage_when_unresolved_refs_present() {
        let impact = PlanImpact {
            per_task: vec![TaskImpact {
                dag_id: "d".to_string(),
                task_id: "t".to_string(),
                dataset_name: "ds".to_string(),
                changes: vec![safe_change("x")],
                affected_downstream: vec![],
                dataset_removed: false,
                dataset_added: false,
            }],
            summary: PlanImpactSummary {
                tasks_compared: 1,
                tasks_changed: 1,
                total_non_breaking_changes: 1,
                ..Default::default()
            },
            coverage: LineageCoverage {
                total_dags: 3,
                strict_dags: 0,
                head_unresolved_refs: 4,
            },
        };
        let md = render_markdown(&impact);
        assert!(md.contains("⚠️ Lineage coverage: partial"));
        assert!(md.contains("4 consumer column reference"));
        assert!(md.contains("0/3 DAGs opted in"));
    }

    #[test]
    fn caps_downstream_list_at_25() {
        let downstream: Vec<DownstreamColumn> = (0..40)
            .map(|i| DownstreamColumn {
                dag_id: "d".to_string(),
                task_id: format!("t{:02}", i),
                column: "col".to_string(),
            })
            .collect();
        let impact = PlanImpact {
            per_task: vec![TaskImpact {
                dag_id: "d".to_string(),
                task_id: "src".to_string(),
                dataset_name: "ds".to_string(),
                changes: vec![breaking_change("c")],
                affected_downstream: downstream,
                dataset_removed: false,
                dataset_added: false,
            }],
            summary: PlanImpactSummary {
                tasks_compared: 1,
                tasks_changed: 1,
                total_breaking_changes: 1,
                total_downstream_columns_affected: 40,
                ..Default::default()
            },
            coverage: LineageCoverage::default(),
        };
        let md = render_markdown(&impact);
        assert!(md.contains("… and 15 more"));
        // Make sure exactly 25 of the entries are present.
        let listed = md.matches("- `d.t").count();
        assert_eq!(listed, 25);
    }

    #[test]
    fn json_format_round_trips() {
        let impact = PlanImpact {
            per_task: vec![],
            summary: PlanImpactSummary::default(),
            coverage: LineageCoverage::default(),
        };
        let json = render(&impact, ImpactFormat::Json);
        let back: PlanImpact = serde_json::from_str(&json).unwrap();
        assert_eq!(back.per_task.len(), 0);
    }

    #[test]
    fn format_parse_handles_aliases() {
        assert_eq!(
            ImpactFormat::parse("markdown").unwrap(),
            ImpactFormat::Markdown
        );
        assert_eq!(ImpactFormat::parse("MD").unwrap(), ImpactFormat::Markdown);
        assert_eq!(ImpactFormat::parse("JSON").unwrap(), ImpactFormat::Json);
        assert!(ImpactFormat::parse("yaml").is_err());
    }
}
