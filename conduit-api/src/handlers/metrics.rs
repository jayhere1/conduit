//! Metric exploration endpoints.
//!
//! Provides demo metric data sourced from contract check definitions
//! and seeded run history. In production these would come from the
//! executor's evidence store.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};

use conduit_compiler::ConduitPlan;

use crate::AppState;

/// GET /api/v1/metrics — list all metrics across all DAGs.
///
/// Returns metrics derived from contract definitions (what metrics
/// tasks are expected to emit) plus simulated historical values
/// for demo purposes.
pub async fn list_metrics(State(state): State<Arc<AppState>>) -> Json<Value> {
    let plan = match ConduitPlan::compile(&state.dags_path) {
        Ok((plan, _)) => plan,
        Err(e) => {
            return Json(json!({
                "error": format!("Compilation error: {}", e),
                "metrics": [],
            }));
        }
    };

    let mut metrics = Vec::new();

    for (dag_id, dag) in &plan.dags {
        for (task_id, task) in &dag.tasks {
            if let Some(tc) = &task.contracts {
                for check in &tc.checks {
                    // Extract metric-bearing contracts
                    let check_str = format!("{:?}", check.check);
                    let metric_name = extract_metric_name(&check_str, &check.name);
                    if let Some(name) = metric_name {
                        let demo_values = generate_demo_values(&name, dag_id, task_id);
                        metrics.push(json!({
                            "dagId": dag_id,
                            "taskId": task_id,
                            "metricName": name,
                            "description": check.description,
                            "severity": format!("{:?}", check.severity).to_lowercase(),
                            "currentValue": demo_values.last().copied().unwrap_or(0.0),
                            "history": demo_values.iter().enumerate().map(|(i, v)| json!({
                                "day": format!("day-{}", 7 - i),
                                "value": v,
                            })).collect::<Vec<_>>(),
                            "trend": compute_trend(&demo_values),
                        }));
                    }
                }
            }
        }
    }

    Json(json!({
        "metrics": metrics,
        "total": metrics.len(),
    }))
}

/// GET /api/v1/metrics/:dag_id/:task_id — get metrics for a specific task.
pub async fn get_task_metrics(
    State(state): State<Arc<AppState>>,
    Path((dag_id, task_id)): Path<(String, String)>,
) -> Json<Value> {
    let plan = match ConduitPlan::compile(&state.dags_path) {
        Ok((plan, _)) => plan,
        Err(e) => {
            return Json(json!({ "error": format!("Compilation error: {}", e) }));
        }
    };

    let dag = match plan.dags.get(&dag_id) {
        Some(d) => d,
        None => return Json(json!({ "error": format!("DAG '{}' not found", dag_id) })),
    };

    let task = match dag.tasks.get(&task_id) {
        Some(t) => t,
        None => return Json(json!({ "error": format!("Task '{}' not found", task_id) })),
    };

    let mut metrics = Vec::new();

    if let Some(tc) = &task.contracts {
        for check in &tc.checks {
            let check_str = format!("{:?}", check.check);
            let metric_name = extract_metric_name(&check_str, &check.name);
            if let Some(name) = metric_name {
                let demo_values = generate_demo_values(&name, &dag_id, &task_id);
                let current_value = demo_values.last().copied().unwrap_or(0.0);
                let history: Vec<serde_json::Value> = demo_values
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        json!({
                            "day": format!("day-{}", 7 - i),
                            "value": v,
                        })
                    })
                    .collect();
                let trend = compute_trend(&demo_values);
                let min_val = demo_values.iter().cloned().fold(f64::INFINITY, f64::min);
                let max_val = demo_values
                    .iter()
                    .cloned()
                    .fold(f64::NEG_INFINITY, f64::max);
                let avg_val = demo_values.iter().sum::<f64>() / demo_values.len() as f64;
                let variance = demo_values
                    .iter()
                    .map(|v| (v - avg_val).powi(2))
                    .sum::<f64>()
                    / demo_values.len() as f64;
                let stddev = variance.sqrt();
                metrics.push(json!({
                    "metricName": name,
                    "description": check.description,
                    "severity": format!("{:?}", check.severity).to_lowercase(),
                    "currentValue": current_value,
                    "history": history,
                    "trend": trend,
                    "stats": {"min": min_val, "max": max_val, "avg": avg_val, "stddev": stddev},
                }));
            }
        }
    }

    Json(json!({
        "dagId": dag_id,
        "taskId": task_id,
        "metrics": metrics,
    }))
}

/// Extract a metric name from a contract check's debug representation.
fn extract_metric_name(check_str: &str, _fallback_name: &str) -> Option<String> {
    // Metric contracts have an explicit metric_name
    if check_str.starts_with("Metric") {
        // Parse metric_name from the debug string
        if let Some(start) = check_str.find("metric_name:") {
            let rest = &check_str[start + 13..];
            let name = rest
                .split([',', '}', '"'])
                .next()
                .map(|s| s.trim().trim_matches('"').to_string());
            return name.filter(|n| !n.is_empty());
        }
    }

    // RowCount, Freshness, etc. emit standard metrics
    if check_str.starts_with("RowCount") {
        return Some("row_count".to_string());
    }
    if check_str.starts_with("Freshness") {
        return Some("data_age_seconds".to_string());
    }
    if check_str.starts_with("Unique") {
        return Some("duplicate_count".to_string());
    }
    if check_str.starts_with("RowCountDelta") {
        return Some("row_count_delta_pct".to_string());
    }

    // Other contract types don't have standalone metrics
    None
}

/// Generate deterministic demo metric values based on the metric name.
/// Returns 7 values (one per day).
fn generate_demo_values(metric_name: &str, dag_id: &str, task_id: &str) -> Vec<f64> {
    // Use a simple hash for deterministic but varied values
    let seed: u64 = metric_name
        .bytes()
        .chain(dag_id.bytes())
        .chain(task_id.bytes())
        .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));

    match metric_name {
        "row_count" => {
            let base = match dag_id {
                "ecommerce_analytics" => match task_id {
                    "extract_orders" => 185000.0,
                    "extract_clickstream" => 12500000.0,
                    "extract_products" => 15200.0,
                    "extract_customers" => 42000.0,
                    _ => 50000.0,
                },
                "saas_metrics" => match task_id {
                    "extract_subscriptions" => 8500.0,
                    "extract_billing_events" => 32000.0,
                    _ => 5000.0,
                },
                "ml_feature_pipeline" => 45000.0,
                _ => 10000.0,
            };
            (0..7)
                .map(|i| {
                    let noise = ((seed.wrapping_add(i * 997)) % 100) as f64 / 100.0;
                    base * (0.92 + noise * 0.16) // ±8% variation
                })
                .collect()
        }
        "data_age_seconds" => {
            (0..7)
                .map(|i| {
                    let base = 3600.0 * 2.5; // ~2.5 hours
                    let noise = ((seed.wrapping_add(i * 773)) % 100) as f64 / 100.0;
                    base * (0.8 + noise * 0.4)
                })
                .collect()
        }
        "duplicate_count" => {
            (0..7)
                .map(|i| {
                    ((seed.wrapping_add(i * 557)) % 10) as f64 // 0-9 duplicates
                })
                .collect()
        }
        "row_count_delta_pct" => {
            (0..7)
                .map(|i| {
                    let noise = ((seed.wrapping_add(i * 443)) % 100) as f64 / 1000.0;
                    noise - 0.02 // -2% to +8%
                })
                .collect()
        }
        "enrichment_rate" | "currency_conversion_rate" | "session_attribution_rate" => {
            (0..7)
                .map(|i| {
                    let noise = ((seed.wrapping_add(i * 331)) % 50) as f64 / 1000.0;
                    0.96 + noise // 0.96 to 1.01 (capped)
                })
                .map(|v| v.min(1.0))
                .collect()
        }
        "referential_integrity_score" | "customer_match_rate" => (0..7)
            .map(|i| {
                let noise = ((seed.wrapping_add(i * 229)) % 30) as f64 / 1000.0;
                0.97 + noise
            })
            .map(|v| v.min(1.0))
            .collect(),
        "holdout_auc" | "training_auc" => {
            let base = if metric_name == "training_auc" {
                0.82
            } else {
                0.78
            };
            (0..7)
                .map(|i| {
                    let noise = ((seed.wrapping_add(i * 661)) % 60) as f64 / 1000.0;
                    base + noise - 0.02
                })
                .collect()
        }
        "holdout_precision" | "holdout_recall" | "holdout_f1" => {
            let base = match metric_name {
                "holdout_precision" => 0.72,
                "holdout_recall" => 0.68,
                _ => 0.70,
            };
            (0..7)
                .map(|i| {
                    let noise = ((seed.wrapping_add(i * 541)) % 80) as f64 / 1000.0;
                    base + noise - 0.03
                })
                .collect()
        }
        "psi_max" | "concept_drift_score" | "covariate_drift_score" => (0..7)
            .map(|i| {
                let noise = ((seed.wrapping_add(i * 881)) % 100) as f64 / 1000.0;
                0.02 + noise
            })
            .collect(),
        "demographic_parity_delta" | "equalized_odds_delta" => (0..7)
            .map(|i| {
                let noise = ((seed.wrapping_add(i * 113)) % 60) as f64 / 1000.0;
                0.02 + noise
            })
            .collect(),
        "logo_churn_rate" | "revenue_churn_rate" => {
            let base = if metric_name == "logo_churn_rate" {
                0.045
            } else {
                0.035
            };
            (0..7)
                .map(|i| {
                    let noise = ((seed.wrapping_add(i * 199)) % 30) as f64 / 1000.0;
                    base + noise - 0.01
                })
                .collect()
        }
        "total_mrr" => (0..7)
            .map(|i| {
                let base = 2_850_000.0;
                let growth = i as f64 * 12000.0;
                let noise = ((seed.wrapping_add(i * 751)) % 100) as f64 * 500.0;
                base + growth + noise
            })
            .collect(),
        "net_dollar_retention" => (0..7)
            .map(|i| {
                let noise = ((seed.wrapping_add(i * 347)) % 40) as f64 / 1000.0;
                1.08 + noise
            })
            .collect(),
        "month_1_retention" => (0..7)
            .map(|i| {
                let noise = ((seed.wrapping_add(i * 421)) % 50) as f64 / 1000.0;
                0.88 + noise
            })
            .map(|v| v.min(1.0))
            .collect(),
        "total_daily_revenue" => (0..7)
            .map(|i| {
                let base = 485000.0;
                let noise = ((seed.wrapping_add(i * 503)) % 100) as f64 * 2500.0;
                base + noise
            })
            .collect(),
        "avg_lifetime_value" => (0..7)
            .map(|i| {
                let base = 342.0;
                let noise = ((seed.wrapping_add(i * 617)) % 40) as f64;
                base + noise - 15.0
            })
            .collect(),
        _ => {
            // Generic metric: produce reasonable positive values
            (0..7)
                .map(|i| {
                    let noise = ((seed.wrapping_add(i * 389)) % 100) as f64 / 100.0;
                    0.5 + noise * 0.5
                })
                .collect()
        }
    }
}

/// Compute a simple trend indicator from a time series.
fn compute_trend(values: &[f64]) -> &'static str {
    if values.len() < 2 {
        return "stable";
    }
    let first_half: f64 =
        values[..values.len() / 2].iter().sum::<f64>() / (values.len() / 2) as f64;
    let second_half: f64 =
        values[values.len() / 2..].iter().sum::<f64>() / (values.len() - values.len() / 2) as f64;
    let change = (second_half - first_half) / first_half.abs().max(0.001);

    if change > 0.02 {
        "up"
    } else if change < -0.02 {
        "down"
    } else {
        "stable"
    }
}
