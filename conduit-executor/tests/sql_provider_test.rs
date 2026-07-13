use std::collections::HashMap;
use std::sync::Arc;

use conduit_common::dag::{ResourceLimits, Task, TaskType, TriggerRule};
use conduit_executor::process_runner::{ProcessRunner, TaskContext};
use conduit_providers::providers::duckdb::DuckDbProvider;
use conduit_providers::registry::ProviderInstance;
use conduit_providers::ProviderRegistry;

fn make_sql_task(id: &str, connection: &str, query: &str) -> Task {
    Task {
        id: id.to_string(),
        task_type: TaskType::Sql {
            connection: connection.to_string(),
            query: query.to_string(),
            target: None,
        },
        dependencies: vec![],
        retries: 0,
        retry_delay: None,
        retry_backoff: None,
        source_hash: None,
        pool: None,
        timeout: None,
        priority: 0,
        resources: ResourceLimits::default(),
        trigger_rule: TriggerRule::default(),
        incremental: None,
        contracts: None,
        inputs: vec![],
        outputs: vec![],
    }
}

fn make_context(task_id: &str) -> TaskContext {
    TaskContext {
        dag_id: "sql_dag".to_string(),
        run_id: format!("test_run_{}", task_id),
        task_id: task_id.to_string(),
        attempt: 1,
        logical_date: chrono::Utc::now(),
        environment: "test".to_string(),
        params: HashMap::new(),
    }
}

#[tokio::test]
async fn sql_task_executes_via_registered_provider() {
    let mut registry = ProviderRegistry::new();
    registry.register(
        "analytics",
        ProviderInstance::Sql(Arc::new(DuckDbProvider::ephemeral())),
    );

    let task = make_sql_task("select_one", "analytics", "SELECT 1 AS n, 'hi' AS greeting");
    let ctx = make_context("select_one");

    let output = ProcessRunner::run_with_providers(&task, &ctx, Some(&registry))
        .await
        .expect("native SQL execution should succeed");

    assert_eq!(output.exit_code, 0);
    // The native path records real row counts — the old stub always said 0.
    let xcom = output.xcom.expect("SQL task should emit xcom");
    assert_eq!(xcom["rows_affected"], 1);
    assert!(
        !output.stdout.contains("SQL execution completed"),
        "must not go through the fake subprocess stub"
    );
}

#[tokio::test]
async fn sql_task_without_provider_fails_loudly() {
    let task = make_sql_task("orphan", "warehouse", "SELECT 1");
    let ctx = make_context("orphan");

    // No registry at all (the old ProcessRunner::run path).
    let err = ProcessRunner::run(&task, &ctx)
        .await
        .expect_err("SQL without a provider must be an error, not fake success");
    let msg = err.to_string();
    assert!(
        msg.contains("warehouse"),
        "error names the connection: {msg}"
    );
    assert!(
        msg.contains("conduit.yaml"),
        "error tells the user the fix: {msg}"
    );
}

#[tokio::test]
async fn sql_task_with_registry_missing_connection_fails_loudly() {
    let registry = ProviderRegistry::new(); // empty
    let task = make_sql_task("orphan2", "warehouse", "SELECT 1");
    let ctx = make_context("orphan2");

    let result = ProcessRunner::run_with_providers(&task, &ctx, Some(&registry)).await;
    assert!(result.is_err());
}
