//! Tests that AppState's operational view (environments, snapshots, run
//! history) survives an API server restart, instead of resetting to blank
//! in-memory stores every time `conduit serve` starts.

use conduit_api::AppState;

fn temp_project() -> (std::path::PathBuf, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("conduit_persist_{}", uuid::Uuid::new_v4()));
    let dags = root.join("dags");
    std::fs::create_dir_all(&dags).unwrap();
    (dags, root)
}

#[tokio::test]
async fn environments_survive_restart() {
    let (dags, state_dir) = temp_project();

    {
        let state = AppState::with_options(dags.clone(), state_dir.clone(), None, false);
        state.env_manager.create("staging", None).unwrap();
        state.persist_environments();
        drop(state);
    } // RocksDB handles released here

    let state = AppState::with_options(dags, state_dir, None, false);
    assert!(
        state.env_manager.get("staging").is_ok(),
        "staging must survive restart"
    );
}

#[tokio::test]
async fn snapshots_survive_restart() {
    let (dags, state_dir) = temp_project();

    {
        let state = AppState::with_options(dags.clone(), state_dir.clone(), None, false);
        let snap = conduit_common::snapshot::Snapshot {
            id: "snap_test_1".to_string(),
            fingerprint: conduit_common::fingerprint::Fingerprint("abc123".to_string()),
            dag_id: "d".to_string(),
            task_id: "t".to_string(),
            created_at: chrono::Utc::now(),
            parent_fingerprints: vec![],
            metadata: Default::default(),
        };
        state.snapshot_store.put(snap).unwrap();
        drop(state);
    }

    let state = AppState::with_options(dags, state_dir, None, false);
    assert!(
        state.snapshot_store.count() >= 1,
        "snapshot must survive restart"
    );
}

#[tokio::test]
async fn run_history_rehydrates_from_event_store() {
    let (dags, state_dir) = temp_project();

    {
        let state = AppState::with_options(dags.clone(), state_dir.clone(), None, false);
        let store = state.event_store.as_ref().expect("event store").clone();
        store
            .append(conduit_common::event::EventKind::DagRunCreated {
                dag_id: "d1".into(),
                run_id: "r1".into(),
                logical_date: chrono::Utc::now(),
                environment: "staging".into(),
                triggered_by: "api".into(),
            })
            .unwrap();
        store
            .append(conduit_common::event::EventKind::TaskCompleted {
                dag_id: "d1".into(),
                run_id: "r1".into(),
                task_id: "t1".into(),
                duration_ms: 10,
                snapshot_id: None,
            })
            .unwrap();
        store
            .append(conduit_common::event::EventKind::DagRunCompleted {
                dag_id: "d1".into(),
                run_id: "r1".into(),
                status: conduit_common::event::RunStatus::Success,
                duration_ms: 12,
            })
            .unwrap();
        drop(store);
        drop(state);
    }

    let state = AppState::with_options(dags, state_dir, None, false);
    let runs = state.get_runs(None);
    let run = runs
        .iter()
        .find(|r| r.run_id == "r1")
        .expect("run rehydrated");
    assert_eq!(run.status, "success");
    assert_eq!(run.environment, "staging");
    assert_eq!(
        run.task_states.get("t1").map(String::as_str),
        Some("success")
    );
}
