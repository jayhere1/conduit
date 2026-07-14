//! End-to-end test for `conduit run --distributed`.
//!
//! Spawns the real `conduit` binary twice: once as the coordinator+scheduler
//! (`run --distributed`) and once as a worker. The worker executes the DAG's
//! bash task; the runner must exit 0 once the run completes.

use assert_cmd::cargo::CommandCargoExt;
use std::fs;
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Poll `addr` until a TCP connection succeeds or `deadline` elapses.
///
/// The worker exits (and does not retry) if it connects before the
/// coordinator's gRPC server has bound, which would then wedge the runner
/// waiting for a worker that never arrives. Waiting for the port to actually
/// accept connections — rather than sleeping a fixed interval — makes the
/// round trip deterministic and mirrors the documented workflow ("Coordinator
/// listening on …" is printed once the endpoint is up, then you start workers).
fn wait_until_listening(addr: &str, deadline: Duration) -> bool {
    let start = Instant::now();
    let sock: std::net::SocketAddr = addr.parse().unwrap();
    while start.elapsed() < deadline {
        if TcpStream::connect_timeout(&sock, Duration::from_millis(200)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Full distributed round trip: `run --distributed` starts a coordinator,
/// a separately spawned `worker` executes the bash task, run exits 0.
#[test]
fn distributed_run_executes_on_a_real_worker() {
    let dir = TempDir::new().unwrap();
    let dags = dir.path().join("dags");
    fs::create_dir_all(&dags).unwrap();
    let out_file = dir.path().join("touched");
    fs::write(
        dags.join("dist_demo.yaml"),
        format!(
            "id: dist_demo\ntasks:\n  touch:\n    type: bash\n    command: \"echo done > {}\"\n",
            out_file.display()
        ),
    )
    .unwrap();

    let port = 19477; // fixed high port; adjust if CI collides
    let bind = format!("127.0.0.1:{port}");

    let mut runner = Command::cargo_bin("conduit")
        .unwrap()
        .args([
            "run",
            "dist_demo",
            "--distributed",
            "--bind",
            &bind,
            "--dags-path",
        ])
        .arg(&dags)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Wait for the coordinator's gRPC endpoint to actually accept connections
    // before launching the worker (the worker doesn't retry a failed connect).
    assert!(
        wait_until_listening(&bind, Duration::from_secs(15)),
        "coordinator never started listening on {bind}"
    );

    let mut worker = Command::cargo_bin("conduit")
        .unwrap()
        .args(["worker", "--coordinator", &bind, "--id", "w-test"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Bounded wait for the run to finish so a genuine failure asserts cleanly
    // instead of hanging the suite forever.
    let run_deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        match runner.try_wait().unwrap() {
            Some(status) => break Some(status),
            None if Instant::now() >= run_deadline => {
                let _ = runner.kill();
                break None;
            }
            None => std::thread::sleep(Duration::from_millis(100)),
        }
    };
    let _ = worker.kill();
    let _ = worker.wait();

    let status = status.expect("distributed run did not complete within 30s");
    assert!(status.success(), "distributed run must exit 0");
    assert!(out_file.exists(), "task must have executed on the worker");
}
