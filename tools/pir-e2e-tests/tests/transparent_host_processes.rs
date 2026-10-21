//! Transparent history and transaction-detail processes launched through the
//! `pir-query-server` host: one-shot verification of each product from local
//! files, its refusals, and a display worker under its control socket.
use std::path::{Path, PathBuf};
use std::process::Command;
use transparent_shared::tx_detail::synth;

#[path = "../../../src/transparent/transparent-query-server/tests/support/display_world.rs"]
mod display_world;
use display_world::*;

#[path = "../../../src/transparent/transparent-query-server/tests/support/operator_set.rs"]
mod operator_set;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// One generated host configuration with a single enabled Transparent section.
fn write_config(root: &Path, section: &str, settings: serde_json::Value) -> PathBuf {
    let path = root.join(format!("{section}-{}.json", free_port()));
    let config = serde_json::json!({
        "management_listen": format!("127.0.0.1:{}", free_port()),
        section: {"enabled": true, "settings": settings},
    });
    std::fs::write(&path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
    path
}

fn host(config: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pir-query-server-test"));
    command.arg("--config").arg(config);
    command
}

/// Runs `--verify-only` while the test itself holds the configured wallet and
/// management addresses: verification must succeed without binding either.
fn verify(config: &Path) -> std::process::Output {
    let raw: serde_json::Value = serde_json::from_slice(&std::fs::read(config).unwrap()).unwrap();
    let mut held = vec![
        std::net::TcpListener::bind(raw["management_listen"].as_str().unwrap())
            .expect("management address is free"),
    ];
    for section in ["transparent_history", "transaction_details"] {
        if let Some(listen) = raw[section]["settings"]["listen"].as_str() {
            held.push(std::net::TcpListener::bind(listen).expect("listen address is free"));
        }
    }
    let output = host(config).arg("--verify-only").output().unwrap();
    drop(held);
    output
}

#[test]
fn history_verify_only_stdout_is_machine_readable_and_checks_cache_capacity() {
    let dir = tempfile::tempdir().unwrap();
    operator_set::publish(dir.path());
    let cache = tempfile::tempdir().unwrap();
    let run = |limit: u64| {
        let config = write_config(
            cache.path(),
            "transparent_history",
            serde_json::json!({
                "listen": format!("127.0.0.1:{}", free_port()),
                "shard_dir": dir.path(),
                "runtime_cache_dir": cache.path().join("runtimes"),
                "runtime_cache_max_bytes": limit,
            }),
        );
        verify(&config)
    };
    let output = run(17_179_869_184);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout is one JSON report");
    assert_eq!(report["verified"], true);
    assert!(report["runtime_cache"]["missing_bytes"].as_u64().unwrap() > 0);
    assert!(
        !run(1).status.success(),
        "insufficient cache capacity fails before activation"
    );
}

#[test]
fn detail_verify_only_reports_the_local_publication() {
    let root = tempfile::tempdir().unwrap();
    let a0 = write_shard(root.path(), &spec(0, 100, 199, true, ""), &records(10, &[]));
    let r0 = write_shard(
        root.path(),
        &spec(1, 200, 260, false, &a0.digest),
        &records(20, &[]),
    );
    let (dir, map_sha256) =
        synth::write_candidate(root.path(), &params(1), &[a0, r0], "p0").unwrap();
    let config = write_config(
        root.path(),
        "transaction_details",
        serde_json::json!({
            "role": "combined",
            "listen": format!("127.0.0.1:{}", free_port()),
            "publication_dir": dir,
        }),
    );
    let output = verify(&config);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout is one JSON report");
    assert_eq!(report["verified"], true);
    assert_eq!(report["role"], "combined");
    assert_eq!(report["map_sha256"], map_sha256);
    assert_eq!(report["held"].as_array().unwrap().len(), 2);
}

#[test]
fn verification_is_refused_outside_its_own_mode() {
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("absent");
    // Product-level verify_only belongs to `--verify-only`, never ordinary startup.
    let flagged = write_config(
        root.path(),
        "transparent_history",
        serde_json::json!({"shard_dir": missing, "verify_only": true,
            "listen": format!("127.0.0.1:{}", free_port())}),
    );
    let output = host(&flagged).output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--verify-only"));
    // Exactly one Transparent section: both enabled is refused before any work.
    let both = root.path().join("both.json");
    std::fs::write(
        &both,
        serde_json::to_vec(&serde_json::json!({
            "management_listen": format!("127.0.0.1:{}", free_port()),
            "transparent_history": {"enabled": true, "settings": {"shard_dir": missing}},
            "transaction_details": {"enabled": true, "settings": {"role": "combined"}},
        }))
        .unwrap(),
    )
    .unwrap();
    let output = host(&both).arg("--verify-only").output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("exactly one"));
    assert!(!missing.exists(), "a refused verification opens no state");
    // `--check` and `--verify-only` are separate modes.
    let output = host(&flagged)
        .args(["--check", "--verify-only"])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

/// Kills a spawned worker however the test ends.
struct Spawned(std::process::Child);

impl Drop for Spawned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A host-launched display worker starts with nothing to serve, takes a
/// publication through `txid-control`, and serves it again from its record on
/// restart.
#[test]
fn the_worker_process_takes_and_restores_a_publication_by_control() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let root = tempfile::tempdir().unwrap();
    let a0 = write_shard(root.path(), &spec(0, 100, 199, true, ""), &records(10, &[]));
    let r0 = write_shard(
        root.path(),
        &spec(1, 200, 260, false, &a0.digest),
        &records(20, &[]),
    );
    let recent = r0.digest.clone();
    let (dir, map_sha256) =
        synth::write_candidate(root.path(), &params(1), &[a0, r0], "p0").unwrap();
    let socket = root.path().join("control.sock");
    let record = root.path().join("worker/active.json");
    let port = free_port();
    let url = format!("http://127.0.0.1:{port}");
    let config = write_config(
        root.path(),
        "transaction_details",
        serde_json::json!({
            "role": "recent-replica",
            "listen": format!("127.0.0.1:{port}"),
            "control_socket": socket,
            "active_record": record,
        }),
    );
    let spawn = || {
        let _ = std::fs::remove_file(&socket);
        let child = host(&config).stderr(Stdio::null()).spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !(socket.exists() && reqwest::blocking::get(format!("{url}/v1/health")).is_ok()) {
            assert!(std::time::Instant::now() < deadline, "worker did not start");
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        Spawned(child)
    };
    let control = |command: serde_json::Value| {
        let mut child = Command::new(env!("CARGO_BIN_EXE_txid-control-test"))
            .arg(&socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(command.to_string().as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        let reply: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap_or_default();
        (output.status.success(), reply)
    };
    let ready = || reqwest::blocking::get(format!("{url}/v1/ready")).unwrap();

    let worker = spawn();
    let (ok, status) = control(serde_json::json!({"operation": "status"}));
    assert!(ok, "{status}");
    assert_eq!(status["active"], serde_json::Value::Null);
    assert_eq!(status["role"], "recent-replica");
    assert_eq!(status["warm"], false);
    assert_eq!(ready().status(), 503);
    let (ok, prepared) = control(serde_json::json!({
        "operation": "prepare",
        "expected": "",
        "publication": {"directory": dir, "map_sha256": map_sha256},
    }));
    assert!(ok, "{prepared}");
    assert_eq!(prepared["warm"], true);
    assert_eq!(prepared["built"], 1);
    let (ok, activated) = control(serde_json::json!({
        "operation": "activate", "expected": "", "map_sha256": map_sha256,
    }));
    assert!(ok, "{activated}");
    let body: serde_json::Value = ready().json().unwrap();
    assert_eq!(body["map_sha256"], map_sha256);
    assert_eq!(body["warm"], true);
    let (ok, refused) = control(serde_json::json!({
        "operation": "invalidate", "expected": map_sha256, "from_height": 150,
    }));
    assert!(!ok);
    assert_eq!(refused["error"], "reorg reaches sealed display shard 0");
    let (ok, _) = control(serde_json::json!({"operation": "stage", "directory": "relative"}));
    assert!(!ok);
    // A reorg below the recent shard's end revokes that revision durably.
    let (ok, invalidated) = control(serde_json::json!({
        "operation": "invalidate", "expected": map_sha256, "from_height": 250,
    }));
    assert!(ok, "{invalidated}");
    assert_eq!(invalidated["invalidated"], serde_json::json!([recent]));
    let revoked = || {
        reqwest::blocking::get(format!("{url}/v2/txid/recent/revisions/{recent}/setup"))
            .unwrap()
            .status()
    };
    assert_eq!(revoked(), 409);
    drop(worker);

    // A restart serves the recorded publication once its runtimes are warm.
    let _worker = spawn();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while ready().status() != 200 {
        assert!(
            std::time::Instant::now() < deadline,
            "restarted worker never warmed"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let (ok, status) = control(serde_json::json!({"operation": "status"}));
    assert!(ok);
    assert_eq!(status["active"]["map_sha256"], map_sha256);
    // The revocation survives the restart: the recorded publication serves,
    // but never the orphaned recent revision.
    assert_eq!(status["revoked_revisions"], 1);
    assert_eq!(revoked(), 409);
}
