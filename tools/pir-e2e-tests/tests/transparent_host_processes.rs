//! Transparent history and transaction-detail processes launched through the
//! `pir-query-server` host: one-shot verification of each product from local
//! files and its refusals.
use std::path::{Path, PathBuf};
use std::process::Command;
use transparent_shared::tx_detail::synth;

#[path = "../../../src/pir-query-server/tests/transparent/support/display_world.rs"]
mod display_world;
use display_world::*;

#[path = "../../../src/pir-query-server/tests/transparent/support/operator_set.rs"]
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
fn history_verify_only_stdout_is_machine_readable() {
    let dir = tempfile::tempdir().unwrap();
    operator_set::publish(dir.path());
    let state = tempfile::tempdir().unwrap();
    let config = write_config(
        state.path(),
        "transparent_history",
        serde_json::json!({
            "listen": format!("127.0.0.1:{}", free_port()),
            "shard_dir": dir.path(),
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
    // Ordinary startup of the same local configuration has no publisher.
    let output = host(&config).arg("--check").output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("HTTPS publisher"));
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
