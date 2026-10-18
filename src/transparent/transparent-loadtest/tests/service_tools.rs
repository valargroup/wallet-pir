//! Operational adapters stay runnable from service-owned Cargo targets.
use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn filter_encoder_keeps_the_batched_measurement_protocol() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_transparent-filter-cli"))
        .arg("batch-build")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    writeln!(
        child.stdin.take().unwrap(),
        "{}",
        serde_json::json!({
        "block_hash_display":"00".repeat(32),"elements":[]})
    )
    .unwrap();
    let result = child.wait_with_output().unwrap();
    assert!(result.status.success());
    let built: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    // BIP 158's empty set is its one-byte zero count.
    assert_eq!(built["filter"], "00");
    assert_eq!(built["bytes"], 1);
    assert_eq!(built["elements"], 0);
    let mut child = Command::new(env!("CARGO_BIN_EXE_transparent-filter-cli"))
        .arg("batch-match")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    writeln!(
        child.stdin.take().unwrap(),
        "{}",
        serde_json::json!({
        "block_hash_display":"00".repeat(32),"filter":"00","scripts":["51"]})
    )
    .unwrap();
    let result = child.wait_with_output().unwrap();
    assert!(result.status.success());
    let matched: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(matched, serde_json::json!({"indices":[],"elements":0}));
}

#[test]
fn txid_canary_keeps_the_independent_oracle_failure_contract() {
    let directory = tempfile::tempdir().unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_txid-quality-canary"))
        .args(["--url", "http://127.0.0.1:1", "--fixture"])
        .arg(directory.path().join("missing-fixture.json"))
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(1));
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["passed"], false);
    assert_eq!(report["category"], "oracle_invalid");
    assert!(report["duration_seconds"].as_f64().unwrap() >= 0.0);
    assert_eq!(report.as_object().unwrap().len(), 3);
}
