//! Operator commands on the query-server binary: role config, evaluator check, row repair flags.
use std::process::Command;

fn binary() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pir-query-server"));
    command
        .env_remove("ENHANCE_MATVEC_BACKEND")
        .env_remove("ENHANCE_CUDA_DEVICE");
    command
}

#[test]
fn config_check_validates_a_role_without_opening_state() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("query-server.json");
    std::fs::write(&path, r#"{"management_listen":"127.0.0.1:18080"}"#).unwrap();
    let output = binary()
        .arg("--config")
        .arg(&path)
        .arg("--check")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout)
        .contains("Query server configuration is valid; runtime readiness was not checked"));
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);

    let missing = binary().arg("--check").output().unwrap();
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("--config"));
}

#[test]
fn check_backend_validates_matvec_before_any_worker_state() {
    let temp = tempfile::tempdir().unwrap();
    let output = binary()
        .current_dir(temp.path())
        .args(["check-backend", "--cuda-device=0"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --matvec-backend cuda"));
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);

    let output = binary()
        .current_dir(temp.path())
        .env("ENHANCE_MATVEC_BACKEND", "cuda")
        .env("ENHANCE_CUDA_DEVICE", "not-a-number")
        .args(["check-backend", "--matvec-backend=cpu", "--cuda-device=0"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --matvec-backend cuda"));
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[test]
fn environment_cuda_selection_fails_explicitly_for_invalid_device_or_build() {
    let temp = tempfile::tempdir().unwrap();
    let output = binary()
        .current_dir(temp.path())
        .env("ENHANCE_MATVEC_BACKEND", "cuda")
        .env("ENHANCE_CUDA_DEVICE", usize::MAX.to_string())
        .arg("check-backend")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    #[cfg(feature = "cuda")]
    assert!(error.contains("ordinal"), "{error}");
    #[cfg(not(feature = "cuda"))]
    assert!(error.contains("--features cuda"), "{error}");
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[test]
fn repair_rows_keeps_the_offline_flags() {
    let output = binary().args(["repair-rows", "--help"]).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains("--data-dir"));
    assert!(help.contains("--source-rows"));
}
