//! Operator commands on the query-server binary: role config, one-shot
//! Transparent verification, evaluator check, row repair flags.
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

#[cfg(feature = "enhance")]
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

#[cfg(feature = "enhance")]
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

#[cfg(feature = "enhance")]
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

fn write(dir: &std::path::Path, config: serde_json::Value) -> std::path::PathBuf {
    let path = dir.join("query-server.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    path
}

#[test]
fn verify_only_needs_one_compiled_transparent_section_and_opens_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let path = write(
        temp.path(),
        serde_json::json!({"management_listen": "127.0.0.1:18080"}),
    );
    let output = binary()
        .arg("--config")
        .arg(&path)
        .arg("--verify-only")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("exactly one"));
    assert!(
        output.stdout.is_empty(),
        "stdout is reserved for the report"
    );
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
    // A separate mode from --check.
    let output = binary()
        .arg("--config")
        .arg(&path)
        .args(["--check", "--verify-only"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot be used with"));
}

#[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
#[test]
fn transparent_verification_refuses_ordinary_flags_and_extra_products() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let (section, settings) = if cfg!(feature = "transparent-history") {
        (
            "transparent_history",
            serde_json::json!({"listen": "127.0.0.1:18092", "shard_dir": state}),
        )
    } else {
        (
            "transaction_details",
            serde_json::json!({"listen": "127.0.0.1:18095", "role": "combined",
                "publication_dir": state}),
        )
    };
    // Ordinary startup refuses the product-level flag before binding anything.
    let mut flagged = settings.clone();
    flagged["verify_only"] = true.into();
    let path = write(
        temp.path(),
        serde_json::json!({"management_listen": "127.0.0.1:18080",
            section: {"enabled": true, "settings": flagged}}),
    );
    for mode in [None, Some("--check")] {
        let mut command = binary();
        command.arg("--config").arg(&path);
        command.args(mode);
        let output = command.output().unwrap();
        assert!(!output.status.success(), "{mode:?}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("--verify-only"));
    }
    // A disabled section is still a typed section, and does no work.
    let path = write(
        temp.path(),
        serde_json::json!({"management_listen": "127.0.0.1:18080",
            section: {"enabled": false, "settings": settings}}),
    );
    let output = binary()
        .arg("--config")
        .arg(&path)
        .arg("--verify-only")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("exactly one"));
    assert!(!state.exists(), "refused verification opened no state");
}

#[cfg(all(
    feature = "transparent-history",
    any(feature = "enhance", feature = "status")
))]
#[test]
fn transparent_verification_refuses_other_enabled_products() {
    let temp = tempfile::tempdir().unwrap();
    let other = if cfg!(feature = "enhance") {
        "enhance"
    } else {
        "status"
    };
    let path = write(
        temp.path(),
        serde_json::json!({"management_listen": "127.0.0.1:18080",
            other: {"enabled": true},
            "transparent_history": {"enabled": true, "settings": {"shard_dir": temp.path().join("state")}}}),
    );
    let output = binary()
        .arg("--config")
        .arg(&path)
        .arg("--verify-only")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains(other));
}
