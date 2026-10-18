//! Run environment tests in child processes so concurrent tests cannot race env vars.
use std::process::Command;
fn worker(root: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_enhance-pir-server"));
    command
        .env_remove("ENHANCE_MATVEC_BACKEND")
        .env_remove("ENHANCE_CUDA_DEVICE")
        .arg("worker")
        .arg("--data-dir")
        .arg(root);
    command
}
#[test]
fn cpu_default_and_cli_over_environment_are_validated_before_state_changes() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("worker");
    let output = worker(&root).arg("--cuda-device=0").output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --matvec-backend cuda"));
    assert!(!root.exists());

    let output = worker(&root)
        .env("ENHANCE_MATVEC_BACKEND", "cuda")
        .env("ENHANCE_CUDA_DEVICE", "not-a-number")
        .args(["--matvec-backend=cpu", "--cuda-device=0"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --matvec-backend cuda"));
    assert!(!root.exists());
}
#[test]
fn environment_cuda_selection_fails_explicitly_for_invalid_device_or_build() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("worker");
    let output = worker(&root)
        .env("ENHANCE_MATVEC_BACKEND", "cuda")
        .env("ENHANCE_CUDA_DEVICE", usize::MAX.to_string())
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    #[cfg(feature = "cuda")]
    assert!(error.contains("ordinal"), "{error}");
    #[cfg(not(feature = "cuda"))]
    assert!(error.contains("--features cuda"), "{error}");
    assert!(!root.exists());
}
