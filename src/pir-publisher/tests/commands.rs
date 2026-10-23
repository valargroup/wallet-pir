//! Offline maintenance stays available; production runs on pir-publisher.
use std::process::Command;

#[cfg(feature = "enhance")]
fn server() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pir-publisher"));
    command.arg("enhance");
    command
}

#[cfg(feature = "enhance")]
#[test]
fn build_info_omits_cuda() {
    let output = server().arg("build-info").output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(!text.contains("cuda"), "{text}");
    assert!(text.contains("\"native_reinspiring\""));
    assert!(text.contains("\"source_revision\""));
}

#[cfg(feature = "enhance")]
#[test]
fn maintenance_commands_remain() {
    let output = server().arg("--help").output().unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for name in ["rebuild-journal", "prepare-publication", "build-info"] {
        assert!(help.contains(name), "{name} missing from:\n{help}");
    }
}

#[cfg(feature = "enhance")]
#[test]
fn standalone_coordinator_is_rejected() {
    let output = server().arg("coordinator").output().unwrap();
    assert!(!output.status.success());
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.contains("unrecognized subcommand"), "{err}");
}

#[test]
fn static_check_creates_no_state() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("publisher.json");
    std::fs::write(&config, r#"{"management_listen":"127.0.0.1:18080"}"#).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pir-publisher"))
        .arg("--config")
        .arg(&config)
        .arg("--check")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
}

#[test]
fn maintenance_commands_require_the_compiled_product() {
    for (name, compiled) in [
        ("enhance", cfg!(feature = "enhance")),
        ("status", cfg!(feature = "status")),
        ("publish-history", cfg!(feature = "transparent-history")),
        (
            "prepare-history-restore",
            cfg!(feature = "transparent-history"),
        ),
        (
            "prepare-transparent-storage",
            cfg!(all(
                feature = "transparent-history",
                feature = "transparent-detail"
            )),
        ),
        ("verify-filters", cfg!(feature = "transparent-history")),
        ("detail", cfg!(feature = "transparent-detail")),
        ("ingest-events", cfg!(feature = "rocksdb-reader")),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_pir-publisher"))
            .args([name, "--help"])
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            compiled,
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
