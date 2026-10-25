//! Offline maintenance stays available; production runs on pir-publisher.
use std::process::Command;

#[test]
fn contract_info_reports_compiled_products_without_configuration() {
    let temp = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pir-publisher"))
        .current_dir(temp.path())
        .arg("--contract-info")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let info: pir_service_discovery::ContractInfo = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        info,
        pir_service_discovery::ContractInfo::new(
            "pir-publisher",
            &info.products.iter().map(String::as_str).collect::<Vec<_>>(),
            &[]
        )
    );
    assert_eq!(
        info.products.iter().any(|name| name == "status"),
        cfg!(feature = "status")
    );
    assert_eq!(
        info.products.iter().any(|name| name == "enhance"),
        cfg!(feature = "enhance")
    );
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

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
fn retired_storage_conversion_is_rejected() {
    let output = Command::new(env!("CARGO_BIN_EXE_pir-publisher"))
        .arg("prepare-transparent-storage")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(2), "{error}");
    assert!(
        error.contains("prepare-transparent-storage")
            && (error.contains("unrecognized subcommand") || error.contains("unexpected argument")),
        "{error}"
    );
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

/// Exercise the Python renderer and root binary together, without opening a
/// node cookie or existing Receiver store.
#[cfg(feature = "receiver")]
#[test]
fn rendered_receiver_publisher_passes_the_root_offline_check() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let rendered = Command::new("python3").arg("-c").arg(r#"
import json,sys
from pathlib import Path
root=Path(sys.argv[1]); sys.path.insert(0,str(root/'tools/lib')); sys.path.insert(0,str(root))
from tools.receiver.lib.roles import publisher
example=json.loads((root/'tools/receiver/deploy/receiver-publisher.example.json').read_text())
s={'config_path':'/etc/receiver-publisher/config.json','management_listen':example['management_listen'],
   'chain':example['chain'],'receiver':example['receiver']['settings']}
print(next(iter(publisher(s,release='/release').files.values())))
"#).arg(root).output().unwrap();
    assert!(
        rendered.status.success(),
        "{}",
        String::from_utf8_lossy(&rendered.stderr)
    );
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("publisher.json");
    std::fs::write(&path, &rendered.stdout).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_pir-publisher"))
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
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
