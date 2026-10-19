//! Publication commands stay on enhance-pir-server. Serving commands do not.
use std::process::Command;

fn server() -> Command {
    Command::new(env!("CARGO_BIN_EXE_enhance-pir-server"))
}

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

#[test]
fn publication_commands_remain() {
    let output = server().arg("--help").output().unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for name in [
        "coordinator",
        "rebuild-journal",
        "prepare-packing-artifacts",
        "restore-legacy-placement",
        "build-info",
    ] {
        assert!(help.contains(name), "{name} missing from:\n{help}");
    }
}

#[test]
fn serving_commands_are_rejected() {
    for name in [
        "worker",
        "query-ingress",
        "packing-router",
        "check-backend",
        "repair-rows",
    ] {
        let output = server().arg(name).output().unwrap();
        assert!(!output.status.success(), "{name}");
        let err = String::from_utf8_lossy(&output.stderr);
        assert!(err.contains("unrecognized subcommand"), "{name}: {err}");
    }
}
