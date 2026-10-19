#[path = "../build_support/identity.rs"]
mod identity;

fn fixture() -> String {
    format!(
        r#"version = 4
[[package]]
name = "zakura-chain"
version = "10.0.0-rc0"
source = "git+https://example.invalid/node?rev={node}#{node}"
[[package]]
name = "zakura-server-chain"
version = "0.0.1-rc0"
source = "git+https://example.invalid/library?rev={adapter}#{adapter}"
dependencies = ["zakura-chain"]
[[package]]
name = "zakura-pir-protocol"
version = "0.0.1-rc0"
source = "git+https://example.invalid/library?rev={adapter}#{adapter}"
"#,
        node = "a".repeat(40),
        adapter = "b".repeat(40)
    )
}

#[test]
fn compiled_node_and_adapter_identities_are_distinct_and_lock_bound() {
    let lock = fixture();
    let value = identity::from_lock(lock.as_bytes()).unwrap();
    assert_eq!(value["node_parser"]["git_revision"], "a".repeat(40));
    assert_eq!(value["server_adapter"]["git_revision"], "b".repeat(40));
    assert_eq!(value["event_codec"]["git_revision"], "b".repeat(40));
    let changed = lock.replace("10.0.0-rc0", "10.0.0-rc1");
    assert_ne!(
        value["cargo_lock_sha256"],
        identity::from_lock(changed.as_bytes()).unwrap()["cargo_lock_sha256"]
    );
    let actual = identity::from_lock(include_bytes!("../Cargo.lock")).unwrap();
    assert_eq!(actual["node_parser"]["package"], "zakura-chain");
    assert_eq!(actual["server_adapter"]["package"], "zakura-server-chain");
}

#[test]
fn incomplete_ambiguous_or_non_git_parser_provenance_is_refused() {
    let lock = fixture();
    for malformed in [
        lock.replace("name = \"zakura-chain\"", "name = \"unrelated-node\""),
        lock.replace(&format!("#{}", "a".repeat(40)), "#short"),
        lock.replace(
            "git+https://example.invalid/node",
            "registry+https://example.invalid/node",
        ),
        lock.replace("dependencies = [\"zakura-chain\"]", "dependencies = []"),
        format!("{lock}\n[[package]]\nname = \"zakura-chain\"\nversion = \"11\"\n"),
    ] {
        assert!(identity::from_lock(malformed.as_bytes()).is_err());
    }
}

#[test]
fn source_info_mode_reports_the_executing_binary_and_its_compilation_lock() {
    use sha2::{Digest, Sha256};
    let binary = env!("CARGO_BIN_EXE_txid-sizing-export");
    let output = std::process::Command::new(binary)
        .arg("--source-info")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let expected = identity::from_lock(include_bytes!("../Cargo.lock")).unwrap();
    for key in [
        "schema",
        "node_parser",
        "server_adapter",
        "event_codec",
        "cargo_lock_sha256",
    ] {
        assert_eq!(actual[key], expected[key], "{key}");
    }
    assert_eq!(
        actual["binary_sha256"],
        format!("{:x}", Sha256::digest(std::fs::read(binary).unwrap()))
    );
    assert_eq!(
        actual["compiled_source_file_sha256"]["extractor"],
        format!(
            "{:x}",
            Sha256::digest(include_bytes!(
                "../../../../../src/shared/pir-publisher/transparent/src/ingest/extract.rs"
            ))
        )
    );
    assert_eq!(
        actual["compiled_source_file_sha256"]["census"],
        format!("{:x}", Sha256::digest(include_bytes!("../src/census.rs")))
    );
}
