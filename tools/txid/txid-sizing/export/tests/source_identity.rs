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
dependencies = ["zakura-chain"]
[[package]]
name = "zakura-pir-protocol"
version = "0.0.1-rc0"
source = "git+https://example.invalid/library?rev={codec}#{codec}"
"#,
        node = "a".repeat(40),
        codec = "b".repeat(40)
    )
}

fn tree() -> String {
    "c".repeat(64)
}

fn manifest_dir() -> &'static std::path::Path {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn compiled_node_codec_and_local_adapter_identities_are_distinct_and_lock_bound() {
    let lock = fixture();
    let value = identity::from_lock(lock.as_bytes(), &tree()).unwrap();
    assert_eq!(value["schema"], "txid-sizing-export-source-v2");
    assert_eq!(value["node_parser"]["git_revision"], "a".repeat(40));
    assert_eq!(value["event_codec"]["git_revision"], "b".repeat(40));
    assert_eq!(
        value["server_adapter"],
        serde_json::json!({"package":"zakura-server-chain","version":"0.0.1-rc0",
            "source":"local","source_tree_sha256":tree()})
    );
    let changed = lock.replace("10.0.0-rc0", "10.0.0-rc1");
    assert_ne!(
        value["cargo_lock_sha256"],
        identity::from_lock(changed.as_bytes(), &tree()).unwrap()["cargo_lock_sha256"]
    );
    let actual = identity::compiled(manifest_dir()).unwrap();
    assert_eq!(actual["node_parser"]["package"], "zakura-chain");
    assert_eq!(actual["server_adapter"]["package"], "zakura-server-chain");
    assert_eq!(actual["server_adapter"]["source"], "local");
    // A local adapter is never attributed a Git source.
    assert!(actual["server_adapter"].get("git_source").is_none());
    assert!(actual["server_adapter"].get("git_revision").is_none());
}

#[test]
fn incomplete_ambiguous_duplicate_or_non_git_provenance_is_refused() {
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
        // The same package identity twice.
        format!("{lock}\n[[package]]\nname = \"zakura-pir-protocol\"\nversion = \"0.0.1-rc0\"\nsource = \"git+https://example.invalid/library?rev={b}#{b}\"\n",
            b = "b".repeat(40)),
        // A Git-sourced adapter is not the repository's local reader.
        lock.replace(
            "version = \"0.0.1-rc0\"\ndependencies",
            &format!("version = \"0.0.1-rc0\"\nsource = \"git+https://example.invalid/library?rev={a}#{a}\"\ndependencies",
                a = "a".repeat(40)),
        ),
    ] {
        assert!(identity::from_lock(malformed.as_bytes(), &tree()).is_err(), "{malformed}");
    }
    for digest in ["", "short", &"C".repeat(64), &"g".repeat(64)] {
        assert!(
            identity::from_lock(lock.as_bytes(), digest).is_err(),
            "{digest}"
        );
    }
}

#[test]
fn adapter_source_tree_digest_covers_sources_and_excludes_outputs() {
    let root = tempfile::tempdir().unwrap();
    let write = |path: &str, bytes: &str| {
        let path = root.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    };
    write("Cargo.toml", "[package]\n");
    write("src/lib.rs", "pub fn read() {}\n");
    write("tests/fixtures/block.hex", "00\n");
    let digest = || identity::source_tree_sha256(root.path()).unwrap();
    let original = digest();
    assert_eq!(original, digest());
    // Build outputs are not source inputs.
    write("target/release/output", "built");
    write("src/target/output", "built");
    assert_eq!(original, digest());
    // Source edits, embedded fixtures, new files and renames all change it.
    write("src/lib.rs", "pub fn read() { }\n");
    let edited = digest();
    assert_ne!(original, edited);
    write("tests/fixtures/block.hex", "01\n");
    assert_ne!(edited, digest());
    let fixture = digest();
    write("src/new.rs", "");
    let added = digest();
    assert_ne!(fixture, added);
    std::fs::rename(
        root.path().join("src/new.rs"),
        root.path().join("src/other.rs"),
    )
    .unwrap();
    assert_ne!(added, digest());
    std::fs::remove_file(root.path().join("Cargo.toml")).unwrap();
    assert!(identity::source_tree_sha256(root.path()).is_err());
}

#[test]
fn compiled_adapter_digest_matches_the_repository_reader() {
    let reader = manifest_dir().join(identity::ADAPTER_DIR);
    let files: Vec<_> = identity::source_files(&reader)
        .unwrap()
        .into_iter()
        .map(|(relative, _)| relative)
        .collect();
    for input in [
        "Cargo.toml",
        "README.md",
        "src/lib.rs",
        "tests/fixtures/ironwood-fee-expiry.hex",
    ] {
        assert!(files.iter().any(|f| f == input), "{input}");
    }
    assert_eq!(
        identity::compiled(manifest_dir()).unwrap()["server_adapter"]["source_tree_sha256"],
        identity::source_tree_sha256(&reader).unwrap()
    );
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
    let expected = identity::compiled(manifest_dir()).unwrap();
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
                "../../../../../src/transparent/transparent-publisher/src/ingest/extract.rs"
            ))
        )
    );
    assert_eq!(
        actual["compiled_source_file_sha256"]["census"],
        format!("{:x}", Sha256::digest(include_bytes!("../src/census.rs")))
    );
}
