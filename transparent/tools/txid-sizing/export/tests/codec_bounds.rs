//! The operations snapshot checker bounds committed event extents with
//! `EVENT_BYTES` and `MAX_EVENT_BYTES`. Both must equal the pinned event codec
//! this reader compiles, reported through `--source-info`.
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;

const BOUNDS: [&str; 2] = ["EVENT_BYTES", "MAX_EVENT_BYTES"];

fn snapshot_module() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../ops/lib/activity_journal_snapshot.py")
}

/// The bounds the Python module at `module` holds after `then` runs.
fn python_bounds(module: &Path, then: &str) -> Value {
    let script = format!(
        "import importlib.util, json, sys\n\
         spec = importlib.util.spec_from_file_location('snapshot', sys.argv[1])\n\
         S = importlib.util.module_from_spec(spec)\n\
         spec.loader.exec_module(S)\n\
         {then}\n\
         print(json.dumps({{'event_bytes': S.EVENT_BYTES, 'max_event_bytes': S.MAX_EVENT_BYTES}}))"
    );
    // The module imports the repository's shared operations package.
    let shared = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../../ops/lib");
    let output = Command::new("python3")
        .env("PYTHONPATH", shared)
        .args(["-B", "-c", &script])
        .arg(module)
        .output()
        .expect("python3");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn compiled_bounds() -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_txid-sizing-export"))
        .arg("--source-info")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let info: Value = serde_json::from_slice(&output.stdout).unwrap();
    info["event_codec_bounds"].clone()
}

#[test]
fn snapshot_bounds_are_the_compiled_codec_bounds() {
    let compiled = compiled_bounds();
    assert_eq!(
        compiled["event_bytes"].as_u64(),
        Some(transparent_events::EVENT_BYTES as u64)
    );
    assert_eq!(
        compiled["max_event_bytes"].as_u64(),
        Some(transparent_events::MAX_EVENT_BYTES as u64)
    );
    assert_eq!(python_bounds(&snapshot_module(), ""), compiled);
}

/// Moving either Python bound, in either direction, breaks the agreement.
#[test]
fn either_moved_snapshot_bound_disagrees_with_the_compiled_codec() {
    let compiled = compiled_bounds();
    for name in BOUNDS {
        for delta in ["+ 1", "- 1"] {
            let moved = python_bounds(&snapshot_module(), &format!("S.{name} = S.{name} {delta}"));
            assert_ne!(moved, compiled, "{name} {delta}");
            let key = name.to_ascii_lowercase();
            assert_ne!(moved[&key], compiled[&key], "{name} {delta}");
        }
    }
}
