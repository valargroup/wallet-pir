//! Keep the operations snapshot bounds tied to the codec compiled by normal
//! Transparent CI, including when only the wallet-libraries pin changes.
use zakura_pir_protocol::transparent::events as transparent_events;

use std::path::Path;
use std::process::Command;

#[test]
fn snapshot_extent_bounds_match_the_compiled_event_codec() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("the host lives under src/pir-publisher");
    let check = r#"
import importlib.util
import sys

spec = importlib.util.spec_from_file_location("snapshot_codec_contract", sys.argv[1])
snapshot = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = snapshot
spec.loader.exec_module(snapshot)
for name, expected in zip(("EVENT_BYTES", "MAX_EVENT_BYTES"), map(int, sys.argv[2:]), strict=True):
    actual = getattr(snapshot, name)
    if actual != expected:
        raise SystemExit(f"snapshot {name}={actual} differs from compiled event codec {expected}")
"#;
    let output = Command::new("python3")
        .args(["-B", "-c", check])
        .arg(repository.join("tools/transparent/lib/activity_journal_snapshot.py"))
        .arg(transparent_events::EVENT_BYTES.to_string())
        .arg(transparent_events::MAX_EVENT_BYTES.to_string())
        .env("PYTHONPATH", repository.join("tools/lib"))
        .output()
        .expect("python3 is required for the snapshot codec contract");
    assert!(
        output.status.success(),
        "snapshot bounds must agree with the compiled codec: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
