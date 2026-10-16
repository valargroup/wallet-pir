//! A direct `--journal-census` attributes its output to the image that ran,
//! however that image's pathname changes while it runs.
#[allow(dead_code)]
#[path = "../../../../services/transparent-filter-server/src/display_journal.rs"]
mod display_journal;
#[allow(dead_code)]
#[path = "../../../../services/transparent-filter-server/src/events.rs"]
mod events;

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use transparent_events::{FeeState, TransactionMetadata, Txid};
use transparent_shard::txid::DisplayOutput;
use transparent_shard::txid_v1::TransparentDisplayRecord;
use transparent_shard::txid_v2x::TransparentDisplayRecordV2x;

/// A committed one-block v2x journal from mainnet genesis.
fn journal(dir: &Path) {
    let hash = transparent_filter::BlockHash::from_display_hex(
        transparent_filter::MAINNET_GENESIS_DISPLAY,
    )
    .unwrap();
    let coinbase = TransparentDisplayRecord {
        txid: Txid([1; 32]),
        coinbase: true,
        metadata: TransactionMetadata {
            fee: FeeState::NotApplicable,
            transparent_input_count: 0,
            has_shielded_components: false,
        },
        outputs: vec![DisplayOutput {
            value: 0,
            script: vec![],
        }],
    };
    let mut store =
        events::EventStore::open(dir, transparent_filter::MAINNET_GENESIS_DISPLAY, 0).unwrap();
    store
        .append_block_with_display(
            0,
            hash,
            &[],
            &[TransparentDisplayRecordV2x {
                record: coinbase,
                inputs: vec![],
            }],
        )
        .unwrap();
    store.commit().unwrap();
}

fn sha256(path: &Path) -> String {
    format!("{:x}", Sha256::digest(std::fs::read(path).unwrap()))
}

fn wait_for(path: &Path, child: &mut Child) {
    let start = Instant::now();
    while !path.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("reader exited before {}: {status}", path.display());
        }
        assert!(
            start.elapsed() < Duration::from_secs(120),
            "barrier timeout"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Where the replacement happens relative to the reader's capture.
#[derive(Clone, Copy, Debug)]
enum Change {
    ReplaceBeforeCapture,
    UnlinkBeforeCapture,
    ReplaceAfterCapture,
    UnlinkAfterCapture,
}

fn census_while(change: Change) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("journal");
    std::fs::create_dir(&source).unwrap();
    journal(&source);
    let scratch = root.path().join("scratch");
    let barriers = root.path().join("barriers");
    std::fs::create_dir_all(&scratch).unwrap();
    std::fs::create_dir_all(&barriers).unwrap();

    // Reader A is a private copy, so its pathname can change under it.
    let a: PathBuf = root.path().join("reader");
    std::fs::copy(env!("CARGO_BIN_EXE_txid-sizing-export"), &a).unwrap();
    let a_sha256 = sha256(&a);
    // B differs from A, as a rebuild would.
    let b = root.path().join("rebuilt");
    let mut b_bytes = std::fs::read(&a).unwrap();
    b_bytes.extend(b"rebuilt");
    std::fs::write(&b, b_bytes).unwrap();
    assert_ne!(sha256(&b), a_sha256);

    let mut child = Command::new(&a)
        .args([
            "--journal-census",
            "--v2x",
            source.to_str().unwrap(),
            "0",
            "--anchor-from-journal",
            scratch.to_str().unwrap(),
        ])
        .env("TXID_SIZING_EXPORT_TEST_BARRIER", &barriers)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let change_path = || match change {
        Change::ReplaceBeforeCapture | Change::ReplaceAfterCapture => {
            std::fs::rename(&b, &a).unwrap()
        }
        Change::UnlinkBeforeCapture | Change::UnlinkAfterCapture => {
            std::fs::remove_file(&a).unwrap()
        }
    };
    let before = matches!(
        change,
        Change::ReplaceBeforeCapture | Change::UnlinkBeforeCapture
    );
    wait_for(&barriers.join("before-capture.reached"), &mut child);
    if before {
        change_path();
    }
    std::fs::write(barriers.join("before-capture.go"), b"").unwrap();
    wait_for(&barriers.join("after-capture.reached"), &mut child);
    if !before {
        change_path();
    }
    std::fs::write(barriers.join("after-capture.go"), b"").unwrap();

    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{change:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    // The scan completed over the synthetic journal.
    assert_eq!(report["schema"], "txid-display-journal-census-v2x");
    assert_eq!(report["measurement"]["distinct_display_records"], 1);
    assert_eq!(report["source"]["binary_sha256"], a_sha256, "{change:?}");
    assert_eq!(
        report["source"]["compiled_reader_identity"]["binary_sha256"], a_sha256,
        "{change:?}"
    );
}

/// One test thread copies and runs every reader: a concurrent fork could
/// inherit the copy's write descriptor and make its exec fail spuriously.
#[test]
fn a_census_identifies_the_image_that_ran_across_pathname_changes() {
    for change in [
        Change::ReplaceBeforeCapture,
        Change::UnlinkBeforeCapture,
        Change::ReplaceAfterCapture,
        Change::UnlinkAfterCapture,
    ] {
        census_while(change);
    }
}
