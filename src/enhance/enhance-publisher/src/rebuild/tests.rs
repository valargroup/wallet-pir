use super::comparison::{compare_journals, query_shard};
use super::reconstruction::sha256_file;
use super::*;
use crate::{
    journal::CommittedJournalSnapshot,
    types::{DatabaseId, ENHANCE_LAYOUT},
};
use enhance_shared::{FLAG_HAS_FEE, RECORD_BYTES, RECORD_FEE_OFFSET, RECORD_FLAGS_OFFSET};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Write,
    path::Path,
    time::Instant,
};

fn record(fee: Option<u64>, flags: u8) -> Vec<u8> {
    let mut bytes = vec![7u8; RECORD_BYTES];
    bytes[RECORD_FLAGS_OFFSET] = flags | fee.map_or(0, |_| FLAG_HAS_FEE);
    bytes[RECORD_FLAGS_OFFSET + 1..RECORD_FEE_OFFSET].copy_from_slice(&100u32.to_le_bytes());
    bytes[RECORD_FEE_OFFSET..].copy_from_slice(&fee.unwrap_or(0).to_le_bytes());
    bytes
}

fn status_kib(field: &str) -> u64 {
    fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap()
}

/// Writes a journal of `blocks` blocks with `per_block` records each,
/// streaming records; `fee` sets the fee of every other record.
fn write_journal(dir: &Path, blocks: u64, per_block: u64, fee: bool) {
    fs::create_dir_all(dir).unwrap();
    let mut records = std::io::BufWriter::new(File::create(dir.join("records.bin")).unwrap());
    for position in 0..blocks * per_block {
        let paid = fee && position % 2 == 0;
        let mut bytes = record(paid.then_some(10_000), 3);
        bytes[..8].copy_from_slice(&position.to_le_bytes());
        records.write_all(&bytes).unwrap();
    }
    records.into_inner().unwrap().sync_all().unwrap();
    let entries: Vec<_> = (0..blocks)
        .map(|b| {
            serde_json::json!({"height": 3_400_000 + b, "hash": format!("{b:064x}"),
                "first_position": b * per_block, "action_count": per_block})
        })
        .collect();
    fs::write(
        dir.join("manifest.json"),
        serde_json::to_vec(&serde_json::json!({
            "version": 7, "record_bytes": RECORD_BYTES, "records_per_row": 33,
            "table": "enhance", "tree_size": blocks * per_block, "blocks": entries,
        }))
        .unwrap(),
    )
    .unwrap();
}

/// Startup cost of adoption at production scale (about 700,000 records in
/// 50,000 blocks). Ignored by default: it writes about 1 GB. Run with
/// `cargo test --profile release-fast -p enhance-publisher --lib
/// adoption_streams -- --ignored --nocapture`.
#[test]
#[ignore]
fn adoption_streams_a_production_sized_journal() {
    let (blocks, per_block) = (50_000, 14);
    let root = tempfile::tempdir().unwrap();
    write_journal(&root.path().join("enhance"), blocks, per_block, false);
    let staged = root.path().join(STAGED_DIR);
    write_journal(&staged, blocks, per_block, true);
    let manifest = fs::read(staged.join("manifest.json")).unwrap();
    let receipt = RebuildReceipt {
        format: RECEIPT_FORMAT.into(),
        receipt_id: "measure-1".into(),
        binary_source_revision: "test".into(),
        binary_sha256: "test".into(),
        height: 3_400_000 + blocks - 1,
        block_hash: format!("{:064x}", blocks - 1),
        tree_size: blocks * per_block,
        records_sha256: sha256_file(&staged.join("records.bin")).unwrap(),
        manifest_sha256: hex::encode(Sha256::digest(&manifest)),
        changed_records: blocks * per_block / 2,
        absent_fee_records: blocks * per_block / 2,
        completed_unix_seconds: 0,
    };
    fs::write(
        staged.join(RECEIPT_FILE),
        serde_json::to_vec(&receipt).unwrap(),
    )
    .unwrap();
    // Reset the peak so fixture construction does not mask adoption.
    fs::write("/proc/self/clear_refs", "5").unwrap();
    let before = status_kib("VmRSS:");
    let started = Instant::now();
    let adopted = adopt_staged(root.path()).unwrap();
    let seconds = started.elapsed().as_secs_f64();
    let growth = status_kib("VmHWM:").saturating_sub(before);
    println!(
        "{}",
        serde_json::json!({"records": blocks * per_block, "blocks": blocks,
            "journal_bytes": blocks * per_block * RECORD_BYTES as u64,
            "adoption_seconds": seconds, "peak_rss_growth_kib": growth,
            "page_cache": "warm (files just written)"})
    );
    assert!(
        matches!(adopted, JournalAdoption::Adopted { .. }),
        "{adopted:?}"
    );
    // Two 2.7 MB chunk buffers plus parsed manifests; never a journal copy.
    assert!(growth < 64 * 1024, "peak RSS grew {growth} KiB");
}

/// One block of `positions` sparse records. Only `at` is a real record;
/// the rest are zero holes, which compare equal and carry no fee.
fn sparse_journal(dir: &Path, positions: u64, at: u64, fee: Option<u64>) {
    use std::os::unix::fs::FileExt;
    fs::create_dir_all(dir).unwrap();
    let file = File::create(dir.join("records.bin")).unwrap();
    file.set_len(positions * RECORD_BYTES as u64).unwrap();
    file.write_all_at(&record(fee, 0), at * RECORD_BYTES as u64)
        .unwrap();
    fs::write(
        dir.join("manifest.json"),
        serde_json::to_vec(&serde_json::json!({
            "version": 7, "record_bytes": RECORD_BYTES, "records_per_row": 33,
            "table": "enhance", "tree_size": positions,
            "blocks": [{"height": 3_400_000, "hash": "11".repeat(32),
                "first_position": 0, "action_count": positions}],
        }))
        .unwrap(),
    )
    .unwrap();
}

#[test]
fn query_shard_uses_the_coverage_span_not_the_layout_unit() {
    let span = enhance_shared::protocol::Geometry::default().max_shard_rows
        * enhance_shared::RECORDS_PER_ROW as u64;
    let unit = ENHANCE_LAYOUT.shard_positions() as u64;
    assert_eq!(span, 32_768 * 33);
    assert_eq!(unit * 4, span);
    assert_eq!(query_shard(0), 0);
    assert_eq!(query_shard(unit), 0, "second layout unit is still domain 0");
    assert_eq!(query_shard(span - 1), 0);
    assert_eq!(query_shard(span), 1);
}

/// A fee added in the second 8,192-row unit of query domain 0 must be
/// refused when that domain is sealed. Dividing by the layout unit would
/// call this position shard 1 and adopt it.
#[test]
fn query_shard_sealed_domain_covers_every_layout_unit() {
    let unit = ENHANCE_LAYOUT.shard_positions() as u64;
    let positions = unit + 1;
    let root = tempfile::tempdir().unwrap();
    let old = root.path().join("old");
    let new = root.path().join("new");
    sparse_journal(&old, positions, unit, None);
    sparse_journal(&new, positions, unit, Some(20_000));
    let old = CommittedJournalSnapshot::read(&old, DatabaseId::Enhance, ENHANCE_LAYOUT).unwrap();
    let new = CommittedJournalSnapshot::read(&new, DatabaseId::Enhance, ENHANCE_LAYOUT).unwrap();
    let Err(refused) = compare_journals(&old, &new, &BTreeSet::from([0])) else {
        panic!("sealed domain 0 must refuse a change in its second unit");
    };
    let message = refused.to_string();
    assert!(message.contains("sealed shard 0"), "{message}");
    assert!(message.contains(&format!("record {unit}")), "{message}");
    // The next domain's id must not swallow a change that is still in domain 0.
    assert!(compare_journals(&old, &new, &BTreeSet::from([1])).is_ok());
}

#[test]
fn oracle_accepts_only_added_fees() {
    let old = record(None, 3);
    assert_eq!(fee_only_change(&old, &old), Ok(false));
    assert_eq!(fee_only_change(&old, &record(Some(20_000), 3)), Ok(true));
    assert!(fee_only_change(&old, &record(Some(20_000), 1)).is_err());
    assert!(fee_only_change(&record(Some(1), 3), &record(Some(2), 3)).is_err());
    let mut ciphertext = record(Some(20_000), 3);
    ciphertext[0] ^= 1;
    assert!(fee_only_change(&old, &ciphertext).is_err());
    let mut expiry = record(Some(20_000), 3);
    expiry[RECORD_FLAGS_OFFSET + 1] ^= 1;
    assert!(fee_only_change(&old, &expiry).is_err());
}
