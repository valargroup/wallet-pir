//! Offline repair of historical Enhance fee metadata and its adoption.
//!
//! The journal stores only encoded records, so a record published without a
//! fee can only be repaired by re-deriving it from chain. `rebuild_journal`
//! re-runs the coordinator's producer over a copy of the live journal's
//! history and accepts a regenerated record only if it differs from the old
//! one in the fee fields alone. `adopt_staged` swaps a completed rebuild in
//! before the coordinator opens its journal, after checking it again against
//! the live journal; it refuses, and keeps the live journal, on any doubt.

use crate::store::{BlockEntry, JournalSnapshot, RecordJournal, StoreError};
use crate::types::{DatabaseId, ENHANCE_LAYOUT};
use crate::zakura::{Prevouts, ZakuraClient, ZakuraError};
use enhance_pir::types::{
    EnhanceRecord, FLAG_HAS_FEE, RECORD_BYTES, RECORD_FEE_OFFSET, RECORD_FLAGS_OFFSET,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, VecDeque};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Staged rebuild directory inside a coordinator data directory.
pub const STAGED_DIR: &str = "enhance-staged";
/// Completion receipt, written last inside the staged directory.
pub const RECEIPT_FILE: &str = "rebuild.json";
const RECEIPT_FORMAT: &str = "enhance-journal-rebuild-v1";
/// Records compared per read when streaming two journals.
const COMPARE_RECORDS: usize = 4096;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum RebuildError {
    #[error("journal error: {0}")]
    Store(#[from] StoreError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Zakura error at height {0}: {1}")]
    Zakura(u64, ZakuraError),
    #[error("rebuild refused: {0}")]
    Refused(String),
    #[error("record {position} differs outside the fee fields: {reason}")]
    Oracle { position: u64, reason: String },
}

/// Completion receipt of a staged rebuild. Digests bind the staged files.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub format: String,
    pub receipt_id: String,
    /// Compile-time `WALLET_PIR_SOURCE_REVISION`, or `unrecorded`.
    pub binary_source_revision: String,
    pub binary_sha256: String,
    pub height: u64,
    pub block_hash: String,
    pub tree_size: u64,
    pub records_sha256: String,
    pub manifest_sha256: String,
    pub changed_records: u64,
    /// Records still without a fee: Ironwood outputs of coinbase transactions.
    pub absent_fee_records: u64,
    pub completed_unix_seconds: u64,
}

pub struct RebuildConfig {
    pub source: PathBuf,
    pub output: PathBuf,
    pub through_height: Option<u64>,
    pub concurrency: usize,
    pub cache_outputs: usize,
}

/// Whether `new` is `old` with only its fee metadata filled in.
///
/// Byte 640 bit 2 and bytes 645..653 may change, and only if `old` had no
/// fee; an old record with a fee must be identical. `new` must decode.
/// Returns whether the record changed.
pub fn fee_only_change(old: &[u8], new: &[u8]) -> Result<bool, String> {
    if old.len() != RECORD_BYTES || new.len() != RECORD_BYTES {
        return Err("record width".into());
    }
    let new_record = EnhanceRecord::from_bytes(new.try_into().expect("checked width"))
        .map_err(|e| e.to_string())?;
    if old == new {
        return Ok(false);
    }
    if old[RECORD_FLAGS_OFFSET] & FLAG_HAS_FEE != 0 {
        return Err("an already published fee changed".into());
    }
    if old[..RECORD_FLAGS_OFFSET] != new[..RECORD_FLAGS_OFFSET]
        || old[RECORD_FLAGS_OFFSET] & !FLAG_HAS_FEE != new[RECORD_FLAGS_OFFSET] & !FLAG_HAS_FEE
        || old[RECORD_FLAGS_OFFSET + 1..RECORD_FEE_OFFSET]
            != new[RECORD_FLAGS_OFFSET + 1..RECORD_FEE_OFFSET]
    {
        return Err("ciphertext, transparent flags or expiry changed".into());
    }
    if new_record.metadata().fee_zatoshis().is_none() {
        return Err("fee payload changed without a fee".into());
    }
    Ok(true)
}

fn sha256_file(path: &Path) -> Result<String, std::io::Error> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(hex::encode(hasher.finalize()));
        }
        hasher.update(&buffer[..read]);
    }
}

fn sync_dir(path: &Path) -> Result<(), std::io::Error> {
    File::open(path)?.sync_all()
}

/// Compares the first `staged.tree_size()` records of two journals, counting
/// changed records and records without a fee. Returns changed positions only
/// when `collect` is set.
fn compare_journals(
    old: &JournalSnapshot,
    new: &JournalSnapshot,
    collect: bool,
) -> Result<(u64, u64, Vec<u64>), RebuildError> {
    let (mut changed, mut absent, mut positions) = (0, 0, Vec::new());
    let mut start = 0;
    while start < new.tree_size() {
        let count = (new.tree_size() - start).min(COMPARE_RECORDS as u64) as usize;
        let old_bytes = old.read_records(start, count)?;
        let new_bytes = new.read_records(start, count)?;
        for (index, (o, n)) in old_bytes
            .chunks_exact(RECORD_BYTES)
            .zip(new_bytes.chunks_exact(RECORD_BYTES))
            .enumerate()
        {
            let position = start + index as u64;
            if fee_only_change(o, n).map_err(|reason| RebuildError::Oracle { position, reason })? {
                changed += 1;
                if collect {
                    positions.push(position);
                }
            }
            if n[RECORD_FLAGS_OFFSET] & FLAG_HAS_FEE == 0 {
                absent += 1;
            }
        }
        start += count as u64;
    }
    Ok((changed, absent, positions))
}

fn is_prefix(staged: &[BlockEntry], live: &[BlockEntry]) -> bool {
    staged.len() <= live.len() && staged.iter().zip(live).all(|(s, l)| s == l)
}

fn now_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Re-derives the source journal's records into `config.output`.
///
/// Reads the source without its writer lock. Resumes from the staged tip; the
/// staged block list must remain a prefix of the source's. Every block hash
/// is checked against the node, every regenerated record against the old one,
/// and the receipt is written only after the staged files are durable.
pub async fn rebuild_journal(
    rpc: ZakuraClient,
    config: RebuildConfig,
) -> Result<Receipt, RebuildError> {
    let source_dir = config.source.join(DatabaseId::Enhance.as_str());
    let source = JournalSnapshot::read(&source_dir, DatabaseId::Enhance, ENHANCE_LAYOUT)?;
    let tip = source
        .blocks()
        .last()
        .ok_or_else(|| RebuildError::Refused("source journal is empty".into()))?
        .height;
    let height = config.through_height.unwrap_or(tip);
    let blocks: Vec<BlockEntry> = source
        .blocks()
        .iter()
        .take_while(|block| block.height <= height)
        .cloned()
        .collect();
    let last = blocks
        .last()
        .filter(|block| block.height == height)
        .cloned()
        .ok_or_else(|| {
            RebuildError::Refused(format!("height {height} is outside the source journal"))
        })?;
    if config.output.join(RECEIPT_FILE).exists() {
        return Err(RebuildError::Refused(
            "output already holds a completed rebuild".into(),
        ));
    }
    if fs::canonicalize(&source_dir).ok() == fs::canonicalize(&config.output).ok() {
        return Err(RebuildError::Refused("output is the source journal".into()));
    }
    let mut staged = RecordJournal::open(&config.output, DatabaseId::Enhance, ENHANCE_LAYOUT)?;
    if !is_prefix(staged.blocks(), &blocks) {
        return Err(RebuildError::Refused(
            "staged blocks are not a prefix of the source journal".into(),
        ));
    }
    let heights: Vec<u64> = blocks.iter().map(|b| b.height).collect();
    let hashes = rpc
        .block_hashes(&heights)
        .await
        .map_err(|e| RebuildError::Zakura(height, e))?;
    if let Some((block, hash)) = blocks.iter().zip(&hashes).find(|(b, h)| b.hash != **h) {
        return Err(RebuildError::Refused(format!(
            "source block {} is {}, chain has {hash}",
            block.height, block.hash
        )));
    }
    let prevouts = Arc::new(Prevouts::new(config.cache_outputs));
    let pending: Vec<BlockEntry> = blocks[staged.blocks().len()..].to_vec();
    let mut fetches = VecDeque::new();
    let mut next_fetch = pending.iter().filter(|b| b.action_count > 0);
    let concurrency = config.concurrency.max(1);
    let started = Instant::now();
    let mut last_report = Instant::now();
    let mut processed = 0u64;
    for block in &pending {
        while fetches.len() < concurrency {
            let Some(entry) = next_fetch.next() else {
                break;
            };
            let (rpc, prevouts, h) = (rpc.clone(), prevouts.clone(), entry.height);
            fetches.push_back((
                h,
                tokio::spawn(async move { rpc.block_records(h, &prevouts).await }),
            ));
        }
        let mut records = Vec::new();
        if block.action_count > 0 {
            let (fetched_height, task) = fetches.pop_front().expect("fetch for a non-empty block");
            debug_assert_eq!(fetched_height, block.height);
            let (hash, regenerated) = task
                .await
                .map_err(|e| RebuildError::Refused(format!("fetch task failed: {e}")))?
                .map_err(|e| RebuildError::Zakura(block.height, e))?;
            if hash != block.hash {
                return Err(RebuildError::Refused(format!(
                    "raw block {} hashes to {hash}, journal has {}",
                    block.height, block.hash
                )));
            }
            if regenerated.len() as u64 != block.action_count {
                return Err(RebuildError::Refused(format!(
                    "block {} regenerated {} actions, journal has {}",
                    block.height,
                    regenerated.len(),
                    block.action_count
                )));
            }
            let old = source.read_records(block.first_position, regenerated.len())?;
            for (index, (o, n)) in old.chunks_exact(RECORD_BYTES).zip(&regenerated).enumerate() {
                fee_only_change(o, n.as_bytes()).map_err(|reason| RebuildError::Oracle {
                    position: block.first_position + index as u64,
                    reason,
                })?;
            }
            records = regenerated;
        }
        staged.append_block(block.height, block.hash.clone(), &records)?;
        processed += 1;
        if last_report.elapsed() >= PROGRESS_INTERVAL {
            last_report = Instant::now();
            let counts = prevouts.counts();
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "rebuild_progress", "height": block.height, "target": height,
                    "blocks": processed, "remaining": pending.len() as u64 - processed,
                    "blocks_per_second": processed as f64 / started.elapsed().as_secs_f64(),
                    "same_block_hits": counts.same_block_hits, "cache_hits": counts.cache_hits,
                    "prevout_fetches": counts.fetched_transactions, "prevout_batches": counts.batches,
                })
            );
        }
    }
    let expected_size = last.first_position + last.action_count;
    if staged.tree_size() != expected_size {
        return Err(RebuildError::Refused(format!(
            "staged tree size {} differs from the source's {expected_size}",
            staged.tree_size()
        )));
    }
    drop(staged);
    let chain_size = rpc
        .tree_size(height)
        .await
        .map_err(|e| RebuildError::Zakura(height, e))?;
    if chain_size != expected_size {
        return Err(RebuildError::Refused(format!(
            "chain Ironwood tree size at {height} is {chain_size}, journal has {expected_size}"
        )));
    }
    let staged = JournalSnapshot::read(&config.output, DatabaseId::Enhance, ENHANCE_LAYOUT)?;
    if staged.uncommitted_bytes()? != 0 {
        return Err(RebuildError::Refused(
            "staged records file has a tail".into(),
        ));
    }
    let (changed_records, absent_fee_records, _) = compare_journals(&source, &staged, false)?;
    let records_sha256 = sha256_file(&config.output.join("records.bin"))?;
    let manifest_sha256 = hex::encode(Sha256::digest(staged.manifest_bytes()));
    let receipt = Receipt {
        format: RECEIPT_FORMAT.into(),
        receipt_id: format!("{height}-{}", &records_sha256[..16]),
        binary_source_revision: option_env!("WALLET_PIR_SOURCE_REVISION")
            .unwrap_or("unrecorded")
            .into(),
        binary_sha256: sha256_file(&std::env::current_exe()?)?,
        height,
        block_hash: last.hash,
        tree_size: expected_size,
        records_sha256,
        manifest_sha256,
        changed_records,
        absent_fee_records,
        completed_unix_seconds: now_seconds(),
    };
    let temporary = config.output.join("rebuild.json.tmp");
    let mut file = File::create(&temporary)?;
    file.write_all(&serde_json::to_vec_pretty(&receipt).expect("receipt serializes"))?;
    file.sync_all()?;
    fs::rename(&temporary, config.output.join(RECEIPT_FILE))?;
    sync_dir(&config.output)?;
    let counts = prevouts.counts();
    eprintln!(
        "{}",
        serde_json::json!({
            "event": "rebuild_complete", "height": height, "blocks": processed,
            "seconds": started.elapsed().as_secs_f64(),
            "same_block_hits": counts.same_block_hits, "cache_hits": counts.cache_hits,
            "prevout_fetches": counts.fetched_transactions, "prevout_batches": counts.batches,
        })
    );
    Ok(receipt)
}

#[derive(Debug, PartialEq, Eq)]
pub enum Adoption {
    /// No completed staged rebuild is present.
    None,
    Adopted {
        receipt_id: String,
        height: u64,
        changed_records: u64,
        previous: PathBuf,
    },
    Rejected {
        reason: String,
        moved_to: PathBuf,
    },
}

/// Verifies a completed staged rebuild against the live journal.
/// Returns the receipt when the staged journal may replace `live`.
fn verify_staged(data_dir: &Path, staged_dir: &Path, live_dir: &Path) -> Result<Receipt, String> {
    let receipt: Receipt = serde_json::from_slice(
        &fs::read(staged_dir.join(RECEIPT_FILE)).map_err(|e| format!("receipt: {e}"))?,
    )
    .map_err(|e| format!("receipt: {e}"))?;
    if receipt.format != RECEIPT_FORMAT {
        return Err(format!("unknown receipt format {}", receipt.format));
    }
    if receipt.receipt_id.is_empty()
        || !receipt
            .receipt_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err("receipt id is not a safe directory suffix".into());
    }
    let staged = JournalSnapshot::read(staged_dir, DatabaseId::Enhance, ENHANCE_LAYOUT)
        .map_err(|e| format!("staged journal: {e}"))?;
    if hex::encode(Sha256::digest(staged.manifest_bytes())) != receipt.manifest_sha256 {
        return Err("staged manifest digest differs from the receipt".into());
    }
    if staged.uncommitted_bytes().map_err(|e| e.to_string())? != 0 {
        return Err("staged records file has a tail".into());
    }
    if sha256_file(&staged_dir.join("records.bin")).map_err(|e| e.to_string())?
        != receipt.records_sha256
    {
        return Err("staged records digest differs from the receipt".into());
    }
    let last = staged.blocks().last().ok_or("staged journal is empty")?;
    if last.height != receipt.height
        || last.hash != receipt.block_hash
        || staged.tree_size() != receipt.tree_size
    {
        return Err("receipt tip differs from the staged journal".into());
    }
    let live = JournalSnapshot::read(live_dir, DatabaseId::Enhance, ENHANCE_LAYOUT)
        .map_err(|e| format!("live journal: {e}"))?;
    if !is_prefix(staged.blocks(), live.blocks()) {
        return Err("staged blocks are not a prefix of the live journal".into());
    }
    let (changed, absent, positions) =
        compare_journals(&live, &staged, true).map_err(|e| e.to_string())?;
    if changed != receipt.changed_records || absent != receipt.absent_fee_records {
        return Err(format!(
            "receipt counts {}/{} differ from the journals' {changed}/{absent}",
            receipt.changed_records, receipt.absent_fee_records
        ));
    }
    let sealed = sealed_shards(data_dir)?;
    let shard_positions = ENHANCE_LAYOUT.shard_positions() as u64;
    if let Some(position) = positions
        .iter()
        .find(|p| sealed.contains(&(**p / shard_positions)))
    {
        return Err(format!(
            "changed record {position} is in sealed shard {}",
            position / shard_positions
        ));
    }
    Ok(receipt)
}

/// Shard ids the coordinator has sealed, read without the control lock.
fn sealed_shards(data_dir: &Path) -> Result<BTreeSet<u64>, String> {
    let path = data_dir.join("control").join("controller.json");
    if !path.exists() {
        return Ok(BTreeSet::new());
    }
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).map_err(|e| format!("control state: {e}"))?)
            .map_err(|e| format!("control state: {e}"))?;
    let Some(sealed) = state.pointer("/recovery/sealed") else {
        return Err("control state has no recovery.sealed".into());
    };
    sealed
        .as_object()
        .ok_or("recovery.sealed is not a map")?
        .keys()
        .map(|id| id.parse().map_err(|_| format!("sealed shard id {id}")))
        .collect()
}

/// Adopts `<data_dir>/enhance-staged` when it holds a verified rebuild.
///
/// Must run before the coordinator opens its journal. The live journal is
/// renamed to `enhance.before-rebuild-<receipt id>` and kept for rollback.
/// A crash between the two renames is completed on the next start. A failed
/// check renames the staged directory to `enhance-staged.rejected-<time>` and
/// keeps the live journal. `control/` is never modified.
pub fn adopt_staged(data_dir: &Path) -> Result<Adoption, std::io::Error> {
    let staged_dir = data_dir.join(STAGED_DIR);
    let live_dir = data_dir.join(DatabaseId::Enhance.as_str());
    if !staged_dir.join(RECEIPT_FILE).exists() {
        // Absent, or a rebuild still in progress: never touch it.
        return Ok(Adoption::None);
    }
    let receipt_id = fs::read(staged_dir.join(RECEIPT_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Receipt>(&bytes).ok())
        .map(|receipt| receipt.receipt_id);
    let previous = receipt_id
        .as_ref()
        .filter(|id| id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'))
        .map(|id| data_dir.join(format!("enhance.before-rebuild-{id}")));
    // After a crash between renames, the live journal is the preserved copy.
    let interrupted = !live_dir.exists() && previous.as_ref().is_some_and(|p| p.exists());
    let compare_dir = if interrupted {
        previous.clone().expect("checked")
    } else {
        live_dir.clone()
    };
    match verify_staged(data_dir, &staged_dir, &compare_dir) {
        Ok(receipt) => {
            let previous = previous.expect("verified receipt id");
            if !interrupted {
                fs::rename(&live_dir, &previous)?;
                sync_dir(data_dir)?;
            }
            fs::rename(&staged_dir, &live_dir)?;
            sync_dir(data_dir)?;
            Ok(Adoption::Adopted {
                receipt_id: receipt.receipt_id,
                height: receipt.height,
                changed_records: receipt.changed_records,
                previous,
            })
        }
        Err(reason) => {
            if interrupted {
                fs::rename(&compare_dir, &live_dir)?;
            }
            let stamp = now_seconds();
            let moved_to = (0..)
                .map(|n| match n {
                    0 => data_dir.join(format!("{STAGED_DIR}.rejected-{stamp}")),
                    n => data_dir.join(format!("{STAGED_DIR}.rejected-{stamp}-{n}")),
                })
                .find(|path| !path.exists())
                .expect("unbounded names");
            fs::rename(&staged_dir, &moved_to)?;
            sync_dir(data_dir)?;
            Ok(Adoption::Rejected { reason, moved_to })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(fee: Option<u64>, flags: u8) -> Vec<u8> {
        let mut bytes = vec![7u8; RECORD_BYTES];
        bytes[RECORD_FLAGS_OFFSET] = flags | fee.map_or(0, |_| FLAG_HAS_FEE);
        bytes[RECORD_FLAGS_OFFSET + 1..RECORD_FEE_OFFSET].copy_from_slice(&100u32.to_le_bytes());
        bytes[RECORD_FEE_OFFSET..].copy_from_slice(&fee.unwrap_or(0).to_le_bytes());
        bytes
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
}
