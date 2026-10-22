//! Re-derive a staged journal from canonical blocks and write its receipt last.
use super::{
    comparison::{compare_journals, JournalComparison},
    fee_only_change, is_prefix, now_seconds, RebuildConfig, RebuildError, RebuildReceipt,
    PROGRESS_INTERVAL, RECEIPT_FILE, RECEIPT_FORMAT,
};
use crate::{
    chain_source::{Prevouts, ZakuraClient},
    journal::{CommittedJournalSnapshot, JournalBlock, RecordJournal},
    types::{DatabaseId, ENHANCE_LAYOUT},
};
use enhance_query_server::RECORD_BYTES;
use pir_control::durable::sync_dir;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, VecDeque},
    fs::{self, File},
    io::Write,
    path::Path,
    sync::Arc,
    time::Instant,
};

pub(super) fn sha256_file(path: &Path) -> Result<String, std::io::Error> {
    pir_control::durable::sha256_file(path).map(hex::encode)
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
) -> Result<RebuildReceipt, RebuildError> {
    let source_dir = config.source.join(DatabaseId::Enhance.as_str());
    let source = CommittedJournalSnapshot::read(&source_dir, DatabaseId::Enhance, ENHANCE_LAYOUT)?;
    let tip = source
        .blocks()
        .last()
        .ok_or_else(|| RebuildError::Refused("source journal is empty".into()))?
        .height;
    let height = config.through_height.unwrap_or(tip);
    let blocks: Vec<JournalBlock> = source
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
    let hashes: Vec<String> = rpc
        .block_hashes(&heights)
        .await
        .map_err(|e| RebuildError::Zakura(height, e))?
        .iter()
        .map(zakura_server_chain::hash::display)
        .collect();
    if let Some((block, hash)) = blocks.iter().zip(&hashes).find(|(b, h)| b.hash != **h) {
        return Err(RebuildError::Refused(format!(
            "source block {} is {}, chain has {hash}",
            block.height, block.hash
        )));
    }
    let prevouts = Arc::new(Prevouts::new(config.cache_outputs));
    let pending: Vec<JournalBlock> = blocks[staged.blocks().len()..].to_vec();
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
                tokio::spawn(async move {
                    crate::chain_source::block_records(&rpc, h, &prevouts).await
                }),
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
    let staged =
        CommittedJournalSnapshot::read(&config.output, DatabaseId::Enhance, ENHANCE_LAYOUT)?;
    if staged.uncommitted_bytes()? != 0 {
        return Err(RebuildError::Refused(
            "staged records file has a tail".into(),
        ));
    }
    // The staged file has no tail, so hashing its committed records hashes the file.
    let JournalComparison {
        changed: changed_records,
        absent: absent_fee_records,
        records_sha256,
    } = compare_journals(&source, &staged, &BTreeSet::new())?;
    let manifest_sha256 = hex::encode(Sha256::digest(staged.manifest_bytes()));
    let receipt = RebuildReceipt {
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
