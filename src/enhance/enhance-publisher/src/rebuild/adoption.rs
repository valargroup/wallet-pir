//! Validate and swap a completed staged journal before the live writer opens.
use super::{
    comparison::compare_journals, is_prefix, now_seconds, JournalAdoption, RebuildReceipt,
    RECEIPT_FILE, RECEIPT_FORMAT, STAGED_DIR,
};
use crate::{
    journal::CommittedJournalSnapshot,
    types::{DatabaseId, ENHANCE_LAYOUT},
};
use pir_control::durable::sync_dir;
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fs, path::Path};

/// Verifies a completed staged rebuild against the live journal.
/// Returns the receipt when the staged journal may replace `live`.
fn verify_staged(
    data_dir: &Path,
    staged_dir: &Path,
    live_dir: &Path,
) -> Result<RebuildReceipt, String> {
    let receipt: RebuildReceipt = serde_json::from_slice(
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
    let staged = CommittedJournalSnapshot::read(staged_dir, DatabaseId::Enhance, ENHANCE_LAYOUT)
        .map_err(|e| format!("staged journal: {e}"))?;
    if hex::encode(Sha256::digest(staged.manifest_bytes())) != receipt.manifest_sha256 {
        return Err("staged manifest digest differs from the receipt".into());
    }
    if staged.uncommitted_bytes().map_err(|e| e.to_string())? != 0 {
        return Err("staged records file has a tail".into());
    }
    let last = staged.blocks().last().ok_or("staged journal is empty")?;
    if last.height != receipt.height
        || last.hash != receipt.block_hash
        || staged.tree_size() != receipt.tree_size
    {
        return Err("receipt tip differs from the staged journal".into());
    }
    let live = CommittedJournalSnapshot::read(live_dir, DatabaseId::Enhance, ENHANCE_LAYOUT)
        .map_err(|e| format!("live journal: {e}"))?;
    if !is_prefix(staged.blocks(), live.blocks()) {
        return Err("staged blocks are not a prefix of the live journal".into());
    }
    let sealed = sealed_shards(data_dir)?;
    // One streaming pass: oracle, sealed shards and the records digest. The
    // staged file has no tail, so the digest covers all of `records.bin`.
    let comparison = compare_journals(&live, &staged, &sealed).map_err(|e| e.to_string())?;
    if comparison.records_sha256 != receipt.records_sha256 {
        return Err("staged records digest differs from the receipt".into());
    }
    if comparison.changed != receipt.changed_records
        || comparison.absent != receipt.absent_fee_records
    {
        return Err(format!(
            "receipt counts {}/{} differ from the journals' {}/{}",
            receipt.changed_records,
            receipt.absent_fee_records,
            comparison.changed,
            comparison.absent
        ));
    }
    Ok(receipt)
}

/// Sealed domain ids from publisher recovery state, read before journal adoption.
fn sealed_shards(data_dir: &Path) -> Result<BTreeSet<u64>, String> {
    let path = data_dir.join("publication").join("publisher.json");
    if !path.exists() {
        return Ok(BTreeSet::new());
    }
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).map_err(|e| format!("publisher state: {e}"))?)
            .map_err(|e| format!("publisher state: {e}"))?;
    let Some(sealed) = state.pointer("/recovery/sealed") else {
        return Err("publisher state has no recovery.sealed".into());
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
pub fn adopt_staged(data_dir: &Path) -> Result<JournalAdoption, std::io::Error> {
    let staged_dir = data_dir.join(STAGED_DIR);
    let live_dir = data_dir.join(DatabaseId::Enhance.as_str());
    if !staged_dir.join(RECEIPT_FILE).exists() {
        // Absent, or a rebuild still in progress: never touch it.
        return Ok(JournalAdoption::None);
    }
    let receipt_id = fs::read(staged_dir.join(RECEIPT_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<RebuildReceipt>(&bytes).ok())
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
    let verified = if !interrupted && previous.as_ref().is_some_and(|p| p.exists()) {
        Err("the rollback copy for this receipt already exists".to_string())
    } else {
        verify_staged(data_dir, &staged_dir, &compare_dir)
    };
    match verified {
        Ok(receipt) => {
            let previous = previous.expect("verified receipt id");
            if !interrupted {
                fs::rename(&live_dir, &previous)?;
                sync_dir(data_dir)?;
            }
            fs::rename(&staged_dir, &live_dir)?;
            sync_dir(data_dir)?;
            Ok(JournalAdoption::Adopted {
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
            Ok(JournalAdoption::Rejected { reason, moved_to })
        }
    }
}
