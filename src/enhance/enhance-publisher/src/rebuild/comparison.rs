//! Stream fee-only comparisons without changing published non-fee bytes.
use super::{RebuildError, COMPARE_RECORDS};
use crate::journal::CommittedJournalSnapshot;
use enhance_material::{
    EnhanceRecord, FLAG_HAS_FEE, RECORD_BYTES, RECORD_FEE_OFFSET, RECORD_FLAGS_OFFSET,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

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

/// One streaming pass over two journals.
pub(super) struct JournalComparison {
    pub(super) changed: u64,
    pub(super) absent: u64,
    /// SHA-256 of the new journal's committed records, in file order.
    pub(super) records_sha256: String,
}

/// Compares the first `new.tree_size()` records of two journals with the fee
/// oracle, counting changed and fee-absent records and hashing `new`.
///
/// Reads both files once in fixed-size chunks through two reused buffers, so
/// memory stays near 2 × `COMPARE_RECORDS` records whatever the journal size.
/// A changed record in a `sealed` shard is an error.
pub(super) fn compare_journals(
    old: &CommittedJournalSnapshot,
    new: &CommittedJournalSnapshot,
    sealed: &BTreeSet<u64>,
) -> Result<JournalComparison, RebuildError> {
    let (mut changed, mut absent) = (0, 0);
    let mut hasher = Sha256::new();
    let mut old_bytes = vec![0u8; COMPARE_RECORDS * RECORD_BYTES];
    let mut new_bytes = vec![0u8; COMPARE_RECORDS * RECORD_BYTES];
    let mut start = 0;
    while start < new.tree_size() {
        let count = (new.tree_size() - start).min(COMPARE_RECORDS as u64) as usize;
        let (old_chunk, new_chunk) = (
            &mut old_bytes[..count * RECORD_BYTES],
            &mut new_bytes[..count * RECORD_BYTES],
        );
        old.read_records_into(start, old_chunk)?;
        new.read_records_into(start, new_chunk)?;
        hasher.update(&*new_chunk);
        for (index, (o, n)) in old_chunk
            .chunks_exact(RECORD_BYTES)
            .zip(new_chunk.chunks_exact(RECORD_BYTES))
            .enumerate()
        {
            let position = start + index as u64;
            if fee_only_change(o, n).map_err(|reason| RebuildError::Oracle { position, reason })? {
                changed += 1;
                let shard = query_shard(position);
                if sealed.contains(&shard) {
                    return Err(RebuildError::Refused(format!(
                        "changed record {position} is in sealed shard {shard}"
                    )));
                }
            }
            if n[RECORD_FLAGS_OFFSET] & FLAG_HAS_FEE == 0 {
                absent += 1;
            }
        }
        start += count as u64;
    }
    Ok(JournalComparison {
        changed,
        absent,
        records_sha256: hex::encode(hasher.finalize()),
    })
}

/// Query-domain id of a journal position.
///
/// `recovery.sealed` stores the ids `Lifecycle::coverage` assigns. A domain
/// holds `max_shard_rows` × records per row (1,081,344 records). The storage
/// layout's `shard_positions` is the 8,192-row unit inside that domain, so
/// dividing by it places three quarters of a sealed domain, and every record
/// of every later domain, outside the sealed set.
pub(super) fn query_shard(position: u64) -> u64 {
    let span = enhance_material::protocol::Geometry::default().max_shard_rows
        * enhance_material::RECORDS_PER_ROW as u64;
    position / span
}
