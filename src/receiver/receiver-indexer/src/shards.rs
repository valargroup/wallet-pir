//! A publication's Receiver shards: sealed shards' segments and filters, then the open
//! shard.
//!
//! A sealed shard's directories never change under one correction generation on one
//! chain. Its companions are rebuilt at each publication's tip until every payment's
//! Ironwood shard has completed, then frozen at the block completing the last of them
//! (or the shard's end, if later): a frozen companion depends on that block, so a rewind
//! past it rebuilds the companion even when the directory is unchanged. The open shard's
//! single segment is rebuilt every publication, its choice seeded by the tip.
use crate::{
    seal::{self, Policy},
    witness::Witnesses,
};
use receiver_store::{Checkpoint, Error, SealedShard, Store, SHARD_LEAVES};
use std::sync::Arc;
use zakura_pir_publisher::receiver::wire::is_final;
use zakura_pir_publisher::receiver::{
    self, encode_slot, query_rows, Companion, CompanionTable, Coverage, Directory, Entry, Paid,
    Projection, Table as _, Tree, OPEN_SEED_ATTEMPTS, ROW_BYTES, ROW_SLOTS, SCHEMA, SLOT_BYTES,
};
use zakura_pir_publisher::receiver::{
    shards::{Coverage as FilterCoverage, ShardFilter},
    Hash, RawRecord,
};

/// One segment's freshly built tables, its directory's entries, its records in slot
/// order and the block its companion is frozen at, or `None` while it is provisional.
pub struct Built {
    pub tables: receiver::Segment,
    pub entries: Vec<Entry>,
    pub records: Arc<Vec<RawRecord>>,
    pub frozen: Option<Checkpoint>,
}

/// A sealed shard's freshly built segments and receiver filter.
pub struct BuiltShard {
    pub shard: SealedShard,
    pub end: Checkpoint,
    pub segments: Vec<Built>,
    pub filter: ShardFilter,
}

/// The coverage of the blocks after `parent` through `end`.
fn coverage(store: &Store, parent: &Checkpoint, end: &Checkpoint, revision: u64) -> Coverage {
    Coverage {
        genesis: store.config().genesis,
        history_revision: revision,
        start_height: parent.height + 1,
        start_parent: parent.hash,
        start_position: parent.position,
        end_height: end.height,
        end_hash: end.hash,
        end_position: end.position,
    }
}

/// Builds every segment of sealed `shard` from the store, replaying the sealer's
/// projection, with each choice seeded by the sealing block and each companion at
/// `tip` or frozen, and its filter.
pub fn sealed(
    store: &mut Store,
    witnesses: &mut Witnesses,
    policy: &Policy,
    shard: &SealedShard,
    tip: &Checkpoint,
    history_revision: u64,
    stop: &dyn Fn() -> bool,
) -> Result<BuiltShard, Error> {
    let parent = store.checkpoint(shard.start_height - 1)?;
    let end = store.checkpoint(shard.end_height)?;
    let coverage = coverage(store, &parent, &end, history_revision);
    let mut receivers = Vec::new();
    let mut segments = Vec::new();
    for (segment, records) in seal::replay(policy, store, shard)? {
        receivers.extend(records.iter().map(|r| r.receiver));
        segments.push(build(
            store,
            witnesses,
            &segment.projection,
            (coverage.clone(), &end.hash, policy.seal_seeds),
            true,
            records,
            tip,
            stop,
        )?);
    }
    let filter_coverage = FilterCoverage {
        start_height: shard.start_height,
        end_height: end.height,
        end_hash: end.hash,
    };
    let filter = ShardFilter::build(
        coverage.genesis,
        history_revision,
        filter_coverage,
        &receivers,
    )?;
    Ok(BuiltShard {
        shard: shard.clone(),
        end,
        segments,
        filter,
    })
}

/// The open shard's segment from `start` through `tip`, from the sealer's projection
/// of those blocks, its choice seeded by the tip.
pub fn open(
    store: &mut Store,
    witnesses: &mut Witnesses,
    (start, projection): (u32, &Projection),
    tip: &Checkpoint,
    history_revision: u64,
    stop: &dyn Fn() -> bool,
) -> Result<Built, Error> {
    let parent = store.checkpoint(start - 1)?;
    let coverage = coverage(store, &parent, tip, history_revision);
    let records = store.records(start, tip.height)?;
    build(
        store,
        witnesses,
        projection,
        (coverage, &tip.hash, OPEN_SEED_ATTEMPTS),
        false,
        records,
        tip,
        stop,
    )
}

/// Builds one segment of `records` under `projection` with `receiver::build`, seeded by
/// `seed` over `attempts`, its companion at `tip` or frozen, reusing final payloads
/// the store recorded. With `keep`, a provisional companion's records are returned for
/// rebuilding it. The companion has exactly the rows its payments need, so
/// its query is never trimmed below them.
#[allow(clippy::too_many_arguments)]
fn build(
    store: &mut Store,
    witnesses: &mut Witnesses,
    projection: &Projection,
    (coverage, seed, attempts): (Coverage, &Hash, u32),
    keep: bool,
    records: Vec<RawRecord>,
    tip: &Checkpoint,
    stop: &dyn Fn() -> bool,
) -> Result<Built, Error> {
    let positions: Vec<u64> = records.iter().map(|r| r.payment.position).collect();
    let (payloads, frozen, tree) =
        payloads(store, witnesses, coverage.end_height, &positions, tip)?;
    halt(stop)?;
    let paid: Vec<Paid> = records
        .into_iter()
        .zip(payloads)
        .map(|(r, payload)| (r.receiver, r.payment, payload))
        .collect();
    let rows = query_rows(paid.len() as u32);
    let tables = receiver::build(projection, coverage, seed, attempts, rows, tree, &paid)?;
    halt(stop)?;
    let entries = tables.directory.validate()?;
    // A kept sealed segment rebuilds its provisional companion from its records; the
    // open segment is rebuilt from the store and keeps none.
    if !keep || frozen.is_some() {
        return Ok(Built {
            tables,
            entries,
            records: Arc::default(),
            frozen,
        });
    }
    let records = (0..tables.companion.manifest.payments)
        .map(|slot| {
            let at = slot_at(slot);
            receiver::decode_slot(&tables.companion.data[at..at + SLOT_BYTES])?
                .map(|(record, _)| record)
                .ok_or(zakura_pir_publisher::receiver::Error::Malformed)
        })
        .collect::<Result<_, _>>()?;
    Ok(Built {
        tables,
        entries,
        records: Arc::new(records),
        frozen,
    })
}

/// Rebuilds a kept sealed segment's companion at `tip` for `directory` from its
/// `records` in slot order. Returns it with the block it is frozen at.
pub fn rebuild(
    store: &mut Store,
    witnesses: &mut Witnesses,
    directory: &Directory,
    records: &[RawRecord],
    tip: &Checkpoint,
    stop: &dyn Fn() -> bool,
) -> Result<(CompanionTable, Option<Checkpoint>), Error> {
    let positions: Vec<u64> = records.iter().map(|r| r.payment.position).collect();
    let end = directory.coverage.end_height;
    let (payloads, frozen, tree) = payloads(store, witnesses, end, &positions, tip)?;
    halt(stop)?;
    let rows = query_rows(records.len() as u32);
    let mut data = vec![0; rows as usize * ROW_BYTES];
    for (slot, (record, payload)) in records.iter().zip(&payloads).enumerate() {
        let at = slot_at(slot as u32);
        data[at..at + SLOT_BYTES].copy_from_slice(&encode_slot(record, payload)?);
    }
    use sha2::Digest;
    let table = CompanionTable {
        manifest: Companion {
            schema: SCHEMA.into(),
            coverage: directory.coverage.clone(),
            directory: directory.revision()?,
            rows,
            query_rows: rows,
            payments: records.len() as u32,
            tree,
            data_sha256: sha2::Sha256::digest(&data).into(),
        },
        data,
    };
    // Checked where it is used: preparation validates it, and a stored copy is reused
    // only with identical bytes.
    Ok((table, frozen))
}

/// The byte offset of companion slot `slot`.
fn slot_at(slot: u32) -> usize {
    (slot / ROW_SLOTS) as usize * ROW_BYTES + (slot % ROW_SLOTS) as usize * SLOT_BYTES
}

/// The payloads of `positions` at `tip`, with the block the companion is frozen at and
/// its tree: frozen at the later of the segment's end and the block completing its last
/// payment's Ironwood shard once that shard is complete at `tip`, else provisional at
/// `tip`. Final payloads are the same at either tree.
#[allow(clippy::type_complexity)]
fn payloads(
    store: &mut Store,
    witnesses: &mut Witnesses,
    end: u32,
    positions: &[u64],
    tip: &Checkpoint,
) -> Result<(Vec<[u8; 512]>, Option<Checkpoint>, Tree), Error> {
    let payloads = witnesses.payloads(store, tip, positions)?;
    let frozen = match positions.iter().max().copied() {
        Some(last) if !is_final(last, tip.position) => None,
        Some(last) => {
            let shard = u32::try_from(last / SHARD_LEAVES).map_err(|_| Error::Malformed)?;
            let completed = store.completed_shard(shard)?.ok_or(Error::Coverage)?;
            Some(store.checkpoint(completed.height.max(end))?)
        }
        None => Some(store.checkpoint(end)?),
    };
    let context = frozen.as_ref().unwrap_or(tip);
    let tree = Tree {
        height: context.height,
        hash: context.hash,
        size: context.position,
    };
    Ok((payloads, frozen, tree))
}

/// [`Error::Stopped`] once `stop` is true.
fn halt(stop: &dyn Fn() -> bool) -> Result<(), Error> {
    match stop() {
        true => Err(Error::Stopped),
        false => Ok(()),
    }
}

#[cfg(test)]
mod tests;
