//! A publication's Receiver shards: sealed shards' segments and filters, then the
//! recent tier.
//!
//! A sealed shard's payment tables never change under one correction generation on
//! one chain. Its companion witness tables are rebuilt at each publication's tip until
//! every payment's Ironwood shard has completed, then frozen at the block completing
//! the last of them (or the shard's end, if later): a frozen companion depends on that
//! block, so a rewind past it rebuilds the companion even when the payment table is
//! unchanged. The recent tier is rebuilt every publication.
use crate::{
    seal::{self, Policy},
    verify,
    witness::{self as witnesses, Witnesses},
};
use receiver_store::{Checkpoint, Error, SealedShard, Store, SALT_ATTEMPTS, SHARD_LEAVES};
use zakura_pir_publisher::receiver::wire::is_final;
use zakura_pir_publisher::receiver::{
    shards::{Coverage, ShardFilter},
    snapshot::{Manifest, Snapshot, MAX_ROWS, PROFILE},
    witness, Hash,
};

/// One segment's freshly built tables, its checked records, and the block its companion
/// is frozen at, or `None` while it is provisional at the publication's tip.
pub struct Built {
    pub snapshot: Snapshot,
    pub witnesses: witness::Table,
    pub leaves: Vec<(u64, Hash, u32)>,
    pub frozen: Option<Checkpoint>,
}

/// A sealed shard's freshly built segments and receiver filter.
pub struct BuiltShard {
    pub shard: SealedShard,
    pub end: Checkpoint,
    pub segments: Vec<Built>,
    pub filter: ShardFilter,
}

/// Builds every segment of sealed `shard` from the store, with each companion at `tip`
/// or frozen, and its filter. Segments are re-split exactly as the sealer split them.
pub fn sealed(
    store: &mut Store,
    witnesses: &mut Witnesses,
    shard: &SealedShard,
    tip: &Checkpoint,
    history_revision: u64,
    stop: &dyn Fn() -> bool,
) -> Result<BuiltShard, Error> {
    let genesis = store.config().genesis;
    let parent = store.checkpoint(shard.start_height - 1)?;
    let end = store.checkpoint(shard.end_height)?;
    let records = store.records(shard.start_height, shard.end_height)?;
    let receivers: Vec<_> = records.iter().map(|r| r.receiver).collect();
    let policy = Policy {
        rows: shard.rows,
        payloads: shard.payloads,
        target: u64::MAX,
    };
    let parts = match shard.segments {
        1 => vec![records],
        _ => seal::split(&policy, genesis, &parent.hash, &records)?,
    };
    if parts.len() != shard.segments as usize {
        return Err(Error::Coverage);
    }
    let template = Manifest {
        profile: PROFILE.into(),
        genesis,
        history_revision,
        start_height: shard.start_height,
        start_parent: parent.hash,
        start_position: parent.position,
        end_height: end.height,
        end_hash: end.hash,
        end_position: end.position,
        rows: shard.rows,
        salt: [0; 32],
        records: 0,
        data_sha256: [0; 32],
    };
    let mut segments = Vec::new();
    for (index, records) in parts.into_iter().enumerate() {
        let salts =
            (0..SALT_ATTEMPTS).map(|attempt| seal::salt(&parent.hash, index as u32, attempt));
        let snapshot = receiver_store::table(template.clone(), records, salts)?;
        segments.push(build(store, witnesses, snapshot, tip, stop)?);
    }
    let coverage = Coverage {
        start_height: shard.start_height,
        end_height: end.height,
        end_hash: end.hash,
    };
    let filter = ShardFilter::build(genesis, history_revision, coverage, &receivers)?;
    Ok(BuiltShard {
        shard: shard.clone(),
        end,
        segments,
        filter,
    })
}

/// The recent tier's payment table, the blocks from `from` through `tip`; [`build`]
/// adds its companion. The table starts at
/// half occupancy and grows by powers of two, retrying salts from the tip's hash as a
/// single table always has; the sealer guarantees it places in `policy`'s pinned
/// geometry, whose salts are the last resort.
pub fn recent(
    store: &mut Store,
    from: u32,
    tip: &Checkpoint,
    min_rows: u32,
    policy: &Policy,
    history_revision: u64,
) -> Result<Snapshot, Error> {
    let records = store.records(from, tip.height)?;
    let mut rows = u32::try_from((records.len() as u64 / 7 + 1).next_power_of_two())
        .map_err(|_| Error::Capacity)?
        .max(min_rows);
    loop {
        if rows > MAX_ROWS {
            let parent = store.checkpoint(from - 1)?;
            let template = Manifest {
                profile: PROFILE.into(),
                genesis: store.config().genesis,
                history_revision,
                start_height: from,
                start_parent: parent.hash,
                start_position: parent.position,
                end_height: tip.height,
                end_hash: tip.hash,
                end_position: tip.position,
                rows: policy.rows,
                salt: [0; 32],
                records: 0,
                data_sha256: [0; 32],
            };
            let salts = (0..SALT_ATTEMPTS).map(|attempt| seal::salt(&parent.hash, 0, attempt));
            return receiver_store::table(template, records, salts);
        }
        match store.snapshot_from(from, rows, history_revision) {
            Ok(snapshot) => return Ok(snapshot),
            Err(Error::Capacity) => rows *= 2,
            Err(error) => return Err(error),
        }
    }
}

/// Builds `snapshot`'s companion at `tip` and checks both: every record, and every final
/// payload against its Ironwood shard's recorded root. A `stop` ends it between steps
/// and during the check, which takes seconds for a tip segment.
pub fn build(
    store: &mut Store,
    witnesses: &mut Witnesses,
    snapshot: Snapshot,
    tip: &Checkpoint,
    stop: &dyn Fn() -> bool,
) -> Result<Built, Error> {
    let leaves = leaves(&snapshot)?;
    halt(stop)?;
    let (table, frozen) = companion(store, witnesses, &snapshot.manifest, &leaves, tip)?;
    halt(stop)?;
    check(store, &leaves, &table, stop)?;
    Ok(Built {
        snapshot,
        witnesses: table,
        leaves,
        frozen,
    })
}

/// Every checked record of `snapshot` as `(position, cmx, ordinal)`, in ordinal order.
pub fn leaves(snapshot: &Snapshot) -> Result<Vec<(u64, Hash, u32)>, Error> {
    let mut leaves = verify::leaves(&snapshot.data)?;
    leaves.sort_unstable_by_key(|leaf| leaf.2);
    Ok(leaves)
}

/// Rebuilds a kept segment's companion at `tip` for its payment table `payments`, whose
/// checked records are `leaves` in ordinal order.
pub fn rewitness(
    store: &mut Store,
    witnesses: &mut Witnesses,
    payments: &Manifest,
    leaves: &[(u64, Hash, u32)],
    tip: &Checkpoint,
    stop: &dyn Fn() -> bool,
) -> Result<(witness::Table, Option<Checkpoint>), Error> {
    let (table, frozen) = companion(store, witnesses, payments, leaves, tip)?;
    halt(stop)?;
    check(store, leaves, &table, stop)?;
    Ok((table, frozen))
}

/// The companion for payment table `m` with records `leaves` in ordinal order: frozen at
/// the later of the table's end and the block completing its last payment's Ironwood
/// shard once that shard is complete at `tip`, else provisional at `tip`.
fn companion(
    store: &mut Store,
    witnesses: &mut Witnesses,
    m: &Manifest,
    leaves: &[(u64, Hash, u32)],
    tip: &Checkpoint,
) -> Result<(witness::Table, Option<Checkpoint>), Error> {
    let positions: Vec<u64> = leaves.iter().map(|leaf| leaf.0).collect();
    let payloads = witnesses.payloads(store, tip, &positions)?;
    let last = positions.iter().max().copied();
    let frozen = match last {
        Some(last) if !is_final(last, tip.position) => None,
        Some(last) => {
            let shard = u32::try_from(last / SHARD_LEAVES).map_err(|_| Error::Malformed)?;
            let completed = store.completed_shard(shard)?.ok_or(Error::Coverage)?;
            Some(store.checkpoint(completed.height.max(m.end_height))?)
        }
        None => Some(store.checkpoint(m.end_height)?),
    };
    let context = frozen.clone().unwrap_or_else(|| tip.clone());
    Ok((witnesses::table_at(m, &payloads, &context)?, frozen))
}

/// [`Error::Stopped`] once `stop` is true.
fn halt(stop: &dyn Fn() -> bool) -> Result<(), Error> {
    match stop() {
        true => Err(Error::Stopped),
        false => Ok(()),
    }
}

/// Checks `table` against `leaves` and every final payload against the store's recorded
/// root of its Ironwood shard.
fn check(
    store: &Store,
    leaves: &[(u64, Hash, u32)],
    table: &witness::Table,
    stop: &dyn Fn() -> bool,
) -> Result<(), Error> {
    for (shard, root) in verify::check_witnesses_until(leaves, table, stop)? {
        let shard = u32::try_from(shard).map_err(|_| Error::Malformed)?;
        if store.completed_shard(shard)?.map(|s| s.root) != Some(root) {
            return Err(Error::Coverage);
        }
    }
    Ok(())
}
