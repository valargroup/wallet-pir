//! Companion witness payloads, built incrementally per Ironwood shard.
//!
//! A payment's payload is its lower path inside its Ironwood shard of 2^16 leaves. A
//! completed shard's payloads are final on its chain: the store keeps the shard's root and
//! every payment's payload, and a rewind past the completing block removes them. Only the
//! filling shard changes between publications. Its paths stay in memory and extend with
//! new commitments; a restart or rewind rebuilds them from that shard's stored
//! commitments. No publication reads the full commitment history.
use crate::verify::directory_error;
use receiver_directory::{snapshot::Snapshot, witness, Hash};
use receiver_store::{Checkpoint, Error, Store, SHARD_LEAVES};
use zakura_receiver_crypto::{
    wire::{decode_payload, is_final, LOWER_LEVELS, PAYLOAD_BYTES},
    witness::{combine, ShardPaths},
};

/// The filling shard's paths and the stored block they extend through.
struct Filling {
    shard: u32,
    paths: ShardPaths,
    through: Checkpoint,
}

/// Reusable witness state across publications. Recreate on restart: everything it holds
/// is rebuilt from the store.
#[derive(Default)]
pub struct Witnesses {
    filling: Option<Filling>,
}

impl Witnesses {
    /// The payloads of `positions` in a tree of `tip.position` leaves, where `tip` is the
    /// store's tip. Records shards completed by `tip` with their final payloads first, then
    /// extends the filling shard. A payment that a correction added to a completed shard
    /// rebuilds that one shard from its stored commitments.
    pub fn payloads(
        &mut self,
        store: &mut Store,
        tip: &Checkpoint,
        positions: &[u64],
    ) -> Result<Vec<[u8; PAYLOAD_BYTES]>, Error> {
        if store.tip()? != *tip || positions.iter().any(|p| *p >= tip.position) {
            return Err(Error::Coverage);
        }
        let complete = shard_of(tip.position)?;
        for shard in store.completed_shards()?..complete {
            let paths = self.extend(store, shard, tip)?;
            let root = paths.root().ok_or(Error::Coverage)?;
            let payloads = shard_payloads(store, paths, shard)?;
            store.complete_shard(shard, root, &payloads)?;
            self.filling = None;
        }
        let mut found = store.payloads(positions)?;
        // One completed shard's paths at a time: each repair persists its payloads.
        let mut repairs = std::collections::BTreeSet::new();
        for (position, payload) in positions.iter().zip(&found) {
            let shard = shard_of(*position)?;
            if payload.is_none() && shard < complete {
                repairs.insert(shard);
            }
        }
        for shard in &repairs {
            repair(store, *shard)?;
        }
        if !repairs.is_empty() {
            found = store.payloads(positions)?;
        }
        for (position, payload) in positions.iter().zip(&mut found) {
            if payload.is_some() {
                continue;
            }
            let shard = shard_of(*position)?;
            if shard < complete {
                return Err(Error::Coverage);
            }
            let paths = self.extend(store, shard, tip)?;
            *payload = Some(paths.payload(*position).map_err(directory_error)?);
        }
        Ok(found.into_iter().map(Option::unwrap).collect())
    }

    /// The paths of Ironwood shard `shard` through `tip`, reusing the filling shard's
    /// paths while the block they were extended through is still stored, else rebuilding
    /// from that shard's commitments.
    fn extend(
        &mut self,
        store: &Store,
        shard: u32,
        tip: &Checkpoint,
    ) -> Result<&ShardPaths, Error> {
        let start = u64::from(shard) * SHARD_LEAVES;
        let end = tip.position.min(start + SHARD_LEAVES);
        let reusable = self.filling.as_ref().is_some_and(|f| {
            f.shard == shard
                && f.paths.tree_size() <= end
                && store.checkpoint(f.through.height).ok().as_ref() == Some(&f.through)
        });
        if !reusable {
            self.filling = Some(Filling {
                shard,
                paths: ShardPaths::new(shard),
                through: tip.clone(),
            });
        }
        let filling = self.filling.as_mut().expect("set above");
        let from = filling.paths.tree_size();
        if from < end {
            filling
                .paths
                .extend(&store.commitments(from, end)?)
                .map_err(directory_error)?;
        }
        filling.through = tip.clone();
        Ok(&filling.paths)
    }
}

/// Rebuilds completed shard `shard` from its stored commitments, checks the recorded
/// root and stores the payloads of every payment in it, for payments whose payloads a
/// correction removed. The rebuilt paths are dropped on return.
fn repair(store: &mut Store, shard: u32) -> Result<(), Error> {
    let start = u64::from(shard) * SHARD_LEAVES;
    let mut paths = ShardPaths::new(shard);
    paths
        .extend(&store.commitments(start, start + SHARD_LEAVES)?)
        .map_err(directory_error)?;
    let recorded = store.completed_shard(shard)?.ok_or(Error::Coverage)?;
    if paths.root() != Some(recorded.root) {
        return Err(Error::Coverage);
    }
    let payloads = shard_payloads(store, &paths, shard)?;
    store.complete_shard(shard, recorded.root, &payloads)
}

/// The Ironwood shard holding `position`.
fn shard_of(position: u64) -> Result<u32, Error> {
    u32::try_from(position / SHARD_LEAVES).map_err(|_| Error::Malformed)
}

/// The payload of every stored payment in `shard`, from its paths.
fn shard_payloads(
    store: &Store,
    paths: &ShardPaths,
    shard: u32,
) -> Result<Vec<(u64, [u8; PAYLOAD_BYTES])>, Error> {
    let start = u64::from(shard) * SHARD_LEAVES;
    store
        .payment_positions(start, start + SHARD_LEAVES)?
        .into_iter()
        .map(|position| Ok((position, paths.payload(position).map_err(directory_error)?)))
        .collect()
}

/// The companion table for `snapshot`, whose records carry ordinals ranked by position:
/// one payload per record in ordinal order, built at the snapshot's own end. Rows are the
/// fewest supported that hold them; more payloads than the largest table holds is
/// [`Error::Capacity`].
pub fn table(
    snapshot: &Snapshot,
    payloads: &[[u8; PAYLOAD_BYTES]],
) -> Result<witness::Table, Error> {
    let m = &snapshot.manifest;
    let rows = u32::try_from(payloads.len().div_ceil(witness::PAYLOADS))
        .map_err(|_| Error::Capacity)?
        .next_power_of_two()
        .max(receiver_pir::MIN_ROWS);
    if rows > receiver_pir::MAX_ROWS {
        return Err(Error::Capacity);
    }
    Ok(witness::Table::build(
        witness::Manifest {
            profile: witness::PROFILE.into(),
            genesis: m.genesis,
            history_revision: m.history_revision,
            payments: m.revision()?,
            payloads: 0,
            rows,
            tree_height: m.end_height,
            tree_hash: m.end_hash,
            tree_size: m.end_position,
            data_sha256: [0; 32],
        },
        payloads,
    )?)
}

/// Folds `position`'s final lower path from its commitment `cmx` to its Ironwood shard's
/// root. Returns `None` for a provisional payload, after checking its layout and that
/// every sibling it supplies is a canonical node.
pub fn shard_root(
    payload: &[u8],
    position: u64,
    cmx: &Hash,
    tree_size: u64,
) -> Result<Option<Hash>, Error> {
    let siblings = decode_payload(payload, position, tree_size).map_err(|_| Error::Malformed)?;
    if !is_final(position, tree_size) {
        for sibling in siblings.iter().flatten() {
            // Combining refuses a noncanonical node; the result is discarded.
            combine(0, sibling, sibling).map_err(directory_error)?;
        }
        return Ok(None);
    }
    let mut node = *cmx;
    for (level, sibling) in siblings.iter().enumerate() {
        let sibling = sibling.as_ref().ok_or(Error::Malformed)?;
        let level = u8::try_from(level).expect("16 levels");
        node = if (position >> level) & 1 == 0 {
            combine(level, &node, sibling)
        } else {
            combine(level, sibling, &node)
        }
        .map_err(directory_error)?;
    }
    debug_assert_eq!(siblings.len(), usize::from(LOWER_LEVELS));
    Ok(Some(node))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::common;
    use receiver_store::{Config, IndexedBlock};

    const L: u64 = SHARD_LEAVES;

    /// A canonical commitment for `position`, or its `variant` on a replacement chain.
    fn cmx(position: u64, variant: u8) -> Hash {
        let mut cmx = [0; 32];
        cmx[..8].copy_from_slice(&(position + 1).to_le_bytes());
        cmx[8] = variant;
        cmx
    }

    /// The block at `height`, hash `[height, variant, ..]`, holding positions
    /// `start..end` and payments at `paid`.
    fn block(
        height: u32,
        variant: u8,
        parent: Hash,
        start: u64,
        end: u64,
        paid: &[u64],
    ) -> IndexedBlock {
        let mut hash = [0; 32];
        (hash[0], hash[1]) = (height as u8, variant);
        let payments = paid
            .iter()
            .map(|&position| {
                let mut payment = common::record(0, 1).payment;
                (payment.height, payment.block_hash, payment.position) = (height, hash, position);
                payment.cmx = cmx(position, variant);
                payment.tx_index = position as u32 + 1;
                payment.txid[..8].copy_from_slice(&position.to_le_bytes());
                (common::receiver(), payment)
            })
            .collect();
        IndexedBlock {
            height,
            hash,
            parent,
            start_position: start,
            end_position: end,
            coinbase_actions: 0,
            payments,
            commitments: (start..end).map(|p| cmx(p, variant)).collect(),
        }
    }

    /// A store starting at height 100 after parent `[2; 32]`, at position zero.
    fn open(dir: &std::path::Path) -> Store {
        let config = Config {
            genesis: [1; 32],
            start_height: 100,
            start_parent: [2; 32],
            start_position: 0,
        };
        Store::open(dir.join("directory.sqlite"), config).unwrap()
    }

    /// The commitments [`block`] gives positions `start..end` on chain `variant`.
    fn leaves(start: u64, end: u64, variant: u8) -> Vec<Hash> {
        (start..end).map(|p| cmx(p, variant)).collect()
    }

    /// Oracle paths of Ironwood shard `shard` over `leaves`, built from the commitments
    /// the test chose, apart from the store and the cache under test. Extending them as
    /// the test appends keeps each shard to one full build per test, which a per-case
    /// time budget needs: a build hashes 2^16 leaves.
    fn reference(shard: u32, leaves: &[Hash]) -> ShardPaths {
        let mut paths = ShardPaths::new(shard);
        paths.extend(leaves).unwrap();
        paths
    }

    /// The oracle payloads of `positions`, each from the paths of its shard in `shards`.
    fn expected(shards: &[&ShardPaths], positions: &[u64]) -> Vec<[u8; PAYLOAD_BYTES]> {
        positions
            .iter()
            .map(|&p| shards[(p / L) as usize].payload(p).unwrap())
            .collect()
    }

    /// A provisional payload supplying a noncanonical node, which wallets would refuse,
    /// is refused before serving; a canonical one passes.
    #[test]
    fn provisional_payloads_must_supply_canonical_nodes() {
        use zakura_receiver_crypto::wire::encode_payload;
        let mut siblings = [None; 16];
        siblings[0] = Some([1; 32]);
        let payload = encode_payload(&siblings, 0, 2).unwrap();
        assert_eq!(shard_root(&payload, 0, &[2; 32], 2).unwrap(), None);
        siblings[0] = Some([0xff; 32]);
        let payload = encode_payload(&siblings, 0, 2).unwrap();
        assert!(matches!(
            shard_root(&payload, 0, &[2; 32], 2),
            Err(Error::Malformed)
        ));
    }

    /// A correction adding payments to two completed shards repairs each from its own
    /// commitments in turn under its recorded root: every payload, old and added, folds
    /// from its commitment to that root, which pins it to the one path at its position.
    #[test]
    fn a_correction_across_completed_shards_repairs_each_in_turn() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        let a = block(100, 0, [2; 32], 0, L, &[5]);
        let b = block(101, 0, a.hash, L, 2 * L + 2, &[L + 5]);
        store.append(&a).unwrap();
        store.append(&b).unwrap();
        let tip = store.tip().unwrap();
        let mut witnesses = Witnesses::default();
        witnesses.payloads(&mut store, &tip, &[5, L + 5]).unwrap();
        assert_eq!(store.completed_shards().unwrap(), 2);
        let roots =
            |store: &Store| [0, 1].map(|shard| store.completed_shard(shard).unwrap().unwrap().root);
        let recorded = roots(&store);
        let corrected = [
            block(100, 0, [2; 32], 0, L, &[5, 9]),
            block(101, 0, a.hash, L, 2 * L + 2, &[L + 5, L + 9]),
        ];
        store.correct(1, &corrected).unwrap();
        let positions = [5, 9, L + 5, L + 9];
        let built = witnesses.payloads(&mut store, &tip, &positions).unwrap();
        assert_eq!(roots(&store), recorded);
        for (&position, payload) in positions.iter().zip(&built) {
            let root = shard_root(payload, position, &cmx(position, 0), tip.position).unwrap();
            assert_eq!(root, Some(recorded[(position / L) as usize]), "{position}");
        }
        assert!(store
            .payloads(&positions)
            .unwrap()
            .iter()
            .all(Option::is_some));
    }

    /// Payloads built incrementally before and across a shard's completion equal the
    /// oracle's, the completed shard records the oracle's root and final payloads, and a
    /// restart rebuilds the filling shard from its own commitments.
    #[test]
    fn incremental_payloads_follow_completion_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        let a = block(100, 0, [2; 32], 0, L - 6, &[3, 70]);
        store.append(&a).unwrap();
        let mut witnesses = Witnesses::default();
        let mut shard0 = reference(0, &leaves(0, L - 6, 0));
        let tip = store.tip().unwrap();
        assert_eq!(
            witnesses.payloads(&mut store, &tip, &[3, 70]).unwrap(),
            expected(&[&shard0], &[3, 70])
        );
        assert_eq!(store.completed_shards().unwrap(), 0);
        let b = block(101, 0, a.hash, L - 6, L + 4, &[L - 1, L + 2]);
        store.append(&b).unwrap();
        shard0.extend(&leaves(L - 6, L, 0)).unwrap();
        let shard1 = reference(1, &leaves(L, L + 4, 0));
        let all = [3, 70, L - 1, L + 2];
        let tip = store.tip().unwrap();
        let built = witnesses.payloads(&mut store, &tip, &all).unwrap();
        assert_eq!(built, expected(&[&shard0, &shard1], &all));
        let shard = store.completed_shard(0).unwrap().unwrap();
        assert_eq!((shard.height, shard.hash), (101, b.hash));
        assert_eq!(Some(shard.root), shard0.root());
        assert_eq!(
            store.payloads(&all).unwrap(),
            [Some(built[0]), Some(built[1]), Some(built[2]), None]
        );
        // A restart rebuilds the filling shard from its own commitments.
        let restarted = Witnesses::default().payloads(&mut store, &tip, &all);
        assert_eq!(restarted.unwrap(), built);
    }

    /// A rewind past the completing block drops the completed shard, and the cached paths
    /// must not survive it: the filling shard is rebuilt, then extends along a replacement
    /// chain to the oracle's payloads for that chain.
    #[test]
    fn a_rewind_past_completion_drops_the_cached_shard() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        let a = block(100, 0, [2; 32], 0, L - 6, &[3, 70]);
        store.append(&a).unwrap();
        store
            .append(&block(101, 0, a.hash, L - 6, L + 4, &[L - 1, L + 2]))
            .unwrap();
        let mut witnesses = Witnesses::default();
        let all = [3, 70, L - 1, L + 2];
        let tip = store.tip().unwrap();
        let built = witnesses.payloads(&mut store, &tip, &all).unwrap();
        assert_eq!(store.completed_shards().unwrap(), 1);
        store.rewind(100, a.hash).unwrap();
        assert_eq!(store.completed_shards().unwrap(), 0);
        let mut shard0 = reference(0, &leaves(0, L - 6, 0));
        let tip = store.tip().unwrap();
        assert_eq!(
            witnesses.payloads(&mut store, &tip, &[3, 70]).unwrap(),
            expected(&[&shard0], &[3, 70])
        );
        let replacement = block(101, 1, a.hash, L - 6, L + 4, &[L - 1, L + 2]);
        store.append(&replacement).unwrap();
        shard0.extend(&leaves(L - 6, L, 1)).unwrap();
        let shard1 = reference(1, &leaves(L, L + 4, 1));
        let tip = store.tip().unwrap();
        let replaced = witnesses.payloads(&mut store, &tip, &all).unwrap();
        assert_eq!(replaced, expected(&[&shard0, &shard1], &all));
        assert_ne!(replaced[0], built[0]);
        let shard = store.completed_shard(0).unwrap().unwrap();
        assert_eq!(
            (shard.hash, Some(shard.root)),
            (replacement.hash, shard0.root())
        );
    }

    /// A correction that adds a payment to a completed shard rebuilds only that shard,
    /// under its recorded root, to the oracle's payloads. A publication's final payloads
    /// then fold to that root, and swapping two of them between ordinals is refused
    /// before serving.
    #[test]
    fn a_correction_rebuilds_a_completed_shard_under_its_root() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        let a = block(100, 0, [2; 32], 0, L - 6, &[3, 70]);
        store.append(&a).unwrap();
        store
            .append(&block(101, 0, a.hash, L - 6, L + 4, &[L - 1, L + 2]))
            .unwrap();
        let mut witnesses = Witnesses::default();
        let tip = store.tip().unwrap();
        witnesses
            .payloads(&mut store, &tip, &[3, 70, L - 1, L + 2])
            .unwrap();
        let root = store.completed_shard(0).unwrap().unwrap().root;
        // A correction adds a payment at position 10 of the completed shard.
        let corrected = block(100, 0, [2; 32], 0, L - 6, &[3, 10, 70]);
        store.correct(1, &[corrected]).unwrap();
        assert_eq!(store.payloads(&[10]).unwrap(), [None]);
        let fixed = [3, 10, 70, L - 1, L + 2];
        let shard0 = reference(0, &leaves(0, L, 0));
        let shard1 = reference(1, &leaves(L, L + 4, 0));
        let repaired = Witnesses::default().payloads(&mut store, &tip, &fixed);
        assert_eq!(repaired.unwrap(), expected(&[&shard0, &shard1], &fixed));
        assert_eq!(store.completed_shard(0).unwrap().unwrap().root, root);
        assert_eq!(shard0.root(), Some(root));
        assert!(store.payloads(&[10]).unwrap()[0].is_some());

        let snapshot = store.snapshot(8, 1).unwrap();
        let mut payloads = witnesses.payloads(&mut store, &tip, &fixed).unwrap();
        let table = super::table(&snapshot, &payloads).unwrap();
        let roots = crate::verify::check_publication(&snapshot, &table).unwrap();
        assert_eq!(roots.get(&0), Some(&root));
        payloads.swap(0, 1);
        let swapped = super::table(&snapshot, &payloads).unwrap();
        assert!(matches!(
            crate::verify::check_publication(&snapshot, &swapped),
            Err(Error::Coverage)
        ));
    }
}
