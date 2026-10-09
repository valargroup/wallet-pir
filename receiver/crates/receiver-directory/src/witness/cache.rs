use super::{WitnessSnapshot, HEADER, MAX_WITNESS_BYTES, MAX_WITNESS_COMMITMENTS, NODE};
use crate::{snapshot::Manifest, Error, Hash};
use incrementalmerkletree::Hashable;
use orchard::{note::ExtractedNoteCommitment, tree::MerkleHashOrchard};
use std::collections::{BTreeMap, BTreeSet};

/// Disposable tree nodes for successive publications. No cached chain anchor is trusted.
/// Nodes use about 64 bytes per leaf, plus spare vector capacity. A restart rebuilds them.
#[derive(Default)]
pub struct WitnessCache {
    levels: Vec<Vec<MerkleHashOrchard>>,
}

impl WitnessCache {
    /// Compare the complete canonical commitment prefix before reusing any tree nodes.
    /// Appends, shorter histories and replacement forks all recompute the affected suffix.
    /// `positions` must hold one position per manifest record, though a matching count
    /// cannot prove they are the right ones. Publication fencing and independent
    /// chain-root validation remain the caller's job. More than
    /// [`MAX_WITNESS_COMMITMENTS`] commitments is [`Error::Capacity`], and leaves the
    /// cache as it was.
    pub fn build(
        &mut self,
        manifest: &Manifest,
        commitments: &[Hash],
        positions: &BTreeSet<u32>,
    ) -> Result<WitnessSnapshot, Error> {
        self.build_within(
            manifest,
            commitments,
            positions,
            MAX_WITNESS_BYTES,
            MAX_WITNESS_COMMITMENTS,
        )
    }

    /// [`Self::build`] with test-sized limits: [`Error::Capacity`] for more than
    /// `max_commitments` commitments before any tree allocation, or as soon as the
    /// file's nodes would exceed `max_bytes`, before collecting the rest.
    fn build_within(
        &mut self,
        manifest: &Manifest,
        commitments: &[Hash],
        positions: &BTreeSet<u32>,
        max_bytes: usize,
        max_commitments: u64,
    ) -> Result<WitnessSnapshot, Error> {
        manifest.validate()?;
        if manifest.start_position != 0
            || manifest.end_position != commitments.len() as u64
            || positions.len() as u64 != manifest.records
            || positions
                .iter()
                .any(|p| u64::from(*p) >= manifest.end_position)
        {
            return Err(Error::Coverage);
        }
        if commitments.len() as u64 > max_commitments {
            return Err(Error::Capacity);
        }
        // An empty tree, so no records or positions either, has no levels to combine.
        if commitments.is_empty() {
            self.levels.clear();
            return WitnessSnapshot::from_nodes(
                manifest,
                MerkleHashOrchard::empty_root(32.into()).to_bytes(),
                BTreeMap::new(),
            );
        }
        self.levels.resize_with(33, Vec::new);
        let shared = self.levels[0]
            .iter()
            .zip(commitments)
            .take_while(|(old, new)| old.to_bytes() == **new)
            .count();
        // Parse before mutating the cache so a malformed suffix cannot leave partial levels.
        let suffix = commitments[shared..]
            .iter()
            .map(|cmx| {
                Option::<ExtractedNoteCommitment>::from(ExtractedNoteCommitment::from_bytes(cmx))
                    .map(|c| MerkleHashOrchard::from_cmx(&c))
                    .ok_or(Error::Malformed)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let changed = shared != self.levels[0].len() || shared != commitments.len();
        self.levels[0].truncate(shared);
        self.levels[0].extend(suffix);
        if changed {
            let mut first_changed = shared;
            for depth in 0..32 {
                let (lower, upper) = self.levels.split_at_mut(depth + 1);
                let children = &lower[depth];
                let parents = &mut upper[0];
                // Recompute the last surviving pair after a rewind too: its right
                // child may now be the empty subtree instead of an orphaned node.
                let first_parent = (first_changed / 2).min((children.len() - 1) / 2);
                parents.truncate(first_parent);
                let empty = MerkleHashOrchard::empty_root((depth as u8).into());
                parents.extend(children[first_parent * 2..].chunks(2).map(|pair| {
                    MerkleHashOrchard::combine(
                        (depth as u8).into(),
                        &pair[0],
                        pair.get(1).unwrap_or(&empty),
                    )
                }));
                first_changed = first_parent;
            }
        }
        let max_nodes = (max_bytes - HEADER) / NODE;
        let mut nodes = BTreeMap::new();
        for depth in 0..32u8 {
            for position in positions {
                let index = (*position >> depth) ^ 1;
                if let Some(hash) = self.levels[depth as usize].get(index as usize) {
                    if nodes.insert((depth, index), *hash).is_none() && nodes.len() > max_nodes {
                        return Err(Error::Capacity);
                    }
                }
            }
        }
        WitnessSnapshot::from_nodes(manifest, self.levels[32][0].to_bytes(), nodes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{FilterSet, MIN_ROWS, PROFILE};

    /// `len` distinct valid commitments.
    fn leaves(len: u64) -> Vec<Hash> {
        (1..=len)
            .map(|i| {
                let mut cmx = [0; 32];
                cmx[..8].copy_from_slice(&i.to_le_bytes());
                cmx
            })
            .collect()
    }

    /// A manifest ending at `len` commitments with `records` records.
    fn manifest(len: u64, records: u64) -> Manifest {
        Manifest {
            profile: PROFILE.into(),
            genesis: [1; 32],
            start_height: 100,
            start_parent: [2; 32],
            start_position: 0,
            end_height: 110,
            end_hash: [3; 32],
            end_position: len,
            rows: MIN_ROWS,
            salt: [3; 32],
            records,
            data_sha256: [0; 32],
            filters: vec![FilterSet {
                label: crate::filter::PAID.into(),
                count: 0,
                window_secs: None,
                since_unix: None,
                until_unix: None,
            }],
            filters_sha256: [0; 32],
        }
    }

    /// Sparse positions need a sibling at nearly every level, so a small cap is
    /// exceeded while the nodes are still being collected.
    #[test]
    fn the_size_cap_stops_collecting_nodes() {
        let leaves = leaves(64);
        let manifest = manifest(64, 2);
        let positions = [0, 63].into_iter().collect();
        let mut cache = WitnessCache::default();
        assert!(matches!(
            cache.build_within(&manifest, &leaves, &positions, HEADER + NODE * 4, 64),
            Err(Error::Capacity)
        ));
        let built = cache.build(&manifest, &leaves, &positions).unwrap();
        assert!(built.nodes.len() > 4);
    }

    /// A valid history with few payments but more commitments than the limit is
    /// refused, the limit itself passes, and the refusal leaves the cache usable.
    #[test]
    fn the_commitment_limit_refuses_long_sparse_histories() {
        let all = leaves(65);
        let positions = [0, 64].into_iter().collect();
        let long = manifest(65, 2);
        let mut cache = WitnessCache::default();
        cache
            .build(&manifest(64, 1), &all[..64], &[3].into_iter().collect())
            .unwrap();
        let before = cache.levels.clone();
        assert!(matches!(
            cache.build_within(&long, &all, &positions, MAX_WITNESS_BYTES, 64),
            Err(Error::Capacity)
        ));
        assert!(cache.levels == before);
        let at_limit = cache
            .build_within(&long, &all, &positions, MAX_WITNESS_BYTES, 65)
            .unwrap();
        assert_eq!(
            at_limit.encode(),
            WitnessSnapshot::build(&long, &all, &positions)
                .unwrap()
                .encode()
        );
    }
}
