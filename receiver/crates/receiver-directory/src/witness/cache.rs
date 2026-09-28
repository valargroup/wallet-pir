use super::{validate_inputs, WitnessSnapshot};
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
    /// Publication fencing and independent chain-root validation remain the caller's job.
    pub fn build(
        &mut self,
        manifest: &Manifest,
        commitments: &[Hash],
        positions: &BTreeSet<u32>,
    ) -> Result<WitnessSnapshot, Error> {
        validate_inputs(manifest, commitments, positions)?;
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
        let mut nodes = BTreeMap::new();
        for depth in 0..32u8 {
            for position in positions {
                let index = (*position >> depth) ^ 1;
                if let Some(hash) = self.levels[depth as usize].get(index as usize) {
                    nodes.insert((depth, index), *hash);
                }
            }
        }
        WitnessSnapshot::from_nodes(manifest, self.levels[32][0].to_bytes(), nodes)
    }
}
