//! Common proof data for the POC. Every wallet downloads the same bytes.
//! Proofs authenticate inclusion only against a root independently accepted by the wallet.
use crate::{snapshot::Manifest, Error, Hash};
use incrementalmerkletree::Hashable;
use orchard::{
    note::ExtractedNoteCommitment,
    tree::{MerkleHashOrchard, MerklePath},
};
use std::collections::{BTreeMap, BTreeSet};

mod cache;
pub use cache::WitnessCache;

const HEADER: usize = 152;
const NODE: usize = 37;
/// Bound allocations before parsing an untrusted common snapshot.
pub const MAX_WITNESS_BYTES: usize = 64 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"IWPROOF1";

pub struct WitnessSnapshot {
    pub genesis: Hash,
    pub directory_revision: Hash,
    pub height: u32,
    pub block_hash: Hash,
    pub tree_size: u64,
    pub root: Hash,
    nodes: BTreeMap<(u8, u32), MerkleHashOrchard>,
}
impl WitnessSnapshot {
    /// Build from every commitment since the pool's empty tree, including coinbase.
    pub fn build(
        manifest: &Manifest,
        commitments: &[Hash],
        positions: &BTreeSet<u32>,
    ) -> Result<Self, Error> {
        validate_inputs(manifest, commitments, positions)?;
        let mut level = commitments
            .iter()
            .map(|cmx| {
                Option::<ExtractedNoteCommitment>::from(ExtractedNoteCommitment::from_bytes(cmx))
                    .map(|c| MerkleHashOrchard::from_cmx(&c))
                    .ok_or(Error::Malformed)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut nodes = BTreeMap::new();
        for depth in 0..32u8 {
            for position in positions {
                let index = (*position >> depth) ^ 1;
                if let Some(hash) = level.get(index as usize) {
                    nodes.insert((depth, index), *hash);
                }
            }
            let empty = MerkleHashOrchard::empty_root(depth.into());
            level = level
                .chunks(2)
                .map(|pair| {
                    MerkleHashOrchard::combine(
                        depth.into(),
                        &pair[0],
                        pair.get(1).unwrap_or(&empty),
                    )
                })
                .collect();
        }
        if level.len() != 1 {
            return Err(Error::Capacity);
        }
        Self::from_nodes(manifest, level[0].to_bytes(), nodes)
    }

    fn from_nodes(
        manifest: &Manifest,
        root: Hash,
        nodes: BTreeMap<(u8, u32), MerkleHashOrchard>,
    ) -> Result<Self, Error> {
        let snapshot = Self {
            genesis: manifest.genesis,
            directory_revision: manifest.revision()?,
            height: manifest.end_height,
            block_hash: manifest.end_hash,
            tree_size: manifest.end_position,
            root,
            nodes,
        };
        if HEADER + NODE * snapshot.nodes.len() > MAX_WITNESS_BYTES {
            return Err(Error::Capacity);
        }
        Ok(snapshot)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER + NODE * self.nodes.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.genesis);
        out.extend_from_slice(&self.directory_revision);
        out.extend_from_slice(&self.height.to_le_bytes());
        out.extend_from_slice(&self.block_hash);
        out.extend_from_slice(&self.tree_size.to_le_bytes());
        out.extend_from_slice(&self.root);
        out.extend_from_slice(&(self.nodes.len() as u32).to_le_bytes());
        for ((level, index), hash) in &self.nodes {
            out.push(*level);
            out.extend_from_slice(&index.to_le_bytes());
            out.extend_from_slice(&hash.to_bytes());
        }
        out
    }

    /// Binds the proof file to one directory publication. Its root is still untrusted.
    pub fn decode(bytes: &[u8], manifest: &Manifest) -> Result<Self, Error> {
        manifest.validate()?;
        if bytes.len() < HEADER || bytes.len() > MAX_WITNESS_BYTES || &bytes[..8] != MAGIC {
            return Err(Error::Malformed);
        }
        let count = u32::from_le_bytes(bytes[148..152].try_into().unwrap()) as usize;
        if count != (bytes.len() - HEADER) / NODE || !(bytes.len() - HEADER).is_multiple_of(NODE) {
            return Err(Error::Malformed);
        }
        let mut result = Self {
            genesis: bytes[8..40].try_into().unwrap(),
            directory_revision: bytes[40..72].try_into().unwrap(),
            height: u32::from_le_bytes(bytes[72..76].try_into().unwrap()),
            block_hash: bytes[76..108].try_into().unwrap(),
            tree_size: u64::from_le_bytes(bytes[108..116].try_into().unwrap()),
            root: bytes[116..148].try_into().unwrap(),
            nodes: BTreeMap::new(),
        };
        if result.genesis != manifest.genesis
            || result.directory_revision != manifest.revision()?
            || result.height != manifest.end_height
            || result.block_hash != manifest.end_hash
            || result.tree_size != manifest.end_position
            || result.tree_size > 1u64 << 32
        {
            return Err(Error::Coverage);
        }
        let mut previous = None;
        for node in bytes[HEADER..].as_chunks::<NODE>().0 {
            let key = (node[0], u32::from_le_bytes(node[1..5].try_into().unwrap()));
            if key.0 >= 32
                || (u64::from(key.1) << key.0) >= result.tree_size
                || previous.is_some_and(|p| p >= key)
            {
                return Err(Error::Malformed);
            }
            let hash = Option::<MerkleHashOrchard>::from(MerkleHashOrchard::from_bytes(
                node[5..37].try_into().unwrap(),
            ))
            .ok_or(Error::Malformed)?;
            result.nodes.insert(key, hash);
            previous = Some(key);
        }
        Ok(result)
    }

    /// Returns a path bound to the requested commitment. The caller must additionally
    /// verify it against local chain state, never just against this file's root.
    pub fn path(&self, position: u32, cmx: Hash) -> Result<[[u8; 32]; 32], Error> {
        if u64::from(position) >= self.tree_size {
            return Err(Error::Coverage);
        }
        let mut path = [MerkleHashOrchard::empty_leaf(); 32];
        for depth in 0..32u8 {
            let index = (position >> depth) ^ 1;
            path[depth as usize] = if (u64::from(index) << depth) >= self.tree_size {
                MerkleHashOrchard::empty_root(depth.into())
            } else {
                *self.nodes.get(&(depth, index)).ok_or(Error::Coverage)?
            };
        }
        let commitment =
            Option::<ExtractedNoteCommitment>::from(ExtractedNoteCommitment::from_bytes(&cmx))
                .ok_or(Error::Malformed)?;
        if MerklePath::from_parts(position, path)
            .root(commitment)
            .to_bytes()
            != self.root
        {
            return Err(Error::Malformed);
        }
        Ok(path.map(|h| h.to_bytes()))
    }
}

fn validate_inputs(
    manifest: &Manifest,
    commitments: &[Hash],
    positions: &BTreeSet<u32>,
) -> Result<(), Error> {
    manifest.validate()?;
    if manifest.start_position != 0
        || manifest.end_position != commitments.len() as u64
        || commitments.is_empty()
        || positions
            .iter()
            .any(|p| u64::from(*p) >= manifest.end_position)
    {
        return Err(Error::Coverage);
    }
    Ok(())
}
