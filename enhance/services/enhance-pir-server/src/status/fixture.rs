//! Deterministic synthetic source for isolated backend qualification. Not chain ingestion.
use super::index::{Block, Snapshot};
use enhance_pir::status::*;
use sha2::{Digest, Sha256};
pub const NETWORK: Hash = [0x31; 32];
pub const SALT: Hash = [0x72; 32];
pub const START: u32 = 1000;
pub const BLOCKS: usize = 64;
pub fn txid(i: u64) -> Hash {
    let mut h = Sha256::new();
    h.update(b"status-pir/synthetic-fixture/txid\0");
    h.update(i.to_le_bytes());
    h.finalize().into()
}
pub fn block_hash(height: u32) -> Hash {
    Sha256::digest(height.to_le_bytes()).into()
}
pub fn source(entries: usize, advanced: bool) -> (Vec<Block>, Vec<Hash>, Vec<Record>) {
    let mined = entries.saturating_sub(2);
    let mut blocks = (0..BLOCKS)
        .map(|i| Block {
            height: START + i as u32,
            hash: block_hash(START + i as u32),
            parent: block_hash(START + i as u32 - 1),
            txids: Vec::new(),
        })
        .collect::<Vec<_>>();
    for i in 0..mined {
        blocks[(i * BLOCKS / mined).min(BLOCKS - 1)]
            .txids
            .push(txid(i as u64));
    }
    let pending = txid(u64::MAX - 1);
    let forks = vec![Record {
        txid: txid(u64::MAX - 2),
        tag: 3,
        height: START + BLOCKS as u32 - 1,
        block: [0x54; 32],
    }];
    if advanced {
        blocks.push(Block {
            height: START + BLOCKS as u32,
            hash: block_hash(START + BLOCKS as u32),
            parent: block_hash(START + BLOCKS as u32 - 1),
            txids: vec![pending],
        });
        (blocks, Vec::new(), forks)
    } else {
        (blocks, vec![pending], forks)
    }
}
pub fn snapshot(entries: usize, advanced: bool) -> Result<Snapshot, Error> {
    let (blocks, mempool, forks) = source(entries, advanced);
    Snapshot::build(NETWORK, SALT, &blocks, &mempool, &forks)
}
pub fn cases(entries: usize, advanced: bool) -> Vec<(Hash, Observation)> {
    vec![
        (
            txid((entries - 3) as u64),
            Observation::Mined(START + BLOCKS as u32 - 1),
        ),
        (
            txid(u64::MAX - 1),
            if advanced {
                Observation::Mined(START + BLOCKS as u32)
            } else {
                Observation::Mempool
            },
        ),
        (txid(u64::MAX - 2), Observation::Forked),
        (txid(u64::MAX), Observation::NotFound),
    ]
}

/// Sample different mined txids throughout the retained window, plus all non-mined states.
pub fn case_at(
    entries: usize,
    advanced: bool,
    coverage_start: u32,
    ordinal: usize,
) -> (Hash, Observation) {
    if ordinal % 8 < 3 {
        return cases(entries, advanced)[1 + ordinal % 8];
    }
    let mined = entries - 2;
    let first = ((coverage_start - START) as usize * mined).div_ceil(BLOCKS);
    let offset = (ordinal as u64).wrapping_mul(6364136223846793005) as usize % (mined - first);
    let i = first + offset;
    (
        txid(i as u64),
        Observation::Mined(START + (i * BLOCKS / mined) as u32),
    )
}
