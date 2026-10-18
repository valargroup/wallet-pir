//! Mock-node chains with no wallet events for chain-follow and recovery tests.

use transparent_filter::BlockHash;

/// A syntactically valid header and a coinbase with no outputs. Consensus
/// acceptance is the mock node's job: roots and proof-of-work are synthetic.
/// The parser rejects blocks without transactions, and the height-derived
/// coinbase gives each block on a branch a distinct transaction ID.
pub(crate) fn block_without_activity(parent: [u8; 32], height: u32, nonce: u8) -> Vec<u8> {
    let mut raw = 4u32.to_le_bytes().to_vec();
    raw.extend(parent);
    raw.extend([0; 64]);
    raw.extend(1_700_000_000u32.to_le_bytes());
    raw.extend(0x1f07ffffu32.to_le_bytes());
    raw.extend([nonce; 32]);
    raw.extend([0xfd, 0x40, 0x05]);
    raw.extend([0; 1344]);
    raw.push(1);
    // A height-zero coinbase must carry the real genesis script, and nothing
    // here reads coinbase heights, so genesis encodes height one.
    raw.extend(zakura_server_chain::testing::coinbase(height.max(1), &[]).bytes());
    raw
}

pub(crate) fn hash(raw: &[u8]) -> BlockHash {
    let block = zakura_server_chain::transparent::FetchedBlock::from_bytes(raw).unwrap();
    BlockHash::from_internal_bytes(block.hash())
}

/// Append `count` blocks; `tag` distinguishes the headers of competing branches.
pub(crate) fn extend(chain: &mut Vec<Vec<u8>>, count: usize, tag: u8) {
    for i in 0..count {
        let parent = chain
            .last()
            .map(|b| *hash(b).internal_bytes())
            .unwrap_or([0; 32]);
        chain.push(block_without_activity(
            parent,
            chain.len() as u32,
            tag + i as u8,
        ));
    }
}
