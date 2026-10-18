//! Mock-node blocks with no wallet events for chain-follow and recovery tests.

pub(crate) fn block_without_activity(parent: [u8; 32], nonce: u8) -> Vec<u8> {
    // The parser requires a transaction, even when no wallet has activity.
    // An unspendable coinbase output needs no previous-output lookup and is
    // excluded from wallet events. Consensus acceptance remains the mock
    // node's responsibility: roots, proof-of-work and height are synthetic.
    // Distinct blocks also need distinct transaction IDs: display ingestion
    // must not mistake the fixture's coinbases for repeated transactions.
    let coinbase = zakura_server_chain::testing::coinbase(u32::from(nonce) + 1, &[(0, &[0x6a])]);
    let mut raw = 4u32.to_le_bytes().to_vec();
    raw.extend(parent);
    raw.extend([0; 64]);
    raw.extend(1_700_000_000u32.to_le_bytes());
    raw.extend(0x1f07ffffu32.to_le_bytes());
    raw.extend([nonce; 32]);
    raw.extend([0xfd, 0x40, 0x05]);
    raw.extend([0; 1344]);
    raw.push(1);
    raw.extend(coinbase.bytes());
    raw
}
