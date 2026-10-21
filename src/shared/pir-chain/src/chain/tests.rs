//! Differential coverage against the pinned node decoder, which is test-only.
use super::*;
use zakura_chain::{
    block::Block as NodeBlock,
    serialization::{ZcashDeserialize, ZcashSerialize},
};

#[test]
fn historical_blocks_match_the_node_decoder() {
    let mainnet = &*zakura_test::vectors::MAINNET_BLOCKS;
    let testnet = &*zakura_test::vectors::TESTNET_BLOCKS;
    assert!(!mainnet.is_empty() && !testnet.is_empty());
    for (height, bytes) in mainnet.iter().chain(testnet.iter()) {
        let node = NodeBlock::zcash_deserialize(&mut &bytes[..]).unwrap();
        let block = Block::zcash_deserialize(&mut &bytes[..]).unwrap();
        assert_eq!(block.hash().0, node.hash().0, "block at height {height}");
        assert_eq!(
            block.header.previous_block_hash.0,
            node.header.previous_block_hash.0
        );
        assert_eq!(block.header.time, node.header.time.timestamp());
        assert_eq!(
            block.coinbase_height().map(|h| h.0),
            node.coinbase_height().map(|h| h.0)
        );
        assert_eq!(block.merkle_root(), node.header.merkle_root.0);
        assert_eq!(block.transactions.len(), node.transactions.len());
        for (decoded, reference) in block.transactions.iter().zip(&node.transactions) {
            assert_eq!(decoded.hash().0, reference.hash().0);
            assert_eq!(decoded.is_coinbase(), reference.is_coinbase());
            assert_eq!(decoded.has_shielded_data(), reference.has_shielded_data());
            assert_eq!(
                decoded.expiry_height().map(|h| h.0).unwrap_or(0),
                reference.expiry_height().map(|h| h.0).unwrap_or(0)
            );
            assert_eq!(decoded.outputs().len(), reference.outputs().len());
            for (output, expected) in decoded.outputs().iter().zip(reference.outputs()) {
                assert_eq!(u64::from(output.value), u64::from(expected.value));
                assert_eq!(
                    output.lock_script.as_raw_bytes(),
                    expected.lock_script.as_raw_bytes()
                );
                assert_eq!(
                    output.zcash_serialize_to_vec().unwrap(),
                    expected.zcash_serialize_to_vec().unwrap()
                );
            }
            assert_eq!(decoded.inputs().len(), reference.inputs().len());
            for (input, expected) in decoded.inputs().iter().zip(reference.inputs()) {
                assert_eq!(
                    input.outpoint().map(|o| (o.hash.0, o.index)),
                    expected.outpoint().map(|o| (o.hash.0, o.index))
                );
            }
            let sapling: Vec<[u8; 32]> = reference.sapling_nullifiers().map(|n| n.0.0).collect();
            assert_eq!(decoded.sapling_nullifiers(), sapling.as_slice());
            let cmus: Vec<_> = reference
                .sapling_note_commitments()
                .map(|c| c.to_bytes())
                .collect();
            let decoded_cmus: Vec<_> = decoded.sapling_outputs().iter().map(|o| o.cmu).collect();
            assert_eq!(decoded_cmus, cmus);
            let orchard: Vec<_> = reference
                .orchard_nullifiers()
                .map(|n| <[u8; 32]>::from(*n))
                .collect();
            let decoded_orchard: Vec<_> = decoded.orchard_actions().map(|a| a.nullifier).collect();
            assert_eq!(decoded_orchard, orchard);
        }
    }
}

#[test]
fn ironwood_transactions_match_the_node_decoder() {
    for hex in [
        include_str!("../../tests/fixtures/ironwood-fee-expiry.hex"),
        include_str!("../../tests/fixtures/receiver-refund.hex"),
    ] {
        let raw = hex::decode(hex.trim()).unwrap();
        let node =
            zakura_chain::transaction::Transaction::zcash_deserialize(&mut raw.as_slice()).unwrap();
        let tx = Transaction::zcash_deserialize(&mut raw.as_slice()).unwrap();
        assert_eq!(tx.hash().0, node.hash().0);
        assert!(tx.ironwood_actions().next().is_some());
        for (action, reference) in tx.ironwood_actions().zip(node.ironwood_actions()) {
            assert_eq!(action.cv, <[u8; 32]>::from(reference.cv));
            assert_eq!(action.nullifier, <[u8; 32]>::from(reference.nullifier));
            assert_eq!(action.cm_x, <[u8; 32]>::from(reference.cm_x));
            assert_eq!(
                action.ephemeral_key,
                <[u8; 32]>::from(reference.ephemeral_key)
            );
            assert_eq!(
                action.enc_ciphertext,
                <[u8; 580]>::from(reference.enc_ciphertext)
            );
            assert_eq!(
                action.out_ciphertext,
                <[u8; 80]>::from(reference.out_ciphertext)
            );
        }
    }
}

#[cfg(feature = "transparent")]
#[test]
fn exact_block_reads_reject_truncation_and_trailing_bytes() {
    use crate::transparent::FetchedBlock;
    let bytes = &*zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES;
    assert!(FetchedBlock::from_exact_bytes(bytes).is_ok());
    assert!(FetchedBlock::from_exact_bytes(&bytes[..bytes.len() - 1]).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(FetchedBlock::from_exact_bytes(&trailing).is_err());
    let mut header = &bytes[..];
    Header::read(&mut header).unwrap();
    let header_len = bytes.len() - header.len();
    let mut huge_count = bytes[..header_len].to_vec();
    CompactSize::write(&mut huge_count, 1_000_000).unwrap();
    assert!(FetchedBlock::from_exact_bytes(&huge_count).is_err());
    let mut bad_header = bytes.clone();
    bad_header[..4].copy_from_slice(&3u32.to_le_bytes());
    assert!(FetchedBlock::from_exact_bytes(&bad_header).is_err());
}

#[test]
fn supported_network_constants_match_the_node() {
    for network in [crate::Network::Mainnet, crate::Network::Testnet] {
        let node = match network {
            crate::Network::Mainnet => zakura_chain::parameters::Network::Mainnet,
            crate::Network::Testnet => zakura_chain::parameters::Network::new_default_testnet(),
        };
        assert_eq!(network.genesis_hash(), node.genesis_hash().0);
        assert_eq!(
            network.ironwood_activation(),
            zakura_chain::parameters::NetworkUpgrade::Nu6_3
                .activation_height(&node)
                .unwrap()
                .0
        );
    }
}

#[test]
fn transaction_reads_advance_past_one_transaction_and_reject_truncation() {
    let raw =
        hex::decode(include_str!("../../tests/fixtures/ironwood-fee-expiry.hex").trim()).unwrap();
    let mut two = raw.clone();
    two.extend_from_slice(&raw);
    let mut input = two.as_slice();
    let first = Transaction::zcash_deserialize(&mut input).unwrap();
    assert_eq!(input, raw.as_slice());
    let second = Transaction::zcash_deserialize(&mut input).unwrap();
    assert!(input.is_empty());
    assert_eq!(first.hash(), second.hash());
    for len in [0, 1, 4, raw.len() / 2, raw.len() - 1] {
        assert!(
            Transaction::zcash_deserialize(&mut &raw[..len]).is_err(),
            "length {len}"
        );
    }
}

#[test]
fn block_reads_advance_and_check_header_framing() {
    let bytes = &*zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES;
    let mut input = &bytes[..];
    let header_len = {
        let mut header = &bytes[..];
        Header::read(&mut header).unwrap();
        bytes.len() - header.len()
    };
    let mut trailing = bytes.clone();
    trailing.extend_from_slice(b"next");
    let mut rest = trailing.as_slice();
    Block::zcash_deserialize(&mut rest).unwrap();
    assert_eq!(rest, b"next");
    Block::zcash_deserialize(&mut input).unwrap();
    assert!(input.is_empty());
    // The solution length prefix is the last CompactSize before the solution.
    let mut short_solution = bytes[..HEADER_FIXED_BYTES].to_vec();
    CompactSize::write(&mut short_solution, 35).unwrap();
    short_solution.extend_from_slice(&[0; 35]);
    short_solution.extend_from_slice(&bytes[header_len..]);
    assert!(Block::zcash_deserialize(&mut short_solution.as_slice()).is_err());
    let mut negative = bytes.clone();
    negative[..4].copy_from_slice(&(-4i32).to_le_bytes());
    assert!(Block::zcash_deserialize(&mut negative.as_slice()).is_err());
    let mut zero = bytes[..header_len].to_vec();
    zero.push(0);
    assert!(Block::zcash_deserialize(&mut zero.as_slice()).is_err());
}

#[test]
fn coinbase_and_null_outpoints_follow_the_node() {
    // Mainnet block 1's coinbase uses a small-integer BIP34 height.
    let block =
        Block::zcash_deserialize(&mut &zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap();
    let coinbase = &block.transactions[0];
    assert!(coinbase.is_coinbase());
    assert_eq!(block.coinbase_height().map(|h| h.0), Some(1));
    assert!(coinbase.inputs()[0].outpoint().is_none());
    // A zero txid with any index other than the null index is rejected.
    let mut raw = zakura_test::vectors::BLOCK_MAINNET_1_BYTES.to_vec();
    let mut header = &raw[..];
    Header::read(&mut header).unwrap();
    // Header, count (1), version (4) and input count (1) precede the outpoint.
    let index = raw.len() - header.len() + 1 + 4 + 1 + 32;
    assert_eq!(raw[index..index + 4], u32::MAX.to_le_bytes());
    raw[index..index + 4].copy_from_slice(&0u32.to_le_bytes());
    let error = Block::zcash_deserialize(&mut raw.as_slice()).unwrap_err();
    assert!(error.to_string().contains("null input index"), "{error}");
}

#[test]
fn outputs_round_trip_and_reject_out_of_range_values() {
    let output = Output {
        value: Amount::from_u64(5).unwrap(),
        lock_script: Script::new(&[0x76, 0xa9]),
    };
    let bytes = output.zcash_serialize_to_vec().unwrap();
    assert_eq!(bytes, [5, 0, 0, 0, 0, 0, 0, 0, 2, 0x76, 0xa9]);
    assert_eq!(Output::zcash_deserialize(bytes.as_slice()).unwrap(), output);
    assert!(Output::zcash_deserialize(&bytes[..bytes.len() - 1]).is_err());
    let mut too_large = bytes.clone();
    too_large[..8].copy_from_slice(&(21_000_000 * 100_000_000u64 + 1).to_le_bytes());
    assert!(Output::zcash_deserialize(too_large.as_slice()).is_err());
    let mut negative = bytes;
    negative[..8].copy_from_slice(&(-1i64).to_le_bytes());
    assert!(Output::zcash_deserialize(negative.as_slice()).is_err());
}
