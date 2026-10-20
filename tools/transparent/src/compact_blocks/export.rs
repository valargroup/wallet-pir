//! Compact messages reconstructed from raw blocks, independently of journal events.
//!
//! The shared chain reader's `zakura_server_chain::transparent` reads the compact fields
//! out of the raw block; this module frames them as lightwalletd's protobuf
//! messages.
use super::proto::*;
use zakura_server_chain::transparent::{self as chain, FetchedBlock};

pub fn compact_block(
    block: &FetchedBlock,
    height: u64,
    counts: &mut [u32; 3],
) -> Result<CompactBlock, Box<dyn std::error::Error + Send + Sync>> {
    let read = block.compact(height, counts)?;
    Ok(CompactBlock {
        height: read.height,
        hash: read.hash.to_vec(),
        prev_hash: read.prev_hash.to_vec(),
        time: read.time,
        vtx: read.transactions.into_iter().map(transaction).collect(),
        chain_metadata: Some(ChainMetadata {
            sapling_commitment_tree_size: read.sapling_tree_size,
            orchard_commitment_tree_size: read.orchard_tree_size,
            ironwood_commitment_tree_size: read.ironwood_tree_size,
        }),
        ..Default::default()
    })
}

fn transaction(tx: chain::CompactTransaction) -> CompactTx {
    CompactTx {
        index: tx.index,
        txid: tx.txid.to_vec(),
        vin: tx
            .inputs
            .into_iter()
            .map(|input| CompactTxIn {
                prevout_txid: input.prevout_txid.to_vec(),
                prevout_index: input.prevout_index,
            })
            .collect(),
        vout: tx
            .outputs
            .into_iter()
            .map(|output| TxOut {
                value: output.value,
                script_pub_key: output.script,
            })
            .collect(),
        spends: tx
            .sapling_spends
            .into_iter()
            .map(|nf| CompactSaplingSpend { nf: nf.to_vec() })
            .collect(),
        outputs: tx
            .sapling_outputs
            .into_iter()
            .map(|output| CompactSaplingOutput {
                cmu: output.cmu,
                ephemeral_key: output.ephemeral_key,
                ciphertext: output.ciphertext,
            })
            .collect(),
        actions: tx.orchard_actions.into_iter().map(action).collect(),
        ironwood_actions: tx.ironwood_actions.into_iter().map(action).collect(),
        ..Default::default()
    }
}

fn action(a: chain::CompactAction) -> CompactOrchardAction {
    CompactOrchardAction {
        nullifier: a.nullifier,
        cmx: a.cmx,
        ephemeral_key: a.ephemeral_key,
        ciphertext: a.ciphertext,
    }
}
