//! Compact messages reconstructed from raw blocks, independently of journal events.
use transparent_blocks::proto::*;
use zakura_chain::{block::Block, serialization::ZcashSerialize, transparent::Input};

pub fn compact_block(
    block: &Block,
    height: u64,
    counts: &mut [u32; 3],
) -> Result<CompactBlock, Box<dyn std::error::Error + Send + Sync>> {
    let mut vtx = Vec::new();
    for (index, tx) in block.transactions.iter().enumerate() {
        let vin = tx
            .inputs()
            .iter()
            .filter_map(|input| match input {
                Input::PrevOut { outpoint, .. } => Some(CompactTxIn {
                    prevout_txid: outpoint.hash.0.to_vec(),
                    prevout_index: outpoint.index,
                }),
                Input::Coinbase { .. } => None,
            })
            .collect();
        let vout = tx
            .outputs()
            .iter()
            .map(|output| TxOut {
                value: u64::from(output.value),
                script_pub_key: output.lock_script.as_raw_bytes().to_vec(),
            })
            .collect();
        let spends = tx
            .sapling_nullifiers()
            .map(|nf| CompactSaplingSpend {
                nf: <[u8; 32]>::from(*nf).to_vec(),
            })
            .collect();
        let outputs = tx
            .sapling_outputs()
            .map(|output| {
                Ok(CompactSaplingOutput {
                    cmu: output.cm_u.to_bytes().to_vec(),
                    ephemeral_key: output.ephemeral_key.zcash_serialize_to_vec()?,
                    ciphertext: output.enc_ciphertext.zcash_serialize_to_vec()?[..52].to_vec(),
                })
            })
            .collect::<Result<Vec<_>, std::io::Error>>()?;
        let actions = tx
            .orchard_actions()
            .map(action)
            .collect::<Result<Vec<_>, _>>()?;
        let ironwood_actions = tx
            .ironwood_actions()
            .map(action)
            .collect::<Result<Vec<_>, _>>()?;
        for (count, n) in
            counts
                .iter_mut()
                .zip([outputs.len(), actions.len(), ironwood_actions.len()])
        {
            *count = count
                .checked_add(u32::try_from(n)?)
                .ok_or("commitment tree size overflow")?;
        }
        vtx.push(CompactTx {
            index: index as u64,
            txid: tx.hash().0.to_vec(),
            vin,
            vout,
            spends,
            outputs,
            actions,
            ironwood_actions,
            ..Default::default()
        });
    }
    Ok(CompactBlock {
        height,
        hash: block.hash().0.to_vec(),
        prev_hash: block.header.previous_block_hash.0.to_vec(),
        time: u32::try_from(block.header.time.timestamp())?,
        vtx,
        chain_metadata: Some(ChainMetadata {
            sapling_commitment_tree_size: counts[0],
            orchard_commitment_tree_size: counts[1],
            ironwood_commitment_tree_size: counts[2],
        }),
        ..Default::default()
    })
}
fn action(a: &zakura_chain::orchard::Action) -> Result<CompactOrchardAction, std::io::Error> {
    Ok(CompactOrchardAction {
        nullifier: <[u8; 32]>::from(a.nullifier).to_vec(),
        cmx: <[u8; 32]>::from(a.cm_x).to_vec(),
        ephemeral_key: a.ephemeral_key.zcash_serialize_to_vec()?,
        ciphertext: a.enc_ciphertext.zcash_serialize_to_vec()?[..52].to_vec(),
    })
}
