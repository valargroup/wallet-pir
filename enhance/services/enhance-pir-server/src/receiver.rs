//! Canonical RPC adapter. The shared crate owns recovery, storage and row encoding.
use crate::zakura::{ZakuraClient, ZakuraError};
use receiver_directory::{
    extract::Action,
    store::{Checkpoint, IndexedBlock},
    Payment,
};
use serde::Deserialize;
use serde_json::json;
use zakura_chain::{block::Block, serialization::ZcashDeserialize};

#[derive(Deserialize)]
struct Trees {
    trees: Tree,
}
#[derive(Deserialize)]
struct Tree {
    ironwood: Option<Size>,
}
#[derive(Deserialize)]
struct Size {
    size: u64,
}

impl ZakuraClient {
    /// Fetch tree size by block hash so a height reorg cannot mix two block versions.
    pub async fn receiver_boundary(&self, height: u32) -> Result<Checkpoint, ZakuraError> {
        let displayed = self.block_hash(u64::from(height)).await?;
        let hash = displayed
            .parse::<zakura_chain::block::Hash>()
            .map_err(|e| ZakuraError::Block(e.to_string()))?
            .0;
        let position = if u64::from(height) < enhance_pir::ACTIVATION_HEIGHT {
            0
        } else {
            let result: Trees = self.call("getblock", json!([displayed, 2])).await?;
            result
                .trees
                .ironwood
                .ok_or(ZakuraError::MissingTreeSize(u64::from(height)))?
                .size
        };
        if self.block_hash(u64::from(height)).await? != displayed {
            return Err(ZakuraError::Block(
                "chain changed while reading boundary".into(),
            ));
        }
        Ok(Checkpoint {
            height,
            hash,
            position,
        })
    }

    pub async fn receiver_block(&self, height: u32) -> Result<IndexedBlock, ZakuraError> {
        let boundary = self.receiver_boundary(height).await?;
        let displayed = zakura_chain::block::Hash(boundary.hash).to_string();
        let raw: String = self.call("getblock", json!([displayed, 0])).await?;
        let bytes = hex::decode(raw)?;
        let mut input = bytes.as_slice();
        let block =
            Block::zcash_deserialize(&mut input).map_err(|e| ZakuraError::Block(e.to_string()))?;
        if !input.is_empty()
            || block.hash().0 != boundary.hash
            || self.block_hash(u64::from(height)).await? != displayed
        {
            return Err(ZakuraError::Block("receiver block is not canonical".into()));
        }
        extract_block(&block, height, boundary.position)
    }
}

/// Exclude coinbase recipients while preserving their contribution to global positions.
pub fn extract_block(
    block: &Block,
    height: u32,
    end_position: u64,
) -> Result<IndexedBlock, ZakuraError> {
    if block.coinbase_height().map(|h| h.0) != Some(height) {
        return Err(ZakuraError::Block("receiver block height mismatch".into()));
    }
    let actions: u64 = block
        .transactions
        .iter()
        .map(|t| t.ironwood_actions().count() as u64)
        .sum();
    let start_position = end_position
        .checked_sub(actions)
        .ok_or_else(|| ZakuraError::Block("receiver tree size underflow".into()))?;
    let mut indexed = IndexedBlock {
        height,
        hash: block.hash().0,
        parent: block.header.previous_block_hash.0,
        start_position,
        end_position,
        coinbase_actions: 0,
        payments: Vec::new(),
    };
    let mut position = start_position;
    for (tx_index, tx) in block.transactions.iter().enumerate() {
        let coinbase = tx.is_coinbase();
        let txid = tx.hash().0;
        for (action_index, a) in tx.ironwood_actions().enumerate() {
            if coinbase {
                indexed.coinbase_actions += 1;
            } else {
                let action = Action {
                    cv: a.cv.into(),
                    nullifier: a.nullifier.into(),
                    cmx: a.cm_x.into(),
                    ephemeral_key: a.ephemeral_key.into(),
                    enc_ciphertext: a.enc_ciphertext.into(),
                    out_ciphertext: a.out_ciphertext.into(),
                };
                if let Some(receiver) = action
                    .recover_receiver()
                    .map_err(|e| ZakuraError::Block(e.to_string()))?
                {
                    indexed.payments.push((
                        receiver,
                        Payment {
                            height,
                            block_hash: indexed.hash,
                            txid,
                            tx_index: u32::try_from(tx_index)
                                .map_err(|e| ZakuraError::Block(e.to_string()))?,
                            action_index: u32::try_from(action_index)
                                .map_err(|e| ZakuraError::Block(e.to_string()))?,
                            position,
                            action_nullifier: action.nullifier,
                            cmx: action.cmx,
                            ephemeral_key: action.ephemeral_key,
                            ciphertext_prefix: action.enc_ciphertext[..52]
                                .try_into()
                                .expect("fixed ciphertext prefix"),
                        },
                    ));
                }
            }
            position += 1;
        }
    }
    Ok(indexed)
}
