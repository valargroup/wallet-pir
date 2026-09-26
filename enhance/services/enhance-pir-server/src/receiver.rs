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

/// Bounds retained block memory and the amount of work before an anchor check.
pub const MAX_RECEIVER_BATCH_BLOCKS: u32 = 64;
/// Bounds concurrent load on the canonical RPC.
pub const MAX_RECEIVER_CONCURRENCY: u32 = 16;

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
            let result: Trees = self.call("getblock", json!([displayed, 1])).await?;
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

    /// Fetch a bounded range concurrently, then validate its complete chain before returning it.
    /// The caller must append every returned block in order from `previous`.
    pub async fn receiver_batch(
        &self,
        previous: &Checkpoint,
        count: u32,
        concurrency: u32,
    ) -> Result<Vec<IndexedBlock>, ZakuraError> {
        if count == 0
            || count > MAX_RECEIVER_BATCH_BLOCKS
            || concurrency == 0
            || concurrency > MAX_RECEIVER_CONCURRENCY
        {
            return Err(ZakuraError::Block("invalid receiver batch limits".into()));
        }
        let end = previous
            .height
            .checked_add(count)
            .ok_or_else(|| ZakuraError::Block("receiver height overflow".into()))?;
        let mut pending = tokio::task::JoinSet::new();
        let mut next = u64::from(previous.height) + 1;
        let mut blocks = Vec::with_capacity(count as usize);
        while next <= u64::from(end) || !pending.is_empty() {
            while next <= u64::from(end) && pending.len() < concurrency as usize {
                let rpc = self.clone();
                let height = next as u32;
                pending.spawn(
                    async move { rpc.receiver_raw_block(height).await.map(|b| (height, b)) },
                );
                next += 1;
            }
            let result = pending
                .join_next()
                .await
                .expect("nonempty receiver fetch set");
            blocks.push(result.map_err(|e| ZakuraError::Block(e.to_string()))??);
        }
        blocks.sort_by_key(|(height, _)| *height);
        let mut parent = previous.hash;
        let mut position = previous.position;
        let mut positions = Vec::with_capacity(blocks.len());
        for (height, block) in &blocks {
            if block.coinbase_height().map(|h| h.0) != Some(*height)
                || block.header.previous_block_hash.0 != parent
            {
                return Err(ZakuraError::Block(
                    "receiver batch does not extend saved chain".into(),
                ));
            }
            // Count every Action, including coinbase and outputs that cannot be recovered.
            position = position
                .checked_add(action_count(block))
                .ok_or_else(|| ZakuraError::Block("receiver position overflow".into()))?;
            positions.push(position);
            parent = block.hash().0;
        }
        // Linked headers bind every downloaded block to this canonical terminal hash.
        // Check tree size once per batch, rather than downloading verbose transactions per block.
        let anchor = self.receiver_boundary(end).await?;
        if anchor.hash != parent || anchor.position != position {
            return Err(ZakuraError::Block(
                "receiver batch anchor or tree size mismatch".into(),
            ));
        }
        blocks
            .into_iter()
            .zip(positions)
            .map(|((height, block), position)| extract_block(&block, height, position))
            .collect()
    }

    async fn receiver_raw_block(&self, height: u32) -> Result<Block, ZakuraError> {
        let raw: String = self
            .call("getblock", json!([height.to_string(), 0]))
            .await?;
        let bytes = hex::decode(raw)?;
        let mut input = bytes.as_slice();
        let block =
            Block::zcash_deserialize(&mut input).map_err(|e| ZakuraError::Block(e.to_string()))?;
        if !input.is_empty() {
            return Err(ZakuraError::Block("trailing receiver block bytes".into()));
        }
        Ok(block)
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
    let actions = action_count(block);
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

fn action_count(block: &Block) -> u64 {
    block
        .transactions
        .iter()
        .map(|t| t.ironwood_actions().count() as u64)
        .sum()
}
