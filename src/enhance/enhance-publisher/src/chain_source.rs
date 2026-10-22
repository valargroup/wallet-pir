//! The canonical chain through the shared server chain adapters. They own the
//! node client, block and transaction decoding, fees and spent-output resolution;
//! this module maps their Enhance actions onto Enhance records.
use enhance_shared::{EnhanceRecord, EnhanceRecordParts, EnhanceTransactionMetadata};
use zakura_server_chain::enhance::EnhanceAction;

pub use zakura_server_chain::enhance::{PrevoutCounts, Prevouts, PREVOUT_BATCH};
pub use zakura_server_chain::{
    NodeClient as ZakuraClient, NodeError as ZakuraError, DEFAULT_CACHE_OUTPUTS,
};

/// One canonical block's Enhance records and the node's Ironwood tree size after it.
#[derive(Debug)]
pub struct CanonicalBlock {
    pub height: u64,
    /// The block hash in RPC display order.
    pub hash: String,
    pub records: Vec<EnhanceRecord>,
    pub tree_size: u64,
}

/// The block at `height` as Enhance records; see
/// [`ZakuraClient::enhance_block`]. Fails, rather than publishing an absent or
/// partial fee, when a spent output cannot be resolved.
pub async fn block(
    rpc: &ZakuraClient,
    height: u64,
    prevouts: &Prevouts,
) -> Result<CanonicalBlock, ZakuraError> {
    let block = rpc.enhance_block(height, prevouts).await?;
    Ok(CanonicalBlock {
        height,
        hash: zakura_server_chain::hash::display(&block.hash),
        records: records(block.actions)?,
        tree_size: block.tree_size,
    })
}

/// The block at `height`'s display hash and Enhance records, without the tree size.
pub async fn block_records(
    rpc: &ZakuraClient,
    height: u64,
    prevouts: &Prevouts,
) -> Result<(String, Vec<EnhanceRecord>), ZakuraError> {
    let (hash, actions) = rpc.enhance_actions(height, prevouts).await?;
    Ok((zakura_server_chain::hash::display(&hash), records(actions)?))
}

/// One record per action, refusing metadata the record cannot encode.
pub fn records(actions: Vec<EnhanceAction>) -> Result<Vec<EnhanceRecord>, ZakuraError> {
    actions
        .into_iter()
        .map(|action| {
            let metadata =
                EnhanceTransactionMetadata::new(action.expiry_height, action.fee_zatoshis)
                    .map_err(|error| ZakuraError::Block(error.to_string()))?;
            Ok(EnhanceRecord::from_parts(EnhanceRecordParts {
                enc_ciphertext_suffix: action.enc_ciphertext_suffix,
                cv_net: action.cv_net,
                out_ciphertext: action.out_ciphertext,
                has_transparent_inputs: action.has_transparent_inputs,
                has_transparent_outputs: action.has_transparent_outputs,
                metadata,
            }))
        })
        .collect()
}
