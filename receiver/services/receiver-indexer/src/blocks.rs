//! The canonical chain, read through wallet-libraries' server chain adapters, as the
//! directory store's blocks. The adapters own the node client, block decoding,
//! note positions and zero-OVK recovery; this module maps their results.
use receiver_directory::store::{Checkpoint, IndexedBlock};
use zakura_server_chain::{receiver, Network, NodeClient, NodeError};

pub use zakura_server_chain::receiver::{MAX_RECEIVER_BATCH_BLOCKS, MAX_RECEIVER_CONCURRENCY};

/// Ironwood's mainnet activation height; earlier blocks have no Ironwood tree.
pub fn ironwood_activation() -> u32 {
    Network::Mainnet.ironwood_activation()
}

/// The block at `height` and the Ironwood note position after it; see
/// [`NodeClient::receiver_boundary`].
pub async fn boundary(rpc: &NodeClient, height: u32) -> Result<Checkpoint, NodeError> {
    let checkpoint = rpc.receiver_boundary(height).await?;
    Ok(Checkpoint {
        height: checkpoint.height,
        hash: checkpoint.hash,
        position: checkpoint.position,
    })
}

/// The `count` blocks after `previous`, checked as one chain; see
/// [`NodeClient::receiver_batch`]. The caller must append every returned block in
/// order from `previous`.
pub async fn batch(
    rpc: &NodeClient,
    previous: &Checkpoint,
    count: u32,
    concurrency: u32,
) -> Result<Vec<IndexedBlock>, NodeError> {
    let previous = receiver::Checkpoint {
        height: previous.height,
        hash: previous.hash,
        position: previous.position,
    };
    Ok(rpc
        .receiver_batch(&previous, count, concurrency)
        .await?
        .into_iter()
        .map(indexed)
        .collect())
}

/// `block` as the store's block.
fn indexed(block: receiver::ReceiverBlock) -> IndexedBlock {
    IndexedBlock {
        height: block.height,
        hash: block.hash,
        parent: block.parent,
        start_position: block.start_position,
        end_position: block.end_position,
        coinbase_actions: block.coinbase_actions,
        payments: block.payments,
        commitments: block.commitments,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn ironwood_activates_where_enhance_expects() {
        assert_eq!(super::ironwood_activation(), 3_428_143);
    }
}
