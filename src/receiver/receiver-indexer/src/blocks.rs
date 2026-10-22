//! The canonical chain, read through the shared server chain adapters, as the
//! directory store's blocks. The adapters own the node client, block decoding,
//! note positions and zero-OVK recovery; this module maps their results.
use receiver_store::{Checkpoint, IndexedBlock};
use zakura_server_chain::{receiver, Network, NodeClient, NodeError};

pub use zakura_server_chain::receiver::{MAX_RECEIVER_BATCH_BLOCKS, MAX_RECEIVER_CONCURRENCY};

/// Ironwood's mainnet activation height; earlier blocks have no Ironwood tree.
pub fn ironwood_activation() -> u32 {
    Network::Mainnet.ironwood_activation()
}

/// The target time the rolling filter window spans: seven days.
pub const FILTER_WINDOW_SECS: u32 = 7 * 24 * 3600;

/// How many blocks the rolling filter window ending at `tip` retains: those spanning
/// [`FILTER_WINDOW_SECS`] of mainnet target time (8,064 of 75 seconds before NU7, up to
/// 24,192 of 25 seconds after it; see [`Network::blocks_spanning`]), or `limit` if an
/// operator set a lower one. The store's byte bound may still leave older blocks out.
pub fn filter_window_blocks(tip: u32, limit: Option<u32>) -> u32 {
    let spanning = Network::Mainnet.blocks_spanning(tip, FILTER_WINDOW_SECS);
    spanning
        .min(receiver_store::blocks::MAX_FILTER_BLOCKS)
        .min(limit.unwrap_or(u32::MAX))
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

    /// The window spans seven target days before NU7, across its activation and after
    /// it, and an operator cap only ever shortens it.
    #[test]
    fn the_filter_window_spans_seven_target_days() {
        use super::filter_window_blocks;
        let nu7 = 3_543_405;
        assert_eq!(
            filter_window_blocks(super::ironwood_activation(), None),
            8_064
        );
        assert_eq!(filter_window_blocks(nu7 - 1, None), 8_064);
        assert_eq!(filter_window_blocks(nu7 + 2_999, None), 3_000 + 7_064);
        assert_eq!(filter_window_blocks(nu7 + 24_191, None), 24_192);
        assert_eq!(filter_window_blocks(nu7 + 100_000, None), 24_192);
        assert_eq!(filter_window_blocks(nu7 + 100_000, Some(100)), 100);
        assert_eq!(filter_window_blocks(nu7 - 1, Some(20_000)), 8_064);
    }
}
