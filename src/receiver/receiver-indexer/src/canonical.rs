//! The canonical guard shared by the indexer and `serve-publication`: which node a
//! pass reads, whether an anchor is on its chain, and the revocation of served
//! anchors that fall off it. Block and chain decoding stay in the
//! server-only chain adapter.
use receiver_pir_server::Publications;
use tracing::warn;
use zakura_pir_publisher::receiver::Hash;
use zakura_server_chain::NodeClient as ZakuraClient;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The node on `genesis`'s network with the highest tip, which runs a whole pass, and
/// its tip (see [`ZakuraClient::ranked`]).
pub async fn freshest(nodes: &[ZakuraClient], genesis: Hash) -> Result<(u64, ZakuraClient)> {
    Ok(ZakuraClient::ranked(nodes, genesis).await?.swap_remove(0))
}

/// Revokes every session once `rpc`, a node at tip `tip`, shows a served anchor is off
/// its chain, unless a revocation or rotation stopped serving that anchor during the
/// check (see [`Publications::revoke_serving`]). A failed request, a node behind an
/// anchor or one no longer on `genesis`'s network proves nothing, so sessions keep
/// serving.
pub async fn check_serving(
    publications: &Publications,
    rpc: &ZakuraClient,
    genesis: Hash,
    tip: u64,
) -> Result<()> {
    let (epoch, anchors) = publications.serving();
    if anchors.is_empty() {
        return Ok(());
    }
    for (height, hash) in anchors {
        if u64::from(height) <= tip && !is_canonical(rpc, height, hash).await? {
            // Ranking read the genesis earlier, and an endpoint can be repointed.
            rpc.check_network(genesis).await?;
            if publications.revoke_serving(epoch, (height, hash)) {
                warn!(height, "revoked noncanonical receiver sessions");
            }
            break;
        }
    }
    Ok(())
}

/// Checks every served anchor against the freshest node every `poll` until the task
/// is dropped, independently of any preparation in progress. A failed check keeps
/// serving: an outage proves nothing about the chain.
pub async fn guard(
    publications: Publications,
    nodes: Vec<ZakuraClient>,
    genesis: Hash,
    poll: std::time::Duration,
) {
    loop {
        let checked = async {
            let (tip, rpc) = freshest(&nodes, genesis).await?;
            check_serving(&publications, &rpc, genesis, tip).await
        };
        if let Err(error) = checked.await {
            warn!(%error, "canonical validation deferred; still serving");
        }
        tokio::time::sleep(poll).await;
    }
}

/// A block a served table depends on: its height and hash, and the Ironwood tree size
/// the table claims after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TreeAnchor {
    pub height: u32,
    pub hash: Hash,
    pub size: u64,
}

impl TreeAnchor {
    /// A payment table's end.
    pub fn payments(m: &zakura_pir_publisher::receiver::snapshot::Manifest) -> Self {
        Self {
            height: m.end_height,
            hash: m.end_hash,
            size: m.end_position,
        }
    }

    /// A companion witness table's construction block.
    pub fn witnesses(m: &zakura_pir_publisher::receiver::witness::Manifest) -> Self {
        Self {
            height: m.tree_height,
            hash: m.tree_hash,
            size: m.tree_size,
        }
    }
}

/// How a node's chain differs from a [`TreeAnchor`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TreeMismatch {
    /// The node has another block at that height.
    OffChain,
    /// The node has the block, with this Ironwood tree size after it.
    Size(u64),
}

/// How `anchor` differs from `rpc`'s chain, `None` when the node has its block with its
/// tree size. Both are read by that block's hash, so a reorg during the read cannot mix
/// two blocks; a node that cannot give the size proves nothing and is an error.
pub async fn tree_mismatch(
    rpc: &ZakuraClient,
    anchor: TreeAnchor,
) -> std::result::Result<Option<TreeMismatch>, zakura_server_chain::NodeError> {
    let boundary = crate::blocks::boundary(rpc, anchor.height).await?;
    Ok(if boundary.hash != anchor.hash {
        Some(TreeMismatch::OffChain)
    } else if boundary.position != anchor.size {
        Some(TreeMismatch::Size(boundary.position))
    } else {
        None
    })
}

/// Whether `hash` is the node's canonical block at `height`.
pub async fn is_canonical(rpc: &ZakuraClient, height: u32, hash: [u8; 32]) -> Result<bool> {
    Ok(rpc.block_hash(u64::from(height)).await? == hash)
}
