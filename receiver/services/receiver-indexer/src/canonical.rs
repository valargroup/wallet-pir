//! The canonical guard shared by the indexer and `serve-publication`: which node a
//! pass reads, whether an anchor is on its chain, and the revocation of served
//! anchors that fall off it. Block and chain decoding stay in [`crate::zakura`];
//! the node is reached only through the shared transport.
use crate::zakura::ZakuraClient;
use receiver_pir_server::Publications;
use tracing::warn;
use zakura_chain::block::Hash;

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

/// Whether `hash` is the node's canonical block at `height`.
pub async fn is_canonical(rpc: &ZakuraClient, height: u32, hash: [u8; 32]) -> Result<bool> {
    Ok(rpc.block_hash(u64::from(height)).await?.parse::<Hash>()?.0 == hash)
}
