//! The root chain follower's concrete node source. Scheduling lives in
//! `pir_pipeline::ingestion`; this host only supplies the RPC transport.
use pir_pipeline::ingestion::{CanonicalChain, ChainFuture, ChainSource};

/// The node RPC client as the follower's source.
pub struct NodeSource(pub pir_node_rpc::RpcClient);
impl CanonicalChain for NodeSource {
    fn hash_at(&self, height: u64) -> ChainFuture<'_, String> {
        Box::pin(async move { Ok(self.0.block_hash_display(height).await?) })
    }
}
impl ChainSource for NodeSource {
    fn tip_height(&self) -> ChainFuture<'_, u64> {
        Box::pin(async move { Ok(self.0.tip_height().await?) })
    }
}
