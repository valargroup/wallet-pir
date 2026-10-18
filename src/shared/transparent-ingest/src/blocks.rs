//! Fetching one block and extracting its transparent events, shared by the
//! filter service's ingest loop and the txid display controller.

use crate::extract::{index, IndexedEvent};
use crate::prevout::OutputCache;
use transparent_filter::BlockHash;
use zakura_server_chain::transparent::FetchedBlock;
use zakura_server_chain::NodeClient;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// One block's indexed events, with the resolution work they cost.
pub struct BuiltEvents {
    pub block_hash: BlockHash,
    pub events: Vec<IndexedEvent>,
    /// Display source records, one per transaction with a transparent input
    /// or output, when requested; otherwise empty.
    pub display: Vec<transparent_shard::txid_v2x::TransparentDisplayRecordV2x>,
    pub rpc_lookups: u64,
    pub cache_hits: u64,
}

/// Fetches a block and extracts its events, touching no shared state.
///
/// Deliberately takes no lock. Resolving previous outputs can mean many RPC
/// round trips, and holding a store's write lock across them would stop the
/// service answering requests from the coverage it already has. The lock is
/// taken only for the append that follows.
///
/// An unresolvable previous output propagates as an error and nothing is
/// appended. Publishing a partial result would be worse than publishing
/// nothing, because a consumer cannot tell the difference.
pub async fn build_block_events(
    zakura: &NodeClient,
    cache: &mut OutputCache,
    height: u64,
    display: bool,
) -> Result<BuiltEvents, BoxError> {
    let fetched = zakura.fetch_block(height).await?;
    build_fetched_block_events(zakura, cache, height, fetched, display).await
}

/// [`build_block_events`] over a block already in hand.
///
/// Split out so a caller can fetch the next block while this one is still being
/// resolved and extracted. Fetching is the one part of the work that touches no
/// cache state: which outputs a block creates and which it spends is a property
/// of the block, and the cache only decides whether they have to be asked for.
/// So the fetch can run ahead, and everything after it cannot.
///
/// The block's own transactions seed the cache before anything is resolved, so
/// a later block spending them needs no lookup and a same-block spend is never
/// requested from the node; everything still missing is then fetched in
/// batches before extraction runs.
pub async fn build_fetched_block_events(
    zakura: &NodeClient,
    cache: &mut OutputCache,
    height: u64,
    fetched: FetchedBlock,
    display: bool,
) -> Result<BuiltEvents, BoxError> {
    let read = zakura.transparent_block(height, fetched, cache).await?;
    let extracted = index(&read.transactions, read.height, display)?;
    Ok(BuiltEvents {
        block_hash: BlockHash::from_internal_bytes(read.hash),
        events: extracted.events,
        display: extracted.display,
        // The pre-pass did the fetching; any straggler resolved during
        // extraction is counted with it.
        rpc_lookups: read.lookups,
        cache_hits: read.cache_hits,
    })
}
