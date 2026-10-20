//! Reading the chain from the node's own database instead of over its RPC.
//!
//! The ingest path this replaces asked the node, over JSON-RPC, for a block and
//! then for every previous transaction the block's inputs spend. The node
//! answers those from its transaction index, so the work was already a database
//! read; the RPC only added a round trip, a hex encoding and a re-parse to it.
//! That cost was real: at height 344,000 a block needed 387 `getrawtransaction`
//! lookups, and the machinery built to avoid them — a bounded output cache, a
//! batching pre-pass, a pipelined fetch — existed only to hide the round trip.
//!
//! The reader is the shared chain reader's
//! `zakura_server_chain::transparent::state::StateReader`, which opens the node's
//! RocksDB as a **secondary instance**: a running node's database is read without
//! stopping it and without a copy, and previous outputs resolve from its
//! transaction index. That module also records why the node's address index is
//! not used: it holds only P2PKH and P2SH, while [`crate::ingest::extract`] indexes every
//! nonempty output script that does not begin with `OP_RETURN`, raw. This module
//! keeps the service's view of it: display-order hashes and event extraction.
//!
//! # Coverage
//!
//! The reader is a snapshot of what the node had committed when it opened, and
//! it does not see the non-finalized state at all. For a backfill of buried
//! history that is irrelevant. Near the tip it means a height can be absent,
//! which surfaces as [`MissingBlock`] rather than as a wrong answer. A long
//! backfill reopens the reader periodically; see
//! `zakura_server_chain::transparent::state`.
//!
//! [`MissingBlock`]: zakura_server_chain::transparent::ExtractError::MissingBlock

use zakura_pir_publisher::transparent::filter as transparent_filter;

use crate::ingest::blocks::BuiltEvents;
use crate::ingest::extract::index;
use std::path::Path;
use transparent_filter::BlockHash;
pub use zakura_server_chain::transparent::state::StateError;
use zakura_server_chain::transparent::FetchedBlock;
use zakura_server_chain::Network;

/// A read-only view of a running node's finalized state.
pub struct StateReader(zakura_server_chain::transparent::state::StateReader);

impl StateReader {
    /// Opens the node's state directory read-only.
    ///
    /// `cache_dir` is the node's `[state] cache_dir` — the directory holding
    /// `state/vN/<network>`, not that inner path. Resolving previous outputs
    /// needs `tx_by_loc`, which only an archive node retains for the whole chain.
    pub fn open(cache_dir: impl AsRef<Path>, network: Network) -> Result<Self, StateError> {
        zakura_server_chain::transparent::state::StateReader::open(cache_dir, network).map(Self)
    }

    /// The highest finalized height the secondary can see.
    pub fn tip_height(&self) -> Result<u64, StateError> {
        self.0.tip_height()
    }

    /// The hash of a finalized block, in the display order a person reads.
    pub fn block_hash(&self, height: u64) -> Result<String, StateError> {
        self.0
            .block_hash(height)
            .map(|hash| BlockHash::from_internal_bytes(hash).to_display_hex())
    }

    /// Read the full raw block without consulting the transparent event journal.
    pub fn raw_block(&self, height: u64) -> Result<FetchedBlock, StateError> {
        self.0.fetch_block(height)
    }

    /// The header time of a finalized block, as the miner stamped it.
    ///
    /// Block times are not monotone — consensus only requires each to exceed
    /// the median of the eleven before it — so a caller deriving a calendar
    /// boundary must search a window, not a single crossing.
    pub fn block_time(
        &self,
        height: u64,
    ) -> Result<chrono::DateTime<chrono::Utc>, crate::ingest::BoxError> {
        let seconds = self.0.block_time(height)?;
        chrono::DateTime::from_timestamp(seconds, 0)
            .ok_or_else(|| format!("block {height} has an unrepresentable time {seconds}").into())
    }

    /// Extracts one block's events, resolving previous outputs from the same
    /// database.
    ///
    /// Synchronous and self-contained: it holds no state that another height's
    /// work depends on, which is what lets a caller run several heights at once
    /// and append them in order. The RPC path could not do that, because every
    /// block wrote the output cache the next one read.
    pub fn block_events(
        &self,
        height: u64,
        display: bool,
    ) -> Result<BuiltEvents, crate::ingest::BoxError> {
        let read = self.0.transparent_block(height)?;
        let extracted = index(&read.transactions, read.height, display)?;
        Ok(BuiltEvents {
            block_hash: BlockHash::from_internal_bytes(read.hash),
            events: extracted.events,
            display: extracted.display,
            // Named for the field the RPC path fills. These are local database
            // reads, and a run reporting thousands of them per second is
            // reporting that, not network traffic.
            rpc_lookups: read.lookups,
            cache_hits: read.cache_hits,
        })
    }
}
