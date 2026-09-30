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
//! Zakura's state crate opens the same RocksDB as a **secondary instance**, so
//! this reads a running node's database without stopping it and without a copy.
//! `ZakuraDb::transaction` resolves a previous output through `tx_loc_by_hash`
//! and `tx_by_loc` as two local point lookups, which is the whole of what the
//! cache was standing in for. So there is no cache here: a miss costs a page
//! cache hit rather than a network round trip, and a second cache layer over
//! RocksDB's own would only duplicate it.
//!
//! # What this deliberately does not read
//!
//! Zakura also maintains `tx_loc_by_transparent_addr_loc`, which is already
//! grouped by address and ordered by height — the shape a shard wants. It is
//! not usable here. That index is built with
//! `filter_map(|utxo| utxo.output.address(network))`, so it holds only P2PKH
//! and P2SH, while [`crate::extract`] indexes every nonempty output script that
//! does not begin with `OP_RETURN`, raw. Sourcing events from the address index
//! would silently drop every nonstandard script, and a wallet told it has no
//! activity does not look again.
//!
//! # Coverage
//!
//! A secondary instance sees the primary's flushed data, so it trails the tip
//! by whatever is still in the primary's memtable, and it does not see the
//! non-finalized state at all. For a backfill of buried history that is
//! irrelevant. Near the tip it means a height can be briefly absent, which
//! surfaces as [`StateError::MissingBlock`] rather than as a wrong answer.

use crate::extract::{extract_events, outpoint_label, PreviousOutputs};
use crate::ingest::BuiltEvents;
use std::path::{Path, PathBuf};
use transparent_filter::BlockHash;
use zakura_chain::parameters::Network;
use zakura_chain::transparent::{OutPoint, Output};
use zakura_state::{Config, HashOrHeight, ReadStateService, StorageMode, ZakuraDb};

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("could not open the Zakura state database at {path}: {source}")]
    Open {
        path: PathBuf,
        #[source]
        source: zakura_state::StateInitError,
    },
    #[error("the state database holds no block at height {0}")]
    MissingBlock(u64),
    #[error("the state database holds no finalized tip")]
    NoTip,
    #[error("height {0} does not fit a block height")]
    HeightRange(u64),
}

/// A read-only view of a running node's finalized state.
///
/// Holds the `ReadStateService` and the non-finalized sender that
/// [`zakura_state::init_read_only`] returns even though neither is used: the
/// secondary instance's workspace is torn down when its handles drop, so
/// dropping them early would close the database this reads.
pub struct StateReader {
    db: ZakuraDb,
    _read_service: ReadStateService,
    _non_finalized: tokio::sync::watch::Sender<zakura_state::NonFinalizedState>,
}

impl StateReader {
    /// Opens the node's state directory read-only.
    ///
    /// `cache_dir` is the node's `[state] cache_dir` — the directory holding
    /// `state/vN/<network>`, not that inner path. The storage mode must match
    /// the node's, because it decides whether raw transactions are expected to
    /// be present; resolving previous outputs needs `tx_by_loc`, which only an
    /// archive node retains for the whole chain.
    pub fn open(cache_dir: impl AsRef<Path>, network: Network) -> Result<Self, StateError> {
        let cache_dir = cache_dir.as_ref().to_path_buf();
        let config = Config {
            cache_dir: cache_dir.clone(),
            ephemeral: false,
            // A reader must never delete a database, and the node's own older
            // format directories are not ours to collect.
            delete_old_database: false,
            should_backup_non_finalized_state: false,
            storage_mode: StorageMode::Archive,
            ..Config::default()
        };
        let (read_service, db, non_finalized) = zakura_state::init_read_only(config, &network)
            .map_err(|source| StateError::Open {
                path: cache_dir,
                source,
            })?;
        Ok(Self {
            db,
            _read_service: read_service,
            _non_finalized: non_finalized,
        })
    }

    /// The highest finalized height the secondary can see.
    pub fn tip_height(&self) -> Result<u64, StateError> {
        self.db
            .finalized_tip_height()
            .map(|height| u64::from(height.0))
            .ok_or(StateError::NoTip)
    }

    /// The hash of a finalized block, in the display order a person reads.
    pub fn block_hash(&self, height: u64) -> Result<String, StateError> {
        let height = to_height(height)?;
        self.db
            .hash(height)
            .map(|hash| hash.to_string())
            .ok_or(StateError::MissingBlock(u64::from(height.0)))
    }

    /// Read the full raw block without consulting the transparent event journal.
    pub fn raw_block(
        &self,
        height: u64,
    ) -> Result<std::sync::Arc<zakura_chain::block::Block>, StateError> {
        self.db
            .block(HashOrHeight::Height(to_height(height)?))
            .ok_or(StateError::MissingBlock(height))
    }

    /// The header time of a finalized block, as the miner stamped it.
    ///
    /// Block times are not monotone — consensus only requires each to exceed
    /// the median of the eleven before it — so a caller deriving a calendar
    /// boundary must search a window, not a single crossing.
    pub fn block_time(&self, height: u64) -> Result<chrono::DateTime<chrono::Utc>, StateError> {
        let block_height = to_height(height)?;
        self.db
            .block_header(HashOrHeight::Height(block_height))
            .map(|header| header.time)
            .ok_or(StateError::MissingBlock(height))
    }

    /// Extracts one block's events, resolving previous outputs from the same
    /// database.
    ///
    /// Synchronous and self-contained: it holds no state that another height's
    /// work depends on, which is what lets a caller run several heights at once
    /// and append them in order. The RPC path could not do that, because every
    /// block wrote the output cache the next one read.
    pub fn block_events(&self, height: u64) -> Result<BuiltEvents, crate::ingest::BoxError> {
        let block_height = to_height(height)?;
        let block = self
            .db
            .block(HashOrHeight::Height(block_height))
            .ok_or(StateError::MissingBlock(height))?;
        let block_hash = BlockHash::from_display_hex(&block.hash().to_string())?;
        let event_height = u32::try_from(height)
            .map_err(|_| format!("height {height} does not fit the event encoding"))?;

        let mut previous = StatePreviousOutputs {
            db: &self.db,
            lookups: 0,
        };
        let events = extract_events(&block.transactions, &mut previous, event_height)?;
        Ok(BuiltEvents {
            block_hash,
            events,
            // Named for the field the RPC path fills. These are local database
            // reads, and a run reporting thousands of them per second is
            // reporting that, not network traffic.
            rpc_lookups: previous.lookups,
            cache_hits: 0,
        })
    }
}

fn to_height(height: u64) -> Result<zakura_chain::block::Height, StateError> {
    u32::try_from(height)
        .ok()
        .filter(|height| *height <= zakura_chain::block::Height::MAX.0)
        .map(zakura_chain::block::Height)
        .ok_or(StateError::HeightRange(height))
}

/// Resolves a spent output's locking script from the node's transaction index.
///
/// Not from `utxo_by_out_loc`: the node deletes an output from the UTXO set
/// when it is spent, and every output this resolver is asked about is one that
/// is being spent. The creating transaction is still in `tx_by_loc` on an
/// archive node, which is why the storage mode is checked when opening.
struct StatePreviousOutputs<'a> {
    db: &'a ZakuraDb,
    /// Previous-output resolutions that reached the database.
    lookups: u64,
}

impl PreviousOutputs for StatePreviousOutputs<'_> {
    fn previous_output(
        &mut self,
        outpoint: &OutPoint,
    ) -> Result<Option<Output>, Box<dyn std::error::Error + Send + Sync>> {
        self.lookups += 1;
        let Some((transaction, _height, _time)) = self.db.transaction(outpoint.hash) else {
            return Ok(None);
        };
        if transaction.hash() != outpoint.hash {
            return Err("previous transaction identity mismatch".into());
        }
        let script = transaction.outputs().get(outpoint.index as usize).cloned();
        if script.is_none() {
            tracing::warn!(
                outpoint = %outpoint_label(outpoint),
                outputs = transaction.outputs().len(),
                "previous transaction has no output at this index"
            );
        }
        Ok(script)
    }
}
