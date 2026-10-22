//! Stable, read-only journal capabilities used by publication builders.
use transparent_shared::journal::events::EventStore;
use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_publisher::transparent::filter::ScriptBytes;

/// Immutable journal view used by both the one-shot tool and controller snapshots.
pub trait EventJournal {
    fn journal_version(&self) -> u16;
    fn genesis_hash(&self) -> &str;
    fn start_height(&self) -> u64;
    fn covered_through(&self) -> Option<u64>;
    fn events_stored(&self) -> u64;
    fn block_at(&self, height: u64) -> Option<transparent_shared::journal::events::BlockEntry>;
    fn events_at(
        &self,
        height: u64,
    ) -> Result<
        Option<Vec<(ScriptBytes, transparent_events::TransparentEvent)>>,
        transparent_shared::journal::events::EventStoreError,
    >;
}
impl EventJournal for EventStore {
    fn journal_version(&self) -> u16 {
        self.version()
    }
    fn genesis_hash(&self) -> &str {
        self.genesis_hash()
    }
    fn start_height(&self) -> u64 {
        self.start_height()
    }
    fn covered_through(&self) -> Option<u64> {
        self.covered_through()
    }
    fn events_stored(&self) -> u64 {
        self.events_stored()
    }
    fn block_at(&self, height: u64) -> Option<transparent_shared::journal::events::BlockEntry> {
        self.block_at(height)
    }
    fn events_at(
        &self,
        height: u64,
    ) -> Result<
        Option<Vec<(ScriptBytes, transparent_events::TransparentEvent)>>,
        transparent_shared::journal::events::EventStoreError,
    > {
        self.events_at(height)
    }
}

/// A committed journal view; retained as the public builder contract.
pub use EventJournal as Journal;

/// A journal that also retains validated transaction-detail records.
pub trait DisplayJournal: EventJournal {
    fn display_at(
        &self,
        height: u64,
    ) -> Result<
        Vec<transparent_shard::txid::DisplayRecord>,
        transparent_shared::journal::events::EventStoreError,
    >;
}
impl DisplayJournal for EventStore {
    fn display_at(
        &self,
        height: u64,
    ) -> Result<
        Vec<transparent_shard::txid::DisplayRecord>,
        transparent_shared::journal::events::EventStoreError,
    > {
        self.display_at(height)
    }
}
