//! A copied journal suffix that publication builds read while ingest keeps
//! appending to the journal itself.

use crate::publication::{BoxError, Journal};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use transparent_events::TransparentEvent;
use transparent_filter::{ScriptBytes, ShardMap};
use transparent_shared::journal::events::{BlockEntry, EventStore, EventStoreError};

/// Snapshot only rebuilt event rows plus the endpoint hashes used to establish
/// reuse. It stays valid even if ingest truncates and rewrites its journal.
pub struct Snapshot {
    journal_version: u16,
    genesis: String,
    first: u64,
    end: u64,
    count: u64,
    blocks: BTreeMap<u64, BlockEntry>,
    events: std::sync::Mutex<std::fs::File>,
    events_offset: u64,
    events_first: u64,
    display_dir: PathBuf,
}
impl Snapshot {
    /// The last journal height the snapshot covers.
    pub fn end(&self) -> u64 {
        self.end
    }

    /// Copies the journal suffix `previous` does not cover as sealed into an
    /// anonymous file under `temp_dir`.
    pub fn capture(
        store: &EventStore,
        previous: &ShardMap,
        temp_dir: &Path,
    ) -> Result<Self, BoxError> {
        let end = store.covered_through().ok_or("empty journal")?;
        let mut first = store.start_height();
        let mut blocks = BTreeMap::new();
        for entry in &previous.shards {
            if let Some(block) = store.block_at(entry.end_height) {
                blocks.insert(entry.end_height, block);
            }
        }
        for entry in &previous.shards {
            if !entry.sealed
                || entry.end_height > end
                || blocks
                    .get(&entry.end_height)
                    .is_none_or(|b| b.block_hash.to_display_hex() != entry.terminal_block_hash)
            {
                break;
            }
            first = entry.end_height + 1;
        }
        blocks.insert(end, store.block_at(end).ok_or("snapshot endpoint missing")?);
        let (events, events_offset) = store.snapshot_suffix(first, temp_dir)?;
        for height in first..=end {
            blocks.insert(
                height,
                store.block_at(height).ok_or("snapshot block missing")?,
            );
        }
        // A followed journal's writer may have rolled back and rewritten the
        // suffix while it was copied. An unchanged endpoint means it did not.
        if !store.still_committed(end, blocks[&end])? {
            return Err("the journal changed while its suffix was copied".into());
        }
        Ok(Self {
            journal_version: store.version(),
            genesis: store.genesis_hash().into(),
            first: store.start_height(),
            end,
            count: store.events_stored(),
            blocks,
            events: std::sync::Mutex::new(events),
            events_offset,
            events_first: first,
            display_dir: store.directory().to_path_buf(),
        })
    }
}
impl Journal for Snapshot {
    fn journal_version(&self) -> u16 {
        self.journal_version
    }
    fn display_at(
        &self,
        height: u64,
    ) -> Result<Vec<transparent_shard::txid::DisplayRecord>, EventStoreError> {
        let block = self
            .blocks
            .get(&height)
            .ok_or_else(|| EventStoreError::Invariant("snapshot display block missing".into()))?;
        let sources = transparent_shared::journal::display_journal::read_sources(
            &self.display_dir,
            block.block_hash,
        )?;
        transparent_shared::journal::display_journal::validate_events(
            &sources,
            &self.events_at(height)?.ok_or_else(|| {
                EventStoreError::Invariant("snapshot display events missing".into())
            })?,
        )?;
        sources
            .iter()
            .map(|source| {
                source
                    .display_record()
                    .map_err(|e| EventStoreError::Invariant(e.to_string()))
            })
            .collect()
    }
    fn genesis_hash(&self) -> &str {
        &self.genesis
    }
    fn start_height(&self) -> u64 {
        self.first
    }
    fn covered_through(&self) -> Option<u64> {
        Some(self.end)
    }
    fn events_stored(&self) -> u64 {
        self.count
    }
    fn block_at(&self, h: u64) -> Option<BlockEntry> {
        self.blocks.get(&h).copied()
    }
    fn events_at(
        &self,
        h: u64,
    ) -> Result<Option<Vec<(ScriptBytes, TransparentEvent)>>, EventStoreError> {
        if h < self.events_first {
            return Ok(None);
        }
        let Some(mut entry) = self.blocks.get(&h).copied() else {
            return Ok(None);
        };
        entry.offset = entry
            .offset
            .checked_sub(self.events_offset)
            .ok_or_else(|| EventStoreError::Invariant("snapshot offset precedes suffix".into()))?;
        let mut file = self.events.lock().unwrap();
        let end = self
            .blocks
            .get(&(h + 1))
            .map_or(file.metadata()?.len(), |next| {
                next.offset - self.events_offset
            });
        let mut events =
            transparent_shared::journal::events::read_event_record(&mut file, entry, end)?;
        if let Some(events) = &mut events {
            events.extend(
                transparent_shared::journal::display_journal::oversized_events(
                    &self.display_dir,
                    entry.block_hash,
                )?,
            );
        }
        Ok(events)
    }
}
