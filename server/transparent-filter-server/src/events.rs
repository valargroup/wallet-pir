//! Append-only storage for the transparent event journal.
//!
//! The filter store answers "what does block *H* commit to"; this answers "what
//! happened, in order, under which script". A shard builder streams this file
//! start to finish, so the layout is optimized for sequential reading and for
//! appending a block atomically — not for random access, which nothing needs.
//!
//! On disk:
//!
//! - `meta.json`: chain identity, start height, format version.
//! - `events.bin`: append-only. Each entry is
//!   `script_len u16 | script bytes | event[EVENT_BYTES]`.
//! - `blocks.bin`: one fixed 48-byte record per covered height, ascending from
//!   `start_height`: `block_hash[32] | offset u64 | event_count u64`.
//! - `checkpoint.bin`: 16 bytes, the committed lengths of the other two.
//!
//! The same reasoning as the filter store: a JSON manifest per block would be
//! rewritten tens of thousands of times during a backfill from activation.
//!
//! # Why coverage is per block and not per event
//!
//! A block's events are appended together and only then does the checkpoint
//! move. A crash mid-block leaves bytes past the checkpoint, which are ignored
//! and overwritten on the next append. Half a block's events must never become
//! visible: a shard sealed over a partial block would be missing spends whose
//! receives it contains, and it would be sealed immutably that way.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use transparent_events::{EventError, TransparentEvent, EVENT_BYTES};
use transparent_filter::{BlockHash, ScriptBytes};

/// Bump when the on-disk layout changes. A mismatch sets the directory aside
/// and re-ingests, as the filter store does.
const STORE_VERSION: u16 = 1;

const BLOCK_RECORD_BYTES: usize = 32 + 8 + 8;

/// Refuses a script longer than any real locking script, before allocating.
///
/// Standard transparent scripts are tens of bytes. The ceiling exists so a
/// corrupt length field cannot make the reader allocate arbitrarily; it is not
/// a consensus rule and must stay well above anything the chain can contain.
const MAX_SCRIPT_BYTES: usize = 10_000;

#[derive(Debug, thiserror::Error)]
pub enum EventStoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("metadata error: {0}")]
    Metadata(#[from] serde_json::Error),
    #[error("event decode error: {0}")]
    Event(#[from] EventError),
    #[error("event store invariant violated: {0}")]
    Invariant(String),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct Meta {
    version: u16,
    /// Chain identity, display hex, as a person would read it.
    genesis_hash: String,
    start_height: u64,
}

/// Where one covered block's events live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockEntry {
    pub block_hash: BlockHash,
    /// Byte offset of this block's first event entry in `events.bin`.
    pub offset: u64,
    pub event_count: u64,
}

pub struct EventStore {
    dir: PathBuf,
    meta: Meta,
    /// Covered blocks, ascending from `meta.start_height`.
    blocks: Vec<BlockEntry>,
    events_len: u64,
    // Held for the lifetime of a writable journal, including backfills.
    _writer_lock: Option<File>,
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("8 bytes"))
}

fn truncate(path: &Path, len: u64) -> Result<(), EventStoreError> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    file.sync_all()?;
    Ok(())
}

impl EventStore {
    /// Opens or creates the store, discarding anything past the checkpoint.
    ///
    /// An existing store whose version, chain identity or start height differs
    /// is moved aside and a fresh one begun. The archive node can re-derive
    /// everything, and a restart must not require an operator on the host.
    pub fn open(
        dir: impl AsRef<Path>,
        genesis_hash: &str,
        start_height: u64,
    ) -> Result<Self, EventStoreError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("writer.lock"))?;
        lock.try_lock().map_err(|error| {
            EventStoreError::Invariant(format!("journal already has a writer: {error}"))
        })?;
        let wanted = Meta {
            version: STORE_VERSION,
            genesis_hash: genesis_hash.to_string(),
            start_height,
        };
        let meta_path = dir.join("meta.json");
        if meta_path.exists() {
            let found: Result<Meta, _> = serde_json::from_slice(&std::fs::read(&meta_path)?);
            if !matches!(&found, Ok(found) if *found == wanted) {
                let label = match &found {
                    Ok(found) => format!("v{}", found.version),
                    Err(_) => "unreadable".to_string(),
                };
                let superseded = dir.join(format!("superseded-{label}"));
                std::fs::create_dir_all(&superseded)?;
                for name in ["meta.json", "events.bin", "blocks.bin", "checkpoint.bin"] {
                    let from = dir.join(name);
                    if from.exists() {
                        std::fs::rename(&from, superseded.join(name))?;
                    }
                }
            }
        }
        if !meta_path.exists() {
            std::fs::write(&meta_path, serde_json::to_vec_pretty(&wanted)?)?;
        }
        for name in ["events.bin", "blocks.bin"] {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join(name))?;
        }

        // Everything past the checkpoint is the tail of an append that did not
        // commit. Cutting it here is what makes a crash lose a block rather
        // than expose half of one.
        let checkpoint = dir.join("checkpoint.bin");
        let (events_len, blocks_len) = if checkpoint.exists() {
            let bytes = std::fs::read(&checkpoint)?;
            if bytes.len() != 16 {
                return Err(EventStoreError::Invariant(
                    "checkpoint is not 16 bytes".into(),
                ));
            }
            (read_u64(&bytes[0..8]), read_u64(&bytes[8..16]))
        } else {
            (0, 0)
        };
        // set_len must never extend a damaged journal with plausible zeroes.
        for (name, length) in [("events.bin", events_len), ("blocks.bin", blocks_len)] {
            if std::fs::metadata(dir.join(name))?.len() < length {
                return Err(EventStoreError::Invariant(format!(
                    "{name} is shorter than its checkpoint"
                )));
            }
        }
        truncate(&dir.join("events.bin"), events_len)?;
        truncate(&dir.join("blocks.bin"), blocks_len)?;

        let raw = std::fs::read(dir.join("blocks.bin"))?;
        if raw.len() % BLOCK_RECORD_BYTES != 0 {
            return Err(EventStoreError::Invariant(
                "blocks.bin is not a whole number of records".into(),
            ));
        }
        let mut blocks = Vec::with_capacity(raw.len() / BLOCK_RECORD_BYTES);
        for record in raw.chunks_exact(BLOCK_RECORD_BYTES) {
            blocks.push(BlockEntry {
                block_hash: BlockHash::from_internal_bytes(
                    record[..32].try_into().expect("32 bytes"),
                ),
                offset: read_u64(&record[32..40]),
                event_count: read_u64(&record[40..48]),
            });
        }

        Ok(Self {
            dir,
            meta: wanted,
            blocks,
            events_len,
            _writer_lock: Some(lock),
        })
    }

    /// Opens an existing store without altering anything on disk.
    ///
    /// [`EventStore::open`] sets an incompatible directory aside and begins a
    /// fresh one, which is right for an ingest that can re-derive everything
    /// and must not need an operator on the host. It is badly wrong for a
    /// reader: a tool that guessed the start height would *move the journal*
    /// rather than report that it guessed wrong. This reads the recorded
    /// metadata and fails instead.
    ///
    /// It also does not truncate to the checkpoint, so it must not be used
    /// while an ingest is appending — anything past the checkpoint is
    /// uncommitted and is ignored here exactly as it would be after a restart.
    pub fn open_existing(dir: impl AsRef<Path>) -> Result<Self, EventStoreError> {
        let dir = dir.as_ref().to_path_buf();
        let meta_path = dir.join("meta.json");
        if !meta_path.exists() {
            return Err(EventStoreError::Invariant(format!(
                "{} holds no event journal",
                dir.display()
            )));
        }
        let meta: Meta = serde_json::from_slice(&std::fs::read(&meta_path)?)?;
        if meta.version != STORE_VERSION {
            return Err(EventStoreError::Invariant(format!(
                "journal is version {}, this build reads {STORE_VERSION}",
                meta.version
            )));
        }

        let checkpoint = dir.join("checkpoint.bin");
        let (events_len, blocks_len) = if checkpoint.exists() {
            let bytes = std::fs::read(&checkpoint)?;
            if bytes.len() != 16 {
                return Err(EventStoreError::Invariant(
                    "checkpoint is not 16 bytes".into(),
                ));
            }
            (read_u64(&bytes[0..8]), read_u64(&bytes[8..16]))
        } else {
            (0, 0)
        };

        let raw = std::fs::read(dir.join("blocks.bin"))?;
        // Only the committed prefix is real; the rest is an append that never
        // committed. Reading it would show blocks the journal does not claim.
        let raw = raw.get(..blocks_len as usize).ok_or_else(|| {
            EventStoreError::Invariant("blocks.bin is shorter than its checkpoint".into())
        })?;
        if raw.len() % BLOCK_RECORD_BYTES != 0 {
            return Err(EventStoreError::Invariant(
                "blocks.bin is not a whole number of records".into(),
            ));
        }
        let mut blocks = Vec::with_capacity(raw.len() / BLOCK_RECORD_BYTES);
        for record in raw.chunks_exact(BLOCK_RECORD_BYTES) {
            blocks.push(BlockEntry {
                block_hash: BlockHash::from_internal_bytes(
                    record[..32].try_into().expect("32 bytes"),
                ),
                offset: read_u64(&record[32..40]),
                event_count: read_u64(&record[40..48]),
            });
        }

        Ok(Self {
            dir,
            meta,
            blocks,
            events_len,
            _writer_lock: None,
        })
    }

    pub fn genesis_hash(&self) -> &str {
        &self.meta.genesis_hash
    }

    pub fn start_height(&self) -> u64 {
        self.meta.start_height
    }

    /// Highest height with durable coverage, if any.
    pub fn covered_through(&self) -> Option<u64> {
        (!self.blocks.is_empty()).then(|| self.meta.start_height + self.blocks.len() as u64 - 1)
    }

    /// The next height this store will accept.
    pub fn next_height(&self) -> u64 {
        self.covered_through()
            .map_or(self.meta.start_height, |height| height + 1)
    }

    pub fn blocks_covered(&self) -> u64 {
        self.blocks.len() as u64
    }

    pub fn events_stored(&self) -> u64 {
        self.blocks.iter().map(|block| block.event_count).sum()
    }

    pub fn block_at(&self, height: u64) -> Option<BlockEntry> {
        let index = height.checked_sub(self.meta.start_height)?;
        self.blocks.get(usize::try_from(index).ok()?).copied()
    }

    /// Appends one block's events at the next height, extending coverage.
    ///
    /// The whole block is written and made durable before the checkpoint moves,
    /// so a crash loses the block rather than recording coverage for events
    /// that are not fully there. Committing is the caller's decision, so a
    /// backfill can amortize the checkpoint over many blocks; nothing appended
    /// since the last commit is visible after a restart.
    ///
    /// An empty block is still an append. Zero events at a height is a fact
    /// about the chain, and skipping it would leave a hole that later reads
    /// could not distinguish from missing coverage.
    pub fn append_block(
        &mut self,
        height: u64,
        block_hash: BlockHash,
        events: &[(ScriptBytes, TransparentEvent)],
    ) -> Result<(), EventStoreError> {
        if height != self.next_height() {
            return Err(EventStoreError::Invariant(format!(
                "block at height {height} does not follow coverage through {:?}",
                self.covered_through()
            )));
        }
        let offset = self.events_len;
        let mut buffer = Vec::new();
        for (script, event) in events {
            let length = u16::try_from(script.as_slice().len()).map_err(|_| {
                EventStoreError::Invariant(format!(
                    "script of {} bytes is too long to store",
                    script.as_slice().len()
                ))
            })?;
            buffer.extend_from_slice(&length.to_le_bytes());
            buffer.extend_from_slice(script.as_slice());
            buffer.extend_from_slice(&event.to_bytes());
        }

        let mut file = OpenOptions::new()
            .append(true)
            .open(self.dir.join("events.bin"))?;
        file.write_all(&buffer)?;
        file.sync_all()?;

        let mut record = Vec::with_capacity(BLOCK_RECORD_BYTES);
        record.extend_from_slice(block_hash.internal_bytes());
        record.extend_from_slice(&offset.to_le_bytes());
        record.extend_from_slice(&(events.len() as u64).to_le_bytes());
        let mut blocks = OpenOptions::new()
            .append(true)
            .open(self.dir.join("blocks.bin"))?;
        blocks.write_all(&record)?;
        blocks.sync_all()?;

        self.events_len += buffer.len() as u64;
        self.blocks.push(BlockEntry {
            block_hash,
            offset,
            event_count: events.len() as u64,
        });
        Ok(())
    }

    /// Reads back one covered block's events, in the order they were appended.
    pub fn events_at(
        &self,
        height: u64,
    ) -> Result<Option<Vec<(ScriptBytes, TransparentEvent)>>, EventStoreError> {
        let Some(entry) = self.block_at(height) else {
            return Ok(None);
        };
        let mut file = File::open(self.dir.join("events.bin"))?;
        read_event_record(&mut file, entry)
    }

    /// Copies a committed suffix while the caller holds the journal lock.
    /// An anonymous file bounds RAM even when a deep fork replaces archive
    /// history; dropping the builder releases its temporary disk automatically.
    pub(crate) fn snapshot_suffix(&self, first: u64) -> Result<(File, u64), EventStoreError> {
        let offset = self.block_at(first).map_or(self.events_len, |b| b.offset);
        let mut source = File::open(self.dir.join("events.bin"))?;
        source.seek(SeekFrom::Start(offset))?;
        let mut snapshot = tempfile::tempfile_in(&self.dir)?;
        let length = self.events_len - offset;
        let copied = std::io::copy(&mut source.take(length), &mut snapshot)?;
        if copied != length {
            return Err(EventStoreError::Invariant(
                "journal shortened while taking snapshot".into(),
            ));
        }
        Ok((snapshot, offset))
    }

    /// Drops coverage above `height`, or all coverage when `None`.
    ///
    /// Unlike filter bytes, events are not content addressed, so the tail of
    /// `events.bin` is genuinely discarded. That is correct: an event carries a
    /// height, and a block that leaves the best chain takes its events with it.
    pub fn rollback_to(&mut self, height: Option<u64>) -> Result<(), EventStoreError> {
        let keep = match height {
            None => 0usize,
            Some(height) if height < self.meta.start_height => 0,
            Some(height) => ((height - self.meta.start_height + 1) as usize).min(self.blocks.len()),
        };
        // The kept prefix ends where the first dropped block began; with none
        // dropped, nothing moves.
        self.events_len = match self.blocks.get(keep) {
            Some(first_dropped) => first_dropped.offset,
            None => self.events_len,
        };
        self.blocks.truncate(keep);
        // Publish the shorter durable prefix first. A crash during truncation
        // leaves extra bytes that open() safely discards, never missing bytes
        // still named by the durable checkpoint.
        self.commit()?;
        truncate(&self.dir.join("events.bin"), self.events_len)?;
        truncate(
            &self.dir.join("blocks.bin"),
            (keep * BLOCK_RECORD_BYTES) as u64,
        )?;
        Ok(())
    }

    /// Makes everything written so far durable.
    pub fn commit(&mut self) -> Result<(), EventStoreError> {
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&self.events_len.to_le_bytes());
        bytes.extend_from_slice(&((self.blocks.len() * BLOCK_RECORD_BYTES) as u64).to_le_bytes());
        let temporary = self.dir.join("checkpoint.bin.tmp");
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(temporary, self.dir.join("checkpoint.bin"))?;
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_events::{ReceiveEvent, SpendEvent, Txid};

    const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
    const START: u64 = 100;

    fn hash(height: u64) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&height.to_le_bytes());
        BlockHash::from_internal_bytes(bytes)
    }

    fn script(tag: u8) -> ScriptBytes {
        ScriptBytes::new(vec![0x76, 0xa9, 0x14, tag])
    }

    fn receive(height: u32, tag: u8) -> (ScriptBytes, TransparentEvent) {
        (
            script(tag),
            TransparentEvent::Receive(ReceiveEvent {
                height,
                txid: Txid([tag; 32]),
                transaction_index: 0,
                output_index: 0,
                value: 1_000,
                coinbase: false,
            }),
        )
    }

    fn spend(height: u32, tag: u8) -> (ScriptBytes, TransparentEvent) {
        (
            script(tag),
            TransparentEvent::Spend(SpendEvent {
                height,
                spending_txid: Txid([tag + 1; 32]),
                transaction_index: 1,
                input_index: 0,
                spent_txid: Txid([tag; 32]),
                spent_output_index: 0,
            }),
        )
    }

    fn store(dir: &Path) -> EventStore {
        EventStore::open(dir, GENESIS, START).expect("open")
    }

    #[test]
    fn events_round_trip_through_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let block = vec![receive(100, 1), spend(100, 2)];
        {
            let mut store = store(dir.path());
            store.append_block(START, hash(START), &block).unwrap();
            store.commit().unwrap();
        }
        let store = store(dir.path());
        assert_eq!(store.covered_through(), Some(START));
        assert_eq!(store.events_stored(), 2);
        assert_eq!(store.events_at(START).unwrap(), Some(block));
    }

    /// A height with no supported activity is a fact, not an absence of
    /// coverage. Skipping it would leave a hole a shard builder could not tell
    /// from missing data.
    #[test]
    fn an_empty_block_still_advances_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = store(dir.path());
        store.append_block(START, hash(START), &[]).unwrap();
        store.commit().unwrap();
        assert_eq!(store.covered_through(), Some(START));
        assert_eq!(store.events_at(START).unwrap(), Some(vec![]));
    }

    #[test]
    fn appending_out_of_order_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = store(dir.path());
        assert!(store.append_block(START + 1, hash(START + 1), &[]).is_err());
        store.append_block(START, hash(START), &[]).unwrap();
        assert!(store.append_block(START, hash(START), &[]).is_err());
    }

    /// The crash case the checkpoint exists for: bytes were appended and made
    /// durable, but the commit never happened. Reopening must discard them
    /// rather than expose a block whose coverage was never recorded.
    #[test]
    fn an_uncommitted_append_is_discarded_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = store(dir.path());
            store
                .append_block(START, hash(START), &[receive(100, 1)])
                .unwrap();
            store.commit().unwrap();
            // Appended and fsynced, but never committed.
            store
                .append_block(START + 1, hash(START + 1), &[receive(101, 2)])
                .unwrap();
        }
        let store = store(dir.path());
        assert_eq!(store.covered_through(), Some(START));
        assert_eq!(store.events_stored(), 1);
        assert_eq!(store.events_at(START + 1).unwrap(), None);
    }

    #[test]
    fn rollback_drops_events_above_the_kept_height_and_allows_reappending() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = store(dir.path());
        for offset in 0..4u64 {
            let height = START + offset;
            store
                .append_block(
                    height,
                    hash(height),
                    &[receive(height as u32, offset as u8)],
                )
                .unwrap();
        }
        store.commit().unwrap();
        assert_eq!(store.events_stored(), 4);

        store.rollback_to(Some(START + 1)).unwrap();
        assert_eq!(store.covered_through(), Some(START + 1));
        assert_eq!(store.events_stored(), 2);
        assert_eq!(store.next_height(), START + 2);

        // A replacement branch writes different events at the same heights.
        store
            .append_block(START + 2, hash(9_999), &[spend(102, 9)])
            .unwrap();
        store.commit().unwrap();
        assert_eq!(
            store.events_at(START + 2).unwrap(),
            Some(vec![spend(102, 9)])
        );

        drop(store);
        let reopened = EventStore::open(dir.path(), GENESIS, START).unwrap();
        assert_eq!(reopened.covered_through(), Some(START + 2));
        assert_eq!(reopened.events_stored(), 3);
    }

    #[test]
    fn rolling_back_everything_empties_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = store(dir.path());
        store
            .append_block(START, hash(START), &[receive(100, 1)])
            .unwrap();
        store.commit().unwrap();
        store.rollback_to(None).unwrap();
        assert_eq!(store.covered_through(), None);
        assert_eq!(store.next_height(), START);
        assert_eq!(store.events_stored(), 0);
    }

    /// A store built for a different chain must not be read as this one's.
    #[test]
    fn an_incompatible_store_is_set_aside_rather_than_refused() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = store(dir.path());
            store
                .append_block(START, hash(START), &[receive(100, 1)])
                .unwrap();
            store.commit().unwrap();
        }
        let fresh = EventStore::open(dir.path(), GENESIS, START + 5).unwrap();
        assert_eq!(fresh.covered_through(), None);
        assert!(dir.path().join("superseded-v1").join("events.bin").exists());
    }

    /// A reader must never disturb the journal it is reading. `open` sets an
    /// incompatible directory aside by design; `open_existing` must not, or a
    /// census tool run with the wrong argument would destroy the data it was
    /// asked to measure.
    #[test]
    fn opening_existing_never_moves_anything_aside() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = store(dir.path());
            store
                .append_block(START, hash(START), &[receive(100, 1)])
                .unwrap();
            store.commit().unwrap();
        }
        let reader = EventStore::open_existing(dir.path()).unwrap();
        assert_eq!(reader.start_height(), START);
        assert_eq!(reader.covered_through(), Some(START));
        assert_eq!(reader.events_stored(), 1);
        assert!(!dir.path().join("superseded-v1").exists());
    }

    #[test]
    fn opening_a_directory_with_no_journal_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(EventStore::open_existing(dir.path()).is_err());
    }

    /// A reader sees the committed prefix only, exactly as a restart would.
    #[test]
    fn opening_existing_ignores_an_uncommitted_tail() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = store(dir.path());
            store
                .append_block(START, hash(START), &[receive(100, 1)])
                .unwrap();
            store.commit().unwrap();
            store
                .append_block(START + 1, hash(START + 1), &[receive(101, 2)])
                .unwrap();
        }
        let reader = EventStore::open_existing(dir.path()).unwrap();
        assert_eq!(reader.covered_through(), Some(START));
        assert_eq!(reader.events_at(START + 1).unwrap(), None);
    }
}

#[cfg(test)]
mod durability_tests {
    use super::*;
    #[test]
    fn a_second_writer_is_refused_without_touching_the_journal() {
        let dir = tempfile::tempdir().unwrap();
        let hash = "00".repeat(32);
        let mut first = EventStore::open(dir.path(), &hash, 0).unwrap();
        first
            .append_block(0, BlockHash::from_internal_bytes([1; 32]), &[])
            .unwrap();
        first.commit().unwrap();
        assert!(EventStore::open(dir.path(), &hash, 0).is_err());
        assert_eq!(
            EventStore::open_existing(dir.path())
                .unwrap()
                .covered_through(),
            Some(0)
        );
        drop(first);
        assert!(EventStore::open(dir.path(), &hash, 0).is_ok());
    }
    #[test]
    fn crash_after_shorter_checkpoint_before_truncation_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let hash = "00".repeat(32);
        let mut store = EventStore::open(dir.path(), &hash, 0).unwrap();
        for h in 0..3 {
            store
                .append_block(h, BlockHash::from_internal_bytes([h as u8; 32]), &[])
                .unwrap();
        }
        store.commit().unwrap();
        // The first durable step of rollback has completed, but neither file
        // has been truncated when the process disappears.
        store.blocks.truncate(1);
        store.commit().unwrap();
        drop(store);
        let mut reopened = EventStore::open(dir.path(), &hash, 0).unwrap();
        assert_eq!(reopened.covered_through(), Some(0));
        assert_eq!(
            std::fs::metadata(dir.path().join("blocks.bin"))
                .unwrap()
                .len(),
            BLOCK_RECORD_BYTES as u64
        );
        reopened
            .append_block(1, BlockHash::from_internal_bytes([9; 32]), &[])
            .unwrap();
        reopened.commit().unwrap();
        assert_eq!(
            reopened.block_at(1).unwrap().block_hash,
            BlockHash::from_internal_bytes([9; 32])
        );
    }
    #[test]
    fn a_checkpoint_past_the_file_is_refused_not_zero_extended() {
        let dir = tempfile::tempdir().unwrap();
        let hash = "00".repeat(32);
        let mut store = EventStore::open(dir.path(), &hash, 0).unwrap();
        store
            .append_block(0, BlockHash::from_internal_bytes([1; 32]), &[])
            .unwrap();
        store.commit().unwrap();
        drop(store);
        truncate(&dir.path().join("blocks.bin"), 0).unwrap();
        assert!(EventStore::open(dir.path(), &hash, 0).is_err());
        assert_eq!(
            std::fs::metadata(dir.path().join("blocks.bin"))
                .unwrap()
                .len(),
            0
        );
    }
}

/// Decode one indexed record from a locked journal or immutable suffix snapshot.
pub(crate) fn read_event_record(
    file: &mut File,
    entry: BlockEntry,
) -> Result<Option<Vec<(ScriptBytes, TransparentEvent)>>, EventStoreError> {
    file.seek(SeekFrom::Start(entry.offset))?;
    let mut reader = std::io::BufReader::new(file);
    let mut events = Vec::with_capacity(entry.event_count as usize);
    for _ in 0..entry.event_count {
        let mut length = [0u8; 2];
        reader.read_exact(&mut length)?;
        let length = u16::from_le_bytes(length) as usize;
        if length > MAX_SCRIPT_BYTES {
            return Err(EventStoreError::Invariant(format!(
                "stored script claims {length} bytes"
            )));
        }
        let mut script = vec![0u8; length];
        reader.read_exact(&mut script)?;
        let mut event = [0u8; EVENT_BYTES];
        reader.read_exact(&mut event)?;
        events.push((
            ScriptBytes::new(script),
            TransparentEvent::from_bytes(&event)?,
        ));
    }
    Ok(Some(events))
}
