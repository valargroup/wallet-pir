use crate::types::{DatabaseId, DatabaseLayout};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// Version 7's manifest binds both record width and row packing. Schema 11 keeps
// the manifest encoding, but its 653-byte width rejects all older 737-byte journals.
// Always rebuild in a separate directory; never reinterpret or truncate old data.
const STORE_VERSION: u16 = 7;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("manifest error: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("store invariant violated: {0}")]
    Invariant(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockEntry {
    pub height: u64,
    pub hash: String,
    pub first_position: u64,
    pub action_count: u64,
}

fn default_table() -> String {
    // Unnamed version-3 journals were ACTION journals, not Enhance journals.
    "action".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoreManifest {
    version: u16,
    #[serde(default)]
    record_bytes: usize,
    #[serde(default)]
    records_per_row: usize,
    #[serde(default = "default_table")]
    table: String,
    tree_size: u64,
    blocks: Vec<BlockEntry>,
}

/// Append-only journal of fixed-size records for one table, indexed by
/// commitment-tree position from zero. Position to offset is
/// `position * layout.record_bytes`.
///
/// After a mutation I/O error the journal is poisoned until reopened. Infallible
/// metadata getters expose only the last in-memory snapshot for diagnostics;
/// fallible reads, writes, and publication must check journal health.
pub struct RecordJournal {
    // Hold an exclusive writer lock for the lifetime of this journal.
    _lock: File,
    dir: PathBuf,
    records_path: PathBuf,
    name: String,
    table: Option<DatabaseId>,
    layout: DatabaseLayout,
    manifest: StoreManifest,
    poisoned: bool,
    #[cfg(test)]
    fail_after: Option<&'static str>,
}

impl RecordJournal {
    /// Opens the journal of a served table.
    pub fn open(
        path: impl AsRef<Path>,
        table: DatabaseId,
        layout: DatabaseLayout,
    ) -> Result<Self, StoreError> {
        Self::open_inner(path.as_ref(), table.as_str(), Some(table), layout)
    }

    /// Opens a journal that is not itself a served table (a log other tables
    /// are built from).
    pub fn open_log(
        path: impl AsRef<Path>,
        name: &str,
        layout: DatabaseLayout,
    ) -> Result<Self, StoreError> {
        Self::open_inner(path.as_ref(), name, None, layout)
    }

    fn open_inner(
        path: &Path,
        name: &str,
        table: Option<DatabaseId>,
        layout: DatabaseLayout,
    ) -> Result<Self, StoreError> {
        let dir = path.to_path_buf();
        fs::create_dir_all(&dir)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("journal.lock"))?;
        lock.try_lock()
            .map_err(|error| StoreError::Invariant(format!("journal is already open: {error}")))?;
        let records_path = dir.join("records.bin");
        let manifest_path = dir.join("manifest.json");
        let fresh = || StoreManifest {
            version: STORE_VERSION,
            record_bytes: layout.record_bytes,
            records_per_row: layout.records_per_row,
            table: name.to_string(),
            tree_size: 0,
            blocks: Vec::new(),
        };
        let manifest = if manifest_path.exists() {
            let bytes = fs::read(&manifest_path)?;
            let parsed: StoreManifest = serde_json::from_slice(&bytes)?;
            if parsed.version != STORE_VERSION
                || parsed.table != name
                || parsed.record_bytes != layout.record_bytes
                || parsed.records_per_row != layout.records_per_row
            {
                return Err(StoreError::Invariant(format!(
                    "incompatible journal version/table: {}/{}; expected {}/{}; rebuild in a separate data directory",
                    parsed.version, parsed.table, STORE_VERSION, name
                )));
            } else {
                parsed
            }
        } else {
            fresh()
        };

        let expected_len = manifest
            .tree_size
            .checked_mul(layout.record_bytes as u64)
            .ok_or_else(|| StoreError::Invariant("record length overflow".to_string()))?;
        let records = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&records_path)?;
        if records.metadata()?.len() < expected_len {
            return Err(StoreError::Invariant(
                "records file is shorter than committed manifest".to_string(),
            ));
        }
        // A prior rename may have succeeded before directory sync failed. Make
        // the selected manifest durable before reclaiming any excess record tail.
        File::open(&dir)?.sync_all()?;
        records.set_len(expected_len)?;
        records.sync_all()?;

        let store = Self {
            _lock: lock,
            dir,
            records_path,
            name: name.to_string(),
            table,
            layout,
            manifest,
            poisoned: false,
            #[cfg(test)]
            fail_after: None,
        };
        if !store.manifest_path().exists() {
            store.persist_manifest(&store.manifest)?;
        }
        Ok(store)
    }

    /// The served table this journal backs, if any.
    pub fn table(&self) -> Option<DatabaseId> {
        self.table
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn layout(&self) -> &DatabaseLayout {
        &self.layout
    }

    pub fn tree_size(&self) -> u64 {
        self.manifest.tree_size
    }

    pub fn last_block(&self) -> Option<&BlockEntry> {
        self.manifest.blocks.last()
    }

    pub fn blocks(&self) -> &[BlockEntry] {
        &self.manifest.blocks
    }

    /// Removes every block above `height`, or all blocks when `height` is `None`.
    /// Commit the smaller manifest before reclaiming records. After an I/O error,
    /// drop and reopen the journal before using it again.
    pub fn rewind_to_height(&mut self, height: Option<u64>) -> Result<(), StoreError> {
        self.ensure_healthy()?;
        let keep = height.map_or(0, |height| {
            self.manifest
                .blocks
                .partition_point(|block| block.height <= height)
        });
        let tree_size = self.manifest.blocks.get(keep).map_or_else(
            || {
                self.manifest
                    .blocks
                    .last()
                    .filter(|_| keep == self.manifest.blocks.len())
                    .map_or(0, |block| block.first_position + block.action_count)
            },
            |block| block.first_position,
        );
        let expected_len = tree_size
            .checked_mul(self.layout.record_bytes as u64)
            .ok_or_else(|| StoreError::Invariant("record length overflow".into()))?;
        let mut next = self.manifest.clone();
        next.blocks.truncate(keep);
        next.tree_size = tree_size;
        let result = (|| {
            self.persist_manifest(&next)?;
            self.manifest = next;
            let records = OpenOptions::new().write(true).open(&self.records_path)?;
            records.set_len(expected_len)?;
            self.checkpoint("truncate")?;
            records.sync_all()?;
            self.checkpoint("truncate_sync")
        })();
        self.poison_on_error(result)
    }

    pub fn append_block<R: AsRef<[u8]>>(
        &mut self,
        height: u64,
        hash: String,
        records: &[R],
    ) -> Result<(), StoreError> {
        self.ensure_healthy()?;
        if let Some(previous) = self.last_block() {
            if previous.height.checked_add(1) != Some(height) {
                return Err(StoreError::Invariant(format!(
                    "block height {height} does not follow {}",
                    previous.height
                )));
            }
        }
        if let Some(bad) = records
            .iter()
            .find(|record| record.as_ref().len() != self.layout.record_bytes)
        {
            return Err(StoreError::Invariant(format!(
                "record has {} bytes, layout needs {}",
                bad.as_ref().len(),
                self.layout.record_bytes
            )));
        }

        let first_position = self.manifest.tree_size;
        let offset = first_position
            .checked_mul(self.layout.record_bytes as u64)
            .ok_or_else(|| StoreError::Invariant("record length overflow".into()))?;
        let mut next = self.manifest.clone();
        next.tree_size = first_position
            .checked_add(records.len() as u64)
            .ok_or_else(|| StoreError::Invariant("tree size overflow".into()))?;
        next.tree_size
            .checked_mul(self.layout.record_bytes as u64)
            .ok_or_else(|| StoreError::Invariant("record length overflow".into()))?;
        next.blocks.push(BlockEntry {
            height,
            hash,
            first_position,
            action_count: records.len() as u64,
        });
        let result = (|| {
            let mut file = OpenOptions::new().write(true).open(&self.records_path)?;
            file.set_len(offset)?;
            file.seek(SeekFrom::Start(offset))?;
            for record in records {
                file.write_all(record.as_ref())?;
                self.checkpoint("record_write")?;
            }
            file.sync_all()?;
            self.checkpoint("record_sync")?;
            self.persist_manifest(&next)?;
            self.manifest = next;
            Ok(())
        })();
        self.poison_on_error(result)
    }

    /// Shards with at least one populated position, from shard zero.
    pub fn shard_ids(&self) -> std::ops::RangeInclusive<u64> {
        let last_position = self.manifest.tree_size.saturating_sub(1);
        0..=last_position / self.layout.shard_positions() as u64
    }

    /// The full padded shard: populated records in order, then zero bytes up
    /// to `layout.shard_bytes()`. Deterministic, so its digest is stable.
    pub fn read_shard_rows(&self, shard_id: u64) -> Result<Vec<u8>, StoreError> {
        self.ensure_healthy()?;
        let shard_positions = self.layout.shard_positions() as u64;
        let shard_start = shard_id
            .checked_mul(shard_positions)
            .ok_or_else(|| StoreError::Invariant("shard position overflow".to_string()))?;
        if shard_start >= self.manifest.tree_size {
            return Err(StoreError::Invariant(format!(
                "shard {shard_id} is outside stored coverage"
            )));
        }
        let available_positions = self
            .manifest
            .tree_size
            .saturating_sub(shard_start)
            .min(shard_positions) as usize;
        let record_bytes = self.layout.record_bytes;
        let mut rows = vec![0u8; self.layout.shard_bytes()];
        let mut file = File::open(&self.records_path)?;
        file.seek(SeekFrom::Start(shard_start * record_bytes as u64))?;
        file.read_exact(&mut rows[..available_positions * record_bytes])?;
        Ok(rows)
    }

    /// Raw bytes of `count` consecutive records from `start`, all of which
    /// must be populated.
    pub fn read_records(&self, start: u64, count: usize) -> Result<Vec<u8>, StoreError> {
        self.ensure_healthy()?;
        let end = start
            .checked_add(count as u64)
            .ok_or_else(|| StoreError::Invariant("record range overflow".to_string()))?;
        if end > self.manifest.tree_size {
            return Err(StoreError::Invariant(format!(
                "records {start}..{end} exceed the journal's {} positions",
                self.manifest.tree_size
            )));
        }
        let record_bytes = self.layout.record_bytes;
        let mut bytes = vec![0u8; count * record_bytes];
        let mut file = File::open(&self.records_path)?;
        file.seek(SeekFrom::Start(start * record_bytes as u64))?;
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    pub fn populated_positions_in_shard(&self, shard_id: u64) -> u64 {
        let shard_positions = self.layout.shard_positions() as u64;
        let start = shard_id.saturating_mul(shard_positions);
        self.manifest
            .tree_size
            .saturating_sub(start)
            .min(shard_positions)
    }

    pub fn rows_digest(rows: &[u8]) -> String {
        hex::encode(Sha256::digest(rows))
    }

    fn manifest_path(&self) -> PathBuf {
        self.dir.join("manifest.json")
    }

    pub(crate) fn ensure_healthy(&self) -> Result<(), StoreError> {
        if self.poisoned {
            return Err(StoreError::Invariant(
                "journal I/O failed; drop and reopen before use".into(),
            ));
        }
        Ok(())
    }

    fn poison_on_error(&mut self, result: Result<(), StoreError>) -> Result<(), StoreError> {
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    // Per-instance failure injection exercises persistence boundaries without global state.
    fn checkpoint(&self, _stage: &'static str) -> Result<(), StoreError> {
        #[cfg(test)]
        if self.fail_after == Some(_stage) {
            return Err(std::io::Error::other(format!("injected after {_stage}")).into());
        }
        Ok(())
    }

    fn persist_manifest(&self, manifest: &StoreManifest) -> Result<(), StoreError> {
        let path = self.manifest_path();
        let temporary = self.dir.join("manifest.json.tmp");
        let bytes = serde_json::to_vec_pretty(manifest)?;
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        self.checkpoint("manifest_write")?;
        file.sync_all()?;
        self.checkpoint("manifest_sync")?;
        fs::rename(temporary, path)?;
        self.checkpoint("manifest_rename")?;
        File::open(&self.dir)?.sync_all()?;
        self.checkpoint("directory_sync")?;
        Ok(())
    }
}

/// Read-only view of a committed journal, taken without its writer lock.
///
/// A live writer replaces the manifest atomically and only extends records
/// after the committed length, so the parsed manifest describes a stable
/// prefix of `records.bin` unless the writer later rewinds below it. Readers
/// must still compare what they read against an independent source.
pub struct JournalSnapshot {
    layout: DatabaseLayout,
    manifest: StoreManifest,
    manifest_bytes: Vec<u8>,
    records: File,
}

impl JournalSnapshot {
    pub fn read(
        path: impl AsRef<Path>,
        table: DatabaseId,
        layout: DatabaseLayout,
    ) -> Result<Self, StoreError> {
        let dir = path.as_ref();
        let manifest_bytes = fs::read(dir.join("manifest.json"))?;
        let manifest: StoreManifest = serde_json::from_slice(&manifest_bytes)?;
        if manifest.version != STORE_VERSION
            || manifest.table != table.as_str()
            || manifest.record_bytes != layout.record_bytes
            || manifest.records_per_row != layout.records_per_row
        {
            return Err(StoreError::Invariant(format!(
                "incompatible journal version/table: {}/{}",
                manifest.version, manifest.table
            )));
        }
        let mut next_position = manifest.blocks.first().map_or(0, |b| b.first_position);
        if next_position != 0 {
            return Err(StoreError::Invariant(
                "journal does not start at position zero".into(),
            ));
        }
        for pair in manifest.blocks.windows(2) {
            if pair[0].height.checked_add(1) != Some(pair[1].height) {
                return Err(StoreError::Invariant(format!(
                    "block height {} does not follow {}",
                    pair[1].height, pair[0].height
                )));
            }
        }
        for block in &manifest.blocks {
            if block.first_position != next_position {
                return Err(StoreError::Invariant(format!(
                    "block {} starts at {}, expected {next_position}",
                    block.height, block.first_position
                )));
            }
            next_position = next_position
                .checked_add(block.action_count)
                .ok_or_else(|| StoreError::Invariant("tree size overflow".into()))?;
        }
        if next_position != manifest.tree_size {
            return Err(StoreError::Invariant(format!(
                "blocks cover {next_position} positions, manifest declares {}",
                manifest.tree_size
            )));
        }
        let records = File::open(dir.join("records.bin"))?;
        let committed = manifest
            .tree_size
            .checked_mul(layout.record_bytes as u64)
            .ok_or_else(|| StoreError::Invariant("record length overflow".into()))?;
        if records.metadata()?.len() < committed {
            return Err(StoreError::Invariant(
                "records file is shorter than committed manifest".into(),
            ));
        }
        Ok(Self {
            layout,
            manifest,
            manifest_bytes,
            records,
        })
    }

    pub fn tree_size(&self) -> u64 {
        self.manifest.tree_size
    }

    pub fn blocks(&self) -> &[BlockEntry] {
        &self.manifest.blocks
    }

    /// The manifest bytes this snapshot was parsed from.
    pub fn manifest_bytes(&self) -> &[u8] {
        &self.manifest_bytes
    }

    /// Bytes of the `records.bin` file beyond this snapshot's committed length.
    pub fn uncommitted_bytes(&self) -> Result<u64, StoreError> {
        Ok(self.records.metadata()?.len() - self.tree_size() * self.layout.record_bytes as u64)
    }

    /// Raw bytes of `count` committed records from `start`.
    pub fn read_records(&self, start: u64, count: usize) -> Result<Vec<u8>, StoreError> {
        use std::os::unix::fs::FileExt;
        let end = start
            .checked_add(count as u64)
            .filter(|end| *end <= self.manifest.tree_size)
            .ok_or_else(|| {
                StoreError::Invariant(format!(
                    "records {start}+{count} exceed the snapshot's {} positions",
                    self.manifest.tree_size
                ))
            })?;
        let record_bytes = self.layout.record_bytes as u64;
        let mut bytes = vec![0u8; ((end - start) * record_bytes) as usize];
        self.records
            .read_exact_at(&mut bytes, start * record_bytes)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EnhanceRecord, ENHANCE_LAYOUT, RECORD_BYTES};

    fn record(byte: u8) -> EnhanceRecord {
        {
            let mut bytes = [byte; RECORD_BYTES];
            bytes[enhance_pir::types::RECORD_FLAGS_OFFSET..].fill(0);
            EnhanceRecord::from_bytes(bytes).unwrap()
        }
    }

    fn open(dir: &Path) -> RecordJournal {
        RecordJournal::open(dir, DatabaseId::Enhance, ENHANCE_LAYOUT).expect("open")
    }

    #[test]
    fn append_failures_reopen_to_a_complete_committed_prefix() {
        for stage in [
            "record_write",
            "record_sync",
            "manifest_write",
            "manifest_sync",
            "manifest_rename",
            "directory_sync",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(dir.path());
            store.append_block(10, "aa".into(), &[record(1)]).unwrap();
            store.fail_after = Some(stage);
            assert!(store
                .append_block(11, "bb".into(), &[record(2), record(3)])
                .is_err());
            assert!(store.read_records(0, 1).is_err());
            assert!(store
                .append_block(11, "retry".into(), &[record(4)])
                .is_err());
            assert!(store.rewind_to_height(None).is_err());
            drop(store);
            let mut recovered = open(dir.path());
            let committed = matches!(stage, "manifest_rename" | "directory_sync");
            assert_eq!(
                recovered.tree_size(),
                if committed { 3 } else { 1 },
                "{stage}"
            );
            assert_eq!(recovered.read_records(0, 1).unwrap(), record(1).as_bytes());
            if committed {
                assert_eq!(recovered.read_records(2, 1).unwrap(), record(3).as_bytes());
            } else {
                recovered
                    .append_block(11, "replacement".into(), &[record(4)])
                    .unwrap();
                assert_eq!(recovered.read_records(1, 1).unwrap(), record(4).as_bytes());
            }
            assert_eq!(
                fs::metadata(dir.path().join("records.bin")).unwrap().len(),
                recovered.tree_size() * RECORD_BYTES as u64
            );
        }
    }

    #[test]
    fn rewind_failures_never_remove_committed_records() {
        for stage in [
            "manifest_write",
            "manifest_sync",
            "manifest_rename",
            "directory_sync",
            "truncate",
            "truncate_sync",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(dir.path());
            store.append_block(10, "aa".into(), &[record(1)]).unwrap();
            store.append_block(11, "bb".into(), &[record(2)]).unwrap();
            store.fail_after = Some(stage);
            assert!(store.rewind_to_height(Some(10)).is_err());
            assert!(store.read_records(0, 1).is_err());
            drop(store);
            let mut recovered = open(dir.path());
            let committed = !matches!(stage, "manifest_write" | "manifest_sync");
            assert_eq!(
                recovered.tree_size(),
                if committed { 1 } else { 2 },
                "{stage}"
            );
            assert_eq!(recovered.read_records(0, 1).unwrap(), record(1).as_bytes());
            recovered.rewind_to_height(Some(10)).unwrap();
            recovered
                .append_block(11, "replacement".into(), &[record(4)])
                .unwrap();
            assert_eq!(recovered.read_records(1, 1).unwrap(), record(4).as_bytes());
        }
    }

    #[test]
    fn ingest_cannot_acknowledge_a_duplicate_after_io_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = crate::ingest::EnhanceJournal {
            records: open(dir.path()),
        };
        let block = crate::zakura::CanonicalBlock {
            height: 10,
            hash: "aa".into(),
            tree_size: 1,
            records: vec![record(1)],
        };
        journal.append_block(&block).unwrap();
        journal.records.fail_after = Some("manifest_write");
        assert!(journal.rewind_to_height(None).is_err());
        assert!(journal.append_block(&block).is_err());
    }

    #[test]
    fn empty_blocks_and_rewind_to_empty_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        store
            .append_block(10, "empty".into(), &[] as &[EnhanceRecord])
            .unwrap();
        store.append_block(11, "aa".into(), &[record(1)]).unwrap();
        store.rewind_to_height(Some(10)).unwrap();
        drop(store);
        let mut store = open(dir.path());
        assert_eq!(store.tree_size(), 0);
        assert_eq!(store.last_block().unwrap().height, 10);
        store.rewind_to_height(None).unwrap();
        drop(store);
        let store = open(dir.path());
        assert!(store.last_block().is_none());
        assert_eq!(store.tree_size(), 0);
    }

    #[test]
    fn append_restart_and_padding_are_deterministic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = open(dir.path());
        store
            .append_block(10, "aa".to_string(), &[record(1), record(2)])
            .expect("append");
        drop(store);

        let store = open(dir.path());
        assert_eq!(store.tree_size(), 2);
        assert_eq!(store.shard_ids(), 0..=0);
        let shard = store.read_shard_rows(0).expect("shard");
        assert_eq!(shard.len(), ENHANCE_LAYOUT.shard_bytes());
        assert_eq!(&shard[..RECORD_BYTES], record(1).as_bytes());
        assert_eq!(&shard[RECORD_BYTES..2 * RECORD_BYTES], record(2).as_bytes());
        assert!(shard[2 * RECORD_BYTES..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn journal_refuses_concurrent_writers() {
        let dir = tempfile::tempdir().unwrap();
        let first = open(dir.path());
        assert!(RecordJournal::open(dir.path(), DatabaseId::Enhance, ENHANCE_LAYOUT).is_err());
        drop(first);
        assert!(RecordJournal::open(dir.path(), DatabaseId::Enhance, ENHANCE_LAYOUT).is_ok());
    }

    #[test]
    fn records_must_match_the_layout() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = open(dir.path());
        let short = vec![0u8; RECORD_BYTES - 1];
        assert!(store.append_block(10, "aa".to_string(), &[short]).is_err());
        assert_eq!(store.tree_size(), 0);
    }

    #[test]
    fn incompatible_journals_are_rejected_without_mutation() {
        for version in [1, 3, 5] {
            let dir = tempfile::tempdir().unwrap();
            let manifest = serde_json::to_vec(&serde_json::json!({
                "version": version, "table": "enhance", "tree_size": 2,
                "blocks": [{"height": 5, "hash": "aa", "first_position": 0, "action_count": 2}]
            }))
            .unwrap();
            let records = vec![7u8; 2 * 725];
            std::fs::write(dir.path().join("manifest.json"), &manifest).unwrap();
            std::fs::write(dir.path().join("records.bin"), &records).unwrap();
            assert!(RecordJournal::open(dir.path(), DatabaseId::Enhance, ENHANCE_LAYOUT).is_err());
            assert_eq!(
                std::fs::read(dir.path().join("manifest.json")).unwrap(),
                manifest
            );
            assert_eq!(
                std::fs::read(dir.path().join("records.bin")).unwrap(),
                records
            );
        }
    }

    #[test]
    fn rewind_truncates_records_and_allows_replacement_blocks() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = open(dir.path());
        store.append_block(10, "aa".into(), &[record(1)]).unwrap();
        store
            .append_block(11, "bb".into(), &[record(2), record(3)])
            .unwrap();
        store.rewind_to_height(Some(10)).unwrap();
        assert_eq!(store.tree_size(), 1);
        assert_eq!(store.last_block().unwrap().hash, "aa");
        store.append_block(11, "cc".into(), &[record(4)]).unwrap();
        assert_eq!(store.tree_size(), 2);
        assert_eq!(
            store.read_records(1, 1).unwrap(),
            record(4).as_bytes().to_vec()
        );
    }
}
