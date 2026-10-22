//! Fixed-width committed records with one writer and read-only prefix snapshots.
mod manifest;
mod snapshot;

use manifest::{JournalManifest, STORE_VERSION};
pub use snapshot::CommittedJournalSnapshot;

use crate::types::{DatabaseId, DatabaseLayout};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("manifest error: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("store invariant violated: {0}")]
    Invariant(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalBlock {
    pub height: u64,
    pub hash: String,
    pub first_position: u64,
    pub action_count: u64,
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
    manifest: JournalManifest,
    poisoned: bool,
    #[cfg(any(test, feature = "fault-injection"))]
    fail_after: Option<&'static str>,
}

impl RecordJournal {
    /// Opens the journal of a served table.
    pub fn open(
        path: impl AsRef<Path>,
        table: DatabaseId,
        layout: DatabaseLayout,
    ) -> Result<Self, JournalError> {
        Self::open_inner(path.as_ref(), table.as_str(), Some(table), layout)
    }

    fn open_inner(
        path: &Path,
        name: &str,
        table: Option<DatabaseId>,
        layout: DatabaseLayout,
    ) -> Result<Self, JournalError> {
        let dir = path.to_path_buf();
        fs::create_dir_all(&dir)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("journal.lock"))?;
        lock.try_lock().map_err(|error| {
            JournalError::Invariant(format!("journal is already open: {error}"))
        })?;
        let records_path = dir.join("records.bin");
        let manifest_path = dir.join("manifest.json");
        let fresh = || JournalManifest {
            version: STORE_VERSION,
            record_bytes: layout.record_bytes,
            records_per_row: layout.records_per_row,
            table: name.to_string(),
            tree_size: 0,
            blocks: Vec::new(),
        };
        let manifest = if manifest_path.exists() {
            let bytes = fs::read(&manifest_path)?;
            let parsed: JournalManifest = serde_json::from_slice(&bytes)?;
            if parsed.version != STORE_VERSION
                || parsed.table != name
                || parsed.record_bytes != layout.record_bytes
                || parsed.records_per_row != layout.records_per_row
            {
                return Err(JournalError::Invariant(format!(
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
            .ok_or_else(|| JournalError::Invariant("record length overflow".to_string()))?;
        let records = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&records_path)?;
        if records.metadata()?.len() < expected_len {
            return Err(JournalError::Invariant(
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
            #[cfg(any(test, feature = "fault-injection"))]
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

    pub(crate) fn snapshot(&self) -> Result<CommittedJournalSnapshot, JournalError> {
        self.ensure_healthy()?;
        CommittedJournalSnapshot::read(
            &self.dir,
            self.table.ok_or_else(|| {
                JournalError::Invariant("publication requires a named table".into())
            })?,
            self.layout,
        )
    }

    pub fn tree_size(&self) -> u64 {
        self.manifest.tree_size
    }

    pub fn last_block(&self) -> Option<&JournalBlock> {
        self.manifest.blocks.last()
    }

    pub fn blocks(&self) -> &[JournalBlock] {
        &self.manifest.blocks
    }

    /// Removes every block above `height`, or all blocks when `height` is `None`.
    /// Commit the smaller manifest before reclaiming records. After an I/O error,
    /// drop and reopen the journal before using it again.
    pub fn rewind_to_height(&mut self, height: Option<u64>) -> Result<(), JournalError> {
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
            .ok_or_else(|| JournalError::Invariant("record length overflow".into()))?;
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

    /// Append one consecutive block and commit its manifest after record bytes
    /// are durable. Invalid input leaves the journal unchanged; a mutation I/O
    /// failure poisons it until reopened, even if a rename already succeeded.
    pub fn append_block<R: AsRef<[u8]>>(
        &mut self,
        height: u64,
        hash: String,
        records: &[R],
    ) -> Result<(), JournalError> {
        self.ensure_healthy()?;
        if let Some(previous) = self.last_block() {
            if previous.height.checked_add(1) != Some(height) {
                return Err(JournalError::Invariant(format!(
                    "block height {height} does not follow {}",
                    previous.height
                )));
            }
        }
        if let Some(bad) = records
            .iter()
            .find(|record| record.as_ref().len() != self.layout.record_bytes)
        {
            return Err(JournalError::Invariant(format!(
                "record has {} bytes, layout needs {}",
                bad.as_ref().len(),
                self.layout.record_bytes
            )));
        }

        let first_position = self.manifest.tree_size;
        let offset = first_position
            .checked_mul(self.layout.record_bytes as u64)
            .ok_or_else(|| JournalError::Invariant("record length overflow".into()))?;
        let mut next = self.manifest.clone();
        next.tree_size = first_position
            .checked_add(records.len() as u64)
            .ok_or_else(|| JournalError::Invariant("tree size overflow".into()))?;
        next.tree_size
            .checked_mul(self.layout.record_bytes as u64)
            .ok_or_else(|| JournalError::Invariant("record length overflow".into()))?;
        next.blocks.push(JournalBlock {
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
    pub fn read_shard_rows(&self, shard_id: u64) -> Result<Vec<u8>, JournalError> {
        self.ensure_healthy()?;
        let shard_positions = self.layout.shard_positions() as u64;
        let shard_start = shard_id
            .checked_mul(shard_positions)
            .ok_or_else(|| JournalError::Invariant("shard position overflow".to_string()))?;
        if shard_start >= self.manifest.tree_size {
            return Err(JournalError::Invariant(format!(
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
    pub fn read_records(&self, start: u64, count: usize) -> Result<Vec<u8>, JournalError> {
        self.ensure_healthy()?;
        let end = start
            .checked_add(count as u64)
            .ok_or_else(|| JournalError::Invariant("record range overflow".to_string()))?;
        if end > self.manifest.tree_size {
            return Err(JournalError::Invariant(format!(
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

    fn manifest_path(&self) -> PathBuf {
        self.dir.join("manifest.json")
    }

    pub fn ensure_healthy(&self) -> Result<(), JournalError> {
        if self.poisoned {
            return Err(JournalError::Invariant(
                "journal I/O failed; drop and reopen before use".into(),
            ));
        }
        Ok(())
    }

    fn poison_on_error(&mut self, result: Result<(), JournalError>) -> Result<(), JournalError> {
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    /// Fails the next persistence step named `stage`, for callers' recovery tests.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn fail_after(&mut self, stage: &'static str) {
        self.fail_after = Some(stage);
    }

    // Per-instance failure injection exercises persistence boundaries without global state.
    fn checkpoint(&self, _stage: &'static str) -> Result<(), JournalError> {
        #[cfg(any(test, feature = "fault-injection"))]
        if self.fail_after == Some(_stage) {
            return Err(std::io::Error::other(format!("injected after {_stage}")).into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
