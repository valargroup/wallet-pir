//! Read a committed prefix without acquiring the journal writer lock.
use super::manifest::{JournalManifest, STORE_VERSION};
use super::{JournalBlock, JournalError};
use crate::types::{DatabaseId, DatabaseLayout};
use std::{
    fs::{self, File},
    path::Path,
};

/// Read-only view of a committed journal, taken without its writer lock.
///
/// A live writer replaces the manifest atomically and only extends records
/// after the committed length, so the parsed manifest describes a stable
/// prefix of `records.bin` unless the writer later rewinds below it. Readers
/// must still compare what they read against an independent source.
pub struct CommittedJournalSnapshot {
    layout: DatabaseLayout,
    manifest: JournalManifest,
    manifest_bytes: Vec<u8>,
    records: File,
}

impl CommittedJournalSnapshot {
    /// Validate the manifest's layout, contiguous blocks, and committed file
    /// length without truncating an uncommitted tail or taking the writer lock.
    /// A later rewind can invalidate reads and must be fenced by the caller.
    pub fn read(
        path: impl AsRef<Path>,
        table: DatabaseId,
        layout: DatabaseLayout,
    ) -> Result<Self, JournalError> {
        let dir = path.as_ref();
        let manifest_bytes = fs::read(dir.join("manifest.json"))?;
        let manifest: JournalManifest = serde_json::from_slice(&manifest_bytes)?;
        if manifest.version != STORE_VERSION
            || manifest.table != table.as_str()
            || manifest.record_bytes != layout.record_bytes
            || manifest.records_per_row != layout.records_per_row
        {
            return Err(JournalError::Invariant(format!(
                "incompatible journal version/table: {}/{}",
                manifest.version, manifest.table
            )));
        }
        let mut next_position = manifest.blocks.first().map_or(0, |b| b.first_position);
        if next_position != 0 {
            return Err(JournalError::Invariant(
                "journal does not start at position zero".into(),
            ));
        }
        for pair in manifest.blocks.windows(2) {
            if pair[0].height.checked_add(1) != Some(pair[1].height) {
                return Err(JournalError::Invariant(format!(
                    "block height {} does not follow {}",
                    pair[1].height, pair[0].height
                )));
            }
        }
        for block in &manifest.blocks {
            if block.first_position != next_position {
                return Err(JournalError::Invariant(format!(
                    "block {} starts at {}, expected {next_position}",
                    block.height, block.first_position
                )));
            }
            next_position = next_position
                .checked_add(block.action_count)
                .ok_or_else(|| JournalError::Invariant("tree size overflow".into()))?;
        }
        if next_position != manifest.tree_size {
            return Err(JournalError::Invariant(format!(
                "blocks cover {next_position} positions, manifest declares {}",
                manifest.tree_size
            )));
        }
        let records = File::open(dir.join("records.bin"))?;
        let committed = manifest
            .tree_size
            .checked_mul(layout.record_bytes as u64)
            .ok_or_else(|| JournalError::Invariant("record length overflow".into()))?;
        if records.metadata()?.len() < committed {
            return Err(JournalError::Invariant(
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

    pub fn blocks(&self) -> &[JournalBlock] {
        &self.manifest.blocks
    }

    /// The manifest bytes this snapshot was parsed from.
    pub fn manifest_bytes(&self) -> &[u8] {
        &self.manifest_bytes
    }

    /// Bytes of the `records.bin` file beyond this snapshot's committed length.
    pub fn uncommitted_bytes(&self) -> Result<u64, JournalError> {
        Ok(self.records.metadata()?.len() - self.tree_size() * self.layout.record_bytes as u64)
    }

    /// Fills `buffer`, a whole number of records, with committed records from `start`.
    pub fn read_records_into(&self, start: u64, buffer: &mut [u8]) -> Result<(), JournalError> {
        use std::os::unix::fs::FileExt;
        let record_bytes = self.layout.record_bytes;
        if !buffer.len().is_multiple_of(record_bytes) {
            return Err(JournalError::Invariant("partial record buffer".into()));
        }
        let count = (buffer.len() / record_bytes) as u64;
        if start
            .checked_add(count)
            .is_none_or(|end| end > self.manifest.tree_size)
        {
            return Err(JournalError::Invariant(format!(
                "records {start}+{count} exceed the snapshot's {} positions",
                self.manifest.tree_size
            )));
        }
        self.records
            .read_exact_at(buffer, start * record_bytes as u64)?;
        Ok(())
    }

    /// Raw bytes of `count` committed records from `start`.
    pub fn read_records(&self, start: u64, count: usize) -> Result<Vec<u8>, JournalError> {
        use std::os::unix::fs::FileExt;
        let end = start
            .checked_add(count as u64)
            .filter(|end| *end <= self.manifest.tree_size)
            .ok_or_else(|| {
                JournalError::Invariant(format!(
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
