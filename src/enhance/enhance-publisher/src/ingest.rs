//! Append finalized Ironwood enhancement records to the active journal.

use crate::chain_source::CanonicalBlock;
use crate::journal::{JournalError, RecordJournal};
use crate::types::{DatabaseId, ENHANCE_LAYOUT};
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("journal error: {0}")]
    Store(#[from] JournalError),
    #[error("ingest invariant violated: {0}")]
    Invariant(String),
}

/// The coordinator's durable Enhance record journal.
///
/// The inner record store is private so every append and rewind goes through
/// the continuity checks here: a block's height must advance and its records
/// must extend the journal to the block's tree size. Readers that publish or
/// inspect the journal borrow it read-only through [`EnhanceJournal::records`].
pub struct EnhanceJournal {
    records: RecordJournal,
}

impl EnhanceJournal {
    pub fn open(data_dir: &Path) -> Result<Self, IngestError> {
        Ok(Self {
            records: RecordJournal::open(
                data_dir.join(DatabaseId::Enhance.as_str()),
                DatabaseId::Enhance,
                ENHANCE_LAYOUT,
            )?,
        })
    }

    /// Read-only view of the committed records, for publication and inspection.
    pub fn records(&self) -> &RecordJournal {
        &self.records
    }

    pub fn tree_size(&self) -> u64 {
        self.records.tree_size()
    }

    pub fn committed_height(&self) -> Option<u64> {
        self.records.last_block().map(|block| block.height)
    }

    pub fn highest_committed(&self) -> Option<(u64, String)> {
        self.records
            .last_block()
            .map(|block| (block.height, block.hash.clone()))
    }

    pub fn rewind_to_height(&mut self, height: Option<u64>) -> Result<(), IngestError> {
        self.records.rewind_to_height(height)?;
        Ok(())
    }

    pub fn append_block(&mut self, block: &CanonicalBlock) -> Result<(), IngestError> {
        self.records.ensure_healthy()?;
        if self
            .records
            .last_block()
            .is_some_and(|last| last.height >= block.height)
        {
            return Ok(());
        }
        let first_position = block
            .tree_size
            .checked_sub(block.records.len() as u64)
            .ok_or_else(|| IngestError::Invariant("block exceeds tree size".to_string()))?;
        if self.records.tree_size() < first_position || self.records.tree_size() > block.tree_size {
            return Err(IngestError::Invariant(format!(
                "journal continuity mismatch at height {}: journal {}, block {}..{}",
                block.height,
                self.records.tree_size(),
                first_position,
                block.tree_size
            )));
        }
        let skip = (self.records.tree_size() - first_position) as usize;
        self.records
            .append_block(block.height, block.hash.clone(), &block.records[skip..])?;
        if self.records.tree_size() != block.tree_size {
            return Err(IngestError::Invariant(format!(
                "Ironwood tree size mismatch at height {}: journal {}, Zakura {}",
                block.height,
                self.records.tree_size(),
                block.tree_size
            )));
        }
        Ok(())
    }

    /// Appends synthetic fixture records as the next block.
    ///
    /// The block takes the next height (the activation height for an empty
    /// journal) and a hash derived from that height, so fixture journals keep
    /// the same layout as before. Returns the appended height.
    pub fn append_fixture_block<R: AsRef<[u8]>>(
        &mut self,
        records: &[R],
    ) -> Result<u64, IngestError> {
        self.records.ensure_healthy()?;
        let height = self
            .committed_height()
            .map_or(enhance_material::ACTIVATION_HEIGHT, |h| h + 1);
        self.records
            .append_block(height, format!("{height:064x}"), records)?;
        Ok(height)
    }
}

#[cfg(test)]
impl EnhanceJournal {
    fn fail_after_for_test(&mut self, stage: &'static str) {
        self.records.fail_after(stage);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enhance_material::EnhanceRecord;

    #[test]
    fn ingest_cannot_acknowledge_a_duplicate_after_io_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = EnhanceJournal::open(dir.path()).unwrap();
        let block = CanonicalBlock {
            height: 10,
            hash: "aa".into(),
            tree_size: 1,
            records: vec![{
                let mut bytes = [1; enhance_material::RECORD_BYTES];
                bytes[enhance_material::RECORD_FLAGS_OFFSET..].fill(0);
                EnhanceRecord::from_bytes(bytes).unwrap()
            }],
        };
        journal.append_block(&block).unwrap();
        journal.fail_after_for_test("manifest_write");
        assert!(journal.rewind_to_height(None).is_err());
        assert!(journal.append_block(&block).is_err());
    }

    #[test]
    fn appends_only_the_active_enhancement_journal() {
        let directory = tempfile::tempdir().unwrap();
        let mut journal = EnhanceJournal::open(directory.path()).unwrap();
        let block = CanonicalBlock {
            height: 3_428_143,
            hash: "01".repeat(32),
            records: vec![EnhanceRecord::from_bytes([0; enhance_material::RECORD_BYTES]).unwrap()],
            tree_size: 1,
        };
        journal.append_block(&block).unwrap();
        assert_eq!(journal.tree_size(), 1);
        assert_eq!(journal.committed_height(), Some(3_428_143));
        assert!(directory.path().join("enhance/records.bin").exists());
        assert!(!directory.path().join("action").exists());
    }

    #[test]
    fn fixture_blocks_advance_height_through_the_journal() {
        let directory = tempfile::tempdir().unwrap();
        let mut journal = EnhanceJournal::open(directory.path()).unwrap();
        let records = vec![vec![0u8; enhance_material::RECORD_BYTES]; 2];
        assert_eq!(
            journal.append_fixture_block(&records).unwrap(),
            enhance_material::ACTIVATION_HEIGHT
        );
        assert_eq!(
            journal.append_fixture_block(&records[..1]).unwrap(),
            enhance_material::ACTIVATION_HEIGHT + 1
        );
        assert_eq!(journal.tree_size(), 3);
        let (height, hash) = journal.highest_committed().unwrap();
        assert_eq!(hash, format!("{height:064x}"));

        journal.fail_after_for_test("manifest_write");
        assert!(journal.append_fixture_block(&records).is_err());
        assert!(journal.append_fixture_block(&records).is_err());
        assert_eq!(journal.tree_size(), 3);
    }
}
