//! Validate recovered chain coverage and rebuild the derived height index.
use super::*;

impl Controller {
    /// Check sealed chain terminals before rebuilding derived state. Table
    /// integrity and rows are verified once by required publication preparation.
    pub(super) fn check_sealed(&self) -> Result<(), Halt> {
        let journal_hash = |height: u64| {
            self.store
                .block_at(height)
                .map(|b| b.block_hash.to_display_hex())
        };
        if journal_hash(self.layout.start_height - 1).as_deref() != Some(&self.layout.base_parent) {
            return Err(Halt::SealedReorg {
                ancestor: i128::from(self.layout.start_height) - 2,
                floor: self.layout.start_height - 1,
            });
        }
        for entry in self.map.shards.iter().filter(|s| s.sealed) {
            if journal_hash(entry.end_height).as_deref() != Some(&entry.terminal_block_hash) {
                return Err(Halt::SealedReorg {
                    ancestor: i128::from(entry.end_height) - 1,
                    floor: entry.end_height,
                });
            }
        }
        Ok(())
    }

    /// Brings the tooling index to the cache tip: drops anything above it,
    /// then rewrites the last indexed height (which may be torn) onwards, or
    /// from `rewrite_from` when that is lower, for heights a rollback the
    /// index never saw may have replaced.
    pub(super) fn sync_index(&mut self, rewrite_from: Option<u64>) -> Result<(), BoxError> {
        let tip = self.cache.next_height() - 1;
        self.index.truncate_after(tip)?;
        let from = match self.index.last_height()? {
            Some(last) => {
                let from = rewrite_from.map_or(last, |h| h.min(last));
                self.index.truncate_after(from - 1)?;
                from
            }
            None => self.layout.start_height,
        };
        for height in from..=tip {
            match self.cache.block(height) {
                Some(block) => self.index.append(height, &block.records)?,
                None => self.index.append(height, &self.store.display_at(height)?)?,
            }
        }
        Ok(())
    }
}
