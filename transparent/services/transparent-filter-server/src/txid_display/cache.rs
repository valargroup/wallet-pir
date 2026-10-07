//! The unsealed range's records, in memory.
//!
//! The recent shard is rebuilt on every block, and re-reading sidecars for it
//! would read each one twice per rebuild (`display_at` validates against the
//! event journal). The controller instead keeps `[S, tip]` here: every block
//! not yet inside a published archive. Queued seal ranges always lie inside
//! it too, so archive builds never touch the journal either.

use crate::publication::{BoxError, Journal};
use std::ops::RangeInclusive;
use std::sync::Arc;
use transparent_filter::BlockHash;
use transparent_shard::display::{height_counts, DisplaySealParams, HeightCounts};
use transparent_shard::txid::TransparentDisplayRecord;

/// One block of the cache.
#[derive(Clone, Debug)]
pub struct CachedBlock {
    pub hash: BlockHash,
    pub records: Arc<[TransparentDisplayRecord]>,
    pub observed_ms: u64,
}

/// Contiguous blocks from `start`; possibly empty.
pub struct DisplayCache {
    params: DisplaySealParams,
    start: u64,
    blocks: Vec<CachedBlock>,
    /// Kept beside `blocks` so the seal rule can read a slice without copying.
    counts: Vec<HeightCounts>,
}

impl DisplayCache {
    pub fn new(params: DisplaySealParams, start: u64) -> Self {
        Self {
            params,
            start,
            blocks: Vec::new(),
            counts: Vec::new(),
        }
    }

    /// Reads `[from, through]` from the journal; empty when `through < from`.
    pub fn load(
        store: &impl Journal,
        params: DisplaySealParams,
        from: u64,
        through: u64,
        observed_ms: u64,
    ) -> Result<Self, BoxError> {
        let mut cache = Self::new(params, from);
        for height in from..=through {
            let hash = store
                .block_at(height)
                .ok_or_else(|| format!("journal has no block at {height}"))?
                .block_hash;
            cache.push(height, hash, store.display_at(height)?, observed_ms)?;
        }
        Ok(cache)
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// The next height the cache accepts.
    pub fn next_height(&self) -> u64 {
        self.start + self.blocks.len() as u64
    }

    pub fn tip(&self) -> Option<(u64, BlockHash)> {
        self.blocks.last().map(|b| (self.next_height() - 1, b.hash))
    }

    pub fn block(&self, height: u64) -> Option<&CachedBlock> {
        let index = height.checked_sub(self.start)?;
        self.blocks.get(usize::try_from(index).ok()?)
    }

    pub fn push(
        &mut self,
        height: u64,
        hash: BlockHash,
        records: Vec<TransparentDisplayRecord>,
        observed_ms: u64,
    ) -> Result<(), BoxError> {
        if height != self.next_height() {
            return Err(format!(
                "display cache expects height {}, got {height}",
                self.next_height()
            )
            .into());
        }
        self.counts.push(height_counts(&self.params, &records));
        self.blocks.push(CachedBlock {
            hash,
            records: records.into(),
            observed_ms,
        });
        Ok(())
    }

    /// Keeps heights at or below `height`: a rollback to that ancestor.
    pub fn truncate_after(&mut self, height: u64) {
        let keep = (height + 1).saturating_sub(self.start) as usize;
        self.blocks.truncate(keep);
        self.counts.truncate(keep);
    }

    /// Forgets heights at or below `height`, which a published archive now holds.
    pub fn drop_through(&mut self, height: u64) {
        if height < self.start {
            return;
        }
        let drop = ((height + 1 - self.start) as usize).min(self.blocks.len());
        self.blocks.drain(..drop);
        self.counts.drain(..drop);
        self.start = height + 1;
    }

    /// Records from the start through the tip.
    pub fn record_count(&self) -> u64 {
        self.blocks.iter().map(|b| b.records.len() as u64).sum()
    }

    /// Seal-rule counts from `height` to the tip.
    pub fn counts_from(&self, height: u64) -> &[HeightCounts] {
        let from = (height.saturating_sub(self.start) as usize).min(self.counts.len());
        &self.counts[from..]
    }

    /// Cheap handles on the records of `range`, for a build off this thread.
    pub fn records(&self, range: RangeInclusive<u64>) -> Vec<Arc<[TransparentDisplayRecord]>> {
        range
            .filter_map(|height| self.block(height).map(|b| b.records.clone()))
            .collect()
    }

    /// Records in `range` per archive bucket.
    pub fn archive_counts(&self, range: RangeInclusive<u64>) -> Vec<u64> {
        let mut sums = vec![0u64; self.params.n_archive as usize];
        for height in range {
            if let Some(counts) = height
                .checked_sub(self.start)
                .and_then(|i| self.counts.get(i as usize))
            {
                for (sum, count) in sums.iter_mut().zip(&counts.archive) {
                    *sum += u64::from(*count);
                }
            }
        }
        sums
    }
}

/// Every record of `parts`, in order, as one slice for the table builder.
pub fn flatten(parts: &[Arc<[TransparentDisplayRecord]>]) -> Vec<TransparentDisplayRecord> {
    parts.iter().flat_map(|p| p.iter().cloned()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txid_display::fixture::record;

    #[test]
    fn push_truncate_and_drop_keep_heights_contiguous() {
        let params = DisplaySealParams::default();
        let mut cache = DisplayCache::new(params, 10);
        assert!(cache.tip().is_none());
        for h in 10..15 {
            let hash = BlockHash::from_internal_bytes([h as u8; 32]);
            cache.push(h, hash, vec![record(h as u32, 20)], h).unwrap();
        }
        assert!(cache
            .push(20, BlockHash::from_internal_bytes([0; 32]), vec![], 0)
            .is_err());
        assert_eq!(cache.tip().unwrap().0, 14);
        assert_eq!(cache.counts_from(12).len(), 3);
        assert_eq!(cache.records(11..=13).len(), 3);
        assert_eq!(cache.archive_counts(10..=14), vec![5]);
        assert_eq!(cache.record_count(), 5);
        cache.truncate_after(12);
        assert_eq!(cache.tip().unwrap().0, 12);
        cache.drop_through(10);
        assert_eq!((cache.start(), cache.len()), (11, 2));
        assert_eq!(cache.block(11).unwrap().observed_ms, 11);
        cache.drop_through(30);
        assert!(cache.is_empty());
        assert_eq!(cache.next_height(), 31);
        assert_eq!(flatten(&[]).len(), 0);
    }
}
