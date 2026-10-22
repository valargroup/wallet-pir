//! Rolling hints rebuilt from the existing chain index. Payment history is never pruned.
use crate::{Error, Store};
use receiver_directory::blocks::{BlockFilter, BlockId, MAX_RECEIVERS};

/// The most blocks a window may retain: seven days at NU7's 25-second target spacing,
/// the shortest. The indexer sizes the window by target time, which is fewer blocks
/// wherever earlier 75-second blocks fall inside it.
pub const MAX_FILTER_BLOCKS: u32 = 7 * 24 * 3600 / 25;

/// The most encoded hint bytes a window holds; older blocks beyond it are left out.
pub const MAX_FILTER_BYTES: usize = 64 * 1024 * 1024;

impl Store {
    /// [`Self::block_filters_within`] [`MAX_FILTER_BYTES`].
    pub fn block_filters(&mut self, retain: u32, revision: u64) -> Result<Vec<BlockFilter>, Error> {
        self.block_filters_within(retain, revision, MAX_FILTER_BYTES)
    }

    /// Reads a consecutive suffix of at most `retain` blocks, including empty blocks, in
    /// one SQLite snapshot, stopping at the oldest block that would take its encoding past
    /// `max_bytes`. The correction revision must not decrease across restarts. Reorgs use
    /// hashes.
    pub fn block_filters_within(
        &mut self,
        retain: u32,
        revision: u64,
        max_bytes: usize,
    ) -> Result<Vec<BlockFilter>, Error> {
        if retain == 0 || retain > MAX_FILTER_BLOCKS || revision > i64::MAX as u64 {
            return Err(Error::Malformed);
        }
        let tx = self.db.transaction()?;
        let previous: u64 =
            tx.query_row("SELECT revision FROM filter_revision", [], |r| r.get(0))?;
        if revision < previous {
            return Err(Error::Coverage);
        }
        tx.execute("UPDATE filter_revision SET revision = ?1", [revision])?;
        let tip = super::tip(&tx, &self.config)?;
        let first = tip
            .height
            .saturating_sub(retain - 1)
            .max(self.config.start_height);
        let mut filters = Vec::new();
        let mut bytes = 0usize;
        // Bound a pathological block before collecting its receivers in memory.
        let mut query =
            tx.prepare("SELECT DISTINCT receiver FROM payments WHERE height = ?1 LIMIT ?2")?;
        for height in (first..=tip.height).rev() {
            let parent = if height == self.config.start_height {
                self.config.start_parent
            } else {
                tx.query_row(
                    "SELECT hash FROM blocks WHERE height = ?1",
                    [height - 1],
                    |r| r.get(0),
                )?
            };
            let hash =
                tx.query_row("SELECT hash FROM blocks WHERE height = ?1", [height], |r| {
                    r.get(0)
                })?;
            let receivers = query
                .query_map(rusqlite::params![height, MAX_RECEIVERS + 1], |r| {
                    r.get::<_, Vec<u8>>(0)
                })?
                .map(|row| row?.try_into().map_err(|_| Error::Malformed))
                .collect::<Result<Vec<[u8; 43]>, Error>>()?;
            let filter = BlockFilter::build(
                &self.config.genesis,
                BlockId { height, hash },
                parent,
                &receivers,
            )?;
            bytes += receiver_directory::blocks::Batch::new(
                self.config.genesis,
                revision,
                vec![filter.clone()],
            )?
            .encode()?
            .len();
            if bytes > max_bytes {
                break;
            }
            filters.push(filter);
        }
        filters.reverse();
        drop(query);
        tx.commit()?;
        Ok(filters)
    }
}
