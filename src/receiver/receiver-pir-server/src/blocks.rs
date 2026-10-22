//! Bounded rolling hints, independent of PIR session preparation and grace periods.
use axum::http::StatusCode;
use receiver_directory::{
    blocks::{Batch, BlockFilter, Head, MAX_BATCH_BYTES, MAX_BLOCKS},
    Hash,
};

/// The most blocks one rolling window holds: seven days at NU7's 25-second target
/// spacing, the shortest.
const MAX_WINDOW_BLOCKS: usize = 7 * 24 * 3600 / 25;

/// Maximum encoded material retained by one rolling window.
const MAX_WINDOW_BYTES: usize = 64 * 1024 * 1024;

/// One immutable consecutive suffix. Only the current window is retained.
pub struct BlockWindow {
    head: Head,
    filters: Vec<BlockFilter>,
    /// Each filter's encoded size as a batch of one, measured once here.
    sizes: Vec<usize>,
}

impl BlockWindow {
    /// Checks linkage and total resource bounds before serving trusted indexer output.
    pub fn new(
        genesis: Hash,
        revision: u64,
        filters: Vec<BlockFilter>,
    ) -> Result<Self, receiver_directory::Error> {
        use receiver_directory::Error;
        if filters.is_empty() || filters.len() > MAX_WINDOW_BLOCKS {
            return Err(Error::Capacity);
        }
        let first = filters[0].block().height;
        let mut bytes = 0usize;
        let mut sizes = Vec::with_capacity(filters.len());
        for (index, filter) in filters.iter().enumerate() {
            if filter.block().height != first.checked_add(index as u32).ok_or(Error::Capacity)?
                || (index > 0 && filter.parent() != filters[index - 1].block().hash)
            {
                return Err(Error::Coverage);
            }
            let size = Batch::new(genesis, revision, vec![filter.clone()])?
                .encode()?
                .len();
            sizes.push(size);
            bytes += size;
            if bytes > MAX_WINDOW_BYTES {
                return Err(Error::Capacity);
            }
        }
        let head = Head {
            genesis,
            revision,
            first,
            tip: filters.last().unwrap().block(),
        };
        head.validate(&genesis)?;
        Ok(Self {
            head,
            filters,
            sizes,
        })
    }

    /// Availability always describes this exact immutable suffix.
    pub fn head(&self) -> &Head {
        &self.head
    }

    /// Returns a nonempty bounded prefix, or refuses stale/pruned requests explicitly.
    pub fn batch(&self, revision: u64, from: u32, limit: u32) -> Result<Vec<u8>, StatusCode> {
        if limit == 0 || limit > MAX_BLOCKS {
            return Err(StatusCode::BAD_REQUEST);
        }
        if revision != self.head.revision || from < self.head.first {
            return Err(StatusCode::GONE);
        }
        if from > self.head.tip.height {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        let start = (from - self.head.first) as usize;
        let end = (start + limit as usize).min(self.filters.len());
        // Single-block encodings include a header, so their sum conservatively bounds the
        // batch, which is the only encoding a request makes.
        let mut size = 0;
        let count = self.sizes[start..end]
            .iter()
            .take_while(|s| {
                size += **s;
                size <= MAX_BATCH_BYTES
            })
            .count();
        let chosen = self.filters[start..start + count].to_vec();
        Batch::new(self.head.genesis, revision, chosen)
            .and_then(|b| b.encode())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}
