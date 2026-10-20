//! Bounded rolling hints, independent of PIR session preparation and grace periods.
use axum::http::StatusCode;
use receiver_directory::{
    blocks::{Batch, BlockFilter, Head, MAX_BATCH_BYTES, MAX_BLOCKS},
    Hash,
};

/// Maximum encoded material retained by one rolling window.
const MAX_WINDOW_BYTES: usize = 64 * 1024 * 1024;

/// One immutable consecutive suffix. Only the current window is retained.
pub struct BlockWindow {
    head: Head,
    filters: Vec<BlockFilter>,
}

impl BlockWindow {
    /// Checks linkage and total resource bounds before serving trusted indexer output.
    pub fn new(
        genesis: Hash,
        revision: u64,
        filters: Vec<BlockFilter>,
    ) -> Result<Self, receiver_directory::Error> {
        use receiver_directory::Error;
        if filters.is_empty() || filters.len() > 8064 {
            return Err(Error::Capacity);
        }
        let first = filters[0].block().height;
        let mut bytes = 0usize;
        for (index, filter) in filters.iter().enumerate() {
            if filter.block().height != first.checked_add(index as u32).ok_or(Error::Capacity)?
                || (index > 0 && filter.parent() != filters[index - 1].block().hash)
            {
                return Err(Error::Coverage);
            }
            bytes += Batch::new(genesis, revision, vec![filter.clone()])?
                .encode()?
                .len();
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
        Ok(Self { head, filters })
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
        let mut chosen = Vec::new();
        // Single-block encodings include a header, so their sum conservatively bounds the batch.
        let mut size = 0;
        for filter in &self.filters[start..end] {
            size += Batch::new(self.head.genesis, revision, vec![filter.clone()])
                .and_then(|b| b.encode())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .len();
            if size > MAX_BATCH_BYTES {
                break;
            }
            chosen.push(filter.clone());
        }
        Batch::new(self.head.genesis, revision, chosen)
            .and_then(|b| b.encode())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}
