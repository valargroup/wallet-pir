//! Bounded block hints, independent of PIR session preparation and retention.
use axum::http::StatusCode;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use zakura_pir_query_server::receiver::{
    blocks::{BlockId, Head, Range, MAX_RANGE_BLOCKS, MAX_RANGE_BYTES, RANGE_HEADER},
    Error, Hash,
};

/// The most blocks one window holds: seven days at NU7's 25-second target spacing,
/// the shortest.
pub(crate) const MAX_WINDOW_BLOCKS: usize = 7 * 24 * 3600 / 25;

/// Maximum encoded material retained by one window.
const MAX_WINDOW_BYTES: usize = 64 * 1024 * 1024;

/// One immutable consecutive suffix of block hints. Only the current window is retained.
pub struct BlockWindow {
    head: Head,
    /// The hash before `head.first`.
    parent: Hash,
    /// Every block's hash from `head.first` through the tip.
    hashes: Vec<Hash>,
    /// The blocks that pay a receiver, ascending, each with its encoded set.
    sets: Vec<Set>,
    /// Cleared when this window is revoked or invalidated, so a response built from it
    /// meanwhile is refused.
    authorized: Arc<AtomicBool>,
}

/// A block's height and encoded set, which responses splice in without decoding.
pub type Set = (u32, Vec<u8>);

/// One response's extent: heights `start..=end` and the sets `sets` it carries.
struct Page {
    start: u32,
    end: u32,
    sets: std::ops::Range<usize>,
}

/// The bytes a CompactSize encoding of `value` takes, as a [`Range`] encodes height gaps.
fn compact_size(value: u32) -> usize {
    match value {
        0..=252 => 1,
        253..=0xffff => 3,
        _ => 5,
    }
}

impl BlockWindow {
    /// Checks ordering and total resource bounds before serving trusted indexer output:
    /// `hashes` from `first`, after `parent`, and `sets` ascending within them.
    pub fn new(
        genesis: Hash,
        revision: u64,
        first: u32,
        parent: Hash,
        hashes: Vec<Hash>,
        sets: Vec<Set>,
    ) -> Result<Self, Error> {
        if hashes.is_empty() || hashes.len() > MAX_WINDOW_BLOCKS {
            return Err(Error::Capacity);
        }
        // The window's length is bounded above, so its last height fits a u32.
        let tip = first
            .checked_add(hashes.len() as u32 - 1)
            .ok_or(Error::Capacity)?;
        let mut bytes = 0usize;
        let mut previous = None;
        for (height, set) in &sets {
            if *height < first || *height > tip || previous >= Some(*height) {
                return Err(Error::Coverage);
            }
            previous = Some(*height);
            bytes += set.len();
            if bytes > MAX_WINDOW_BYTES {
                return Err(Error::Capacity);
            }
        }
        let head = Head {
            genesis,
            revision,
            first,
            tip: BlockId {
                height: tip,
                hash: *hashes.last().expect("nonempty"),
            },
        };
        head.validate(&genesis)?;
        Ok(Self {
            head,
            parent,
            hashes,
            sets,
            authorized: Arc::new(AtomicBool::new(true)),
        })
    }

    /// Availability always describes this exact immutable suffix.
    pub fn head(&self) -> &Head {
        &self.head
    }

    /// The hash before the window's first block.
    pub fn parent(&self) -> Hash {
        self.parent
    }

    /// Every block's hash from the window's first block through its tip.
    pub fn hashes(&self) -> &[Hash] {
        &self.hashes
    }

    /// Whether the window still serves; see [`Self::revoke`].
    pub fn authorized(&self) -> bool {
        self.authorized.load(Ordering::Acquire)
    }

    /// Refuses responses still being built from this window.
    pub fn revoke(&self) {
        self.authorized.store(false, Ordering::Release);
    }

    /// The hash at `height`, which is within the window or the block before it.
    fn hash(&self, height: u32) -> Hash {
        match height.checked_sub(self.head.first) {
            Some(index) => self.hashes[index as usize],
            None => self.parent,
        }
    }

    /// The response starting at `start`, ending at `last` or earlier where the next set
    /// would take it past [`MAX_RANGE_BYTES`].
    fn page(&self, start: u32, last: u32) -> Page {
        let from = self.sets.partition_point(|(height, _)| *height < start);
        let (mut bytes, mut next, mut end) = (RANGE_HEADER, start, last);
        let mut to = from;
        for (height, set) in &self.sets[from..] {
            if *height > last {
                break;
            }
            let entry = compact_size(height - next) + set.len();
            if bytes + entry > MAX_RANGE_BYTES {
                end = height - 1;
                break;
            }
            bytes += entry;
            next = height + 1;
            to += 1;
        }
        Page {
            start,
            end,
            sets: from..to,
        }
    }

    /// The last height a response from `from` of at most `limit` heights may reach.
    fn last(&self, from: u32, limit: u32) -> u32 {
        from.saturating_add(limit - 1).min(self.head.tip.height)
    }

    /// Returns a nonempty prefix of at most `limit` heights from `from` as one [`Range`]
    /// within [`MAX_RANGE_BYTES`], refusing a stale revision or pruned height as gone, a
    /// height past the tip as not yet available and an invalid limit as a bad request.
    pub fn range(&self, revision: u64, from: u32, limit: u32) -> Result<Vec<u8>, StatusCode> {
        if limit == 0 || limit > MAX_RANGE_BLOCKS {
            return Err(StatusCode::BAD_REQUEST);
        }
        self.check(revision, from)?;
        let page = self.page(from, self.last(from, limit));
        if page.end < from {
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        self.encode(revision, page)
    }

    /// Encodes `page` under `revision` from the stored encoded sets, decoding none.
    fn encode(&self, revision: u64, page: Page) -> Result<Vec<u8>, StatusCode> {
        Range::write(
            revision,
            page.start,
            page.end - page.start + 1,
            self.hash(page.start - 1),
            self.hash(page.end),
            self.sets[page.sets]
                .iter()
                .map(|(height, set)| (*height, set.as_slice())),
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }

    /// The whole window as consecutive encoded ranges, each within [`MAX_RANGE_BYTES`].
    pub fn ranges(&self) -> Result<Vec<Vec<u8>>, Error> {
        let mut ranges = Vec::new();
        let mut start = self.head.first;
        while start <= self.head.tip.height {
            let page = self.page(start, self.last(start, MAX_RANGE_BLOCKS));
            if page.end < start {
                return Err(Error::Capacity);
            }
            start = page.end + 1;
            ranges.push(
                self.encode(self.head.revision, page)
                    .map_err(|_| Error::Capacity)?,
            );
        }
        Ok(ranges)
    }

    /// Refuses a stale revision or pruned height as gone and a height past the tip as
    /// not yet available.
    fn check(&self, revision: u64, from: u32) -> Result<(), StatusCode> {
        if revision != self.head.revision || from < self.head.first {
            return Err(StatusCode::GONE);
        }
        if from > self.head.tip.height {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        Ok(())
    }
}
