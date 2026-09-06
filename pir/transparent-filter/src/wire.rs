//! JSON metadata shapes exchanged with the filter service.
//!
//! These live outside the `client` feature so the service can build its
//! responses from the same definitions a wallet parses, rather than the two
//! sides drifting apart in separate hand-written structs.
//!
//! Hashes appear here in display hex, because these are the human-facing
//! surfaces. Binary serialization uses internal order; see `envelope.rs`.

use serde::{Deserialize, Serialize};

/// Response shape of `GET /v1/filters/info`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FilterServiceInfo {
    /// Genesis block hash in display hex; the chain's identity.
    pub genesis_hash: String,
    pub network: String,
    pub profile: String,
    pub envelope_version: u16,
    /// First height this service publishes a filter for.
    pub start_height: u64,
    /// Highest height with durable coverage, if any.
    pub covered_through: Option<u64>,
    pub covered_block_hash: Option<String>,
    pub max_records_per_batch: u64,
    pub max_filter_bytes: usize,
}

/// Response shape of `GET /v1/health`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FilterServiceHealth {
    /// `syncing`, `serving` or `failed`.
    pub phase: String,
    pub detail: Option<String>,
    pub covered_through: Option<u64>,
    pub tip_height: Option<u64>,
    pub filters_stored: u64,
}

/// One entry of `GET /v1/filters/digests`.
///
/// The digest is derivable from filter bytes the caller already holds, so this
/// exists for the case where it does not hold them: comparing what different
/// operators publish for the same block without downloading both sets of
/// filters. Agreement across independent operators is evidence about
/// construction that a single operator's own digest cannot provide, since a
/// digest supplied alongside a false filter simply commits to the false filter.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FilterDigestEntry {
    pub height: u64,
    pub block_hash: String,
    /// Double-SHA-256 of the serialized filter bytes, in display hex.
    pub filter_hash: String,
}

/// One entry of `GET /v1/filters/chain`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChainEntry {
    pub height: u64,
    /// Display hex, as a human-facing JSON field.
    pub block_hash: String,
}

/// The seal thresholds a shard set was built under.
///
/// Part of the published map because they are schema, not tuning: a wallet that
/// cached shards built under different thresholds would be holding two
/// incompatible partitions of the same chain. Changing any of them re-shards.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SealParameters {
    /// Distinct scripts, which size the filter and the directory.
    pub max_scripts: u64,
    /// Page rows, which size the pages table. Not derivable from the event
    /// count: pages are per-script and padded, so many short histories cost far
    /// more rows than the same number of events in a few long ones.
    pub max_page_rows: u64,
    /// Distinct transaction ids, which size the transaction-detail table.
    pub max_txids: u64,
}

/// One entry of the published height-to-shard map.
///
/// Occupancy counts are published so a wallet — or an independent operator —
/// can check that a shard was sealed where the thresholds say it should have
/// been, rather than taking the boundary on trust.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ShardMapEntry {
    pub shard_id: u64,
    pub start_height: u64,
    pub end_height: u64,
    /// The block before `start_height`, in display hex.
    pub parent_block_hash: String,
    /// The block at `end_height`, in display hex.
    pub terminal_block_hash: String,
    /// Double-SHA-256 of the shard's serialized filter bytes, in display hex.
    pub filter_hash: String,
    pub scripts: u64,
    pub page_rows: u64,
    pub txids: u64,
    /// Segments in the shard's directory table, and in its pages table.
    ///
    /// One apiece in the ordinary case. A shard whose content did not fit one
    /// segment of the pinned geometry holds more, and a wallet needs the counts
    /// before it can query: a row is addressed within the shard's whole logical
    /// row space, and every segment is asked, so the segment a script lands in
    /// is never named in a request.
    pub directory_segments: u32,
    pub page_segments: u32,
    /// Digest of the shard's manifest: the identity of *this revision* of it.
    ///
    /// A growing tail is republished as a new revision with its own digest, so
    /// this is what a wallet records alongside provisional coverage in order to
    /// notice that the range it covered has been superseded.
    pub manifest_digest: String,
    /// Which published revision of this shard the entry describes.
    ///
    /// Zero for a shard published once. A sealed shard's revision is final.
    pub revision: u32,
    /// False only for the tail, which is still growing toward a threshold.
    ///
    /// Coverage taken from an unsealed shard is provisional: it is recorded
    /// with `manifest_digest` and re-derived when a later revision, or the
    /// sealed shard, appears.
    pub sealed: bool,
}

/// Response shape of `GET /v1/filters/shards`.
///
/// This is protocol data, not a convenience index. Shard boundaries are derived
/// from chain content, so a wallet cannot recompute them arithmetically and an
/// operator must reproduce these boundaries rather than derive its own — which
/// is what keeps a disagreement about one event localized to one shard's
/// digests instead of shifting every later boundary.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ShardMap {
    /// Genesis block hash in display hex; the chain's identity.
    pub genesis_hash: String,
    pub network: String,
    /// The range profile, distinct from the per-block profile.
    pub profile: String,
    pub range_envelope_version: u16,
    /// First height shard zero covers.
    pub start_height: u64,
    pub seal: SealParameters,
    /// Ascending by `shard_id`, gapless, starting at zero.
    pub shards: Vec<ShardMapEntry>,
}

impl ShardMap {
    /// The shard covering `height`, if the map covers it.
    ///
    /// Binary search rather than arithmetic: with content-sealed boundaries
    /// there is no width to divide by. This is how a wallet turns its birthday
    /// into the first shard it must sync.
    pub fn shard_for_height(&self, height: u64) -> Option<&ShardMapEntry> {
        let index = self
            .shards
            .binary_search_by(|shard| {
                if shard.end_height < height {
                    std::cmp::Ordering::Less
                } else if shard.start_height > height {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()?;
        self.shards.get(index)
    }

    /// Checks the map is internally well formed before anything is fetched.
    ///
    /// A wallet that skipped this could sync a map with a hole in it and treat
    /// the result as complete coverage. Height continuity is checked here;
    /// binding the hashes to the wallet's accepted chain needs wallet state and
    /// belongs to `check_range_batch`.
    pub fn check_shape(&self) -> Result<(), String> {
        if self.shards.is_empty() {
            return Err("shard map is empty".into());
        }
        if self.shards[0].start_height != self.start_height {
            return Err(format!(
                "shard 0 starts at {} but the map starts at {}",
                self.shards[0].start_height, self.start_height
            ));
        }
        for (index, shard) in self.shards.iter().enumerate() {
            if shard.shard_id != index as u64 {
                return Err(format!(
                    "shard at position {index} claims id {}",
                    shard.shard_id
                ));
            }
            if shard.end_height < shard.start_height {
                return Err(format!("shard {index} ends before it starts"));
            }
            // A shard with no segment has no table to query, and a wallet that
            // accepted one would advance coverage over a range it never read.
            if shard.directory_segments == 0 || shard.page_segments == 0 {
                return Err(format!("shard {index} declares no segments"));
            }
            if shard.manifest_digest.len() != 64 {
                return Err(format!(
                    "shard {index} has no manifest digest to identify its revision"
                ));
            }
            if let Some(previous) = index.checked_sub(1).and_then(|i| self.shards.get(i)) {
                if shard.start_height != previous.end_height + 1 {
                    return Err(format!(
                        "shard {index} starts at {} but shard {} ends at {}",
                        shard.start_height, previous.shard_id, previous.end_height
                    ));
                }
                // The hash chain is what makes a gap detectable at all. Heights
                // alone would accept a map whose shards were built over two
                // different branches, since both would still be contiguous.
                if shard.parent_block_hash != previous.terminal_block_hash {
                    return Err(format!(
                        "shard {} does not chain to shard {}",
                        shard.shard_id, previous.shard_id
                    ));
                }
                // A sealed shard after an unsealed one would mean the tail was
                // published out of order, and the wallet would have no way to
                // tell which of the two is current.
                if !previous.sealed {
                    return Err(format!(
                        "shard {} follows unsealed shard {}",
                        shard.shard_id, previous.shard_id
                    ));
                }
            }
        }
        Ok(())
    }
}
