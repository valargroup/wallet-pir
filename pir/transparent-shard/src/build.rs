//! Assembling one sealed shard's published objects.
//!
//! Given the events of a sealed range, this produces the four things a shard
//! publishes: a public activity filter, a private script directory, private
//! event pages, and the occupancy counts that go in its manifest.
//!
//! Everything here is a deterministic function of the events and the shard's
//! identity. Two operators with the same journal must produce byte-identical
//! tables, or the published digests stop being comparable and the only
//! cross-operator check the design has stops working. That is why scripts are
//! processed in sorted order, events in their own total order, and bucket
//! placement resolved by a rule rather than by iteration order.

use crate::layout::{page_rows_for, segments_for, DIRECTORY_ROWS, INLINE_EVENTS, PAGE_ROWS};
use crate::records::{encode_directory_row, DirectoryEntry, Page, RecordError, MAX_SCRIPT_BYTES};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use transparent_events::TransparentEvent;
use transparent_filter::{
    build_range_filter, BlockHash, FilterBytes, FilterError, ScriptBytes, ShardKey,
};

/// Independently salted candidate rows per script.
///
/// Two choices, as the mainnet study selected: it packs far better than one and
/// costs one more query than one, where four costs two more queries for less
/// improvement. A script that fits in neither candidate is not resolved by a
/// public lookup, which would reveal the script it was trying to place; it is
/// resolved by giving the shard another segment and placing again.
pub const DIRECTORY_CHOICES: usize = 2;

/// Segments a shard's directory may be given before placement is called
/// pathological.
///
/// This is not the availability bound. Adding a segment re-hashes every script
/// over a larger row space, so the load falls with each attempt and one or two
/// attempts settle any real shard — the largest block ever produced is many
/// orders of magnitude below what this many segments hold. It exists so a bug
/// in the placement rule fails loudly instead of allocating forever.
const MAX_DIRECTORY_SEGMENTS: u32 = 1_024;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("record: {0}")]
    Record(#[from] RecordError),
    #[error("filter: {0}")]
    Filter(#[from] FilterError),
    #[error("script {script} fits in neither candidate row at {MAX_DIRECTORY_SEGMENTS} segments")]
    DirectoryFull { script: String },
    #[error("{0}")]
    Invalid(String),
}

/// One shard's published objects.
pub struct BuiltShard {
    pub shard_id: u64,
    pub start_height: u64,
    pub end_height: u64,
    /// The public filter. Contains every element the range yields, including
    /// scripts too long to index privately.
    pub filter: FilterBytes,
    /// The directory table, one entry per segment.
    ///
    /// One segment is the ordinary case. More means this shard's content did
    /// not fit one, which happens when a single block exceeds a seal capacity
    /// on its own. Every segment has the same pinned geometry, so the shared
    /// parameter set is unaffected.
    pub directory: Vec<Vec<u8>>,
    /// The pages table, one entry per segment. Segments concatenate in order
    /// into the shard's page space, which is what a directory extent indexes.
    pub pages: Vec<Vec<u8>>,
    /// Scripts placed in the directory.
    pub scripts: u64,
    pub page_rows: u64,
    pub events: u64,
    /// Scripts in the filter but not the directory, because they exceed
    /// [`MAX_SCRIPT_BYTES`].
    ///
    /// Published so the coverage gap is measured rather than silent. A wallet
    /// holding such a script is outside coverage and must not read a directory
    /// miss as absence.
    pub excluded_scripts: u64,
}

impl BuiltShard {
    /// Segments in this shard's directory table. One in the ordinary case.
    pub fn directory_segments(&self) -> u32 {
        self.directory.len() as u32
    }

    /// Segments in this shard's pages table. One in the ordinary case.
    pub fn page_segments(&self) -> u32 {
        self.pages.len() as u32
    }

    /// One row of the shard's logical directory space, which is its segments
    /// concatenated. Panics outside the space, which is a programming error.
    pub fn directory_row(&self, row: u64) -> &[u8] {
        let (segment, within) = crate::layout::split_row(row, DIRECTORY_ROWS as u64);
        let at = within as usize * crate::layout::DIRECTORY_ROW_BYTES;
        &self.directory[segment as usize][at..at + crate::layout::DIRECTORY_ROW_BYTES]
    }

    /// One row of the shard's logical page space, which a directory extent
    /// indexes across segment boundaries as if it were one table.
    pub fn page_row(&self, page: u64) -> &[u8] {
        let (segment, within) = crate::layout::split_row(page, PAGE_ROWS as u64);
        let at = within as usize * crate::layout::PAGE_ROW_BYTES;
        &self.pages[segment as usize][at..at + crate::layout::PAGE_ROW_BYTES]
    }
}

/// Derives one of a shard's independently salted bucket keys.
///
/// Salting per shard means the same script lands in different rows in different
/// shards, so an observer who learns one shard's row for a script learns
/// nothing about where it sits in any other.
fn bucket_salt(shard_id: u64, choice: usize) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"zcash-transparent-range-v1/directory-bucket\0");
    hasher.update(shard_id.to_le_bytes());
    hasher.update((choice as u32).to_le_bytes());
    hasher.finalize().into()
}

/// The row a script hashes to under one salt.
pub fn bucket_for(salt: &[u8; 32], script: &[u8], rows: u64) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(salt);
    hasher.update((script.len() as u32).to_le_bytes());
    hasher.update(script);
    let digest = hasher.finalize();
    u64::from_le_bytes(digest[..8].try_into().expect("8 bytes")) % rows
}

/// Both candidate rows for a script in this shard.
pub fn candidate_rows(shard_id: u64, script: &[u8], rows: u64) -> [u64; DIRECTORY_CHOICES] {
    std::array::from_fn(|choice| bucket_for(&bucket_salt(shard_id, choice), script, rows))
}

/// Builds one shard from the events of its sealed range.
///
/// `events` need not be sorted; they are ordered here, because the published
/// bytes must not depend on the order the journal happened to yield them.
///
/// `terminal_block_hash` is the block at `end_height`, and is part of the
/// filter's keying, so a filter cannot be replayed under another shard's
/// identity.
#[allow(clippy::too_many_arguments)]
pub fn build_shard(
    shard_id: u64,
    start_height: u64,
    end_height: u64,
    genesis: BlockHash,
    terminal_block_hash: BlockHash,
    profile: &str,
    events: &[(ScriptBytes, TransparentEvent)],
) -> Result<BuiltShard, BuildError> {
    // Group by script, in a sorted map so the walk order is the scripts' own
    // order rather than a hash map's.
    let mut by_script: BTreeMap<Vec<u8>, Vec<TransparentEvent>> = BTreeMap::new();
    for (script, event) in events {
        if event.height() < start_height as u32 || event.height() > end_height as u32 {
            return Err(BuildError::Invalid(format!(
                "event at height {} is outside the shard's {start_height}-{end_height}",
                event.height()
            )));
        }
        by_script
            .entry(script.as_slice().to_vec())
            .or_default()
            .push(*event);
    }

    // The filter covers every element, including scripts the private tables
    // cannot index. Excluding them here would tell a wallet holding one that
    // its script had no activity, which is worse than telling it the script is
    // outside coverage.
    let filter_elements: Vec<ScriptBytes> = by_script
        .keys()
        .map(|script| ScriptBytes::new(script.clone()))
        .collect();
    let key = ShardKey::derive(
        profile,
        genesis,
        shard_id,
        start_height,
        end_height,
        terminal_block_hash,
    );
    let filter = build_range_filter(key, &filter_elements)?;

    // Lay out pages first: a directory entry has to name where its pages are.
    let mut pages: Vec<Page> = Vec::new();
    let mut entries: Vec<DirectoryEntry> = Vec::new();
    let mut excluded_scripts = 0u64;
    let mut total_events = 0u64;

    for (script, history) in &by_script {
        total_events += history.len() as u64;
        if script.len() > MAX_SCRIPT_BYTES {
            excluded_scripts += 1;
            continue;
        }
        let mut history = history.clone();
        history.sort_by_key(|event| event.sort_key());

        // The newest events go inline, so an active script's recent history
        // needs no page query at all. The older remainder is paged in
        // ascending order, which is the order a replay wants them in.
        let inline_from = history.len().saturating_sub(INLINE_EVENTS as usize);
        let (older, inline) = history.split_at(inline_from);

        let first_page = pages.len() as u32;
        let page_count = page_rows_for(history.len() as u32) as u32;
        if page_count as usize
            != older
                .len()
                .div_ceil(crate::layout::EVENTS_PER_PAGE as usize)
        {
            return Err(BuildError::Invalid(format!(
                "page accounting disagrees for a {}-event history",
                history.len()
            )));
        }
        for (ordinal, chunk) in older
            .chunks(crate::layout::EVENTS_PER_PAGE as usize)
            .enumerate()
        {
            pages.push(Page::new(
                script.clone(),
                ordinal as u32,
                page_count,
                chunk.to_vec(),
            )?);
        }

        entries.push(DirectoryEntry {
            script: script.clone(),
            total_events: history.len() as u32,
            inline: inline.to_vec(),
            first_page,
            page_count,
        });
    }

    // A shard takes as many page segments as its pages need. Failing instead
    // would mean one oversized block could stop publication.
    let page_segments = segments_for(pages.len() as u64, PAGE_ROWS as u64);
    let page_rows = pages.len() as u64;
    let mut page_table =
        Vec::with_capacity(page_segments as usize * PAGE_ROWS * crate::layout::PAGE_ROW_BYTES);
    for page in &pages {
        page_table.extend_from_slice(&page.encode()?);
    }
    // Unused page rows are zero and decode as absent. They are indistinguishable
    // in a response from occupied ones, which is the point.
    page_table.resize(
        page_segments as usize * PAGE_ROWS * crate::layout::PAGE_ROW_BYTES,
        0,
    );
    let page_table = page_table
        .chunks(PAGE_ROWS * crate::layout::PAGE_ROW_BYTES)
        .map(<[u8]>::to_vec)
        .collect();

    let (directory, scripts) = place_directory(shard_id, entries)?;

    Ok(BuiltShard {
        shard_id,
        start_height,
        end_height,
        filter,
        directory,
        pages: page_table,
        scripts,
        page_rows,
        events: total_events,
        excluded_scripts,
    })
}

/// Places every entry, adding a segment whenever placement does not fit.
///
/// Rows are addressed over the shard's whole logical row space, so adding a
/// segment re-hashes every script rather than spilling the leftovers into a
/// last segment that would then be the only crowded one.
///
/// Returns one encoded table per segment, and the number of scripts placed.
fn place_directory(
    shard_id: u64,
    entries: Vec<DirectoryEntry>,
) -> Result<(Vec<Vec<u8>>, u64), BuildError> {
    let mut segments = 1u32;
    loop {
        let rows = DIRECTORY_ROWS as u64 * segments as u64;
        // Place each script into the less loaded of its two candidate rows.
        // Ties go to the first candidate, so placement is a function of the
        // script set and not of the order rows happened to fill.
        let mut buckets: Vec<Vec<&DirectoryEntry>> = vec![Vec::new(); rows as usize];
        let mut overflowed: Option<&DirectoryEntry> = None;
        for entry in &entries {
            let candidates = candidate_rows(shard_id, &entry.script, rows);
            let chosen = candidates
                .iter()
                .map(|row| *row as usize)
                .min_by_key(|row| buckets[*row].len())
                .expect("two candidates");
            if buckets[chosen].len() >= crate::records::DIRECTORY_SLOTS {
                overflowed = Some(entry);
                break;
            }
            buckets[chosen].push(entry);
        }

        if let Some(entry) = overflowed {
            segments += 1;
            if segments > MAX_DIRECTORY_SEGMENTS {
                return Err(BuildError::DirectoryFull {
                    script: hex::encode(&entry.script),
                });
            }
            continue;
        }

        let mut scripts = 0u64;
        let mut tables = Vec::with_capacity(segments as usize);
        for segment in buckets.chunks_mut(DIRECTORY_ROWS) {
            let mut table = Vec::with_capacity(DIRECTORY_ROWS * crate::layout::DIRECTORY_ROW_BYTES);
            for bucket in segment.iter_mut() {
                // Within a row, entries are ordered by script, so the row's
                // bytes do not depend on placement order.
                bucket.sort_by(|a, b| a.script.cmp(&b.script));
                scripts += bucket.len() as u64;
                let owned: Vec<DirectoryEntry> = bucket.iter().map(|e| (*e).clone()).collect();
                table.extend_from_slice(&encode_directory_row(&owned)?);
            }
            tables.push(table);
        }
        return Ok((tables, scripts));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::decode_directory_row;
    use transparent_events::{ReceiveEvent, Txid};
    use transparent_filter::{validate_filter, FilterLimits, RANGE_PROFILE};

    fn genesis() -> BlockHash {
        BlockHash::from_display_hex(transparent_filter::MAINNET_GENESIS_DISPLAY).unwrap()
    }

    fn terminal() -> BlockHash {
        BlockHash::from_internal_bytes([0x5a; 32])
    }

    fn script(tag: u32) -> ScriptBytes {
        let mut bytes = vec![0x76, 0xa9, 0x14];
        bytes.extend_from_slice(&tag.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 16]);
        bytes.extend_from_slice(&[0x88, 0xac]);
        ScriptBytes::new(bytes)
    }

    fn event(height: u32, nonce: u32) -> TransparentEvent {
        let mut txid = [0u8; 32];
        txid[..4].copy_from_slice(&nonce.to_le_bytes());
        TransparentEvent::Receive(ReceiveEvent {
            height,
            txid: Txid(txid),
            transaction_index: 0,
            output_index: nonce,
            value: 1_000,
            coinbase: false,
        })
    }

    /// `count` scripts, each with `per` events, all inside the shard's declared
    /// 100-200 range. Heights cycle so a long history stays in range while
    /// still spanning it, which is what makes the paging tests meaningful.
    fn fixture(count: u32, per: u32) -> Vec<(ScriptBytes, TransparentEvent)> {
        let mut events = Vec::new();
        for tag in 0..count {
            for i in 0..per {
                events.push((script(tag), event(100 + i % 101, tag * 100_000 + i)));
            }
        }
        events
    }

    fn build(events: &[(ScriptBytes, TransparentEvent)]) -> BuiltShard {
        build_shard(0, 100, 200, genesis(), terminal(), RANGE_PROFILE, events).expect("build")
    }

    #[test]
    fn tables_are_exactly_the_pinned_size_whatever_the_shard_holds() {
        for (count, per) in [(1u32, 1u32), (200, 3), (50, 40)] {
            let built = build(&fixture(count, per));
            assert_eq!(built.directory_segments(), 1, "an ordinary shard");
            assert_eq!(built.page_segments(), 1, "an ordinary shard");
            for segment in &built.directory {
                assert_eq!(
                    segment.len(),
                    DIRECTORY_ROWS * crate::layout::DIRECTORY_ROW_BYTES
                );
            }
            for segment in &built.pages {
                assert_eq!(segment.len(), PAGE_ROWS * crate::layout::PAGE_ROW_BYTES);
            }
        }
    }

    /// The availability property at the builder: content that cannot fit one
    /// segment gets another, of the same pinned geometry, rather than failing.
    /// A shard that failed to build would stop publication altogether.
    #[test]
    fn content_too_large_for_one_segment_takes_another() {
        // Enough distinct histories to overrun the pinned page table, which is
        // what one oversized block looks like to the builder.
        let per = crate::layout::EVENTS_PER_PAGE + INLINE_EVENTS;
        let scripts = PAGE_ROWS as u32 + 100;
        let built = build(&fixture(scripts, per));

        assert!(built.page_segments() >= 2, "the pages did not fit one");
        assert_eq!(built.page_rows, scripts as u64);
        for segment in &built.pages {
            assert_eq!(segment.len(), PAGE_ROWS * crate::layout::PAGE_ROW_BYTES);
        }
        for segment in &built.directory {
            assert_eq!(
                segment.len(),
                DIRECTORY_ROWS * crate::layout::DIRECTORY_ROW_BYTES
            );
        }

        // Every script is still retrievable, and its extent still resolves —
        // across the segment boundary for the scripts placed past it.
        let rows = DIRECTORY_ROWS as u64 * built.directory_segments() as u64;
        let mut crossed = false;
        for tag in 0..scripts {
            let wanted = script(tag);
            let entry = candidate_rows(0, wanted.as_slice(), rows)
                .iter()
                .find_map(|row| {
                    decode_directory_row(built.directory_row(*row))
                        .unwrap()
                        .into_iter()
                        .find(|entry| entry.script == wanted.as_slice())
                })
                .unwrap_or_else(|| panic!("script {tag} is in neither candidate row"));
            assert_eq!(entry.total_events, per);
            if entry.first_page as usize >= PAGE_ROWS {
                crossed = true;
            }
            let page = Page::decode(built.page_row(entry.first_page as u64))
                .unwrap()
                .expect("a located page is occupied");
            assert_eq!(page.script, wanted.as_slice());
        }
        assert!(crossed, "some extent should land past the first segment");
    }

    /// Every script must be findable at one of its two candidate rows, with its
    /// exact bytes. This is the property the whole directory exists to provide.
    #[test]
    fn every_script_is_retrievable_from_one_of_its_candidate_rows() {
        let built = build(&fixture(300, 2));
        for tag in 0..300u32 {
            let wanted = script(tag);
            let rows = DIRECTORY_ROWS as u64 * built.directory_segments() as u64;
            let candidates = candidate_rows(0, wanted.as_slice(), rows);
            let found = candidates.iter().any(|row| {
                decode_directory_row(built.directory_row(*row))
                    .unwrap()
                    .iter()
                    .any(|entry| entry.script == wanted.as_slice())
            });
            assert!(found, "script {tag} is in neither candidate row");
        }
        assert_eq!(built.scripts, 300);
    }

    /// A script's whole history must be reconstructible: the inline events plus
    /// its pages, with nothing lost and nothing duplicated.
    #[test]
    fn inline_events_and_pages_together_hold_the_complete_history() {
        let per = crate::layout::EVENTS_PER_PAGE + INLINE_EVENTS + 7;
        let built = build(&fixture(3, per));

        for tag in 0..3u32 {
            let wanted = script(tag);
            let rows = DIRECTORY_ROWS as u64 * built.directory_segments() as u64;
            let entry = candidate_rows(0, wanted.as_slice(), rows)
                .iter()
                .find_map(|row| {
                    decode_directory_row(built.directory_row(*row))
                        .unwrap()
                        .into_iter()
                        .find(|entry| entry.script == wanted.as_slice())
                })
                .expect("entry");

            assert_eq!(entry.total_events, per);
            let mut recovered = entry.inline.clone();
            for ordinal in 0..entry.page_count {
                let page = Page::decode(built.page_row((entry.first_page + ordinal) as u64))
                    .unwrap()
                    .expect("a located page is occupied");
                assert_eq!(page.script, wanted.as_slice());
                assert_eq!(page.ordinal, ordinal);
                assert_eq!(page.page_count, entry.page_count);
                recovered.extend(page.events);
            }

            recovered.sort_by_key(|event| event.sort_key());
            recovered.dedup();
            assert_eq!(
                recovered.len() as u32,
                per,
                "history for script {tag} did not reassemble"
            );
        }
    }

    /// The inline events must be the *newest*, so a script that was recently
    /// active needs no page query. Getting this backwards would make the common
    /// case the expensive one.
    #[test]
    fn the_inline_events_are_the_newest_ones() {
        let built = build(&fixture(1, 50));
        let wanted = script(0);
        let rows = DIRECTORY_ROWS as u64 * built.directory_segments() as u64;
        let entry = candidate_rows(0, wanted.as_slice(), rows)
            .iter()
            .find_map(|row| {
                decode_directory_row(built.directory_row(*row))
                    .unwrap()
                    .into_iter()
                    .find(|entry| entry.script == wanted.as_slice())
            })
            .expect("entry");
        // Compare on the events' own total order rather than on height alone,
        // since a shard's heights repeat across transactions.
        let earliest_inline = entry
            .inline
            .iter()
            .map(|event| event.sort_key())
            .min()
            .unwrap();
        let page = Page::decode(built.page_row(entry.first_page as u64))
            .unwrap()
            .unwrap();
        let latest_paged = page
            .events
            .iter()
            .map(|event| event.sort_key())
            .max()
            .unwrap();
        assert!(
            latest_paged < earliest_inline,
            "paged events should all precede the inline ones"
        );
    }

    /// The public filter must match every script the shard indexed, or a wallet
    /// would be told it had no activity in a shard that holds its history.
    #[test]
    fn the_filter_matches_every_script_the_shard_holds() {
        let built = build(&fixture(200, 2));
        let validated = validate_filter(built.filter.as_slice(), FilterLimits::default()).unwrap();
        let key = ShardKey::derive(RANGE_PROFILE, genesis(), 0, 100, 200, terminal());
        let scripts: Vec<ScriptBytes> = (0..200).map(script).collect();
        let matched = transparent_filter::match_range_scripts(&validated, key, &scripts).unwrap();
        assert_eq!(matched.len(), 200);
    }

    /// A script too long to index privately still belongs in the public filter.
    /// Omitting it would report "no activity" to a wallet that has activity;
    /// including it lets that wallet see a match and learn from the manifest
    /// that it is outside coverage.
    #[test]
    fn an_unindexable_script_is_filtered_but_counted_as_excluded() {
        let long = ScriptBytes::new(vec![0xab; MAX_SCRIPT_BYTES + 5]);
        let mut events = fixture(2, 1);
        events.push((long.clone(), event(100, 9_999)));
        let built = build(&events);

        assert_eq!(built.excluded_scripts, 1);
        assert_eq!(
            built.scripts, 2,
            "only indexable scripts reach the directory"
        );

        let validated = validate_filter(built.filter.as_slice(), FilterLimits::default()).unwrap();
        let key = ShardKey::derive(RANGE_PROFILE, genesis(), 0, 100, 200, terminal());
        let matched = transparent_filter::match_range_scripts(&validated, key, &[long]).unwrap();
        assert_eq!(
            matched,
            vec![0],
            "the long script must still be in the filter"
        );
    }

    /// Two operators with the same journal must produce identical bytes, or the
    /// published digests stop being comparable and the design loses its only
    /// cross-operator check.
    #[test]
    fn building_is_deterministic_regardless_of_input_order() {
        let events = fixture(120, 3);
        let forward = build(&events);
        let mut shuffled = events.clone();
        shuffled.reverse();
        let reversed = build(&shuffled);

        assert_eq!(forward.directory, reversed.directory);
        assert_eq!(forward.pages, reversed.pages);
        assert_eq!(forward.filter, reversed.filter);
    }

    /// A shard's bucket salt includes its id, so the same script sits in
    /// unrelated rows in different shards. An observer who learned one row
    /// would otherwise learn the script's row everywhere.
    #[test]
    fn the_same_script_lands_in_unrelated_rows_in_different_shards() {
        let wanted = script(1);
        let a = candidate_rows(0, wanted.as_slice(), DIRECTORY_ROWS as u64);
        let b = candidate_rows(1, wanted.as_slice(), DIRECTORY_ROWS as u64);
        assert_ne!(a, b);
    }

    #[test]
    fn events_outside_the_declared_range_are_refused() {
        let events = vec![(script(0), event(500, 0))];
        assert!(matches!(
            build_shard(0, 100, 200, genesis(), terminal(), RANGE_PROFILE, &events),
            Err(BuildError::Invalid(_))
        ));
    }
}
