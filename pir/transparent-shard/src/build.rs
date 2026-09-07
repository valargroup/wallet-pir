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

use crate::layout::{
    entries_per_row, fragments_for, segments_for, shape_of, PackedDemand, Shape, DIRECTORY_ROWS,
    INLINE_EVENTS, PAGE_ROWS,
};
use crate::page_row::{encode_page_row, PageEntry};
use crate::records::{encode_directory_row, DirectoryEntry, RecordError, MAX_SCRIPT_BYTES};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
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
    /// Page rows emitted, after packing. Compared against what the sealer
    /// sized this shard for; a disagreement is a failed publish.
    pub page_rows: u64,
    /// Fragments across every indexed history, which is what a wallet's page
    /// queries count and what the live-byte accounting charges an entry header
    /// for. Not the row count: short histories share rows.
    pub fragments: u64,
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

    // Lay out page rows first: a directory entry has to name where its
    // fragments are. Three passes, because placement is no longer a running
    // counter — a short history's row depends on which other short histories of
    // the same length exist, so nothing can be assigned until they are all
    // known.
    let mut excluded_scripts = 0u64;
    let mut total_events = 0u64;

    // 1. Classify. Each supported script's history splits into the newest
    //    events, which live inline and cost no fragment, and the older
    //    remainder, which is paged in ascending order — the order a replay
    //    wants them in.
    struct Classified<'a> {
        script: &'a [u8],
        inline: Vec<TransparentEvent>,
        older: Vec<TransparentEvent>,
        total: u32,
    }
    let mut short: BTreeMap<u32, Vec<Classified>> = BTreeMap::new();
    let mut long: Vec<Classified> = Vec::new();
    let mut demand = PackedDemand::default();

    for (script, history) in &by_script {
        total_events += history.len() as u64;
        if script.len() > MAX_SCRIPT_BYTES {
            excluded_scripts += 1;
            continue;
        }
        let mut history = history.clone();
        history.sort_by_key(|event| event.sort_key());
        let total = history.len() as u32;
        let inline_from = history.len().saturating_sub(INLINE_EVENTS as usize);
        let (older, inline) = history.split_at(inline_from);
        let classified = Classified {
            script,
            inline: inline.to_vec(),
            older: older.to_vec(),
            total,
        };
        demand.shift(0, total);
        match shape_of(total) {
            Shape::None => {}
            Shape::Short(p) => short.entry(p).or_default().push(classified),
            Shape::Long(_) => long.push(classified),
        }
    }

    // 2. Allocate. Short classes in increasing length, each class filling rows
    //    with whole entries; then long histories, each taking a contiguous run
    //    of its own. A `BTreeMap` keyed by length and a `by_script` iterated in
    //    script order make the result a function of the content, not of the
    //    order the events arrived in.
    let mut rows: Vec<Vec<PageEntry>> = Vec::new();
    let mut first_page: HashMap<&[u8], u32> = HashMap::new();

    for (p, class) in &short {
        let per_row = entries_per_row(*p) as usize;
        for chunk in class.chunks(per_row) {
            let row = rows.len() as u32;
            let mut entries = Vec::with_capacity(chunk.len());
            for item in chunk {
                first_page.insert(item.script, row);
                entries.push(PageEntry::new(
                    item.script.to_vec(),
                    0,
                    1,
                    item.older.clone(),
                )?);
            }
            rows.push(entries);
        }
    }
    for item in &long {
        first_page.insert(item.script, rows.len() as u32);
        let fragments = fragments_for(item.total) as u32;
        for (ordinal, fragment) in item
            .older
            .chunks(crate::layout::EVENTS_PER_PAGE as usize)
            .enumerate()
        {
            // A long history's final fragment keeps a row to itself rather than
            // sharing one. Sharing it would make a history's placement depend
            // on unrelated content, for a row saved per long history.
            rows.push(vec![PageEntry::new(
                item.script.to_vec(),
                ordinal as u32,
                fragments,
                fragment.to_vec(),
            )?]);
        }
    }

    // The sealer sized this shard's table from the same rule, and the publisher
    // compares the two. Checking it here as well is what makes a disagreement a
    // failed build rather than a table sized for less than it holds — which
    // would come back as extra segments, and every wallet querying this shard
    // pays for those.
    if rows.len() as u64 != demand.rows() {
        return Err(BuildError::Invalid(format!(
            "emitted {} page rows where the packing rule demands {}",
            rows.len(),
            demand.rows()
        )));
    }

    // 3. Encode, and build the directory entries that name what was assigned.
    let mut entries: Vec<DirectoryEntry> = Vec::new();
    for class in short.values() {
        for item in class {
            entries.push(DirectoryEntry {
                script: item.script.to_vec(),
                total_events: item.total,
                inline: item.inline.clone(),
                first_page: first_page[item.script],
                page_count: fragments_for(item.total) as u32,
            });
        }
    }
    for item in &long {
        entries.push(DirectoryEntry {
            script: item.script.to_vec(),
            total_events: item.total,
            inline: item.inline.clone(),
            first_page: first_page[item.script],
            page_count: fragments_for(item.total) as u32,
        });
    }
    // Histories inside the inline allowance hold no fragment and name no row.
    for (script, history) in &by_script {
        if script.len() > MAX_SCRIPT_BYTES || shape_of(history.len() as u32) != Shape::None {
            continue;
        }
        let mut history = history.clone();
        history.sort_by_key(|event| event.sort_key());
        entries.push(DirectoryEntry {
            script: script.clone(),
            total_events: history.len() as u32,
            inline: history,
            first_page: 0,
            page_count: 0,
        });
    }

    let fragments: u64 = by_script
        .iter()
        .filter(|(script, _)| script.len() <= MAX_SCRIPT_BYTES)
        .map(|(_, history)| fragments_for(history.len() as u32))
        .sum();

    // A shard takes as many page segments as its rows need. Failing instead
    // would mean one oversized block could stop publication.
    let page_segments = segments_for(rows.len() as u64, PAGE_ROWS as u64);
    let page_rows = rows.len() as u64;
    let mut page_table =
        Vec::with_capacity(page_segments as usize * PAGE_ROWS * crate::layout::PAGE_ROW_BYTES);
    for row in &rows {
        page_table.extend_from_slice(&encode_page_row(row)?);
    }
    // Unused page rows are zero and decode as empty. They are indistinguishable
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
        fragments,
        events: total_events,
        excluded_scripts,
    })
}

/// What two-choice placement costs a script set.
///
/// The census reports this instead of `scripts / slots`, which is the segment
/// count placement would need if every row filled evenly. It never does: two
/// choices per script still leave the fullest row well above the mean, and the
/// difference is not a rounding error but the whole question of how close to a
/// segment's slot count a seal target may safely sit. A modelled count is a
/// lower bound; this is the count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Placement {
    /// Segments the placement rule needed.
    pub segments: u32,
    /// Entries in the fullest row.
    ///
    /// The headroom that matters. A shard whose fullest row sits at the slot
    /// count placed only because some script happened to have a usable second
    /// choice, and the next script to arrive is what adds a segment — which
    /// every wallet querying that shard then pays for.
    pub max_row_load: u64,
}

/// Runs the placement rule over a script set without building anything.
///
/// The same rule, in the same order, as [`place_directory`] — which calls it,
/// so there is one implementation and a census cannot report a placement the
/// builder would not produce. Returns the offending script, hex encoded, if the
/// retry cap is reached.
pub fn place_scripts(
    shard_id: u64,
    scripts: &[&[u8]],
    rows_per_segment: u64,
    slots: u64,
) -> Result<(Placement, Vec<u32>), String> {
    let mut segments = 1u32;
    loop {
        let rows = (rows_per_segment * segments as u64) as usize;
        // Occupancy by row, holding the index of each script placed there.
        // Indices rather than bytes because relocation has to read a resident
        // entry's *other* candidate, which means re-deriving its hash.
        let mut occupants: Vec<Vec<u32>> = vec![Vec::new(); rows];
        let mut assignment = vec![u32::MAX; scripts.len()];
        let mut overflowed: Option<&[u8]> = None;

        for (index, script) in scripts.iter().enumerate() {
            let candidates = candidate_rows(shard_id, script, rows as u64);
            let [a, b] = [candidates[0] as usize, candidates[1] as usize];

            // The cheap case, and the overwhelming majority: a candidate has
            // room. Less loaded first, ties to the first candidate, exactly as
            // the rule read before relocation existed.
            let direct = if occupants[a].len() <= occupants[b].len() {
                [a, b]
            } else {
                [b, a]
            };
            if let Some(&row) = direct
                .iter()
                .find(|&&row| occupants[row].len() < slots as usize)
            {
                occupants[row].push(index as u32);
                assignment[index] = row as u32;
                continue;
            }

            match relocate(
                shard_id,
                scripts,
                &mut occupants,
                &mut assignment,
                [a, b],
                slots,
            ) {
                Some(row) => {
                    occupants[row].push(index as u32);
                    assignment[index] = row as u32;
                }
                None => {
                    overflowed = Some(script);
                    break;
                }
            }
        }

        if let Some(script) = overflowed {
            segments += 1;
            if segments > MAX_DIRECTORY_SEGMENTS {
                return Err(hex::encode(script));
            }
            continue;
        }

        let max_row_load = occupants
            .iter()
            .map(|row| row.len() as u64)
            .max()
            .unwrap_or(0);
        return Ok((
            Placement {
                segments,
                max_row_load,
            },
            assignment,
        ));
    }
}

/// Rows a relocation search may visit before giving up and taking a segment.
///
/// A bound rather than a budget for the whole shard: the search is per script,
/// and the shortest path to a row with space is a handful of hops even at high
/// load. Reaching this means the two-choice graph is genuinely saturated
/// around both candidates, which another segment fixes and more searching does
/// not.
const MAX_RELOCATION_VISITS: usize = 512;

/// Frees a slot in one of `roots` by moving residents to their other candidate.
///
/// Breadth-first, so the path taken is the shortest available and the search
/// cannot cycle — a depth-first walk with a kick budget can revisit rows
/// forever at high load, and a random walk would not be reproducible. Ties are
/// broken by the order rows enter the frontier and, within a row, by the order
/// entries were placed, both of which are functions of the script set alone.
/// Two operators therefore relocate identically or the published digests stop
/// being comparable, which is the property this whole module exists to keep.
///
/// Returns the row left with space, having applied every move on the path.
fn relocate(
    shard_id: u64,
    scripts: &[&[u8]],
    occupants: &mut [Vec<u32>],
    assignment: &mut [u32],
    roots: [usize; 2],
    slots: u64,
) -> Option<usize> {
    let rows = occupants.len() as u64;
    // Each visited row remembers how it was reached: the row before it, and
    // the entry that would move between them. The root rows have no such edge.
    let mut came_from: std::collections::HashMap<usize, (usize, u32)> = Default::default();
    let mut queue: std::collections::VecDeque<usize> = Default::default();
    for root in roots {
        if came_from.insert(root, (usize::MAX, u32::MAX)).is_none() {
            queue.push_back(root);
        }
    }

    let mut visits = 0usize;
    while let Some(row) = queue.pop_front() {
        visits += 1;
        if visits > MAX_RELOCATION_VISITS {
            return None;
        }
        for &occupant in &occupants[row] {
            let candidates = candidate_rows(shard_id, scripts[occupant as usize], rows);
            let alternate = candidates
                .iter()
                .map(|alternate| *alternate as usize)
                .find(|alternate| *alternate != row)
                // A script whose two candidates collide has no alternate, so it
                // can never be moved out of the row it is in.
                .unwrap_or(row);
            if alternate == row || came_from.contains_key(&alternate) {
                continue;
            }
            came_from.insert(alternate, (row, occupant));
            if occupants[alternate].len() < slots as usize {
                // Walk the path back to a root, moving each entry forward as we
                // go, which leaves exactly one slot free in the root.
                let mut at = alternate;
                while let Some(&(previous, moved)) = came_from.get(&at) {
                    if previous == usize::MAX {
                        return Some(at);
                    }
                    let position = occupants[previous]
                        .iter()
                        .position(|entry| *entry == moved)
                        .expect("the entry was read from that row");
                    occupants[previous].remove(position);
                    occupants[at].push(moved);
                    assignment[moved as usize] = at as u32;
                    at = previous;
                }
                return Some(at);
            }
            queue.push_back(alternate);
        }
    }
    None
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
    let scripts_only: Vec<&[u8]> = entries
        .iter()
        .map(|entry| entry.script.as_slice())
        .collect();
    let (placed, assignment) = place_scripts(
        shard_id,
        &scripts_only,
        DIRECTORY_ROWS as u64,
        crate::records::DIRECTORY_SLOTS as u64,
    )
    .map_err(|script| BuildError::DirectoryFull { script })?;
    let segments = placed.segments;
    let rows = DIRECTORY_ROWS as u64 * segments as u64;
    // The placer's own assignment, not a second rule applied to the same
    // input. Relocation means a script's row is a function of every script
    // placed before it, so a greedy replay here would put entries in rows the
    // segment count was not decided for — and only some of them, which is the
    // kind of disagreement that produces a table a wallet cannot read.
    let mut buckets: Vec<Vec<&DirectoryEntry>> = vec![Vec::new(); rows as usize];
    for (entry, row) in entries.iter().zip(&assignment) {
        buckets[*row as usize].push(entry);
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
    Ok((tables, scripts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_row::{decode_page_row, PageEntry};
    use crate::records::decode_directory_row;

    /// The one entry in `row` belonging to `script`, which is what a wallet
    /// does with a packed row: several scripts share it, and the rest are
    /// discarded.
    fn fragment_of(row: &[u8], script: &[u8], ordinal: u32) -> PageEntry {
        let mut found: Vec<PageEntry> = decode_page_row(row)
            .expect("a located row decodes")
            .into_iter()
            .filter(|entry| entry.script == script && entry.ordinal == ordinal)
            .collect();
        assert_eq!(found.len(), 1, "exactly one entry may claim a fragment");
        found.pop().expect("one")
    }
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

    /// Short histories of the same length share rows, and every class from one
    /// paged event to a full fragment does it at its own capacity. This is the
    /// whole point of the format, so it is checked per class rather than
    /// sampled.
    #[test]
    fn short_histories_of_a_length_share_rows() {
        for p in 1..=crate::layout::EVENTS_PER_PAGE {
            let per_row = entries_per_row(p) as usize;
            let scripts = per_row as u32 * 2 + 1;
            let built = build(&fixture(scripts, p + INLINE_EVENTS));

            // Three rows: two full and one holding the remainder.
            assert_eq!(
                built.page_rows,
                (scripts as usize).div_ceil(per_row) as u64,
                "class {p} did not pack {scripts} scripts at {per_row} per row"
            );
            assert_eq!(built.fragments, scripts as u64, "class {p} fragments");

            // Every script is still retrievable, and only from the row its
            // directory entry names.
            let rows = DIRECTORY_ROWS as u64 * built.directory_segments() as u64;
            for tag in 0..scripts {
                let wanted = script(tag);
                let entry = candidate_rows(0, wanted.as_slice(), rows)
                    .iter()
                    .find_map(|row| {
                        decode_directory_row(built.directory_row(*row))
                            .unwrap()
                            .into_iter()
                            .find(|e| e.script == wanted.as_slice())
                    })
                    .expect("entry");
                assert_eq!(entry.page_count, 1, "class {p} should be one fragment");
                let fragment = fragment_of(
                    built.page_row(entry.first_page as u64),
                    wanted.as_slice(),
                    0,
                );
                assert_eq!(fragment.events.len(), p as usize);
            }
        }
    }

    /// Two scripts sharing a row are both there, at the same locator, each
    /// finding only its own events. A reader has to select within the row.
    #[test]
    fn two_scripts_can_name_the_same_row() {
        let built = build(&fixture(2, 1 + INLINE_EVENTS));
        assert_eq!(built.page_rows, 1, "two one-event histories share a row");

        let rows = DIRECTORY_ROWS as u64 * built.directory_segments() as u64;
        let mut located = Vec::new();
        for tag in 0..2u32 {
            let wanted = script(tag);
            let entry = candidate_rows(0, wanted.as_slice(), rows)
                .iter()
                .find_map(|row| {
                    decode_directory_row(built.directory_row(*row))
                        .unwrap()
                        .into_iter()
                        .find(|e| e.script == wanted.as_slice())
                })
                .expect("entry");
            located.push(entry.first_page);
            let fragment = fragment_of(
                built.page_row(entry.first_page as u64),
                wanted.as_slice(),
                0,
            );
            assert_eq!(fragment.events.len(), 1);
        }
        assert_eq!(located[0], located[1], "both should name the same row");
        assert_eq!(
            decode_page_row(built.page_row(located[0] as u64))
                .unwrap()
                .len(),
            2,
            "the shared row should hold both"
        );
    }

    /// A history past one fragment takes a contiguous run to itself and shares
    /// with nothing, including its short final fragment. Sharing that would
    /// make a long history's placement depend on unrelated content.
    #[test]
    fn a_long_history_keeps_its_run_to_itself() {
        let per = crate::layout::EVENTS_PER_PAGE * 2 + 5 + INLINE_EVENTS;
        // One long history and enough one-event ones to fill a row beside it.
        let mut events = fixture(1, per);
        events.extend(
            fixture(entries_per_row(1), 1 + INLINE_EVENTS)
                .into_iter()
                .map(|(s, e)| (ScriptBytes::new([b"short-", s.as_slice()].concat()), e)),
        );
        let built = build(&events);

        let wanted = script(0);
        let rows = DIRECTORY_ROWS as u64 * built.directory_segments() as u64;
        let entry = candidate_rows(0, wanted.as_slice(), rows)
            .iter()
            .find_map(|row| {
                decode_directory_row(built.directory_row(*row))
                    .unwrap()
                    .into_iter()
                    .find(|e| e.script == wanted.as_slice())
            })
            .expect("entry");
        assert_eq!(entry.page_count, 3, "two full fragments and a short one");
        for ordinal in 0..entry.page_count {
            let row = decode_page_row(built.page_row((entry.first_page + ordinal) as u64)).unwrap();
            assert_eq!(row.len(), 1, "fragment {ordinal} shared its row");
            assert_eq!(row[0].script, wanted.as_slice());
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
        // A full fragment is one entry per row even packed, so this shard
        // needs a row per script exactly as the unpacked layout did. That is
        // the boundary case: one event more and the history is long, one less
        // and it starts sharing.
        assert_eq!(built.page_rows, scripts as u64);
        assert_eq!(built.fragments, scripts as u64);
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
            let fragment = fragment_of(
                built.page_row(entry.first_page as u64),
                wanted.as_slice(),
                0,
            );
            assert_eq!(fragment.script, wanted.as_slice());
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

    /// Scripts for a placement test, distinct and of the supported width.
    fn placement_set(count: u32) -> Vec<Vec<u8>> {
        (0..count)
            .map(|tag| script(tag).as_slice().to_vec())
            .collect()
    }

    fn place(count: u32, rows: u64, slots: u64) -> Placement {
        let owned = placement_set(count);
        let refs: Vec<&[u8]> = owned.iter().map(|s| s.as_slice()).collect();
        place_scripts(0, &refs, rows, slots)
            .expect("placement terminates")
            .0
    }

    /// Relocation is what decides how full the directory may run.
    ///
    /// Without it a script never moves once placed, so the first row to reach
    /// its slot count ends the placement and costs the shard a whole segment —
    /// which every wallet touching that shard then pays for, twice, in queries
    /// and in setup. The measured ceiling for that rule was around 58% of slot
    /// capacity. Following an augmenting path instead keeps one segment far
    /// past it, at no cost to the client, which still reads the same two
    /// candidate rows.
    #[test]
    fn relocation_keeps_one_segment_past_the_greedy_ceiling() {
        let rows = 2_048u64;
        let slots = crate::records::DIRECTORY_SLOTS as u64;
        let capacity = rows * slots;

        // 86% of capacity, the load at which the shipped seal target overflowed
        // 42% of shards on the real journal under the greedy rule.
        let placed = place((capacity * 6 / 7) as u32, rows, slots);
        assert_eq!(
            placed.segments, 1,
            "relocation should hold 86% of capacity in one segment"
        );
        assert!(placed.max_row_load <= slots);
    }

    /// Placement must not depend on anything but the script set, because two
    /// operators disagreeing here publish different bytes under the same
    /// identity. Relocation makes this sharper than it was: a script's row now
    /// depends on every script placed before it, so a search that explored in a
    /// different order would silently produce a different table.
    #[test]
    fn placement_is_reproducible() {
        let rows = 2_048u64;
        let slots = crate::records::DIRECTORY_SLOTS as u64;
        let owned = placement_set((rows * slots * 6 / 7) as u32);
        let refs: Vec<&[u8]> = owned.iter().map(|s| s.as_slice()).collect();

        let first = place_scripts(0, &refs, rows, slots).expect("placement");
        let again = place_scripts(0, &refs, rows, slots).expect("placement");
        assert_eq!(first.0, again.0);
        assert_eq!(first.1, again.1, "the same input must place identically");
    }

    /// Every script must still be in one of its own two candidate rows. This is
    /// the only thing a wallet relies on, and the one property relocation could
    /// plausibly break: an entry that moved has to land on *its* alternate, not
    /// on a row that happened to have space.
    #[test]
    fn a_relocated_script_is_still_in_one_of_its_candidates() {
        let rows = 2_048u64;
        let slots = crate::records::DIRECTORY_SLOTS as u64;
        let owned = placement_set((rows * slots * 6 / 7) as u32);
        let refs: Vec<&[u8]> = owned.iter().map(|s| s.as_slice()).collect();

        let (placed, assignment) = place_scripts(0, &refs, rows, slots).expect("placement");
        let span = rows * placed.segments as u64;
        let mut occupied = vec![0u64; span as usize];
        for (script, row) in refs.iter().zip(&assignment) {
            let candidates = candidate_rows(0, script, span);
            assert!(
                candidates.contains(&u64::from(*row)),
                "a script was placed outside both of its candidates"
            );
            occupied[*row as usize] += 1;
        }
        assert!(
            occupied.iter().all(|load| *load <= slots),
            "a row was filled past its slot count"
        );
        assert_eq!(assignment.len(), refs.len(), "every script is placed once");
    }

    /// What the sealer predicts a shard's placement costs must be what the
    /// builder then produces.
    ///
    /// The census reports the sealer's figure and a geometry is chosen from it,
    /// so a divergence would mean choosing against a number no published set
    /// ever realises. Relocation makes this sharp: a script's row now depends on
    /// every script placed before it, so the two must agree not merely on how
    /// many segments but on the script set and the order it is placed in — the
    /// sealer sorts its own map, the builder walks a `BTreeMap`, and nothing but
    /// this test says those are the same sequence.
    #[test]
    fn the_sealer_predicts_the_placement_the_builder_produces() {
        use crate::seal::{Limit, PageBasis, SealPolicy, Sealer};

        let events = fixture(4_000, 3);
        let policy = SealPolicy {
            scripts: Limit::new(1_000_000, 2_000_000).expect("valid"),
            page_rows: Limit::new(1_000_000, 2_000_000).expect("valid"),
        };
        let mut sealer = Sealer::with_basis(policy, 100, PageBasis::default());
        sealer.measure_placement(true);
        // The sealer takes blocks in height order; the builder takes the range
        // whole. Same events either way, which is the point.
        for height in 100..=200u64 {
            let block: Vec<_> = events
                .iter()
                .filter(|(_, event)| u64::from(event.height()) == height)
                .cloned()
                .collect();
            sealer.push_block(height, &block).expect("valid block");
        }
        let predicted = sealer
            .finish()
            .expect("a tail")
            .placement
            .expect("placement was asked for");

        let built = build(&events);
        assert_eq!(
            predicted.segments,
            built.directory_segments(),
            "the sealer and the builder disagree about segments"
        );
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
                let fragment = fragment_of(
                    built.page_row((entry.first_page + ordinal) as u64),
                    wanted.as_slice(),
                    ordinal,
                );
                assert_eq!(fragment.fragment_count, entry.page_count);
                recovered.extend(fragment.events);
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
        let fragment = fragment_of(
            built.page_row(entry.first_page as u64),
            wanted.as_slice(),
            0,
        );
        let latest_paged = fragment
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
