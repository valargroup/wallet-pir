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

use crate::layout::{segments_for, Geometry, PackedDemand, INLINE_EVENTS};
use crate::packing::{self, HistoryLayout, ROW_PAYLOAD};
use crate::page_row::{encode_page_row, PageEntry};
use crate::records::{encode_directory_row, DirectoryEntry, RecordError, MAX_SCRIPT_BYTES};
use crate::tag::{resolve_tag_salt, script_tag};
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap};
use transparent_events::TransparentEvent;
use transparent_filter::{
    build_range_filter_for, BlockHash, FilterBytes, FilterError, ScriptBytes, ShardKey,
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
    /// The geometry this shard was built at.
    ///
    /// Carried rather than assumed, because a set may mix geometries: archive
    /// shards before the recent cutoff and recent shards after it. Every offset
    /// below is taken against this, so a shard cannot be read at the wrong row
    /// count by a caller that happens to hold a different default.
    pub geometry: &'static Geometry,
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
    /// Which candidate row holds each placed script, for a single-lookup
    /// directory. See [`crate::choice`].
    ///
    /// Always built, because it is a function of the placement just made and
    /// costs a fraction of it; the publisher decides whether to publish it.
    /// `None` only if no seed peeled, which leaves the shard on two queries
    /// rather than failing the build.
    pub choice: Option<crate::choice::ChoiceTable>,
    /// Scripts in the filter but not the directory, because they exceed
    /// [`MAX_SCRIPT_BYTES`].
    ///
    /// Published so the coverage gap is measured rather than silent. A wallet
    /// holding such a script is outside coverage and must not read a directory
    /// miss as absence.
    pub excluded_scripts: u64,
    /// The tag-salt counter this build settled on. Zero unless a collision
    /// forced a rebuild.
    pub tag_salt_counter: u32,
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
        let (segment, within) = crate::layout::split_row(row, self.geometry.directory_rows);
        let width = self.geometry.directory_row_bytes;
        let at = within as usize * width;
        &self.directory[segment as usize][at..at + width]
    }

    /// One row of the shard's logical page space, which a directory extent
    /// indexes across segment boundaries as if it were one table.
    pub fn page_row(&self, page: u64) -> &[u8] {
        let (segment, within) = crate::layout::split_row(page, self.geometry.page_rows);
        let width = self.geometry.page_row_bytes;
        let at = within as usize * width;
        &self.pages[segment as usize][at..at + width]
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
    candidates(&bucket_salts(shard_id), script, rows)
}

/// A shard's bucket salts, for deriving many scripts' candidate rows.
fn bucket_salts(shard_id: u64) -> [[u8; 32]; DIRECTORY_CHOICES] {
    std::array::from_fn(|choice| bucket_salt(shard_id, choice))
}

/// [`candidate_rows`] under salts already derived for the shard.
fn candidates(
    salts: &[[u8; 32]; DIRECTORY_CHOICES],
    script: &[u8],
    rows: u64,
) -> [u64; DIRECTORY_CHOICES] {
    std::array::from_fn(|choice| bucket_for(&salts[choice], script, rows))
}

/// Descending encoded directory size, with raw script bytes breaking ties.
/// Both the builder and census feed this order to byte-based placement.
pub fn placement_order<'a>(
    scripts: impl IntoIterator<Item = (&'a [u8], usize)>,
) -> Vec<(&'a [u8], usize)> {
    let mut entries: Vec<_> = scripts.into_iter().collect();
    entries.sort_by(|a, b| (Reverse(a.1), a.0).cmp(&(Reverse(b.1), b.0)));
    entries
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
    geometry: &'static Geometry,
    events: &[(ScriptBytes, TransparentEvent)],
) -> Result<BuiltShard, BuildError> {
    // A geometry this build cannot encode would produce tables no wallet could
    // read, so it is refused before any bytes are written rather than after.
    geometry
        .validate_publishable()
        .map_err(BuildError::Invalid)?;
    // Group by script, in a sorted map so the walk order is the scripts' own
    // order rather than a hash map's.
    let mut by_script: BTreeMap<&[u8], Vec<TransparentEvent>> = BTreeMap::new();
    for (script, event) in events {
        if event.height() < start_height as u32 || event.height() > end_height as u32 {
            return Err(BuildError::Invalid(format!(
                "event at height {} is outside the shard's {start_height}-{end_height}",
                event.height()
            )));
        }
        by_script.entry(script.as_slice()).or_default().push(*event);
    }

    // The filter covers every element, including scripts the private tables
    // cannot index. Excluding them here would tell a wallet holding one that
    // its script had no activity, which is worse than telling it the script is
    // outside coverage.
    let filter_elements: Vec<ScriptBytes> = by_script
        .keys()
        .map(|script| ScriptBytes::new(script.to_vec()))
        .collect();
    let key = ShardKey::derive(
        profile,
        genesis,
        shard_id,
        start_height,
        end_height,
        terminal_block_hash,
    );
    // The profile named in the map and manifest fixes the filter's P and M; a
    // name this build does not know is refused rather than encoded under
    // another profile's parameters.
    let range = transparent_filter::range_profile(profile)
        .ok_or_else(|| BuildError::Invalid(format!("unknown range profile {profile:?}")))?;
    let filter = build_range_filter_for(range, key, &filter_elements)?;

    // Summarize histories with exactly the same incremental rule as sealing.
    let mut excluded_scripts = 0u64;
    let mut total_events = 0u64;
    let mut demand = PackedDemand::default();
    let mut histories = BTreeMap::new();
    for (script, mut history) in by_script {
        total_events += history.len() as u64;
        if script.len() > MAX_SCRIPT_BYTES {
            excluded_scripts += 1;
            continue;
        }
        history.sort_by_key(TransparentEvent::sort_key);
        let mut summary = HistoryLayout::default();
        for event in &history {
            summary.push(*event, INLINE_EVENTS);
        }
        demand.shift(&HistoryLayout::default(), &summary);
        histories.insert(script, (history, summary));
    }
    let indexed: Vec<&[u8]> = histories.keys().copied().collect();
    let (tag_salt_counter, salt) =
        resolve_tag_salt(shard_id, &terminal_block_hash, &indexed, script_tag)
            .map_err(BuildError::Invalid)?;
    let mut rows: Vec<Vec<PageEntry>> = Vec::new();
    let mut first_page: HashMap<&[u8], u32> = HashMap::new();
    let mut short = Vec::new();
    let mut free: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
    let mixed = demand.use_mixed();
    // Long runs are allocated in script order and remain contiguous. Only their
    // last row is offered to single-fragment histories.
    for (&script, (history, summary)) in &histories {
        let older = &history[..history.len().saturating_sub(INLINE_EVENTS as usize)];
        let fragments = packing::fragments(older);
        if fragments.len() == 1 {
            let entry = PageEntry::new(script_tag(&salt, script), 0, 1, older.to_vec())?;
            short.push((script, entry));
        } else if fragments.len() > 1 {
            first_page.insert(
                script,
                u32::try_from(rows.len() + 1)
                    .map_err(|_| BuildError::Invalid("page locator overflow".into()))?,
            );
            for (ordinal, events) in fragments.iter().enumerate() {
                rows.push(vec![PageEntry::new(
                    script_tag(&salt, script),
                    ordinal as u32,
                    summary.fragments,
                    events.to_vec(),
                )?]);
            }
            if mixed {
                let row = rows.len() - 1;
                free.entry(ROW_PAYLOAD - rows[row][0].encoded_len())
                    .or_default()
                    .insert(row);
            }
        }
    }
    short.sort_by(|a, b| (Reverse(a.1.encoded_len()), a.0).cmp(&(Reverse(b.1.encoded_len()), b.0)));
    let mut last_size = None;
    for (script, entry) in short {
        let size = entry.encoded_len();
        if !mixed && last_size != Some(size) {
            free.clear();
        }
        last_size = Some(size);
        let (space, row) = if let Some((&space, indices)) = free.range(size..).next() {
            let row = *indices.first().expect("nonempty bin set");
            let indices = free.get_mut(&space).expect("existing bin set");
            indices.remove(&row);
            if indices.is_empty() {
                free.remove(&space);
            }
            (space, row)
        } else {
            rows.push(Vec::new());
            (ROW_PAYLOAD, rows.len() - 1)
        };
        first_page.insert(
            script,
            u32::try_from(row + 1)
                .map_err(|_| BuildError::Invalid("page locator overflow".into()))?,
        );
        rows[row].push(entry);
        free.entry(space - size).or_default().insert(row);
    }
    if rows.len() as u64 != demand.rows() {
        return Err(BuildError::Invalid(format!(
            "emitted {} rows, predicted {}",
            rows.len(),
            demand.rows()
        )));
    }
    let mut entries: Vec<_> = histories
        .iter()
        .map(|(&script, (_, summary))| DirectoryEntry {
            script: script.to_vec(),
            tag: script_tag(&salt, script),
            event_count: summary.events,
            inline: summary.inline.clone(),
            first_page: first_page.get(script).copied().unwrap_or(0),
        })
        .collect();
    entries.sort_by(|a, b| {
        (Reverse(a.encoded_len()), &a.script).cmp(&(Reverse(b.encoded_len()), &b.script))
    });
    let fragments = demand.fragments();

    // A shard takes as many page segments as its rows need. Failing instead
    // would mean one oversized block could stop publication.
    let page_segments = segments_for(rows.len() as u64, geometry.page_rows);
    let page_rows = rows.len() as u64;
    let segment_bytes = geometry.page_rows as usize * geometry.page_row_bytes;
    let mut page_table = Vec::with_capacity(page_segments as usize * segment_bytes);
    for row in &rows {
        page_table.extend_from_slice(&encode_page_row(row)?);
    }
    // Unused page rows are zero and decode as empty. They are indistinguishable
    // in a response from occupied ones, which is the point.
    page_table.resize(page_segments as usize * segment_bytes, 0);
    let page_table = page_table
        .chunks(segment_bytes)
        .map(<[u8]>::to_vec)
        .collect();

    let (directory, scripts, choice) = place_directory(shard_id, entries, geometry)?;

    Ok(BuiltShard {
        shard_id,
        geometry,
        start_height,
        end_height,
        filter,
        directory,
        pages: page_table,
        scripts,
        choice,
        page_rows,
        fragments,
        events: total_events,
        excluded_scripts,
        tag_salt_counter,
    })
}

/// What two-choice placement costs a script set.
///
/// Shared by the builder and census. Byte density alone is a lower bound;
/// this reports the actual weighted two-choice placement and segment count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Placement {
    /// Segments the placement rule needed.
    pub segments: u32,
    /// Entries in the fullest row, for diagnostics. Bytes constrain capacity.
    pub max_row_load: u64,
    /// Maximum occupied payload bytes in any row.
    pub max_row_bytes: u64,
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
    sizes: &[usize],
    rows_per_segment: u64,
    capacity: usize,
) -> Result<(Placement, Vec<u32>), String> {
    assert_eq!(scripts.len(), sizes.len());
    if let Some(i) = sizes.iter().position(|size| *size == 0 || *size > capacity) {
        return Err(hex::encode(scripts[i]));
    }
    let salts = bucket_salts(shard_id);
    let mut segments = 1;
    loop {
        let rows = (rows_per_segment * u64::from(segments)) as usize;
        let mut occupants: Vec<Vec<u32>> = vec![Vec::new(); rows];
        let mut used = vec![0usize; rows];
        let mut assignment = vec![u32::MAX; scripts.len()];
        let mut overflowed = None;
        for (index, script) in scripts.iter().enumerate() {
            let [a, b] = candidates(&salts, script, rows as u64).map(|row| row as usize);
            let direct = if used[a] <= used[b] { [a, b] } else { [b, a] };
            let row = direct
                .into_iter()
                .find(|row| used[*row] + sizes[index] <= capacity)
                .or_else(|| {
                    relocate(
                        &salts,
                        scripts,
                        sizes,
                        &mut occupants,
                        &mut used,
                        &mut assignment,
                        [a, b],
                        sizes[index],
                        capacity,
                    )
                });
            if let Some(row) = row {
                occupants[row].push(index as u32);
                used[row] += sizes[index];
                assignment[index] = row as u32;
            } else {
                overflowed = Some(script);
                break;
            }
        }
        if let Some(script) = overflowed {
            segments += 1;
            if segments > MAX_DIRECTORY_SEGMENTS {
                return Err(hex::encode(script));
            }
        } else {
            return Ok((
                Placement {
                    segments,
                    max_row_load: occupants.iter().map(|r| r.len() as u64).max().unwrap_or(0),
                    max_row_bytes: used.iter().copied().max().unwrap_or(0) as u64,
                },
                assignment,
            ));
        }
    }
}

/// The choice table for a placement: which candidate row each script took.
///
/// `scripts` and `assignment` are what [`place_scripts`] was given and
/// returned, and `rows` is the logical row space it settled on. A script whose
/// two candidates name the same row takes bit 0; either answer names that row.
pub fn choice_table(
    shard_id: u64,
    scripts: &[&[u8]],
    assignment: &[u32],
    rows: u64,
) -> Result<crate::choice::ChoiceTable, crate::choice::ChoiceError> {
    let salts = bucket_salts(shard_id);
    let entries: Vec<(&[u8], u8)> = scripts
        .iter()
        .zip(assignment)
        .map(|(script, row)| {
            let [first, _] = candidates(&salts, script, rows);
            (*script, u8::from(u64::from(*row) != first))
        })
        .collect();
    crate::choice::ChoiceTable::build(shard_id, &entries)
}

/// Checks that `table` names the row each script is held in.
///
/// `row_of(i)` is the logical row holding `scripts[i]`, over `rows` rows.
/// Returns the index of the first script it misroutes, or `None` if it routes
/// every one.
pub fn verify_choice(
    shard_id: u64,
    table: &crate::choice::ChoiceTable,
    scripts: &[&[u8]],
    rows: u64,
    row_of: impl Fn(usize) -> u64,
) -> Option<usize> {
    let salts = bucket_salts(shard_id);
    scripts.iter().enumerate().position(|(index, script)| {
        candidates(&salts, script, rows)[table.choice(shard_id, script)] != row_of(index)
    })
}

/// Rows a relocation search may visit before giving up and taking a segment.
///
/// A bound rather than a budget for the whole shard: the search is per script,
/// and the shortest path to a row with space is a handful of hops even at high
/// load. The bound and single-resident moves may miss a feasible packing;
/// retrying with another segment preserves correctness in that case.
const MAX_RELOCATION_VISITS: usize = 512;

/// Frees enough bytes in a root by moving residents to their other candidate.
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
#[allow(clippy::too_many_arguments)]
fn relocate(
    salts: &[[u8; 32]; DIRECTORY_CHOICES],
    scripts: &[&[u8]],
    sizes: &[usize],
    occupants: &mut [Vec<u32>],
    used: &mut [usize],
    assignment: &mut [u32],
    roots: [usize; 2],
    incoming: usize,
    capacity: usize,
) -> Option<usize> {
    // Each edge moves one resident large enough to satisfy its predecessor's
    // byte deficit. A path may never revisit a row. Failed searches mutate nothing.
    struct Node {
        row: usize,
        need: usize,
        parent: Option<(usize, u32)>,
    }
    let mut nodes = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for row in roots {
        if seen.insert((row, incoming)) {
            nodes.push(Node {
                row,
                need: incoming,
                parent: None,
            });
        }
    }
    let mut cursor = 0;
    while cursor < nodes.len() && cursor < MAX_RELOCATION_VISITS {
        let row = nodes[cursor].row;
        let need = nodes[cursor].need;
        if used[row] + need <= capacity {
            let mut at = cursor;
            while let Some((parent, moved)) = nodes[at].parent {
                let from = nodes[parent].row;
                let to = nodes[at].row;
                let position = occupants[from]
                    .iter()
                    .position(|e| *e == moved)
                    .expect("resident");
                occupants[from].remove(position);
                occupants[to].push(moved);
                used[from] -= sizes[moved as usize];
                used[to] += sizes[moved as usize];
                assignment[moved as usize] = to as u32;
                at = parent;
            }
            return Some(nodes[at].row);
        }
        for &resident in &occupants[row] {
            let size = sizes[resident as usize];
            if used[row] - size + need > capacity {
                continue;
            }
            let alternate = candidates(salts, scripts[resident as usize], occupants.len() as u64)
                .into_iter()
                .map(|r| r as usize)
                .find(|r| *r != row);
            let Some(alternate) = alternate else {
                continue;
            };
            let mut ancestor = Some(cursor);
            let mut cyclic = false;
            while let Some(at) = ancestor {
                if nodes[at].row == alternate {
                    cyclic = true;
                    break;
                }
                ancestor = nodes[at].parent.map(|(p, _)| p);
            }
            if !cyclic && seen.insert((alternate, size)) && nodes.len() < MAX_RELOCATION_VISITS {
                nodes.push(Node {
                    row: alternate,
                    need: size,
                    parent: Some((cursor, resident)),
                });
            }
        }
        cursor += 1;
    }
    None
}

/// A placed directory: one encoded table per segment, the number of scripts
/// placed, and the choice table for the placement.
type PlacedDirectory = (Vec<Vec<u8>>, u64, Option<crate::choice::ChoiceTable>);

/// Checks the encoded bytes, not the placer's intent.
///
/// Rows carry tags, so a server cannot recompute a script's route; this is
/// the last point where the scripts are known. Every entry's tag must decode
/// from the row its assignment names, no tag may appear twice in the shard,
/// and the rows must hold exactly the entries placed. A miss here would reach
/// a wallet as absence, which it cannot tell from an empty history.
fn verify_encoded_rows(
    tables: &[Vec<u8>],
    entries: &[DirectoryEntry],
    assignment: &[u32],
    geometry: &Geometry,
) -> Result<(), BuildError> {
    let per_segment = geometry.directory_rows as usize;
    let row_bytes = geometry.directory_row_bytes;
    let mut rows: Vec<Vec<[u8; crate::tag::SCRIPT_TAG_BYTES]>> =
        Vec::with_capacity(per_segment * tables.len());
    let mut seen = std::collections::HashSet::with_capacity(entries.len());
    for table in tables {
        for raw in table.chunks_exact(row_bytes) {
            let decoded = crate::records::decode_directory_row(raw)?;
            let tags: Vec<_> = decoded.into_iter().map(|entry| entry.tag).collect();
            for tag in &tags {
                if !seen.insert(*tag) {
                    return Err(BuildError::Invalid(
                        "a script tag appears twice in the directory".into(),
                    ));
                }
            }
            rows.push(tags);
        }
    }
    if seen.len() != entries.len() {
        return Err(BuildError::Invalid(format!(
            "the directory holds {} entries, {} were placed",
            seen.len(),
            entries.len()
        )));
    }
    for (entry, row) in entries.iter().zip(assignment) {
        let held = rows
            .get(*row as usize)
            .is_some_and(|tags| tags.contains(&entry.tag));
        if !held {
            return Err(BuildError::Invalid(format!(
                "script {} is not in its assigned row {row}",
                hex::encode(&entry.script)
            )));
        }
    }
    Ok(())
}

/// Returns the encoded directory and its choice table.
fn place_directory(
    shard_id: u64,
    entries: Vec<DirectoryEntry>,
    geometry: &Geometry,
) -> Result<PlacedDirectory, BuildError> {
    let scripts_only: Vec<&[u8]> = entries
        .iter()
        .map(|entry| entry.script.as_slice())
        .collect();
    let (placed, assignment) = place_scripts(
        shard_id,
        &scripts_only,
        &entries
            .iter()
            .map(DirectoryEntry::encoded_len)
            .collect::<Vec<_>>(),
        geometry.directory_rows,
        geometry.directory_row_bytes - crate::records::DIRECTORY_ROW_HEADER_BYTES,
    )
    .map_err(|script| BuildError::DirectoryFull { script })?;
    let segments = placed.segments;
    let rows = geometry.directory_rows * segments as u64;
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
    for segment in buckets.chunks_mut(geometry.directory_rows as usize) {
        let mut table =
            Vec::with_capacity(geometry.directory_rows as usize * geometry.directory_row_bytes);
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
    verify_encoded_rows(&tables, &entries, &assignment, geometry)?;
    let choice = choice_table(shard_id, &scripts_only, &assignment, rows).ok();
    if let Some(table) = &choice {
        // A wrong bit is not an error a wallet can see: it queries the other
        // row, finds no entry, and reads the script as absent. So every placed
        // script is checked before the table can be published.
        if let Some(index) = verify_choice(shard_id, table, &scripts_only, rows, |i| {
            u64::from(assignment[i])
        }) {
            return Err(BuildError::Invalid(format!(
                "choice table sends script {} to the wrong row",
                hex::encode(scripts_only[index])
            )));
        }
    }
    Ok((tables, scripts, choice))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{DIRECTORY_ROWS, PAGE_ROWS, RECENT_8K};
    use crate::page_row::{decode_page_row, PageEntry};
    use crate::records::decode_directory_row;

    /// The one entry in `row` belonging to `tag`, which is what a wallet
    /// does with a packed row: several scripts share it, and the rest are
    /// discarded.
    fn fragment_of(
        row: &[u8],
        tag: &[u8; crate::tag::SCRIPT_TAG_BYTES],
        ordinal: u32,
    ) -> PageEntry {
        let mut found: Vec<PageEntry> = decode_page_row(row)
            .expect("a located row decodes")
            .into_iter()
            .filter(|entry| &entry.tag == tag && entry.ordinal == ordinal)
            .collect();
        assert_eq!(found.len(), 1, "exactly one entry may claim a fragment");
        found.pop().expect("one")
    }

    fn tag_for(script: &[u8]) -> [u8; crate::tag::SCRIPT_TAG_BYTES] {
        crate::script_tag(&crate::tag_salt(0, &terminal(), 0), script)
    }

    fn page_at(entry: &crate::records::DirectoryEntry) -> u64 {
        u64::from(entry.page_base().expect("a paged entry"))
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
            metadata: None,
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
    /// The builder's last check reads the encoded rows: an entry outside its
    /// assigned row, a tag held twice, or a missing entry is refused before
    /// publication, because a wallet would read any of them as absence.
    #[test]
    fn encoded_rows_must_hold_every_entry_where_it_was_assigned() {
        let entry = |byte: u8| DirectoryEntry {
            script: vec![0x76, byte],
            tag: [byte; crate::tag::SCRIPT_TAG_BYTES],
            event_count: 1,
            inline: vec![event(100, u32::from(byte))],
            first_page: 0,
        };
        let geometry = RECENT_8K;
        let (a, b) = (entry(1), entry(2));
        let table = |rows: &[&[DirectoryEntry]]| -> Vec<Vec<u8>> {
            vec![rows
                .iter()
                .flat_map(|row| encode_directory_row(row).unwrap())
                .collect()]
        };
        let entries = [a.clone(), b.clone()];
        let (one_a, one_b) = (std::slice::from_ref(&a), std::slice::from_ref(&b));
        let good = table(&[one_a, one_b]);
        verify_encoded_rows(&good, &entries, &[0, 1], &geometry).unwrap();
        assert!(verify_encoded_rows(&good, &entries, &[1, 0], &geometry).is_err());
        let repeated = table(&[one_a, one_a]);
        assert!(verify_encoded_rows(&repeated, &entries, &[0, 1], &geometry).is_err());
        let missing = table(&[one_a, &[]]);
        assert!(verify_encoded_rows(&missing, &entries, &[0, 1], &geometry).is_err());
    }

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
        build_shard(
            0,
            100,
            200,
            genesis(),
            terminal(),
            RANGE_PROFILE,
            &RECENT_8K,
            events,
        )
        .expect("build")
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
        for p in 1..=79 {
            let per_row = 4092 / (34 + p as usize * 51);
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
                            .find(|e| e.tag == tag_for(wanted.as_slice()))
                    })
                    .expect("entry");
                let fragment = fragment_of(
                    built.page_row(page_at(&entry)),
                    &tag_for(wanted.as_slice()),
                    0,
                );
                assert_eq!(
                    fragment.fragment_count, 1,
                    "class {p} should be one fragment"
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
                        .find(|e| e.tag == tag_for(wanted.as_slice()))
                })
                .expect("entry");
            located.push(page_at(&entry));
            let fragment = fragment_of(
                built.page_row(page_at(&entry)),
                &tag_for(wanted.as_slice()),
                0,
            );
            assert_eq!(fragment.events.len(), 1);
        }
        assert_eq!(located[0], located[1], "both should name the same row");
        assert_eq!(
            decode_page_row(built.page_row(located[0])).unwrap().len(),
            2,
            "the shared row should hold both"
        );
    }

    /// A history past one fragment takes a contiguous run to itself and shares
    /// with nothing, including its short final fragment. Sharing that would
    /// make a long history's placement depend on unrelated content.
    #[test]
    fn a_long_history_keeps_its_run_and_shares_only_its_tail() {
        let per = crate::layout::EVENTS_PER_PAGE * 2 + 5 + INLINE_EVENTS;
        // One long history and enough one-event ones to fill a row beside it.
        let mut events = fixture(1, per);
        events.extend(
            fixture(48, 1 + INLINE_EVENTS)
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
                    .find(|e| e.tag == tag_for(wanted.as_slice()))
            })
            .expect("entry");
        let base = page_at(&entry);
        let first = fragment_of(built.page_row(base), &tag_for(wanted.as_slice()), 0);
        assert_eq!(
            first.fragment_count, 3,
            "two full fragments and a short one"
        );
        for ordinal in 0..first.fragment_count {
            let row = decode_page_row(built.page_row(base + u64::from(ordinal))).unwrap();
            if ordinal + 1 == first.fragment_count {
                assert!(row.len() > 1, "tail was not filled");
            } else {
                assert_eq!(row.len(), 1);
            }
            assert_eq!(row[0].tag, tag_for(wanted.as_slice()));
        }
    }

    /// The availability property at the builder: content that cannot fit one
    /// segment gets another, of the same pinned geometry, rather than failing.
    /// A shard that failed to build would stop publication altogether.
    #[test]
    fn content_too_large_for_one_segment_takes_another() {
        // Enough distinct histories to overrun the pinned page table, which is
        // what one oversized block looks like to the builder.
        let per = 79 + INLINE_EVENTS;
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
                        .find(|entry| entry.tag == tag_for(wanted.as_slice()))
                })
                .unwrap_or_else(|| panic!("script {tag} is in neither candidate row"));
            assert!(entry.first_page >= 1);
            if page_at(&entry) as usize >= PAGE_ROWS {
                crossed = true;
            }
            let fragment = fragment_of(
                built.page_row(page_at(&entry)),
                &tag_for(wanted.as_slice()),
                0,
            );
            assert_eq!(fragment.tag, tag_for(wanted.as_slice()));
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
                    .any(|entry| entry.tag == tag_for(wanted.as_slice()))
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
        place_scripts(0, &refs, &vec![1; refs.len()], rows, slots as usize)
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

        let first =
            place_scripts(0, &refs, &vec![1; refs.len()], rows, slots as usize).expect("placement");
        let again =
            place_scripts(0, &refs, &vec![1; refs.len()], rows, slots as usize).expect("placement");
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

        let (placed, assignment) =
            place_scripts(0, &refs, &vec![1; refs.len()], rows, slots as usize).expect("placement");
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

    /// Scripts of every shape: inline-only, short paged and long histories,
    /// interleaved in raw-byte order so that the builder's placement order
    /// and a lexicographic one differ.
    fn mixed_fixture(count: u32) -> Vec<(ScriptBytes, TransparentEvent)> {
        let mut events = Vec::new();
        for tag in 0..count {
            let per = [1, 2, 3, 5, 12, 30, 41, 90][(tag % 8) as usize];
            for i in 0..per {
                events.push((script(tag), event(100 + i % 101, tag * 100_000 + i)));
            }
        }
        events
    }

    /// The rows [`place_scripts`] assigns, fed [`placement_order`], must be the
    /// rows the builder publishes, entry for entry.
    ///
    /// Row membership rather than a load summary, because placement is order
    /// dependent and a census fed another order agrees on segment counts long
    /// before it agrees on which rows are full.
    #[test]
    fn placement_order_reproduces_the_published_rows() {
        let events = mixed_fixture(6_000);
        let built = build(&events);

        let mut counts: BTreeMap<Vec<u8>, u32> = BTreeMap::new();
        for (script, _) in &events {
            *counts.entry(script.as_slice().to_vec()).or_default() += 1;
        }
        let ordered = placement_order(
            counts
                .iter()
                .map(|(s, n)| (s.as_slice(), 19 + (*n).min(2) as usize * 51)),
        );
        let order: Vec<_> = ordered.iter().map(|(s, _)| *s).collect();
        let sizes: Vec<_> = ordered.iter().map(|(_, n)| *n).collect();
        let (placement, assignment) =
            place_scripts(0, &order, &sizes, RECENT_8K.directory_rows, 4092).expect("fits");
        assert_eq!(placement.segments, built.directory_segments());

        let rows = RECENT_8K.directory_rows * placement.segments as u64;
        let mut predicted: Vec<Vec<&[u8]>> = vec![Vec::new(); rows as usize];
        for (script, row) in order.iter().zip(&assignment) {
            predicted[*row as usize].push(script);
        }
        for (row, expected) in predicted.iter_mut().enumerate() {
            expected.sort_unstable();
            let mut expected_tags: Vec<_> = expected.iter().map(|script| tag_for(script)).collect();
            expected_tags.sort_unstable();
            let mut published: Vec<_> = decode_directory_row(built.directory_row(row as u64))
                .expect("decodes")
                .into_iter()
                .map(|entry| entry.tag)
                .collect();
            published.sort_unstable();
            assert_eq!(expected_tags, published, "row {row} differs");
        }

        // The lexicographic order the census used before is a different
        // placement on this content, which is why the order is shared.
        let mut lexicographic = order.clone();
        lexicographic.sort_unstable();
        let (_, other) = place_scripts(
            0,
            &lexicographic,
            &lexicographic
                .iter()
                .map(|s| 19 + counts[*s].min(2) as usize * 51)
                .collect::<Vec<_>>(),
            RECENT_8K.directory_rows,
            4092,
        )
        .expect("fits");
        let by_script = |order: &[&[u8]], rows: &[u32]| -> BTreeMap<Vec<u8>, u32> {
            order
                .iter()
                .map(|s| s.to_vec())
                .zip(rows.iter().copied())
                .collect()
        };
        assert_ne!(
            by_script(&order, &assignment),
            by_script(&lexicographic, &other)
        );
    }

    /// One row, chosen by the published choice bit, holds every placed script:
    /// the single-lookup property, at the default geometry and at the
    /// half-height directory, where placement runs near 80% of capacity.
    #[test]
    fn the_choice_bit_names_the_one_row_holding_each_script() {
        let rows = 4_096u64;
        let slots = crate::records::DIRECTORY_SLOTS as u64;
        for (count, rows) in [(6_000u32, 8_192u64), (45_454, rows)] {
            let owned = placement_set(count);
            let refs: Vec<&[u8]> = owned.iter().map(|s| s.as_slice()).collect();
            let (placement, assignment) =
                place_scripts(11, &refs, &vec![1; refs.len()], rows, slots as usize).expect("fits");
            // The largest recent shard of the September census fits one
            // half-height segment under two-choice placement with relocation.
            assert_eq!(placement.segments, 1, "{count} scripts at {rows} rows");
            let space = rows * placement.segments as u64;
            let table = choice_table(11, &refs, &assignment, space).expect("peels");
            for (script, row) in refs.iter().zip(&assignment) {
                let chosen = candidate_rows(11, script, space)[table.choice(11, script)];
                assert_eq!(chosen, u64::from(*row), "{count} scripts at {rows} rows");
            }
        }
    }

    /// Routing holds across segments: a script set too large for one segment
    /// is placed over the whole logical row space, and the table names rows in
    /// that space.
    #[test]
    fn the_choice_bit_routes_across_segments() {
        let rows = 2_048u64;
        let slots = crate::records::DIRECTORY_SLOTS as u64;
        let owned = placement_set((rows * slots) as u32 + 1_000);
        let refs: Vec<&[u8]> = owned.iter().map(|s| s.as_slice()).collect();
        let (placement, assignment) =
            place_scripts(5, &refs, &vec![1; refs.len()], rows, slots as usize).expect("fits");
        assert!(placement.segments > 1);
        let space = rows * u64::from(placement.segments);
        let table = choice_table(5, &refs, &assignment, space).expect("peels");
        assert_eq!(
            verify_choice(5, &table, &refs, space, |i| u64::from(assignment[i])),
            None
        );
        // And a check against a different placement does catch misrouting.
        assert!(verify_choice(5, &table, &refs, space, |i| u64::from(assignment[i]) ^ 1).is_some());
    }

    /// The sealer's measured placement matches the builder's on content of
    /// mixed shapes, not only on the single-shape fixture above.
    #[test]
    fn the_sealer_measures_the_builders_fullest_row_on_mixed_shapes() {
        use crate::seal::{Limit, PageBasis, SealPolicy, Sealer};

        let events = mixed_fixture(6_000);
        let policy = SealPolicy {
            scripts: Limit::new(1_000_000, 2_000_000).expect("valid"),
            page_rows: Limit::new(1_000_000, 2_000_000).expect("valid"),
        };
        let mut sealer = Sealer::with_basis(policy, 100, PageBasis::default());
        sealer.measure_placement(true);
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
        let rows = RECENT_8K.directory_rows * built.directory_segments() as u64;
        let fullest = (0..rows)
            .map(|row| {
                decode_directory_row(built.directory_row(row))
                    .expect("decodes")
                    .len() as u64
            })
            .max()
            .unwrap_or(0);
        assert_eq!(predicted.segments, built.directory_segments());
        assert_eq!(predicted.max_row_load, fullest);
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
                        .find(|entry| entry.tag == tag_for(wanted.as_slice()))
                })
                .expect("entry");

            let mut recovered = entry.inline.clone();
            let base = page_at(&entry);
            let first = fragment_of(built.page_row(base), &tag_for(wanted.as_slice()), 0);
            for ordinal in 0..first.fragment_count {
                let fragment = fragment_of(
                    built.page_row(base + u64::from(ordinal)),
                    &tag_for(wanted.as_slice()),
                    ordinal,
                );
                assert_eq!(fragment.fragment_count, first.fragment_count);
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
                    .find(|entry| entry.tag == tag_for(wanted.as_slice()))
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
            built.page_row(page_at(&entry)),
            &tag_for(wanted.as_slice()),
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
        assert_eq!(forward.tag_salt_counter, 0);
    }

    /// The salt is a function of the revision. Rebuilding the same revision
    /// reproduces the bytes; a different terminal block does not.
    #[test]
    fn the_tag_salt_is_reproduced_by_a_rebuild_and_moves_with_the_terminal_block() {
        let events = fixture(4, 2);
        let once = build(&events);
        let again = build(&events);
        assert_eq!(once.tag_salt_counter, 0);
        assert_eq!(once.directory, again.directory);

        let moved = build_shard(
            0,
            100,
            200,
            genesis(),
            BlockHash::from_internal_bytes([0x5b; 32]),
            RANGE_PROFILE,
            &RECENT_8K,
            &events,
        )
        .unwrap();
        assert_ne!(moved.directory, once.directory);
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
            build_shard(
                0,
                100,
                200,
                genesis(),
                terminal(),
                RANGE_PROFILE,
                &RECENT_8K,
                &events
            ),
            Err(BuildError::Invalid(_))
        ));
    }
    #[test]
    fn mixed_size_directory_placement_preserves_every_byte_budget() {
        let rows = 256u64;
        let mut entries = Vec::new();
        let mut bytes = 0;
        for i in 0..100_000u32 {
            let size = [70, 98, 113, 121, 149, 177][i as usize % 6];
            if bytes + size > rows as usize * 4092 * 6 / 7 {
                break;
            }
            entries.push((script(i).as_slice().to_vec(), size));
            bytes += size;
        }
        let ordered = placement_order(entries.iter().map(|(s, n)| (s.as_slice(), *n)));
        let scripts: Vec<_> = ordered.iter().map(|(s, _)| *s).collect();
        let sizes: Vec<_> = ordered.iter().map(|(_, n)| *n).collect();
        let (placed, assignment) = place_scripts(42, &scripts, &sizes, rows, 4092).unwrap();
        assert_eq!(placed.segments, 1);
        let mut used = vec![0usize; rows as usize];
        for ((script, size), row) in ordered.iter().zip(&assignment) {
            assert!(candidate_rows(42, script, rows).contains(&u64::from(*row)));
            used[*row as usize] += size;
        }
        assert!(used.iter().all(|bytes| *bytes <= 4092));
        assert_eq!(used.iter().sum::<usize>(), bytes);
        assert_eq!(
            used.iter().max().copied().unwrap() as u64,
            placed.max_row_bytes
        );
    }
}
