//! Building and verifying one display shard's tables.

use super::{bucket, candidate_rows, DisplayTable, MAX_BUCKETS};
use crate::layout::Geometry;
use crate::txid::{encode_row, row_entries, DisplayEntry, Error, Tag, ROW_BYTES, SLOTS_PER_ROW};
use std::collections::BTreeSet;
use transparent_events::Txid;

fn bad(s: &str) -> Error {
    Error(s.into())
}

/// Evictions one insertion may make before its homeless entry moves to the
/// next segment. Only a nearly full table gets near it.
const MAX_KICKS: usize = 512;

/// One bucket's table.
#[derive(Clone, Debug)]
pub struct BuiltBucket {
    /// One `rows × ROW_BYTES` segment per entry; at least one.
    pub directory: Vec<Vec<u8>>,
    pub records: u64,
}

/// A display shard's tables.
#[derive(Clone, Debug)]
pub struct BuiltDisplay {
    pub buckets: Vec<BuiltBucket>,
    pub records: u64,
}

impl BuiltDisplay {
    /// The smallest bucket, which is the shard's anonymity floor.
    pub fn min_bucket_records(&self) -> u64 {
        self.buckets.iter().map(|b| b.records).min().unwrap_or(0)
    }

    /// Segment bytes of `table`.
    pub fn segments(&self, table: DisplayTable) -> &[Vec<u8>] {
        &self.buckets[table.bucket() as usize].directory
    }

    /// Every table, in bucket order.
    pub fn tables(&self) -> Vec<DisplayTable> {
        (0..self.buckets.len() as u32)
            .map(DisplayTable::Directory)
            .collect()
    }
}

type Rows = Vec<Vec<DisplayEntry>>;

/// The other candidate row of an entry sitting in `row`.
fn other_row(entry: &DisplayEntry, shard_id: u64, b: u32, rows: u64, row: usize) -> usize {
    let [a, c] = candidate_rows(&entry.tag, shard_id, b, rows).map(|r| r as usize);
    if a == row {
        c
    } else {
        a
    }
}

/// Cuckoo insertion into one segment: the emptier candidate if either has
/// room, otherwise evict along a bounded, deterministic walk. Returns the
/// entry left homeless, which may be one placed earlier, or `None`.
fn insert(
    segment: &mut Rows,
    entry: DisplayEntry,
    shard_id: u64,
    b: u32,
    rows: u64,
) -> Option<DisplayEntry> {
    let [a, c] = candidate_rows(&entry.tag, shard_id, b, rows).map(|r| r as usize);
    let row = if segment[a].len() <= segment[c].len() {
        a
    } else {
        c
    };
    if segment[row].len() < SLOTS_PER_ROW {
        segment[row].push(entry);
        return None;
    }
    let mut homeless = entry;
    let mut row = a;
    for kick in 0..MAX_KICKS {
        let slot = kick % segment[row].len();
        std::mem::swap(&mut segment[row][slot], &mut homeless);
        row = other_row(&homeless, shard_id, b, rows, row);
        if segment[row].len() < SLOTS_PER_ROW {
            segment[row].push(homeless);
            return None;
        }
    }
    Some(homeless)
}

/// Lays out one shard: each entry in its bucket's table, at one of its two
/// candidate rows.
///
/// Deterministic in the entry set, the shard id, the geometry and the bucket
/// count, so rebuilding a sealed range reproduces its bytes.
pub fn build_shard(
    shard_id: u64,
    geometry: &Geometry,
    n_buckets: u32,
    entries: &[impl AsRef<DisplayEntry>],
) -> Result<BuiltDisplay, Error> {
    if n_buckets == 0 || n_buckets > MAX_BUCKETS {
        return Err(bad("bucket count"));
    }
    let rows = geometry.directory_rows;
    let mut sorted: Vec<DisplayEntry> = entries.iter().map(|e| *e.as_ref()).collect();
    sorted.sort_by_key(|e| e.tag);
    if sorted.windows(2).any(|w| w[0].tag == w[1].tag) {
        return Err(bad("duplicate transaction identity"));
    }
    let mut tables: Vec<Vec<Rows>> = (0..n_buckets)
        .map(|_| vec![vec![Vec::new(); rows as usize]])
        .collect();
    let mut records = vec![0u64; n_buckets as usize];
    for entry in sorted.iter().copied() {
        let b = bucket(&entry.tag, n_buckets);
        records[b as usize] += 1;
        let segments = &mut tables[b as usize];
        let mut homeless = entry;
        let mut segment = 0;
        loop {
            if segment == segments.len() {
                segments.push(vec![Vec::new(); rows as usize]);
            }
            match insert(&mut segments[segment], homeless, shard_id, b, rows) {
                None => break,
                Some(left) => {
                    homeless = left;
                    segment += 1;
                }
            }
        }
    }
    let buckets = tables
        .into_iter()
        .zip(records)
        .map(|(segments, records)| {
            let directory = segments
                .into_iter()
                .map(|segment| {
                    let mut bytes = Vec::with_capacity(segment.len() * ROW_BYTES);
                    for row in segment {
                        bytes.extend(encode_row(&row)?);
                    }
                    Ok(bytes)
                })
                .collect::<Result<_, Error>>()?;
            Ok(BuiltBucket { directory, records })
        })
        .collect::<Result<_, Error>>()?;
    Ok(BuiltDisplay {
        buckets,
        records: sorted.len() as u64,
    })
}

/// Checks every row of a shard, streaming rows.
///
/// Every row must be canonical, each entry must sit in its own bucket at one
/// of its two candidate rows, no tag may appear twice in the shard, and the
/// per-bucket counts must match. Returns those counts.
pub fn verify_rows(
    shard_id: u64,
    geometry: &Geometry,
    directory_segments: &[usize],
    bucket_records: &[u64],
    mut row_at: impl FnMut(DisplayTable, usize, usize) -> Result<Vec<u8>, Error>,
) -> Result<Vec<u64>, Error> {
    let n_buckets = directory_segments.len() as u32;
    if n_buckets == 0
        || n_buckets > MAX_BUCKETS
        || bucket_records.len() != directory_segments.len()
        || directory_segments.contains(&0)
    {
        return Err(bad("display shard shape"));
    }
    let mut seen = BTreeSet::new();
    let mut counts = vec![0u64; n_buckets as usize];
    for (b, segments) in directory_segments.iter().enumerate() {
        let table = DisplayTable::Directory(b as u32);
        for segment in 0..*segments {
            for row in 0..geometry.directory_rows as usize {
                for entry in row_entries(&row_at(table, segment, row)?)? {
                    if bucket(&entry.tag, n_buckets) != b as u32
                        || !candidate_rows(&entry.tag, shard_id, b as u32, geometry.directory_rows)
                            .contains(&(row as u64))
                        || !seen.insert(entry.tag)
                    {
                        return Err(bad("entry bucket/placement/duplicate"));
                    }
                    counts[b] += 1;
                }
            }
        }
    }
    if counts != bucket_records {
        return Err(bad("bucket record count"));
    }
    Ok(counts)
}

/// [`verify_rows`] over tables held in memory.
pub fn verify(shard_id: u64, geometry: &Geometry, built: &BuiltDisplay) -> Result<Vec<u64>, Error> {
    let segments: Vec<usize> = built.buckets.iter().map(|b| b.directory.len()).collect();
    let records: Vec<u64> = built.buckets.iter().map(|b| b.records).collect();
    verify_rows(
        shard_id,
        geometry,
        &segments,
        &records,
        |table, segment, row| {
            built
                .segments(table)
                .get(segment)
                .and_then(|s| s.get(row * ROW_BYTES..(row + 1) * ROW_BYTES))
                .map(<[u8]>::to_vec)
                .ok_or_else(|| bad("table row bounds"))
        },
    )
}

/// Rows a client would retrieve for `txid`: both candidates of its bucket
/// across every segment of that bucket. For tests and tooling.
pub fn rows_for(
    built: &BuiltDisplay,
    shard_id: u64,
    geometry: &Geometry,
    txid: &Txid,
) -> Vec<Vec<u8>> {
    let tag = Tag::of(txid);
    let b = bucket(&tag, built.buckets.len() as u32);
    let mut out = Vec::new();
    for row in candidate_rows(&tag, shard_id, b, geometry.directory_rows) {
        for segment in &built.buckets[b as usize].directory {
            let row = row as usize;
            out.push(segment[row * ROW_BYTES..(row + 1) * ROW_BYTES].to_vec());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::display::TXID_2K;
    use crate::txid::{find_entry, DisplayFacts, DisplayOutput};
    use sha2::{Digest, Sha256};

    fn txid(id: u32) -> Txid {
        Txid(Sha256::digest(id.to_le_bytes()).into())
    }

    fn entry(id: u32) -> DisplayEntry {
        let p2pkh = |b: u8| [&[0x76, 0xa9, 0x14][..], &[b; 20], &[0x88, 0xac]].concat();
        DisplayFacts {
            txid: txid(id),
            coinbase: false,
            fee: 1_000,
            has_shielded_components: false,
            spent: vec![DisplayOutput {
                value: 6_000,
                script: p2pkh(1),
            }],
            outputs: vec![DisplayOutput {
                value: 5_000,
                script: p2pkh(id as u8),
            }],
        }
        .entry()
        .unwrap()
    }

    fn lookup(built: &BuiltDisplay, shard: u64, g: &Geometry, id: u32) -> Option<DisplayEntry> {
        find_entry(&rows_for(built, shard, g, &txid(id)), &txid(id)).unwrap()
    }

    #[test]
    fn entries_are_found_in_their_own_bucket() {
        let g = TXID_2K;
        let entries: Vec<_> = (0..3_000).map(entry).collect();
        for n in [1, 4] {
            let built = build_shard(7, &g, n, &entries).unwrap();
            assert_eq!(built.records, entries.len() as u64);
            assert_eq!(built.buckets.len(), n as usize);
            assert!(built.buckets.iter().all(|b| b.directory.len() == 1));
            let counts = verify(7, &g, &built).unwrap();
            assert_eq!(counts.iter().sum::<u64>(), 3_000);
            for (id, e) in entries.iter().enumerate() {
                assert_eq!(lookup(&built, 7, &g, id as u32).as_ref(), Some(e));
                // Every other bucket's candidates miss it.
                let b = bucket(&e.tag, n);
                for other in (0..n).filter(|o| *o != b) {
                    for row in candidate_rows(&e.tag, 7, other, g.directory_rows) {
                        let row = row as usize;
                        let bytes = &built.buckets[other as usize].directory[0]
                            [row * ROW_BYTES..(row + 1) * ROW_BYTES];
                        assert!(row_entries(bytes).unwrap().iter().all(|x| x.tag != e.tag));
                    }
                }
            }
            assert_eq!(lookup(&built, 7, &g, 1_000_000), None);
        }
    }

    #[test]
    fn layout_is_deterministic_in_the_entry_set() {
        let g = TXID_2K;
        let entries: Vec<_> = (0..500).map(entry).collect();
        let mut shuffled = entries.clone();
        shuffled.reverse();
        let a = build_shard(3, &g, 4, &entries).unwrap();
        let b = build_shard(3, &g, 4, &shuffled).unwrap();
        for table in a.tables() {
            assert_eq!(a.segments(table), b.segments(table));
        }
        assert!(build_shard(3, &g, 4, &[entries[0], entries[0]]).is_err());
        assert!(build_shard(3, &g, 0, &entries).is_err());
        assert!(build_shard(3, &g, MAX_BUCKETS + 1, &entries).is_err());
    }

    #[test]
    fn cuckoo_fills_a_table_well_past_two_choice() {
        // 64 rows × 36 slots = 2,304; 95% load stays in one segment.
        let g = Geometry {
            directory_rows: 64,
            ..TXID_2K
        };
        let entries: Vec<_> = (0..2_188).map(entry).collect();
        let built = build_shard(5, &g, 1, &entries).unwrap();
        assert_eq!(built.buckets[0].directory.len(), 1);
        verify(5, &g, &built).unwrap();
        for id in [0, 1_000, 2_187] {
            assert!(lookup(&built, 5, &g, id).is_some());
        }
    }

    #[test]
    fn a_full_table_overflows_into_segments_per_bucket() {
        let g = Geometry {
            directory_rows: 1,
            ..TXID_2K
        };
        let entries: Vec<_> = (0..160).map(entry).collect();
        let built = build_shard(0, &g, 2, &entries).unwrap();
        assert!(built.buckets.iter().all(|b| b.directory.len() > 1));
        verify(0, &g, &built).unwrap();
        for id in 0..160 {
            assert!(lookup(&built, 0, &g, id).is_some());
        }
    }

    #[test]
    fn verification_refuses_moved_duplicated_and_miscounted_entries() {
        let g = TXID_2K;
        let entries: Vec<_> = (0..200).map(entry).collect();
        let built = build_shard(1, &g, 2, &entries).unwrap();
        verify(1, &g, &built).unwrap();

        // The wrong shard id moves every candidate row.
        assert!(verify(2, &g, &built).is_err());

        // A bucket's table presented as the other bucket's.
        let mut swapped = built.clone();
        swapped.buckets.swap(0, 1);
        assert!(verify(1, &g, &swapped).is_err());

        // A wrong count.
        let mut miscounted = built.clone();
        miscounted.buckets[0].records += 1;
        assert!(verify(1, &g, &miscounted).is_err());

        // An extra copy of a segment duplicates every entry in it.
        let mut duplicated = built.clone();
        let extra = duplicated.buckets[0].directory[0].clone();
        duplicated.buckets[0].directory.push(extra);
        assert!(verify(1, &g, &duplicated).is_err());

        // A noncanonical row: one byte of padding.
        let mut padded = built.clone();
        padded.buckets[0].directory[0][ROW_BYTES - 1] = 1;
        assert!(verify(1, &g, &padded).is_err());
    }
}
