//! Building and verifying one display shard's tables.

use super::{bucket, candidate_rows, DisplayTable, MAX_BUCKETS};
use crate::layout::Geometry;
use crate::txid::{
    self, assemble, entries, finish, push, segments, u32_at, DirectoryEntry, Error,
    TransparentDisplayRecord, FRAGMENT_BYTES, INLINE_BYTES, ROW_BYTES,
};
use std::collections::{BTreeMap, BTreeSet};
use transparent_events::Txid;

fn bad(s: &str) -> Error {
    Error(s.into())
}

/// One bucket's directory table.
#[derive(Clone, Debug)]
pub struct BuiltBucket {
    /// One `rows × ROW_BYTES` segment per entry; at least one.
    pub directory: Vec<Vec<u8>>,
    pub records: u64,
    pub inline_records: u64,
    /// Records by page count; 0 is inline.
    pub page_histogram: BTreeMap<u32, u64>,
    /// Row bytes occupied before zero padding, across every segment.
    pub used_bytes: u64,
}

/// A display shard's tables.
#[derive(Clone, Debug)]
pub struct BuiltDisplay {
    pub buckets: Vec<BuiltBucket>,
    /// Shard-scoped overflow pages; at least one segment, possibly empty.
    pub pages: Vec<Vec<u8>>,
    pub records: u64,
    pub payload_bytes: u64,
    /// Page rows holding any fragment.
    pub page_rows_used: u64,
}

impl BuiltDisplay {
    /// The smallest bucket, which is the shard's anonymity floor.
    pub fn min_bucket_records(&self) -> u64 {
        self.buckets.iter().map(|b| b.records).min().unwrap_or(0)
    }

    /// Segment bytes of `table`.
    pub fn segments(&self, table: DisplayTable) -> &[Vec<u8>] {
        match table {
            DisplayTable::Directory(b) => &self.buckets[b as usize].directory,
            DisplayTable::Pages => &self.pages,
        }
    }

    /// Every table, directories first.
    pub fn tables(&self) -> Vec<DisplayTable> {
        (0..self.buckets.len() as u32)
            .map(DisplayTable::Directory)
            .chain([DisplayTable::Pages])
            .collect()
    }
}

/// Lays out one shard: records sorted by txid, inline payloads in their
/// bucket's directory, larger ones fragmented into the shard's pages.
///
/// Deterministic in the record set, the shard id, the geometry and the bucket
/// count, so rebuilding a sealed range reproduces its bytes.
pub fn build_shard(
    shard_id: u64,
    geometry: &Geometry,
    n_buckets: u32,
    records: &[TransparentDisplayRecord],
) -> Result<BuiltDisplay, Error> {
    if n_buckets == 0 || n_buckets > MAX_BUCKETS {
        return Err(bad("bucket count"));
    }
    let rows = geometry.directory_rows as usize;
    let mut sorted = BTreeMap::new();
    for record in records {
        if sorted.insert(record.txid.0, record.encode()?).is_some() {
            return Err(bad("duplicate transaction identity"));
        }
    }
    let mut pages = vec![vec![0; 4]];
    let mut directories: Vec<Vec<Vec<Vec<u8>>>> = (0..n_buckets)
        .map(|_| vec![vec![vec![0; 4]; rows]])
        .collect();
    let mut stats: Vec<(u64, u64, BTreeMap<u32, u64>)> =
        (0..n_buckets).map(|_| (0, 0, BTreeMap::new())).collect();
    let mut payload_bytes = 0;
    for (id, payload) in &sorted {
        payload_bytes += payload.len() as u64;
        let txid = Txid(*id);
        let mut entry = DirectoryEntry {
            txid,
            total: payload.len() as u32,
            first_page: 0,
            pages: 0,
            inline: Vec::new(),
        };
        if payload.len() <= INLINE_BYTES {
            entry.inline = payload.clone();
        } else {
            let mut first = None;
            for (i, chunk) in payload.chunks(FRAGMENT_BYTES).enumerate() {
                let mut fragment = id.to_vec();
                fragment.extend((i as u32 * FRAGMENT_BYTES as u32).to_le_bytes());
                fragment.extend((payload.len() as u32).to_le_bytes());
                fragment.extend(chunk);
                if !push(pages.last_mut().unwrap(), &fragment) {
                    pages.push(vec![0; 4]);
                    assert!(push(pages.last_mut().unwrap(), &fragment));
                }
                first.get_or_insert(pages.len() - 1);
            }
            let first = first.unwrap();
            entry.first_page =
                u32::try_from(first + 1).map_err(|_| bad("page locator overflow"))?;
            entry.pages =
                u32::try_from(pages.len() - first).map_err(|_| bad("page count overflow"))?;
        }
        let b = bucket(&txid, n_buckets);
        let raw = entry.encode();
        let [a, c] =
            candidate_rows(&txid, shard_id, b, geometry.directory_rows).map(|r| r as usize);
        // Greedy two choice by bytes, as `txid::build`; a full pair in every
        // segment opens another segment of this bucket only.
        let segments = &mut directories[b as usize];
        let mut placed = false;
        for segment in segments.iter_mut() {
            let choice = if segment[a].len() <= segment[c].len() {
                a
            } else {
                c
            };
            if push(&mut segment[choice], &raw)
                || choice != a && push(&mut segment[a], &raw)
                || choice != c && push(&mut segment[c], &raw)
            {
                placed = true;
                break;
            }
        }
        if !placed {
            let mut segment = vec![vec![0; 4]; rows];
            assert!(push(&mut segment[a], &raw));
            segments.push(segment);
        }
        let stat = &mut stats[b as usize];
        stat.0 += 1;
        stat.1 += u64::from(entry.pages == 0);
        *stat.2.entry(entry.pages).or_default() += 1;
    }
    let page_rows_used = if pages.len() == 1 && pages[0].len() == 4 {
        0
    } else {
        pages.len() as u64
    };
    let buckets = directories
        .into_iter()
        .zip(stats)
        .map(|(segments, (records, inline_records, page_histogram))| {
            let used_bytes = segments.iter().flatten().map(|r| r.len() as u64).sum();
            BuiltBucket {
                directory: segments
                    .into_iter()
                    .map(|s| s.into_iter().flat_map(finish).collect())
                    .collect(),
                records,
                inline_records,
                page_histogram,
                used_bytes,
            }
        })
        .collect();
    Ok(BuiltDisplay {
        buckets,
        pages: segments(pages, geometry.page_rows as usize),
        records: sorted.len() as u64,
        payload_bytes,
        page_rows_used,
    })
}

/// What verification recounted from the rows themselves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedDisplay {
    pub bucket_records: Vec<u64>,
    pub page_histograms: Vec<BTreeMap<u32, u64>>,
}

/// Checks every directory and page row of a shard, streaming rows.
///
/// Each entry must sit in its own bucket at one of its two candidate rows, no
/// txid may appear twice in any bucket, every paged record must assemble from
/// its declared extent, per-bucket counts must match, and no fragment may hide
/// outside a declared extent.
pub fn verify_rows(
    shard_id: u64,
    geometry: &Geometry,
    directory_segments: &[usize],
    page_segments: usize,
    bucket_records: &[u64],
    mut row_at: impl FnMut(DisplayTable, usize, usize) -> Result<Vec<u8>, Error>,
) -> Result<VerifiedDisplay, Error> {
    let n_buckets = directory_segments.len() as u32;
    if n_buckets == 0
        || n_buckets > MAX_BUCKETS
        || bucket_records.len() != directory_segments.len()
        || directory_segments.contains(&0)
        || page_segments == 0
    {
        return Err(bad("display shard shape"));
    }
    let page_rows = geometry.page_rows as usize;
    let mut seen = BTreeSet::new();
    let mut paged = BTreeMap::new();
    let mut counts = vec![0u64; n_buckets as usize];
    let mut histograms = vec![BTreeMap::new(); n_buckets as usize];
    for (b, segments) in directory_segments.iter().enumerate() {
        let table = DisplayTable::Directory(b as u32);
        for segment in 0..*segments {
            for row in 0..geometry.directory_rows as usize {
                let bytes = row_at(table, segment, row)?;
                if bytes.len() != ROW_BYTES {
                    return Err(bad("row width"));
                }
                for raw in entries(&bytes)? {
                    let entry = DirectoryEntry::decode(raw)?;
                    if bucket(&entry.txid, n_buckets) != b as u32
                        || !candidate_rows(&entry.txid, shard_id, b as u32, geometry.directory_rows)
                            .contains(&(row as u64))
                        || !seen.insert(entry.txid.0)
                    {
                        return Err(bad("directory bucket/placement/duplicate"));
                    }
                    counts[b] += 1;
                    *histograms[b].entry(entry.pages).or_insert(0u64) += 1;
                    if entry.pages > 0 {
                        let first = entry.first_page as usize - 1;
                        let end = first
                            .checked_add(entry.pages as usize)
                            .filter(|end| *end <= page_segments * page_rows)
                            .ok_or_else(|| bad("page extent"))?;
                        let rows: Vec<_> = (first..end)
                            .map(|r| row_at(DisplayTable::Pages, r / page_rows, r % page_rows))
                            .collect::<Result<_, _>>()?;
                        assemble(&entry, &rows)?;
                        paged.insert(entry.txid.0, (entry.total, first, end));
                    }
                }
            }
        }
    }
    if counts != bucket_records {
        return Err(bad("bucket record count"));
    }
    for segment in 0..page_segments {
        for row in 0..page_rows {
            let bytes = row_at(DisplayTable::Pages, segment, row)?;
            if bytes.len() != ROW_BYTES {
                return Err(bad("row width"));
            }
            for raw in entries(&bytes)? {
                if raw.len() <= 40 {
                    return Err(bad("fragment header/length"));
                }
                let id: [u8; 32] = raw[..32].try_into().unwrap();
                let (total, first, end) =
                    paged.get(&id).ok_or_else(|| bad("orphan page fragment"))?;
                let position = segment * page_rows + row;
                if !(*first..*end).contains(&position) || u32_at(raw, 36)? != *total {
                    return Err(bad("fragment outside declared extent"));
                }
            }
        }
    }
    Ok(VerifiedDisplay {
        bucket_records: counts,
        page_histograms: histograms,
    })
}

/// [`verify_rows`] over tables held in memory.
pub fn verify(
    shard_id: u64,
    geometry: &Geometry,
    built: &BuiltDisplay,
) -> Result<VerifiedDisplay, Error> {
    let segments: Vec<usize> = built.buckets.iter().map(|b| b.directory.len()).collect();
    let records: Vec<u64> = built.buckets.iter().map(|b| b.records).collect();
    verify_rows(
        shard_id,
        geometry,
        &segments,
        built.pages.len(),
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
pub fn directory_rows_for(
    built: &BuiltDisplay,
    shard_id: u64,
    geometry: &Geometry,
    txid: &Txid,
) -> Vec<Vec<u8>> {
    let b = bucket(txid, built.buckets.len() as u32);
    let mut out = Vec::new();
    for row in candidate_rows(txid, shard_id, b, geometry.directory_rows) {
        for segment in &built.buckets[b as usize].directory {
            let row = row as usize;
            out.push(segment[row * ROW_BYTES..(row + 1) * ROW_BYTES].to_vec());
        }
    }
    out
}

/// Deduplicated decode of fetched rows, as a client must: a coincident pair
/// of candidates returns the same row twice.
pub fn find_directory_unique(
    rows: &[Vec<u8>],
    txid: &Txid,
) -> Result<Option<DirectoryEntry>, Error> {
    let unique: BTreeSet<&Vec<u8>> = rows.iter().collect();
    let unique: Vec<Vec<u8>> = unique.into_iter().cloned().collect();
    txid::find_directory(&unique, *txid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::display::TXID_2K;
    use transparent_events::{FeeState, TransactionMetadata};

    fn record(id: u32, script: usize) -> TransparentDisplayRecord {
        let mut txid = [0u8; 32];
        txid[..4].copy_from_slice(&id.to_le_bytes());
        txid[31] = 0xa5;
        TransparentDisplayRecord {
            txid: Txid(sha2::Sha256::digest(txid).into()),
            coinbase: false,
            metadata: TransactionMetadata {
                fee: FeeState::Exact(1_000),
                transparent_input_count: 1,
                has_shielded_components: false,
            },
            outputs: vec![crate::txid::DisplayOutput {
                value: 5,
                script: vec![0x51; script],
            }],
        }
    }
    use sha2::Digest;

    fn lookup(built: &BuiltDisplay, shard: u64, g: &Geometry, r: &TransparentDisplayRecord) {
        let rows = directory_rows_for(built, shard, g, &r.txid);
        let entry = find_directory_unique(&rows, &r.txid).unwrap().unwrap();
        let pages: Vec<Vec<u8>> = if entry.pages > 0 {
            (entry.first_page - 1..entry.first_page - 1 + entry.pages)
                .map(|p| {
                    let p = p as usize;
                    let (segment, row) = (p / g.page_rows as usize, p % g.page_rows as usize);
                    built.pages[segment][row * ROW_BYTES..(row + 1) * ROW_BYTES].to_vec()
                })
                .collect()
        } else {
            Vec::new()
        };
        assert_eq!(&assemble(&entry, &pages).unwrap(), r);
    }

    #[test]
    fn inline_and_paged_records_are_found_from_their_own_bucket() {
        let g = TXID_2K;
        let mut records: Vec<_> = (0..300).map(|i| record(i, 25)).collect();
        records.push(record(1_000, 200));
        records.push(record(1_001, 9_000));
        for n in [1, 4] {
            let built = build_shard(7, &g, n, &records).unwrap();
            assert_eq!(built.records, records.len() as u64);
            assert_eq!(built.buckets.len(), n as usize);
            let verified = verify(7, &g, &built).unwrap();
            assert_eq!(
                verified.page_histograms,
                built
                    .buckets
                    .iter()
                    .map(|b| b.page_histogram.clone())
                    .collect::<Vec<_>>()
            );
            for r in &records {
                lookup(&built, 7, &g, r);
                // Every other bucket's candidates miss it.
                let b = bucket(&r.txid, n);
                for other in (0..n).filter(|o| *o != b) {
                    for row in candidate_rows(&r.txid, 7, other, g.directory_rows) {
                        let row = row as usize;
                        let bytes = &built.buckets[other as usize].directory[0]
                            [row * ROW_BYTES..(row + 1) * ROW_BYTES];
                        assert!(crate::txid::find_directory(&[bytes.to_vec()], r.txid)
                            .unwrap()
                            .is_none());
                    }
                }
            }
            let histogram: u64 = built
                .buckets
                .iter()
                .map(|b| b.page_histogram.get(&3).copied().unwrap_or(0))
                .sum();
            assert_eq!(histogram, 1, "the 9,000-byte record takes three pages");
            let inline: u64 = built.buckets.iter().map(|b| b.inline_records).sum();
            assert_eq!(inline, 300);
        }
    }

    #[test]
    fn layout_is_deterministic_in_the_record_set() {
        let g = TXID_2K;
        let records: Vec<_> = (0..500).map(|i| record(i, (i % 7) as usize * 40)).collect();
        let mut shuffled = records.clone();
        shuffled.reverse();
        let a = build_shard(3, &g, 4, &records).unwrap();
        let b = build_shard(3, &g, 4, &shuffled).unwrap();
        for table in a.tables() {
            assert_eq!(a.segments(table), b.segments(table));
        }
        assert!(build_shard(3, &g, 4, &[records[0].clone(), records[0].clone()]).is_err());
        assert!(build_shard(3, &g, 0, &records).is_err());
        assert!(build_shard(3, &g, MAX_BUCKETS + 1, &records).is_err());
    }

    #[test]
    fn tiny_geometry_overflows_into_segments_per_bucket() {
        let g = Geometry {
            directory_rows: 1,
            page_rows: 1,
            ..TXID_2K
        };
        let mut records: Vec<_> = (0..160).map(|i| record(i, 80)).collect();
        records.push(record(9_999, 9_000));
        let built = build_shard(0, &g, 2, &records).unwrap();
        assert!(built.buckets.iter().all(|b| b.directory.len() > 1));
        assert!(built.pages.len() > 1);
        verify(0, &g, &built).unwrap();
        for r in &records {
            lookup(&built, 0, &g, r);
        }
    }

    #[test]
    fn verification_refuses_moved_duplicated_and_orphaned_entries() {
        let g = TXID_2K;
        let mut records: Vec<_> = (0..200).map(|i| record(i, 25)).collect();
        records.push(record(500, 9_000));
        let built = build_shard(1, &g, 2, &records).unwrap();
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

        // An extra copy of a directory segment duplicates every entry in it.
        let mut duplicated = built.clone();
        let extra = duplicated.buckets[0].directory[0].clone();
        duplicated.buckets[0].directory.push(extra);
        assert!(verify(1, &g, &duplicated).is_err());

        // An orphan fragment in an otherwise empty page row.
        let mut orphan = built.clone();
        let mut row = vec![0u8; 4];
        let mut fragment = [0x44u8; 32].to_vec();
        fragment.extend(0u32.to_le_bytes());
        fragment.extend(10u32.to_le_bytes());
        fragment.extend([1u8; 10]);
        assert!(push(&mut row, &fragment));
        let row = finish(row);
        let last = orphan.pages[0].len() - ROW_BYTES;
        orphan.pages[0][last..].copy_from_slice(&row);
        assert!(verify(1, &g, &orphan).is_err());

        // A missing fragment.
        let mut missing = built.clone();
        let rows_used = missing.page_rows_used as usize;
        missing.pages[0][(rows_used - 1) * ROW_BYTES..rows_used * ROW_BYTES].fill(0);
        assert!(verify(1, &g, &missing).is_err());
    }
}
