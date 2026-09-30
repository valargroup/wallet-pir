//! Versioned display facts, not transaction authentication or financial coverage.
//! Full txids avoid tag ambiguity. Small records share rows; overflow locators
//! are obtained privately and never become HTTP routing information.
use crate::{Geometry, TableGeometry};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use transparent_events::{decode_varint, encode_varint, TransactionMetadata, Txid, MAX_MONEY};

pub const CODEC: &str = "transparent-txid-display-v1";
pub const INLINE_BYTES: usize = 128;
pub const ROW_BYTES: usize = 4096;
/// Twice the supported 2 MB transaction ceiling allows varint expansion.
pub const MAX_RECORD_BYTES: usize = 4_000_000;
pub const MAX_OUTPUTS: usize = 2_000_000 / 9;
const FRAGMENT_BYTES: usize = ROW_BYTES - 4 - 2 - 40;

#[derive(Debug, thiserror::Error)]
#[error("invalid txid display data: {0}")]
pub struct Error(pub String);
fn bad(s: &str) -> Error {
    Error(s.into())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisplayOutput {
    pub value: u64,
    pub script: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransparentDisplayRecord {
    pub txid: Txid,
    pub coinbase: bool,
    pub metadata: TransactionMetadata,
    pub outputs: Vec<DisplayOutput>,
}
fn read_var(bytes: &[u8], at: &mut usize, max: u64) -> Result<u64, Error> {
    let (v, n) = decode_varint(bytes.get(*at..).ok_or_else(|| bad("truncated"))?, max)
        .map_err(|e| Error(e.to_string()))?;
    *at += n;
    Ok(v)
}
impl TransparentDisplayRecord {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.metadata
            .validate(self.coinbase)
            .map_err(|e| Error(e.to_string()))?;
        if self.outputs.len() > MAX_OUTPUTS {
            return Err(bad("output count"));
        }
        // A lower bound on the canonical transaction size: even empty input
        // scripts need an outpoint, CompactSize and sequence. The publisher
        // additionally parses canonical accepted transactions, including the
        // bytes of shielded components that this display record omits.
        let inputs = if self.coinbase {
            1
        } else {
            u64::from(self.metadata.transparent_input_count)
        };
        let compact_size = |n: u64| {
            if n < 253 {
                1
            } else if n <= 65535 {
                3
            } else {
                5
            }
        };
        let mut canonical_minimum =
            8 + compact_size(inputs) + 41 * inputs + compact_size(self.outputs.len() as u64);
        if canonical_minimum > 2_000_000 {
            return Err(bad("transaction input size bound"));
        }
        let mut out = vec![1, self.metadata.flags() | u8::from(self.coinbase)];
        self.metadata.encode(&mut out);
        encode_varint(self.outputs.len() as u64, &mut out);
        let mut sum = 0u64;
        for output in &self.outputs {
            sum = sum
                .checked_add(output.value)
                .filter(|v| *v <= MAX_MONEY)
                .ok_or_else(|| bad("output value total"))?;
            if output.script.len() > 2_000_000 {
                return Err(bad("script length"));
            }
            canonical_minimum +=
                8 + compact_size(output.script.len() as u64) + output.script.len() as u64;
            if canonical_minimum > 2_000_000 {
                return Err(bad("transaction output size bound"));
            }
            encode_varint(output.value, &mut out);
            encode_varint(output.script.len() as u64, &mut out);
            out.extend_from_slice(&output.script);
            if out.len() > MAX_RECORD_BYTES {
                return Err(bad("record length"));
            }
        }
        Ok(out)
    }
    pub fn decode(txid: Txid, bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_RECORD_BYTES || bytes.len() < 3 || bytes[0] != 1 || bytes[1] & !57 != 0
        {
            return Err(bad("version, flags or size"));
        }
        let coinbase = bytes[1] & 1 != 0;
        let (metadata, n) = TransactionMetadata::decode(&bytes[2..], bytes[1] & !1, coinbase)
            .map_err(|e| Error(e.to_string()))?;
        let metadata = metadata.ok_or_else(|| bad("missing metadata"))?;
        let mut at = 2 + n;
        let count = read_var(bytes, &mut at, MAX_OUTPUTS as u64)? as usize;
        if count > bytes.len().saturating_sub(at) / 2 {
            return Err(bad("truncated output list"));
        }
        let mut outputs = Vec::with_capacity(count);
        for _ in 0..count {
            let value = read_var(bytes, &mut at, MAX_MONEY)?;
            let len = read_var(bytes, &mut at, 2_000_000)? as usize;
            let end = at.checked_add(len).ok_or_else(|| bad("script overflow"))?;
            let script = bytes
                .get(at..end)
                .ok_or_else(|| bad("truncated script"))?
                .to_vec();
            at = end;
            outputs.push(DisplayOutput { value, script });
        }
        if at != bytes.len() {
            return Err(bad("trailing data"));
        }
        let record = Self {
            txid,
            coinbase,
            metadata,
            outputs,
        };
        if record.encode()? != bytes {
            return Err(bad("noncanonical record"));
        }
        Ok(record)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DisplayTables {
    pub codec: String,
    pub records: u64,
    pub directory_segments: Vec<TableGeometry>,
    pub page_segments: Vec<TableGeometry>,
}
impl DisplayTables {
    pub fn validate(&self) -> Result<(), Error> {
        if self.codec != CODEC
            || self.directory_segments.is_empty()
            || self.page_segments.is_empty()
        {
            return Err(bad("display capability"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub txid: Txid,
    pub total: u32,
    pub first_page: u32,
    pub pages: u32,
    pub inline: Vec<u8>,
}
impl DirectoryEntry {
    fn encode(&self) -> Vec<u8> {
        let mut out = self.txid.0.to_vec();
        for v in [self.total, self.first_page, self.pages] {
            out.extend(v.to_le_bytes());
        }
        out.extend((self.inline.len() as u16).to_le_bytes());
        out.extend(&self.inline);
        out
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 46 {
            return Err(bad("directory entry"));
        }
        let txid = Txid(bytes[..32].try_into().unwrap());
        let total = u32_at(bytes, 32)?;
        let first_page = u32_at(bytes, 36)?;
        let pages = u32_at(bytes, 40)?;
        let n = u16::from_le_bytes(bytes[44..46].try_into().unwrap()) as usize;
        if bytes.len() != 46 + n || total == 0 || total as usize > MAX_RECORD_BYTES {
            return Err(bad("directory lengths"));
        }
        if total as usize <= INLINE_BYTES {
            if n != total as usize || first_page != 0 || pages != 0 {
                return Err(bad("inline extent"));
            }
            TransparentDisplayRecord::decode(txid, &bytes[46..])?;
        } else if n != 0
            || first_page == 0
            || pages as usize != (total as usize).div_ceil(FRAGMENT_BYTES)
        {
            return Err(bad("paged extent"));
        }
        Ok(Self {
            txid,
            total,
            first_page,
            pages,
            inline: bytes[46..].to_vec(),
        })
    }
}
fn u32_at(b: &[u8], i: usize) -> Result<u32, Error> {
    Ok(u32::from_le_bytes(
        b.get(i..i + 4)
            .ok_or_else(|| bad("truncated u32"))?
            .try_into()
            .unwrap(),
    ))
}
fn entries(row: &[u8]) -> Result<Vec<&[u8]>, Error> {
    if row.len() != ROW_BYTES {
        return Err(bad("row width"));
    }
    let count = u32_at(row, 0)? as usize;
    if count > (ROW_BYTES - 4) / 2 {
        return Err(bad("row entry count"));
    }
    let mut at = 4;
    let mut out = Vec::new();
    for _ in 0..count {
        let len = u16::from_le_bytes(
            row.get(at..at + 2)
                .ok_or_else(|| bad("row length"))?
                .try_into()
                .unwrap(),
        ) as usize;
        at += 2;
        out.push(
            row.get(at..at + len)
                .ok_or_else(|| bad("row entry length"))?,
        );
        at += len;
    }
    if row[at..].iter().any(|b| *b != 0) {
        return Err(bad("nonzero row padding"));
    }
    Ok(out)
}
fn push(row: &mut Vec<u8>, entry: &[u8]) -> bool {
    if row.len() + 2 + entry.len() > ROW_BYTES {
        return false;
    }
    let count = u32::from_le_bytes(row[..4].try_into().unwrap()) + 1;
    row[..4].copy_from_slice(&count.to_le_bytes());
    row.extend((entry.len() as u16).to_le_bytes());
    row.extend(entry);
    true
}
fn finish(mut row: Vec<u8>) -> Vec<u8> {
    row.resize(ROW_BYTES, 0);
    row
}
pub fn candidate_rows(txid: Txid, shard: u64, rows: u64) -> [u64; 2] {
    std::array::from_fn(|choice| {
        let mut hash = Sha256::new();
        hash.update(CODEC);
        hash.update(b"/directory/");
        hash.update([choice as u8]);
        hash.update(shard.to_le_bytes());
        hash.update(txid.0);
        let d = hash.finalize();
        u64::from_le_bytes(d[..8].try_into().unwrap()) % rows
    })
}
pub fn find_directory(rows: &[Vec<u8>], txid: Txid) -> Result<Option<DirectoryEntry>, Error> {
    let mut found = None;
    for row in rows {
        for raw in entries(row)? {
            let entry = DirectoryEntry::decode(raw)?;
            if entry.txid == txid {
                if found.is_some() {
                    return Err(bad("duplicate txid"));
                }
                found = Some(entry);
            }
        }
    }
    Ok(found)
}
pub fn assemble(
    entry: &DirectoryEntry,
    rows: &[Vec<u8>],
) -> Result<TransparentDisplayRecord, Error> {
    if !entry.inline.is_empty() {
        return TransparentDisplayRecord::decode(entry.txid, &entry.inline);
    }
    // Rows from all segments may be interleaved by transport; offsets establish order.
    let mut fragments = BTreeMap::new();
    for row in rows {
        for raw in entries(row)? {
            if raw.len() < 40 {
                return Err(bad("fragment header"));
            }
            if raw[..32] != entry.txid.0 {
                continue;
            }
            let offset = u32_at(raw, 32)?;
            let total = u32_at(raw, 36)?;
            if total != entry.total
                || raw.len() == 40
                || offset as usize >= total as usize
                || raw.len() - 40 > total as usize - offset as usize
                || !(offset as usize).is_multiple_of(FRAGMENT_BYTES)
                || raw.len() - 40 != (total as usize - offset as usize).min(FRAGMENT_BYTES)
                || fragments.insert(offset, &raw[40..]).is_some()
            {
                return Err(bad("fragment extent or duplicate"));
            }
        }
    }
    let mut bytes = Vec::new();
    for (offset, data) in fragments {
        if offset as usize != bytes.len() {
            return Err(bad("missing/reordered fragment"));
        }
        bytes.extend(data);
    }
    if bytes.len() != entry.total as usize {
        return Err(bad("incomplete output list"));
    }
    TransparentDisplayRecord::decode(entry.txid, &bytes)
}
pub struct BuiltTables {
    pub directory: Vec<Vec<u8>>,
    pub pages: Vec<Vec<u8>>,
    pub records: u64,
    pub payload_bytes: u64,
    pub page_rows: u64,
}
fn segments(mut rows: Vec<Vec<u8>>, count: usize) -> Vec<Vec<u8>> {
    if rows.is_empty() {
        rows.push(vec![0; 4]);
    }
    while !rows.len().is_multiple_of(count) {
        rows.push(vec![0; 4]);
    }
    rows.chunks(count)
        .map(|part| part.iter().flat_map(|r| finish(r.clone())).collect())
        .collect()
}
impl BuiltTables {
    pub fn manifest(&self, geometry: &Geometry) -> DisplayTables {
        let tables = |data: &[Vec<u8>], rows| {
            data.iter()
                .map(|b| TableGeometry {
                    rows,
                    row_bytes: ROW_BYTES as u32,
                    sha256: hex::encode(Sha256::digest(b)),
                })
                .collect()
        };
        DisplayTables {
            codec: CODEC.into(),
            records: self.records,
            directory_segments: tables(&self.directory, geometry.directory_rows),
            page_segments: tables(&self.pages, geometry.page_rows),
        }
    }
}
pub fn build(
    shard: u64,
    geometry: &Geometry,
    records: &[TransparentDisplayRecord],
) -> Result<BuiltTables, Error> {
    let mut sorted = BTreeMap::new();
    let mut payload_bytes = 0;
    for record in records {
        if sorted.insert(record.txid.0, record.encode()?).is_some() {
            return Err(bad("duplicate transaction identity"));
        }
    }
    let mut pages = vec![vec![0; 4]];
    let mut directory_rows = vec![vec![vec![0; 4]; geometry.directory_rows as usize]];
    for (id, payload) in &sorted {
        payload_bytes += payload.len() as u64;
        let mut entry = DirectoryEntry {
            txid: Txid(*id),
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
        let raw = entry.encode();
        let choices = candidate_rows(entry.txid, shard, geometry.directory_rows);
        let mut placed = false;
        for segment in &mut directory_rows {
            let [a, b] = choices.map(|r| r as usize);
            let choice = if segment[a].len() <= segment[b].len() {
                a
            } else {
                b
            };
            if push(&mut segment[choice], &raw) {
                placed = true;
                break;
            }
            if choice != a && push(&mut segment[a], &raw)
                || choice != b && push(&mut segment[b], &raw)
            {
                placed = true;
                break;
            }
        }
        if !placed {
            let mut segment = vec![vec![0; 4]; geometry.directory_rows as usize];
            assert!(push(&mut segment[choices[0] as usize], &raw));
            directory_rows.push(segment);
        }
    }
    let page_rows = if pages.len() == 1 && pages[0].len() == 4 {
        0
    } else {
        pages.len() as u64
    };
    Ok(BuiltTables {
        directory: directory_rows
            .into_iter()
            .map(|r| r.into_iter().flat_map(finish).collect())
            .collect(),
        pages: segments(pages, geometry.page_rows as usize),
        records: sorted.len() as u64,
        payload_bytes,
        page_rows,
    })
}
/// Validate placement and every full record at server load, not only on first query.
pub fn verify(
    shard: u64,
    geometry: &Geometry,
    directory: &[Vec<u8>],
    pages: &[Vec<u8>],
    count: u64,
) -> Result<(), Error> {
    verify_rows(
        shard,
        geometry,
        directory.len(),
        pages.len(),
        count,
        |table, segment, row| {
            let bytes = if table {
                &pages[segment]
            } else {
                &directory[segment]
            };
            bytes
                .get(row * ROW_BYTES..(row + 1) * ROW_BYTES)
                .map(|b| b.to_vec())
                .ok_or_else(|| bad("table row bounds"))
        },
    )
}
/// Stream already digest-verified files. Validation holds only an individual
/// record's fragments, rather than both complete display tables in memory.
pub fn verify_rows(
    shard: u64,
    geometry: &Geometry,
    directory_segments: usize,
    page_segments: usize,
    count: u64,
    mut row_at: impl FnMut(bool, usize, usize) -> Result<Vec<u8>, Error>,
) -> Result<(), Error> {
    let mut seen = BTreeSet::new();
    let mut paged = BTreeMap::new();
    for segment in 0..directory_segments {
        for row in 0..geometry.directory_rows as usize {
            let bytes = row_at(false, segment, row)?;
            for raw in entries(&bytes)? {
                let entry = DirectoryEntry::decode(raw)?;
                if !candidate_rows(entry.txid, shard, geometry.directory_rows)
                    .contains(&(row as u64))
                    || !seen.insert(entry.txid.0)
                {
                    return Err(bad("directory placement/duplicate"));
                }
                if entry.pages > 0 {
                    let end = (entry.first_page as usize - 1)
                        .checked_add(entry.pages as usize)
                        .ok_or_else(|| bad("page extent"))?;
                    if end > page_segments * geometry.page_rows as usize {
                        return Err(bad("page extent"));
                    }
                    let rows: Vec<_> = (entry.first_page as usize - 1..end)
                        .map(|r| {
                            row_at(
                                true,
                                r / geometry.page_rows as usize,
                                r % geometry.page_rows as usize,
                            )
                        })
                        .collect::<Result<_, _>>()?;
                    assemble(&entry, &rows)?;
                    paged.insert(
                        entry.txid.0,
                        (entry.total, entry.first_page as usize - 1, end),
                    );
                }
            }
        }
    }
    if seen.len() as u64 != count {
        return Err(bad("record count"));
    }
    // No malformed or orphan fragments may hide in an unqueried page.
    for segment in 0..page_segments {
        for row in 0..geometry.page_rows as usize {
            let bytes = row_at(true, segment, row)?;
            for raw in entries(&bytes)? {
                if raw.len() <= 40 {
                    return Err(bad("fragment header/length"));
                }
                let id: [u8; 32] = raw[..32].try_into().unwrap();
                let (total, first, end) =
                    paged.get(&id).ok_or_else(|| bad("orphan page fragment"))?;
                let position = segment * geometry.page_rows as usize + row;
                if !(*first..*end).contains(&position) || u32_at(raw, 36)? != *total {
                    return Err(bad("fragment outside declared extent"));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_events::FeeState;
    fn record(n: usize) -> TransparentDisplayRecord {
        TransparentDisplayRecord {
            txid: Txid([7; 32]),
            coinbase: false,
            metadata: TransactionMetadata {
                fee: FeeState::Exact(10_000),
                transparent_input_count: 1,
                has_shielded_components: false,
            },
            outputs: vec![DisplayOutput {
                value: 5,
                script: vec![0x51; n],
            }],
        }
    }
    #[test]
    fn canonical_and_bounds() {
        let r = record(25);
        let b = r.encode().unwrap();
        assert_eq!(TransparentDisplayRecord::decode(r.txid, &b).unwrap(), r);
        for i in 0..b.len() {
            assert!(TransparentDisplayRecord::decode(r.txid, &b[..i]).is_err());
        }
        let mut b = b;
        b[1] |= 128;
        assert!(TransparentDisplayRecord::decode(r.txid, &b).is_err());
        let mut r = record(0);
        r.outputs[0].value = MAX_MONEY + 1;
        assert!(r.encode().is_err());
        let mut r = record(0);
        r.metadata.transparent_input_count = u32::MAX;
        assert!(r.encode().is_err());
        let mut r = record(0);
        r.metadata.fee = FeeState::Unknown;
        let unknown = r.encode().unwrap();
        assert_eq!(
            TransparentDisplayRecord::decode(r.txid, &unknown).unwrap(),
            r
        );
        r.metadata.fee = FeeState::Exact(0);
        assert_ne!(r.encode().unwrap(), unknown);
        r.outputs.clear();
        assert_eq!(
            TransparentDisplayRecord::decode(r.txid, &r.encode().unwrap()).unwrap(),
            r
        );
        r.coinbase = true;
        assert!(r.encode().is_err());
        r.metadata.fee = FeeState::NotApplicable;
        r.metadata.transparent_input_count = 0;
        assert_eq!(
            TransparentDisplayRecord::decode(r.txid, &r.encode().unwrap()).unwrap(),
            r
        );
        // Overlong unsigned LEB128 input count and trailing data are rejected.
        let r = record(1);
        let mut b = r.encode().unwrap();
        b.splice(2..3, [0x81, 0]);
        assert!(TransparentDisplayRecord::decode(r.txid, &b).is_err());
        let mut b = r.encode().unwrap();
        b.push(0);
        assert!(TransparentDisplayRecord::decode(r.txid, &b).is_err());
    }
    #[test]
    fn packed_overflow_and_incomplete() {
        let g = crate::RECENT_4K;
        for n in [0, 120, 128, 9000] {
            let r = record(n);
            let t = build(0, &g, std::slice::from_ref(&r)).unwrap();
            verify(0, &g, &t.directory, &t.pages, 1).unwrap();
            let candidates = candidate_rows(r.txid, 0, g.directory_rows);
            let rows: Vec<_> = candidates
                .into_iter()
                .collect::<BTreeSet<_>>()
                .iter()
                .map(|i| {
                    t.directory[0][*i as usize * ROW_BYTES..(*i as usize + 1) * ROW_BYTES].to_vec()
                })
                .collect();
            let e = find_directory(&rows, r.txid).unwrap().unwrap();
            if e.pages > 0 {
                assert!(assemble(&e, &[]).is_err());
            } else {
                assert_eq!(assemble(&e, &[]).unwrap(), r);
            }
            assert!(build(0, &g, &[r.clone(), r]).is_err());
        }
    }
    #[test]
    fn segments_fragment_identity_offsets_and_orphans() {
        // Tiny synthetic geometry exercises multiple segments without inventing
        // confirmed-chain evidence. Native demo uses the registered 4k geometry.
        let g = Geometry {
            directory_rows: 1,
            page_rows: 1,
            ..crate::RECENT_4K
        };
        let mut records = Vec::new();
        for id in 1..=64 {
            let mut r = record(80);
            r.txid = Txid([id; 32]);
            records.push(r);
        }
        let mut large = record(9000);
        large.txid = Txid([99; 32]);
        records.push(large.clone());
        let t = build(0, &g, &records).unwrap();
        assert!(t.directory.len() > 1 && t.pages.len() > 1);
        verify(0, &g, &t.directory, &t.pages, records.len() as u64).unwrap();
        let rows: Vec<_> = t
            .directory
            .iter()
            .map(|b| b[..ROW_BYTES].to_vec())
            .collect();
        let entry = find_directory(&rows, large.txid).unwrap().unwrap();
        let pages: Vec<_> = t.pages.iter().map(|b| b[..ROW_BYTES].to_vec()).collect();
        assert_eq!(assemble(&entry, &pages).unwrap(), large);
        assert!(assemble(&entry, &pages[..pages.len() - 1]).is_err());
        let mut bad_pages = pages.clone();
        // row count (4), entry length (2), txid (32), then offset.
        bad_pages[0][38..42].copy_from_slice(&1u32.to_le_bytes());
        assert!(assemble(&entry, &bad_pages).is_err());
        let mut duplicate = pages.clone();
        duplicate.push(pages[0].clone());
        assert!(assemble(&entry, &duplicate).is_err());
        let mut orphan = t.pages.clone();
        orphan[0][6..38].fill(44);
        assert!(verify(0, &g, &t.directory, &orphan, records.len() as u64).is_err());
        let mut duplicate_dirs = t.directory.clone();
        duplicate_dirs.push(t.directory[0].clone());
        assert!(verify(0, &g, &duplicate_dirs, &t.pages, records.len() as u64).is_err());
    }
}
