//! Variable-length entries inside fixed-width schema-v10 directory rows.
use crate::compact;
use crate::layout::{DIRECTORY_ROW_BYTES, EVENTS_PER_PAGE, INLINE_EVENTS};
use crate::tag::SCRIPT_TAG_BYTES;
use transparent_events::{EventError, TransparentEvent};

pub const MAX_SCRIPT_BYTES: usize = 40;
pub const DIRECTORY_ENTRY_HEADER_BYTES: usize = SCRIPT_TAG_BYTES + 4 + 1;
pub const DIRECTORY_ROW_HEADER_BYTES: usize = 4;
pub const MAX_DIRECTORY_ENTRY_BYTES: usize =
    DIRECTORY_ENTRY_HEADER_BYTES + INLINE_EVENTS as usize * compact::SPEND_BYTES;
/// Maximum count, attained with one receive per entry. Actual capacity is bytes.
pub const DIRECTORY_SLOTS: usize = (DIRECTORY_ROW_BYTES - DIRECTORY_ROW_HEADER_BYTES)
    / (DIRECTORY_ENTRY_HEADER_BYTES + compact::RECEIVE_BYTES);
const DE_TAG: usize = 0;
const DE_FIRST_PAGE: usize = SCRIPT_TAG_BYTES;
const DE_INLINE_COUNT: usize = DE_FIRST_PAGE + 4;
const DE_INLINE_EVENTS: usize = DE_INLINE_COUNT + 1;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RecordError {
    #[error("record is {got} bytes, expected {want}")]
    Length { got: usize, want: usize },
    #[error("script is {0} bytes, more than a private table can hold")]
    ScriptTooLong(usize),
    #[error("reserved bytes are not zero")]
    Reserved,
    #[error("{0}")]
    Malformed(String),
    #[error("event: {0}")]
    Event(#[from] EventError),
    #[error("too many entries: {0}")]
    TooManyEntries(usize),
    #[error("a fragment holds at most {EVENTS_PER_PAGE} events, was given {0}")]
    TooManyEvents(usize),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    /// Placement key, omitted from the wire. Decoding leaves it empty.
    pub script: Vec<u8>,
    pub tag: [u8; SCRIPT_TAG_BYTES],
    /// Builder metadata, omitted from the wire.
    pub event_count: u32,
    pub inline: Vec<TransparentEvent>,
    /// 1-based start of the contiguous page run; zero means inline-only.
    pub first_page: u32,
}

impl DirectoryEntry {
    pub fn page_base(&self) -> Option<u32> {
        self.first_page.checked_sub(1)
    }
    pub fn encoded_len(&self) -> usize {
        DIRECTORY_ENTRY_HEADER_BYTES + compact::encoded_len(&self.inline)
    }

    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        if self.script.len() > MAX_SCRIPT_BYTES {
            return Err(RecordError::ScriptTooLong(self.script.len()));
        }
        if self.inline.is_empty()
            || self.inline.len() > INLINE_EVENTS as usize
            || self.first_page != 0 && self.inline.len() != INLINE_EVENTS as usize
        {
            return Err(RecordError::Malformed("invalid inline count".into()));
        }
        if self
            .inline
            .windows(2)
            .any(|w| w[0].sort_key() > w[1].sort_key())
        {
            return Err(RecordError::Malformed("inline events out of order".into()));
        }
        let mut bytes = Vec::with_capacity(self.encoded_len());
        bytes.extend_from_slice(&self.tag);
        bytes.extend_from_slice(&self.first_page.to_le_bytes());
        bytes.push(self.inline.len() as u8);
        compact::encode(&self.inline, &mut bytes)?;
        Ok(bytes)
    }

    /// Decode one occupied entry from a row prefix and report its exact length.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), RecordError> {
        if bytes.len() < DIRECTORY_ENTRY_HEADER_BYTES {
            return Err(RecordError::Malformed("truncated directory entry".into()));
        }
        let tag = bytes[DE_TAG..DE_FIRST_PAGE].try_into().expect("14 bytes");
        let first_page = u32::from_le_bytes(
            bytes[DE_FIRST_PAGE..DE_INLINE_COUNT]
                .try_into()
                .expect("4 bytes"),
        );
        let count = bytes[DE_INLINE_COUNT] as usize;
        if count == 0
            || count > INLINE_EVENTS as usize
            || first_page != 0 && count != INLINE_EVENTS as usize
        {
            return Err(RecordError::Malformed("invalid inline count".into()));
        }
        let (inline, used) = compact::decode(&bytes[DE_INLINE_EVENTS..], count)?;
        if inline.windows(2).any(|w| w[0].sort_key() > w[1].sort_key()) {
            return Err(RecordError::Malformed("inline events out of order".into()));
        }
        Ok((
            Self {
                script: Vec::new(),
                tag,
                event_count: count as u32,
                inline,
                first_page,
            },
            DE_INLINE_EVENTS + used,
        ))
    }
}

pub fn encode_directory_row(entries: &[DirectoryEntry]) -> Result<Vec<u8>, RecordError> {
    if entries.len() > DIRECTORY_SLOTS {
        return Err(RecordError::TooManyEntries(entries.len()));
    }
    let mut row = Vec::with_capacity(DIRECTORY_ROW_BYTES);
    row.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (i, entry) in entries.iter().enumerate() {
        if entries[..i].iter().any(|other| other.tag == entry.tag) {
            return Err(RecordError::Malformed("duplicate directory tag".into()));
        }
        row.extend_from_slice(&entry.encode()?);
    }
    if row.len() > DIRECTORY_ROW_BYTES {
        return Err(RecordError::Malformed(
            "directory row byte capacity exceeded".into(),
        ));
    }
    row.resize(DIRECTORY_ROW_BYTES, 0);
    Ok(row)
}

pub fn decode_directory_row(row: &[u8]) -> Result<Vec<DirectoryEntry>, RecordError> {
    if row.len() != DIRECTORY_ROW_BYTES {
        return Err(RecordError::Length {
            got: row.len(),
            want: DIRECTORY_ROW_BYTES,
        });
    }
    let count = u32::from_le_bytes(row[..4].try_into().expect("4 bytes")) as usize;
    if count > DIRECTORY_SLOTS {
        return Err(RecordError::TooManyEntries(count));
    }
    let mut entries: Vec<DirectoryEntry> = Vec::with_capacity(count);
    let mut at = DIRECTORY_ROW_HEADER_BYTES;
    for _ in 0..count {
        let (entry, used) = DirectoryEntry::decode(&row[at..])?;
        if entries.iter().any(|other| other.tag == entry.tag) {
            return Err(RecordError::Malformed("duplicate directory tag".into()));
        }
        entries.push(entry);
        at += used;
    }
    if row[at..].iter().any(|b| *b != 0) {
        return Err(RecordError::Reserved);
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_events::{ReceiveEvent, Txid};

    fn script(tag: u8) -> Vec<u8> {
        let mut bytes = vec![0x76, 0xa9, 0x14];
        bytes.extend_from_slice(&[tag; 20]);
        bytes.extend_from_slice(&[0x88, 0xac]);
        bytes
    }

    fn event(height: u32, nonce: u8) -> TransparentEvent {
        TransparentEvent::Receive(ReceiveEvent {
            height,
            txid: Txid([nonce; 32]),
            transaction_index: 0,
            output_index: nonce as u32,
            value: 1_000,
            coinbase: false,
        })
    }

    fn entry(byte: u8, inline: usize, first_page: u32) -> DirectoryEntry {
        let mut tag = [0u8; crate::tag::SCRIPT_TAG_BYTES];
        tag[0] = byte;
        DirectoryEntry {
            script: script(byte),
            tag,
            event_count: inline as u32,
            inline: (0..inline)
                .map(|i| event(100 + i as u32, i as u8))
                .collect(),
            first_page,
        }
    }

    fn decoded_eq(decoded: &DirectoryEntry, built: &DirectoryEntry) {
        assert_eq!(decoded.tag, built.tag);
        assert_eq!(decoded.inline, built.inline);
        assert_eq!(decoded.first_page, built.first_page);
        assert!(decoded.script.is_empty());
    }

    /// The pinned numbers, restated so a change to the layout has to be a
    /// deliberate edit here rather than a silent shift in every table.
    #[test]
    fn the_geometry_is_what_the_layout_promises() {
        assert_eq!(MAX_DIRECTORY_ENTRY_BYTES, 177);
        assert_eq!(DIRECTORY_SLOTS, 58);
        assert_eq!(entry(1, 1, 0).encoded_len(), 70);
        assert_eq!(entry(1, 2, 0).encoded_len(), 121);
    }

    #[test]
    fn a_directory_entry_round_trips() {
        for e in [entry(1, 2, 0), entry(2, 2, 8), entry(3, 1, 0)] {
            let encoded = e.encode().unwrap();
            decoded_eq(&DirectoryEntry::decode(&encoded).unwrap().0, &e);
        }
    }

    #[test]
    fn an_empty_slot_decodes_as_absent() {
        assert!(decode_directory_row(&[0; DIRECTORY_ROW_BYTES])
            .unwrap()
            .is_empty());
    }

    /// A slot that hid a payload behind a zero length would be a channel a
    /// server could use and a client would never inspect.
    #[test]
    fn an_empty_slot_carrying_content_is_refused() {
        let mut empty = [0u8; MAX_DIRECTORY_ENTRY_BYTES];
        empty[MAX_DIRECTORY_ENTRY_BYTES - 1] = 1;
        assert!(DirectoryEntry::decode(&empty).is_err());
    }

    /// Every row is the same size regardless of how full it is. A row whose
    /// occupancy showed in its geometry would leak how many scripts share a
    /// bucket, which is information about the scripts themselves.
    #[test]
    fn every_directory_row_is_the_same_size_however_full() {
        let full: Vec<DirectoryEntry> = (0..DIRECTORY_SLOTS)
            .map(|i| entry(i as u8 + 1, 1, 0))
            .collect();
        for entries in [vec![], vec![entry(1, 2, 0)], full] {
            assert_eq!(
                encode_directory_row(&entries).unwrap().len(),
                DIRECTORY_ROW_BYTES
            );
        }
    }

    #[test]
    fn a_directory_row_round_trips_and_rejects_overfill() {
        let entries: Vec<DirectoryEntry> = (0..DIRECTORY_SLOTS)
            .map(|i| entry(i as u8 + 1, 1, 0))
            .collect();
        let row = encode_directory_row(&entries).unwrap();
        let decoded = decode_directory_row(&row).unwrap();
        assert_eq!(decoded.len(), entries.len());
        for (got, want) in decoded.iter().zip(&entries) {
            decoded_eq(got, want);
        }

        let too_many: Vec<DirectoryEntry> = (0..DIRECTORY_SLOTS + 1)
            .map(|i| entry((i + 1) as u8, 2, 0))
            .collect();
        assert_eq!(
            encode_directory_row(&too_many),
            Err(RecordError::TooManyEntries(DIRECTORY_SLOTS + 1))
        );
    }

    /// A count that understated the row would leave entries a client never
    /// decodes, which is indistinguishable from those scripts having no history.
    #[test]
    fn a_row_with_content_past_its_count_is_refused() {
        let mut row = encode_directory_row(&[entry(1, 2, 0)]).unwrap();
        row[DIRECTORY_ROW_BYTES - 1] = 1;
        assert!(decode_directory_row(&row).is_err());
    }

    /// Inline count is explicit and bounded; a page locator requires two
    /// inline events so the newest suffix has one canonical representation.
    #[test]
    fn inline_count_is_bounded_and_paged_entries_require_two() {
        for count in [0, 3, 255] {
            let mut encoded = entry(1, 1, 0).encode().unwrap();
            encoded[DE_INLINE_COUNT] = count;
            assert!(DirectoryEntry::decode(&encoded).is_err());
        }
        assert!(entry(1, 1, 4).encode().is_err());
    }

    #[test]
    fn a_duplicate_tag_in_a_row_is_refused() {
        assert!(encode_directory_row(&[entry(1, 1, 0), entry(1, 1, 0)]).is_err());
        let mut row = encode_directory_row(&[entry(1, 1, 0), entry(2, 1, 0)]).unwrap();
        row[4 + 70] = 1;
        assert!(decode_directory_row(&row).is_err());
    }

    #[test]
    fn a_script_too_long_for_a_private_table_is_refused_not_truncated() {
        let entry = DirectoryEntry {
            script: vec![0xab; MAX_SCRIPT_BYTES + 1],
            tag: [1; crate::tag::SCRIPT_TAG_BYTES],
            event_count: 1,
            inline: vec![event(100, 0)],
            first_page: 0,
        };
        assert_eq!(
            entry.encode(),
            Err(RecordError::ScriptTooLong(MAX_SCRIPT_BYTES + 1))
        );
    }
}
