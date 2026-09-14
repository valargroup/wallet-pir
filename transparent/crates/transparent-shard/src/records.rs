//! On-disk record shapes for a shard's private tables.
//!
//! Every row is fixed width and every field is at a fixed offset, because a PIR
//! response has fixed geometry: a row whose size depended on what it held would
//! leak that through the response, and a table whose rows varied could not be
//! addressed at all. Padding is therefore not waste, it is the mechanism.
//!
//! # What a decoder must not trust
//!
//! These bytes arrive from a PIR response, which means they arrive from a
//! server that chose them. A decoder must treat every field as a claim:
//!
//! - the **script** in a record is checked against the script the client asked
//!   for, because the row is selected by a hash of the script and a hash can
//!   collide, be mis-hashed, or be misplaced by a faulty builder;
//! - the **page locators** are checked against the page's own header, because a
//!   directory entry could point anywhere;
//! - **unused bytes must be zero**, so that a server cannot smuggle data past a
//!   client that ignores them, and so two implementations cannot disagree about
//!   a record while both calling it valid.

use crate::layout::{DIRECTORY_ROW_BYTES, EVENTS_PER_PAGE, INLINE_EVENTS};
use transparent_events::{EventError, TransparentEvent, EVENT_BYTES};

/// Longest script a private table can hold.
///
/// P2PKH is 25 bytes and P2SH is 23, so this covers the profile's supported
/// classes with room to spare. It is a *coverage* limit, not a filter limit:
/// the public filter contains every element the block yields, including scripts
/// too long to index here.
///
/// That asymmetry has to be published rather than papered over. A wallet whose
/// script exceeds this is outside coverage, and must be told so — treating a
/// directory miss as absence would report "no activity" for a script that has
/// activity. The shard manifest carries this limit and the count of scripts it
/// excluded, so the gap is visible and measured instead of silent.
pub const MAX_SCRIPT_BYTES: usize = 40;

/// Bytes of an entry before its inline events.
///
/// Split out from [`DIRECTORY_ENTRY_BYTES`] because the inline allowance is the
/// only part of an entry a geometry sweep can move, and the rest is what it
/// cannot: the script it is keyed by, its length, and the locators. At two
/// inline events the header is 56 bytes of a 248-byte entry, so 77% of the
/// directory's width is the events it carries to save a page query.
pub const DIRECTORY_ENTRY_HEADER_BYTES: usize = 2 + MAX_SCRIPT_BYTES + 4 + 1 + 1 + 4 + 4;

/// Bytes in one directory entry.
pub const DIRECTORY_ENTRY_BYTES: usize =
    DIRECTORY_ENTRY_HEADER_BYTES + INLINE_EVENTS as usize * EVENT_BYTES;

/// Bytes at the head of a directory row, before its entries.
pub const DIRECTORY_ROW_HEADER_BYTES: usize = 4;

/// Entries one directory row holds.
pub const DIRECTORY_SLOTS: usize =
    (DIRECTORY_ROW_BYTES - DIRECTORY_ROW_HEADER_BYTES) / DIRECTORY_ENTRY_BYTES;

// Directory entry field offsets.
const DE_SCRIPT_LEN: usize = 0;
const DE_SCRIPT: usize = 2;
const DE_TOTAL_EVENTS: usize = DE_SCRIPT + MAX_SCRIPT_BYTES;
const DE_INLINE_COUNT: usize = DE_TOTAL_EVENTS + 4;
const DE_RESERVED: usize = DE_INLINE_COUNT + 1;
const DE_FIRST_PAGE: usize = DE_RESERVED + 1;
const DE_PAGE_COUNT: usize = DE_FIRST_PAGE + 4;
const DE_INLINE_EVENTS: usize = DE_PAGE_COUNT + 4;

/// A row must be able to hold the slots it advertises, and a page header must
/// fit the space reserved for it. These are properties of the constants above,
/// so a layout change that broke one should fail the build rather than a test.
const _: () = assert!(
    DIRECTORY_ROW_HEADER_BYTES + DIRECTORY_SLOTS * DIRECTORY_ENTRY_BYTES <= DIRECTORY_ROW_BYTES
);

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
    #[error("a row holds at most {DIRECTORY_SLOTS} entries, was given {0}")]
    TooManyEntries(usize),
    #[error("a fragment holds at most {EVENTS_PER_PAGE} events, was given {0}")]
    TooManyEvents(usize),
}

/// One script's directory record.
///
/// `total_events` is the script's whole history *within this shard*, so a
/// client can tell a complete inline history from a truncated one, and can
/// check that the pages it retrieves account for every event the directory
/// promised.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub script: Vec<u8>,
    pub total_events: u32,
    /// The newest events, carried here so a short history needs no page query.
    pub inline: Vec<TransparentEvent>,
    /// Row index of this script's first page, within the shard's page table.
    pub first_page: u32,
    pub page_count: u32,
}

impl DirectoryEntry {
    pub fn encode(&self) -> Result<[u8; DIRECTORY_ENTRY_BYTES], RecordError> {
        if self.script.len() > MAX_SCRIPT_BYTES {
            return Err(RecordError::ScriptTooLong(self.script.len()));
        }
        if self.inline.len() > INLINE_EVENTS as usize {
            return Err(RecordError::Malformed(format!(
                "{} inline events, at most {INLINE_EVENTS} fit",
                self.inline.len()
            )));
        }
        let mut bytes = [0u8; DIRECTORY_ENTRY_BYTES];
        bytes[DE_SCRIPT_LEN..DE_SCRIPT].copy_from_slice(&(self.script.len() as u16).to_le_bytes());
        bytes[DE_SCRIPT..DE_SCRIPT + self.script.len()].copy_from_slice(&self.script);
        bytes[DE_TOTAL_EVENTS..DE_INLINE_COUNT].copy_from_slice(&self.total_events.to_le_bytes());
        bytes[DE_INLINE_COUNT] = self.inline.len() as u8;
        bytes[DE_FIRST_PAGE..DE_PAGE_COUNT].copy_from_slice(&self.first_page.to_le_bytes());
        bytes[DE_PAGE_COUNT..DE_INLINE_EVENTS].copy_from_slice(&self.page_count.to_le_bytes());
        for (index, event) in self.inline.iter().enumerate() {
            let at = DE_INLINE_EVENTS + index * EVENT_BYTES;
            bytes[at..at + EVENT_BYTES].copy_from_slice(&event.to_bytes());
        }
        Ok(bytes)
    }

    /// Decodes one entry, or `None` for an empty slot.
    ///
    /// An empty slot and an occupied one are the same size and are
    /// indistinguishable in a response; only the decoded content differs. That
    /// is deliberate — a row whose occupancy showed in its geometry would leak
    /// how many scripts share a bucket.
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, RecordError> {
        if bytes.len() != DIRECTORY_ENTRY_BYTES {
            return Err(RecordError::Length {
                got: bytes.len(),
                want: DIRECTORY_ENTRY_BYTES,
            });
        }
        let script_len =
            u16::from_le_bytes(bytes[DE_SCRIPT_LEN..DE_SCRIPT].try_into().expect("2 bytes"))
                as usize;
        if script_len == 0 {
            // An empty slot must be entirely zero. A slot that carried a
            // payload behind a zero length would be a channel a server could
            // use and a client would never look at.
            if bytes.iter().any(|byte| *byte != 0) {
                return Err(RecordError::Malformed(
                    "an empty directory slot is not all zero".into(),
                ));
            }
            return Ok(None);
        }
        if script_len > MAX_SCRIPT_BYTES {
            return Err(RecordError::ScriptTooLong(script_len));
        }
        // Padding after the script must be zero, or the same script could be
        // encoded many ways and its digest would not be stable.
        if bytes[DE_SCRIPT + script_len..DE_TOTAL_EVENTS]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(RecordError::Malformed("script padding is not zero".into()));
        }
        if bytes[DE_RESERVED] != 0 {
            return Err(RecordError::Reserved);
        }

        let total_events = u32::from_le_bytes(
            bytes[DE_TOTAL_EVENTS..DE_INLINE_COUNT]
                .try_into()
                .expect("4 bytes"),
        );
        let inline_count = bytes[DE_INLINE_COUNT] as usize;
        if inline_count > INLINE_EVENTS as usize {
            return Err(RecordError::Malformed(format!(
                "entry claims {inline_count} inline events"
            )));
        }
        let first_page = u32::from_le_bytes(
            bytes[DE_FIRST_PAGE..DE_PAGE_COUNT]
                .try_into()
                .expect("4 bytes"),
        );
        let page_count = u32::from_le_bytes(
            bytes[DE_PAGE_COUNT..DE_INLINE_EVENTS]
                .try_into()
                .expect("4 bytes"),
        );

        let mut inline = Vec::with_capacity(inline_count);
        for index in 0..inline_count {
            let at = DE_INLINE_EVENTS + index * EVENT_BYTES;
            inline.push(TransparentEvent::from_bytes(&bytes[at..at + EVENT_BYTES])?);
        }
        // Unused inline slots must be zero, for the same reason as the padding.
        let used = DE_INLINE_EVENTS + inline_count * EVENT_BYTES;
        if bytes[used..].iter().any(|byte| *byte != 0) {
            return Err(RecordError::Malformed(
                "unused inline event slots are not zero".into(),
            ));
        }

        // The counts have to agree with each other. A directory that promised
        // more history than its pages could hold would leave a client unable to
        // tell a short history from a truncated one.
        let inline_u32 = inline_count as u32;
        if total_events < inline_u32 {
            return Err(RecordError::Malformed(format!(
                "entry holds {inline_count} inline events but claims {total_events} in total"
            )));
        }
        if page_count == 0 && total_events != inline_u32 {
            return Err(RecordError::Malformed(format!(
                "entry claims {total_events} events with no pages but only {inline_count} inline"
            )));
        }

        Ok(Some(Self {
            script: bytes[DE_SCRIPT..DE_SCRIPT + script_len].to_vec(),
            total_events,
            inline,
            first_page,
            page_count,
        }))
    }
}

/// Packs entries into one fixed-width directory row.
///
/// The count in the header is what a decoder reads; the rest of the row is
/// zero. Every row is the same size whether it holds one entry or all of them.
pub fn encode_directory_row(entries: &[DirectoryEntry]) -> Result<Vec<u8>, RecordError> {
    if entries.len() > DIRECTORY_SLOTS {
        return Err(RecordError::TooManyEntries(entries.len()));
    }
    let mut row = vec![0u8; DIRECTORY_ROW_BYTES];
    row[..4].copy_from_slice(&(entries.len() as u32).to_le_bytes());
    for (index, entry) in entries.iter().enumerate() {
        let at = DIRECTORY_ROW_HEADER_BYTES + index * DIRECTORY_ENTRY_BYTES;
        row[at..at + DIRECTORY_ENTRY_BYTES].copy_from_slice(&entry.encode()?);
    }
    Ok(row)
}

/// Reads every entry in a decoded directory row.
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
    let mut entries = Vec::with_capacity(count);
    for index in 0..count {
        let at = DIRECTORY_ROW_HEADER_BYTES + index * DIRECTORY_ENTRY_BYTES;
        match DirectoryEntry::decode(&row[at..at + DIRECTORY_ENTRY_BYTES])? {
            Some(entry) => entries.push(entry),
            None => {
                return Err(RecordError::Malformed(format!(
                    "slot {index} is empty but the row claims {count} entries"
                )))
            }
        }
    }
    // Slots past the claimed count must be empty, so the count cannot hide
    // entries a client would never decode.
    let used = DIRECTORY_ROW_HEADER_BYTES + count * DIRECTORY_ENTRY_BYTES;
    if row[used..].iter().any(|byte| *byte != 0) {
        return Err(RecordError::Malformed(
            "a directory row has content past its entry count".into(),
        ));
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

    fn entry(tag: u8, total: u32, inline: usize, pages: u32) -> DirectoryEntry {
        DirectoryEntry {
            script: script(tag),
            total_events: total,
            inline: (0..inline)
                .map(|i| event(100 + i as u32, i as u8))
                .collect(),
            first_page: 7,
            page_count: pages,
        }
    }

    /// The pinned numbers, restated so a change to the layout has to be a
    /// deliberate edit here rather than a silent shift in every table.
    #[test]
    fn the_geometry_is_what_the_layout_promises() {
        assert_eq!(DIRECTORY_ENTRY_BYTES, 248);
        assert_eq!(DIRECTORY_SLOTS, 14);
    }

    #[test]
    fn a_directory_entry_round_trips() {
        for e in [entry(1, 2, 2, 0), entry(2, 500, 2, 3), entry(3, 1, 1, 0)] {
            let encoded = e.encode().unwrap();
            assert_eq!(DirectoryEntry::decode(&encoded).unwrap(), Some(e));
        }
    }

    #[test]
    fn an_empty_slot_decodes_as_absent() {
        let empty = [0u8; DIRECTORY_ENTRY_BYTES];
        assert_eq!(DirectoryEntry::decode(&empty).unwrap(), None);
    }

    /// A slot that hid a payload behind a zero length would be a channel a
    /// server could use and a client would never inspect.
    #[test]
    fn an_empty_slot_carrying_content_is_refused() {
        let mut empty = [0u8; DIRECTORY_ENTRY_BYTES];
        empty[DIRECTORY_ENTRY_BYTES - 1] = 1;
        assert!(DirectoryEntry::decode(&empty).is_err());
    }

    /// Every row is the same size regardless of how full it is. A row whose
    /// occupancy showed in its geometry would leak how many scripts share a
    /// bucket, which is information about the scripts themselves.
    #[test]
    fn every_directory_row_is_the_same_size_however_full() {
        let full: Vec<DirectoryEntry> = (0..DIRECTORY_SLOTS)
            .map(|i| entry(i as u8, 2, 2, 0))
            .collect();
        for entries in [vec![], vec![entry(1, 2, 2, 0)], full] {
            assert_eq!(
                encode_directory_row(&entries).unwrap().len(),
                DIRECTORY_ROW_BYTES
            );
        }
    }

    #[test]
    fn a_directory_row_round_trips_and_rejects_overfill() {
        let entries: Vec<DirectoryEntry> = (0..DIRECTORY_SLOTS)
            .map(|i| entry(i as u8, 2, 2, 0))
            .collect();
        let row = encode_directory_row(&entries).unwrap();
        assert_eq!(decode_directory_row(&row).unwrap(), entries);

        let too_many: Vec<DirectoryEntry> = (0..DIRECTORY_SLOTS + 1)
            .map(|i| entry(i as u8, 2, 2, 0))
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
        let mut row = encode_directory_row(&[entry(1, 2, 2, 0)]).unwrap();
        row[DIRECTORY_ROW_BYTES - 1] = 1;
        assert!(decode_directory_row(&row).is_err());
    }

    /// The counts must be consistent, or a client cannot tell a complete short
    /// history from one that was truncated.
    #[test]
    fn an_entry_whose_counts_disagree_is_refused() {
        // Claims more events than it holds, with no pages to hold the rest.
        let mut bad = entry(1, 2, 2, 0);
        bad.total_events = 9;
        let encoded = bad.encode().unwrap();
        assert!(DirectoryEntry::decode(&encoded).is_err());

        // Claims fewer events in total than it carries inline.
        let mut bad = entry(1, 2, 2, 0);
        bad.total_events = 1;
        let encoded = bad.encode().unwrap();
        assert!(DirectoryEntry::decode(&encoded).is_err());
    }

    #[test]
    fn a_script_too_long_for_a_private_table_is_refused_not_truncated() {
        let entry = DirectoryEntry {
            script: vec![0xab; MAX_SCRIPT_BYTES + 1],
            total_events: 1,
            inline: vec![event(100, 0)],
            first_page: 0,
            page_count: 0,
        };
        assert_eq!(
            entry.encode(),
            Err(RecordError::ScriptTooLong(MAX_SCRIPT_BYTES + 1))
        );
    }
}
