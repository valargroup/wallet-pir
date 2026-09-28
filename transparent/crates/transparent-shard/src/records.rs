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
//! - the **script tag** in a record is checked against the tag the client
//!   derived, because the row is selected by a hash of the script and a hash
//!   can collide, be mis-hashed, or be misplaced by a faulty builder;
//! - the **page locators** are checked against the page's own header, because a
//!   directory entry could point anywhere;
//! - **unused bytes must be zero**, so that a server cannot smuggle data past a
//!   client that ignores them, and so two implementations cannot disagree about
//!   a record while both calling it valid.

use crate::layout::{DIRECTORY_ROW_BYTES, EVENTS_PER_PAGE, INLINE_EVENTS};
use crate::tag::SCRIPT_TAG_BYTES;
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
/// cannot: the 14-byte script tag and the 1-based first-page locator. At two
/// inline events the header is 18 bytes of a 192-byte entry.
pub const DIRECTORY_ENTRY_HEADER_BYTES: usize = SCRIPT_TAG_BYTES + 4;

/// Bytes in one directory entry.
pub const DIRECTORY_ENTRY_BYTES: usize =
    DIRECTORY_ENTRY_HEADER_BYTES + INLINE_EVENTS as usize * EVENT_BYTES;

/// Bytes at the head of a directory row, before its entries.
pub const DIRECTORY_ROW_HEADER_BYTES: usize = 4;

/// Entries one directory row holds.
pub const DIRECTORY_SLOTS: usize =
    (DIRECTORY_ROW_BYTES - DIRECTORY_ROW_HEADER_BYTES) / DIRECTORY_ENTRY_BYTES;

// Directory entry field offsets. Packed, with no alignment padding.
const DE_TAG: usize = 0;
const DE_FIRST_PAGE: usize = SCRIPT_TAG_BYTES;
const DE_INLINE_EVENTS: usize = DE_FIRST_PAGE + 4;

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
/// The raw script is what placement and the choice table are keyed on. It is
/// not stored: a row carries `tag` instead. Decode leaves `script` empty.
/// `event_count` is builder metadata for placement order and is not stored.
/// Page extent is the 1-based `first_page` field (`0` means the history is
/// exactly the inline slots) plus the fragment count on the first page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub script: Vec<u8>,
    pub tag: [u8; SCRIPT_TAG_BYTES],
    /// Not encoded. The builder uses it to order placement.
    pub event_count: u32,
    /// The newest events, carried here so a short history needs no page query.
    ///
    /// Occupied slots are a zero-free prefix. All-zero event bytes are the
    /// empty sentinel, not a receive.
    pub inline: Vec<TransparentEvent>,
    /// 1-based locator. `0` means no page queries. `r >= 1` means the
    /// contiguous page run starts at PIR row `r - 1`.
    pub first_page: u32,
}

impl DirectoryEntry {
    /// Zero-based PIR row of the first page, when this history has pages.
    pub fn page_base(&self) -> Option<u32> {
        self.first_page.checked_sub(1)
    }
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
        if self.inline.is_empty() {
            return Err(RecordError::Malformed(
                "an occupied directory entry has no events".into(),
            ));
        }
        if self.first_page != 0 && self.inline.len() != INLINE_EVENTS as usize {
            return Err(RecordError::Malformed(
                "a paged history must fill both inline slots".into(),
            ));
        }
        let mut bytes = [0u8; DIRECTORY_ENTRY_BYTES];
        bytes[DE_TAG..DE_FIRST_PAGE].copy_from_slice(&self.tag);
        bytes[DE_FIRST_PAGE..DE_INLINE_EVENTS].copy_from_slice(&self.first_page.to_le_bytes());
        for (index, event) in self.inline.iter().enumerate() {
            let at = DE_INLINE_EVENTS + index * EVENT_BYTES;
            let encoded = event.to_bytes();
            if encoded.iter().all(|byte| *byte == 0) {
                return Err(RecordError::Malformed(
                    "an inline event is the empty sentinel".into(),
                ));
            }
            bytes[at..at + EVENT_BYTES].copy_from_slice(&encoded);
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
        if bytes.iter().all(|byte| *byte == 0) {
            return Ok(None);
        }
        let tag = bytes[DE_TAG..DE_FIRST_PAGE].try_into().expect("14 bytes");
        let first_page = u32::from_le_bytes(
            bytes[DE_FIRST_PAGE..DE_INLINE_EVENTS]
                .try_into()
                .expect("4 bytes"),
        );
        let mut inline = Vec::with_capacity(INLINE_EVENTS as usize);
        for index in 0..INLINE_EVENTS as usize {
            let at = DE_INLINE_EVENTS + index * EVENT_BYTES;
            let slot = &bytes[at..at + EVENT_BYTES];
            if slot.iter().all(|byte| *byte == 0) {
                let rest = &bytes[at + EVENT_BYTES..];
                if rest.iter().any(|byte| *byte != 0) {
                    return Err(RecordError::Malformed(
                        "an empty inline slot is followed by an occupied one".into(),
                    ));
                }
                break;
            }
            inline.push(TransparentEvent::from_bytes(slot)?);
        }
        if inline.is_empty() {
            return Err(RecordError::Malformed(
                "an occupied directory entry has no events".into(),
            ));
        }
        if first_page != 0 && inline.len() != INLINE_EVENTS as usize {
            return Err(RecordError::Malformed(
                "a paged history must fill both inline slots".into(),
            ));
        }
        Ok(Some(Self {
            script: Vec::new(),
            tag,
            event_count: inline.len() as u32,
            inline,
            first_page,
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
    for (index, entry) in entries.iter().enumerate() {
        if entries[..index].iter().any(|other| other.tag == entry.tag) {
            return Err(RecordError::Malformed(
                "a directory row holds a script tag twice".into(),
            ));
        }
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
        assert_eq!(DIRECTORY_ENTRY_BYTES, 192);
        assert_eq!(DIRECTORY_SLOTS, 21);
        assert_eq!(
            DIRECTORY_ROW_HEADER_BYTES + DIRECTORY_SLOTS * DIRECTORY_ENTRY_BYTES,
            4_036
        );
    }

    #[test]
    fn a_directory_entry_round_trips() {
        for e in [entry(1, 2, 0), entry(2, 2, 8), entry(3, 1, 0)] {
            let encoded = e.encode().unwrap();
            decoded_eq(&DirectoryEntry::decode(&encoded).unwrap().unwrap(), &e);
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
            .map(|i| entry(i as u8 + 1, 2, 0))
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
            .map(|i| entry(i as u8 + 1, 2, 0))
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

    /// Inline occupancy is a zero-free prefix, and a page locator requires both
    /// slots. A gap or a short inline history that also names a page is not a
    /// history the wallet can finish.
    #[test]
    fn inline_slots_must_be_a_prefix_and_pages_require_both() {
        let mut encoded = entry(1, 1, 0).encode().unwrap();
        let first = encoded[DE_INLINE_EVENTS..DE_INLINE_EVENTS + EVENT_BYTES].to_vec();
        encoded[DE_INLINE_EVENTS..DE_INLINE_EVENTS + EVENT_BYTES].fill(0);
        encoded[DE_INLINE_EVENTS + EVENT_BYTES..DE_INLINE_EVENTS + 2 * EVENT_BYTES]
            .copy_from_slice(&first);
        assert!(DirectoryEntry::decode(&encoded).is_err());

        assert!(entry(1, 1, 4).encode().is_err());
    }

    #[test]
    fn a_duplicate_tag_in_a_row_is_refused() {
        let row = encode_directory_row(&[entry(1, 1, 0), entry(1, 1, 0)]).unwrap();
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
