//! The packed page row: several scripts' histories sharing one fixed row.
//!
//! A v4 page row belonged to one script. The measured history distribution is
//! p50 two events, p90 five, p95 eight, and the newest two live inline in the
//! directory entry, so the common script that needs a page at all needs a
//! handful of event slots out of thirty-six. A row per script spent the rest on
//! padding nobody chose.
//!
//! Here a row holds a count and then that many variable-length entries, each a
//! 64-byte header followed by its events. Twenty-two one-event histories fit in
//! a row; thirteen two-event ones; ten three-event ones. Past seventeen paged
//! events an entry is more than half a row and packing stops paying, which is
//! why histories longer than one fragment get a contiguous run of rows to
//! themselves and share with nothing.
//!
//! # What packing does not change
//!
//! A directory entry still names `first_page` and `page_count`, and a history
//! still costs `ceil(p / 36)` page queries. What changed is that a reader
//! selects an entry *within* the row rather than taking the row whole. Several
//! scripts naming the same row is expected; a reader must find its own entry and
//! ignore the rest.
//!
//! It must also not *use* the rest. Two of a wallet's own scripts sharing a row
//! still cost two fetches. Skipping the second because the first response
//! happened to carry it would make the request transcript depend on which
//! scripts share a row, which is a fact about other people's history.
//!
//! # What a decoder must not trust
//!
//! These bytes come from a PIR response, so every field is a claim by the
//! server. The entry count is bounded before any offset is computed from it,
//! which is what keeps the arithmetic unreachable by malformed input rather than
//! merely checked; every read goes through [`take`]; and no two entries in a row
//! may name the same script, which is what makes "select the one entry matching
//! my script" a well-defined operation rather than a choice.

use crate::layout::{
    entries_per_row, entry_bytes, EVENTS_PER_PAGE, MAX_ENTRIES_PER_ROW, PAGE_ENTRY_HEADER_BYTES,
    PAGE_ROW_BYTES, PAGE_ROW_HEADER_BYTES,
};
use crate::records::{RecordError, MAX_SCRIPT_BYTES};
use transparent_events::{TransparentEvent, EVENT_BYTES};

// Entry header field offsets, relative to the start of the entry.
const PE_SCRIPT_LEN: usize = 0;
const PE_SCRIPT: usize = 2;
const PE_ORDINAL: usize = PE_SCRIPT + MAX_SCRIPT_BYTES;
const PE_FRAGMENT_COUNT: usize = PE_ORDINAL + 4;
const PE_EVENT_COUNT: usize = PE_FRAGMENT_COUNT + 4;
const PE_MIN_HEIGHT: usize = PE_EVENT_COUNT + 4;
const PE_MAX_HEIGHT: usize = PE_MIN_HEIGHT + 4;
const PE_RESERVED: usize = PE_MAX_HEIGHT + 4;
const PE_END: usize = PE_RESERVED + 2;

/// The header must be exactly the width the layout reserves for it: the entry
/// size formula and every capacity derived from it depend on this being true, so
/// a field added without widening the header should fail the build.
const _: () = assert!(PE_END == PAGE_ENTRY_HEADER_BYTES);
const _: () = assert!(PAGE_ROW_HEADER_BYTES + entry_bytes(EVENTS_PER_PAGE) <= PAGE_ROW_BYTES);

/// One fragment of one script's history, as it sits inside a shared row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageEntry {
    pub script: Vec<u8>,
    pub ordinal: u32,
    /// Fragments this script's history occupies in this shard. Equal to the
    /// directory entry's `page_count`, and checked against it.
    pub fragment_count: u32,
    pub events: Vec<TransparentEvent>,
    /// Bounds over this entry's own events, so a client can navigate to the
    /// first fragment newer than its checkpoint without a plaintext index.
    pub min_height: u32,
    pub max_height: u32,
}

impl PageEntry {
    /// Builds an entry from events, deriving its height bounds from them.
    pub fn new(
        script: Vec<u8>,
        ordinal: u32,
        fragment_count: u32,
        events: Vec<TransparentEvent>,
    ) -> Result<Self, RecordError> {
        if script.len() > MAX_SCRIPT_BYTES {
            return Err(RecordError::ScriptTooLong(script.len()));
        }
        if events.is_empty() {
            return Err(RecordError::Malformed("an entry holds no events".into()));
        }
        if events.len() > EVENTS_PER_PAGE as usize {
            return Err(RecordError::TooManyEvents(events.len()));
        }
        if fragment_count == 0 || ordinal >= fragment_count {
            return Err(RecordError::Malformed(format!(
                "fragment {ordinal} of {fragment_count}"
            )));
        }
        let min_height = events.iter().map(|e| e.height()).min().expect("nonempty");
        let max_height = events.iter().map(|e| e.height()).max().expect("nonempty");
        Ok(Self {
            script,
            ordinal,
            fragment_count,
            events,
            min_height,
            max_height,
        })
    }

    /// Bytes this entry occupies inside a row.
    pub fn encoded_len(&self) -> usize {
        entry_bytes(self.events.len() as u32)
    }
}

/// Reads `len` bytes at `at`, or fails.
///
/// Every read of untrusted row bytes goes through here. Offsets are computed
/// from a count the server chose, so the addition itself has to be checked
/// rather than the result.
fn take(row: &[u8], at: usize, len: usize) -> Result<&[u8], RecordError> {
    let end = at
        .checked_add(len)
        .ok_or_else(|| RecordError::Malformed("entry offset overflows".into()))?;
    row.get(at..end)
        .ok_or_else(|| RecordError::Malformed("entry runs past the row".into()))
}

fn u16_at(row: &[u8], at: usize) -> Result<u16, RecordError> {
    Ok(u16::from_le_bytes(take(row, at, 2)?.try_into().expect("2")))
}

fn u32_at(row: &[u8], at: usize) -> Result<u32, RecordError> {
    Ok(u32::from_le_bytes(take(row, at, 4)?.try_into().expect("4")))
}

/// Encodes entries into one full page row.
///
/// The caller has already decided what shares this row; this refuses only what
/// cannot be represented — more bytes than a row holds, or two entries claiming
/// the same script, which would make selection ambiguous for every reader.
pub fn encode_page_row(entries: &[PageEntry]) -> Result<Vec<u8>, RecordError> {
    if entries.len() > MAX_ENTRIES_PER_ROW {
        return Err(RecordError::TooManyEntries(entries.len()));
    }
    let mut used = PAGE_ROW_HEADER_BYTES;
    for entry in entries {
        if entry.script.len() > MAX_SCRIPT_BYTES {
            return Err(RecordError::ScriptTooLong(entry.script.len()));
        }
        if entry.events.is_empty() || entry.events.len() > EVENTS_PER_PAGE as usize {
            return Err(RecordError::TooManyEvents(entry.events.len()));
        }
        used += entry.encoded_len();
    }
    if used > PAGE_ROW_BYTES {
        return Err(RecordError::Malformed(format!(
            "{} entries need {used} bytes, a row holds {PAGE_ROW_BYTES}",
            entries.len()
        )));
    }
    for (index, entry) in entries.iter().enumerate() {
        if entries[..index]
            .iter()
            .any(|other| other.script == entry.script)
        {
            return Err(RecordError::Malformed(
                "a page row holds a script twice".into(),
            ));
        }
    }

    let mut row = vec![0u8; PAGE_ROW_BYTES];
    row[..PAGE_ROW_HEADER_BYTES].copy_from_slice(&(entries.len() as u32).to_le_bytes());
    let mut at = PAGE_ROW_HEADER_BYTES;
    for entry in entries {
        let head = &mut row[at..at + PAGE_ENTRY_HEADER_BYTES];
        head[PE_SCRIPT_LEN..PE_SCRIPT].copy_from_slice(&(entry.script.len() as u16).to_le_bytes());
        head[PE_SCRIPT..PE_SCRIPT + entry.script.len()].copy_from_slice(&entry.script);
        head[PE_ORDINAL..PE_FRAGMENT_COUNT].copy_from_slice(&entry.ordinal.to_le_bytes());
        head[PE_FRAGMENT_COUNT..PE_EVENT_COUNT]
            .copy_from_slice(&entry.fragment_count.to_le_bytes());
        head[PE_EVENT_COUNT..PE_MIN_HEIGHT]
            .copy_from_slice(&(entry.events.len() as u32).to_le_bytes());
        head[PE_MIN_HEIGHT..PE_MAX_HEIGHT].copy_from_slice(&entry.min_height.to_le_bytes());
        head[PE_MAX_HEIGHT..PE_RESERVED].copy_from_slice(&entry.max_height.to_le_bytes());
        at += PAGE_ENTRY_HEADER_BYTES;
        for event in &entry.events {
            row[at..at + EVENT_BYTES].copy_from_slice(&event.to_bytes());
            at += EVENT_BYTES;
        }
    }
    Ok(row)
}

/// Decodes a page row into its entries, or an empty vector for an unused row.
///
/// An unused row and a full one are the same size and are indistinguishable in
/// a response; only the decoded content differs. That is the point.
pub fn decode_page_row(row: &[u8]) -> Result<Vec<PageEntry>, RecordError> {
    if row.len() != PAGE_ROW_BYTES {
        return Err(RecordError::Length {
            got: row.len(),
            want: PAGE_ROW_BYTES,
        });
    }
    let count = u32::from_le_bytes(row[..PAGE_ROW_HEADER_BYTES].try_into().expect("4")) as usize;
    if count == 0 {
        // An unused row must be entirely zero. A payload hidden behind a zero
        // count would be a channel a server could use and a client never reads.
        if row.iter().any(|byte| *byte != 0) {
            return Err(RecordError::Malformed(
                "an unused page row is not all zero".into(),
            ));
        }
        return Ok(Vec::new());
    }
    // Bounded before any offset is derived from it, so the arithmetic below
    // cannot be driven anywhere by a chosen count.
    if count > MAX_ENTRIES_PER_ROW {
        return Err(RecordError::TooManyEntries(count));
    }

    let mut entries: Vec<PageEntry> = Vec::with_capacity(count);
    let mut at = PAGE_ROW_HEADER_BYTES;
    for _ in 0..count {
        let script_len = u16_at(row, at + PE_SCRIPT_LEN)? as usize;
        if script_len == 0 {
            return Err(RecordError::Malformed("an entry has no script".into()));
        }
        if script_len > MAX_SCRIPT_BYTES {
            return Err(RecordError::ScriptTooLong(script_len));
        }
        // Padding after the script must be zero, or one script would have many
        // encodings and a row's digest would not be stable.
        if take(
            row,
            at + PE_SCRIPT + script_len,
            MAX_SCRIPT_BYTES - script_len,
        )?
        .iter()
        .any(|byte| *byte != 0)
        {
            return Err(RecordError::Malformed("script padding is not zero".into()));
        }
        if take(row, at + PE_RESERVED, 2)?.iter().any(|b| *b != 0) {
            return Err(RecordError::Reserved);
        }

        let ordinal = u32_at(row, at + PE_ORDINAL)?;
        let fragment_count = u32_at(row, at + PE_FRAGMENT_COUNT)?;
        let event_count = u32_at(row, at + PE_EVENT_COUNT)? as usize;
        let min_height = u32_at(row, at + PE_MIN_HEIGHT)?;
        let max_height = u32_at(row, at + PE_MAX_HEIGHT)?;

        if event_count == 0 || event_count > EVENTS_PER_PAGE as usize {
            return Err(RecordError::Malformed(format!(
                "an entry claims {event_count} events"
            )));
        }
        if fragment_count == 0 || ordinal >= fragment_count {
            return Err(RecordError::Malformed(format!(
                "fragment {ordinal} of {fragment_count}"
            )));
        }

        let script = take(row, at + PE_SCRIPT, script_len)?.to_vec();
        let mut events = Vec::with_capacity(event_count);
        let body = at + PAGE_ENTRY_HEADER_BYTES;
        for index in 0..event_count {
            let bytes = take(row, body + index * EVENT_BYTES, EVENT_BYTES)?;
            events.push(TransparentEvent::from_bytes(bytes)?);
        }

        // Ordering is what a replay depends on, and what lets a client stop
        // reading once it is past its checkpoint. A row that arrived shuffled
        // would replay the same events in a different order.
        if events.windows(2).any(|w| w[0].sort_key() > w[1].sort_key()) {
            return Err(RecordError::Malformed(
                "an entry's events are not in canonical order".into(),
            ));
        }
        // The header's bounds are a claim about the entry's own events, and a
        // client navigates by them. Bounds that disagreed could steer it past
        // history it needed.
        let actual_min = events.iter().map(|e| e.height()).min().expect("nonempty");
        let actual_max = events.iter().map(|e| e.height()).max().expect("nonempty");
        if actual_min != min_height || actual_max != max_height {
            return Err(RecordError::Malformed(format!(
                "an entry claims heights {min_height}-{max_height} but holds \
                 {actual_min}-{actual_max}"
            )));
        }
        // Two entries for one script would make "the entry matching my script"
        // a choice rather than a selection, for every reader of this row.
        if entries.iter().any(|other| other.script == script) {
            return Err(RecordError::Malformed(
                "a page row holds a script twice".into(),
            ));
        }

        at = body + event_count * EVENT_BYTES;
        entries.push(PageEntry {
            script,
            ordinal,
            fragment_count,
            events,
            min_height,
            max_height,
        });
    }

    if row[at..].iter().any(|byte| *byte != 0) {
        return Err(RecordError::Malformed(
            "a page row has content past its entries".into(),
        ));
    }
    Ok(entries)
}

/// Entries of a `p`-event history that fit one row, re-exported where the codec
/// that depends on it lives.
pub const fn row_capacity(p: u32) -> u32 {
    entries_per_row(p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_events::{ReceiveEvent, Txid};

    fn script(tag: u8, len: usize) -> Vec<u8> {
        let mut bytes = vec![tag; len];
        bytes[0] = 0x76;
        bytes
    }

    fn event(height: u32, nonce: u32) -> TransparentEvent {
        let mut txid = [0u8; 32];
        txid[..4].copy_from_slice(&nonce.to_le_bytes());
        TransparentEvent::Receive(ReceiveEvent {
            height,
            txid: Txid(txid),
            transaction_index: 0,
            output_index: 0,
            value: 1,
            coinbase: false,
        })
    }

    fn entry(tag: u8, events: u32) -> PageEntry {
        PageEntry::new(
            script(tag, 25),
            0,
            1,
            (0..events).map(|i| event(100 + i, i)).collect(),
        )
        .expect("valid entry")
    }

    #[test]
    fn an_unused_row_decodes_as_empty() {
        assert_eq!(decode_page_row(&vec![0u8; PAGE_ROW_BYTES]).unwrap(), vec![]);
    }

    #[test]
    fn a_row_round_trips_whatever_it_holds() {
        for count in [1usize, 2, 7, MAX_ENTRIES_PER_ROW] {
            let entries: Vec<PageEntry> = (0..count).map(|i| entry(i as u8 + 1, 1)).collect();
            let row = encode_page_row(&entries).expect("encodes");
            assert_eq!(row.len(), PAGE_ROW_BYTES);
            assert_eq!(decode_page_row(&row).unwrap(), entries);
        }
    }

    /// Every class from one paged event to a full fragment, at both a full row
    /// and a partly full one. The capacities are the whole point of the format,
    /// so each is exercised rather than sampled.
    #[test]
    fn every_class_fills_a_row_to_its_capacity() {
        for p in 1..=EVENTS_PER_PAGE {
            let per = entries_per_row(p) as usize;
            assert!(per >= 1, "class {p} must fit at least one entry");
            let full: Vec<PageEntry> = (0..per).map(|i| entry(i as u8 + 1, p)).collect();
            let row = encode_page_row(&full).expect("a full class row encodes");
            assert_eq!(decode_page_row(&row).unwrap(), full);

            // One more must not fit; that is what `entries_per_row` asserts.
            let over: Vec<PageEntry> = (0..per + 1).map(|i| entry(i as u8 + 1, p)).collect();
            assert!(encode_page_row(&over).is_err(), "class {p} overfilled");

            if per > 1 {
                let partial = &full[..per - 1];
                let row = encode_page_row(partial).expect("a partial row encodes");
                assert_eq!(decode_page_row(&row).unwrap(), partial.to_vec());
            }
        }
    }

    #[test]
    fn a_full_fragment_uses_the_bytes_the_layout_promises() {
        let one = entry(1, EVENTS_PER_PAGE);
        assert_eq!(one.encoded_len(), 3_520);
        let row = encode_page_row(std::slice::from_ref(&one)).unwrap();
        assert_eq!(row[3_524..].iter().filter(|b| **b != 0).count(), 0);
        assert_eq!(decode_page_row(&row).unwrap(), vec![one]);
    }

    #[test]
    fn a_row_of_the_wrong_length_is_refused() {
        assert!(matches!(
            decode_page_row(&[0u8; 16]),
            Err(RecordError::Length { .. })
        ));
    }

    #[test]
    fn a_payload_behind_a_zero_count_is_refused() {
        let mut row = vec![0u8; PAGE_ROW_BYTES];
        row[PAGE_ROW_BYTES - 1] = 1;
        assert!(decode_page_row(&row).is_err());
    }

    #[test]
    fn an_impossible_entry_count_is_refused_before_it_is_used() {
        let mut row = vec![0u8; PAGE_ROW_BYTES];
        row[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode_page_row(&row),
            Err(RecordError::TooManyEntries(_))
        ));
    }

    /// A count within the hard bound but past what the declared entries fit.
    /// The row cannot hold them, so the walk must stop rather than read past.
    #[test]
    fn a_count_larger_than_the_row_holds_is_refused() {
        let entries: Vec<PageEntry> = (0..3)
            .map(|i| entry(i as u8 + 1, EVENTS_PER_PAGE))
            .collect();
        let mut row = encode_page_row(&[entry(1, EVENTS_PER_PAGE)]).unwrap();
        row[..4].copy_from_slice(&3u32.to_le_bytes());
        assert!(decode_page_row(&row).is_err());
        let _ = entries;
    }

    #[test]
    fn an_entry_without_a_script_is_refused() {
        let mut row = encode_page_row(&[entry(1, 2)]).unwrap();
        row[4..6].copy_from_slice(&0u16.to_le_bytes());
        assert!(decode_page_row(&row).is_err());
    }

    #[test]
    fn an_over_long_script_is_refused() {
        let mut row = encode_page_row(&[entry(1, 2)]).unwrap();
        row[4..6].copy_from_slice(&((MAX_SCRIPT_BYTES + 1) as u16).to_le_bytes());
        assert!(matches!(
            decode_page_row(&row),
            Err(RecordError::ScriptTooLong(_))
        ));
    }

    #[test]
    fn nonzero_script_padding_is_refused() {
        let mut row = encode_page_row(&[entry(1, 2)]).unwrap();
        row[PAGE_ROW_HEADER_BYTES + PE_SCRIPT + 30] = 1;
        assert!(decode_page_row(&row).is_err());
    }

    #[test]
    fn nonzero_reserved_bytes_are_refused() {
        let mut row = encode_page_row(&[entry(1, 2)]).unwrap();
        row[PAGE_ROW_HEADER_BYTES + PE_RESERVED] = 1;
        assert!(matches!(decode_page_row(&row), Err(RecordError::Reserved)));
    }

    #[test]
    fn an_impossible_event_count_is_refused() {
        for claimed in [0u32, EVENTS_PER_PAGE + 1] {
            let mut row = encode_page_row(&[entry(1, 2)]).unwrap();
            row[PAGE_ROW_HEADER_BYTES + PE_EVENT_COUNT..PAGE_ROW_HEADER_BYTES + PE_EVENT_COUNT + 4]
                .copy_from_slice(&claimed.to_le_bytes());
            assert!(decode_page_row(&row).is_err(), "claimed {claimed}");
        }
    }

    #[test]
    fn an_ordinal_outside_its_fragment_count_is_refused() {
        let mut row = encode_page_row(&[entry(1, 2)]).unwrap();
        row[PAGE_ROW_HEADER_BYTES + PE_ORDINAL..PAGE_ROW_HEADER_BYTES + PE_ORDINAL + 4]
            .copy_from_slice(&5u32.to_le_bytes());
        assert!(decode_page_row(&row).is_err());
        assert!(PageEntry::new(script(1, 25), 3, 3, vec![event(1, 0)]).is_err());
        assert!(PageEntry::new(script(1, 25), 0, 0, vec![event(1, 0)]).is_err());
    }

    #[test]
    fn heights_that_disagree_with_the_events_are_refused() {
        let mut row = encode_page_row(&[entry(1, 3)]).unwrap();
        row[PAGE_ROW_HEADER_BYTES + PE_MAX_HEIGHT..PAGE_ROW_HEADER_BYTES + PE_MAX_HEIGHT + 4]
            .copy_from_slice(&9_999u32.to_le_bytes());
        assert!(decode_page_row(&row).is_err());
    }

    /// Ordering is what a replay depends on, so a shuffled entry must fail
    /// rather than replay the same events in a different order.
    #[test]
    fn events_out_of_canonical_order_are_refused() {
        let mut e = entry(1, 3);
        e.events.reverse();
        let row = encode_page_row(&[e]).unwrap();
        assert!(decode_page_row(&row).is_err());
    }

    #[test]
    fn content_past_the_last_entry_is_refused() {
        let mut row = encode_page_row(&[entry(1, 2)]).unwrap();
        row[PAGE_ROW_BYTES - 1] = 1;
        assert!(decode_page_row(&row).is_err());
    }

    /// Two entries for one script would make "the entry matching my script" a
    /// choice rather than a selection. Refused at both ends, because the client
    /// check that depends on it cannot be the only thing enforcing it.
    #[test]
    fn a_script_may_not_appear_twice_in_a_row() {
        let twice = vec![entry(1, 2), entry(1, 3)];
        assert!(encode_page_row(&twice).is_err());

        // Built by hand, since the encoder refuses to produce it.
        let mut row = encode_page_row(&[entry(1, 2), entry(2, 2)]).unwrap();
        let second = PAGE_ROW_HEADER_BYTES + entry_bytes(2);
        let first_script = row[PAGE_ROW_HEADER_BYTES + PE_SCRIPT
            ..PAGE_ROW_HEADER_BYTES + PE_SCRIPT + MAX_SCRIPT_BYTES]
            .to_vec();
        row[second + PE_SCRIPT..second + PE_SCRIPT + MAX_SCRIPT_BYTES]
            .copy_from_slice(&first_script);
        assert!(decode_page_row(&row).is_err());
    }

    /// Arbitrary bytes must produce an error, never a panic. Seeded so a
    /// failure reproduces exactly, and without a property-testing dependency
    /// the rest of the workspace does not have.
    #[test]
    fn arbitrary_bytes_never_panic() {
        let mut state = 0x243f_6a88_85a3_08d3u64;
        let mut next = move || {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        };
        for _ in 0..2_000 {
            let mut row = vec![0u8; PAGE_ROW_BYTES];
            for chunk in row.chunks_mut(8) {
                let bytes = next().to_le_bytes();
                chunk.copy_from_slice(&bytes[..chunk.len()]);
            }
            // Keep the count in range most of the time, so the walk is actually
            // entered rather than rejected at the first bound.
            row[..4].copy_from_slice(&((next() % 30) as u32).to_le_bytes());
            let _ = decode_page_row(&row);
        }
    }

    /// Frozen encodings. A change to the event width, the script limit or any
    /// offset shifts these, and shifting them without republishing is what
    /// would make two builds disagree about a row while both call it valid.
    #[test]
    fn the_encoding_is_frozen() {
        use sha2::{Digest, Sha256};
        for (label, entries, want) in [
            (
                "empty",
                vec![],
                "6cf1b57d59e7111bc218dfb01dda93ac0f776715599a1c69f89035bd20c16a10",
            ),
            (
                "one class-1 entry",
                vec![entry(1, 1)],
                "7ad0d18b85db2935646d39492fe2b26f7a82cf9c2e4f793f45729b950f224611",
            ),
            (
                "a full class-1 row",
                (0..entries_per_row(1))
                    .map(|i| entry(i as u8 + 1, 1))
                    .collect(),
                "0709cf462f4e9a730c1c48914d4fd9aa4fc87423ed4ee765219bd8b45130940a",
            ),
            (
                "a full class-2 row",
                (0..entries_per_row(2))
                    .map(|i| entry(i as u8 + 1, 2))
                    .collect(),
                "6289f1ef5650c5e4bfb3901d37b4d644496aa64214f5b97936981d0586fc7ed6",
            ),
            (
                "a full fragment",
                vec![entry(1, EVENTS_PER_PAGE)],
                "ce259054ffd48aa3624b380bda53722cd68f9880c90e2678dffbd9137ce784e8",
            ),
        ] {
            let row = encode_page_row(&entries).expect("encodes");
            let digest = hex::encode(Sha256::digest(&row));
            assert_eq!(digest, want, "{label}");
            assert_eq!(decode_page_row(&row).unwrap(), entries, "{label}");
        }
    }
}
