//! Immutable block-hash-addressed display sidecars. The event checkpoint is
//! the authority: sidecars are durable before its block entry can be appended.
//! Uncommitted/reorged sidecars remain unreachable, never coverage evidence.
//!
//! A sidecar holds each transaction's v2x source record: whole-transaction
//! metadata, every output, and every input with the output it spends.
//! Published v2 entries are derived from it when read, so changing what an
//! entry holds is a republish rather than a new ingest. Earlier v1 sidecars
//! are read only for the oversized history events they carry.
use crate::events::EventStoreError;
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    path::Path,
};
use transparent_events::{decode_varint, encode_varint, TransparentEvent, Txid, MAX_EVENT_BYTES};
use transparent_filter::{BlockHash, ScriptBytes};
use transparent_shard::txid::DisplayRecord;
use transparent_shard::txid_v2x::TransparentDisplayRecordV2x;
// Conservative envelope bound over a 2 MB canonical block, including txid and
// length headers for every indexed transaction. Never allocate from disk length.
const MAX_BLOCK_DISPLAY_BYTES: usize = 8_000_076;
pub(crate) const JOURNAL_SCRIPT_LIMIT: usize = 10_000;
type IndexedEvents = Vec<(ScriptBytes, TransparentEvent)>;

/// One sidecar family: its directory, envelope magic and block size bound.
/// v1 is read only for oversized history events; v2x is written and read.
struct Envelope {
    dir: &'static str,
    magic: &'static [u8; 8],
    max_bytes: usize,
}
const V1: Envelope = Envelope {
    dir: "display-v1",
    magic: b"TPIRTX01",
    max_bytes: MAX_BLOCK_DISPLAY_BYTES,
};
/// The source sidecars `event-ingest --txid-display` writes. Prevout scripts
/// are not bounded by the block size, so neither is this envelope; the bound
/// stops the ingest loudly instead of truncating.
pub const V2X_DIR: &str = "display-v2x";
const V2X: Envelope = Envelope {
    dir: V2X_DIR,
    magic: b"TPIRTX2X",
    max_bytes: 256 << 20,
};

pub fn write(
    dir: &Path,
    hash: BlockHash,
    records: &[TransparentDisplayRecordV2x],
) -> Result<(), EventStoreError> {
    write_with_events(dir, hash, records, &[])
}
/// Writes the block's source records, plus the events whose scripts the
/// journal cannot store. Event bytes use the shared event codec unchanged;
/// those scripts stay outside the private history profile but remain in the
/// public filter and the source records.
pub fn write_with_events(
    dir: &Path,
    hash: BlockHash,
    records: &[TransparentDisplayRecordV2x],
    oversized: &[(ScriptBytes, TransparentEvent)],
) -> Result<(), EventStoreError> {
    let payloads = records.iter().map(|r| (r.record.txid, r.encode()));
    write_envelope(&V2X, dir, hash, payloads, oversized)
}
fn write_envelope(
    envelope: &Envelope,
    dir: &Path,
    hash: BlockHash,
    payloads: impl ExactSizeIterator<Item = (Txid, Result<Vec<u8>, transparent_shard::txid::Error>)>,
    oversized: &[(ScriptBytes, TransparentEvent)],
) -> Result<(), EventStoreError> {
    let bound = || EventStoreError::Invariant("display block size bound".into());
    let mut bytes = envelope.magic.to_vec();
    bytes.extend(hash.internal_bytes());
    bytes.extend((payloads.len() as u32).to_le_bytes());
    let mut ids = std::collections::BTreeSet::new();
    for (txid, payload) in payloads {
        if !ids.insert(txid.0) {
            return Err(EventStoreError::Invariant("duplicate display txid".into()));
        }
        let payload = payload.map_err(|e| EventStoreError::Invariant(e.to_string()))?;
        bytes.extend(txid.0);
        bytes.extend((payload.len() as u32).to_le_bytes());
        bytes.extend(payload);
        if bytes.len() > envelope.max_bytes {
            return Err(bound());
        }
    }
    encode_varint(oversized.len() as u64, &mut bytes);
    for (script, event) in oversized {
        if !(JOURNAL_SCRIPT_LIMIT + 1..=2_000_000).contains(&script.as_slice().len()) {
            return Err(EventStoreError::Invariant("sidecar script extent".into()));
        }
        event.validate_metadata()?;
        encode_varint(script.as_slice().len() as u64, &mut bytes);
        bytes.extend(script.as_slice());
        let event = event.to_bytes();
        encode_varint(event.len() as u64, &mut bytes);
        bytes.extend(event);
        if bytes.len() > envelope.max_bytes {
            return Err(bound());
        }
    }
    bytes.extend(Sha256::digest(&bytes));
    if bytes.len() > envelope.max_bytes {
        return Err(bound());
    }
    let dir = dir.join(envelope.dir);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{}.bin", hash.to_display_hex()));
    if path.exists() {
        if bounded_read(&path, envelope.max_bytes)? != bytes {
            return Err(EventStoreError::Invariant(
                "display sidecar contradiction".into(),
            ));
        }
        return Ok(());
    }
    let tmp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(tmp, path)?;
    std::fs::File::open(&dir)?.sync_all()?;
    std::fs::File::open(dir.parent().unwrap())?.sync_all()?;
    Ok(())
}
/// The block's published entries, derived from its source records.
pub fn read(dir: &Path, hash: BlockHash) -> Result<Vec<DisplayRecord>, EventStoreError> {
    read_sources(dir, hash)?
        .iter()
        .map(|source| {
            source
                .display_record()
                .map_err(|e| EventStoreError::Invariant(e.to_string()))
        })
        .collect()
}
/// The block's source records.
pub fn read_sources(
    dir: &Path,
    hash: BlockHash,
) -> Result<Vec<TransparentDisplayRecordV2x>, EventStoreError> {
    Ok(read_envelope(&V2X, dir, hash, TransparentDisplayRecordV2x::decode)?.0)
}
fn read_envelope<R>(
    envelope: &Envelope,
    dir: &Path,
    hash: BlockHash,
    decode: impl Fn(Txid, &[u8]) -> Result<R, transparent_shard::txid::Error>,
) -> Result<(Vec<R>, IndexedEvents), EventStoreError> {
    let bytes = bounded_read(
        &dir.join(envelope.dir)
            .join(format!("{}.bin", hash.to_display_hex())),
        envelope.max_bytes,
    )?;
    let fail = || EventStoreError::Invariant("invalid display sidecar".into());
    if bytes.len() < 76 {
        return Err(fail());
    }
    let (bytes, digest) = bytes.split_at(bytes.len() - 32);
    if Sha256::digest(bytes).as_slice() != digest {
        return Err(fail());
    }
    if bytes.len() < 44
        || bytes[..8] != envelope.magic[..]
        || bytes[8..40] != hash.internal_bytes()[..]
    {
        return Err(fail());
    }
    let count = u32::from_le_bytes(bytes[40..44].try_into().unwrap()) as usize;
    if count > bytes.len() / 39 {
        return Err(fail());
    }
    let mut at = 44;
    let mut records = Vec::new();
    let mut ids = std::collections::BTreeSet::new();
    for _ in 0..count {
        let header = bytes.get(at..at + 36).ok_or_else(fail)?;
        let txid = Txid(header[..32].try_into().unwrap());
        let n = u32::from_le_bytes(header[32..].try_into().unwrap()) as usize;
        at += 36;
        let end = at.checked_add(n).ok_or_else(fail)?;
        let payload = bytes.get(at..end).ok_or_else(fail)?;
        if !ids.insert(txid.0) {
            return Err(fail());
        }
        records.push(decode(txid, payload).map_err(|e| EventStoreError::Invariant(e.to_string()))?);
        at = end;
    }
    let (count, n) = decode_varint(
        bytes.get(at..).ok_or_else(fail)?,
        envelope.max_bytes as u64 / JOURNAL_SCRIPT_LIMIT as u64,
    )?;
    at += n;
    let mut oversized = Vec::new();
    for _ in 0..count {
        let (length, n) = decode_varint(bytes.get(at..).ok_or_else(fail)?, 2_000_000)?;
        at += n;
        if length as usize <= JOURNAL_SCRIPT_LIMIT {
            return Err(fail());
        }
        let script = bytes
            .get(at..at + length as usize)
            .ok_or_else(fail)?
            .to_vec();
        at += length as usize;
        let (length, n) = decode_varint(bytes.get(at..).ok_or_else(fail)?, MAX_EVENT_BYTES as u64)?;
        at += n;
        let event =
            TransparentEvent::from_bytes(bytes.get(at..at + length as usize).ok_or_else(fail)?)?;
        at += length as usize;
        oversized.push((ScriptBytes::new(script), event));
    }
    if at != bytes.len() {
        return Err(fail());
    }
    Ok((records, oversized))
}
/// Events whose scripts exceed the journal's ceiling, kept in the block's
/// sidecar. A block without a sidecar has none, so a journal copy must carry
/// every committed block's sidecar. Existing history journals keep them in v1
/// sidecars, whose display records are skipped unread.
pub fn oversized_events(dir: &Path, hash: BlockHash) -> Result<IndexedEvents, EventStoreError> {
    let name = format!("{}.bin", hash.to_display_hex());
    if dir.join(V2X.dir).join(&name).exists() {
        return Ok(read_envelope(&V2X, dir, hash, |_, _| Ok(()))?.1);
    }
    if dir.join(V1.dir).join(&name).exists() {
        return Ok(read_envelope(&V1, dir, hash, |_, _| Ok(()))?.1);
    }
    Ok(Vec::new())
}
/// Each source record's metadata and outputs against the receive events,
/// plus: every spend event names the
/// listed input at its index, with the same outpoint and script, and every
/// listed input with an indexable script has exactly one spend event.
pub fn validate_events(
    records: &[TransparentDisplayRecordV2x],
    events: &[(ScriptBytes, TransparentEvent)],
) -> Result<(), EventStoreError> {
    validate_record_events(records.iter().map(|r| &r.record), events)?;
    let contradiction = || EventStoreError::Invariant("display/spend contradiction".into());
    let by_id: std::collections::BTreeMap<_, _> =
        records.iter().map(|r| (r.record.txid.0, r)).collect();
    let mut spent = std::collections::BTreeSet::new();
    for (script, event) in events {
        let TransparentEvent::Spend(spend) = event else {
            continue;
        };
        let record = by_id
            .get(&spend.spending_txid.0)
            .ok_or_else(|| EventStoreError::Invariant("event missing display record".into()))?;
        let input = record
            .inputs
            .get(spend.input_index as usize)
            .ok_or_else(contradiction)?;
        if input.prevout_txid != spend.spent_txid
            || input.prevout_index != spend.spent_output_index
            || input.script != script.as_slice()
            || !spent.insert((spend.spending_txid.0, spend.input_index))
        {
            return Err(contradiction());
        }
    }
    let indexable = records
        .iter()
        .flat_map(|r| &r.inputs)
        .filter(|i| ScriptBytes::new(i.script.clone()).is_filter_element())
        .count();
    if indexable != spent.len() {
        return Err(contradiction());
    }
    Ok(())
}
/// The history events a block's source records imply: a receive for every
/// output and a spend for every input whose script the filter indexes, in
/// transaction order, as extraction writes them. Synthetic and scripted
/// journals derive their events from their sources with this.
pub fn implied_events(
    height: u64,
    sources: &[TransparentDisplayRecordV2x],
) -> Result<IndexedEvents, EventStoreError> {
    use transparent_events::{ReceiveEvent, SpendEvent};
    let bound =
        |what: &str| EventStoreError::Invariant(format!("{what} exceeds the event encoding"));
    let height = u32::try_from(height).map_err(|_| bound("height"))?;
    let mut events = Vec::new();
    for (index, source) in sources.iter().enumerate() {
        let transaction_index = u16::try_from(index).map_err(|_| bound("transaction index"))?;
        let record = &source.record;
        let metadata = Some(record.metadata);
        for (output_index, output) in record.outputs.iter().enumerate() {
            let script = ScriptBytes::new(output.script.clone());
            if script.is_filter_element() {
                events.push((
                    script,
                    TransparentEvent::Receive(ReceiveEvent {
                        metadata,
                        height,
                        txid: record.txid,
                        transaction_index,
                        output_index: output_index as u32,
                        value: output.value,
                        coinbase: record.coinbase,
                    }),
                ));
            }
        }
        for (input_index, input) in source.inputs.iter().enumerate() {
            let script = ScriptBytes::new(input.script.clone());
            if script.is_filter_element() {
                events.push((
                    script,
                    TransparentEvent::Spend(SpendEvent {
                        metadata,
                        height,
                        spending_txid: record.txid,
                        transaction_index,
                        input_index: input_index as u32,
                        spent_txid: input.prevout_txid,
                        spent_output_index: input.prevout_index,
                    }),
                ));
            }
        }
    }
    Ok(events)
}
fn validate_record_events<'a>(
    records: impl Iterator<Item = &'a transparent_shard::txid_v1::TransparentDisplayRecord>,
    events: &[(ScriptBytes, TransparentEvent)],
) -> Result<(), EventStoreError> {
    let by_id: std::collections::BTreeMap<_, _> = records.map(|r| (r.txid.0, r)).collect();
    for (script, event) in events {
        let record = by_id
            .get(&event.txid().0)
            .ok_or_else(|| EventStoreError::Invariant("event missing display record".into()))?;
        if event.metadata() != Some(record.metadata) {
            return Err(EventStoreError::Invariant(
                "display/event metadata contradiction".into(),
            ));
        }
        if let TransparentEvent::Receive(receive) = event {
            let output = record
                .outputs
                .get(receive.output_index as usize)
                .ok_or_else(|| EventStoreError::Invariant("display output missing".into()))?;
            if output.value != receive.value
                || output.script != script.as_slice()
                || receive.coinbase != record.coinbase
            {
                return Err(EventStoreError::Invariant(
                    "display/receive contradiction".into(),
                ));
            }
        }
    }
    Ok(())
}
fn bounded_read(path: &Path, max_bytes: usize) -> Result<Vec<u8>, EventStoreError> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(EventStoreError::Invariant(
            "display block size bound".into(),
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventStore;
    use transparent_events::{
        FeeState, ReceiveEvent, SpendEvent, TransactionMetadata, TransparentEvent,
    };
    use transparent_filter::ScriptBytes;
    use transparent_shard::txid::DisplayOutput;
    use transparent_shard::txid_v1::TransparentDisplayRecord;
    use transparent_shard::txid_v2x::DisplayInput;
    fn source(output_script: Vec<u8>) -> TransparentDisplayRecordV2x {
        TransparentDisplayRecordV2x {
            record: TransparentDisplayRecord {
                txid: Txid([1; 32]),
                coinbase: false,
                metadata: TransactionMetadata {
                    fee: FeeState::Exact(3),
                    transparent_input_count: 1,
                    has_shielded_components: false,
                },
                outputs: vec![DisplayOutput {
                    value: 7,
                    script: output_script,
                }],
            },
            inputs: vec![DisplayInput {
                prevout_txid: Txid([8; 32]),
                prevout_index: 4,
                value: 10,
                script: vec![0x53],
            }],
        }
    }
    fn events(r: &TransparentDisplayRecordV2x) -> IndexedEvents {
        let metadata = Some(r.record.metadata);
        vec![
            (
                ScriptBytes::new(r.record.outputs[0].script.clone()),
                TransparentEvent::Receive(ReceiveEvent {
                    height: 1,
                    txid: r.record.txid,
                    transaction_index: 0,
                    output_index: 0,
                    value: 7,
                    coinbase: false,
                    metadata,
                }),
            ),
            (
                ScriptBytes::new(vec![0x53]),
                TransparentEvent::Spend(SpendEvent {
                    height: 1,
                    spending_txid: r.record.txid,
                    transaction_index: 0,
                    input_index: 0,
                    spent_txid: Txid([8; 32]),
                    spent_output_index: 4,
                    metadata,
                }),
            ),
        ]
    }
    #[test]
    fn immutable_identity_reload_and_malformed() {
        let temp = tempfile::tempdir().unwrap();
        let hash = BlockHash::from_internal_bytes([2; 32]);
        let r = source(vec![0x51]);
        write(temp.path(), hash, std::slice::from_ref(&r)).unwrap();
        assert_eq!(read_sources(temp.path(), hash).unwrap(), vec![r.clone()]);
        assert_eq!(
            read(temp.path(), hash).unwrap(),
            vec![r.display_record().unwrap()]
        );
        let mut altered = r.clone();
        altered.record.outputs[0].value -= 1;
        altered.record.metadata.fee = FeeState::Exact(4);
        assert!(write(temp.path(), hash, &[altered]).is_err());
        assert!(write(
            temp.path(),
            BlockHash::from_internal_bytes([3; 32]),
            &[r.clone(), r]
        )
        .is_err());
        let path = temp
            .path()
            .join(V2X_DIR)
            .join(format!("{}.bin", hash.to_display_hex()));
        let mut b = std::fs::read(&path).unwrap();
        b.push(0);
        std::fs::write(&path, b).unwrap();
        assert!(read(temp.path(), hash).is_err());
    }
    #[test]
    fn implied_events_are_the_extracted_events() {
        let r = source(vec![0x51]);
        assert_eq!(
            implied_events(1, std::slice::from_ref(&r)).unwrap(),
            events(&r)
        );
    }
    #[test]
    fn spends_must_match_the_listed_inputs() {
        let r = source(vec![0x51]);
        let all = events(&r);
        validate_events(std::slice::from_ref(&r), &all).unwrap();
        // A missing, duplicated or moved spend contradicts the input list.
        let missing = vec![all[0].clone()];
        let duplicated = vec![all[0].clone(), all[1].clone(), all[1].clone()];
        let mut moved = all.clone();
        if let TransparentEvent::Spend(s) = &mut moved[1].1 {
            s.spent_output_index = 5;
        }
        for bad in [missing, duplicated, moved] {
            assert!(validate_events(std::slice::from_ref(&r), &bad).is_err());
        }
    }
    #[test]
    fn checkpoint_interruption_contradiction_and_reorg() {
        let temp = tempfile::tempdir().unwrap();
        let r = source(vec![0x51]);
        let hash = BlockHash::from_internal_bytes([2; 32]);
        let mut store =
            EventStore::open(temp.path(), transparent_filter::MAINNET_GENESIS_DISPLAY, 1).unwrap();
        let events = events(&r);
        let mut altered = r.clone();
        altered.record.outputs[0].value -= 1;
        altered.record.metadata.fee = FeeState::Exact(4);
        assert!(store
            .append_block_with_display(1, hash, &events, &[altered])
            .is_err());
        store
            .append_block_with_display(1, hash, &events, std::slice::from_ref(&r))
            .unwrap();
        drop(store); // Durable sidecar alone never advances committed coverage.
        let mut store =
            EventStore::open(temp.path(), transparent_filter::MAINNET_GENESIS_DISPLAY, 1).unwrap();
        assert!(store.display_at(1).is_err());
        store
            .append_block_with_display(1, hash, &events, std::slice::from_ref(&r))
            .unwrap();
        store.commit().unwrap();
        drop(store);
        let mut store =
            EventStore::open(temp.path(), transparent_filter::MAINNET_GENESIS_DISPLAY, 1).unwrap();
        assert_eq!(
            store.display_at(1).unwrap(),
            vec![r.display_record().unwrap()]
        );
        store.rollback_to(None).unwrap();
        assert!(store.display_at(1).is_err());
        let replacement = BlockHash::from_internal_bytes([4; 32]);
        store
            .append_block_with_display(1, replacement, &events, std::slice::from_ref(&r))
            .unwrap();
        store.commit().unwrap();
        assert!(store.display_at(1).is_ok());
        std::fs::remove_file(
            temp.path()
                .join(V2X_DIR)
                .join(format!("{}.bin", replacement.to_display_hex())),
        )
        .unwrap();
        assert!(store.display_at(1).is_err()); // Missing sidecars cannot manufacture coverage.
    }
    /// History journals written before v2x keep oversized-script events in
    /// `display-v1` sidecars; they must still reach `events_at`, and their
    /// display records are never read.
    #[test]
    fn oversized_events_are_read_from_v1_sidecars() {
        let temp = tempfile::tempdir().unwrap();
        let hash = BlockHash::from_internal_bytes([6; 32]);
        let script = vec![0x51; 20_000];
        let event = TransparentEvent::Receive(ReceiveEvent {
            height: 1,
            txid: Txid([1; 32]),
            transaction_index: 0,
            output_index: 0,
            value: 7,
            coinbase: false,
            metadata: None,
        });
        // A v1 sidecar: one opaque record payload, then the oversized event.
        let mut bytes = V1.magic.to_vec();
        bytes.extend(hash.internal_bytes());
        bytes.extend(1u32.to_le_bytes());
        bytes.extend([1u8; 32]);
        bytes.extend(5u32.to_le_bytes());
        bytes.extend([9u8; 5]);
        encode_varint(1, &mut bytes);
        encode_varint(script.len() as u64, &mut bytes);
        bytes.extend(&script);
        let raw = event.to_bytes();
        encode_varint(raw.len() as u64, &mut bytes);
        bytes.extend(raw);
        bytes.extend(Sha256::digest(&bytes));
        let dir = temp.path().join(V1.dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.bin", hash.to_display_hex()));
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            oversized_events(temp.path(), hash).unwrap(),
            vec![(ScriptBytes::new(script), event)]
        );
        assert!(read(temp.path(), hash).is_err(), "no source sidecar");
        // A corrupted v1 sidecar is refused, not treated as empty.
        bytes[50] ^= 1;
        std::fs::write(&path, &bytes).unwrap();
        assert!(oversized_events(temp.path(), hash).is_err());
        let other = BlockHash::from_internal_bytes([7; 32]);
        assert!(oversized_events(temp.path(), other).unwrap().is_empty());
    }
    #[test]
    fn large_raw_script_remains_in_display_and_filter_projection() {
        let temp = tempfile::tempdir().unwrap();
        let r = source(vec![0x51; 65_536]);
        let events = events(&r);
        let mut store =
            EventStore::open(temp.path(), transparent_filter::MAINNET_GENESIS_DISPLAY, 1).unwrap();
        store
            .append_block_with_display(
                1,
                BlockHash::from_internal_bytes([2; 32]),
                &events,
                std::slice::from_ref(&r),
            )
            .unwrap();
        store.commit().unwrap();
        drop(store);
        let store = EventStore::open_existing(temp.path()).unwrap();
        assert_eq!(
            store.display_at(1).unwrap(),
            vec![r.display_record().unwrap()]
        );
        let mut read_back = store.events_at(1).unwrap().unwrap();
        let mut expected = events.clone();
        read_back.sort_by_key(|(s, _)| s.as_slice().len());
        expected.sort_by_key(|(s, _)| s.as_slice().len());
        assert_eq!(read_back, expected);
    }
}
