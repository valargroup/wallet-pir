//! Immutable block-hash-addressed display sidecars. The event checkpoint is
//! the authority: sidecars are durable before its block entry can be appended.
//! Uncommitted/reorged sidecars remain unreachable, never coverage evidence.
use crate::events::EventStoreError;
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    path::Path,
};
use transparent_events::{decode_varint, encode_varint, TransparentEvent, Txid, MAX_EVENT_BYTES};
use transparent_filter::{BlockHash, ScriptBytes};
use transparent_shard::txid::TransparentDisplayRecord;
use transparent_shard::txid_v2x::TransparentDisplayRecordV2x;
// Conservative envelope bound over a 2 MB canonical block, including txid and
// length headers for every indexed transaction. Never allocate from disk length.
const MAX_BLOCK_DISPLAY_BYTES: usize = 8_000_076;
pub(crate) const JOURNAL_SCRIPT_LIMIT: usize = 10_000;
type IndexedEvents = Vec<(ScriptBytes, TransparentEvent)>;

/// One sidecar family: its directory, envelope magic and block size bound.
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
/// EXPERIMENTAL, census-only sidecars written by `--txid-display-inputs`.
/// Prevout scripts are not bounded by the block size, so neither is this
/// envelope; the bound is a measurement ceiling that stops the ingest loudly.
pub const V2X_DIR: &str = "display-v2x";
const V2X: Envelope = Envelope {
    dir: V2X_DIR,
    magic: b"TPIRTX2X",
    max_bytes: 256 << 20,
};

pub fn write(
    dir: &Path,
    hash: BlockHash,
    records: &[TransparentDisplayRecord],
) -> Result<(), EventStoreError> {
    write_with_events(dir, hash, records, &[])
}
/// Sidecar-only envelope for scripts that the existing journal cannot store.
/// Event bytes still use the metadata dependency's codec, unchanged. These
/// scripts remain outside the supported private history profile but must remain
/// in the public filter and the complete display record.
pub fn write_with_events(
    dir: &Path,
    hash: BlockHash,
    records: &[TransparentDisplayRecord],
    oversized: &[(ScriptBytes, TransparentEvent)],
) -> Result<(), EventStoreError> {
    let payloads = records.iter().map(|r| (r.txid, r.encode()));
    write_envelope(&V1, dir, hash, payloads, oversized)
}
/// The experimental v2x sidecar: the same envelope, durability and
/// contradiction rules as v1, in its own directory with its own magic.
pub fn write_inputs_with_events(
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
pub fn read(dir: &Path, hash: BlockHash) -> Result<Vec<TransparentDisplayRecord>, EventStoreError> {
    Ok(read_block(dir, hash)?.0)
}
fn read_block(
    dir: &Path,
    hash: BlockHash,
) -> Result<(Vec<TransparentDisplayRecord>, IndexedEvents), EventStoreError> {
    read_envelope(&V1, dir, hash, TransparentDisplayRecord::decode)
}
/// Reads an experimental v2x sidecar. Only offline measurement calls this;
/// no server, controller or client path does.
pub fn read_inputs(
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
pub fn oversized_events(dir: &Path, hash: BlockHash) -> Result<IndexedEvents, EventStoreError> {
    let name = format!("{}.bin", hash.to_display_hex());
    let path = dir.join(V1.dir).join(&name);
    if !path.exists() {
        // Oversized events of an experimental journal live only in its v2x
        // sidecars, which no serving path reads. Refuse rather than return
        // a block that silently lacks them.
        if dir.join(V2X.dir).join(&name).exists() {
            return Err(EventStoreError::Invariant(
                "block was ingested with experimental --txid-display-inputs; its journal is census-only".into(),
            ));
        }
        return Ok(Vec::new());
    }
    Ok(read_block(dir, hash)?.1)
}
pub fn validate_events(
    records: &[TransparentDisplayRecord],
    events: &[(ScriptBytes, TransparentEvent)],
) -> Result<(), EventStoreError> {
    validate_record_events(records.iter(), events)
}
/// The v1 checks on the stripped records, plus: every spend event names the
/// listed input at its index, with the same outpoint and script, and every
/// listed input with an indexable script has exactly one spend event.
pub fn validate_input_events(
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
fn validate_record_events<'a>(
    records: impl Iterator<Item = &'a TransparentDisplayRecord>,
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
    use transparent_events::{FeeState, ReceiveEvent, TransactionMetadata, TransparentEvent};
    use transparent_filter::ScriptBytes;
    use transparent_shard::txid::DisplayOutput;
    fn record() -> TransparentDisplayRecord {
        TransparentDisplayRecord {
            txid: Txid([1; 32]),
            coinbase: false,
            metadata: TransactionMetadata {
                fee: FeeState::Unknown,
                transparent_input_count: 1,
                has_shielded_components: false,
            },
            outputs: vec![DisplayOutput {
                value: 7,
                script: vec![0x51],
            }],
        }
    }
    #[test]
    fn immutable_identity_reload_and_malformed() {
        let temp = tempfile::tempdir().unwrap();
        let hash = BlockHash::from_internal_bytes([2; 32]);
        let r = record();
        write(temp.path(), hash, std::slice::from_ref(&r)).unwrap();
        assert_eq!(read(temp.path(), hash).unwrap(), vec![r.clone()]);
        let mut altered = r.clone();
        altered.outputs[0].value += 1;
        assert!(write(temp.path(), hash, &[altered]).is_err());
        assert!(write(
            temp.path(),
            BlockHash::from_internal_bytes([3; 32]),
            &[r.clone(), r]
        )
        .is_err());
        let path = temp
            .path()
            .join("display-v1")
            .join(format!("{}.bin", hash.to_display_hex()));
        let mut b = std::fs::read(&path).unwrap();
        b.push(0);
        std::fs::write(&path, b).unwrap();
        assert!(read(temp.path(), hash).is_err());
    }
    #[test]
    fn checkpoint_interruption_contradiction_and_reorg() {
        let temp = tempfile::tempdir().unwrap();
        let r = record();
        let hash = BlockHash::from_internal_bytes([2; 32]);
        let mut store =
            EventStore::open(temp.path(), transparent_filter::MAINNET_GENESIS_DISPLAY, 1).unwrap();
        let events = vec![(
            ScriptBytes::new(vec![0x51]),
            TransparentEvent::Receive(ReceiveEvent {
                height: 1,
                txid: r.txid,
                transaction_index: 0,
                output_index: 0,
                value: 7,
                coinbase: false,
                metadata: Some(r.metadata),
            }),
        )];
        let mut altered = r.clone();
        altered.outputs[0].value += 1;
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
        assert_eq!(store.display_at(1).unwrap(), vec![r.clone()]);
        store.rollback_to(None).unwrap();
        assert!(store.display_at(1).is_err());
        let replacement = BlockHash::from_internal_bytes([4; 32]);
        store
            .append_block_with_display(1, replacement, &events, &[r])
            .unwrap();
        store.commit().unwrap();
        assert!(store.display_at(1).is_ok());
        std::fs::remove_file(
            temp.path()
                .join("display-v1")
                .join(format!("{}.bin", replacement.to_display_hex())),
        )
        .unwrap();
        assert!(store.display_at(1).is_err()); // Old journals/missing sidecars cannot manufacture coverage.
    }
    /// Pins the published v1 sidecar bytes. The digest was taken from the
    /// writer before the experimental v2x envelope shared its code.
    #[test]
    fn v1_sidecar_bytes_are_pinned() {
        let temp = tempfile::tempdir().unwrap();
        let hash = BlockHash::from_internal_bytes([9; 32]);
        let coinbase = TransparentDisplayRecord {
            txid: Txid([3; 32]),
            coinbase: true,
            metadata: TransactionMetadata {
                fee: FeeState::NotApplicable,
                transparent_input_count: 0,
                has_shielded_components: false,
            },
            outputs: vec![DisplayOutput {
                value: 625_000_000,
                script: vec![0x51; 25],
            }],
        };
        let mut spend = record();
        spend.metadata.fee = FeeState::Exact(1_000);
        spend.metadata.has_shielded_components = true;
        spend.outputs.push(DisplayOutput {
            value: 0,
            script: vec![0x52; JOURNAL_SCRIPT_LIMIT + 1],
        });
        let event = TransparentEvent::Receive(ReceiveEvent {
            height: 1,
            txid: spend.txid,
            transaction_index: 1,
            output_index: 1,
            value: 0,
            coinbase: false,
            metadata: Some(spend.metadata),
        });
        write_with_events(
            temp.path(),
            hash,
            &[coinbase, spend.clone()],
            &[(ScriptBytes::new(spend.outputs[1].script.clone()), event)],
        )
        .unwrap();
        let bytes = std::fs::read(
            temp.path()
                .join("display-v1")
                .join(format!("{}.bin", hash.to_display_hex())),
        )
        .unwrap();
        assert_eq!(
            hex::encode(Sha256::digest(&bytes)),
            "90dd7bb4d44d5e661ce7fa61cdfe08e1f5e9163cc640a7b9262325f6386ddab6"
        );
    }
    #[test]
    fn v2x_sidecar_is_separate_immutable_and_cross_checked() {
        use transparent_events::SpendEvent;
        use transparent_shard::txid_v2x::DisplayInput;
        let temp = tempfile::tempdir().unwrap();
        let hash = BlockHash::from_internal_bytes([2; 32]);
        let mut r = TransparentDisplayRecordV2x {
            record: record(),
            inputs: vec![DisplayInput {
                prevout_txid: Txid([8; 32]),
                prevout_index: 4,
                value: 9,
                script: vec![0x53; 20_000],
            }],
        };
        r.record.metadata.fee = FeeState::Exact(2);
        let spend = |input_index| {
            (
                ScriptBytes::new(vec![0x53; 20_000]),
                TransparentEvent::Spend(SpendEvent {
                    height: 1,
                    spending_txid: r.record.txid,
                    transaction_index: 0,
                    input_index,
                    spent_txid: Txid([8; 32]),
                    spent_output_index: 4,
                    metadata: Some(r.record.metadata),
                }),
            )
        };
        validate_input_events(std::slice::from_ref(&r), &[spend(0)]).unwrap();
        // A missing, misplaced or duplicated spend contradicts the input list.
        for events in [vec![], vec![spend(0), spend(0)]] {
            assert!(validate_input_events(std::slice::from_ref(&r), &events).is_err());
        }
        let mut moved = spend(0);
        if let TransparentEvent::Spend(s) = &mut moved.1 {
            s.spent_output_index = 5;
        }
        assert!(validate_input_events(std::slice::from_ref(&r), &[moved]).is_err());

        write_inputs_with_events(temp.path(), hash, std::slice::from_ref(&r), &[spend(0)]).unwrap();
        assert_eq!(read_inputs(temp.path(), hash).unwrap(), vec![r.clone()]);
        assert!(
            read(temp.path(), hash).is_err(),
            "no v1 sidecar was written"
        );
        assert!(oversized_events(temp.path(), hash).is_err());
        let mut altered = r.clone();
        altered.inputs[0].script.pop();
        assert!(write_inputs_with_events(temp.path(), hash, &[altered], &[]).is_err());
        // The v1 reader refuses v2x bytes even under the v1 name.
        std::fs::create_dir_all(temp.path().join("display-v1")).unwrap();
        let name = format!("{}.bin", hash.to_display_hex());
        std::fs::copy(
            temp.path().join(V2X_DIR).join(&name),
            temp.path().join("display-v1").join(&name),
        )
        .unwrap();
        assert!(read(temp.path(), hash).is_err());
        let path = temp.path().join(V2X_DIR).join(&name);
        let mut b = std::fs::read(&path).unwrap();
        b[60] ^= 1;
        std::fs::write(&path, b).unwrap();
        assert!(read_inputs(temp.path(), hash).is_err());
    }
    #[test]
    fn large_raw_script_remains_in_display_and_filter_projection() {
        let temp = tempfile::tempdir().unwrap();
        let mut r = record();
        r.outputs[0].script = vec![0x51; 65_536];
        let event = TransparentEvent::Receive(ReceiveEvent {
            height: 1,
            txid: r.txid,
            transaction_index: 0,
            output_index: 0,
            value: 7,
            coinbase: false,
            metadata: Some(r.metadata),
        });
        let events = vec![(ScriptBytes::new(r.outputs[0].script.clone()), event)];
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
        assert_eq!(store.display_at(1).unwrap(), vec![r]);
        assert_eq!(store.events_at(1).unwrap(), Some(events));
        // No change to existing event/journal encoding was needed.
        assert_eq!(
            std::fs::metadata(temp.path().join("events.bin"))
                .unwrap()
                .len(),
            0
        );
    }
}
