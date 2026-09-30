//! Immutable block-hash-addressed display sidecars. The event checkpoint is
//! the authority: sidecars are durable before its block entry can be appended.
//! Uncommitted/reorged sidecars remain unreachable, never coverage evidence.
use crate::events::EventStoreError;
use std::{
    io::{Read, Write},
    path::Path,
};
use transparent_events::{decode_varint, encode_varint, TransparentEvent, Txid, MAX_EVENT_BYTES};
use transparent_filter::{BlockHash, ScriptBytes};
use transparent_shard::txid::TransparentDisplayRecord;
// Conservative envelope bound over a 2 MB canonical block, including txid and
// length headers for every indexed transaction. Never allocate from disk length.
const MAX_BLOCK_DISPLAY_BYTES: usize = 8_000_044;
pub(crate) const JOURNAL_SCRIPT_LIMIT: usize = 10_000;
type IndexedEvents = Vec<(ScriptBytes, TransparentEvent)>;

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
    let mut bytes = b"TPIRTX01".to_vec();
    bytes.extend(hash.internal_bytes());
    bytes.extend((records.len() as u32).to_le_bytes());
    let mut ids = std::collections::BTreeSet::new();
    for record in records {
        if !ids.insert(record.txid.0) {
            return Err(EventStoreError::Invariant("duplicate display txid".into()));
        }
        let payload = record
            .encode()
            .map_err(|e| EventStoreError::Invariant(e.to_string()))?;
        bytes.extend(record.txid.0);
        bytes.extend((payload.len() as u32).to_le_bytes());
        bytes.extend(payload);
        if bytes.len() > MAX_BLOCK_DISPLAY_BYTES {
            return Err(EventStoreError::Invariant(
                "display block size bound".into(),
            ));
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
        if bytes.len() > MAX_BLOCK_DISPLAY_BYTES {
            return Err(EventStoreError::Invariant(
                "display block size bound".into(),
            ));
        }
    }
    let dir = dir.join("display-v1");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{}.bin", hash.to_display_hex()));
    if path.exists() {
        if bounded_read(&path)? != bytes {
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
    let bytes = bounded_read(
        &dir.join("display-v1")
            .join(format!("{}.bin", hash.to_display_hex())),
    )?;
    let fail = || EventStoreError::Invariant("invalid display sidecar".into());
    if bytes.len() < 44 || &bytes[..8] != b"TPIRTX01" || bytes[8..40] != hash.internal_bytes()[..] {
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
        records.push(
            TransparentDisplayRecord::decode(txid, payload)
                .map_err(|e| EventStoreError::Invariant(e.to_string()))?,
        );
        at = end;
    }
    let (count, n) = decode_varint(
        bytes.get(at..).ok_or_else(fail)?,
        MAX_BLOCK_DISPLAY_BYTES as u64 / JOURNAL_SCRIPT_LIMIT as u64,
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
    let path = dir
        .join("display-v1")
        .join(format!("{}.bin", hash.to_display_hex()));
    if !path.exists() {
        return Ok(Vec::new());
    }
    Ok(read_block(dir, hash)?.1)
}
fn bounded_read(path: &Path) -> Result<Vec<u8>, EventStoreError> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(MAX_BLOCK_DISPLAY_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_BLOCK_DISPLAY_BYTES {
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
