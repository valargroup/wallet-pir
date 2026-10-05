//! Re-encodes a version-1 event journal as version 2, without touching it.
//!
//! The two versions differ only in the event codec: v1 stores a 96-byte record
//! (kind byte, flags, and 8 reserved bytes), v2 the packed 87-byte record. The
//! script bytes, the block hashes and the order of every event are the same, so
//! a v2 journal is a pure function of a v1 one. Deriving it here takes one
//! sequential pass instead of a re-ingest from genesis.
//!
//! The source is read, never opened for writing: no writer lock is taken, so
//! the continuous publisher keeps appending to it while this runs. Only the
//! committed prefix is read, and the pass stops `--margin` blocks below the
//! committed tip, so a reorg rolling the source back cannot reach the range
//! being read. After the pass the source's block records for that range are
//! read again and must be unchanged. The ingester extends the result to the
//! node's tip and reconciles its last blocks against the node on start.
//!
//! The destination is written under `<to>.partial` and renamed into place only
//! after every file, the checkpoint and finally `meta.json` are durable, so an
//! interrupted run leaves nothing that opens as a journal.

use clap::Parser;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use transparent_events::{ReceiveEvent, SpendEvent, TransparentEvent, Txid, EVENT_BYTES};
use transparent_filter_server::events::EventStore;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const V1_EVENT_BYTES: usize = 96;
const BLOCK_RECORD_BYTES: usize = 32 + 8 + 8;
/// Bytes between page-cache drops, so the pass does not evict the node's
/// RocksDB pages from a host that serves live queries.
const DROP_EVERY: u64 = 256 << 20;

#[derive(Parser)]
#[command(
    name = "journal-convert-v1",
    about = "Re-encode a version-1 event journal as version 2"
)]
struct Cli {
    /// The version-1 journal. Read only.
    #[arg(long)]
    from: PathBuf,
    /// A directory that must not exist yet.
    #[arg(long)]
    to: PathBuf,
    /// Blocks left unconverted below the source's committed tip.
    #[arg(long, default_value_t = 1_000)]
    margin: u64,
    /// Last height to convert. Defaults to the committed tip minus `--margin`.
    #[arg(long)]
    through: Option<u64>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Meta {
    version: u16,
    genesis_hash: String,
    start_height: u64,
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("8 bytes"))
}

/// The version-1 codec, frozen. Strict in the same ways the v1 decoder was.
fn decode_v1(bytes: &[u8; V1_EVENT_BYTES]) -> Result<TransparentEvent, String> {
    const FLAG_COINBASE: u8 = 1;
    if bytes[88..].iter().any(|byte| *byte != 0) {
        return Err("reserved bytes are not zero".into());
    }
    let flags = bytes[1];
    if flags & !FLAG_COINBASE != 0 {
        return Err(format!("unknown flag bits {flags:#04x}"));
    }
    let transaction_index = u16::from_le_bytes(bytes[2..4].try_into().expect("2"));
    let height = u32::from_le_bytes(bytes[4..8].try_into().expect("4"));
    let value = u64::from_le_bytes(bytes[8..16].try_into().expect("8"));
    let txid = Txid(bytes[16..48].try_into().expect("32"));
    let index = u32::from_le_bytes(bytes[48..52].try_into().expect("4"));
    let spent_txid = Txid(bytes[52..84].try_into().expect("32"));
    let spent_output_index = u32::from_le_bytes(bytes[84..88].try_into().expect("4"));
    match bytes[0] {
        0 => {
            if spent_txid != Txid([0; 32]) || spent_output_index != 0 {
                return Err("a receive sets the consumed outpoint".into());
            }
            Ok(TransparentEvent::Receive(ReceiveEvent {
                metadata: None,
                height,
                txid,
                transaction_index,
                output_index: index,
                value,
                coinbase: flags & FLAG_COINBASE != 0,
            }))
        }
        1 => {
            if value != 0 || flags != 0 {
                return Err("a spend sets a value or the coinbase flag".into());
            }
            Ok(TransparentEvent::Spend(SpendEvent {
                metadata: None,
                height,
                spending_txid: txid,
                transaction_index,
                input_index: index,
                spent_txid,
                spent_output_index,
            }))
        }
        kind => Err(format!("unknown event kind {kind}")),
    }
}

#[cfg(target_os = "linux")]
fn drop_cache(file: &File, upto: u64) {
    use std::os::unix::io::AsRawFd;
    // Advisory only; a failure costs page cache, not correctness.
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), 0, upto as i64, libc::POSIX_FADV_DONTNEED);
    }
}

#[cfg(not(target_os = "linux"))]
fn drop_cache(_file: &File, _upto: u64) {}

struct Summary {
    through: u64,
    blocks: u64,
    events: u64,
    source_bytes: u64,
    written_bytes: u64,
}

/// The committed block records of a v1 journal, and its committed event bytes.
fn source_prefix(from: &Path) -> Result<(Meta, Vec<u8>, u64), BoxError> {
    let meta: Meta = serde_json::from_slice(&std::fs::read(from.join("meta.json"))?)?;
    if meta.version != 1 {
        return Err(format!("{} is version {}, not 1", from.display(), meta.version).into());
    }
    let checkpoint = std::fs::read(from.join("checkpoint.bin"))?;
    if checkpoint.len() != 16 {
        return Err("checkpoint is not 16 bytes".into());
    }
    let events_len = read_u64(&checkpoint[..8]);
    let blocks_len = read_u64(&checkpoint[8..]) as usize;
    let mut raw = Vec::with_capacity(blocks_len);
    File::open(from.join("blocks.bin"))?
        .take(blocks_len as u64)
        .read_to_end(&mut raw)?;
    if raw.len() != blocks_len || raw.len() % BLOCK_RECORD_BYTES != 0 {
        return Err("blocks.bin does not hold its committed records".into());
    }
    Ok((meta, raw, events_len))
}

fn convert(cli: &Cli) -> Result<Summary, BoxError> {
    if cli.to.exists() {
        return Err(format!("{} already exists", cli.to.display()).into());
    }
    let (meta, records, events_len) = source_prefix(&cli.from)?;
    let committed = (records.len() / BLOCK_RECORD_BYTES) as u64;
    if committed == 0 {
        return Err("the source journal covers no blocks".into());
    }
    let tip = meta.start_height + committed - 1;
    let through = match cli.through {
        Some(height) => height,
        None => tip
            .checked_sub(cli.margin)
            .ok_or("the margin is larger than the journal")?,
    };
    if through < meta.start_height || through + cli.margin > tip {
        return Err(format!(
            "height {through} is not at least {} blocks below the committed tip {tip}",
            cli.margin
        )
        .into());
    }
    let blocks = through - meta.start_height + 1;
    let records = &records[..blocks as usize * BLOCK_RECORD_BYTES];

    let partial = cli.to.with_extension("partial");
    if partial.exists() {
        return Err(format!(
            "{} exists from an earlier run; remove it",
            partial.display()
        )
        .into());
    }
    std::fs::create_dir_all(&partial)?;

    let source = File::open(cli.from.join("events.bin"))?;
    let mut reader = BufReader::with_capacity(8 << 20, source.try_clone()?);
    let events_out = File::create(partial.join("events.bin"))?;
    let mut writer = BufWriter::with_capacity(8 << 20, events_out.try_clone()?);
    let mut blocks_out = BufWriter::new(File::create(partial.join("blocks.bin"))?);

    let mut read_at = 0u64;
    let mut written = 0u64;
    let mut events = 0u64;
    let mut dropped = 0u64;
    let mut v1 = [0u8; V1_EVENT_BYTES];
    for (index, record) in records.chunks_exact(BLOCK_RECORD_BYTES).enumerate() {
        let height = meta.start_height + index as u64;
        let offset = read_u64(&record[32..40]);
        let count = read_u64(&record[40..48]);
        if offset != read_at {
            return Err(format!(
                "block {height} starts at byte {offset}, but the previous block ended at {read_at}"
            )
            .into());
        }
        let block_offset = written;
        for _ in 0..count {
            let mut length = [0u8; 2];
            reader.read_exact(&mut length)?;
            let length = u16::from_le_bytes(length);
            let mut script = vec![0u8; length as usize];
            reader.read_exact(&mut script)?;
            reader.read_exact(&mut v1)?;
            let event = decode_v1(&v1).map_err(|error| format!("block {height}: {error}"))?;
            if u64::from(event.height()) != height {
                return Err(format!("block {height} holds an event at {}", event.height()).into());
            }
            let v2 = event.to_legacy_bytes();
            if TransparentEvent::from_bytes(&v2)? != event {
                return Err(format!("block {height}: v2 bytes do not round-trip").into());
            }
            writer.write_all(&length.to_le_bytes())?;
            writer.write_all(&script)?;
            writer.write_all(&v2)?;
            read_at += (2 + script.len() + V1_EVENT_BYTES) as u64;
            written += (2 + script.len() + EVENT_BYTES) as u64;
            events += 1;
        }
        blocks_out.write_all(&record[..32])?;
        blocks_out.write_all(&block_offset.to_le_bytes())?;
        blocks_out.write_all(&count.to_le_bytes())?;

        if read_at - dropped >= DROP_EVERY {
            writer.flush()?;
            events_out.sync_data()?;
            drop_cache(&source, read_at);
            drop_cache(&events_out, written);
            dropped = read_at;
        }
        if (index + 1) % 100_000 == 0 {
            eprintln!("converted through {height}: {events} events, {read_at} of {events_len} source bytes");
        }
    }
    if read_at > events_len {
        return Err("the converted range runs past the committed event bytes".into());
    }

    writer.flush()?;
    drop(writer);
    events_out.sync_all()?;
    drop_cache(&events_out, written);
    blocks_out.flush()?;
    blocks_out.get_ref().sync_all()?;
    drop(blocks_out);

    // The range read must still be the range the source commits. A rollback
    // that reached it would show here as a changed record.
    let (_, again, _) = source_prefix(&cli.from)?;
    if again.get(..records.len()) != Some(records) {
        return Err("the source's block records changed during the pass".into());
    }

    let mut checkpoint = Vec::with_capacity(16);
    checkpoint.extend_from_slice(&written.to_le_bytes());
    checkpoint.extend_from_slice(&(records.len() as u64).to_le_bytes());
    write_durable(&partial.join("checkpoint.bin"), &checkpoint)?;
    let meta = Meta {
        version: 2,
        genesis_hash: meta.genesis_hash,
        start_height: meta.start_height,
    };
    write_durable(
        &partial.join("meta.json"),
        &serde_json::to_vec_pretty(&meta)?,
    )?;
    File::open(&partial)?.sync_all()?;
    std::fs::rename(&partial, &cli.to)?;
    if let Some(parent) = cli.to.parent() {
        File::open(parent)?.sync_all()?;
    }

    Ok(Summary {
        through,
        blocks,
        events,
        source_bytes: read_at,
        written_bytes: written,
    })
}

fn write_durable(path: &Path, bytes: &[u8]) -> Result<(), BoxError> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let summary = convert(&cli)?;
    // Opened through the same read-only path a publish uses.
    let store = EventStore::open_existing(&cli.to)?;
    if store.covered_through() != Some(summary.through) || store.events_stored() != summary.events {
        return Err("the converted journal does not open to the range written".into());
    }
    println!(
        "{}",
        serde_json::json!({
            "from": cli.from,
            "to": cli.to,
            "through": summary.through,
            "blocks": summary.blocks,
            "events": summary.events,
            "source_bytes": summary.source_bytes,
            "written_bytes": summary.written_bytes,
        })
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_filter::{BlockHash, ScriptBytes};

    fn encode_v1(event: &TransparentEvent) -> [u8; V1_EVENT_BYTES] {
        let mut bytes = [0u8; V1_EVENT_BYTES];
        match event {
            TransparentEvent::Receive(e) => {
                bytes[1] = u8::from(e.coinbase);
                bytes[2..4].copy_from_slice(&e.transaction_index.to_le_bytes());
                bytes[4..8].copy_from_slice(&e.height.to_le_bytes());
                bytes[8..16].copy_from_slice(&e.value.to_le_bytes());
                bytes[16..48].copy_from_slice(&e.txid.0);
                bytes[48..52].copy_from_slice(&e.output_index.to_le_bytes());
            }
            TransparentEvent::Spend(e) => {
                bytes[0] = 1;
                bytes[2..4].copy_from_slice(&e.transaction_index.to_le_bytes());
                bytes[4..8].copy_from_slice(&e.height.to_le_bytes());
                bytes[16..48].copy_from_slice(&e.spending_txid.0);
                bytes[48..52].copy_from_slice(&e.input_index.to_le_bytes());
                bytes[52..84].copy_from_slice(&e.spent_txid.0);
                bytes[84..88].copy_from_slice(&e.spent_output_index.to_le_bytes());
            }
        }
        bytes
    }

    fn events_at(height: u32) -> Vec<(ScriptBytes, TransparentEvent)> {
        (0..height % 3)
            .map(|i| {
                let script = ScriptBytes::new(vec![0x76, 0xa9, height as u8, i as u8]);
                let event = if i % 2 == 0 {
                    TransparentEvent::Receive(ReceiveEvent {
                        metadata: None,
                        height,
                        txid: Txid([height as u8 ^ 0x5a; 32]),
                        transaction_index: i as u16,
                        output_index: i,
                        value: u64::from(height) * 1_000 + u64::from(i),
                        coinbase: i == 0,
                    })
                } else {
                    TransparentEvent::Spend(SpendEvent {
                        metadata: None,
                        height,
                        spending_txid: Txid([height as u8; 32]),
                        transaction_index: i as u16,
                        input_index: i,
                        spent_txid: Txid([7; 32]),
                        spent_output_index: 3,
                    })
                };
                (script, event)
            })
            .collect()
    }

    /// Writes a v1 journal in the layout the v1 `EventStore` wrote, plus an
    /// uncommitted tail that must not be read.
    fn write_v1(dir: &Path, start: u32, blocks: u32) {
        std::fs::create_dir_all(dir).unwrap();
        let mut events = Vec::new();
        let mut records = Vec::new();
        for height in start..start + blocks {
            let at_block = events_at(height);
            records.extend_from_slice(&[height as u8; 32]);
            records.extend_from_slice(&(events.len() as u64).to_le_bytes());
            records.extend_from_slice(&(at_block.len() as u64).to_le_bytes());
            for (script, event) in at_block {
                events.extend_from_slice(&(script.as_slice().len() as u16).to_le_bytes());
                events.extend_from_slice(script.as_slice());
                events.extend_from_slice(&encode_v1(&event));
            }
        }
        let mut checkpoint = (events.len() as u64).to_le_bytes().to_vec();
        checkpoint.extend_from_slice(&(records.len() as u64).to_le_bytes());
        events.extend_from_slice(b"uncommitted");
        std::fs::write(dir.join("events.bin"), events).unwrap();
        std::fs::write(dir.join("blocks.bin"), records).unwrap();
        std::fs::write(dir.join("checkpoint.bin"), checkpoint).unwrap();
        std::fs::write(
            dir.join("meta.json"),
            format!(r#"{{"version":1,"genesis_hash":"g","start_height":{start}}}"#),
        )
        .unwrap();
    }

    #[test]
    fn a_converted_journal_holds_the_same_events_and_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("v1");
        let to = dir.path().join("v2");
        write_v1(&from, 100, 40);
        let before: Vec<Vec<u8>> = ["events.bin", "blocks.bin", "checkpoint.bin", "meta.json"]
            .iter()
            .map(|name| std::fs::read(from.join(name)).unwrap())
            .collect();

        let summary = convert(&Cli {
            from: from.clone(),
            to: to.clone(),
            margin: 5,
            through: None,
        })
        .unwrap();
        assert_eq!(summary.through, 134);

        let store = EventStore::open_existing(&to).unwrap();
        assert_eq!(store.start_height(), 100);
        assert_eq!(store.covered_through(), Some(134));
        for height in 100..=134u32 {
            assert_eq!(
                store.events_at(u64::from(height)).unwrap().unwrap(),
                events_at(height),
                "height {height}"
            );
            assert_eq!(
                store.block_at(u64::from(height)).unwrap().block_hash,
                BlockHash::from_internal_bytes([height as u8; 32])
            );
        }
        // The source is untouched and no lock file was created in it.
        let after: Vec<Vec<u8>> = ["events.bin", "blocks.bin", "checkpoint.bin", "meta.json"]
            .iter()
            .map(|name| std::fs::read(from.join(name)).unwrap())
            .collect();
        assert_eq!(before, after);
        assert!(!from.join("writer.lock").exists());
        assert!(!dir.path().join("v2.partial").exists());
    }

    /// The frozen converter retains 87-byte v2 records; the current v3 writer
    /// frames its records differently but must recover the same legacy facts.
    #[test]
    fn frozen_v2_bytes_and_v3_semantics_agree() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("v1");
        write_v1(&from, 0, 20);
        let converted = dir.path().join("converted");
        convert(&Cli {
            from,
            to: converted.clone(),
            margin: 0,
            through: Some(19),
        })
        .unwrap();
        let mut expected = Vec::new();
        let mut direct = EventStore::open(dir.path().join("v3"), "g", 0).unwrap();
        for height in 0..20u32 {
            let events = events_at(height);
            for (script, event) in &events {
                expected.extend_from_slice(&(script.as_slice().len() as u16).to_le_bytes());
                expected.extend_from_slice(script.as_slice());
                expected.extend_from_slice(&event.to_legacy_bytes());
            }
            direct
                .append_block(
                    u64::from(height),
                    BlockHash::from_internal_bytes([height as u8; 32]),
                    &events,
                )
                .unwrap();
        }
        direct.commit().unwrap();
        assert_eq!(
            std::fs::read(converted.join("events.bin")).unwrap(),
            expected
        );
        let legacy = EventStore::open_existing(&converted).unwrap();
        assert_eq!(legacy.version(), 2);
        for height in 0..20u64 {
            assert_eq!(
                legacy.events_at(height).unwrap(),
                direct.events_at(height).unwrap()
            );
        }
    }

    #[test]
    fn a_range_too_close_to_the_tip_or_an_existing_target_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("v1");
        write_v1(&from, 0, 10);
        let to = dir.path().join("v2");
        assert!(convert(&Cli {
            from: from.clone(),
            to: to.clone(),
            margin: 5,
            through: Some(7),
        })
        .is_err());
        std::fs::create_dir_all(&to).unwrap();
        assert!(convert(&Cli {
            from,
            to,
            margin: 0,
            through: None,
        })
        .is_err());
    }

    #[test]
    fn dirty_v1_records_are_refused() {
        let event = events_at(4).remove(0).1;
        let mut bytes = encode_v1(&event);
        assert_eq!(decode_v1(&bytes).unwrap(), event);
        bytes[95] = 1;
        assert!(decode_v1(&bytes).is_err());
        let mut bytes = encode_v1(&event);
        bytes[0] = 2;
        assert!(decode_v1(&bytes).is_err());
    }
}
