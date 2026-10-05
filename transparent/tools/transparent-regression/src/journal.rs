//! Read-only journal reader for fixture export. Pins committed lengths once.
//! Kept independent of the ingest service and its RocksDB/node dependencies.
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::{
    cell::RefCell,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};
use transparent_events::{TransparentEvent, EVENT_BYTES, MAX_EVENT_BYTES};
use transparent_filter::{BlockHash, ScriptBytes};
#[derive(Deserialize)]
struct Meta {
    version: u16,
    genesis_hash: String,
    start_height: u64,
}
#[derive(Clone, Copy)]
pub struct BlockEntry {
    pub block_hash: BlockHash,
    offset: u64,
    count: u64,
}
pub struct EventStore {
    meta: Meta,
    blocks: Vec<BlockEntry>,
    events: RefCell<File>,
    events_len: u64,
}
fn number(raw: &[u8]) -> u64 {
    u64::from_le_bytes(raw.try_into().expect("checked fixed-size record"))
}
impl EventStore {
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let meta: Meta = serde_json::from_slice(&std::fs::read(path.join("meta.json"))?)?;
        if !matches!(meta.version, 2 | 3) {
            bail!("unsupported journal version");
        }
        let checkpoint = std::fs::read(path.join("checkpoint.bin"))?;
        if checkpoint.len() != 16 {
            bail!("invalid committed-length checkpoint");
        }
        let events_len = number(&checkpoint[..8]);
        let blocks_len = number(&checkpoint[8..]);
        if !blocks_len.is_multiple_of(48) {
            bail!("partial committed block record");
        }
        let events = File::open(path.join("events.bin"))?;
        let mut blocks_file = File::open(path.join("blocks.bin"))?;
        if events.metadata()?.len() < events_len || blocks_file.metadata()?.len() < blocks_len {
            bail!("journal files shorter than commit");
        }
        let mut raw = vec![0; usize::try_from(blocks_len)?];
        blocks_file.read_exact(&mut raw)?;
        let mut blocks = Vec::new();
        let mut previous = 0;
        for row in raw.chunks_exact(48) {
            let offset = number(&row[32..40]);
            let count = number(&row[40..48]);
            if offset < previous
                || offset > events_len
                || count > events_len / ((EVENT_BYTES + 2) as u64)
            {
                bail!("invalid committed block extent");
            }
            blocks.push(BlockEntry {
                block_hash: BlockHash::from_internal_bytes(row[..32].try_into().unwrap()),
                offset,
                count,
            });
            previous = offset;
        }
        Ok(Self {
            meta,
            blocks,
            events: RefCell::new(events),
            events_len,
        })
    }
    pub fn genesis_hash(&self) -> &str {
        &self.meta.genesis_hash
    }
    pub fn start_height(&self) -> u64 {
        self.meta.start_height
    }
    pub fn covered_through(&self) -> Option<u64> {
        self.blocks
            .len()
            .checked_sub(1)
            .map(|n| self.meta.start_height + n as u64)
    }
    pub fn block_at(&self, h: u64) -> Option<BlockEntry> {
        self.blocks
            .get(usize::try_from(h.checked_sub(self.meta.start_height)?).ok()?)
            .copied()
    }
    pub fn events_at(&self, h: u64) -> Result<Option<Vec<(ScriptBytes, TransparentEvent)>>> {
        let Some(block) = self.block_at(h) else {
            return Ok(None);
        };
        let end = self.block_at(h + 1).map_or(self.events_len, |b| b.offset);
        let mut file = self.events.borrow_mut();
        file.seek(SeekFrom::Start(block.offset))?;
        let size = usize::try_from(
            end.checked_sub(block.offset)
                .context("backwards block offset")?,
        )?;
        let mut block_bytes = vec![0; size];
        file.read_exact(&mut block_bytes)?;
        let mut reader = block_bytes.as_slice();
        let mut events = Vec::new();
        for _ in 0..block.count {
            let mut length = [0; 2];
            reader.read_exact(&mut length)?;
            let length = u16::from_le_bytes(length) as usize;
            if length > 10_000 {
                bail!("invalid journal script length");
            }
            let mut script = vec![0; length];
            reader.read_exact(&mut script)?;
            let event_len = if self.meta.version == 3 {
                let mut length = [0; 2];
                reader.read_exact(&mut length)?;
                usize::from(u16::from_le_bytes(length))
            } else {
                EVENT_BYTES
            };
            if !(EVENT_BYTES..=MAX_EVENT_BYTES).contains(&event_len) {
                bail!("invalid journal event length");
            }
            let mut raw = vec![0; event_len];
            reader.read_exact(&mut raw)?;
            let event = TransparentEvent::from_bytes(&raw)?;
            if crate::height(&event) != h {
                bail!("journal event stored under wrong block");
            }
            events.push((ScriptBytes::new(script), event));
        }
        if !reader.is_empty() {
            bail!("journal block count disagrees with its extent");
        }
        Ok(Some(events))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn respects_committed_lengths_without_truncating_the_journal() {
        let dir = tempfile::tempdir().unwrap();
        let event = TransparentEvent::Receive(transparent_events::ReceiveEvent {
            metadata: None,
            height: 0,
            txid: transparent_events::Txid([7; 32]),
            transaction_index: 0,
            output_index: 0,
            value: 5,
            coinbase: true,
        });
        let mut record = vec![1, 0, 0x51];
        record.extend_from_slice(&event.to_bytes());
        let committed = record.len() as u64;
        record.extend_from_slice(b"uncommitted event suffix");
        std::fs::write(dir.path().join("events.bin"), &record).unwrap();
        let mut block = vec![0; 32];
        block.extend_from_slice(&0u64.to_le_bytes());
        block.extend_from_slice(&1u64.to_le_bytes());
        block.extend_from_slice(b"uncommitted block suffix");
        std::fs::write(dir.path().join("blocks.bin"), &block).unwrap();
        std::fs::write(
            dir.path().join("meta.json"),
            r#"{"version":2,"genesis_hash":"test","start_height":0}"#,
        )
        .unwrap();
        let mut cp = committed.to_le_bytes().to_vec();
        cp.extend_from_slice(&48u64.to_le_bytes());
        std::fs::write(dir.path().join("checkpoint.bin"), cp).unwrap();
        let store = EventStore::open_existing(dir.path()).unwrap();
        assert_eq!(store.covered_through(), Some(0));
        assert_eq!(store.events_at(0).unwrap().unwrap()[0].1, event);
        assert!(store.events_at(1).unwrap().is_none());
        drop(store);
        assert_eq!(
            std::fs::read(dir.path().join("events.bin")).unwrap(),
            record
        );
        assert_eq!(std::fs::read(dir.path().join("blocks.bin")).unwrap(), block);
        // A count claiming absence while the extent contains an event is corruption.
        let mut b = std::fs::OpenOptions::new()
            .write(true)
            .open(dir.path().join("blocks.bin"))
            .unwrap();
        b.seek(SeekFrom::Start(40)).unwrap();
        b.write_all(&0u64.to_le_bytes()).unwrap();
        drop(b);
        assert!(EventStore::open_existing(dir.path())
            .unwrap()
            .events_at(0)
            .is_err());
    }
}
