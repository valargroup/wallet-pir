//! What the controller did and when: the measurement record.
//!
//! `<root>/timeline.jsonl` gets one JSON object per event, each with `kind`
//! and `unix_ms`: `block`, `cycle`, `seal`, `drop`, `reorg`, `error`, `lag`
//! and `bootstrap`. Lines are appended and never rewritten, so a reader can
//! follow the file while the controller runs.
//!
//! `<root>/tooling/heights.bin` maps txids to heights for fixture tooling:
//! fixed records of `txid(32) || height u64 LE`, heights non-decreasing,
//! truncated when the chain rolls back. It is not served.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use transparent_shard::txid::TransparentDisplayRecord;

pub const TIMELINE_FILE: &str = "timeline.jsonl";
pub const HEIGHTS_FILE: &str = "tooling/heights.bin";

pub struct Timeline {
    file: Mutex<File>,
}

impl Timeline {
    pub fn open(root: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join(TIMELINE_FILE))?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    /// Appends one event. The timeline is evidence, not state: a failed write
    /// is logged and the controller carries on.
    pub fn record(&self, kind: &str, fields: serde_json::Value) {
        let mut event = serde_json::json!({"kind": kind, "unix_ms": super::now_ms()});
        if let (Some(event), serde_json::Value::Object(fields)) = (event.as_object_mut(), fields) {
            event.extend(fields);
        }
        let mut line = serde_json::to_vec(&event).expect("a timeline event serializes");
        line.push(b'\n');
        if let Err(error) = self.file.lock().unwrap().write_all(&line) {
            tracing::error!(%error, kind, "timeline write failed");
        }
    }
}

/// Reads every event of a timeline, skipping a torn last line.
pub fn read_timeline(root: &Path) -> std::io::Result<Vec<serde_json::Value>> {
    let bytes = std::fs::read(root.join(TIMELINE_FILE))?;
    Ok(bytes
        .split(|b| *b == b'\n')
        .filter_map(|line| serde_json::from_slice(line).ok())
        .collect())
}

const INDEX_RECORD: u64 = 40;

/// The tooling txid → height index.
pub struct HeightIndex {
    path: PathBuf,
    file: File,
    len: u64,
}

impl HeightIndex {
    /// Opens or creates the index, dropping a torn trailing record.
    pub fn open(root: &Path) -> std::io::Result<Self> {
        let path = root.join(HEIGHTS_FILE);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        let len = file.metadata()?.len() / INDEX_RECORD * INDEX_RECORD;
        file.set_len(len)?;
        Ok(Self { path, file, len })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn records(&self) -> u64 {
        self.len / INDEX_RECORD
    }

    fn height_at(&self, index: u64) -> std::io::Result<u64> {
        let mut bytes = [0u8; 8];
        self.file
            .read_exact_at(&mut bytes, index * INDEX_RECORD + 32)?;
        Ok(u64::from_le_bytes(bytes))
    }

    pub fn last_height(&self) -> std::io::Result<Option<u64>> {
        match self.records() {
            0 => Ok(None),
            n => self.height_at(n - 1).map(Some),
        }
    }

    pub fn append(
        &mut self,
        height: u64,
        records: &[TransparentDisplayRecord],
    ) -> std::io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut bytes = Vec::with_capacity(records.len() * INDEX_RECORD as usize);
        for record in records {
            bytes.extend(record.txid.0);
            bytes.extend(height.to_le_bytes());
        }
        self.file.write_all_at(&bytes, self.len)?;
        self.len += bytes.len() as u64;
        Ok(())
    }

    /// Drops every record above `height`. Heights are non-decreasing, so the
    /// cut is found by binary search.
    pub fn truncate_after(&mut self, height: u64) -> std::io::Result<()> {
        let (mut low, mut high) = (0, self.records());
        while low < high {
            let mid = (low + high) / 2;
            if self.height_at(mid)? <= height {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        self.len = low * INDEX_RECORD;
        self.file.set_len(self.len)
    }

    /// Every `(txid, height)` record; for tests and tooling.
    pub fn read_all(&self) -> std::io::Result<Vec<([u8; 32], u64)>> {
        let mut bytes = vec![0u8; self.len as usize];
        self.file.read_exact_at(&mut bytes, 0)?;
        Ok(bytes
            .chunks_exact(INDEX_RECORD as usize)
            .map(|r| {
                (
                    r[..32].try_into().unwrap(),
                    u64::from_le_bytes(r[32..].try_into().unwrap()),
                )
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txid_display::fixture::record;

    #[test]
    fn index_appends_truncates_and_drops_torn_records() {
        let root = tempfile::tempdir().unwrap();
        let mut index = HeightIndex::open(root.path()).unwrap();
        assert_eq!(index.last_height().unwrap(), None);
        for h in [5u64, 6, 8] {
            index
                .append(h, &[record(h as u32, 10), record(h as u32 + 100, 10)])
                .unwrap();
        }
        index.append(9, &[]).unwrap();
        assert_eq!(index.records(), 6);
        assert_eq!(index.last_height().unwrap(), Some(8));
        index.truncate_after(7).unwrap();
        assert_eq!(index.last_height().unwrap(), Some(6));
        index.truncate_after(100).unwrap();
        assert_eq!(index.records(), 4);
        drop(index);
        // A torn append is cut back to whole records on open.
        let path = root.path().join(HEIGHTS_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend([1, 2, 3]);
        std::fs::write(&path, bytes).unwrap();
        let index = HeightIndex::open(root.path()).unwrap();
        assert_eq!(index.records(), 4);
        assert_eq!(index.read_all().unwrap()[0].1, 5);

        let timeline = Timeline::open(root.path()).unwrap();
        timeline.record("lag", serde_json::json!({"behind_steps": 2}));
        let events = read_timeline(root.path()).unwrap();
        assert_eq!(events[0]["kind"], "lag");
        assert_eq!(events[0]["behind_steps"], 2);
    }
}
