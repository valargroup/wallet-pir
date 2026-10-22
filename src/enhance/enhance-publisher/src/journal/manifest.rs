//! Journal encoding and the rename-and-sync commit boundary.
use super::{JournalBlock, JournalError, RecordJournal};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;

// Version 7's manifest binds both record width and row packing. Schema 11 keeps
// the manifest encoding, but its 653-byte width rejects all older 737-byte journals.
// Always rebuild in a separate directory; never reinterpret or truncate old data.
pub(super) const STORE_VERSION: u16 = 7;

fn default_table() -> String {
    // Unnamed version-3 journals were ACTION journals, not Enhance journals.
    "action".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct JournalManifest {
    pub(super) version: u16,
    #[serde(default)]
    pub(super) record_bytes: usize,
    #[serde(default)]
    pub(super) records_per_row: usize,
    #[serde(default = "default_table")]
    pub(super) table: String,
    pub(super) tree_size: u64,
    pub(super) blocks: Vec<JournalBlock>,
}

impl RecordJournal {
    pub(super) fn persist_manifest(&self, manifest: &JournalManifest) -> Result<(), JournalError> {
        let path = self.manifest_path();
        let temporary = self.dir.join("manifest.json.tmp");
        let bytes = serde_json::to_vec_pretty(manifest)?;
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        self.checkpoint("manifest_write")?;
        file.sync_all()?;
        self.checkpoint("manifest_sync")?;
        fs::rename(temporary, path)?;
        self.checkpoint("manifest_rename")?;
        File::open(&self.dir)?.sync_all()?;
        self.checkpoint("directory_sync")?;
        Ok(())
    }
}
