//! Durable JSON records written so that a crash leaves either the old record
//! or the new one.

use serde::Serialize;
use std::io::Write;
use std::path::Path;

/// Writes `value` as JSON to `path` so that a crash leaves either the old
/// record or the new one: written beside it, synced, renamed over it, and the
/// directory synced so the rename itself survives.
pub(crate) fn persist(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let parent = path.parent().ok_or("record needs a parent directory")?;
    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let temp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temp).map_err(|e| e.to_string())?;
    file.write_all(&serde_json::to_vec(value).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    std::fs::rename(temp, path).map_err(|e| e.to_string())?;
    std::fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}
