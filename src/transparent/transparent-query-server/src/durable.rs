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
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    pir_control::durable::replace_durable(path, |file| file.write_all(&bytes))
        .map_err(|error| error.to_string())
}
