//! Atomic replacement of publisher metadata and authority records.
use crate::BoxError;
use std::path::Path;

/// Writes `bytes` to `path` so that a reader sees either all of them or none.
///
/// Temp file, fsync, rename. A published set is read back by a service that
/// refuses to start on a file that does not match its digest, so a half-written
/// file is not a corruption a reader has to detect — but only if the partial
/// state is never visible under the final name. `std::fs::write` truncates in
/// place, so an interrupted publish leaves a short file *at the name the map
/// points to*, which is exactly the state a deploy would then try to ship.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), BoxError> {
    write_durable(path, bytes)
}

/// [`write_atomic`] for a record that must survive a crash before its caller
/// acts on it: completion evidence that permits activation and cheap restart,
/// and the integrity-failure record that keeps corrupt bytes withdrawn.
///
/// Every step is an error: the parent directory is
/// opened before the final name changes, and a failed directory sync fails
/// the write. After a sync failure the final name may already hold the new
/// bytes, but the caller has not been told they are durable. A failed attempt
/// removes its temporary and leaves the final name as it was or whole.
pub fn write_durable(path: &Path, bytes: &[u8]) -> Result<(), BoxError> {
    use std::io::Write;
    pir_control::durable::replace_durable(path, |file| file.write_all(bytes))
        .map_err(|error| format!("replace durable record {}: {error}", path.display()).into())
}

pub fn write_immutable(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), BoxError> {
    let path = dir.join(name);
    if path.exists() {
        // Published shards are immutable. If the bytes differ, either the
        // journal or the builder changed while the identity did not, and
        // overwriting would replace a published shard in place.
        let existing = std::fs::read(&path)?;
        if existing != bytes {
            return Err(format!("{} already exists with different content", path.display()).into());
        }
        return Ok(());
    }
    write_atomic(&path, bytes)
}
