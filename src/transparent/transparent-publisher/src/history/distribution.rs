//! Durable distribution revision shared by history and transaction-detail publication.
use crate::history::publication::BoxError;
use std::io::Write;
use std::path::Path;

pub(crate) fn advance_distribution_revision(root: &Path) -> Result<u64, BoxError> {
    advance_distribution_revision_with_write(root, |path, bytes| {
        pir_control::durable::replace_durable(path, |file| file.write_all(bytes))
    })
}

/// Reserve the next revision only after its record is durable. An uncertain
/// write stays reserved on disk and is skipped by the next successful call.
pub(crate) fn advance_distribution_revision_with_write(
    root: &Path,
    write: impl FnOnce(&Path, &[u8]) -> std::io::Result<()>,
) -> Result<u64, BoxError> {
    let path = root.join("distribution-revision.json");
    let previous: u64 = match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    let next = previous
        .checked_add(1)
        .ok_or("distribution revision exhausted")?;
    write(&path, &serde_json::to_vec(&next)?)?;
    Ok(next)
}
