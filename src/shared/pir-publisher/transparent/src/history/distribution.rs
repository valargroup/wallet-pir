//! Durable distribution revision shared by history and transaction-detail publication.
use crate::history::publication::BoxError;
use std::io::Write;
use std::path::Path;

pub(crate) fn advance_distribution_revision(root: &Path) -> Result<u64, BoxError> {
    advance_distribution_revision_with_sync(root, |directory| directory.sync_all())
}

// Keep the strict fence local: publication::write_atomic has a
// best-effort directory sync and cannot acknowledge this durable authority floor.
pub(crate) fn advance_distribution_revision_with_sync(
    root: &Path,
    sync_directory: impl FnOnce(&std::fs::File) -> std::io::Result<()>,
) -> Result<u64, BoxError> {
    // Opening before rename also refuses directory-open errors before changing
    // the counter. A post-rename sync error leaves an uncertain reservation on
    // disk, but no caller may expose it; the next successful call skips it.
    let directory = std::fs::File::open(root)?;
    let path = root.join("distribution-revision.json");
    let previous: u64 = match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    let next = previous
        .checked_add(1)
        .ok_or("distribution revision exhausted")?;
    let temporary = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(&serde_json::to_vec(&next)?)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temporary, &path)?;
    sync_directory(&directory)?;
    Ok(next)
}
