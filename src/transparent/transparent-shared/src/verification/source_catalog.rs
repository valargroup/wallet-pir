//! Checked revision-directory references used by both Transparent products.
use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// Resolve a publication's revision to its physical directory. The only allowed
/// redirect is `<publication>/<digest>` to `../.sources/<digest>` beside it.
/// The catalog and revision must be real directories. File readers receive the
/// checked target, so their regular-file checks never follow a logical parent.
pub(crate) fn revision_directory(publication: &Path, entry: &Path) -> io::Result<PathBuf> {
    let metadata = fs::symlink_metadata(entry)?;
    if metadata.is_dir() {
        return Ok(entry.to_path_buf());
    }
    let refused = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid revision catalog link: {}", entry.display()),
        )
    };
    let name = entry
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| crate::publication::is_digest(name))
        .ok_or_else(refused)?;
    let catalog = publication.parent().ok_or_else(refused)?.join(".sources");
    let target = catalog.join(name);
    // A missing catalog or source is an invalid link, not a reading error:
    // callers refuse the publication with that reason instead of reporting
    // that a path could not be read.
    if !metadata.file_type().is_symlink()
        || fs::read_link(entry).ok().as_deref() != Some(&Path::new("../.sources").join(name))
        || !fs::symlink_metadata(&catalog).is_ok_and(|m| m.is_dir())
        || !fs::symlink_metadata(&target).is_ok_and(|m| m.is_dir())
    {
        return Err(refused());
    }
    Ok(target)
}
