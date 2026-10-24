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
    let catalog_metadata = |path: &Path| {
        fs::symlink_metadata(path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "invalid revision catalog link: {}: {error}",
                    entry.display()
                ),
            )
        })
    };
    if !metadata.file_type().is_symlink()
        || fs::read_link(entry)? != Path::new("../.sources").join(name)
        || !catalog_metadata(&catalog)?.is_dir()
        || !catalog_metadata(&target)?.is_dir()
    {
        return Err(refused());
    }
    Ok(target)
}
