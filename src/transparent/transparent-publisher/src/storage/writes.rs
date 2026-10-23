//! Coordinate object references with the collector's complete scan and sweep.
use std::{
    fs::{File, OpenOptions},
    io,
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

pub(super) const LOCK_FILE: &str = ".object-writes.lock";

fn open(root: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .custom_flags(libc::O_NOFOLLOW)
        .open(root.join(LOCK_FILE))?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::other("object write lock is not a regular file"));
    }
    Ok(file)
}

/// Writers may run together, but keep collection out until references exist.
pub(super) fn shared(root: &Path) -> io::Result<File> {
    let file = open(root)?;
    file.lock_shared()?;
    Ok(file)
}

/// Hold across both discovery of references and deletion of unused objects.
pub(super) fn exclusive(root: &Path) -> io::Result<File> {
    let file = open(root)?;
    file.lock()?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linked_lock_files_are_refused_without_changing_their_targets() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let outside = temp.path().join("journal");
        std::fs::write(&outside, b"committed bytes").unwrap();
        std::os::unix::fs::symlink(&outside, root.join(LOCK_FILE)).unwrap();
        assert!(shared(&root).is_err());
        assert!(exclusive(&root).is_err());
        assert_eq!(std::fs::read(outside).unwrap(), b"committed bytes");
    }
}
