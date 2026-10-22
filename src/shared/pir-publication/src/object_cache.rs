//! A query server's local cache of fetched publication objects.
//!
//! Objects arrive in partial files, become read-only and are renamed to their
//! digest only after the caller verified the whole transfer, and are collected
//! when no authoritative reference retains them. A cached file confers no
//! authority; the caller still observes and checks the publication.
use crate::{is_object_digest, ObjectReference};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs, io,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

const PARTIAL_PREFIX: &str = ".download-";

/// Exclusive owner of one object directory. The caller holds the directory's
/// service lock, so interrupted partial files are safe to remove on open.
pub struct ObjectCache {
    directory: PathBuf,
}

impl ObjectCache {
    /// Create the directory if needed and remove partial files left by an
    /// interrupted transfer. Completed objects are kept for re-verification.
    pub fn open(directory: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&directory)?;
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry.file_name().to_string_lossy().starts_with(PARTIAL_PREFIX)
            {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(Self { directory })
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    /// Where `object` lives once promoted. The path alone proves nothing.
    pub fn object_path(&self, object: &ObjectReference) -> PathBuf {
        self.directory.join(&object.sha256)
    }
    /// The promoted file for `object` after re-verifying its full length and
    /// digest, or `None` when it is absent. A present file with different bytes
    /// is an error: an immutable promoted object was altered.
    pub fn verified_cached(&self, object: &ObjectReference) -> io::Result<Option<PathBuf>> {
        let path = self.object_path(object);
        let file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        if file.metadata()?.len() != object.bytes {
            return Err(io::Error::other("cached object length differs"));
        }
        let mut reader = io::BufReader::new(file);
        let mut buffer = [0u8; 65536];
        let mut hash = Sha256::new();
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
        }
        if hex::encode(hash.finalize()) != object.sha256 {
            return Err(io::Error::other("cached object digest differs"));
        }
        Ok(Some(path))
    }
    /// Start a new partial file for `object`. Dropping the partial before
    /// [`PartialObject::promote`] removes it.
    pub fn begin_partial(&self, object: &ObjectReference) -> io::Result<PartialObject> {
        let partial_path = self.directory.join(format!(
            "{PARTIAL_PREFIX}{}",
            hex::encode(rand::random::<[u8; 16]>())
        ));
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial_path)?;
        Ok(PartialObject {
            directory: self.directory.clone(),
            partial_path,
            target_path: self.object_path(object),
            file: Some(file),
        })
    }
    /// Remove every promoted object not in `retained`. Unrelated and partial
    /// files are left alone.
    pub fn collect_unreferenced(&self, retained: &BTreeSet<String>) -> io::Result<()> {
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if is_object_digest(&name) && entry.file_type()?.is_file() && !retained.contains(&name) {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }
}

/// One in-progress object transfer.
pub struct PartialObject {
    directory: PathBuf,
    partial_path: PathBuf,
    target_path: PathBuf,
    file: Option<fs::File>,
}
impl PartialObject {
    /// The open partial file. The caller writes the complete, verified body.
    pub fn take_file(&mut self) -> io::Result<fs::File> {
        self.file
            .take()
            .ok_or_else(|| io::Error::other("partial object file already taken"))
    }
    /// Make the verified partial immutable and durable under its digest name:
    /// sync file contents, mark it read-only, rename, then sync the directory.
    /// An error before the rename removes the partial and leaves no object; an
    /// error after it leaves a promoted file that the next use re-verifies.
    pub fn promote(mut self, written: fs::File) -> io::Result<PathBuf> {
        written.sync_all()?;
        drop(written);
        fs::set_permissions(&self.partial_path, fs::Permissions::from_mode(0o444))?;
        fs::rename(&self.partial_path, &self.target_path)?;
        // Nothing is left to clean up once the rename succeeded.
        self.partial_path = PathBuf::new();
        fs::File::open(&self.directory)?.sync_all()?;
        Ok(std::mem::take(&mut self.target_path))
    }
}
impl Drop for PartialObject {
    fn drop(&mut self) {
        if !self.partial_path.as_os_str().is_empty() {
            let _ = fs::remove_file(&self.partial_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn interrupted_transfers_never_become_cached_objects() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ObjectCache::open(dir.path().join("objects")).unwrap();
        let object = ObjectReference::for_bytes(b"complete object");
        let mut partial = cache.begin_partial(&object).unwrap();
        partial.take_file().unwrap().write_all(b"complete").unwrap();
        drop(partial);
        assert_eq!(cache.verified_cached(&object).unwrap(), None);
        assert_eq!(fs::read_dir(cache.directory()).unwrap().count(), 0);
    }

    #[test]
    fn restart_reverifies_cached_files_and_removes_partials() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ObjectCache::open(dir.path().join("objects")).unwrap();
        let object = ObjectReference::for_bytes(b"promoted");
        let mut partial = cache.begin_partial(&object).unwrap();
        let mut file = partial.take_file().unwrap();
        file.write_all(b"promoted").unwrap();
        let promoted = partial.promote(file).unwrap();
        assert!(fs::metadata(&promoted).unwrap().permissions().readonly());
        let leftover = cache.begin_partial(&object).unwrap();
        std::mem::forget(leftover);

        let reopened = ObjectCache::open(dir.path().join("objects")).unwrap();
        assert_eq!(reopened.verified_cached(&object).unwrap(), Some(promoted.clone()));
        assert_eq!(fs::read_dir(reopened.directory()).unwrap().count(), 1);

        // A promoted object altered after promotion is refused, never served.
        fs::set_permissions(&promoted, fs::Permissions::from_mode(0o644)).unwrap();
        fs::write(&promoted, b"promoteD").unwrap();
        assert!(reopened.verified_cached(&object).is_err());
    }

    #[test]
    fn collection_keeps_only_retained_objects() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ObjectCache::open(dir.path().to_path_buf()).unwrap();
        let mut names = Vec::new();
        for bytes in [b"first".as_slice(), b"second"] {
            let object = ObjectReference::for_bytes(bytes);
            let mut partial = cache.begin_partial(&object).unwrap();
            let mut file = partial.take_file().unwrap();
            file.write_all(bytes).unwrap();
            partial.promote(file).unwrap();
            names.push(object.sha256);
        }
        fs::write(dir.path().join("active.json"), b"{}").unwrap();
        cache
            .collect_unreferenced(&BTreeSet::from([names[1].clone()]))
            .unwrap();
        assert!(!dir.path().join(&names[0]).exists());
        assert!(dir.path().join(&names[1]).exists());
        assert!(dir.path().join("active.json").exists());
    }
}
