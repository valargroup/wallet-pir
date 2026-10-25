//! A query server's local cache of fetched publication objects.
//!
//! Objects arrive in partial files, become read-only and are renamed to their
//! digest only after the caller verified the whole transfer, and are collected
//! when no authoritative reference retains them. A cached file confers no
//! authority; the caller still observes and checks the publication.
use crate::{
    is_object_digest, open_object_file, verify_object_file, FileIdentity, ObjectReference,
    ObjectVerification, ReceivedObjectFile,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const PARTIAL_PREFIX: &str = ".download-";

/// Exclusive owner of one object directory. The caller holds the directory's
/// service lock, so interrupted partial files are safe to remove on open.
/// Clones refer to the same directory and owner.
#[derive(Clone)]
pub struct ObjectCache {
    directory: PathBuf,
    verified: Arc<Mutex<BTreeMap<String, (ObjectReference, FileIdentity)>>>,
}

/// An opened object whose exact inode and bytes passed integrity verification.
/// This proof is local to this process and grants no publication authority.
pub struct VerifiedObjectFile {
    path: PathBuf,
    file: fs::File,
    verification: ObjectVerification,
}
impl VerifiedObjectFile {
    /// Verify a physical file without reusing evidence from another owner.
    pub fn open(path: PathBuf, object: &ObjectReference) -> io::Result<Self> {
        let mut file = open_object_file(&path, Some(object.bytes))?;
        let verified = verify_object_file(&path, &mut file, Some(object), None, &|| false)?;
        Ok(Self {
            path,
            file,
            verification: verified,
        })
    }
    /// Seal a verified cache entry without invalidating the proof through our
    /// own permission change. Existing writable entries use the same inode.
    pub fn make_read_only(mut self) -> io::Result<Self> {
        self.check()?;
        if !self.file.metadata()?.permissions().readonly() {
            self.file
                .set_permissions(fs::Permissions::from_mode(0o444))?;
            self.verification.identity = self
                .verification
                .identity
                .after_rename(&self.path, &self.file)?;
            self.verification.reusable = false;
        }
        Ok(self)
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn reference(&self) -> &ObjectReference {
        &self.verification.reference
    }
    pub fn file(&self) -> &fs::File {
        &self.file
    }
    /// Ensure the open file and its name still select the verified inode.
    pub fn check(&self) -> io::Result<()> {
        self.verification
            .identity
            .check(&self.path, &self.file)
            .map_err(io::Error::from)
    }
    /// Read the verified file from the beginning, rejecting concurrent changes.
    pub fn read_bytes(&self) -> io::Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        self.check()?;
        let size = self.verification.reference.bytes;
        let capacity =
            usize::try_from(size).map_err(|_| io::Error::other("object exceeds address space"))?;
        let mut bytes = vec![0u8; capacity];
        let mut read = 0;
        while read < bytes.len() {
            let count = self.file.read_at(&mut bytes[read..], read as u64)?;
            if count == 0 {
                return Err(io::Error::other("verified object length changed"));
            }
            read += count;
        }
        self.check()?;
        Ok(bytes)
    }
}

/// Checked open files retained across transfer and preparation. A lookup binds
/// the complete expected reference, including length, before reading the handle.
#[derive(Default)]
pub struct VerifiedObjects(BTreeMap<String, Arc<VerifiedObjectFile>>);
impl VerifiedObjects {
    pub fn insert(&mut self, file: VerifiedObjectFile) {
        self.0
            .insert(file.reference().sha256.clone(), Arc::new(file));
    }
    pub fn get(&self, expected: &ObjectReference) -> io::Result<Arc<VerifiedObjectFile>> {
        self.0
            .get(&expected.sha256)
            .filter(|file| file.reference() == expected)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "required checked object is absent")
            })
            .cloned()
    }
    pub fn read(&self, expected: &ObjectReference) -> io::Result<Vec<u8>> {
        self.get(expected)?.read_bytes()
    }
}

impl ObjectCache {
    /// Create the directory if needed and remove partial files left by an
    /// interrupted transfer. Completed objects are kept for re-verification.
    pub fn open(directory: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&directory)?;
        if !fs::symlink_metadata(&directory)?.is_dir() {
            return Err(io::Error::other("object cache must be a real directory"));
        }
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(PARTIAL_PREFIX)
            {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(Self {
            directory,
            verified: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    /// Where `object` lives once promoted. The path alone proves nothing. A
    /// name that is not a SHA-256 digest is refused, so a reference can never
    /// address a file outside the object namespace.
    pub fn object_path(&self, object: &ObjectReference) -> io::Result<PathBuf> {
        if !is_object_digest(&object.sha256) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "object reference is not a SHA-256 digest",
            ));
        }
        Ok(self.directory.join(&object.sha256))
    }
    /// The promoted file for `object` after checking its integrity, or `None`
    /// when absent. Settled, unchanged files reuse process-local evidence; a
    /// changed file is hashed again and altered bytes are an error.
    pub fn verified_cached(&self, object: &ObjectReference) -> io::Result<Option<PathBuf>> {
        Ok(self.open_verified(object)?.map(|file| file.path))
    }
    /// Open and verify an object, reusing only settled integrity evidence for
    /// the same reference and unchanged physical file. Clones share evidence;
    /// reopening the cache after a restart deliberately starts without it.
    pub fn open_verified(
        &self,
        object: &ObjectReference,
    ) -> io::Result<Option<VerifiedObjectFile>> {
        let path = self.object_path(object)?;
        let mut file = match open_object_file(&path, Some(object.bytes)) {
            Ok(file) => file,
            Err(crate::ObjectFileError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(None)
            }
            Err(error) => return Err(error.into()),
        };
        let prior = self.verified.lock().unwrap().get(&object.sha256).cloned();
        let verified = verify_object_file(
            &path,
            &mut file,
            Some(object),
            prior.as_ref().map(|(r, i)| (r, *i)),
            &|| false,
        )?;
        let mut evidence = self.verified.lock().unwrap();
        if verified.reusable {
            evidence.insert(object.sha256.clone(), (object.clone(), verified.identity));
        } else {
            evidence.remove(&object.sha256);
        }
        Ok(Some(VerifiedObjectFile {
            path,
            file,
            verification: verified,
        }))
    }
    /// Open received bytes by length only. No digest evidence is created or
    /// inferred, including after restart. The product owns deferred validation.
    pub fn open_received(
        &self,
        object: &ObjectReference,
    ) -> io::Result<Option<ReceivedObjectFile>> {
        let path = self.object_path(object)?;
        match ReceivedObjectFile::open(path, object.clone()) {
            Ok(file) => Ok(Some(file.seal()?)),
            Err(crate::ObjectFileError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }
    /// Start a new partial file for `object`. Dropping the partial before
    /// [`PartialObject::promote`] removes it.
    pub fn begin_partial(&self, object: &ObjectReference) -> io::Result<PartialObject> {
        let target_path = self.object_path(object)?;
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
            target_path,
            expected: object.clone(),
            file: Some(file),
        })
    }
    /// Disk charge of every directory entry, including unfinished transfers.
    pub fn occupied_bytes(&self) -> io::Result<u64> {
        fs::read_dir(&self.directory)?.try_fold(0u64, |sum, entry| {
            sum.checked_add(entry?.metadata()?.len())
                .ok_or_else(|| io::Error::other("cache size overflow"))
        })
    }
    /// Remove every promoted object not in `retained`. Unrelated and partial
    /// files are left alone.
    pub fn collect_unreferenced(&self, retained: &BTreeSet<String>) -> io::Result<()> {
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if is_object_digest(&name) && entry.file_type()?.is_file() && !retained.contains(&name)
            {
                fs::remove_file(entry.path())?;
                self.verified.lock().unwrap().remove(&name);
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
    expected: ObjectReference,
    file: Option<fs::File>,
}
impl PartialObject {
    /// The open partial file. The caller checks the complete body according to its
    /// product policy; promotion itself establishes no digest or authority.
    pub fn take_file(&mut self) -> io::Result<fs::File> {
        self.file
            .take()
            .ok_or_else(|| io::Error::other("partial object file already taken"))
    }
    /// Publish length-checked bytes for products whose whole-file validation
    /// runs later. The receipt carries the same opened inode across promotion.
    pub fn promote_received(self, written: fs::File) -> io::Result<ReceivedObjectFile> {
        written.set_permissions(fs::Permissions::from_mode(0o444))?;
        written.sync_all()?;
        let received = ReceivedObjectFile::open(self.partial_path.clone(), self.expected.clone())?;
        let path = self.promote(written)?;
        Ok(received.renamed(path)?)
    }

    /// Seal and verify the partial before exposing its completed name. The
    /// checked handle survives promotion; loading does not hash it again.
    pub fn promote_verified(self, written: fs::File) -> io::Result<VerifiedObjectFile> {
        written.set_permissions(fs::Permissions::from_mode(0o444))?;
        written.sync_all()?;
        let mut checked = VerifiedObjectFile::open(self.partial_path.clone(), &self.expected)?;
        checked.check()?;
        let path = self.promote(written)?;
        checked.verification.identity = checked
            .verification
            .identity
            .after_rename(&path, &checked.file)?;
        // A new name must settle before its evidence can be reused by a later
        // independent open. This handle itself remains checked and usable.
        checked.verification.reusable = false;
        checked.path = path;
        Ok(checked)
    }

    /// Make a completed partial immutable and durable under its expected digest name:
    /// sync file contents, mark it read-only, rename, then sync the directory.
    /// An error before the rename removes the partial and leaves no object; an
    /// error after it leaves a promoted file that the next use re-verifies.
    pub fn promote(mut self, written: fs::File) -> io::Result<PathBuf> {
        if !written.metadata()?.permissions().readonly() {
            written.set_permissions(fs::Permissions::from_mode(0o444))?;
        }
        written.sync_all()?;
        drop(written);
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
    use std::io::{Seek, Write};

    #[test]
    fn checked_reads_are_independent_of_the_shared_file_cursor() {
        use std::io::{Seek, SeekFrom};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("checked");
        let bytes = vec![7u8; 4096];
        fs::write(&path, &bytes).unwrap();
        let checked =
            Arc::new(VerifiedObjectFile::open(path, &ObjectReference::for_bytes(&bytes)).unwrap());
        let mut cursor = checked.file();
        cursor.seek(SeekFrom::Start(17)).unwrap();
        std::thread::scope(|scope| {
            for _ in 0..2 {
                let checked = checked.clone();
                let expected = &bytes;
                scope.spawn(move || {
                    for _ in 0..10 {
                        assert_eq!(&checked.read_bytes().unwrap(), expected);
                    }
                });
            }
        });
        assert_eq!(cursor.stream_position().unwrap(), 17);
    }

    #[test]
    fn settled_objects_reuse_integrity_across_clones_but_not_restarts_or_changes() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ObjectCache::open(dir.path().to_path_buf()).unwrap();
        let object = ObjectReference::for_bytes(b"verified");
        let path = cache.object_path(&object).unwrap();
        fs::write(&path, b"verified").unwrap();
        // The shared verifier deliberately distrusts the current filesystem
        // timestamp tick; allow this file to settle before recording evidence.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        // Whole-file verification leaves its fresh file at EOF; reuse leaves
        // a newly opened handle at zero. Observe reads through this file fixture.
        let first = cache.open_verified(&object).unwrap().unwrap();
        assert_eq!((&first.file).stream_position().unwrap(), object.bytes);
        let opened = cache.clone().open_verified(&object).unwrap().unwrap();
        assert_eq!((&opened.file).stream_position().unwrap(), 0);
        assert_eq!(opened.read_bytes().unwrap(), b"verified");
        let restarted = ObjectCache::open(dir.path().to_path_buf()).unwrap();
        let reopened = restarted.open_verified(&object).unwrap().unwrap();
        assert_eq!((&reopened.file).stream_position().unwrap(), object.bytes);

        // Equal length and the same digest-shaped name cannot hide corruption.
        fs::write(&path, b"verifieD").unwrap();
        assert!(cache.open_verified(&object).is_err());
        assert!(opened.read_bytes().is_err());
        fs::remove_file(&path).unwrap();
        fs::write(&path, b"verified").unwrap();
        let replacement = cache.open_verified(&object).unwrap().unwrap();
        assert_eq!((&replacement.file).stream_position().unwrap(), object.bytes);
        assert!(opened.check().is_err());
        assert!(cache
            .open_verified(&ObjectReference {
                bytes: object.bytes + 1,
                ..object.clone()
            })
            .is_err());
        let target = dir.path().join("other");
        fs::rename(&path, &target).unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        assert!(cache.open_verified(&object).is_err());
    }

    #[test]
    fn transferred_handles_bind_length_and_detect_replacement_before_loading() {
        let root = tempfile::tempdir().unwrap();
        let cache = ObjectCache::open(root.path().into()).unwrap();
        let expected = ObjectReference::for_bytes(b"checked bytes");
        let path = cache.object_path(&expected).unwrap();
        fs::write(&path, b"checked bytes").unwrap();
        let mut objects = VerifiedObjects::default();
        objects.insert(cache.open_verified(&expected).unwrap().unwrap());
        assert_eq!(objects.read(&expected).unwrap(), b"checked bytes");
        let mut wrong_length = expected.clone();
        wrong_length.bytes += 1;
        assert!(objects.read(&wrong_length).is_err());
        fs::remove_file(&path).unwrap();
        fs::write(&path, b"checked bytes").unwrap();
        assert!(
            objects.read(&expected).is_err(),
            "a new inode needs its own verification"
        );
    }

    #[test]
    fn invalid_transfers_never_acquire_a_completed_name() {
        let root = tempfile::tempdir().unwrap();
        let cache = ObjectCache::open(root.path().into()).unwrap();
        let expected = ObjectReference::for_bytes(b"valid");
        for body in [b"bad".as_slice(), b"wrong".as_slice()] {
            let mut partial = cache.begin_partial(&expected).unwrap();
            let mut file = partial.take_file().unwrap();
            file.write_all(body).unwrap();
            assert!(partial.promote_verified(file).is_err());
            assert!(!cache.object_path(&expected).unwrap().exists());
        }
        let mut partial = cache.begin_partial(&expected).unwrap();
        let mut file = partial.take_file().unwrap();
        file.write_all(b"valid").unwrap();
        let checked = partial.promote_verified(file).unwrap();
        assert_eq!(checked.read_bytes().unwrap(), b"valid");
    }

    #[test]
    fn opening_a_symlink_does_not_clean_another_directory() {
        let root = tempfile::tempdir().unwrap();
        let real = root.path().join("real");
        fs::create_dir(&real).unwrap();
        let partial = real.join(format!("{PARTIAL_PREFIX}unfinished"));
        fs::write(&partial, b"unfinished").unwrap();
        let alias = root.path().join("objects");
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        assert!(ObjectCache::open(alias).is_err());
        assert!(partial.exists());
    }

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
        assert_eq!(
            reopened.verified_cached(&object).unwrap(),
            Some(promoted.clone())
        );
        assert_eq!(fs::read_dir(reopened.directory()).unwrap().count(), 1);

        // A promoted object altered after promotion is refused, never served.
        fs::set_permissions(&promoted, fs::Permissions::from_mode(0o644)).unwrap();
        fs::write(&promoted, b"promoteD").unwrap();
        assert!(reopened.verified_cached(&object).is_err());
    }

    #[test]
    fn a_failed_promotion_before_rename_leaves_no_object() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ObjectCache::open(dir.path().to_path_buf()).unwrap();
        let object = ObjectReference::for_bytes(b"blocked");
        // A non-empty directory at the target name makes the rename fail.
        fs::create_dir(cache.object_path(&object).unwrap()).unwrap();
        fs::write(cache.object_path(&object).unwrap().join("occupant"), b"").unwrap();
        let mut partial = cache.begin_partial(&object).unwrap();
        let mut file = partial.take_file().unwrap();
        file.write_all(b"blocked").unwrap();
        assert!(partial.promote(file).is_err());
        let names: Vec<_> = fs::read_dir(cache.directory())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(
            names,
            vec![object.sha256.clone()],
            "the partial was removed"
        );
        assert!(cache.verified_cached(&object).is_err());
    }

    #[test]
    fn references_that_are_not_digests_never_reach_the_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ObjectCache::open(dir.path().join("objects")).unwrap();
        fs::write(dir.path().join("secret"), b"x").unwrap();
        let escape = ObjectReference {
            sha256: "../secret".into(),
            bytes: 1,
        };
        assert!(cache.object_path(&escape).is_err());
        assert!(cache.verified_cached(&escape).is_err());
        assert!(cache.begin_partial(&escape).is_err());
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
