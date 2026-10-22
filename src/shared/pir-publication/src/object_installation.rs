//! Durable installation of publisher-owned immutable objects.
//!
//! Installing bytes proves their identity and durability, never permission to
//! serve them. Publication references and their lifetimes belong to the caller.
mod staging;

use crate::object_file::check_stop;
use crate::{hash_object_file, open_object_file, FileIdentity, ObjectFileError};
use crate::{is_object_digest, ObjectReference};
use staging::StagedObject;
use std::{
    fs, io,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

/// An object installed or verified in the publisher's store.
#[derive(Debug)]
pub struct InstalledObject {
    pub reference: ObjectReference,
    /// False when an existing, fully verified object won installation.
    pub installed: bool,
    /// Body bytes hashed by this attempt, including an existing destination.
    pub bytes_hashed: u64,
}

/// Installs publisher objects below one real directory. Completed objects are
/// never replaced or modified. Each attempt owns private staging, so concurrent
/// installers cannot remove another attempt's work. No staging sweep or object
/// collection occurs here.
pub struct ObjectInstaller {
    directory: PathBuf,
}

/// The exact staged inode whose bytes were identified. No filesystem check
/// after hashing may silently substitute a newer observation of that file.
struct VerifiedStaging {
    staging: StagedObject,
    file: fs::File,
    reference: ObjectReference,
    identity: FileIdentity,
    source: Option<PathBuf>,
}
impl VerifiedStaging {
    fn unchanged(&self) -> Result<(), ObjectFileError> {
        self.identity.check(self.staging.path(), &self.file)?;
        if let Some(path) = &self.source {
            self.identity.check(path, &self.file)?;
        }
        Ok(())
    }
}

fn seal(staging: &StagedObject) -> Result<fs::File, ObjectFileError> {
    let file = open_object_file(staging.path(), None)?;
    if !file.metadata()?.permissions().readonly() {
        file.set_permissions(fs::Permissions::from_mode(0o444))?;
    }
    Ok(file)
}

impl ObjectInstaller {
    pub fn open(directory: PathBuf) -> Result<Self, ObjectFileError> {
        fs::create_dir_all(&directory)?;
        if !fs::symlink_metadata(&directory)?.is_dir() {
            return Err(ObjectFileError::NotRegularFile(directory));
        }
        // The directory itself must survive a first installation too.
        if let Some(parent) = directory
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(Self { directory })
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Write and hash a stream with bounded memory in private staging. A read,
    /// write or cancellation failure exposes no completed object. Check `stop`
    /// between reads and immediately before installation.
    pub fn install_stream(
        &self,
        mut input: impl Read,
        stop: &dyn Fn() -> bool,
    ) -> Result<InstalledObject, ObjectFileError> {
        let mut staging = StagedObject::create(&self.directory)?;
        let reference = staging.write(&mut input, stop)?;
        let file = seal(&staging)?;
        let identity = FileIdentity::capture(staging.path(), &file)?;
        self.finish(
            VerifiedStaging {
                staging,
                file,
                reference,
                identity,
                source: None,
            },
            stop,
        )
    }

    /// Adopt a complete publisher-owned prepared file on this filesystem.
    /// A private staging link avoids copying large runtimes. Only real regular
    /// files are accepted; the original name and opened file must remain the
    /// same throughout verification. `expected_digest`, when known from a
    /// manifest, must match. Cross-filesystem links fail without a copy fallback.
    ///
    /// Callers hand over completed files, with no outstanding writer, and must
    /// never write them again. Reference-only publication reuse must not call
    /// this method: it keeps the existing object and its verification evidence.
    pub fn install_prepared_file(
        &self,
        source: &Path,
        expected_digest: Option<&str>,
        stop: &dyn Fn() -> bool,
    ) -> Result<InstalledObject, ObjectFileError> {
        if expected_digest.is_some_and(|digest| !is_object_digest(digest)) {
            return Err(ObjectFileError::InvalidDigest);
        }
        check_stop(stop)?;
        if !fs::symlink_metadata(source)?.is_file() {
            return Err(ObjectFileError::NotRegularFile(source.to_path_buf()));
        }
        let staging = StagedObject::adopt(&self.directory, source)?;
        let mut staged = seal(&staging)?;
        let identity = FileIdentity::capture(staging.path(), &staged)?;
        identity.check(source, &staged)?;
        let (reference, _) = hash_object_file(staging.path(), &mut staged, None, stop)?;
        identity.check(source, &staged)?;
        if let Some(expected) = expected_digest.filter(|expected| *expected != reference.sha256) {
            return Err(ObjectFileError::DigestMismatch {
                path: source.to_path_buf(),
                expected: expected.to_owned(),
                actual: reference.sha256.clone(),
            });
        }
        self.finish(
            VerifiedStaging {
                staging,
                file: staged,
                reference,
                identity,
                source: Some(source.to_path_buf()),
            },
            stop,
        )
    }

    fn finish(
        &self,
        verified: VerifiedStaging,
        stop: &dyn Fn() -> bool,
    ) -> Result<InstalledObject, ObjectFileError> {
        check_stop(stop)?;
        verified.unchanged()?;
        let reference = &verified.reference;
        let target = self.directory.join(&reference.sha256);
        // Permission changes preceded the identity check. Sync failures happen
        // before the file acquires a completed object name.
        verified.file.sync_all()?;
        check_stop(stop)?;
        verified.unchanged()?;
        let installed = match fs::hard_link(verified.staging.path(), &target) {
            Ok(()) => true,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
            Err(error) => return Err(error.into()),
        };
        let mut bytes_hashed = reference.bytes;
        if !installed {
            let mut existing = open_object_file(&target, Some(reference.bytes))?;
            hash_object_file(&target, &mut existing, Some(reference), stop)?;
            existing.sync_all()?;
            bytes_hashed += reference.bytes;
            verified.unchanged()?;
        }
        // Remove only our staging link before syncing the final namespace.
        // A failed directory sync leaves a whole object, but returns no success
        // that could authorize a durable publication to reference it.
        let VerifiedStaging {
            staging, reference, ..
        } = verified;
        drop(staging);
        fs::File::open(&self.directory)?.sync_all()?;
        Ok(InstalledObject {
            reference,
            installed,
            bytes_hashed,
        })
    }
}
