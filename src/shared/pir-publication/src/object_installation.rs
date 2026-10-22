//! Durable installation of publisher-owned immutable objects.
//!
//! Installing bytes proves their identity and durability, never permission to
//! serve them. Publication references and their lifetimes belong to the caller.
mod staging;
mod verification;

use crate::{is_object_digest, ObjectReference};
use staging::StagedObject;
use std::{
    fmt, fs, io,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};
use verification::{ensure_unchanged, open_regular, verify, FileIdentity};

/// An object installed or verified in the publisher's store.
#[derive(Debug)]
pub struct InstalledObject {
    pub reference: ObjectReference,
    /// False when an existing, fully verified object won installation.
    pub installed: bool,
    /// Body bytes hashed by this attempt, including an existing destination.
    pub bytes_hashed: u64,
}

/// Why a publisher could not establish a complete, durable object.
#[derive(Debug)]
pub enum ObjectInstallError {
    Io(io::Error),
    Cancelled,
    InvalidDigest,
    NotRegularFile(PathBuf),
    FileChanged(PathBuf),
    DigestMismatch(PathBuf),
    LengthMismatch { expected: u64, actual: u64 },
}

impl fmt::Display for ObjectInstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Cancelled => f.write_str("object installation cancelled"),
            Self::InvalidDigest => f.write_str("object digest is not lowercase SHA-256"),
            Self::NotRegularFile(path) => {
                write!(f, "{} is not a regular object file", path.display())
            }
            Self::FileChanged(path) => {
                write!(f, "{} changed during object verification", path.display())
            }
            Self::DigestMismatch(path) => {
                write!(f, "{} has a conflicting object digest", path.display())
            }
            Self::LengthMismatch { expected, actual } => {
                write!(f, "object length {actual} differs from expected {expected}")
            }
        }
    }
}
impl std::error::Error for ObjectInstallError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
impl From<io::Error> for ObjectInstallError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
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
    fn unchanged(&self) -> Result<(), ObjectInstallError> {
        ensure_unchanged(self.staging.path(), &self.file, self.identity)?;
        if let Some(path) = &self.source {
            ensure_unchanged(path, &self.file, self.identity)?;
        }
        Ok(())
    }
}

fn seal(staging: &StagedObject) -> Result<fs::File, ObjectInstallError> {
    let file = open_regular(staging.path())?;
    if !file.metadata()?.permissions().readonly() {
        file.set_permissions(fs::Permissions::from_mode(0o444))?;
    }
    Ok(file)
}

impl ObjectInstaller {
    pub fn open(directory: PathBuf) -> Result<Self, ObjectInstallError> {
        fs::create_dir_all(&directory)?;
        if !fs::symlink_metadata(&directory)?.is_dir() {
            return Err(ObjectInstallError::NotRegularFile(directory));
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
    ) -> Result<InstalledObject, ObjectInstallError> {
        let mut staging = StagedObject::create(&self.directory)?;
        let reference = staging.write(&mut input, stop)?;
        let file = seal(&staging)?;
        let identity = FileIdentity::of(&file.metadata()?);
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
    ) -> Result<InstalledObject, ObjectInstallError> {
        if expected_digest.is_some_and(|digest| !is_object_digest(digest)) {
            return Err(ObjectInstallError::InvalidDigest);
        }
        check_stop(stop)?;
        if !fs::symlink_metadata(source)?.is_file() {
            return Err(ObjectInstallError::NotRegularFile(source.to_path_buf()));
        }
        let staging = StagedObject::adopt(&self.directory, source)?;
        let mut staged = seal(&staging)?;
        let identity = FileIdentity::of(&staged.metadata()?);
        ensure_unchanged(source, &staged, identity)?;
        let reference = verify(staging.path(), &mut staged, None, stop)?;
        ensure_unchanged(source, &staged, identity)?;
        if expected_digest.is_some_and(|expected| expected != reference.sha256) {
            return Err(ObjectInstallError::DigestMismatch(source.to_path_buf()));
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
    ) -> Result<InstalledObject, ObjectInstallError> {
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
            let mut existing = open_regular(&target)?;
            verify(&target, &mut existing, Some(reference), stop)?;
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

fn check_stop(stop: &dyn Fn() -> bool) -> Result<(), ObjectInstallError> {
    if stop() {
        Err(ObjectInstallError::Cancelled)
    } else {
        Ok(())
    }
}
