//! Whole-file identity checks on the opened inode and the name that selected it.
use super::{check_stop, ObjectInstallError};
use crate::ObjectReference;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, Metadata},
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::Path,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct FileIdentity {
    device: u64,
    inode: u64,
    bytes: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}
impl FileIdentity {
    pub(super) fn of(metadata: &Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: metadata.len(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }
}

pub(super) fn open_regular(path: &Path) -> Result<File, ObjectInstallError> {
    let named = fs::symlink_metadata(path)?;
    if !named.is_file() {
        return Err(ObjectInstallError::NotRegularFile(path.to_path_buf()));
    }
    let file = File::open(path)?;
    ensure_unchanged(path, &file, FileIdentity::of(&named))?;
    Ok(file)
}

pub(super) fn ensure_unchanged(
    path: &Path,
    file: &File,
    identity: FileIdentity,
) -> Result<(), ObjectInstallError> {
    let opened = file.metadata()?;
    let named = fs::symlink_metadata(path)?;
    if !named.is_file()
        || !opened.is_file()
        || FileIdentity::of(&opened) != identity
        || FileIdentity::of(&named) != identity
    {
        return Err(ObjectInstallError::FileChanged(path.to_path_buf()));
    }
    Ok(())
}

pub(super) fn verify(
    path: &Path,
    file: &mut File,
    expected: Option<&ObjectReference>,
    stop: &dyn Fn() -> bool,
) -> Result<ObjectReference, ObjectInstallError> {
    let identity = FileIdentity::of(&file.metadata()?);
    ensure_unchanged(path, file, identity)?;
    if let Some(expected) = expected {
        if expected.bytes != identity.bytes {
            return Err(ObjectInstallError::LengthMismatch {
                expected: expected.bytes,
                actual: identity.bytes,
            });
        }
    }
    // Read at most the recorded length plus one byte, so a concurrent append
    // cannot turn verification into an unbounded scan.
    let reference = copy_and_hash(
        &mut (&mut *file).take(identity.bytes.saturating_add(1)),
        std::io::sink(),
        stop,
    )?;
    ensure_unchanged(path, file, identity)?;
    if reference.bytes != identity.bytes {
        return Err(ObjectInstallError::LengthMismatch {
            expected: identity.bytes,
            actual: reference.bytes,
        });
    }
    if let Some(expected) = expected {
        if expected.bytes != reference.bytes {
            return Err(ObjectInstallError::LengthMismatch {
                expected: expected.bytes,
                actual: reference.bytes,
            });
        }
        if expected.sha256 != reference.sha256 {
            return Err(ObjectInstallError::DigestMismatch(path.to_path_buf()));
        }
    }
    Ok(reference)
}

/// One bounded streaming loop for construction and verification. A verifier
/// discards the output; a constructor writes the same bytes it identifies.
pub(super) fn copy_and_hash(
    input: &mut impl Read,
    mut output: impl Write,
    stop: &dyn Fn() -> bool,
) -> Result<ObjectReference, ObjectInstallError> {
    let mut buffer = vec![0u8; 1 << 20];
    let mut hash = Sha256::new();
    let mut bytes = 0u64;
    loop {
        check_stop(stop)?;
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        output.write_all(&buffer[..count])?;
        hash.update(&buffer[..count]);
        bytes = bytes
            .checked_add(count as u64)
            .ok_or_else(|| std::io::Error::other("object length overflow"))?;
    }
    Ok(ObjectReference {
        sha256: hex::encode(hash.finalize()),
        bytes,
    })
}
