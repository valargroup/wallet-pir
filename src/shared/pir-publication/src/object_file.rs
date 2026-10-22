//! Whole-file integrity and exact filesystem identity. Neither is serving authority.
mod error;
mod identity;

use crate::{is_object_digest, ObjectReference};
pub use error::ObjectFileError;
pub use identity::FileIdentity;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{Read, Seek, Write},
    path::Path,
};

/// Hash a whole regular file from offset zero, checking its opened and named
/// identity before and after the read. Reads are bounded to the initial length
/// plus one byte. Failure produces no verification evidence. The caller owns
/// evidence lifetimes and semantic checks; use only checked physical paths.
pub fn hash_object_file(
    path: &Path,
    file: &mut File,
    expected: Option<&ObjectReference>,
    stop: &dyn Fn() -> bool,
) -> Result<(ObjectReference, FileIdentity), ObjectFileError> {
    if expected.is_some_and(|object| !is_object_digest(&object.sha256)) {
        return Err(ObjectFileError::InvalidDigest);
    }
    let identity = FileIdentity::capture(path, file)?;
    let bytes = file.metadata()?.len();
    if let Some(expected) = expected {
        check_length(expected.bytes, bytes)?;
    }
    file.rewind()?;
    let reference = copy_and_hash(
        &mut (&mut *file).take(bytes.saturating_add(1)),
        std::io::sink(),
        stop,
    )?;
    identity.check(path, file)?;
    check_length(bytes, reference.bytes)?;
    if let Some(expected) = expected {
        if expected.sha256 != reference.sha256 {
            return Err(ObjectFileError::DigestMismatch {
                path: path.to_path_buf(),
                expected: expected.sha256.clone(),
                actual: reference.sha256,
            });
        }
    }
    Ok((reference, identity))
}

/// Open a physical regular file without reading its contents or changing its
/// metadata. Its parent must be a real directory, and the opened file must
/// still match the named entry. `expected_bytes`, when known, checks exact
/// length, not the body's integrity or serving authority.
/// Callers resolve logical paths and enforce publication membership first;
/// publisher-owned ancestors and cooperative immutable writers are assumed.
pub fn open_object_file(path: &Path, expected_bytes: Option<u64>) -> Result<File, ObjectFileError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if !fs::symlink_metadata(parent)?.is_dir() {
        return Err(ObjectFileError::NotDirectory(parent.to_path_buf()));
    }
    let named = fs::symlink_metadata(path)?;
    let identity = FileIdentity::from_metadata(&named)
        .ok_or_else(|| ObjectFileError::NotRegularFile(path.to_path_buf()))?;
    if let Some(expected) = expected_bytes {
        check_length(expected, named.len())?;
    }
    let file = File::open(path)?;
    identity.check(path, &file)?;
    if !fs::symlink_metadata(parent)?.is_dir() {
        return Err(ObjectFileError::NotDirectory(parent.to_path_buf()));
    }
    Ok(file)
}

fn check_length(expected: u64, actual: u64) -> Result<(), ObjectFileError> {
    if expected != actual {
        return Err(ObjectFileError::LengthMismatch { expected, actual });
    }
    Ok(())
}

/// The same bounded loop identifies newly written streams and existing files.
pub(crate) fn copy_and_hash(
    input: &mut impl Read,
    mut output: impl Write,
    stop: &dyn Fn() -> bool,
) -> Result<ObjectReference, ObjectFileError> {
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

pub(crate) fn check_stop(stop: &dyn Fn() -> bool) -> Result<(), ObjectFileError> {
    if stop() {
        Err(ObjectFileError::Cancelled)
    } else {
        Ok(())
    }
}
