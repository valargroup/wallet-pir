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
        if expected.bytes != bytes {
            return Err(ObjectFileError::LengthMismatch {
                expected: expected.bytes,
                actual: bytes,
            });
        }
    }
    file.rewind()?;
    let reference = copy_and_hash(
        &mut (&mut *file).take(bytes.saturating_add(1)),
        std::io::sink(),
        stop,
    )?;
    identity.check(path, file)?;
    if reference.bytes != bytes {
        return Err(ObjectFileError::LengthMismatch {
            expected: bytes,
            actual: reference.bytes,
        });
    }
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

pub(crate) fn open_regular(path: &Path) -> Result<File, ObjectFileError> {
    let named = fs::symlink_metadata(path)?;
    let identity = FileIdentity::from_metadata(&named)
        .ok_or_else(|| ObjectFileError::NotRegularFile(path.to_path_buf()))?;
    let file = File::open(path)?;
    identity.check(path, &file)?;
    Ok(file)
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
