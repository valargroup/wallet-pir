//! One rule for reusing whole-file integrity evidence across publishers.
use crate::object_file::check_stop;
use crate::{hash_object_file, FileIdentity, ObjectFileError, ObjectReference};
use std::{fs::File, path::Path, time::SystemTime};

/// Integrity of one exact file, and whether this pass read its body. Semantic
/// preparation evidence remains with the product that checked the source and
/// runtime parameters; this result alone never authorizes publication.
pub struct ObjectVerification {
    pub reference: ObjectReference,
    pub identity: FileIdentity,
    pub reused: bool,
    pub reusable: bool,
}

/// Verify `file` at its checked physical path. `prior` must come from a
/// successful, settled verification in this process under the same semantic
/// context. Changed files are hashed again; changes during either checking or
/// hashing fail the attempt. Fresh files are accepted without reusable evidence.
pub fn verify_object_file(
    path: &Path,
    file: &mut File,
    expected: Option<&ObjectReference>,
    prior: Option<(&ObjectReference, FileIdentity)>,
    stop: &dyn Fn() -> bool,
) -> Result<ObjectVerification, ObjectFileError> {
    if expected.is_some_and(|object| !crate::is_object_digest(&object.sha256))
        || prior.is_some_and(|(object, _)| !crate::is_object_digest(&object.sha256))
    {
        return Err(ObjectFileError::InvalidDigest);
    }
    check_stop(stop)?;
    let started = SystemTime::now();
    let identity = FileIdentity::capture(path, file)?;
    if let Some((reference, previous)) = prior {
        if previous == identity
            && expected.is_none_or(|expected| expected == reference)
            && file.metadata()?.len() == reference.bytes
        {
            check_stop(stop)?;
            identity.check(path, file)?;
            return Ok(ObjectVerification {
                reference: reference.clone(),
                identity,
                reused: true,
                reusable: true,
            });
        }
    }
    let (reference, identity) = hash_object_file(path, file, expected, stop)?;
    Ok(ObjectVerification {
        reference,
        identity,
        reused: false,
        reusable: identity.settled_before(started),
    })
}
