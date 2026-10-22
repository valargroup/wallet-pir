//! Content-addressed object identities.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

/// Distribution path, relative to the product prefix, below which every object
/// is served by its lowercase SHA-256 name.
pub const OBJECTS_PATH: &str = "objects/";

/// Lowercase hexadecimal SHA-256 of `bytes`.
pub fn object_digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Whether `name` is exactly a lowercase SHA-256 hex digest, and therefore a
/// safe file name that cannot escape an object directory.
pub fn is_object_digest(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// One immutable object: its SHA-256 and exact length. A reference binds bytes,
/// never the chain or product truth those bytes describe.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct ObjectReference {
    pub sha256: String,
    pub bytes: u64,
}

/// Why an object reference or its bytes were refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectReferenceError {
    /// The digest is not lowercase SHA-256 hex, or the length is zero or above
    /// the caller's bound. Nothing was read.
    MalformedReference,
    /// The bytes have a different length than the reference declares.
    LengthDiffers { expected: u64, actual: u64 },
    /// The bytes have the declared length but a different digest.
    DigestDiffers,
}
impl fmt::Display for ObjectReferenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedReference => f.write_str("malformed object reference or size"),
            Self::LengthDiffers { expected, actual } => {
                write!(f, "object length {actual} differs from declared {expected}")
            }
            Self::DigestDiffers => f.write_str("object digest differs"),
        }
    }
}
impl std::error::Error for ObjectReferenceError {}

impl ObjectReference {
    /// The reference that names exactly `bytes`.
    pub fn for_bytes(bytes: &[u8]) -> Self {
        Self {
            sha256: object_digest(bytes),
            bytes: bytes.len() as u64,
        }
    }
    /// Check the reference shape and that it declares between one and
    /// `max_bytes` bytes, before any request or file access uses it.
    pub fn validate(&self, max_bytes: u64) -> Result<(), ObjectReferenceError> {
        if !is_object_digest(&self.sha256) || self.bytes == 0 || self.bytes > max_bytes {
            return Err(ObjectReferenceError::MalformedReference);
        }
        Ok(())
    }
    /// Check that `bytes` are exactly the referenced object.
    pub fn verify(&self, bytes: &[u8]) -> Result<(), ObjectReferenceError> {
        if bytes.len() as u64 != self.bytes {
            return Err(ObjectReferenceError::LengthDiffers {
                expected: self.bytes,
                actual: bytes.len() as u64,
            });
        }
        if object_digest(bytes) != self.sha256 {
            return Err(ObjectReferenceError::DigestDiffers);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_addresses_and_lengths_are_checked_independently() {
        let object = ObjectReference::for_bytes(b"material");
        assert!(object.verify(b"material").is_ok());
        assert_eq!(
            object.verify(b"materiaL"),
            Err(ObjectReferenceError::DigestDiffers)
        );
        assert!(matches!(
            object.verify(b"materialx"),
            Err(ObjectReferenceError::LengthDiffers { .. })
        ));
        assert!(object.validate(7).is_err());
        assert!(!is_object_digest("../objects"));
        let mut uppercase = object;
        uppercase.sha256 = uppercase.sha256.to_uppercase();
        assert!(uppercase.validate(100).is_err());
    }
}
