//! Checksums of persisted artifact bytes, independent of protocol commitments.
//!
//! Products own authority, serialization and activation. A checksum establishes
//! byte consistency only. Legacy algorithms are supported for explicit offline
//! conversion; serving accepts only the current algorithm.
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use std::io::{self, Read, Write};

pub mod catalog;
pub mod migration;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Algorithm {
    Sha256,
    Blake3,
}

/// The sole selection point for new artifact checksums.
pub const CURRENT: Algorithm = Algorithm::Blake3;

/// Keep predecessor objects available for rollback without allowing a new
/// algorithm to reuse an implicitly identified cache entry.
pub fn namespace(kind: &str) -> String {
    format!("{kind}/{}", CURRENT.name())
}

impl Algorithm {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::Blake3 => "blake3",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checksum {
    pub algorithm: Algorithm,
    pub digest: String,
}

impl Checksum {
    pub fn validate(&self) -> io::Result<()> {
        if !valid_digest(&self.digest) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid artifact checksum",
            ));
        }
        Ok(())
    }

    pub fn require_current(&self) -> io::Result<()> {
        self.validate()?;
        if self.algorithm != CURRENT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "artifact checksum algorithm differs",
            ));
        }
        Ok(())
    }

    /// Algorithm-qualified filename or single URL path component.
    pub fn key(&self) -> io::Result<String> {
        self.validate()?;
        Ok(format!("{}-{}", self.algorithm.name(), self.digest))
    }

    pub fn verify(&self, bytes: &[u8]) -> io::Result<()> {
        self.validate()?;
        let mut hash = Hasher::for_algorithm(self.algorithm);
        hash.update(bytes);
        if hash.finish() != *self {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "artifact checksum differs",
            ));
        }
        Ok(())
    }
}

pub fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub fn checksum(bytes: &[u8]) -> Checksum {
    let mut hash = Hasher::new();
    hash.update(bytes);
    hash.finish()
}

/// Current checksum's hexadecimal digest for existing internal indexes. Wire
/// values use `Checksum` or `current` below so the algorithm is never implicit.
pub fn digest(bytes: &[u8]) -> String {
    checksum(bytes).digest
}

#[derive(Clone)]
enum State {
    Sha256(sha2::Sha256),
    Blake3(Box<blake3::Hasher>),
}

#[derive(Clone)]
pub struct Hasher(State);

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher {
    pub fn new() -> Self {
        Self::for_algorithm(CURRENT)
    }

    pub fn for_algorithm(algorithm: Algorithm) -> Self {
        Self(match algorithm {
            Algorithm::Sha256 => State::Sha256(sha2::Sha256::new()),
            Algorithm::Blake3 => State::Blake3(Box::new(blake3::Hasher::new())),
        })
    }

    pub fn update(&mut self, bytes: impl AsRef<[u8]>) {
        match &mut self.0 {
            State::Sha256(hash) => hash.update(bytes.as_ref()),
            State::Blake3(hash) => {
                hash.update(bytes.as_ref());
            }
        }
    }

    pub fn finalize(self) -> [u8; 32] {
        match self.0 {
            State::Sha256(hash) => hash.finalize().into(),
            State::Blake3(hash) => *hash.finalize().as_bytes(),
        }
    }

    pub fn finish(self) -> Checksum {
        let algorithm = match &self.0 {
            State::Sha256(_) => Algorithm::Sha256,
            State::Blake3(_) => Algorithm::Blake3,
        };
        Checksum {
            algorithm,
            digest: hex::encode(self.finalize()),
        }
    }
}

/// Bounded reads suitable for a caller-controlled scheduling policy.
pub fn hash_reader(mut reader: impl Read) -> io::Result<(Checksum, u64)> {
    let mut hash = Hasher::new();
    let mut buffer = vec![0; 1 << 20];
    let mut length = 0u64;
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        length = length
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::other("artifact length overflow"))?;
        hash.update(&buffer[..count]);
    }
    Ok((hash.finish(), length))
}

/// Write and hash the same persisted bytes. The caller owns the parent-directory
/// durability barrier and publication of metadata referring to the completed file.
pub fn write_atomic_with(
    directory: &std::path::Path,
    name: &str,
    synced: impl FnOnce(&std::fs::File),
    write: impl FnOnce(&mut dyn Write) -> io::Result<()>,
) -> io::Result<String> {
    struct Writer<W> {
        inner: W,
        hash: Hasher,
    }
    impl<W: Write> Write for Writer<W> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let count = self.inner.write(bytes)?;
            self.hash.update(&bytes[..count]);
            Ok(count)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }
    if name.is_empty() || name.contains('/') || name == "." || name == ".." {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid artifact filename",
        ));
    }
    let temporary = tempfile::NamedTempFile::new_in(directory)?;
    let mut writer = std::io::BufWriter::with_capacity(
        1 << 20,
        Writer {
            inner: temporary.as_file(),
            hash: Hasher::new(),
        },
    );
    write(&mut writer)?;
    writer.flush()?;
    temporary.as_file().sync_all()?;
    synced(temporary.as_file());
    let digest = hex::encode(writer.get_ref().hash.clone().finalize());
    drop(writer);
    temporary
        .persist(directory.join(name))
        .map_err(|error| error.error)?;
    Ok(digest)
}

/// Tagged wire encoding for product indexes that retain hexadecimal strings
/// internally. Deserialization never accepts a legacy digest implicitly.
pub mod current {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(digest: &str, serializer: S) -> Result<S::Ok, S::Error> {
        let value = Checksum {
            algorithm: CURRENT,
            digest: digest.into(),
        };
        value.require_current().map_err(serde::ser::Error::custom)?;
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
        let value = Checksum::deserialize(deserializer)?;
        value.require_current().map_err(serde::de::Error::custom)?;
        Ok(value.digest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn official_empty_and_abc_vectors_and_chunking() {
        assert_eq!(
            digest(b""),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
        assert_eq!(
            digest(b"abc"),
            "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
        );
        let mut hash = Hasher::new();
        for byte in b"abc" {
            hash.update([*byte]);
        }
        assert_eq!(hash.finish(), checksum(b"abc"));
    }

    #[test]
    fn corrupted_bytes_and_legacy_algorithms_are_refused_by_serving() {
        assert!(checksum(b"abc").verify(b"abd").is_err());
        let mut legacy = Hasher::for_algorithm(Algorithm::Sha256);
        legacy.update(b"abc");
        let value = legacy.finish();
        assert_eq!(
            value.digest,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        value.verify(b"abc").unwrap();
        assert!(value.require_current().is_err());
    }

    #[test]
    fn wire_values_are_tagged_and_algorithm_qualified() {
        #[derive(Serialize, Deserialize)]
        struct Record {
            #[serde(with = "current")]
            checksum: String,
        }
        let record = Record {
            checksum: digest(b"abc"),
        };
        let encoded = serde_json::to_value(&record).unwrap();
        assert_eq!(encoded["checksum"]["algorithm"], "blake3");
        assert!(
            serde_json::from_value::<Record>(serde_json::json!({"checksum": record.checksum}))
                .is_err()
        );
        assert!(serde_json::from_value::<Record>(
            serde_json::json!({"checksum": {"algorithm": "sha256", "digest": digest(b"abc")}})
        )
        .is_err());
        assert!(checksum(b"abc").key().unwrap().starts_with("blake3-"));
    }
}
