//! Local receipts for bytes checked while downloading from the publisher.
//!
//! Local storage is trusted: receipts and completed files are not protected
//! against same-size local corruption. Every reuse checks size and the opened
//! file's identity; no body is hashed again. Receipts preserve first-load
//! semantic checks across hard-linked publications and process restarts.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

// Older staging could attach a different object's identity to existing bytes.
// Neither its download identities nor semantic checks are reusable evidence.
const RECEIVED_VERSION: u32 = 2;
const CHECK_DOMAIN: &[u8] = b"wallet-pir-local-check-v2\0";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Received {
    version: u32,
    sha256: String,
    bytes: u64,
    object: PathBuf,
}

fn marker(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .expect("artifact has a filename")
        .to_os_string();
    name.push(".received.json");
    path.with_file_name(name)
}

fn read_received(path: &Path, file: &File) -> Option<Received> {
    use std::os::unix::fs::MetadataExt;
    let mut raw = Vec::new();
    File::open(marker(path))
        .ok()?
        .take(8193)
        .read_to_end(&mut raw)
        .ok()?;
    if raw.len() > 8192 {
        return None;
    }
    let received: Received = serde_json::from_slice(&raw).ok()?;
    let opened = file.metadata().ok()?;
    let named = std::fs::symlink_metadata(path).ok()?;
    if received.version != RECEIVED_VERSION
        || !crate::publication::is_digest(&received.sha256)
        || !opened.is_file()
        || !named.is_file()
        || opened.len() != received.bytes
        || (opened.dev(), opened.ino()) != (named.dev(), named.ino())
    {
        return None;
    }
    Some(received)
}

/// A completed download's recorded length remains authoritative on disk.
pub(crate) fn check_received_size(path: &Path, file: &File) -> io::Result<()> {
    let received = match File::open(marker(path)) {
        Ok(input) => serde_json::from_reader::<_, Received>(input.take(8193))?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() != received.bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "received artifact length mismatch",
        ));
    }
    Ok(())
}

pub(crate) fn open(path: &Path) -> io::Result<File> {
    if !std::fs::symlink_metadata(path)?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "artifact is not a regular file",
        ));
    }
    File::open(path)
}

fn persist(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "receipt has no parent"))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|e| e.error)?;
    File::open(parent)?.sync_all()
}

/// Called only by the receiving/staging code, after the network hash succeeds
/// or when reusing its completed digest-named cache object. Copies get their
/// own size check; all aliases share the original object's semantic receipts.
pub(crate) fn received(path: &Path, sha256: &str, bytes: u64, object: &Path) -> io::Result<()> {
    let file = open(path)?;
    if file.metadata()?.len() != bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "received artifact length mismatch",
        ));
    }
    let next = Received {
        version: RECEIVED_VERSION,
        sha256: sha256.into(),
        bytes,
        object: std::fs::canonicalize(object)?,
    };
    if read_received(path, &file).as_ref() == Some(&next) {
        return Ok(());
    }
    let raw = serde_json::to_vec(&next)?;
    persist(&marker(path), &raw)
}

pub(crate) fn matches_received(path: &Path, file: &File, sha256: &str, bytes: u64) -> bool {
    read_received(path, file).is_some_and(|r| r.sha256 == sha256 && r.bytes == bytes)
}

pub(crate) struct Receipt {
    path: PathBuf,
}
impl Receipt {
    pub(crate) fn new(path: &Path, file: &File, context: &[u8]) -> Option<Self> {
        let received = read_received(path, file)?;
        let mut hash = Sha256::new();
        hash.update(CHECK_DOMAIN);
        hash.update(received.sha256.as_bytes());
        hash.update(received.bytes.to_le_bytes());
        hash.update(context);
        let mut name = received.object.file_name()?.to_os_string();
        name.push(format!(".checked-{}", hex::encode(hash.finalize())));
        Some(Self {
            path: received.object.with_file_name(name),
        })
    }
    pub(crate) fn matches(&self) -> bool {
        let Ok(file) = File::open(&self.path) else {
            return false;
        };
        let mut raw = Vec::new();
        file.take(11).read_to_end(&mut raw).is_ok() && raw == b"checked-v1"
    }
    /// Save only after this complete semantic check succeeds. A failed write
    /// merely means the next load repeats the check.
    pub(crate) fn record(&self) {
        if !self.matches() {
            if let Err(error) = persist(&self.path, b"checked-v1") {
                tracing::debug!(%error, "local check receipt not retained");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_download_and_semantic_receipts_are_not_reused() {
        let dir = tempfile::tempdir().unwrap();
        let object = dir.path().join("object");
        std::fs::write(&object, b"network bytes").unwrap();
        let sha = crate::publication::digest(b"network bytes");
        let context = b"source-and-parameters";
        let legacy = Received {
            version: 1,
            sha256: sha.clone(),
            bytes: 13,
            object: object.clone(),
        };
        persist(&marker(&object), &serde_json::to_vec(&legacy).unwrap()).unwrap();
        let mut hash = Sha256::new();
        hash.update(b"wallet-pir-local-check-v1\0");
        hash.update(sha.as_bytes());
        hash.update(13u64.to_le_bytes());
        hash.update(context);
        let legacy_check =
            object.with_file_name(format!("object.checked-{}", hex::encode(hash.finalize())));
        persist(&legacy_check, b"checked-v1").unwrap();
        let file = open(&object).unwrap();
        assert!(!matches_received(&object, &file, &sha, 13));
        assert!(Receipt::new(&object, &file, context).is_none());

        received(&object, &sha, 13, &object).unwrap();
        let check = Receipt::new(&object, &file, context).unwrap();
        assert!(!check.matches(), "an old semantic receipt is not evidence");
        check.record();
        assert!(Receipt::new(&object, &file, context).unwrap().matches());
    }

    #[test]
    fn received_files_reuse_checks_after_reopen_but_size_and_context_must_match() {
        let dir = tempfile::tempdir().unwrap();
        let object = dir.path().join("object");
        let linked = dir.path().join("linked");
        std::fs::write(&object, b"network bytes").unwrap();
        let sha = crate::publication::digest(b"network bytes");
        received(&object, &sha, 13, &object).unwrap();
        let file = open(&object).unwrap();
        assert!(matches_received(&object, &file, &sha, 13));
        let check = Receipt::new(&object, &file, b"source-and-parameters").unwrap();
        assert!(!check.matches());
        check.record();
        drop(file);
        std::fs::hard_link(&object, &linked).unwrap();
        received(&linked, &sha, 13, &object).unwrap();
        let file = open(&linked).unwrap();
        assert!(Receipt::new(&linked, &file, b"source-and-parameters")
            .unwrap()
            .matches());
        assert!(!Receipt::new(&linked, &file, b"other-parameters")
            .unwrap()
            .matches());
        assert!(!matches_received(&linked, &file, &"00".repeat(32), 13));
        std::fs::write(&object, b"same size!!!!").unwrap();
        assert!(
            matches_received(&linked, &file, &sha, 13),
            "same-size local writes are outside the network integrity boundary"
        );
        std::fs::write(&object, b"short").unwrap();
        assert!(!matches_received(&linked, &file, &sha, 13));
    }

    #[test]
    fn partial_missing_and_unmarked_files_do_not_supply_check_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("object");
        std::fs::write(&path, b"data").unwrap();
        assert!(Receipt::new(&path, &open(&path).unwrap(), b"check").is_none());
        std::fs::write(marker(&path), b"{broken").unwrap();
        assert!(Receipt::new(&path, &open(&path).unwrap(), b"check").is_none());
        std::fs::remove_file(&path).unwrap();
        assert!(open(&path).is_err());
    }
}
