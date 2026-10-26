//! A publisher's local record of preparation it finished, kept apart from
//! content checks made by the running process.
use crate::FileIdentity;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::File, path::Path, time::SystemTime};

/// The digest, length and identity of each sealed file a publisher finished
/// preparing for one completed publication, which the product names by the
/// digest of its own completion record.
///
/// The record grants no serving authority and never proves a hash made by this
/// process. A restarted publisher may list a file it names without reading it
/// only while the file keeps its recorded identity, and must then hash and
/// check the bytes again in the background, withdrawing what lists them on a
/// mismatch. A record that disagrees with the current publication is invalid,
/// and the caller verifies everything instead.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationRecord {
    semantics: String,
    completion_sha256: String,
    files: BTreeMap<String, (String, u64, FileIdentity)>,
}

impl PreparationRecord {
    /// An empty record of `semantics`, the product's record version, for the
    /// completion whose digest is `completion_sha256`.
    pub fn new(semantics: &str, completion_sha256: String) -> Self {
        Self {
            semantics: semantics.into(),
            completion_sha256,
            files: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, path: String, sha256: String, bytes: u64, identity: FileIdentity) {
        self.files.insert(path, (sha256, bytes, identity));
    }

    /// Each recorded path's digest, length and identity.
    pub fn files(&self) -> &BTreeMap<String, (String, u64, FileIdentity)> {
        &self.files
    }

    /// Whether `saved` describes this record's publication: the same semantics
    /// and completion, and the same digest and length for exactly these paths.
    /// Identities may differ; [`Self::adopts`] decides each file.
    pub fn agrees(&self, saved: &Self) -> bool {
        saved.semantics == self.semantics
            && saved.completion_sha256 == self.completion_sha256
            && saved.files.len() == self.files.len()
            && self.files.iter().all(|(path, (sha256, bytes, _))| {
                saved
                    .files
                    .get(path)
                    .is_some_and(|(s, b, _)| s == sha256 && b == bytes)
            })
    }

    /// Whether `path`, now observed with `identity`, may be listed from this
    /// saved record without being read: it keeps its recorded identity, and
    /// that identity settled before `now`.
    pub fn adopts(&self, path: &str, identity: FileIdentity, now: SystemTime) -> bool {
        identity.settled_before(now)
            && self
                .files
                .get(path)
                .is_some_and(|(_, _, saved)| *saved == identity)
    }
}

/// Whether `physical`, opened as `file`, is a sealed object: a read-only file
/// directly in the publisher's object `store`. Only these may be recorded.
pub fn is_sealed_object(store: &Path, physical: &Path, file: &File) -> std::io::Result<bool> {
    Ok(physical.parent() == Some(store) && file.metadata()?.permissions().readonly())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn sealed(dir: &Path, name: &str, bytes: &[u8]) -> (std::path::PathBuf, FileIdentity) {
        let path = dir.join(name);
        File::create(&path).unwrap().write_all(bytes).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&path, permissions).unwrap();
        let identity = FileIdentity::capture(&path, &File::open(&path).unwrap()).unwrap();
        (path, identity)
    }

    fn later() -> SystemTime {
        SystemTime::now() + std::time::Duration::from_secs(5)
    }

    #[test]
    fn a_saved_record_adopts_only_unchanged_settled_files_of_its_completion() {
        let dir = tempfile::tempdir().unwrap();
        let (_, identity) = sealed(dir.path(), "a", b"bytes");
        let mut record = PreparationRecord::new("test-v1", "c".repeat(64));
        record.insert("a".into(), "d".repeat(64), 5, identity);
        // Publishers already hold records in this exact shape.
        let json = serde_json::to_value(&record).unwrap();
        let keys: Vec<_> = json.as_object().unwrap().keys().cloned().collect();
        assert_eq!(keys, ["completion_sha256", "files", "semantics"]);
        let saved: PreparationRecord = serde_json::from_value(json).unwrap();
        assert_eq!(saved, record);
        assert!(record.agrees(&saved));
        assert!(saved.adopts("a", identity, later()));
        // Too recent to tell apart from a write in the same timestamp tick.
        assert!(!saved.adopts("a", identity, SystemTime::now()));
        assert!(!saved.adopts("b", identity, later()));

        let (_, other) = sealed(dir.path(), "b", b"bytes");
        assert!(!saved.adopts("a", other, later()));
        let mut changed = record.clone();
        changed.insert("a".into(), "e".repeat(64), 5, identity);
        assert!(!changed.agrees(&saved));
        let mut extra = record.clone();
        extra.insert("b".into(), "d".repeat(64), 5, other);
        assert!(!extra.agrees(&saved));
        let elsewhere = PreparationRecord::new("test-v1", "f".repeat(64));
        assert!(!elsewhere.agrees(&PreparationRecord::new("test-v1", "c".repeat(64))));
        let version = PreparationRecord::new("test-v2", "c".repeat(64));
        assert!(!version.agrees(&PreparationRecord::new("test-v1", "c".repeat(64))));
    }

    #[test]
    fn only_read_only_files_directly_in_the_store_are_sealed() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("objects");
        std::fs::create_dir(&store).unwrap();
        let (path, _) = sealed(&store, "a", b"bytes");
        assert!(is_sealed_object(&store, &path, &File::open(&path).unwrap()).unwrap());
        let (outside, _) = sealed(dir.path(), "b", b"bytes");
        assert!(!is_sealed_object(&store, &outside, &File::open(&outside).unwrap()).unwrap());
        let writable = store.join("c");
        File::create(&writable).unwrap();
        assert!(!is_sealed_object(&store, &writable, &File::open(&writable).unwrap()).unwrap());
    }
}
