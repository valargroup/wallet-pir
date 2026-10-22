//! A publisher's distributed objects and the retention that authorizes serving them.
use crate::{is_object_digest, open_object_file, ObjectInstaller, ObjectReference};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs, io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::RwLock,
};

const RETENTION_RECORD: &str = "retained-publications.json";

/// Objects of one publication and the time after which no consumer may still
/// activate it. The newest retained publication is kept even after that time.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedPublication {
    objects: BTreeSet<String>,
    activatable_until_ms: u64,
}

/// Immutable, durable, content-addressed objects below one directory. Only
/// objects of retained publications are served; everything else is refused
/// before the file is opened and is removed by the next retention change.
pub struct PublishedObjectStore {
    installer: ObjectInstaller,
    retained: RwLock<Vec<RetainedPublication>>,
}

impl PublishedObjectStore {
    /// Open `directory`, restoring the durable retention record.
    pub fn open(directory: PathBuf) -> io::Result<Self> {
        let installer = ObjectInstaller::open(directory.clone()).map_err(io::Error::other)?;
        let retained = match fs::read(directory.join(RETENTION_RECORD)) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error),
        };
        Ok(Self {
            installer,
            retained: RwLock::new(retained),
        })
    }
    /// Store `bytes` durably and read-only under their digest. Existing bytes
    /// under that name are verified rather than rewritten. Storing does not
    /// authorize serving.
    pub fn store(&self, bytes: &[u8]) -> io::Result<ObjectReference> {
        self.installer
            .install_stream(bytes, &|| false)
            .map(|installed| installed.reference)
            .map_err(io::Error::other)
    }
    /// Authorize serving `objects` until `activatable_until_ms`, drop
    /// predecessors no consumer can still activate at `now_ms`, persist the
    /// record, then remove objects no retained publication references.
    /// Call before publishing a pointer that references `objects`.
    pub fn retain_publication(
        &self,
        objects: impl IntoIterator<Item = ObjectReference>,
        activatable_until_ms: u64,
        now_ms: u64,
    ) -> io::Result<()> {
        let objects: BTreeSet<String> = objects.into_iter().map(|object| object.sha256).collect();
        if !objects.iter().all(|name| is_object_digest(name)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "retained object reference is not a SHA-256 digest",
            ));
        }
        let next = RetainedPublication {
            objects,
            activatable_until_ms,
        };
        let mut retained = self.retained.write().unwrap();
        let mut updated: Vec<_> = retained
            .iter()
            .filter(|old| old.activatable_until_ms >= now_ms && old.objects != next.objects)
            .cloned()
            .collect();
        updated.push(next);
        write_retention_record(
            self.installer.directory(),
            RETENTION_RECORD,
            &serde_json::to_vec(&updated).map_err(io::Error::other)?,
        )?;
        *retained = updated;
        let referenced: BTreeSet<&String> = retained.iter().flat_map(|p| &p.objects).collect();
        for entry in fs::read_dir(self.installer.directory())? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if is_object_digest(&name)
                && entry.file_type()?.is_file()
                && !referenced.contains(&name)
            {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }
    /// Open a retained object for serving. A malformed name or an object no
    /// retained publication references is refused before any file access.
    pub fn open_retained(&self, name: &str) -> io::Result<fs::File> {
        if !is_object_digest(name) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not an object name",
            ));
        }
        let retained = self.retained.read().unwrap();
        if !retained.iter().any(|p| p.objects.contains(name)) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "object not retained",
            ));
        }
        open_object_file(&self.installer.directory().join(name), None).map_err(io::Error::from)
    }
}

/// Replace the mutable retention record, separately from object installation.
fn write_retention_record(directory: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    pir_control::durable::write_atomic(directory, name, |file| file.write_all(bytes))?;
    fs::set_permissions(directory.join(name), fs::Permissions::from_mode(0o444))?;
    pir_control::durable::sync_dir(directory)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_retained_objects_are_served_and_kept() {
        let dir = tempfile::tempdir().unwrap();
        let store = PublishedObjectStore::open(dir.path().into()).unwrap();
        let first = store.store(b"first").unwrap();
        let unpublished = store.store(b"prepared but never published").unwrap();
        assert!(store.open_retained(&first.sha256).is_err());
        store.retain_publication([first.clone()], 100, 50).unwrap();
        assert!(store.open_retained(&first.sha256).is_ok());
        assert!(!dir.path().join(&unpublished.sha256).exists());
        assert!(store
            .open_retained("../retained-publications.json")
            .is_err());

        // A still-activatable predecessor remains served for peer handover.
        let second = store.store(b"second").unwrap();
        store.retain_publication([second.clone()], 200, 90).unwrap();
        assert!(store.open_retained(&first.sha256).is_ok());

        // After restart the record still authorizes; expiry then collects it.
        let reopened = PublishedObjectStore::open(dir.path().into()).unwrap();
        assert!(reopened.open_retained(&first.sha256).is_ok());
        let third = reopened.store(b"third").unwrap();
        reopened.retain_publication([third], 300, 150).unwrap();
        assert!(reopened.open_retained(&first.sha256).is_err());
        assert!(!dir.path().join(&first.sha256).exists());
        assert!(reopened.open_retained(&second.sha256).is_ok());
    }
}
