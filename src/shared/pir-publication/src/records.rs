//! Durable JSON records written so that a crash leaves either the old record
//! or the new one.

use serde::Serialize;
use std::io::Write;
use std::path::Path;

/// Writes `value` as JSON to `path` so that a crash leaves either the old
/// record or the new one: written beside it, synced, renamed over it, and the
/// directory synced so the rename itself survives.
pub fn persist(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let parent = path.parent().ok_or("record needs a parent directory")?;
    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    #[cfg(feature = "test-support")]
    if let Some(result) = test_support::write(path, &bytes) {
        return result.map_err(|error| error.to_string());
    }
    pir_control::durable::replace_durable(path, |file| file.write_all(&bytes))
        .map_err(|error| error.to_string())
}

/// Scoped record writers for integration tests. Callbacks run on the same
/// blocking worker as the ordinary durable write; each exact path is isolated
/// from concurrent tests. Service builds do not include this module.
#[cfg(feature = "test-support")]
pub mod test_support {
    use std::{
        collections::HashMap,
        io,
        path::{Path, PathBuf},
        sync::{Arc, Mutex, OnceLock},
    };

    type Writer = dyn Fn(&Path, &[u8]) -> io::Result<()> + Send + Sync;

    fn writers() -> &'static Mutex<HashMap<PathBuf, Arc<Writer>>> {
        static WRITERS: OnceLock<Mutex<HashMap<PathBuf, Arc<Writer>>>> = OnceLock::new();
        WRITERS.get_or_init(Mutex::default)
    }

    /// Restores ordinary writes on drop. An already running callback retains
    /// its ownership until it returns, just like an ordinary blocking write.
    pub struct RecordWriter {
        path: PathBuf,
    }

    /// Replace writes to one exact record path until the returned guard drops.
    /// The callback owns persistence: it must save the supplied bytes itself
    /// when simulating a successful write or an error after replacement.
    /// Panics if another guard already owns the path.
    ///
    /// ```no_run
    /// # use std::path::Path;
    /// let record = Path::new("/tmp/test-service/active.json");
    /// let refused = pir_publication::records::test_support::record_writer(record, |_, _| {
    ///     Err(std::io::Error::other("record write refused"))
    /// });
    /// // Exercise the service while writes to this record are refused.
    /// drop(refused); // Subsequent writes use the ordinary durable writer.
    /// ```
    pub fn record_writer(
        path: &Path,
        write: impl Fn(&Path, &[u8]) -> io::Result<()> + Send + Sync + 'static,
    ) -> RecordWriter {
        let mut writers = writers().lock().unwrap();
        assert!(
            !writers.contains_key(path),
            "record already has a test writer"
        );
        writers.insert(path.to_path_buf(), Arc::new(write));
        RecordWriter { path: path.into() }
    }

    impl Drop for RecordWriter {
        fn drop(&mut self) {
            writers().lock().unwrap().remove(&self.path);
        }
    }

    pub(super) fn write(path: &Path, bytes: &[u8]) -> Option<io::Result<()>> {
        let writer = writers().lock().unwrap().get(path).cloned()?;
        Some(writer(path, bytes))
    }
}

/// Read one local record or artifact with the product's explicit byte budget.
/// Opening follows the caller's existing path policy; this creates no digest
/// evidence and never grants permission to serve.
pub fn read_bounded(path: &Path, limit: u64) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(std::io::Error::other(format!(
            "{} is larger than {limit} bytes",
            path.display()
        )));
    }
    Ok(bytes)
}
/// Read a fixed-length product artifact without adding a second digest pass.
pub fn read_exact_len(path: &Path, len: u64) -> std::io::Result<Vec<u8>> {
    let bytes = read_bounded(path, len)?;
    if bytes.len() as u64 != len {
        return Err(std::io::Error::other(format!(
            "{} is {} bytes, the manifest says {len}",
            path.display(),
            bytes.len()
        )));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn local_reads_enforce_the_record_budget_and_manifest_length() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("local-record");
        std::fs::write(&path, b"abcd").unwrap();
        assert_eq!(read_bounded(&path, 4).unwrap(), b"abcd");
        assert!(read_bounded(&path, 3).is_err());
        assert_eq!(read_exact_len(&path, 4).unwrap(), b"abcd");
        assert!(read_exact_len(&path, 5).is_err());
        assert!(read_exact_len(&path, 3).is_err());
        std::fs::write(&path, []).unwrap();
        assert!(read_exact_len(&path, 0).unwrap().is_empty());
        assert!(read_bounded(&directory.path().join("missing"), 4).is_err());
    }
}
