//! Durable JSON records written so that a crash leaves either the old record
//! or the new one.

use serde::Serialize;
use std::io::Write;
use std::path::Path;

/// Writes `value` as JSON to `path` so that a crash leaves either the old
/// record or the new one: written beside it, synced, renamed over it, and the
/// directory synced so the rename itself survives.
pub(crate) fn persist(path: &Path, value: &impl Serialize) -> Result<(), String> {
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
    /// let refused = pir_query_server::transparent::test_support::record_writer(record, |_, _| {
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
