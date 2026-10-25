//! Control the real service's record-write boundary without depending on the
//! durable helper's private temporary filenames. Every wait is bounded, and
//! dropping a blocked write releases its worker even if an assertion panics.
use pir_publication::records::test_support::{record_writer, RecordWriter};
use std::{io, io::Write, path::Path, sync::Mutex, time::Duration};

pub fn fail_before_replace(path: &Path) -> RecordWriter {
    record_writer(path, |_, _| Err(io::Error::other("record write refused")))
}

pub fn fail_after_replace(path: &Path) -> RecordWriter {
    record_writer(path, |path, bytes| {
        pir_control::durable::replace_durable(path, |file| file.write_all(bytes))?;
        Err(io::Error::other("record directory sync refused"))
    })
}

pub struct BlockedWrite {
    _writer: RecordWriter,
    entered: Option<tokio::sync::oneshot::Receiver<()>>,
    release: Option<std::sync::mpsc::Sender<()>>,
}

impl BlockedWrite {
    pub fn new(path: &Path) -> Self {
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let entered = Mutex::new(Some(entered));
        let released = Mutex::new(released);
        let writer = record_writer(path, move |path, bytes| {
            entered.lock().unwrap().take().unwrap().send(()).ok();
            released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .map_err(io::Error::other)?;
            pir_control::durable::replace_durable(path, |file| file.write_all(bytes))
        });
        Self {
            _writer: writer,
            entered: Some(entry),
            release: Some(release),
        }
    }

    pub async fn entered(&mut self) {
        tokio::time::timeout(Duration::from_secs(2), self.entered.take().unwrap())
            .await
            .expect("record writer did not start")
            .expect("record writer ended before reporting entry");
    }

    pub fn release(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

impl Drop for BlockedWrite {
    fn drop(&mut self) {
        self.release();
    }
}
