//! Large public artifacts are consumed into resident runtime state. Keeping an
//! extra kernel file-cache copy competes with query/build admission under the
//! worker cgroup. Advise away only files we have finished consuming, or snapshots
//! whose durability barrier has completed. This never substitutes for integrity
//! checks or changes the admission ceiling. The kernel may ignore the advice.
//!
//! Linux cache advice can itself block while submitting dirty-page writes. Keep
//! that optional work off publication loading and runtime construction. One
//! background worker holds at most one active file and four queued descriptors;
//! saturation skips advice rather than delaying readiness or accumulating work.

pub(crate) fn consumed(file: &std::fs::File) {
    #[cfg(target_os = "linux")]
    {
        static QUEUE: std::sync::OnceLock<Option<queue::AdviceQueue>> = std::sync::OnceLock::new();
        let queue = QUEUE.get_or_init(|| match queue::AdviceQueue::new(4, advise) {
            Ok(queue) => Some(queue),
            Err(error) => {
                tracing::warn!(%error, "public artifact cache advice worker unavailable");
                None
            }
        });
        if let Some(queue) = queue {
            match queue.submit(file) {
                Ok(true) => {}
                Ok(false) => tracing::debug!("public artifact cache advice queue unavailable"),
                Err(error) => {
                    tracing::debug!(%error, "public artifact cache advice descriptor unavailable")
                }
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = file;
}

#[cfg(target_os = "linux")]
fn advise(file: std::fs::File) {
    let started = std::time::Instant::now();
    if let Err(error) = rustix::fs::fadvise(&file, 0, None, rustix::fs::Advice::DontNeed) {
        tracing::debug!(%error, "public artifact cache advice unavailable");
    }
    let elapsed = started.elapsed();
    if elapsed >= std::time::Duration::from_millis(100) {
        use std::os::fd::AsRawFd;
        tracing::warn!(
            fd = file.as_raw_fd(),
            seconds = elapsed.as_secs_f64(),
            "public artifact cache advice delayed"
        );
    }
}

#[cfg(any(target_os = "linux", test))]
mod queue {
    use std::{fs::File, io, sync::mpsc};

    pub(super) struct AdviceQueue {
        sender: mpsc::SyncSender<File>,
    }

    impl AdviceQueue {
        pub(super) fn new(
            capacity: usize,
            mut action: impl FnMut(File) + Send + 'static,
        ) -> io::Result<Self> {
            let (sender, receiver) = mpsc::sync_channel(capacity);
            std::thread::Builder::new()
                .name("artifact-cache-advice".into())
                .spawn(move || {
                    for file in receiver {
                        action(file);
                    }
                })?;
            Ok(Self { sender })
        }

        // Clone the descriptor, not the path: renaming or removing an artifact
        // after submission must not redirect advice to a replacement file.
        // try_send never waits for the worker or for space in the queue.
        pub(super) fn submit(&self, file: &File) -> io::Result<bool> {
            Ok(self.sender.try_send(file.try_clone()?).is_ok())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn blocked_advice_does_not_block_consumers_or_grow_queue() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let mut first = true;
        let queue = super::queue::AdviceQueue::new(2, move |_| {
            if first {
                first = false;
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            finished_tx.send(()).unwrap();
        })
        .unwrap();
        let file = tempfile::tempfile().unwrap();
        assert!(queue.submit(&file).unwrap());
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (submitted_tx, submitted_rx) = mpsc::channel();
        let producer = std::thread::spawn(move || {
            let accepted = (0..3)
                .map(|_| queue.submit(&file).unwrap())
                .collect::<Vec<_>>();
            submitted_tx.send(accepted).unwrap();
        });
        // The advice worker remains held until after the producer completes.
        let result = submitted_rx.recv_timeout(Duration::from_secs(2));
        release_tx.send(()).unwrap();
        assert_eq!(result.unwrap(), vec![true, true, false]);
        producer.join().unwrap();
        for _ in 0..3 {
            finished_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        assert!(finished_rx.recv_timeout(Duration::from_secs(2)).is_err());
    }

    #[test]
    fn queued_descriptor_survives_source_removal_and_replacement() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (bytes_tx, bytes_rx) = mpsc::channel();
        let queue = super::queue::AdviceQueue::new(1, move |mut file| {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            bytes_tx.send(bytes).unwrap();
        })
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("artifact");
        std::fs::write(&path, b"original").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        assert!(queue.submit(&file).unwrap());
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        drop(file);
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        release_tx.send(()).unwrap();
        assert_eq!(
            bytes_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            b"original"
        );
    }

    #[test]
    fn advice_preserves_contents_and_file_position() {
        let mut file = tempfile::tempfile().unwrap();
        let bytes: Vec<u8> = (0..16387).map(|i| (i % 251) as u8).collect();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        file.seek(SeekFrom::Start(37)).unwrap();
        #[cfg(target_os = "linux")]
        super::advise(file.try_clone().unwrap());
        #[cfg(not(target_os = "linux"))]
        super::consumed(&file);
        assert_eq!(file.stream_position().unwrap(), 37);
        file.rewind().unwrap();
        let mut actual = Vec::new();
        file.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, bytes);
    }
}
