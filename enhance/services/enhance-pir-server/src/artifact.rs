//! Bounded artifact I/O. Open handles pin immutable, atomically replaced files.
use axum::body::Bytes;
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub const IO_BUFFER_BYTES: usize = 64 * 1024;
const STREAM_QUEUE_CHUNKS: usize = 2;

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Small publication reference; no decoded or encoded CRS is retained here.
#[derive(Clone)]
pub struct PublicationArtifact {
    file: Arc<File>,
    length: u64,
    digest: String,
    failed: Arc<AtomicBool>,
}

impl PublicationArtifact {
    pub(crate) fn open(path: &Path, length: u64, digest: String) -> io::Result<Self> {
        let file = File::open(path)?;
        if file.metadata()?.len() != length {
            return Err(invalid("persisted CRS has the wrong size"));
        }
        Ok(Self {
            file: Arc::new(file),
            length,
            digest,
            failed: Arc::new(AtomicBool::new(false)),
        })
    }

    #[cfg(test)]
    pub(crate) fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// Revalidates a cached publication before promising a reusable hint.
    /// Shape failures must invalidate it too: a decoder may reject a damaged
    /// header before the reader reaches the final checksum.
    pub(crate) fn validate(&self, blocks: usize, degree: usize) -> io::Result<()> {
        let result = crate::wire::validate_crs_blocks(self.reader(), blocks, degree);
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        result
    }

    /// Every reader has an independent offset and verifies the pinned file.
    pub fn reader(&self) -> impl Read + Send + 'static {
        BufReader::with_capacity(
            IO_BUFFER_BYTES,
            VerifiedReader::new(
                PositionalReader {
                    file: self.file.clone(),
                    offset: 0,
                },
                self.length,
                self.digest.clone(),
            )
            .with_failure_flag(self.failed.clone()),
        )
    }

    /// Bounded, backpressured disk-to-HTTP transfer. Closing the consumer stops
    /// the blocking reader; the final chunk is released only after validation.
    pub fn stream(&self) -> ReceiverStream<io::Result<Bytes>> {
        stream_reader(self.reader())
    }
}

pub(crate) fn stream_reader(
    mut reader: impl Read + Send + 'static,
) -> ReceiverStream<io::Result<Bytes>> {
    let (sender, receiver) = mpsc::channel(STREAM_QUEUE_CHUNKS);
    tokio::task::spawn_blocking(move || loop {
        if sender.is_closed() {
            break;
        }
        let mut chunk = vec![0; IO_BUFFER_BYTES];
        match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                chunk.truncate(n);
                if sender.blocking_send(Ok(Bytes::from(chunk))).is_err() {
                    break;
                }
            }
            Err(e) => {
                let _ = sender.blocking_send(Err(e));
                break;
            }
        }
    });
    ReceiverStream::new(receiver)
}

struct PositionalReader {
    file: Arc<File>,
    offset: u64,
}

impl Read for PositionalReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read_at(bytes, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }
}

/// Checks exact length and digest before returning the final bytes. Hashing
/// sits beneath buffering, so it processes chunks rather than coefficients.
pub(crate) struct VerifiedReader<R> {
    inner: R,
    remaining: u64,
    hash: Sha256,
    digest: String,
    complete: bool,
    failed: Arc<AtomicBool>,
}

impl<R: Read> VerifiedReader<R> {
    pub(crate) fn new(inner: R, length: u64, digest: String) -> Self {
        Self {
            inner,
            remaining: length,
            hash: Sha256::new(),
            digest,
            complete: false,
            failed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn with_failure_flag(mut self, failed: Arc<AtomicBool>) -> Self {
        self.failed = failed;
        self
    }

    fn read_checked(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.remaining.min(bytes.len() as u64) as usize;
        let n = if count == 0 {
            0
        } else {
            self.inner.read(&mut bytes[..count])?
        };
        if n == 0 && self.remaining != 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated artifact",
            ));
        }
        self.hash.update(&bytes[..n]);
        self.remaining -= n as u64;
        if self.remaining == 0 {
            let mut extra = [0];
            let extra_count = loop {
                match self.inner.read(&mut extra) {
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    result => break result?,
                }
            };
            if extra_count != 0 {
                return Err(invalid("trailing artifact bytes"));
            }
            if hex::encode(self.hash.clone().finalize()) != self.digest {
                return Err(invalid("persisted artifact digest mismatch"));
            }
            self.complete = true;
        }
        Ok(n)
    }
}

impl<R: Read> Read for VerifiedReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(invalid("artifact validation failed"));
        }
        if self.complete {
            return Ok(0);
        }
        match self.read_checked(bytes) {
            Err(e) if e.kind() != io::ErrorKind::Interrupted => {
                self.failed.store(true, Ordering::Release);
                Err(e)
            }
            result => result,
        }
    }
}

struct HashWriter<W> {
    inner: W,
    hash: Sha256,
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(bytes)?;
        self.hash.update(&bytes[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Commits one file after a successful flush and sync. Callers commit metadata
/// last and sync the directory. A failed attempt never renames its temporary.
pub(crate) fn write_atomic(
    directory: &Path,
    name: &str,
    write: impl FnOnce(&mut dyn Write) -> io::Result<()>,
) -> io::Result<String> {
    let temporary = directory.join(format!("{name}.tmp"));
    let result = (|| {
        let file = File::create(&temporary)?;
        let mut writer = BufWriter::with_capacity(
            IO_BUFFER_BYTES,
            HashWriter {
                inner: file,
                hash: Sha256::new(),
            },
        );
        write(&mut writer)?;
        writer.flush()?;
        writer.get_ref().inner.sync_all()?;
        let digest = hex::encode(writer.get_ref().hash.clone().finalize());
        fs::rename(&temporary, directory.join(name))?;
        Ok(digest)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_stream::StreamExt;

    fn artifact(path: &Path, bytes: &[u8]) -> PublicationArtifact {
        fs::write(path, bytes).unwrap();
        PublicationArtifact::open(path, bytes.len() as u64, hex::encode(Sha256::digest(bytes)))
            .unwrap()
    }

    #[test]
    fn failure_is_shared_but_partial_reads_and_interrupts_do_not_invalidate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hint");
        let data = vec![7; IO_BUFFER_BYTES * 2];
        let pinned = artifact(&path, &data);
        let peer = pinned.clone();
        let mut reader = pinned.reader();
        reader.read_exact(&mut [0; 8]).unwrap();
        drop(reader);
        assert!(
            !peer.is_failed(),
            "cancellation must not invalidate a healthy artifact"
        );
        struct InterruptedThenFailed(bool);
        impl Read for InterruptedThenFailed {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                if std::mem::replace(&mut self.0, false) {
                    Err(io::Error::from(io::ErrorKind::Interrupted))
                } else {
                    Err(io::Error::other("injected disk failure"))
                }
            }
        }
        let mut reader = VerifiedReader::new(
            InterruptedThenFailed(true),
            data.len() as u64,
            pinned.digest.clone(),
        )
        .with_failure_flag(pinned.failed.clone());
        assert_eq!(
            reader.read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
        assert!(!peer.is_failed());
        assert!(reader.read(&mut [0]).is_err());
        assert!(peer.is_failed());
        assert!(peer.reader().read(&mut [0]).is_err());
    }

    #[test]
    fn pinned_files_survive_replacement_and_have_independent_readers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hint");
        let old = artifact(&path, b"old bytes");
        let mut a = old.reader();
        let mut b = old.reader();
        let mut prefix = [0; 3];
        a.read_exact(&mut prefix).unwrap();
        assert_eq!(&prefix, b"old");
        write_atomic(dir.path(), "hint", |w| w.write_all(b"new bytes")).unwrap();
        drop(old);
        let mut all = Vec::new();
        b.read_to_end(&mut all).unwrap();
        assert_eq!(all, b"old bytes");
        all.clear();
        a.read_to_end(&mut all).unwrap();
        assert_eq!(all, b" bytes");
        assert_eq!(fs::read(&path).unwrap(), b"new bytes");
    }

    #[test]
    fn verification_rejects_corruption_truncation_and_trailing_data() {
        let data = vec![7u8; IO_BUFFER_BYTES * 2 + 17];
        for corrupt in [
            data[..data.len() - 1].to_vec(),
            [data.as_slice(), &[0]].concat(),
            vec![8; data.len()],
        ] {
            let mut reader = VerifiedReader::new(
                Cursor::new(corrupt),
                data.len() as u64,
                hex::encode(Sha256::digest(&data)),
            );
            let mut output = Vec::new();
            assert!(reader.read_to_end(&mut output).is_err());
            assert!(output.len() < data.len(), "must withhold final bytes");
            assert!(
                reader.read(&mut [0]).is_err(),
                "validation failure stays terminal"
            );
        }
    }

    #[test]
    fn failed_atomic_write_keeps_previous_file_and_cleans_temporary() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("hint"), b"committed").unwrap();
        let result = write_atomic(dir.path(), "hint", |w| {
            w.write_all(&vec![9; IO_BUFFER_BYTES + 1])?;
            Err(io::Error::other("injected write failure"))
        });
        assert!(result.is_err());
        assert_eq!(fs::read(dir.path().join("hint")).unwrap(), b"committed");
        assert!(!dir.path().join("hint.tmp").exists());
        let digest = write_atomic(dir.path(), "hint", |w| w.write_all(b"replacement")).unwrap();
        assert_eq!(digest, hex::encode(Sha256::digest(b"replacement")));
    }

    struct CountingReader {
        reads: Arc<AtomicUsize>,
        stopped: Option<tokio::sync::oneshot::Sender<()>>,
    }
    impl Read for CountingReader {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            assert!(bytes.len() <= IO_BUFFER_BYTES);
            self.reads.fetch_add(1, Ordering::SeqCst);
            bytes.fill(1);
            Ok(bytes.len())
        }
    }
    impl Drop for CountingReader {
        fn drop(&mut self) {
            if let Some(stopped) = self.stopped.take() {
                let _ = stopped.send(());
            }
        }
    }

    #[tokio::test]
    async fn slow_consumer_bounds_read_ahead_and_cancellation_stops_reader() {
        let reads = Arc::new(AtomicUsize::new(0));
        let (stopped, finished) = tokio::sync::oneshot::channel();
        let stream = stream_reader(CountingReader {
            reads: reads.clone(),
            stopped: Some(stopped),
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while reads.load(Ordering::SeqCst) < STREAM_QUEUE_CHUNKS + 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(reads.load(Ordering::SeqCst), STREAM_QUEUE_CHUNKS + 1);
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(5), finished)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), STREAM_QUEUE_CHUNKS + 1);
    }

    #[tokio::test]
    async fn stream_keeps_pin_after_eviction_and_reports_final_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hint");
        let bytes = vec![4; IO_BUFFER_BYTES * 3 + 9];
        let pinned = artifact(&path, &bytes);
        let mut stream = pinned.stream();
        drop(pinned);
        fs::remove_file(&path).unwrap();
        let mut decoded = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            assert!(chunk.len() <= IO_BUFFER_BYTES);
            decoded.extend_from_slice(&chunk);
        }
        assert_eq!(decoded, bytes);
        let pinned = artifact(&path, &bytes);
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .write_at(&[5], bytes.len() as u64 - 1)
            .unwrap();
        let mut stream = pinned.stream();
        let mut received = 0;
        let mut failed = false;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => received += bytes.len(),
                Err(_) => failed = true,
            }
        }
        assert!(failed);
        assert!(received < bytes.len());
    }
}
