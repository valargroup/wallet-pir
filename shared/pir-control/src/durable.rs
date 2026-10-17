//! Files a crash leaves either as they were or fully replaced.
//!
//! A serving process records what it serves (active publications, fences,
//! authority, checkpoints) in files it reads back on restart. Each is written
//! beside its final name, synced, then renamed over it, so a reader never sees
//! a partial file under the name. A caller syncs the directory once the
//! rename, or the last of several, must survive a crash too.

use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Write};
use std::path::Path;

/// Buffer size for streaming a file in or out.
pub const IO_BUFFER_BYTES: usize = 64 * 1024;

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

/// Replaces `directory/name` with what `write` writes, and returns the hex
/// SHA-256 of those bytes.
///
/// The bytes go to `name.tmp` first, are flushed and synced, and only then
/// renamed over `name`. A failed attempt never renames its temporary and
/// removes it. The directory is not synced: see [`sync_dir`].
pub fn write_atomic(
    directory: &Path,
    name: &str,
    write: impl FnOnce(&mut dyn Write) -> io::Result<()>,
) -> io::Result<String> {
    write_atomic_with(directory, name, |_| {}, write)
}

/// [`write_atomic`], calling `synced` with the file once its bytes are on disk
/// and before it is renamed; for advice such as dropping it from page cache.
pub fn write_atomic_with(
    directory: &Path,
    name: &str,
    synced: impl FnOnce(&File),
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
        synced(&writer.get_ref().inner);
        let digest = hex::encode(writer.get_ref().hash.clone().finalize());
        fs::rename(&temporary, directory.join(name))?;
        Ok(digest)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

/// Syncs a directory, so renames into it survive a crash.
pub fn sync_dir(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

/// SHA-256 of a file's contents, streamed.
pub fn sha256_file(path: &Path) -> io::Result<[u8; 32]> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(hasher.finalize().into());
        }
        hasher.update(&buffer[..read]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_fixed_short_and_streamed_vectors() {
        assert_eq!(
            hex::encode(Sha256::digest(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let mut hash = Sha256::new();
        for _ in 0..1000 {
            hash.update([b'a'; 1000]);
        }
        assert_eq!(
            hex::encode(hash.finalize()),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    #[test]
    fn a_replacement_lands_whole_and_returns_its_digest() {
        let dir = tempfile::tempdir().unwrap();
        write_atomic(dir.path(), "record", |w| w.write_all(b"old")).unwrap();
        let digest = write_atomic(dir.path(), "record", |w| w.write_all(b"new")).unwrap();
        assert_eq!(fs::read(dir.path().join("record")).unwrap(), b"new");
        assert_eq!(digest, hex::encode(Sha256::digest(b"new")));
        assert_eq!(
            sha256_file(&dir.path().join("record")).unwrap(),
            <[u8; 32]>::from(Sha256::digest(b"new"))
        );
        assert!(!dir.path().join("record.tmp").exists());
        sync_dir(dir.path()).unwrap();
    }

    #[test]
    fn a_failed_write_keeps_the_old_file_and_leaves_no_temporary() {
        let dir = tempfile::tempdir().unwrap();
        write_atomic(dir.path(), "record", |w| w.write_all(b"old")).unwrap();
        let failed = write_atomic(dir.path(), "record", |w| {
            w.write_all(&vec![1; IO_BUFFER_BYTES + 1])?;
            Err(io::Error::other("interrupted"))
        });
        assert!(failed.is_err());
        assert_eq!(fs::read(dir.path().join("record")).unwrap(), b"old");
        assert!(!dir.path().join("record.tmp").exists());
    }

    #[test]
    fn synced_sees_the_file_before_the_rename() {
        let dir = tempfile::tempdir().unwrap();
        let mut seen = None;
        write_atomic_with(
            dir.path(),
            "record",
            |file| seen = Some(file.metadata().unwrap().len()),
            |w| w.write_all(b"bytes"),
        )
        .unwrap();
        assert_eq!(seen, Some(5));
    }
}
