//! Local, disposable snapshots of public preprocessing. The format has no
//! allocator-controlled dimensions: shapes are derived from verified parameters.
//! Bump FORMAT when dependency layout, preprocessing representation, or setup
//! derivation changes. The dependency revisions below are part of the
//! compatibility key.
//!
//! # Layout
//!
//! ```text
//! identity (32) | sha256 of everything after this header (32)
//! database: rows u16 per column, column-major, little-endian
//! published masks: exactly what clients download
//! reinspiring::prepared_native (RNMAP002), to the end of the file
//! ```
//!
//! Every section before the preprocessing has a length fixed by the verified
//! parameters. The preprocessing is variable: its compiled matrix is stored at
//! four- or eight-byte words, so a valid entry's length lies in a range rather
//! than at one value. It starts eight-byte aligned and ends the file, so it can
//! be mapped rather than copied. A mapped entry's inode is immutable: writers
//! replace paths atomically and collection only unlinks, so a runtime keeps its
//! mapping valid for as long as it lives.
//!
//! On restore the checksum covers every byte, the preprocessing is re-validated
//! by `prepared_native::read`, and the masks it publishes must equal the stored
//! bytes — so a restored runtime cannot publish masks its preprocessing does
//! not answer under.
use super::{RuntimeKey, SharedParams, TableRuntime};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

// ipir-sp 1f2aec6 is v0.1.0-rc.6, which pins reinspiring 0.1.2.
const FORMAT: &[u8] = b"transparent-runtime-v2/native-two-mask-m29/ipir-1f2aec6/reinspiring-0.1.2";

/// Identity and checksum.
const HEADER_BYTES: u64 = 64;

/// The writer uses a 1 MiB buffer, 8 KiB word staging and small hash/metadata
/// state, and `prepared_native::write` converts one compiled matrix at a time.
/// Runtime coefficients remain owned/accounted separately. Queued writers also
/// retain this reservation.
pub(super) const SAVE_SCRATCH_BYTES: u64 = 2 << 20;

#[derive(Clone, Debug)]
pub struct DiskCache {
    pub directory: PathBuf,
    pub max_bytes: u64,
    /// Independent restore concurrency; cold fallbacks still use build slots.
    pub restore_slots: usize,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl DiskCache {
    pub fn new(directory: PathBuf, max_bytes: u64) -> io::Result<Self> {
        if max_bytes == 0 {
            return Err(invalid("runtime disk cache budget must be positive"));
        }
        fs::create_dir_all(&directory)?;
        Ok(Self {
            directory,
            max_bytes,
            restore_slots: 4,
        })
    }

    fn identity(key: &RuntimeKey, shared: &SharedParams, source_sha: &str) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(FORMAT);
        // These are trusted, locally derived values, serialized only to form a
        // key. The native scheme carries the parameter encoding, every bit
        // width and both setup seeds.
        hash.update(
            serde_json::to_vec(&(
                &key.0,
                key.1.as_str(),
                key.2,
                shared.geometry.name,
                source_sha,
                shared.setup_seed,
                shared.scheme(),
                &shared.transport,
            ))
            .expect("parameters serialize"),
        );
        hash.finalize().into()
    }

    pub fn path(&self, key: &RuntimeKey, shared: &SharedParams, source_sha: &str) -> PathBuf {
        // Hash the manifest too rather than interpreting caller text as a path.
        self.directory.join(format!(
            "{}-{}.runtime",
            hex::encode(Sha256::digest(key.0.as_bytes())),
            hex::encode(Self::identity(key, shared, source_sha))
        ))
    }

    /// Bytes before the preprocessing: header, database and published masks.
    fn fixed_bytes(shared: &SharedParams) -> u64 {
        HEADER_BYTES + shared.profile.database_bytes() + shared.scheme().public_bytes as u64
    }

    /// Every length a valid entry can have, derived from the pinned layout
    /// before anything is allocated.
    pub fn entry_bytes_range(shared: &SharedParams) -> RangeInclusive<u64> {
        let fixed = Self::fixed_bytes(shared);
        fixed + shared.profile.prepared_min_bytes()..=fixed + shared.profile.prepared_max_bytes()
    }

    /// The largest valid entry, which is what a write reserves before it
    /// starts. Once written, an entry is charged its file length.
    pub fn entry_bytes(shared: &SharedParams) -> u64 {
        *Self::entry_bytes_range(shared).end()
    }

    /// An entry's length with its compiled matrix at four-byte words, the
    /// counterpart of [`SharedParams::held_bytes`] and what plans charge an
    /// entry not yet written.
    pub fn planned_entry_bytes(shared: &SharedParams) -> u64 {
        *Self::entry_bytes_range(shared).start()
    }

    pub fn load(
        &self,
        key: &RuntimeKey,
        shared: &SharedParams,
        source_sha: &str,
    ) -> io::Result<TableRuntime> {
        let mut file = File::open(self.path(key, shared, source_sha))?;
        let length = file.metadata()?.len();
        if !Self::entry_bytes_range(shared).contains(&length) {
            return Err(invalid("runtime cache length mismatch"));
        }
        let mut header = [0u8; HEADER_BYTES as usize];
        file.read_exact(&mut header)?;
        if header[..32] != Self::identity(key, shared, source_sha) {
            return Err(invalid("runtime cache identity mismatch"));
        }
        let fixed = Self::fixed_bytes(shared);
        let mut reader = BufReader::with_capacity(
            1 << 20,
            HashReader {
                input: (&file).take(fixed - HEADER_BYTES),
                hash: Sha256::new(),
            },
        );
        let profile = &shared.profile;
        let mut error = None;
        let n = profile.rows * profile.cols;
        // The constructor consumes exactly n logical column-major coefficients
        // and recreates padding and the locally selected CPU kernel. I/O failures
        // yield placeholders to satisfy its iterator contract, then fail below.
        // Every u16 is a canonical plaintext at p = 2^16.
        let mut buffer = [0u8; 8192];
        let coefficients = (0..n).map(|index| {
            let offset = index % (buffer.len() / 2);
            if offset == 0 && error.is_none() {
                let count = (n - index).min(buffer.len() / 2);
                if let Err(e) = reader.read_exact(&mut buffer[..count * 2]) {
                    error = Some(e);
                }
            }
            if error.is_some() {
                return 0;
            }
            u16::from_le_bytes([buffer[offset * 2], buffer[offset * 2 + 1]])
        });
        let server = super::database_server(shared, coefficients, true);
        if let Some(error) = error {
            return Err(error);
        }
        let mut public = vec![0u8; shared.scheme().public_bytes];
        reader.read_exact(&mut public)?;
        let mut hash = reader.into_inner().hash;
        // SAFETY: the entry's inode is never modified after its atomic rename;
        // writers replace paths and collection only unlinks, which leaves an
        // existing mapping valid. The mapped bytes are checksummed and then
        // re-validated by `prepared_native::read` before anything uses them.
        let map = unsafe { memmap2::Mmap::map(&file) }?;
        if map.len() as u64 != length {
            return Err(invalid("runtime cache changed while restoring"));
        }
        hash.update(&map[fixed as usize..]);
        if hash.finalize().as_slice() != &header[32..] {
            return Err(invalid("runtime cache checksum mismatch"));
        }
        let preprocessed = reinspiring::prepared_native::read(
            map,
            fixed as usize,
            &profile.setup,
            profile.blocks(),
        )
        .map_err(|error| invalid(&error.to_string()))?;
        let runtime = TableRuntime::assemble(server, preprocessed).map_err(|e| invalid(&e))?;
        if runtime.public_params != public {
            return Err(invalid("runtime cache public mask binding"));
        }
        // Mapped preprocessing pages are not dropped by the advice; the
        // database and mask pages already copied into owned memory are.
        crate::filecache::consumed(&file);
        Ok(runtime)
    }

    /// The cross-process lock bounds concurrent writes. No active/rollback entry
    /// is evicted to make room: deployment prunes unreferenced revisions later.
    ///
    /// Like the memory cache, a write reserves the largest valid entry before
    /// it starts, and once it is renamed into place the entry counts at its
    /// file length, so the bound's excess returns to the budget for the next
    /// write. Writers are serialized by the lock, so at most one entry is ever
    /// charged its bound.
    pub fn save(
        &self,
        key: &RuntimeKey,
        shared: &SharedParams,
        source_sha: &str,
        runtime: &TableRuntime,
    ) -> io::Result<()> {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.directory.join(".lock"))?;
        lock.lock()?;
        let path = self.path(key, shared, source_sha);
        // A previous interrupted writer cannot still own the lock.
        let _ = fs::remove_file(path.with_extension("partial"));
        let used = self.used_bytes()?;
        let replacing = path.metadata().map(|m| m.len()).unwrap_or(0);
        if used
            .saturating_sub(replacing)
            .saturating_add(Self::entry_bytes(shared))
            > self.max_bytes
        {
            return Err(io::Error::other("runtime disk cache budget exhausted"));
        }
        self.write(key, shared, source_sha, runtime, true)
            .map(|_| ())
    }

    /// Writes one entry at [`Self::path`] through a `.partial` and an atomic
    /// rename, without the cache's lock or budget, and returns its length.
    /// [`Self::save`] calls it under both, `durable`: the file, then the
    /// directory, are fsynced. A shipped-runtime writer calls it alone on a
    /// directory nothing else writes to, not durable: a file lost to a crash
    /// is only a counted fallback build on the worker, and fsyncing two
    /// 40 MiB files would add over a second to every block.
    fn write(
        &self,
        key: &RuntimeKey,
        shared: &SharedParams,
        source_sha: &str,
        runtime: &TableRuntime,
        durable: bool,
    ) -> io::Result<u64> {
        let path = self.path(key, shared, source_sha);
        let temp = path.with_extension("partial");
        let result = (|| {
            let file = File::create(&temp)?;
            (&file).write_all(&Self::identity(key, shared, source_sha))?;
            // The checksum is patched in once the body has been hashed.
            (&file).write_all(&[0u8; 32])?;
            let mut writer = CheckedWriter {
                output: BufWriter::with_capacity(
                    1 << 20,
                    HashWriter {
                        output: IncrementalWriteback {
                            file: &file,
                            pending: 0,
                            sync: durable,
                        },
                        hash: Sha256::new(),
                    },
                ),
            };
            let rows = shared.profile.rows;
            let padded = runtime.server.db_rows_padded();
            for column in 0..shared.profile.cols {
                writer.words(
                    &runtime.server.db()[column * padded..column * padded + rows],
                    u16::to_le_bytes,
                )?;
            }
            writer.bytes(&runtime.public_params)?;
            reinspiring::prepared_native::write(&mut writer.output, &runtime.preprocessed)?;
            writer.output.flush()?;
            let checksum = writer.output.get_ref().hash.clone().finalize();
            drop(writer);
            (&file).seek(SeekFrom::Start(32))?;
            (&file).write_all(&checksum)?;
            if durable {
                file.sync_all()?;
                crate::filecache::consumed(&file);
            }
            let length = file.metadata()?.len();
            if !Self::entry_bytes_range(shared).contains(&length) {
                return Err(invalid(
                    "runtime export layout changed; bump the cache format",
                ));
            }
            fs::rename(&temp, &path)?;
            if durable {
                File::open(&self.directory)?.sync_all()?;
            }
            Ok(length)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }

    /// Required additional persistent bytes and one atomic-write temporary.
    /// Source tables have already passed ShardSet verification. Cache contents
    /// are still checksum-validated on restore; a length in range is only sizing.
    ///
    /// Entries are charged as [`Self::save`] leaves them: each missing entry at
    /// its planned four-byte-word length, plus the bound's excess once, for the
    /// single write the directory lock lets run at a time. That never exceeds
    /// charging every missing entry its bound, as this check once did.
    /// `temporary_bytes` stays the largest valid entry.
    pub fn check_set(&self, set: &crate::shardset::ShardSet) -> io::Result<serde_json::Value> {
        let mut params = std::collections::HashMap::new();
        let mut missing = 0u64;
        let mut excess = 0u64;
        let mut largest = 0u64;
        let mut total = 0u64;
        for shard in set.current() {
            for table in super::Table::HISTORY {
                if shard.segments(table) == 0 {
                    continue;
                }
                let shared = params
                    .entry((shard.geometry.name, table))
                    .or_insert_with(|| SharedParams::build(shard.geometry, table));
                let shared = shared.as_ref().map_err(|e| invalid(e))?;
                let bytes = Self::planned_entry_bytes(shared);
                let valid = Self::entry_bytes_range(shared);
                largest = largest.max(Self::entry_bytes(shared));
                for segment in 0..shard.segments(table) {
                    total = total.saturating_add(bytes);
                    let source = shard.segment(table, segment).expect("published segment");
                    let key = (shard.digest.clone(), table, segment);
                    let present = self
                        .path(&key, shared, &source.sha256)
                        .metadata()
                        .map(|m| m.len())
                        .unwrap_or(0);
                    if !valid.contains(&present) {
                        missing = missing.saturating_add(bytes);
                        excess = excess.max(Self::entry_bytes(shared) - bytes);
                    }
                }
            }
        }
        let used = self.used_bytes()?;
        if used.saturating_add(missing).saturating_add(excess) > self.max_bytes {
            return Err(io::Error::other(format!(
                "runtime cache needs {missing} additional bytes with {used} retained; limit {}",
                self.max_bytes
            )));
        }
        Ok(
            serde_json::json!({"used_bytes":used,"assignment_bytes":total,
            "missing_bytes":missing,"temporary_bytes":largest,"limit_bytes":self.max_bytes}),
        )
    }

    pub fn used_bytes(&self) -> io::Result<u64> {
        let mut bytes = 0u64;
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            if entry
                .path()
                .extension()
                .is_some_and(|ext| ext == "runtime" || ext == "partial")
            {
                bytes = bytes.saturating_add(entry.metadata()?.len());
            }
        }
        Ok(bytes)
    }

    /// Called only after rollout verification, with all active and rollback
    /// revision digests (including retained revisions), under the writer lock.
    pub fn prune(&self, keep: &std::collections::HashSet<String>) -> io::Result<u64> {
        self.prune_with_lock(keep, true)
            .map(|bytes| bytes.expect("blocking lock"))
    }

    /// Live collection may defer optional cache reclamation while a snapshot is
    /// being written. The writer still enforces max_bytes under the same lock;
    /// it refuses new saves when full rather than evicting retained revisions.
    pub fn try_prune(&self, keep: &std::collections::HashSet<String>) -> io::Result<Option<u64>> {
        self.prune_with_lock(keep, false)
    }

    fn prune_with_lock(
        &self,
        keep: &std::collections::HashSet<String>,
        wait: bool,
    ) -> io::Result<Option<u64>> {
        let started = std::time::Instant::now();
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.directory.join(".lock"))?;
        if wait {
            lock.lock()?;
        } else {
            match lock.try_lock() {
                Ok(()) => {}
                Err(std::fs::TryLockError::WouldBlock) => {
                    tracing::info!("runtime disk collection deferred: snapshot writer busy");
                    return Ok(None);
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(error),
            }
        }
        let lock_wait_seconds = started.elapsed().as_secs_f64();
        let pruning_started = std::time::Instant::now();
        let prefixes: std::collections::HashSet<_> = keep
            .iter()
            .map(|digest| hex::encode(Sha256::digest(digest.as_bytes())))
            .collect();
        let mut freed = 0;
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let retained = name
                .split_once('-')
                .is_some_and(|(prefix, _)| prefixes.contains(prefix));
            if name.ends_with(".partial") || (name.ends_with(".runtime") && !retained) {
                freed += entry.metadata()?.len();
                fs::remove_file(entry.path())?;
            }
        }
        tracing::info!(
            lock_wait_seconds,
            pruning_seconds = pruning_started.elapsed().as_secs_f64(),
            freed_bytes = freed,
            "runtime disk collection stages"
        );
        Ok(Some(freed))
    }
}

/// Runtimes a publisher built and shipped beside a publication, in this
/// module's entry format, as plain files of the publication directory.
///
/// A view, not a cache: it has no budget, lock or collection, because the
/// directory belongs to the publication and goes with it. A worker only
/// loads from it, and a runtime loaded from it is never saved to the
/// worker's own disk cache. A file is found by the same identity a cache
/// entry is, so one made for another revision, table, segment, source or
/// scheme is simply not found, and a loaded one passes every check
/// [`DiskCache::load`] makes.
#[derive(Clone, Debug)]
pub struct ShippedRuntimes(DiskCache);

impl ShippedRuntimes {
    pub fn new(directory: PathBuf) -> Self {
        Self(DiskCache {
            directory,
            max_bytes: u64::MAX,
            restore_slots: 1,
        })
    }

    pub fn directory(&self) -> &Path {
        &self.0.directory
    }

    /// Whether `name` is the file name of a shipped runtime.
    pub fn is_entry(name: &str) -> bool {
        name.ends_with(".runtime")
    }

    pub fn path(&self, key: &RuntimeKey, shared: &SharedParams, source_sha: &str) -> PathBuf {
        self.0.path(key, shared, source_sha)
    }

    pub fn load(
        &self,
        key: &RuntimeKey,
        shared: &SharedParams,
        source_sha: &str,
    ) -> io::Result<TableRuntime> {
        self.0.load(key, shared, source_sha)
    }

    /// Writes one runtime into the directory, atomically but not durably;
    /// returns its length. For the publisher only.
    pub fn write(
        &self,
        key: &RuntimeKey,
        shared: &SharedParams,
        source_sha: &str,
        runtime: &TableRuntime,
    ) -> io::Result<u64> {
        self.0.write(key, shared, source_sha, runtime, false)
    }
}

struct HashReader<R> {
    input: R,
    hash: Sha256,
}
impl<R: Read> Read for HashReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let n = self.input.read(bytes)?;
        self.hash.update(&bytes[..n]);
        Ok(n)
    }
}
/// Limit dirty data produced by this optional snapshot writer before waiting
/// for writeback. Otherwise an entire runtime can accumulate before its final
/// fsync and interfere with the small, latency-sensitive publication record.
/// This blocks only the background saver. The final file fsync, atomic rename
/// and directory fsync in `save` still establish snapshot durability.
struct IncrementalWriteback<'a> {
    file: &'a File,
    pending: usize,
    /// Off for a writer that does not fsync at all.
    sync: bool,
}
impl<'a> IncrementalWriteback<'a> {
    const CHUNK: usize = 8 << 20;

    #[cfg(test)]
    fn new(file: &'a File) -> Self {
        Self {
            file,
            pending: 0,
            sync: true,
        }
    }
}
impl Write for IncrementalWriteback<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        // Sync before accepting more data: a failed barrier must not report an
        // error after consuming bytes, which could make a retry duplicate them.
        if self.pending == Self::CHUNK {
            if self.sync {
                self.file.sync_data()?;
            }
            self.pending = 0;
        }
        let n = self
            .file
            .write(&bytes[..bytes.len().min(Self::CHUNK - self.pending)])?;
        self.pending += n;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

struct HashWriter<W> {
    output: W,
    hash: Sha256,
}
impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let n = self.output.write(bytes)?;
        self.hash.update(&bytes[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}
struct CheckedWriter<W> {
    output: W,
}
impl<W: Write> CheckedWriter<W> {
    fn bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.output.write_all(bytes)
    }
    /// Batch the same little-endian words into bounded stack storage. This
    /// avoids a buffered I/O call per coefficient without changing the format,
    /// checksum input, atomic rename, or durability barrier.
    fn words<T: Copy, const N: usize>(
        &mut self,
        values: &[T],
        encode: impl Fn(T) -> [u8; N],
    ) -> io::Result<()> {
        let mut buffer = [0u8; 8192];
        for chunk in values.chunks(buffer.len() / N) {
            let bytes = &mut buffer[..chunk.len() * N];
            for (value, output) in chunk.iter().zip(bytes.chunks_exact_mut(N)) {
                output.copy_from_slice(&encode(*value));
            }
            self.bytes(bytes)?;
        }
        Ok(())
    }
}

/// Collect all retained revision directory names from an active/rollback set.
pub fn retained_digests(set: &Path) -> io::Result<std::collections::HashSet<String>> {
    let mut keep = std::collections::HashSet::new();
    for entry in fs::read_dir(set)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type()?.is_dir()
            && name.len() == 64
            && name.bytes().all(|b| b.is_ascii_hexdigit())
        {
            keep.insert(name);
        }
    }
    Ok(keep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shardset::Table;
    use transparent_shard::layout::{ARCHIVE_WIDE, RECENT_4K, RECENT_8K};

    #[test]
    fn busy_collection_defers_then_prunes_only_unretained_files() {
        let dir = tempfile::tempdir().unwrap();
        let disk = DiskCache::new(dir.path().join("cache"), 1024).unwrap();
        let digest = "ab".repeat(32);
        let prefix = hex::encode(Sha256::digest(digest.as_bytes()));
        let retained = disk.directory.join(format!("{prefix}-table.runtime"));
        let stale = disk.directory.join("unretained-table.runtime");
        let partial = disk.directory.join("writer.partial");
        for path in [&retained, &stale, &partial] {
            fs::write(path, b"data").unwrap();
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(disk.directory.join(".lock"))
            .unwrap();
        lock.lock().unwrap();
        let keep: std::collections::HashSet<_> = [digest].into_iter().collect();
        let (tx, rx) = std::sync::mpsc::channel();
        let collector = std::thread::spawn({
            let disk = disk.clone();
            let keep = keep.clone();
            move || {
                let _ = tx.send(disk.try_prune(&keep));
            }
        });
        let result = rx.recv_timeout(std::time::Duration::from_secs(1));
        let untouched = [&retained, &stale, &partial]
            .iter()
            .all(|path| path.exists());
        drop(lock);
        collector.join().unwrap();
        assert_eq!(result.expect("collection waited for writer").unwrap(), None);
        assert!(untouched, "deferred collection removed files");
        assert_eq!(disk.try_prune(&keep).unwrap(), Some(8));
        assert!(retained.exists());
        assert!(!stale.exists());
        assert!(!partial.exists());
        assert_eq!(disk.used_bytes().unwrap(), 4);
    }

    #[test]
    fn incremental_snapshot_preserves_bytes_and_checksum_across_writeback() {
        let mut file = tempfile::tempfile().unwrap();
        let bytes: Vec<u8> = (0..IncrementalWriteback::CHUNK * 2 + 37)
            .map(|i| (i % 251) as u8)
            .collect();
        let mut writer = HashWriter {
            output: IncrementalWriteback::new(&file),
            hash: Sha256::new(),
        };
        // A single oversized call exercises Write's short-write contract;
        // a trailing write crosses the second barrier without alignment.
        writer.write_all(&bytes[..bytes.len() - 19]).unwrap();
        writer.write_all(&bytes[bytes.len() - 19..]).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.hash.finalize(), Sha256::digest(&bytes));
        file.sync_all().unwrap();
        file.rewind().unwrap();
        let mut actual = Vec::new();
        file.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, bytes);
    }

    #[test]
    fn batched_words_preserve_legacy_bytes_across_buffer_boundaries() {
        for count in [0, 1, 1023, 1024, 1025, 4095, 4096, 4097] {
            let values: Vec<u64> = (0..count).map(|i| u64::MAX.wrapping_mul(i)).collect();
            let mut writer = CheckedWriter { output: Vec::new() };
            writer.words(&values, u64::to_le_bytes).unwrap();
            assert_eq!(
                writer.output,
                values
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>()
            );
            let values: Vec<u16> = values.iter().map(|v| *v as u16).collect();
            let mut writer = CheckedWriter { output: Vec::new() };
            writer.words(&values, u16::to_le_bytes).unwrap();
            assert_eq!(
                writer.output,
                values
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>()
            );
        }
        let mut short = [0u8; 8];
        let mut writer = CheckedWriter {
            output: &mut short[..],
        };
        assert!(writer.words(&[1u64, 2], u64::to_le_bytes).is_err());
    }

    #[test]
    fn restored_runtimes_answer_identically_at_deployed_geometries() {
        for geometry in [&RECENT_8K, &ARCHIVE_WIDE] {
            for table in [Table::Directory, Table::Pages, Table::TxDirectory] {
                let dir = tempfile::tempdir().unwrap();
                let shared = SharedParams::build(geometry, table).unwrap();
                let rows: Vec<_> = (0..table.rows(geometry) as usize
                    * table.row_bytes(geometry) as usize)
                    .map(|i| (i.wrapping_mul(17) ^ (i >> 13)) as u8)
                    .collect();
                let key = ("ab".repeat(32), table, 1);
                let source_sha = hex::encode(Sha256::digest(&rows));
                let disk = DiskCache::new(dir.path().into(), shared.reserved_bytes() * 2).unwrap();
                let start = std::time::Instant::now();
                let cold = TableRuntime::build(&shared, &rows).unwrap();
                let build_time = start.elapsed();
                disk.save(&key, &shared, &source_sha, &cold).unwrap();
                let path = disk.path(&key, &shared, &source_sha);
                let length = path.metadata().unwrap().len();
                assert!(DiskCache::entry_bytes_range(&shared).contains(&length));
                let start = std::time::Instant::now();
                let restored = disk.load(&key, &shared, &source_sha).unwrap();
                eprintln!(
                    "{} {} build={build_time:?} restore={:?} bytes={length}",
                    geometry.name,
                    table.as_str(),
                    start.elapsed(),
                );
                assert_eq!(cold.public_params, restored.public_params);
                assert_eq!(cold.public_params.len(), 14_848);
                assert_eq!(cold.public_params_sha256, restored.public_params_sha256);
                assert_eq!(cold.public_params_epoch, restored.public_params_epoch);
                assert_eq!(cold.server.db(), restored.server.db());
                let binding = transparent_shard::manifest::query_binding(&key.0, table.as_str());
                let width = table.row_bytes(geometry) as usize;
                for selected in [0, table.rows(geometry) as usize - 1] {
                    let (secret, upload) = shared.profile.prepare(selected).unwrap();
                    let mut body = binding.to_vec();
                    body.extend(upload);
                    // A snapshot written and restored answers byte-for-byte as
                    // the runtime it was taken from.
                    let answer = restored.evaluate(&shared, binding, &body).unwrap();
                    assert_eq!(answer, cold.evaluate(&shared, binding, &body).unwrap());
                    assert_eq!(answer.len(), shared.response_bytes());
                    let decoded = shared
                        .profile
                        .decode(&secret, &restored.public_params, &answer[16..])
                        .unwrap();
                    assert_eq!(decoded, &rows[selected * width..(selected + 1) * width]);
                }
                drop(restored);
                // A different revision, segment or plaintext identity is a miss.
                let mut other = key.clone();
                other.2 += 1;
                assert!(disk.load(&other, &shared, &source_sha).is_err());
                other = key.clone();
                other.0 = "cd".repeat(32);
                assert!(disk.load(&other, &shared, &source_sha).is_err());
                assert!(disk.load(&key, &shared, "different-source").is_err());
                let mut file = OpenOptions::new().write(true).open(&path).unwrap();
                file.seek(SeekFrom::End(-32)).unwrap();
                file.write_all(&[0; 32]).unwrap();
                assert!(disk.load(&key, &shared, &source_sha).is_err());
                file.set_len(16).unwrap();
                assert!(disk.load(&key, &shared, &source_sha).is_err());
                // Replace a rejected entry atomically; abandoned partial is recoverable.
                fs::write(path.with_extension("partial"), b"interrupted").unwrap();
                disk.save(&key, &shared, &source_sha, &cold).unwrap();
                assert!(!path.with_extension("partial").exists());
                assert!(disk.load(&key, &shared, &source_sha).is_ok());
                let too_small = DiskCache::new(dir.path().into(), 1).unwrap();
                assert!(too_small.save(&key, &shared, &source_sha, &cold).is_err());
                assert_eq!(
                    disk.prune(&[key.0.clone()].into_iter().collect()).unwrap(),
                    0
                );
                assert!(disk.prune(&Default::default()).unwrap() > 0);
            }
        }
    }

    /// A body tampered after the checksum was written is refused even when its
    /// length stays valid, and so is an entry whose stored masks do not match
    /// what its preprocessing publishes.
    #[test]
    fn a_restored_entry_must_publish_the_masks_it_stored() {
        let dir = tempfile::tempdir().unwrap();
        let shared = SharedParams::build(&RECENT_4K, Table::Directory).unwrap();
        let rows = vec![3u8; (RECENT_4K.directory_rows * 4_096) as usize];
        let key = ("ef".repeat(32), Table::Directory, 0);
        let disk = DiskCache::new(dir.path().into(), shared.reserved_bytes() * 2).unwrap();
        let cold = TableRuntime::build(&shared, &rows).unwrap();
        disk.save(&key, &shared, "source", &cold).unwrap();
        let path = disk.path(&key, &shared, "source");
        let mut bytes = fs::read(&path).unwrap();
        let public_at = (DiskCache::fixed_bytes(&shared) as usize) - 14_848;
        bytes[public_at] ^= 1;
        // Recompute the checksum so only the mask binding can catch it.
        let checksum = Sha256::digest(&bytes[64..]);
        bytes[32..64].copy_from_slice(&checksum);
        fs::remove_file(&path).unwrap();
        fs::write(&path, &bytes).unwrap();
        let error = disk.load(&key, &shared, "source").err().unwrap();
        assert!(error.to_string().contains("public mask binding"), "{error}");
    }
}
#[cfg(test)]
mod cache_integration_tests {
    use super::*;
    use crate::metrics::Metrics;
    use crate::runtime::RuntimeCache;
    use crate::shardset::{SegmentSource, Table};
    use std::sync::Arc;
    use transparent_shard::layout::RECENT_4K;

    async fn wait_for_saves(metrics: &Arc<Metrics>) {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while Metrics::get(&metrics.disk_save_pending) != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("snapshot writer did not finish");
    }

    #[tokio::test]
    async fn blocked_snapshot_does_not_delay_serving_or_release_owned_memory() {
        let dir = tempfile::tempdir().unwrap();
        let shared = Arc::new(SharedParams::build(&RECENT_4K, Table::Directory).unwrap());
        let rows = vec![
            7u8;
            (Table::Directory.rows(&RECENT_4K) * Table::Directory.row_bytes(&RECENT_4K) as u64)
                as usize
        ];
        let source = SegmentSource {
            path: dir.path().join("table.bin"),
            rows: Table::Directory.rows(&RECENT_4K),
            row_bytes: Table::Directory.row_bytes(&RECENT_4K),
            sha256: hex::encode(Sha256::digest(&rows)),
            zero_from_row: Table::Directory.rows(&RECENT_4K),
        };
        fs::write(&source.path, &rows).unwrap();
        drop(rows);
        let disk = DiskCache::new(dir.path().join("cache"), shared.reserved_bytes() * 2).unwrap();
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(disk.directory.join(".lock"))
            .unwrap();
        lock.lock().unwrap();
        let metrics = Arc::new(Metrics::default());
        let cache = Arc::new(
            RuntimeCache::new(shared.reserved_bytes(), 1, metrics.clone())
                .with_disk(Some(disk.clone())),
        );
        let key = ("ab".repeat(32), Table::Directory, 0);
        let request = {
            let (cache, shared, source, key) =
                (cache.clone(), shared.clone(), source.clone(), key.clone());
            tokio::spawn(async move { cache.get(key, shared, source).await })
        };
        let handle = tokio::time::timeout(std::time::Duration::from_secs(60), request)
            .await
            .expect("snapshot lock blocked runtime readiness")
            .unwrap()
            .unwrap();
        assert_eq!(Metrics::get(&metrics.disk_save_pending), 1);
        assert_eq!(cache.build_slots.available_permits(), 1);
        // An independent caller also receives the same already-built runtime
        // while persistence is blocked. Dropping both callers cannot free it.
        let again = cache
            .get(key.clone(), shared.clone(), source.clone())
            .await
            .unwrap();
        assert!(std::ptr::eq(handle.get(), again.get()));
        let held = handle.get().held_bytes();
        drop(handle);
        drop(again);
        cache.evict_unpinned();
        assert_eq!(
            cache.work_memory.reserved_bytes(),
            shared.reserved_bytes() + SAVE_SCRATCH_BYTES
        );
        // The writer's work reservation keeps the bound for serialization
        // scratch; the cache charges the runtime what it holds.
        assert_eq!(cache.resident_bytes(), held);
        assert!(held < shared.reserved_bytes());
        assert_eq!(Metrics::get(&metrics.builds), 1);
        // RAII unlock also prevents a test failure from leaving a blocking task
        // stuck during runtime shutdown.
        drop(lock);
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while Metrics::get(&metrics.disk_save_pending) != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(Metrics::get(&metrics.builds), 1);
        assert!(disk.load(&key, &shared, &source.sha256).is_ok());
    }

    #[tokio::test]
    async fn restart_coalesces_restore_and_corruption_falls_back_without_losing_budget() {
        let dir = tempfile::tempdir().unwrap();
        let shared = Arc::new(SharedParams::build(&RECENT_4K, Table::Directory).unwrap());
        let rows = vec![
            7u8;
            (Table::Directory.rows(&RECENT_4K) * Table::Directory.row_bytes(&RECENT_4K) as u64)
                as usize
        ];
        let source = SegmentSource {
            path: dir.path().join("table.bin"),
            rows: Table::Directory.rows(&RECENT_4K),
            row_bytes: Table::Directory.row_bytes(&RECENT_4K),
            sha256: hex::encode(Sha256::digest(&rows)),
            zero_from_row: Table::Directory.rows(&RECENT_4K),
        };
        fs::write(&source.path, &rows).unwrap();
        drop(rows);
        let disk = DiskCache::new(dir.path().join("cache"), shared.reserved_bytes() * 2).unwrap();
        let key = ("ab".repeat(32), Table::Directory, 0);
        let metrics = Arc::new(Metrics::default());
        let cache = RuntimeCache::new(shared.reserved_bytes(), 1, metrics.clone())
            .with_disk(Some(disk.clone()));
        let first = cache
            .get(key.clone(), shared.clone(), source.clone())
            .await
            .unwrap();
        let expected = first.get().public_params_sha256.clone();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while Metrics::get(&metrics.disk_save_pending) != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(Metrics::get(&metrics.builds), 1);
        drop(first);
        drop(cache);
        let metrics = Arc::new(Metrics::default());
        let cache = RuntimeCache::new(shared.reserved_bytes(), 2, metrics.clone())
            .with_disk(Some(disk.clone()));
        // Occupied cold-build slots must not delay a valid disk restore.
        let builds = cache.build_slots.acquire_many(2).await.unwrap();
        let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            tokio::join!(
                cache.get(key.clone(), shared.clone(), source.clone()),
                cache.get(key.clone(), shared.clone(), source.clone())
            )
        })
        .await
        .expect("disk restore was blocked by occupied build slots");
        assert_eq!(a.unwrap().get().public_params_sha256, expected);
        assert_eq!(b.unwrap().get().public_params_sha256, expected);
        assert_eq!(Metrics::get(&metrics.builds), 0);
        assert_eq!(Metrics::get(&metrics.disk_hits), 1);
        drop(builds);
        drop(cache);
        #[cfg(unix)]
        cancelled_restore_keeps_its_reservation(&disk, &key, &shared, &source).await;
        fs::write(disk.path(&key, &shared, &source.sha256), b"corrupt").unwrap();
        let metrics = Arc::new(Metrics::default());
        let cache = RuntimeCache::new(shared.reserved_bytes(), 1, metrics.clone())
            .with_disk(Some(disk.clone()));
        assert_eq!(
            cache
                .get(key.clone(), shared.clone(), source.clone())
                .await
                .unwrap()
                .get()
                .public_params_sha256,
            expected
        );
        assert_eq!(Metrics::get(&metrics.builds), 1);
        assert_eq!(Metrics::get(&metrics.disk_misses), 1);
        wait_for_saves(&metrics).await;
        drop(cache);
        let metrics = Arc::new(Metrics::default());
        let tiny_disk = DiskCache::new(dir.path().join("too-small"), 1).unwrap();
        let cache = RuntimeCache::new(shared.reserved_bytes(), 1, metrics.clone())
            .with_disk(Some(tiny_disk));
        assert_eq!(
            cache
                .get(key.clone(), shared.clone(), source.clone())
                .await
                .unwrap()
                .get()
                .public_params_sha256,
            expected
        );
        wait_for_saves(&metrics).await;
        assert_eq!(Metrics::get(&metrics.disk_write_failures), 1);
        assert_eq!(Metrics::get(&metrics.builds), 1);
        drop(cache);
        // A valid cache must not conceal plaintext corruption after startup.
        fs::write(&source.path, b"corrupt source").unwrap();
        let cache = RuntimeCache::new(shared.reserved_bytes(), 1, Arc::new(Metrics::default()))
            .with_disk(Some(disk));
        assert!(cache.get(key, shared, source).await.is_err());
        assert_eq!(cache.resident_bytes(), 0);
    }

    #[cfg(unix)]
    async fn cancelled_restore_keeps_its_reservation(
        disk: &DiskCache,
        key: &RuntimeKey,
        shared: &Arc<SharedParams>,
        source: &SegmentSource,
    ) {
        use super::super::{CacheError, Metrics, RuntimeCache};
        // A FIFO holds source verification after the cached runtime has been
        // allocated. Cancelling its caller must not release the slot or its
        // reservation while this blocking verification is still alive.
        let rows = fs::read(&source.path).unwrap();
        fs::remove_file(&source.path).unwrap();
        assert!(std::process::Command::new("mkfifo")
            .arg(&source.path)
            .status()
            .unwrap()
            .success());
        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let path = source.path.clone();
        let writer = std::thread::spawn(move || {
            let mut file = File::create(path).unwrap();
            let _ = opened_tx.send(());
            if release_rx.recv().is_ok() {
                file.write_all(&rows).unwrap();
            }
            rows
        });
        let cache = Arc::new(
            RuntimeCache::new(shared.reserved_bytes(), 1, Arc::new(Metrics::default()))
                .with_disk(Some(disk.clone())),
        );
        let request = {
            let (cache, key, shared, source) =
                (cache.clone(), key.clone(), shared.clone(), source.clone());
            tokio::spawn(async move { cache.get(key, shared, source).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(30), opened_rx)
            .await
            .unwrap()
            .unwrap();
        request.abort();
        assert!(matches!(request.await, Err(error) if error.is_cancelled()));
        assert_eq!(
            cache.restore_slots.available_permits(),
            disk.restore_slots - 1
        );
        let mut other = key.clone();
        other.2 += 1;
        assert!(matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(3),
                cache.get(other, shared.clone(), source.clone())
            )
            .await
            .expect("cancelled restore released its memory reservation"),
            Err(CacheError::Overloaded)
        ));
        release_tx.send(()).unwrap();
        let rows = writer.join().unwrap();
        let _restores = cache
            .restore_slots
            .acquire_many(disk.restore_slots as u32)
            .await
            .unwrap();
        fs::remove_file(&source.path).unwrap();
        fs::write(&source.path, rows).unwrap();
    }
}
#[cfg(test)]
mod shipped_tests {
    use super::*;
    use crate::metrics::Metrics;
    use crate::runtime::{Produced, RuntimeCache};
    use crate::shardset::{SegmentSource, Table};
    use std::sync::Arc;
    use transparent_shard::display::TXID_2K;

    struct Segment {
        shared: Arc<SharedParams>,
        rows: Vec<u8>,
        source: SegmentSource,
        key: RuntimeKey,
    }

    /// A `txid-2k` segment of `table` whose first `filled` rows hold bytes
    /// drawn from `seed`, written under `dir`.
    fn segment(dir: &Path, table: Table, filled: usize, seed: usize) -> Segment {
        let shared = Arc::new(SharedParams::build(&TXID_2K, table).unwrap());
        let profile = &shared.profile;
        let mut rows = vec![0u8; profile.rows * profile.row_bytes];
        for (i, byte) in rows[..filled * profile.row_bytes].iter_mut().enumerate() {
            *byte = ((i + seed * 7_919).wrapping_mul(2_654_435_761) >> 9) as u8;
        }
        let path = dir.join(format!("{}-{seed}.bin", table.as_str()));
        fs::write(&path, &rows).unwrap();
        let source = SegmentSource {
            path,
            rows: profile.rows as u64,
            row_bytes: profile.row_bytes as u32,
            sha256: hex::encode(Sha256::digest(&rows)),
            zero_from_row: profile.rows as u64,
        };
        Segment {
            shared,
            rows,
            source,
            key: (format!("{}/directory-0", "ab".repeat(32)), table, 0),
        }
    }

    fn candidate(dir: &Path, name: &str) -> ShippedRuntimes {
        let path = dir.join(name);
        fs::create_dir_all(&path).unwrap();
        ShippedRuntimes::new(path)
    }

    /// A runtime loaded from a shipped file is the runtime this worker would
    /// have built: the same published masks, digest and epoch, byte-identical
    /// answers and correctly decoded rows, for a full directory and a partly
    /// filled page table. It is never written to the worker's disk cache.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_shipped_runtime_is_the_locally_built_runtime() {
        for (table, filled) in [(Table::TxDirectory, 2_048), (Table::TxDirectory, 700)] {
            let dir = tempfile::tempdir().unwrap();
            let s = segment(dir.path(), table, filled, 1);
            let local = TableRuntime::build(&s.shared, &s.rows).unwrap();
            let shipped = candidate(dir.path(), "candidate");
            let bytes = shipped
                .write(&s.key, &s.shared, &s.source.sha256, &local)
                .unwrap();
            assert!(DiskCache::entry_bytes_range(&s.shared).contains(&bytes));
            let disk =
                DiskCache::new(dir.path().join("cache"), s.shared.reserved_bytes() * 4).unwrap();
            let metrics = Arc::new(Metrics::default());
            let cache = RuntimeCache::new(s.shared.reserved_bytes() * 2, 1, metrics.clone())
                .with_disk(Some(disk.clone()));
            let (handle, produced) = cache
                .get_from(
                    s.key.clone(),
                    s.shared.clone(),
                    s.source.clone(),
                    Some(shipped),
                )
                .await
                .unwrap();
            assert!(matches!(produced, Produced::Shipped { .. }), "{produced:?}");
            assert_eq!(Metrics::get(&metrics.shipped_loads), 1);
            assert_eq!(Metrics::get(&metrics.shipped_fallbacks), 0);
            assert_eq!(Metrics::get(&metrics.builds), 0);
            assert_eq!(Metrics::get(&metrics.disk_hits), 0);
            assert_eq!(Metrics::get(&metrics.disk_save_pending), 0);
            assert_eq!(
                disk.used_bytes().unwrap(),
                0,
                "a shipped runtime is never saved"
            );
            let loaded = handle.get();
            assert_eq!(loaded.public_params, local.public_params);
            assert_eq!(loaded.public_params_sha256, local.public_params_sha256);
            assert_eq!(loaded.public_params_epoch, local.public_params_epoch);
            let profile = &s.shared.profile;
            let binding = [9u8; 8];
            for selected in [0, filled / 2, filled - 1, profile.rows - 1] {
                let (secret, upload) = profile.prepare(selected).unwrap();
                let mut body = binding.to_vec();
                body.extend(upload);
                let answer = loaded.evaluate(&s.shared, binding, &body).unwrap();
                assert_eq!(answer, local.evaluate(&s.shared, binding, &body).unwrap());
                let row = profile
                    .decode(&secret, &loaded.public_params, &answer[16..])
                    .unwrap();
                let at = selected * profile.row_bytes;
                assert_eq!(
                    row,
                    &s.rows[at..at + profile.row_bytes],
                    "{table:?} {selected}"
                );
            }
        }
    }

    /// Every shipped file the worker cannot use is counted as a fallback and
    /// the runtime is built here, publishing what a local build publishes: a
    /// missing file, a truncated or corrupted one, and one made for another
    /// revision or another table and placed under this one's name.
    #[tokio::test(flavor = "multi_thread")]
    async fn refused_shipped_files_fall_back_to_a_counted_local_build() {
        let dir = tempfile::tempdir().unwrap();
        let s = segment(dir.path(), Table::TxDirectory, 700, 2);
        let local = TableRuntime::build(&s.shared, &s.rows).unwrap();
        let mut other_revision = s.key.clone();
        other_revision.0 = "cd".repeat(32);
        let mut other_bucket = s.key.clone();
        other_bucket.0 = format!("{}/directory-1", "ab".repeat(32));
        type Prepare<'a> = Box<dyn Fn(&ShippedRuntimes) + 'a>;
        let cases: Vec<(&str, Prepare)> = vec![
            ("missing", Box::new(|_| {})),
            (
                "truncated",
                Box::new(|shipped| {
                    let path = shipped.path(&s.key, &s.shared, &s.source.sha256);
                    let length = shipped
                        .write(&s.key, &s.shared, &s.source.sha256, &local)
                        .unwrap();
                    let file = OpenOptions::new().write(true).open(path).unwrap();
                    file.set_len(length / 2).unwrap();
                }),
            ),
            (
                "corrupted",
                Box::new(|shipped| {
                    let path = shipped.path(&s.key, &s.shared, &s.source.sha256);
                    shipped
                        .write(&s.key, &s.shared, &s.source.sha256, &local)
                        .unwrap();
                    let mut bytes = fs::read(&path).unwrap();
                    let middle = bytes.len() / 2;
                    bytes[middle] ^= 0x40;
                    fs::write(&path, bytes).unwrap();
                }),
            ),
            (
                "another revision",
                Box::new(|shipped| {
                    shipped
                        .write(&other_revision, &s.shared, &s.source.sha256, &local)
                        .unwrap();
                    fs::rename(
                        shipped.path(&other_revision, &s.shared, &s.source.sha256),
                        shipped.path(&s.key, &s.shared, &s.source.sha256),
                    )
                    .unwrap();
                }),
            ),
            (
                "another table",
                Box::new(|shipped| {
                    shipped
                        .write(&other_bucket, &s.shared, &s.source.sha256, &local)
                        .unwrap();
                    fs::rename(
                        shipped.path(&other_bucket, &s.shared, &s.source.sha256),
                        shipped.path(&s.key, &s.shared, &s.source.sha256),
                    )
                    .unwrap();
                }),
            ),
        ];
        for (name, prepare) in cases {
            let shipped = candidate(dir.path(), name);
            prepare(&shipped);
            let metrics = Arc::new(Metrics::default());
            let cache = RuntimeCache::new(s.shared.reserved_bytes() * 2, 1, metrics.clone());
            let (handle, produced) = cache
                .get_from(
                    s.key.clone(),
                    s.shared.clone(),
                    s.source.clone(),
                    Some(shipped),
                )
                .await
                .unwrap();
            assert_eq!(produced, Produced::Fallback, "{name}");
            assert_eq!(Metrics::get(&metrics.shipped_fallbacks), 1, "{name}");
            assert_eq!(Metrics::get(&metrics.shipped_loads), 0, "{name}");
            assert_eq!(Metrics::get(&metrics.builds), 1, "{name}");
            assert_eq!(handle.get().public_params, local.public_params, "{name}");
        }
        // With nothing offered, a build is not a fallback.
        let metrics = Arc::new(Metrics::default());
        let cache = RuntimeCache::new(s.shared.reserved_bytes() * 2, 1, metrics.clone());
        let (_, produced) = cache
            .get_from(s.key.clone(), s.shared.clone(), s.source.clone(), None)
            .await
            .unwrap();
        assert_eq!(produced, Produced::Built);
        assert_eq!(Metrics::get(&metrics.shipped_fallbacks), 0);
    }

    /// The disk format's own checks cannot tell whether a database matches
    /// its preprocessing, or is the segment's at all. The self-check can: a
    /// file holding rows A's database with rows B's preprocessing, saved
    /// under A's identity, loads, but fails the self-check and is replaced by
    /// a local build; so does a consistent runtime of rows B under A's name.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_self_check_refuses_a_database_its_preprocessing_was_not_built_from() {
        let dir = tempfile::tempdir().unwrap();
        let a = segment(dir.path(), Table::TxDirectory, 2_048, 4);
        let b = segment(dir.path(), Table::TxDirectory, 2_048, 5);
        let from_a = TableRuntime::build(&a.shared, &a.rows).unwrap();
        let expected = from_a.public_params.clone();
        let from_b = TableRuntime::build(&b.shared, &b.rows).unwrap();
        let TableRuntime { server, .. } = from_a;
        let forged = TableRuntime::assemble(server, from_b.preprocessed).unwrap();
        let from_b = TableRuntime::build(&b.shared, &b.rows).unwrap();
        // The forged database is the segment's, so only its answer can fail.
        for (name, runtime, database_differs) in
            [("forged", &forged, false), ("other rows", &from_b, true)]
        {
            let shipped = candidate(dir.path(), name);
            shipped
                .write(&a.key, &a.shared, &a.source.sha256, runtime)
                .unwrap();
            let loaded = shipped.load(&a.key, &a.shared, &a.source.sha256).unwrap();
            let refused = loaded.self_check(&a.shared, &a.rows).unwrap_err();
            assert_eq!(
                refused.contains("differs from the segment"),
                database_differs,
                "{name}: {refused}"
            );
            let metrics = Arc::new(Metrics::default());
            let cache = RuntimeCache::new(a.shared.reserved_bytes() * 2, 1, metrics.clone());
            let (handle, produced) = cache
                .get_from(
                    a.key.clone(),
                    a.shared.clone(),
                    a.source.clone(),
                    Some(shipped),
                )
                .await
                .unwrap();
            assert_eq!(produced, Produced::Fallback, "{name}");
            assert_eq!(Metrics::get(&metrics.shipped_fallbacks), 1);
            assert_eq!(handle.get().public_params, expected);
        }
        // The check passes for a runtime of the segment's own rows.
        let own = TableRuntime::build(&a.shared, &a.rows).unwrap();
        assert!(own.self_check(&a.shared, &a.rows).unwrap() < 2_048);
    }
}
