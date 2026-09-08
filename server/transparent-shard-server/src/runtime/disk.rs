//! Local, disposable snapshots of public preprocessing. The format has no
//! allocator-controlled dimensions: shapes are derived from verified parameters.
//! Bump FORMAT when dependency layout, NTT representation, or setup derivation
//! changes. The dependency revisions below are part of the compatibility key.
use super::{published_c1_rows, RuntimeKey, SharedParams, TableRuntime};
use inspiring::QueryPackPreprocessed;
use ipir_sp::server::IPIRServer;
use sha2::{Digest, Sha256};
use spiral_rs::poly::{PolyMatrix, PolyMatrixNTT};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const FORMAT: &[u8] = b"transparent-runtime-v1/ipir-223626f/spiral-6f5b66c";

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
        // These are trusted, locally derived values, serialized only to form a key.
        hash.update(
            serde_json::to_vec(&(
                &key.0,
                key.1.as_str(),
                key.2,
                shared.geometry.name,
                source_sha,
                shared.setup_seed,
                &shared.scheme,
                shared.rlwe.d,
                shared.rlwe.q,
                shared.rlwe.p,
                shared.rlwe.gadget.ell,
                shared.rlwe.gadget.bits_per,
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

    /// Exact file length derived from the pinned layout, before allocating.
    pub fn entry_bytes(shared: &SharedParams) -> u64 {
        let r = shared.rlwe;
        let blocks = shared.scheme.db_cols / r.d;
        let words = blocks * (1 + (r.d - 1) * r.gadget.ell) * r.d * r.spiral.crt_count;
        64 + 2 * (shared.scheme.db_rows * shared.scheme.db_cols) as u64 + 8 * words as u64
    }

    pub fn load(
        &self,
        key: &RuntimeKey,
        shared: &SharedParams,
        source_sha: &str,
    ) -> io::Result<TableRuntime> {
        let mut file = File::open(self.path(key, shared, source_sha))?;
        if file.metadata()?.len() != Self::entry_bytes(shared) {
            return Err(invalid("runtime cache length mismatch"));
        }
        file.seek(SeekFrom::End(-32))?;
        let mut expected = [0u8; 32];
        file.read_exact(&mut expected)?;
        file.rewind()?;
        let payload = file.take(Self::entry_bytes(shared) - 32);
        let mut reader = CheckedReader {
            input: BufReader::with_capacity(
                1 << 20,
                HashReader {
                    input: payload,
                    hash: Sha256::new(),
                },
            ),
        };
        let identity = reader.bytes::<32>()?;
        if identity != Self::identity(key, shared, source_sha) {
            return Err(invalid("runtime cache identity mismatch"));
        }
        let mut error = None;
        let n = shared.scheme.db_rows * shared.scheme.db_cols;
        // The constructor consumes exactly n logical column-major coefficients
        // and recreates padding and the locally selected CPU kernel. I/O failures
        // yield placeholders to satisfy its iterator contract, then fail below.
        let coefficients = (0..n).map(|_| match reader.bytes::<2>() {
            Ok(bytes) => {
                let value = u16::from_le_bytes(bytes);
                if value as u64 >= shared.scheme.p {
                    error = Some(invalid("noncanonical database coefficient"));
                }
                value
            }
            Err(e) => {
                error = Some(e);
                0
            }
        });
        let server =
            IPIRServer::<u16>::new_auto_kernel(shared.scheme.clone(), coefficients, true, true);
        if let Some(error) = error {
            return Err(error);
        }
        let mut preprocessed = Vec::new();
        for _ in 0..shared.scheme.db_cols / shared.rlwe.d {
            let collapse_a_final_ntt = reader.matrix(shared, 1, 1)?;
            let mut digits_ntt = Vec::with_capacity(shared.rlwe.d - 1);
            for _ in 0..shared.rlwe.d - 1 {
                digits_ntt.push(reader.matrix(shared, shared.rlwe.gadget.ell, 1)?);
            }
            preprocessed.push(QueryPackPreprocessed {
                params: shared.rlwe,
                collapse_a_final_ntt,
                digits_ntt,
            });
        }
        if reader.input.into_inner().hash.finalize().as_slice() != expected {
            return Err(invalid("runtime cache checksum mismatch"));
        }
        let public_params = published_c1_rows(&preprocessed, shared.rlwe.q);
        let digest = Sha256::digest(&public_params);
        let mut epoch = [0u8; 8];
        epoch.copy_from_slice(&digest[..8]);
        Ok(TableRuntime {
            server,
            preprocessed,
            public_params,
            public_params_sha256: hex::encode(digest),
            public_params_epoch: epoch,
        })
    }

    /// The cross-process lock bounds concurrent writes. No active/rollback entry
    /// is evicted to make room: deployment prunes unreferenced revisions later.
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
        let temp = path.with_extension("partial");
        let result = (|| {
            let file = File::create(&temp)?;
            let mut writer = CheckedWriter {
                output: BufWriter::with_capacity(
                    1 << 20,
                    HashWriter {
                        output: file,
                        hash: Sha256::new(),
                    },
                ),
            };
            writer.bytes(&Self::identity(key, shared, source_sha))?;
            let rows = shared.scheme.db_rows;
            let padded = runtime.server.db_rows_padded();
            for column in 0..shared.scheme.db_cols {
                for value in &runtime.server.db()[column * padded..column * padded + rows] {
                    writer.bytes(&value.to_le_bytes())?;
                }
            }
            for pre in &runtime.preprocessed {
                writer.matrix(&pre.collapse_a_final_ntt)?;
                for matrix in &pre.digits_ntt {
                    writer.matrix(matrix)?;
                }
            }
            writer.output.flush()?;
            let checksum = writer.output.get_ref().hash.clone().finalize();
            writer.output.get_mut().output.write_all(&checksum)?;
            writer.output.get_ref().output.sync_all()?;
            if writer.output.get_ref().output.metadata()?.len() != Self::entry_bytes(shared) {
                return Err(invalid(
                    "runtime export layout changed; bump the cache format",
                ));
            }
            fs::rename(&temp, &path)?;
            File::open(&self.directory)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }

    /// Required additional persistent bytes and one atomic-write temporary.
    /// Source tables have already passed ShardSet verification. Cache contents
    /// are still checksum-validated on restore; matching length is only sizing.
    pub fn check_set(&self, set: &crate::shardset::ShardSet) -> io::Result<serde_json::Value> {
        let mut params = std::collections::HashMap::new();
        let mut missing = 0u64;
        let mut largest = 0u64;
        let mut total = 0u64;
        for shard in set.revisions() {
            for table in [super::Table::Directory, super::Table::Pages] {
                let shared = params
                    .entry((shard.geometry.name, table))
                    .or_insert_with(|| SharedParams::build(shard.geometry, table));
                let shared = shared.as_ref().map_err(|e| invalid(e))?;
                let bytes = Self::entry_bytes(shared);
                largest = largest.max(bytes);
                for segment in 0..shard.segments(table) {
                    total = total.saturating_add(bytes);
                    let source = shard.segment(table, segment).expect("published segment");
                    let key = (shard.digest.clone(), table, segment);
                    if self
                        .path(&key, shared, &source.sha256)
                        .metadata()
                        .map(|m| m.len())
                        .unwrap_or(0)
                        != bytes
                    {
                        missing = missing.saturating_add(bytes);
                    }
                }
            }
        }
        let used = self.used_bytes()?;
        if used.saturating_add(missing) > self.max_bytes {
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
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.directory.join(".lock"))?;
        lock.lock()?;
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
        Ok(freed)
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
struct CheckedReader<R> {
    input: R,
}
impl<R: Read> CheckedReader<R> {
    fn bytes<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let mut bytes = [0; N];
        self.input.read_exact(&mut bytes)?;
        Ok(bytes)
    }
    fn matrix(
        &mut self,
        shared: &SharedParams,
        rows: usize,
        cols: usize,
    ) -> io::Result<PolyMatrixNTT<'static>> {
        let mut matrix = PolyMatrixNTT::zero(&shared.rlwe.spiral, rows, cols);
        for value in matrix.as_mut_slice() {
            *value = u64::from_le_bytes(self.bytes()?);
            if *value >= shared.rlwe.q {
                return Err(invalid("noncanonical NTT coefficient"));
            }
        }
        Ok(matrix)
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
    fn matrix(&mut self, matrix: &PolyMatrixNTT<'_>) -> io::Result<()> {
        for value in matrix.as_slice() {
            self.bytes(&value.to_le_bytes())?;
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
    use transparent_shard::layout::{ARCHIVE_WIDE, RECENT_8K};

    #[test]
    fn restored_runtimes_answer_identically_at_deployed_geometries() {
        for geometry in [&RECENT_8K, &ARCHIVE_WIDE] {
            for table in [Table::Directory, Table::Pages] {
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
                let start = std::time::Instant::now();
                let restored = disk.load(&key, &shared, &source_sha).unwrap();
                eprintln!(
                    "{} {} build={build_time:?} restore={:?} bytes={}",
                    geometry.name,
                    table.as_str(),
                    start.elapsed(),
                    disk.used_bytes().unwrap()
                );
                assert_eq!(cold.public_params, restored.public_params);
                assert_eq!(cold.public_params_sha256, restored.public_params_sha256);
                assert_eq!(cold.public_params_epoch, restored.public_params_epoch);
                assert_eq!(cold.server.db(), restored.server.db());
                let client = ipir_sp::IPIRClient::new(shared.rlwe, &shared.scheme);
                let mut seed = [0u8; 32];
                seed[..8].copy_from_slice(&shared.setup_seed.to_le_bytes());
                let setup = client.generate_public_query_setup_simplepir_from_seed(seed);
                let selected = table.rows(geometry) as usize - 1;
                let (query, keys, query_seed) =
                    client.generate_fresh_query_simplepir(&setup, selected);
                let binding = transparent_shard::manifest::query_binding(&key.0, table.as_str());
                let mut body = binding.to_vec();
                body.extend(
                    ipir_sp::serialize::serialize_packing_keys(shared.rlwe, &keys).unwrap(),
                );
                body.extend(query.to_switched_bytes(shared.rlwe.q, shared.scheme.query_bits));
                let answer = restored.evaluate(&shared, binding, &body).unwrap();
                assert_eq!(answer, cold.evaluate(&shared, binding, &body).unwrap());
                let c1 = ipir_sp::modulus_switch::recover_published_c1(
                    &restored.public_params,
                    shared.rlwe.d,
                    shared.scheme.db_cols / shared.rlwe.d,
                    shared.rlwe.q,
                );
                let decoded = client.decode_response_simplepir(query_seed, &c1, &answer[16..]);
                let width = table.row_bytes(geometry) as usize;
                assert_eq!(
                    &decoded[..width],
                    &rows[selected * width..(selected + 1) * width]
                );
                drop(restored);
                // A different revision, segment or plaintext identity is a miss.
                let mut other = key.clone();
                other.2 += 1;
                assert!(disk.load(&other, &shared, &source_sha).is_err());
                other = key.clone();
                other.0 = "cd".repeat(32);
                assert!(disk.load(&other, &shared, &source_sha).is_err());
                assert!(disk.load(&key, &shared, "different-source").is_err());
                let path = disk.path(&key, &shared, &source_sha);
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
}

#[cfg(test)]
mod cache_integration_tests {
    use super::*;
    use crate::metrics::Metrics;
    use crate::runtime::RuntimeCache;
    use crate::shardset::{SegmentSource, Table};
    use std::sync::Arc;
    use transparent_shard::layout::RECENT_4K;

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
