//! Preparing one segment's runtime ahead of the workers that serve it.
//!
//! A publisher writes each runtime it prepares as a plain file beside the
//! publication's revision directories, in the disk-cache entry format (see
//! [`ShippedRuntimes`]). What a product's segments are, and which parameters
//! they take, stays with the product: Display prepares its recent revision
//! (`crate::tx_detail::prebuild`), history every revision of its set
//! (`crate::runtime::prebuild`). This module only turns one verified segment into one
//! file, or links a completed file an earlier publication already holds.
//!
//! A file is found by its identity, which hashes the runtime key, the
//! segment's source digest, the geometry and the native parameters. A file
//! of the same name in a retained publication is therefore a runtime of the
//! same rows under the same scheme, and linking it costs nothing. It is
//! linked only if it is complete: a renamed entry, never a `.partial` left by
//! an interrupted write, with a valid length and its own identity in its
//! header. Anything else is rebuilt. A worker checksums and self-checks every
//! file it loads whichever way the publisher came by it.

use super::disk::ShippedRuntimes;
use super::{build_pool, RuntimeKey, SharedParams, TableRuntime};
use crate::history::shardset::SegmentSource;
use serde::Serialize;
use std::time::Instant;

/// One runtime file a publisher holds beside a publication.
#[derive(Clone, Debug, Serialize)]
pub struct Prepared {
    pub name: String,
    pub table: String,
    pub segment: u32,
    pub bytes: u64,
    /// Linked from a retained publication's completed file, or already
    /// present, rather than built.
    pub linked: bool,
    /// Encode, hint and preprocessing, without reading or writing; zero
    /// when linked.
    pub build_seconds: f64,
    /// Writing or linking the file.
    pub write_seconds: f64,
}

/// Why a segment's runtime could not be prepared.
#[derive(Debug)]
pub enum SegmentError {
    /// The segment's own bytes did not verify: the publication itself is
    /// faulty, and must not be published.
    Source(String),
    /// Building or writing the optional runtime failed. The publication is
    /// sound; workers build this runtime themselves.
    Runtime(String),
}

impl std::fmt::Display for SegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SegmentError::Source(error) | SegmentError::Runtime(error) => write!(f, "{error}"),
        }
    }
}

/// Holds one segment's runtime in `out`: the completed file `out` already
/// has, else a hard link to the first completed one in `retained`, else a
/// runtime built from the segment's verified rows and written atomically.
///
/// `label` names the table in the record. Builds run on the runtime build
/// pool.
pub fn prepare_segment(
    out: &ShippedRuntimes,
    retained: &[ShippedRuntimes],
    key: &RuntimeKey,
    shared: &SharedParams,
    source: &SegmentSource,
    label: String,
) -> Result<Prepared, SegmentError> {
    let path = out.path(key, shared, source.sha256());
    let name = path
        .file_name()
        .expect("an entry path has a name")
        .to_string_lossy()
        .into_owned();
    let record = |bytes, linked, build_seconds, write_seconds| Prepared {
        name: name.clone(),
        table: label.clone(),
        segment: key.2,
        bytes,
        linked,
        build_seconds,
        write_seconds,
    };
    let started = Instant::now();
    if let Ok(bytes) = out.completed(key, shared, source.sha256()) {
        return Ok(record(bytes, true, 0.0, started.elapsed().as_secs_f64()));
    }
    for from in retained {
        if from.directory() == out.directory() {
            continue;
        }
        if let Some(bytes) = link(from, out, key, shared, source.sha256()) {
            return Ok(record(bytes, true, 0.0, started.elapsed().as_secs_f64()));
        }
    }
    let rows = source
        .load()
        .map_err(|error| SegmentError::Source(error.to_string()))?;
    let started = Instant::now();
    let runtime = build_pool()
        .install(|| TableRuntime::build(shared, &rows))
        .map_err(SegmentError::Runtime)?;
    let build_seconds = started.elapsed().as_secs_f64();
    drop(rows);
    let started = Instant::now();
    let bytes = out
        .write(key, shared, source.sha256(), &runtime)
        .map_err(|error| SegmentError::Runtime(format!("writing the {label} runtime: {error}")))?;
    Ok(record(
        bytes,
        false,
        build_seconds,
        started.elapsed().as_secs_f64(),
    ))
}

/// Links `from`'s completed file for `key` into `out`, replacing an invalid
/// one there; `None` when `from` has none or the link cannot be made. The
/// linked path must then name the very inode that was checked.
fn link(
    from: &ShippedRuntimes,
    out: &ShippedRuntimes,
    key: &RuntimeKey,
    shared: &SharedParams,
    source_sha: &str,
) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    let bytes = from.completed(key, shared, source_sha).ok()?;
    let (source, target) = (
        from.path(key, shared, source_sha),
        out.path(key, shared, source_sha),
    );
    let _ = std::fs::remove_file(&target);
    std::fs::hard_link(&source, &target).ok()?;
    let (a, b) = (
        std::fs::metadata(&source).ok()?,
        std::fs::metadata(&target).ok()?,
    );
    if a.dev() != b.dev() || a.ino() != b.ino() || b.len() != bytes {
        let _ = std::fs::remove_file(&target);
        return None;
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::shardset::Table;
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::os::unix::fs::MetadataExt;
    use zakura_pir_protocol::transparent::shard::layout::DECOUPLED_2K_8K;

    fn segment(dir: &std::path::Path, seed: u8) -> (std::sync::Arc<SharedParams>, SegmentSource) {
        let (geometry, table) =
            crate::runtime::tests::fixture_shape(&DECOUPLED_2K_8K, Table::Pages);
        let shared = crate::runtime::tests::fixture_params(geometry, table, false);
        let profile = &shared.profile;
        let rows = if cfg!(feature = "qualification-tests") {
            let mut rows = vec![0u8; profile.rows * profile.row_bytes];
            for (i, byte) in rows[..300 * profile.row_bytes].iter_mut().enumerate() {
                *byte = (i as u8).wrapping_mul(31) ^ seed;
            }
            rows
        } else {
            crate::runtime::tests::recovery_rows(&shared, false)
        };
        let path = dir.join(format!("pages-{seed}.bin"));
        fs::write(&path, &rows).unwrap();
        let source = SegmentSource::published(
            path,
            profile.rows as u64,
            profile.row_bytes as u32,
            hex::encode(Sha256::digest(&rows)),
            profile.rows as u64,
        );
        (shared, source)
    }

    fn publication(dir: &std::path::Path, name: &str) -> ShippedRuntimes {
        let path = dir.join(name);
        fs::create_dir_all(&path).unwrap();
        ShippedRuntimes::new(path)
    }

    #[tokio::test]
    async fn required_prepared_refuses_missing_and_corrupt_on_later_cache_misses() {
        use crate::runtime::{metrics::Metrics, Produced, RuntimeCache};
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let (shared, source) = segment(dir.path(), 3);
        let key = ("12".repeat(32), shared.table, 0);
        let out = publication(dir.path(), "strict");
        let strict = ShippedRuntimes::required(out.directory().to_path_buf());
        let metrics = Arc::new(Metrics::default());
        let cache = RuntimeCache::new(4 << 30, 1, metrics.clone());
        assert!(cache
            .get_from(
                key.clone(),
                shared.clone(),
                source.clone(),
                Some(strict.clone())
            )
            .await
            .is_err());
        assert_eq!(Metrics::get(&metrics.shipped_fallbacks), 0);
        let prepared = prepare_segment(&out, &[], &key, &shared, &source, "pages".into()).unwrap();
        assert!(!prepared.linked);
        assert!(prepared.build_seconds > 0.0);
        let (handle, produced) = cache
            .get_from(
                key.clone(),
                shared.clone(),
                source.clone(),
                Some(strict.clone()),
            )
            .await
            .unwrap();
        assert!(matches!(produced, Produced::Shipped { .. }));
        drop(handle);
        cache.evict_unpinned();
        // The same strict view is used after activation on a query cache miss.
        let file = out.path(&key, &shared, source.sha256());
        fs::write(&file, [0u8; 64]).unwrap();
        assert!(cache
            .get_from(
                key.clone(),
                shared.clone(),
                source.clone(),
                Some(strict.clone())
            )
            .await
            .is_err());
        fs::remove_file(&file).unwrap();
        assert!(cache
            .get_from(key.clone(), shared.clone(), source.clone(), Some(strict))
            .await
            .is_err());
        assert_eq!(Metrics::get(&metrics.shipped_fallbacks), 0);
        assert_eq!(Metrics::get(&metrics.builds), 0);
        // An existing optional deployment still builds when no shipped file
        // exists. Its behavior is preserved, with a counted fallback.
        let (_handle, produced) = cache
            .get_from(
                key.clone(),
                shared.clone(),
                source.clone(),
                Some(out.clone()),
            )
            .await
            .unwrap();
        assert!(matches!(produced, Produced::Fallback));
        assert_eq!(Metrics::get(&metrics.shipped_fallbacks), 1);
    }

    /// A retained publication's completed file is linked, not rebuilt; a
    /// missing, truncated, foreign or interrupted one is rebuilt, and a
    /// corrupt source fails as the publication's own fault.
    #[test]
    fn completed_files_are_linked_and_anything_else_is_rebuilt() {
        let dir = tempfile::tempdir().unwrap();
        let (shared, source) = segment(dir.path(), 1);
        let key = ("ab".repeat(32), shared.table, 0);
        let first = publication(dir.path(), "first");
        let rows = fs::read(source.path()).unwrap();
        let runtime = crate::runtime::tests::prepared_fixture(&shared, &rows).unwrap();
        let prepared_bytes = first
            .write(&key, &shared, source.sha256(), &runtime)
            .unwrap();
        drop(runtime);
        // Preparing the same publication again keeps its file.
        let again = prepare_segment(&first, &[], &key, &shared, &source, "pages".into()).unwrap();
        assert!(again.linked);

        let second = publication(dir.path(), "second");
        let linked = prepare_segment(
            &second,
            std::slice::from_ref(&first),
            &key,
            &shared,
            &source,
            "pages".into(),
        )
        .unwrap();
        assert!(linked.linked);
        assert_eq!(linked.bytes, prepared_bytes);
        let (a, b) = (
            fs::metadata(first.path(&key, &shared, source.sha256())).unwrap(),
            fs::metadata(second.path(&key, &shared, source.sha256())).unwrap(),
        );
        assert_eq!((a.dev(), a.ino()), (b.dev(), b.ino()));

        // A truncated retained file is not linked; the new one is built.
        let truncated = publication(dir.path(), "truncated");
        fs::copy(
            first.path(&key, &shared, source.sha256()),
            truncated.path(&key, &shared, source.sha256()),
        )
        .unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(truncated.path(&key, &shared, source.sha256()))
            .unwrap()
            .set_len(prepared_bytes / 2)
            .unwrap();
        let third = publication(dir.path(), "third");
        let rebuilt = prepare_segment(
            &third,
            std::slice::from_ref(&truncated),
            &key,
            &shared,
            &source,
            "pages".into(),
        )
        .unwrap();
        assert!(!rebuilt.linked);

        // Foreign and interrupted files are the same "not linked, so rebuilt"
        // rule as truncation. Qualification repeats them; fast keeps one rebuild.
        if cfg!(feature = "qualification-tests") {
            // A file of another identity placed under this one's name is not
            // linked either.
            let foreign = publication(dir.path(), "foreign");
            let other = ("cd".repeat(32), shared.table, 0);
            let runtime = first.load(&key, &shared, source.sha256()).unwrap();
            foreign
                .write(&other, &shared, source.sha256(), &runtime)
                .unwrap();
            drop(runtime);
            fs::rename(
                foreign.path(&other, &shared, source.sha256()),
                foreign.path(&key, &shared, source.sha256()),
            )
            .unwrap();
            // Only an interrupted write's partial file in another.
            let interrupted = publication(dir.path(), "interrupted");
            fs::copy(
                first.path(&key, &shared, source.sha256()),
                interrupted
                    .path(&key, &shared, source.sha256())
                    .with_extension("partial"),
            )
            .unwrap();
            let fourth = publication(dir.path(), "fourth");
            let rebuilt = prepare_segment(
                &fourth,
                &[foreign, interrupted],
                &key,
                &shared,
                &source,
                "pages".into(),
            )
            .unwrap();
            assert!(!rebuilt.linked);
            // An invalid file already in the publication is replaced.
            fs::write(fourth.path(&key, &shared, source.sha256()), b"invalid").unwrap();
            let replaced = prepare_segment(
                &fourth,
                std::slice::from_ref(&first),
                &key,
                &shared,
                &source,
                "pages".into(),
            )
            .unwrap();
            assert!(replaced.linked);
            assert_eq!(
                fs::metadata(fourth.path(&key, &shared, source.sha256()))
                    .unwrap()
                    .ino(),
                a.ino()
            );
        }

        // A source that no longer matches its digest is the publication's
        // fault, not an optional runtime's.
        fs::write(source.path(), b"corrupt").unwrap();
        let fifth = publication(dir.path(), "fifth");
        assert!(matches!(
            prepare_segment(&fifth, &[], &key, &shared, &source, "pages".into()),
            Err(SegmentError::Source(_))
        ));
    }
}
