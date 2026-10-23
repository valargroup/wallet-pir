//! Resolving transaction-detail requests inside one loaded display set.
//!
//! A revision is resolved with its tier and the set's role, a table is one
//! bucket's entries, and the binding names the bucket. A superseded snapshot
//! answers only from runtimes still resident, so a client on a stale map
//! never takes build capacity from the candidate the next activation awaits.
//! Retirement is read when a request takes its runtimes, after admission.

use crate::history::shardset::SegmentSource;
use crate::runtime::disk::ShippedRuntimes;
use crate::runtime::metrics::Metrics;
use crate::runtime::{CacheError, Produced, RuntimeCache, RuntimeHandle, SharedParams};
use crate::serving::{Artifact, Prepared, PreparedQuery, PreparedSetup, ServingError, SourceParts};
use crate::table::Table;
use crate::tx_detail::set::{DisplayRevision, DisplaySet};
use crate::tx_detail::{kind, runtime_key, tier};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use zakura_pir_protocol::transparent::shard::display::{self, DisplayTable};
use zakura_pir_protocol::transparent::shard::layout::Geometry;

type ParamsKey = (&'static str, Table);

/// The native parameters of each geometry's table kind, derived once per
/// process and shared by every snapshot.
#[derive(Default)]
pub struct DisplayParams {
    params: Mutex<HashMap<ParamsKey, Arc<SharedParams>>>,
}

impl DisplayParams {
    pub fn get(
        &self,
        geometry: &'static Geometry,
        table: Table,
    ) -> Result<Arc<SharedParams>, String> {
        let mut params = self.params.lock().expect("display params");
        if let Some(shared) = params.get(&(geometry.name, table)) {
            return Ok(shared.clone());
        }
        let shared = Arc::new(SharedParams::build_display(geometry, table)?);
        params.insert((geometry.name, table), shared.clone());
        Ok(shared)
    }
}

/// One loaded display set with its parameters and runtime cache.
pub struct DisplayServing {
    set: DisplaySet,
    /// One parameter set per geometry the set names, per table kind.
    params: HashMap<ParamsKey, Arc<SharedParams>>,
    cache: Arc<RuntimeCache>,
    metrics: Arc<Metrics>,
    /// Managed consumers may only load publisher-prepared native files.
    required: bool,
    max_query_bytes: usize,
    /// Set once a successor is active; see [`DisplayServing::retire`].
    retired: Arc<AtomicBool>,
}

impl DisplayServing {
    /// Prepares a serving snapshot without loading its runtimes. Required
    /// publisher material is checked by size first.
    pub fn new(
        mut set: DisplaySet,
        derived: &DisplayParams,
        cache: Arc<RuntimeCache>,
        metrics: Arc<Metrics>,
        required: bool,
    ) -> Result<Self, String> {
        if required {
            // Check required files by size even if runtimes are already resident.
            let root = set
                .current()
                .first()
                .and_then(|r| r.dir.parent())
                .ok_or("display set has no root")?
                .to_path_buf();
            let shipped = ShippedRuntimes::required(root);
            // The set already checked the map and source tables. Verify
            // native files here without rebuilding a publisher distribution:
            // provider-owned stable revision links are not publisher inputs.
            let mut checked = 0usize;
            for revision in set.current() {
                for (table, segment) in revision.targets() {
                    let source = revision
                        .segment(table, segment)
                        .ok_or("missing display source")?;
                    let params = derived.get(revision.geometry, kind(table))?;
                    let key = runtime_key(&revision.digest, table, segment);
                    shipped
                        .check_size(&key, &params, source.sha256())
                        .map_err(|e| {
                            format!(
                                "required prepared runtime for display revision {} segment {segment}: {e}",
                                revision.digest
                            )
                        })?;
                    checked += 1;
                }
            }
            tracing::info!(checked, "display required runtime sizes checked");
            set.shipped = Some(shipped);
        }
        let mut params = HashMap::new();
        let mut max_query_bytes = 0usize;
        for geometry in set.geometries() {
            let shared = derived.get(geometry, Table::TxDirectory)?;
            max_query_bytes = max_query_bytes.max(shared.query_bytes());
            params.insert((geometry.name, Table::TxDirectory), shared);
        }
        if params.is_empty() {
            return Err("the display set names no geometry to serve".into());
        }
        Ok(Self {
            set,
            params,
            cache,
            metrics,
            required,
            max_query_bytes,
            retired: Arc::default(),
        })
    }

    pub fn set(&self) -> &DisplaySet {
        &self.set
    }

    pub fn cache(&self) -> &Arc<RuntimeCache> {
        &self.cache
    }

    /// Marks the snapshot superseded. Its requests are answered only from
    /// runtimes still resident from then on, prepared ones included: whether
    /// a request may build is decided when it takes each runtime, after it
    /// was admitted, not when it was resolved.
    pub fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }

    pub fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    /// The largest body any geometry of this set can legitimately send.
    pub fn max_query_bytes(&self) -> usize {
        self.max_query_bytes
    }

    /// One geometry's parameters for one table kind.
    pub fn params(&self, geometry: &'static str, table: Table) -> Option<&Arc<SharedParams>> {
        self.params.get(&(geometry, table))
    }

    fn shared(&self, revision: &DisplayRevision, table: DisplayTable) -> Arc<SharedParams> {
        self.params
            .get(&(revision.geometry.name, kind(table)))
            .expect("every named geometry has parameters")
            .clone()
    }

    /// Runtimes the prewarm holds: every target of every current held revision.
    pub fn warm_target_count(&self) -> usize {
        self.set
            .current()
            .iter()
            .filter(|r| r.held())
            .map(|r| r.targets().len())
            .sum()
    }

    /// Warm targets whose runtimes are resident now.
    pub fn resident_targets(&self) -> usize {
        self.set
            .warm_targets()
            .into_iter()
            .filter(|(digest, table, segment)| {
                self.cache
                    .cached(&runtime_key(digest, *table, *segment))
                    .is_some()
            })
            .count()
    }

    /// Resolves a digest-addressed request inside this set.
    ///
    /// The tier must be the revision's own and the role must hold it; both
    /// are routing faults, not stale clients. A digest this set does not know
    /// is stale, and the client refreshes its map.
    fn revision(
        &self,
        sealed: bool,
        shard_id: u64,
        digest: &str,
    ) -> Result<&DisplayRevision, ServingError> {
        let Some(revision) = self.set.revision(digest) else {
            Metrics::incr(&self.metrics.stale_revisions);
            return Err(stale(shard_id, digest));
        };
        if revision.manifest.shard_id != shard_id {
            return Err(ServingError::Invalid(format!(
                "revision {digest} is not a revision of display shard {shard_id}"
            )));
        }
        if revision.manifest.sealed != sealed {
            Metrics::incr(&self.metrics.unassigned_refusals);
            return Err(ServingError::WrongHoldings(format!(
                "revision {digest} of display shard {shard_id} is {}, not {}",
                tier(revision.manifest.sealed),
                tier(sealed)
            )));
        }
        if !revision.held() {
            Metrics::incr(&self.metrics.unassigned_refusals);
            return Err(ServingError::WrongHoldings(format!(
                "this {} worker does not serve {} display revisions",
                self.set.role.as_str(),
                tier(sealed)
            )));
        }
        Ok(revision)
    }

    /// A table label of `revision`, and its segment count. A bucket at or
    /// past the shard's bucket count is a bad request, not a stale one.
    fn table(revision: &DisplayRevision, label: &str) -> Result<(DisplayTable, u32), ServingError> {
        let table = DisplayTable::parse(label)
            .ok_or_else(|| ServingError::Invalid(format!("unknown display table {label:?}")))?;
        let segments = revision.segments(table).ok_or_else(|| {
            ServingError::Invalid(format!(
                "display shard {} has {} buckets; {label} is not one of them",
                revision.manifest.shard_id, revision.manifest.n_buckets
            ))
        })?;
        Ok((table, segments))
    }

    /// Where the runtimes of `sources` come from: the cache while the
    /// snapshot serves, only resident ones once it is retired.
    fn source(
        &self,
        revision: &DisplayRevision,
        table: DisplayTable,
        shard_id: u64,
        sources: Vec<(u32, SegmentSource)>,
    ) -> SourceParts {
        let prepared = Prepared {
            cache: self.cache.clone(),
            shared: self.shared(revision, table),
            metrics: self.metrics.clone(),
        };
        let digest = &revision.digest;
        prepared
            .build(
                sources
                    .into_iter()
                    .map(|(segment, source)| (runtime_key(digest, table, segment), source))
                    .collect(),
                None,
            )
            .retired_by(self.retired.clone(), stale(shard_id, digest))
    }

    /// A query to every segment of one bucket's table.
    pub fn prepare_query(
        &self,
        sealed: bool,
        shard_id: u64,
        digest: &str,
        label: &str,
    ) -> Result<PreparedQuery, ServingError> {
        let revision = self.revision(sealed, shard_id, digest)?;
        let (table, segments) = Self::table(revision, label)?;
        let sources = (0..segments)
            .map(|segment| {
                revision
                    .segment(table, segment)
                    .cloned()
                    .map(|s| (segment, s))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| ServingError::Invalid(format!("{label} is not held")))?;
        let query_rows = self.shared(revision, table).profile.rows;
        self.source(revision, table, shard_id, sources).query(
            query_rows,
            // Names the revision and the bucket: a body prepared for
            // another bucket's table is refused by `SharedParams::parse`.
            display::query_binding(digest, table),
            label.to_string(),
            digest.to_string(),
        )
    }

    /// The public setup of one segment of one bucket's table.
    pub fn prepare_setup(
        &self,
        sealed: bool,
        shard_id: u64,
        digest: &str,
        label: &str,
        segment: u32,
    ) -> Result<PreparedSetup, ServingError> {
        let revision = self.revision(sealed, shard_id, digest)?;
        let (table, segments) = Self::table(revision, label)?;
        if segment >= segments {
            return Err(ServingError::Invalid(format!(
                "{label} of display shard {shard_id} has {segments} segments"
            )));
        }
        let source = revision.segment(table, segment).cloned().ok_or_else(|| {
            ServingError::Invalid(format!("{label} segment {segment} is not held"))
        })?;
        Ok(self
            .source(revision, table, shard_id, vec![(segment, source)])
            .setup(segment, segments, table.label(), Some(table.bucket())))
    }

    /// Finds or builds one held revision's runtime for the prewarm, retrying
    /// transient admission pressure. Says whether and how it had to be
    /// produced (`None` when it was resident).
    ///
    /// Shipped runtimes are tried first for an unsealed revision only:
    /// archives are built once per seal and never shipped.
    pub async fn warm(
        &self,
        digest: &str,
        table: DisplayTable,
        segment: u32,
        cancelled: &AtomicBool,
    ) -> Option<Result<(RuntimeHandle, Option<Produced>), CacheError>> {
        let revision = self.set.held(digest)?;
        Some(self.produce(revision, table, segment, cancelled).await)
    }

    async fn produce(
        &self,
        revision: &DisplayRevision,
        table: DisplayTable,
        segment: u32,
        cancelled: &AtomicBool,
    ) -> Result<(RuntimeHandle, Option<Produced>), CacheError> {
        let source = revision
            .segment(table, segment)
            .cloned()
            .ok_or_else(|| CacheError::Failed("display segment is not held".into()))?;
        let key = runtime_key(&revision.digest, table, segment);
        if let Some(handle) = self.cache.cached(&key) {
            return Ok((handle, None));
        }
        let shared = self.shared(revision, table);
        let shipped = self.set.shipped.as_ref();
        let shipped = if self.required {
            Some(shipped.ok_or_else(|| {
                CacheError::Failed("managed display requires prepared material".into())
            })?)
        } else {
            shipped.filter(|_| !revision.manifest.sealed)
        };
        crate::runtime::prewarm::retry(cancelled, std::time::Duration::from_secs(30), || {
            self.cache.get_background_from(
                key.clone(),
                shared.clone(),
                source.clone(),
                shipped.cloned(),
            )
        })
        .await
        .map(|(handle, produced)| (handle, Some(produced)))
    }

    /// Every source table and shipped runtime file of every held revision,
    /// retained predecessors included: a wallet can still name those.
    pub fn artifacts(&self) -> Vec<Artifact> {
        let mut artifacts = Vec::new();
        for revision in self.set.revisions().iter().filter(|r| r.held()) {
            for (table, segment) in revision.targets() {
                let Some(source) = revision.segment(table, segment) else {
                    continue;
                };
                let shared = self.shared(revision, table);
                let key = runtime_key(&revision.digest, table, segment);
                let artifact = |path| Artifact {
                    digest: revision.digest.clone(),
                    shard_id: revision.manifest.shard_id,
                    path,
                    key: key.clone(),
                };
                artifacts.push(artifact(source.path().to_path_buf()));
                if let Some(shipped) = &self.set.shipped {
                    artifacts.push(artifact(shipped.path(&key, &shared, source.sha256())));
                }
            }
        }
        artifacts
    }
}

/// The revision named is no longer served: the wallet refreshes its txid map.
pub fn stale(shard_id: u64, digest: &str) -> ServingError {
    ServingError::Stale {
        message: format!("revision {digest} of display shard {shard_id} is no longer served"),
        retry: "refresh the txid map",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_detail::synth::{self, ShardSpec};
    use crate::tx_detail::DisplayRole;
    use std::path::Path;
    use zakura_pir_protocol::transparent::shard::display::{DisplaySealParams, TXID_2K8};

    fn spec(shard_id: u64, start: u64, end: u64, sealed: bool) -> ShardSpec {
        ShardSpec {
            shard_id,
            start_height: start,
            end_height: end,
            sealed,
            revision: 0,
            supersedes: String::new(),
            parent_manifest_digest: String::new(),
            n_buckets: 1,
            archive_target: 1,
            geometry: &TXID_2K8,
        }
    }

    /// One archive and one recent shard, written and linked into a candidate.
    fn publication(root: &Path) -> (std::path::PathBuf, Vec<synth::Published>) {
        let records = synth::records(40, 7);
        let archive = synth::write_shard(root, &spec(0, 10, 19, true), &records[..20]).unwrap();
        let mut recent = spec(1, 20, 29, false);
        recent.parent_manifest_digest = archive.digest.clone();
        let recent = synth::write_shard(root, &recent, &records[20..]).unwrap();
        let shards = vec![archive, recent];
        let params = DisplaySealParams {
            n_archive: 1,
            n_recent: 1,
            archive_target: 1,
            recent_window: 1,
            reorg_margin: 1,
        };
        let (dir, _) = synth::write_candidate(root, &params, &shards, "c0").unwrap();
        (dir, shards)
    }

    fn serving(dir: &Path, role: DisplayRole, budget: u64) -> (DisplayServing, Arc<Metrics>) {
        let metrics = Arc::new(Metrics::default());
        let cache = Arc::new(RuntimeCache::new(budget, 1, metrics.clone()));
        let set = DisplaySet::open(dir, 3, role).unwrap();
        let serving = DisplayServing::new(
            set,
            &DisplayParams::default(),
            cache,
            metrics.clone(),
            false,
        )
        .unwrap();
        (serving, metrics)
    }

    fn refused<T>(result: Result<T, ServingError>) -> ServingError {
        match result {
            Ok(_) => panic!("expected a refusal"),
            Err(error) => error,
        }
    }

    /// Each address fault is its own typed refusal, decided before any body
    /// is read: a stale digest, the wrong tier, a role that does not hold the
    /// tier, another shard's revision, an unknown table or bucket, a segment
    /// past the table's.
    #[test]
    fn requests_resolve_to_typed_refusals() {
        let root = tempfile::tempdir().unwrap();
        let (dir, shards) = publication(root.path());
        let (owner, metrics) = serving(&dir, DisplayRole::ArchiveOwner, u64::MAX);
        let (archive, recent) = (&shards[0].digest, &shards[1].digest);

        assert!(owner.prepare_query(true, 0, archive, "directory-0").is_ok());
        assert_eq!(
            refused(owner.prepare_query(true, 0, "00", "directory-0")),
            stale(0, "00")
        );
        assert_eq!(Metrics::get(&metrics.stale_revisions), 1);
        assert!(matches!(
            refused(owner.prepare_query(false, 0, archive, "directory-0")),
            ServingError::WrongHoldings(_)
        ));
        assert!(matches!(
            refused(owner.prepare_query(false, 1, recent, "directory-0")),
            ServingError::WrongHoldings(_)
        ));
        assert_eq!(Metrics::get(&metrics.unassigned_refusals), 2);
        assert!(matches!(
            refused(owner.prepare_query(true, 1, archive, "directory-0")),
            ServingError::Invalid(_)
        ));
        for label in ["pages", "directory-1"] {
            assert!(matches!(
                refused(owner.prepare_query(true, 0, archive, label)),
                ServingError::Invalid(_)
            ));
        }
        assert!(matches!(
            refused(owner.prepare_setup(true, 0, archive, "directory-0", 1)),
            ServingError::Invalid(_)
        ));
    }

    /// The prepared query accepts only the table's exact lengths, and an
    /// evaluation refuses a body bound to another revision or bucket before
    /// it touches any runtime.
    #[test]
    fn evaluation_checks_length_and_binding_first() {
        let root = tempfile::tempdir().unwrap();
        let (dir, shards) = publication(root.path());
        let (combined, _) = serving(&dir, DisplayRole::Combined, u64::MAX);
        let digest = &shards[1].digest;
        let query = combined
            .prepare_query(false, 1, digest, "directory-0")
            .unwrap();
        assert_eq!(query.segments(), 1);
        let shared = combined.params(TXID_2K8.name, Table::TxDirectory).unwrap();
        let len = shared.query_bytes();
        assert!(query.accepts_len(len));
        assert!(!query.accepts_len(len - 1));
        assert!(query.lengths().contains(&len.to_string()));

        let mut body = vec![0u8; len];
        body[..8].copy_from_slice(&display::query_binding(
            &shards[0].digest,
            DisplayTable::parse("directory-0").unwrap(),
        ));
        let error = query.evaluator().evaluate(&[], &body).unwrap_err();
        assert!(error.contains("does not name this revision"), "{error}");
        body[..8].copy_from_slice(&display::query_binding(
            digest,
            DisplayTable::parse("directory-0").unwrap(),
        ));
        let error = query
            .evaluator()
            .evaluate(&[], &body[..len - 1])
            .unwrap_err();
        assert!(error.contains("wrong fixed length"), "{error}");
    }

    /// A retired snapshot answers only from resident runtimes: with none
    /// resident it is stale before the body queues, and its setup is stale
    /// rather than a cold build.
    #[tokio::test]
    async fn a_retired_snapshot_never_builds() {
        let root = tempfile::tempdir().unwrap();
        let (dir, shards) = publication(root.path());
        let (combined, metrics) = serving(&dir, DisplayRole::Combined, u64::MAX);
        let digest = &shards[1].digest;
        combined.retire();
        assert_eq!(
            refused(combined.prepare_query(false, 1, digest, "directory-0")),
            stale(1, digest)
        );
        let setup = combined
            .prepare_setup(false, 1, digest, "directory-0", 0)
            .unwrap();
        assert_eq!(refused(setup.load().await), stale(1, digest));
        assert_eq!(Metrics::get(&metrics.stale_revisions), 2);
        assert_eq!(combined.cache().entries(), 0);
    }

    /// A request resolved while its snapshot served and admitted after the
    /// snapshot retired never builds: retirement is read when it takes its
    /// runtimes. Without a resident runtime it is the same stale refusal; with
    /// one it is answered from it.
    #[tokio::test]
    async fn retirement_during_admission_is_read_at_acquisition() {
        let root = tempfile::tempdir().unwrap();
        let (dir, shards) = publication(root.path());
        let (combined, metrics) = serving(&dir, DisplayRole::Combined, u64::MAX);
        let digest = &shards[1].digest;
        let query = combined
            .prepare_query(false, 1, digest, "directory-0")
            .unwrap();
        let setup = combined
            .prepare_setup(false, 1, digest, "directory-0", 0)
            .unwrap();
        combined.retire();
        assert_eq!(refused(query.runtime(0).await), stale(1, digest));
        assert_eq!(refused(setup.load().await), stale(1, digest));
        assert_eq!(Metrics::get(&metrics.stale_revisions), 2);
        assert_eq!(combined.cache().entries(), 0);

        // Resident before the retirement: still answered, and nothing built.
        let (serving, _) = serving(&dir, DisplayRole::Combined, u64::MAX);
        let query = serving
            .prepare_query(false, 1, digest, "directory-0")
            .unwrap();
        let resident = query.runtime(0).await.unwrap();
        let waiting = serving
            .prepare_query(false, 1, digest, "directory-0")
            .unwrap();
        serving.retire();
        let answered = waiting.runtime(0).await.unwrap();
        assert!(std::ptr::eq(resident.get(), answered.get()));
        assert_eq!(serving.cache().entries(), 1);
    }

    /// A cache that cannot make room refuses retryably instead of exceeding
    /// its budget.
    #[tokio::test]
    async fn cache_pressure_is_a_capacity_refusal() {
        let root = tempfile::tempdir().unwrap();
        let (dir, shards) = publication(root.path());
        let (combined, _) = serving(&dir, DisplayRole::Combined, 0);
        let query = combined
            .prepare_query(false, 1, &shards[1].digest, "directory-0")
            .unwrap();
        assert_eq!(refused(query.runtime(0).await), ServingError::Capacity);
    }

    /// A managed snapshot without the publisher's prepared files is refused
    /// when it is built, by size, before anything is served.
    #[test]
    fn required_material_is_checked_before_serving() {
        let root = tempfile::tempdir().unwrap();
        let (dir, _) = publication(root.path());
        let metrics = Arc::new(Metrics::default());
        let cache = Arc::new(RuntimeCache::new(u64::MAX, 1, metrics.clone()));
        let set = DisplaySet::open(&dir, 3, DisplayRole::Combined).unwrap();
        let error = DisplayServing::new(set, &DisplayParams::default(), cache, metrics, true)
            .err()
            .expect("missing prepared material");
        assert!(error.contains("required prepared runtime"), "{error}");
    }
}
