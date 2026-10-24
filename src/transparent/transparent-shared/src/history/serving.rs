//! Resolving history requests inside one loaded shard set.
//!
//! **Parameters are shared per geometry, published data is not.** Every shard
//! naming one geometry shares one native parameter set per table, because the
//! native profile is a function of schema, geometry and table alone.
//!
//! **Requests name a revision.** A digest this snapshot no longer holds is
//! stale, so the wallet refreshes its map; a shard the map names but this
//! snapshot is not assigned is a routing fault.

use crate::history::shardset::{LoadedShard, ShardSet, Table, TableRef};
use crate::runtime::disk::ShippedRuntimes;
use crate::runtime::metrics::Metrics;
use crate::runtime::{CacheError, Produced, RuntimeCache, RuntimeHandle, RuntimeKey, SharedParams};
use crate::serving::{Artifact, Prepared, PreparedQuery, PreparedSetup, ServingError};
use std::collections::HashMap;
use std::sync::Arc;
use zakura_pir_protocol::transparent::shard::manifest::query_binding;

/// A runtime's address before the digest is resolved.
type ParamsKey = (&'static str, Table);

/// One loaded history set with its parameters and runtime cache.
pub struct HistoryServing {
    set: ShardSet,
    /// One parameter set per geometry the loaded set actually uses, per table.
    params: HashMap<ParamsKey, Arc<SharedParams>>,
    cache: Arc<RuntimeCache>,
    metrics: Arc<Metrics>,
    /// Runtimes the publisher prepared beside this publication, tried before
    /// a restore or build. `None` when the publication was staged without
    /// them.
    shipped: Option<ShippedRuntimes>,
    max_query_bytes: usize,
}

/// One runtime the prewarm produces.
pub struct WarmTarget {
    cache: Arc<RuntimeCache>,
    key: RuntimeKey,
    shared: Arc<SharedParams>,
    source: crate::history::shardset::SegmentSource,
    shipped: Option<ShippedRuntimes>,
}

impl WarmTarget {
    /// Finds, loads, restores or builds the runtime under the background
    /// slots, saying how it was produced.
    pub async fn produce(&self) -> Result<(RuntimeHandle, Produced), CacheError> {
        self.cache
            .get_background_from(
                self.key.clone(),
                self.shared.clone(),
                self.source.clone(),
                self.shipped.clone(),
            )
            .await
    }
}

impl HistoryServing {
    /// Derives the parameters of every geometry the set names, held here or
    /// not, reusing `previous`'s: the init document is set-wide and a wallet
    /// refuses a map it does not fully declare.
    pub fn new(
        set: ShardSet,
        cache: Arc<RuntimeCache>,
        metrics: Arc<Metrics>,
        shipped: Option<ShippedRuntimes>,
        previous: Option<&Self>,
    ) -> Result<Self, String> {
        let mut params: HashMap<ParamsKey, Arc<SharedParams>> = HashMap::new();
        let mut max_query_bytes = 0usize;
        for geometry in set.geometries() {
            for table in Table::HISTORY {
                let shared = match previous.and_then(|s| s.params.get(&(geometry.name, table))) {
                    Some(shared) => shared.clone(),
                    None => Arc::new(SharedParams::build(geometry, table)?),
                };
                max_query_bytes = max_query_bytes.max(shared.query_bytes());
                params.insert((geometry.name, table), shared);
            }
        }
        if params.is_empty() {
            return Err("the shard set names no geometry to serve".into());
        }
        Ok(Self {
            set,
            params,
            cache,
            metrics,
            shipped,
            max_query_bytes,
        })
    }

    /// The largest upload any approved history geometry accepts, rounded to
    /// a kibibyte: derives parameters only, constructing no runtime.
    pub fn largest_query_bytes() -> Result<u64, String> {
        let mut largest = 0u64;
        for geometry in zakura_pir_protocol::transparent::shard::layout::PROFILES {
            for table in Table::HISTORY {
                let bytes = SharedParams::build(geometry, table)?.query_bytes() as u64;
                largest = largest.max(bytes.div_ceil(1024) * 1024);
            }
        }
        Ok(largest)
    }

    pub fn set(&self) -> &ShardSet {
        &self.set
    }

    pub fn cache(&self) -> &Arc<RuntimeCache> {
        &self.cache
    }

    pub fn shipped(&self) -> Option<&ShippedRuntimes> {
        self.shipped.as_ref()
    }

    /// The longest body any geometry this set serves can legitimately send.
    pub fn max_query_bytes(&self) -> usize {
        self.max_query_bytes
    }

    /// One geometry's parameters for one table.
    pub fn params(&self, geometry: &'static str, table: Table) -> Option<&Arc<SharedParams>> {
        self.params.get(&(geometry, table))
    }

    fn shared(&self, shard: &LoadedShard, table: Table) -> Arc<SharedParams> {
        self.params
            .get(&(shard.geometry.name, table))
            .expect("every held geometry has parameters")
            .clone()
    }

    fn key(shard: &LoadedShard, table: TableRef, segment: u32) -> RuntimeKey {
        // A page shard's runtime is keyed by its own identity, so a sealed
        // one is shared by every revision of its directory shard.
        (
            shard.runtime_identity(table).to_string(),
            table.kind(),
            segment,
        )
    }

    fn prepared(&self, shard: &LoadedShard, table: TableRef) -> Prepared {
        Prepared {
            cache: self.cache.clone(),
            shared: self.shared(shard, table.kind()),
            metrics: self.metrics.clone(),
        }
    }

    /// Resolves a revision the request named.
    ///
    /// A digest this snapshot does not hold is not a malformed request: it is
    /// a wallet holding a revision since superseded and pruned.
    fn revision(&self, shard_id: u64, digest: &str) -> Result<&LoadedShard, ServingError> {
        match self.set.revision(digest) {
            Some(shard) if shard.manifest.shard_id == shard_id => Ok(shard),
            // A digest that exists but belongs to another shard is a routing
            // error, not a stale client.
            Some(_) => Err(ServingError::Invalid(format!(
                "revision {digest} is not a revision of shard {shard_id}"
            ))),
            None => {
                // A shard the map names but this snapshot is not assigned is
                // a routing fault: refreshing the map would not help.
                if self.set.names(shard_id) && !self.set.is_assigned(shard_id) {
                    Metrics::incr(&self.metrics.unassigned_refusals);
                    let worker_id = self
                        .set
                        .scope()
                        .map(|scope| scope.worker_id.clone())
                        .unwrap_or_default();
                    return Err(ServingError::WrongHoldings(format!(
                        "shard {shard_id} is not assigned to worker {worker_id}"
                    )));
                }
                Metrics::incr(&self.metrics.stale_revisions);
                Err(stale(shard_id, digest))
            }
        }
    }

    /// A query to every segment of one revision's table.
    ///
    /// Every segment answers the same query, in segment order: asking only
    /// the segment holding the script would disclose its placement.
    pub fn prepare_query(
        &self,
        shard_id: u64,
        digest: &str,
        table: TableRef,
    ) -> Result<PreparedQuery, ServingError> {
        let shard = self.revision(shard_id, digest)?;
        let segments = shard.segments(table);
        if segments == 0 {
            return Err(ServingError::Invalid("table capability unavailable".into()));
        }
        let shared = self.shared(shard, table.kind());
        // A page shard's queries may select over only the rows its manifest
        // entry says can hold data; the directory selects every row.
        let query_rows = shard.query_rows(table, shared.profile.rows);
        let sources = (0..segments)
            .map(|segment| {
                shard
                    .segment(table, segment)
                    .cloned()
                    .map(|source| (Self::key(shard, table, segment), source))
                    .ok_or_else(|| missing(shard, table, segment))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.prepared(shard, table)
            .build(sources, self.shipped.clone())
            .query(
                query_rows,
                query_binding(digest, &table.name()),
                table.name(),
                digest.to_string(),
            )
    }

    /// The public setup of one segment of one revision's table.
    pub fn prepare_setup(
        &self,
        shard_id: u64,
        digest: &str,
        table: TableRef,
        segment: u32,
    ) -> Result<PreparedSetup, ServingError> {
        let shard = self.revision(shard_id, digest)?;
        let source = shard
            .segment(table, segment)
            .cloned()
            .ok_or_else(|| missing(shard, table, segment))?;
        Ok(self
            .prepared(shard, table)
            .build(
                vec![(Self::key(shard, table, segment), source)],
                self.shipped.clone(),
            )
            .setup(segment, shard.segments(table), table.name(), None))
    }

    /// One current runtime the prewarm should produce, if this set still
    /// holds `digest`.
    pub fn warm_target(&self, digest: &str, table: TableRef, segment: u32) -> Option<WarmTarget> {
        let shard = self.set.revision(digest)?;
        let source = shard.segment(table, segment).cloned()?;
        Some(WarmTarget {
            cache: self.cache.clone(),
            key: Self::key(shard, table, segment),
            shared: self.shared(shard, table.kind()),
            source,
            shipped: self.shipped.clone(),
        })
    }

    /// Checks required prepared files by size, without loading runtimes or
    /// scanning their contents.
    pub fn check_required_material(&self) -> Result<usize, String> {
        let shipped = self
            .shipped
            .as_ref()
            .ok_or("provider requires prepared runtimes")?;
        let mut checked = 0;
        for shard in self.set.current() {
            for table in shard.tables() {
                let shared = self.shared(shard, table.kind());
                for segment in 0..shard.segments(table) {
                    let source = shard
                        .segment(table, segment)
                        .ok_or("missing runtime source")?;
                    shipped
                        .check_size(&Self::key(shard, table, segment), &shared, source.sha256())
                        .map_err(|e| {
                            format!(
                                "required prepared runtime for shard {} segment {segment}: {e}",
                                shard.manifest.shard_id
                            )
                        })?;
                    checked += 1;
                }
            }
        }
        Ok(checked)
    }

    /// Every source table and shipped runtime file of every held revision,
    /// retained predecessors included: a wallet can still name those.
    pub fn artifacts(&self) -> Vec<Artifact> {
        let mut artifacts = Vec::new();
        for shard in self.set.revisions() {
            for table in shard.tables() {
                let shared = self.shared(shard, table.kind());
                for segment in 0..shard.segments(table) {
                    let Some(source) = shard.segment(table, segment) else {
                        continue;
                    };
                    let key = Self::key(shard, table, segment);
                    let artifact = |path| Artifact {
                        digest: shard.digest.clone(),
                        shard_id: shard.manifest.shard_id,
                        path,
                        key: key.clone(),
                    };
                    artifacts.push(artifact(source.path().to_path_buf()));
                    if let Some(shipped) = &self.shipped {
                        artifacts.push(artifact(shipped.path(&key, &shared, source.sha256())));
                    }
                }
            }
        }
        artifacts
    }

    /// Reclaims idle runtimes of revisions this set no longer serves as
    /// current; its current runtimes stay resident obligations.
    pub fn evict_unpinned_except_current(&self) {
        let current: std::collections::HashSet<String> = self
            .set
            .current()
            .flat_map(|shard| shard.runtime_identities())
            .collect();
        self.cache
            .evict_unpinned_except(|(identity, _, _)| current.contains(identity));
    }
}

fn missing(shard: &LoadedShard, table: TableRef, segment: u32) -> ServingError {
    ServingError::Invalid(format!(
        "shard {} has no {} segment {segment}",
        shard.manifest.shard_id,
        table.name()
    ))
}

/// The revision named is no longer served: the wallet refreshes its map and
/// re-derives the shard's coverage.
pub fn stale(shard_id: u64, digest: &str) -> ServingError {
    ServingError::Stale {
        message: format!("revision {digest} of shard {shard_id} is no longer served"),
        retry: "refresh the shard map and re-derive coverage for this shard",
    }
}
