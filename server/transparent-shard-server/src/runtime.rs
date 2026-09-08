//! Prepared PIR state, and the budget it lives inside.
//!
//! A shard's plaintext table is a few tens of megabytes. The state needed to
//! *answer queries against it* is several times that, and building it takes
//! about a second. The predecessor of this module built that state on first use
//! and kept it forever, which has two failure modes that only appear at fleet
//! size.
//!
//! **It could not be bounded.** Nothing evicted, so a worker's memory was
//! decided by how many distinct shards its traffic happened to touch. The unit
//! file's `MemoryMax` was the only limit, and reaching it is an OOM kill rather
//! than a miss.
//!
//! **It duplicated work under concurrency.** The build ran outside the lock, so
//! N requests arriving together for a cold shard each built a full runtime and
//! discarded all but one — the worst possible behaviour exactly when the worker
//! is busiest.
//!
//! Both are fixed here: a byte-bounded LRU, and one build per identity that
//! every other caller waits on.
//!
//! # Reserving before building
//!
//! Room is reserved from the budget *before* a build starts, from the size the
//! geometry implies, because the build itself is what allocates. Checking
//! afterwards would mean the budget was only ever exceeded, never enforced.
//!
//! If the budget cannot be freed — because everything resident is pinned by a
//! request in flight — the caller is told so explicitly and can retry. That is
//! worse service than an unbounded cache gives, and it is the point: the
//! alternative is not "better service", it is being killed.
//!
//! # Nothing is evicted while it is in use
//!
//! A caller holds a [`RuntimeHandle`] for as long as it needs the runtime, and
//! the cache treats an entry as pinned while any handle to it exists. Dropping
//! the accounting for a runtime a request is still evaluating against would
//! free budget that is not free.

use crate::metrics::Metrics;
use crate::shardset::{SegmentSource, Table};
use enhance_pir_server::ipir::RowPlaintextIter;
use inspiring::{QueryPackPreprocessed, RlweParams, TopKeyImages};
use ipir_sp::serialize::serialized_packing_keys_len;
use ipir_sp::server::IPIRServer;
use ipir_sp::server::{
    build_pack_preprocessed_blocks, pack_intermediate_blocks, published_c1_rows,
};
use ipir_sp::{IPIRClient, YpirSchemeParams};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use transparent_shard::layout::Geometry;

/// Bytes one prepared runtime reserves at these parameters.
///
/// Two terms, both derived rather than measured:
///
/// - the **encoded database**, `db_rows * db_cols` of `u16`, which is the only
///   term that follows the row count; and
/// - the **pack matrices**, three per block of `d x d` 64-bit coefficients,
///   which follow `db_cols` and the RLWE degree instead. `shard-scaling
///   --geometry-sweep` prices the same two terms the same way, so a sweep's
///   projection and a running worker's budget cannot drift apart.
///
/// At the pinned geometry this is 32 MiB of database and 96 MiB of matrices,
/// which is what `shard-residency` measured as the 128.5 MiB marginal cost of
/// one held runtime.
///
/// It is a **reservation**, not a measurement of the process. It excludes
/// allocator fragmentation, the transient plaintext buffer a build reads, the
/// shared parameters, and anything a request allocates. A cache budget is
/// therefore always set below the host's real headroom, and re-checked with
/// `shard-residency` whenever the scheme moves — an estimate that drifted low
/// would turn a bounded cache back into an unbounded one.
pub fn reserved_bytes(rlwe: &RlweParams, scheme: &YpirSchemeParams) -> u64 {
    let database = scheme.db_rows as u64 * scheme.db_cols as u64 * 2;
    let blocks = (scheme.db_cols / rlwe.d) as u64;
    let pack = blocks * 3 * rlwe.d as u64 * rlwe.d as u64 * 8;
    database + pack
}

/// The parameters every shard of one geometry and table shares.
///
/// `params_for_simplepir` is a pure function of `(rows, item_size_bits)`, so
/// every shard naming the same geometry shares one of these and a client
/// validates it once. A set mixing archive and recent shards leaks one per
/// geometry per table — at most eight for the whole registry — rather than one
/// per shard, which is what naming geometries rather than choosing them per
/// shard is for.
pub struct SharedParams {
    pub geometry: &'static Geometry,
    pub table: Table,
    pub rlwe: &'static RlweParams,
    pub scheme: YpirSchemeParams,
    pub top_key_images: TopKeyImages<'static>,
    pub setup_seed: u64,
}

impl SharedParams {
    pub fn build(geometry: &'static Geometry, table: Table) -> Result<Self, String> {
        let rows = table.rows(geometry);
        let row_bits = (table.row_bytes(geometry) as u64) * 8;
        let (rlwe, scheme) =
            ipir_sp::params_for_simplepir(rows, row_bits).map_err(|error| error.to_string())?;
        let rlwe: &'static RlweParams = Box::leak(Box::new(rlwe));
        let top_key_images = TopKeyImages::build(rlwe);
        Ok(Self {
            geometry,
            table,
            rlwe,
            scheme,
            top_key_images,
            setup_seed: crate::shardset::setup_seed(geometry, table),
        })
    }

    /// The exact length every query body must have.
    ///
    /// Fixed, because a body whose size varied with the selection would leak it
    /// through its length alone.
    pub fn query_bytes(&self) -> usize {
        8 + serialized_packing_keys_len(self.rlwe)
            + (self.scheme.db_rows * self.scheme.query_bits).div_ceil(8)
    }

    pub fn reserved_bytes(&self) -> u64 {
        reserved_bytes(self.rlwe, &self.scheme)
    }
}

/// One segment of one shard revision's table, prepared to answer queries.
pub struct TableRuntime {
    preprocessed: Vec<QueryPackPreprocessed<'static>>,
    server: IPIRServer<u16>,
    pub public_params: Vec<u8>,
    pub public_params_sha256: String,
    pub public_params_epoch: [u8; 8],
}

impl TableRuntime {
    /// Builds the runtime for one segment.
    ///
    /// `rows` is the segment's verified plaintext. It is borrowed, not kept:
    /// what survives is the encoded database inside the server, and holding the
    /// plaintext beside it would double the resident cost of every shard for no
    /// benefit.
    pub fn build(shared: &SharedParams, rows: &[u8]) -> Result<Self, String> {
        let coefficients = RowPlaintextIter::new(
            rows,
            shared.table.row_bytes(shared.geometry) as usize,
            shared.scheme.db_rows,
            shared.scheme.db_cols,
            shared.scheme.p.trailing_zeros() as usize,
        );
        let server =
            IPIRServer::<u16>::new_auto_kernel(shared.scheme.clone(), coefficients, false, true);

        // The setup is derived from a published seed, so a client reproduces it
        // exactly. It is public: it carries no secret and no selection.
        let mut seed = [0u8; 32];
        seed[..8].copy_from_slice(&shared.setup_seed.to_le_bytes());
        let setup = IPIRClient::new(shared.rlwe, &shared.scheme)
            .generate_public_query_setup_simplepir_from_seed(seed);
        let crs_blocks = server
            .perform_offline_precomputation_simplepir(shared.rlwe, &setup)
            .crs_blocks;
        let preprocessed =
            build_pack_preprocessed_blocks(shared.rlwe, &crs_blocks).map_err(|e| e.to_string())?;
        let public_params = published_c1_rows(&preprocessed, shared.rlwe.q);
        let digest = Sha256::digest(&public_params);
        let mut epoch = [0u8; 8];
        epoch.copy_from_slice(&digest[..8]);

        Ok(Self {
            preprocessed,
            server,
            public_params,
            public_params_sha256: hex::encode(digest),
            public_params_epoch: epoch,
        })
    }

    /// Answers one query. The row selected is never known to this function.
    ///
    /// `binding` is the prefix the query must carry and the response repeats:
    /// it names the revision and table this runtime belongs to, so a body
    /// routed to the wrong one fails here rather than returning rows from a
    /// range the wallet did not ask about.
    pub fn evaluate(
        &self,
        shared: &SharedParams,
        binding: [u8; 8],
        body: &[u8],
    ) -> Result<Vec<u8>, String> {
        if body.get(..8) != Some(binding.as_slice()) {
            return Err("query does not name this revision and table".to_string());
        }
        // A fixed length for every query: a body that varied with the selection
        // would leak through its size alone.
        if body.len() != shared.query_bytes() {
            return Err("query has the wrong fixed length".to_string());
        }
        let packing_len = serialized_packing_keys_len(shared.rlwe);
        let packing_keys =
            ipir_sp::serialize::deserialize_packing_keys(shared.rlwe, &body[8..8 + packing_len])
                .map_err(|e| e.to_string())?;
        let query = enhance_pir_server::ipir::deserialize_first_dim_query(
            shared.rlwe,
            &shared.scheme,
            &body[8 + packing_len..],
        )
        .map_err(|e| e.to_string())?;
        let intermediate = self.server.multiply_query(shared.rlwe, &query);
        let packed = pack_intermediate_blocks(
            &intermediate,
            &packing_keys,
            &shared.top_key_images,
            &self.preprocessed,
        )
        .map_err(|e| e.to_string())?;
        let c2 = ipir_sp::modulus_switch::serialize_rlwe_response_bodies(
            &packed,
            shared.scheme.q_prime_1,
        );
        let mut response = Vec::with_capacity(16 + c2.len());
        response.extend_from_slice(&binding);
        response.extend_from_slice(&self.public_params_epoch);
        response.extend_from_slice(&c2);
        Ok(response)
    }
}

/// A runtime's address: which revision of which shard, which table, which
/// segment.
///
/// Keyed by the manifest digest rather than the shard id, because a republished
/// tail is a different revision of the same shard with different bytes. Keying
/// by id would let a query prepared against one revision be answered from
/// another's runtime, which is precisely the confusion the digest exists to
/// prevent.
pub type RuntimeKey = (String, Table, u32);

/// Why a runtime could not be produced.
#[derive(Debug)]
pub enum CacheError {
    /// The budget has no room that can be freed right now. Retryable.
    Overloaded,
    /// The segment's bytes could not be read, verified, or prepared.
    Failed(String),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::Overloaded => write!(f, "no cache capacity is free; retry shortly"),
            CacheError::Failed(error) => write!(f, "{error}"),
        }
    }
}

/// A borrowed runtime.
///
/// Holding one pins its cache entry. The cache decides an entry is in use by
/// whether any handle to it exists, so a handle must live for exactly as long
/// as the runtime is being read — no longer, or the cache cannot reclaim it,
/// and no shorter, or it could be reclaimed mid-query.
#[derive(Clone)]
pub struct RuntimeHandle {
    slot: Arc<Slot>,
}

impl RuntimeHandle {
    pub fn get(&self) -> &TableRuntime {
        self.slot
            .runtime
            .get()
            .expect("a handle is only issued once its slot is built")
    }
}

/// One cache entry's shared state.
struct Slot {
    runtime: tokio::sync::OnceCell<TableRuntime>,
}

struct Entry {
    slot: Arc<Slot>,
    reserved: u64,
    last_used: u64,
}

struct Inner {
    entries: HashMap<RuntimeKey, Entry>,
    resident: u64,
    clock: u64,
}

/// A byte-bounded, single-flight cache of prepared runtimes.
pub struct RuntimeCache {
    inner: Mutex<Inner>,
    budget: u64,
    build_slots: tokio::sync::Semaphore,
    metrics: Arc<Metrics>,
}

impl RuntimeCache {
    pub fn new(budget: u64, build_slots: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                resident: 0,
                clock: 0,
            }),
            budget,
            build_slots: tokio::sync::Semaphore::new(build_slots.max(1)),
            metrics,
        }
    }

    pub fn budget(&self) -> u64 {
        self.budget
    }

    /// The runtime for one key, building it if this is its first use.
    ///
    /// At most one build runs per key: the slot is published to the map before
    /// the build begins, so a second caller finds it and waits on the same
    /// `OnceCell` rather than starting its own.
    pub async fn get(
        &self,
        key: RuntimeKey,
        shared: Arc<SharedParams>,
        source: SegmentSource,
    ) -> Result<RuntimeHandle, CacheError> {
        let need = shared.reserved_bytes();
        let (slot, fresh) = self.slot_for(key.clone(), need)?;
        if !fresh {
            // Either it is already built, or another request is building it.
            // Both are the same wait, and neither starts a second build.
            if slot.runtime.initialized() {
                Metrics::incr(&self.metrics.cache_hits);
            } else {
                Metrics::incr(&self.metrics.build_coalesced);
            }
        } else {
            Metrics::incr(&self.metrics.cache_misses);
        }

        let handle = RuntimeHandle { slot: slot.clone() };
        let built = slot
            .runtime
            .get_or_try_init(|| async {
                let _permit = self
                    .build_slots
                    .acquire()
                    .await
                    .map_err(|_| CacheError::Failed("server is shutting down".into()))?;
                let started = std::time::Instant::now();
                let result = tokio::task::spawn_blocking(move || {
                    // Read and re-verify here rather than at startup only: a
                    // file replaced or truncated since must fail the build, not
                    // be packed into a runtime and served.
                    let bytes = source.load().map_err(|error| error.to_string())?;
                    TableRuntime::build(&shared, &bytes)
                })
                .await
                .map_err(|error| CacheError::Failed(error.to_string()))?
                .map_err(CacheError::Failed);
                match &result {
                    Ok(_) => {
                        Metrics::incr(&self.metrics.builds);
                        Metrics::add(
                            &self.metrics.build_micros,
                            started.elapsed().as_micros() as u64,
                        );
                        self.metrics.build_seconds.observe(started.elapsed());
                    }
                    Err(_) => Metrics::incr(&self.metrics.build_failures),
                }
                result
            })
            .await;

        if built.is_err() {
            // A failed build leaves nothing to serve, so the reservation is
            // released and the next request may try again. Keeping the entry
            // would hold budget for a runtime that does not exist.
            self.forget(&key);
        }
        built.map(|_| handle)
    }

    /// Finds or creates the slot for `key`, reserving `need` bytes if it is new.
    ///
    /// Returns whether the slot was created by this call, which is what
    /// distinguishes a miss from a request that joined an in-flight build.
    fn slot_for(&self, key: RuntimeKey, need: u64) -> Result<(Arc<Slot>, bool), CacheError> {
        let mut inner = self.inner.lock().expect("runtime cache");
        inner.clock += 1;
        let now = inner.clock;
        if let Some(entry) = inner.entries.get_mut(&key) {
            entry.last_used = now;
            return Ok((entry.slot.clone(), false));
        }

        // Make room before building, not after: the build is what allocates,
        // so a budget checked afterwards is a budget that is only ever
        // exceeded.
        while inner.resident + need > self.budget {
            let Some(victim) = Self::least_recently_used_unpinned(&inner) else {
                Metrics::incr(&self.metrics.overloads);
                return Err(CacheError::Overloaded);
            };
            let entry = inner.entries.remove(&victim).expect("victim exists");
            inner.resident -= entry.reserved;
            Metrics::incr(&self.metrics.evictions);
        }

        let slot = Arc::new(Slot {
            runtime: tokio::sync::OnceCell::new(),
        });
        inner.entries.insert(
            key,
            Entry {
                slot: slot.clone(),
                reserved: need,
                last_used: now,
            },
        );
        inner.resident += need;
        self.publish(&inner);
        Ok((slot, true))
    }

    /// The coldest entry no request is currently holding.
    ///
    /// An entry whose slot has other owners is pinned: a request is either
    /// evaluating against it or waiting for it to finish building. Evicting
    /// either would release budget for memory that is still allocated, which is
    /// the accounting error a bounded cache exists to avoid.
    fn least_recently_used_unpinned(inner: &Inner) -> Option<RuntimeKey> {
        inner
            .entries
            .iter()
            .filter(|(_, entry)| Arc::strong_count(&entry.slot) == 1)
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| key.clone())
    }

    fn forget(&self, key: &RuntimeKey) {
        let mut inner = self.inner.lock().expect("runtime cache");
        if let Some(entry) = inner.entries.remove(key) {
            inner.resident -= entry.reserved;
        }
        self.publish(&inner);
    }

    fn publish(&self, inner: &Inner) {
        Metrics::set(&self.metrics.resident_bytes, inner.resident);
        Metrics::set(&self.metrics.cache_entries, inner.entries.len() as u64);
    }

    /// Bytes currently reserved, for health reporting and tests.
    pub fn resident_bytes(&self) -> u64 {
        self.inner.lock().expect("runtime cache").resident
    }

    /// Entries currently held, for health reporting and tests.
    pub fn entries(&self) -> usize {
        self.inner.lock().expect("runtime cache").entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_shard::layout::{ARCHIVE_WIDE, RECENT_8K};

    fn reservation(rows: u64, row_bytes: usize) -> u64 {
        let (rlwe, scheme) = ipir_sp::params_for_simplepir(rows, row_bytes as u64 * 8)
            .expect("a registry geometry has parameters");
        reserved_bytes(&rlwe, &scheme)
    }

    /// The reservation must match what `shard-residency` measured at the pinned
    /// geometry: a 32 MiB database plus 96 MiB of pack matrices, 128.5 MiB per
    /// table segment and about 257 MiB per shard.
    ///
    /// If this fails, the cache budget is being computed against a different
    /// scheme than the one deployed, and every sizing decision downstream of it
    /// is wrong. Re-measure with `shard-residency` rather than adjusting the
    /// formula to match.
    #[test]
    fn the_reservation_matches_what_residency_measured() {
        assert_eq!(
            reservation(RECENT_8K.directory_rows, RECENT_8K.directory_row_bytes),
            (32 << 20) + (96 << 20)
        );
    }

    /// Only the database term follows the row count: the pack matrices follow
    /// `db_cols`, which the scheme holds at 2,048 across the registry. That is
    /// why a wider table is nearly free in memory while it divides the shard
    /// count, and it is the whole basis of the archive tier.
    #[test]
    fn only_the_database_term_follows_the_row_count() {
        let narrow = reservation(RECENT_8K.page_rows, RECENT_8K.page_row_bytes);
        let wide = reservation(ARCHIVE_WIDE.page_rows, ARCHIVE_WIDE.page_row_bytes);
        let pack = 96 << 20;
        let ratio = ARCHIVE_WIDE.page_rows / RECENT_8K.page_rows;
        assert_eq!(wide - pack, (narrow - pack) * ratio);
    }

    /// Every registry geometry must have parameters and a reservation that
    /// fits a plausible budget. A geometry whose runtime did not fit one worker
    /// could never be served at all, and finding that out at first query would
    /// be finding it out in production.
    #[test]
    fn every_registry_geometry_reserves_something_servable() {
        for geometry in transparent_shard::layout::PROFILES {
            for (rows, row_bytes) in [
                (geometry.directory_rows, geometry.directory_row_bytes),
                (geometry.page_rows, geometry.page_row_bytes),
            ] {
                let bytes = reservation(rows, row_bytes);
                assert!(bytes > 0, "{}", geometry.name);
                assert!(
                    bytes < (2 << 30),
                    "{} reserves {bytes} bytes for one segment",
                    geometry.name
                );
            }
        }
    }
}
