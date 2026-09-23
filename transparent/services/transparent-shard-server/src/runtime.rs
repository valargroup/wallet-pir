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

pub mod disk;

use crate::metrics::Metrics;
use crate::shardset::{SegmentSource, Table};
use enhance_pir_server::ipir::RowPlaintextIter;
use inspiring::{QueryPackPreprocessed, RlweParams, TopKeyImages};
use ipir_sp::serialize::serialized_packing_keys_len;
use ipir_sp::server::IPIRServer;
use ipir_sp::server::{
    build_pack_preprocessed_blocks_with_top, pack_intermediate_blocks, published_c1_rows,
};
use ipir_sp::{IPIRClient, YpirSchemeParams};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use transparent_shard::layout::Geometry;

/// Explicit build policy for reproducible hardware-path comparisons. The
/// portable feature selects the library's existing fallback; it changes no
/// parameters or wire encoding and never depends on a client request.
pub const KERNEL_POLICY: &str = if cfg!(feature = "portable-kernel") {
    "chunked-split"
} else {
    "auto"
};

fn database_server(
    shared: &SharedParams,
    coefficients: impl Iterator<Item = u16>,
    transposed: bool,
) -> IPIRServer<u16> {
    #[cfg(feature = "portable-kernel")]
    let server = IPIRServer::<u16>::new(shared.scheme.clone(), coefficients, transposed, true);
    #[cfg(not(feature = "portable-kernel"))]
    let server =
        IPIRServer::<u16>::new_auto_kernel(shared.scheme.clone(), coefficients, transposed, true);
    server
}

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
        let mut phase = std::time::Instant::now();
        let coefficients = RowPlaintextIter::new(
            rows,
            shared.table.row_bytes(shared.geometry) as usize,
            shared.scheme.db_rows,
            shared.scheme.db_cols,
            shared.scheme.p.trailing_zeros() as usize,
        );
        let server = database_server(shared, coefficients, false);

        tracing::debug!(
            geometry = shared.geometry.name,
            table = shared.table.as_str(),
            seconds = phase.elapsed().as_secs_f64(),
            stage = "encode_database",
            "construction stage"
        );
        phase = std::time::Instant::now();
        // The setup is derived from a published seed, so a client reproduces it
        // exactly. It is public: it carries no secret and no selection.
        let mut seed = [0u8; 32];
        seed[..8].copy_from_slice(&shared.setup_seed.to_le_bytes());
        let setup = IPIRClient::from_profile(
            shared.scheme.num_items,
            shared.scheme.item_size_bits,
            ipir_sp::SimplePirProfile::P14,
        )
        .map_err(|error| error.to_string())?
        .generate_public_query_setup_simplepir_from_seed(seed);
        tracing::debug!(
            geometry = shared.geometry.name,
            table = shared.table.as_str(),
            seconds = phase.elapsed().as_secs_f64(),
            stage = "public_setup",
            "construction stage"
        );
        phase = std::time::Instant::now();
        let crs_blocks = server
            .perform_offline_precomputation_simplepir(shared.rlwe, setup.polys())
            .crs_blocks;
        tracing::debug!(
            geometry = shared.geometry.name,
            table = shared.table.as_str(),
            seconds = phase.elapsed().as_secs_f64(),
            stage = "hint_columns",
            "construction stage"
        );
        phase = std::time::Instant::now();
        let preprocessed = build_pack_preprocessed_blocks_with_top(
            shared.rlwe,
            &crs_blocks,
            &shared.top_key_images,
        )
        .map_err(|e| e.to_string())?;
        tracing::debug!(
            geometry = shared.geometry.name,
            table = shared.table.as_str(),
            seconds = phase.elapsed().as_secs_f64(),
            stage = "pack_preprocessing",
            "construction stage"
        );
        phase = std::time::Instant::now();
        let public_params = published_c1_rows(&preprocessed, shared.rlwe.q);
        let digest = Sha256::digest(&public_params);
        let mut epoch = [0u8; 8];
        epoch.copy_from_slice(&digest[..8]);

        tracing::debug!(
            geometry = shared.geometry.name,
            table = shared.table.as_str(),
            seconds = phase.elapsed().as_secs_f64(),
            stage = "publish_parameters",
            "construction stage"
        );
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

/// The pending gauge drops only after all writer-owned allocations and pins.
struct PendingSave(Arc<Metrics>);
impl Drop for PendingSave {
    fn drop(&mut self) {
        Metrics::sub(&self.0.disk_save_pending, 1);
    }
}

/// One cache entry's shared state.
struct Slot {
    runtime: tokio::sync::OnceCell<Arc<TableRuntime>>,
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
    pub(crate) work_memory: Arc<crate::memory::WorkMemory>,
    inner: Mutex<Inner>,
    budget: u64,
    build_slots: Arc<tokio::sync::Semaphore>,
    restore_slots: Arc<tokio::sync::Semaphore>,
    prewarm_concurrency: usize,
    metrics: Arc<Metrics>,
    disk: Option<disk::DiskCache>,
}

impl RuntimeCache {
    pub fn new(budget: u64, build_slots: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            work_memory: Arc::new(crate::memory::WorkMemory::default()),
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                resident: 0,
                clock: 0,
            }),
            budget,
            build_slots: Arc::new(tokio::sync::Semaphore::new(build_slots.max(1))),
            restore_slots: Arc::new(tokio::sync::Semaphore::new(1)),
            prewarm_concurrency: build_slots.max(1),
            metrics,
            disk: None,
        }
    }

    pub fn with_disk(mut self, disk: Option<disk::DiskCache>) -> Self {
        let restores = disk.as_ref().map_or(1, |disk| disk.restore_slots.max(1));
        self.restore_slots = Arc::new(tokio::sync::Semaphore::new(restores));
        self.prewarm_concurrency = self.prewarm_concurrency.max(restores);
        self.disk = disk;
        self
    }

    pub fn prewarm_concurrency(&self) -> usize {
        self.prewarm_concurrency
    }

    pub fn disk_status(&self) -> serde_json::Value {
        self.disk.as_ref().map_or(serde_json::Value::Null, |disk|
            serde_json::json!({"bytes":disk.used_bytes().ok(),"limit_bytes":disk.max_bytes,
                "hits":Metrics::get(&self.metrics.disk_hits),"misses":Metrics::get(&self.metrics.disk_misses),
                "write_failures":Metrics::get(&self.metrics.disk_write_failures),
                "pending_saves":Metrics::get(&self.metrics.disk_save_pending),
                "restore_slots":disk.restore_slots.max(1)}))
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
                let result = self
                    .load_or_build(key.clone(), shared, source, slot.clone())
                    .await;
                if result.is_err() {
                    Metrics::incr(&self.metrics.build_failures);
                }
                result
            })
            .await;

        if built.is_err() {
            // A failed build leaves nothing to serve, so the reservation is
            // released and the next request may try again. Keeping the entry
            // would hold budget for a runtime that does not exist.
            drop(handle);
            self.forget(&key, &slot);
            return built.map(|_| unreachable!("failed initialization"));
        }
        Ok(handle)
    }

    /// Restores use streaming buffers and their own concurrency bound. A miss
    /// releases that slot before waiting for a cold-build slot. Owned permits
    /// stay with blocking work even if its asynchronous caller is cancelled.
    async fn load_or_build(
        &self,
        key: RuntimeKey,
        shared: Arc<SharedParams>,
        source: SegmentSource,
        pin: Arc<Slot>,
    ) -> Result<Arc<TableRuntime>, CacheError> {
        let attempt = std::time::Instant::now();
        if let Some(disk) = self.disk.clone() {
            let permit = self
                .restore_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| CacheError::Failed("server is shutting down".into()))?;
            tracing::debug!(key = ?key, seconds = attempt.elapsed().as_secs_f64(), stage = "restore_slot", "runtime stage");
            let memory = self
                .work_memory
                .reserve(shared.reserved_bytes().saturating_mul(2))
                .ok_or_else(|| {
                    tracing::debug!(key = ?key, stage = "restore_admission", "runtime admission denied");
                    CacheError::Overloaded
                })?;
            let restore_pin = pin.clone();
            let restore_key = key.clone();
            let restore_shared = shared.clone();
            let restore_source = source.clone();
            let metrics = self.metrics.clone();
            let restored =
                tokio::task::spawn_blocking(move || -> Result<Option<TableRuntime>, CacheError> {
                    let _memory = memory;
                    let _permit = permit;
                    let _pin = restore_pin;
                    let started = std::time::Instant::now();
                    let restored = disk.load(&restore_key, &restore_shared, &restore_source.sha256);
                    tracing::debug!(key = ?restore_key, seconds = started.elapsed().as_secs_f64(), hit = restored.is_ok(), stage = "disk_restore", "runtime stage");
                    match restored {
                        Ok(runtime) => {
                            // A cache hit must not conceal a source changed since startup.
                            let verify_started = std::time::Instant::now();
                            restore_source
                                .verify()
                                .map_err(|error| CacheError::Failed(error.to_string()))?;
                            tracing::debug!(key = ?restore_key, seconds = verify_started.elapsed().as_secs_f64(), stage = "source_verify", "runtime stage");
                            Metrics::incr(&metrics.disk_hits);
                            Metrics::add(
                                &metrics.disk_load_micros,
                                started.elapsed().as_micros() as u64,
                            );
                            tracing::info!(
                                seconds = started.elapsed().as_secs_f64(),
                                "restored runtime cache"
                            );
                            Ok(Some(runtime))
                        }
                        Err(error) => {
                            Metrics::incr(&metrics.disk_misses);
                            if error.kind() != std::io::ErrorKind::NotFound {
                                tracing::warn!(%error, "runtime cache rejected; rebuilding");
                            }
                            Ok(None)
                        }
                    }
                })
                .await
                .map_err(|error| CacheError::Failed(error.to_string()))??;
            if let Some(runtime) = restored {
                return Ok(Arc::new(runtime));
            }
        }
        let slot_started = std::time::Instant::now();
        let permit = self
            .build_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| CacheError::Failed("server is shutting down".into()))?;
        tracing::debug!(key = ?key, seconds = slot_started.elapsed().as_secs_f64(), stage = "build_slot", "runtime stage");
        // Includes the new runtime and conservative scratch space for encoding,
        // setup/preprocessing and disk serialization. Calibrate in residency tests.
        let memory = self
            .work_memory
            .reserve_build(shared.reserved_bytes().saturating_mul(4))
            .await
            .ok_or_else(|| {
                tracing::debug!(key = ?key, stage = "build_admission", "runtime admission denied");
                CacheError::Overloaded
            })?;
        let disk = self.disk.clone();
        let metrics = self.metrics.clone();
        tokio::task::spawn_blocking(move || {
            let mut memory = memory;
            let started = std::time::Instant::now();
            let bytes = source
                .load()
                .map_err(|error| CacheError::Failed(error.to_string()))?;
            tracing::debug!(key = ?key, seconds = started.elapsed().as_secs_f64(), stage = "source_load", "runtime stage");
            let compute_started = std::time::Instant::now();
            let runtime = TableRuntime::build(&shared, &bytes).map_err(CacheError::Failed)?;
            tracing::debug!(key = ?key, seconds = compute_started.elapsed().as_secs_f64(), stage = "runtime_build", "runtime stage");
            drop(bytes);
            // The optional cache must not delay serving this verified runtime.
            // Its writer owns the runtime, pin and reduced reservation until
            // persistence finishes, independently of HTTP/control cancellation.
            memory.shrink_to(shared.reserved_bytes() + disk::SAVE_SCRATCH_BYTES);
            drop(permit);
            let runtime = Arc::new(runtime);
            Metrics::incr(&metrics.builds);
            Metrics::add(&metrics.build_micros, started.elapsed().as_micros() as u64);
            metrics.build_seconds.observe(started.elapsed());
            if let Some(disk) = disk {
                Metrics::incr(&metrics.disk_save_pending);
                // Tuple fields drop in order; pending becomes zero only after
                // the runtime reference, resident pin and work guard are gone.
                let work = (runtime.clone(), pin, memory, PendingSave(metrics.clone()));
                tokio::task::spawn_blocking(move || {
                    let save_started = std::time::Instant::now();
                    if let Err(error) = disk.save(&key, &shared, &source.sha256, &work.0) {
                        Metrics::incr(&metrics.disk_write_failures);
                        tracing::warn!(%error, "runtime cache write failed; runtime remains servable");
                    }
                    tracing::debug!(key = ?key, seconds = save_started.elapsed().as_secs_f64(), stage = "disk_save", "runtime stage");
                    drop(work);
                });
            }
            Ok(runtime)
        })
        .await
        .map_err(|error| CacheError::Failed(error.to_string()))?
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

    fn forget(&self, key: &RuntimeKey, failed: &Arc<Slot>) {
        let mut inner = self.inner.lock().expect("runtime cache");
        // A waiter on a failed slot may resume after a retry installed a new
        // slot under the same key. It must not release the new allocation's budget.
        if inner
            .entries
            .get(key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.slot, failed) && Arc::strong_count(failed) == 2)
        {
            let entry = inner.entries.remove(key).expect("matching slot");
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

    /// Reclaim idle revisions before a publication build needs scratch memory.
    /// Current residency pins, in-flight queries and builds remain accounted.
    pub fn evict_unpinned(&self) {
        let mut inner = self.inner.lock().expect("runtime cache");
        while let Some(key) = Self::least_recently_used_unpinned(&inner) {
            let entry = inner.entries.remove(&key).expect("victim exists");
            inner.resident -= entry.reserved;
            Metrics::incr(&self.metrics.evictions);
        }
        self.publish(&inner);
        drop(inner);
        let before = crate::procmem::process_rss_bytes();
        crate::procmem::release_allocator_pages();
        tracing::info!(
            rss_before = before,
            rss_after = crate::procmem::process_rss_bytes(),
            "retired runtime memory reclaimed"
        );
    }

    pub(crate) fn prune_disk(
        &self,
        keep: &std::collections::HashSet<String>,
    ) -> Result<Option<u64>, String> {
        self.disk.as_ref().map_or(Ok(Some(0)), |disk| {
            disk.try_prune(keep).map_err(|e| e.to_string())
        })
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

    #[test]
    fn failed_waiter_cannot_forget_a_replacement_slot() {
        let cache = RuntimeCache::new(1024, 1, Arc::new(Metrics::default()));
        let key = ("revision".to_string(), Table::Directory, 0);
        let (failed, _) = cache.slot_for(key.clone(), 100).unwrap();
        cache.forget(&key, &failed);
        let (replacement, _) = cache.slot_for(key.clone(), 100).unwrap();
        cache.forget(&key, &failed);
        assert_eq!(cache.resident_bytes(), 100);
        assert!(Arc::ptr_eq(
            &cache.slot_for(key, 100).unwrap().0,
            &replacement
        ));
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
