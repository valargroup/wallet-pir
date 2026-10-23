//! Bounded runtime residency, coalesced work and snapshot ownership.
use super::*;

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

/// How [`RuntimeCache::get_from`] came by a runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Produced {
    /// Already built, or produced by a concurrent caller this one joined.
    Joined,
    /// Restored from this worker's disk cache.
    Restored,
    /// Built here, with no shipped runtime offered.
    Built,
    /// Loaded from a shipped file and self-checked; `check_micros` is the
    /// self-check's own time.
    Shipped { check_micros: u64 },
    /// A shipped runtime was offered but was missing, rejected or failed its
    /// self-check, so the runtime was restored or built here instead.
    Fallback,
}

/// A borrowed runtime.
///
/// Holding one pins its cache entry. The cache decides an entry is in use by
/// whether any handle to it exists, so a handle must live for exactly as long
/// as the runtime is being read — no longer, or the cache cannot reclaim it,
/// and no shorter, or it could be reclaimed mid-query.
#[derive(Clone)]
pub struct RuntimeHandle {
    pub(super) slot: Arc<Slot>,
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
pub(super) struct PendingSave(Arc<Metrics>);
impl Drop for PendingSave {
    fn drop(&mut self) {
        Metrics::sub(&self.0.disk_save_pending, 1);
    }
}

/// One cache entry's shared state.
pub(super) struct Slot {
    pub(super) runtime: tokio::sync::OnceCell<Arc<TableRuntime>>,
}

pub(super) struct Entry {
    pub(super) slot: Arc<Slot>,
    /// The bound until the runtime is built, then what it holds.
    pub(super) reserved: u64,
    pub(super) last_used: u64,
}

pub(super) struct Inner {
    pub(super) entries: HashMap<RuntimeKey, Entry>,
    pub(super) resident: u64,
    pub(super) clock: u64,
}

/// A byte-bounded, single-flight cache of prepared runtimes.
pub struct RuntimeCache {
    pub(super) work_memory: Arc<crate::runtime::memory::WorkMemory>,
    pub(super) inner: Mutex<Inner>,
    pub(super) budget: u64,
    pub(super) build_slots: Arc<tokio::sync::Semaphore>,
    pub(super) restore_slots: Arc<tokio::sync::Semaphore>,
    pub(super) prewarm_concurrency: usize,
    pub(super) background_slots: Arc<tokio::sync::Semaphore>,
    pub(super) metrics: Arc<Metrics>,
    pub(super) disk: Option<disk::DiskCache>,
    /// Shipped files refused by a production operation that then could not
    /// be admitted to restore or build, by key and shipped directory. Its
    /// admission retry goes straight to restore or build, so one operation
    /// loads, and counts, a refused file once. Cleared for a key when its
    /// operation produces the runtime or fails for good.
    pub(super) refused_shipped: Mutex<std::collections::HashSet<(RuntimeKey, std::path::PathBuf)>>,
}

/// Refusals [`RuntimeCache::refused_shipped`] keeps for operations still
/// retrying admission; past this the record is cleared, so a retry that
/// never came cannot grow it. A cleared operation may count once more.
pub(super) const REFUSALS_KEPT: usize = 4096;

impl RuntimeCache {
    pub fn new(budget: u64, build_slots: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            work_memory: Arc::new(crate::runtime::memory::WorkMemory::default()),
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                resident: 0,
                clock: 0,
            }),
            budget,
            build_slots: Arc::new(tokio::sync::Semaphore::new(build_slots.max(1))),
            restore_slots: Arc::new(tokio::sync::Semaphore::new(2)),
            prewarm_concurrency: 1,
            background_slots: Arc::new(tokio::sync::Semaphore::new(1)),
            metrics,
            disk: None,
            refused_shipped: Mutex::new(std::collections::HashSet::new()),
        }
    }

    pub fn with_disk(mut self, disk: Option<disk::DiskCache>) -> Self {
        let restores = disk.as_ref().map_or(2, |disk| disk.restore_slots.max(2));
        self.restore_slots = Arc::new(tokio::sync::Semaphore::new(restores));

        self.disk = disk;
        self
    }

    /// The allocation budget shared by runtime construction and admitted queries.
    pub fn work_memory(&self) -> &Arc<crate::runtime::memory::WorkMemory> {
        &self.work_memory
    }

    /// At most one background load across all snapshots; other restore
    /// capacity remains available to requests.
    pub async fn background_slot(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.background_slots
            .clone()
            .acquire_owned()
            .await
            .expect("background semaphore stays open")
    }

    /// Keep an admitted background load and its permit alive if its snapshot
    /// stops warming. The detached operation owns no serving snapshot.
    pub async fn get_background_from(
        self: &Arc<Self>,
        key: RuntimeKey,
        shared: Arc<SharedParams>,
        source: SegmentSource,
        shipped: Option<disk::ShippedRuntimes>,
    ) -> Result<(RuntimeHandle, Produced), CacheError> {
        let background = self.background_slot().await;
        let cache = self.clone();
        tokio::spawn(async move {
            let _background = background;
            cache.get_from(key, shared, source, shipped).await
        })
        .await
        .map_err(|error| CacheError::Failed(error.to_string()))?
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
                "restore_slots":disk.restore_slots.max(2)}))
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
        self.get_from(key, shared, source, None)
            .await
            .map(|(handle, _)| handle)
    }

    /// [`Self::get`], trying `shipped` first when the runtime has to be
    /// produced, and saying how it was.
    ///
    /// A publisher-validated runtime is loaded read-only under the restore
    /// path's slot and memory reservation, without hashing or a self-test.
    /// It is never saved to
    /// this worker's disk cache. A missing, rejected or failing file is
    /// counted as a fallback, and the runtime is restored or built as if none
    /// had been offered.
    pub async fn get_from(
        &self,
        key: RuntimeKey,
        shared: Arc<SharedParams>,
        source: SegmentSource,
        shipped: Option<disk::ShippedRuntimes>,
    ) -> Result<(RuntimeHandle, Produced), CacheError> {
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
        let produced = Mutex::new(Produced::Joined);
        let built = slot
            .runtime
            .get_or_try_init(|| async {
                let result = self
                    .load_or_build(key.clone(), shared, source, slot.clone(), shipped)
                    .await;
                match &result {
                    // The runtime exists and its size is fixed, so the
                    // bound's excess can return to the budget. This runs
                    // before any waiter is handed the runtime, and this
                    // closure holds the slot, so the entry cannot have been
                    // evicted meanwhile.
                    Ok((runtime, how)) => {
                        self.settle(&key, &slot, runtime.held_bytes());
                        *produced.lock().expect("produced") = *how;
                        self.end_operation(&key);
                    }
                    Err(CacheError::Overloaded) => Metrics::incr(&self.metrics.build_failures),
                    Err(CacheError::Failed(_)) => {
                        Metrics::incr(&self.metrics.build_failures);
                        self.end_operation(&key);
                    }
                }
                result.map(|(runtime, _)| runtime)
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
        let produced = *produced.lock().expect("produced");
        Ok((handle, produced))
    }

    /// The runtime for `key` only if it is already built: never builds,
    /// restores or waits, so a caller can refuse work a cold build would cost.
    pub fn cached(&self, key: &RuntimeKey) -> Option<RuntimeHandle> {
        let mut inner = self.inner.lock().expect("runtime cache");
        inner.clock += 1;
        let now = inner.clock;
        let entry = inner.entries.get_mut(key)?;
        if !entry.slot.runtime.initialized() {
            return None;
        }
        entry.last_used = now;
        Metrics::incr(&self.metrics.cache_hits);
        Some(RuntimeHandle {
            slot: entry.slot.clone(),
        })
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
        shipped: Option<disk::ShippedRuntimes>,
    ) -> Result<(Arc<TableRuntime>, Produced), CacheError> {
        let attempt = std::time::Instant::now();
        let fallback = match shipped {
            None => false,
            Some(shipped) => {
                let required = shipped.is_required();
                let refusal = (key.clone(), shipped.directory().to_path_buf());
                if self
                    .refused_shipped
                    .lock()
                    .expect("refusals")
                    .contains(&refusal)
                {
                    if required {
                        return Err(CacheError::Failed(
                            "required prepared runtime was refused".into(),
                        ));
                    }
                    // An admission retry of the operation that refused this
                    // file: already counted, and not loaded again.
                    true
                } else {
                    match self
                        .load_shipped(&key, &shared, &source, &pin, shipped)
                        .await?
                    {
                        Ok((runtime, check_micros)) => {
                            return Ok((Arc::new(runtime), Produced::Shipped { check_micros }))
                        }
                        Err(error) => {
                            if required {
                                return Err(CacheError::Failed(format!(
                                    "required prepared runtime: {error}"
                                )));
                            }
                            let mut refused = self.refused_shipped.lock().expect("refusals");
                            if refused.len() >= REFUSALS_KEPT {
                                refused.clear();
                            }
                            refused.insert(refusal);
                            drop(refused);
                            Metrics::incr(&self.metrics.shipped_fallbacks);
                            tracing::warn!(key = ?key, %error, "shipped runtime refused; producing it here");
                            true
                        }
                    }
                }
            }
        };
        let produced = |local: Produced| if fallback { Produced::Fallback } else { local };
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
                    let restored = disk.load(&restore_key, &restore_shared, restore_source.sha256());
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
                return Ok((Arc::new(runtime), produced(Produced::Restored)));
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
            let runtime = build_pool()
                .install(|| TableRuntime::build(&shared, &bytes))
                .map_err(CacheError::Failed)?;
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
                    if let Err(error) = disk.save(&key, &shared, source.sha256(), &work.0) {
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
        .map(|runtime| (runtime, produced(Produced::Built)))
    }

    /// Loads and self-checks one shipped runtime. The outer error is the
    /// segment's own fault, which fails the request as a build would; the
    /// inner one refuses only the shipped file.
    async fn load_shipped(
        &self,
        key: &RuntimeKey,
        shared: &Arc<SharedParams>,
        source: &SegmentSource,
        pin: &Arc<Slot>,
        shipped: disk::ShippedRuntimes,
    ) -> Result<Result<(TableRuntime, u64), String>, CacheError> {
        let attempt = std::time::Instant::now();
        let permit = self
            .restore_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| CacheError::Failed("server is shutting down".into()))?;
        tracing::debug!(key = ?key, seconds = attempt.elapsed().as_secs_f64(), stage = "shipped_slot", "runtime stage");
        let memory = self
            .work_memory
            .reserve(shared.reserved_bytes().saturating_mul(2))
            .ok_or_else(|| {
                tracing::debug!(key = ?key, stage = "shipped_admission", "runtime admission denied");
                CacheError::Overloaded
            })?;
        let (key, shared, source, pin) = (key.clone(), shared.clone(), source.clone(), pin.clone());
        let metrics = self.metrics.clone();
        tokio::task::spawn_blocking(move || {
            let _memory = memory;
            let _pin = pin;
            let started = std::time::Instant::now();
            let path = shipped.path(&key, &shared, source.sha256());
            let file = match crate::verification::open(&path) {
                Ok(file) => file,
                Err(error) => return Ok(Err(error.to_string())),
            };
            let runtime = match file.try_clone().and_then(|handle| {
                disk::ShippedRuntimes::load_received_file(
                    &path,
                    handle,
                    &key,
                    &shared,
                    source.sha256(),
                )
            }) {
                Ok(runtime) => runtime,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(Err(format!(
                        "no shipped runtime in {}",
                        shipped.directory().display()
                    )))
                }
                Err(error) => return Ok(Err(error.to_string())),
            };
            let loaded = started.elapsed();
            drop(permit);
            let check_micros = 0;
            Metrics::incr(&metrics.shipped_loads);
            tracing::info!(
                key = ?key,
                load_seconds = loaded.as_secs_f64(),
                check_seconds = check_micros as f64 / 1e6,
                "loaded shipped runtime"
            );
            Ok(Ok((runtime, check_micros)))
        })
        .await
        .map_err(|error| CacheError::Failed(error.to_string()))?
    }

    /// Forgets the shipped refusals of `key`'s production operation, which
    /// has produced the runtime or failed for good.
    fn end_operation(&self, key: &RuntimeKey) {
        self.refused_shipped
            .lock()
            .expect("refusals")
            .retain(|(refused, _)| refused != key);
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

    /// Charges `slot`'s entry what its built runtime holds, releasing the rest
    /// of its reservation.
    ///
    /// Only ever lowers a charge: a runtime that came out at or above its
    /// bound keeps the bound, which is what admission already allowed for.
    /// A slot no longer installed under `key` is left alone, so a stale
    /// caller cannot rewrite a replacement's charge.
    fn settle(&self, key: &RuntimeKey, slot: &Arc<Slot>, held: u64) {
        let mut inner = self.inner.lock().expect("runtime cache");
        let Some(entry) = inner
            .entries
            .get_mut(key)
            .filter(|entry| Arc::ptr_eq(&entry.slot, slot))
        else {
            return;
        };
        let released = entry.reserved.saturating_sub(held);
        entry.reserved -= released;
        inner.resident -= released;
        self.publish(&inner);
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

    /// Bytes currently charged: built entries at what they hold, entries
    /// still building or restoring at their reservation. For health
    /// reporting and tests.
    pub fn resident_bytes(&self) -> u64 {
        self.inner.lock().expect("runtime cache").resident
    }

    /// Reclaim idle revisions before a publication build needs scratch memory.
    /// Current residency pins, in-flight queries and builds remain accounted.
    pub fn evict_unpinned(&self) {
        self.evict_unpinned_except(|_| false);
    }

    /// [`Self::evict_unpinned`], keeping idle entries `keep` selects: a
    /// serving snapshot's current runtimes stay resident while a candidate is
    /// prepared beside it.
    pub fn evict_unpinned_except(&self, keep: impl Fn(&RuntimeKey) -> bool) {
        let mut inner = self.inner.lock().expect("runtime cache");
        let victims: Vec<RuntimeKey> = inner
            .entries
            .iter()
            .filter(|(key, entry)| Arc::strong_count(&entry.slot) == 1 && !keep(key))
            .map(|(key, _)| key.clone())
            .collect();
        for key in victims {
            let entry = inner.entries.remove(&key).expect("victim exists");
            inner.resident -= entry.reserved;
            Metrics::incr(&self.metrics.evictions);
        }
        self.publish(&inner);
        drop(inner);
        let before = crate::runtime::procmem::process_rss_bytes();
        crate::runtime::procmem::release_allocator_pages();
        tracing::info!(
            rss_before = before,
            rss_after = crate::runtime::procmem::process_rss_bytes(),
            "retired runtime memory reclaimed"
        );
    }

    /// Remove these idle entries and persisted derivatives of their runtime
    /// identities before a withdrawn shard is repaired. A live query, load or
    /// snapshot writer keeps its accounting until it finishes. A busy disk
    /// writer defers eviction rather than blocking the cache on its file lock.
    pub fn evict_keys(&self, keys: &[RuntimeKey]) -> std::io::Result<bool> {
        let mut inner = self.inner.lock().expect("runtime cache");
        if keys.iter().any(|key| {
            inner
                .entries
                .get(key)
                .is_some_and(|entry| Arc::strong_count(&entry.slot) != 1)
        }) {
            return Ok(false);
        }
        if let Some(disk) = &self.disk {
            if !disk.invalidate(keys)? {
                return Ok(false);
            }
        }
        for key in keys {
            if let Some(entry) = inner.entries.remove(key) {
                inner.resident -= entry.reserved;
                Metrics::incr(&self.metrics.evictions);
            }
        }
        self.publish(&inner);
        Ok(true)
    }

    /// Prune unused persisted runtimes while retaining the caller's active keys.
    pub fn prune_disk(
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
pub(crate) mod tests;
