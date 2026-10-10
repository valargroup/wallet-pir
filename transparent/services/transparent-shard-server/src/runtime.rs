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
//! # Releasing what the build did not use
//!
//! The reservation is an upper bound: it charges the compiled packing matrix
//! at eight-byte words, and a built runtime has so far held it at four (or
//! fewer, packed). Once a build or restore has produced the runtime, its entry
//! is charged what it actually holds, [`TableRuntime::held_bytes`], and the
//! difference returns to the budget. The release happens only after the
//! runtime exists, never before, so a build in flight is always charged its
//! bound, and an entry's charge can only fall. A cache that kept the bound
//! would hold barely more than half the runtimes its budget can carry.
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
use ipir_sp::server::IPIRServer;
use ipir_sp::YpirSchemeParams;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use transparent_native::{
    self as native, NativeKeys, NativePreprocessed, NativeScheme, TableProfile,
};
use transparent_shard::layout::Geometry;

const _: () = assert!(transparent_shard::manifest::QUERY_ROW_QUANTUM as usize == native::D);

/// Explicit build policy for reproducible hardware-path comparisons. The
/// portable feature selects the library's existing fallback; it changes no
/// parameters or wire encoding and never depends on a client request.
pub const KERNEL_POLICY: &str = if cfg!(feature = "portable-kernel") {
    "chunked-split"
} else {
    "auto"
};

pub(crate) fn database_server(
    shared: &SharedParams,
    coefficients: impl Iterator<Item = u16>,
    transposed: bool,
) -> IPIRServer<u16> {
    #[cfg(feature = "portable-kernel")]
    let server = IPIRServer::<u16>::new(shared.transport.clone(), coefficients, transposed, true);
    #[cfg(not(feature = "portable-kernel"))]
    let server = IPIRServer::<u16>::new_auto_kernel(
        shared.transport.clone(),
        coefficients,
        transposed,
        true,
    );
    server
}

/// The first-dimension scan shape for one table: full 16-bit plaintexts, one
/// column per coefficient, and the native transport's 49-bit query and 22-bit
/// response widths. The scan itself runs modulo the native `q` = 2^54, on the
/// selection already lifted back to `q`, so it is the same for a 44-bit
/// dithered query; the 49 here only keeps the shape, and with it every runtime
/// disk-cache key, what it was before dithered queries.
pub fn transport_params(rows: u64, row_bytes: u32) -> Result<YpirSchemeParams, String> {
    let (_, mut params) = ipir_sp::params_for_simplepir_profile(
        rows,
        row_bytes as u64 * 8,
        ipir_sp::SimplePirProfile::P16Q48,
    )
    .map_err(|error| error.to_string())?;
    params.query_bits = native::QUERY_BITS;
    params.q_prime_1 = 1 << native::RESPONSE_BITS;
    Ok(params)
}

/// Bytes one prepared runtime reserves for a `rows` by `row_bytes` segment.
///
/// Three terms, all derived rather than measured:
///
/// - the **encoded database**, `rows * row_bytes / 2` coefficients of `u16`,
///   which is the only term that follows the row count;
/// - the **published masks**, 14,848 bytes per 4,096-byte instance; and
/// - the **two-mask preprocessing** at its upper bound: per block both masks
///   and a `d x d*ell` compiled matrix at eight-byte words (64 MiB). The
///   compiled matrix is stored at four-byte words when every entry fits, so a
///   real runtime may hold half of that term; the reservation takes the bound
///   because it is made before the build that decides it, and the cache
///   releases the difference once the build is done.
///
/// `shard-scaling --geometry-sweep` and the router price the same terms, so a
/// sweep's projection and a running worker's budget cannot drift apart.
///
/// It is a **reservation**, not a measurement of the process. It excludes
/// allocator fragmentation, the transient plaintext buffer and hint a build
/// reads, the shared parameters, and anything a request allocates. A cache
/// budget is therefore always set below the host's real headroom, and
/// re-checked with `shard-residency` whenever the scheme moves.
pub fn reserved_bytes(rows: u64, row_bytes: u32) -> u64 {
    let blocks = (row_bytes as usize / native::INSTANCE_BYTES) as u64;
    rows * row_bytes as u64
        + blocks * native::BLOCK_PUBLIC_BYTES as u64
        + native::PREPARED_HEADER_BYTES
        + blocks * native::PREPARED_BLOCK_MAX_BYTES
}

/// Bytes a prepared runtime holds once built with its compiled matrix at
/// four-byte words: [`reserved_bytes`] less half of the matrix term.
///
/// This is the size the cache charges a built runtime in practice, so it is
/// what warm-fit and disk preflight checks plan with. It is a planning figure,
/// not a bound: a runtime whose matrix needs eight-byte words is charged its
/// full reservation, and a plan made with this figure then comes up short and
/// says so through overloads rather than through memory. `shard-residency`
/// measured `txid-2k` at 40.3-41.3 MiB and `txid-4k` at 48.2-48.3 MiB per
/// runtime against the 40.05 and 48.05 MiB given here.
pub fn held_bytes(rows: u64, row_bytes: u32) -> u64 {
    let blocks = (row_bytes as usize / native::INSTANCE_BYTES) as u64;
    reserved_bytes(rows, row_bytes)
        - blocks * (native::PREPARED_BLOCK_MAX_BYTES - native::PREPARED_BLOCK_MIN_BYTES)
}

/// Bytes a warm worker's cache needs to build or restore every runtime in
/// `runtimes` and keep them all: each charged at its built size, plus the
/// bound's excess for the `in_flight` largest, since that many may hold their
/// reservation at once before they are released.
///
/// Never more than the sum of the reservations, which is what this check
/// demanded before the cache released anything: every assignment that fitted
/// then fits now.
pub fn warm_bytes<'a>(
    runtimes: impl IntoIterator<Item = &'a SharedParams>,
    in_flight: usize,
) -> u64 {
    let mut held = 0u64;
    let mut excess = Vec::new();
    for shared in runtimes {
        held += shared.held_bytes();
        excess.push(shared.reserved_bytes() - shared.held_bytes());
    }
    excess.sort_unstable_by(|a, b| b.cmp(a));
    held + excess.iter().take(in_flight).sum::<u64>()
}

/// The parameters every shard of one geometry and table shares.
///
/// The native profile is a pure function of the schema, geometry and table, so
/// every shard naming the same geometry shares one of these and a client
/// validates it once. Query masks and the packing setup are shared too, which
/// is what lets one query be answered by every segment of a shard. A set mixing
/// archive and recent shards leaks one per geometry per table rather than one
/// per shard.
pub struct SharedParams {
    pub geometry: &'static Geometry,
    pub table: Table,
    pub profile: TableProfile,
    /// The first-dimension scan shape.
    pub transport: YpirSchemeParams,
    pub setup_seed: u64,
}

/// A query body parsed once and answered by every segment.
pub struct ParsedQuery {
    keys: NativeKeys,
    query: Vec<u64>,
}

impl SharedParams {
    pub fn build(geometry: &'static Geometry, table: Table) -> Result<Self, String> {
        let rows = table.rows(geometry);
        let row_bytes = table.row_bytes(geometry);
        Ok(Self {
            geometry,
            table,
            profile: TableProfile::new(
                transparent_shard::manifest::SCHEMA,
                geometry.name,
                table.as_str(),
                rows,
                row_bytes,
            )?,
            transport: transport_params(rows, row_bytes)?,
            setup_seed: crate::shardset::setup_seed(geometry, table),
        })
    }

    /// What `/v1/shards/init` publishes for this table as its scheme: the
    /// 49-bit nearest-rounded query. Also what keys the runtime disk cache.
    pub fn scheme(&self) -> &NativeScheme {
        &self.profile.scheme
    }

    /// The 44-bit dithered scheme `/v1/shards/init` publishes beside it.
    pub fn dithered_scheme(&self) -> &NativeScheme {
        &self.profile.dithered_scheme
    }

    /// The length of a 49-bit query body: the 8-byte binding, the `K_g`
    /// packing key and the 49-bit selection. The longer of the two accepted
    /// lengths, so it is also the most a body may be.
    pub fn query_bytes(&self) -> usize {
        8 + self.profile.scheme.request_bytes
    }

    /// The length of a 44-bit dithered query body.
    pub fn dithered_query_bytes(&self) -> usize {
        8 + self.profile.dithered_scheme.request_bytes
    }

    /// Whether `len` is one of the exact lengths a query body may have: the
    /// selection over every row or, for a revision whose queries may omit
    /// every row from `query_rows` on, over only those rows, at either width.
    ///
    /// Each is fixed, because a body whose size varied with the selection
    /// would leak it through its length alone. The width says only which
    /// scheme the wallet sends, which every query of that wallet shares, and
    /// `query_rows` is the revision's, which every query to the table shares.
    pub fn accepts_query_bytes(&self, query_rows: usize, len: usize) -> bool {
        len.checked_sub(8)
            .is_some_and(|len| self.profile.accepts_selection_len(query_rows, len))
    }

    /// Bytes one segment's answer carries: binding, epoch and body.
    pub fn response_bytes(&self) -> usize {
        16 + self.profile.scheme.response_bytes
    }

    pub fn reserved_bytes(&self) -> u64 {
        reserved_bytes(self.profile.rows as u64, self.profile.row_bytes as u32)
    }

    /// See [`held_bytes`].
    pub fn held_bytes(&self) -> u64 {
        held_bytes(self.profile.rows as u64, self.profile.row_bytes as u32)
    }

    /// Checks the binding and length, then parses the key and selection once
    /// for every segment that will answer them.
    ///
    /// `query_rows` is the revision's [`pages_query_rows`] for pages and every
    /// row otherwise. A body selecting over only those rows is zero-filled to
    /// the full selection; the rows it omits are zero in every segment.
    ///
    /// [`pages_query_rows`]: transparent_shard::manifest::ShardManifest::pages_query_rows
    pub fn parse(
        &self,
        binding: [u8; 8],
        body: &[u8],
        query_rows: usize,
    ) -> Result<ParsedQuery, String> {
        if body.get(..8) != Some(binding.as_slice()) {
            return Err("query does not name this revision and table".to_string());
        }
        // Fixed lengths for every query to a revision's table: a body that
        // varied with the selection would leak it through its size alone. The
        // length picks the width and whether the selection is the prefix.
        if !self.accepts_query_bytes(query_rows, body.len()) {
            return Err("query has the wrong fixed length".to_string());
        }
        let (keys, query) = self.profile.parse_selection(&body[8..], query_rows)?;
        Ok(ParsedQuery { keys, query })
    }
}

/// Row-major u16 coefficients of a segment's plaintext, in the order
/// `IPIRServer` ingests a non-transposed database.
struct RowCoefficients<'a> {
    rows: &'a [u8],
    row_bytes: usize,
    cols: usize,
    position: usize,
    total: usize,
}

impl Iterator for RowCoefficients<'_> {
    type Item = u16;

    fn next(&mut self) -> Option<u16> {
        if self.position >= self.total {
            return None;
        }
        let (row, col) = (self.position / self.cols, self.position % self.cols);
        self.position += 1;
        Some(native::row_coefficient(self.rows, self.row_bytes, row, col))
    }
}

/// Geometries whose runtime hint skips trailing zero blocks and is computed
/// by [`native::batched_hint`]: the recent tails and the display tables, whose
/// recent shard is rebuilt at every block. Every other geometry, the history
/// archives included, keeps the reference product over every block.
const BATCHED_HINT_GEOMETRIES: [&Geometry; 5] = [
    &transparent_shard::layout::RECENT_8K,
    &transparent_shard::layout::RECENT_4K,
    &transparent_shard::layout::RECENT_4K_8K,
    &transparent_shard::display::TXID_2K,
    &transparent_shard::display::TXID_4K,
];

fn uses_batched_hint(geometry: &Geometry) -> bool {
    BATCHED_HINT_GEOMETRIES.contains(&geometry)
}

/// Leading `D`-row blocks of a `table_rows`-row plaintext that hold any
/// nonzero byte, and at least one.
fn used_blocks(rows: &[u8], row_bytes: usize, table_rows: usize) -> usize {
    let last = rows
        .iter()
        .rposition(|byte| *byte != 0)
        .map_or(0, |at| at / row_bytes);
    (last / native::D + 1).min(table_rows / native::D)
}

/// A uniformly random row among the leading rows that hold any nonzero
/// byte, or row 0 of an empty table. Random per call, so a fault is not
/// hidden behind a row the publisher could predict.
fn sample_row(rows: &[u8], row_bytes: usize, table_rows: usize) -> usize {
    use std::hash::BuildHasher;
    let used = rows
        .iter()
        .rposition(|byte| *byte != 0)
        .map_or(1, |at| at / row_bytes + 1)
        .min(table_rows);
    let random = std::collections::hash_map::RandomState::new().hash_one(rows.len());
    (random % used as u64) as usize
}

/// One segment of one shard revision's table, prepared to answer queries.
pub struct TableRuntime {
    pub(crate) preprocessed: Vec<NativePreprocessed>,
    pub(crate) server: IPIRServer<u16>,
    pub public_params: Vec<u8>,
    pub public_params_sha256: String,
    pub public_params_epoch: [u8; 8],
}

/// Scheduling priority of runtime construction threads (a nice value).
#[cfg(target_os = "linux")]
const BUILD_NICE: i32 = 10;

/// Runtime construction runs here, never in the global rayon pool that query
/// evaluation uses.
///
/// Every recent replica rebuilds its tail runtimes at every publication. In
/// the global pool that build took all of a 4-vCPU host's cores for seconds,
/// queries queued behind its parallel work items, and p99 reached 2.5 s at
/// 20 QPS on two replicas. Half the cores at lower priority keep builds
/// progressing while query evaluation wins under contention.
/// `TRANSPARENT_BUILD_THREADS` overrides the thread count.
pub fn build_pool() -> &'static rayon::ThreadPool {
    static POOL: std::sync::OnceLock<rayon::ThreadPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let threads = std::env::var("TRANSPARENT_BUILD_THREADS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|threads| *threads > 0)
            .unwrap_or_else(|| {
                std::thread::available_parallelism().map_or(1, |cores| (cores.get() / 2).max(1))
            });
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("runtime-build-{index}"))
            .start_handler(|_| lower_priority())
            .build()
            .expect("runtime build pool")
    })
}

#[cfg(target_os = "linux")]
fn lower_priority() {
    // On Linux, PRIO_PROCESS with a thread id changes only that thread.
    if let Err(error) =
        rustix::process::setpriority_process(Some(rustix::thread::gettid()), BUILD_NICE)
    {
        tracing::warn!(%error, "could not lower runtime build thread priority");
    }
}

#[cfg(not(target_os = "linux"))]
fn lower_priority() {}

impl TableRuntime {
    /// Builds the runtime for one segment.
    ///
    /// `rows` is the segment's verified plaintext. It is borrowed, not kept:
    /// what survives is the encoded database inside the server, and holding the
    /// plaintext beside it would double the resident cost of every shard for no
    /// benefit.
    pub fn build(shared: &SharedParams, rows: &[u8]) -> Result<Self, String> {
        let profile = &shared.profile;
        let mut phase = std::time::Instant::now();
        let server = database_server(
            shared,
            RowCoefficients {
                rows,
                row_bytes: profile.row_bytes,
                cols: profile.cols,
                position: 0,
                total: profile.rows * profile.cols,
            },
            false,
        );
        tracing::debug!(
            geometry = shared.geometry.name,
            table = shared.table.as_str(),
            seconds = phase.elapsed().as_secs_f64(),
            stage = "encode_database",
            "construction stage"
        );
        phase = std::time::Instant::now();
        // The query masks are derived from a published seed, so a client
        // reproduces them exactly. The hint is public: the masks times the
        // database, exactly lifted.
        let padded = server.db_rows_padded();
        let db = server.db();
        let hint = if uses_batched_hint(shared.geometry) {
            // Zero rows add nothing to `masks * database`, so trailing blocks
            // of them are left out of the product. A growing tail's page table
            // fills from its first row and is mostly empty for much of its
            // life; the hint, and every byte published from it, is the same.
            // The batched hint computes the reference's exact integers several
            // times faster.
            let used = used_blocks(rows, profile.row_bytes, profile.rows);
            native::batched_hint::hint(
                &profile.masks[..used],
                used * native::D,
                profile.cols,
                |col| &db[col * padded..col * padded + used * native::D],
            )?
        } else {
            native::hint(&profile.masks, profile.rows, profile.cols, |col| {
                &db[col * padded..col * padded + profile.rows]
            })?
        };
        tracing::debug!(
            geometry = shared.geometry.name,
            table = shared.table.as_str(),
            seconds = phase.elapsed().as_secs_f64(),
            stage = "hint_columns",
            "construction stage"
        );
        phase = std::time::Instant::now();
        let preprocessed = native::preprocess(&profile.setup, &hint)?;
        drop(hint);
        tracing::debug!(
            geometry = shared.geometry.name,
            table = shared.table.as_str(),
            seconds = phase.elapsed().as_secs_f64(),
            stage = "pack_preprocessing",
            "construction stage"
        );
        Self::assemble(server, preprocessed)
    }

    /// Bytes this runtime holds, priced by the same terms as
    /// [`reserved_bytes`] but at the compiled matrix's actual word width.
    ///
    /// Each block's charge is its id and width word plus the library's
    /// retained coefficient storage (both masks and the matrix), which is the
    /// bound's block term with the real matrix in place of the eight-byte one.
    /// A restored runtime's matrix is mapped from its cache file and is
    /// charged the same.
    pub fn held_bytes(&self) -> u64 {
        const BLOCK_ID_AND_WIDTH_BYTES: u64 = 32 + 8;
        self.server.db().len() as u64 * 2
            + self.public_params.len() as u64
            + native::PREPARED_HEADER_BYTES
            + self
                .preprocessed
                .iter()
                .map(|block| BLOCK_ID_AND_WIDTH_BYTES + block.coefficient_bytes() as u64)
                .sum::<u64>()
    }

    /// Publishes the masks of prepared blocks and derives their epoch.
    pub(crate) fn assemble(
        server: IPIRServer<u16>,
        preprocessed: Vec<NativePreprocessed>,
    ) -> Result<Self, String> {
        let public_params = native::publish(&preprocessed)?;
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
        self.answer(binding, &shared.parse(binding, body, shared.profile.rows)?)
    }

    /// Checks a runtime this process did not build against the segment's
    /// verified plaintext `rows`, end to end.
    ///
    /// The encoded database must equal `rows` coefficient for coefficient,
    /// and a fresh client query for one sampled populated row must decode,
    /// under the published masks, to that row. The first proves the database
    /// is the segment's; the second that it answers under the preprocessing
    /// it was loaded with, which a database built from other rows does not,
    /// at any row. Together they cover what [`disk::DiskCache::load`] does not
    /// check. Returns the row checked.
    pub fn self_check(&self, shared: &SharedParams, rows: &[u8]) -> Result<usize, String> {
        let profile = &shared.profile;
        if rows.len() != profile.rows * profile.row_bytes {
            return Err("self-check rows have the wrong length".into());
        }
        let padded = self.server.db_rows_padded();
        let db = self.server.db();
        for col in 0..profile.cols {
            let column = &db[col * padded..col * padded + profile.rows];
            if (0..profile.rows).any(|row| {
                column[row] != native::row_coefficient(rows, profile.row_bytes, row, col)
            }) {
                return Err(format!("database column {col} differs from the segment"));
            }
        }
        let row = sample_row(rows, profile.row_bytes, profile.rows);
        let binding = *b"selfchck";
        let (secret, upload) = profile.prepare(row)?;
        let mut body = binding.to_vec();
        body.extend(upload);
        let answer = self.evaluate(shared, binding, &body)?;
        let decoded = profile.decode(&secret, &self.public_params, &answer[16..])?;
        let at = row * profile.row_bytes;
        if decoded != rows[at..at + profile.row_bytes] {
            return Err(format!("row {row} does not decode to the segment's row"));
        }
        Ok(row)
    }

    /// Answers a query already parsed by [`SharedParams::parse`].
    pub fn answer(&self, binding: [u8; 8], query: &ParsedQuery) -> Result<Vec<u8>, String> {
        let intermediate = self
            .server
            .try_multiply_power_of_two(native::Q, &query.query)
            .map_err(|error| error.to_string())?;
        let body = native::pack(&self.preprocessed, &query.keys, &intermediate)?;
        let mut response = Vec::with_capacity(16 + body.len());
        response.extend_from_slice(&binding);
        response.extend_from_slice(&self.public_params_epoch);
        response.extend_from_slice(&body);
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
    /// The bound until the runtime is built, then what it holds.
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
        self.get_from(key, shared, source, None)
            .await
            .map(|(handle, _)| handle)
    }

    /// [`Self::get`], trying `shipped` first when the runtime has to be
    /// produced, and saying how it was.
    ///
    /// A shipped runtime is loaded read-only under the restore path's slot
    /// and memory reservation, then self-checked against the segment's
    /// verified rows ([`TableRuntime::self_check`]). It is never saved to
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
                    }
                    Err(_) => Metrics::incr(&self.metrics.build_failures),
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
                match self
                    .load_shipped(&key, &shared, &source, &pin, shipped)
                    .await?
                {
                    Ok((runtime, check_micros)) => {
                        return Ok((Arc::new(runtime), Produced::Shipped { check_micros }))
                    }
                    Err(error) => {
                        Metrics::incr(&self.metrics.shipped_fallbacks);
                        tracing::warn!(key = ?key, %error, "shipped runtime refused; producing it here");
                        true
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
            let runtime = match shipped.load(&key, &shared, &source.sha256) {
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
            // The restore slot bounds concurrent reads of runtime files; the
            // self-check below is build-pool work under the memory reservation,
            // so release the slot here and let the next shipped file load.
            drop(permit);
            // Verifies the segment as a restore does, and keeps its rows for
            // the self-check.
            let rows = source
                .load()
                .map_err(|error| CacheError::Failed(error.to_string()))?;
            let check_started = std::time::Instant::now();
            let checked = build_pool().install(|| runtime.self_check(&shared, &rows));
            let check_micros = check_started.elapsed().as_micros() as u64;
            Metrics::add(&metrics.shipped_check_micros, check_micros);
            let row = match checked {
                Ok(row) => row,
                Err(error) => return Ok(Err(format!("self-check failed: {error}"))),
            };
            Metrics::incr(&metrics.shipped_loads);
            tracing::info!(
                key = ?key,
                load_seconds = loaded.as_secs_f64(),
                check_seconds = check_micros as f64 / 1e6,
                row,
                "loaded shipped runtime"
            );
            Ok(Ok((runtime, check_micros)))
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

    #[test]
    fn runtime_builds_run_in_their_own_smaller_pool() {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        let (index, threads) =
            build_pool().install(|| (rayon::current_thread_index(), rayon::current_num_threads()));
        assert!(index.is_some(), "the closure runs on a build pool thread");
        assert!(threads <= cores.max(1));
        assert!(
            rayon::current_thread_index().is_none(),
            "the caller is not a pool thread"
        );
        #[cfg(target_os = "linux")]
        assert_eq!(
            build_pool().install(|| rustix::process::getpriority_process(Some(
                rustix::thread::gettid()
            ))
            .unwrap()),
            BUILD_NICE
        );
    }

    use super::*;
    use transparent_shard::display::TXID_2K;
    use transparent_shard::layout::{ARCHIVE_WIDE, RECENT_8K};

    fn reservation(rows: u64, row_bytes: usize) -> u64 {
        reserved_bytes(rows, row_bytes as u32)
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

    /// `shard-residency` on roman-dev-2 (2026-10-07), steady per-runtime
    /// increments after the allocator warmed up, in MiB: the lowest and
    /// highest of runtimes 4-6 of each table.
    /// `transparent/evidence/txid-display-backfill-sizing-2026-10-07/residency`.
    const MEASURED_TXID_2K_MIB: (f64, f64) = (40.29, 41.34);
    const MEASURED_TXID_4K_MIB: (f64, f64) = (48.21, 48.30);

    fn mib(bytes: u64) -> f64 {
        bytes as f64 / f64::from(1 << 20)
    }

    /// The planned built size is the four-byte-word size the residency
    /// measurement found, and the reservation is the eight-byte bound: 72.05
    /// against 40.05 MiB at `txid-2k`, 80.05 against 48.05 MiB at `txid-4k`.
    /// The planned size sits just under each measured range, which carries
    /// allocator overhead the formula leaves out.
    #[test]
    fn the_planned_built_size_is_the_measured_display_runtime() {
        use transparent_shard::display::TXID_4K;
        for (geometry, reserved, held, measured) in [
            (&TXID_2K, 75_545_144, 41_990_712, MEASURED_TXID_2K_MIB),
            (&TXID_4K, 83_933_752, 50_379_320, MEASURED_TXID_4K_MIB),
        ] {
            let shared = SharedParams::build(geometry, Table::TxDirectory).unwrap();
            assert_eq!(shared.reserved_bytes(), reserved, "{}", geometry.name);
            assert_eq!(shared.held_bytes(), held, "{}", geometry.name);
            assert_eq!(reserved - held, 32 << 20, "half of one compiled matrix");
            assert!(mib(held) <= measured.0, "{}", geometry.name);
            assert!(measured.1 - mib(held) < 1.5, "{}", geometry.name);
        }
    }

    /// A built runtime, history or display, holds no more than the planned
    /// four-byte size and no less than the 27-bit packed matrix a CPU with
    /// AVX-512 VBMI may choose, so its charge never exceeds its reservation.
    #[test]
    fn a_built_runtime_holds_its_planned_size_or_less() {
        let matrix_narrow = (native::D * native::D * native::ELL * 4) as u64;
        let matrix_packed = (native::D * (native::D * native::ELL / 8) * 27 + 8) as u64;
        for (geometry, table) in [
            (&RECENT_8K, Table::Directory),
            (&TXID_2K, Table::TxDirectory),
            (&transparent_shard::display::TXID_4K, Table::TxDirectory),
        ] {
            let shared = SharedParams::build(geometry, table).unwrap();
            let profile = &shared.profile;
            let rows: Vec<u8> = (0..profile.rows * profile.row_bytes)
                .map(|i| (i.wrapping_mul(2_654_435_761) >> 9) as u8)
                .collect();
            let held = TableRuntime::build(&shared, &rows).unwrap().held_bytes();
            eprintln!(
                "{} {}: reserved {:.2} MiB, planned {:.2} MiB, built {:.2} MiB",
                geometry.name,
                table.as_str(),
                mib(shared.reserved_bytes()),
                mib(shared.held_bytes()),
                mib(held)
            );
            assert!(held <= shared.held_bytes(), "{}", geometry.name);
            assert!(
                held >= shared.held_bytes() - (matrix_narrow - matrix_packed),
                "{}",
                geometry.name
            );
            assert!(held < shared.reserved_bytes());
        }
    }

    /// Admission is unchanged: a new entry is admitted against its full
    /// bound, and while it is building that bound stays charged, so builds in
    /// flight together can never be admitted past the budget on the strength
    /// of sizes they have not reached yet. Only once a runtime exists does its
    /// charge fall, never rise, and eviction returns exactly what is charged.
    #[test]
    fn a_reservation_is_released_only_after_its_build_and_only_downward() {
        let shared = SharedParams::build(&TXID_2K, Table::TxDirectory).unwrap();
        let (bound, held) = (shared.reserved_bytes(), shared.held_bytes());
        let metrics = Arc::new(Metrics::default());
        let cache = RuntimeCache::new(bound + held, 2, metrics.clone());
        let key = |segment| ("revision".to_string(), Table::TxDirectory, segment);
        let (a, _) = cache.slot_for(key(0), bound).unwrap();
        assert_eq!(cache.resident_bytes(), bound);
        // A second build may not start while the first still holds its bound,
        // although both would fit at their built size.
        assert!(matches!(
            cache.slot_for(key(1), bound),
            Err(CacheError::Overloaded)
        ));
        cache.settle(&key(0), &a, held);
        assert_eq!(cache.resident_bytes(), held);
        assert_eq!(Metrics::get(&metrics.resident_bytes), held);
        let (b, _) = cache.slot_for(key(1), bound).unwrap();
        assert_eq!(cache.resident_bytes(), held + bound);
        assert_eq!(cache.resident_bytes(), cache.budget());
        // A runtime that came out larger than planned keeps its bound, and a
        // second settle cannot raise a charge already lowered.
        cache.settle(&key(1), &b, bound + 1);
        cache.settle(&key(0), &a, bound);
        assert_eq!(cache.resident_bytes(), held + bound);
        // A stale slot cannot rewrite a replacement's charge.
        let stale = Arc::new(Slot {
            runtime: tokio::sync::OnceCell::new(),
        });
        cache.settle(&key(1), &stale, 0);
        assert_eq!(cache.resident_bytes(), held + bound);
        assert!(matches!(
            cache.slot_for(key(2), bound),
            Err(CacheError::Overloaded)
        ));
        drop((a, b));
        cache.evict_unpinned();
        assert_eq!(cache.resident_bytes(), 0);
        assert_eq!(cache.entries(), 0);
        assert_eq!(Metrics::get(&metrics.resident_bytes), 0);
        assert_eq!(Metrics::get(&metrics.cache_entries), 0);
        assert_eq!(Metrics::get(&metrics.evictions), 2);
    }

    /// The same through real builds: a build stalled on its source keeps its
    /// whole bound charged, so a second build that would fit only at the
    /// first's built size is refused until the first runtime exists. Then the
    /// first is charged what it holds, and the second is admitted beside it
    /// without evicting anything.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_build_in_flight_keeps_its_bound_until_the_runtime_exists() {
        use std::io::Write;
        let geometry = &transparent_shard::layout::RECENT_4K;
        let dir = tempfile::tempdir().unwrap();
        let shared = Arc::new(SharedParams::build(geometry, Table::Directory).unwrap());
        let rows = vec![7u8; shared.profile.rows * shared.profile.row_bytes];
        let source = SegmentSource {
            path: dir.path().join("table.bin"),
            rows: Table::Directory.rows(geometry),
            row_bytes: Table::Directory.row_bytes(geometry),
            sha256: hex::encode(Sha256::digest(&rows)),
            zero_from_row: Table::Directory.rows(geometry),
        };
        assert!(std::process::Command::new("mkfifo")
            .arg(&source.path)
            .status()
            .unwrap()
            .success());
        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let writer = {
            let (path, rows) = (source.path.clone(), rows.clone());
            std::thread::spawn(move || {
                let mut file = std::fs::File::create(path).unwrap();
                let _ = opened_tx.send(());
                release_rx.recv().unwrap();
                file.write_all(&rows).unwrap();
            })
        };
        let (bound, planned) = (shared.reserved_bytes(), shared.held_bytes());
        let metrics = Arc::new(Metrics::default());
        let cache = Arc::new(RuntimeCache::new(bound + planned, 2, metrics.clone()));
        let key = |segment| ("ab".repeat(32), Table::Directory, segment);
        let first = {
            let (cache, shared, source) = (cache.clone(), shared.clone(), source.clone());
            tokio::spawn(async move { cache.get(key(0), shared, source).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(30), opened_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cache.resident_bytes(), bound);
        assert!(matches!(
            cache.get(key(1), shared.clone(), source.clone()).await,
            Err(CacheError::Overloaded)
        ));
        assert_eq!(Metrics::get(&metrics.overloads), 1);
        assert_eq!(cache.resident_bytes(), bound);
        release_tx.send(()).unwrap();
        writer.join().unwrap();
        let first = first.await.unwrap().unwrap();
        let held = first.get().held_bytes();
        assert!(held <= planned);
        assert_eq!(cache.resident_bytes(), held);
        assert_eq!(Metrics::get(&metrics.resident_bytes), held);
        std::fs::remove_file(&source.path).unwrap();
        std::fs::write(&source.path, &rows).unwrap();
        let second = cache.get(key(1), shared.clone(), source).await.unwrap();
        assert_eq!(cache.resident_bytes(), held + second.get().held_bytes());
        assert!(cache.resident_bytes() <= cache.budget());
        assert_eq!(Metrics::get(&metrics.builds), 2);
        assert_eq!(Metrics::get(&metrics.evictions), 0);
        assert_eq!(Metrics::get(&metrics.cache_entries), 2);
    }

    /// The warm-fit check is never stricter than the sum of reservations it
    /// replaces, for every history and display geometry: equal while every
    /// runtime can be in flight at once, smaller beyond that by the bound's
    /// excess of each runtime that cannot.
    #[test]
    fn the_warm_fit_never_exceeds_the_reservations_it_replaces() {
        use transparent_shard::display::DISPLAY_PROFILES;
        let mut every = Vec::new();
        for geometry in transparent_shard::layout::PROFILES {
            for table in [Table::Directory, Table::Pages] {
                every.push(SharedParams::build(geometry, table).unwrap());
            }
        }
        for geometry in DISPLAY_PROFILES {
            every.push(SharedParams::build(geometry, Table::TxDirectory).unwrap());
        }
        for shared in &every {
            let excess = shared.reserved_bytes() - shared.held_bytes();
            for count in 0..8u64 {
                for in_flight in 0..6u64 {
                    let set = std::iter::repeat_n(shared, count as usize);
                    let needed = warm_bytes(set, in_flight as usize);
                    let charged = count * shared.reserved_bytes();
                    assert!(needed <= charged);
                    assert_eq!(charged - needed, count.saturating_sub(in_flight) * excess);
                }
            }
        }
        let all = warm_bytes(&every, 4);
        assert!(all < every.iter().map(SharedParams::reserved_bytes).sum::<u64>());
        assert_eq!(
            warm_bytes(&every, every.len()),
            every.iter().map(SharedParams::reserved_bytes).sum::<u64>()
        );
    }

    /// The txid display genesis plan: 850 `txid-2k` runtimes hold 33.2 GiB at
    /// their built size (33.4 GiB with four in flight at their bound), against
    /// 59.8 GiB charged at the bound.
    #[test]
    fn genesis_display_runtimes_fit_at_their_built_size() {
        let shared = SharedParams::build(&TXID_2K, Table::TxDirectory).unwrap();
        let gib = |bytes: u64| bytes as f64 / f64::from(1 << 30);
        let runtimes = std::iter::repeat_n(&shared, 850);
        let needed = warm_bytes(runtimes, 4);
        assert!((gib(needed) - 33.4).abs() < 0.1, "{}", gib(needed));
        assert!((gib(850 * shared.reserved_bytes()) - 59.8).abs() < 0.1);
    }

    /// The native reservation at the pinned geometry: a 32 MiB database, one
    /// block of published masks, and the eight-byte-word bound on one block of
    /// two-mask preprocessing (64 MiB of compiled matrix plus both masks).
    ///
    /// If this fails, the cache budget is being computed against a different
    /// scheme than the one deployed, and every sizing decision downstream of it
    /// is wrong. Re-measure with `shard-residency` rather than adjusting the
    /// formula to match.
    #[test]
    fn the_reservation_is_the_native_upper_bound() {
        assert_eq!(
            reservation(RECENT_8K.directory_rows, RECENT_8K.directory_row_bytes),
            (32 << 20) + 14_848 + 16 + (32 + 2 * 2_048 * 8 + 8) + (64 << 20)
        );
    }

    /// Only the database term follows the row count: the published masks and
    /// preprocessing follow the row width, which the registry holds at one
    /// instance. That is why a taller table is cheap in memory while it divides
    /// the shard count, and it is the whole basis of the archive tier.
    #[test]
    fn only_the_database_term_follows_the_row_count() {
        let narrow = reservation(RECENT_8K.page_rows, RECENT_8K.page_row_bytes);
        let wide = reservation(ARCHIVE_WIDE.page_rows, ARCHIVE_WIDE.page_row_bytes);
        let fixed = narrow - RECENT_8K.page_rows * RECENT_8K.page_row_bytes as u64;
        let ratio = ARCHIVE_WIDE.page_rows / RECENT_8K.page_rows;
        assert_eq!(wide - fixed, (narrow - fixed) * ratio);
    }

    /// The certificate tool rebuilds a segment's masks from its row bytes
    /// without the scan server. It binds its report to a snapshot by requiring
    /// those masks to equal the served ones, so the two derivations must agree.
    #[test]
    fn masks_rebuilt_from_row_bytes_equal_the_served_masks() {
        let shared =
            SharedParams::build(&transparent_shard::layout::RECENT_4K, Table::Pages).unwrap();
        let profile = &shared.profile;
        let rows: Vec<u8> = (0..profile.rows * profile.row_bytes)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 7) as u8)
            .collect();
        let columns: Vec<Vec<u16>> = (0..profile.cols)
            .map(|col| {
                (0..profile.rows)
                    .map(|row| native::row_coefficient(&rows, profile.row_bytes, row, col))
                    .collect()
            })
            .collect();
        let hint =
            native::hint(&profile.masks, profile.rows, profile.cols, |c| &columns[c]).unwrap();
        let rebuilt = native::publish(&native::preprocess(&profile.setup, &hint).unwrap()).unwrap();
        assert_eq!(
            rebuilt,
            TableRuntime::build(&shared, &rows).unwrap().public_params
        );
    }

    /// Leaving trailing zero blocks out of the hint publishes exactly the
    /// masks the whole-table product does: an empty table, content ending
    /// on either side of a block boundary, and content in the last row.
    #[test]
    fn trailing_zero_blocks_leave_the_published_masks_unchanged() {
        let shared =
            SharedParams::build(&transparent_shard::layout::RECENT_4K, Table::Pages).unwrap();
        let profile = &shared.profile;
        let full = |rows: &[u8]| {
            let columns: Vec<Vec<u16>> = (0..profile.cols)
                .map(|col| {
                    (0..profile.rows)
                        .map(|row| native::row_coefficient(rows, profile.row_bytes, row, col))
                        .collect()
                })
                .collect();
            let hint =
                native::hint(&profile.masks, profile.rows, profile.cols, |c| &columns[c]).unwrap();
            native::publish(&native::preprocess(&profile.setup, &hint).unwrap()).unwrap()
        };
        let bytes = profile.rows * profile.row_bytes;
        for (filled, expected_blocks) in [
            (0, 1),
            (1, 1),
            (native::D * profile.row_bytes, 1),
            (native::D * profile.row_bytes + 1, 2),
            (bytes, 2),
        ] {
            let mut rows = vec![0u8; bytes];
            for (i, byte) in rows[..filled].iter_mut().enumerate() {
                *byte = (i.wrapping_mul(2_654_435_761) >> 7) as u8 | 1;
            }
            assert_eq!(
                used_blocks(&rows, profile.row_bytes, profile.rows),
                expected_blocks
            );
            assert_eq!(
                TableRuntime::build(&shared, &rows).unwrap().public_params,
                full(&rows),
                "{filled} leading bytes"
            );
        }
    }

    /// The batched hint changes nothing a client or the disk cache sees. At
    /// the deployed history and display geometries, for a full directory and
    /// a partly filled page table, the runtime publishes the masks of one
    /// prepared from the reference hint over every block, answers byte for
    /// byte as it does, and decodes to the selected row.
    #[test]
    fn the_batched_hint_runtime_is_the_reference_runtime() {
        for (geometry, table, filled) in [
            (&RECENT_8K, Table::Directory, 8_192),
            (&RECENT_8K, Table::Pages, 5_000),
            (&TXID_2K, Table::TxDirectory, 2_048),
            (&TXID_2K, Table::TxDirectory, 700),
        ] {
            let shared = SharedParams::build(geometry, table).unwrap();
            let profile = &shared.profile;
            let mut rows = vec![0u8; profile.rows * profile.row_bytes];
            for (i, byte) in rows[..filled * profile.row_bytes].iter_mut().enumerate() {
                *byte = (i.wrapping_mul(2_654_435_761) >> 9) as u8;
            }
            let fast = TableRuntime::build(&shared, &rows).unwrap();
            let server = database_server(
                &shared,
                (0..profile.rows * profile.cols).map(|i| {
                    native::row_coefficient(
                        &rows,
                        profile.row_bytes,
                        i / profile.cols,
                        i % profile.cols,
                    )
                }),
                false,
            );
            let (padded, db) = (server.db_rows_padded(), server.db());
            let hint = native::hint(&profile.masks, profile.rows, profile.cols, |c| {
                &db[c * padded..c * padded + profile.rows]
            })
            .unwrap();
            let preprocessed = native::preprocess(&profile.setup, &hint).unwrap();
            let reference = TableRuntime::assemble(server, preprocessed).unwrap();
            assert_eq!(fast.public_params, reference.public_params);
            assert_eq!(fast.public_params_epoch, reference.public_params_epoch);
            let binding = [7u8; 8];
            for selected in [0, filled - 1, profile.rows - 1] {
                let (secret, upload) = profile.prepare(selected).unwrap();
                let mut body = binding.to_vec();
                body.extend(upload);
                let answer = fast.evaluate(&shared, binding, &body).unwrap();
                assert_eq!(answer, reference.evaluate(&shared, binding, &body).unwrap());
                let row = profile
                    .decode(&secret, &fast.public_params, &answer[16..])
                    .unwrap();
                let at = selected * profile.row_bytes;
                assert_eq!(
                    row,
                    &rows[at..at + profile.row_bytes],
                    "{} {table:?} row {selected}",
                    geometry.name
                );
            }
        }
    }

    /// Only the recent history geometries and the display geometries take the
    /// batched hint, and their deployed masks are within its capacity, so it
    /// never silently falls back there. Both history archive geometries, and
    /// any history geometry added later, keep the reference product over every
    /// block.
    #[test]
    fn the_batched_hint_is_dispatched_for_recent_geometries_only() {
        use transparent_shard::display::DISPLAY_PROFILES;
        use transparent_shard::layout::{ARCHIVE_32K, PROFILES};
        for geometry in PROFILES {
            assert_eq!(
                uses_batched_hint(geometry),
                geometry.name.starts_with("recent-"),
                "{}",
                geometry.name
            );
        }
        assert!(!uses_batched_hint(&ARCHIVE_32K));
        assert!(!uses_batched_hint(&ARCHIVE_WIDE));
        assert!(uses_batched_hint(&RECENT_8K));
        assert!(DISPLAY_PROFILES.iter().all(uses_batched_hint));
        for geometry in BATCHED_HINT_GEOMETRIES {
            let tables = if DISPLAY_PROFILES.contains(geometry) {
                vec![Table::TxDirectory]
            } else {
                vec![Table::Directory, Table::Pages]
            };
            for table in tables {
                let shared = SharedParams::build(geometry, table).unwrap();
                assert_eq!(
                    native::batched_hint::path(&shared.profile.masks).unwrap(),
                    native::batched_hint::Path::Batched,
                    "{} {table:?}",
                    geometry.name
                );
            }
        }
    }

    /// Every registry geometry must have parameters and a reservation that
    /// fits a plausible budget. A geometry whose runtime did not fit one worker
    /// could never be served at all, and finding that out at first query would
    /// be finding it out in production.
    #[test]
    fn every_registry_geometry_reserves_something_servable() {
        for geometry in transparent_shard::layout::PROFILES {
            for table in [Table::Directory, Table::Pages] {
                let shared = SharedParams::build(geometry, table).unwrap();
                let bytes = shared.reserved_bytes();
                assert!(bytes > 0, "{}", geometry.name);
                assert!(
                    bytes < (2 << 30),
                    "{} reserves {bytes} bytes for one segment",
                    geometry.name
                );
                assert_eq!(shared.profile.blocks(), 1, "{}", geometry.name);
            }
        }
    }
}
