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

use zakura_pir_protocol::transparent::shard as transparent_shard;
use zakura_pir_query_server::transparent::native as transparent_native;

pub mod disk;
pub(crate) mod filecache;
pub mod memory;
pub mod metrics;
pub mod prebuild;
pub mod prepare;
pub mod prewarm;
pub mod procmem;

use crate::history::shardset::{SegmentSource, Table};
use crate::runtime::metrics::Metrics;
use ipir_sp::server::IPIRServer;
use ipir_sp::YpirSchemeParams;
use pir_runtime::evaluation::PreparedDatabase;
use rayon::prelude::*;
use reinspiring::native_kernel::PreparedU16Query;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use transparent_native::{
    self as native, NativeKeys, NativePreprocessed, NativeScheme, Profile, TableProfile,
};
use transparent_shard::layout::Geometry;
use zakura_pir_client::native::ProfileClientExt as _;
use zakura_pir_query_server::native::ProfileQueryExt as _;
use zakura_pir_query_server::transparent::native::TableQueryExt as _;
use zakura_pir_server_core::native::ProfilePublicationExt as _;

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
) -> PreparedDatabase {
    let transport = shared.transport.clone().expect("a 16-bit table");
    #[cfg(feature = "portable-kernel")]
    let server = IPIRServer::<u16>::new(transport, coefficients, transposed, true);
    #[cfg(not(feature = "portable-kernel"))]
    let server = IPIRServer::<u16>::new_auto_kernel(transport, coefficients, transposed, true);
    PreparedDatabase::new(server)
}

/// The first-dimension scan shape for one 16-bit table: full 16-bit plaintexts, one
/// column per coefficient, and the native transport's 49-bit query and 22-bit
/// response widths. The scan itself runs modulo the native `q` = 2^54, on the
/// selection already lifted back to `q`, so it is the same for a 44-bit
/// dithered query; the 49 here only keeps the shape, and with it every runtime
/// disk-cache key, what it was before dithered queries.
pub fn transport_params(rows: u64, row_bytes: u32) -> Result<YpirSchemeParams, String> {
    let mut params = pir_runtime::params::p16q48(rows, row_bytes as u64 * 8)?
        .ypir()
        .clone();
    params.query_bits = native::QUERY_BITS;
    params.q_prime_1 = 1 << native::RESPONSE_BITS;
    Ok(params)
}

/// Bytes one prepared runtime reserves for a `rows` by `row_bytes` segment of
/// a `table`, under that table's native profile.
///
/// Three terms, all derived rather than measured:
///
/// - the **encoded database**, `rows * row_bytes` bytes under either profile
///   (u16 coefficients or one byte each), which is the only term that follows
///   the row count;
/// - the **published masks**, per instance: 14,848 bytes per 4,096-byte
///   16-bit instance, 11,264 per 2,048-byte 8-bit one; and
/// - the **two-mask preprocessing** at its upper bound: per block both masks
///   and a `d x d*ell` compiled matrix at eight-byte words (64 MiB for the
///   two-digit 16-bit key, 32 MiB for the one-digit 8-bit key). A two-digit
///   matrix is stored at four-byte words when every entry fits, so a real
///   runtime may hold half of that term; the reservation takes the bound
///   because it is made before the build that decides it, and the cache
///   releases the difference once the build is done. A one-digit matrix
///   needs eight-byte words unless its table is nearly empty.
///
/// `shard-scaling --geometry-sweep` and the router price the same terms, so a
/// sweep's projection and a running worker's budget cannot drift apart.
///
/// It is a **reservation**, not a measurement of the process. It excludes
/// allocator fragmentation, the transient plaintext buffer and hint a build
/// reads, the shared parameters, and anything a request allocates. A cache
/// budget is therefore always set below the host's real headroom, and
/// re-checked with `shard-residency` whenever the scheme moves.
pub fn reserved_bytes(table: Table, rows: u64, row_bytes: u32) -> u64 {
    let profile = native::profile_for(table.as_str());
    let blocks = (row_bytes as usize / profile.instance_bytes()) as u64;
    rows * row_bytes as u64
        + blocks * profile.block_public_bytes() as u64
        + native::PREPARED_HEADER_BYTES
        + blocks * profile.prepared_block_max_bytes()
}

/// Bytes a prepared runtime plans to hold once built: [`reserved_bytes`] less
/// half of the matrix term for a two-digit key, whose matrix fits four-byte
/// words, and the reservation itself for a one-digit key, whose matrix needs
/// eight-byte words on any table with data.
///
/// This is the size the cache charges a built runtime in practice, so it is
/// what warm-fit and disk preflight checks plan with. It is a planning figure,
/// not a bound: a runtime whose matrix needs eight-byte words is charged its
/// full reservation, and a plan made with this figure then comes up short and
/// says so through overloads rather than through memory.
pub fn held_bytes(table: Table, rows: u64, row_bytes: u32) -> u64 {
    let profile = native::profile_for(table.as_str());
    let blocks = (row_bytes as usize / profile.instance_bytes()) as u64;
    reserved_bytes(table, rows, row_bytes)
        - blocks * (profile.prepared_block_max_bytes() - profile.prepared_block_planned_bytes())
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
    /// The first-dimension scan shape of a 16-bit table; an 8-bit table is
    /// scanned by the byte kernel and has none.
    pub transport: Option<YpirSchemeParams>,
    pub setup_seed: u64,
}

/// A query body parsed once and answered by every segment.
pub struct ParsedQuery {
    keys: NativeKeys,
    query: Vec<u64>,
}

impl SharedParams {
    /// A history table's parameters, under the history schema.
    pub fn build(geometry: &'static Geometry, table: Table) -> Result<Self, String> {
        Self::build_in(transparent_shard::manifest::SCHEMA, geometry, table)
    }

    /// A display table's parameters, under display's own native schema.
    pub fn build_display(geometry: &'static Geometry, table: Table) -> Result<Self, String> {
        Self::build_in(transparent_shard::display::NATIVE_SCHEMA, geometry, table)
    }

    fn build_in(schema: &str, geometry: &'static Geometry, table: Table) -> Result<Self, String> {
        let rows = table.rows(geometry);
        let row_bytes = table.row_bytes(geometry);
        Ok(Self {
            geometry,
            table,
            profile: TableProfile::new(schema, geometry.name, table.as_str(), rows, row_bytes)?,
            transport: if native::profile_for(table.as_str()) == native::P16 {
                Some(transport_params(rows, row_bytes)?)
            } else {
                None
            },
            setup_seed: transparent_shard::display::setup_seed_in(
                schema,
                geometry.name,
                table.as_str(),
            ),
        })
    }

    /// What `/v2/shards/init` publishes for this table as its scheme: the
    /// 49-bit nearest-rounded query. Also what keys the runtime disk cache.
    pub fn scheme(&self) -> &NativeScheme {
        &self.profile.scheme
    }

    /// The 44-bit dithered scheme `/v2/shards/init` publishes beside a 16-bit
    /// table's; an 8-bit table's own scheme is its dithered one.
    pub fn dithered_scheme(&self) -> Option<&NativeScheme> {
        self.profile.dithered_scheme.as_ref()
    }

    /// The length of a 49-bit query body: the 8-byte binding, the `K_g`
    /// packing key and the 49-bit selection. The longer of the two accepted
    /// lengths, so it is also the most a body may be.
    pub fn query_bytes(&self) -> usize {
        8 + self.profile.scheme.request_bytes
    }

    /// The length of a 16-bit table's 44-bit dithered query body.
    pub fn dithered_query_bytes(&self) -> Option<usize> {
        self.dithered_scheme()
            .map(|scheme| 8 + scheme.request_bytes)
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

    /// Every exact length a query body to this table may have, for refusals:
    /// each accepted width over every row and, when `query_rows` is fewer,
    /// over only those rows.
    pub fn query_lengths(&self, query_rows: usize) -> String {
        let native = self.profile.native;
        let widths: Vec<usize> = native
            .nearest_bits
            .into_iter()
            .chain([native.dithered_bits])
            .collect();
        let mut lengths: Vec<String> = widths
            .iter()
            .map(|&bits| (8 + native.request_len_bits(self.profile.rows, bits)).to_string())
            .collect();
        if query_rows < self.profile.rows {
            lengths.extend(widths.iter().map(|&bits| {
                format!(
                    "{} over its first {query_rows} rows",
                    8 + native.request_len_bits(query_rows, bits)
                )
            }));
        }
        lengths.join(" or ")
    }

    /// Bytes one segment's answer carries: binding, epoch and body.
    pub fn response_bytes(&self) -> usize {
        16 + self.profile.scheme.response_bytes
    }

    pub fn reserved_bytes(&self) -> u64 {
        reserved_bytes(
            self.table,
            self.profile.rows as u64,
            self.profile.row_bytes as u32,
        )
    }

    /// See [`held_bytes`].
    pub fn held_bytes(&self) -> u64 {
        held_bytes(
            self.table,
            self.profile.rows as u64,
            self.profile.row_bytes as u32,
        )
    }

    /// Checks the binding and length, then parses the key and selection once
    /// for every segment that will answer them.
    ///
    /// `query_rows` is the page shard's [`query_rows`] for pages and every
    /// row otherwise. A body selecting over only those rows is zero-filled to
    /// the full selection; the rows it omits are zero in every segment.
    ///
    /// [`query_rows`]: transparent_shard::manifest::PageChild::query_rows
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

/// Columns [`first_differing_column`] compares per pass over the rows: one
/// interleaved block of a byte database, so both it and the row-major
/// segment are read a cache line at a time instead of one line per byte.
const SELF_CHECK_COLUMNS: usize = 16;

/// The lowest column with a row where `differs`, visiting every cell of a
/// `cols` x `rows` table. Columns are taken in tiles of
/// [`SELF_CHECK_COLUMNS`] and every row is read within a tile; a column-major
/// walk of row-major segment bytes touches a new page for every byte.
fn first_differing_column(
    cols: usize,
    rows: usize,
    differs: impl Fn(usize, usize) -> bool,
) -> Option<usize> {
    (0..cols).step_by(SELF_CHECK_COLUMNS).find_map(|start| {
        let tile = start..(start + SELF_CHECK_COLUMNS).min(cols);
        (0..rows)
            .any(|row| tile.clone().any(|col| differs(row, col)))
            .then(|| {
                tile.clone()
                    .find(|&col| (0..rows).any(|row| differs(row, col)))
            })
            .flatten()
    })
}

/// A runtime's encoded database.
pub(crate) enum Database {
    /// A 16-bit table's coefficients, scanned by ipir-sp's server.
    Wide(PreparedDatabase),
    /// An 8-bit table's bytes, one per coefficient, in the byte kernel's
    /// interleaved layout ([`interleave`]).
    Bytes(Vec<u8>),
}

#[cfg(test)]
impl Database {
    /// The stored database as bytes, to compare two runtimes'.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        match self {
            Database::Wide(server) => server.db().iter().flat_map(|c| c.to_le_bytes()).collect(),
            Database::Bytes(db) => db.clone(),
        }
    }
}

/// An 8-bit table's bytes in column-major order: entry `c` of row `r` is byte
/// `c` of that row.
fn column_major_bytes(rows: &[u8], row_count: usize, row_bytes: usize) -> Vec<u8> {
    let mut db = vec![0u8; row_count * row_bytes];
    db.par_chunks_mut(row_count)
        .enumerate()
        .for_each(|(col, column)| {
            for (row, entry) in column.iter_mut().enumerate() {
                *entry = rows[row * row_bytes + col];
            }
        });
    db
}

/// Rearranges column-major bytes into the byte kernel's tiles, band by band
/// of 16 columns. The layout is the same on every CPU: the kernel's portable
/// path scans it too, so a disk-cache entry restores anywhere.
pub(crate) fn interleave(db: &mut [u8], rows: usize) {
    db.par_chunks_mut(rows * 16)
        .for_each(|band| PreparedU16Query::interleave_u8_columns(band, rows));
}

/// Where entry (`col`, `row`) of a `rows`-row table sits in [`interleave`]'s
/// layout: its 16-column band, then four rows of each of the band's columns.
pub(crate) fn interleaved_index(rows: usize, col: usize, row: usize) -> usize {
    (col / 16) * rows * 16 + (row - row % 4) * 16 + (col % 16) * 4 + row % 4
}

/// One segment of one shard revision's table, prepared to answer queries.
pub struct TableRuntime {
    pub(crate) native: Profile,
    pub(crate) preprocessed: Vec<NativePreprocessed>,
    pub(crate) db: Database,
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
        if shared.profile.native == native::P8 {
            return Self::build_bytes(shared, rows);
        }
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
        let hint = zakura_pir_server_core::native::hint(
            &profile.masks,
            profile.rows,
            profile.cols,
            |col| &db[col * padded..col * padded + profile.rows],
        )?;
        tracing::debug!(
            geometry = shared.geometry.name,
            table = shared.table.as_str(),
            seconds = phase.elapsed().as_secs_f64(),
            stage = "hint_columns",
            "construction stage"
        );
        phase = std::time::Instant::now();
        let preprocessed = zakura_pir_server_core::native::preprocess(&profile.setup, &hint)?;
        drop(hint);
        tracing::debug!(
            geometry = shared.geometry.name,
            table = shared.table.as_str(),
            seconds = phase.elapsed().as_secs_f64(),
            stage = "pack_preprocessing",
            "construction stage"
        );
        Self::assemble(profile.native, Database::Wide(server), preprocessed)
    }

    /// [`Self::build`] for an 8-bit table: the reference hint over every
    /// column-major row, then the bytes rearranged for the byte kernel.
    fn build_bytes(shared: &SharedParams, rows: &[u8]) -> Result<Self, String> {
        let profile = &shared.profile;
        let row_count = profile.rows;
        let mut db = column_major_bytes(rows, row_count, profile.row_bytes);
        let hint =
            zakura_pir_server_core::native::hint(&profile.masks, row_count, profile.cols, |col| {
                &db[col * row_count..(col + 1) * row_count]
            })?;
        let preprocessed = zakura_pir_server_core::native::preprocess(&profile.setup, &hint)?;
        drop(hint);
        interleave(&mut db, row_count);
        Self::assemble(profile.native, Database::Bytes(db), preprocessed)
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
        let database = match &self.db {
            Database::Wide(server) => server.db().len() as u64 * 2,
            Database::Bytes(db) => db.len() as u64,
        };
        database
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
        native: Profile,
        db: Database,
        preprocessed: Vec<NativePreprocessed>,
    ) -> Result<Self, String> {
        let public_params = native.publish(&preprocessed)?;
        let digest = Sha256::digest(&public_params);
        let mut epoch = [0u8; 8];
        epoch.copy_from_slice(&digest[..8]);
        Ok(Self {
            native,
            preprocessed,
            db,
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
        match &self.db {
            Database::Wide(server) => {
                let padded = server.db_rows_padded();
                let db = server.db();
                if let Some(col) = first_differing_column(profile.cols, profile.rows, |row, col| {
                    db[col * padded + row]
                        != native::row_coefficient(rows, profile.row_bytes, row, col)
                }) {
                    return Err(format!("database column {col} differs from the segment"));
                }
            }
            Database::Bytes(db) => {
                if db.len() != rows.len() {
                    return Err("database length differs from the segment".into());
                }
                if let Some(col) = first_differing_column(profile.cols, profile.rows, |row, col| {
                    db[interleaved_index(profile.rows, col, row)]
                        != rows[row * profile.row_bytes + col]
                }) {
                    return Err(format!("database column {col} differs from the segment"));
                }
            }
        }
        let row = sample_row(rows, profile.row_bytes, profile.rows);
        let binding = *b"selfchck";
        let (secret, upload) =
            profile
                .native
                .prepare(&profile.setup, &profile.masks, profile.rows, row)?;
        let mut body = binding.to_vec();
        body.extend(upload);
        let answer = self.evaluate(shared, binding, &body)?;
        let mut decoded = profile.native.decode_cols(
            &secret,
            &self.public_params,
            &answer[16..],
            profile.cols,
        )?;
        decoded.truncate(profile.row_bytes);
        let at = row * profile.row_bytes;
        if decoded != rows[at..at + profile.row_bytes] {
            return Err(format!("row {row} does not decode to the segment's row"));
        }
        Ok(row)
    }

    /// Answers a query already parsed by [`SharedParams::parse`].
    pub fn answer(&self, binding: [u8; 8], query: &ParsedQuery) -> Result<Vec<u8>, String> {
        let intermediate = match &self.db {
            Database::Wide(server) => server
                .try_multiply_power_of_two(native::Q, &query.query)
                .map_err(|error| error.to_string())?,
            Database::Bytes(db) => {
                let rows = query.query.len();
                let prepared =
                    PreparedU16Query::new(&query.query, native::Q).map_err(|e| e.to_string())?;
                let mut intermediate = vec![0u64; self.preprocessed.len() * native::D];
                if db.len() != rows * intermediate.len() {
                    return Err("query rows do not match the database".into());
                }
                intermediate
                    .par_chunks_mut(16)
                    .zip(db.par_chunks(rows * 16))
                    .for_each(|(out, band)| prepared.multiply_interleaved_u8(band, out));
                intermediate
            }
        };
        let body = self
            .native
            .pack(&self.preprocessed, &query.keys, &intermediate)?;
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
    work_memory: Arc<crate::runtime::memory::WorkMemory>,
    inner: Mutex<Inner>,
    budget: u64,
    build_slots: Arc<tokio::sync::Semaphore>,
    restore_slots: Arc<tokio::sync::Semaphore>,
    prewarm_concurrency: usize,
    background_slots: Arc<tokio::sync::Semaphore>,
    metrics: Arc<Metrics>,
    disk: Option<disk::DiskCache>,
    /// Shipped files refused by a production operation that then could not
    /// be admitted to restore or build, by key and shipped directory. Its
    /// admission retry goes straight to restore or build, so one operation
    /// loads, and counts, a refused file once. Cleared for a key when its
    /// operation produces the runtime or fails for good.
    refused_shipped: Mutex<std::collections::HashSet<(RuntimeKey, std::path::PathBuf)>>,
}

/// Refusals [`RuntimeCache::refused_shipped`] keeps for operations still
/// retrying admission; past this the record is cleared, so a retry that
/// never came cannot grow it. A cleared operation may count once more.
const REFUSALS_KEPT: usize = 4096;

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
        let mut inner = self.inner.lock().expect("runtime cache");
        while let Some(key) = Self::least_recently_used_unpinned(&inner) {
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
pub(crate) mod tests {

    #[test]
    fn repair_evicts_only_affected_idle_runtime_entries() {
        let root = tempfile::tempdir().unwrap();
        let disk = disk::DiskCache::new(root.path().to_path_buf(), 1024).unwrap();
        let cache = RuntimeCache::new(1024, 1, Arc::new(Metrics::default())).with_disk(Some(disk));
        let affected = ("affected".to_string(), Table::Directory, 0);
        let healthy = ("healthy".to_string(), Table::Directory, 0);
        use sha2::{Digest, Sha256};
        let affected_disk = root.path().join(format!(
            "{}-cached.runtime",
            hex::encode(Sha256::digest(affected.0.as_bytes()))
        ));
        let healthy_disk = root.path().join(format!(
            "{}-cached.runtime",
            hex::encode(Sha256::digest(healthy.0.as_bytes()))
        ));
        std::fs::write(&affected_disk, b"derived from corrupt source").unwrap();
        std::fs::write(&healthy_disk, b"unrelated runtime").unwrap();
        let pinned = Arc::new(Slot {
            runtime: tokio::sync::OnceCell::new(),
        });
        {
            let mut inner = cache.inner.lock().unwrap();
            inner.entries.insert(
                affected.clone(),
                Entry {
                    slot: pinned.clone(),
                    reserved: 10,
                    last_used: 0,
                },
            );
            inner.entries.insert(
                healthy.clone(),
                Entry {
                    slot: Arc::new(Slot {
                        runtime: tokio::sync::OnceCell::new(),
                    }),
                    reserved: 20,
                    last_used: 0,
                },
            );
            inner.resident = 30;
        }
        assert!(!cache.evict_keys(std::slice::from_ref(&affected)).unwrap());
        assert!(
            affected_disk.exists(),
            "an active snapshot writer keeps its file"
        );
        assert_eq!(
            cache.resident_bytes(),
            30,
            "an in-flight request retains its budget"
        );
        drop(pinned);
        let writer = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.path().join(".lock"))
            .unwrap();
        writer.lock().unwrap();
        assert!(
            !cache.evict_keys(std::slice::from_ref(&affected)).unwrap(),
            "an unrelated disk writer must defer repair without blocking the cache"
        );
        assert!(affected_disk.exists());
        drop(writer);
        assert!(cache.evict_keys(std::slice::from_ref(&affected)).unwrap());
        assert!(
            !affected_disk.exists(),
            "a repaired source cannot restore its old derivative"
        );
        assert!(healthy_disk.exists());
        assert_eq!(cache.resident_bytes(), 20);
        assert!(cache.inner.lock().unwrap().entries.contains_key(&healthy));
    }

    /// The scan shape keys every prepared runtime and disk cache entry. The
    /// digest was captured on the base commit before the shared runtime
    /// extraction; it must never change.
    #[test]
    fn transport_params_are_pinned() {
        let shapes = [(2048, 4096), (4096, 4096), (65536, 4096)]
            .map(|(rows, row_bytes)| super::transport_params(rows, row_bytes).unwrap());
        assert_eq!(
            hex::encode(<sha2::Sha256 as sha2::Digest>::digest(
                serde_json::to_vec(&shapes).unwrap()
            )),
            "d270081346ced7386c22ffc61e3b504090e9951242f5fa5b3e1598eac2c6ee48"
        );
    }

    /// The tiled walk visits every cell and names the lowest differing
    /// column, as the column-order walk it replaced did, at tile edges and in
    /// a partial last tile.
    #[test]
    fn self_check_tiles_find_the_lowest_differing_column() {
        let (cols, rows) = (2 * SELF_CHECK_COLUMNS + 5, 9);
        assert_eq!(first_differing_column(cols, rows, |_, _| false), None);
        for col in 0..cols {
            for row in [0, rows / 2, rows - 1] {
                assert_eq!(
                    first_differing_column(cols, rows, |r, c| (r, c) == (row, col)),
                    Some(col)
                );
            }
            let other = (col + SELF_CHECK_COLUMNS) % cols;
            assert_eq!(
                first_differing_column(cols, rows, |r, c| (r == 0 && c == other)
                    || (r == rows - 1 && c == col)),
                Some(col.min(other))
            );
        }
    }

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
    use transparent_shard::display::{TXID_2K8, TXID_4K8};
    use transparent_shard::layout::{DECOUPLED_2K_8K, DECOUPLED_32K_32K};

    // Cache units exercise the generic native API with one supported column
    // instance. These are test domains, never advertised publication geometries.
    // Production HTTP checks and qualification retain the registered layouts.
    pub(crate) const NATIVE_INSTANCE: transparent_shard::layout::Geometry =
        transparent_shard::layout::Geometry {
            name: "test-native-instance",
            directory_row_bytes: 2_048,
            page_row_bytes: 2_048,
            ..TXID_2K8
        };

    /// The masks a table's rows publish, from the whole-table hint over the
    /// columns the profile encodes (single bytes for an 8-bit table, u16
    /// pairs for a 16-bit one), without the scan server: what the certificate
    /// tool rebuilds.
    /// Ordinary cache/identity checks need valid native arithmetic, not large
    /// history rows. Full qualification retains the caller's deployed shape.
    pub(crate) fn fixture_shape(
        geometry: &'static transparent_shard::layout::Geometry,
        table: Table,
    ) -> (&'static transparent_shard::layout::Geometry, Table) {
        if cfg!(feature = "qualification-tests") {
            (geometry, table)
        } else {
            (&NATIVE_INSTANCE, Table::TxDirectory)
        }
    }

    /// Share only parameters of the same schema. History and display derive
    /// different masks even when their test dimensions match.
    pub(crate) fn fixture_params(
        geometry: &'static transparent_shard::layout::Geometry,
        table: Table,
        display: bool,
    ) -> Arc<SharedParams> {
        if cfg!(feature = "qualification-tests") {
            return Arc::new(if display {
                SharedParams::build_display(geometry, table).unwrap()
            } else {
                SharedParams::build(geometry, table).unwrap()
            });
        }
        assert_eq!(geometry, &NATIVE_INSTANCE);
        assert_eq!(table, Table::TxDirectory);
        static HISTORY: std::sync::LazyLock<Arc<SharedParams>> = std::sync::LazyLock::new(|| {
            Arc::new(SharedParams::build(&NATIVE_INSTANCE, Table::TxDirectory).unwrap())
        });
        static DISPLAY: std::sync::LazyLock<Arc<SharedParams>> = std::sync::LazyLock::new(|| {
            Arc::new(SharedParams::build_display(&NATIVE_INSTANCE, Table::TxDirectory).unwrap())
        });
        if display {
            DISPLAY.clone()
        } else {
            HISTORY.clone()
        }
    }

    /// Two fully occupied deterministic inputs for recovery tests. All rows
    /// differ between inputs, so a random self-check cannot miss the forgery.
    pub(crate) fn recovery_rows(shared: &SharedParams, different: bool) -> Vec<u8> {
        let seed = if different { 5 } else { 4 };
        (0..shared.profile.rows * shared.profile.row_bytes)
            .map(|i| ((i + seed * 7_919).wrapping_mul(2_654_435_761) >> 9) as u8)
            .collect()
    }

    /// Prepare immutable native bytes once; every consumer gets a private file
    /// before testing corruption, replacement or retirement. Qualification
    /// deliberately keeps independent cold builds.
    pub(crate) fn prepared_fixture(
        shared: &SharedParams,
        rows: &[u8],
    ) -> Result<TableRuntime, String> {
        use disk::ShippedRuntimes;
        if cfg!(feature = "qualification-tests") {
            return TableRuntime::build(shared, rows);
        }
        static LOCAL: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
        let root = std::env::var_os("WALLET_PIR_TEST_RUNTIME_CACHE")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                LOCAL
                    .get_or_init(|| tempfile::tempdir().unwrap())
                    .path()
                    .to_path_buf()
            })
            .join("transparent-predecessors");
        std::fs::create_dir_all(&root).unwrap();
        let shared_files = ShippedRuntimes::new(root.clone());
        let key = ("ab".repeat(32), shared.table, 0);
        let digest = hex::encode(Sha256::digest(rows));
        let path = shared_files.path(&key, shared, &digest);
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join("fixture.lock"))
            .unwrap();
        lock.lock().unwrap();
        if !path.exists() {
            let runtime = TableRuntime::build(shared, rows)?;
            shared_files.write(&key, shared, &digest, &runtime).unwrap();
        }
        let private = tempfile::tempdir().unwrap();
        let files = ShippedRuntimes::new(private.path().to_path_buf());
        std::fs::copy(&path, files.path(&key, shared, &digest)).unwrap();
        drop(lock);
        // Loaded runtimes own their mappings after the private pathname is
        // removed; no consumer can mutate the retained fixture's inode.
        files
            .load(&key, shared, &digest)
            .map_err(|error| error.to_string())
    }

    fn reference_masks(profile: &native::TableProfile, rows: &[u8]) -> Vec<u8> {
        let hint = if profile.native.entry_bytes() == 1 {
            let columns: Vec<Vec<u8>> = (0..profile.cols)
                .map(|col| {
                    (0..profile.rows)
                        .map(|row| rows[row * profile.row_bytes + col])
                        .collect()
                })
                .collect();
            zakura_pir_server_core::native::hint(&profile.masks, profile.rows, profile.cols, |c| {
                &columns[c]
            })
        } else {
            let columns: Vec<Vec<u16>> = (0..profile.cols)
                .map(|col| {
                    (0..profile.rows)
                        .map(|row| native::row_coefficient(rows, profile.row_bytes, row, col))
                        .collect()
                })
                .collect();
            zakura_pir_server_core::native::hint(&profile.masks, profile.rows, profile.cols, |c| {
                &columns[c]
            })
        }
        .unwrap();
        profile
            .native
            .publish(&zakura_pir_server_core::native::preprocess(&profile.setup, &hint).unwrap())
            .unwrap()
    }

    fn reservation(table: Table, rows: u64, row_bytes: usize) -> u64 {
        reserved_bytes(table, rows, row_bytes as u32)
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

    fn mib(bytes: u64) -> f64 {
        bytes as f64 / f64::from(1 << 20)
    }

    /// A `txid-2k8` display runtime is 8-bit: its 8,192-byte rows are four
    /// 2,048-byte blocks, each with a one-digit compiled matrix at eight-byte
    /// words, so the planned built size is the reservation, 144.17 MiB.
    #[test]
    fn the_planned_built_size_of_a_display_runtime_is_its_reservation() {
        let shared = SharedParams::build_display(&TXID_2K8, Table::TxDirectory).unwrap();
        assert_eq!(shared.profile.blocks(), 4);
        let (reserved, held) = (shared.reserved_bytes(), shared.held_bytes());
        assert_eq!((reserved, held), (151_171_248, 151_171_248));
        assert!((mib(held) - 144.17).abs() < 0.01);
    }

    /// A `txid-4k8` runtime has twice the rows at the same four blocks: the
    /// database doubles, the compiled matrices do not, so 160.17 MiB.
    #[test]
    fn the_planned_built_size_of_a_txid_4k8_runtime_is_its_reservation() {
        let shared = SharedParams::build_display(&TXID_4K8, Table::TxDirectory).unwrap();
        assert_eq!(shared.profile.blocks(), 4);
        let (reserved, held) = (shared.reserved_bytes(), shared.held_bytes());
        assert_eq!(reserved, held);
        assert!((mib(held) - 160.17).abs() < 0.01, "{reserved}");
    }

    /// A built runtime, history or display, holds no more than the planned
    /// four-byte size and no less than the 27-bit packed matrix a CPU with
    /// AVX-512 VBMI may choose, so its charge never exceeds its reservation.
    #[test]
    fn a_built_runtime_holds_its_planned_size_or_less() {
        let matrix_narrow = (native::D * native::D * native::ELL * 4) as u64;
        let matrix_packed = (native::D * (native::D * native::ELL / 8) * 27 + 8) as u64;
        for (geometry, table) in [
            (&DECOUPLED_2K_8K, Table::Directory),
            fixture_shape(&TXID_2K8, Table::TxDirectory),
        ] {
            let shared = SharedParams::build(geometry, table).unwrap();
            let profile = &shared.profile;
            let blocks = profile.blocks() as u64;
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
            if profile.native.ell == 1 {
                // A one-digit matrix needs eight-byte words on a table with
                // data: the 8-bit history tables hold their reservation.
                assert_eq!(held, shared.reserved_bytes(), "{}", geometry.name);
                continue;
            }
            assert!(
                held >= shared.held_bytes() - blocks * (matrix_narrow - matrix_packed),
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
        let shared = SharedParams::build_display(&TXID_2K8, Table::TxDirectory).unwrap();
        // A runtime built below its bound, as an empty table's is.
        let (bound, held) = (shared.reserved_bytes(), shared.reserved_bytes() / 2);
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
        // An empty 8-bit table: its one-digit matrix fits four-byte words, so
        // it is built narrower than its bound, which a table with data is not.
        let (geometry, table) = fixture_shape(&TXID_2K8, Table::TxDirectory);
        let dir = tempfile::tempdir().unwrap();
        let shared = Arc::new(SharedParams::build(geometry, table).unwrap());
        let rows = vec![0u8; shared.profile.rows * shared.profile.row_bytes];
        let built = TableRuntime::build(&shared, &rows).unwrap().held_bytes();
        let bound = shared.reserved_bytes();
        assert!(built < bound, "an empty table is built below its bound");
        let source = SegmentSource::published(
            dir.path().join("table.bin"),
            table.rows(geometry),
            table.row_bytes(geometry),
            hex::encode(Sha256::digest(&rows)),
            table.rows(geometry),
        );
        assert!(std::process::Command::new("mkfifo")
            .arg(source.path())
            .status()
            .unwrap()
            .success());
        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let writer = {
            let (path, rows) = (source.path().to_path_buf(), rows.clone());
            std::thread::spawn(move || {
                let mut file = std::fs::File::create(path).unwrap();
                let _ = opened_tx.send(());
                release_rx.recv().unwrap();
                file.write_all(&rows).unwrap();
            })
        };
        let metrics = Arc::new(Metrics::default());
        let cache = Arc::new(RuntimeCache::new(bound + built, 2, metrics.clone()));
        let key = move |segment| ("ab".repeat(32), table, segment);
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
        assert_eq!(held, built);
        assert_eq!(cache.resident_bytes(), held);
        assert_eq!(Metrics::get(&metrics.resident_bytes), held);
        std::fs::remove_file(source.path()).unwrap();
        std::fs::write(source.path(), &rows).unwrap();
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
            every.push(SharedParams::build_display(geometry, Table::TxDirectory).unwrap());
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
        // Every table is 8-bit and holds its bound, so there is no excess.
        let all = warm_bytes(&every, 4);
        assert!(all <= every.iter().map(SharedParams::reserved_bytes).sum::<u64>());
        assert_eq!(
            warm_bytes(&every, every.len()),
            every.iter().map(SharedParams::reserved_bytes).sum::<u64>()
        );
    }

    /// Runtimes a combined display worker holds at genesis coverage: 189
    /// archives of 90,000 entries (17.05M entries at the 2026-10-11 tip), the
    /// recent shard and one retired recent revision.
    pub(crate) const GENESIS_RUNTIMES: usize = 191;

    /// The txid display genesis plan: 191 8-bit `txid-2k8` runtimes hold
    /// 26.9 GiB, their built size being their bound, which fits a 32 GiB
    /// cache and the display worker's 48G `MemoryMax`.
    #[test]
    fn genesis_display_runtimes_fit_at_their_built_size() {
        let shared = SharedParams::build_display(&TXID_2K8, Table::TxDirectory).unwrap();
        let gib = |bytes: u64| bytes as f64 / f64::from(1 << 30);
        let runtimes = std::iter::repeat_n(&shared, GENESIS_RUNTIMES);
        let needed = warm_bytes(runtimes, 4);
        assert!((gib(needed) - 26.89).abs() < 0.1, "{}", gib(needed));
        assert_eq!(needed, GENESIS_RUNTIMES as u64 * shared.reserved_bytes());
        assert!(needed < 32 << 30, "a 32 GiB cache");
    }

    /// At `txid-4k8` with 180,000-entry archives, genesis coverage is about
    /// 95 archives, so about 97 runtimes: 15.2 GiB.
    #[test]
    fn genesis_txid_4k8_runtimes_fit_at_their_built_size() {
        let shared = SharedParams::build_display(&TXID_4K8, Table::TxDirectory).unwrap();
        let gib = |bytes: u64| bytes as f64 / f64::from(1 << 30);
        let needed = warm_bytes(std::iter::repeat_n(&shared, 97), 4);
        assert!((gib(needed) - 15.17).abs() < 0.1, "{}", gib(needed));
        assert!(needed < 32 << 30, "a 32 GiB cache");
    }

    /// The native reservation at the pinned geometry. A recent page table
    /// (8-bit): a 32 MiB database, two blocks of published masks, and the
    /// bound on two blocks of one-digit preprocessing (32 MiB of compiled
    /// matrix plus both masks each). A txid display table (8-bit) the same
    /// per block, at four 2,048-byte blocks per 8,192-byte row.
    ///
    /// If this fails, the cache budget is being computed against a different
    /// scheme than the one deployed, and every sizing decision downstream of it
    /// is wrong. Re-measure with `shard-residency` rather than adjusting the
    /// formula to match.
    #[test]
    fn the_reservation_is_the_native_upper_bound() {
        assert_eq!(
            reservation(
                Table::Pages,
                DECOUPLED_2K_8K.page_rows,
                DECOUPLED_2K_8K.page_row_bytes
            ),
            (32 << 20) + 2 * 11_264 + 16 + 2 * ((32 + 2 * 2_048 * 8 + 8) + (32 << 20))
        );
        let (rows, row_bytes) = (
            Table::TxDirectory.rows(&TXID_2K8),
            Table::TxDirectory.row_bytes(&TXID_2K8),
        );
        let blocks = u64::from(row_bytes) / 2_048;
        assert_eq!(
            reservation(Table::TxDirectory, rows, row_bytes as usize),
            rows * u64::from(row_bytes)
                + blocks * 11_264
                + 16
                + blocks * ((32 + 2 * 2_048 * 8 + 8) + (32 << 20))
        );
    }

    /// Only the database term follows the row count: the published masks and
    /// preprocessing follow the row width, which the registry holds at one
    /// instance. That is why a taller table is cheap in memory while it divides
    /// the shard count, and it is the whole basis of the archive tier.
    #[test]
    fn only_the_database_term_follows_the_row_count() {
        let narrow = reservation(
            Table::Pages,
            DECOUPLED_2K_8K.page_rows,
            DECOUPLED_2K_8K.page_row_bytes,
        );
        let wide = reservation(
            Table::Pages,
            DECOUPLED_32K_32K.page_rows,
            DECOUPLED_32K_32K.page_row_bytes,
        );
        let fixed = narrow - DECOUPLED_2K_8K.page_rows * DECOUPLED_2K_8K.page_row_bytes as u64;
        let ratio = DECOUPLED_32K_32K.page_rows / DECOUPLED_2K_8K.page_rows;
        assert_eq!(wide - fixed, (narrow - fixed) * ratio);
    }

    /// The certificate tool rebuilds a segment's masks from its row bytes
    /// without the scan server. It binds its report to a snapshot by requiring
    /// those masks to equal the served ones, so the two derivations must agree.
    #[test]
    fn masks_rebuilt_from_row_bytes_equal_the_served_masks() {
        // Fast uses one native column instance. Qualification keeps the
        // deployed history page geometry this oracle was first pinned on.
        let (geometry, table) = fixture_shape(&DECOUPLED_2K_8K, Table::Pages);
        let shared = SharedParams::build(geometry, table).unwrap();
        let profile = &shared.profile;
        let rows: Vec<u8> = (0..profile.rows * profile.row_bytes)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 7) as u8)
            .collect();
        let rebuilt = reference_masks(profile, &rows);
        assert_eq!(
            rebuilt,
            TableRuntime::build(&shared, &rows).unwrap().public_params
        );
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
                // One byte per coefficient: a 4,096-byte row is two blocks.
                assert_eq!(shared.profile.blocks(), 2, "{}", geometry.name);
            }
        }
    }
}
