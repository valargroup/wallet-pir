//! Table profiles, request shapes and runtime memory estimates.
use super::*;

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
    pub setup_seed: u64,
}

/// A query body parsed once and answered by every segment.
pub struct ParsedQuery {
    pub(super) keys: NativeKeys,
    pub(super) query: Vec<u64>,
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
            setup_seed: transparent_shard::display::setup_seed_in(
                schema,
                geometry.name,
                table.as_str(),
            ),
        })
    }

    /// The P8 scheme published by setup and included in runtime cache identities.
    pub fn scheme(&self) -> &NativeScheme {
        &self.profile.scheme
    }

    /// Optional secondary scheme in the setup response. Current P8 tables
    /// have no secondary scheme; their primary query is already dithered.
    pub fn dithered_scheme(&self) -> Option<&NativeScheme> {
        self.profile.dithered_scheme.as_ref()
    }

    /// The full-row query length: binding, packing key and dithered selection.
    pub fn query_bytes(&self) -> usize {
        8 + self.profile.scheme.request_bytes
    }

    /// Optional secondary query length, absent for current P8 tables.
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
