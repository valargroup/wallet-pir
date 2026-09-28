//! Pinned table geometry, identical for every shard.
//!
//! Uniformity is the whole point. The native ReinspiRING profile's public query
//! setup is a pure function of the geometry and table, so shards that share a
//! geometry share one set of scheme parameters — a client validates parameters and derives its
//! query setup once for every shard it will ever query, instead of once per
//! shard. That saving is only available if *every* shard fits the same
//! geometry, which is what the seal logic exists to guarantee.
//!
//! Both tables use the scheme's single-instance width, 4,096 bytes: one
//! 2,048-coefficient polynomial of 16-bit plaintexts. The
//! directory carries two inline events per entry; the page row is sized for
//! storage rather than for the query count of the longest history, which is
//! what [`PAGE_ROW_BYTES`] explains.
//!
//! # Segments
//!
//! Row *counts* are pinned per segment, not per shard. A shard normally has one
//! segment of each table and nothing below is visible; a shard whose content
//! does not fit one segment gets another of the same geometry, so the parameter
//! set stays shared. A shard's logical row space is its segments concatenated,
//! and a row index is taken over that whole space — which is why placement
//! stays even instead of spilling into a last segment, and why a page extent
//! that crosses a segment is ordinary addressing.

use transparent_events::EVENT_BYTES;

/// Bytes in one directory row.
pub const DIRECTORY_ROW_BYTES: usize = 4_096;

/// Bytes in one packed page row for the baseline geometry.
///
/// One PIR instance carries 4,096 bytes. Short histories can share a row;
/// larger histories use multiple fragments and private page requests.
pub const PAGE_ROW_BYTES: usize = 4_096;

/// Directory rows per segment for the baseline `recent-8k` geometry.
///
/// Named profiles below may use other row counts. At 21 slots per row, this
/// baseline has capacity for 172,032 script entries before placement slack.
/// Deployment decisions and coverage-matched evidence are maintained in
/// `transparent/docs/deployment.md` and `transparent/evidence/README.md`.
pub const DIRECTORY_ROWS: usize = 8_192;

/// Page rows per segment for the baseline `recent-8k` geometry.
///
/// A profile may pair a different page count with its directory count. Sealing
/// considers both script capacity and exact packed page demand. Smaller tables
/// may add shard boundaries, so evaluate total retrieval cost over equal ranges.
pub const PAGE_ROWS: usize = 8_192;

/// Events stored directly in a script's directory entry.
///
/// Two covered 79.18% of scripts active in the study's sample, which is the
/// point: most scripts never need a page query at all. Raising it improves that
/// coverage but widens every row, including the overwhelming majority that hold
/// far fewer events, and the study found four and eight did not pay for
/// themselves.
pub const INLINE_EVENTS: u32 = 2;

/// Events that fit in one fragment.
///
/// Derived from what a row leaves after its own header and one entry header,
/// which is the largest a single history's fragment can be. It is 46 at the
/// v9 layout's 4,096-byte row (41 at the v8 96-byte event, 36 at the earlier
/// 3,584-byte row), and the assertion below pins that: the manifest publishes
/// this number and every fixture is written against it, so a re-derivation
/// that quietly moved it would be a schema change wearing the clothes of a
/// refactor.
pub const EVENTS_PER_PAGE: u32 =
    ((PAGE_ROW_BYTES - PAGE_ROW_HEADER_BYTES - PAGE_ENTRY_HEADER_BYTES) / EVENT_BYTES) as u32;

const _: () = assert!(EVENTS_PER_PAGE == 46);

/// Fragments a script's history occupies, which is also its page-query count.
///
/// This is a *per-history* quantity. It is not the number of rows a shard's
/// page table needs: under packed rows several short histories share one row,
/// so aggregate row demand is [`PackedDemand::rows`] and the two must not be
/// confused. Naming them apart is the point of this rename.
///
/// Monotone in `events`, unlike the aggregate. The first [`INLINE_EVENTS`] live
/// in the directory entry and cost no fragment. A script at or below that
/// threshold therefore occupies none at all — the common case, and the reason
/// inline events exist.
pub const fn fragments_for(events: u32) -> u64 {
    fragments_for_inline(events, INLINE_EVENTS)
}

/// [`fragments_for`] against an inline allowance other than the compiled one.
///
/// For scoring a candidate geometry. The allowance is what decides whether a
/// history touches the page table at all, so a sweep that held it fixed would
/// be sweeping the cheaper half of the question.
pub const fn fragments_for_inline(events: u32, inline: u32) -> u64 {
    if events <= inline {
        return 0;
    }
    let paged = events - inline;
    // Integer ceiling division; `const fn` cannot call `div_ceil`.
    (paged as u64).div_ceil(EVENTS_PER_PAGE as u64)
}

/// Bytes of header at the start of a packed page row, before its first entry.
///
/// Carries the entry count and nothing else. Everything a reader needs to walk
/// the row is either here or in each entry's own header, so a row can be parsed
/// without consulting the directory entry that located it.
pub const PAGE_ROW_HEADER_BYTES: usize = 4;

/// Bytes of header on each entry within a packed page row.
///
/// The v4 layout spent 128 bytes of header per row because a row held one
/// script. Packing pays this per *entry* instead, so it has to be tight: a
/// 14-byte script tag, then 4 each for ordinal, fragment count, event count
/// and the two height bounds. Integer fields are packed without alignment.
pub const PAGE_ENTRY_HEADER_BYTES: usize = 34;

/// Bytes one entry holding `events` events occupies.
pub const fn entry_bytes(events: u32) -> usize {
    PAGE_ENTRY_HEADER_BYTES + events as usize * EVENT_BYTES
}

/// Entries of a `p`-event history that fit in one packed row.
///
/// The reason packing is worth doing, and the reason it stops paying at 24:
/// 33 at `p` = 1, 19 at 2, 13 at 3, and 1 from 24 upward, where an entry is
/// more than half a row. The measured distribution is p50 2 events, p90 5,
/// p95 8, and the newest two stay inline, so the common paged history lands
/// in the range where this is worth several-fold.
pub const fn entries_per_row(p: u32) -> u32 {
    ((PAGE_ROW_BYTES - PAGE_ROW_HEADER_BYTES) / entry_bytes(p)) as u32
}

/// The most entries any row can hold, which is the decoder's hard bound.
///
/// A decoder must reject a claimed count above this *before* it computes any
/// offset from it, which is what keeps the arithmetic unreachable by malformed
/// input rather than merely checked.
pub const MAX_ENTRIES_PER_ROW: usize = entries_per_row(1) as usize;

const _: () = assert!(PAGE_ROW_HEADER_BYTES + entry_bytes(EVENTS_PER_PAGE) <= PAGE_ROW_BYTES);
const _: () = assert!(entries_per_row(1) == 33);
const _: () = assert!(entries_per_row(2) == 19);
const _: () = assert!(entries_per_row(3) == 13);
const _: () = assert!(entries_per_row(23) == 2);
const _: () = assert!(entries_per_row(24) == 1);
const _: () = assert!(entries_per_row(EVENTS_PER_PAGE) == 1);

/// How a script's history occupies the page table.
///
/// Stated over the script's *total* event count, so a transition is expressed
/// as a pair of shapes rather than as a set of special cases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// Within the inline allowance: no entry, no row, no query.
    None,
    /// One entry of `p` paged events, sharing a row with other histories.
    Short(u32),
    /// A dedicated contiguous run of this many rows, one full fragment each
    /// but for the last. Long histories are never packed with anything else.
    Long(u64),
}

/// The shape a history of `events` total events takes.
pub const fn shape_of(events: u32) -> Shape {
    shape_of_inline(events, INLINE_EVENTS)
}

/// [`shape_of`] against an inline allowance other than the compiled one.
pub const fn shape_of_inline(events: u32, inline: u32) -> Shape {
    if events <= inline {
        return Shape::None;
    }
    let paged = events - inline;
    if paged <= EVENTS_PER_PAGE {
        Shape::Short(paged)
    } else {
        Shape::Long((paged as u64).div_ceil(EVENTS_PER_PAGE as u64))
    }
}

/// A shard's aggregate packed page-row demand, maintained incrementally.
///
/// Holds one count per short class plus the rows long histories have reserved,
/// which is everything needed to compute `R = sum over p of
/// ceil(N[p] / entries_per_row(p)) + L`.
///
/// # R is not monotone
///
/// Unlike distinct scripts or event count, `R` can *fall* as events arrive.
/// With `N[1]` = 26 (two rows, since 25 fit in one) and `N[2]` = 14 (one row),
/// `R` is 3; move a single script from class 1 to class 2 and `N[1]` = 25 (one
/// row) and `N[2]` = 15 (one row), so `R` is 2.
///
/// Nothing in the seal logic depends on monotonicity — capacity is checked on a
/// projection of the exact post-absorb state, and a shard closes the moment a
/// target is crossed, so `R` never gets the chance to dip back under a threshold
/// it has passed. But no caller may reason that a shard which does not fit now
/// can never fit later, and no monotone-progress assertion may be made across
/// tail revisions: a republished tail covering more blocks can need fewer rows.
///
/// What replaces monotonicity, where the target-below-capacity gap needs a
/// bound, is that one block raises `R` by at most the number of distinct scripts
/// it touches. Each script's move decrements one class, which never raises that
/// class's ceiling, and increments another, which raises its ceiling by at most
/// one; a 41-to-42 crossing gives up one class-41 row and takes two long rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackedDemand {
    /// Short histories by paged-event count. Index 0 is unused.
    n: [u64; EVENTS_PER_PAGE as usize + 1],
    /// Rows reserved by long histories, which share with nothing.
    long_rows: u64,
    /// Long histories, kept so the class counts can be checked against the
    /// number of scripts above the inline allowance.
    long_scripts: u64,
}

// `[u64; 37]` is past the length `Default` is derived for, so this is written
// out rather than derived.
impl Default for PackedDemand {
    fn default() -> Self {
        Self {
            n: [0; EVENTS_PER_PAGE as usize + 1],
            long_rows: 0,
            long_scripts: 0,
        }
    }
}

impl PackedDemand {
    /// Moves one script from `old` total events to `new`.
    ///
    /// Absolute rather than incremental on purpose: stating both endpoints
    /// covers crossing the inline allowance, moving between short classes, and
    /// growing out of the short classes entirely, without any of them being a
    /// case in the code.
    pub fn shift(&mut self, old: u32, new: u32) {
        self.shift_inline(old, new, INLINE_EVENTS);
    }

    /// [`PackedDemand::shift`] against an inline allowance other than the
    /// compiled one.
    pub fn shift_inline(&mut self, old: u32, new: u32, inline: u32) {
        debug_assert!(
            new >= old,
            "a script's history never shrinks within a shard"
        );
        match shape_of_inline(old, inline) {
            Shape::None => {}
            Shape::Short(p) => self.n[p as usize] -= 1,
            Shape::Long(rows) => {
                self.long_rows -= rows;
                self.long_scripts -= 1;
            }
        }
        match shape_of_inline(new, inline) {
            Shape::None => {}
            Shape::Short(p) => self.n[p as usize] += 1,
            Shape::Long(rows) => {
                self.long_rows += rows;
                self.long_scripts += 1;
            }
        }
    }

    /// Page rows this demand needs.
    ///
    /// Recomputed over the 41 classes rather than maintained as a running
    /// total, because the per-class ceilings do not move monotonically and a
    /// running total is where that would bite.
    pub fn rows(&self) -> u64 {
        (1..=EVENTS_PER_PAGE as usize)
            .map(|p| self.n[p].div_ceil(entries_per_row(p as u32) as u64))
            .sum::<u64>()
            + self.long_rows
    }

    /// Short histories in class `p`, for reporting and tests.
    pub fn class(&self, p: u32) -> u64 {
        self.n[p as usize]
    }

    /// Rows reserved by long histories.
    pub fn long_rows(&self) -> u64 {
        self.long_rows
    }

    /// Histories too long to pack.
    pub fn long_scripts(&self) -> u64 {
        self.long_scripts
    }

    /// Histories that occupy the page table at all.
    pub fn paged_scripts(&self) -> u64 {
        self.n.iter().sum::<u64>() + self.long_scripts
    }
}

/// A table geometry to score a candidate against.
///
/// The constants above are the geometry that is *built*. This is how a census
/// asks what a different one would have cost, without a rebuild and without any
/// builder being able to reach it: [`Default`] is the compiled set, so a run
/// that overrides nothing measures exactly what ships.
///
/// # What the scheme allows
///
/// Neither dimension is free. The native profile requires a row count that is a
/// multiple of `POLY_LEN` = 2,048 and refuses fewer, and it quantises the row
/// width into instances of 2,048 x 16 bits = 4,096 bytes. So the smallest legal
/// table is 2,048 rows of 4,096 bytes — a capacity of 32,768 scripts is the
/// scheme's floor rather than a number anyone chose. [`Geometry::validate`] is where that is enforced, so a
/// sweep cannot quietly score a shape the scheme would never serve.
///
/// The two dimensions do not cost the same. Row count is charged on the query
/// upload alone, and mildly: 2,048 rows cost 40,200 bytes a query and 4,096
/// cost 52,744, +31.2%. Row width is charged on the response *and* the
/// published setup, and in whole instances, so the next legal width doubles
/// both. Widening to buy directory slots is therefore the expensive way to buy
/// them and doubling the row count is the cheap one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Geometry {
    /// The registry name this shape is published under.
    ///
    /// A shard names its geometry rather than restating its dimensions, so a
    /// consumer selects one validated parameter set by name instead of
    /// inferring it from numbers it would have to trust. The dimensions are
    /// still published per segment and still checked against the named entry;
    /// the name is what decides, and an unknown one is refused.
    pub name: &'static str,
    pub directory_rows: u64,
    pub directory_row_bytes: usize,
    pub page_rows: u64,
    pub page_row_bytes: usize,
    /// Events carried in a directory entry, which decides both how wide an
    /// entry is and whether a history touches the page table at all.
    pub inline_events: u32,
}

/// The default recent geometry, and what an unconfigured build publishes.
pub const RECENT_8K: Geometry = Geometry {
    name: "recent-8k",
    directory_rows: 8_192,
    directory_row_bytes: INSTANCE_BYTES,
    page_rows: 8_192,
    page_row_bytes: INSTANCE_BYTES,
    inline_events: 2,
};

/// A narrower recent candidate: 50,176 fewer upload bytes per matched shard.
///
/// Not a default. Halving the scanned database does not halve latency, and more
/// boundaries add filter downloads, setup and private queries that can erase
/// the saving. Promote it only on measured total client bytes.
pub const RECENT_4K: Geometry = Geometry {
    name: "recent-4k",
    directory_rows: 4_096,
    page_rows: 4_096,
    ..RECENT_8K
};

/// A recent candidate with half the directory rows and all of the page rows.
///
/// Not a default. It lowers each directory query's selection upload without
/// shrinking page capacity, which is what closed every sealed recent shard in
/// the September census, so it should not add boundaries the way `recent-4k`
/// does. Its 57,344 directory slots held every recent shard of that census
/// under the earlier 14-slot rows only on dense arithmetic; two-choice placement at this load must be replayed
/// before it is promoted. `recent-4k` keeps its own meaning.
pub const RECENT_4K_8K: Geometry = Geometry {
    name: "recent-4k-8k",
    directory_rows: 4_096,
    page_rows: 8_192,
    ..RECENT_8K
};

/// The square archive candidate, and the fallback if the wider one does not
/// validate.
pub const ARCHIVE_32K: Geometry = Geometry {
    name: "archive-32k",
    directory_rows: 32_768,
    page_rows: 32_768,
    ..RECENT_8K
};

/// The preferred archive candidate: a narrower directory than its pages.
///
/// The observed maximum is 284,221 scripts in a shard against the 524,288 a
/// 32,768-row directory holds, so the directory has room to stay narrow while
/// the page table absorbs dense old history. That keeps directory-only
/// restoration at 228,360 upload bytes rather than 429,064.
pub const ARCHIVE_WIDE: Geometry = Geometry {
    name: "archive-wide",
    directory_rows: 32_768,
    page_rows: 65_536,
    ..RECENT_8K
};

/// Every geometry a published shard may name.
///
/// A closed set rather than server-chosen parameters. Each entry is a shape the
/// scheme serves, whose parameters a client validates once and reuses for every
/// shard of that geometry; an arbitrary shape would cost a parameter set per
/// shard and give a wallet nothing to check the server's choice against.
pub const PROFILES: &[Geometry] = &[
    RECENT_8K,
    RECENT_4K,
    RECENT_4K_8K,
    ARCHIVE_32K,
    ARCHIVE_WIDE,
];

/// The registry entry named `name`, or `None` if there is no such geometry.
///
/// Callers must treat `None` as a hard error. A shard whose geometry this build
/// does not know cannot be decoded, and skipping it would advance coverage over
/// history that was never retrieved.
pub fn by_name(name: &str) -> Option<&'static Geometry> {
    PROFILES.iter().find(|geometry| geometry.name == name)
}

impl Default for Geometry {
    fn default() -> Self {
        RECENT_8K
    }
}

impl Geometry {
    /// Bytes one directory entry occupies at this inline allowance.
    pub const fn directory_entry_bytes(&self) -> usize {
        crate::records::DIRECTORY_ENTRY_HEADER_BYTES + self.inline_events as usize * EVENT_BYTES
    }

    /// Entries one directory row holds.
    ///
    /// Derived, never given. A slot count set independently of the entry width
    /// would describe no table that could be built.
    pub const fn directory_slots(&self) -> u64 {
        ((self.directory_row_bytes - crate::records::DIRECTORY_ROW_HEADER_BYTES)
            / self.directory_entry_bytes()) as u64
    }

    /// Scripts one directory segment holds.
    pub const fn directory_capacity(&self) -> u64 {
        self.directory_rows * self.directory_slots()
    }

    /// Bytes of a row a directory's slots cannot reach.
    ///
    /// A row holds whole entries, so whatever is left under one is dead. At the
    /// compiled geometry that is 60 bytes of every 4,096, 132 short of a
    /// twenty-second slot.
    pub const fn directory_row_slack(&self) -> usize {
        self.directory_row_bytes
            - crate::records::DIRECTORY_ROW_HEADER_BYTES
            - self.directory_slots() as usize * self.directory_entry_bytes()
    }

    pub const fn directory_bytes_per_segment(&self) -> u64 {
        self.directory_rows * self.directory_row_bytes as u64
    }

    pub const fn page_bytes_per_segment(&self) -> u64 {
        self.page_rows * self.page_row_bytes as u64
    }

    pub const fn fragments_for(&self, events: u32) -> u64 {
        fragments_for_inline(events, self.inline_events)
    }

    pub const fn shape_of(&self, events: u32) -> Shape {
        shape_of_inline(events, self.inline_events)
    }

    /// Rejects a shape the PIR scheme would not serve.
    ///
    /// Checked rather than assumed because the whole point of a scoring
    /// geometry is that it is reachable from a command line, and a candidate
    /// the scheme rounds up is a candidate whose reported cost is not its cost.
    pub fn validate(&self) -> Result<(), String> {
        if self.directory_rows < POLY_LEN || !self.directory_rows.is_multiple_of(POLY_LEN) {
            return Err(format!(
                "{} directory rows: the scheme pads to a multiple of {POLY_LEN} and refuses fewer",
                self.directory_rows
            ));
        }
        if self.page_rows < POLY_LEN || !self.page_rows.is_multiple_of(POLY_LEN) {
            return Err(format!(
                "{} page rows: the scheme pads to a multiple of {POLY_LEN} and refuses fewer",
                self.page_rows
            ));
        }
        if self.directory_row_bytes < INSTANCE_BYTES
            || !self.directory_row_bytes.is_multiple_of(INSTANCE_BYTES)
        {
            return Err(format!(
                "{} directory row bytes: the scheme charges in whole instances of {INSTANCE_BYTES}",
                self.directory_row_bytes
            ));
        }
        if self.page_row_bytes < INSTANCE_BYTES
            || !self.page_row_bytes.is_multiple_of(INSTANCE_BYTES)
        {
            return Err(format!(
                "{} page row bytes: the scheme charges in whole instances of {INSTANCE_BYTES}",
                self.page_row_bytes
            ));
        }
        if self.directory_slots() == 0 {
            return Err(format!(
                "a {}-byte entry does not fit a {}-byte row",
                self.directory_entry_bytes(),
                self.directory_row_bytes
            ));
        }
        Ok(())
    }

    /// Rejects a shape this build could not *encode*, as distinct from one the
    /// scheme could not serve.
    ///
    /// [`Geometry::validate`] answers whether the PIR scheme would serve a
    /// shape, which is the question a census asks of a candidate it will only
    /// score. Publishing asks a stricter one, because the record codec in
    /// [`crate::records`] and the fragment arithmetic above are compiled
    /// against one row width: `DIRECTORY_SLOTS`, `EVENTS_PER_PAGE` and
    /// `entries_per_row` are all derived from it, and every fixture is written
    /// against the numbers that come out.
    ///
    /// So the registry varies row *counts* only. Widening a row is a codec
    /// change — it moves `EVENTS_PER_PAGE` off 46 and `DIRECTORY_SLOTS` off 21,
    /// which is a schema bump and a re-publication, not a new registry entry.
    /// Lifting this means making those quantities functions of the geometry,
    /// carrying them in `ManifestLayout`, and fixing `geometry_costs.rs`, which
    /// prices the directory at the *page* width and is correct today only
    /// because the two agree.
    pub fn validate_publishable(&self) -> Result<(), String> {
        self.validate()?;
        if self.directory_row_bytes != DIRECTORY_ROW_BYTES {
            return Err(format!(
                "{} directory row bytes: this build encodes {DIRECTORY_ROW_BYTES}-byte rows",
                self.directory_row_bytes
            ));
        }
        if self.page_row_bytes != PAGE_ROW_BYTES {
            return Err(format!(
                "{} page row bytes: this build encodes {PAGE_ROW_BYTES}-byte rows",
                self.page_row_bytes
            ));
        }
        if self.inline_events != INLINE_EVENTS {
            return Err(format!(
                "{} inline events: this build encodes {INLINE_EVENTS}",
                self.inline_events
            ));
        }
        Ok(())
    }
}

/// The scheme's RLWE degree, which is also its minimum and quantum of rows.
const POLY_LEN: u64 = 2_048;

/// Bytes of one scheme instance, which is the quantum of row width.
const INSTANCE_BYTES: usize = 4_096;

const _: () = assert!(DIRECTORY_ROW_BYTES.is_multiple_of(INSTANCE_BYTES));
const _: () = assert!(PAGE_ROW_BYTES.is_multiple_of(INSTANCE_BYTES));

/// Segments needed to hold `rows` rows, at `per_segment` rows each.
///
/// Never zero: a shard with no events still publishes an empty segment, because
/// a wallet must be able to establish absence by querying rather than by being
/// told there is nothing to query.
pub const fn segments_for(rows: u64, per_segment: u64) -> u32 {
    let needed = rows.div_ceil(per_segment);
    if needed == 0 {
        1
    } else {
        needed as u32
    }
}

/// Splits a row index over a shard's logical row space into its segment and the
/// row within that segment.
///
/// The row within the segment is what a query names. It is the same index in
/// every segment, because every segment is asked: naming the segment would
/// disclose which one holds the script, and the script is what chose it.
pub const fn split_row(row: u64, per_segment: u64) -> (u32, u64) {
    ((row / per_segment) as u32, row % per_segment)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every registry entry must be servable by the scheme *and* encodable by
    /// this build. A shape that failed either would be published as a name no
    /// consumer could act on.
    #[test]
    fn every_registry_entry_is_publishable() {
        for geometry in PROFILES {
            geometry
                .validate_publishable()
                .unwrap_or_else(|error| panic!("{}: {error}", geometry.name));
        }
    }

    /// Names are the addressing, so two entries sharing one would make the
    /// registry ambiguous and `by_name` silently pick the first.
    #[test]
    fn registry_names_are_distinct_and_resolvable() {
        for geometry in PROFILES {
            assert_eq!(by_name(geometry.name), Some(geometry), "{}", geometry.name);
        }
        let names: std::collections::BTreeSet<&str> =
            PROFILES.iter().map(|geometry| geometry.name).collect();
        assert_eq!(names.len(), PROFILES.len(), "duplicate registry name");
        assert_eq!(by_name("recent-16k"), None);
        assert_eq!(by_name(""), None);
    }

    /// The registry varies row counts and nothing else. If this ever fails, the
    /// record codec and `ManifestLayout` are part of the change — see
    /// [`Geometry::validate_publishable`].
    #[test]
    fn the_registry_varies_row_counts_only() {
        for geometry in PROFILES {
            assert_eq!(geometry.directory_row_bytes, 4_096, "{}", geometry.name);
            assert_eq!(geometry.page_row_bytes, 4_096, "{}", geometry.name);
            assert_eq!(geometry.inline_events, 2, "{}", geometry.name);
            assert_eq!(geometry.directory_slots(), 21, "{}", geometry.name);
        }
    }

    /// The archive candidates are the ones the deployment plan turns on, so
    /// their capacities are pinned rather than left to be recomputed by hand.
    /// `archive-wide` exists because 284,221 observed scripts per shard fit a
    /// 32,768-row directory's 688,128 slots with room, so the directory can
    /// stay narrow while the page table absorbs dense history.
    #[test]
    fn the_archive_candidates_hold_what_they_claim() {
        assert_eq!(ARCHIVE_32K.directory_capacity(), 688_128);
        assert_eq!(ARCHIVE_WIDE.directory_capacity(), 688_128);
        assert_eq!(ARCHIVE_WIDE.page_rows, 2 * ARCHIVE_WIDE.directory_rows);
        assert_eq!(RECENT_4K.directory_capacity(), 86_016);
        assert_eq!(RECENT_4K_8K.directory_capacity(), 86_016);
        assert_eq!(RECENT_4K_8K.page_rows, RECENT_8K.page_rows);
        // A narrower directory buys a smaller query; a wider page table buys
        // rows for old history. The two move independently, which is the point
        // of having a pair rather than one number.
        assert_eq!(ARCHIVE_WIDE.directory_bytes_per_segment(), 134_217_728);
        assert_eq!(ARCHIVE_WIDE.page_bytes_per_segment(), 268_435_456);
    }

    /// A width the scheme *would* serve is still refused for publication,
    /// because the record codec is compiled against one. Keeping the two
    /// questions apart is what lets a census score a shape it must not build.
    #[test]
    fn a_servable_width_is_not_automatically_a_publishable_one() {
        let wide = Geometry {
            directory_row_bytes: 8_192,
            ..Default::default()
        };
        wide.validate().expect("two instances is a legal width");
        assert!(wide.validate_publishable().is_err());
    }

    /// The compiled geometry is what `Default` reports. Everything a sweep says
    /// about a candidate is relative to this, so a drift here would silently
    /// rebase every comparison.
    #[test]
    fn the_default_geometry_is_the_one_that_ships() {
        let g = Geometry::default();
        assert_eq!(g.directory_entry_bytes(), 192);
        assert_eq!(g.directory_slots(), 21);
        assert_eq!(g.directory_capacity(), 172_032);
        assert_eq!(g.directory_row_slack(), 60);
        // Both tables are 8,192 rows: they have to close a shard at about the
        // same occupancy, or the one that does not is padding.
        assert_eq!(g.directory_bytes_per_segment(), 33_554_432);
        assert_eq!(g.page_bytes_per_segment(), 33_554_432);
        g.validate().expect("what ships must be servable");
    }

    /// Directory capacity is bought by the inline allowance, in both
    /// directions. Dropping to one event gives 62% more scripts per segment and
    /// pushes every one-paged-event history into a page query; raising it to
    /// three costs 31% of them.
    #[test]
    fn the_inline_allowance_is_what_buys_directory_slots() {
        for (inline, slots, capacity) in [
            (0u32, 227u64, 1_859_584u64),
            (1, 38, 311_296),
            (2, 21, 172_032),
            (3, 14, 114_688),
        ] {
            let g = Geometry {
                inline_events: inline,
                ..Default::default()
            };
            assert_eq!(g.directory_slots(), slots, "inline {inline}");
            assert_eq!(g.directory_capacity(), capacity, "inline {inline}");
        }
    }

    /// Doubling the row count is the other way to the same capacity, and the
    /// one that leaves the inline allowance alone.
    #[test]
    fn doubling_the_rows_doubles_the_capacity() {
        let g = Geometry {
            directory_rows: 4_096,
            ..Default::default()
        };
        assert_eq!(g.directory_capacity(), 86_016);
        assert_eq!(g.directory_bytes_per_segment(), 16_777_216);
        g.validate()
            .expect("a multiple of the poly length is servable");
    }

    /// A shape the scheme would round up is a shape whose reported cost is not
    /// its cost, so scoring it is worse than refusing it.
    #[test]
    fn a_geometry_the_scheme_would_not_serve_is_refused() {
        for bad in [
            Geometry {
                directory_rows: 1_024,
                ..Default::default()
            },
            Geometry {
                directory_rows: 3_000,
                ..Default::default()
            },
            Geometry {
                page_rows: 5_000,
                ..Default::default()
            },
            Geometry {
                directory_row_bytes: 3_584,
                ..Default::default()
            },
            // 47 inline events is 4,107 bytes of entry: wider than the row.
            Geometry {
                inline_events: 47,
                ..Default::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?} should be refused");
        }
        // The next legal width up is servable, and it buys 42 slots for a
        // doubled response and setup.
        let wide = Geometry {
            directory_row_bytes: 8_192,
            ..Default::default()
        };
        wide.validate().expect("two instances is a legal width");
        assert_eq!(wide.directory_slots(), 42);
    }

    /// The inline allowance decides whether a history is paged at all, so the
    /// scoring functions must move with it and the compiled wrappers must not.
    #[test]
    fn scoring_functions_follow_the_inline_allowance() {
        assert_eq!(fragments_for(3), 1);
        assert_eq!(fragments_for_inline(3, 3), 0);
        assert_eq!(shape_of_inline(3, 3), Shape::None);
        assert_eq!(shape_of_inline(3, 1), Shape::Short(2));

        let mut wide = PackedDemand::default();
        let mut narrow = PackedDemand::default();
        wide.shift_inline(0, 3, 3);
        narrow.shift_inline(0, 3, 1);
        assert_eq!(
            wide.rows(),
            0,
            "a history inside the allowance costs no row"
        );
        assert_eq!(narrow.rows(), 1);
    }

    #[test]
    fn a_shard_always_has_at_least_one_segment_of_each_table() {
        assert_eq!(segments_for(0, PAGE_ROWS as u64), 1);
        assert_eq!(segments_for(1, PAGE_ROWS as u64), 1);
        assert_eq!(segments_for(PAGE_ROWS as u64, PAGE_ROWS as u64), 1);
        assert_eq!(segments_for(PAGE_ROWS as u64 + 1, PAGE_ROWS as u64), 2);
    }

    /// A page extent that runs past a segment boundary is ordinary addressing,
    /// not a special case: the shard's page space is its segments concatenated.
    #[test]
    fn a_row_splits_into_its_segment_and_its_row_within_it() {
        let per = PAGE_ROWS as u64;
        assert_eq!(split_row(0, per), (0, 0));
        assert_eq!(split_row(per - 1, per), (0, per - 1));
        assert_eq!(split_row(per, per), (1, 0));
        assert_eq!(split_row(2 * per + 7, per), (2, 7));
    }

    #[test]
    fn a_fragment_holds_the_events_its_width_allows() {
        assert_eq!(EVENTS_PER_PAGE, 46);
        assert!(
            PAGE_ROW_HEADER_BYTES
                + PAGE_ENTRY_HEADER_BYTES
                + EVENTS_PER_PAGE as usize * EVENT_BYTES
                <= PAGE_ROW_BYTES,
            "a full fragment must fit its row"
        );
    }

    /// The inline events are free of page cost. Most scripts in a shard never
    /// exceed them, so getting this boundary wrong would misprice the dominant
    /// case and, through the seal logic, the whole shard geometry.
    #[test]
    fn histories_within_the_inline_allowance_occupy_no_page() {
        assert_eq!(fragments_for(0), 0);
        assert_eq!(fragments_for(1), 0);
        assert_eq!(fragments_for(INLINE_EVENTS), 0);
        assert_eq!(fragments_for(INLINE_EVENTS + 1), 1);
    }

    #[test]
    fn pages_are_added_only_when_the_previous_one_is_full() {
        let full = INLINE_EVENTS + EVENTS_PER_PAGE;
        assert_eq!(fragments_for(full), 1);
        assert_eq!(fragments_for(full + 1), 2);
        assert_eq!(fragments_for(INLINE_EVENTS + 2 * EVENTS_PER_PAGE), 2);
    }

    /// Fragment cost is driven by the number of *scripts* with history, not by
    /// the event count: many short histories need far more fragments than the
    /// same events concentrated in a few long ones. This is why a shard cannot
    /// be sealed on its event count alone.
    ///
    /// This is the waste packing exists to recover, and it is a statement about
    /// *fragments*, not rows — see the row comparison below, where sharing
    /// closes most of this gap.
    #[test]
    fn many_short_histories_need_more_fragments_than_one_long_one() {
        let events = 3_000u32;
        let concentrated = fragments_for(events);
        let spread: u64 = (0..events / 3).map(|_| fragments_for(3)).sum();
        assert!(
            spread > concentrated * 10,
            "spread {spread} should dwarf concentrated {concentrated}"
        );
    }

    fn demand(histories: &[u32]) -> PackedDemand {
        let mut demand = PackedDemand::default();
        for events in histories {
            demand.shift(0, *events);
        }
        demand
    }

    /// The table the whole optimisation rests on. A one-event history costs a
    /// row to itself under v4 and a thirty-third of one here; from 24 paged
    /// events an entry is more than half a row and packing stops paying.
    #[test]
    fn entries_per_row_is_what_packing_buys() {
        for (p, expected) in [
            (1, 33),
            (2, 19),
            (3, 13),
            (4, 10),
            (5, 8),
            (6, 7),
            (7, 6),
            (8, 5),
            (13, 3),
            (14, 3),
            (20, 2),
            (23, 2),
            (24, 1),
            (EVENTS_PER_PAGE, 1),
        ] {
            assert_eq!(entries_per_row(p), expected, "entries_per_row({p})");
        }
    }

    /// A full fragment plus both headers must fit the row it is written into,
    /// with the tail left over to be zeroed.
    #[test]
    fn a_full_fragment_fits_its_row() {
        let used = PAGE_ROW_HEADER_BYTES + entry_bytes(EVENTS_PER_PAGE);
        assert_eq!(used, 4_040);
        assert!(used <= PAGE_ROW_BYTES);
    }

    /// The boundary between sharing a row and reserving a run of them. It is
    /// the one transition that changes a history's kind rather than its class.
    #[test]
    fn a_history_becomes_long_exactly_when_it_outgrows_a_fragment() {
        assert_eq!(shape_of(0), Shape::None);
        assert_eq!(shape_of(INLINE_EVENTS), Shape::None);
        assert_eq!(shape_of(INLINE_EVENTS + 1), Shape::Short(1));
        let full = INLINE_EVENTS + EVENTS_PER_PAGE;
        assert_eq!(shape_of(full), Shape::Short(EVENTS_PER_PAGE));
        assert_eq!(shape_of(full + 1), Shape::Long(2));
        assert_eq!(
            shape_of(INLINE_EVENTS + 3 * EVENTS_PER_PAGE),
            Shape::Long(3)
        );
    }

    /// `shift` is stated over absolute endpoints so that every transition —
    /// crossing the inline allowance, moving between classes, growing out of
    /// them — is the same two lines of code. This checks each one lands where
    /// the shape says it should.
    #[test]
    fn a_history_moves_between_classes_as_it_grows() {
        let mut d = PackedDemand::default();

        // Below the inline allowance nothing is occupied at all.
        d.shift(0, 2);
        assert_eq!(d, PackedDemand::default());

        // Crossing it opens class 1.
        d.shift(2, 3);
        assert_eq!(d.class(1), 1);
        assert_eq!(d.rows(), 1);

        // Class to class: one leaves, one arrives.
        d.shift(3, 7);
        assert_eq!(d.class(1), 0);
        assert_eq!(d.class(5), 1);

        // The last short class, then out of the short classes entirely.
        d.shift(7, INLINE_EVENTS + EVENTS_PER_PAGE);
        assert_eq!(d.class(EVENTS_PER_PAGE), 1);
        assert_eq!(d.long_scripts(), 0);
        d.shift(
            INLINE_EVENTS + EVENTS_PER_PAGE,
            INLINE_EVENTS + EVENTS_PER_PAGE + 1,
        );
        assert_eq!(d.class(EVENTS_PER_PAGE), 0);
        assert_eq!(d.long_scripts(), 1);
        assert_eq!(d.long_rows(), 2);

        // Long histories only ever reserve more rows.
        d.shift(
            INLINE_EVENTS + EVENTS_PER_PAGE + 1,
            INLINE_EVENTS + 5 * EVENTS_PER_PAGE,
        );
        assert_eq!(d.long_rows(), 5);
        assert_eq!(d.paged_scripts(), 1);

        // A script can arrive already long, in one block.
        d.shift(0, INLINE_EVENTS + 500);
        assert_eq!(d.long_scripts(), 2);
    }

    /// Packing can never cost more rows than giving every history its own, and
    /// it cannot beat the bytes it has to store. Both bounds hold at once for
    /// any mixture, which is the cheap sandwich that catches a wrong
    /// `entries_per_row` without recomputing it.
    #[test]
    fn packed_rows_sit_between_the_byte_floor_and_the_unpacked_cost() {
        let histories: Vec<u32> = (0..400u32).map(|i| 3 + (i * 7) % 90).collect();
        let d = demand(&histories);

        let unpacked: u64 = histories.iter().map(|e| fragments_for(*e)).sum();
        assert!(
            d.rows() <= unpacked,
            "packed {} should not exceed unpacked {unpacked}",
            d.rows()
        );

        let bytes: usize = histories
            .iter()
            .map(|e| {
                let paged = (e - INLINE_EVENTS) as usize;
                let full = paged / EVENTS_PER_PAGE as usize;
                let rest = paged % EVENTS_PER_PAGE as usize;
                full * entry_bytes(EVENTS_PER_PAGE)
                    + if rest > 0 {
                        entry_bytes(rest as u32)
                    } else {
                        0
                    }
            })
            .sum();
        let floor = (bytes as u64).div_ceil((PAGE_ROW_BYTES - PAGE_ROW_HEADER_BYTES) as u64);
        assert!(
            d.rows() >= floor,
            "packed {} is below the byte floor {floor}",
            d.rows()
        );
    }

    /// Every paged script is in exactly one class or is long. A shift that lost
    /// or duplicated one would show up here and nowhere else.
    #[test]
    fn every_paged_script_is_counted_exactly_once() {
        let histories: Vec<u32> = (0..300u32).map(|i| i % 120).collect();
        let d = demand(&histories);
        let expected = histories.iter().filter(|e| **e > INLINE_EVENTS).count() as u64;
        assert_eq!(d.paged_scripts(), expected);
    }

    /// Row demand is **not** monotone as events arrive, unlike distinct scripts
    /// or event count. This is the counterexample, recorded so the property is
    /// known rather than accidental: emptying a class's second, nearly-empty row
    /// can cost less than the class the script moves into gains.
    ///
    /// Nothing in [`crate::seal`] depends on monotonicity — capacity is checked
    /// against the exact post-absorb state and a shard closes the moment a
    /// target is crossed — but no caller may reason that content which does not
    /// fit now can never fit later.
    #[test]
    fn row_demand_can_fall_as_events_arrive() {
        let mut d = PackedDemand::default();
        // 34 one-event histories need two rows, since 33 share one.
        for _ in 0..34 {
            d.shift(0, INLINE_EVENTS + 1);
        }
        // 18 two-event histories need one, since 19 share one.
        for _ in 0..18 {
            d.shift(0, INLINE_EVENTS + 2);
        }
        let before = d.rows();
        assert_eq!(before, 3);

        // One more event for one script, and the total *falls*.
        d.shift(INLINE_EVENTS + 1, INLINE_EVENTS + 2);
        assert_eq!(d.class(1), 33);
        assert_eq!(d.class(2), 19);
        assert_eq!(d.rows(), 2, "row demand should fall from {before}");
    }
}
