//! Measures what shard boundaries a journal actually produces.
//!
//! The seal thresholds are schema: once a shard set is published under them,
//! changing them re-partitions the chain. So they should be chosen from the
//! journal rather than from an estimate, which is what this reports.
//!
//! Two things are printed. First the journal's own shape — how density varies
//! across the chain — because that is what decides whether content sealing is
//! worth its complexity at all. Then, for each candidate policy, the shard
//! boundaries it yields and how full those shards get.
//!
//! Read-only by default: it opens the journal, streams it, and writes nothing.
//! The one exception is `--shard-matches`, which spills sorted runs to a
//! directory the caller names and removes them when it has merged them.

use clap::Parser;
use std::path::PathBuf;
use transparent_filter_server::events::EventStore;
use transparent_filter_server::shard_matches::MatchCounter;
use transparent_shard::layout::{
    by_name as geometry_by_name, entries_per_row, Geometry, EVENTS_PER_PAGE, PAGE_ROW_BYTES,
    PAGE_ROW_HEADER_BYTES,
};
use transparent_shard::seal::{Limit, PageBasis, SealPolicy, SealReason, SealedShard, Sealer};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "shard-census",
    about = "Report the shard boundaries a transparent event journal produces"
)]
struct Cli {
    #[arg(long, default_value = "./transparent-event-data")]
    data_dir: PathBuf,
    /// Candidate policies, as `scripts_target:scripts_cap,pages_target:pages_cap`.
    ///
    /// Repeatable. With none given, a default sweep around the geometry the
    /// mainnet study measured is used.
    #[arg(long = "policy")]
    policies: Vec<String>,
    /// Blocks per bucket in the density profile.
    #[arg(long, default_value_t = 4_096)]
    density_bucket: u64,
    /// Close shards on a row per fragment, as the layout did before short
    /// histories shared rows.
    ///
    /// The default is what the builder emits. This reports what the same
    /// journal would have cost unpacked, so the two can be compared over one
    /// input; a set must not be built this way, because the table would then be
    /// sized for more than the builder writes.
    #[arg(long)]
    unpacked: bool,
    /// Page rows per segment to score against, when trying a table size other
    /// than the one compiled in.
    ///
    /// Reporting only: it changes the segment and pinned-byte arithmetic, not
    /// what is built. Pair it with a policy whose page-row capacity is at most
    /// this, so ordinary accumulation seals before it would need a second
    /// segment — segments are for the indivisible block that cannot fit, not
    /// for routine capacity, because a wallet must query every one of them.
    #[arg(long)]
    page_rows_per_segment: Option<u64>,
    /// Directory rows per segment to score against.
    ///
    /// Reporting only, on the same terms as `--page-rows-per-segment`. The
    /// scheme pads a row count up to a multiple of 2,048 and refuses fewer, so
    /// the compiled 2,048 is its floor and this can only go up. What it buys is
    /// scripts per shard, and therefore fewer shards for a wallet to open;
    /// what it costs is the row-count term of every directory query upload,
    /// which at 2,048 rows is 10,240 bytes of 96,264.
    #[arg(long)]
    directory_rows_per_segment: Option<u64>,
    /// Directory row bytes to score against.
    ///
    /// Quantised: the scheme charges in whole instances of 3,584 bytes, so the
    /// only step up is 7,168, and it doubles both the query response and the
    /// published setup. Slots per row are derived from this and the inline
    /// allowance, never set directly.
    #[arg(long)]
    directory_row_bytes: Option<usize>,
    /// Events carried inline in a directory entry, to score against.
    ///
    /// Unlike the two above this changes *boundaries*, not just arithmetic: the
    /// allowance decides whether a history reaches the page table, so it moves
    /// page demand and the shard limit that binds. It is also what buys
    /// directory slots — one inline event gives 23 per row against 14, two
    /// gives 14, three gives 10.
    #[arg(long)]
    inline_events: Option<u32>,
    /// Run the real two-choice placer over each shard's scripts.
    ///
    /// Off by default because it costs a sort and a placement pass per shard,
    /// and because leaving it off is what keeps a default run comparable with
    /// the archived censuses. On, it replaces the one modelled figure in this
    /// report — `scripts / slots` — with what the builder would actually do,
    /// and reports how much headroom the fullest row has left.
    #[arg(long)]
    placement: bool,
    /// Count how many shards each exact script appears in, spilling sorted runs
    /// under this directory.
    ///
    /// The one thing this tool writes, which is why it takes a path rather than
    /// choosing one. A script active in `g` shards costs `2 * g` directory
    /// queries to restore, so this is what turns a boundary policy into a
    /// wallet's bill — and it cannot be derived from the occupancy counts,
    /// which say what a shard holds rather than how many shards hold a script.
    /// Disk is proportional to script-shard occurrences; the runs are removed
    /// when the merge completes.
    #[arg(long, value_name = "DIR")]
    shard_matches: Option<PathBuf>,

    /// Emit one TSV row per sealed shard, for the first policy only.
    ///
    /// The summary below says how many shards each limit closed, but not where
    /// they are. Whether pages or scripts bind is not uniform along the chain —
    /// the early chain is dense and heavily reused, so a shard fills with page
    /// rows long before it runs out of directory slots, and that ratio moves as
    /// reuse falls. A geometry chosen on the aggregate is chosen on a mixture.
    ///
    /// Columns: shard_id, start_height, end_height, blocks, scripts, page_rows,
    /// fragments, events, reason. Written to stdout, so the run's log carries
    /// it and no path has to be agreed.
    #[arg(long, default_value_t = false)]
    per_shard: bool,
    /// Registry geometry to score against, before any override below.
    ///
    /// The baseline every reported figure is relative to. The individual
    /// overrides still apply on top, so a sweep can hold a registry shape and
    /// vary one dimension of it.
    #[arg(long, default_value = "recent-8k")]
    geometry: String,
    /// First height to census, inclusive. Defaults to the journal's first.
    ///
    /// Bounded runs are how a two-tier candidate is scored: the archive tier
    /// over the early chain and the recent tier over the last six months are
    /// different questions, and censusing the whole journal at one geometry
    /// answers neither.
    #[arg(long)]
    start_height: Option<u64>,
    /// Last height to census, inclusive. Defaults to the journal's last.
    ///
    /// Inclusive at both ends, matching the journal's own `covered_through`.
    /// An off-by-one here is a boundary that moves, and a boundary that moves
    /// re-shards the chain.
    #[arg(long)]
    end_height: Option<u64>,
}

/// Bytes one private row query uploads at this table shape.
///
/// The same formula the server bounds a request with, so a projection here and
/// a measurement there cannot drift: packing keys, which the row count does not
/// move, plus a body that it does. Response and published setup follow the row
/// *width* instead and so are equal across every candidate compared here, which
/// is why only the query is priced.
fn query_bytes(rows: u64, row_bytes: usize) -> u64 {
    let (rlwe, params) = ipir_sp::params_for_simplepir(rows, (row_bytes as u64) * 8)
        .expect("a validated geometry has parameters");
    (8 + ipir_sp::serialize::serialized_packing_keys_len(&rlwe)
        + (params.db_rows * params.query_bits).div_ceil(8)) as u64
}

fn parse_limit(text: &str) -> Result<Limit, BoxError> {
    let (target, capacity) = text
        .split_once(':')
        .ok_or_else(|| format!("limit {text:?} is not target:capacity"))?;
    Ok(Limit::new(target.parse()?, capacity.parse()?)?)
}

fn parse_policy(text: &str) -> Result<SealPolicy, BoxError> {
    let parts: Vec<&str> = text.split(',').collect();
    if parts.len() != 2 {
        return Err(format!("policy {text:?} needs two comma-separated limits").into());
    }
    Ok(SealPolicy {
        scripts: parse_limit(parts[0])?,
        page_rows: parse_limit(parts[1])?,
    })
}

/// Candidate policies around the study's measured geometry.
///
/// A directory segment holds 14 slots across 2,048 rows — 28,672 scripts — so
/// every target below sits inside one segment with slack, and the sweep varies
/// the script limit alone.
///
/// Page rows are held at the publisher's default so the sweep measures the
/// geometry actually in use. Setting them lower makes page rows bind first and
/// the script limit inert, which reads as a script-limit result and is not one.
///
/// The page capacity tracks [`transparent_shard::PAGE_ROWS`] rather than a
/// literal, so a table resized without revisiting this would not quietly sweep
/// policies that overrun it — the check in `main` refuses those, and a default
/// sweep that tripped its own check would be a poor way to find out.
fn default_policies() -> Vec<(String, SealPolicy)> {
    let capacity = transparent_shard::PAGE_ROWS as u64;
    // A little under capacity, so an ordinary block seals before it overruns.
    let target = capacity - capacity / 32;
    [
        ("scripts 4k", 4_096u64),
        ("scripts 8k", 8_192),
        ("scripts 12k", 12_288),
        ("scripts 16k", 16_384),
        ("scripts 24k", 24_576),
    ]
    .into_iter()
    .map(|(name, scripts)| {
        (
            name.to_string(),
            SealPolicy {
                scripts: Limit::new(scripts, scripts * 2).expect("valid"),
                page_rows: Limit::new(target, capacity).expect("valid"),
            },
        )
    })
    .collect()
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[index.saturating_sub(1).min(sorted.len() - 1)]
}

fn describe(label: &str, values: &mut [u64]) {
    values.sort_unstable();
    let sum: u64 = values.iter().sum();
    println!(
        "  {label:<14} min {:>8}  p50 {:>8}  p95 {:>8}  max {:>8}  mean {:>8}",
        values.first().copied().unwrap_or(0),
        percentile(values, 50.0),
        percentile(values, 95.0),
        values.last().copied().unwrap_or(0),
        if values.is_empty() {
            0
        } else {
            sum / values.len() as u64
        }
    );
}

/// How much of what a shard stores is real data.
///
/// Nothing else in the pipeline reports this. Every other check asserts the
/// opposite property — that a table is the same size whatever it holds, which
/// is what keeps a response from leaking its contents — so the cost of that
/// padding is invisible unless it is measured here.
///
/// Three separate ratios, because they fail independently and the fix differs:
/// how many rows of a pinned table are used at all, how full each used row is,
/// and the two together against the bytes actually stored.
fn utilisation(shards: &[SealedShard], geometry: &Geometry) {
    if shards.is_empty() {
        return;
    }
    let n = shards.len() as u64;
    let page_rows_per_segment = geometry.page_rows;
    let dir_rows_per_segment = geometry.directory_rows;
    let slots = geometry.directory_slots();
    let per_page = transparent_shard::EVENTS_PER_PAGE as u64;

    let scripts: u64 = shards.iter().map(|s| s.occupancy.scripts).sum();
    let page_rows: u64 = shards.iter().map(|s| s.occupancy.fragments).sum();
    let paged: u64 = shards.iter().map(|s| s.occupancy.paged_events()).sum();

    // A shard takes as many segments as its content needs, so pinned bytes are
    // per shard rather than a constant.
    let mut dir_pinned = 0u64;
    let mut page_pinned = 0u64;
    let mut dir_segments = 0u64;
    let mut page_segments = 0u64;
    for shard in shards {
        let d = transparent_shard::layout::segments_for(
            shard.occupancy.scripts.div_ceil(slots),
            dir_rows_per_segment,
        ) as u64;
        let p = transparent_shard::layout::segments_for(
            shard.occupancy.fragments,
            page_rows_per_segment,
        ) as u64;
        dir_segments += d;
        page_segments += p;
        dir_pinned += d * geometry.directory_bytes_per_segment();
        page_pinned += p * geometry.page_bytes_per_segment();
    }
    let pinned = dir_pinned + page_pinned;

    // Live bytes: what a reader would actually get back if the padding were
    // stripped. Directory entries carry their inline events, so those are not
    // counted again as page content.
    let live = scripts * geometry.directory_entry_bytes() as u64
        + page_rows * transparent_shard::PAGE_ENTRY_HEADER_BYTES as u64
        + paged * transparent_events::EVENT_BYTES as u64;

    let dir_row_use = scripts.div_ceil(slots) as f64 / (dir_segments * dir_rows_per_segment) as f64;
    let dir_fill = scripts as f64 / (dir_segments * dir_rows_per_segment * slots) as f64;
    let page_row_use = page_rows as f64 / (page_segments * page_rows_per_segment) as f64;
    let page_fill = if page_rows == 0 {
        0.0
    } else {
        paged as f64 / (page_rows * per_page) as f64
    };

    println!("  utilisation");
    println!(
        "    directory   rows {:>5.1}%  slots {:>5.1}%  ({:.1} of {slots} per row, {} segments)",
        dir_row_use * 100.0,
        dir_fill * 100.0,
        scripts as f64 / (dir_segments * dir_rows_per_segment) as f64,
        dir_segments,
    );
    println!(
        "    pages       rows {:>5.1}%  slots {:>5.1}%  ({:.1} of {per_page} per row, {} segments)",
        page_row_use * 100.0,
        page_fill * 100.0,
        if page_rows == 0 {
            0.0
        } else {
            paged as f64 / page_rows as f64
        },
        page_segments,
    );
    println!(
        "    live bytes  {:.1} MB of {:.1} MB pinned = {:>5.2}%   ({:.1} MB per shard)",
        live as f64 / 1e6,
        pinned as f64 / 1e6,
        live as f64 / pinned as f64 * 100.0,
        pinned as f64 / n as f64 / 1e6,
    );
    // Split, because the two tables answer to different limits and a geometry
    // change moves bytes between them. At the compiled geometry the directory
    // is a fifth of the total, which is why a page-row decision has dominated
    // the pinned-byte argument and a directory-row decision will not.
    println!(
        "    pinned split  directory {:.1} MB ({:>4.1}%), pages {:.1} MB ({:>4.1}%)",
        dir_pinned as f64 / 1e6,
        dir_pinned as f64 / pinned as f64 * 100.0,
        page_pinned as f64 / 1e6,
        page_pinned as f64 / pinned as f64 * 100.0,
    );
    placement(shards, geometry, dir_segments);
}

/// What the real two-choice placer does with the same script sets.
///
/// Everything above models the directory as `scripts / slots`, which is what
/// placement would cost if rows filled evenly. They do not, and the difference
/// is the entire question of how close to a segment's capacity a script target
/// may sit: the fullest row is what adds a segment, and a segment doubles the
/// directory query and setup cost of every wallet that touches the shard.
///
/// Silent when no shard was asked for its placement, so the default run is
/// unchanged and remains comparable with the archived censuses.
fn placement(shards: &[SealedShard], geometry: &Geometry, modelled_segments: u64) {
    let placed: Vec<(&SealedShard, transparent_shard::build::Placement)> = shards
        .iter()
        .filter_map(|shard| shard.placement.map(|p| (shard, p)))
        .collect();
    if placed.is_empty() {
        return;
    }
    let slots = geometry.directory_slots();
    let real_segments: u64 = placed.iter().map(|(_, p)| p.segments as u64).sum();
    let stacked = placed.iter().filter(|(_, p)| p.segments > 1).count();

    // Row loads are what placement *settled* at, so a shard that overflowed and
    // took another segment reports its load over the larger row space — a low
    // number there means it stacked, not that it had room. Headroom is
    // therefore read only off the shards that fit one segment, and the stacked
    // count is what says whether that population is the whole set.
    let mut loads: Vec<u64> = placed
        .iter()
        .filter(|(_, p)| p.segments == 1)
        .map(|(_, p)| p.max_row_load)
        .collect();
    loads.sort_unstable();
    let (worst, worst_shard) = placed
        .iter()
        .filter(|(_, p)| p.segments == 1)
        .map(|(shard, p)| (p.max_row_load, shard.shard_id))
        .max()
        .unwrap_or((0, 0));

    println!("  placement (the real two-choice placer, not scripts / slots)");
    println!(
        "    segments     {real_segments} placed against {modelled_segments} modelled; \
{stacked} of {} shards need more than one",
        placed.len()
    );
    if loads.is_empty() {
        println!("    fullest row  no shard placed in one segment");
    } else {
        println!(
            "    fullest row  {worst} of {slots} in shard {worst_shard}, p50 {}, p95 {} \
(over the {} shards that fit one segment)",
            percentile(&loads, 50.0),
            percentile(&loads, 95.0),
            loads.len(),
        );
    }
    if stacked > 0 {
        println!(
            "    this script target has no placement headroom: {stacked} shards overflowed a \
directory segment, and each added segment doubles the directory query and setup cost of every \
wallet that touches that shard"
        );
    } else if worst + 1 >= slots {
        println!(
            "    the fullest row is one entry short of its slot count: this target is at the \
edge of needing a second segment"
        );
    }
}

/// What the same content would cost with short histories packed into shared rows.
///
/// This is arithmetic over per-script event counts, not a measurement. No v5
/// codec, builder or published set exists at this commit: the boundaries below
/// are the v4 boundaries above, with packed demand carried alongside them as a
/// passenger. It says what the packing rule *would* ask for, and the only way
/// it can be wrong is if a builder that does not yet exist disagrees.
///
/// Two further caveats it shares with the measured figures above: directory
/// placement is modelled as `scripts / slots` rather than run through the real
/// two-choice placer, so directory segment counts are a lower bound; and
/// encoding overhead is a modelled constant rather than emitted bytes.
fn projection(shards: &[SealedShard], policy: &SealPolicy, geometry: &Geometry, basis: PageBasis) {
    if shards.is_empty() {
        return;
    }
    let per_row = PAGE_ROW_BYTES - PAGE_ROW_HEADER_BYTES;
    let page_rows_per_segment = geometry.page_rows;
    let dir_rows_per_segment = geometry.directory_rows;
    let slots = geometry.directory_slots();

    println!();
    match basis {
        PageBasis::Packed => {
            println!("  === Packed rows: what the builder lays out. The block above says what");
            println!("      the same content would cost with a row per fragment, which is what");
            println!("      the layout did before this. Row demand here comes from the packing");
            println!("      rule, and the builder asserts it emits exactly this many. ===");
        }
        PageBasis::Fragments => {
            println!("  === Packed rows, carried alongside boundaries chosen by the unpacked");
            println!("      figure. Not a set that could be published: the builder packs, so");
            println!("      these are not boundaries it would produce. ===");
        }
    }

    // Per class: scripts across the whole set, and rows, which must be summed
    // per shard because the ceiling is per shard.
    println!("  proj_classes (p = paged events; a class of one script still costs a row)");
    let mut class_scripts = [0u64; EVENTS_PER_PAGE as usize + 1];
    let mut class_rows = [0u64; EVENTS_PER_PAGE as usize + 1];
    for shard in shards {
        for p in 1..=EVENTS_PER_PAGE as usize {
            let n = shard.occupancy.demand.class(p as u32);
            class_scripts[p] += n;
            class_rows[p] += n.div_ceil(entries_per_row(p as u32) as u64);
        }
    }
    for p in 1..=EVENTS_PER_PAGE as usize {
        if class_scripts[p] == 0 {
            continue;
        }
        let per = entries_per_row(p as u32) as u64;
        let waste = class_rows[p] * per - class_scripts[p];
        println!(
            "    p {p:>2}  scripts {:>9}  rows {:>8}  {per:>2} per row  tail slack {waste:>7}",
            class_scripts[p], class_rows[p]
        );
    }
    let short_rows: u64 = class_rows.iter().sum();
    let short_scripts: u64 = class_scripts.iter().sum();
    let long_rows: u64 = shards.iter().map(|s| s.occupancy.demand.long_rows()).sum();
    let long_scripts: u64 = shards
        .iter()
        .map(|s| s.occupancy.demand.long_scripts())
        .sum();
    println!(
        "    short  scripts {short_scripts:>9}  rows {short_rows:>8}   long  scripts {long_scripts:>7}  rows {long_rows:>8}"
    );

    // Per shard, R against the unpacked figure that actually sealed it.
    let mut proj: Vec<u64> = shards
        .iter()
        .map(|s| s.occupancy.packed_page_rows)
        .collect();
    describe("proj_rows", &mut proj);
    let fragments: u64 = shards.iter().map(|s| s.occupancy.fragments).sum();
    let packed: u64 = shards.iter().map(|s| s.occupancy.packed_page_rows).sum();
    // The unpacked figure still counts scripts too long for a directory entry,
    // which the builder never pages and packing therefore never sees. Where
    // that count is nonzero the ratio flatters packing slightly.
    println!(
        "  proj_R {packed} rows against {fragments} unpacked fragments = {:.2}x fewer rows",
        if packed == 0 {
            0.0
        } else {
            fragments as f64 / packed as f64
        }
    );

    // The byte model under packing: one entry header per fragment, one row
    // header per used row, and the events themselves.
    let scripts: u64 = shards.iter().map(|s| s.occupancy.scripts).sum();
    let paged: u64 = shards.iter().map(|s| s.occupancy.paged_events()).sum();
    let live = scripts * geometry.directory_entry_bytes() as u64
        + fragments * transparent_shard::PAGE_ENTRY_HEADER_BYTES as u64
        + packed * PAGE_ROW_HEADER_BYTES as u64
        + paged * transparent_events::EVENT_BYTES as u64;
    let mut pinned = 0u64;
    let mut page_segments = 0u64;
    let mut dir_segments = 0u64;
    for shard in shards {
        let d = transparent_shard::layout::segments_for(
            shard.occupancy.scripts.div_ceil(slots),
            dir_rows_per_segment,
        ) as u64;
        let g = transparent_shard::layout::segments_for(
            shard.occupancy.packed_page_rows,
            page_rows_per_segment,
        ) as u64;
        dir_segments += d;
        page_segments += g;
        pinned +=
            d * geometry.directory_bytes_per_segment() + g * geometry.page_bytes_per_segment();
    }

    // Row fill is measured in bytes, not events. Once a row mixes classes,
    // "events of 36" describes nothing: a row of 22 one-event entries is 98%
    // full and holds 22 events.
    let used_bytes: u64 = fragments * transparent_shard::PAGE_ENTRY_HEADER_BYTES as u64
        + paged * transparent_events::EVENT_BYTES as u64;
    println!(
        "  proj_utilisation  page rows {:>5.1}%  row bytes {:>5.1}%  ({} page segments)",
        packed as f64 / (page_segments * page_rows_per_segment) as f64 * 100.0,
        if packed == 0 {
            0.0
        } else {
            used_bytes as f64 / (packed * per_row as u64) as f64 * 100.0
        },
        page_segments,
    );
    // Segments are for the indivisible block that cannot fit, not for routine
    // capacity. A wallet must query every segment of a generation, so an
    // n-segment generation multiplies its setup, responses and server work by
    // n for every lookup in it. A geometry that makes ordinary generations
    // stack is not cheaper, whatever it does to pinned bytes.
    let stacked = shards
        .iter()
        .filter(|s| {
            transparent_shard::layout::segments_for(
                s.occupancy.packed_page_rows,
                page_rows_per_segment,
            ) > 1
        })
        .count();
    println!(
        "  proj_segments  {dir_segments} directory, {page_segments} page, across {} shards; \
{stacked} generations need more than one page segment",
        shards.len()
    );
    println!(
        "  proj_live bytes  {:.1} MB of {:.1} MB pinned = {:>5.2}%   (fleet ~{:.2} GB)",
        live as f64 / 1e6,
        pinned as f64 / 1e6,
        live as f64 / pinned as f64 * 100.0,
        pinned as f64 / 1e9,
    );

    // Which limit would close a shard if R replaced the unpacked figure. This
    // is the question the projection exists to answer: if page rows stop
    // binding, the script limit binds instead and the page table becomes the
    // mostly-empty one.
    let would_reach_pages = shards
        .iter()
        .filter(|s| s.occupancy.packed_page_rows >= policy.page_rows.target)
        .count();
    let would_reach_scripts = shards
        .iter()
        .filter(|s| s.occupancy.scripts >= policy.scripts.target)
        .count();
    println!(
        "  proj_binding  {would_reach_pages} of {} shards would still reach the page-row target {}; \
{would_reach_scripts} reach the script target {}",
        shards.len(),
        policy.page_rows.target,
        policy.scripts.target,
    );
}

fn report(
    name: &str,
    shards: &[SealedShard],
    policy: &SealPolicy,
    geometry: &Geometry,
    basis: PageBasis,
    per_shard: bool,
) {
    let sealed: Vec<&SealedShard> = shards.iter().filter(|s| s.reason.is_some()).collect();
    println!("\n=== {name} ===");
    println!(
        "  shards {} ({} sealed, {} tail)",
        shards.len(),
        sealed.len(),
        shards.len() - sealed.len()
    );

    let mut by_reason: std::collections::BTreeMap<String, usize> = Default::default();
    for shard in &sealed {
        let key = match shard.reason {
            Some(SealReason::ReachedTarget(q)) => format!("target: {q}"),
            Some(SealReason::WouldExceedCapacity(q)) => format!("capacity: {q}"),
            // A single block past a capacity: this shard holds only that block
            // and needs more than one segment per table.
            Some(SealReason::BlockExceedsCapacity(q)) => format!("oversized block: {q}"),
            // A census sweeps one geometry at a time, so nothing here forces a
            // boundary; only the publisher's two-tier mode does.
            Some(SealReason::GeometryChanged) => "geometry change".to_string(),
            None => unreachable!(),
        };
        *by_reason.entry(key).or_default() += 1;
    }
    for (reason, count) in &by_reason {
        println!("  sealed by {reason:<24} {count}");
    }

    if per_shard {
        // Tab separated and prefixed, so the rows can be lifted out of a run's
        // log with a grep and fed to anything, without the surrounding report
        // needing a machine-readable format of its own.
        println!("  per_shard\tshard_id\tstart\tend\tblocks\tscripts\tpage_rows\tfragments\tevents\treason");
        for shard in &sealed {
            let reason = match shard.reason {
                Some(SealReason::ReachedTarget(q)) => format!("target:{q}"),
                Some(SealReason::WouldExceedCapacity(q)) => format!("capacity:{q}"),
                Some(SealReason::BlockExceedsCapacity(q)) => format!("oversized:{q}"),
                Some(SealReason::GeometryChanged) => "geometry-change".to_string(),
                None => unreachable!("sealed shards all carry a reason"),
            };
            println!(
                "  per_shard\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                shard.shard_id,
                shard.start_height,
                shard.end_height,
                shard.occupancy.blocks,
                shard.occupancy.scripts,
                shard.occupancy.page_rows,
                shard.occupancy.fragments,
                shard.occupancy.events,
                reason.replace(' ', "_"),
            );
        }
    }

    describe(
        "blocks",
        &mut shards
            .iter()
            .map(|s| s.occupancy.blocks)
            .collect::<Vec<_>>(),
    );
    describe(
        "scripts",
        &mut shards
            .iter()
            .map(|s| s.occupancy.scripts)
            .collect::<Vec<_>>(),
    );
    describe(
        "fragments",
        &mut shards
            .iter()
            .map(|s| s.occupancy.fragments)
            .collect::<Vec<_>>(),
    );
    describe(
        "txids",
        &mut shards.iter().map(|s| s.occupancy.txids).collect::<Vec<_>>(),
    );
    describe(
        "events",
        &mut shards
            .iter()
            .map(|s| s.occupancy.events)
            .collect::<Vec<_>>(),
    );

    // Filter bytes at roughly (P+1)/8 = 2.5 bytes per element, which is what
    // the Golomb-Rice encoding costs at this profile's parameters. A wallet
    // downloads every one of these, so their sum is the unavoidable floor of a
    // restoration and the number the whole design is trying to keep small.
    let scripts: u64 = shards.iter().map(|s| s.occupancy.scripts).sum();
    let filter_bytes = scripts * 5 / 2;
    println!(
        "  all filters ~{:.2} MB across {} shards",
        filter_bytes as f64 / 1e6,
        shards.len()
    );

    utilisation(shards, geometry);
    projection(shards, policy, geometry, basis);

    // Plaintext table bytes, if every shard is padded to the capacity its
    // policy pins. This is what the service stores and what its PIR
    // preprocessing is built over.
    let max_scripts = shards
        .iter()
        .map(|s| s.occupancy.scripts)
        .max()
        .unwrap_or(0);
    let max_pages = shards
        .iter()
        .map(|s| s.occupancy.fragments)
        .max()
        .unwrap_or(0);
    let max_txids = shards.iter().map(|s| s.occupancy.txids).max().unwrap_or(0);
    println!("  observed maxima: scripts {max_scripts}, fragments {max_pages}, txids {max_txids}");
    let per_shard = geometry.directory_bytes_per_segment() + geometry.page_bytes_per_segment();
    println!(
        "  pinned plaintext per shard ~{:.0} MB, fleet ~{:.1} GB",
        per_shard as f64 / 1e6,
        (per_shard * shards.len() as u64) as f64 / 1e9
    );
}

fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    // Read-only: `open` would set an incompatible journal aside, which for a
    // measurement tool means destroying the thing it was asked to measure.
    let store = EventStore::open_existing(&cli.data_dir)?;

    let Some(covered) = store.covered_through() else {
        return Err("the journal is empty".into());
    };
    let journal_first = store.start_height();
    let first = cli.start_height.unwrap_or(journal_first);
    let covered = cli.end_height.unwrap_or(covered);
    if first < journal_first || covered > store.covered_through().expect("non-empty") {
        return Err(format!(
            "requested {first}-{covered} is outside the journal's {journal_first}-{}",
            store.covered_through().expect("non-empty")
        )
        .into());
    }
    if first > covered {
        return Err(format!("requested range {first}-{covered} is empty").into());
    }

    // Provenance, printed before anything derived from it. An archived census
    // whose coverage is not on its face gets read as whatever the reader
    // assumes: the genesis-range runs under `docs/transparent-pir-evaluation`
    // cover 9.4% of chain height and were taken for full-chain results in later
    // work. The anchor hash is what makes a run identifiable at all, since two
    // journals can cover the same heights on different branches.
    let anchor = store
        .block_at(covered)
        .ok_or_else(|| format!("height {covered} is missing from the journal"))?;
    println!(
        "journal: heights {journal_first}-{}, {} blocks, {} events",
        store.covered_through().expect("non-empty"),
        store.blocks_covered(),
        store.events_stored()
    );
    println!("genesis: {}", store.genesis_hash());
    println!(
        "census:  heights {first}-{covered} inclusive, anchor {}",
        anchor.block_hash.to_display_hex()
    );

    let policies: Vec<(String, SealPolicy)> = if cli.policies.is_empty() {
        default_policies()
    } else {
        cli.policies
            .iter()
            .map(|text| Ok((text.clone(), parse_policy(text)?)))
            .collect::<Result<_, BoxError>>()?
    };

    // One pass, every policy fed from it. Holding the journal first and
    // replaying it per policy is what the earlier version did, and it does not
    // survive a genesis-to-tip journal: a hundred million events carrying a
    // heap-allocated script each is tens of gigabytes, far worse in memory than
    // on disk. Reading each height once and handing it to every sealer gives
    // the same "every policy sees exactly the same input" property for the cost
    // of one block at a time, and a second pass would buy nothing because the
    // run is dominated by reading and decoding the journal.
    let compiled = *geometry_by_name(&cli.geometry)
        .ok_or_else(|| format!("unknown geometry {:?}", cli.geometry))?;
    let geometry = Geometry {
        directory_rows: cli
            .directory_rows_per_segment
            .unwrap_or(compiled.directory_rows),
        directory_row_bytes: cli
            .directory_row_bytes
            .unwrap_or(compiled.directory_row_bytes),
        page_rows: cli.page_rows_per_segment.unwrap_or(compiled.page_rows),
        inline_events: cli.inline_events.unwrap_or(compiled.inline_events),
        ..compiled
    };
    // A candidate the scheme would round up is a candidate whose reported cost
    // is not its cost, so it is refused rather than scored.
    geometry.validate()?;
    let page_rows_per_segment = geometry.page_rows;
    let basis = if cli.unpacked {
        PageBasis::Fragments
    } else {
        PageBasis::Packed
    };
    if geometry != compiled {
        println!(
            "\nscoring against {} directory rows x {} slots ({} scripts, {} B rows, {} inline \
events) and {} page rows, rather than the compiled {} x {} ({} scripts) and {}",
            geometry.directory_rows,
            geometry.directory_slots(),
            geometry.directory_capacity(),
            geometry.directory_row_bytes,
            geometry.inline_events,
            geometry.page_rows,
            compiled.directory_rows,
            compiled.directory_slots(),
            compiled.directory_capacity(),
            compiled.page_rows,
        );
        // The inline allowance is the one override that is not merely
        // arithmetic: it decides whether a history is paged, so it moves the
        // boundaries themselves. Saying so is the difference between a figure
        // that can be compared with the compiled run and one that cannot.
        if geometry.inline_events != compiled.inline_events {
            println!(
                "  the inline allowance moves the boundaries, not just the arithmetic: these \
shards are not the compiled run's shards rescored"
            );
        } else {
            println!("  segment and pinned-byte figures follow the override, nothing built does");
        }
    }
    // A policy whose script capacity exceeds a directory segment can seal into
    // a second one as a matter of routine, which is the one thing segments must
    // not be used for. This warns rather than refusing, unlike the page check
    // below, because the default sweep sets capacity to twice its target
    // deliberately — an inert ceiling, so that only the target bites — and
    // refusing it would make the tool unable to reproduce its own archived
    // runs. Whether the ceiling was reached is reported per policy either way,
    // in the directory segment count.
    let over: Vec<&str> = policies
        .iter()
        .filter(|(_, policy)| policy.scripts.capacity > geometry.directory_capacity())
        .map(|(name, _)| name.as_str())
        .collect();
    if !over.is_empty() {
        println!(
            "\nnote: {} allow more scripts than the {} a directory segment holds; \
a shard that reached that ceiling would need a second directory segment. Check the \
directory segment counts below before reading any of them as a single-segment result.",
            over.join(", "),
            geometry.directory_capacity(),
        );
    }
    if matches!(basis, PageBasis::Fragments) {
        println!(
            "sealing on a row per fragment, which is not what the builder emits; this reports \
what the journal would have cost before short histories shared rows, and must not be used to \
choose parameters for a set that will be published"
        );
    }
    // A policy that may exceed the table it is scored against would seal into a
    // second segment as a matter of routine, which is the one thing segments
    // must not be used for.
    for (name, policy) in &policies {
        if policy.page_rows.capacity > page_rows_per_segment {
            return Err(format!(
                "policy {name:?} allows {} page rows against a segment holding \
{page_rows_per_segment}; ordinary generations would stack segments",
                policy.page_rows.capacity
            )
            .into());
        }
    }

    // Where the shard-match counter spills its sorted runs. A directory the
    // operator names, because this is the one thing the census writes and it
    // should not invent a location for it.
    let spill = cli.shard_matches.as_deref();
    if let Some(dir) = spill {
        std::fs::create_dir_all(dir)?;
        println!(
            "\ncounting shard matches per script, spilling sorted runs under {}",
            dir.display()
        );
    }

    struct Run {
        name: String,
        policy: SealPolicy,
        sealer: Sealer,
        shards: Vec<SealedShard>,
        matches: Option<MatchCounter>,
    }
    let mut runs: Vec<Run> = policies
        .into_iter()
        .map(|(name, policy)| Run {
            sealer: {
                let mut sealer = Sealer::with_geometry(policy, first, basis, geometry);
                sealer.measure_placement(cli.placement);
                sealer.retain_scripts(spill.is_some());
                sealer
            },
            matches: spill.map(|dir| MatchCounter::new(dir, &name)),
            name,
            policy,
            shards: Vec::new(),
        })
        .collect();

    println!(
        "\n=== density across the chain ({} blocks per bucket) ===",
        cli.density_bucket
    );
    let mut bucket_start = first;
    let mut bucket_events = 0u64;
    let mut bucket_blocks = 0u64;
    for height in first..=covered {
        let events = store
            .events_at(height)?
            .ok_or_else(|| format!("height {height} is missing from the journal"))?;

        bucket_events += events.len() as u64;
        bucket_blocks += 1;
        if bucket_blocks == cli.density_bucket || height == covered {
            println!(
                "  {bucket_start:>8}-{height:<8} {:>7.1} events/block",
                bucket_events as f64 / bucket_blocks as f64
            );
            bucket_start = height + 1;
            bucket_events = 0;
            bucket_blocks = 0;
        }

        for run in &mut runs {
            let sealed = run.sealer.push_block(height, &events)?;
            drain_scripts(&mut run.matches, sealed, &mut run.shards)?;
        }
    }

    for (index, run) in runs.iter_mut().enumerate() {
        let sealed: Vec<SealedShard> = run.sealer.finish().into_iter().collect();
        drain_scripts(&mut run.matches, sealed, &mut run.shards)?;
        // Only the first policy: the rows are per shard, and a full sweep
        // would bury the report under thousands of them.
        report(
            &run.name,
            &run.shards,
            &run.policy,
            &geometry,
            basis,
            cli.per_shard && index == 0,
        );
        if let Some(counter) = run.matches.take() {
            let distribution = counter.finish()?;
            distribution.report();
            // Priced at this geometry's own query sizes, because that is the
            // comparison: a wider table costs more per query and is asked
            // fewer of them, and only the product says which wins.
            distribution.report_cost(
                query_bytes(geometry.directory_rows, geometry.directory_row_bytes),
                query_bytes(geometry.page_rows, PAGE_ROW_BYTES),
            );
        }
    }

    Ok(())
}

/// Feeds each sealed shard's scripts to the counter and drops them.
///
/// The dropping is the point. [`SealedShard::scripts`] is one shard's set,
/// which is bounded; keeping every shard's would hold the journal's whole
/// script set, and this census exists partly because an earlier version could
/// not survive a genesis-to-tip journal in memory.
fn drain_scripts(
    matches: &mut Option<MatchCounter>,
    sealed: Vec<SealedShard>,
    into: &mut Vec<SealedShard>,
) -> Result<(), BoxError> {
    for mut shard in sealed {
        if let (Some(scripts), Some(counter)) = (shard.scripts.take(), matches.as_mut()) {
            for (script, fragments) in scripts {
                counter.record(&script, fragments)?;
            }
        }
        into.push(shard);
    }
    Ok(())
}
