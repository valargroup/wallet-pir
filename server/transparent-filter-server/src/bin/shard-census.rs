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
//! Read-only. It opens the journal, streams it, and writes nothing.

use clap::Parser;
use std::path::PathBuf;
use transparent_filter_server::events::EventStore;
use transparent_shard::layout::{
    entries_per_row, DIRECTORY_ROW_BYTES, EVENTS_PER_PAGE, PAGE_ROW_BYTES, PAGE_ROW_HEADER_BYTES,
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
    /// Close shards on packed row demand rather than on a row per fragment.
    ///
    /// For measuring a geometry before a packed builder exists. A published set
    /// must not be built this way until the builder packs, or the table would
    /// be sized for less than the builder writes.
    #[arg(long)]
    packed: bool,
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
fn default_policies() -> Vec<(String, SealPolicy)> {
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
                page_rows: Limit::new(7_900, 8_192).expect("valid"),
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
fn utilisation(shards: &[SealedShard], page_rows_per_segment: u64) {
    if shards.is_empty() {
        return;
    }
    let n = shards.len() as u64;
    let dir_rows_per_segment = transparent_shard::DIRECTORY_ROWS as u64;
    let slots = transparent_shard::DIRECTORY_SLOTS as u64;
    let per_page = transparent_shard::EVENTS_PER_PAGE as u64;

    let scripts: u64 = shards.iter().map(|s| s.occupancy.scripts).sum();
    let page_rows: u64 = shards.iter().map(|s| s.occupancy.fragments).sum();
    let paged: u64 = shards.iter().map(|s| s.occupancy.paged_events()).sum();

    // A shard takes as many segments as its content needs, so pinned bytes are
    // per shard rather than a constant.
    let mut pinned = 0u64;
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
        pinned += d * dir_rows_per_segment * DIRECTORY_ROW_BYTES as u64
            + p * page_rows_per_segment * PAGE_ROW_BYTES as u64;
    }

    // Live bytes: what a reader would actually get back if the padding were
    // stripped. Directory entries carry their inline events, so those are not
    // counted again as page content.
    let live = scripts * transparent_shard::DIRECTORY_ENTRY_BYTES as u64
        + page_rows * transparent_shard::layout::PAGE_HEADER_BYTES as u64
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
fn projection(
    shards: &[SealedShard],
    policy: &SealPolicy,
    page_rows_per_segment: u64,
    basis: PageBasis,
) {
    if shards.is_empty() {
        return;
    }
    let per_row = PAGE_ROW_BYTES - PAGE_ROW_HEADER_BYTES;
    let dir_rows_per_segment = transparent_shard::DIRECTORY_ROWS as u64;
    let slots = transparent_shard::DIRECTORY_SLOTS as u64;

    println!();
    println!("  === PROJECTION ONLY — arithmetic over per-script event counts. No v5");
    println!("      codec, builder or published set exists. These are projected ROW");
    println!("      DEMANDS derived from the packing rule, not measured bytes. ===");
    match basis {
        PageBasis::Fragments => {
            println!("      Boundaries are the unpacked ones above, with R carried alongside them.")
        }
        PageBasis::Packed => {
            println!("      Boundaries were chosen by R. The block above says what the");
            println!("      unpacked layout would cost for the same content, which is");
            println!("      not what shipped.");
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
    let live = scripts * transparent_shard::DIRECTORY_ENTRY_BYTES as u64
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
        pinned += d * dir_rows_per_segment * DIRECTORY_ROW_BYTES as u64
            + g * page_rows_per_segment * PAGE_ROW_BYTES as u64;
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
    page_rows_per_segment: u64,
    basis: PageBasis,
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
            None => unreachable!(),
        };
        *by_reason.entry(key).or_default() += 1;
    }
    for (reason, count) in &by_reason {
        println!("  sealed by {reason:<24} {count}");
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

    utilisation(shards, page_rows_per_segment);
    projection(shards, policy, page_rows_per_segment, basis);

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
    let per_shard = transparent_shard::DIRECTORY_ROWS as u64 * DIRECTORY_ROW_BYTES as u64
        + page_rows_per_segment * PAGE_ROW_BYTES as u64;
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
    let first = store.start_height();
    println!(
        "journal: heights {first}-{covered} ({} blocks), {} events",
        store.blocks_covered(),
        store.events_stored()
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
    let page_rows_per_segment = cli
        .page_rows_per_segment
        .unwrap_or(transparent_shard::PAGE_ROWS as u64);
    let basis = if cli.packed {
        PageBasis::Packed
    } else {
        PageBasis::Fragments
    };
    if page_rows_per_segment != transparent_shard::PAGE_ROWS as u64 {
        println!(
            "\nscoring against {page_rows_per_segment} page rows per segment rather than the \
compiled {}; segment and pinned-byte figures follow the override, nothing built does",
            transparent_shard::PAGE_ROWS
        );
    }
    if matches!(basis, PageBasis::Packed) {
        println!(
            "sealing on packed row demand; the builder at this commit still emits a row per \
fragment, so this measures a geometry rather than describing a set that could be published"
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

    struct Run {
        name: String,
        policy: SealPolicy,
        sealer: Sealer,
        shards: Vec<SealedShard>,
    }
    let mut runs: Vec<Run> = policies
        .into_iter()
        .map(|(name, policy)| Run {
            name,
            sealer: Sealer::with_basis(policy, first, basis),
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
            run.shards.extend(run.sealer.push_block(height, &events)?);
        }
    }

    for run in &mut runs {
        run.shards.extend(run.sealer.finish());
        report(
            &run.name,
            &run.shards,
            &run.policy,
            page_rows_per_segment,
            basis,
        );
    }

    // A restoring wallet makes two directory queries for every generation
    // containing one of its scripts. Occupancy counts script-generation pairs
    // but not how often the same exact script recurs across boundaries. Replay
    // the bounded study journal once against the selected boundaries to report
    // that distribution exactly.
    //
    // This keeps one exact-script map per policy. A genesis-scale sweep with
    // many policies should spill fixed-width keys to disk and externally
    // aggregate them instead of assuming every historical script fits in RAM.
    struct MatchRun<'a> {
        name: &'a str,
        shards: &'a [SealedShard],
        shard_index: usize,
        current: std::collections::HashSet<Vec<u8>>,
        matches: std::collections::HashMap<Vec<u8>, u32>,
    }

    let mut match_runs: Vec<MatchRun<'_>> = runs
        .iter()
        .map(|run| MatchRun {
            name: &run.name,
            shards: &run.shards,
            shard_index: 0,
            current: Default::default(),
            matches: Default::default(),
        })
        .collect();

    for height in first..=covered {
        let events = store
            .events_at(height)?
            .ok_or_else(|| format!("height {height} is missing from the journal"))?;
        for run in &mut match_runs {
            for (script, _) in &events {
                if script.as_slice().len() <= transparent_shard::records::MAX_SCRIPT_BYTES {
                    run.current.insert(script.as_slice().to_vec());
                }
            }
            if height == run.shards[run.shard_index].end_height {
                for script in run.current.drain() {
                    *run.matches.entry(script).or_insert(0) += 1;
                }
                run.shard_index += 1;
            }
        }
    }

    println!("\n=== exact generation matches per supported script ===");
    for run in &match_runs {
        let mut counts: Vec<u64> = run
            .matches
            .values()
            .map(|count| u64::from(*count))
            .collect();
        let occurrences: u64 = counts.iter().sum();
        counts.sort_unstable();
        let distinct = counts.len() as u64;
        let mean = if distinct == 0 {
            0.0
        } else {
            occurrences as f64 / distinct as f64
        };
        println!("  {}", run.name);
        println!(
            "    distinct {distinct}  script-generation entries {occurrences}  mean {mean:.3}"
        );
        println!(
            "    matches      p50 {}  p90 {}  p95 {}  p99 {}  max {}",
            percentile(&counts, 50.0),
            percentile(&counts, 90.0),
            percentile(&counts, 95.0),
            percentile(&counts, 99.0),
            counts.last().copied().unwrap_or(0),
        );
        println!(
            "    directory queries per active script: mean {:.3}  p50 {}  p90 {}  p95 {}  p99 {}  max {}",
            2.0 * mean,
            2 * percentile(&counts, 50.0),
            2 * percentile(&counts, 90.0),
            2 * percentile(&counts, 95.0),
            2 * percentile(&counts, 99.0),
            2 * counts.last().copied().unwrap_or(0),
        );
    }

    Ok(())
}
