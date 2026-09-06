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
use transparent_shard::layout::{DIRECTORY_ROW_BYTES, PAGE_ROW_BYTES};
use transparent_shard::seal::{Limit, SealPolicy, SealReason, SealedShard, Sealer};

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
fn utilisation(shards: &[SealedShard]) {
    if shards.is_empty() {
        return;
    }
    let n = shards.len() as u64;
    let dir_rows_per_segment = transparent_shard::DIRECTORY_ROWS as u64;
    let page_rows_per_segment = transparent_shard::PAGE_ROWS as u64;
    let slots = transparent_shard::DIRECTORY_SLOTS as u64;
    let per_page = transparent_shard::EVENTS_PER_PAGE as u64;

    let scripts: u64 = shards.iter().map(|s| s.occupancy.scripts).sum();
    let page_rows: u64 = shards.iter().map(|s| s.occupancy.page_rows).sum();
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
            shard.occupancy.page_rows,
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

fn report(name: &str, shards: &[SealedShard]) {
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
        "page rows",
        &mut shards
            .iter()
            .map(|s| s.occupancy.page_rows)
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

    utilisation(shards);

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
        .map(|s| s.occupancy.page_rows)
        .max()
        .unwrap_or(0);
    let max_txids = shards.iter().map(|s| s.occupancy.txids).max().unwrap_or(0);
    println!("  observed maxima: scripts {max_scripts}, page rows {max_pages}, txids {max_txids}");
    let per_shard = transparent_shard::DIRECTORY_ROWS as u64 * DIRECTORY_ROW_BYTES as u64
        + transparent_shard::PAGE_ROWS as u64 * PAGE_ROW_BYTES as u64;
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

    // Load once. The journal is small enough to hold, and every candidate
    // policy has to see exactly the same input for the comparison to mean
    // anything.
    let mut blocks = Vec::with_capacity(store.blocks_covered() as usize);
    for height in first..=covered {
        let events = store
            .events_at(height)?
            .ok_or_else(|| format!("height {height} is missing from the journal"))?;
        blocks.push((height, events));
    }

    println!(
        "\n=== density across the chain ({} blocks per bucket) ===",
        cli.density_bucket
    );
    let mut bucket_start = first;
    let mut bucket_events = 0u64;
    let mut bucket_blocks = 0u64;
    for (height, events) in &blocks {
        bucket_events += events.len() as u64;
        bucket_blocks += 1;
        if bucket_blocks == cli.density_bucket || *height == covered {
            println!(
                "  {bucket_start:>8}-{height:<8} {:>7.1} events/block",
                bucket_events as f64 / bucket_blocks as f64
            );
            bucket_start = height + 1;
            bucket_events = 0;
            bucket_blocks = 0;
        }
    }

    let policies: Vec<(String, SealPolicy)> = if cli.policies.is_empty() {
        default_policies()
    } else {
        cli.policies
            .iter()
            .map(|text| Ok((text.clone(), parse_policy(text)?)))
            .collect::<Result<_, BoxError>>()?
    };

    for (name, policy) in policies {
        let mut sealer = Sealer::new(policy, first);
        let mut shards = Vec::new();
        for (height, events) in &blocks {
            shards.extend(sealer.push_block(*height, events)?);
        }
        shards.extend(sealer.finish());
        report(&name, &shards);
    }

    Ok(())
}
