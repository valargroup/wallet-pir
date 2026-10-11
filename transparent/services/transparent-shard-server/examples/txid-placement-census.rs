//! Placement census for the `txid-2k8` display layout, from a published map.
//!
//! ```text
//! cargo run --profile release-fast -p transparent-shard-server \
//!   --example txid-placement-census -- --map txid-shards.json > census.json
//! ```
//!
//! A display map lists every archive's range and entry count. An archive
//! seals at the first block that lifts it to the target, so its count minus
//! the target is the part of that block past the boundary: a sample, weighted
//! by block size, of what a seal at any target overshoots by. The census
//! re-seals the map's entries at `--target`, giving archive `i` the target
//! plus the `i`-th observed overshoot (cyclically), and the recent window the
//! window plus the next one.
//!
//! Each table is then placed by the production placement
//! (`transparent_shard::display::place_bucket`) over pseudorandom tags. A tag
//! is a SHA-256 output, and a row is a SHA-256 of the tag under the seed, so
//! random tags have the placement statistics of real ones; only which seed a
//! given archive keeps differs from a run over the journal's own txids. The
//! `--sizes` grid repeats the placement `--trials` times per table size and
//! reports how often the first seed fits beside the Poisson model's figure.
//!
//! Nothing is read but the map, and nothing is written but stdout.
use rayon::prelude::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::Instant;
use transparent_shard::display::{place_bucket, Placement, MAX_PLACEMENT_SEEDS, TXID_2K8};
use transparent_shard::txid::{slots_per_row, Tag};

type Error = Box<dyn std::error::Error + Send + Sync>;

struct Args {
    map: String,
    target: u64,
    window: u64,
    trials: u64,
    sizes: Vec<u64>,
    seed: u64,
}

fn args() -> Result<Args, Error> {
    let mut parsed = Args {
        map: String::new(),
        target: 90_000,
        window: 90_000,
        trials: 200,
        sizes: vec![
            90_000, 92_000, 94_000, 96_000, 98_000, 100_000, 105_000, 110_000,
        ],
        seed: 1,
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or(format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--map" => parsed.map = value,
            "--target" => parsed.target = value.parse()?,
            "--window" => parsed.window = value.parse()?,
            "--trials" => parsed.trials = value.parse()?,
            "--seed" => parsed.seed = value.parse()?,
            "--sizes" => {
                parsed.sizes = value.split(',').map(str::parse).collect::<Result<_, _>>()?
            }
            other => return Err(format!("unknown flag {other}").into()),
        }
    }
    if parsed.map.is_empty() {
        return Err("--map is required".into());
    }
    Ok(parsed)
}

/// `count` pseudorandom tags, distinct per (`seed`, `table`).
fn tags(seed: u64, table: u64, count: u64) -> Vec<Tag> {
    (0..count)
        .map(|i| {
            let digest = Sha256::new()
                .chain_update(b"txid-placement-census")
                .chain_update(seed.to_le_bytes())
                .chain_update(table.to_le_bytes())
                .chain_update(i.to_le_bytes())
                .finalize();
            Tag(digest[..16].try_into().unwrap())
        })
        .collect()
}

fn place(seed: u64, table: u64, shard_id: u64, count: u64) -> Placement {
    place_bucket(&tags(seed, table, count), shard_id, 0, &TXID_2K8)
}

/// The Poisson model's chance that one seed leaves no row over capacity.
fn poisson_fit(entries: u64) -> f64 {
    let rows = TXID_2K8.directory_rows as f64;
    let lambda = entries as f64 / rows;
    let slots = slots_per_row(TXID_2K8.directory_row_bytes);
    let (mut term, mut cdf) = ((-lambda).exp(), 0.0);
    for k in 0..=slots {
        if k > 0 {
            term *= lambda / k as f64;
        }
        cdf += term;
    }
    cdf.powf(rows)
}

fn quantile(sorted: &[u32], q: f64) -> u32 {
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

fn main() -> Result<(), Error> {
    let args = args()?;
    let map: Value = serde_json::from_slice(&std::fs::read(&args.map)?)?;
    let old_target = map["seal"]["archive_target"]
        .as_u64()
        .ok_or("seal.archive_target")?;
    let shards = map["shards"].as_array().ok_or("shards")?;
    let records = |s: &Value| s["records"].as_u64().unwrap_or(0);
    let total: u64 = shards.iter().map(records).sum();
    let overshoot: Vec<u64> = shards
        .iter()
        .filter(|s| s["sealed"] == true)
        .map(|s| records(s) - old_target)
        .collect();
    if overshoot.is_empty() {
        return Err("the map lists no archive".into());
    }

    // Re-seal: archives while what is left still fills a target.
    let mut sizes = Vec::new();
    let mut left = total;
    while left >= args.target + overshoot[sizes.len() % overshoot.len()] {
        let size = args.target + overshoot[sizes.len() % overshoot.len()];
        sizes.push(size);
        left -= size;
    }
    let archives = sizes.len() as u64;
    let recent = args.window + overshoot[sizes.len() % overshoot.len()];

    let placed: Vec<(u64, u64, Placement)> = sizes
        .par_iter()
        .enumerate()
        .map(|(id, size)| {
            (
                id as u64,
                *size,
                place(args.seed, id as u64, id as u64, *size),
            )
        })
        .chain(rayon::iter::once((
            archives,
            recent,
            place(args.seed, archives, archives, recent),
        )))
        .collect();
    let archive_tries: Vec<u32> = {
        let mut tries: Vec<u32> = placed[..sizes.len()].iter().map(|p| p.2.tries).collect();
        tries.sort_unstable();
        tries
    };
    let overflowed: Vec<u64> = placed
        .iter()
        .filter(|p| p.2.segments > 1)
        .map(|p| p.0)
        .collect();

    // Per table size: how often the first seed fits, the tries kept, and the
    // time one seed's pass over every tag takes on one thread.
    let grid: Vec<Value> = args
        .sizes
        .iter()
        .map(|&size| {
            let tags = tags(args.seed ^ 0x7e57, size, size);
            let started = Instant::now();
            let one = place_bucket(&tags, size, 0, &TXID_2K8);
            let pass_s = started.elapsed().as_secs_f64() / f64::from(one.tries);
            let mut tries: Vec<u32> = (0..args.trials)
                .into_par_iter()
                .map(|t| place(args.seed ^ 0x5eed, size * 1_000_000 + t, t, size).tries)
                .collect();
            tries.sort_unstable();
            let first = tries.iter().filter(|t| **t == 1).count();
            json!({
                "entries": size,
                "fill": size as f64 / (2_048 * 72) as f64,
                "trials": args.trials,
                "first_seed_fit": first as f64 / args.trials as f64,
                "poisson_first_seed_fit": poisson_fit(size),
                "tries_p50": quantile(&tries, 0.5),
                "tries_p95": quantile(&tries, 0.95),
                "tries_max": tries.last(),
                "overflowed": tries.iter().filter(|t| **t == MAX_PLACEMENT_SEEDS).count(),
                "seed_pass_seconds": pass_s,
                "worst_placement_seconds": pass_s * f64::from(MAX_PLACEMENT_SEEDS),
            })
        })
        .collect();

    let report = json!({
        "schema": "txid-display-placement-census-v1",
        "geometry": TXID_2K8.name,
        "rows": TXID_2K8.directory_rows,
        "slots_per_row": slots_per_row(TXID_2K8.directory_row_bytes),
        "max_placement_seeds": MAX_PLACEMENT_SEEDS,
        "source_map": {
            "path": args.map,
            "sha256": hex::encode(Sha256::digest(std::fs::read(&args.map)?)),
            "schema": map["schema"],
            "archive_target": old_target,
            "archives": overshoot.len(),
            "start_height": map["start_height"],
            "tip": shards.last().and_then(|s| s["end_height"].as_u64()),
            "entries": total,
            "overshoot_max": overshoot.iter().max(),
            "overshoot_mean": overshoot.iter().sum::<u64>() as f64 / overshoot.len() as f64,
        },
        "target": args.target,
        "window": args.window,
        "tag_seed": args.seed,
        "archives": archives,
        "archive_entries_min": sizes.iter().min(),
        "archive_entries_max": sizes.iter().max(),
        "unsealed_entries_at_tip": left,
        "recent_entries": recent,
        "archive_seed_tries": {
            "p50": quantile(&archive_tries, 0.5),
            "p95": quantile(&archive_tries, 0.95),
            "max": archive_tries.last(),
            "first_seed": archive_tries.iter().filter(|t| **t == 1).count(),
        },
        "recent_seed_tries": placed.last().map(|p| p.2.tries),
        "max_row_load": placed.iter().map(|p| p.2.max_load).max(),
        "overflowed_tables": overflowed,
        "single_segment": overflowed.is_empty(),
        "tables": placed.iter().map(|(id, size, p)| json!({
            "shard_id": id, "entries": size, "seed": p.seed, "tries": p.tries,
            "max_load": p.max_load, "segments": p.segments,
            "recent": *id == archives,
        })).collect::<Vec<_>>(),
        "grid": grid,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
