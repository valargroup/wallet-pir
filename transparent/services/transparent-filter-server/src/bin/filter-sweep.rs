//! Encoded range-filter sizes under alternative Golomb-Rice parameters.
//!
//! Rebuilds each shard's exact filter element set from the journal, over the
//! shard boundaries a census printed, and encodes it under every requested
//! `(P, M)`. Sizes are exact for those element sets. A false match costs a
//! wallet one directory lookup; its rate per absent script tested against one
//! shard is `1 / M` by construction, so it is reported, not sampled.
//!
//! Read only: nothing is written but standard output.

use clap::Parser;
use std::collections::BTreeSet;
use std::path::PathBuf;
use transparent_filter::{build_range_filter_with, BlockHash, ScriptBytes, ShardKey};
use transparent_filter_server::events::EventStore;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "filter-sweep",
    about = "Range filter sizes under alternative P and M"
)]
struct Cli {
    #[arg(long)]
    data_dir: PathBuf,
    /// A census output; its `per_shard` rows give shard id and inclusive range.
    #[arg(long)]
    census: PathBuf,
    /// Parameters to encode under, as `P:M`, comma separated.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "19:784931,16:98304,14:24576,13:12288,12:4096,11:2048,10:1024,8:256"
    )]
    params: Vec<String>,
}

fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let params: Vec<(u8, u64)> = cli
        .params
        .iter()
        .map(|pair| {
            let (p, m) = pair.split_once(':').ok_or("expected P:M")?;
            Ok::<_, BoxError>((p.parse()?, m.parse()?))
        })
        .collect::<Result<_, _>>()?;
    let mut shards: Vec<(u64, u64, u64)> = Vec::new();
    for line in std::fs::read_to_string(&cli.census)?.lines() {
        let fields: Vec<&str> = line.trim_start().split('\t').collect();
        if fields.first() == Some(&"per_shard") && fields.len() > 3 {
            if let (Ok(id), Ok(start), Ok(end)) =
                (fields[1].parse(), fields[2].parse(), fields[3].parse())
            {
                if !shards.iter().any(|(seen, _, _)| *seen == id) {
                    shards.push((id, start, end));
                }
            }
        }
    }
    let store = EventStore::open_existing(&cli.data_dir)?;
    // Extend the last shard to the journal's end: it is the tail.
    let covered = store.covered_through().ok_or("empty journal")?;
    if let Some(last) = shards.last() {
        if last.2 < covered {
            let next = (last.0 + 1, last.2 + 1, covered);
            shards.push(next);
        }
    }
    let genesis = BlockHash::from_display_hex(store.genesis_hash())?;

    println!(
        "shard\tstart\tend\telements\t{}",
        params
            .iter()
            .map(|(p, m)| format!("P{p}M{m}"))
            .collect::<Vec<_>>()
            .join("\t")
    );
    let mut totals = vec![0u64; params.len()];
    let mut elements_total = 0u64;
    for (id, start, end) in &shards {
        let mut set: BTreeSet<Vec<u8>> = BTreeSet::new();
        for height in *start..=*end {
            for (script, _) in store.events_at(height)?.unwrap_or_default() {
                set.insert(script.as_slice().to_vec());
            }
        }
        let elements: Vec<ScriptBytes> = set.into_iter().map(ScriptBytes::new).collect();
        let terminal = store.block_at(*end).ok_or("uncovered end")?.block_hash;
        let key = ShardKey::derive(
            transparent_filter::RANGE_PROFILE,
            genesis,
            *id,
            *start,
            *end,
            terminal,
        );
        let mut row = Vec::new();
        for (index, (p, m)) in params.iter().enumerate() {
            let bytes = build_range_filter_with(key, &elements, *m, *p)?.len() as u64;
            totals[index] += bytes;
            row.push(bytes.to_string());
        }
        elements_total += elements.len() as u64;
        println!(
            "{id}\t{start}\t{end}\t{}\t{}",
            elements.len(),
            row.join("\t")
        );
    }
    println!(
        "total\t\t\t{elements_total}\t{}",
        totals
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join("\t")
    );
    for ((p, m), bytes) in params.iter().zip(&totals) {
        println!(
            "summary\tP={p}\tM={m}\tbytes={bytes}\tbits_per_element={:.3}\tfalse_match_per_absent_test={:.3e}",
            *bytes as f64 * 8.0 / elements_total.max(1) as f64,
            1.0 / *m as f64
        );
    }
    Ok(())
}
