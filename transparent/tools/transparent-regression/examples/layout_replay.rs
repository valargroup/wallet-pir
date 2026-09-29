//! Read-only, coverage-matched v10 census against a recorded v9 shard map.
//! Usage: cargo run --release -p transparent-regression --example layout_replay -- JOURNAL V9_MAP
//! Reuses the independent committed-prefix reader; never opens a writer lock.
#[path = "../src/journal.rs"]
mod journal;
use anyhow::{bail, Context, Result};
use std::{fs, time::Instant};
use transparent_events::TransparentEvent;
use transparent_filter::ShardMap;
use transparent_shard::{
    geometry_by_name, layout::segments_for, seal::PageBasis, SealPolicy, SealedShard, Sealer,
};
fn height(event: &TransparentEvent) -> u64 {
    u64::from(event.height())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        bail!("usage: layout_replay JOURNAL V9_MAP");
    }
    let journal = journal::EventStore::open_existing(&args[1])?;
    let map: ShardMap = serde_json::from_slice(&fs::read(&args[2])?)?;
    map.check_shape().map_err(anyhow::Error::msg)?;
    if map.genesis_hash != journal.genesis_hash() {
        bail!("genesis mismatch");
    }
    let first = map
        .shards
        .iter()
        .position(|s| s.start_height == journal.start_height())
        .context("journal start is not a recorded shard boundary")?;
    let last = map
        .shards
        .iter()
        .rposition(|s| Some(s.end_height) <= journal.covered_through())
        .context("journal does not cover a recorded shard")?;
    if first > last {
        bail!("no matched coverage");
    }
    let end = map.shards[last].end_height;
    for shard in &map.shards[first..=last] {
        if journal
            .block_at(shard.end_height)
            .context("missing terminal block")?
            .block_hash
            .to_display_hex()
            != shard.terminal_block_hash
        {
            bail!("journal disagrees with recorded shard {}", shard.shard_id);
        }
    }
    let baseline_bytes: u64 = map.shards[first..=last]
        .iter()
        .map(|s| {
            let g = geometry_by_name(&s.geometry).expect("recorded geometry");
            u64::from(s.directory_segments) * g.directory_bytes_per_segment()
                + u64::from(s.page_segments) * g.page_bytes_per_segment()
        })
        .sum();
    let started = Instant::now();
    let mut geometry = geometry_by_name(&map.shards[first].geometry).context("unknown geometry")?;
    let mut sealer = Sealer::resume(
        SealPolicy::for_geometry(geometry),
        journal.start_height(),
        PageBasis::Packed,
        *geometry,
        map.shards[first].shard_id,
    );
    sealer.measure_placement(true);
    let mut index = first;
    let mut bytes = 0u64;
    let mut shards = 0u64;
    let mut total_events = 0u64;
    let mut emit = |shard: SealedShard, geometry: &transparent_shard::Geometry| {
        let directory = shard.placement.expect("placement requested");
        let pages = segments_for(shard.occupancy.packed_page_rows, geometry.page_rows);
        bytes += u64::from(directory.segments) * geometry.directory_bytes_per_segment()
            + u64::from(pages) * geometry.page_bytes_per_segment();
        shards += 1;
        println!(
            "{}",
            serde_json::json!({"type":"shard", "id":shard.shard_id, "geometry":geometry.name,
            "start":shard.start_height, "end":shard.end_height, "scripts":shard.occupancy.scripts,
            "events":shard.occupancy.events, "page_rows":shard.occupancy.packed_page_rows,
            "directory_bytes":shard.occupancy.directory_bytes, "directory_segments":directory.segments,
            "page_segments":pages, "max_directory_row_bytes":directory.max_row_bytes})
        );
    };
    for h in journal.start_height()..=end {
        while h > map.shards[index].end_height {
            index += 1;
        }
        let next = geometry_by_name(&map.shards[index].geometry).context("unknown geometry")?;
        if next.name != geometry.name {
            if let Some(shard) = sealer.seal_at_geometry_change() {
                emit(shard, geometry);
            }
            let id = sealer.next_shard_id();
            geometry = next;
            sealer = Sealer::resume(
                SealPolicy::for_geometry(geometry),
                h,
                PageBasis::Packed,
                *geometry,
                id,
            );
            sealer.measure_placement(true);
        }
        let events = journal.events_at(h)?.context("missing block")?;
        total_events += events.len() as u64;
        for shard in sealer.push_block(h, &events)? {
            emit(shard, geometry);
        }
    }
    if let Some(shard) = sealer.finish() {
        emit(shard, geometry);
    }
    println!(
        "{}",
        serde_json::json!({"type":"summary", "schema":transparent_shard::SCHEMA,
        "classification":"coverage-matched read-only census; exact directory placement and shared page-demand calculation; not a publication or throughput benchmark",
        "start":journal.start_height(), "end":end, "events":total_events,
        "v9_shards":last-first+1, "v10_shards":shards, "v9_allocated_plaintext_bytes":baseline_bytes,
        "v10_allocated_plaintext_bytes":bytes, "capacity_gain_percent":(baseline_bytes as f64/bytes as f64-1.0)*100.0,
        "elapsed_seconds":started.elapsed().as_secs_f64()})
    );
    Ok(())
}
