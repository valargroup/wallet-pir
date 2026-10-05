//! Inventory of a txid display publication, from the publisher's own files.
//!
//! - `census`: re-verifies every segment, decodes every bucket row and counts
//!   distinct txids per (shard, bucket) and per (shard, bucket, page count),
//!   the anonymity classes a lookup falls into.
//! - `fixture`: samples txids per (tier, class) with their heights and the
//!   SHA-256 of each record's canonical encoding, plus absent controls, for
//!   `txid-rate` and `txid-bandwidth`.
//! - `audit`: checks a series of saved maps for sealed-shard immutability.
//! - `synth`: writes a synthetic multi-shard publication for local runs.
//!
//! Runs on the coordinator; it never contacts a worker.

use clap::{Parser, Subcommand};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use transparent_events::Txid;
use transparent_shard::display::{
    self, display_by_name, DisplayMap, DisplaySealParams, DisplayTable,
};
use transparent_shard::txid::{self, DirectoryEntry, ROW_BYTES};
use transparent_shard_server::display::set::{DisplayRevision, MAP_FILE};
use transparent_shard_server::display::synth::{self, ShardSpec};
use transparent_shard_server::display::tier;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "txid-inventory",
    about = "Inventory of a txid display publication"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Distinct txids per (shard, bucket) and per (shard, bucket, pages).
    Census {
        /// A candidate directory, or a root whose newest candidate is used.
        #[arg(long)]
        publication: PathBuf,
        /// Classes below this many txids are listed.
        #[arg(long, default_value_t = 10_000)]
        floor: u64,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Lookup samples per (tier, class) with exactness digests.
    Fixture {
        #[arg(long)]
        publication: PathBuf,
        /// The controller's `tooling/heights.bin`.
        #[arg(long)]
        heights: PathBuf,
        #[arg(long, default_value_t = 40)]
        per_class: usize,
        #[arg(long, default_value_t = 200)]
        absent: usize,
        #[arg(long, default_value_t = 0)]
        seed: u64,
        #[arg(long)]
        out: PathBuf,
    },
    /// Sealed-shard immutability across saved maps, taken in file-name order.
    Audit {
        #[arg(long)]
        maps: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// A synthetic publication: `shards - 1` sealed archives and a recent shard.
    Synth {
        #[arg(long)]
        records: usize,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 3)]
        shards: u64,
        #[arg(long, default_value_t = 1)]
        n_buckets: u32,
        #[arg(long, default_value = "txid-2k")]
        geometry: String,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 1_000_000)]
        start_height: u64,
        #[arg(long, default_value_t = 1_000)]
        blocks_per_shard: u64,
        /// Absolute id of the first shard; above zero, as after a window drop.
        #[arg(long, default_value_t = 0)]
        first_shard_id: u64,
    },
}

fn main() -> Result<(), Error> {
    match Cli::parse().command {
        Command::Census {
            publication,
            floor,
            out,
        } => {
            let report = census(&candidate_dir(&publication)?, floor)?;
            eprint!("{}", census_text(&report));
            write_json(out.as_deref(), &report)
        }
        Command::Fixture {
            publication,
            heights,
            per_class,
            absent,
            seed,
            out,
        } => {
            let report = fixture(
                &candidate_dir(&publication)?,
                &heights,
                per_class,
                absent,
                seed,
            )?;
            eprintln!(
                "fixture: {} samples, {} absent controls",
                report["samples"].as_array().map_or(0, Vec::len),
                report["absent"].as_array().map_or(0, Vec::len)
            );
            write_json(Some(&out), &report)
        }
        Command::Audit { maps, out } => {
            let report = audit(&maps)?;
            write_json(out.as_deref(), &report)?;
            if report["ok"] != true {
                eprintln!("audit found violations");
                std::process::exit(1);
            }
            Ok(())
        }
        Command::Synth {
            records,
            out,
            shards,
            n_buckets,
            geometry,
            seed,
            start_height,
            blocks_per_shard,
            first_shard_id,
        } => {
            let geometry = display_by_name(&geometry).ok_or("unknown display geometry")?;
            let report = synthesize(
                &out,
                records,
                shards,
                n_buckets,
                geometry,
                seed,
                start_height,
                blocks_per_shard,
                first_shard_id,
            )?;
            write_json(None, &report)
        }
    }
}

fn write_json(path: Option<&Path>, value: &serde_json::Value) -> Result<(), Error> {
    let text = serde_json::to_string_pretty(value)? + "\n";
    match path {
        Some(path) => std::fs::write(path, text)?,
        None => print!("{text}"),
    }
    Ok(())
}

/// `path` if it holds a map, else its newest `candidate-*` child that does.
fn candidate_dir(path: &Path) -> Result<PathBuf, Error> {
    if path.join(MAP_FILE).is_file() {
        return Ok(path.to_path_buf());
    }
    let mut candidates: Vec<(std::time::SystemTime, PathBuf)> = std::fs::read_dir(path)?
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_name().to_string_lossy().starts_with("candidate-")
                && e.path().join(MAP_FILE).is_file()
        })
        .filter_map(|e| Some((e.metadata().ok()?.modified().ok()?, e.path())))
        .collect();
    candidates.sort();
    candidates
        .pop()
        .map(|(_, path)| path)
        .ok_or_else(|| format!("no display candidate under {}", path.display()).into())
}

fn load_map(dir: &Path) -> Result<DisplayMap, Error> {
    let raw = std::fs::read(dir.join(MAP_FILE))?;
    let map: DisplayMap = serde_json::from_slice(&raw)?;
    map.check_shape()?;
    if map.to_bytes() != raw {
        return Err("the display map is not canonical".into());
    }
    Ok(map)
}

/// Reads one row of a held revision's table.
struct Rows {
    files: BTreeMap<(DisplayTable, usize), std::fs::File>,
}

impl Rows {
    fn open(revision: &DisplayRevision) -> Result<Self, Error> {
        let mut files = BTreeMap::new();
        for (table, segment) in revision.targets() {
            let source = revision.segment(table, segment).ok_or("table not held")?;
            files.insert(
                (table, segment as usize),
                std::fs::File::open(&source.path)?,
            );
        }
        Ok(Self { files })
    }

    fn row(&mut self, table: DisplayTable, segment: usize, row: u64) -> Result<Vec<u8>, Error> {
        let file = self
            .files
            .get_mut(&(table, segment))
            .ok_or("segment out of range")?;
        file.seek(SeekFrom::Start(row * ROW_BYTES as u64))?;
        let mut bytes = vec![0; ROW_BYTES];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    /// Every directory entry of bucket `bucket`, checked for membership.
    fn entries(
        &mut self,
        revision: &DisplayRevision,
        bucket: u32,
    ) -> Result<Vec<DirectoryEntry>, Error> {
        let table = DisplayTable::Directory(bucket);
        let mut out = Vec::new();
        for segment in 0..revision.segments(table).unwrap_or(0) as usize {
            for row in 0..revision.geometry.directory_rows {
                for entry in
                    display::row_entries(&self.row(table, segment, row)?).map_err(|e| e.0)?
                {
                    if display::bucket(&entry.txid, revision.manifest.n_buckets) != bucket {
                        return Err(format!(
                            "shard {} bucket {bucket} holds a txid of another bucket",
                            revision.manifest.shard_id
                        )
                        .into());
                    }
                    out.push(entry);
                }
            }
        }
        Ok(out)
    }

    fn assemble(
        &mut self,
        revision: &DisplayRevision,
        entry: &DirectoryEntry,
    ) -> Result<txid::TransparentDisplayRecord, Error> {
        let page_rows = revision.geometry.page_rows;
        let mut rows = Vec::new();
        if entry.pages > 0 {
            let first = u64::from(entry.first_page) - 1;
            for page in first..first + u64::from(entry.pages) {
                rows.push(self.row(
                    DisplayTable::Pages,
                    (page / page_rows) as usize,
                    page % page_rows,
                )?);
            }
        }
        Ok(txid::assemble(entry, &rows).map_err(|e| e.0)?)
    }
}

/// Every map entry's revision, verified in full: segment digests and rows.
fn revisions(dir: &Path, map: &DisplayMap) -> Result<Vec<DisplayRevision>, Error> {
    map.shards
        .iter()
        .map(|entry| {
            let revision = DisplayRevision::read(&dir.join(&entry.manifest_digest))?;
            if !entry.describes(&revision.manifest) {
                return Err(
                    format!("shard {} disagrees with its map entry", entry.shard_id).into(),
                );
            }
            Ok(revision.verify_tables()?)
        })
        .collect()
}

/// Payload sizes are binned so inline-cutoff what-ifs can be read off.
const PAYLOAD_EDGES: [u64; 15] = [
    64, 128, 192, 256, 384, 512, 768, 1_024, 2_048, 4_050, 8_100, 12_150, 16_200, 20_250, 40_500,
];
const INLINE_CUTOFFS: [u64; 6] = [128, 192, 256, 384, 512, 1_024];

fn census(dir: &Path, floor: u64) -> Result<serde_json::Value, Error> {
    let map = load_map(dir)?;
    let revisions = revisions(dir, &map)?;
    let mut shards = Vec::new();
    let mut classes: Vec<(u64, u32, u32, u64)> = Vec::new();
    let mut buckets_seen: Vec<(u64, u32, u64)> = Vec::new();
    let mut payloads = vec![0u64; PAYLOAD_EDGES.len() + 1];
    let mut inline_if = [0u64; INLINE_CUTOFFS.len()];
    let mut total = 0u64;
    for revision in &revisions {
        let manifest = &revision.manifest;
        let mut rows = Rows::open(revision)?;
        let mut buckets = Vec::new();
        for bucket in 0..manifest.n_buckets {
            let mut txids = BTreeSet::new();
            let mut by_pages: BTreeMap<u32, BTreeSet<[u8; 32]>> = BTreeMap::new();
            for entry in rows.entries(revision, bucket)? {
                txids.insert(entry.txid.0);
                by_pages
                    .entry(entry.pages)
                    .or_default()
                    .insert(entry.txid.0);
                let size = u64::from(entry.total);
                payloads[PAYLOAD_EDGES
                    .iter()
                    .take_while(|edge| size > **edge)
                    .count()] += 1;
                for (count, cutoff) in inline_if.iter_mut().zip(INLINE_CUTOFFS) {
                    *count += u64::from(size <= cutoff);
                }
            }
            let declared = &manifest.buckets[bucket as usize];
            let counted: BTreeMap<u32, u64> = by_pages
                .iter()
                .map(|(pages, ids)| (*pages, ids.len() as u64))
                .collect();
            if txids.len() as u64 != declared.records || counted != declared.page_histogram {
                return Err(format!(
                    "shard {} bucket {bucket} disagrees with its manifest",
                    manifest.shard_id
                )
                .into());
            }
            total += txids.len() as u64;
            buckets_seen.push((manifest.shard_id, bucket, txids.len() as u64));
            for (pages, count) in &counted {
                classes.push((manifest.shard_id, bucket, *pages, *count));
            }
            buckets.push(serde_json::json!({
                "bucket": bucket,
                "txids": txids.len(),
                "classes": counted.iter().map(|(p, c)| (p.to_string(), *c)).collect::<BTreeMap<_, _>>(),
            }));
        }
        shards.push(serde_json::json!({
            "shard_id": manifest.shard_id,
            "digest": revision.digest,
            "sealed": manifest.sealed,
            "tier": tier(manifest.sealed),
            "start_height": manifest.start_height,
            "end_height": manifest.end_height,
            "geometry": manifest.geometry,
            "records": manifest.records,
            "n_buckets": manifest.n_buckets,
            "directory_segments": manifest.buckets.iter().map(|b| b.directory_segments.len()).collect::<Vec<_>>(),
            "page_segments": manifest.page_segments.len(),
            "buckets": buckets,
        }));
    }
    let class_json = |(shard_id, bucket, pages, txids): &(u64, u32, u32, u64)| serde_json::json!({"shard_id": shard_id, "bucket": bucket, "pages": pages, "txids": txids});
    let min_bucket = buckets_seen.iter().min_by_key(|(_, _, n)| *n);
    let min_class = classes.iter().min_by_key(|c| c.3);
    let small: Vec<_> = classes.iter().filter(|c| c.3 < floor).collect();
    let mut cumulative = 0u64;
    let histogram: Vec<_> = payloads
        .iter()
        .enumerate()
        .map(|(index, count)| {
            cumulative += count;
            serde_json::json!({
                "le": PAYLOAD_EDGES.get(index).copied(),
                "records": count,
                "cumulative": cumulative,
            })
        })
        .collect();
    Ok(serde_json::json!({
        "map_sha256": map.sha256(),
        "candidate": dir,
        "floor": floor,
        "shards": shards,
        "summary": {
            "total_txids": total,
            "min_shard_bucket": min_bucket.map(|(s, b, n)| serde_json::json!({"shard_id": s, "bucket": b, "txids": n})),
            "min_class": min_class.map(class_json),
            "classes_below_floor": small.iter().map(|c| class_json(c)).collect::<Vec<_>>(),
            "txids_in_small_classes": small.iter().map(|c| c.3).sum::<u64>(),
            "recent_min_bucket": buckets_seen
                .iter()
                .filter(|(s, _, _)| map.shards.last().is_some_and(|r| r.shard_id == *s))
                .map(|(_, _, n)| *n)
                .min(),
        },
        "payload": {
            "histogram": histogram,
            "inline_if": INLINE_CUTOFFS
                .iter()
                .zip(inline_if)
                .map(|(cutoff, records)| serde_json::json!({
                    "cutoff": cutoff,
                    "inline_records": records,
                    "share": records as f64 / total.max(1) as f64,
                }))
                .collect::<Vec<_>>(),
        },
    }))
}

fn census_text(report: &serde_json::Value) -> String {
    let summary = &report["summary"];
    let mut text = format!(
        "census {}: {} txids in {} shards\n  smallest (shard, bucket): {}\n  smallest (shard, bucket, pages) class: {}\n  classes below {}: {} holding {} txids\n",
        report["map_sha256"].as_str().unwrap_or_default(),
        summary["total_txids"],
        report["shards"].as_array().map_or(0, Vec::len),
        summary["min_shard_bucket"],
        summary["min_class"],
        report["floor"],
        summary["classes_below_floor"].as_array().map_or(0, Vec::len),
        summary["txids_in_small_classes"],
    );
    for shard in report["shards"].as_array().into_iter().flatten() {
        for bucket in shard["buckets"].as_array().into_iter().flatten() {
            text.push_str(&format!(
                "  shard {} ({}) bucket {}: {} txids, classes {}\n",
                shard["shard_id"],
                shard["tier"].as_str().unwrap_or_default(),
                bucket["bucket"],
                bucket["txids"],
                bucket["classes"]
            ));
        }
    }
    text
}

fn class_label(pages: u32) -> String {
    if pages == 0 {
        "inline".into()
    } else {
        format!("pages-{pages}")
    }
}

/// A fixture candidate: its sort key, revision index, bucket and entry.
type Candidate = ([u8; 32], usize, u32, DirectoryEntry);

fn fixture(
    dir: &Path,
    heights: &Path,
    per_class: usize,
    absent: usize,
    seed: u64,
) -> Result<serde_json::Value, Error> {
    let map = load_map(dir)?;
    let revisions = revisions(dir, &map)?;
    let heights: HashMap<[u8; 32], u64> = synth::read_heights(heights)?
        .into_iter()
        .map(|(txid, height)| (txid.0, height))
        .collect();
    let order = |txid: &[u8; 32]| -> [u8; 32] {
        Sha256::new()
            .chain_update(seed.to_le_bytes())
            .chain_update(txid)
            .finalize()
            .into()
    };
    // (tier, class) -> candidates in a seed-determined order.
    let mut pools: BTreeMap<(String, String), Vec<Candidate>> = BTreeMap::new();
    let mut every = BTreeSet::new();
    let mut unplaced = 0u64;
    for (index, revision) in revisions.iter().enumerate() {
        let mut rows = Rows::open(revision)?;
        for bucket in 0..revision.manifest.n_buckets {
            for entry in rows.entries(revision, bucket)? {
                every.insert(entry.txid.0);
                let Some(height) = heights.get(&entry.txid.0) else {
                    unplaced += 1;
                    continue;
                };
                if !(revision.manifest.start_height..=revision.manifest.end_height).contains(height)
                {
                    return Err(format!(
                        "txid {} is in shard {} but indexed at height {height}",
                        entry.txid.to_display_hex(),
                        revision.manifest.shard_id
                    )
                    .into());
                }
                pools
                    .entry((
                        tier(revision.manifest.sealed).to_string(),
                        class_label(entry.pages),
                    ))
                    .or_default()
                    .push((order(&entry.txid.0), index, bucket, entry));
            }
        }
    }
    let mut samples = Vec::new();
    let mut available = BTreeMap::new();
    for ((tier, class), pool) in &mut pools {
        available.insert(format!("{tier}/{class}"), pool.len());
        pool.sort_by_key(|candidate| candidate.0);
        for (_, index, bucket, entry) in pool.iter().take(per_class) {
            let revision = &revisions[*index];
            let mut rows = Rows::open(revision)?;
            let record = rows.assemble(revision, entry)?;
            if record.txid != entry.txid {
                return Err("assembled another transaction".into());
            }
            samples.push(serde_json::json!({
                "txid": entry.txid.to_display_hex(),
                "height": heights[&entry.txid.0],
                "shard_id": revision.manifest.shard_id,
                "tier": tier,
                "digest": revision.digest,
                "bucket": bucket,
                "class": class,
                "pages": entry.pages,
                "record_sha256": hex::encode(Sha256::digest(record.encode().map_err(|e| e.0)?)),
                "expect": "found",
            }));
        }
    }
    // Absent controls: unknown txids anywhere in range, and real txids asked
    // at a height in another shard, which must not find them.
    let first = map.start_height;
    let span = map.covered_through().unwrap_or(first) - first + 1;
    let mut controls = Vec::new();
    for i in 0..absent as u64 {
        let draw: [u8; 32] = Sha256::new()
            .chain_update(b"txid-display/absent")
            .chain_update(seed.to_le_bytes())
            .chain_update(i.to_le_bytes())
            .finalize()
            .into();
        let height = first + u64::from_le_bytes(draw[..8].try_into().unwrap()) % span;
        let wrong_shard = i % 2 == 1 && map.shards.len() > 1 && !samples.is_empty();
        let (txid, kind, height) = if wrong_shard {
            let sample = &samples[(i as usize / 2) % samples.len()];
            let own = sample["shard_id"].as_u64().unwrap_or_default();
            let other = map
                .shards
                .iter()
                .cycle()
                .skip(i as usize)
                .find(|s| s.shard_id != own)
                .expect("two shards");
            let txid = sample["txid"].as_str().unwrap_or_default().to_string();
            (
                txid,
                "wrong-shard",
                other.start_height + height % (other.end_height - other.start_height + 1),
            )
        } else {
            if every.contains(&draw) {
                continue;
            }
            (Txid(draw).to_display_hex(), "random", height)
        };
        let shard = map
            .shard_for_height(height)
            .ok_or("height outside the map")?;
        controls.push(serde_json::json!({
            "txid": txid,
            "height": height,
            "shard_id": shard.shard_id,
            "tier": shard.tier(),
            "kind": kind,
            "class": "absent",
            "expect": "absent",
        }));
    }
    Ok(serde_json::json!({
        "schema": "transparent-txid-display-fixture-v1",
        "map_sha256": map.sha256(),
        "candidate": dir,
        "seed": seed,
        "per_class": per_class,
        "available": available,
        "unplaced_txids": unplaced,
        "samples": samples,
        "absent": controls,
    }))
}

fn audit(dir: &Path) -> Result<serde_json::Value, Error> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort();
    let mut violations = Vec::new();
    let mut sealed: BTreeMap<u64, (String, u64, u64)> = BTreeMap::new();
    let mut seals = Vec::new();
    let mut drops = Vec::new();
    let mut previous: Option<DisplayMap> = None;
    let mut maps = 0usize;
    for file in &files {
        let name = file.display().to_string();
        let map: DisplayMap = match std::fs::read(file)
            .map_err(Error::from)
            .and_then(|raw| Ok(serde_json::from_slice(&raw)?))
        {
            Ok(map) => map,
            Err(error) => {
                violations.push(format!("{name}: unreadable map: {error}"));
                continue;
            }
        };
        maps += 1;
        if let Err(error) = map.check_shape() {
            violations.push(format!("{name}: {error}"));
            continue;
        }
        if let Some(previous) = &previous {
            if map.first_shard_id < previous.first_shard_id {
                violations.push(format!("{name}: the window moved backwards"));
            }
            for id in previous.first_shard_id..map.first_shard_id {
                match previous.shard(id) {
                    Some(dropped) if dropped.sealed => drops.push(serde_json::json!({
                        "map": name, "shard_id": id, "digest": dropped.manifest_digest,
                    })),
                    _ => violations.push(format!("{name}: dropped shard {id} was never sealed")),
                }
            }
        }
        for entry in map.shards.iter().filter(|e| e.sealed) {
            let identity = (
                entry.manifest_digest.clone(),
                entry.start_height,
                entry.end_height,
            );
            match sealed.get(&entry.shard_id) {
                Some(seen) if *seen != identity => violations.push(format!(
                    "{name}: sealed shard {} changed from {:?} to {:?}",
                    entry.shard_id, seen, identity
                )),
                Some(_) => {}
                None => {
                    seals.push(serde_json::json!({
                        "map": name, "shard_id": entry.shard_id, "digest": entry.manifest_digest,
                        "start_height": entry.start_height, "end_height": entry.end_height,
                    }));
                    sealed.insert(entry.shard_id, identity);
                }
            }
        }
        // The sealed prefix only grows, apart from the window's front.
        for id in sealed.keys().filter(|id| **id >= map.first_shard_id) {
            if !map.shard(*id).is_some_and(|e| e.sealed) {
                violations.push(format!("{name}: sealed shard {id} is no longer sealed"));
            }
        }
        previous = Some(map);
    }
    Ok(serde_json::json!({
        "ok": violations.is_empty(),
        "maps": maps,
        "files": files,
        "sealed": sealed
            .iter()
            .map(|(id, (digest, start, end))| (id.to_string(), serde_json::json!({"digest": digest, "start_height": start, "end_height": end})))
            .collect::<BTreeMap<_, _>>(),
        "seals": seals,
        "drops": drops,
        "violations": violations,
    }))
}

#[allow(clippy::too_many_arguments)]
fn synthesize(
    out: &Path,
    records: usize,
    shards: u64,
    n_buckets: u32,
    geometry: &'static transparent_shard::layout::Geometry,
    seed: u64,
    start_height: u64,
    blocks_per_shard: u64,
    first_shard_id: u64,
) -> Result<serde_json::Value, Error> {
    if shards == 0 || records < shards as usize || blocks_per_shard == 0 {
        return Err("need at least one record per shard and one block per shard".into());
    }
    let all = synth::records(records, seed);
    let per_shard = records / shards as usize;
    let mut parts = Vec::new();
    for index in 0..shards as usize {
        let end = if index + 1 == shards as usize {
            records
        } else {
            (index + 1) * per_shard
        };
        parts.push(&all[index * per_shard..end]);
    }
    let bucket_min = |part: &[txid::TransparentDisplayRecord]| {
        let mut counts = vec![0u64; n_buckets as usize];
        for record in part {
            counts[display::bucket(&record.txid, n_buckets) as usize] += 1;
        }
        counts.into_iter().min().unwrap_or(0)
    };
    let archive_target = parts[..parts.len() - 1]
        .iter()
        .map(|part| bucket_min(part))
        .min()
        .unwrap_or(1)
        .max(1);
    let params = DisplaySealParams {
        n_archive: n_buckets,
        n_recent: n_buckets,
        archive_target,
        recent_floor: bucket_min(parts[parts.len() - 1]),
        reorg_margin: 100,
    };
    let mut published: Vec<synth::Published> = Vec::new();
    let mut index = Vec::new();
    for (i, part) in parts.iter().enumerate() {
        let start = start_height + i as u64 * blocks_per_shard;
        let end = start + blocks_per_shard - 1;
        let shard_id = first_shard_id + i as u64;
        let parent = match published.last() {
            Some(previous) => previous.digest.clone(),
            // A window that starts above zero chains to a dropped archive.
            None if shard_id > 0 => hex::encode(Sha256::digest(b"txid-display/dropped")),
            None => String::new(),
        };
        let spec = ShardSpec {
            shard_id,
            start_height: start,
            end_height: end,
            sealed: i + 1 < parts.len(),
            revision: 0,
            supersedes: String::new(),
            parent_manifest_digest: parent,
            n_buckets,
            archive_target,
            geometry,
        };
        published.push(synth::write_shard(out, &spec, part)?);
        for (j, record) in part.iter().enumerate() {
            index.push((
                record.txid,
                start + (j as u64 * blocks_per_shard) / part.len() as u64,
            ));
        }
    }
    let tip = published.last().expect("one shard").manifest.end_height;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let label = format!("{tip}-{}-{nanos}", &synth::block_hash(tip)[..16]);
    let (candidate, map_sha256) = synth::write_candidate(out, &params, &published, &label)?;
    let heights = out.join("tooling/heights.bin");
    let _ = std::fs::remove_file(&heights);
    synth::append_heights(&heights, &index)?;
    Ok(serde_json::json!({
        "candidate": candidate,
        "map_sha256": map_sha256,
        "heights": heights,
        "seal": params,
        "shards": published.iter().map(|p| serde_json::json!({
            "shard_id": p.manifest.shard_id,
            "digest": p.digest,
            "sealed": p.manifest.sealed,
            "start_height": p.manifest.start_height,
            "end_height": p.manifest.end_height,
            "records": p.manifest.records,
            "min_bucket_records": p.manifest.min_bucket_records(),
        })).collect::<Vec<_>>(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synth_census_fixture_and_audit_agree() {
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("publication");
        let report = synthesize(&out, 600, 3, 2, &display::TXID_2K, 5, 100, 50, 4).unwrap();
        let candidate = PathBuf::from(report["candidate"].as_str().unwrap());
        assert_eq!(candidate_dir(&out).unwrap(), candidate);

        let census = census(&candidate, 10_000).unwrap();
        assert_eq!(census["summary"]["total_txids"], 600);
        assert_eq!(census["shards"].as_array().unwrap().len(), 3);
        assert_eq!(census["shards"][0]["shard_id"], 4);
        assert!(!census["summary"]["classes_below_floor"]
            .as_array()
            .unwrap()
            .is_empty());

        let fixture = fixture(&candidate, &out.join("tooling/heights.bin"), 2, 6, 0).unwrap();
        let samples = fixture["samples"].as_array().unwrap();
        assert!(samples
            .iter()
            .any(|s| s["tier"] == "archive" && s["class"] == "inline"));
        assert!(samples.iter().any(|s| s["tier"] == "recent"));
        assert!(samples
            .iter()
            .all(|s| s["record_sha256"].as_str().unwrap().len() == 64));
        let absent = fixture["absent"].as_array().unwrap();
        assert!(absent.iter().any(|a| a["kind"] == "wrong-shard"));
        assert!(absent.iter().all(|a| a["expect"] == "absent"));

        // Audit: a later map that reseals shard 4 differently is a violation.
        let maps = root.path().join("maps");
        std::fs::create_dir_all(&maps).unwrap();
        let map = load_map(&candidate).unwrap();
        std::fs::write(maps.join("0001.json"), map.to_bytes()).unwrap();
        let mut dropped = map.clone();
        dropped.shards.remove(0);
        dropped.first_shard_id += 1;
        dropped.start_height = dropped.shards[0].start_height;
        std::fs::write(maps.join("0002.json"), dropped.to_bytes()).unwrap();
        let clean = audit(&maps).unwrap();
        assert_eq!(clean["ok"], true, "{clean}");
        assert_eq!(clean["drops"].as_array().unwrap().len(), 1);
        let mut changed = dropped.clone();
        changed.shards[0].manifest_digest = "99".repeat(32);
        std::fs::write(maps.join("0003.json"), changed.to_bytes()).unwrap();
        let report = audit(&maps).unwrap();
        assert_eq!(report["ok"], false);
    }
}
