//! Inventory of a txid display publication, from the publisher's own files.
//!
//! - `census`: re-verifies every segment, decodes every bucket row and counts
//!   distinct entries (by tag) per (shard, bucket), the anonymity class a
//!   lookup falls into, and how many entries are complete and what the rest
//!   leave out.
//! - `fixture`: samples txids per (tier, class) with their heights and the
//!   SHA-256 of each entry's canonical encoding, plus absent controls, for
//!   `txid-rate` and `txid-bandwidth`. Tables hold tags, not txids, so the
//!   txids come from the controller's heights index.
//! - `audit`: checks a series of saved maps for sealed-shard immutability.
//! - `synth`: writes a synthetic multi-shard publication for local runs.
//!
//! Runs on the coordinator; it never contacts a worker.

use clap::{Parser, Subcommand};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use transparent_events::Txid;
use transparent_shard::display::{
    self, display_by_name, DisplayMap, DisplaySealParams, DisplayTable,
};
use transparent_shard::txid::{self, AddressKind, DisplayEntry, Tag, ROW_BYTES};
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
    /// Distinct entries per (shard, bucket), and their omissions.
    Census {
        /// A candidate directory, or a root whose newest candidate is used.
        #[arg(long)]
        publication: PathBuf,
        /// (shard, bucket) classes below this many entries are listed.
        #[arg(long, default_value_t = 10_000)]
        floor: u64,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Lookup samples per (tier, class) with exactness digests. The class is
    /// `complete` or `omission`.
    Fixture {
        #[arg(long)]
        publication: PathBuf,
        /// The controller's `tooling/heights.bin`.
        #[arg(long)]
        heights: PathBuf,
        #[arg(long, default_value_t = 40)]
        per_class: usize,
        /// Instead of `per_class`, this many txids per tier drawn uniformly
        /// over every class, so a load follows the chain's own class mix.
        #[arg(long)]
        natural: Option<usize>,
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
        /// `mapwatch.jsonl`, whose `map` events give the observed order.
        /// Without it, maps are ordered by their own coverage.
        #[arg(long)]
        order: Option<PathBuf>,
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
            natural,
            absent,
            seed,
            out,
        } => {
            let report = fixture(
                &candidate_dir(&publication)?,
                &heights,
                per_class,
                natural,
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
        Command::Audit { maps, order, out } => {
            let report = audit(&maps, order.as_deref())?;
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

    /// Every entry of bucket `bucket`, checked for membership.
    fn entries(
        &mut self,
        revision: &DisplayRevision,
        bucket: u32,
    ) -> Result<Vec<DisplayEntry>, Error> {
        let table = DisplayTable::Directory(bucket);
        let mut out = Vec::new();
        for segment in 0..revision.segments(table).unwrap_or(0) as usize {
            for row in 0..revision.geometry.directory_rows {
                for entry in
                    display::row_entries(&self.row(table, segment, row)?).map_err(|e| e.0)?
                {
                    if display::bucket(&entry.tag, revision.manifest.n_buckets) != bucket {
                        return Err(format!(
                            "shard {} bucket {bucket} holds an entry of another bucket",
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

    /// What a client's two queries find for `txid` in this revision: both
    /// candidate rows of its bucket, in every segment of that bucket's table.
    fn lookup(
        &mut self,
        revision: &DisplayRevision,
        txid: &Txid,
    ) -> Result<Option<DisplayEntry>, Error> {
        let tag = Tag::of(txid);
        let bucket = display::bucket(&tag, revision.manifest.n_buckets);
        let table = DisplayTable::Directory(bucket);
        let mut rows = Vec::new();
        for row in display::candidate_rows(
            &tag,
            revision.manifest.shard_id,
            bucket,
            revision.geometry.directory_rows,
        ) {
            for segment in 0..revision.segments(table).unwrap_or(0) as usize {
                rows.push(self.row(table, segment, row)?);
            }
        }
        Ok(txid::find_entry(&rows, txid).map_err(|e| e.0)?)
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

/// The omissions `entry` names, in a fixed order: empty exactly when the
/// entry is complete.
fn omissions(entry: &DisplayEntry) -> Vec<&'static str> {
    [
        (entry.multiple_source_scripts, "multiple_source_scripts"),
        (entry.more_than_two_outputs(), "more_than_two_outputs"),
        (
            entry.shielded_and_transparent_funding,
            "shielded_and_transparent_funding",
        ),
        (entry.source.kind == AddressKind::Other, "source_other"),
        (
            entry
                .outputs
                .iter()
                .flatten()
                .any(|output| output.address.kind == AddressKind::Other),
            "output_other",
        ),
    ]
    .into_iter()
    .filter_map(|(named, omission)| named.then_some(omission))
    .collect()
}

/// How many entries hold every fact a wallet displays, and what the others
/// leave out. An entry may name several omissions, so the omission counts
/// can add up to more than `omitted`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
struct EntryCounts {
    entries: u64,
    complete: u64,
    /// Entries naming at least one omission.
    omitted: u64,
    /// The inputs spend two or more distinct scripts.
    multiple_source_scripts: u64,
    more_than_two_outputs: u64,
    /// Transparent inputs, and the shielded pools also paid in.
    shielded_and_transparent_funding: u64,
    /// Inputs exist but none spends an address-shaped script.
    source_other: u64,
    /// At least one held output has no address.
    output_other: u64,
    /// Neither a transparent input nor coinbase: paid from the shielded
    /// pools. Complete facts, not an omission.
    unshield: u64,
    coinbase: u64,
}

impl EntryCounts {
    fn add(&mut self, entry: &DisplayEntry) -> Result<(), Error> {
        let named = omissions(entry);
        // The codec decides completeness; every omission it knows must be
        // one this census counts.
        if named.is_empty() != entry.is_complete() {
            return Err("an entry's completeness disagrees with the omissions counted".into());
        }
        self.entries += 1;
        if named.is_empty() {
            self.complete += 1;
        } else {
            self.omitted += 1;
        }
        for omission in named {
            *match omission {
                "multiple_source_scripts" => &mut self.multiple_source_scripts,
                "more_than_two_outputs" => &mut self.more_than_two_outputs,
                "shielded_and_transparent_funding" => &mut self.shielded_and_transparent_funding,
                "source_other" => &mut self.source_other,
                "output_other" => &mut self.output_other,
                other => unreachable!("omission {other} has no counter"),
            } += 1;
        }
        self.unshield += u64::from(entry.input_count == 0 && !entry.coinbase);
        self.coinbase += u64::from(entry.coinbase);
        Ok(())
    }

    fn merge(&mut self, other: &Self) {
        self.entries += other.entries;
        self.complete += other.complete;
        self.omitted += other.omitted;
        self.multiple_source_scripts += other.multiple_source_scripts;
        self.more_than_two_outputs += other.more_than_two_outputs;
        self.shielded_and_transparent_funding += other.shielded_and_transparent_funding;
        self.source_other += other.source_other;
        self.output_other += other.output_other;
        self.unshield += other.unshield;
        self.coinbase += other.coinbase;
    }
}

fn census(dir: &Path, floor: u64) -> Result<serde_json::Value, Error> {
    let map = load_map(dir)?;
    let revisions = revisions(dir, &map)?;
    let mut shards = Vec::new();
    // (shard, bucket, entries): a lookup's anonymity class.
    let mut buckets_seen: Vec<(u64, u32, u64)> = Vec::new();
    let mut overall = EntryCounts::default();
    let mut total = 0u64;
    for revision in &revisions {
        let manifest = &revision.manifest;
        let mut rows = Rows::open(revision)?;
        let mut buckets = Vec::new();
        let mut counts = EntryCounts::default();
        for bucket in 0..manifest.n_buckets {
            let mut tags = BTreeSet::new();
            for entry in rows.entries(revision, bucket)? {
                tags.insert(entry.tag);
                counts.add(&entry)?;
            }
            if tags.len() as u64 != manifest.buckets[bucket as usize].records {
                return Err(format!(
                    "shard {} bucket {bucket} disagrees with its manifest",
                    manifest.shard_id
                )
                .into());
            }
            total += tags.len() as u64;
            buckets_seen.push((manifest.shard_id, bucket, tags.len() as u64));
            buckets.push(serde_json::json!({
                "bucket": bucket,
                "txids": tags.len(),
            }));
        }
        if counts.entries != manifest.records {
            return Err(format!(
                "shard {} holds {} entries but its manifest declares {}",
                manifest.shard_id, counts.entries, manifest.records
            )
            .into());
        }
        overall.merge(&counts);
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
            "buckets": buckets,
            "entries": counts,
        }));
    }
    let bucket_json = |(shard_id, bucket, txids): &(u64, u32, u64)| serde_json::json!({"shard_id": shard_id, "bucket": bucket, "txids": txids});
    let min_bucket = buckets_seen.iter().min_by_key(|(_, _, n)| *n);
    let small: Vec<_> = buckets_seen.iter().filter(|b| b.2 < floor).collect();
    Ok(serde_json::json!({
        "map_sha256": map.sha256(),
        "candidate": dir,
        "floor": floor,
        "shards": shards,
        "summary": {
            "total_txids": total,
            "min_shard_bucket": min_bucket.map(bucket_json),
            "buckets_below_floor": small.iter().map(|b| bucket_json(b)).collect::<Vec<_>>(),
            "txids_in_small_buckets": small.iter().map(|b| b.2).sum::<u64>(),
            "recent_min_bucket": buckets_seen
                .iter()
                .filter(|(s, _, _)| map.shards.last().is_some_and(|r| r.shard_id == *s))
                .map(|(_, _, n)| *n)
                .min(),
            "entries": overall,
        },
    }))
}

/// One line of entry counts, from their JSON.
fn counts_text(counts: &serde_json::Value) -> String {
    let percent = 100.0 * counts["complete"].as_f64().unwrap_or(0.0)
        / counts["entries"].as_f64().unwrap_or(0.0).max(1.0);
    format!(
        "{} of {} complete ({percent:.2}%); omitted {}: multiple_source_scripts {}, more_than_two_outputs {}, shielded_and_transparent_funding {}, source_other {}, output_other {}; unshield {}, coinbase {}",
        counts["complete"],
        counts["entries"],
        counts["omitted"],
        counts["multiple_source_scripts"],
        counts["more_than_two_outputs"],
        counts["shielded_and_transparent_funding"],
        counts["source_other"],
        counts["output_other"],
        counts["unshield"],
        counts["coinbase"],
    )
}

fn census_text(report: &serde_json::Value) -> String {
    let summary = &report["summary"];
    let mut text = format!(
        "census {}: {} txids in {} shards\n  smallest (shard, bucket): {}\n  (shard, bucket) classes below {}: {} holding {} txids\n  entries: {}\n",
        report["map_sha256"].as_str().unwrap_or_default(),
        summary["total_txids"],
        report["shards"].as_array().map_or(0, Vec::len),
        summary["min_shard_bucket"],
        report["floor"],
        summary["buckets_below_floor"].as_array().map_or(0, Vec::len),
        summary["txids_in_small_buckets"],
        counts_text(&summary["entries"]),
    );
    for shard in report["shards"].as_array().into_iter().flatten() {
        let tier = shard["tier"].as_str().unwrap_or_default();
        text.push_str(&format!(
            "  shard {} ({tier}) entries: {}\n",
            shard["shard_id"],
            counts_text(&shard["entries"])
        ));
        for bucket in shard["buckets"].as_array().into_iter().flatten() {
            text.push_str(&format!(
                "  shard {} ({tier}) bucket {}: {} txids\n",
                shard["shard_id"], bucket["bucket"], bucket["txids"]
            ));
        }
    }
    text
}

/// A sample's class: whether its entry holds every displayed fact.
fn class_label(entry: &DisplayEntry) -> &'static str {
    if entry.is_complete() {
        "complete"
    } else {
        "omission"
    }
}

/// SHA-256 of an entry's canonical encoding: the exactness oracle a fixture
/// carries.
fn entry_sha256(entry: &DisplayEntry) -> Result<String, Error> {
    Ok(hex::encode(Sha256::digest(
        entry.encode().map_err(|e| e.0)?,
    )))
}

/// A fixture candidate.
struct Candidate {
    /// Seed-determined sort key.
    order: [u8; 32],
    /// Index into the map's revisions.
    revision: usize,
    bucket: u32,
    txid: Txid,
    height: u64,
    entry: DisplayEntry,
}

fn fixture(
    dir: &Path,
    heights: &Path,
    per_class: usize,
    natural: Option<usize>,
    absent: usize,
    seed: u64,
) -> Result<serde_json::Value, Error> {
    let map = load_map(dir)?;
    let revisions = revisions(dir, &map)?;
    // Tables hold tags; the index names the txid and height behind each.
    let mut by_tag: HashMap<Tag, (Txid, u64)> = HashMap::new();
    for (txid, height) in synth::read_heights(heights)? {
        if let Some((other, _)) = by_tag.insert(Tag::of(&txid), (txid, height)) {
            if other != txid {
                return Err(format!(
                    "txids {} and {} share a tag",
                    other.to_display_hex(),
                    txid.to_display_hex()
                )
                .into());
            }
        }
    }
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
                every.insert(entry.tag);
                let Some(&(txid, height)) = by_tag.get(&entry.tag) else {
                    unplaced += 1;
                    continue;
                };
                if !(revision.manifest.start_height..=revision.manifest.end_height)
                    .contains(&height)
                {
                    return Err(format!(
                        "txid {} is in shard {} but indexed at height {height}",
                        txid.to_display_hex(),
                        revision.manifest.shard_id
                    )
                    .into());
                }
                pools
                    .entry((
                        tier(revision.manifest.sealed).to_string(),
                        class_label(&entry).to_string(),
                    ))
                    .or_default()
                    .push(Candidate {
                        order: order(&txid.0),
                        revision: index,
                        bucket,
                        txid,
                        height,
                        entry,
                    });
            }
        }
    }
    // Every revision's files are open only while it is read, so a mainnet
    // publication stays within the open-file limit.
    let lookup = |revision: &DisplayRevision, txid: &Txid| -> Result<Option<DisplayEntry>, Error> {
        Rows::open(revision)?.lookup(revision, txid)
    };
    let mut samples = Vec::new();
    let mut available = BTreeMap::new();
    if let Some(natural) = natural {
        // One pool per tier: the seed order is uniform over its txids, so the
        // first `natural` follow the class mix rather than equalising it.
        let mut tiers: BTreeMap<(String, String), Vec<Candidate>> = BTreeMap::new();
        for ((tier, class), pool) in std::mem::take(&mut pools) {
            available.insert(format!("{tier}/{class}"), pool.len());
            tiers
                .entry((tier, "natural".to_string()))
                .or_default()
                .extend(pool);
        }
        for pool in tiers.values_mut() {
            pool.sort_by_key(|candidate| candidate.order);
            pool.truncate(natural);
        }
        pools = tiers;
    }
    for ((tier, class), pool) in &mut pools {
        if natural.is_none() {
            available.insert(format!("{tier}/{class}"), pool.len());
        }
        pool.sort_by_key(|candidate| candidate.order);
        let take = natural.unwrap_or(per_class);
        for candidate in pool.iter().take(take) {
            let revision = &revisions[candidate.revision];
            let entry = &candidate.entry;
            // What the client's two queries return must be this entry.
            if lookup(revision, &candidate.txid)? != Some(*entry) {
                return Err(format!(
                    "txid {} is not found at its candidate rows in shard {}",
                    candidate.txid.to_display_hex(),
                    revision.manifest.shard_id
                )
                .into());
            }
            samples.push(serde_json::json!({
                "txid": candidate.txid.to_display_hex(),
                "height": candidate.height,
                "shard_id": revision.manifest.shard_id,
                "tier": tier,
                "digest": revision.digest,
                "bucket": candidate.bucket,
                "class": class_label(entry),
                "omissions": omissions(entry),
                "entry_sha256": entry_sha256(entry)?,
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
            if every.contains(&Tag::of(&Txid(draw))) {
                continue;
            }
            (Txid(draw).to_display_hex(), "random", height)
        };
        let shard = map
            .shard_for_height(height)
            .ok_or("height outside the map")?;
        // A control is kept only if the client's two queries in that shard
        // find nothing, so `expect` is what the tables say.
        let revision = revisions
            .iter()
            .find(|r| r.manifest.shard_id == shard.shard_id)
            .ok_or("a mapped shard has no revision")?;
        let parsed = parse_txid(&txid).ok_or("unparseable control txid")?;
        if lookup(revision, &parsed)?.is_some() {
            continue;
        }
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
        "schema": FIXTURE_SCHEMA,
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

/// Fixture documents carry `entry_sha256`; the v1 ones carried
/// `record_sha256` of the variable-length record and are refused.
const FIXTURE_SCHEMA: &str = "transparent-txid-display-fixture-v2";

/// Parses a txid in display (reversed) hex.
fn parse_txid(display_hex: &str) -> Option<Txid> {
    let mut bytes: [u8; 32] = hex::decode(display_hex).ok()?.try_into().ok()?;
    bytes.reverse();
    Some(Txid(bytes))
}

/// Saved maps in the order they were served. File names are map digests, so
/// name order says nothing about time.
fn ordered_maps(dir: &Path, order: Option<&Path>) -> Result<Vec<PathBuf>, Error> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort();
    if let Some(order) = order {
        let mut rank = BTreeMap::new();
        for line in std::fs::read_to_string(order)?.lines() {
            let event: serde_json::Value = serde_json::from_str(line)?;
            if let (Some("map"), Some(sha)) =
                (event["event"].as_str(), event["map_sha256"].as_str())
            {
                let next = rank.len();
                rank.entry(sha.to_string()).or_insert(next);
            }
        }
        let key = |p: &PathBuf| {
            let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or_default();
            rank.get(stem).copied().unwrap_or(usize::MAX)
        };
        files.sort_by_key(key);
    } else {
        // Coverage only grows apart from reorgs, so it orders an unannotated
        // series; unreadable files sort last and are reported below.
        let key = |p: &PathBuf| {
            std::fs::read(p)
                .ok()
                .and_then(|raw| serde_json::from_slice::<DisplayMap>(&raw).ok())
                .map(|m| (m.covered_through().unwrap_or(0), m.first_shard_id))
                .unwrap_or((u64::MAX, u64::MAX))
        };
        files.sort_by_cached_key(key);
    }
    Ok(files)
}

fn audit(dir: &Path, order: Option<&Path>) -> Result<serde_json::Value, Error> {
    let files = ordered_maps(dir, order)?;
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
    // Entry `index` is the one of `synth::txid(seed, index)`.
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
    let bucket_min = |part: &[DisplayEntry]| {
        let mut counts = vec![0u64; n_buckets as usize];
        for entry in part {
            counts[display::bucket(&entry.tag, n_buckets) as usize] += 1;
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
        for (j, entry) in part.iter().enumerate() {
            let txid = synth::txid(seed, (i * per_shard + j) as u64);
            if Tag::of(&txid) != entry.tag {
                return Err("a synthetic entry is not its txid's".into());
            }
            index.push((
                txid,
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
        assert!(!census["summary"]["buckets_below_floor"]
            .as_array()
            .unwrap()
            .is_empty());
        // Entry counts agree with the generator's own entries.
        let mut expected = EntryCounts::default();
        for entry in synth::records(600, 5) {
            expected.add(&entry).unwrap();
        }
        assert_eq!(
            census["summary"]["entries"],
            serde_json::to_value(expected).unwrap()
        );
        assert_eq!(expected.complete + expected.omitted, 600);
        assert!(expected.omitted > 0 && expected.unshield > 0);
        assert!(expected.more_than_two_outputs > 0 && expected.multiple_source_scripts > 0);
        let per_shard: u64 = census["shards"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["entries"]["entries"].as_u64().unwrap())
            .sum();
        assert_eq!(per_shard, 600);

        let natural = fixture(
            &candidate,
            &out.join("tooling/heights.bin"),
            2,
            Some(5),
            0,
            0,
        )
        .unwrap();
        let drawn = natural["samples"].as_array().unwrap();
        // At most `natural` per tier, whatever their classes.
        for tier in ["archive", "recent"] {
            assert!(drawn.iter().filter(|s| s["tier"] == tier).count() <= 5);
        }
        let fixture = fixture(&candidate, &out.join("tooling/heights.bin"), 2, None, 6, 0).unwrap();
        assert_eq!(fixture["schema"], FIXTURE_SCHEMA);
        let samples = fixture["samples"].as_array().unwrap();
        for class in ["complete", "omission"] {
            assert!(samples
                .iter()
                .any(|s| s["tier"] == "archive" && s["class"] == class));
        }
        assert!(samples.iter().any(|s| s["tier"] == "recent"));
        // Each digest is the generator's entry for that txid, encoded.
        let generated: HashMap<String, DisplayEntry> = synth::records(600, 5)
            .into_iter()
            .enumerate()
            .map(|(i, e)| (synth::txid(5, i as u64).to_display_hex(), e))
            .collect();
        for sample in samples {
            let entry = &generated[sample["txid"].as_str().unwrap()];
            assert_eq!(sample["entry_sha256"], entry_sha256(entry).unwrap());
            assert_eq!(
                sample["omissions"].as_array().unwrap().is_empty(),
                sample["class"] == "complete"
            );
        }
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
        let clean = audit(&maps, None).unwrap();
        assert_eq!(clean["ok"], true, "{clean}");
        assert_eq!(clean["drops"].as_array().unwrap().len(), 1);
        let mut changed = dropped.clone();
        changed.shards[0].manifest_digest = "99".repeat(32);
        std::fs::write(maps.join("0003.json"), changed.to_bytes()).unwrap();
        let report = audit(&maps, None).unwrap();
        assert_eq!(report["ok"], false);
    }
}
