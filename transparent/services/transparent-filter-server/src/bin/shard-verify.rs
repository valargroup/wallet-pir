//! Verifies a published shard set against what it claims to be, before any
//! service loads it.
//!
//! The loader already refuses a set whose manifests, tables or filters do not
//! digest to what they publish, whose chain of manifests is broken, or whose
//! map disagrees with its shards. This tool runs that loader and then checks
//! the things the loader cannot know: that the set covers exactly the range it
//! was published for, ends at the anchor block it was pinned to, divides into
//! tiers at the recorded boundary, and — given the journal — that a sample of
//! its shards is reproduced byte for byte by building them again from the
//! events. That last check is what makes "deterministic construction" a
//! measured fact rather than a design claim.
//!
//! Read-only. The expectations come from the publication record the publish
//! workflow wrote beside the map, or from explicit flags.

use clap::Parser;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use transparent_filter::BlockHash;
use transparent_filter_server::events::EventStore;
use transparent_shard::build::build_shard;
use transparent_shard_server::shardset::{ShardSet, DEFAULT_RETAIN_REVISIONS};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "shard-verify",
    about = "Verify a published shard set's digests, coverage, anchor, tiers and reproducibility"
)]
struct Cli {
    #[arg(long)]
    shard_dir: PathBuf,
    /// The `publication.json` the publish workflow wrote; supplies the
    /// expectations below unless they are given explicitly.
    #[arg(long)]
    publication: Option<PathBuf>,
    #[arg(long)]
    expect_start: Option<u64>,
    #[arg(long)]
    expect_through: Option<u64>,
    /// Display-hex hash of the anchor block, the last block published.
    #[arg(long)]
    expect_anchor_hash: Option<String>,
    /// First height of the recent tier; every shard from here must use the
    /// recent geometry and every shard below it the archive geometry.
    #[arg(long)]
    expect_recent_from: Option<u64>,
    #[arg(long)]
    expect_recent_geometry: Option<String>,
    #[arg(long)]
    expect_archive_geometry: Option<String>,
    /// SHA-256 of `shards.json` as written.
    #[arg(long)]
    expect_map_sha256: Option<String>,
    /// The journal to rebuild sampled shards from.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Shards to rebuild and compare, chosen by seed; needs `--data-dir`.
    #[arg(long, default_value_t = 4)]
    rebuild: usize,
    /// Rebuild every shard. Hours over a full chain; the pilot's evidence.
    #[arg(long)]
    rebuild_all: bool,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = DEFAULT_RETAIN_REVISIONS)]
    retain_revisions: usize,
    #[arg(long)]
    out: Option<PathBuf>,
    #[arg(long)]
    source_sha: Option<String>,
}

#[derive(Default)]
struct Expectations {
    start: Option<u64>,
    through: Option<u64>,
    anchor_hash: Option<String>,
    recent_from: Option<u64>,
    recent_geometry: Option<String>,
    archive_geometry: Option<String>,
    map_sha256: Option<String>,
}

fn expectations(cli: &Cli) -> Result<Expectations, BoxError> {
    let mut expected = Expectations::default();
    if let Some(path) = &cli.publication {
        let record: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
        let published = record
            .get("published")
            .ok_or("publication record has no `published` object")?;
        let u64_at = |key: &str| published.get(key).and_then(serde_json::Value::as_u64);
        let str_at = |key: &str| {
            published
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        };
        expected.start = u64_at("start_height");
        expected.through = u64_at("through");
        expected.anchor_hash = str_at("anchor_hash");
        expected.recent_from = u64_at("recent_from");
        expected.recent_geometry = str_at("recent_geometry");
        expected.archive_geometry = str_at("archive_geometry");
        expected.map_sha256 = str_at("map_sha256");
    }
    // Explicit flags override the record.
    expected.start = cli.expect_start.or(expected.start);
    expected.through = cli.expect_through.or(expected.through);
    expected.anchor_hash = cli.expect_anchor_hash.clone().or(expected.anchor_hash);
    expected.recent_from = cli.expect_recent_from.or(expected.recent_from);
    expected.recent_geometry = cli
        .expect_recent_geometry
        .clone()
        .or(expected.recent_geometry);
    expected.archive_geometry = cli
        .expect_archive_geometry
        .clone()
        .or(expected.archive_geometry);
    expected.map_sha256 = cli.expect_map_sha256.clone().or(expected.map_sha256);
    Ok(expected)
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
}

fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let expected = expectations(&cli)?;
    let mut checks: Vec<serde_json::Value> = Vec::new();
    let mut failures = 0usize;
    let mut check = |name: &str, ok: bool, detail: String| {
        if !ok {
            failures += 1;
        }
        eprintln!("{} {name}: {detail}", if ok { "ok  " } else { "FAIL" });
        checks.push(serde_json::json!({"check": name, "ok": ok, "detail": detail}));
    };

    // The loader: every digest, every segment length, the manifest chain,
    // schema and geometry names, map/shard agreement, retained revisions.
    let started = std::time::Instant::now();
    let set = match ShardSet::open(&cli.shard_dir, cli.retain_revisions) {
        Ok(set) => set,
        Err(error) => {
            check("load", false, error.to_string());
            return finish(&cli, checks, failures, None);
        }
    };
    check(
        "load",
        true,
        format!(
            "{} shards, {} revisions held, every manifest, table segment and filter verified in {:.1}s",
            set.len(),
            set.revisions().len(),
            started.elapsed().as_secs_f64()
        ),
    );

    let map = &set.map;
    let first = map.shards.first().map(|entry| entry.start_height);
    let last = map.shards.last();

    // Coverage: contiguous, no gap, no overlap.
    let mut contiguous = true;
    let mut previous_end: Option<u64> = None;
    for entry in &map.shards {
        if let Some(end) = previous_end {
            if entry.start_height != end + 1 {
                contiguous = false;
                break;
            }
        }
        if entry.end_height < entry.start_height {
            contiguous = false;
            break;
        }
        previous_end = Some(entry.end_height);
    }
    check(
        "coverage-contiguous",
        contiguous,
        format!(
            "{}-{} across {} shards",
            first.unwrap_or(0),
            last.map(|entry| entry.end_height).unwrap_or(0),
            map.shards.len()
        ),
    );
    if let Some(start) = expected.start {
        check(
            "coverage-start",
            first == Some(start) && map.start_height == start,
            format!("map starts at {}, expected {start}", map.start_height),
        );
    }
    if let Some(through) = expected.through {
        let end = last.map(|entry| entry.end_height);
        check(
            "coverage-through",
            end == Some(through),
            format!("last shard ends at {end:?}, expected {through}"),
        );
    }
    if let Some(hash) = &expected.anchor_hash {
        let terminal = last.map(|entry| entry.terminal_block_hash.as_str());
        check(
            "anchor-hash",
            terminal == Some(hash.as_str()),
            format!("terminal block {terminal:?}, expected {hash}"),
        );
    }

    // Tiers: the boundary is a shard boundary and each side uses its geometry.
    match (
        expected.recent_from,
        &expected.recent_geometry,
        &expected.archive_geometry,
    ) {
        (Some(recent_from), Some(recent), Some(archive)) => {
            let boundary_is_shard_start = map.shards.iter().any(|e| e.start_height == recent_from);
            let mut wrong = Vec::new();
            for entry in &map.shards {
                let want = if entry.start_height < recent_from {
                    archive.as_str()
                } else {
                    recent.as_str()
                };
                if entry.geometry != want {
                    wrong.push(entry.shard_id);
                }
                if entry.start_height < recent_from && entry.end_height >= recent_from {
                    wrong.push(entry.shard_id);
                }
            }
            let archive_shards = map
                .shards
                .iter()
                .filter(|e| e.start_height < recent_from)
                .count();
            check(
                "tiers",
                boundary_is_shard_start && wrong.is_empty(),
                format!(
                    "{archive_shards} {archive} shards below {recent_from}, {} {recent} shards from it{}",
                    map.shards.len() - archive_shards,
                    if wrong.is_empty() {
                        String::new()
                    } else {
                        format!("; shards {wrong:?} straddle or misname")
                    }
                ),
            );
        }
        (None, _, _) => {
            let geometries: std::collections::BTreeSet<&str> =
                map.shards.iter().map(|e| e.geometry.as_str()).collect();
            check(
                "tiers",
                geometries.len() == 1,
                format!("single-tier set using {geometries:?}"),
            );
        }
        _ => check(
            "tiers",
            false,
            "recent_from given without both geometry names".into(),
        ),
    }

    // Map identity: the file as written, and the compact form the service digests.
    let map_file = std::fs::read(cli.shard_dir.join("shards.json"))?;
    let file_sha256 = hex::encode(Sha256::digest(&map_file));
    if let Some(expected_map) = &expected.map_sha256 {
        check(
            "map-sha256",
            &file_sha256 == expected_map,
            format!("shards.json digests to {file_sha256}, expected {expected_map}"),
        );
    }
    eprintln!(
        "     served map digest (compact serialization): {}",
        set.map_digest
    );

    // Multi-segment shards, listed so the pilot can target them.
    let multi: Vec<u64> = map
        .shards
        .iter()
        .filter(|e| e.directory_segments > 1 || e.page_segments > 1)
        .map(|e| e.shard_id)
        .collect();
    eprintln!("     multi-segment shards: {multi:?}");

    // Reproducibility: rebuild a sample from the journal and compare digests.
    let mut rebuilt: Vec<serde_json::Value> = Vec::new();
    if let Some(data_dir) = &cli.data_dir {
        let store = EventStore::open_existing(data_dir)?;
        if store.genesis_hash() != map.genesis_hash {
            check(
                "journal-genesis",
                false,
                format!(
                    "journal genesis {} differs from the map's {}",
                    store.genesis_hash(),
                    map.genesis_hash
                ),
            );
        }
        let genesis = BlockHash::from_display_hex(&map.genesis_hash)?;
        let mut chosen: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
        if cli.rebuild_all {
            chosen.extend(0..map.shards.len());
        } else {
            let mut lcg = Lcg(cli.seed ^ 0x9E37_79B9_7F4A_7C15);
            // Always the first, the last, and every multi-segment shard.
            chosen.insert(0);
            chosen.insert(map.shards.len() - 1);
            for (index, entry) in map.shards.iter().enumerate() {
                if entry.directory_segments > 1 || entry.page_segments > 1 {
                    chosen.insert(index);
                }
            }
            while chosen.len() < cli.rebuild.min(map.shards.len()) {
                chosen.insert((lcg.next() % map.shards.len() as u64) as usize);
            }
        }
        for index in chosen {
            let entry = &map.shards[index];
            let shard = set.get(entry.shard_id).expect("the map names it");
            let manifest = &shard.manifest;
            let started = std::time::Instant::now();
            let mut events = Vec::new();
            for height in manifest.start_height..=manifest.end_height {
                events.extend(
                    store
                        .events_at(height)?
                        .ok_or_else(|| format!("height {height} is missing from the journal"))?,
                );
            }
            let terminal = BlockHash::from_display_hex(&manifest.terminal_block_hash)?;
            let built = build_shard(
                manifest.shard_id,
                manifest.start_height,
                manifest.end_height,
                genesis,
                terminal,
                &manifest.profile,
                shard.geometry,
                &events,
            )?;
            let filter_ok = transparent_filter::filter_hash(built.filter.as_slice())
                .to_display_hex()
                == manifest.filter_hash;
            let directory_ok = built.directory.len() == manifest.directory_segments.len()
                && built
                    .directory
                    .iter()
                    .zip(manifest.directory_segments.iter())
                    .all(|(bytes, published)| {
                        hex::encode(Sha256::digest(bytes)) == published.sha256
                    });
            let pages_ok = built.pages.len() == manifest.page_segments.len()
                && built.pages.iter().zip(manifest.page_segments.iter()).all(
                    |(bytes, published)| hex::encode(Sha256::digest(bytes)) == published.sha256,
                );
            let ok = filter_ok && directory_ok && pages_ok;
            check(
                &format!("rebuild-{}", entry.shard_id),
                ok,
                format!(
                    "{}-{} {} events, filter {}, directory {}, pages {}, {:.1}s",
                    manifest.start_height,
                    manifest.end_height,
                    events.len(),
                    if filter_ok { "same" } else { "DIFFERENT" },
                    if directory_ok { "same" } else { "DIFFERENT" },
                    if pages_ok { "same" } else { "DIFFERENT" },
                    started.elapsed().as_secs_f64()
                ),
            );
            rebuilt.push(serde_json::json!({
                "shard_id": entry.shard_id,
                "geometry": entry.geometry,
                "events": events.len(),
                "filter": filter_ok,
                "directory": directory_ok,
                "pages": pages_ok,
                "seconds": started.elapsed().as_secs_f64(),
            }));
        }
    }

    let summary = serde_json::json!({
        "shards": set.len(),
        "revisions_held": set.revisions().len(),
        "start_height": first,
        "through": last.map(|e| e.end_height),
        "terminal_block_hash": last.map(|e| e.terminal_block_hash.clone()),
        "geometries": set.geometries().iter().map(|g| g.name).collect::<Vec<_>>(),
        "multi_segment_shards": multi,
        "map_file_sha256": file_sha256,
        "map_served_sha256": set.map_digest,
        "rebuilt": rebuilt,
    });
    finish(&cli, checks, failures, Some(summary))
}

fn finish(
    cli: &Cli,
    checks: Vec<serde_json::Value>,
    failures: usize,
    summary: Option<serde_json::Value>,
) -> Result<(), BoxError> {
    let record = serde_json::json!({
        "schema": "transparent-shard-verify-v1",
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "tool_sha": cli.source_sha,
        "shard_dir": cli.shard_dir,
        "publication": cli.publication,
        "checks": checks,
        "failures": failures,
        "set": summary,
    });
    let bytes = serde_json::to_vec_pretty(&record)?;
    match &cli.out {
        Some(path) => std::fs::write(path, &bytes)?,
        None => println!("{}", String::from_utf8(bytes)?),
    }
    if failures > 0 {
        return Err(format!("{failures} check(s) failed").into());
    }
    eprintln!("every check passed");
    Ok(())
}
