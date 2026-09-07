//! Publishes a shard set from the transparent event journal.
//!
//! Streams the journal through the sealer, builds each shard's filter,
//! directory and pages, and writes them to an output directory named by each
//! shard's own manifest digest — plus the shard map, which is the protocol data
//! a wallet binary-searches to turn its birthday into a first shard.
//!
//! Publication is immutable. Re-running over the same journal reproduces the
//! same bytes, so a shard directory that already exists is verified rather than
//! rewritten; a mismatch is a hard error, because it means either the journal
//! or the builder changed under a published identity.
//!
//! A tail is different only in that it is expected to be republished. Running
//! again over a longer journal publishes a *new revision* of it, beside the old
//! one and under its own digest, recording which revision it supersedes. Bytes
//! already served never change; a wallet holding the earlier revision sees that
//! the range it covered was replaced rather than extended.
//!
//! Read-only against the journal.

use clap::Parser;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use transparent_filter::{filter_hash, BlockHash, ScriptBytes};
use transparent_filter_server::events::EventStore;
use transparent_filter_server::zakura::ZakuraClient;
use transparent_shard::build::build_shard;
use transparent_shard::layout::{by_name as geometry_by_name, Geometry};
use transparent_shard::manifest::{
    ManifestLayout, ManifestOccupancy, ManifestSeal, PublishedRevision, ShardManifest,
    TableGeometry, SCHEMA,
};
use transparent_shard::seal::{PageBasis, SealPolicy, Sealer};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(name = "shard-publish", about = "Publish a transparent shard set")]
struct Cli {
    #[arg(long, default_value = "./transparent-event-data")]
    data_dir: PathBuf,
    #[arg(long, default_value = "./transparent-shards")]
    output: PathBuf,
    /// Geometry for history at and after `--recent-from`.
    ///
    /// The window wallets synchronise constantly, so it is sized for a small
    /// query rather than for holding the most content.
    #[arg(long, default_value = "recent-8k")]
    recent_geometry: String,
    /// Geometry for history before `--recent-from`.
    ///
    /// Defaults to the recent geometry, which publishes a single-geometry set —
    /// what every existing published set is. Give both this and `--recent-from`
    /// to publish the two-tier set the deployment plan describes.
    #[arg(long)]
    archive_geometry: Option<String>,
    /// First height of the recent tier.
    ///
    /// A forced shard boundary: a shard's rows are addressed at one row count,
    /// so no shard may span the change. Derive it from the pinned anchor's
    /// chain timestamp rather than from a block count — a height standing in
    /// for "six months" drifts with the interval, and re-deriving it later
    /// would re-shard the chain.
    #[arg(long)]
    recent_from: Option<u64>,
    /// Needed for exactly one thing: the block hash before the journal's first
    /// height, which is shard zero's parent and is by definition not in the
    /// journal.
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    zakura_rpc_url: String,
    #[arg(long)]
    zakura_cookie: PathBuf,
}

/// Writes `bytes` to `path` so that a reader sees either all of them or none.
///
/// Temp file, fsync, rename. A published set is read back by a service that
/// refuses to start on a file that does not match its digest, so a half-written
/// file is not a corruption a reader has to detect — but only if the partial
/// state is never visible under the final name. `std::fs::write` truncates in
/// place, so an interrupted publish leaves a short file *at the name the map
/// points to*, which is exactly the state a deploy would then try to ship.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), BoxError> {
    use std::io::Write;
    let temp = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temp)?;
        file.write_all(bytes)?;
        // The rename is ordered after the data only if the data is on disk
        // first. Without this, a crash can leave the new name pointing at a
        // file whose contents never arrived.
        file.sync_all()?;
    }
    std::fs::rename(&temp, path)?;
    // Durably link the new name into the directory, so the rename survives a
    // crash rather than only the bytes it points to.
    if let Some(parent) = path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn write_immutable(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), BoxError> {
    let path = dir.join(name);
    if path.exists() {
        // Published shards are immutable. If the bytes differ, either the
        // journal or the builder changed while the identity did not, and
        // overwriting would replace a published shard in place.
        let existing = std::fs::read(&path)?;
        if existing != bytes {
            return Err(format!("{} already exists with different content", path.display()).into());
        }
        return Ok(());
    }
    write_atomic(&path, bytes)
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let store = EventStore::open_existing(&cli.data_dir)?;
    let Some(covered) = store.covered_through() else {
        return Err("the journal is empty".into());
    };
    let first = store.start_height();

    let recent: &'static Geometry = geometry_by_name(&cli.recent_geometry)
        .ok_or_else(|| format!("unknown geometry {:?}", cli.recent_geometry))?;
    let archive: &'static Geometry = match &cli.archive_geometry {
        Some(name) => geometry_by_name(name).ok_or_else(|| format!("unknown geometry {name:?}"))?,
        None => recent,
    };
    // A cutoff is only meaningful if the two tiers differ, and two differing
    // tiers are only publishable if there is a cutoff. Accepting either alone
    // would silently publish a single-geometry set under a command line that
    // asked for two.
    let cutoff = match (cli.recent_from, archive.name == recent.name) {
        (Some(_), true) => {
            return Err("--recent-from needs an --archive-geometry that differs from                         --recent-geometry"
                .into())
        }
        (None, false) => {
            return Err("--archive-geometry needs --recent-from to say where the tiers                         divide"
                .into())
        }
        (Some(height), false) => Some(height),
        (None, true) => None,
    };
    if let Some(height) = cutoff {
        if height <= first || height > covered {
            return Err(format!(
                "--recent-from {height} is outside the journal's {first}-{covered}"
            )
            .into());
        }
    }

    // Thresholds are the geometry's, not the operator's. They are schema — two
    // sets built under different ones are different partitions of the chain —
    // and deriving them from the row counts is what stops a geometry acquiring
    // a second, disagreeing policy.
    let archive_policy = SealPolicy::for_geometry(archive);
    let recent_policy = SealPolicy::for_geometry(recent);
    let genesis = BlockHash::from_display_hex(store.genesis_hash())?;

    // Shard zero's parent is the block before coverage begins, which the
    // journal does not hold. One RPC call, and the only reason this needs a
    // node at all.
    let zakura = ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, &cli.zakura_cookie)?;
    let base_parent = BlockHash::from_display_hex(&zakura.block_hash(first - 1).await?)?;

    std::fs::create_dir_all(&cli.output)?;
    match cutoff {
        None => eprintln!(
            "journal {first}-{covered}, all {} sealing at {archive_policy:?}",
            recent.name
        ),
        Some(height) => eprintln!(
            "journal {first}-{covered}, {} below {height} at {archive_policy:?}, \
             {} from {height} at {recent_policy:?}",
            archive.name, recent.name
        ),
    }

    // Buffer each shard's events as the sealer decides boundaries. A shard's
    // events are needed all at once, since pages are per script and a script's
    // history can span the whole shard.
    let mut geometry = if cutoff.is_some() { archive } else { recent };
    let mut policy = if cutoff.is_some() {
        archive_policy
    } else {
        recent_policy
    };
    let mut sealer = Sealer::with_geometry(policy, first, PageBasis::default(), *geometry);
    let mut pending: Vec<(
        u64,
        Vec<(ScriptBytes, transparent_events::TransparentEvent)>,
    )> = Vec::new();
    let mut entries: Vec<transparent_filter::ShardMapEntry> = Vec::new();
    let mut parent_manifest_digest = String::new();
    let mut parent_block_hash = base_parent;

    // The map from a previous run, if any. It is what tells a republished tail
    // which revision it supersedes; without it a growing tail would look like a
    // first publication every time.
    let previous: std::collections::BTreeMap<u64, transparent_filter::ShardMapEntry> =
        match std::fs::read(cli.output.join("shards.json")) {
            Ok(raw) => serde_json::from_slice::<transparent_filter::ShardMap>(&raw)
                // A map this build cannot read is a set published under another
                // schema. Publishing beside it would leave two incompatible
                // sets in one directory, which the loader refuses to serve, so
                // say what to do instead of reporting a parse error.
                .map_err(|error| {
                    format!(
                        "{} was published under a schema this build cannot read \
                         ({error}); publish into a directory of its own",
                        cli.output.join("shards.json").display()
                    )
                })?
                .shards
                .into_iter()
                .map(|entry| (entry.shard_id, entry))
                .collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Default::default(),
            Err(error) => return Err(error.into()),
        };

    let publish = |shard: transparent_shard::SealedShard,
                   blocks: &[(
        u64,
        Vec<(ScriptBytes, transparent_events::TransparentEvent)>,
    )],
                   parent_block_hash: BlockHash,
                   parent_manifest_digest: &str,
                   geometry: &'static Geometry,
                   policy: SealPolicy|
     -> Result<(String, BlockHash, transparent_filter::ShardMapEntry), BoxError> {
        let terminal = store
            .block_at(shard.end_height)
            .ok_or("shard end is not covered")?
            .block_hash;
        let events: Vec<_> = blocks
            .iter()
            .flat_map(|(_, events)| events.iter().cloned())
            .collect();

        let built = build_shard(
            shard.shard_id,
            shard.start_height,
            shard.end_height,
            genesis,
            terminal,
            transparent_filter::RANGE_PROFILE,
            geometry,
            &events,
        )?;

        // The sealer sized this shard's tables from the packing rule; the
        // builder laid them out under the same rule. Nothing compared the two
        // before, so a divergence would have published a table sized for
        // something other than what it holds — and the surplus comes back as
        // extra segments, which every wallet querying this shard pays for,
        // because it must query all of them.
        if built.page_rows != shard.occupancy.page_rows
            || built.fragments != shard.occupancy.fragments
        {
            return Err(format!(
                "shard {} over {}-{} emitted {} page rows and {} fragments, \
                 but was sealed for {} and {}",
                shard.shard_id,
                shard.start_height,
                shard.end_height,
                built.page_rows,
                built.fragments,
                shard.occupancy.page_rows,
                shard.occupancy.fragments,
            )
            .into());
        }

        // A shard already in the previous map is being republished. Building it
        // under the published numbering first says which kind of republication
        // this is: one that reproduces the published digest is the same shard
        // again and keeps its identity, while one that does not is a tail that
        // has grown and takes the next revision.
        let make = |revision: u32, supersedes: String| ShardManifest {
            schema: SCHEMA.to_string(),
            profile: transparent_filter::RANGE_PROFILE.to_string(),
            geometry: geometry.name.to_string(),
            network: transparent_filter::NETWORK.to_string(),
            genesis_hash: store.genesis_hash().to_string(),
            shard_id: shard.shard_id,
            start_height: shard.start_height,
            end_height: shard.end_height,
            parent_block_hash: parent_block_hash.to_display_hex(),
            terminal_block_hash: terminal.to_display_hex(),
            parent_manifest_digest: parent_manifest_digest.to_string(),
            sealed: shard.reason.is_some(),
            revision,
            supersedes,
            seal: ManifestSeal {
                scripts_target: policy.scripts.target,
                scripts_capacity: policy.scripts.capacity,
                page_rows_target: policy.page_rows.target,
                page_rows_capacity: policy.page_rows.capacity,
            },
            layout: ManifestLayout {
                max_script_bytes: transparent_shard::MAX_SCRIPT_BYTES as u32,
                inline_events: transparent_shard::INLINE_EVENTS,
                events_per_page: transparent_shard::EVENTS_PER_PAGE,
                page_row_header_bytes: transparent_shard::PAGE_ROW_HEADER_BYTES as u32,
                page_entry_header_bytes: transparent_shard::PAGE_ENTRY_HEADER_BYTES as u32,
                directory_choices: transparent_shard::build::DIRECTORY_CHOICES as u32,
            },
            filter_hash: filter_hash(built.filter.as_slice()).to_display_hex(),
            directory_segments: built
                .directory
                .iter()
                .map(|segment| TableGeometry {
                    rows: geometry.directory_rows,
                    row_bytes: geometry.directory_row_bytes as u32,
                    sha256: hex::encode(Sha256::digest(segment)),
                })
                .collect(),
            page_segments: built
                .pages
                .iter()
                .map(|segment| TableGeometry {
                    rows: geometry.page_rows,
                    row_bytes: geometry.page_row_bytes as u32,
                    sha256: hex::encode(Sha256::digest(segment)),
                })
                .collect(),
            occupancy: ManifestOccupancy {
                scripts: built.scripts,
                page_rows: built.page_rows,
                fragments: built.fragments,
                events: built.events,
                blocks: shard.occupancy.blocks,
                txids: shard.occupancy.txids,
                excluded_scripts: built.excluded_scripts,
            },
        };

        let published = match previous.get(&shard.shard_id) {
            None => None,
            Some(entry) => {
                let raw = std::fs::read(
                    cli.output
                        .join(&entry.manifest_digest)
                        .join("manifest.json"),
                )?;
                let published: ShardManifest = serde_json::from_slice(&raw)?;
                Some(PublishedRevision {
                    digest: entry.manifest_digest.clone(),
                    revision: published.revision,
                    supersedes: published.supersedes,
                    sealed: published.sealed,
                })
            }
        };
        let reproduced = published.as_ref().is_some_and(|previous| {
            make(previous.revision, previous.supersedes.clone()).digest() == previous.digest
        });
        let (revision, supersedes) =
            PublishedRevision::next(shard.shard_id, published.as_ref(), reproduced)?;
        let manifest = make(revision, supersedes);

        let digest = manifest.digest();
        let dir = cli.output.join(&digest);
        std::fs::create_dir_all(&dir)?;
        write_immutable(&dir, "manifest.json", &manifest.canonical_bytes())?;
        write_immutable(&dir, "filter.bin", built.filter.as_slice())?;
        // One file per segment, in segment order. A shard normally has one of
        // each; more means its content did not fit a single segment.
        for (index, segment) in built.directory.iter().enumerate() {
            write_immutable(&dir, &format!("directory.{index}.bin"), segment)?;
        }
        for (index, segment) in built.pages.iter().enumerate() {
            write_immutable(&dir, &format!("pages.{index}.bin"), segment)?;
        }

        eprintln!(
            "shard {:>3} {}-{} ({} blocks) scripts {} pages {} events {} segments {}/{} {}",
            shard.shard_id,
            shard.start_height,
            shard.end_height,
            shard.occupancy.blocks,
            built.scripts,
            built.page_rows,
            built.events,
            built.directory_segments(),
            built.page_segments(),
            if manifest.sealed {
                "sealed".to_string()
            } else {
                format!("TAIL r{revision}")
            },
        );

        let entry = transparent_filter::ShardMapEntry {
            shard_id: shard.shard_id,
            geometry: geometry.name.to_string(),
            start_height: shard.start_height,
            end_height: shard.end_height,
            parent_block_hash: manifest.parent_block_hash.clone(),
            terminal_block_hash: manifest.terminal_block_hash.clone(),
            filter_hash: manifest.filter_hash.clone(),
            scripts: built.scripts,
            page_rows: built.page_rows,
            txids: shard.occupancy.txids,
            directory_segments: built.directory_segments(),
            page_segments: built.page_segments(),
            manifest_digest: digest.clone(),
            revision,
            sealed: manifest.sealed,
        };
        Ok((digest, terminal, entry))
    };

    for height in first..=covered {
        // The tiers divide at a height, so the archive sealer is closed before
        // the first recent block is offered to anything. A shard's rows are
        // addressed at one row count; one spanning the change could not be
        // read at either.
        if cutoff == Some(height) {
            if let Some(shard) = sealer.seal_at_geometry_change() {
                let (digest, terminal, entry) = publish(
                    shard,
                    &pending,
                    parent_block_hash,
                    &parent_manifest_digest,
                    geometry,
                    policy,
                )?;
                parent_manifest_digest = digest;
                parent_block_hash = terminal;
                entries.push(entry);
                pending.clear();
            }
            // Ids continue across the change: a shard id is the wallet's stable
            // handle on a range, and restarting at zero would publish two
            // shards under one id.
            let next_shard_id = sealer.next_shard_id();
            geometry = recent;
            policy = recent_policy;
            sealer = Sealer::resume(
                policy,
                height,
                PageBasis::default(),
                *geometry,
                next_shard_id,
            );
        }

        let events = store
            .events_at(height)?
            .ok_or_else(|| format!("height {height} is missing from the journal"))?;
        pending.push((height, events));
        let block = pending.last().expect("just pushed");
        // One block can close two shards: the one it would have overrun, and
        // itself, when it exceeds a capacity on its own.
        for shard in sealer.push_block(block.0, &block.1)? {
            // A capacity seal closes before the block that triggered it, so the
            // block just pushed may belong to the *next* shard, not this one.
            let split = pending
                .iter()
                .position(|(h, _)| *h > shard.end_height)
                .unwrap_or(pending.len());
            let carried = pending.split_off(split);
            let (digest, terminal, entry) = publish(
                shard,
                &pending,
                parent_block_hash,
                &parent_manifest_digest,
                geometry,
                policy,
            )?;
            parent_manifest_digest = digest;
            parent_block_hash = terminal;
            entries.push(entry);
            pending = carried;
        }
    }
    if let Some(shard) = sealer.finish() {
        let (_, _, entry) = publish(
            shard,
            &pending,
            parent_block_hash,
            &parent_manifest_digest,
            geometry,
            policy,
        )?;
        entries.push(entry);
    }

    let map = transparent_filter::ShardMap {
        genesis_hash: store.genesis_hash().to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: first,
        // One entry per geometry the set actually used. A single-tier set
        // publishes one; the two-tier set publishes both, because the two were
        // sealed under different thresholds and one figure would describe
        // neither.
        seal: entries
            .iter()
            .map(|entry| entry.geometry.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|name| {
                let used = if name == recent.name {
                    recent_policy
                } else {
                    archive_policy
                };
                (
                    name,
                    transparent_filter::SealParameters {
                        max_scripts: used.scripts.target,
                        max_page_rows: used.page_rows.target,
                        max_txids: 0,
                    },
                )
            })
            .collect(),
        shards: entries,
    };
    map.check_shape()
        .map_err(|error| format!("the published map is malformed: {error}"))?;
    // Written last, and atomically. The map is what names every shard, so a
    // truncated one is a set that cannot be loaded at all; and until it names
    // them, the shard directories beside it are simply not part of any set.
    write_atomic(
        &cli.output.join("shards.json"),
        &serde_json::to_vec_pretty(&map)?,
    )?;

    let filter_bytes: u64 = map.shards.iter().map(|s| s.scripts * 5 / 2).sum();
    eprintln!(
        "published {} shards covering {}-{}; filters ~{:.2} MB",
        map.shards.len(),
        first,
        covered,
        filter_bytes as f64 / 1e6
    );
    Ok(())
}
