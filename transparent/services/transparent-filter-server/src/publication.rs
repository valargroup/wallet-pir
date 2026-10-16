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

use crate::events::EventStore;
use clap::Parser;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use transparent_filter::{filter_hash, BlockHash, ScriptBytes};

use transparent_shard::build::{Built, BuiltChild, BuiltDirectory, ShardStream};
use transparent_shard::layout::{by_name as geometry_by_name, Geometry};
use transparent_shard::manifest::{
    ManifestLayout, ManifestOccupancy, ManifestSeal, PageChild, PublishedRevision, ShardManifest,
    TableGeometry, SCHEMA,
};
use transparent_shard::seal::{DecoupledPolicy, SealedDirectory};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(name = "shard-publish", about = "Publish a transparent shard set")]
pub struct PublishOptions {
    #[arg(long, default_value = "./transparent-event-data")]
    pub data_dir: PathBuf,
    /// Continue a verified publication, reusing its unchanged sealed prefix.
    #[arg(long)]
    pub previous: Option<PathBuf>,
    #[arg(long, default_value = "./transparent-shards")]
    pub output: PathBuf,
    /// Geometry for history at and after `--recent-from`.
    ///
    /// The window wallets synchronise constantly, so it is sized for a small
    /// query rather than for holding the most content.
    #[arg(long, default_value = "decoupled-2k-8k")]
    pub recent_geometry: String,
    /// Geometry for history before `--recent-from`.
    ///
    /// Defaults to the recent geometry, which publishes a single-geometry set —
    /// what every existing published set is. Give both this and `--recent-from`
    /// to publish the two-tier set the deployment plan describes.
    #[arg(long)]
    pub archive_geometry: Option<String>,
    /// First height of the recent tier.
    ///
    /// A forced shard boundary: a shard's rows are addressed at one row count,
    /// so no shard may span the change. Derive it from the pinned anchor's
    /// chain timestamp rather than from a block count — a height standing in
    /// for "six months" drifts with the interval, and re-deriving it later
    /// would re-shard the chain.
    #[arg(long)]
    pub recent_from: Option<u64>,
    /// Needed for exactly one thing: the block hash before the journal's first
    /// height, which is shard zero's parent and is by definition not in the
    /// journal.
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    pub zakura_rpc_url: String,
    #[arg(long)]
    pub zakura_cookie: PathBuf,
    /// Last height to publish, inclusive. Defaults to the journal's end.
    ///
    /// The anchor a publication is pinned to. A journal keeps growing under
    /// ingest, and a publish that silently took whatever it found would move
    /// the anchor every run; naming it keeps the published range the one the
    /// cutoff record and the correctness pilot were computed for.
    #[arg(long)]
    pub through: Option<u64>,
    /// Where to write a JSON record of this publication: inputs, journal
    /// identity, range, geometries, map digest, shard count and elapsed time.
    #[arg(long)]
    pub record: Option<PathBuf>,
    /// Source commit of this tool, recorded verbatim.
    #[arg(long)]
    pub source_sha: Option<String>,
    /// Which newly built shards publish a directory choice table.
    ///
    /// `off` publishes none, which is every set before this option. `sealed`
    /// adds one to shards as they seal, `all` to tail revisions too. A shard
    /// whose published revision is reproduced keeps whatever it published, so
    /// turning this on never changes an existing digest.
    #[arg(long, value_enum, default_value_t = DirectoryChoice::Off)]
    pub directory_choice: DirectoryChoice,
    /// Range-filter profile every shard is published under.
    ///
    /// Fixes the filters' Golomb-Rice parameters and is named in the map and
    /// every manifest. Changing it changes every filter, so a publication
    /// under a new profile cannot continue a previous one and must be written
    /// into a directory of its own.
    #[arg(long, default_value = transparent_filter::RANGE_PROFILE)]
    pub range_profile: String,
}

/// Where the publisher publishes directory choice tables.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    clap::ValueEnum,
    serde::Deserialize,
    serde::Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum DirectoryChoice {
    #[default]
    Off,
    Sealed,
    All,
}

/// Writes `bytes` to `path` so that a reader sees either all of them or none.
///
/// Temp file, fsync, rename. A published set is read back by a service that
/// refuses to start on a file that does not match its digest, so a half-written
/// file is not a corruption a reader has to detect — but only if the partial
/// state is never visible under the final name. `std::fs::write` truncates in
/// place, so an interrupted publish leaves a short file *at the name the map
/// points to*, which is exactly the state a deploy would then try to ship.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), BoxError> {
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

pub(crate) fn write_immutable(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), BoxError> {
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

pub fn publish(
    cli: &PublishOptions,
    store: &impl Journal,
    base_parent: BlockHash,
) -> Result<transparent_filter::ShardMap, BoxError> {
    publish_with(cli, store, base_parent, &DecoupledPolicy::for_geometry)
}

/// [`publish`] under the seal policy `policy_of` gives each geometry.
///
/// A published set seals under the policy its geometry implies; tests use
/// small ones so a few blocks close page shards and directory shards.
pub fn publish_with(
    cli: &PublishOptions,
    store: &impl Journal,
    base_parent: BlockHash,
    policy_of: &dyn Fn(&Geometry) -> DecoupledPolicy,
) -> Result<transparent_filter::ShardMap, BoxError> {
    if store.journal_version() != 3 {
        return Err("v12 publication requires a v3 journal with transaction metadata".into());
    }
    let started = std::time::Instant::now();
    let Some(journal_end) = store.covered_through() else {
        return Err("the journal is empty".into());
    };
    let first = store.start_height();
    let covered = match cli.through {
        Some(through) if through > journal_end => {
            return Err(format!(
                "--through {through} is past the journal's committed end {journal_end}"
            )
            .into())
        }
        Some(through) if through < first => {
            return Err(format!("--through {through} is before the journal's start {first}").into())
        }
        Some(through) => through,
        None => journal_end,
    };

    if transparent_filter::range_profile(&cli.range_profile).is_none() {
        return Err(format!("unknown range profile {:?}", cli.range_profile).into());
    }
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
            return Err(
                "--recent-from needs an --archive-geometry that differs from --recent-geometry"
                    .into(),
            )
        }
        (None, false) => {
            return Err(
                "--archive-geometry needs --recent-from to say where the tiers divide".into(),
            )
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
    let archive_policy = policy_of(archive);
    let recent_policy = policy_of(recent);
    let genesis = BlockHash::from_display_hex(store.genesis_hash())?;

    std::fs::create_dir_all(&cli.output)?;
    match cutoff {
        None => eprintln!(
            "journal {first}-{covered}, all {} sealing at {recent_policy:?}",
            recent.name
        ),
        Some(height) => eprintln!(
            "journal {first}-{covered}, {} below {height} at {archive_policy:?}, \
             {} from {height} at {recent_policy:?}",
            archive.name, recent.name
        ),
    }

    let mut entries: Vec<transparent_filter::ShardMapEntry> = Vec::new();
    let mut parent_manifest_digest = String::new();
    let mut parent_block_hash = base_parent;

    let previous_dir = cli.previous.as_ref().unwrap_or(&cli.output);
    // The map from a previous run, if any. It is what tells a republished tail
    // which revision it supersedes; without it a growing tail would look like a
    // first publication every time.
    let mut previous: std::collections::BTreeMap<u64, transparent_filter::ShardMapEntry> =
        match std::fs::read(previous_dir.join("shards.json")) {
            Ok(raw) => serde_json::from_slice::<transparent_filter::ShardMap>(&raw)
                // A map this build cannot read is a set published under another
                // schema. Publishing beside it would leave two incompatible
                // sets in one directory, which the loader refuses to serve, so
                // say what to do instead of reporting a parse error.
                .map_err(|error| {
                    format!(
                        "{} was published under a schema this build cannot read \
                         ({error}); publish into a directory of its own",
                        previous_dir.join("shards.json").display()
                    )
                })?
                .pipe_validate(
                    store,
                    recent,
                    archive,
                    cutoff,
                    &cli.range_profile,
                    policy_of,
                )?
                .shards
                .into_iter()
                .map(|entry| (entry.shard_id, entry))
                .collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Default::default(),
            Err(error) => return Err(error.into()),
        };

    // Preserve sealed directory shards, with every page shard they hold, only
    // while their chain endpoints agree. A changed sealed suffix belongs to a
    // separate publication directory. The open directory shard is rebuilt
    // from its start: the sealer resumes there, never at a page-shard
    // boundary, and its sealed page shards come out byte-identical.
    let mut resume_height = first;
    for entry in previous.values() {
        if !entry.sealed
            || entry.end_height > covered
            || store
                .block_at(entry.end_height)
                .is_none_or(|b| b.block_hash.to_display_hex() != entry.terminal_block_hash)
        {
            break;
        }
        if entry.start_height != resume_height {
            return Err("previous map is not contiguous".into());
        }
        link_revision(previous_dir, &cli.output, &entry.manifest_digest)?;
        entries.push(entry.clone());
        resume_height = entry.end_height + 1;
        parent_manifest_digest = entry.manifest_digest.clone();
        parent_block_hash = BlockHash::from_display_hex(&entry.terminal_block_hash)?;
    }
    let reorg = previous.get(&(entries.len() as u64)).is_some_and(|e| {
        e.sealed
            || e.end_height > covered
            || store
                .block_at(e.end_height)
                .is_none_or(|b| b.block_hash.to_display_hex() != e.terminal_block_hash)
    });
    if reorg && previous_dir == &cli.output {
        return Err("reorg replacement requires a separate output directory".into());
    }
    if reorg {
        previous.retain(|id, _| *id < entries.len() as u64);
    }
    // The published page shards of the open directory shard's latest
    // revision, so a sealed page shard rebuilt identically is linked rather
    // than rewritten.
    let open_previous: Option<(PathBuf, ShardManifest)> =
        match previous.get(&(entries.len() as u64)) {
            Some(entry) if !entry.sealed => {
                let dir = previous_dir.join(&entry.manifest_digest);
                let manifest: ShardManifest =
                    serde_json::from_slice(&std::fs::read(dir.join("manifest.json"))?)?;
                Some((dir, manifest))
            }
            _ => None,
        };

    let geometry_at = |height: u64| -> &'static Geometry {
        if cutoff.is_some_and(|h| height < h) {
            archive
        } else {
            recent
        }
    };
    let mut stream = ShardStream::with_policy(
        geometry_at(resume_height),
        policy_of(geometry_at(resume_height)),
        resume_height,
        entries.len() as u64,
        genesis,
        &cli.range_profile,
    )?;
    let staging = cli.output.join(".staging");
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }
    let mut children: Vec<PageChild> = Vec::new();

    let terminal = |height: u64| store.block_at(height).map(|block| block.block_hash);
    let publish_built = |built: Vec<Built>,
                         children: &mut Vec<PageChild>,
                         entries: &mut Vec<transparent_filter::ShardMapEntry>,
                         parent_block_hash: &mut BlockHash,
                         parent_manifest_digest: &mut String|
     -> Result<(), BoxError> {
        for item in built {
            match item {
                Built::Child(child) => {
                    let geometry = geometry_at(child.start_height);
                    children.push(stage_child(
                        &staging,
                        &child,
                        geometry,
                        open_previous
                            .as_ref()
                            .filter(|(_, manifest)| manifest.shard_id == child.shard_id),
                    )?);
                }
                Built::Directory { built, sealed } => {
                    let policy = policy_of(built.geometry);
                    let entry = publish_directory(
                        cli,
                        store,
                        &previous,
                        previous_dir,
                        &staging,
                        built,
                        &sealed,
                        std::mem::take(children),
                        policy,
                        *parent_block_hash,
                        parent_manifest_digest,
                    )?;
                    *parent_manifest_digest = entry.manifest_digest.clone();
                    *parent_block_hash = BlockHash::from_display_hex(&entry.terminal_block_hash)?;
                    entries.push(entry);
                }
            }
        }
        Ok(())
    };

    for height in resume_height..=covered {
        // The tiers divide at a height, so the archive directory shard is
        // closed before the first recent block is offered to anything. A
        // shard's rows are addressed at one row count; one spanning the change
        // could not be read at either. Ids continue across the change.
        if cutoff == Some(height) && stream.geometry().name != recent.name {
            let built =
                stream.change_geometry_with_policy(recent, recent_policy, height, &terminal)?;
            publish_built(
                built,
                &mut children,
                &mut entries,
                &mut parent_block_hash,
                &mut parent_manifest_digest,
            )?;
        }
        let events = store
            .events_at(height)?
            .ok_or_else(|| format!("height {height} is missing from the journal"))?;
        if events.iter().any(|(_, event)| event.metadata().is_none()) {
            return Err(
                format!("height {height} lacks transaction metadata required by v12").into(),
            );
        }
        let built = stream.push_block((height, events), &terminal)?;
        publish_built(
            built,
            &mut children,
            &mut entries,
            &mut parent_block_hash,
            &mut parent_manifest_digest,
        )?;
    }
    let built = stream.finish(&terminal)?;
    publish_built(
        built,
        &mut children,
        &mut entries,
        &mut parent_block_hash,
        &mut parent_manifest_digest,
    )?;
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }

    let map = transparent_filter::ShardMap {
        genesis_hash: store.genesis_hash().to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: cli.range_profile.clone(),
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
                (name, seal_parameters(&used))
            })
            .collect(),
        shards: entries,
        // This publisher never re-cuts. A re-cutting one must carry every
        // earlier declaration forward, with seal parameters for each geometry
        // it names, or wallets holding superseded revisions stop recognizing
        // them.
        recuts: Vec::new(),
    };
    if !reorg && previous_dir != &cli.output && previous_dir.exists() {
        link_superseded_tails(previous_dir, &cli.output, &map.shards, store)?;
    }
    map.validate_supported()
        .map_err(|error| format!("the published map is malformed: {error}"))?;
    // Written last, and atomically. The map is what names every shard, so a
    // truncated one is a set that cannot be loaded at all; and until it names
    // them, the shard directories beside it are simply not part of any set.
    let map_bytes = serde_json::to_vec_pretty(&map)?;
    write_atomic(&cli.output.join("shards.json"), &map_bytes)?;

    if let Some(path) = &cli.record {
        let by_geometry = |name: &str| {
            map.shards
                .iter()
                .filter(|entry| entry.geometry == name)
                .count()
        };
        let record = serde_json::json!({
            "schema": "transparent-publication-v1",
            "generated_at": chrono::Utc::now().to_rfc3339(),
            "tool_sha": cli.source_sha,
            "data_dir": cli.data_dir,
            "output": cli.output,
            "network": transparent_filter::NETWORK,
            "genesis_hash": store.genesis_hash(),
            "shard_schema": SCHEMA,
            "journal": {
                "start_height": first,
                "covered_through": journal_end,
                "events_stored": store.events_stored(),
            },
            "published": {
                "start_height": first,
                "through": covered,
                "anchor_hash": store
                    .block_at(covered)
                    .map(|entry| entry.block_hash.to_display_hex()),
                "recent_from": cutoff,
                "recent_geometry": recent.name,
                "archive_geometry": cutoff.map(|_| archive.name),
                "recent_policy": format!("{recent_policy:?}"),
                "archive_policy": cutoff.map(|_| format!("{archive_policy:?}")),
                "shards": map.shards.len(),
                "page_shards": map.shards.iter().map(|entry| u64::from(entry.page_segments)).sum::<u64>(),
                "recent_shards": by_geometry(recent.name),
                "archive_shards": cutoff.map(|_| by_geometry(archive.name)),
                "multi_segment_shards": map
                    .shards
                    .iter()
                    .filter(|entry| entry.directory_segments > 1)
                    .count(),
                "tail_revision": map.shards.last().map(|entry| entry.revision),
                "map_sha256": hex::encode(Sha256::digest(&map_bytes)),
                "map_bytes": map_bytes.len(),
            },
            "elapsed_seconds": started.elapsed().as_secs_f64(),
        });
        write_atomic(path, &serde_json::to_vec_pretty(&record)?)?;
    }

    let filter_bytes: u64 = map.shards.iter().map(|s| s.scripts * 5 / 2).sum();
    eprintln!(
        "published {} shards covering {}-{}; filters ~{:.2} MB",
        map.shards.len(),
        first,
        covered,
        filter_bytes as f64 / 1e6
    );
    Ok(map)
}

/// What the map publishes about a policy.
fn seal_parameters(policy: &DecoupledPolicy) -> transparent_filter::SealParameters {
    transparent_filter::SealParameters {
        max_scripts: policy.scripts.target,
        max_page_rows: policy.page_rows.target,
        max_txids: 0,
    }
}

/// The file one segment of page shard `child` is published under.
pub fn page_segment_file(child: u32, segment: usize) -> String {
    format!(
        "{}.{segment}.bin",
        transparent_shard::manifest::pages_table(child)
    )
}

/// Writes a page shard's segments into the staging directory of its
/// directory shard and describes it.
///
/// A page shard the open directory shard's latest revision already
/// published with the same identity is linked from there instead of
/// rewritten: sealed page shards come out of every rebuild byte-identical,
/// and an archive one is 128 MiB a segment.
fn stage_child(
    staging: &Path,
    child: &BuiltChild,
    geometry: &Geometry,
    previous: Option<&(PathBuf, ShardManifest)>,
) -> Result<PageChild, BoxError> {
    let described = child.describe(geometry);
    let dir = staging.join(child.shard_id.to_string());
    std::fs::create_dir_all(&dir)?;
    let reusable = previous.filter(|(_, manifest)| {
        manifest
            .page_children
            .get(child.index as usize)
            .is_some_and(|published| *published == described)
    });
    for segment in 0..child.segments as usize {
        let name = page_segment_file(child.index, segment);
        let target = dir.join(&name);
        if let Some((published, _)) = reusable {
            if std::fs::hard_link(published.join(&name), &target).is_ok() {
                continue;
            }
        }
        write_atomic(&target, &child.segment(geometry, segment as u32))?;
    }
    Ok(described)
}

/// Writes one directory shard revision: its manifest, filter, directory
/// segments and the staged page shards, and returns its map entry.
#[allow(clippy::too_many_arguments)]
fn publish_directory(
    cli: &PublishOptions,
    store: &impl Journal,
    previous: &std::collections::BTreeMap<u64, transparent_filter::ShardMapEntry>,
    previous_dir: &Path,
    staging: &Path,
    built: BuiltDirectory,
    sealed: &SealedDirectory,
    children: Vec<PageChild>,
    policy: DecoupledPolicy,
    parent_block_hash: BlockHash,
    parent_manifest_digest: &str,
) -> Result<transparent_filter::ShardMapEntry, BoxError> {
    let geometry = built.geometry;
    let terminal = store
        .block_at(built.end_height)
        .ok_or("shard end is not covered")?
        .block_hash;
    let shard_sealed = sealed.reason.is_some();
    let wanted_choice = match cli.directory_choice {
        DirectoryChoice::Off => false,
        DirectoryChoice::Sealed => shard_sealed,
        DirectoryChoice::All => true,
    };
    if wanted_choice && built.choice.is_none() {
        eprintln!(
            "shard {} has no directory choice table: no seed peeled; \
             publishing it for two directory queries",
            built.shard_id
        );
    }
    let filter_digest = filter_hash(built.filter.as_slice()).to_display_hex();
    let directory_segments: Vec<TableGeometry> = built
        .directory
        .iter()
        .map(|segment| TableGeometry {
            rows: geometry.directory_rows,
            row_bytes: geometry.directory_row_bytes as u32,
            sha256: hex::encode(Sha256::digest(segment)),
        })
        .collect();
    let page_rows: u64 = children.iter().map(|c| c.occupancy.page_rows).sum();
    let fragments: u64 = children.iter().map(|c| c.occupancy.fragments).sum();
    let make = |revision: u32, supersedes: String, with_choice: bool| ShardManifest {
        schema: SCHEMA.to_string(),
        profile: cli.range_profile.clone(),
        geometry: geometry.name.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        genesis_hash: store.genesis_hash().to_string(),
        shard_id: built.shard_id,
        start_height: built.start_height,
        end_height: built.end_height,
        parent_block_hash: parent_block_hash.to_display_hex(),
        terminal_block_hash: terminal.to_display_hex(),
        tag_salt_counter: built.tag_salt_counter,
        parent_manifest_digest: parent_manifest_digest.to_string(),
        sealed: shard_sealed,
        revision,
        supersedes,
        seal: ManifestSeal {
            scripts_target: policy.scripts.target,
            scripts_capacity: policy.scripts.capacity,
            directory_bytes_target: policy.directory_bytes.target,
            directory_bytes_capacity: policy.directory_bytes.capacity,
            page_rows_target: policy.page_rows.target,
            page_rows_capacity: policy.page_rows.capacity,
            max_page_children: policy.max_children,
        },
        layout: ManifestLayout::current(),
        filter_hash: filter_digest.clone(),
        directory_segments: directory_segments.clone(),
        page_children: children.clone(),
        occupancy: ManifestOccupancy {
            scripts: built.scripts,
            page_rows,
            fragments,
            events: built.events,
            blocks: sealed.occupancy.blocks,
            txids: built.txids,
            excluded_scripts: built.excluded_scripts,
        },
        directory_choice: built
            .choice
            .as_ref()
            .filter(|_| with_choice)
            .map(transparent_shard::manifest::encode_directory_choice),
    };

    // A shard already in the previous map is being republished. Building it
    // under the published numbering first says which kind of republication
    // this is: one that reproduces the published digest is the same shard
    // again and keeps its identity, while one that does not is a tail that
    // has grown and takes the next revision.
    let published = match previous.get(&built.shard_id) {
        None => None,
        Some(entry) => {
            let raw = std::fs::read(
                previous_dir
                    .join(&entry.manifest_digest)
                    .join("manifest.json"),
            )?;
            let published: ShardManifest = serde_json::from_slice(&raw)?;
            Some((
                PublishedRevision {
                    digest: entry.manifest_digest.clone(),
                    revision: published.revision,
                    supersedes: published.supersedes,
                    sealed: published.sealed,
                },
                published.directory_choice.is_some(),
            ))
        }
    };
    // Reproduction is judged against what the published revision chose, so
    // the same content keeps its identity whatever this run's option says.
    let reproduced = published.as_ref().is_some_and(|(previous, had_choice)| {
        make(previous.revision, previous.supersedes.clone(), *had_choice).digest()
            == previous.digest
    });
    let with_choice = match &published {
        Some((_, had_choice)) if reproduced => *had_choice,
        _ => wanted_choice,
    };
    let published = published.map(|(previous, _)| previous);
    let (revision, supersedes) =
        PublishedRevision::next(built.shard_id, published.as_ref(), reproduced)?;
    let manifest = make(revision, supersedes, with_choice);
    // The builder and sealer agreed on every page shard; this is the shape
    // every loader and wallet checks, so a set that fails it is not written.
    manifest
        .check_children(geometry.page_rows, geometry.page_row_bytes as u32)
        .map_err(|error| format!("shard {}: {error}", built.shard_id))?;

    let digest = manifest.digest();
    let dir = cli.output.join(&digest);
    std::fs::create_dir_all(&dir)?;
    write_immutable(&dir, "manifest.json", &manifest.canonical_bytes())?;
    write_immutable(&dir, "filter.bin", built.filter.as_slice())?;
    // One file per segment, in segment order. A shard normally has one;
    // more means its content did not fit a single segment.
    for (index, segment) in built.directory.iter().enumerate() {
        write_immutable(&dir, &format!("directory.{index}.bin"), segment)?;
    }
    let staged = staging.join(built.shard_id.to_string());
    for child in &manifest.page_children {
        for (segment, published) in child.page_segments.iter().enumerate() {
            let name = page_segment_file(child.index, segment);
            let target = dir.join(&name);
            if target.exists() {
                // Published files are immutable; a file already under this
                // revision must be the one its manifest names.
                if hex::encode(sha256_file(&target)?) != published.sha256 {
                    return Err(format!(
                        "{} already exists with different content",
                        target.display()
                    )
                    .into());
                }
                continue;
            }
            std::fs::rename(staged.join(&name), &target)?;
        }
    }
    if staged.exists() {
        std::fs::remove_dir_all(&staged)?;
    }

    eprintln!(
        "shard {:>3} {}-{} ({} blocks) scripts {} page shards {} pages {} events {} {}",
        built.shard_id,
        built.start_height,
        built.end_height,
        sealed.occupancy.blocks,
        built.scripts,
        manifest.page_children.len(),
        page_rows,
        built.events,
        if manifest.sealed {
            "sealed".to_string()
        } else {
            format!("TAIL r{revision}")
        },
    );

    Ok(transparent_filter::ShardMapEntry {
        shard_id: built.shard_id,
        geometry: geometry.name.to_string(),
        start_height: built.start_height,
        end_height: built.end_height,
        parent_block_hash: manifest.parent_block_hash.clone(),
        terminal_block_hash: manifest.terminal_block_hash.clone(),
        filter_hash: manifest.filter_hash.clone(),
        scripts: built.scripts,
        page_rows,
        txids: built.txids,
        directory_segments: built.directory_segments(),
        // A directory shard's page shards stand where v11 counted page
        // segments: a wallet needs the count before it fetches the manifest.
        page_segments: manifest.page_children.len() as u32,
        manifest_digest: digest,
        revision,
        sealed: manifest.sealed,
    })
}

/// SHA-256 of a file, streamed.
fn sha256_file(path: &Path) -> Result<[u8; 32], BoxError> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

/// Immutable journal view used by both the one-shot tool and controller snapshots.
pub trait Journal {
    fn journal_version(&self) -> u16;
    fn display_at(
        &self,
        _height: u64,
    ) -> Result<Vec<transparent_shard::txid::DisplayRecord>, crate::events::EventStoreError> {
        Err(crate::events::EventStoreError::Invariant(
            "journal has no display capability".into(),
        ))
    }
    fn genesis_hash(&self) -> &str;
    fn start_height(&self) -> u64;
    fn covered_through(&self) -> Option<u64>;
    fn events_stored(&self) -> u64;
    fn block_at(&self, height: u64) -> Option<crate::events::BlockEntry>;
    fn events_at(
        &self,
        height: u64,
    ) -> Result<
        Option<Vec<(ScriptBytes, transparent_events::TransparentEvent)>>,
        crate::events::EventStoreError,
    >;
}
impl Journal for EventStore {
    fn journal_version(&self) -> u16 {
        self.version()
    }
    fn display_at(
        &self,
        height: u64,
    ) -> Result<Vec<transparent_shard::txid::DisplayRecord>, crate::events::EventStoreError> {
        self.display_at(height)
    }
    fn genesis_hash(&self) -> &str {
        self.genesis_hash()
    }
    fn start_height(&self) -> u64 {
        self.start_height()
    }
    fn covered_through(&self) -> Option<u64> {
        self.covered_through()
    }
    fn events_stored(&self) -> u64 {
        self.events_stored()
    }
    fn block_at(&self, height: u64) -> Option<crate::events::BlockEntry> {
        self.block_at(height)
    }
    fn events_at(
        &self,
        height: u64,
    ) -> Result<
        Option<Vec<(ScriptBytes, transparent_events::TransparentEvent)>>,
        crate::events::EventStoreError,
    > {
        self.events_at(height)
    }
}

/// Carries the newest three superseded tail revisions of each shard forward
/// from `previous`, for wallets still holding them, while their endpoints are
/// on the journal's chain.
///
/// Every manifest in `previous` is read once. A set holds a manifest per shard,
/// and archive manifests carry choice tables of hundreds of kilobytes, so
/// reading them all again for every shard was quadratic in the set's size.
fn link_superseded_tails(
    previous: &Path,
    output: &Path,
    shards: &[transparent_filter::ShardMapEntry],
    store: &impl Journal,
) -> Result<(), BoxError> {
    if shards.is_empty() {
        return Ok(());
    }
    let mut superseded = Vec::new();
    for path in std::fs::read_dir(previous)? {
        let path = path?.path();
        if !path.is_dir() {
            continue;
        }
        let Ok(raw) = std::fs::read(path.join("manifest.json")) else {
            continue;
        };
        let Ok(manifest) = serde_json::from_slice::<ShardManifest>(&raw) else {
            continue;
        };
        if !manifest.sealed
            && store
                .block_at(manifest.end_height)
                .is_some_and(|b| b.block_hash.to_display_hex() == manifest.terminal_block_hash)
        {
            superseded.push((manifest.shard_id, manifest.revision, manifest.digest()));
        }
    }
    for entry in shards {
        // Directory order, then a stable sort: the order the per-shard scan
        // retained them in.
        let mut retained: Vec<_> = superseded
            .iter()
            .filter(|(shard_id, revision, _)| {
                *shard_id == entry.shard_id && *revision < entry.revision
            })
            .map(|(_, revision, digest)| (*revision, digest))
            .collect();
        retained.sort_by_key(|entry| std::cmp::Reverse(entry.0));
        for (_, digest) in retained.into_iter().take(3) {
            link_revision(previous, output, digest)?;
        }
    }
    Ok(())
}

/// Copies immutable revision files without rebuilding their contents.
pub fn link_revision(from: &Path, to: &Path, digest: &str) -> Result<(), BoxError> {
    if from == to {
        return Ok(());
    }
    let target = to.join(digest);
    std::fs::create_dir_all(&target)?;
    for entry in std::fs::read_dir(from.join(digest))? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err("unexpected revision artifact".into());
        }
        let dest = target.join(entry.file_name());
        if dest.exists() {
            continue;
        }
        if std::fs::hard_link(entry.path(), &dest).is_err() {
            std::fs::copy(entry.path(), &dest)?;
        }
    }
    Ok(())
}

trait ValidatePrevious {
    fn pipe_validate(
        self,
        store: &impl Journal,
        recent: &Geometry,
        archive: &Geometry,
        cutoff: Option<u64>,
        range_profile: &str,
        policy_of: &dyn Fn(&Geometry) -> DecoupledPolicy,
    ) -> Result<Self, BoxError>
    where
        Self: Sized;
}
impl ValidatePrevious for transparent_filter::ShardMap {
    fn pipe_validate(
        self,
        store: &impl Journal,
        recent: &Geometry,
        archive: &Geometry,
        cutoff: Option<u64>,
        range_profile: &str,
        policy_of: &dyn Fn(&Geometry) -> DecoupledPolicy,
    ) -> Result<Self, BoxError> {
        self.validate_supported()?;
        if self.genesis_hash != store.genesis_hash()
            || self.start_height != store.start_height()
            || self.network != transparent_filter::NETWORK
            || self.profile != range_profile
        {
            return Err("previous publication identity mismatch".into());
        }
        for e in &self.shards {
            let g = if cutoff.is_some_and(|h| e.start_height < h) {
                archive
            } else {
                recent
            };
            if e.geometry != g.name
                || cutoff.is_some_and(|h| e.start_height < h && e.end_height >= h)
            {
                return Err("previous publication cutoff/geometry mismatch".into());
            }
            let seal = self
                .seal
                .get(g.name)
                .ok_or("previous publication lacks seal parameters")?;
            if *seal != seal_parameters(&policy_of(g)) {
                return Err("previous publication seal policy mismatch".into());
            }
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::Snapshot;
    fn options(output: &Path, previous: Option<&Path>) -> PublishOptions {
        PublishOptions {
            data_dir: PathBuf::new(),
            output: output.to_owned(),
            previous: previous.map(Path::to_owned),
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: Some("decoupled-32k-32k".into()),
            recent_from: Some(2),
            zakura_rpc_url: String::new(),
            zakura_cookie: PathBuf::new(),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
        }
    }
    fn block(store: &mut EventStore, h: u64, tag: u8) {
        use transparent_events::{ReceiveEvent, TransparentEvent};
        let events = vec![(
            ScriptBytes::new(vec![0x51, tag]),
            TransparentEvent::Receive(ReceiveEvent {
                metadata: Some(transparent_events::TransactionMetadata {
                    fee: transparent_events::FeeState::Exact(0),
                    transparent_input_count: 1,
                    has_shielded_components: false,
                }),
                height: h as u32,
                txid: transparent_events::Txid([tag; 32]),
                transaction_index: 0,
                output_index: 0,
                value: 100,
                coinbase: false,
            }),
        )];
        store
            .append_block(h, BlockHash::from_internal_bytes([tag; 32]), &events)
            .unwrap();
        store.commit().unwrap();
    }
    #[test]
    fn incremental_extension_reuses_sealed_prefix_and_matches_full_tables() {
        let root = tempfile::tempdir().unwrap();
        let mut journal =
            EventStore::open(root.path().join("journal"), &"00".repeat(32), 0).unwrap();
        for h in 0..4 {
            block(&mut journal, h, h as u8 + 1);
        }
        let a = root.path().join("a");
        let b = root.path().join("b");
        let full = root.path().join("full");
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let before = publish(&options(&a, None), &journal, zero).unwrap();
        assert!(before.shards[0].sealed);
        assert!(!before.shards[1].sealed);
        block(&mut journal, 4, 5);
        let snapshot = Snapshot::capture(&journal, &before, root.path()).unwrap();
        let after = publish(&options(&b, Some(&a)), &snapshot, zero).unwrap();
        let reference = publish(&options(&full, None), &journal, zero).unwrap();
        assert_eq!(before.shards[0], after.shards[0]);
        assert_eq!(after.shards[1].revision, 1);
        assert_eq!(after.shards[1].end_height, 4);
        let read = |dir: &Path, e: &transparent_filter::ShardMapEntry| -> ShardManifest {
            serde_json::from_slice(
                &std::fs::read(dir.join(&e.manifest_digest).join("manifest.json")).unwrap(),
            )
            .unwrap()
        };
        let grown = read(&b, &after.shards[1]);
        let rebuilt = read(&full, &reference.shards[1]);
        assert_eq!(grown.directory_segments, rebuilt.directory_segments);
        assert_eq!(grown.page_children, rebuilt.page_children);
        assert_eq!(grown.supersedes, before.shards[1].manifest_digest);
        assert!(b.join(&before.shards[1].manifest_digest).exists());
        let repeated = publish(&options(&b, None), &journal, zero).unwrap();
        assert_eq!(after, repeated);
        transparent_server_backend::shardset::ShardSet::open(&b, 3).unwrap();
    }
    /// A block where a few busy scripts receive several times, so events are
    /// pushed out of the directory every block, and a new script appears.
    fn busy_block(store: &mut EventStore, h: u64, fork: u8) {
        use transparent_events::{ReceiveEvent, TransparentEvent};
        let mut events = Vec::new();
        for k in 0..4u8 {
            let tag = if k == 3 { 100 + (h % 150) as u8 } else { k };
            let mut txid = [fork; 32];
            txid[..8].copy_from_slice(&h.to_le_bytes());
            txid[8] = k;
            events.push((
                ScriptBytes::new(vec![0x51, tag]),
                TransparentEvent::Receive(ReceiveEvent {
                    metadata: Some(transparent_events::TransactionMetadata {
                        fee: transparent_events::FeeState::Exact(0),
                        transparent_input_count: 1,
                        has_shielded_components: false,
                    }),
                    height: h as u32,
                    txid: transparent_events::Txid(txid),
                    transaction_index: u16::from(k),
                    output_index: 0,
                    value: 100,
                    coinbase: false,
                }),
            ));
        }
        let mut hash = [fork; 32];
        hash[..8].copy_from_slice(&h.to_le_bytes());
        store
            .append_block(h, BlockHash::from_internal_bytes(hash), &events)
            .unwrap();
        store.commit().unwrap();
    }

    /// Small enough that a few dozen blocks close many page shards and
    /// several directory shards, by page rows, by the child limit and by
    /// directory bytes.
    fn small(geometry: &Geometry) -> DecoupledPolicy {
        use transparent_shard::seal::Limit;
        DecoupledPolicy {
            scripts: Limit::new(60, 400).unwrap(),
            directory_bytes: Limit::new(2_500, 40_000).unwrap(),
            page_rows: Limit::new(2, 4).unwrap(),
            max_children: 3,
            ..DecoupledPolicy::for_geometry(geometry)
        }
    }

    fn single_tier(output: &Path, previous: Option<&Path>, through: u64) -> PublishOptions {
        PublishOptions {
            archive_geometry: None,
            recent_from: None,
            through: Some(through),
            ..options(output, previous)
        }
    }

    fn read_manifest(dir: &Path, e: &transparent_filter::ShardMapEntry) -> ShardManifest {
        serde_json::from_slice(
            &std::fs::read(dir.join(&e.manifest_digest).join("manifest.json")).unwrap(),
        )
        .unwrap()
    }

    /// Publishing block by block, each candidate continuing the last, is a
    /// fresh build: every directory shard and page shard has the same
    /// content, differing only in revision numbering. Sealed page
    /// shards of the open directory shard are carried by link, not rewritten.
    #[test]
    fn block_by_block_publication_equals_a_fresh_build() {
        use std::os::unix::fs::MetadataExt;
        let root = tempfile::tempdir().unwrap();
        let mut journal =
            EventStore::open(root.path().join("journal"), &"00".repeat(32), 0).unwrap();
        for h in 0..70 {
            busy_block(&mut journal, h, 0);
        }
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let mut previous: Option<PathBuf> = None;
        let mut map = None;
        let mut linked = 0;
        for through in (5..70).step_by(3).chain([69]) {
            let output = root.path().join(format!("c{through}"));
            let published = publish_with(
                &single_tier(&output, previous.as_deref(), through),
                &journal,
                zero,
                &small,
            )
            .unwrap();
            transparent_server_backend::shardset::ShardSet::open(&output, 3).unwrap();
            if let Some(before) = &previous {
                // A sealed page shard of the open directory shard that the
                // last candidate published is the same file in this one.
                let tail = published.shards.last().unwrap();
                let grown = read_manifest(&output, tail);
                let old_map: transparent_filter::ShardMap =
                    serde_json::from_slice(&std::fs::read(before.join("shards.json")).unwrap())
                        .unwrap();
                if let Some(old) = old_map.shards.get(tail.shard_id as usize) {
                    let old_manifest = read_manifest(before, old);
                    for (child, was) in grown.page_children.iter().zip(&old_manifest.page_children)
                    {
                        if child == was && was.sealed && !was.page_segments.is_empty() {
                            let name = page_segment_file(child.index, 0);
                            let a =
                                std::fs::metadata(before.join(&old.manifest_digest).join(&name))
                                    .unwrap();
                            let b =
                                std::fs::metadata(output.join(&tail.manifest_digest).join(&name))
                                    .unwrap();
                            assert_eq!((a.dev(), a.ino()), (b.dev(), b.ino()));
                            linked += 1;
                        }
                    }
                }
            }
            previous = Some(output);
            map = Some(published);
        }
        assert!(linked > 0, "some sealed page shard was carried forward");
        let map = map.unwrap();
        let fresh_dir = root.path().join("fresh");
        let fresh =
            publish_with(&single_tier(&fresh_dir, None, 69), &journal, zero, &small).unwrap();
        assert!(fresh.shards.len() > 2, "several directory shards");
        let incremental_dir = previous.unwrap();
        assert_eq!(map.shards.len(), fresh.shards.len());
        let mut children = 0;
        for (a, b) in map.shards.iter().zip(&fresh.shards) {
            let mut x = read_manifest(&incremental_dir, a);
            let y = read_manifest(&fresh_dir, b);
            children += y.page_children.len();
            // A shard published block by block was a tail first, so it
            // carries revision numbering a fresh build does not; its content,
            // every table digest included, is the same.
            assert_eq!(a.sealed, b.sealed);
            x.revision = 0;
            x.supersedes = String::new();
            x.parent_manifest_digest = y.parent_manifest_digest.clone();
            assert_eq!(x, y, "shard {}", a.shard_id);
        }
        assert!(
            children > fresh.shards.len(),
            "page shards inside directory shards"
        );
    }

    /// A reorg at a page-shard boundary, at a directory-shard boundary and
    /// inside a page shard republishes from the open directory shard, and
    /// every directory shard sealed below the fork is the same revision.
    #[test]
    fn a_reorg_republishes_only_what_holds_a_changed_block() {
        let root = tempfile::tempdir().unwrap();
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let build = |name: &str, fork_at: Option<u64>| {
            let mut journal =
                EventStore::open(root.path().join(name).join("journal"), &"00".repeat(32), 0)
                    .unwrap();
            for h in 0..60 {
                busy_block(&mut journal, h, u8::from(fork_at.is_some_and(|f| h >= f)));
            }
            journal
        };
        let original = build("original", None);
        let base = root.path().join("base");
        let before = publish_with(&single_tier(&base, None, 59), &original, zero, &small).unwrap();
        let sealed: Vec<_> = before.shards.iter().filter(|s| s.sealed).collect();
        assert!(sealed.len() >= 2);
        let first = read_manifest(&base, sealed[0]);
        let mut forks = vec![
            sealed[0].end_height + 1,
            first.page_children[0].end_height + 1,
            first.page_children[0].start_height + 1,
        ];
        forks.sort();
        forks.dedup();
        for fork in forks {
            let journal = build(&format!("fork{fork}"), Some(fork));
            let output = root.path().join(format!("out{fork}"));
            let after = publish_with(
                &single_tier(&output, Some(&base), 59),
                &journal,
                zero,
                &small,
            )
            .unwrap();
            transparent_server_backend::shardset::ShardSet::open(&output, 3).unwrap();
            for entry in before
                .shards
                .iter()
                .filter(|e| e.sealed && e.end_height < fork)
            {
                assert_eq!(&after.shards[entry.shard_id as usize], entry, "fork {fork}");
            }
            assert!(
                after
                    .shards
                    .iter()
                    .any(|e| e.end_height >= fork && !before.shards.contains(e)),
                "fork {fork} changed something"
            );
        }
    }

    /// The choice option never changes an existing digest: a republication
    /// that reproduces keeps what it published, and only content that changes
    /// anyway (a grown tail, a fresh set) takes the option.
    #[test]
    fn directory_choice_is_added_without_changing_published_digests() {
        let root = tempfile::tempdir().unwrap();
        let mut journal =
            EventStore::open(root.path().join("journal"), &"00".repeat(32), 0).unwrap();
        for h in 0..4 {
            block(&mut journal, h, h as u8 + 1);
        }
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let with = |output: &Path, previous: Option<&Path>, choice: DirectoryChoice| {
            let mut options = options(output, previous);
            options.directory_choice = choice;
            options
        };
        let read = |dir: &Path, e: &transparent_filter::ShardMapEntry| -> ShardManifest {
            serde_json::from_slice(
                &std::fs::read(dir.join(&e.manifest_digest).join("manifest.json")).unwrap(),
            )
            .unwrap()
        };

        let a = root.path().join("a");
        let before = publish(&with(&a, None, DirectoryChoice::Off), &journal, zero).unwrap();
        for entry in &before.shards {
            assert_eq!(read(&a, entry).directory_choice, None);
        }
        // The same journal again, now asking for tables everywhere: nothing
        // changes, because every shard reproduces.
        let again = publish(&with(&a, None, DirectoryChoice::All), &journal, zero).unwrap();
        assert_eq!(before, again);

        // A grown tail is a new revision, and takes the option. The sealed
        // prefix is reused as published.
        block(&mut journal, 4, 5);
        let b = root.path().join("b");
        let snapshot = Snapshot::capture(&journal, &before, root.path()).unwrap();
        let after = publish(&with(&b, Some(&a), DirectoryChoice::All), &snapshot, zero).unwrap();
        assert_eq!(before.shards[0], after.shards[0]);
        let tail = read(&b, &after.shards[1]);
        assert_eq!(tail.revision, 1);
        let table = tail
            .directory_choice()
            .unwrap()
            .expect("a grown tail takes the option");
        assert_eq!(u64::from(table.keys()), tail.occupancy.scripts);
        transparent_server_backend::shardset::ShardSet::open(&b, 3).unwrap();

        // A fresh set under `sealed` tables its sealed shards and not its tail.
        let c = root.path().join("c");
        let sealed = publish(&with(&c, None, DirectoryChoice::Sealed), &journal, zero).unwrap();
        for entry in &sealed.shards {
            let manifest = read(&c, entry);
            assert_eq!(
                manifest.directory_choice().unwrap().is_some(),
                entry.sealed,
                "shard {}",
                entry.shard_id
            );
        }
        transparent_server_backend::shardset::ShardSet::open(&c, 3).unwrap();
    }
    /// A publication under the v2 range profile names it everywhere and
    /// loads; continuing a set published under another profile is refused.
    #[test]
    fn a_range_profile_change_is_a_new_publication() {
        let root = tempfile::tempdir().unwrap();
        let mut journal =
            EventStore::open(root.path().join("journal"), &"00".repeat(32), 0).unwrap();
        for h in 0..4 {
            block(&mut journal, h, h as u8 + 1);
        }
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let v1 = root.path().join("v1");
        publish(&options(&v1, None), &journal, zero).unwrap();

        let v2 = root.path().join("v2");
        let mut fresh = options(&v2, None);
        fresh.range_profile = transparent_filter::RANGE_PROFILE_V2.name.into();
        let map = publish(&fresh, &journal, zero).unwrap();
        assert_eq!(map.profile, transparent_filter::RANGE_PROFILE_V2.name);
        for entry in &map.shards {
            let manifest: ShardManifest = serde_json::from_slice(
                &std::fs::read(v2.join(&entry.manifest_digest).join("manifest.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(manifest.profile, transparent_filter::RANGE_PROFILE_V2.name);
        }
        transparent_server_backend::shardset::ShardSet::open(&v2, 3).unwrap();

        let mut continued = options(&root.path().join("v2-continued"), Some(&v1));
        continued.range_profile = transparent_filter::RANGE_PROFILE_V2.name.into();
        assert!(publish(&continued, &journal, zero).is_err());

        let mut unknown = options(&root.path().join("unknown"), None);
        unknown.range_profile = "zcash-transparent-range-v99".into();
        assert!(publish(&unknown, &journal, zero).is_err());
    }
    /// The scan `link_superseded_tails` replaced: every manifest read again
    /// for every shard of the map.
    fn link_superseded_tails_per_shard(
        previous: &Path,
        output: &Path,
        shards: &[transparent_filter::ShardMapEntry],
        store: &impl Journal,
    ) {
        for entry in shards {
            let mut retained = Vec::new();
            for path in std::fs::read_dir(previous).unwrap() {
                let path = path.unwrap().path();
                if !path.is_dir() {
                    continue;
                }
                let Ok(raw) = std::fs::read(path.join("manifest.json")) else {
                    continue;
                };
                let Ok(manifest) = serde_json::from_slice::<ShardManifest>(&raw) else {
                    continue;
                };
                if manifest.shard_id == entry.shard_id
                    && !manifest.sealed
                    && manifest.revision < entry.revision
                    && store.block_at(manifest.end_height).is_some_and(|b| {
                        b.block_hash.to_display_hex() == manifest.terminal_block_hash
                    })
                {
                    retained.push((manifest.revision, manifest.digest()));
                }
            }
            retained.sort_by_key(|entry| std::cmp::Reverse(entry.0));
            for (_, digest) in retained.into_iter().take(3) {
                link_revision(previous, output, &digest).unwrap();
            }
        }
    }

    /// Reading each manifest once carries forward exactly the revisions the
    /// per-shard scan did: the newest three superseded tails per shard whose
    /// endpoints are canonical, never a sealed or orphaned one, and nothing
    /// for directories without a readable manifest.
    #[test]
    fn superseded_tails_are_carried_as_the_per_shard_scan_did() {
        let root = tempfile::tempdir().unwrap();
        let mut journal =
            EventStore::open(root.path().join("journal"), &"00".repeat(32), 0).unwrap();
        for h in 0..4 {
            block(&mut journal, h, h as u8 + 1);
        }
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let a = root.path().join("a");
        // Six revisions of the growing tail, published beside each other.
        let mut map = publish(&options(&a, None), &journal, zero).unwrap();
        for h in 4..9 {
            block(&mut journal, h, h as u8 + 1);
            map = publish(&options(&a, None), &journal, zero).unwrap();
        }
        // A tail revision whose endpoint left the chain, and directories
        // without a readable manifest.
        let orphan = root.path().join("orphan");
        let mut fork = EventStore::open(root.path().join("fork"), &"00".repeat(32), 0).unwrap();
        for h in 0..7 {
            block(&mut fork, h, if h < 5 { h as u8 + 1 } else { h as u8 + 40 });
        }
        let forked = publish(&options(&orphan, None), &fork, zero).unwrap();
        let digest = &forked.shards.last().unwrap().manifest_digest;
        link_revision(&orphan, &a, digest).unwrap();
        std::fs::create_dir(a.join("empty")).unwrap();
        std::fs::create_dir(a.join("malformed")).unwrap();
        std::fs::write(a.join("malformed").join("manifest.json"), b"{").unwrap();

        let tails = map.shards.iter().filter(|s| s.revision > 0).count();
        assert!(tails > 0 && map.shards.len() > 1);
        let listing = |dir: &Path| -> std::collections::BTreeSet<_> {
            std::fs::read_dir(dir)
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect()
        };
        let single = root.path().join("single");
        let reference = root.path().join("reference");
        link_superseded_tails(&a, &single, &map.shards, &journal).unwrap();
        link_superseded_tails_per_shard(&a, &reference, &map.shards, &journal);
        assert_eq!(listing(&single), listing(&reference));
        assert_eq!(listing(&single).len(), 3, "the newest three tails");
        assert!(
            !single.join(digest).exists(),
            "an orphaned tail stays behind"
        );
        for name in listing(&single) {
            assert_eq!(
                listing(&single.join(&name)),
                listing(&reference.join(&name))
            );
        }
    }

    /// Retention cost over a publication shaped like production's: one
    /// manifest per shard of the retained v10 census, each carrying a choice
    /// table sized for its script count, and three superseded tails. Prints
    /// both scans' seconds; run with `--ignored --nocapture` on an optimized
    /// build.
    #[test]
    #[ignore]
    fn retention_scan_cost_at_census_scale() {
        let census = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../evidence/compact-layout-2026-09-28/census.jsonl"),
        )
        .unwrap();
        let scripts: Vec<u64> = census
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .filter(|row| row["type"] == "shard")
            .map(|row| row["scripts"].as_u64().unwrap())
            .collect();
        let root = tempfile::tempdir().unwrap();
        let mut journal =
            EventStore::open(root.path().join("journal"), &"00".repeat(32), 0).unwrap();
        for h in 0..4 {
            block(&mut journal, h, h as u8 + 1);
        }
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let template_dir = root.path().join("template");
        let mut options = options(&template_dir, None);
        options.directory_choice = DirectoryChoice::All;
        let template_map = publish(&options, &journal, zero).unwrap();
        let tail = template_map.shards.last().unwrap();
        let template: ShardManifest = serde_json::from_slice(
            &std::fs::read(
                template_dir
                    .join(&tail.manifest_digest)
                    .join("manifest.json"),
            )
            .unwrap(),
        )
        .unwrap();

        let previous = root.path().join("previous");
        let mut shards = Vec::new();
        let mut manifest_bytes = 0;
        let last = scripts.len() as u64 - 1;
        let mut write = |manifest: &ShardManifest| {
            let bytes = manifest.canonical_bytes();
            manifest_bytes += bytes.len();
            let dir = previous.join(manifest.digest());
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("manifest.json"), bytes).unwrap();
            manifest.digest()
        };
        for (id, keys) in scripts.iter().enumerate() {
            let segment = (keys * 123).div_ceil(100).saturating_add(32).div_ceil(3);
            let encoded = (13 + (3 * segment).div_ceil(8)).div_ceil(3) * 4;
            let mut manifest = template.clone();
            manifest.shard_id = id as u64;
            manifest.sealed = id as u64 != last;
            manifest.revision = if manifest.sealed { 0 } else { 4 };
            manifest.directory_choice = Some("A".repeat(encoded as usize));
            if id as u64 == last {
                for revision in 1..4 {
                    let mut superseded = manifest.clone();
                    superseded.revision = revision;
                    write(&superseded);
                }
            }
            let mut entry = tail.clone();
            entry.shard_id = id as u64;
            entry.sealed = manifest.sealed;
            entry.revision = manifest.revision;
            entry.manifest_digest = write(&manifest);
            shards.push(entry);
        }

        let started = std::time::Instant::now();
        link_superseded_tails_per_shard(&previous, &root.path().join("a"), &shards, &journal);
        let per_shard = started.elapsed().as_secs_f64();
        let started = std::time::Instant::now();
        link_superseded_tails(&previous, &root.path().join("b"), &shards, &journal).unwrap();
        let single = started.elapsed().as_secs_f64();
        println!(
            "{}",
            serde_json::json!({
                "shards": shards.len(),
                "manifests": shards.len() + 3,
                "manifest_bytes": manifest_bytes,
                "per_shard_scan_seconds": per_shard,
                "single_scan_seconds": single,
            })
        );
    }

    #[test]
    fn sealed_reorg_builds_a_separate_suffix_and_snapshot_survives_rollback() {
        let root = tempfile::tempdir().unwrap();
        let mut journal =
            EventStore::open(root.path().join("journal"), &"00".repeat(32), 0).unwrap();
        for h in 0..4 {
            block(&mut journal, h, h as u8 + 1);
        }
        let a = root.path().join("a");
        let b = root.path().join("b");
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let before = publish(&options(&a, None), &journal, zero).unwrap();
        let snapshot = Snapshot::capture(&journal, &before, root.path()).unwrap();
        journal.rollback_to(Some(0)).unwrap();
        for h in 1..4 {
            block(&mut journal, h, h as u8 + 10);
        }
        assert_eq!(
            snapshot.block_at(3).unwrap().block_hash,
            BlockHash::from_internal_bytes([4; 32])
        );
        assert!(publish(&options(&a, None), &journal, zero).is_err());
        let replacement = publish(&options(&b, Some(&a)), &journal, zero).unwrap();
        assert_ne!(
            before.shards[0].manifest_digest,
            replacement.shards[0].manifest_digest
        );
        assert_eq!(replacement.shards[0].revision, 0);
        transparent_server_backend::shardset::ShardSet::open(&b, 3).unwrap();
        // Original immutable artifact remains exactly the original map.
        let old: transparent_filter::ShardMap =
            serde_json::from_slice(&std::fs::read(a.join("shards.json")).unwrap()).unwrap();
        assert_eq!(old, before);
    }
}
