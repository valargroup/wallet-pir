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

#[cfg(test)]
use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_publisher::transparent::filter as transparent_filter;

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
#[cfg(test)]
use transparent_filter::ScriptBytes;
use transparent_filter::{filter_hash, BlockHash};
#[cfg(test)]
use transparent_shared::journal::events::EventStore;

use transparent_shard::build::{Built, BuiltChild, BuiltDirectory, ShardStream};
use transparent_shard::layout::{by_name as geometry_by_name, Geometry};
use transparent_shard::manifest::{
    ManifestLayout, ManifestOccupancy, ManifestSeal, PageChild, PublishedRevision, ShardManifest,
    TableGeometry, SCHEMA,
};
use transparent_shard::seal::{DecoupledPolicy, SealedDirectory};

pub use crate::BoxError;

/// Inputs and shard policy for one immutable history build.
#[derive(Clone, Debug)]
pub struct PublishOptions {
    pub data_dir: PathBuf,
    pub previous: Option<PathBuf>,
    pub output: PathBuf,
    pub recent_geometry: String,
    pub archive_geometry: Option<String>,
    pub recent_from: Option<u64>,
    pub zakura_rpc_url: String,
    pub zakura_cookie: PathBuf,
    pub through: Option<u64>,
    pub record: Option<PathBuf>,
    pub source_sha: Option<String>,
    pub directory_choice: DirectoryChoice,
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

#[cfg(test)]
pub(crate) use crate::storage::records::write_durable_with;
pub use crate::storage::records::{write_atomic, write_durable, write_immutable};

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
    publish_until(cli, store, base_parent, policy_of, &|| false)
}

/// [`publish_with`], asking `stop` before each journal block and before
/// staging or publishing each shard that block completes. A stop returns
/// [`transparent_shared::publication::Stopped`] before the map is written,
/// so the output never names a set; a shard already being written finishes
/// its files first.
pub fn publish_until(
    cli: &PublishOptions,
    store: &impl Journal,
    base_parent: BlockHash,
    policy_of: &dyn Fn(&Geometry) -> DecoupledPolicy,
    stop: &dyn Fn() -> bool,
) -> Result<transparent_filter::ShardMap, BoxError> {
    let stopped = || -> BoxError { Box::new(transparent_shared::publication::Stopped) };
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
            Ok(raw) => {
                serde_json::from_slice::<transparent_filter::ShardMap>(&raw)
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
                    .collect()
            }
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
        if stop() {
            return Err(stopped());
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
    // Every revision a shard has ever been published under, carried across
    // reorgs and into every later output directory. A reorg drops the changed
    // suffix from `previous`, so without this its replacement would restart
    // at revision 0 and reuse a number a wallet already accepted for other
    // content, which it must withdraw as an equivocation.
    let mut floor = revision_floor(previous_dir)?;
    for entry in previous.values() {
        raise_floor(&mut floor, entry.shard_id, entry.revision);
    }
    let highest_revision = floor.values().copied().max();
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
            if stop() {
                return Err(stopped());
            }
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
                        highest_revision,
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
        if stop() {
            return Err(stopped());
        }
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
    if stop() {
        return Err(stopped());
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
    // Recorded before the map, so no set is ever served whose revisions the
    // next publication cannot see.
    for entry in &map.shards {
        raise_floor(&mut floor, entry.shard_id, entry.revision);
    }
    write_atomic(
        &cli.output.join(REVISION_FLOOR),
        &serde_json::to_vec_pretty(&floor)?,
    )?;
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

pub use crate::storage::page_segment_file;

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
            if link_file(&published.join(&name), &target).is_ok() {
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
    highest_revision: Option<u32>,
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
    let (revision, supersedes) = if reproduced {
        PublishedRevision::next(built.shard_id, published.as_ref(), reproduced)?
    } else {
        // Wallet sources follow geometry and start height, not shard numbers.
        // A reorg can change sealing boundaries and renumber a later source,
        // so new content must exceed the history of every shard number.
        let highest = published
            .as_ref()
            .map(|p| p.revision)
            .into_iter()
            .chain(highest_revision)
            .max();
        let revision = match highest {
            None => 0,
            Some(highest) => highest.checked_add(1).ok_or_else(|| {
                format!(
                    "shard {} has exhausted its revision numbers at {highest}",
                    built.shard_id
                )
            })?,
        };
        // Check exhaustion before the helper increments the predecessor.
        let (_, supersedes) = PublishedRevision::next(built.shard_id, published.as_ref(), false)?;
        (revision, supersedes)
    };
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

/// The highest revision published under each shard number.
///
/// Allocation uses the maximum across all numbers because reorgs can renumber
/// the geometry/start-height sources whose revisions wallets remember.
pub type RevisionFloor = std::collections::BTreeMap<u64, u32>;

/// The file beside a publication's map that records its [`RevisionFloor`].
///
/// Not protocol data: wallets never read it. Each publication writes the floor
/// it inherited raised by what it published, so the record only ever rises. An
/// operator recovering from a publication that predates it may write one into
/// the previous directory, holding the highest revision any retained
/// publication shows for each shard; it is only ever raised from there.
pub const REVISION_FLOOR: &str = "revision-floor.json";

fn raise_floor(floor: &mut RevisionFloor, shard_id: u64, revision: u32) {
    let current = floor.entry(shard_id).or_insert(revision);
    *current = (*current).max(revision);
}

/// The floor `dir` records, raised by every manifest retained in it.
///
/// A publication from before the floor was recorded still holds its
/// superseded tails, so its manifests are the evidence the floor starts from;
/// a recorded floor is raised by them too, so it never sits below a revision
/// the directory still holds. A manifest that is present but cannot be read
/// or parsed is refused rather than skipped: it may be the highest revision.
pub fn revision_floor(dir: &Path) -> Result<RevisionFloor, BoxError> {
    let recorded = dir.join(REVISION_FLOOR);
    let mut floor = match std::fs::read(&recorded) {
        Ok(raw) => serde_json::from_slice::<RevisionFloor>(&raw)
            .map_err(|error| format!("{}: {error}", recorded.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => RevisionFloor::new(),
        Err(error) => return Err(format!("{}: {error}", recorded.display()).into()),
    };
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(floor),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let path = entry.path().join("manifest.json");
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(format!("{}: {error}", path.display()).into()),
        };
        let manifest = serde_json::from_slice::<ShardManifest>(&raw)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        raise_floor(&mut floor, manifest.shard_id, manifest.revision);
    }
    Ok(floor)
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

pub use crate::ingest::journal::Journal;

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
        let kind = entry.file_type()?;
        if !kind.is_file() && !kind.is_symlink() {
            return Err("unexpected revision artifact".into());
        }
        let dest = target.join(entry.file_name());
        if std::fs::symlink_metadata(&dest).is_ok() {
            continue;
        }
        if link_file(&entry.path(), &dest).is_err() {
            if kind.is_symlink() {
                return Err("could not link a stored revision segment".into());
            }
            std::fs::copy(entry.path(), &dest)?;
        }
    }
    Ok(())
}

/// Gives `to` the file `from` names. A link into the publisher's segment
/// store is linked again, never followed, so republishing a stored segment
/// leaves its object untouched; the loader checks the link is exact.
fn link_file(from: &Path, to: &Path) -> std::io::Result<()> {
    if std::fs::symlink_metadata(from)?.file_type().is_symlink() {
        return std::os::unix::fs::symlink(std::fs::read_link(from)?, to);
    }
    std::fs::hard_link(from, to)
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
    use crate::ingest::snapshot::Snapshot;

    #[test]
    fn a_durable_record_refuses_an_unopenable_directory_before_replacing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history-restore.json");
        std::fs::write(&path, b"old").unwrap();
        let error = write_durable_with(
            &path,
            b"new",
            |_| Err(std::io::Error::other("open refused")),
            |_| unreachable!("no directory to sync"),
        )
        .unwrap_err();
        assert!(error.to_string().contains("open refused"), "{error}");
        assert_eq!(std::fs::read(&path).unwrap(), b"old");
        assert!(!path.with_extension("tmp").exists());
    }

    #[test]
    fn a_durable_record_reports_a_failed_directory_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("integrity-failures.json");
        let error = write_durable_with(
            &path,
            b"[]",
            |p| std::fs::File::open(p),
            |_| Err(std::io::Error::other("sync refused")),
        )
        .unwrap_err();
        assert!(error.to_string().contains("sync refused"), "{error}");
        // The rename happened, so the name holds whole bytes, but the caller
        // was not told they are durable.
        assert_eq!(std::fs::read(&path).unwrap(), b"[]");
        write_durable(&path, b"[1]").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"[1]");
    }

    #[test]
    fn an_interrupted_durable_replacement_keeps_the_previous_record_whole() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history-restore.json");
        std::fs::write(&path, b"old").unwrap();
        // A crash before the rename leaves only a partial temporary, which the
        // next write replaces without ever exposing it under the final name.
        std::fs::write(path.with_extension("tmp"), b"ne").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"old");
        write_durable(&path, b"new").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
        assert!(!path.with_extension("tmp").exists());
        // A temporary that cannot be written fails without touching the name.
        std::fs::create_dir(path.with_extension("tmp")).unwrap();
        assert!(write_durable(&path, b"newer").is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
    }
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
        transparent_shared::history::shardset::ShardSet::open(&b, 3).unwrap();
    }
    fn stored_load_options() -> transparent_shared::history::shardset::LoadOptions {
        transparent_shared::history::shardset::LoadOptions {
            prune_excess: true,
            ..transparent_shared::history::shardset::LoadOptions::whole(
                transparent_shared::history::shardset::DEFAULT_RETAIN_REVISIONS,
            )
        }
    }

    /// A stored file, with its inode number and change time.
    type Stored = (PathBuf, (u64, i64, i64));

    /// The stored file `link` names, and its stamp.
    fn stored(link: &Path) -> Stored {
        use std::os::unix::fs::MetadataExt;
        let physical = std::fs::canonicalize(link).unwrap();
        let meta = std::fs::metadata(&physical).unwrap();
        (physical, (meta.ino(), meta.ctime(), meta.ctime_nsec()))
    }

    /// Successive real publications store each table segment once. Every
    /// segment a candidate shares with a publication two cycles old, settled
    /// and unchanged since, is reused without being hashed again and its
    /// object never changes; collection removes exactly what no remaining
    /// publication links; and a same-size change with the modification time
    /// restored is hashed again and refused.
    #[test]
    fn successive_stored_publications_hash_only_new_or_changed_segments() {
        use std::os::unix::fs::FileExt;
        use transparent_shared::history::shardset::{ShardSet, OBJECT_STORE};
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("publications");
        std::fs::create_dir(&base).unwrap();
        let mut journal =
            EventStore::open(root.path().join("journal"), &"00".repeat(32), 0).unwrap();
        for h in 0..4 {
            block(&mut journal, h, h as u8 + 1);
        }
        let zero = BlockHash::from_internal_bytes([0; 32]);
        // One small geometry, sealing a shard every two scripts: sealed
        // shards accumulate and the tail changes every cycle.
        let small = |output: &Path, previous: Option<&Path>| PublishOptions {
            archive_geometry: None,
            recent_from: None,
            ..options(output, previous)
        };
        let seal = |geometry: &Geometry| {
            let mut policy = DecoupledPolicy::for_geometry(geometry);
            policy.scripts.target = 2;
            policy
        };
        let never = || false;
        let mut previous: Option<(PathBuf, ShardSet, transparent_filter::ShardMap)> = None;
        // Each cycle's segment objects by digest, with their stamps.
        let mut history: Vec<std::collections::BTreeMap<String, Stored>> = Vec::new();
        for cycle in 0..5u64 {
            let output = base.join(format!("candidate-{cycle}"));
            let map = match &previous {
                None => publish_with(&small(&output, None), &journal, zero, &seal).unwrap(),
                Some((dir, _, map)) => {
                    block(&mut journal, 4 + cycle - 1, 10 + cycle as u8);
                    let snapshot = Snapshot::capture(&journal, map, root.path()).unwrap();
                    publish_with(&small(&output, Some(dir)), &snapshot, zero, &seal).unwrap()
                }
            };
            crate::history::store::adopt_segments(&output, &never).unwrap();
            let set = ShardSet::open_reusing(
                &output,
                &stored_load_options(),
                previous.as_ref().map(|p| &p.1),
            )
            .unwrap();
            let mut objects = std::collections::BTreeMap::new();
            for shard in set.revisions() {
                for table in shard.tables() {
                    for segment in 0..shard.segments(table) {
                        let source = shard.segment(table, segment).unwrap();
                        objects.insert(source.sha256().to_owned(), stored(source.path()));
                    }
                }
            }
            let report = set.verification().clone();
            if cycle >= 2 {
                // Settled before the previous cycle began and unchanged since.
                let stable: u64 = objects
                    .iter()
                    .filter(|(sha, now)| history[cycle as usize - 2].get(*sha) == Some(now))
                    .map(|(sha, _)| {
                        let shard = set.revisions().iter().find_map(|shard| {
                            shard.tables().find_map(|table| {
                                (0..shard.segments(table))
                                    .filter_map(|i| shard.segment(table, i))
                                    .find(|s| s.sha256() == sha)
                                    .map(|s| s.bytes() as u64)
                            })
                        });
                        shard.unwrap()
                    })
                    .sum();
                assert!(stable > 0, "cycle {cycle}: nothing stayed stable");
                let total = report.bytes_hashed + report.bytes_reused;
                assert!(report.bytes_reused >= stable, "cycle {cycle}: {report:?}");
                assert!(
                    report.bytes_hashed <= total - stable,
                    "cycle {cycle}: {report:?}"
                );
                // A segment kept across cycles keeps its object untouched.
                for (sha, old) in &history[cycle as usize - 1] {
                    if let Some(now) = objects.get(sha) {
                        assert_eq!(now.0, old.0);
                    }
                }
            }
            if cycle >= 2 {
                // Collect a publication older than the last two, with an
                // object nothing links, as a crash between installing and
                // linking it leaves; what remains still loads.
                std::fs::remove_dir_all(base.join(format!("candidate-{}", cycle - 2))).unwrap();
                let orphan = base.join(OBJECT_STORE).join("ff".repeat(32));
                std::fs::write(&orphan, b"orphan").unwrap();
                let removed = crate::history::store::collect(&base).unwrap();
                assert!(removed.objects >= 1, "{removed:?}");
                assert!(!orphan.exists());
                for kept in [&output, &previous.as_ref().unwrap().0] {
                    ShardSet::open_for_serving(kept, &stored_load_options()).unwrap();
                }
                for (path, _) in objects.values() {
                    assert!(path.exists(), "a linked object was collected");
                }
            }
            history.push(objects);
            previous = Some((output, set, map));
            // Let every object written this cycle settle before the next.
            std::thread::sleep(std::time::Duration::from_millis(1100));
        }
        // A same-size change to a stable object, with its modification time
        // restored, is hashed again and refused.
        let (dir, set, map) = previous.unwrap();
        let link = dir
            .join(&map.shards[0].manifest_digest)
            .join("directory.0.bin");
        let (object, _) = stored(&link);
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&object, std::fs::Permissions::from_mode(0o644)).unwrap();
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&object)
            .unwrap();
        let modified = file.metadata().unwrap().modified().unwrap();
        let mut first = [0u8; 1];
        file.read_exact_at(&mut first, 0).unwrap();
        file.write_all_at(&[first[0] ^ 1], 0).unwrap();
        file.set_modified(modified).unwrap();
        let error = ShardSet::open_reusing(&dir, &stored_load_options(), Some(&set))
            .err()
            .unwrap();
        assert!(error.to_string().contains("digest"), "{error}");
    }

    /// A tail reorg publishes its replacement into the same stores without
    /// touching the active publication: every object the active one links
    /// keeps its inode and bytes, and it still verifies in full. A stop
    /// during storing proves nothing and a retry completes the candidate.
    #[test]
    fn a_stored_reorg_or_stopped_candidate_leaves_active_coverage_intact() {
        use transparent_shared::history::shardset::ShardSet;
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("publications");
        std::fs::create_dir(&base).unwrap();
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let journal = |name: &str, tip: u64, fork: Option<u64>| {
            let mut store = EventStore::open(root.path().join(name), &"00".repeat(32), 0).unwrap();
            for h in 0..=tip {
                let forked = if fork.is_some_and(|f| h >= f) {
                    0x80
                } else {
                    0
                };
                block(&mut store, h, forked | (h as u8 + 1));
            }
            store
        };
        let never = || false;
        let active = base.join("candidate-active");
        publish(&options(&active, None), &journal("a", 4, None), zero).unwrap();
        crate::history::store::adopt_segments(&active, &never).unwrap();
        let held = ShardSet::open(&active, 3).unwrap();
        let mut before = std::collections::BTreeMap::new();
        for shard in held.revisions() {
            for table in shard.tables() {
                for segment in 0..shard.segments(table) {
                    let source = shard.segment(table, segment).unwrap();
                    before.insert(source.path().to_path_buf(), stored(source.path()));
                }
            }
        }
        // The tip block replaced.
        let replacement = base.join("candidate-reorg");
        publish(
            &options(&replacement, Some(&active)),
            &journal("f", 4, Some(4)),
            zero,
        )
        .unwrap();
        // A stop while storing leaves the candidate unproved.
        let error = crate::history::store::adopt_segments(&replacement, &|| true).unwrap_err();
        assert!(
            error.is::<transparent_shared::publication::Stopped>(),
            "{error}"
        );
        crate::history::store::adopt_segments(&replacement, &never).unwrap();
        let reorged =
            ShardSet::open_reusing(&replacement, &stored_load_options(), Some(&held)).unwrap();
        assert_ne!(reorged.map_digest, held.map_digest);
        for (path, stamp) in &before {
            assert_eq!(&stored(path), stamp, "{} changed", path.display());
        }
        ShardSet::open(&active, 3).unwrap();
        // Retiring the replacement collects only what it alone linked.
        std::fs::remove_dir_all(&replacement).unwrap();
        crate::history::store::collect(&base).unwrap();
        ShardSet::open(&active, 3).unwrap();
    }

    /// A stop during a build ends it between units of work, before the map
    /// names a set; publishing again without a stop completes the same set.
    #[test]
    fn a_stopped_build_writes_no_map_and_can_resume() {
        let root = tempfile::tempdir().unwrap();
        let mut journal =
            EventStore::open(root.path().join("journal"), &"00".repeat(32), 0).unwrap();
        for h in 0..4 {
            block(&mut journal, h, h as u8 + 1);
        }
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let out = root.path().join("out");
        let policy = DecoupledPolicy::for_geometry;
        // Already requested: no block is read.
        let asked = std::cell::Cell::new(0);
        let error = publish_until(&options(&out, None), &journal, zero, &policy, &|| {
            asked.set(asked.get() + 1);
            true
        })
        .unwrap_err();
        assert!(error.is::<transparent_shared::publication::Stopped>());
        assert_eq!(asked.get(), 1);
        // Requested after the second unit.
        let asked = std::cell::Cell::new(0);
        let error = publish_until(&options(&out, None), &journal, zero, &policy, &|| {
            asked.set(asked.get() + 1);
            asked.get() > 2
        })
        .unwrap_err();
        assert!(error.is::<transparent_shared::publication::Stopped>());
        assert_eq!(asked.get(), 3);
        assert!(!out.join("shards.json").exists());
        let resumed = publish_with(&options(&out, None), &journal, zero, &policy).unwrap();
        let reference = publish(&options(&root.path().join("full"), None), &journal, zero).unwrap();
        assert_eq!(resumed, reference);
        transparent_shared::history::shardset::ShardSet::open(&out, 3).unwrap();
    }
    /// A reorg of the tail never reuses a revision a wallet may already hold:
    /// its replacement, and every later growth, numbers above every revision
    /// the shard was published under, across output directories.
    #[test]
    fn a_tail_reorg_never_reuses_a_revision() {
        let root = tempfile::tempdir().unwrap();
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let journal = |name: &str, tip: u64, fork: Option<u64>| {
            let mut store = EventStore::open(root.path().join(name), &"00".repeat(32), 0).unwrap();
            for h in 0..=tip {
                let forked = if fork.is_some_and(|f| h >= f) {
                    0x80
                } else {
                    0
                };
                let tag = forked | (h as u8 + 1);
                block(&mut store, h, tag);
            }
            store
        };
        let dir = |name: &str| root.path().join(name);
        let mut published = Vec::new();
        let mut previous: Option<PathBuf> = None;
        for tip in 3..=6 {
            let name = format!("p{tip}");
            let map = publish(
                &options(&dir(&name), previous.as_deref()),
                &journal(&format!("j{tip}"), tip, None),
                zero,
            )
            .unwrap();
            published.push(map.shards.last().unwrap().clone());
            previous = Some(dir(&name));
        }
        assert_eq!(published.last().unwrap().revision, 3);
        // One block replaced at the tip.
        let reorged = journal("fork", 6, Some(6));
        let replaced = publish(&options(&dir("r"), previous.as_deref()), &reorged, zero).unwrap();
        let tail = replaced.shards.last().unwrap();
        assert_eq!(tail.shard_id, published[0].shard_id);
        assert_eq!(tail.end_height, 6);
        assert_eq!(tail.revision, 4, "a reorg continues above revision 3");
        assert_eq!(read_tail(&dir("r"), tail).supersedes, "");
        transparent_shared::history::shardset::ShardSet::open(&dir("r"), 3).unwrap();
        // A publication continuing the replacement keeps rising.
        let mut grown = journal("fork2", 6, Some(6));
        block(&mut grown, 7, 0x88);
        let next = publish(&options(&dir("n"), Some(&dir("r"))), &grown, zero).unwrap();
        assert_eq!(next.shards.last().unwrap().revision, 5);
        assert_eq!(revision_floor(&dir("n")).unwrap()[&tail.shard_id], 5);
    }

    #[test]
    fn a_reorg_that_renumbers_a_source_exceeds_its_old_revisions() {
        use transparent_shard::seal::Limit;
        let root = tempfile::tempdir().unwrap();
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let policy = |geometry: &Geometry| DecoupledPolicy {
            scripts: Limit::new(2, 2).unwrap(),
            ..DecoupledPolicy::for_geometry(geometry)
        };
        let mut original = EventStore::open(root.path().join("old"), &"00".repeat(32), 0).unwrap();
        for (height, tag) in [1, 1, 1, 2, 5].into_iter().enumerate() {
            block(&mut original, height as u64, tag);
        }
        let options = |name: &str, previous: Option<&Path>| PublishOptions {
            archive_geometry: None,
            recent_from: None,
            ..options(&root.path().join(name), previous)
        };
        let first = root.path().join("first");
        let initial = publish_with(&options("first", None), &original, zero, &policy).unwrap();
        block(&mut original, 5, 5);
        let previous = root.path().join("previous");
        let grown =
            publish_with(&options("previous", Some(&first)), &original, zero, &policy).unwrap();
        let old_tail = grown.shards.last().unwrap();
        assert_eq!(old_tail.start_height, 4);
        assert_eq!(old_tail.shard_id, 1);
        assert!(old_tail.revision > initial.shards.last().unwrap().revision);

        let mut fork = EventStore::open(root.path().join("fork"), &"00".repeat(32), 0).unwrap();
        for (height, tag) in [1, 2, 3, 4, 5, 5].into_iter().enumerate() {
            block(&mut fork, height as u64, tag);
        }
        let repaired =
            publish_with(&options("repaired", Some(&previous)), &fork, zero, &policy).unwrap();
        let new_tail = repaired.shards.last().unwrap();
        assert_eq!(new_tail.start_height, old_tail.start_height);
        assert_eq!(new_tail.geometry, old_tail.geometry);
        assert_eq!(new_tail.shard_id, 2);
        assert!(new_tail.revision > old_tail.revision);
    }

    #[test]
    fn an_exhausted_previous_manifest_returns_an_error_before_incrementing() {
        let root = tempfile::tempdir().unwrap();
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let mut journal =
            EventStore::open(root.path().join("journal"), &"00".repeat(32), 0).unwrap();
        for h in 0..4 {
            block(&mut journal, h, h as u8 + 1);
        }
        let previous = root.path().join("previous");
        let mut map = publish(&options(&previous, None), &journal, zero).unwrap();
        let entry = map.shards.last_mut().unwrap();
        let mut manifest = read_tail(&previous, entry);
        manifest.revision = u32::MAX;
        let digest = manifest.digest();
        link_revision(&previous, &root.path().join("copy"), &entry.manifest_digest).unwrap();
        std::fs::rename(
            root.path().join("copy").join(&entry.manifest_digest),
            previous.join(&digest),
        )
        .unwrap();
        std::fs::write(
            previous.join(&digest).join("manifest.json"),
            manifest.canonical_bytes(),
        )
        .unwrap();
        entry.revision = u32::MAX;
        entry.manifest_digest = digest;
        std::fs::write(
            previous.join("shards.json"),
            serde_json::to_vec(&map).unwrap(),
        )
        .unwrap();
        block(&mut journal, 4, 5);
        let error = publish(
            &options(&root.path().join("next"), Some(&previous)),
            &journal,
            zero,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("exhausted"), "{error}");
        assert!(!root.path().join("next/shards.json").exists());
    }

    /// An operator-recorded floor from retained evidence is honoured and
    /// never lowered, and an exhausted revision space is refused.
    #[test]
    fn a_recorded_floor_is_raised_never_lowered_and_overflow_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let mut store = EventStore::open(root.path().join("j"), &"00".repeat(32), 0).unwrap();
        for h in 0..4 {
            block(&mut store, h, h as u8 + 1);
        }
        let a = root.path().join("a");
        let first = publish(&options(&a, None), &store, zero).unwrap();
        let shard = first.shards.last().unwrap().shard_id;
        let record = |floor: RevisionFloor| {
            std::fs::write(a.join(REVISION_FLOOR), serde_json::to_vec(&floor).unwrap()).unwrap()
        };
        record(RevisionFloor::from([(shard, 2309)]));
        block(&mut store, 4, 5);
        let b = root.path().join("b");
        let grown = publish(&options(&b, Some(&a)), &store, zero).unwrap();
        assert_eq!(grown.shards.last().unwrap().revision, 2310);
        assert_eq!(revision_floor(&b).unwrap()[&shard], 2310);

        record(RevisionFloor::from([(shard, u32::MAX)]));
        let error = publish(&options(&root.path().join("c"), Some(&a)), &store, zero)
            .unwrap_err()
            .to_string();
        assert!(error.contains("exhausted"), "{error}");
    }

    /// Retained manifests above a stale recorded floor raise it, and a
    /// present manifest that cannot be parsed stops the publication.
    #[test]
    fn retained_evidence_raises_a_stale_floor_and_corrupt_evidence_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let zero = BlockHash::from_internal_bytes([0; 32]);
        let mut store = EventStore::open(root.path().join("j"), &"00".repeat(32), 0).unwrap();
        for h in 0..4 {
            block(&mut store, h, h as u8 + 1);
        }
        let a = root.path().join("a");
        let first = publish(&options(&a, None), &store, zero).unwrap();
        let tail = first.shards.last().unwrap().clone();
        // A retained tail at revision 7 beside a floor recording only 2.
        let mut manifest = read_tail(&a, &tail);
        manifest.revision = 7;
        let retained = a.join(manifest.digest());
        std::fs::create_dir(&retained).unwrap();
        for file in std::fs::read_dir(a.join(&tail.manifest_digest)).unwrap() {
            let file = file.unwrap();
            std::fs::copy(file.path(), retained.join(file.file_name())).unwrap();
        }
        std::fs::write(
            retained.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        std::fs::write(
            a.join(REVISION_FLOOR),
            serde_json::to_vec(&RevisionFloor::from([(tail.shard_id, 2)])).unwrap(),
        )
        .unwrap();
        assert_eq!(revision_floor(&a).unwrap()[&tail.shard_id], 7);
        block(&mut store, 4, 5);
        let grown = publish(&options(&root.path().join("b"), Some(&a)), &store, zero).unwrap();
        assert_eq!(grown.shards.last().unwrap().revision, 8);

        std::fs::write(retained.join("manifest.json"), b"{\"truncated").unwrap();
        let error = publish(&options(&root.path().join("c"), Some(&a)), &store, zero)
            .unwrap_err()
            .to_string();
        assert!(error.contains("manifest.json"), "{error}");
        assert!(!root.path().join("c").join("shards.json").exists());
    }

    fn read_tail(dir: &Path, e: &transparent_filter::ShardMapEntry) -> ShardManifest {
        serde_json::from_slice(
            &std::fs::read(dir.join(&e.manifest_digest).join("manifest.json")).unwrap(),
        )
        .unwrap()
    }

    /// A block where a few busy scripts receive several times, so events are
    /// pushed out of the directory every block, and a new script appears.
    #[cfg(feature = "qualification-tests")]
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
    #[cfg(feature = "qualification-tests")]
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

    #[cfg(feature = "qualification-tests")]
    fn single_tier(output: &Path, previous: Option<&Path>, through: u64) -> PublishOptions {
        PublishOptions {
            archive_geometry: None,
            recent_from: None,
            through: Some(through),
            ..options(output, previous)
        }
    }

    #[cfg(feature = "qualification-tests")]
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
    #[cfg(feature = "qualification-tests")]
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
        let throughs = if cfg!(feature = "qualification-tests") {
            (5..70).step_by(3).chain([69]).collect::<Vec<_>>()
        } else {
            // Keep consecutive growth, sealed-page linking and the independent
            // complete rebuild; qualification repeats every intermediate step.
            vec![20, 23, 26, 29, 69]
        };
        for through in throughs {
            let output = root.path().join(format!("c{through}"));
            let published = publish_with(
                &single_tier(&output, previous.as_deref(), through),
                &journal,
                zero,
                &small,
            )
            .unwrap();
            transparent_shared::history::shardset::ShardSet::open(&output, 3).unwrap();
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
    #[cfg(feature = "qualification-tests")]
    enum ForkBoundary {
        Directory,
        Page,
        InsidePage,
    }

    #[cfg(feature = "qualification-tests")]
    #[test]
    fn a_directory_boundary_reorg_republishes_only_changed_blocks() {
        check_reorg(ForkBoundary::Directory);
    }

    #[cfg(feature = "qualification-tests")]
    #[test]
    fn a_page_boundary_reorg_republishes_only_changed_blocks() {
        check_reorg(ForkBoundary::Page);
    }

    #[cfg(feature = "qualification-tests")]
    #[test]
    fn a_reorg_inside_a_page_republishes_only_changed_blocks() {
        check_reorg(ForkBoundary::InsidePage);
    }

    #[cfg(feature = "qualification-tests")]
    fn check_reorg(boundary: ForkBoundary) {
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
        let fork = match boundary {
            ForkBoundary::Directory => sealed[0].end_height + 1,
            ForkBoundary::Page => first.page_children[0].end_height + 1,
            ForkBoundary::InsidePage => first.page_children[0].start_height + 1,
        };
        let journal = build(&format!("fork{fork}"), Some(fork));
        let output = root.path().join(format!("out{fork}"));
        let after = publish_with(
            &single_tier(&output, Some(&base), 59),
            &journal,
            zero,
            &small,
        )
        .unwrap();
        transparent_shared::history::shardset::ShardSet::open(&output, 3).unwrap();
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

    /// The choice option never changes an existing digest: a republication
    /// that reproduces keeps what it published, and only content that changes
    /// anyway (a grown tail, a fresh set) takes the option.
    #[cfg(feature = "qualification-tests")]
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
        transparent_shared::history::shardset::ShardSet::open(&b, 3).unwrap();

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
        transparent_shared::history::shardset::ShardSet::open(&c, 3).unwrap();
    }
    /// A publication under the v2 range profile names it everywhere and
    /// loads; continuing a set published under another profile is refused.
    #[cfg(feature = "qualification-tests")]
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
        transparent_shared::history::shardset::ShardSet::open(&v2, 3).unwrap();

        let mut continued = options(&root.path().join("v2-continued"), Some(&v1));
        continued.range_profile = transparent_filter::RANGE_PROFILE_V2.name.into();
        assert!(publish(&continued, &journal, zero).is_err());

        let mut unknown = options(&root.path().join("unknown"), None);
        unknown.range_profile = "zcash-transparent-range-v99".into();
        assert!(publish(&unknown, &journal, zero).is_err());
    }
    /// The scan `link_superseded_tails` replaced: every manifest read again
    /// for every shard of the map.
    #[cfg(feature = "qualification-tests")]
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
    #[cfg(feature = "qualification-tests")]
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
    #[cfg(feature = "qualification-tests")]
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

    #[cfg(feature = "qualification-tests")]
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
        // The replacement never reuses a revision the old publication exposed.
        assert!(replacement.shards[0].revision > before.shards[0].revision);
        transparent_shared::history::shardset::ShardSet::open(&b, 3).unwrap();
        // Original immutable artifact remains exactly the original map.
        let old: transparent_filter::ShardMap =
            serde_json::from_slice(&std::fs::read(a.join("shards.json")).unwrap()).unwrap();
        assert_eq!(old, before);
    }
}
