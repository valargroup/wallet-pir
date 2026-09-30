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

use transparent_shard::build::build_shard;
use transparent_shard::layout::{by_name as geometry_by_name, Geometry};
use transparent_shard::manifest::{
    ManifestLayout, ManifestOccupancy, ManifestSeal, PublishedRevision, ShardManifest,
    TableGeometry, SCHEMA,
};
use transparent_shard::seal::{PageBasis, SealPolicy, Sealer};

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
    #[arg(long, default_value = "recent-8k")]
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
    /// Require complete display sidecars and publish private txid tables.
    #[arg(long)]
    pub txid_display: bool,
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

pub fn publish(
    cli: &PublishOptions,
    store: &impl Journal,
    base_parent: BlockHash,
) -> Result<transparent_filter::ShardMap, BoxError> {
    if store.journal_version() != 3 {
        return Err("v11 publication requires a fresh v3 journal with transaction metadata".into());
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
    let mut geometry;
    let mut policy;
    let mut sealer;
    let mut pending: Vec<(
        u64,
        Vec<(ScriptBytes, transparent_events::TransparentEvent)>,
    )> = Vec::new();
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
                .pipe_validate(store, recent, archive, cutoff, &cli.range_profile)?
                .shards
                .into_iter()
                .map(|entry| (entry.shard_id, entry))
                .collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Default::default(),
            Err(error) => return Err(error.into()),
        };

    if previous
        .values()
        .any(|entry| entry.txid_segments.is_some() != cli.txid_display)
    {
        return Err("changing txid capability requires a separate publication directory".into());
    }

    // Preserve sealed artifacts only while their chain endpoints agree. A
    // changed sealed suffix belongs to a separate publication directory.
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
    geometry = if cutoff.is_some_and(|h| resume_height < h) {
        archive
    } else {
        recent
    };
    policy = SealPolicy::for_geometry(geometry);
    sealer = Sealer::resume(
        policy,
        resume_height,
        PageBasis::default(),
        *geometry,
        entries.len() as u64,
    );

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
            &cli.range_profile,
            geometry,
            &events,
        )?;

        let display = if cli.txid_display {
            let mut records = Vec::new();
            for height in shard.start_height..=shard.end_height {
                records.extend(store.display_at(height)?);
            }
            Some(transparent_shard::txid::build(
                shard.shard_id,
                geometry,
                &records,
            )?)
        } else {
            None
        };

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
        let wanted_choice = match cli.directory_choice {
            DirectoryChoice::Off => false,
            DirectoryChoice::Sealed => shard.reason.is_some(),
            DirectoryChoice::All => true,
        };
        if wanted_choice && built.choice.is_none() {
            eprintln!(
                "shard {} has no directory choice table: no seed peeled; \
                 publishing it for two directory queries",
                shard.shard_id
            );
        }
        let make = |revision: u32, supersedes: String, with_choice: bool| ShardManifest {
            schema: SCHEMA.to_string(),
            profile: cli.range_profile.clone(),
            geometry: geometry.name.to_string(),
            network: transparent_filter::NETWORK.to_string(),
            genesis_hash: store.genesis_hash().to_string(),
            shard_id: shard.shard_id,
            start_height: shard.start_height,
            end_height: shard.end_height,
            parent_block_hash: parent_block_hash.to_display_hex(),
            terminal_block_hash: terminal.to_display_hex(),
            tag_salt_counter: built.tag_salt_counter,
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
            txid_display: display.as_ref().map(|d| d.manifest(geometry)),
            directory_choice: built
                .choice
                .as_ref()
                .filter(|_| with_choice)
                .map(transparent_shard::manifest::encode_directory_choice),
        };

        let published = match previous.get(&shard.shard_id) {
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
        // Reproduction is judged against what the published revision chose,
        // so the same content keeps its identity whatever this run's option
        // says. Only a revision that changes anyway takes the option.
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
            PublishedRevision::next(shard.shard_id, published.as_ref(), reproduced)?;
        let manifest = make(revision, supersedes, with_choice);

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

        if let Some(display) = &display {
            for (index, segment) in display.directory.iter().enumerate() {
                write_immutable(&dir, &format!("txdirectory.{index}.bin"), segment)?;
            }
            for (index, segment) in display.pages.iter().enumerate() {
                write_immutable(&dir, &format!("txpages.{index}.bin"), segment)?;
            }
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
            txid_segments: display
                .as_ref()
                .map(|d| [d.directory.len() as u32, d.pages.len() as u32]),
            manifest_digest: digest.clone(),
            revision,
            sealed: manifest.sealed,
        };
        Ok((digest, terminal, entry))
    };

    for height in resume_height..=covered {
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
        if events.iter().any(|(_, event)| event.metadata().is_none()) {
            return Err(
                format!("height {height} lacks transaction metadata required by v11").into(),
            );
        }
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
    if !reorg && previous_dir != &cli.output && previous_dir.exists() {
        for entry in &map.shards {
            let mut retained = Vec::new();
            for path in std::fs::read_dir(previous_dir)? {
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
                link_revision(previous_dir, &cli.output, &digest)?;
            }
        }
    }
    map.check_shape()
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
                "recent_shards": by_geometry(recent.name),
                "archive_shards": cutoff.map(|_| by_geometry(archive.name)),
                "multi_segment_shards": map
                    .shards
                    .iter()
                    .filter(|entry| entry.directory_segments > 1 || entry.page_segments > 1)
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

/// Immutable journal view used by both the one-shot tool and controller snapshots.
pub trait Journal {
    fn journal_version(&self) -> u16;
    fn display_at(
        &self,
        _height: u64,
    ) -> Result<
        Vec<transparent_shard::txid::TransparentDisplayRecord>,
        crate::events::EventStoreError,
    > {
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
    ) -> Result<
        Vec<transparent_shard::txid::TransparentDisplayRecord>,
        crate::events::EventStoreError,
    > {
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
    ) -> Result<Self, BoxError> {
        self.check_shape()?;
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
            let policy = SealPolicy::for_geometry(g);
            let seal = self
                .seal
                .get(g.name)
                .ok_or("previous publication lacks seal parameters")?;
            if seal.max_scripts != policy.scripts.target
                || seal.max_page_rows != policy.page_rows.target
            {
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
            recent_geometry: "recent-4k".into(),
            archive_geometry: Some("recent-8k".into()),
            recent_from: Some(2),
            zakura_rpc_url: String::new(),
            zakura_cookie: PathBuf::new(),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: DirectoryChoice::Off,
            txid_display: false,
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
        let snapshot = Snapshot::capture(&journal, &before).unwrap();
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
        assert_eq!(grown.page_segments, rebuilt.page_segments);
        assert_eq!(grown.supersedes, before.shards[1].manifest_digest);
        assert!(b.join(&before.shards[1].manifest_digest).exists());
        let repeated = publish(&options(&b, None), &journal, zero).unwrap();
        assert_eq!(after, repeated);
        transparent_shard_server::shardset::ShardSet::open(&b, 3).unwrap();
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
        let snapshot = Snapshot::capture(&journal, &before).unwrap();
        let after = publish(&with(&b, Some(&a), DirectoryChoice::All), &snapshot, zero).unwrap();
        assert_eq!(before.shards[0], after.shards[0]);
        let tail = read(&b, &after.shards[1]);
        assert_eq!(tail.revision, 1);
        let table = tail
            .directory_choice()
            .unwrap()
            .expect("a grown tail takes the option");
        assert_eq!(u64::from(table.keys()), tail.occupancy.scripts);
        transparent_shard_server::shardset::ShardSet::open(&b, 3).unwrap();

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
        transparent_shard_server::shardset::ShardSet::open(&c, 3).unwrap();
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
        transparent_shard_server::shardset::ShardSet::open(&v2, 3).unwrap();

        let mut continued = options(&root.path().join("v2-continued"), Some(&v1));
        continued.range_profile = transparent_filter::RANGE_PROFILE_V2.name.into();
        assert!(publish(&continued, &journal, zero).is_err());

        let mut unknown = options(&root.path().join("unknown"), None);
        unknown.range_profile = "zcash-transparent-range-v99".into();
        assert!(publish(&unknown, &journal, zero).is_err());
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
        let snapshot = Snapshot::capture(&journal, &before).unwrap();
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
        transparent_shard_server::shardset::ShardSet::open(&b, 3).unwrap();
        // Original immutable artifact remains exactly the original map.
        let old: transparent_filter::ShardMap =
            serde_json::from_slice(&std::fs::read(a.join("shards.json")).unwrap()).unwrap();
        assert_eq!(old, before);
    }
}
