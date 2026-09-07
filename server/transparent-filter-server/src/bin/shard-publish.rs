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
use transparent_shard::layout::{DIRECTORY_ROWS, DIRECTORY_ROW_BYTES, PAGE_ROWS, PAGE_ROW_BYTES};
use transparent_shard::manifest::{
    ManifestLayout, ManifestOccupancy, ManifestSeal, PublishedRevision, ShardManifest,
    TableGeometry, SCHEMA,
};
use transparent_shard::records::DIRECTORY_SLOTS;
use transparent_shard::seal::{Limit, SealPolicy, Sealer};

/// Scripts one directory segment holds, which is the geometry and not a guess.
const DIRECTORY_CAPACITY: u64 = DIRECTORY_ROWS as u64 * DIRECTORY_SLOTS as u64;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(name = "shard-publish", about = "Publish a transparent shard set")]
struct Cli {
    #[arg(long, default_value = "./transparent-event-data")]
    data_dir: PathBuf,
    #[arg(long, default_value = "./transparent-shards")]
    output: PathBuf,
    /// Scripts at which a shard prefers to seal.
    ///
    /// Six sevenths of what a directory segment holds. The remaining seventh is
    /// what one more block may add before the seal takes effect, and what
    /// two-choice placement needs to keep every shard in one segment: relocation
    /// holds this load, and the measured run placed 511 of 511 shards without a
    /// second segment.
    #[arg(long, default_value_t = DIRECTORY_CAPACITY - DIRECTORY_CAPACITY / 7)]
    scripts_target: u64,
    /// Scripts a shard's directory can hold.
    ///
    /// Derived, never restated. This was a literal 28,672 that happened to
    /// equal the geometry, which is a number that stops being true the moment
    /// the geometry moves and says nothing when it does.
    #[arg(long, default_value_t = DIRECTORY_CAPACITY)]
    scripts_capacity: u64,
    /// Page rows at which a shard prefers to seal.
    ///
    /// Held under `PAGE_ROWS` with headroom, so a shard stays single-segment: a
    /// second segment multiplies query cost for every user of the shard.
    #[arg(long, default_value_t = PAGE_ROWS as u64 - PAGE_ROWS as u64 / 32)]
    page_rows_target: u64,
    #[arg(long, default_value_t = PAGE_ROWS as u64)]
    page_rows_capacity: u64,
    /// Needed for exactly one thing: the block hash before the journal's first
    /// height, which is shard zero's parent and is by definition not in the
    /// journal.
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    zakura_rpc_url: String,
    #[arg(long)]
    zakura_cookie: PathBuf,
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
    std::fs::write(&path, bytes)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let store = EventStore::open_existing(&cli.data_dir)?;
    let Some(covered) = store.covered_through() else {
        return Err("the journal is empty".into());
    };
    let first = store.start_height();

    let policy = SealPolicy {
        scripts: Limit::new(cli.scripts_target, cli.scripts_capacity)?,
        page_rows: Limit::new(cli.page_rows_target, cli.page_rows_capacity)?,
    };
    let genesis = BlockHash::from_display_hex(store.genesis_hash())?;

    // Shard zero's parent is the block before coverage begins, which the
    // journal does not hold. One RPC call, and the only reason this needs a
    // node at all.
    let zakura = ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, &cli.zakura_cookie)?;
    let base_parent = BlockHash::from_display_hex(&zakura.block_hash(first - 1).await?)?;

    std::fs::create_dir_all(&cli.output)?;
    eprintln!("journal {first}-{covered}, sealing at {policy:?}");

    // Buffer each shard's events as the sealer decides boundaries. A shard's
    // events are needed all at once, since pages are per script and a script's
    // history can span the whole shard.
    let mut sealer = Sealer::new(policy, first);
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
                   parent_manifest_digest: &str|
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
                    rows: DIRECTORY_ROWS as u64,
                    row_bytes: DIRECTORY_ROW_BYTES as u32,
                    sha256: hex::encode(Sha256::digest(segment)),
                })
                .collect(),
            page_segments: built
                .pages
                .iter()
                .map(|segment| TableGeometry {
                    rows: PAGE_ROWS as u64,
                    row_bytes: PAGE_ROW_BYTES as u32,
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
            let (digest, terminal, entry) =
                publish(shard, &pending, parent_block_hash, &parent_manifest_digest)?;
            parent_manifest_digest = digest;
            parent_block_hash = terminal;
            entries.push(entry);
            pending = carried;
        }
    }
    if let Some(shard) = sealer.finish() {
        let (_, _, entry) = publish(shard, &pending, parent_block_hash, &parent_manifest_digest)?;
        entries.push(entry);
    }

    let map = transparent_filter::ShardMap {
        genesis_hash: store.genesis_hash().to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: first,
        seal: transparent_filter::SealParameters {
            max_scripts: policy.scripts.target,
            max_page_rows: policy.page_rows.target,
            max_txids: 0,
        },
        shards: entries,
    };
    map.check_shape()
        .map_err(|error| format!("the published map is malformed: {error}"))?;
    std::fs::write(
        cli.output.join("shards.json"),
        serde_json::to_vec_pretty(&map)?,
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
