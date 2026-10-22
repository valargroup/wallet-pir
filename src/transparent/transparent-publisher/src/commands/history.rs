//! One-shot shard publication; continuous operation uses the same builder.
use zakura_pir_publisher::transparent::filter as transparent_filter;

use crate::history::publication::DirectoryChoice;
use crate::history::publication::{publish, PublishOptions as BuildOptions};
use crate::BoxError;
use clap::Parser;
use std::path::PathBuf;
use transparent_filter::BlockHash;
use transparent_shared::journal::events::EventStore;
#[derive(Parser)]
#[command(name = "shard-publish", about = "Publish a transparent shard set")]
struct PublishOptions {
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

#[derive(Parser)]
#[command(
    name = "pir-publisher publish-history",
    about = "Publish a transparent shard set"
)]
struct Cli {
    #[command(flatten)]
    options: PublishOptions,
    /// Display hash of the block before the journal's first height, instead
    /// of asking the node for it.
    ///
    /// For publishing a journal slice offline, where the one thing the node
    /// is needed for is already known. Ignored for a journal starting at zero.
    #[arg(long)]
    parent_block_hash: Option<String>,
    /// Read a journal another process is writing, without its lock.
    ///
    /// For building a publication beside a live writer, such as the first v12
    /// set from the journal v11's controller ingests. Nothing is written
    /// beside the journal. Give `--through` a margin below the tip: a rollback
    /// rewrites the suffix it drops, so the publish refuses its result unless
    /// the block at `--through` is still committed when it finishes.
    #[arg(long, requires = "through")]
    follow_journal: bool,
}

impl From<PublishOptions> for BuildOptions {
    fn from(options: PublishOptions) -> Self {
        Self {
            data_dir: options.data_dir,
            previous: options.previous,
            output: options.output,
            recent_geometry: options.recent_geometry,
            archive_geometry: options.archive_geometry,
            recent_from: options.recent_from,
            zakura_rpc_url: options.zakura_rpc_url,
            zakura_cookie: options.zakura_cookie,
            through: options.through,
            record: options.record,
            source_sha: options.source_sha,
            directory_choice: options.directory_choice,
            range_profile: options.range_profile,
        }
    }
}

pub async fn run(args: Vec<std::ffi::OsString>) -> Result<(), BoxError> {
    let Cli {
        options: cli,
        parent_block_hash,
        follow_journal,
    } = Cli::parse_from(args);
    let cli: BuildOptions = cli.into();
    // A standalone build has no controller snapshot. Unless told to follow a
    // live writer, refuse concurrent ingest rather than reading through a
    // truncation/reorg under an old checkpoint.
    let _lock = if follow_journal {
        None
    } else {
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(cli.data_dir.join("writer.lock"))?;
        lock.try_lock_shared().map_err(|e| {
            format!(
                "journal is being ingested; pause its writer, use controller publication \
                 or pass --follow-journal: {e}"
            )
        })?;
        Some(lock)
    };
    let store = EventStore::open_existing(&cli.data_dir)?;
    let parent = if store.start_height() == 0 {
        BlockHash::from_internal_bytes([0; 32])
    } else if let Some(parent) = parent_block_hash {
        BlockHash::from_display_hex(&parent)?
    } else {
        let rpc = crate::ingest::zakura::connect(&cli.zakura_rpc_url, &cli.zakura_cookie)?;
        BlockHash::from_internal_bytes(rpc.block_hash(store.start_height() - 1).await?)
    };
    let endpoint = cli
        .through
        .and_then(|through| store.block_at(through).map(|b| (through, b)));
    publish(&cli, &store, parent)?;
    if follow_journal {
        let (through, entry) = endpoint.ok_or("--through is not committed in the journal")?;
        if !store.still_committed(through, entry)? {
            return Err(format!(
                "the journal's writer replaced block {through} while it was published; \
                 the publication is not of the chain the journal now holds"
            )
            .into());
        }
    }
    Ok(())
}
