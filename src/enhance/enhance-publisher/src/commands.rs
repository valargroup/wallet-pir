//! Enhance publisher diagnostics and offline maintenance.
use crate::chain_source::ZakuraClient;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Report compiled capabilities without opening state or loading a GPU driver.
    BuildInfo,
    /// Offline transfer of recovery floors for the locked HTTPS cutover.
    HandoffHttpsState {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        transaction: String,
        /// Fence retained sessions before restoring the previous runtime.
        #[arg(long)]
        rollback: bool,
    },
    /// Prepare a complete immutable publication from a stopped canonical journal.
    /// The wallet manifest pins coverage, recovery identities and expected setup.
    PreparePublication {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Offline re-derivation of a coordinator journal's records from chain into
    /// a staged journal that a restarted coordinator adopts. Reads the source
    /// without its lock; only fee metadata may differ.
    RebuildJournal {
        /// Coordinator data directory holding `enhance/`.
        #[arg(long)]
        source: PathBuf,
        /// Staged journal, normally `<data-dir>/enhance-staged`.
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        zakura_rpc_url: String,
        #[arg(long)]
        zakura_cookie: PathBuf,
        /// Last height to rebuild; defaults to the source journal's tip.
        #[arg(long)]
        through_height: Option<u64>,
        /// Non-empty blocks fetched concurrently.
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[arg(long, default_value_t = crate::chain_source::DEFAULT_CACHE_OUTPUTS)]
        cache_outputs: usize,
    },
}

pub async fn run(
    args: Vec<std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse_from(args);
    match cli.command {
        Command::HandoffHttpsState {
            data_dir,
            transaction,
            rollback,
        } => {
            println!(
                "{}",
                crate::handoff::transfer(&data_dir, rollback, &transaction)?
            );
        }
        Command::PreparePublication {
            source,
            manifest,
            output,
        } => {
            let report = tokio::task::spawn_blocking(move || -> Result<_, String> {
                let manifest: enhance_material::protocol::Manifest =
                    crate::publication::read_metadata(&manifest)?;
                manifest.validate()?;
                let journal = crate::journal::CommittedJournalSnapshot::read(
                    &source, crate::types::DatabaseId::Enhance, crate::types::ENHANCE_LAYOUT,
                ).map_err(|e| e.to_string())?;
                if journal.tree_size() != manifest.coverage.records
                    || journal.blocks().last().is_none_or(|block| {
                        block.height != manifest.anchor_height || block.hash != manifest.anchor_block_hash
                    })
                {
                    return Err("journal differs from publication anchor or coverage".into());
                }
                let prepared = crate::publication::prepare_publication(
                    &output, manifest.coverage.clone(), &manifest.domain_recovery_epochs,
                    |start, count| journal.read_records(start, count).map_err(|e| e.to_string()),
                )?;
                let inventory = prepared.inventory();
                if !inventory.supports(&manifest)? {
                    return Err("prepared material differs from wallet manifest".into());
                }
                Ok(serde_json::json!({"inventory_sha256": enhance_material::protocol::digest(inventory), "domains": inventory.domains.len()}))
            }).await??;
            println!("{}", serde_json::to_string(&report)?);
        }
        Command::BuildInfo => {
            println!(
                "{}",
                serde_json::json!({
                    "native_reinspiring": cfg!(feature = "native-reinspiring"),
                    "source_revision": option_env!("WALLET_PIR_SOURCE_REVISION"),
                })
            );
        }
        Command::RebuildJournal {
            source,
            output,
            zakura_rpc_url,
            zakura_cookie,
            through_height,
            concurrency,
            cache_outputs,
        } => {
            let receipt = crate::rebuild::rebuild_journal(
                ZakuraClient::from_cookie_file(&zakura_rpc_url, &zakura_cookie)?,
                crate::rebuild::RebuildConfig {
                    source,
                    output,
                    through_height,
                    concurrency,
                    cache_outputs,
                },
            )
            .await?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
    }
    Ok(())
}
