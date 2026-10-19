//! The Status publisher and its diagnostics. Query roles run in `pir-query-server`;
//! this binary never serves wallets.
use clap::{Parser, Subcommand};
use status_publisher::cli;
use std::{net::SocketAddr, path::PathBuf};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Open-loop live mined-answer smoke load; publication qualification is assessed separately.
    ProbeLiveLoad {
        /// The router's wallet origin: init, session and query.
        #[arg(long)]
        origin: String,
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        rpc_url: String,
        #[arg(long)]
        cookie: PathBuf,
        #[arg(long, default_value_t = 60)]
        seconds: u64,
        #[arg(long)]
        report_dir: PathBuf,
    },
    /// Encrypted oracle validation across real `pir-query-server` router and
    /// worker child processes.
    ValidateDistributed {
        #[arg(long, default_value_t = 32)]
        entries: usize,
        #[arg(long)]
        cuda: bool,
        #[arg(long)]
        state_dir: PathBuf,
        /// `pir-query-server` built with the `status` feature (and `cuda` for
        /// `--cuda`). Default: beside this executable.
        #[arg(long)]
        query_server: Option<PathBuf>,
        /// Validate an already installed synthetic-fixture worker instead of spawning one.
        #[arg(long)]
        existing_worker: Option<String>,
        /// Fixture artifact listener; must match an existing worker's artifact origin.
        #[arg(long, default_value = "127.0.0.1:0")]
        artifact_listen: SocketAddr,
        /// Diagnostic request deadline, separate from production transport policy.
        #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u64).range(1..=900))]
        request_timeout_seconds: u64,
    },
    /// Read a consistent live block/mempool window without publishing it.
    ObserveLive {
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        rpc_url: String,
        #[arg(long)]
        cookie: PathBuf,
        /// Public index salt, exactly 32 bytes of hex.
        #[arg(long)]
        salt_hex: String,
        #[arg(long, default_value_t = 64)]
        window_blocks: u32,
        #[arg(long, default_value_t = 1)]
        samples: usize,
        #[arg(long, default_value_t = 1000)]
        interval_ms: u64,
        /// Persist canonical block and fork cache; every restart still reobserves the source.
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Check a canonical mined txid through a live encrypted query on loopback.
    ProbeLive {
        #[arg(long, default_value = "http://127.0.0.1:8380")]
        origin: String,
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        rpc_url: String,
        #[arg(long)]
        cookie: PathBuf,
        #[arg(long, default_value_t = 4)]
        queries: usize,
    },
}

#[tokio::main]
async fn main() -> Result<(), cli::AnyError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    match Args::parse().command {
        Command::ProbeLiveLoad {
            origin,
            rpc_url,
            cookie,
            seconds,
            report_dir,
        } => cli::probe_live_load(origin, rpc_url, cookie, seconds, report_dir).await?,
        Command::ValidateDistributed {
            entries,
            cuda,
            state_dir,
            query_server,
            existing_worker,
            artifact_listen,
            request_timeout_seconds,
        } => {
            cli::validate_distributed(
                entries,
                cuda,
                state_dir,
                query_server,
                existing_worker,
                artifact_listen,
                request_timeout_seconds,
            )
            .await?
        }
        Command::ObserveLive {
            rpc_url,
            cookie,
            salt_hex,
            window_blocks,
            samples,
            interval_ms,
            state_dir,
        } => {
            cli::observe_live(
                rpc_url,
                cookie,
                salt_hex,
                window_blocks,
                samples,
                interval_ms,
                state_dir,
            )
            .await?
        }
        Command::ProbeLive {
            origin,
            rpc_url,
            cookie,
            queries,
        } => cli::probe_live(origin, rpc_url, cookie, queries).await?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_controller_is_rejected_and_probes_remain() {
        assert!(Args::try_parse_from(["status-pir", "serve-distributed"]).is_err());
        for command in [
            "probe-live",
            "probe-live-load",
            "observe-live",
            "validate-distributed",
        ] {
            let error = Args::try_parse_from(["status-pir", command, "--help"])
                .err()
                .unwrap();
            assert_eq!(error.kind(), clap::error::ErrorKind::DisplayHelp);
        }
    }
}
