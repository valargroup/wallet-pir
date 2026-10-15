//! Isolated synthetic backend. Uses a declared synthetic source; never enables wallet traffic.
use clap::{Parser, Subcommand};
use enhance_pir::status::MAX_ENTRIES;
use enhance_pir_server::status::{self, cli};
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
        #[arg(long)]
        origin: String,
        /// Send encrypted queries directly here; initialization still uses origin.
        #[arg(long)]
        query_origin: Option<String>,
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        rpc_url: String,
        #[arg(long)]
        cookie: PathBuf,
        #[arg(long, default_value_t = 60)]
        seconds: u64,
        #[arg(long)]
        report_dir: PathBuf,
    },
    /// Qualification controller using the same publisher integrated into Enhance.
    ServeDistributed {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        rpc_url: String,
        #[arg(long)]
        cookie: PathBuf,
        /// Loopback listener for the public `/v1/status/*` routes that the
        /// HTTPS ingress proxies to. Required when the config enables public
        /// routes; refused otherwise.
        #[arg(long)]
        public_listen: Option<SocketAddr>,
    },
    /// Encrypted oracle validation across real router and worker child processes.
    ValidateDistributed {
        #[arg(long, default_value_t = 32)]
        entries: usize,
        #[arg(long)]
        cuda: bool,
        #[arg(long)]
        state_dir: PathBuf,
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
    /// Independent private role. Loopback endpoints require authenticated SSH forwarding.
    Role {
        #[arg(long, value_enum)]
        role: status::distributed::Role,
        #[arg(long)]
        network_hex: String,
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        listen: SocketAddr,
        /// Router only: serve `/v1/status/query` on this separate loopback
        /// listener and keep control, artifact, public, evaluate and telemetry
        /// on `--listen`. Omitted: one merged listener.
        #[arg(long)]
        query_listen: Option<SocketAddr>,
        #[arg(long)]
        artifact_origin: String,
        #[arg(long, default_value = "http://127.0.0.1:8381")]
        worker_origin: String,
        #[arg(long)]
        cuda: bool,
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
    /// Run the synthetic backend on loopback; use an SSH tunnel for remote probes.
    Serve {
        #[arg(long,default_value_t=MAX_ENTRIES)]
        entries: usize,
        #[arg(long)]
        cuda: bool,
        #[arg(long, default_value = "127.0.0.1:8380")]
        listen: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8381")]
        worker: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8382")]
        router: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8383")]
        monitoring: SocketAddr,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Experimental live Status serving on loopback. Roles still share this process.
    ServeLive {
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        rpc_url: String,
        #[arg(long)]
        cookie: PathBuf,
        #[arg(long)]
        salt_hex: String,
        #[arg(long, default_value_t = 64)]
        window_blocks: u32,
        #[arg(long)]
        cuda: bool,
        #[arg(long, default_value = "127.0.0.1:8380")]
        listen: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8381")]
        worker: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8382")]
        router: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8383")]
        monitoring: SocketAddr,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Full encrypted HTTP correctness, update/rebuild differential, and failure checks.
    Validate {
        #[arg(long,default_value_t=MAX_ENTRIES)]
        entries: usize,
        #[arg(long)]
        cuda: bool,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Probe or load an already running synthetic backend, validating all answers.
    Probe {
        #[arg(long, default_value = "http://127.0.0.1:8380")]
        origin: String,
        #[arg(long,default_value_t=MAX_ENTRIES)]
        entries: usize,
        #[arg(long, default_value_t = 4)]
        queries: usize,
        #[arg(long, default_value_t = 1)]
        concurrency: usize,
        #[arg(long, default_value_t = 0)]
        qps: u64,
        /// Schedule qps * seconds arrivals; intended for long open-loop runs.
        #[arg(long)]
        seconds: Option<u64>,
        /// Optional per-arrival JSONL evidence. Contains no txids or query material.
        #[arg(long)]
        report_jsonl: Option<PathBuf>,
        /// Fail the probe if complete-lookup p99 exceeds this bound.
        #[arg(long)]
        max_p99_ms: Option<f64>,
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
            query_origin,
            rpc_url,
            cookie,
            seconds,
            report_dir,
        } => {
            cli::probe_live_load(origin, query_origin, rpc_url, cookie, seconds, report_dir).await?
        }
        Command::ServeDistributed {
            config,
            rpc_url,
            cookie,
            public_listen,
        } => cli::serve_distributed(config, rpc_url, cookie, public_listen).await?,
        Command::ValidateDistributed {
            entries,
            cuda,
            state_dir,
            existing_worker,
            artifact_listen,
            request_timeout_seconds,
        } => {
            cli::validate_distributed(
                entries,
                cuda,
                state_dir,
                existing_worker,
                artifact_listen,
                request_timeout_seconds,
            )
            .await?
        }
        Command::Role {
            role,
            network_hex,
            state_dir,
            listen,
            query_listen,
            artifact_origin,
            worker_origin,
            cuda,
        } => {
            cli::role(cli::RoleConfig {
                role,
                network_hex,
                state_dir,
                listen,
                query_listen,
                artifact_origin,
                worker_origin,
                cuda,
            })
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
        Command::Probe {
            origin,
            entries,
            queries,
            concurrency,
            qps,
            seconds,
            report_jsonl,
            max_p99_ms,
        } => {
            cli::probe(cli::ProbeConfig {
                origin,
                entries,
                queries,
                concurrency,
                qps,
                seconds,
                report_jsonl,
                max_p99_ms,
            })
            .await?
        }
        Command::ProbeLive {
            origin,
            rpc_url,
            cookie,
            queries,
        } => cli::probe_live(origin, rpc_url, cookie, queries).await?,
        Command::Serve {
            entries,
            cuda,
            listen,
            worker,
            router,
            monitoring,
            state_dir,
        } => cli::serve(entries, cuda, listen, worker, router, monitoring, state_dir).await?,
        Command::ServeLive {
            rpc_url,
            cookie,
            salt_hex,
            window_blocks,
            cuda,
            listen,
            worker,
            router,
            monitoring,
            state_dir,
        } => {
            cli::serve_live(cli::LiveConfig {
                rpc_url,
                cookie,
                salt_hex,
                window_blocks,
                cuda,
                listen,
                worker,
                router,
                monitoring,
                state_dir,
            })
            .await?
        }
        Command::Validate {
            entries,
            cuda,
            state_dir,
        } => cli::validate(entries, cuda, state_dir).await?,
    }
    Ok(())
}
