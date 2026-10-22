//! Resumable Receiver indexer and serving command.
use clap::Parser;
use receiver_indexer::service::Config;
use tracing_subscriber::EnvFilter;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[tokio::main]
async fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("serve-publication") {
        // Serves what an operator stages; never indexes, opens no database and reads
        // no partner key. See `receiver_indexer::serve_publication`.
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
            .init();
        let args = receiver_indexer::serve_publication::Args::parse_from(std::env::args().skip(1));
        return receiver_indexer::serve_publication::main(args).await;
    }
    if std::env::args().nth(1).as_deref() == Some("probe") {
        let args = receiver_indexer::probe::Args::parse_from(std::env::args().skip(1));
        std::process::exit(if receiver_indexer::probe::run(args).await {
            0
        } else {
            1
        });
    }
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Config::parse();
    let stop = pir_pipeline::service::Shutdown::default();
    let signals = stop.clone();
    let mut tasks = pir_pipeline::service::Tasks::default();
    let task = tokio::spawn(async move {
        shutdown().await;
        signals.request();
    });
    tasks.own(&task);
    receiver_indexer::service::run(
        args,
        pir_pipeline::service::Http::new(|addr, app| async move {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            // The bound address, which differs from `addr` for port 0.
            tracing::info!(address = %listener.local_addr()?, "receiver PIR bound");
            axum::serve(listener, app).await
        }),
        stop,
    )
    .await
}

/// Resolves on Ctrl-C, or on SIGTERM on Unix.
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
