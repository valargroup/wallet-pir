use clap::Parser;
use receiver_pir_server::{Publication, Publications};
use std::{error::Error, net::SocketAddr, path::PathBuf};

#[derive(Parser)]
#[command(about = "Serve one immutable receiver publication on loopback for local testing")]
struct Args {
    /// current.json or an immutable revision manifest produced by receiver-directory.
    #[arg(long)]
    manifest: PathBuf,
    #[arg(long, default_value = "127.0.0.1:18380")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();
    if !args.bind.ip().is_loopback() {
        return Err("this integration service only binds loopback".into());
    }
    let publications = Publications::default();
    publications.publish(Publication::load(&args.manifest)?, 0);
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    eprintln!("Receiver PIR ready at {}", listener.local_addr()?);
    axum::serve(
        listener,
        receiver_pir_server::router_with_publications(publications),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}
