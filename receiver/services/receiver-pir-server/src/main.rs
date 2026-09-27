use clap::Parser;
use receiver_directory::snapshot::{Manifest, Snapshot, ROW_BYTES};
use receiver_pir::{server::Server, ROWS};
use std::{error::Error, fs::File, io::Read, net::SocketAddr, path::PathBuf};

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
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    if !args.bind.ip().is_loopback() {
        return Err("this integration service only binds loopback".into());
    }
    let mut bytes = Vec::new();
    File::open(&args.manifest)?
        .take(16385)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 16384 {
        return Err("manifest too large".into());
    }
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    manifest.validate()?;
    if manifest.rows != ROWS as u32 {
        return Err("publication needs 8192 rows for this PIR profile".into());
    }
    let revision = hex::encode(manifest.revision()?);
    let rows = args.manifest.with_file_name(format!("{revision}.rows"));
    let mut file = File::open(rows)?;
    if file.metadata()?.len() != (ROWS * ROW_BYTES) as u64 {
        return Err("incorrect row file length".into());
    }
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;
    eprintln!("Preparing receiver PIR publication {revision}");
    let proof_path = args.manifest.with_file_name(format!("{revision}.witness"));
    let witnesses = if proof_path.exists() {
        let mut proof = Vec::new();
        File::open(proof_path)?
            .take((receiver_directory::witness::MAX_WITNESS_BYTES + 1) as u64)
            .read_to_end(&mut proof)?;
        receiver_directory::witness::WitnessSnapshot::decode(&proof, &manifest)?;
        Some(proof)
    } else {
        None
    };
    let server = Server::new(Snapshot { manifest, data })?;
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    eprintln!("Receiver PIR ready at {}", listener.local_addr()?);
    axum::serve(
        listener,
        receiver_pir_server::router_with_witnesses(server, witnesses),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}
