use clap::{Parser, Subcommand};
use enhance_pir::client::EnhancePirClient;

#[derive(Parser)]
#[command(
    name = "enhance-pir-cli",
    about = "Query the Ironwood Enhance PIR service"
)]
struct Cli {
    #[arg(long)]
    server: String,
    /// Select the incompatible architecture-2 protocol on its separate origin.
    #[arg(long)]
    v4: bool,
    /// Public query domain for a v4 dummy query; defaults to the first current shard.
    #[arg(long, requires = "v4")]
    shard: Option<u64>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Metadata,
    Query { position: u64 },
    Dummy,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if !cli.v4 {
        return Err("legacy client transport is retired; pass --v4 for schema 11".into());
    }
    if cli.v4 {
        let mut client = enhance_pir::v4_client::EnhancePirClient::connect(&cli.server).await?;
        match cli.command {
            Command::Metadata => println!("{}", serde_json::to_string_pretty(client.manifest())?),
            Command::Query { position } => println!(
                "{}",
                hex::encode(
                    client
                        .query_position_with_timing(position)
                        .await?
                        .0
                        .as_bytes()
                )
            ),
            Command::Dummy => {
                let shard = cli.shard.unwrap_or(client.manifest().coverage.shards[0].id);
                client.query_dummy(shard).await?;
                println!("dummy query completed");
            }
        }
        return Ok(());
    }
    let client = EnhancePirClient::connect(&cli.server).await?;
    match cli.command {
        Command::Metadata => println!("{}", serde_json::to_string_pretty(client.generation())?),
        Command::Query { position } => {
            let record = client.query_position(position).await?;
            println!("{}", hex::encode(record.as_bytes()));
        }
        Command::Dummy => {
            client.query_dummy().await?;
            println!("dummy query completed");
        }
    }
    Ok(())
}
