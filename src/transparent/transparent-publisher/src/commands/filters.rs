//! Verify a staged filter publication without a node or listener.
use crate::history::shard_filters::ShardFilters;
use clap::Parser;
use zakura_pir_publisher::transparent as transparent_shard;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    shard_dir: std::path::PathBuf,
}
pub async fn run(args: Vec<std::ffi::OsString>) -> Result<(), crate::ingest::BoxError> {
    let args = Args::parse_from(args);
    let dir = &args.shard_dir;
    let filters = ShardFilters::open(dir)?;
    let map = filters.map();
    println!(
        "{}",
        serde_json::json!({
            "shard_dir": dir,
            "schema": transparent_shard::manifest::SCHEMA,
            "profile": map.profile,
            "network": map.network,
            "genesis_hash": map.genesis_hash,
            "shards": filters.shard_count(),
            "start_height": map.start_height,
            "covered_through": filters.covered_through(),
            "map_sha256": filters.map_digest(),
        })
    );
    Ok(())
}
