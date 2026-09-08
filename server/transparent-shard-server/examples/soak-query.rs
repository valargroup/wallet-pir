//! Operator-only sustained private queries, checked against published plaintext.
//! No scripts or returned row bytes are logged. Stale revisions are retried;
//! a transport failure or an incorrect decoded row fails the run.
use clap::Parser;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use transparent_wallet::{
    client::{Table, TableClient},
    http::{HttpFilterSource, HttpOptions, HttpShardTransport},
    transport::{BoxError, FilterSource, Overloaded, ShardTransport, StaleRevision},
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    url: String,
    #[arg(long)]
    publications: PathBuf,
    #[arg(long, default_value_t = 173)]
    shard: usize,
    #[arg(long, default_value_t = 21600)]
    seconds: u64,
}
#[derive(Deserialize)]
struct Setup {
    public_params: String,
    public_params_sha256: String,
}

fn once(args: &Args, table: Table) -> Result<serde_json::Value, BoxError> {
    let options = HttpOptions {
        timeout: Duration::from_secs(60),
        ..Default::default()
    };
    let mut filters = HttpFilterSource::new(&args.url, &options)?;
    let mut transport = HttpShardTransport::new(&args.url, &options)?;
    let (bytes, _) = filters.shard_map()?;
    let map: transparent_filter::ShardMap = serde_json::from_slice(&bytes)?;
    map.check_shape().map_err(|e| format!("map: {e}"))?;
    let entry = map.shards.get(args.shard).ok_or("shard absent")?;
    let geometry = transport.geometry()?;
    let g = geometry
        .geometries
        .iter()
        .find(|g| g.name == entry.geometry)
        .ok_or("geometry absent")?;
    let (rows, width, seed, scheme, segments, name) = match table {
        Table::Directory => (
            g.directory_rows,
            g.directory_row_bytes,
            g.directory_setup_seed,
            &g.directory_scheme,
            entry.directory_segments,
            "directory",
        ),
        Table::Pages => (
            g.page_rows,
            g.page_row_bytes,
            g.pages_setup_seed,
            &g.pages_scheme,
            entry.page_segments,
            "pages",
        ),
    };
    let directory = find_revision(&args.publications, &entry.manifest_digest)?;
    let manifest_bytes = std::fs::read(directory.join("manifest.json"))?;
    let manifest: transparent_shard::manifest::ShardManifest =
        serde_json::from_slice(&manifest_bytes)?;
    if manifest.digest() != entry.manifest_digest {
        return Err("local manifest digest mismatch".into());
    }
    let sources = if name == "directory" {
        &manifest.directory_segments
    } else {
        &manifest.page_segments
    };
    let mut expected = Vec::new();
    // Hold the expected bytes before the publisher can collect this directory.
    for (segment, source) in sources.iter().enumerate() {
        let path = directory.join(format!("{name}.{segment}.bin"));
        let mut file = std::fs::File::open(&path)?;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 65536];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        if hex::encode(hash.finalize()) != source.sha256 {
            return Err("local table digest mismatch".into());
        }
        file.seek(SeekFrom::Start(7 * u64::from(width)))?;
        let mut row = vec![0; width as usize];
        file.read_exact(&mut row)?;
        expected.push(row);
    }
    let mut client = TableClient::new(table, rows, width, seed, scheme)?;
    for segment in 0..segments {
        let (bytes, _) = transport.setup(entry.shard_id, &entry.manifest_digest, table, segment)?;
        let setup: Setup = serde_json::from_slice(&bytes)?;
        client.open_segment(
            &entry.manifest_digest,
            segment,
            &setup.public_params,
            &setup.public_params_sha256,
        )?;
    }
    let query = client.prepare(&entry.manifest_digest, 7)?;
    let started = Instant::now();
    let response = transport.query(entry.shard_id, &entry.manifest_digest, table, &query.body)?;
    let decoded = client.decode(&entry.manifest_digest, segments, query, &response)?;
    if decoded != expected {
        return Err("private query differs from published plaintext".into());
    }
    Ok(
        serde_json::json!({"event":"query", "height":entry.end_height,"revision":entry.manifest_digest,"table":name,"seconds":started.elapsed().as_secs_f64(),"exact":true}),
    )
}

fn find_revision(root: &Path, digest: &str) -> Result<PathBuf, BoxError> {
    for item in std::fs::read_dir(root)? {
        let directory = item?.path().join(digest);
        if directory.join("manifest.json").is_file() {
            return Ok(directory);
        }
    }
    Err("published source revision unavailable".into())
}
fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let started = Instant::now();
    let mut last_success = Instant::now();
    let mut queries = 0u64;
    let mut retries = 0u64;
    while started.elapsed() < Duration::from_secs(args.seconds) {
        if last_success.elapsed() > Duration::from_secs(60) {
            return Err("no successful private query for sixty seconds".into());
        }
        let table = if queries.is_multiple_of(2) {
            Table::Directory
        } else {
            Table::Pages
        };
        match once(&args, table) {
            Ok(value) => {
                println!("{value}");
                queries += 1;
                last_success = Instant::now();
            }
            Err(error)
                if StaleRevision::found_in(&error).is_some()
                    || Overloaded::found_in(&error).is_some() =>
            {
                retries += 1;
                println!(
                    "{}",
                    serde_json::json!({"event":"retry","error":error.to_string()})
                );
                std::thread::sleep(Duration::from_secs(1));
            }
            Err(error) => return Err(error),
        }
    }
    println!(
        "{}",
        serde_json::json!({"event":"result","queries":queries,"retries":retries,"seconds":started.elapsed().as_secs_f64(),"passed":queries>0})
    );
    if queries == 0 {
        return Err("no queries completed".into());
    }
    Ok(())
}
