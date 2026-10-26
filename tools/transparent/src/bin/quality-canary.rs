//! One public native query, checked against a pinned independently extracted row hash.
use clap::Parser;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use zakura_pir_client::transparent::engine::{
    client::{Table, TableClient},
    http::{HttpOptions, HttpShardTransport},
    transport::{BoxError, ShardTransport},
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    url: String,
    #[arg(long)]
    fixture: PathBuf,
    #[arg(long)]
    fixture_sha256: String,
    #[arg(long, required_unless_present = "skip_chain_check")]
    rpc_url: Option<String>,
    #[arg(long, required_unless_present = "skip_chain_check")]
    cookie: Option<PathBuf>,
    #[arg(long, required_unless_present = "skip_chain_check")]
    anchor_height: Option<u64>,
    #[arg(long, required_unless_present = "skip_chain_check")]
    anchor_hash: Option<String>,
    /// Trust the pinned fixture's anchor without asking a chain node. Deploy probes
    /// run on query hosts, which have none; the row hashes are still checked.
    #[arg(long)]
    skip_chain_check: bool,
    /// Smoke every table in the pinned fixture, covering every segment.
    /// Run after the service is ready; this process never controls the service.
    #[arg(long)]
    all: bool,
}
#[derive(Deserialize)]
struct Fixture {
    schema: String,
    tables: Vec<FixtureTable>,
}
#[derive(Deserialize)]
struct FixtureTable {
    shard_id: u64,
    revision: String,
    geometry: String,
    table: String,
    rows: u64,
    row_bytes: u32,
    segments: u32,
    samples: Vec<Sample>,
}
#[derive(Deserialize)]
struct Sample {
    row: usize,
    sha256: Vec<String>,
    nonempty: bool,
}
#[derive(Deserialize)]
struct Setup {
    public_params: String,
    public_params_sha256: String,
}
fn canonical(args: &Args) -> Result<bool, BoxError> {
    let (Some(rpc_url), Some(cookie), Some(anchor_height), Some(anchor_hash)) = (
        &args.rpc_url,
        &args.cookie,
        args.anchor_height,
        &args.anchor_hash,
    ) else {
        return Ok(args.skip_chain_check);
    };
    let cookie = std::fs::read_to_string(cookie)?;
    let (user, password) = cookie
        .trim()
        .split_once(':')
        .ok_or("invalid RPC credential format")?;
    let response:serde_json::Value=reqwest::blocking::Client::builder().timeout(Duration::from_secs(4)).build()?.post(rpc_url).basic_auth(user,Some(password)).json(&serde_json::json!({"jsonrpc":"1.0","id":"pir-canary","method":"getblockhash","params":[anchor_height]})).send()?.error_for_status()?.json()?;
    Ok(response["result"].as_str() == Some(anchor_hash.as_str()) && response["error"].is_null())
}
fn probe(args: &Args) -> Result<&'static str, BoxError> {
    let raw = std::fs::read(&args.fixture)?;
    if hex::encode(Sha256::digest(&raw)) != args.fixture_sha256 || !canonical(args)? {
        return Ok("oracle_invalid");
    }
    let fixture: Fixture = serde_json::from_slice(&raw)?;
    let options = HttpOptions {
        timeout: Duration::from_secs(8),
        user_agent: "pir-independent-canary".into(),
    };
    let mut transport = HttpShardTransport::new(&args.url, &options)?
        .with_retry_attempts(1)
        .with_transient_retry_attempts(1);
    let geometry = transport.geometry()?;
    if geometry.schema != fixture.schema {
        return Ok("oracle_invalid");
    }
    let minute = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() / 60;
    let wanted = match minute % 4 {
        0 => ("decoupled-2k-8k", "directory"),
        1 => ("decoupled-2k-8k", "pages"),
        2 => ("decoupled-32k-32k", "directory"),
        _ => ("decoupled-32k-32k", "pages"),
    };
    let tables: Vec<_> = fixture
        .tables
        .iter()
        // A page shard's table is `pages-<j>`; the rotation picks by kind.
        .filter(|t| {
            args.all
                || (t.geometry == wanted.0
                    && Table::parse(&t.table).map(Table::kind) == Some(wanted.1))
        })
        .collect();
    if tables.is_empty() {
        return Ok("oracle_invalid");
    }
    let selected = if args.all {
        tables
    } else {
        vec![tables[(minute / 4) as usize % tables.len()]]
    };
    for f in selected {
        let table = Table::parse(&f.table).ok_or("unknown table")?;
        let g = geometry
            .geometries
            .iter()
            .find(|g| g.name == f.geometry)
            .ok_or("missing geometry")?;
        let (rows, width, scheme, dithered) = match table {
            Table::Directory => (
                g.directory_rows,
                g.directory_row_bytes,
                &g.directory_scheme,
                g.directory_scheme_dq44.as_ref(),
            ),
            Table::Pages(_) => (
                g.page_rows,
                g.page_row_bytes,
                &g.pages_scheme,
                g.pages_scheme_dq44.as_ref(),
            ),
        };
        if rows != f.rows || width != f.row_bytes || f.segments == 0 || f.samples.is_empty() {
            return Ok("oracle_invalid");
        }
        let sample = if args.all {
            f.samples
                .iter()
                .find(|s| s.nonempty)
                .unwrap_or(&f.samples[0])
        } else if minute % 5 == 0 {
            &f.samples[(minute / 20) as usize % f.samples.len()]
        } else {
            f.samples
                .iter()
                .find(|s| s.nonempty)
                .ok_or("oracle has no occupied row")?
        };
        if sample.sha256.len() != f.segments as usize {
            return Ok("oracle_invalid");
        }
        // Bind the query to the schema just checked against both the service and
        // the fixture, not to whichever schema this binary was compiled with.
        let mut client = TableClient::new_with_schema(
            &fixture.schema,
            table,
            &f.geometry,
            rows,
            width,
            scheme,
            dithered,
        )?;
        for segment in 0..f.segments {
            let (bytes, _) = transport.setup(f.shard_id, &f.revision, table, segment)?;
            let setup: Setup = serde_json::from_slice(&bytes)?;
            client.open_segment(
                &f.revision,
                table,
                segment,
                &setup.public_params,
                &setup.public_params_sha256,
            )?;
        }
        let query = client.prepare(&f.revision, table, sample.row)?;
        let response = transport.query(f.shard_id, &f.revision, table, &query.body)?;
        let decoded = client.decode(&f.revision, table, f.segments, query, &response)?;
        if !canonical(args)? {
            return Ok("oracle_invalid");
        }
        let hashes: Vec<_> = decoded
            .iter()
            .map(|b| hex::encode(Sha256::digest(b)))
            .collect();
        if hashes != sample.sha256 {
            return Ok("answer_mismatch");
        }
    }
    Ok("")
}
fn main() {
    let args = Args::parse();
    let start = Instant::now();
    let category = probe(&args).unwrap_or("request_or_protocol_failure");
    println!(
        "{}",
        serde_json::json!({"passed":category.is_empty(),"category":category,"duration_seconds":start.elapsed().as_secs_f64()})
    );
    if !category.is_empty() {
        std::process::exit(1);
    }
}
