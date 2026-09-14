//! Operator-only sustained private queries, checked against published plaintext.
//! No scripts or returned row bytes are logged. Stale revisions are retried;
//! a transport failure or an incorrect decoded row fails the run.
use clap::Parser;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use transparent_wallet::{
    client::Table,
    transport::{BoxError, Overloaded, StaleRevision},
};
#[path = "support/query.rs"]
mod query;

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
        match query::once(&args.url, &args.publications, args.shard, table) {
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
