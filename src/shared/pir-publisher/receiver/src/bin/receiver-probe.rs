//! The receiver directory's `pir-monitor` probe for the monitor host; see
//! `receiver_indexer::probe`. `receiver-directory probe` runs the same probe.
use clap::Parser;
use receiver_indexer::probe::{run, Args};

#[tokio::main]
async fn main() {
    std::process::exit(if run(Args::parse()).await { 0 } else { 1 });
}
