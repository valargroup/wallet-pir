use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

mod load;

#[derive(Debug, Parser)]
#[command(
    name = "enhance-pir-load-test",
    about = "Apply sustained random-position load to an Enhance PIR origin"
)]
struct Args {
    /// Exact-answer oracle for the explicitly isolated positional fixture.
    #[arg(long, conflicts_with = "oracle")]
    fixture_oracle: bool,

    /// JSON array of {position, record_hex} from an independent canonical extractor.
    #[arg(long)]
    oracle: Option<PathBuf>,

    /// Open-loop aggregate offered QPS; report scheduled-to-completion latency.
    #[arg(long)]
    rate: Option<f64>,
    /// Enhance PIR server base URL.
    #[arg(long)]
    server: String,

    /// Number of closed-loop workers.
    #[arg(long, default_value_t = 8)]
    parallelism: usize,

    /// Length of the measured phase.
    #[arg(long, default_value = "60s", value_parser = humantime::parse_duration)]
    duration: Duration,

    /// Unmeasured load before the measured phase.
    #[arg(long, default_value = "10s", value_parser = humantime::parse_duration)]
    warmup: Duration,

    /// Seed used to make the random position pool repeatable.
    #[arg(long)]
    seed: Option<u64>,

    /// Write the report as pretty JSON.
    #[arg(long)]
    json_out: Option<PathBuf>,

    /// Atomically write a JSON marker when warmup finishes and measurement begins.
    #[arg(long)]
    phase_file: Option<PathBuf>,

    /// Stop issuing queries after the first query error or incorrect answer.
    #[arg(long)]
    fail_fast: bool,

    /// Fail when the measured error rate exceeds this fraction.
    #[arg(long, default_value_t = 0.01)]
    max_error_rate: f64,

    /// Fail when measured end-to-end p99 exceeds this value.
    #[arg(long)]
    slo_p99_ms: Option<f64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    load::run(args).await
}
