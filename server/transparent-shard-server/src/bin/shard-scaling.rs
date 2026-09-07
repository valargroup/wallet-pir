//! Does a second shard on the same host cost what the first one did?
//!
//! The fleet shape turns on this. One 256 GB host and eight 32 GB hosts cost
//! exactly the same per month and hold exactly the same shards, so if
//! evaluation scaled linearly with cores inside one box there would be no
//! reason to prefer eight. The design documents assert it does not — "online
//! evaluation is memory-bandwidth-bound, scale horizontally across workers with
//! independent memory bandwidth" — but nothing here had measured it.
//!
//! What it measures is the first-dimension multiply, `multiply_query`, which is
//! the part that streams a shard's whole 32 MiB encoded database per query.
//! That is where the bandwidth goes; the packing that follows works on a much
//! smaller intermediate. Each thread gets its **own** server, so this models
//! independent shards on one host and not cache sharing between threads that
//! happen to read the same table.
//!
//! Reading the result: throughput that keeps climbing with threads means the
//! work is core-bound and one big host is as good as many small ones.
//! Throughput that flattens means the cores are waiting on memory, and the only
//! way to buy more of it is more hosts.

use clap::Parser;
use enhance_pir_server::ipir::RowPlaintextIter;
use ipir_sp::server::IPIRServer;
use std::time::{Duration, Instant};
use transparent_shard_server::shardset::Table;

#[derive(Parser)]
#[command(
    name = "shard-scaling",
    about = "Measure how shard evaluation scales with concurrency on one host"
)]
struct Cli {
    /// Thread counts to measure, ascending.
    #[arg(long, value_delimiter = ',', default_value = "1,2,4,8,12,16")]
    threads: Vec<usize>,
    /// Seconds of evaluation per thread count.
    #[arg(long, default_value_t = 3.0)]
    seconds: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let table = Table::Directory;
    let (rlwe, scheme) =
        ipir_sp::params_for_simplepir(table.rows(), u64::from(table.row_bytes()) * 8)?;

    let row_bytes = table.row_bytes() as usize;
    let mut rows = vec![0u8; table.rows() as usize * row_bytes];
    for (index, byte) in rows.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }

    // Below the modulus, which is all `multiply_query` requires of it. The
    // values do not change the work done: every coefficient is touched either
    // way, which is the point of a PIR scan.
    let query = vec![12_345u64; scheme.db_rows];
    let encoded_mib = (scheme.db_rows * scheme.db_cols * 2) as f64 / (1024.0 * 1024.0);
    println!(
        "one shard's encoded database: {encoded_mib:.1} MiB ({} x {} u16)",
        scheme.db_rows, scheme.db_cols
    );
    println!("each thread scans its own copy, so this is independent shards on one host\n");

    println!(
        "{:>7} {:>12} {:>12} {:>10} {:>12}",
        "threads", "evals/s", "GiB/s", "speedup", "efficiency"
    );

    let mut baseline = 0.0;
    let max = *cli.threads.iter().max().unwrap_or(&1);
    // Built once at the widest count and reused, so no measurement pays for
    // construction and every thread count reads the same memory.
    let servers: Vec<IPIRServer<u16>> = (0..max)
        .map(|_| {
            let coefficients = RowPlaintextIter::new(
                &rows,
                row_bytes,
                scheme.db_rows,
                scheme.db_cols,
                scheme.p.trailing_zeros() as usize,
            );
            IPIRServer::<u16>::new_auto_kernel(scheme.clone(), coefficients, false, true)
        })
        .collect();

    for &threads in &cli.threads {
        let budget = Duration::from_secs_f64(cli.seconds);
        let counts: Vec<u64> = std::thread::scope(|scope| {
            let handles: Vec<_> = servers[..threads]
                .iter()
                .map(|server| {
                    let query = &query;
                    let rlwe = &rlwe;
                    scope.spawn(move || {
                        let started = Instant::now();
                        let mut done = 0u64;
                        while started.elapsed() < budget {
                            std::hint::black_box(server.multiply_query(rlwe, query));
                            done += 1;
                        }
                        done
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        let total: u64 = counts.iter().sum();
        let rate = total as f64 / cli.seconds;
        if threads == cli.threads[0] {
            baseline = rate / threads as f64;
        }
        let ideal = baseline * threads as f64;
        println!(
            "{threads:>7} {:>12.1} {:>12.2} {:>9.2}x {:>11.0}%",
            rate,
            rate * encoded_mib / 1024.0,
            rate / baseline,
            100.0 * rate / ideal,
        );
    }
    Ok(())
}
