//! Does a second shard on the same host cost what the first one did?
//!
//! The fleet shape turns on this. One 256 GB host and eight 32 GB hosts cost
//! exactly the same per month and hold exactly the same shards, so if
//! evaluation scaled linearly with cores inside one box there would be no
//! reason to prefer eight. The design documents assert it does not — "online
//! evaluation is memory-bandwidth-bound, scale horizontally across workers with
//! independent memory bandwidth" — but nothing here had measured it.
//!
//! What it measures is the first-dimension multiply, `try_multiply_power_of_two`
//! modulo the native q = 2^54, which is
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
use ipir_sp::server::IPIRServer;
use std::time::{Duration, Instant};
use transparent_shard::layout::{by_name as geometry_by_name, RECENT_8K};
use transparent_shard_server::runtime::{reserved_bytes, transport_params};
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
    /// Report the memory shape of candidate row counts and exit.
    #[arg(long, default_value_t = false)]
    geometry_sweep: bool,
    /// Seconds of evaluation per thread count.
    #[arg(long, default_value_t = 3.0)]
    seconds: f64,

    /// Which registry geometry to scan. Ignored by `--geometry-sweep`, which
    /// sweeps row counts directly.
    #[arg(long, default_value = "recent-8k")]
    geometry: String,

    /// Which of the geometry's tables to scan.
    ///
    /// It matters once a geometry's tables differ. `archive-wide` pairs a
    /// 32,768-row directory with a 65,536-row page table, so scanning only the
    /// directory would measure something it shares with `archive-32k` and miss
    /// the half that distinguishes it.
    #[arg(long, default_value = "directory")]
    table: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if cli.geometry_sweep {
        // Resident cost is db_rows*db_cols*2 for the encoded database plus the
        // native two-mask preprocessing (bounded at eight-byte words), which is
        // d x d*ell per block and so does not follow the row count. With one
        // block per row, a taller table is nearly free in memory while halving
        // the shard count -- which is the opposite of how the geometry notes
        // weigh storage.
        println!(
            "{:>7} {:>8} {:>8} {:>10} {:>10} {:>10} {:>10}",
            "rows", "db_rows", "db_cols", "db MiB", "pack MiB", "total MiB", "query B"
        );
        for rows in [
            2_048u64, 4_096, 8_192, 16_384, 32_768, 65_536, 131_072, 262_144,
        ] {
            let row_bytes = Table::Directory.row_bytes(&RECENT_8K);
            let sc = transport_params(rows, row_bytes)?;
            let db = (sc.db_rows * sc.db_cols * 2) as f64 / 1048576.0;
            let pack =
                (reserved_bytes(rows, row_bytes) - rows * u64::from(row_bytes)) as f64 / 1048576.0;
            // What a wallet uploads per query: the binding, the K_g key, and
            // the first-dimension query, which is the only term that follows
            // the row count.
            let query = 8 + transparent_native::request_len(sc.db_rows);
            println!(
                "{rows:>7} {:>8} {:>8} {db:>10.1} {pack:>10.1} {:>10.1} {query:>10}",
                sc.db_rows,
                sc.db_cols,
                db + pack
            );
        }
        return Ok(());
    }
    let geometry = geometry_by_name(&cli.geometry)
        .ok_or_else(|| format!("unknown geometry {:?}", cli.geometry))?;
    let table = Table::parse(&cli.table).ok_or_else(|| format!("unknown table {:?}", cli.table))?;
    let scheme = transport_params(table.rows(geometry), table.row_bytes(geometry))?;

    let row_bytes = table.row_bytes(geometry) as usize;
    let mut rows = vec![0u8; table.rows(geometry) as usize * row_bytes];
    for (index, byte) in rows.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }

    // Below the modulus, which is all the scan requires of it. The
    // values do not change the work done: every coefficient is touched either
    // way, which is the point of a PIR scan.
    let query = vec![12_345u64; scheme.db_rows];
    let encoded_mib = (scheme.db_rows * scheme.db_cols * 2) as f64 / (1024.0 * 1024.0);
    println!(
        "one shard's encoded database: {encoded_mib:.1} MiB ({} x {} u16)",
        scheme.db_rows, scheme.db_cols
    );
    println!(
        "geometry {} table {} ({} rows x {} B)",
        geometry.name,
        table.as_str(),
        table.rows(geometry),
        table.row_bytes(geometry)
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
            let cols = scheme.db_cols;
            let rows = &rows;
            let coefficients = (0..scheme.db_rows * cols).map(move |i| {
                transparent_native::row_coefficient(rows, row_bytes, i / cols, i % cols)
            });
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
                    scope.spawn(move || {
                        let started = Instant::now();
                        let mut done = 0u64;
                        while started.elapsed() < budget {
                            std::hint::black_box(
                                server
                                    .try_multiply_power_of_two(transparent_native::Q, query)
                                    .expect("scan"),
                            );
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
