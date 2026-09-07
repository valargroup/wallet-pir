//! What one shard's PIR runtime actually costs a worker, in resident bytes.
//!
//! This is the figure the fleet plan turns on, and the one the geometry
//! evaluation flagged as unmeasured: "Server resident memory per loaded shard
//! is also unmeasured and is the figure most likely to bind in practice: each
//! shard pins ~29 MB of tables, and it is the loaded-shard count, not the disk
//! total, that decides what a server can serve."
//!
//! It measures against synthetic rows rather than a published set on purpose. A
//! shard pins every row of its declared geometry whether the row is occupied or
//! empty, and `TableRuntime::build` is a function of the geometry and the bytes
//! alone — not of what the bytes mean. So a runtime over arbitrary rows of the
//! pinned geometry is the same size as one over a real shard's, and this needs
//! neither a journal nor a published set to run.
//!
//! # Why it is written as one straight line
//!
//! RSS only reports what the process has taken from the kernel, and a general
//! allocator does not return freed arenas promptly. An earlier version of this
//! built one table's runtimes, dropped them, then built the other's, and read
//! the difference: it reported the two tables as 143 MiB and 62 MiB apiece,
//! which cannot be true, because both tables have exactly the same geometry and
//! the build is a function of geometry. The freed arenas were absorbing the
//! second table's allocations.
//!
//! So: one table, every runtime held to the end, nothing freed in between, and
//! the marginal cost read from the slope rather than from any single delta. The
//! first build is excluded because it also pays for whatever the process
//! allocates once.
//!
//! Rows are filled rather than zeroed. A zeroed `Vec` comes from the allocator
//! as untouched pages that do not count toward RSS until something writes them,
//! which would flatter the plaintext side of the measurement.

use clap::Parser;
use transparent_shard_server::service::{SharedParams, TableRuntime};
use transparent_shard_server::shardset::Table;

#[derive(Parser)]
#[command(
    name = "shard-residency",
    about = "Measure resident bytes per built shard runtime at the pinned geometry"
)]
struct Cli {
    /// Runtimes to build, all held at once. The slope across them is the
    /// per-runtime cost; one build alone cannot separate it from start-up.
    #[arg(long, default_value_t = 6)]
    runtimes: usize,

    /// Which table's geometry to measure. Both are pinned to the same rows and
    /// row width, so this exists to confirm that rather than to choose.
    #[arg(long, default_value = "directory")]
    table: String,
}

/// This process's resident set size, in bytes.
fn rss() -> u64 {
    let mut system = sysinfo::System::new();
    let Ok(pid) = sysinfo::get_current_pid() else {
        return 0;
    };
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).map_or(0, |process| process.memory())
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let table = match cli.table.as_str() {
        "directory" => Table::Directory,
        "pages" => Table::Pages,
        other => return Err(format!("unknown table {other:?}").into()),
    };
    if cli.runtimes < 2 {
        return Err("at least two runtimes are needed to read a slope".into());
    }

    let plaintext = table.rows() * u64::from(table.row_bytes());
    println!(
        "table {:<9} {} rows x {} B = {:.2} MiB of plaintext",
        table.as_str(),
        table.rows(),
        table.row_bytes(),
        mib(plaintext),
    );

    let baseline = rss();
    let shared = SharedParams::build(table)?;
    println!("scheme  {}", serde_json::to_string(&shared.scheme)?);

    // Held for every build and never rebuilt, so the plaintext is charged once
    // here rather than counted against any runtime.
    let mut rows = vec![0u8; plaintext as usize];
    for (index, byte) in rows.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    let before_runtimes = rss();
    println!(
        "\nbaseline {:.2} MiB, parameters and one plaintext copy {:.2} MiB",
        mib(baseline),
        mib(before_runtimes.saturating_sub(baseline)),
    );

    let mut held = Vec::with_capacity(cli.runtimes);
    let mut after_first = before_runtimes;
    let mut previous = before_runtimes;
    for index in 0..cli.runtimes {
        held.push(TableRuntime::build(&shared, table, &rows)?);
        let now = rss();
        println!(
            "  runtime {:>2}: +{:>7.2} MiB   (RSS {:>8.2} MiB)",
            index + 1,
            mib(now.saturating_sub(previous)),
            mib(now),
        );
        if index == 0 {
            after_first = now;
        }
        previous = now;
    }

    // The slope from the first build to the last, which is the cost of holding
    // one more shard's segment resident.
    let marginal = previous.saturating_sub(after_first) as f64 / (cli.runtimes - 1) as f64;
    println!(
        "\nmarginal cost of one held runtime: {:.2} MiB",
        mib(marginal as u64)
    );

    // A shard holds one segment of each table in the ordinary case, and the two
    // tables share a geometry, so a shard costs two of these.
    let per_shard = mib(marginal as u64) * 2.0;
    println!("per shard, one segment of each table: {per_shard:.2} MiB");
    for (label, shards) in [("pilot set", 21u64), ("genesis journal", 511)] {
        println!(
            "  {:<16} {:>4} shards -> {:>8.2} GiB if all are held",
            label,
            shards,
            per_shard * shards as f64 / 1024.0,
        );
    }

    // Keeps every runtime alive to the last measurement; dropping them earlier
    // is what made the previous version of this tool report two identical
    // geometries as different sizes.
    drop(held);
    Ok(())
}
