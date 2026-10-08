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
use transparent_shard::layout::{by_name as geometry_by_name, Geometry};
use transparent_shard_server::runtime::{SharedParams, TableRuntime};
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

    /// Which table to measure: `directory` or `pages` of a history geometry,
    /// or `txdirectory`, a display geometry's only table.
    #[arg(long, default_value = "directory")]
    table: String,

    /// Which registry geometry to measure, history or display.
    ///
    /// The reservation the serving cache makes is a formula over the scheme,
    /// and this is what checks it against a real process. Measure every
    /// geometry the fleet will actually hold before sizing a cache budget from
    /// the formula alone.
    #[arg(long, default_value = "recent-8k")]
    geometry: String,
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
        "txdirectory" => Table::TxDirectory,
        other => return Err(format!("unknown table {other:?}").into()),
    };
    if cli.runtimes < 2 {
        return Err("at least two runtimes are needed to read a slope".into());
    }

    let geometry: &'static Geometry = geometry_by_name(&cli.geometry)
        .or_else(|| transparent_shard::display::display_by_name(&cli.geometry))
        .ok_or_else(|| format!("unknown geometry {:?}", cli.geometry))?;

    let plaintext = table.rows(geometry) * u64::from(table.row_bytes(geometry));
    println!(
        "geometry {:<13} table {:<9} {} rows x {} B = {:.2} MiB of plaintext",
        geometry.name,
        table.as_str(),
        table.rows(geometry),
        table.row_bytes(geometry),
        mib(plaintext),
    );

    let baseline = rss();
    let shared = SharedParams::build(geometry, table)?;
    println!("scheme  {}", serde_json::to_string(shared.scheme())?);
    // What the serving cache reserves for one of these. The measurement below
    // is what decides whether that reservation is honest; a formula that came
    // in under the real slope would turn a bounded cache back into an
    // unbounded one.
    println!(
        "cache reserves {:.2} MiB per runtime before a build, plans {:.2} MiB built",
        mib(shared.reserved_bytes()),
        mib(shared.held_bytes())
    );

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
        // Build time decides whether a retained revision has to stay resident
        // or can be rebuilt when something actually asks for it, which is the
        // difference between the tail needing a host of its own and sharing one.
        let started = std::time::Instant::now();
        held.push(TableRuntime::build(&shared, &rows)?);
        let elapsed = started.elapsed();
        let now = rss();
        // What the cache charges this runtime once it is built.
        let charged = held.last().expect("just built").held_bytes();
        println!(
            "  runtime {:>2}: +{:>7.2} MiB   (RSS {:>8.2} MiB)   charged {:>6.2} MiB   built in {:>6.2} s",
            index + 1,
            mib(now.saturating_sub(previous)),
            mib(now),
            mib(charged),
            elapsed.as_secs_f64(),
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

    // A shard holds one segment of each table. Doubling the measured one was
    // right only while every geometry had square tables: at archive-wide the
    // directory is 32,768 rows and the pages 65,536, so doubling the pages
    // overstates a shard by about 20% -- in the direction that makes a fleet
    // look more expensive than it is, which is still a wrong number to size on.
    //
    // The other table is charged at its planned built size rather than
    // measured, since measuring it means a second run. That is what the cache
    // charges it once built; the reservation, which charges the compiled
    // matrix at eight-byte words, is held only while a build is in flight.
    // A display shard bucket has one table, so nothing else is charged.
    let other = match table {
        Table::Directory => Some(Table::Pages),
        Table::Pages => Some(Table::Directory),
        Table::TxDirectory => None,
    };
    let other_planned = match other {
        Some(other) => SharedParams::build(geometry, other)?.held_bytes(),
        None => 0,
    };
    let per_shard = mib(marginal as u64) + mib(other_planned);
    println!(
        "per shard: {:.2} MiB measured {} + {:.2} MiB planned {} = {per_shard:.2} MiB",
        mib(marginal as u64),
        table.as_str(),
        mib(other_planned),
        other.map_or("(none)", Table::as_str),
    );
    // Shard counts measured over the complete genesis-to-tip journal, per
    // geometry, in `transparent/evidence/baselines/shard-utilisation/`. The
    // extrapolation used to read "genesis journal, 511 shards" at every
    // geometry: 511 is the partial census covering 9.4% of chain height, and
    // the whole chain is 1,091 at this geometry. Extrapolating held memory from
    // it understated the fleet by more than half, which is the opposite of what
    // a sizing tool is for. These counts were measured under the v7 14-slot
    // directory row; v8's 16-slot row has not been re-censused.
    let full_chain = match geometry.name {
        "recent-8k" => 1_091u64,
        "archive-32k" => 314,
        "archive-wide" => 162,
        // Not censused. Say so rather than borrowing another geometry's count.
        _ => 0,
    };
    let mut fleets = vec![("pilot set", 3u64)];
    if full_chain > 0 {
        fleets.push(("full chain", full_chain));
    } else {
        println!("  full chain      not censused at {}", geometry.name);
    }
    for (label, shards) in fleets {
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
