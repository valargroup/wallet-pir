//! Times a segment's public hint both ways in the runtime build pool, and
//! proves the two give the same runtime.
//!
//! ```text
//! TRANSPARENT_BUILD_THREADS=2 cargo run --profile release-fast \
//!   -p transparent-shard-server --example hint_bench -- \
//!   --geometry recent-8k --repeats 3 directory=directory.0.bin pages=pages.0.bin
//! ```
//!
//! For each table it builds the column-major database the runtime scans, over
//! the leading row blocks that hold any nonzero byte as the runtime does. It
//! then alternates the reference `pir_native::hint` with the batched
//! `transparent_native::batched_hint::hint`, requires every hint to be equal,
//! and requires the masks published from the reference hint to equal those of
//! a full `TableRuntime::build`. One JSON line per table goes to stdout.
use serde_json::json;
use std::time::Instant;
use transparent_native::{self as native, D};
use transparent_shard_server::runtime::{build_pool, SharedParams, TableRuntime};
use transparent_shard_server::shardset::Table;

fn main() -> Result<(), String> {
    let mut geometry = "recent-8k".to_string();
    let mut repeats = 3usize;
    let mut tables = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--geometry" => geometry = args.next().ok_or("--geometry value")?,
            "--repeats" => {
                repeats = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .ok_or("--repeats value")?
            }
            table => tables.push(
                table
                    .split_once('=')
                    .map(|(t, p)| (t.to_string(), p.to_string()))
                    .ok_or("expected table=path")?,
            ),
        }
    }
    let geometry = transparent_shard::layout::PROFILES
        .iter()
        .find(|g| g.name == geometry)
        .ok_or("unknown geometry")?;
    let threads = build_pool().current_num_threads();
    for (name, path) in tables {
        let table = match name.as_str() {
            "directory" => Table::Directory,
            "pages" => Table::Pages,
            _ => return Err(format!("unknown table {name}")),
        };
        let shared = SharedParams::build(geometry, table)?;
        let profile = &shared.profile;
        let bytes = std::fs::read(&path).map_err(|e| format!("{path}: {e}"))?;
        if bytes.len() != profile.rows * profile.row_bytes {
            return Err(format!("{path}: {} bytes", bytes.len()));
        }
        let last = bytes
            .iter()
            .rposition(|b| *b != 0)
            .map_or(0, |at| at / profile.row_bytes);
        let used = (last / D + 1).min(profile.rows / D);
        let rows = used * D;
        let columns: Vec<Vec<u16>> = (0..profile.cols)
            .map(|col| {
                (0..rows)
                    .map(|row| native::row_coefficient(&bytes, profile.row_bytes, row, col))
                    .collect()
            })
            .collect();
        let masks = &profile.masks[..used];
        let (mut reference_s, mut batched_s, mut runtime_s) = (vec![], vec![], vec![]);
        let mut reference = None;
        for _ in 0..repeats {
            let started = Instant::now();
            let r = build_pool()
                .install(|| native::hint(masks, rows, profile.cols, |c| &columns[c]))?;
            reference_s.push(started.elapsed().as_secs_f64());
            let started = Instant::now();
            let b = build_pool().install(|| {
                native::batched_hint::hint(masks, rows, profile.cols, |c| &columns[c])
            })?;
            batched_s.push(started.elapsed().as_secs_f64());
            if r != b {
                return Err(format!("{path}: batched hint differs from the reference"));
            }
            reference = Some(r);
        }
        let published = native::publish(&build_pool().install(|| {
            native::preprocess(&profile.setup, &reference.expect("at least one repeat"))
        })?)?;
        let mut runtime_sha256 = String::new();
        for _ in 0..repeats {
            let started = Instant::now();
            let runtime = build_pool().install(|| TableRuntime::build(&shared, &bytes))?;
            runtime_s.push(started.elapsed().as_secs_f64());
            if runtime.public_params != published {
                return Err(format!("{path}: runtime masks differ from the reference"));
            }
            runtime_sha256 = runtime.public_params_sha256.clone();
        }
        println!(
            "{}",
            json!({
                "table": name, "path": path, "geometry": geometry.name, "threads": threads,
                "used_blocks": used, "blocks": profile.rows / D,
                "public_params_sha256": runtime_sha256, "hints_equal": true,
                "reference_hint_seconds": reference_s, "batched_hint_seconds": batched_s,
                "runtime_build_seconds": runtime_s,
            })
        );
    }
    Ok(())
}
