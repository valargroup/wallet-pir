//! Scratch measurement tool (not committed): builds one display table's
//! runtime from a served table file exactly as the worker does, then times
//! server-side query evaluation (`TableRuntime::evaluate`: parse plus answer)
//! for fresh client queries, one at a time and then two at a time, and
//! checks every answer decodes to the table's row.
//!   p8_query_bench <rows-bin> [queries]
use std::time::Instant;
use transparent_shard_server::runtime::{build_pool, SharedParams, TableRuntime};
use transparent_shard_server::shardset::Table;

fn pct(v: &mut Vec<f64>, p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let path = args.first().ok_or("rows-bin")?;
    let queries: usize = args.get(1).map_or(20, |q| q.parse().unwrap());
    let geometry = transparent_shard::display::display_by_name("txid-2k8").ok_or("geometry")?;
    let shared = SharedParams::build_display(geometry, Table::TxDirectory)?;
    let profile = &shared.profile;
    let rows = std::fs::read(path).map_err(|e| e.to_string())?;
    assert_eq!(rows.len(), profile.rows * profile.row_bytes);
    let t = Instant::now();
    let runtime = build_pool().install(|| TableRuntime::build(&shared, &rows))?;
    let build_s = t.elapsed().as_secs_f64();
    let binding = *b"p8qbench";
    let one = |i: usize| -> Result<(f64, usize, usize), String> {
        let target = (i * 7919 + 13) % profile.rows;
        let (secret, upload) = profile.prepare(target)?;
        let mut body = binding.to_vec();
        body.extend(upload);
        let t = Instant::now();
        let answer = runtime.evaluate(&shared, binding, &body)?;
        let s = t.elapsed().as_secs_f64();
        let decoded = profile.decode(&secret, &runtime.public_params, &answer[16..])?;
        let at = target * profile.row_bytes;
        if decoded != rows[at..at + profile.row_bytes] {
            return Err(format!("row {target} decoded wrong"));
        }
        Ok((s, body.len(), answer.len()))
    };
    one(0)?; // warm-up, not recorded
    let mut serial = Vec::new();
    let (mut up, mut down) = (0, 0);
    for i in 0..queries {
        let (s, u, d) = one(i + 1)?;
        serial.push(s);
        (up, down) = (u, d);
    }
    let mut paired = Vec::new();
    for i in 0..queries / 2 {
        let (a, b) = std::thread::scope(|scope| {
            let a = scope.spawn(|| one(1000 + 2 * i));
            let b = scope.spawn(|| one(1001 + 2 * i));
            (a.join().unwrap(), b.join().unwrap())
        });
        paired.push(a?.0);
        paired.push(b?.0);
    }
    let rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmHWM:")).map(|l| l.to_string()));
    let ms = |v: &mut Vec<f64>| {
        serde_json::json!({"n": v.len(), "p50_ms": pct(v, 0.5) * 1e3, "p95_ms": pct(v, 0.95) * 1e3, "max_ms": pct(v, 1.0) * 1e3})
    };
    println!(
        "{}",
        serde_json::json!({
            "rows_bin": path, "build_seconds": build_s, "build_threads": build_pool().current_num_threads(),
            "query_threads": rayon::current_num_threads(), "upload_bytes": up, "response_bytes": down,
            "exact": true, "one_at_a_time": ms(&mut serial), "two_at_a_time": ms(&mut paired),
            "public_params_sha256": runtime.public_params_sha256, "peak_rss": rss,
        })
    );
    Ok(())
}
