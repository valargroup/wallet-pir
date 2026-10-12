//! Measures one Transparent table under today's 16-bit profile and under
//! ipir-sp's 8-bit native profile, and exports the 8-bit table's noise report
//! for `certify_native.py`. One table per process, so peak memory is the
//! table's own. One JSON line goes to stdout.
//!
//! ```text
//! cargo run --profile release-fast -p transparent-shard-server --example p8_trial -- \
//!   --variant baseline|p8-19x2|p8-27x1|certify-19x2|certify-27x1 \
//!   --geometry decoupled-32k-32k --table directory|pages --rows-bin directory.0.bin \
//!   [--queries 10] [--query-rows N] [--query-bits 28] [--report out.json]
//! ```
//!
//! - `baseline` builds the served `TableRuntime` and answers 44-bit dithered
//!   queries exactly as a worker does.
//! - `p8-<gadget>` stores each byte of the table as one 8-bit entry, so a
//!   4,096-byte row is 4,096 entries (two packing blocks instead of one), and
//!   answers through ipir-sp's `NativeServer` with byte storage: two-mask
//!   output, 22-bit published masks, dithered queries at `--query-bits` and
//!   10-bit responses. `19x2` is today's two-digit packing key, `27x1` the
//!   one-digit key of half the size.
//! - `certify-<gadget>` builds the same table with snapshot-specific noise
//!   statistics and writes the `native_noise` report for this exact table to
//!   `--report`.
//!
//! Every answered query is decoded and must equal the table's row. Seeds are
//! trial seeds, domain-separated from the served profile; the masks they
//! expand to are what the certificate covers.
use ipir_sp::native::{NativeProfile, NativePublicSetup, NativeRequest, NativeServer};
use rayon::prelude::*;
use reinspiring::native::{NativeParams, SecretDistribution};
use reinspiring::noise::{gaussian_cdf_sha256, gaussian_counts, WeightNorms};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::Instant;
use transparent_shard_server::runtime::{build_pool, SharedParams, TableRuntime};
use transparent_shard_server::shardset::Table;

/// Published-mask, response and default dithered query widths of the 8-bit
/// profile's certified floors.
const P8_MASK_BITS: usize = 22;
const P8_RESPONSE_BITS: usize = 10;
const P8_QUERY_BITS: usize = 28;
/// The wallet-pir framing around a body: the 8-byte binding on a query, and
/// binding plus epoch on a response.
const QUERY_FRAMING: usize = 8;
const RESPONSE_FRAMING: usize = 16;

fn status_bytes(key: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with(key))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

fn packed_len(n: usize, bits: usize) -> usize {
    (n * bits).div_ceil(8)
}

fn percentile(samples: &[f64], p: f64) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

fn latency(samples: &[f64]) -> Value {
    json!({
        "n": samples.len(), "p50_ms": percentile(samples, 0.5) * 1e3,
        "p99_ms": percentile(samples, 0.99) * 1e3,
        "max_ms": percentile(samples, 1.0) * 1e3,
    })
}

/// A uniformly random populated row, so decodes check real bytes.
fn targets(rows: &[u8], row_bytes: usize, count: usize) -> Vec<usize> {
    use std::hash::BuildHasher;
    let used = rows
        .iter()
        .rposition(|byte| *byte != 0)
        .map_or(1, |at| at / row_bytes + 1);
    let state = std::collections::hash_map::RandomState::new();
    (0..count)
        .map(|i| (state.hash_one(i) % used as u64) as usize)
        .collect()
}

/// Runs `answer` for every target alone, then two at a time from two threads
/// (a worker's two query slots), returning both latency sets.
fn timed<F>(targets: &[usize], answer: F) -> Result<(Vec<f64>, Vec<f64>), String>
where
    F: Fn(usize) -> Result<f64, String> + Sync,
{
    let mut single = Vec::new();
    for &target in targets {
        single.push(answer(target)?);
    }
    let mut paired = Vec::new();
    for pair in targets.chunks(2) {
        let results: Vec<Result<f64, String>> = std::thread::scope(|scope| {
            let handles: Vec<_> = pair
                .iter()
                .map(|&target| {
                    let answer = &answer;
                    scope.spawn(move || answer(target))
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for result in results {
            paired.push(result?);
        }
    }
    Ok((single, paired))
}

fn baseline(
    shared: &SharedParams,
    rows: Vec<u8>,
    queries: usize,
    query_rows: usize,
) -> Result<Value, String> {
    let profile = &shared.profile;
    let picks = targets(&rows, profile.row_bytes, queries);
    let started = Instant::now();
    let runtime = build_pool().install(|| TableRuntime::build(shared, &rows))?;
    let build_seconds = started.elapsed().as_secs_f64();
    let expected: HashMap<usize, Vec<u8>> = picks
        .iter()
        .map(|&r| {
            (
                r,
                rows[r * profile.row_bytes..(r + 1) * profile.row_bytes].to_vec(),
            )
        })
        .collect();
    drop(rows);
    let rss_built = status_bytes("VmRSS:");
    let binding = *b"p8-trial";
    let (single, paired) = timed(&picks, |target| {
        let (secret, upload) = profile.prepare_dithered(target)?;
        let mut body = binding.to_vec();
        body.extend(upload);
        let started = Instant::now();
        let parsed = shared.parse(binding, &body, profile.rows)?;
        let response = runtime.answer(binding, &parsed)?;
        let seconds = started.elapsed().as_secs_f64();
        let decoded = profile.decode(&secret, &runtime.public_params, &response[16..])?;
        if decoded != expected[&target] {
            return Err(format!("row {target} decoded wrongly"));
        }
        Ok(seconds)
    })?;
    let dithered = &profile.dithered_scheme;
    let key_bytes = dithered.request_bytes - packed_len(profile.rows, dithered.query_bits);
    Ok(json!({
        "blocks": profile.blocks(),
        "build_seconds": build_seconds,
        "held_bytes": runtime.held_bytes(),
        "rss_built_bytes": rss_built,
        "upload_bytes": QUERY_FRAMING + dithered.request_bytes,
        "upload_prefix_bytes": QUERY_FRAMING + key_bytes + packed_len(query_rows, dithered.query_bits),
        "key_bytes": key_bytes,
        "response_bytes": RESPONSE_FRAMING + dithered.response_bytes,
        "published_bytes": runtime.public_params.len(),
        "answer_single": latency(&single),
        "answer_paired": latency(&paired),
    }))
}

/// Trial seed for one table: the served derivation's shape under its own label.
fn trial_seed(geometry: &str, table: &str, gadget: &str) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(transparent_shard::manifest::SCHEMA.as_bytes());
    hash.update(b"/native-reinspiring-p8-trial/");
    hash.update(gadget.as_bytes());
    hash.update(b"\0");
    hash.update(geometry.as_bytes());
    hash.update(b"\0");
    hash.update(table.as_bytes());
    hash.finalize().into()
}

fn p8_setup(
    shared: &SharedParams,
    gadget: &str,
    query_bits: usize,
) -> Result<NativePublicSetup, String> {
    let (bits, ell) = match gadget {
        "19x2" => (19, 2),
        "27x1" => (27, 1),
        other => return Err(format!("unknown gadget {other}")),
    };
    let e = |e: reinspiring::ReinspiringError| e.to_string();
    let params =
        NativeParams::new(2048, 54, 8, bits, ell, SecretDistribution::Gaussian).map_err(e)?;
    let profile = NativeProfile::new(params, shared.profile.rows, shared.profile.row_bytes)
        .map_err(e)?
        .with_two_mask_output()
        .map_err(e)?
        .with_published_mask_bits(P8_MASK_BITS)
        .map_err(e)?
        .with_dithered_query_bits(query_bits)
        .map_err(e)?
        .with_response_bits(P8_RESPONSE_BITS)
        .map_err(e)?;
    let seed = trial_seed(shared.geometry.name, shared.table.as_str(), gadget);
    Ok(NativePublicSetup::new(profile, seed, [0; 32]))
}

/// The table's bytes as column-major 8-bit entries: entry `c` of row `r` is
/// byte `c` of that row.
fn column_major(rows: &[u8], row_count: usize, row_bytes: usize) -> Vec<u8> {
    let mut db = vec![0u8; rows.len()];
    db.par_chunks_mut(row_count)
        .enumerate()
        .for_each(|(col, column)| {
            for (row, entry) in column.iter_mut().enumerate() {
                *entry = rows[row * row_bytes + col];
            }
        });
    db
}

fn p8(
    shared: &SharedParams,
    rows: Vec<u8>,
    gadget: &str,
    query_bits: usize,
    queries: usize,
    query_rows: usize,
) -> Result<Value, String> {
    let profile = &shared.profile;
    let setup = p8_setup(shared, gadget, query_bits)?;
    let picks = targets(&rows, profile.row_bytes, queries);
    let expected: HashMap<usize, Vec<u64>> = picks
        .iter()
        .map(|&r| {
            let row = &rows[r * profile.row_bytes..(r + 1) * profile.row_bytes];
            (r, row.iter().map(|&b| b as u64).collect())
        })
        .collect();
    let started = Instant::now();
    let db = column_major(&rows, profile.rows, profile.row_bytes);
    drop(rows);
    let transpose_seconds = started.elapsed().as_secs_f64();
    let started = Instant::now();
    let server =
        NativeServer::build_u8_with_concurrency(setup, db, 2).map_err(|e| e.to_string())?;
    let build_seconds = started.elapsed().as_secs_f64();
    let rss_built = status_bytes("VmRSS:");
    let published = server.published();
    let published_bytes = published.to_bytes().len();
    let offline = server.offline_timing();
    let stages = std::sync::Mutex::new(Vec::new());
    let (single, paired) = timed(&picks, |target| {
        let request = NativeRequest::generate(server.setup(), target).map_err(|e| e.to_string())?;
        let started = Instant::now();
        let (response, timing) = server.respond(request.bytes()).map_err(|e| e.to_string())?;
        let seconds = started.elapsed().as_secs_f64();
        stages.lock().unwrap().push((
            timing.matrix_vector.as_secs_f64(),
            timing.packing.as_secs_f64(),
        ));
        let decoded = request
            .decode(&published, &response)
            .map_err(|e| e.to_string())?;
        if decoded != expected[&target] {
            return Err(format!("row {target} decoded wrongly"));
        }
        Ok(seconds)
    })?;
    let stages = stages.into_inner().unwrap();
    let scan: Vec<f64> = stages.iter().map(|s| s.0).collect();
    let pack: Vec<f64> = stages.iter().map(|s| s.1).collect();
    let p = server.setup().profile();
    let key_bytes = packed_len(p.packing().ell() * p.packing().d(), 54);
    let held = server.database_bytes() + server.coefficient_bytes() + published_bytes;
    Ok(json!({
        "gadget": gadget, "query_bits": p.query_bits(), "response_bits": p.response_bits(),
        "published_mask_bits": P8_MASK_BITS, "cols": p.cols(), "blocks": p.cols() / 2048,
        "u8_storage": server.is_u8_storage(),
        "matrix_storage": format!("{:?}", server.matrix_storage()),
        "transpose_seconds": transpose_seconds,
        "build_seconds": build_seconds,
        "offline": {
            "hints_block_seconds": offline.hints.as_secs_f64(),
            "packing_block_seconds": offline.packing.as_secs_f64(),
            "layout_seconds": offline.layout.as_secs_f64(),
            "total_seconds": offline.total.as_secs_f64(),
        },
        "database_bytes": server.database_bytes(),
        "coefficient_bytes": server.coefficient_bytes(),
        "held_bytes": held,
        "rss_built_bytes": rss_built,
        "upload_bytes": QUERY_FRAMING + key_bytes + packed_len(profile.rows, p.query_bits()),
        "upload_prefix_bytes": QUERY_FRAMING + key_bytes + packed_len(query_rows, p.query_bits()),
        "key_bytes": key_bytes,
        "response_bytes": RESPONSE_FRAMING + packed_len(p.cols(), p.response_bits()),
        "published_bytes": published_bytes,
        "native_request_bytes": NativeRequest::generate(server.setup(), 0).map_err(|e| e.to_string())?.bytes().len(),
        "answer_single": latency(&single),
        "answer_paired": latency(&paired),
        "scan": latency(&scan),
        "packing": latency(&pack),
    }))
}

fn norms(w: WeightNorms) -> Value {
    json!({"l1": w.l1.to_string(), "l2_squared": w.l2_squared.to_string(), "max": w.max.to_string()})
}

/// The `native_noise` report for this table, as ipir-sp rc.7's
/// `ipir-sp/examples/native_noise.rs` writes it for a synthetic one.
fn certify(
    shared: &SharedParams,
    rows: Vec<u8>,
    gadget: &str,
    query_bits: usize,
    report: &str,
) -> Result<Value, String> {
    let profile = &shared.profile;
    let setup = p8_setup(shared, gadget, query_bits)?;
    let db = column_major(&rows, profile.rows, profile.row_bytes);
    drop(rows);
    // Same digest convention as native_noise: each entry as a little-endian u16.
    let mut hash = Sha256::new();
    for &x in &db {
        hash.update((x as u16).to_le_bytes());
    }
    let database_sha256 = hex::encode(hash.finalize());
    let started = Instant::now();
    let (server, blocks) = NativeServer::build_u8_analyzed(setup, db).map_err(|e| e.to_string())?;
    let analyze_seconds = started.elapsed().as_secs_f64();
    let p = server.setup().profile();
    let pack = p.packing();
    let blocks: Vec<Value> = blocks
        .into_iter()
        .map(|b| {
            json!({
                "weights": norms(b.packing.weights.independent(b.query)),
                "query_l1": b.query.l1.to_string(),
                "query_l2_squared": b.query.l2_squared.to_string(),
                "kh_l1": b.packing.kh_l1.to_string(),
                "public_mask_screens": b.packing.public_mask_screens.iter()
                    .map(|(bits, w)| json!({"bits": bits, "weights": norms(w.independent(b.query))}))
                    .collect::<Vec<_>>(),
                "one_limb": b.packing.one_limb.into_iter()
                    .map(|v| json!({"bits": v.bits, "weights": norms(v.weights.independent(b.query))}))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    let out = json!({
        "format": "native-noise-two-mask-rounded-dithered-v1",
        "d": 2048, "q_bits": 54, "p_bits": 8, "ell": pack.ell(),
        "gadget_bits": pack.gadget_bits(), "dropped_bits": pack.dropped_bits(),
        "published_mask_bits": P8_MASK_BITS, "published_bytes": server.published().to_bytes().len(),
        "rows": p.rows(), "cols": p.cols(), "query_bits": p.query_bits(), "query_rounding": "dithered",
        "response_bits": p.response_bits(), "kh_bits": p.kh_bits(),
        "setup_id": hex::encode(server.setup().id()),
        "database_sha256": database_sha256,
        "sampler_sha256": hex::encode(gaussian_cdf_sha256()),
        "sampler_counts": gaussian_counts().into_iter().map(|(x, n)| json!([x, n.to_string()])).collect::<Vec<_>>(),
        "blocks": blocks,
    });
    std::fs::write(report, serde_json::to_vec(&out).unwrap())
        .map_err(|e| format!("{report}: {e}"))?;
    Ok(
        json!({"gadget": gadget, "query_bits": p.query_bits(), "analyze_seconds": analyze_seconds, "report": report}),
    )
}

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flags: HashMap<_, _> = args
        .chunks_exact(2)
        .map(|kv| (kv[0].trim_start_matches("--").to_owned(), kv[1].clone()))
        .collect();
    let flag = |name: &str| flags.get(name).ok_or(format!("--{name} is required"));
    let geometry =
        transparent_shard::layout::by_name(flag("geometry")?).ok_or("unknown geometry")?;
    let table = Table::parse(flag("table")?).ok_or("directory or pages")?;
    let shared = SharedParams::build(geometry, table)?;
    let path = flag("rows-bin")?;
    let rows = std::fs::read(path).map_err(|e| format!("{path}: {e}"))?;
    if rows.len() != shared.profile.rows * shared.profile.row_bytes {
        return Err(format!("{path}: {} bytes", rows.len()));
    }
    let queries: usize = flags
        .get("queries")
        .map_or(Ok(10), |v| v.parse())
        .map_err(|_| "--queries")?;
    let query_rows: usize = flags
        .get("query-rows")
        .map_or(Ok(shared.profile.rows), |v| v.parse())
        .map_err(|_| "--query-rows")?;
    let query_bits: usize = flags
        .get("query-bits")
        .map_or(Ok(P8_QUERY_BITS), |v| v.parse())
        .map_err(|_| "--query-bits")?;
    let nonzero_rows = rows
        .iter()
        .rposition(|byte| *byte != 0)
        .map_or(0, |at| at / shared.profile.row_bytes + 1);
    let variant = flag("variant")?.as_str();
    let result = match variant {
        "baseline" => baseline(&shared, rows, queries, query_rows)?,
        v if v.starts_with("p8-") => p8(&shared, rows, &v[3..], query_bits, queries, query_rows)?,
        v if v.starts_with("certify-") => {
            certify(&shared, rows, &v[8..], query_bits, flag("report")?)?
        }
        other => return Err(format!("unknown variant {other}")),
    };
    println!(
        "{}",
        json!({
            "variant": variant, "geometry": geometry.name, "table": table.as_str(), "path": path,
            "rows": shared.profile.rows, "row_bytes": shared.profile.row_bytes,
            "nonzero_rows": nonzero_rows, "query_rows": query_rows,
            "threads": rayon::current_num_threads(), "build_threads": build_pool().current_num_threads(),
            "peak_rss_bytes": status_bytes("VmHWM:"), "result": result,
        })
    );
    Ok(())
}
