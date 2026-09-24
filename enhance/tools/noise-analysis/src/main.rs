mod certificate;
mod fast_weights;
use clap::Parser;
use enhance_pir::{
    protocol::{self, Geometry},
    RECORDS_PER_ROW, RECORD_BYTES,
};
use enhance_pir_server::ipir::{
    add_crs_blocks_assign_mod, add_intermediate_assign_mod, deserialize_first_dim_query,
};
use inspiring::TopKeyImages;
use ipir_sp::modulus_switch::{recover_published_c1, serialize_rlwe_response_bodies};
use ipir_sp::server::{
    build_pack_preprocessed_blocks, pack_intermediate_blocks, published_c1_rows, YServer,
};
use ipir_sp::{IPIRClient, ProductionSimplePirParams, SimplePirProfile};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{io::Write, path::PathBuf, time::Instant};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long)]
    matrix: bool,
    #[arg(long, default_value_t = 8192)]
    used_rows: u64,
    #[arg(long, default_value = "max")]
    pattern: String,
    #[arg(long, default_value_t = 0)]
    shard: u64,
    #[arg(long, default_value_t = 128)]
    queries: usize,
    /// Schema-11 records, captured read-only; zero padding is generated locally.
    #[arg(long)]
    records: Option<PathBuf>,
    /// Compute weights even if the deterministic sufficient budget is negative.
    #[arg(long)]
    force_weights: bool,
    /// Historical schema-10 captures use 737; synthetic cases remain schema 11.
    #[arg(long, default_value_t = RECORD_BYTES)]
    record_width: usize,
    /// Require a reconstructed historical public setup to match this digest.
    #[arg(long)]
    expected_public_sha256: Option<String>,
}
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
fn matrix() -> serde_json::Value {
    let g = Geometry::default();
    let mut ranges = std::collections::BTreeMap::<(u64, Vec<u64>), (u64, u64)>::new();
    for used in 1..=32768 {
        let records = used * RECORDS_PER_ROW as u64;
        let key = (
            g.logical_rows(records).unwrap(),
            g.units(records)
                .unwrap()
                .iter()
                .map(|u| u.allocated_rows)
                .collect(),
        );
        ranges
            .entry(key)
            .and_modify(|v| v.1 = used)
            .or_insert((used, used));
    }
    json!(ranges.into_iter().map(|((domain,units),(first,last))| json!({"domain_rows":domain,"units":units,"first_used_rows":first,"last_used_rows":last,"params":protocol::parameters(domain).unwrap()})).collect::<Vec<_>>())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.matrix {
        println!("{}", serde_json::to_string_pretty(&matrix())?);
        return Ok(());
    }
    if args.used_rows == 0 || args.used_rows > 32768 {
        return Err("used rows must be in 1..=32768".into());
    }
    if args.queries == 0 {
        return Err("queries must be positive".into());
    }
    if !["zero", "max", "alternating", "impulse", "random", "records"]
        .contains(&args.pattern.as_str())
    {
        return Err("unknown fixture".into());
    }
    let output = args.output.as_ref().ok_or("--output required")?;
    if output.exists() {
        return Err("output already exists".into());
    }
    if ![653, 737].contains(&args.record_width) {
        return Err("unsupported record width".into());
    }
    let captured = args.records.as_ref().map(std::fs::read).transpose()?;
    if captured
        .as_ref()
        .is_some_and(|v| v.is_empty() || v.len() % args.record_width != 0)
    {
        return Err(
            "record file length must be a positive multiple of the selected record width".into(),
        );
    }
    if captured.is_some() && args.pattern != "records" {
        return Err("--records requires --pattern records".into());
    }
    if ![653, 737].contains(&args.record_width)
        || (args.record_width != RECORD_BYTES && captured.is_none())
    {
        return Err("historical record width requires a captured record file".into());
    }
    let record_width = args.record_width;
    let row_bytes = record_width * RECORDS_PER_ROW;
    let records = captured
        .as_ref()
        .map(|v| v.len() / record_width)
        .unwrap_or(args.used_rows as usize * RECORDS_PER_ROW - 1) as u64;
    let g = Geometry::default();
    let rows = g.logical_rows(records)?;
    let specs = g.units(records)?;
    let used = records.div_ceil(RECORDS_PER_ROW as u64) as usize;
    let profile =
        ProductionSimplePirParams::new(rows, (row_bytes * 8) as u64, SimplePirProfile::P16Q48)?;
    let r = profile.rlwe();
    let p = profile.ypir();
    if record_width == RECORD_BYTES {
        assert_eq!(
            serde_json::to_value(p)?,
            serde_json::to_value(protocol::parameters(rows)?)?
        );
    }
    let start = Instant::now();
    let value = |row: usize, col: usize| -> u16 {
        if row >= used {
            return 0;
        }
        match args.pattern.as_str() {
            "zero" => 0,
            "max" => u16::MAX,
            "alternating" => {
                if (row + col) & 1 == 0 {
                    u16::MAX
                } else {
                    0
                }
            }
            "impulse" => {
                if row == used - 1 && col == 0 {
                    u16::MAX
                } else {
                    0
                }
            }
            "random" => mix((row as u64).wrapping_mul(0x9e3779b97f4a7c15) ^ (col as u64)) as u16,
            "records" => {
                let byte = |offset: usize| -> u8 {
                    let slot = offset / record_width;
                    let within = offset % record_width;
                    let record = row * RECORDS_PER_ROW + slot;
                    if offset >= row_bytes || record >= records as usize {
                        return 0;
                    }
                    if let Some(data) = &captured {
                        return data[record * record_width + within];
                    }
                    // Random ciphertext bytes; zero flags, expiry and absent fee form valid metadata.
                    if within >= 640 {
                        0
                    } else {
                        (mix((record as u64) * 1024 + within as u64) & 255) as u8
                    }
                };
                u16::from_le_bytes([byte(2 * col), byte(2 * col + 1)])
            }
            _ => unreachable!(),
        }
    };
    let client = IPIRClient::new(&profile);
    let seed = protocol::setup_seed(args.shard);
    let setup = client.generate_public_query_setup_simplepir_from_seed(seed);
    // Generate the public fixture once; both production/reference servers own
    // separate transposed copies. This avoids recomputing record bytes while
    // hashing and constructing each partition.
    let database: Vec<u16> = (0..p.db_rows)
        .flat_map(|row| (0..p.db_cols).map(move |col| value(row, col)))
        .collect();
    let mono = YServer::from_profile(&profile, database.iter().copied(), false, true);
    let mut db_hash = Sha256::new();
    let mut sums = vec![0u64; p.db_cols];
    let mut encoded_row = vec![0; p.db_cols * 2];
    for row in database.chunks_exact(p.db_cols) {
        for (col, (&v, sum)) in row.iter().zip(&mut sums).enumerate() {
            encoded_row[col * 2..col * 2 + 2].copy_from_slice(&v.to_le_bytes());
            *sum += u64::from(v);
        }
        db_hash.update(&encoded_row);
    }
    let mut units = Vec::new();
    let mut combined: Option<Vec<ipir_sp::server::CrsBlock>> = None;
    for spec in &specs {
        let up = ProductionSimplePirParams::new(
            spec.allocated_rows,
            (row_bytes * 8) as u64,
            SimplePirProfile::P16Q48,
        )?;
        let offset = spec.local_row_start as usize;
        let count = spec.allocated_rows as usize;
        let server = YServer::new_auto_kernel_from_profile(
            &up,
            database[offset * p.db_cols..(offset + count) * p.db_cols]
                .iter()
                .copied(),
            false,
            true,
        );
        let hint = server
            .perform_offline_precomputation_simplepir(
                r,
                &setup.polys()[offset / r.d..(offset + count) / r.d],
            )
            .crs_blocks;
        if let Some(sum) = &mut combined {
            add_crs_blocks_assign_mod(sum, &hint, r)?;
        } else {
            combined = Some(hint);
        }
        units.push(server);
    }
    let hint = combined.unwrap();
    let reference = mono.perform_offline_precomputation_simplepir(r, setup.polys());
    for (a, b) in hint.iter().zip(&reference.crs_blocks) {
        assert_eq!(a.rows, b.rows, "partitioned hint differs");
    }
    drop(reference);
    let pre = build_pack_preprocessed_blocks(r, &hint)?;
    drop(hint);
    let published = published_c1_rows(&pre, r.q);
    let published_hash = hex::encode(Sha256::digest(&published));
    if args
        .expected_public_sha256
        .as_ref()
        .is_some_and(|expected| expected != &published_hash)
    {
        return Err("captured public-material hash mismatch".into());
    }
    let c1 = recover_published_c1(&published, r.d, p.db_cols / r.d, r.q);
    let top = TopKeyImages::build(r);
    let threshold = r.delta / 2;
    let rounding = r.q.div_ceil(1u64 << (p.query_bits + 1)) + 1;
    let deterministic: Vec<u64> = sums
        .iter()
        .map(|&l| (65 + rounding) * l + r.q.div_ceil(1 << 21) + 1 + r.q % r.p)
        .collect();
    let deterministic_possible = deterministic.iter().all(|&d| d < threshold);
    eprintln!("prepared rows={rows} used={used} pattern={} shard={} in {:.1}s; deterministic_possible={deterministic_possible}",args.pattern,args.shard,start.elapsed().as_secs_f64());
    let blocks = if deterministic_possible || args.force_weights {
        Some(certificate::weights(r, &pre, &top))
    } else {
        None
    };
    let cert_seconds = start.elapsed().as_secs_f64();
    let mut targets = vec![0, used - 1];
    for spec in &specs {
        let first = spec.local_row_start as usize;
        if first > 0 {
            targets.extend([first - 1, first]);
        }
    }
    targets.sort_unstable();
    targets.dedup();
    let mut worst = 0;
    let mut failures = 0;
    let mut results = Vec::new();
    for i in 0..args.queries {
        let target = if i < targets.len() {
            targets[i]
        } else {
            (mix(i as u64) % used as u64) as usize
        };
        let expected: Vec<u64> = database[target * p.db_cols..(target + 1) * p.db_cols]
            .iter()
            .map(|&v| u64::from(v))
            .collect();
        let (query, keys, private_seed) = client.generate_fresh_query_simplepir(&setup, target);
        let bytes = query.to_switched_bytes(r.q, p.query_bits);
        let coeffs = deserialize_first_dim_query(r, p, &bytes)?;
        let mut intermediate = vec![0; p.db_cols];
        for (unit, spec) in units.iter().zip(&specs) {
            let offset = spec.local_row_start as usize;
            let partial =
                unit.multiply_query(r, &coeffs[offset..offset + spec.allocated_rows as usize]);
            add_intermediate_assign_mod(&mut intermediate, &partial, r.q)?;
        }
        assert_eq!(
            intermediate,
            mono.multiply_query(r, &coeffs),
            "partitioned intermediate differs"
        );
        let packed = pack_intermediate_blocks(&intermediate, &keys, &top, &pre)?;
        let response = serialize_rlwe_response_bodies(&packed, p.q_prime_1);
        let (decoded, error) = client.decode_response_simplepir_with_expected_phase_error(
            private_seed,
            &c1,
            &response,
            &expected,
        );
        worst = worst.max(error);
        failures += usize::from(decoded != expected);
        results.push(json!({"target":target,"max_expected_phase_error":error,"exact_answer":decoded==expected}));
    }
    let dg = spiral_rs::discrete_gaussian::DiscreteGaussian::init(r.spiral.noise_width);
    let result = json!({"format":1,"implementation":"a16f456dfaf5fadca77d472c0c502a93cf0977d0","schema":if record_width == 737 {10} else {protocol::SCHEMA_VERSION},"record_width":record_width,"expected_public_sha256":args.expected_public_sha256,"pattern":args.pattern,"shard":args.shard,
        "rows":rows,"used_rows":used,"records":records,"units":specs,"params":p,"n":r.d,"q":r.q,"p":r.p,"ell":r.gadget.ell,
        "setup_seed":hex::encode(seed),"database_sha256":hex::encode(db_hash.finalize()),"public_c1_sha256":hex::encode(Sha256::digest(&published)),
        "record_file_sha256":captured.as_ref().map(|v|hex::encode(Sha256::digest(v))),
        "column_sums":sums,"deterministic_bounds":deterministic,"threshold":threshold,"blocks":blocks,
        "cdf_table":dg.cdf_table,"cdf_max_val":dg.max_val,"partitioned_hint_matches":true,"partitioned_intermediates_match":true,
        "queries":results,"failures":failures,"max_expected_phase_error":worst,"threshold_utilization":worst as f64/threshold as f64,
        "certificate_seconds":cert_seconds,"total_seconds":start.elapsed().as_secs_f64(),"architecture":std::env::consts::ARCH,"os":std::env::consts::OS,
        "binary_sha256":hex::encode(Sha256::digest(std::fs::read(std::env::current_exe()?)?))});
    let mut f = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output)?;
    writeln!(f, "{}", serde_json::to_string(&result)?)?;
    f.sync_all()?;
    eprintln!(
        "finished {} queries, failures={failures}, utilization={:.4}, {:.1}s",
        args.queries,
        worst as f64 / threshold as f64,
        start.elapsed().as_secs_f64()
    );
    if failures > 0 {
        return Err("decoding failure; evidence saved".into());
    }
    Ok(())
}
