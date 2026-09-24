//! Isolated D9 geometry comparison; permits an unqualified 2K unit layout only in this example.
use clap::Parser;
use enhance_pir::{client::QuerySession, protocol::*, RECORDS_PER_ROW, RECORD_BYTES};
use enhance_pir_server::{
    ipir::{shard_artifact_dir, PreparedShard},
    runtime::{self, DomainPlan, Evaluation, Packing},
    types::{DatabaseId, DatabaseLayout, ENHANCE_LAYOUT},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{io::Write, path::PathBuf, sync::Arc, time::Instant};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    output: PathBuf,
    #[arg(long, value_parser = clap::value_parser!(u64).range(2048..=8192))]
    cap: u64,
    #[arg(long, default_value_t = 12)]
    queries: usize,
    #[arg(long, default_value_t = 0)]
    retained_revisions: usize,
}
fn records(start: u64, count: usize) -> Result<Vec<u8>, String> {
    let mut bytes = vec![0; count * RECORD_BYTES];
    for (i, record) in bytes.chunks_exact_mut(RECORD_BYTES).enumerate() {
        record[..8].copy_from_slice(&(start + i as u64).to_le_bytes());
    }
    Ok(bytes)
}
fn ms(at: Instant) -> f64 {
    at.elapsed().as_secs_f64() * 1000.0
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.cap != 2048 && args.cap != 8192 {
        return Err("cap must be 2048 or 8192".into());
    }
    std::fs::create_dir(&args.output)?;
    let mut log = std::fs::File::create_new(args.output.join("timings.jsonl"))?;
    let mut emit = |value: serde_json::Value| -> std::io::Result<()> {
        writeln!(log, "{value}")?;
        log.flush()?;
        println!("{value}");
        Ok(())
    };
    let record_count = 32768 * RECORDS_PER_ROW as u64 - 1;
    let coverage = Lifecycle::default().coverage(record_count, Geometry::default())?;
    let canonical_shard = coverage.shards[0].clone();
    let mut shard = canonical_shard.clone();
    shard.units = (0..32768 / args.cap)
        .map(|i| MutableUnit {
            local_row_start: i * args.cap,
            used_rows: args.cap,
            allocated_rows: args.cap,
        })
        .collect();
    let params = parameters(32768)?;
    let client = ipir_sp::IPIRClient::from_profile(
        params.num_items,
        params.item_size_bits,
        ipir_sp::SimplePirProfile::P16Q48,
    )?;
    let setup = client.generate_public_query_setup_simplepir_from_seed(setup_seed(0));
    emit(
        json!({"kind":"environment","cap":args.cap,"queries":args.queries,
        "os":std::env::consts::OS,"arch":std::env::consts::ARCH,
        "binary_sha256":hex::encode(Sha256::digest(std::fs::read(std::env::current_exe()?)?)),
        "scope":"synthetic full-size single domain; experimental unit partition; no service, replica, or publication"}),
    )?;
    let mut cached = Vec::new();
    let mut identities = Vec::new();
    for spec in &shard.units {
        let start = spec.local_row_start * RECORDS_PER_ROW as u64;
        let count = ((record_count - start).min(args.cap * RECORDS_PER_ROW as u64)) as usize;
        let mut bytes = records(start, count)?;
        bytes.resize(args.cap as usize * ENHANCE_LAYOUT.row_bytes(), 0);
        let hash = hex::encode(Sha256::digest(&bytes));
        let layout = DatabaseLayout {
            shard_rows: args.cap as usize,
            ..ENHANCE_LAYOUT
        };
        let at = Instant::now();
        let prepared = PreparedShard::build(
            &layout,
            0,
            spec.local_row_start as usize,
            hash.clone(),
            &bytes,
            runtime::rlwe(),
            setup.polys(),
        )?;
        let build_ms = ms(at);
        let at = Instant::now();
        let unit = prepared.persist(
            &shard_artifact_dir(&args.output, DatabaseId::Enhance, spec.local_row_start),
            DatabaseId::Enhance,
            &layout,
            runtime::rlwe(),
        )?;
        let persist_ms = ms(at);
        cached.push(Arc::new(unit));
        identities.push(UnitIdentity {
            table: "enhance".into(),
            shard_id: 0,
            local_row_start: spec.local_row_start,
            allocated_rows: args.cap,
            setup_sha256: hex::encode(Sha256::digest(setup_seed(0))),
            parameter_id: unit_parameter_id(args.cap)?,
            content_sha256: hash,
        });
        emit(
            json!({"kind":"unit","cap":args.cap,"offset":spec.local_row_start,
            "build_ms":build_ms,"persist_ms":persist_ms} ),
        )?;
    }
    let mut eval = Evaluation {
        plan: DomainPlan {
            shard,
            units: identities,
        },
        units: cached,
    };
    let at = Instant::now();
    let hint = eval.hint()?;
    let hint_ms = ms(at);
    let at = Instant::now();
    let packing = Packing::new(32768, &hint)?;
    let packing_ms = ms(at);
    emit(
        json!({"kind":"publication","cap":args.cap,"units":eval.units.len(),
        "hint_ms":hint_ms,"packing_ms":packing_ms}),
    )?;
    let canonical_plan = runtime::plan(canonical_shard, records)?;
    let manifest = Manifest {
        schema_version: SCHEMA_VERSION,
        protocol_revision: PROTOCOL_REVISION.into(),
        network: "main".into(),
        pool: "ironwood".into(),
        generation: 1,
        anchor_height: 3428143,
        anchor_block_hash: "01".repeat(32),
        geometry: Geometry::default(),
        coverage,
        sessions: vec![packing.reference(0)?],
        unit_identities: [(0, canonical_plan.units)].into(),
    };
    let session = QuerySession::new(&manifest, packing.session(1, 0))?;
    for index in 0..args.queries {
        let position = (index as u64 * 7919) % record_count;
        let (query, slot) = session.prepare_position(position)?;
        let coeffs =
            packing.query_coefficients(query.body(), QueryBinding::decode(query.body())?)?;
        let at = Instant::now();
        let intermediate = eval.evaluate(&coeffs)?;
        let evaluate_ms = ms(at);
        let at = Instant::now();
        let response = packing.pack(query.body(), &intermediate)?;
        let pack_ms = ms(at);
        let row = session.decode(query, &response)?;
        if row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES] != records(position, 1)? {
            return Err("incorrect encrypted answer".into());
        }
        emit(json!({"kind":"query","cap":args.cap,"index":index,
            "evaluate_ms":evaluate_ms,"pack_ms":pack_ms,"correct":true}))?;
    }
    let mut retained = Vec::new();
    retained.push(eval.units.last().unwrap().clone());
    for revision in 1..=args.retained_revisions {
        let spec = eval.plan.shard.units.last().unwrap();
        let start = spec.local_row_start * RECORDS_PER_ROW as u64;
        let count = (record_count - start) as usize;
        let mut bytes = records(start, count)?;
        bytes[0..8].copy_from_slice(&(start + revision as u64).to_le_bytes());
        bytes.resize(args.cap as usize * ENHANCE_LAYOUT.row_bytes(), 0);
        let hash = hex::encode(Sha256::digest(&bytes));
        let layout = DatabaseLayout {
            shard_rows: args.cap as usize,
            ..ENHANCE_LAYOUT
        };
        let at = Instant::now();
        let prepared = PreparedShard::build(
            &layout,
            0,
            spec.local_row_start as usize,
            hash,
            &bytes,
            runtime::rlwe(),
            setup.polys(),
        )?;
        let build_ms = ms(at);
        let at = Instant::now();
        let unit = prepared.persist(
            &shard_artifact_dir(
                &args.output.join(format!("revision-{revision}")),
                DatabaseId::Enhance,
                spec.local_row_start,
            ),
            DatabaseId::Enhance,
            &layout,
            runtime::rlwe(),
        )?;
        let persist_ms = ms(at);
        let unit = Arc::new(unit);
        retained.push(unit.clone());
        *eval.units.last_mut().unwrap() = unit;
        let at = Instant::now();
        let hint = eval.hint()?;
        let hint_ms = ms(at);
        let at = Instant::now();
        let _packing = Packing::new(32768, &hint)?;
        let packing_ms = ms(at);
        let rss_kib = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse::<u64>().ok());
        emit(json!({"kind":"revision","cap":args.cap,"revision":revision,
            "build_ms":build_ms,"persist_ms":persist_ms,"hint_ms":hint_ms,
            "packing_ms":packing_ms,"rss_kib":rss_kib,"retained_units":retained.len()}))?;
    }
    emit(json!({"kind":"complete","cap":args.cap}))?;
    Ok(())
}
