//! Offline phase timings using the production implementation. Never qualifies a host.
use clap::Parser;
use enhance_pir::{client::QuerySession, protocol::*, RECORDS_PER_ROW, RECORD_BYTES};
use enhance_pir_server::{
    ipir::{shard_artifact_dir, PreparedShard, ShardRuntime},
    runtime::{self, Engine, Packing},
    types::{DatabaseId, DatabaseLayout, ENHANCE_LAYOUT},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{io::Write, path::PathBuf, time::Instant};

#[derive(Parser)]
struct Args {
    /// New, disposable artifact directory; existing paths are refused.
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..=20))]
    repetitions: u32,
}
fn records(start: u64, count: usize) -> Result<Vec<u8>, String> {
    let mut bytes = vec![0; count * RECORD_BYTES];
    for (index, record) in bytes.chunks_exact_mut(RECORD_BYTES).enumerate() {
        record[..8].copy_from_slice(&(start + index as u64).to_le_bytes());
    }
    Ok(bytes)
}
fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let budget = enhance_pir_server::PackingBudget::coordinator();
    let args = Args::parse();
    std::fs::create_dir(&args.output)?;
    let mut log = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(args.output.join("timings.jsonl"))?;
    let mut emit = |value: serde_json::Value| -> std::io::Result<()> {
        writeln!(log, "{value}")?;
        log.flush()?;
        println!("{value}");
        Ok(())
    };
    emit(json!({"kind":"environment","qualification":"unqualified",
        "architecture":std::env::consts::ARCH,"os":std::env::consts::OS,
        "binary_sha256":hex::encode(Sha256::digest(std::fs::read(std::env::current_exe()?)?)),
        "repetitions":args.repetitions,"scope":"offline synthetic phases; no network, replication or concurrent traffic; reload may use OS page cache"}))?;
    for repeat in 0..args.repetitions {
        let at = Instant::now();
        let params = parameters(32768)?;
        let client = ipir_sp::IPIRClient::from_profile(
            params.num_items,
            params.item_size_bits,
            ipir_sp::SimplePirProfile::P16Q48,
        )?;
        let setup = client.generate_public_query_setup_simplepir_from_seed(setup_seed(0));
        emit(json!({"kind":"full_shard_public_setup","repeat":repeat,"ms":ms(at)}))?;
        for rows in [2048, 4096, 8192] {
            let layout = DatabaseLayout {
                shard_rows: rows,
                ..ENHANCE_LAYOUT
            };
            let bytes = records(0, rows * RECORDS_PER_ROW)?;
            let hash = hex::encode(Sha256::digest(&bytes));
            let at = Instant::now();
            let prepared = PreparedShard::build(
                &layout,
                0,
                0,
                hash.clone(),
                &bytes,
                runtime::rlwe(),
                setup.polys(),
            )?;
            let build_ms = ms(at);
            let directory = args.output.join(format!("unit-{repeat}-{rows}"));
            let at = Instant::now();
            let cached = prepared.persist(
                &shard_artifact_dir(&directory, DatabaseId::Enhance, 0),
                DatabaseId::Enhance,
                &layout,
                runtime::rlwe(),
            )?;
            let persist_ms = ms(at);
            drop(cached);
            let at = Instant::now();
            let reloaded = ShardRuntime::load_cached(
                &directory,
                DatabaseId::Enhance,
                &layout,
                0,
                0,
                &hash,
                runtime::rlwe(),
            )?;
            let reload_ms = ms(at);
            drop(reloaded);
            emit(json!({"kind":"unit","repeat":repeat,"rows":rows,
                "build_ms":build_ms,"persist_ms":persist_ms,"reload_ms":reload_ms,
                "build_scope":"row-to-database encoding and offline PIR precomputation; fixture generation, hashing and public setup excluded"}))?;
            std::fs::remove_dir_all(directory)?;
        }
        drop(setup);
        for rows in [4096_u64, 8192, 16384, 32768] {
            // At the exact 32K boundary the lifecycle creates a lender and borrower.
            // One record below it exercises a single full 32K allocated domain.
            let record_count = rows * RECORDS_PER_ROW as u64 - u64::from(rows == 32768);
            let coverage = Lifecycle::default().coverage(record_count, Geometry::default())?;
            let at = Instant::now();
            let plan = runtime::plan(coverage.shards[0].clone(), records)?;
            let plan_ms = ms(at);
            let directory = args.output.join(format!("shard-{repeat}-{rows}"));
            let mut engine = Engine::new(&directory);
            let at = Instant::now();
            let eval = engine.prepare(plan.clone(), records)?;
            let cold_prepare_ms = ms(at);
            let at = Instant::now();
            let hint = eval.hint()?;
            let hint_ms = ms(at);
            let at = Instant::now();
            let packing = Packing::new(rows, &hint, &budget)?;
            let packing_ms = ms(at);
            let manifest = Manifest {
                recovery_epoch: 0,
                placement_revision: 0,
                domain_recovery_epochs: [(0, "0".into())].into(),
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
                unit_identities: [(0, plan.units.clone())].into(),
            };
            let client = QuerySession::new(&manifest, packing.session(&manifest, 0))?;
            let mut pack_samples_ms = Vec::new();
            let mut query_bytes = 0;
            let mut response_bytes = 0;
            for position in [0, 32, 33, record_count - 1] {
                let (query, slot) = client.prepare_position(position)?;
                let coefficients = packing.query_coefficients(
                    query.body(),
                    QueryBinding::decode(query.body())?,
                    client.query_rows(),
                )?;
                let intermediate = eval.evaluate(&coefficients)?;
                let at = Instant::now();
                let response = packing.pack(query.body(), &intermediate, client.query_rows())?;
                pack_samples_ms.push(ms(at));
                query_bytes = query.body().len();
                response_bytes = response.len();
                let row = client.decode(query, &response)?;
                if row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES] != records(position, 1)? {
                    return Err("incorrect benchmark answer".into());
                }
            }
            let at = Instant::now();
            let reused = engine.prepare(plan.clone(), |_, _| {
                Err("unexpected canonical reread".into())
            })?;
            let reuse_ms = ms(at);
            let database_bytes = engine.live_bytes();
            drop(reused);
            drop(eval);
            drop(engine);
            let mut restarted = Engine::new(&directory);
            let at = Instant::now();
            let reloaded = restarted.prepare(plan, |_, _| Err("unexpected rebuild".into()))?;
            let reload_ms = ms(at);
            emit(
                json!({"kind":"shard","repeat":repeat,"rows":rows,"populated_records":record_count,"database_bytes":database_bytes,
                "plan_ms":plan_ms,"cold_prepare_ms":cold_prepare_ms,"hint_ms":hint_ms,
                "packing_ms":packing_ms,"reuse_ms":reuse_ms,"reload_ms":reload_ms,
                "pack_samples_ms":pack_samples_ms,"query_bytes":query_bytes,"response_bytes":response_bytes,
                "pack_scope":"Packing::pack only, after evaluation; includes revalidation and key deserialization, excludes request reception and response transfer",
                "exact_answers":4,"prepare_scope":"production Engine including setup, row generation, unit build and artifact persistence"}),
            )?;
            drop(reloaded);
            drop(restarted);
            std::fs::remove_dir_all(directory)?;
        }
    }
    emit(json!({"kind":"complete","qualification":"unqualified"}))?;
    Ok(())
}
