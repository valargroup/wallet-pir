//! Isolated packing allocation/cgroup probe. Fixture preparation is a separate process.
//! This measures production Packing, not HTTP admission, routing or deployment readiness.
use clap::{Parser, Subcommand};
use enhance_pir::{client::QuerySession, protocol::*, RECORDS_PER_ROW, RECORD_BYTES};
use enhance_pir_server::{
    runtime::{self, Engine, Packing},
    wire::{read_crs_blocks, write_crs_blocks},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    fs::{self, File},
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
    time::{Duration, Instant},
};

struct Tracked;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
fn charge(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}
// Track requested live Rust heap bytes, separately from allocator RSS and cgroup charges.
unsafe impl GlobalAlloc for Tracked {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            charge(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            charge(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, old, size) };
        if !result.is_null() {
            if size >= old.size() {
                charge(size - old.size());
            } else {
                LIVE.fetch_sub(old.size() - size, Ordering::Relaxed);
            }
        }
        result
    }
}
#[global_allocator]
static ALLOCATOR: Tracked = Tracked;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Prepare {
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 32768)]
        rows: u64,
        #[arg(long, default_value_t = 16)]
        queries: usize,
    },
    Measure {
        #[arg(long)]
        fixture: PathBuf,
        #[arg(long, default_value_t = 1)]
        copies: usize,
        #[arg(long, default_value_t = 1)]
        concurrency: usize,
        #[arg(long, default_value_t = 16)]
        iterations: usize,
        #[arg(long, default_value_t = 0)]
        hold_ms: u64,
        #[arg(long)]
        overlap: bool,
    },
}
#[derive(Serialize, Deserialize)]
struct Sample {
    body: Vec<u8>,
    intermediate: Vec<u64>,
    response_sha256: String,
}
#[derive(Serialize, Deserialize)]
struct Fixture {
    rows: u64,
    blocks: usize,
    samples: Vec<Sample>,
}
fn emit(stage: &str, extra: serde_json::Value) {
    let live = LIVE.load(Ordering::Relaxed);
    let peak = PEAK.load(Ordering::Relaxed);
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let rss: Vec<_> = status
        .lines()
        .filter(|l| l.starts_with("VmRSS:") || l.starts_with("VmHWM:"))
        .collect();
    println!(
        "{}",
        json!({"stage":stage,"heap_live":live,"heap_peak":peak,"rss":rss,"detail":extra})
    );
    std::io::stdout().flush().unwrap();
}
fn records(start: u64, count: usize) -> Result<Vec<u8>, String> {
    let mut bytes = vec![0; count * RECORD_BYTES];
    for (index, record) in bytes.chunks_exact_mut(RECORD_BYTES).enumerate() {
        // Nonzero deterministic data across the complete record, including its final byte.
        for (offset, byte) in record.iter_mut().enumerate() {
            *byte = (start
                .wrapping_add(index as u64)
                .wrapping_mul(37)
                .wrapping_add(offset as u64 * 13)
                % 251) as u8;
        }
        record[..8].copy_from_slice(&(start + index as u64).to_le_bytes());
    }
    Ok(bytes)
}
fn load_packing(path: &Path, fixture: &Fixture) -> Result<Packing, String> {
    let hint = read_crs_blocks(
        BufReader::new(File::open(path.join("hint.bin")).map_err(|e| e.to_string())?),
        fixture.blocks,
        runtime::rlwe().d,
    )
    .map_err(|e| e.to_string())?;
    Packing::new(fixture.rows, &hint)
}
fn prepare(output: PathBuf, rows: u64, queries: usize) -> Result<(), Box<dyn std::error::Error>> {
    if ![4096, 8192, 16384, 32768].contains(&rows) || queries == 0 {
        return Err("unsupported fixture geometry/count".into());
    }
    fs::create_dir(&output)?;
    let count = rows * RECORDS_PER_ROW as u64 - u64::from(rows == 32768);
    let coverage = Lifecycle::default().coverage(count, Geometry::default())?;
    let plan = runtime::plan(coverage.shards[0].clone(), records)?;
    let mut engine = Engine::new(&output.join("database"));
    let eval = engine.prepare(plan.clone(), records)?;
    let hint = eval.hint()?;
    let mut file = BufWriter::new(File::create(output.join("hint.bin"))?);
    write_crs_blocks(&mut file, &hint)?;
    file.flush()?;
    let packing = Packing::new(rows, &hint)?;
    let manifest = Manifest {
        recovery_epoch: 0,
        placement_revision: 1,
        domain_recovery_epochs: [(0, "0".into())].into(),
        schema_version: SCHEMA_VERSION,
        protocol_revision: PROTOCOL_REVISION.into(),
        // Protocol requires main/ironwood even for this offline synthetic database.
        network: "main".into(),
        pool: "ironwood".into(),
        generation: 1,
        anchor_height: 0,
        anchor_block_hash: "00".repeat(32),
        geometry: Geometry::default(),
        coverage,
        sessions: vec![packing.reference(0)?],
        unit_identities: [(0, plan.units)].into(),
    };
    let client = QuerySession::new(&manifest, packing.session(&manifest, 0))?;
    let mut samples = Vec::new();
    for index in 0..queries {
        let position = match index % 4 {
            0 => 0,
            1 => 32,
            2 => 33,
            _ => count - 1,
        };
        let (query, slot) = client.prepare_position(position)?;
        let body = query.body().to_vec();
        let coefficients = packing.query_coefficients(&body, QueryBinding::decode(&body)?)?;
        let intermediate = eval.evaluate(&coefficients)?;
        let response = packing.pack(&body, &intermediate)?;
        let row = client.decode(query, &response)?;
        if row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES] != records(position, 1)? {
            return Err("incorrect fixture answer".into());
        }
        samples.push(Sample {
            body,
            intermediate,
            response_sha256: hex::encode(Sha256::digest(response)),
        });
    }
    serde_json::to_writer(
        BufWriter::new(File::create(output.join("fixture.json"))?),
        &Fixture {
            rows,
            blocks: hint.len(),
            samples,
        },
    )?;
    emit(
        "fixture_complete",
        json!({"rows":rows,"exact_answers":queries,"hint_bytes":fs::metadata(output.join("hint.bin"))?.len()}),
    );
    drop(eval);
    drop(engine);
    fs::remove_dir_all(output.join("database"))?;
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Args::parse().command {
        Command::Prepare {
            output,
            rows,
            queries,
        } => prepare(output, rows, queries)?,
        Command::Measure {
            fixture: path,
            copies,
            concurrency,
            iterations,
            hold_ms,
            overlap,
        } => {
            if copies == 0 || concurrency == 0 || iterations == 0 {
                return Err("counts must be positive".into());
            }
            let fixture: Fixture =
                serde_json::from_reader(BufReader::new(File::open(path.join("fixture.json"))?))?;
            emit(
                "baseline",
                json!({"rows":fixture.rows,"copies":copies,"concurrency":concurrency,"iterations":iterations,"overlap":overlap,"hold_ms":hold_ms,"fixture_samples":fixture.samples.len(),"body_bytes":fixture.samples[0].body.len(),"intermediate_bytes":fixture.samples[0].intermediate.len()*8}),
            );
            let mut packing = Vec::new();
            for index in 0..copies {
                let at = Instant::now();
                packing.push(load_packing(&path, &fixture)?);
                emit(
                    "resident",
                    json!({"copies":index+1,"setup_ms":at.elapsed().as_secs_f64()*1000.0}),
                );
            }
            PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
            let start = Instant::now();
            let barrier = Arc::new(Barrier::new(concurrency + usize::from(overlap)));
            std::thread::scope(|scope| -> Result<(), String> {
                let mut handles = Vec::new();
                for worker in 0..concurrency {
                    let pack = &packing[worker % copies];
                    let fixture = &fixture;
                    let barrier = barrier.clone();
                    handles.push(scope.spawn(move || -> Result<Vec<f64>, String> {
                        let mut times = Vec::new();
                        for iteration in 0..iterations {
                            let sample =
                                &fixture.samples[(worker + iteration) % fixture.samples.len()];
                            let body = sample.body.clone();
                            let intermediate = sample.intermediate.clone();
                            let coefficients =
                                pack.query_coefficients(&body, QueryBinding::decode(&body)?)?;
                            if iteration == 0 {
                                barrier.wait();
                            }
                            let at = Instant::now();
                            let response = pack.pack(&body, &intermediate)?;
                            times.push(at.elapsed().as_secs_f64() * 1000.0);
                            if hex::encode(Sha256::digest(&response)) != sample.response_sha256 {
                                return Err("response differs from exact-answer fixture".into());
                            }
                            std::thread::sleep(Duration::from_millis(hold_ms));
                            std::hint::black_box((&body, &coefficients, &intermediate, &response));
                        }
                        Ok(times)
                    }));
                }
                let building = if overlap {
                    let barrier = barrier.clone();
                    let path = &path;
                    let fixture = &fixture;
                    Some(scope.spawn(move || {
                        barrier.wait();
                        let result = load_packing(path, fixture);
                        emit("overlap_built", json!({}));
                        result
                    }))
                } else {
                    None
                };
                let mut times = Vec::new();
                for handle in handles {
                    times.extend(handle.join().map_err(|_| "packing thread panicked")??);
                }
                let added = building
                    .map(|h| {
                        h.join()
                            .map_err(|_| "builder panicked")
                            .and_then(|r| r.map_err(|_| "builder failed"))
                    })
                    .transpose()?;
                times.sort_by(f64::total_cmp);
                emit(
                    "complete",
                    json!({"responses_verified":times.len(),"seconds":start.elapsed().as_secs_f64(),"pack_p50_ms":times[times.len()/2],"pack_p99_ms":times[(times.len()*99/100).min(times.len()-1)],"overlap_retained":added.is_some()}),
                );
                std::hint::black_box(&added);
                Ok(())
            })?;
            std::hint::black_box(&packing);
        }
    }
    Ok(())
}
