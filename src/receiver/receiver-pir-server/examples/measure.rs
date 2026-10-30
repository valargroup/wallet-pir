//! Measures Receiver's directory and companion tables (plan S8): message sizes, setup and server time,
//! per-table preparation memory, and segment build latency near the seal target.
//!
//! ```text
//! cargo run --profile release-fast -p receiver-pir-server --example measure -- table directory 2048
//! cargo run --profile release-fast -p receiver-pir-server --example measure -- table companion 65536 [payments]
//! cargo run --profile release-fast -p receiver-pir-server --example measure -- build 317440
//! ```
//!
//! `table` prepares one table in this process, so its peak RSS is that table's
//! preparation, then times queries. `build` projects and builds a segment of that many
//! payments, one receiver each, then a block of 100 more, and replays the projection.
//! Each prints one JSON line.
use std::time::Instant;
use zakura_pir_client::native::{ProfileClientExt as _, D, P8};
use zakura_pir_publisher::receiver::{
    self as receiver, build, initial_salt, native, Coverage, Kind, Paid, Projection, Segment, Tree,
    OPEN_SEED_ATTEMPTS, ROW_SLOTS,
};
use zakura_pir_publisher::receiver::{Payment, ReceiverBytes};
use zakura_pir_query_server::receiver::server::Server;
use zakura_pir_query_server::receiver::session::HEADER_BYTES;

/// Counts live heap bytes and their peak, so a table's charge is measured exactly
/// rather than through the allocator's retained pages.
struct Counting;

static LIVE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static PEAK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

unsafe impl std::alloc::GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        use std::sync::atomic::Ordering::Relaxed;
        let live = LIVE.fetch_add(layout.size() as u64, Relaxed) + layout.size() as u64;
        PEAK.fetch_max(live, Relaxed);
        std::alloc::System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        LIVE.fetch_sub(layout.size() as u64, std::sync::atomic::Ordering::Relaxed);
        std::alloc::System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Live heap bytes now, and the peak since the last call, which resets it.
fn memory() -> (u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    let live = LIVE.load(Relaxed);
    (live, PEAK.swap(live, Relaxed))
}

/// A structurally valid payload for `position` at `size`: a placeholder node for every
/// sibling that tree supplies. Node values are not checked structurally.
fn payload(position: u64, size: u64) -> [u8; 512] {
    use zakura_pir_publisher::receiver::wire::{encode_payload, supplied};
    let siblings: [Option<[u8; 32]>; 16] =
        std::array::from_fn(|level| supplied(position, size, level as u8).then_some([1; 32]));
    encode_payload(&siblings, position, size).unwrap()
}

/// A prepared table's answer to one query body.
type Respond = Box<dyn Fn(&[u8]) -> Vec<u8>>;

/// `n` payments to distinct receivers in one block at height 100.
fn paid(n: u32) -> Vec<Paid> {
    (0..n)
        .map(|i| {
            let mut receiver: ReceiverBytes = [0; 43];
            receiver[..4].copy_from_slice(&i.to_le_bytes());
            let mut txid = [0; 32];
            txid[..4].copy_from_slice(&i.to_le_bytes());
            let payment = Payment {
                height: 100,
                block_hash: [3; 32],
                txid,
                tx_index: i + 1,
                action_index: 0,
                position: i.into(),
                action_nullifier: [5; 32],
                cmx: [6; 32],
                ephemeral_key: [7; 32],
                ciphertext_prefix: [(i % 251) as u8 + 1; 52],
            };
            (receiver, payment, payload(i.into(), u64::from(n).max(1)))
        })
        .collect()
}

/// Coverage of one block holding `positions` notes.
fn coverage(positions: u64) -> Coverage {
    Coverage {
        genesis: [1; 32],
        history_revision: 0,
        start_height: 100,
        start_parent: [2; 32],
        start_position: 0,
        end_height: 100,
        end_hash: [3; 32],
        end_position: positions,
    }
}

/// A segment of `n` payments with a companion of `rows` rows.
fn segment(n: u32, rows: u32) -> Segment {
    let paid = paid(n);
    let mut projection = Projection::new(initial_salt(&[2; 32], 0));
    let receivers: Vec<_> = paid.iter().map(|p| p.0).collect();
    projection.project(&receivers, &[3; 32]).unwrap();
    let tree = Tree {
        height: 100,
        hash: [3; 32],
        size: u64::from(n).max(1),
    };
    build(
        &projection,
        coverage(u64::from(n).max(1)),
        &[3; 32],
        OPEN_SEED_ATTEMPTS,
        rows,
        tree,
        &paid,
    )
    .unwrap()
}

/// Times one encrypted query of `sent` rows against `respond`, returning upload,
/// download, client and server milliseconds.
fn query(
    kind: Kind,
    rows: u32,
    sent: u32,
    id: [u8; 32],
    public: &[u8],
    respond: &dyn Fn(&[u8]) -> Vec<u8>,
) -> serde_json::Value {
    let profile = native::profile(kind, rows).unwrap();
    let started = Instant::now();
    let (secret, upload) = P8
        .prepare(
            &profile.setup,
            &profile.masks[..sent as usize / D],
            sent as usize,
            1,
        )
        .unwrap();
    let body = [&native::MAGIC[..], &id, &[0; 16], &upload].concat();
    let prepare = started.elapsed();
    let started = Instant::now();
    let response = respond(&body);
    let server = started.elapsed();
    let started = Instant::now();
    P8.decode_cols(&secret, public, &response[HEADER_BYTES..], native::COLS)
        .unwrap();
    let decode = started.elapsed();
    serde_json::json!({
        "rows_sent": sent, "upload": body.len(), "download": response.len(),
        "client_ms": (prepare + decode).as_secs_f64() * 1e3,
        "server_ms": server.as_secs_f64() * 1e3,
    })
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args[0].as_str() {
        "table" => {
            let rows: u32 = args[2].parse().unwrap();
            let kind = match args[1].as_str() {
                "directory" => Kind::Directory,
                _ => Kind::Companion,
            };
            // A full companion of the requested rows, or a directory over one row's payments.
            let payments = match (kind, args.get(3)) {
                (Kind::Directory, _) => 1000,
                (Kind::Companion, Some(payments)) => payments.parse().unwrap(),
                (Kind::Companion, None) => rows * ROW_SLOTS,
            };
            let rows_c = match kind {
                Kind::Directory => receiver::QUERY_BLOCK_ROWS,
                Kind::Companion => rows,
            };
            let tables = segment(payments.min(receiver::CAPACITY), rows_c);
            let (directory_manifest, entries) = (
                tables.directory.manifest.clone(),
                tables.directory.validate().unwrap(),
            );
            // Charges count from a baseline holding only the rows being prepared, as a
            // load that reads them from disk would.
            let (directory, companion) = match kind {
                Kind::Directory => (Some(tables.directory), None),
                Kind::Companion => (None, Some(tables.companion)),
            };
            let input = (directory.as_ref().map(|t| t.data.len() + t.choice.len()))
                .or(companion.as_ref().map(|t| t.data.len()))
                .unwrap() as u64;
            let (base, _) = memory();
            let started = Instant::now();
            let (id, public, respond): (_, Vec<u8>, Respond) = match kind {
                Kind::Directory => {
                    let (server, _) = Server::directory(directory.unwrap()).unwrap();
                    (
                        server.id(),
                        server.public().to_vec(),
                        Box::new(move |b| server.respond(b).unwrap()),
                    )
                }
                Kind::Companion => {
                    let server =
                        Server::companion(companion.unwrap(), &directory_manifest, &entries)
                            .unwrap();
                    (
                        server.id(),
                        server.public().to_vec(),
                        Box::new(move |b| server.respond(b).unwrap()),
                    )
                }
            };
            let setup = started.elapsed();
            let (live, peak) = memory();
            let q = query(kind, rows, rows, id, &public, &respond);
            let mut queries = vec![q];
            let trimmed = receiver::query_rows(payments);
            if kind == Kind::Companion && trimmed < rows {
                queries.push(query(kind, rows, trimmed, id, &public, &respond));
            }
            let result = serde_json::json!({
                "setup_ms": setup.as_secs_f64() * 1e3,
                "resident": live + input - base,
                "peak": peak + input - base,
                "public": public.len(),
                "query": queries,
            });
            println!(
                "{}",
                serde_json::json!({"kind": args[1], "rows": rows, "result": result})
            );
        }
        "build" => {
            let n: u32 = args[1].parse().unwrap();
            let paid = paid(n + 100);
            let receivers: Vec<_> = paid.iter().map(|p| p.0).collect();
            let started = Instant::now();
            let mut projection = Projection::new(initial_salt(&[2; 32], 0));
            projection
                .project(&receivers[..n as usize], &[3; 32])
                .unwrap();
            let replay = started.elapsed();
            let started = Instant::now();
            projection
                .project(&receivers[n as usize..], &[4; 32])
                .unwrap();
            let block = started.elapsed();
            let tree = Tree {
                height: 100,
                hash: [3; 32],
                size: u64::from(n) + 100,
            };
            let rows = receiver::query_rows(n + 100);
            let started = Instant::now();
            let built = build(
                &projection,
                coverage(u64::from(n) + 100),
                &[3; 32],
                OPEN_SEED_ATTEMPTS,
                rows,
                tree,
                &paid,
            )
            .unwrap();
            let build_ms = started.elapsed();
            let started = Instant::now();
            built.directory.validate().unwrap();
            let validate = started.elapsed();
            println!(
                "{}",
                serde_json::json!({
                    "payments": n + 100, "companion_rows": rows,
                    "replay_projection_ms": replay.as_secs_f64() * 1e3,
                    "block_projection_ms": block.as_secs_f64() * 1e3,
                    "build_ms": build_ms.as_secs_f64() * 1e3,
                    "directory_validate_ms": validate.as_secs_f64() * 1e3,
                    "choice_bytes": built.directory.choice.len(),
                })
            );
        }
        _ => panic!("table|build"),
    }
}
