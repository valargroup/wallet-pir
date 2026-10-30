use super::*;
use receiver_store::{Config, IndexedBlock};
use std::time::Instant;

/// Appends a block of `count` payments, each to its own receiver and transaction,
/// every position a payment.
fn append(store: &mut Store, count: usize, first: usize) {
    let tip = store.tip().unwrap();
    let height = tip.height + 1;
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&height.to_le_bytes());
    let payments = (0..count)
        .map(|i| {
            let mut receiver = [7; 43];
            receiver[..8].copy_from_slice(&((first + i) as u64).to_le_bytes());
            let mut payment = crate::probe::common::record(0, 1).payment;
            (payment.height, payment.block_hash) = (height, hash);
            payment.position = tip.position + i as u64;
            payment.tx_index = i as u32 + 1;
            payment.txid = [0; 32];
            payment.txid[..8].copy_from_slice(&payment.position.to_le_bytes());
            let mut cmx = [0; 32];
            cmx[..8].copy_from_slice(&(payment.position + 1).to_le_bytes());
            payment.cmx = cmx;
            (receiver, payment)
        })
        .collect::<Vec<_>>();
    let commitments = payments.iter().map(|(_, p)| p.cmx).collect();
    store
        .append(&IndexedBlock {
            height,
            hash,
            parent: tip.hash,
            start_position: tip.position,
            end_position: tip.position + count as u64,
            coinbase_actions: 0,
            payments,
            commitments,
        })
        .unwrap();
}

/// Times one open-segment publication: build with final payloads reused, then both
/// tables' preparation.
fn publish(store: &mut Store, witnesses: &mut Witnesses, sealer: &mut seal::Sealer) -> String {
    let started = Instant::now();
    for shard in sealer.advance(store).unwrap() {
        store.seal(&shard).unwrap();
    }
    let advanced = started.elapsed();
    let tip = store.tip().unwrap();
    let (start, segment) = sealer.open().unwrap();
    let projection = segment.projection.clone();
    let started = Instant::now();
    let built = open(store, witnesses, (start, &projection), &tip, 0, &|| false).unwrap();
    let build = started.elapsed();
    let started = Instant::now();
    let entries = built.tables.directory.validate().unwrap();
    let (directory, _) =
        zakura_pir_query_server::receiver::server::Server::directory(built.tables.directory)
            .unwrap();
    let manifest = directory.session().manifest.clone();
    zakura_pir_query_server::receiver::server::Server::companion(
        built.tables.companion,
        &manifest,
        &entries,
    )
    .unwrap();
    let prepare = started.elapsed();
    format!("advance={advanced:?} build={build:?} prepare={prepare:?}")
}

/// Open-segment publication latency at block cadence near the seal target, and replay
/// after a restart: `cargo test -p receiver-indexer --profile release-fast
/// open_publication_latency -- --ignored --nocapture`.
#[test]
#[ignore = "a measurement, minutes of setup"]
fn open_publication_latency() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(
        dir.path().join("directory.sqlite"),
        Config {
            genesis: [1; 32],
            start_height: 100,
            start_parent: [2; 32],
            start_position: 0,
        },
    )
    .unwrap();
    let (blocks, per) = (100, 3_172);
    for block in 0..blocks {
        append(&mut store, per, block * per);
    }
    let (mut witnesses, mut sealer) = (Witnesses::default(), seal::Sealer::new(Policy::default()));
    println!(
        "first: {}",
        publish(&mut store, &mut witnesses, &mut sealer)
    );
    append(&mut store, 100, blocks * per);
    println!(
        "next block: {}",
        publish(&mut store, &mut witnesses, &mut sealer)
    );
    let (mut witnesses, mut sealer) = (Witnesses::default(), seal::Sealer::new(Policy::default()));
    println!(
        "restart: {}",
        publish(&mut store, &mut witnesses, &mut sealer)
    );
    println!("payments={}", store.counts().unwrap().0);
}
