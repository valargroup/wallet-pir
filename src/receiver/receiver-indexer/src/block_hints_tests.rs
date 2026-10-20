//! Real producer/store/HTTP checks, with an independent list of expected recipients.
use receiver_directory::blocks::{Batch, Head};
use receiver_pir_server::{blocks::BlockWindow, Publications};
use receiver_store::{Config, IndexedBlock, Store};

/// An isolated chain whose first block contains one fixed authenticated-format record.
fn index(path: &std::path::Path) -> Store {
    Store::open(
        path,
        Config {
            genesis: [1; 32],
            start_height: 100,
            start_parent: [2; 32],
            start_position: 0,
        },
    )
    .unwrap()
}

/// Extends the fixture with either its independently pinned receiver or an empty block.
fn append(store: &mut Store, height: u32, hash: [u8; 32], paid: bool) {
    let tip = store.tip().unwrap();
    let mut payment = crate::probe::common::record(0, 1).payment;
    payment.height = height;
    payment.block_hash = hash;
    payment.position = tip.position;
    payment.txid = [height as u8; 32];
    let block = IndexedBlock {
        height,
        hash,
        parent: tip.hash,
        start_position: tip.position,
        end_position: tip.position + u64::from(paid),
        coinbase_actions: 0,
        payments: if paid {
            vec![(crate::probe::common::receiver(), payment.clone())]
        } else {
            vec![]
        },
        commitments: if paid { vec![payment.cmx] } else { vec![] },
    };
    crate::verify::append(store, &block).unwrap();
}

/// Pruning only shortens available hints. Restart, a fork and corrections preserve PIR history.
#[test]
fn rolling_hints_preserve_full_history_across_restart_and_reorg() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("directory.sqlite");
    let mut store = index(&path);
    for height in 100..104 {
        append(
            &mut store,
            height,
            [height as u8; 32],
            height == 100 || height == 103,
        );
    }
    let suffix = store.block_filters(2, 0).unwrap();
    let head = BlockWindow::new([1; 32], 0, suffix).unwrap();
    assert_eq!(head.head().first, 102);
    assert_eq!(head.batch(0, 100, 1).unwrap_err().as_u16(), 410);
    let bytes = head.batch(0, 102, 2).unwrap();
    let batch = Batch::decode(&bytes).unwrap();
    batch.check(head.head(), 102, 2).unwrap();
    let receivers = [crate::probe::common::receiver()];
    assert_eq!(batch.matches(0, &receivers).unwrap(), [false]);
    assert_eq!(batch.matches(1, &receivers).unwrap(), [true]);
    let before = store.snapshot(8, 0).unwrap();
    assert_eq!(before.manifest.records, 2);
    assert_eq!(
        store
            .payment_positions(before.manifest.start_position, before.manifest.end_position)
            .unwrap()
            .len(),
        2
    );
    drop(store);
    let mut store = index(&path);
    assert_eq!(store.block_filters(2, 0).unwrap(), batch.filters());
    store.rewind(102, [102; 32]).unwrap();
    append(&mut store, 103, [99; 32], false);
    let fork = BlockWindow::new([1; 32], 0, store.block_filters(2, 0).unwrap()).unwrap();
    assert_ne!(fork.head().tip, head.head().tip);
    let new = Batch::decode(&fork.batch(0, 102, 2).unwrap()).unwrap();
    assert!(new.check(head.head(), 102, 2).is_err());
    assert_eq!(new.matches(1, &receivers).unwrap(), [false]);
    assert_eq!(store.snapshot(8, 0).unwrap().manifest.records, 1);
    store.block_filters(2, 1).unwrap();
    let corrected = store.snapshot(8, 1).unwrap();
    assert_eq!(corrected.manifest.history_revision, 1);
    assert!(store.snapshot(8, 0).is_err());
    drop(store);
    let mut store = index(&path);
    assert!(store.block_filters(2, 0).is_err());
    assert_eq!(store.block_filters(2, 1).unwrap(), new.filters());
}

/// Real HTTP serves hints before any PIR session exists and revokes them with the chain fence.
#[tokio::test]
async fn hints_are_independent_of_pir_and_refuse_pruned_or_stale_requests() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = index(&dir.path().join("directory.sqlite"));
    append(&mut store, 100, [100; 32], true);
    append(&mut store, 101, [101; 32], false);
    let publications = Publications::default();
    let window = BlockWindow::new([1; 32], 7, store.block_filters(2, 7).unwrap()).unwrap();
    assert!(publications.advance_history_revision(7));
    let epoch = publications.epoch();
    assert!(publications.publish_filters(window, epoch));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let router = crate::router::router_with_publications(publications.clone());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let http = reqwest::Client::new();
    let head: Head = http
        .get(format!("{origin}/v1/receiver/block-filters/head"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(head.revision, 7);
    assert_eq!(
        http.get(format!("{origin}/v1/receiver/inventory"))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        503
    );
    let response = http
        .get(format!("{origin}/v1/receiver/block-filters/7/100/2"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    let bytes = response.bytes().await.unwrap();
    let batch = Batch::decode(&bytes).unwrap();
    batch.check(&head, 100, 2).unwrap();
    assert_eq!(
        batch
            .matches(0, &[crate::probe::common::receiver()])
            .unwrap(),
        [true]
    );
    for (path, status) in [
        ("block-filters/7/99/1", 410),
        ("block-filters/6/100/1", 410),
        ("block-filters/7/102/1", 503),
        ("block-filters/7/100/0", 400),
        ("block-filters/7/100/257", 400),
        ("filters/old-session", 404),
    ] {
        let response = http
            .get(format!("{origin}/v1/receiver/{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status, "{path}");
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
    // A chain guard that inspected this anchor revokes hints even without a PIR session.
    assert!(publications.revoke_serving(epoch, (101, [101; 32])));
    assert_eq!(
        http.get(format!("{origin}/v1/receiver/block-filters/head"))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        503
    );
    let stale = BlockWindow::new([1; 32], 7, store.block_filters(2, 7).unwrap()).unwrap();
    assert!(!publications.publish_filters(stale, epoch));
    task.abort();
}

/// A full retained empty-chain window remains bounded without truncating chain history.
#[test]
fn full_retention_window_is_small_and_configurable() {
    let mut store = index(std::path::Path::new(":memory:"));
    for height in 100..100 + receiver_store::blocks::MAX_FILTER_BLOCKS {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_le_bytes());
        append(&mut store, height, hash, false);
    }
    let started = std::time::Instant::now();
    let window = BlockWindow::new(
        [1; 32],
        0,
        store
            .block_filters(receiver_store::blocks::MAX_FILTER_BLOCKS, 0)
            .unwrap(),
    )
    .unwrap();
    let mut bytes = 0;
    for from in (100..=window.head().tip.height).step_by(256) {
        bytes += window.batch(0, from, 256).unwrap().len();
    }
    assert!(bytes < 1024 * 1024);
    assert_eq!(window.head().first, 100);
    let shortened = BlockWindow::new([1; 32], 0, store.block_filters(1, 0).unwrap()).unwrap();
    assert_eq!(shortened.head().first, window.head().tip.height);
    assert_eq!(store.checkpoint(100).unwrap().height, 100);
    eprintln!(
        "rolling filter fixture: {} blocks, {bytes} bytes, {:?}",
        receiver_store::blocks::MAX_FILTER_BLOCKS,
        started.elapsed()
    );
}
