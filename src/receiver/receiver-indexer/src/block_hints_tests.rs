//! Real producer/store/HTTP checks, with an independent list of expected recipients.
use receiver_pir_server::{blocks::BlockWindow, Publications};
use receiver_store::{Config, IndexedBlock, Store};
use zakura_pir_publisher::receiver::blocks::{
    Head, HeightSet, Range, MAX_RANGE_BLOCKS, MAX_RANGE_BYTES, MAX_RECEIVERS, RANGE_HEADER,
};

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

/// The window of `store`'s hints from `from` to its tip under `revision`.
fn window(store: &Store, from: u32, revision: u64) -> BlockWindow {
    let tip = store.tip().unwrap().height;
    let hints = store.hints(from, tip, usize::MAX).unwrap();
    BlockWindow::new(
        [1; 32],
        revision,
        hints.first,
        hints.parent,
        hints.hashes,
        hints.sets,
    )
    .unwrap()
}

/// The heights at which `range` matches the fixture's receiver.
fn paid(range: &Range) -> Vec<u32> {
    range
        .matches(&[1; 32], &[crate::probe::common::receiver()])
        .into_iter()
        .map(|(height, _)| height)
        .collect()
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
    let hints = window(&store, 102, 0);
    assert_eq!(hints.head().first, 102);
    assert_eq!(hints.range(0, 100, 1).unwrap_err().as_u16(), 410);
    let range = Range::decode(&hints.range(0, 102, 2).unwrap()).unwrap();
    range.check(hints.head(), 102, 2).unwrap();
    assert_eq!((range.parent(), range.end().hash), ([101; 32], [103; 32]));
    assert_eq!(paid(&range), [103]);
    let tip = store.tip().unwrap();
    assert_eq!(store.records(100, tip.height).unwrap().len(), 2);
    assert_eq!(store.payment_positions(0, tip.position).unwrap().len(), 2);
    drop(store);
    let mut store = index(&path);
    assert_eq!(
        window(&store, 102, 0).range(0, 102, 2).unwrap(),
        range.encode().unwrap()
    );
    store.rewind(102, [102; 32]).unwrap();
    append(&mut store, 103, [99; 32], false);
    let fork = window(&store, 102, 0);
    assert_ne!(fork.head().tip, hints.head().tip);
    let new = Range::decode(&fork.range(0, 102, 2).unwrap()).unwrap();
    assert!(new.check(hints.head(), 102, 2).is_err());
    assert!(paid(&new).is_empty());
    assert_eq!(store.records(100, 103).unwrap().len(), 1);
    store.raise_history_revision(1).unwrap();
    assert_eq!(store.history_revision().unwrap(), 1);
    drop(store);
    let store = index(&path);
    assert_eq!(store.history_revision().unwrap(), 1);
    let revised = window(&store, 102, 1);
    assert_eq!(revised.range(0, 102, 2).unwrap_err().as_u16(), 410);
    assert_eq!(
        Range::decode(&revised.range(1, 102, 2).unwrap())
            .unwrap()
            .end(),
        new.end()
    );
}

/// Real HTTP serves hints before any PIR session exists and revokes them with the chain fence.
#[tokio::test]
async fn hints_are_independent_of_pir_and_refuse_pruned_or_stale_requests() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = index(&dir.path().join("directory.sqlite"));
    append(&mut store, 100, [100; 32], true);
    append(&mut store, 101, [101; 32], false);
    let publications = Publications::default();
    assert!(publications.advance_history_revision(7));
    let epoch = publications.epoch();
    assert!(publications.publish_filters(window(&store, 100, 7), epoch));
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
    assert_eq!((head.revision, head.first), (7, 100));
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
        .get(format!(
            "{origin}/v1/receiver/block-filters/7/100/{MAX_RANGE_BLOCKS}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    let bytes = response.bytes().await.unwrap();
    let range = Range::decode(&bytes).unwrap();
    range.check(&head, 100, MAX_RANGE_BLOCKS).unwrap();
    assert_eq!(paid(&range), [100]);
    let too_many = format!("block-filters/7/100/{}", MAX_RANGE_BLOCKS + 1);
    for (path, status) in [
        ("block-filters/7/99/1", 410),
        ("block-filters/6/100/1", 410),
        ("block-filters/7/102/1", 503),
        ("block-filters/7/100/0", 400),
        (too_many.as_str(), 400),
        ("block-filters/range/7/100/1", 404),
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
    assert!(!publications.publish_filters(window(&store, 100, 7), epoch));
    task.abort();
}

/// A full retained empty-chain window is one header-only range and stays bounded.
#[test]
fn full_retention_window_is_small_and_configurable() {
    let mut store = index(std::path::Path::new(":memory:"));
    for height in 100..100 + receiver_store::blocks::MAX_FILTER_BLOCKS {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_le_bytes());
        append(&mut store, height, hash, false);
    }
    let started = std::time::Instant::now();
    let full = window(&store, 100, 0);
    let range = full.range(0, 100, MAX_RANGE_BLOCKS).unwrap();
    assert_eq!(range.len(), RANGE_HEADER);
    assert_eq!(Range::decode(&range).unwrap().end(), full.head().tip);
    let shortened = window(&store, full.head().tip.height, 0);
    assert_eq!(shortened.head().first, full.head().tip.height);
    assert_eq!(store.checkpoint(100).unwrap().height, 100);
    eprintln!(
        "rolling filter fixture: {} blocks in one {}-byte range, {:?}",
        receiver_store::blocks::MAX_FILTER_BLOCKS,
        range.len(),
        started.elapsed()
    );
}

/// The byte bound still holds whatever the block count allows: hints whose older
/// blocks would take them past the bound leave those out.
#[test]
fn the_byte_bound_still_limits_the_window() {
    let mut store = index(std::path::Path::new(":memory:"));
    for height in 100..110 {
        append(&mut store, height, [height as u8; 32], true);
    }
    let all = store.hints(100, 109, usize::MAX).unwrap();
    let bytes: usize = all.sets[7..].iter().map(|(_, set)| set.len()).sum();
    let capped = store.hints(100, 109, bytes).unwrap();
    assert_eq!(capped.first, 107);
    assert_eq!(capped.parent, [106; 32]);
    assert_eq!(capped.hashes, all.hashes[7..]);
    assert_eq!(capped.sets, all.sets[7..]);
    assert_eq!(store.hints(100, 109, bytes - 1).unwrap().first, 108);
}

/// A dense window larger than one response returns a nonempty prefix within the byte cap
/// that continues by parent hash.
#[test]
fn a_dense_range_stops_at_the_byte_cap_and_continues() {
    use sha2::{Digest, Sha256};
    let hash = |height: u32| -> [u8; 32] { Sha256::digest(height.to_le_bytes()).into() };
    let receivers: Vec<[u8; 43]> = (0..MAX_RECEIVERS)
        .map(|i| {
            let mut receiver = [0; 43];
            receiver[..4].copy_from_slice(&i.to_le_bytes());
            receiver
        })
        .collect();
    // About 190 KB per block, so 40 blocks are well past one 4 MiB response.
    let sets: Vec<_> = (100..140)
        .map(|height| {
            let set = HeightSet::build(&[1; 32], height, &receivers)
                .unwrap()
                .unwrap();
            (height, set.bytes().to_vec())
        })
        .collect();
    let window = BlockWindow::new(
        [1; 32],
        0,
        100,
        hash(99),
        (100..140).map(hash).collect(),
        sets.clone(),
    )
    .unwrap();
    let (mut from, mut fetched, mut previous) = (100, 0, None::<Range>);
    let mut received = Vec::new();
    while from < 140 {
        let bytes = window.range(0, from, MAX_RANGE_BLOCKS).unwrap();
        assert!(bytes.len() <= MAX_RANGE_BYTES, "{}", bytes.len());
        let range = Range::decode(&bytes).unwrap();
        range.check(window.head(), from, MAX_RANGE_BLOCKS).unwrap();
        assert!(previous.as_ref().is_none_or(|p| range.follows(p)));
        assert_eq!(range.end().hash, hash(range.end().height));
        received.extend(
            Range::raw_sets(&bytes)
                .unwrap()
                .into_iter()
                .map(|(height, set)| (height, set.to_vec())),
        );
        fetched += bytes.len() as u64;
        from = range.end().height + 1;
        previous = Some(range);
    }
    assert!(fetched > MAX_RANGE_BYTES as u64);
    assert_eq!(received, sets);
}
