//! A node behind the served History endpoint is not a fork: publication,
//! epoch and journal stay; a block it reports differently, or another network,
//! still withdraws before any rewind.
use super::*;

/// Publishes the fixture at height four and returns its authority.
async fn served_at_four(follow: &Follow) -> Authority {
    let authority = follow.authority(Default::default());
    follow.ingest(&authority).await;
    publish_follow(&follow.config, &follow.client, &follow.store, &authority)
        .await
        .unwrap();
    assert_eq!(authority.0.active.read().unwrap().publication.height, 4);
    authority
}

fn active(follow: &Follow) -> Vec<u8> {
    std::fs::read(follow.config.publication_root.join("active.json")).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_node_or_outage_keeps_the_served_endpoint_and_a_divergent_block_withdraws() {
    let follow = follow_fixture().await;
    let authority = served_at_four(&follow).await;
    let (notify, _) = tokio::sync::watch::channel(0);
    let mut writer = HistoryIngestion::fixture(
        &follow.config,
        &follow.client,
        &follow.store,
        &authority,
        &notify,
    )
    .await;
    let served = active(&follow);
    let epoch = authority.0.epoch.load(Ordering::Acquire);
    let canonical = follow.chain.read().unwrap().clone();

    // The node falls back to height two, below the served height four, with
    // the same blocks: nothing is withdrawn, rewound or re-fetched.
    follow.chain.write().unwrap().truncate(3);
    for _ in 0..2 {
        writer.pass().await.unwrap();
    }
    assert!(!authority.0.withdrawn.load(Ordering::Acquire));
    assert_eq!(authority.0.epoch.load(Ordering::Acquire), epoch);
    assert_eq!(active(&follow), served);
    assert_eq!(follow.store.lock().await.covered_through(), Some(4));

    // Caught up again, the same endpoint stays current without new work.
    *follow.chain.write().unwrap() = canonical.clone();
    let served_blocks = follow.node.blocks_served.load(Ordering::Relaxed);
    writer.pass().await.unwrap();
    assert!(!authority.0.withdrawn.load(Ordering::Acquire));
    assert_eq!(authority.0.epoch.load(Ordering::Acquire), epoch);
    assert_eq!(
        follow.node.blocks_served.load(Ordering::Relaxed),
        served_blocks
    );

    // Still lagging, the node now reports height three differently: that
    // committed block is evidence of a fork below the served endpoint.
    {
        let mut chain = follow.chain.write().unwrap();
        chain.truncate(3);
        extend(&mut chain, 1, 80);
    }
    writer.pass().await.unwrap();
    assert!(authority.0.withdrawn.load(Ordering::Acquire));
    assert!(authority.0.epoch.load(Ordering::Acquire) > epoch);
    let replaced = hash(&follow.chain.read().unwrap()[3]);
    let store = follow.store.lock().await;
    assert_eq!(store.covered_through(), Some(3));
    assert_eq!(store.block_at(3).unwrap().block_hash, replaced);
    drop(store);

    // A source outage fails the pass without advancing or withdrawing more.
    // The mock node answers no request while its chain is empty.
    let epoch = authority.0.epoch.load(Ordering::Acquire);
    let reported = std::mem::take(&mut *follow.chain.write().unwrap());
    assert!(writer.pass().await.is_err());
    assert!(writer.pass().await.is_err());
    assert_eq!(authority.0.epoch.load(Ordering::Acquire), epoch);
    assert_eq!(follow.store.lock().await.covered_through(), Some(3));
    *follow.chain.write().unwrap() = reported;
    writer.pass().await.unwrap();
    assert_eq!(authority.0.epoch.load(Ordering::Acquire), epoch);
    follow.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_on_another_network_withdraws_and_leaves_the_journal() {
    let follow = follow_fixture().await;
    let authority = served_at_four(&follow).await;
    let (notify, _) = tokio::sync::watch::channel(0);
    let mut writer = HistoryIngestion::fixture(
        &follow.config,
        &follow.client,
        &follow.store,
        &authority,
        &notify,
    )
    .await;
    let journal = std::fs::read(follow.config.data_dir.join("blocks.bin")).unwrap();
    let served_blocks = follow.node.blocks_served.load(Ordering::Relaxed);

    // A shorter chain with another genesis: lag alone must not hide it.
    {
        let mut chain = follow.chain.write().unwrap();
        chain.clear();
        extend(&mut chain, 3, 90);
    }
    let refused = writer.pass().await.unwrap_err().to_string();
    assert!(refused.contains("genesis"), "{refused}");
    assert!(authority.0.withdrawn.load(Ordering::Acquire));
    assert_eq!(follow.store.lock().await.covered_through(), Some(4));
    assert_eq!(
        std::fs::read(follow.config.data_dir.join("blocks.bin")).unwrap(),
        journal
    );
    assert_eq!(
        follow.node.blocks_served.load(Ordering::Relaxed),
        served_blocks
    );
    follow.server.abort();
}
