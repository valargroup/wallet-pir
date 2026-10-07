//! Synthetic geometry qualification complements the confirmed-vector demo.
#[path = "../examples/support/txquery.rs"]
mod txquery;
use clap::Parser;
use sha2::{Digest, Sha256};
use transparent_events::{FeeState, TransactionMetadata, Txid};
use transparent_filter::BlockHash;
use transparent_filter_server::{
    events::EventStore,
    publication::{publish, PublishOptions},
};
use transparent_shard::{
    layout::RECENT_4K,
    txid::{DisplayOutput, TransparentDisplayRecord},
};
use transparent_shard_server::{
    service::{router, ReadinessMode, ServiceConfig, ServiceState},
    shardset::{ShardSet, DEFAULT_RETAIN_REVISIONS},
};

#[tokio::test]
async fn native_display_queries_bind_and_decode_every_segment() -> Result<(), txquery::Error> {
    let temp = tempfile::tempdir()?;
    let journal = temp.path().join("journal");
    let original = temp.path().join("one-segment");
    let published = temp.path().join("two-segments");
    let record = TransparentDisplayRecord {
        txid: Txid([7; 32]),
        coinbase: false,
        metadata: TransactionMetadata {
            fee: FeeState::Unknown,
            transparent_input_count: 1,
            has_shielded_components: true,
        },
        outputs: vec![DisplayOutput {
            value: 9,
            script: vec![0x51; 65_536],
        }],
    };
    let mut store = EventStore::open(&journal, transparent_filter::MAINNET_GENESIS_DISPLAY, 1)?;
    let options = PublishOptions::parse_from([
        "publish",
        "--output",
        original.to_str().unwrap(),
        "--zakura-cookie",
        "/unused",
        "--recent-geometry",
        "recent-4k",
        "--txid-display",
    ]);
    store.append_block(1, BlockHash::from_internal_bytes([3; 32]), &[])?;
    store.commit()?;
    assert!(publish(&options, &store, BlockHash::from_internal_bytes([2; 32])).is_err());
    store.rollback_to(None)?;
    let events = vec![(
        transparent_filter::ScriptBytes::new(record.outputs[0].script.clone()),
        transparent_events::TransparentEvent::Receive(transparent_events::ReceiveEvent {
            height: 1,
            txid: record.txid,
            transaction_index: 0,
            output_index: 0,
            value: 9,
            coinbase: false,
            metadata: Some(record.metadata),
        }),
    )];
    store.append_block_with_display(
        1,
        BlockHash::from_internal_bytes([3; 32]),
        &events,
        std::slice::from_ref(&record),
    )?;
    store.commit()?;
    let mut map = publish(&options, &store, BlockHash::from_internal_bytes([2; 32]))?;
    let old = original.join(&map.shards[0].manifest_digest);
    let mut manifest: transparent_shard::ShardManifest =
        serde_json::from_slice(&std::fs::read(old.join("manifest.json"))?)?;
    assert_eq!(manifest.occupancy.excluded_scripts, 1);
    let zeros = vec![0; RECENT_4K.directory_rows as usize * 4096];
    let extra = transparent_shard::TableGeometry {
        rows: RECENT_4K.directory_rows,
        row_bytes: 4096,
        sha256: hex::encode(Sha256::digest(&zeros)),
    };
    let tables = manifest.txid_display.as_mut().unwrap();
    tables.directory_segments.push(extra.clone());
    tables.page_segments.push(extra);
    // A new temporary publication identity; never rewrite the original revision.
    let dir = published.join(manifest.digest());
    std::fs::create_dir_all(&dir)?;
    for name in [
        "filter.bin",
        "directory.0.bin",
        "pages.0.bin",
        "txdirectory.0.bin",
        "txpages.0.bin",
    ] {
        std::fs::copy(old.join(name), dir.join(name))?;
    }
    for table in ["txdirectory", "txpages"] {
        std::fs::write(dir.join(format!("{table}.1.bin")), &zeros)?;
    }
    std::fs::write(dir.join("manifest.json"), serde_json::to_vec(&manifest)?)?;
    map.shards[0].manifest_digest = manifest.digest();
    map.shards[0].txid_segments = Some([2, 2]);
    std::fs::write(published.join("shards.json"), serde_json::to_vec(&map)?)?;
    let set = ShardSet::open(&published, DEFAULT_RETAIN_REVISIONS)?;
    let denied = ServiceConfig {
        readiness: ReadinessMode::Warm,
        cache_bytes: 1,
        ..Default::default()
    };
    assert!(ServiceState::build(set, denied).is_err()); // Display tables count toward warm admission.
    let set = ShardSet::open(&published, DEFAULT_RETAIN_REVISIONS)?;
    assert_eq!(set.warm_targets().len(), 6);
    let state = ServiceState::build(
        set,
        ServiceConfig {
            readiness: ReadinessMode::Warm,
            ..Default::default()
        },
    )?;
    state.spawn_prewarm().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router(state)).await });
    let mut client = txquery::PrivateClient::new(format!("http://{address}"));
    assert!(client
        .http
        .get(format!("http://{address}/v1/ready"))
        .send()
        .await?
        .status()
        .is_success());
    let txquery::LookupResult::Found(actual) =
        client.lookup_mined(&map, record.txid, Some(1)).await?
    else {
        panic!("missing record")
    };
    assert_eq!(actual, record);
    assert!(matches!(
        client.lookup_mined(&map, Txid([99; 32]), Some(1)).await?,
        txquery::LookupResult::Absent
    ));
    assert!(matches!(
        client.lookup_mined(&map, record.txid, None).await?,
        txquery::LookupResult::PlacementUnknown
    ));
    let mut unsupported = map;
    unsupported.shards[0].txid_segments = None;
    assert!(matches!(
        client
            .lookup_mined(&unsupported, record.txid, Some(1))
            .await?,
        txquery::LookupResult::Unsupported
    ));
    assert!(
        client
            .paths
            .iter()
            .any(|p| p.ends_with("/setup/txdirectory/1"))
            && client.paths.iter().any(|p| p.ends_with("/setup/txpages/1"))
    );
    server.abort();
    Ok(())
}

/// A hard-linked candidate must take over every table source of a reused
/// revision. Display sources once kept pointing into the previous directory,
/// so collecting it broke the next cold build of `txdirectory`.
#[tokio::test]
async fn relinked_revision_rebuilds_display_tables_after_old_directory_is_removed(
) -> Result<(), txquery::Error> {
    use tower::ServiceExt;
    use transparent_shard_server::shardset::LoadOptions;
    let temp = tempfile::tempdir()?;
    let old = temp.path().join("old");
    let new = temp.path().join("new");
    let record = TransparentDisplayRecord {
        txid: Txid([5; 32]),
        coinbase: false,
        metadata: TransactionMetadata {
            fee: FeeState::Exact(10),
            transparent_input_count: 1,
            has_shielded_components: false,
        },
        outputs: vec![DisplayOutput {
            value: 4,
            script: vec![0x51; 25],
        }],
    };
    let mut store = EventStore::open(
        temp.path().join("journal"),
        transparent_filter::MAINNET_GENESIS_DISPLAY,
        1,
    )?;
    let events = vec![(
        transparent_filter::ScriptBytes::new(record.outputs[0].script.clone()),
        transparent_events::TransparentEvent::Receive(transparent_events::ReceiveEvent {
            height: 1,
            txid: record.txid,
            transaction_index: 0,
            output_index: 0,
            value: 4,
            coinbase: false,
            metadata: Some(record.metadata),
        }),
    )];
    store.append_block_with_display(
        1,
        BlockHash::from_internal_bytes([3; 32]),
        &events,
        std::slice::from_ref(&record),
    )?;
    store.commit()?;
    let options = PublishOptions::parse_from([
        "publish",
        "--output",
        old.to_str().unwrap(),
        "--zakura-cookie",
        "/unused",
        "--recent-geometry",
        "recent-4k",
        "--txid-display",
    ]);
    let map = publish(&options, &store, BlockHash::from_internal_bytes([2; 32]))?;
    let previous = ShardSet::open(&old, DEFAULT_RETAIN_REVISIONS)?;
    let digest = map.shards[0].manifest_digest.clone();
    std::fs::create_dir_all(new.join(&digest))?;
    for entry in std::fs::read_dir(old.join(&digest))? {
        let entry = entry?;
        std::fs::hard_link(entry.path(), new.join(&digest).join(entry.file_name()))?;
    }
    std::fs::copy(old.join("shards.json"), new.join("shards.json"))?;
    let set = ShardSet::open_reusing(
        &new,
        &LoadOptions::whole(DEFAULT_RETAIN_REVISIONS),
        Some(&previous),
    )?;
    drop(previous);
    std::fs::remove_dir_all(&old)?;
    let state = ServiceState::build(set, ServiceConfig::default())?;
    for table in ["txdirectory", "txpages", "directory", "pages"] {
        let response = router(state.clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/v1/shards/0/revisions/{digest}/setup/{table}/0"))
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        assert_eq!(
            response.status(),
            200,
            "{table} rebuilds from the new links"
        );
    }
    Ok(())
}
