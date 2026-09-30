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
            script: vec![0x6a; 9000],
        }],
    };
    let mut store = EventStore::open(&journal, transparent_filter::MAINNET_GENESIS_DISPLAY, 1)?;
    store.append_block_with_display(
        1,
        BlockHash::from_internal_bytes([3; 32]),
        &[],
        std::slice::from_ref(&record),
    )?;
    store.commit()?;
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
    let mut map = publish(&options, &store, BlockHash::from_internal_bytes([2; 32]))?;
    let old = original.join(&map.shards[0].manifest_digest);
    let mut manifest: transparent_shard::ShardManifest =
        serde_json::from_slice(&std::fs::read(old.join("manifest.json"))?)?;
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
    let state = ServiceState::build(set, ServiceConfig::default())?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router(state)).await });
    let mut client = txquery::PrivateClient::new(format!("http://{address}"));
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
