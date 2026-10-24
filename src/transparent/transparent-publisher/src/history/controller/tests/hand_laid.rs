//! History under root ingestion over journals this binary did not write, and
//! prepared candidates that a reorg reconciled by the root ingestion contract
//! orphans before activation.
use super::*;
use std::collections::BTreeMap;
use transparent_filter::ScriptBytes;
use transparent_shared::journal::display_journal;
use zakura_pir_protocol::transparent::events::{ReceiveEvent, SpendEvent, TransparentEvent, Txid};

fn receive(height: u64, tag: u8, script: &[u8]) -> (ScriptBytes, TransparentEvent) {
    let event = TransparentEvent::Receive(ReceiveEvent {
        metadata: None,
        height: height as u32,
        txid: Txid([tag; 32]),
        transaction_index: 0,
        output_index: u32::from(tag),
        value: 5_000,
        coinbase: false,
    });
    (ScriptBytes::new(script.to_vec()), event)
}

fn spend(height: u64, tag: u8, script: &[u8]) -> (ScriptBytes, TransparentEvent) {
    let event = TransparentEvent::Spend(SpendEvent {
        metadata: None,
        height: height as u32,
        spending_txid: Txid([tag; 32]),
        transaction_index: 1,
        input_index: 0,
        spent_txid: Txid([1; 32]),
        spent_output_index: 1,
    });
    (ScriptBytes::new(script.to_vec()), event)
}

/// Every file under `root` with its bytes, except the writer's lock file.
fn tree(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut found = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if !path.ends_with("writer.lock") {
                found.insert(path.clone(), std::fs::read(&path).unwrap());
            }
        }
    }
    found
}

/// A version-3 journal laid out by hand from the documented format: blocks
/// `0..=tip` of `chain`, with events at heights one and three and an
/// oversized script held only in height two's display sidecar.
fn lay_version_three(
    dir: &Path,
    chain: &[Vec<u8>],
    tip: u64,
) -> Vec<Vec<(ScriptBytes, TransparentEvent)>> {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(
        dir.join("meta.json"),
        format!(
            r#"{{"version":3,"genesis_hash":"{}","start_height":0}}"#,
            hash(&chain[0]).to_display_hex()
        ),
    )
    .unwrap();
    let (mut events, mut blocks, mut laid) = (vec![], vec![], vec![]);
    for height in 0..=tip {
        let block_hash = hash(&chain[height as usize]);
        let in_journal = match height {
            1 => vec![
                receive(1, 1, &[0x76, 0xa9, 0x14, 1]),
                receive(1, 2, &[0x6a, 0x01, 0xff]),
            ],
            3 => vec![spend(3, 3, &[0x76, 0xa9, 0x14, 1])],
            _ => vec![],
        };
        let offset = events.len() as u64;
        for (script, event) in &in_journal {
            let raw = event.to_bytes().unwrap();
            events.extend((script.as_slice().len() as u16).to_le_bytes());
            events.extend(script.as_slice());
            events.extend((raw.len() as u16).to_le_bytes());
            events.extend(raw);
        }
        blocks.extend(block_hash.internal_bytes());
        blocks.extend(offset.to_le_bytes());
        blocks.extend((in_journal.len() as u64).to_le_bytes());
        let mut all = in_journal;
        if height == 2 {
            let oversized = receive(2, 9, &[0x51; 20_000]);
            display_journal::write_envelope(
                &display_journal::V1,
                dir,
                block_hash,
                std::iter::empty(),
                std::slice::from_ref(&oversized),
            )
            .unwrap();
            all.push(oversized);
        }
        laid.push(all);
    }
    let checkpoint: Vec<u8> = [events.len() as u64, blocks.len() as u64]
        .into_iter()
        .flat_map(u64::to_le_bytes)
        .collect();
    std::fs::write(dir.join("events.bin"), events).unwrap();
    std::fs::write(dir.join("blocks.bin"), blocks).unwrap();
    std::fs::write(dir.join("checkpoint.bin"), checkpoint).unwrap();
    laid
}

/// An existing version-3 journal is opened in place by the root-scheduled
/// writer: an unchanged tip extracts and rewrites nothing, an extension and a
/// same-height reorg touch only blocks above the kept ones, and a restart
/// still reads the original records and sidecar under the original codec.
#[tokio::test(flavor = "multi_thread")]
async fn a_hand_laid_version_three_journal_resumes_in_place_under_root_ingestion() {
    let follow = follow_fixture().await;
    let dir = follow._root.path().join("hand-laid-v3");
    let laid = lay_version_three(&dir, &follow.chain.read().unwrap(), 4);
    let genesis = hash(&follow.chain.read().unwrap()[0]).to_display_hex();
    let before = tree(&dir);
    let journal = EventStore::open(&dir, &genesis, 0).unwrap();
    assert_eq!(journal.version(), 3);
    assert!(
        EventStore::open(&dir, &genesis, 0).is_err(),
        "a second writer is refused"
    );
    let store = Arc::new(tokio::sync::Mutex::new(journal));
    let authority = follow.authority(Default::default());
    let (notify, _) = tokio::sync::watch::channel(0);
    let mut writer =
        HistoryIngestion::fixture(&follow.config, &follow.client, &store, &authority, &notify)
            .await;

    // Unchanged tip: current without extraction or any byte changing.
    for _ in 0..2 {
        writer.pass().await.unwrap();
    }
    assert_eq!(follow.node.blocks_served.load(Ordering::Relaxed), 0);
    assert!(writer.current.load(Ordering::Acquire));
    assert_eq!(tree(&dir), before, "no committed byte was rewritten");

    // A bounded extension appends after the original bytes.
    {
        let mut chain = follow.chain.write().unwrap();
        extend(&mut chain, 2, 40);
    }
    writer.pass().await.unwrap();
    assert_eq!(follow.node.blocks_served.load(Ordering::Relaxed), 2);
    assert_eq!(store.lock().await.covered_through(), Some(6));
    for name in ["events.bin", "blocks.bin"] {
        assert!(std::fs::read(dir.join(name))
            .unwrap()
            .starts_with(&before[&dir.join(name)]));
    }

    // A same-height reorg at the tip replaces only that block.
    let epoch = authority.0.epoch.load(Ordering::Acquire);
    {
        let mut chain = follow.chain.write().unwrap();
        chain.truncate(6);
        extend(&mut chain, 1, 60);
    }
    writer.pass().await.unwrap();
    assert!(
        authority.0.epoch.load(Ordering::Acquire) > epoch,
        "withdrawn first"
    );
    assert_eq!(follow.node.blocks_served.load(Ordering::Relaxed), 3);
    let replaced = hash(&follow.chain.read().unwrap()[6]);
    assert_eq!(store.lock().await.block_at(6).unwrap().block_hash, replaced);
    drop(writer);
    drop(store);

    // A restart reads the hand-laid records and sidecar under version 3.
    let reopened = EventStore::open(&dir, &genesis, 0).unwrap();
    assert_eq!(reopened.version(), 3);
    assert_eq!(reopened.covered_through(), Some(6));
    for (height, events) in laid.iter().enumerate() {
        assert_eq!(&reopened.events_at(height as u64).unwrap().unwrap(), events);
    }
    for (path, bytes) in &before {
        if path.ends_with("meta.json") || path.starts_with(dir.join("display-v1")) {
            assert_eq!(&std::fs::read(path).unwrap(), bytes, "{}", path.display());
        }
    }
    drop(reopened);

    // A journal for another chain is refused without any change.
    let refused = tree(&dir);
    assert!(EventStore::open(&dir, &"00".repeat(32), 0).is_err());
    assert!(EventStore::open(&dir, &genesis, 1).is_err());
    assert_eq!(tree(&dir), refused);
    follow.server.abort();
}

/// A candidate prepared from committed History blocks that a reorg then
/// replaces is refused at activation after root reconciliation withdraws the
/// orphaned work; the still-canonical predecessor keeps serving, and the next
/// cycle activates the replacement.
#[tokio::test(flavor = "multi_thread")]
async fn a_candidate_prepared_before_a_reconciled_reorg_never_activates() {
    let follow = follow_fixture().await;
    let config = &follow.config;
    let authority = follow.authority(Default::default());
    follow.ingest(&authority).await;
    let Input::Work(work) = acquire_history(config, &follow.client, &follow.store, &authority)
        .await
        .unwrap()
    else {
        panic!("new durable work expected");
    };
    let Input::Work(ready) = prepare_follow(config, &follow.client, &authority, work)
        .await
        .unwrap()
    else {
        panic!("prepared candidate expected");
    };
    // The candidate's tip block is replaced at the same height while ready.
    {
        let mut chain = follow.chain.write().unwrap();
        chain.truncate(4);
        extend(&mut chain, 1, 70);
    }
    follow.ingest(&authority).await;
    let refused = ready.commit().await.unwrap_err().to_string();
    assert!(
        refused.contains("invalidated before activation"),
        "{refused}"
    );
    assert!(!config.publication_root.join("active.json").exists());
    assert!(!authority.0.withdrawn.load(Ordering::Acquire));
    assert_eq!(authority.0.active.read().unwrap().publication.height, 2);

    publish_follow(config, &follow.client, &follow.store, &authority)
        .await
        .unwrap();
    let active = authority.0.active.read().unwrap().publication.clone();
    assert_eq!(active.height, 4);
    assert_eq!(
        active.hash,
        hash(&follow.chain.read().unwrap()[4]).to_display_hex()
    );
    follow.server.abort();
}
