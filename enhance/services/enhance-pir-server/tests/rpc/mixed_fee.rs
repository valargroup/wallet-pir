//! Exact fees of mixed transactions through the real producer path:
//! serialized blocks and transactions served by a JSON-RPC test double.

use crate::support::*;
use enhance_pir::ACTIVATION_HEIGHT;
use enhance_pir_server::fee::FeeError;
use enhance_pir_server::zakura::{Prevouts, ZakuraClient, ZakuraError};
use std::sync::{Arc, Mutex};
use zakura_chain::serialization::ZcashDeserialize;
use zakura_chain::transaction::Transaction;

struct Node {
    chain: SharedChain,
    client: ZakuraClient,
    _dir: tempfile::TempDir,
    _task: tokio::task::JoinHandle<()>,
}

async fn node() -> Node {
    let chain: SharedChain = Arc::new(Mutex::new(Chain::default()));
    let (url, task) = serve_chain(chain.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let client = ZakuraClient::from_cookie_file(url, cookie(dir.path())).unwrap();
    Node {
        chain,
        client,
        _dir: dir,
        _task: task,
    }
}

impl Node {
    /// Publishes `previous` as fetchable chain history outside any served block.
    fn history(&self, previous: &[&Transaction]) {
        let mut chain = self.chain.lock().unwrap();
        for tx in previous {
            chain.add_transaction(tx);
        }
    }

    async fn records(
        &self,
        height: u64,
        transactions: &[Transaction],
        prevouts: &Prevouts,
    ) -> Result<Vec<enhance_pir::EnhanceRecord>, ZakuraError> {
        self.chain
            .lock()
            .unwrap()
            .push_block(height, transactions, height as u8);
        self.client.block(height, prevouts).await.map(|b| b.records)
    }
}

fn fee_and_flags(record: &enhance_pir::EnhanceRecord) -> (Option<u64>, bool, bool) {
    // Decode through the wallet-facing parser, not the producer's types.
    let decoded = enhance_pir::EnhanceRecord::from_bytes(*record.as_bytes()).unwrap();
    let flags = decoded.as_bytes()[enhance_pir::types::RECORD_FLAGS_OFFSET];
    (
        decoded.metadata().fee_zatoshis(),
        flags & enhance_pir::types::FLAG_HAS_TRANSPARENT_INPUTS != 0,
        flags & enhance_pir::types::FLAG_HAS_TRANSPARENT_OUTPUTS != 0,
    )
}

const H: u64 = ACTIVATION_HEIGHT;

#[tokio::test]
async fn transparent_spends_into_ironwood_publish_the_whole_fee() {
    let node = node().await;
    let prevouts = Prevouts::new(1000);
    let a = funding(1, &[420_000]);
    let b = funding(2, &[200_000]);
    node.history(&[&a, &b]);
    let first = Mixed::new(vec![spend(&a, 0)], &[], -400_000).build();
    let second = Mixed::new(vec![spend(&b, 0)], &[], -180_000).build();
    let records = node.records(H, &[first, second], &prevouts).await.unwrap();
    assert_eq!(records.len(), 4);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(fee_and_flags(record), (Some(20_000), true, false));
        assert_fixture_action(record, i % 2);
    }
    let chain = node.chain.lock().unwrap();
    // Both previous transactions came from one batch.
    assert_eq!(chain.batches, 1);
    assert_eq!(chain.count("getrawtransaction"), 2);
    assert_eq!(prevouts.counts().fetched_transactions, 2);
}

#[tokio::test]
async fn multiple_inputs_with_transparent_change() {
    let node = node().await;
    let prevouts = Prevouts::new(1000);
    let a = funding(1, &[300_000, 7]);
    let b = funding(2, &[150_000]);
    node.history(&[&a, &b]);
    // 450,000 in; 100,000 transparent change; 330,000 into Ironwood.
    let tx = Mixed::new(vec![spend(&a, 0), spend(&b, 0)], &[100_000], -330_000).build();
    let records = node.records(H, &[tx], &prevouts).await.unwrap();
    for record in &records {
        assert_eq!(fee_and_flags(record), (Some(20_000), true, true));
    }
}

#[tokio::test]
async fn same_block_and_cached_outputs_need_no_lookup() {
    let node = node().await;
    let prevouts = Prevouts::new(1000);
    let a = funding(1, &[420_000, 50_000]);
    let spends_a = Mixed::new(vec![spend(&a, 0)], &[], -400_000).build();
    let records = node
        .records(H, &[a.clone(), spends_a], &prevouts)
        .await
        .unwrap();
    assert!(records
        .iter()
        .all(|r| fee_and_flags(r) == (Some(20_000), true, false)));
    // A later block spends the earlier block's other output from the cache.
    let later = Mixed::new(vec![spend(&a, 1)], &[], -45_000).build();
    let records = node.records(H + 1, &[later], &prevouts).await.unwrap();
    assert!(records
        .iter()
        .all(|r| fee_and_flags(r) == (Some(5_000), true, false)));
    assert_eq!(node.chain.lock().unwrap().count("getrawtransaction"), 0);
    let counts = prevouts.counts();
    assert_eq!((counts.same_block_hits, counts.cache_hits), (1, 1));
}

#[tokio::test]
async fn pure_ironwood_fixture_keeps_its_fee_without_extra_rpc() {
    let node = node().await;
    let prevouts = Prevouts::new(1000);
    let records = node.records(H, &[fixture()], &prevouts).await.unwrap();
    assert_eq!(records, vec![fixture_record(0), fixture_record(1)]);
    assert_eq!(fee_and_flags(&records[0]), (Some(10_000), false, false));
    let chain = node.chain.lock().unwrap();
    assert_eq!(chain.batches, 0);
    assert_eq!(chain.calls, ["getblock", "getblock"]);
}

#[tokio::test]
async fn orchard_and_ironwood_together() {
    let node = node().await;
    let prevouts = Prevouts::new(1000);
    let a = funding(1, &[100_000]);
    node.history(&[&a]);
    // 100,000 transparent and 50,000 from Orchard; 130,000 into Ironwood.
    let mut mixed = Mixed::new(vec![spend(&a, 0)], &[], -130_000);
    mixed.orchard = Some(50_000);
    let tx = mixed.build();
    assert!(tx.orchard_actions().next().is_some());
    let records = node.records(H, &[tx], &prevouts).await.unwrap();
    assert_eq!(records.len(), 2, "only Ironwood actions become records");
    for (i, record) in records.iter().enumerate() {
        assert_eq!(fee_and_flags(record), (Some(20_000), true, false));
        assert_fixture_action(record, i);
    }
    // Without transparent inputs, a shielded-only mixed transaction needs no lookup.
    let mut shielded = Mixed::new(vec![], &[], -40_000);
    shielded.orchard = Some(60_000);
    let records = node
        .records(H + 1, &[shielded.build()], &prevouts)
        .await
        .unwrap();
    assert_eq!(fee_and_flags(&records[0]), (Some(20_000), false, false));
    assert_eq!(node.chain.lock().unwrap().count("getrawtransaction"), 1);
}

/// Public mainnet V5 Sapling transaction; see fixtures/sapling-v5-1687106.md.
fn sapling_bundle() -> zakura_chain::sapling::ShieldedData<zakura_chain::sapling::SharedAnchor> {
    let raw = hex::decode(include_str!("../fixtures/sapling-v5-1687106.hex").trim()).unwrap();
    match Transaction::zcash_deserialize(raw.as_slice()).unwrap() {
        Transaction::V5 {
            sapling_shielded_data: Some(sapling),
            ..
        } => sapling,
        _ => panic!("fixture is a V5 Sapling transaction"),
    }
}

#[tokio::test]
async fn sapling_and_ironwood_together() {
    let node = node().await;
    let prevouts = Prevouts::new(1000);
    let a = funding(1, &[70_000]);
    node.history(&[&a]);
    // 70,000 transparent and 30,000 from Sapling; 80,000 into Ironwood.
    let mut mixed = Mixed::new(vec![spend(&a, 0)], &[], -80_000);
    mixed.sapling = Some((sapling_bundle(), 30_000));
    let tx = mixed.build();
    assert!(tx.sapling_outputs().next().is_some());
    let records = node.records(H, &[tx], &prevouts).await.unwrap();
    for record in &records {
        assert_eq!(fee_and_flags(record), (Some(20_000), true, false));
    }
}

#[tokio::test]
async fn coinbase_has_no_fee_and_needs_no_lookup() {
    let node = node().await;
    let prevouts = Prevouts::new(1000);
    let records = node
        .records(H, &[ironwood_coinbase(H as u32)], &prevouts)
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    for (i, record) in records.iter().enumerate() {
        // The coinbase input counts as a transparent input, as before.
        assert_eq!(fee_and_flags(record), (None, true, true));
        assert_fixture_action(record, i);
    }
    assert_eq!(node.chain.lock().unwrap().batches, 0);
}

async fn failure(
    previous: &[&Transaction],
    tx: Transaction,
    fault: Option<BatchFault>,
) -> ZakuraError {
    let node = node().await;
    node.history(previous);
    node.chain.lock().unwrap().fault = fault;
    node.records(H, &[tx], &Prevouts::new(1000))
        .await
        .expect_err("block must fail rather than publish a partial fee")
}

#[tokio::test]
async fn unresolvable_or_invalid_inputs_fail_the_block() {
    let a = funding(1, &[420_000]);
    let spends_a = || Mixed::new(vec![spend(&a, 0)], &[], -400_000).build();
    assert!(matches!(
        failure(&[], spends_a(), None).await,
        ZakuraError::MissingTransaction(txid) if txid == a.hash().to_string()
    ));
    assert!(matches!(
        failure(
            &[&a],
            Mixed::new(vec![spend(&a, 1)], &[], -400_000).build(),
            None
        )
        .await,
        ZakuraError::PrevoutIndex { outputs: 1, .. }
    ));
    assert!(matches!(
        failure(&[&a], spends_a(), Some(BatchFault::EntryError(-5))).await,
        ZakuraError::MissingTransaction(_)
    ));
    assert!(matches!(
        failure(&[&a], spends_a(), Some(BatchFault::EntryError(-32603))).await,
        ZakuraError::Rpc(-32603, _)
    ));
    assert!(matches!(
        failure(&[&a], spends_a(), Some(BatchFault::WholeBatchError)).await,
        ZakuraError::Rpc(-32600, _)
    ));
    for fault in [
        BatchFault::Short,
        BatchFault::DuplicateId,
        BatchFault::OutOfRangeId,
    ] {
        let b = funding(2, &[1]);
        let two = Mixed::new(vec![spend(&a, 0), spend(&b, 0)], &[], -400_000).build();
        assert!(
            matches!(
                failure(&[&a, &b], two, Some(fault)).await,
                ZakuraError::Batch(_)
            ),
            "{fault:?}"
        );
    }
    let other = funding(3, &[5]);
    assert!(matches!(
        failure(&[&other], spends_a(), Some(BatchFault::WrongTransaction)).await,
        ZakuraError::Transaction(..)
    ));
    assert!(matches!(
        failure(
            &[&a],
            Mixed::new(vec![spend(&a, 0), spend(&a, 0)], &[], -400_000).build(),
            None
        )
        .await,
        ZakuraError::Fee(_, FeeError::DuplicateInput(_))
    ));
    assert!(matches!(
        failure(
            &[&a],
            Mixed::new(vec![spend(&a, 0)], &[30_000], -400_000).build(),
            None
        )
        .await,
        ZakuraError::Fee(_, FeeError::Negative(_))
    ));
}
