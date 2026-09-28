//! Synthetic wallet recovery benchmark. Uses disposable file-backed wallets and loopback PIR.
mod fixture;
mod transport;
use fixture::{BATCH, Fixture, KEYS, START, Wallet};
use futures::StreamExt;
use orchard::tree::{MerkleHashOrchard, MerklePath};
use receiver_directory::{Payment, Receiver};
use receiver_pir::{AcceptedCoverage, http::HttpClient, server::Server};
use receiver_pir_server::{Publication, Publications};
use serde_json::{Value, json};
use std::{
    num::NonZeroU32,
    path::PathBuf,
    time::{Duration, Instant},
};
use zakura_pir_enhance::{
    AcceptedAnchor, ClientResourceLimits, GenerationAcceptance, transport::PendingClient,
};
use zakura_swap_receiving::{KeyId, Purpose, lifecycle::ChainAnchor, recovery::EncryptedNote};
use zcash_client_backend::data_api::{
    Account as _, WalletCommitmentTrees, WalletRead, WalletWrite,
};
use zcash_client_sqlite::wallet::swap_receiving::{PaymentApplication, PendingPayment};
use zcash_primitives::{block::BlockHash, transaction::TxId};

fn inventory(st: &Wallet) -> Vec<Value> {
    let mut query = st
        .wallet()
        .conn()
        .prepare(
            "SELECT t.txid, n.action_index, n.commitment_tree_position, n.value, n.nf,
                EXISTS(SELECT 1 FROM ironwood_received_note_spends s
                       WHERE s.ironwood_received_note_id = n.id)
         FROM ironwood_received_notes n
         JOIN transactions t ON t.id_tx = n.transaction_id
         WHERE n.value > 0 ORDER BY n.commitment_tree_position",
        )
        .unwrap();
    query
        .query_map([], |row| {
            Ok(json!({
                "txid": hex::encode(row.get::<_, Vec<u8>>(0)?),
                "action_index": row.get::<_, u32>(1)?,
                "position": row.get::<_, u64>(2)?,
                "value": row.get::<_, u64>(3)?,
                "nullifier": hex::encode(row.get::<_, Vec<u8>>(4)?),
                "spent": row.get::<_, bool>(5)?,
            }))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
}
async fn run(
    f: &Fixture,
    receiver_origin: &str,
    enhance_origin: &str,
    pir: bool,
    trial: u32,
) -> Value {
    transport::take();
    let total = Instant::now();
    let setup = Instant::now();
    let mut st = fixture::wallet(&f.blocks);
    let account = st.test_account().unwrap().id();
    assert_eq!(
        orchard::keys::FullViewingKey::from(st.test_account().unwrap().usk().orchard()).to_bytes(),
        f.parent.to_bytes()
    );
    if pir {
        st.wallet_mut()
            .db_mut()
            .enable_private_swap_recovery(account)
            .unwrap();
    }
    for i in 0..KEYS {
        st.wallet_mut()
            .db_mut()
            .watch_swap_receive_key(account, u64::from(i), START.into())
            .unwrap();
    }
    let through = ChainAnchor {
        height: f.snapshot.manifest.end_height.into(),
        hash: f.snapshot.manifest.end_hash,
    };
    st.wallet_mut()
        .db_mut()
        .update_chain_tip(through.height)
        .unwrap();
    let setup_us = setup.elapsed().as_micros();
    let scan = Instant::now();
    // A restored test wallet has no generator-side cached tree state. Supply each
    // batch's actual preceding frontier, as a wallet does using its tree-state source.
    for (batch, offset) in (0..f.blocks.len()).step_by(BATCH).enumerate() {
        st.try_scan_cached_blocks_with_state(
            (START + offset as u32).into(),
            &f.batch_states[batch],
            (f.blocks.len() - offset).min(BATCH),
        )
        .unwrap_or_else(|e| panic!("scan batch at {} failed: {e:?}", START + offset as u32));
    }
    let scan_us = scan.elapsed().as_micros();
    assert_eq!(
        st.wallet()
            .db()
            .block_fully_scanned()
            .unwrap()
            .unwrap()
            .block_height(),
        through.height
    );
    assert_eq!(
        st.wallet()
            .db()
            .get_block_hash(through.height)
            .unwrap()
            .unwrap()
            .0,
        through.hash
    );
    if pir {
        assert!(inventory(&st).is_empty());
    } else {
        assert_eq!(inventory(&st), f.expected);
    }
    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let transport = transport::Http(http.clone());
    let enhance_start = Instant::now();
    let pending = PendingClient::fetch(&transport, enhance_origin)
        .await
        .unwrap();
    let mut display = through.hash;
    display.reverse();
    let accepted = GenerationAcceptance::new(
        "main",
        u64::from(START),
        AcceptedAnchor::new(
            u64::from(u32::from(through.height)),
            display,
            f.snapshot.manifest.end_position,
        ),
        ClientResourceLimits::with_cache(32768, 2),
    );
    let mut enhance = pending.accept(&accepted).unwrap();
    let enhance_setup_us = enhance_start.elapsed().as_micros();
    let mut directory_setup_us = 0;
    let mut witness_download_us = 0;
    let mut lookup_us = 0;
    let mut enhance_us = 0;
    let mut apply_us = 0;
    let mut discovered = Vec::new();
    let mut witnesses = None;
    if pir {
        let start = Instant::now();
        let accepted = AcceptedCoverage {
            genesis: [1; 32],
            required_start: START,
            height: through.height.into(),
            hash: through.hash,
        };
        let client = HttpClient::connect(receiver_origin, http.clone(), accepted)
            .await
            .unwrap();
        directory_setup_us = start.elapsed().as_micros();
        let start = Instant::now();
        witnesses = Some(client.witnesses().await.unwrap());
        witness_download_us = start.elapsed().as_micros();
        for (i, key) in f.keys.iter().enumerate() {
            let start = Instant::now();
            let payments = client
                .lookup(
                    Receiver::from_bytes(
                        key.address_at(0u32, orchard::keys::Scope::External)
                            .to_raw_address_bytes(),
                    )
                    .unwrap(),
                    NonZeroU32::new(32).unwrap(),
                    accepted,
                )
                .await
                .unwrap();
            lookup_us += start.elapsed().as_micros();
            for payment in payments {
                discovered.push((KeyId::new(Purpose::Receive, i as u64), payment));
            }
        }
    } else {
        // The baseline locates these outputs by trial decryption during ordinary scanning.
        // Fetch full ciphertext for the positions actually stored by that scan.
        let notes = inventory(&st);
        for note in notes {
            let r = f
                .records
                .iter()
                .find(|r| r.payment.position == note["position"].as_u64().unwrap())
                .unwrap();
            let i = f
                .keys
                .iter()
                .position(|k| {
                    k.address_at(0u32, orchard::keys::Scope::External)
                        .to_raw_address_bytes()
                        == *r.receiver.as_bytes()
                })
                .unwrap();
            discovered.push((KeyId::new(Purpose::Receive, i as u64), r.payment.clone()));
        }
    }
    assert_eq!(discovered.len(), 5);
    for (key, payment) in discovered {
        let start = Instant::now();
        let stream = enhance.query_batch(&transport, [payment.position]).unwrap();
        futures::pin_mut!(stream);
        let record = stream.next().await.unwrap().record.unwrap();
        enhance_us += start.elapsed().as_micros();
        let start = Instant::now();
        let candidate = candidate(&payment, record.enc_ciphertext_suffix());
        let decoded = candidate.encrypted_note.decrypt(&f.parent, key).unwrap();
        assert_eq!(decoded.note().value().inner(), 100_000);
        // Baseline notes and witnesses were already inserted by the ordinary scan.
        // Both paths authenticate the memo here. UI transaction enrichment is excluded.
        if pir {
            let proof = witnesses
                .as_ref()
                .unwrap()
                .path(candidate.position, payment.cmx)
                .unwrap();
            let path = MerklePath::from_parts(
                candidate.position,
                proof.map(|p| MerkleHashOrchard::from_bytes(&p).unwrap()),
            );
            st.wallet_mut()
                .db_mut()
                .queue_swap_payment(account, key, &candidate)
                .unwrap();
            assert_eq!(
                st.wallet_mut()
                    .db_mut()
                    .apply_pending_swap_payment(
                        account,
                        key,
                        &candidate,
                        through,
                        Some((through, &path))
                    )
                    .unwrap(),
                PaymentApplication::Applied
            );
        }
        apply_us += start.elapsed().as_micros();
    }
    if pir {
        for i in 0..KEYS {
            st.wallet_mut()
                .db_mut()
                .mark_swap_directory_checked(
                    account,
                    KeyId::new(Purpose::Receive, u64::from(i)),
                    through,
                )
                .unwrap();
        }
    }
    let total_us = total.elapsed().as_micros();
    let notes = inventory(&st);
    assert_eq!(notes, f.expected);
    // Check spendability evidence after timing, identically for both modes.
    let common =
        receiver_directory::witness::WitnessSnapshot::decode(&f.proof, &f.snapshot.manifest)
            .unwrap();
    for note in notes.iter().filter(|n| n["spent"] == false) {
        let position = note["position"].as_u64().unwrap();
        let path = st
            .wallet_mut()
            .with_ironwood_tree_mut(|tree| {
                tree.witness_at_checkpoint_id(position.into(), &through.height)
            })
            .unwrap()
            .unwrap()
            .unwrap();
        let payment = &f
            .records
            .iter()
            .find(|r| r.payment.position == position)
            .unwrap()
            .payment;
        let cmx = orchard::note::ExtractedNoteCommitment::from_bytes(&payment.cmx).unwrap();
        assert_eq!(MerklePath::from(path).root(cmx).to_bytes(), common.root);
    }
    let requests = transport::take();
    let receiver_queries = requests
        .iter()
        .filter(|r| r["component"] == "receiver" && r["kind"] == "query")
        .count();
    assert_eq!(receiver_queries, if pir { KEYS as usize } else { 0 });
    let result = json!({
        "trial": trial,
        "mode": if pir { "pir" } else { "scan" },
        "keys": KEYS,
        "blocks": f.blocks.len(),
        "actions": f.snapshot.manifest.end_position,
        "fixture_sha256": f.hash,
        "setup_us": setup_us,
        "scan_us": scan_us,
        "enhance_setup_us": enhance_setup_us,
        "directory_setup_us": directory_setup_us,
        "witness_download_us": witness_download_us,
        "lookup_us": lookup_us,
        "enhance_us": enhance_us,
        "apply_us": apply_us,
        "total_us": total_us,
        "notes": notes,
        "http": requests,
        "complete": true,
    });
    println!("{}", result);
    result
}
fn candidate(p: &Payment, suffix: &[u8; 528]) -> PendingPayment {
    PendingPayment {
        txid: TxId::from_bytes(p.txid),
        action_index: p.action_index,
        height: p.height.into(),
        block_hash: BlockHash(p.block_hash),
        tx_index: p.tx_index.try_into().unwrap(),
        position: p.position.try_into().unwrap(),
        encrypted_note: EncryptedNote::from_parts(
            p.action_nullifier,
            p.cmx,
            p.ephemeral_key,
            p.ciphertext_prefix,
            suffix,
        ),
    }
}
struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    transport::init();
    let args: Vec<_> = std::env::args().collect();
    assert!(
        (3..=5).contains(&args.len()),
        "usage: recovery-benchmark ENHANCE_FIXTURE_BINARY NEW_OUTPUT_DIRECTORY [PUBLIC_SHAPE_JSON [CACHED_COMPACT_BLOCKS]]"
    );
    let root = PathBuf::from(&args[2]);
    std::fs::create_dir(&root).unwrap();
    let shape: Option<Value> = args
        .get(3)
        .map(|p| serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap());
    let cache = args
        .get(4)
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("compact.bin"));
    let f = fixture::generate(shape.as_ref(), &cache);
    std::fs::write(
        root.join("enhance.json"),
        serde_json::to_vec(&f.enhance).unwrap(),
    )
    .unwrap();
    let fixture_metadata = json!({
        "sha256": f.hash,
        "keys": KEYS,
        "blocks": f.blocks.len(),
        "actions": f.snapshot.manifest.end_position,
        "expected": f.expected,
        "directory_records": f.records.len(),
        "witness_bytes": f.proof.len(),
        "scope": "synthetic wallet pipeline, local compact cache and loopback PIR; excludes mainnet download and UI",
    });
    std::fs::write(
        root.join("fixture.json"),
        serde_json::to_vec_pretty(&fixture_metadata).unwrap(),
    )
    .unwrap();
    let log = std::fs::File::create(root.join("enhance-server.log")).unwrap();
    // The parent owns fixture storage so it is removed even when the server is killed.
    let server_storage = tempfile::tempdir().unwrap();
    let mut child = Child(
        std::process::Command::new(&args[1])
            .arg(root.join("enhance.json"))
            .arg(root.join("ready.json"))
            .arg(server_storage.path())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap(),
    );
    let started = Instant::now();
    while !root.join("ready.json").exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "fixture server failed; inspect its log"
        );
        assert!(started.elapsed() < Duration::from_secs(240));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let ready: Value =
        serde_json::from_slice(&std::fs::read(root.join("ready.json")).unwrap()).unwrap();
    let publications = Publications::default();
    let snapshot = receiver_directory::snapshot::Snapshot {
        manifest: f.snapshot.manifest.clone(),
        data: f.snapshot.data.clone(),
    };
    assert!(publications.publish(
        Publication::new(Server::new(snapshot).unwrap(), Some(f.proof.clone())).unwrap(),
        0
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            receiver_pir_server::router_with_publications(publications),
        )
        .await
        .unwrap()
    });
    let mut results = Vec::new();
    for trial in 0..3 {
        for pir in if trial % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            results.push(run(&f, &origin, ready["origin"].as_str().unwrap(), pir, trial).await);
        }
    }
    std::fs::write(
        root.join("results.json"),
        serde_json::to_vec_pretty(&results).unwrap(),
    )
    .unwrap();
    task.abort();
    drop(child);
}
