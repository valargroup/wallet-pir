use receiver_directory::{
    extract::Action,
    snapshot::{Manifest, Snapshot, PROFILE},
    Payment, Receiver, Record,
};
use receiver_pir::{http::HttpClient, server::Server, AcceptedCoverage, Error, ROWS};
use std::{num::NonZeroU32, time::Duration};

fn fixture() -> Action {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../crates/receiver-directory/tests/fixtures/zero-ovk-action.json"
    ))
    .unwrap();
    fn field<const N: usize>(v: &serde_json::Value, key: &str) -> [u8; N] {
        hex::decode(v["action"][key].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap()
    }
    Action {
        cv: field(&v, "cv"),
        nullifier: field(&v, "nullifier"),
        cmx: field(&v, "cmx"),
        ephemeral_key: field(&v, "ephemeralKey"),
        enc_ciphertext: field(&v, "encCiphertext"),
        out_ciphertext: field(&v, "outCiphertext"),
    }
}
fn receiver() -> Receiver {
    fixture().recover_receiver().unwrap().unwrap()
}
fn accepted() -> AcceptedCoverage {
    AcceptedCoverage {
        genesis: [1; 32],
        required_start: 100,
        height: 101,
        hash: [3; 32],
    }
}
fn snapshot(count: u32) -> Snapshot {
    let manifest = Manifest {
        profile: PROFILE.into(),
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 200,
        end_height: 101,
        end_hash: [3; 32],
        end_position: 300,
        rows: ROWS as u32,
        salt: [4; 32],
        records: 0,
        data_sha256: [0; 32],
    };
    let records: Vec<_> = (0..count)
        .map(|page| Record {
            receiver: receiver(),
            page,
            total: count,
            payment: Payment {
                height: 101,
                block_hash: [3; 32],
                txid: [page as u8; 32],
                tx_index: page,
                action_index: 0,
                position: 200 + u64::from(page),
                action_nullifier: [5; 32],
                cmx: [6; 32],
                ephemeral_key: [7; 32],
                ciphertext_prefix: [8; 52],
            },
        })
        .collect();
    Snapshot::build(manifest, &records).unwrap()
}
struct Running {
    origin: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn serve(snapshot: Snapshot) -> Running {
    let app = receiver_pir_server::router(Server::new(snapshot).unwrap());
    serve_router(app).await
}
async fn serve_router(app: axum::Router) -> Running {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    Running { origin, task }
}

#[tokio::test]
async fn rotate_canonical_sessions_and_revoke_orphaned_work() {
    use receiver_pir_server::{Publication, Publications};
    let publications = Publications::default();
    assert!(publications.publish(
        Publication::new(Server::new(snapshot(2)).unwrap(), None).unwrap(),
        0
    ));
    let server = serve_router(receiver_pir_server::router_with_publications(
        publications.clone(),
    ))
    .await;
    let old = HttpClient::connect(&server.origin, http(), accepted())
        .await
        .unwrap();
    let old_id = hex::encode(old.manifest().id().unwrap());
    let mut next = snapshot(2);
    next.manifest.end_height = 102;
    next.manifest.end_hash = [9; 32];
    // Empty canonical extension: old payments remain unchanged.
    assert!(publications.publish(
        Publication::new(
            Server::new(Snapshot {
                manifest: next.manifest.clone(),
                data: next.data.clone()
            })
            .unwrap(),
            None
        )
        .unwrap(),
        0
    ));
    let public = http()
        .get(format!("{}/v1/receiver/public/{old_id}", server.origin))
        .send()
        .await
        .unwrap();
    assert!(public.status().is_success());
    assert_eq!(
        old.lookup(receiver(), NonZeroU32::new(2).unwrap(), accepted())
            .await
            .unwrap()
            .len(),
        2
    );
    let mut anchor = accepted();
    anchor.height = 102;
    anchor.hash = [9; 32];
    let new = HttpClient::connect(&server.origin, http(), anchor)
        .await
        .unwrap();
    assert_eq!(
        new.lookup(receiver(), NonZeroU32::new(2).unwrap(), anchor)
            .await
            .unwrap()
            .len(),
        2
    );

    // Preparation started before the canonical guard detected the fork.
    let preparing_epoch = publications.epoch();
    let prepared = Publication::new(Server::new(next).unwrap(), None).unwrap();
    publications.revoke();
    assert!(!publications.publish(prepared, preparing_epoch));
    assert_eq!(
        http()
            .get(format!("{}/v1/receiver/init", server.origin))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        http()
            .get(format!("{}/v1/receiver/public/{old_id}", server.origin))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::GONE
    );
    assert!(matches!(old.witnesses().await, Err(Error::Revision)));
    assert!(matches!(
        new.lookup(receiver(), NonZeroU32::new(2).unwrap(), anchor)
            .await,
        Err(Error::Revision)
    ));

    // Canonical replacement drops the orphaned payment entirely.
    let mut replacement = snapshot(0);
    replacement.manifest.end_height = 102;
    replacement.manifest.end_hash = [10; 32];
    assert!(publications.publish(
        Publication::new(Server::new(replacement).unwrap(), None).unwrap(),
        publications.epoch()
    ));
    anchor.hash = [10; 32];
    let recovered = HttpClient::connect(&server.origin, http(), anchor)
        .await
        .unwrap();
    assert!(recovered
        .lookup(receiver(), NonZeroU32::new(1).unwrap(), anchor)
        .await
        .unwrap()
        .is_empty());
}
fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

#[tokio::test]
async fn retrieve_complete_history_and_enforce_limits_over_http() {
    let server = serve(snapshot(2)).await;
    let client = HttpClient::connect(&server.origin, http(), accepted())
        .await
        .unwrap();
    let payments = client
        .lookup(receiver(), NonZeroU32::new(2).unwrap(), accepted())
        .await
        .unwrap();
    assert_eq!(payments.len(), 2);
    assert_eq!(payments[0].position, 200);
    assert_eq!(payments[1].position, 201);
    assert!(matches!(
        client
            .lookup(receiver(), NonZeroU32::new(1).unwrap(), accepted())
            .await,
        Err(Error::PageBudget)
    ));
    let mut wrong = accepted();
    wrong.hash[0] ^= 1;
    assert!(client
        .lookup(receiver(), NonZeroU32::new(2).unwrap(), wrong)
        .await
        .is_err());
    let oversized = http()
        .post(format!("{}/v1/receiver/query", server.origin))
        .body(vec![0; receiver_pir::query_bytes() + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(oversized.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    let malformed = http()
        .post(format!("{}/v1/receiver/query", server.origin))
        .body(vec![0; 8])
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), reqwest::StatusCode::BAD_REQUEST);
    let absent = serve(snapshot(0)).await;
    let client = HttpClient::connect(&absent.origin, http(), accepted())
        .await
        .unwrap();
    assert!(client
        .lookup(receiver(), NonZeroU32::new(1).unwrap(), accepted())
        .await
        .unwrap()
        .is_empty());
}

/// Public chain fixture only. Supply the previously verified backfill manifest, never a wallet DB.
#[tokio::test]
#[ignore = "requires local mainnet publication; set RECEIVER_MAINNET_MANIFEST"]
async fn known_mainnet_refund_over_encrypted_http() {
    mainnet_lookup().await;
}

async fn mainnet_lookup() -> Payment {
    let path = std::path::PathBuf::from(std::env::var("RECEIVER_MAINNET_MANIFEST").unwrap());
    let manifest: Manifest = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let revision = hex::encode(manifest.revision().unwrap());
    let data = std::fs::read(path.with_file_name(format!("{revision}.rows"))).unwrap();
    fn rpc_hash(s: &str) -> [u8; 32] {
        let mut b = hex::decode(s).unwrap();
        b.reverse();
        b.try_into().unwrap()
    }
    // Anchor independently verified against the canonical node at backfill completion.
    let accepted = AcceptedCoverage {
        genesis: rpc_hash("00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08"),
        required_start: 3428143,
        height: 3497109,
        hash: rpc_hash("00000000001a95c9545f79c6cceda87a7ba1e4bb06398f0559d39a330afb6fc5"),
    };
    let server = serve(Snapshot { manifest, data }).await;
    let client = HttpClient::connect(&server.origin, http(), accepted)
        .await
        .unwrap();
    let found = client
        .lookup(receiver(), NonZeroU32::new(10).unwrap(), accepted)
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    let p = &found[0];
    assert_eq!(p.position, 610503);
    assert_eq!((p.height, p.tx_index, p.action_index), (3496114, 16, 0));
    assert_eq!(
        p.txid,
        rpc_hash("2060cf68088b55dcd9e2f91556c72528e1ab8c6834ea3f71b8c80fca9fc51653")
    );
    let a = fixture();
    assert_eq!(p.action_nullifier, a.nullifier);
    assert_eq!(p.cmx, a.cmx);
    assert_eq!(p.ephemeral_key, a.ephemeral_key);
    assert_eq!(p.ciphertext_prefix, a.enc_ciphertext[..52]);
    println!(
        "Verified mainnet receiver lookup: revision={revision}, position={}, height={}",
        p.position, p.height
    );
    found.into_iter().next().unwrap()
}

#[tokio::test]
async fn reject_incomplete_or_inconsistent_pagination() {
    use receiver_directory::{snapshot::ROW_BYTES, RECORD_BYTES};
    use sha2::{Digest, Sha256};
    // Model a faulty indexer that publishes correctly hashed but inconsistent page data.
    for fault in 0..3 {
        let mut data = snapshot(2);
        for row in data.data.as_chunks_mut::<ROW_BYTES>().0.iter_mut() {
            for slot in row[..14 * RECORD_BYTES]
                .as_chunks_mut::<RECORD_BYTES>()
                .0
                .iter_mut()
            {
                if let Some(mut r) = Record::decode(slot).unwrap() {
                    if r.page == 1 {
                        match fault {
                            0 => {
                                slot.fill(0);
                                continue;
                            }
                            1 => r.total = 3,
                            _ => r.payment.position = 200,
                        }
                        slot.copy_from_slice(&r.encode().unwrap());
                    }
                }
            }
        }
        data.manifest.data_sha256 = Sha256::digest(&data.data).into();
        let server = serve(data).await;
        let client = HttpClient::connect(&server.origin, http(), accepted())
            .await
            .unwrap();
        assert!(
            client
                .lookup(receiver(), NonZeroU32::new(5).unwrap(), accepted())
                .await
                .is_err(),
            "fault {fault} must not produce partial success"
        );
    }
}

/// This is an authenticated public-output test, not a wallet ownership or witness test.
#[tokio::test]
#[ignore = "requires mainnet publication and ENHANCE_PIR_ORIGIN for an isolated integration service"]
async fn known_mainnet_refund_through_receiver_and_enhance_pir() {
    let payment = mainnet_lookup().await;
    let origin = std::env::var("ENHANCE_PIR_ORIGIN").unwrap();
    let mut client = enhance_pir::client::EnhancePirClient::connect(&origin)
        .await
        .unwrap();
    assert_eq!(client.manifest().network, "main");
    assert_eq!(client.manifest().pool, "ironwood");
    assert!(client.manifest().anchor_height >= u64::from(payment.height));
    let (enhancement, timing) = client
        .query_position_with_timing(payment.position)
        .await
        .unwrap();
    let joined = Action::from_payment(
        &payment,
        enhancement.enc_ciphertext_suffix(),
        *enhancement.cv_net(),
        *enhancement.out_ciphertext(),
    );
    let expected = fixture();
    assert_eq!(joined.enc_ciphertext, expected.enc_ciphertext);
    assert_eq!(joined.cv, expected.cv);
    assert_eq!(joined.out_ciphertext, expected.out_ciphertext);
    assert_eq!(joined.recover_receiver().unwrap(), Some(receiver()));
    println!("Authenticated receiver + Enhance result: position={}, generation={}, anchor_height={}, anchor_hash={}, enhance_query_ms={}",
        payment.position, client.manifest().generation, client.manifest().anchor_height,
        client.manifest().anchor_block_hash, timing.total.as_millis());
}

#[tokio::test]
async fn common_witness_file_uses_the_same_publication() {
    use receiver_directory::witness::WitnessSnapshot;
    let mut manifest = snapshot(0).manifest;
    manifest.start_position = 0;
    manifest.end_position = 1;
    let snapshot = Snapshot::build(manifest, &[]).unwrap();
    let proof = WitnessSnapshot::build(&snapshot.manifest, &[[1; 32]], &[0].into_iter().collect())
        .unwrap()
        .encode();
    let app =
        receiver_pir_server::router_with_witnesses(Server::new(snapshot).unwrap(), Some(proof));
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    let server = Running { origin, task };
    let client = HttpClient::connect(&server.origin, http(), accepted())
        .await
        .unwrap();
    let proof = client.witnesses().await.unwrap();
    proof.path(0, [1; 32]).unwrap();
    assert!(proof.path(0, [2; 32]).is_err());
}

#[tokio::test]
async fn host_transport_handles_setup_queries_and_revision_errors() {
    use receiver_pir::transport::{DirectoryClient, Transport};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Host {
        http: reqwest::Client,
        gets: AtomicUsize,
        posts: AtomicUsize,
    }
    impl Transport for Host {
        async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            Transport::get(&self.http, url, limit).await
        }
        async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
            self.posts.fetch_add(1, Ordering::SeqCst);
            Transport::post(&self.http, url, body, limit).await
        }
    }
    let server = serve(snapshot(2)).await;
    let host = Host {
        http: http(),
        gets: AtomicUsize::new(0),
        posts: AtomicUsize::new(0),
    };
    let client = DirectoryClient::connect(&server.origin, &host, accepted())
        .await
        .unwrap();
    let payments = client
        .lookup(receiver(), NonZeroU32::new(2).unwrap(), accepted())
        .await
        .unwrap();
    assert_eq!(payments.len(), 2);
    assert_eq!(host.gets.load(Ordering::SeqCst), 2);
    assert_eq!(host.posts.load(Ordering::SeqCst), 2);
    let missing = host
        .get(
            &format!("{}/v1/receiver/public/{}", server.origin, "00".repeat(32)),
            1024,
        )
        .await;
    assert!(matches!(missing, Err(Error::Revision)));
}
