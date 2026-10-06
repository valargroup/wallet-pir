//! Receiver PIR evaluation and the HTTP service that wallets reach through a [`Transport`].
#[path = "../../../crates/receiver-directory/tests/common/mod.rs"]
mod common;
use common::{manifest, receiver, record};
use receiver_directory::{snapshot::Snapshot, Record};
use receiver_pir::{
    server::Server,
    transport::{DirectoryClient, Transport},
    AcceptedCoverage, Client, Error, MIN_ROWS,
};
use receiver_pir_server::{Publication, Publications};
use std::{num::NonZeroU32, time::Duration};

/// The chain anchor that [`manifest`] ends at.
fn accepted() -> AcceptedCoverage {
    AcceptedCoverage {
        genesis: [1; 32],
        required_start: 100,
        height: 101,
        hash: [3; 32],
    }
}
fn snapshot(count: u32) -> Snapshot {
    snapshot_rows(count, MIN_ROWS)
}
/// A publication of `rows` rows with `count` pages for the fixture receiver.
fn snapshot_rows(count: u32, rows: u32) -> Snapshot {
    let records: Vec<_> = (0..count).map(|page| record(page, count)).collect();
    Snapshot::build(manifest(rows), &records).unwrap()
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
    serve_publication(Publication::new(Server::new(snapshot).unwrap(), None).unwrap()).await
}
/// Serve one prepared publication through the production router.
async fn serve_publication(publication: Publication) -> Running {
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    serve_router(receiver_pir_server::router_with_publications(publications)).await
}
async fn serve_router(app: axum::Router) -> Running {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    Running { origin, task }
}

#[tokio::test]
async fn rotate_canonical_sessions_and_revoke_orphaned_work() {
    let publications = Publications::default();
    assert!(publications.publish(
        Publication::new(Server::new(snapshot(2)).unwrap(), None).unwrap(),
        0
    ));
    let server = serve_router(receiver_pir_server::router_with_publications(
        publications.clone(),
    ))
    .await;
    let old = connect(&server.origin, Http(http()), accepted(), 0)
        .await
        .unwrap();
    let old_id = hex::encode(old.manifest().id().unwrap());
    let mut next = snapshot_rows(2, MIN_ROWS * 2);
    next.manifest.end_height = 102;
    next.manifest.end_hash = [9; 32];
    // Empty canonical extension: old payments remain unchanged.
    assert!(publications.publish(
        Publication::new(Server::new(next.clone()).unwrap(), None).unwrap(),
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
    let new = connect(&server.origin, Http(http()), anchor, 0)
        .await
        .unwrap();
    assert_eq!(
        new.lookup(receiver(), NonZeroU32::new(2).unwrap(), anchor)
            .await
            .unwrap()
            .len(),
        2
    );

    let file = connect(&server.origin, Http(http()), anchor, 10_000)
        .await
        .unwrap();
    assert_eq!(
        file.lookup(receiver(), NonZeroU32::new(2).unwrap(), anchor)
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
    assert_eq!(
        http()
            .get(format!(
                "{}/v1/receiver/public/{}",
                server.origin,
                "00".repeat(32)
            ))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
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
    let recovered = connect(&server.origin, Http(http()), anchor, 0)
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

/// A wallet transport over reqwest. It maps 409 and 410 to [`Error::Revision`].
struct Http(reqwest::Client);

impl Transport for Http {
    async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
        read(self.0.get(url), limit).await
    }
    async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
        read(self.0.post(url).body(body), limit).await
    }
}

/// Send `request` and return its successful body, failing if it exceeds `limit` bytes.
async fn read(request: reqwest::RequestBuilder, limit: usize) -> Result<Vec<u8>, Error> {
    let failed = |e: reqwest::Error| Error::Transport(e.to_string());
    let response = request.send().await.map_err(failed)?;
    match response.status() {
        reqwest::StatusCode::CONFLICT | reqwest::StatusCode::GONE => return Err(Error::Revision),
        status if !status.is_success() => return Err(Error::Transport(status.to_string())),
        _ => {}
    }
    let body = response.bytes().await.map_err(failed)?;
    if body.len() > limit {
        return Err(Error::Malformed);
    }
    Ok(body.to_vec())
}

/// Connect to the publication `origin` advertises, accepted at `accepted`, for
/// `remaining` lookups.
async fn connect<T: Transport>(
    origin: &str,
    http: T,
    accepted: AcceptedCoverage,
    remaining: usize,
) -> Result<DirectoryClient<T>, Error> {
    let manifest = DirectoryClient::fetch_manifest(origin, &http).await?;
    DirectoryClient::connect_manifest(origin, http, accepted, manifest, remaining).await
}

#[tokio::test]
async fn retrieve_complete_history_and_enforce_limits_over_http() {
    let server = serve(snapshot(2)).await;
    let client = connect(&server.origin, Http(http()), accepted(), 0)
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
        .body(vec![
            0;
            receiver_pir::query_bytes(receiver_pir::MAX_ROWS)
                .unwrap()
                + 1
        ])
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
    let client = connect(&absent.origin, Http(http()), accepted(), 0)
        .await
        .unwrap();
    assert!(client
        .lookup(receiver(), NonZeroU32::new(1).unwrap(), accepted())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn reject_incomplete_or_inconsistent_pagination() {
    use receiver_directory::{
        snapshot::{ROW_BYTES, SLOTS},
        RECORD_BYTES,
    };
    use sha2::{Digest, Sha256};
    // Model a faulty indexer that publishes correctly hashed but inconsistent page data.
    for fault in 0..3 {
        let mut data = snapshot(2);
        for row in data.data.as_chunks_mut::<ROW_BYTES>().0.iter_mut() {
            for slot in row[..SLOTS * RECORD_BYTES]
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
        let client = connect(&server.origin, Http(http()), accepted(), 0)
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
    let server =
        serve_publication(Publication::new(Server::new(snapshot).unwrap(), Some(proof)).unwrap())
            .await;
    let client = connect(&server.origin, Http(http()), accepted(), 0)
        .await
        .unwrap();
    let proof = client.witnesses().await.unwrap();
    proof.path(0, [1; 32]).unwrap();
    assert!(proof.path(0, [2; 32]).is_err());
}

#[tokio::test]
async fn adaptive_discovery_uses_remaining_work_and_reuses_verified_file() {
    use receiver_pir::transport::prefer_directory_file;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Counted {
        http: Http,
        up: AtomicUsize,
        down: AtomicUsize,
        posts: AtomicUsize,
        gets: AtomicUsize,
    }
    impl Transport for Counted {
        async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            let data = Transport::get(&self.http, url, limit).await?;
            self.down.fetch_add(data.len(), Ordering::Relaxed);
            Ok(data)
        }
        async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
            self.posts.fetch_add(1, Ordering::Relaxed);
            self.up.fetch_add(body.len(), Ordering::Relaxed);
            let data = Transport::post(&self.http, url, body, limit).await?;
            self.down.fetch_add(data.len(), Ordering::Relaxed);
            Ok(data)
        }
    }
    let server = serve(snapshot(1)).await;
    for count in [50, 250, 10_000] {
        let host = Counted {
            http: Http(http()),
            up: AtomicUsize::new(0),
            down: AtomicUsize::new(0),
            posts: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
        };
        let mut client = connect(&server.origin, &host, accepted(), count)
            .await
            .unwrap();
        assert_eq!(
            prefer_directory_file(client.manifest(), count).unwrap(),
            count >= 250
        );
        for _ in 0..count {
            assert_eq!(
                client
                    .lookup(receiver(), NonZeroU32::new(1).unwrap(), accepted())
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
        assert_eq!(
            host.posts.load(Ordering::Relaxed),
            if count == 50 { 50 } else { 0 }
        );
        assert_eq!(host.gets.load(Ordering::Relaxed), 2);
        println!(
            "receivers={count} upload={} download={} requests={} (HTTP bodies, includes setup)",
            host.up.load(Ordering::Relaxed),
            host.down.load(Ordering::Relaxed),
            host.gets.load(Ordering::Relaxed) + host.posts.load(Ordering::Relaxed)
        );
        client.use_file_for_work(10_000).await.unwrap();
        let requests = host.gets.load(Ordering::Relaxed);
        client.use_file_for_work(10_000).await.unwrap();
        client
            .lookup(receiver(), NonZeroU32::new(1).unwrap(), accepted())
            .await
            .unwrap();
        assert_eq!(host.gets.load(Ordering::Relaxed), requests);
        assert_eq!(requests, if count == 50 { 3 } else { 2 });
    }
}

#[tokio::test]
async fn directory_file_rejects_bad_digest_length_and_pagination() {
    struct Corrupt {
        http: Http,
        truncate: bool,
    }
    impl Transport for Corrupt {
        async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
            let mut bytes = Transport::get(&self.http, url, limit).await?;
            if url.contains("/rows/") {
                if self.truncate {
                    bytes.pop();
                } else {
                    bytes[0] ^= 1;
                }
            }
            Ok(bytes)
        }
        async fn post(&self, _: &str, _: Vec<u8>, _: usize) -> Result<Vec<u8>, Error> {
            panic!("file mode never posts")
        }
    }
    let server = serve(snapshot(2)).await;
    for truncate in [false, true] {
        assert!(matches!(
            connect(
                &server.origin,
                Corrupt {
                    http: Http(http()),
                    truncate
                },
                accepted(),
                250
            )
            .await,
            Err(Error::Malformed)
        ));
    }
    let client = connect(&server.origin, Http(http()), accepted(), 250)
        .await
        .unwrap();
    assert!(matches!(
        client
            .lookup(receiver(), NonZeroU32::new(1).unwrap(), accepted())
            .await,
        Err(Error::PageBudget)
    ));
    assert_eq!(
        client
            .lookup(receiver(), NonZeroU32::new(2).unwrap(), accepted())
            .await
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn encrypted_publication_roundtrip_and_fail_closed() {
    let records = [record(0, 2), record(1, 2)];
    let server = Server::new(Snapshot::build(manifest(MIN_ROWS), &records).unwrap()).unwrap();
    let client = Client::new(server.manifest().clone(), server.public(), accepted()).unwrap();
    let first = client.prepare(receiver(), 0).unwrap();
    let second = client.prepare(receiver(), 0).unwrap();
    assert_ne!(
        first.body(),
        second.body(),
        "fresh encryption even for the same receiver"
    );
    assert_eq!(
        first.body().len(),
        receiver_pir::query_bytes(MIN_ROWS).unwrap()
    );
    let answer = server.respond(first.body()).unwrap();
    assert_eq!(
        answer.len(),
        receiver_pir::response_bytes(MIN_ROWS).unwrap()
    );
    assert!(
        client.decode(second, &answer).is_err(),
        "reject another request's answer"
    );
    assert_eq!(
        client.decode(first, &answer).unwrap(),
        Some(records[0].clone())
    );
    let q = client.prepare(receiver(), 1).unwrap();
    let a = server.respond(q.body()).unwrap();
    assert_eq!(client.decode(q, &a).unwrap(), Some(records[1].clone()));
    let q = client.prepare(receiver(), 2).unwrap();
    let a = server.respond(q.body()).unwrap();
    assert!(
        client.decode(q, &a).is_err(),
        "missing continuation is not absence"
    );

    let mut anchor = accepted();
    anchor.hash[0] ^= 1;
    assert!(Client::new(server.manifest().clone(), server.public(), anchor).is_err());
    anchor = accepted();
    anchor.required_start = 99;
    assert!(Client::new(server.manifest().clone(), server.public(), anchor).is_err());
    let mut public = server.public().to_vec();
    public[0] ^= 1;
    assert!(Client::new(server.manifest().clone(), &public, accepted()).is_err());
    let mut stale = server.manifest().clone();
    stale.directory.salt[0] ^= 1;
    let stale = Client::new(stale, server.public(), accepted()).unwrap();
    let q = stale.prepare(receiver(), 0).unwrap();
    assert!(matches!(server.respond(q.body()), Err(Error::Revision)));
    assert!(server.respond(&[]).is_err());
    let mut malformed = client.prepare(receiver(), 0).unwrap().body().to_vec();
    malformed[52..60].fill(255);
    assert!(server.respond(&malformed).is_err());
    println!(
        "public_bytes={} query_bytes={} response_bytes={}",
        server.public().len(),
        receiver_pir::query_bytes(MIN_ROWS).unwrap(),
        receiver_pir::response_bytes(MIN_ROWS).unwrap()
    );
}

#[test]
fn reject_corrupt_rows_before_preprocessing() {
    let mut snapshot = Snapshot::build(manifest(MIN_ROWS), &[]).unwrap();
    snapshot.data[0] ^= 1;
    assert!(matches!(Server::new(snapshot), Err(Error::Malformed)));
    assert!(matches!(
        Server::new(Snapshot::build(manifest(4096), &[]).unwrap()),
        Err(Error::Unsupported)
    ));
}

#[test]
fn every_growth_geometry_roundtrips_above_the_previous_capacity() {
    for rows in [16_384, 32_768, receiver_pir::MAX_ROWS] {
        let mut m = manifest(rows);
        // Select the upper half so truncating to the previous geometry cannot pass.
        while receiver_directory::snapshot::row_for(&m, &receiver(), 0).unwrap() < rows as usize / 2
        {
            m.salt[0] = m.salt[0].wrapping_add(1);
        }
        let records = [record(0, 2), record(1, 2)];
        let server = Server::new(Snapshot::build(m, &records).unwrap()).unwrap();
        let client = Client::new(server.manifest().clone(), server.public(), accepted()).unwrap();
        assert_eq!(
            server.public().len(),
            receiver_pir::public_bytes(rows).unwrap()
        );
        for record in records {
            let q = client.prepare(receiver(), record.page).unwrap();
            assert_eq!(q.body().len(), receiver_pir::query_bytes(rows).unwrap());
            let answer = server.respond(q.body()).unwrap();
            assert_eq!(answer.len(), receiver_pir::response_bytes(rows).unwrap());
            assert_eq!(client.decode(q, &answer).unwrap(), Some(record));
        }
        let mut wrong_size = client.prepare(receiver(), 0).unwrap().body().to_vec();
        wrong_size.truncate(receiver_pir::query_bytes(MIN_ROWS).unwrap());
        assert!(matches!(server.respond(&wrong_size), Err(Error::Malformed)));
    }
}

#[test]
fn geometry_contract_and_transport_cost_use_the_same_bounds() {
    for rows in [0, 1, 4096, 8193, 131072, u32::MAX] {
        assert!(receiver_pir::validate_rows(rows).is_err());
        assert!(receiver_pir::query_bytes(rows).is_err());
    }
    for rows in [MIN_ROWS, 16_384, 32_768, receiver_pir::MAX_ROWS] {
        let m = receiver_pir::Manifest {
            protocol: receiver_pir::PROTOCOL.into(),
            directory: manifest(rows),
            public_digest: [0; 32],
        };
        m.validate().unwrap();
        let per_lookup =
            receiver_pir::query_bytes(rows).unwrap() + receiver_pir::response_bytes(rows).unwrap();
        let crossover =
            (rows as usize * receiver_directory::snapshot::ROW_BYTES).div_ceil(per_lookup);
        assert!(!receiver_pir::transport::prefer_directory_file(&m, crossover - 1).unwrap());
        assert!(receiver_pir::transport::prefer_directory_file(&m, crossover).unwrap());
    }
}
