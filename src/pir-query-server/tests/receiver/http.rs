//! Receiver PIR evaluation and the HTTP service that wallets reach through a [`Transport`].
#[path = "../../../receiver/receiver-store/tests/common/mod.rs"]
mod common;
#[path = "../../../receiver/receiver-pir-server/tests/common/history.rs"]
mod history;
#[path = "../../../receiver/receiver-pir-server/tests/common/prepared.rs"]
mod prepared;
#[path = "../../../receiver/receiver-pir-server/tests/common/router.rs"]
mod router;
use common::{manifest, receiver, record};
use history::{history, Accepted, Session};
use receiver_pir_server::{Publication, Publications};
use router::router_with_publications;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use zakura_pir_client::receiver::transport::{InventoryClient, Transport};
use zakura_pir_client::receiver::Client;
use zakura_pir_publisher::receiver::snapshot::SnapshotPublisherExt as _;
use zakura_pir_query_server::receiver::server::Server;
use zakura_pir_query_server::receiver::session::Error;
use zakura_pir_query_server::receiver::session::MIN_ROWS;
use zakura_pir_query_server::receiver::snapshot::Snapshot;

/// The chain anchor that [`manifest`] ends at.
fn accepted() -> Accepted {
    Accepted {
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
    let mut manifest = manifest(rows);
    // Positions start at 200; a long history needs room after them.
    manifest.end_position = manifest.end_position.max(200 + u64::from(count));
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
    serve_publication(prepared::prepare_payments(snapshot).unwrap()).await
}
/// Serve one prepared publication through the production router.
async fn serve_publication(publication: Publication) -> Running {
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    serve_router(router_with_publications(publications)).await
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
    assert!(publications.publish(prepared::prepare_payments(snapshot(2)).unwrap(), 0));
    let server = serve_router(router_with_publications(publications.clone())).await;
    let old = connect(&server.origin, Http(http()), accepted())
        .await
        .unwrap();
    let old_id = hex::encode(old.manifest().id().unwrap());
    let old_head = http()
        .get(format!("{}/v1/receiver/inventory", server.origin))
        .send()
        .await
        .unwrap()
        .json::<zakura_pir_query_server::receiver::inventory::Head>()
        .await
        .unwrap();
    let mut next = snapshot_rows(2, MIN_ROWS * 2);
    next.manifest.end_height = 102;
    next.manifest.end_hash = [9; 32];
    // Empty canonical extension: old payments remain unchanged.
    assert!(publications.publish(prepared::prepare_payments(next.clone()).unwrap(), 0));
    let public = http()
        .get(format!("{}/v1/receiver/public/{old_id}", server.origin))
        .send()
        .await
        .unwrap();
    assert!(public.status().is_success());
    assert_eq!(history(&old, receiver()).await.unwrap().len(), 2);
    let mut anchor = accepted();
    anchor.height = 102;
    anchor.hash = [9; 32];
    let new = connect(&server.origin, Http(http()), anchor).await.unwrap();
    assert_eq!(history(&new, receiver()).await.unwrap().len(), 2);

    // Preparation started before the canonical guard detected the fork.
    let preparing_epoch = publications.epoch();
    let prepared = prepared::prepare_payments(next).unwrap();
    publications.revoke();
    assert!(!publications.publish(prepared, preparing_epoch));
    assert_eq!(
        http()
            .get(format!("{}/v1/receiver/inventory", server.origin))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    // A captured inventory's chunks are revoked with its sessions.
    assert_eq!(
        http()
            .get(format!(
                "{}/v1/receiver/inventory/{}/0",
                server.origin,
                hex::encode(old_head.inventory)
            ))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::GONE
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
    assert!(matches!(old.witness(0).await, Err(Error::Revision)));
    assert!(matches!(
        history(&new, receiver()).await,
        Err(Error::Revision)
    ));

    // Canonical replacement drops the orphaned payment entirely.
    let mut replacement = snapshot(0);
    replacement.manifest.end_height = 102;
    replacement.manifest.end_hash = [10; 32];
    assert!(publications.publish(
        prepared::prepare_payments(replacement).unwrap(),
        publications.epoch()
    ));
    anchor.hash = [10; 32];
    let recovered = connect(&server.origin, Http(http()), anchor).await.unwrap();
    assert!(history(&recovered, receiver()).await.unwrap().is_empty());
}
/// Whether `response` forbids caching it.
fn no_store(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(reqwest::header::CACHE_CONTROL)
        .is_some_and(|value| value == "no-store")
}

/// Every session not served now is 410, however many were retired since, and refusals
/// are `no-store`: a revoked session's 410 must not outlive the republication of the
/// same deterministic session id, which then serves again at the same URL.
#[tokio::test]
async fn refusals_are_not_cached_and_a_republished_session_serves_again() {
    let snapshot = snapshot(1);
    let pir = prepared::prepare_payments(snapshot.clone()).unwrap();
    let id = hex::encode(
        pir.inventory().shards()[0].segments[0]
            .payments
            .id()
            .unwrap(),
    );
    let publications = Publications::default();
    assert!(publications.publish(pir, 0));
    let server = serve_router(router_with_publications(publications.clone())).await;
    let get = |path: String| http().get(format!("{}{path}", server.origin)).send();
    let unknown = get(format!("/v1/receiver/public/{}", "00".repeat(32)))
        .await
        .unwrap();
    assert_eq!(unknown.status(), reqwest::StatusCode::GONE);
    assert!(no_store(&unknown));
    publications.revoke();
    // More retirements than the server ever remembered.
    for salt in 0..9 {
        let mut m = manifest(MIN_ROWS);
        m.salt[0] = salt;
        let other = prepared::prepare_payments(Snapshot::build(m, &[]).unwrap()).unwrap();
        assert!(publications.publish(other, publications.epoch()));
        publications.revoke();
    }
    let public = format!("/v1/receiver/public/{id}");
    let gone = get(public.clone()).await.unwrap();
    assert_eq!(gone.status(), reqwest::StatusCode::GONE);
    assert!(no_store(&gone));

    // The identical publication has the same session id and serves the same URL.
    let expected = Server::new(snapshot.clone()).unwrap().public().to_vec();
    let pir = prepared::prepare_payments(snapshot).unwrap();
    assert!(publications.publish(pir, publications.epoch()));
    let restored = get(public).await.unwrap();
    assert!(restored.status().is_success() && no_store(&restored));
    assert_eq!(restored.bytes().await.unwrap(), expected);
}

/// Health serves each publication's own report with its ID.
#[tokio::test]
async fn health_serves_the_owners_report() {
    let publications = Publications::default();
    assert!(publications.publish(
        prepared::prepare_payments(snapshot(0))
            .unwrap()
            .with_report(serde_json::json!({"missing_payouts": 0})),
        0
    ));
    let server = serve_router(router_with_publications(publications.clone())).await;
    let health = || async {
        http()
            .get(format!("{}/v1/receiver/health", server.origin))
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()
    };
    assert_eq!(health().await["indexer"]["missing_payouts"], 0);
    let mut next = snapshot(1);
    next.manifest.end_height = 102;
    next.manifest.end_hash = [9; 32];
    let next = prepared::prepare_payments(next).unwrap();
    let id = hex::encode(next.id());
    assert!(publications.publish(
        next.with_report(serde_json::json!({"missing_payouts": 3})),
        0
    ));
    let health = health().await;
    assert_eq!(health["serving"], id.as_str());
    assert_eq!(health["indexer"]["missing_payouts"], 3);
}
/// Health adds the owner's process-local fields, read on each request, without letting
/// them replace its own.
#[tokio::test]
async fn health_reports_the_owners_fields_beside_its_own() {
    let reads = std::sync::Arc::new(AtomicUsize::new(0));
    let fields: pir_query_server::receiver::HealthFields = {
        let reads = reads.clone();
        std::sync::Arc::new(move || {
            let value = serde_json::json!({"reads": reads.load(Ordering::SeqCst), "serving": "x"});
            value.as_object().unwrap().clone()
        })
    };
    let server = serve_router(pir_query_server::receiver::router_with_health(
        Publications::default(),
        fields,
    ))
    .await;
    let health = || async {
        http()
            .get(format!("{}/v1/receiver/health", server.origin))
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()
    };
    let first = health().await;
    assert_eq!(
        (&first["reads"], &first["serving"]),
        (&0.into(), &serde_json::Value::Null)
    );
    reads.store(2, Ordering::SeqCst);
    assert_eq!(health().await["reads"], 2);
}
/// A displaced inventory keeps serving its head's chunks and its sessions for its
/// retention, so a wallet that captured it can finish, then answers 410.
#[tokio::test]
async fn a_displaced_inventory_serves_until_its_retention_ends() {
    let publications = Publications::with_retention(Duration::from_millis(1500));
    let publish = |height: u32| {
        let mut m = manifest(MIN_ROWS);
        (m.end_height, m.end_hash) = (height, [height as u8; 32]);
        // Below the moving tip, so the record need not repeat its hash.
        let mut paid = record(0, 1);
        paid.payment.height = 100;
        let next = Snapshot::build(m, &[paid]).unwrap();
        publications.publish(prepared::prepare_payments(next).unwrap(), 0)
    };
    assert!(publish(101));
    let server = serve_router(router_with_publications(publications.clone())).await;
    let mut anchor = accepted();
    (anchor.height, anchor.hash) = (101, [101; 32]);
    let old = connect(&server.origin, Http(http()), anchor).await.unwrap();
    let old_id = hex::encode(old.client.inventory().head().inventory);
    assert!(publish(102));
    assert!(publications.ready_at().is_none());
    assert_eq!(publications.anchors(), [(102, [102; 32]), (101, [101; 32])]);
    assert_eq!(history(&old, receiver()).await.unwrap().len(), 1);
    let chunk = format!("{}/v1/receiver/inventory/{old_id}/0", server.origin);
    assert!(http()
        .get(&chunk)
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert_eq!(publications.anchors(), [(102, [102; 32])]);
    let gone = http().get(&chunk).send().await.unwrap();
    assert_eq!(gone.status(), reqwest::StatusCode::GONE);
    assert!(matches!(
        history(&old, receiver()).await,
        Err(Error::Revision)
    ));
}
/// Health reports the shared process identity and the served revision, and metrics count
/// requests by route category without session IDs.
#[tokio::test]
async fn health_and_metrics_follow_the_serving_contract() {
    let snapshot = snapshot(1);
    let revision = snapshot.manifest.revision().unwrap();
    let server = serve(snapshot).await;
    let health: serde_json::Value = http()
        .get(format!("{}/v1/receiver/health", server.origin))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let identity = pir_control::Identity::process();
    assert_eq!(health["identity"]["incarnation"], identity.incarnation);
    assert!(health["serving"].as_str().is_some());
    let session = connect(&server.origin, Http(http()), accepted())
        .await
        .unwrap();
    assert_eq!(session.manifest().directory.revision().unwrap(), revision);
    let metrics = http()
        .get(format!("{}/metrics", server.origin))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("pir_http_arrivals_total{endpoint=\"receiver_inventory\"} 2"));
    assert!(metrics.contains("pir_http_arrivals_total{endpoint=\"receiver_health\"} 1"));
    for family in [
        "arrivals_total",
        "inflight",
        "cancelled_total",
        "request_body_bytes_total",
        "response_body_bytes_total",
        "body_errors_total",
        "incomplete_responses_total",
    ] {
        assert!(
            metrics.contains(&format!(
                "pir_http_{family}{{endpoint=\"receiver_health\"}}"
            )),
            "{family}"
        );
    }
    assert!(
        metrics.contains("pir_http_responses_total{endpoint=\"receiver_health\",code=\"2xx\"} 1")
    );
    assert!(metrics
        .contains("pir_http_duration_seconds_bucket{endpoint=\"receiver_health\",le=\"0.001\"}"));
    assert!(metrics.contains("pir_http_duration_seconds_count{endpoint=\"receiver_health\"} 1"));
    assert!(!metrics.contains(&hex::encode(revision)));
    server.task.abort();
}

/// A client at its cap of queries in flight, uploads included, is refused with 429 and
/// `Retry-After: 1`, as Enhance refuses overload; other clients are still admitted.
#[tokio::test]
async fn a_client_over_its_cap_is_told_to_retry() {
    use tokio::io::AsyncWriteExt;
    let server = serve(snapshot(1)).await;
    let address = server.origin.trim_start_matches("http://").to_owned();
    let mut uploads = Vec::new();
    for _ in 0..2 {
        let mut stream = tokio::net::TcpStream::connect(&address).await.unwrap();
        stream
            .write_all(
                b"POST /v1/receiver/query HTTP/1.1\r\nHost: x\r\nX-Forwarded-For: 10.0.0.1\r\n\
                  Content-Length: 1000\r\n\r\nRPQ1",
            )
            .await
            .unwrap();
        uploads.push(stream);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let query = |client: &'static str| {
        http()
            .post(format!("{}/v1/receiver/query", server.origin))
            .header("x-forwarded-for", client)
            .body(vec![0; 8])
            .send()
    };
    let refused = query("10.0.0.1").await.unwrap();
    assert_eq!(refused.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(refused.headers()["retry-after"], "1");
    assert!(no_store(&refused));
    assert_eq!(
        query("10.0.0.2").await.unwrap().status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    drop(uploads);
}

fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

/// A test transport over reqwest; wallets bring their own. It maps 410 to [`Error::Revision`].
struct Http(reqwest::Client);

impl Transport for Http {
    async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
        read(self.0.get(url), limit).await
    }
    async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
        read(self.0.post(url).body(body), limit).await
    }
}

/// Send `request` and return its successful body, failing as soon as it exceeds `limit`
/// bytes.
async fn read(request: reqwest::RequestBuilder, limit: usize) -> Result<Vec<u8>, Error> {
    let failed = |e: reqwest::Error| Error::Transport(e.to_string());
    let mut response = request.send().await.map_err(failed)?;
    match response.status() {
        reqwest::StatusCode::GONE => return Err(Error::Revision),
        status if !status.is_success() => return Err(Error::Transport(status.to_string())),
        _ => {}
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(failed)? {
        if body.len() + chunk.len() > limit {
            return Err(Error::Malformed);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// The transport refuses a chunked body once it crosses the limit, without waiting for
/// the server to end it.
#[tokio::test]
async fn the_transport_stops_reading_at_its_limit() {
    use tokio::io::AsyncWriteExt;
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", socket.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut stream, _) = socket.accept().await.unwrap();
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                  10\r\n0123456789abcdef\r\n10\r\n0123456789abcdef\r\n",
            )
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });
    let transport = Http(http());
    let read = Transport::get(&transport, &url, 20);
    let result = tokio::time::timeout(Duration::from_secs(5), read).await;
    assert!(matches!(result, Ok(Err(Error::Malformed))));
    server.abort();
}

/// Connect over PIR to the publication `origin` advertises, accepted at `accepted`.
async fn connect<T: Transport>(
    origin: &str,
    http: T,
    accepted: Accepted,
) -> Result<Session<T>, Error> {
    history::connect(origin, http, accepted).await
}

#[tokio::test]
async fn retrieve_complete_history_and_enforce_limits_over_http() {
    let server = serve(snapshot(2)).await;
    let client = connect(&server.origin, Http(http()), accepted())
        .await
        .unwrap();
    let payments = history(&client, receiver()).await.unwrap();
    assert_eq!(payments.len(), 2);
    assert_eq!(payments[0].position, 200);
    assert_eq!(payments[1].position, 201);
    // A client refuses a publication that does not end at the wallet's anchor.
    let mut wrong = accepted();
    wrong.hash[0] ^= 1;
    assert!(connect(&server.origin, Http(http()), wrong).await.is_err());
    let oversized = http()
        .post(format!("{}/v1/receiver/query", server.origin))
        .body(vec![
            0;
            zakura_pir_query_server::receiver::native::query_bytes(
                zakura_pir_query_server::receiver::session::MAX_ROWS
            )
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
    let client = connect(&absent.origin, Http(http()), accepted())
        .await
        .unwrap();
    assert!(history(&client, receiver()).await.unwrap().is_empty());
}

/// A transport that counts requests and records any row file request.
struct Counted {
    http: Http,
    gets: AtomicUsize,
    posts: AtomicUsize,
    row_files: AtomicUsize,
}

impl Transport for Counted {
    async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        if url.contains("/v1/receiver/rows/") {
            self.row_files.fetch_add(1, Ordering::Relaxed);
        }
        Transport::get(&self.http, url, limit).await
    }
    async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
        self.posts.fetch_add(1, Ordering::Relaxed);
        Transport::post(&self.http, url, body, limit).await
    }
}

/// A history longer than the old 16-page PIR limit completes over PIR alone: one
/// query per page, no row file request, and no GET beyond the inventory head, its one
/// chunk and the two sessions' setup.
#[tokio::test]
async fn long_histories_use_only_pir() {
    const PAGES: u32 = 20;
    let server = serve(snapshot(PAGES)).await;
    let host = Counted {
        http: Http(http()),
        gets: AtomicUsize::new(0),
        posts: AtomicUsize::new(0),
        row_files: AtomicUsize::new(0),
    };
    let client = connect(&server.origin, &host, accepted()).await.unwrap();
    let payments = history(&client, receiver()).await.unwrap();
    assert_eq!(payments.len(), PAGES as usize);
    assert!(payments.windows(2).all(|p| p[0].position < p[1].position));
    assert_eq!(host.row_files.load(Ordering::Relaxed), 0);
    assert_eq!(host.gets.load(Ordering::Relaxed), 4);
    assert_eq!(host.posts.load(Ordering::Relaxed), PAGES as usize);
}

/// The companion witness table answers an ordinal over the shared query route under
/// its own session, which the inventory lists beside the payment session; this crate never
/// interprets payloads, and a wallet verifies them.
#[tokio::test]
async fn companion_witness_payloads_are_read_over_pir() {
    let snapshot = snapshot(2);
    let payloads = [[7; 512], [9; 512]];
    let table = prepared::companion_table(&snapshot, &payloads);
    let server = serve_publication(prepared::prepare(snapshot, table).unwrap()).await;
    let client = connect(&server.origin, Http(http()), accepted())
        .await
        .unwrap();
    assert_eq!(client.witness(1).await.unwrap(), payloads[1]);
    assert_eq!(client.witness(0).await.unwrap(), payloads[0]);
    // An ordinal past the table's payloads is never sent.
    assert!(client.witness(2).await.is_err());
}

#[test]
fn encrypted_publication_roundtrip_and_fail_closed() {
    let records = [record(0, 2), record(1, 2)];
    let server = Server::new(Snapshot::build(manifest(MIN_ROWS), &records).unwrap()).unwrap();
    let client = Client::new(server.manifest().clone(), server.public()).unwrap();
    let first = client.prepare(receiver(), 0).unwrap();
    let second = client.prepare(receiver(), 0).unwrap();
    assert_ne!(
        first.body(),
        second.body(),
        "fresh encryption even for the same receiver"
    );
    assert_eq!(
        first.body().len(),
        zakura_pir_query_server::receiver::native::query_bytes(MIN_ROWS).unwrap()
    );
    let answer = server.respond(first.body()).unwrap();
    assert_eq!(
        answer.len(),
        zakura_pir_query_server::receiver::native::response_bytes(MIN_ROWS).unwrap()
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

    let mut public = server.public().to_vec();
    public[0] ^= 1;
    assert!(Client::new(server.manifest().clone(), &public).is_err());
    let mut stale = server.manifest().clone();
    stale.directory.salt[0] ^= 1;
    let stale = Client::new(stale, server.public()).unwrap();
    let q = stale.prepare(receiver(), 0).unwrap();
    assert!(matches!(server.respond(q.body()), Err(Error::Revision)));
    assert!(server.respond(&[]).is_err());
    let mut malformed = client.prepare(receiver(), 0).unwrap().body().to_vec();
    malformed[0] ^= 1;
    assert!(matches!(server.respond(&malformed), Err(Error::Malformed)));
    println!(
        "public_bytes={} query_bytes={} response_bytes={}",
        server.public().len(),
        zakura_pir_query_server::receiver::native::query_bytes(MIN_ROWS).unwrap(),
        zakura_pir_query_server::receiver::native::response_bytes(MIN_ROWS).unwrap()
    );
}

#[test]
fn reject_corrupt_rows_before_preprocessing() {
    let mut snapshot = Snapshot::build(manifest(MIN_ROWS), &[]).unwrap();
    snapshot.data[0] ^= 1;
    assert!(matches!(
        Server::new(snapshot),
        Err(Error::Directory(
            zakura_pir_query_server::receiver::Error::Malformed
        ))
    ));
    // No PIR profile serves a smaller table.
    let small = Snapshot::build(manifest(MIN_ROWS / 2), &[]).unwrap();
    assert!(matches!(Server::new(small), Err(Error::Unsupported)));
}

#[test]
fn every_growth_geometry_roundtrips_above_the_previous_capacity() {
    for rows in [
        16_384,
        32_768,
        zakura_pir_query_server::receiver::session::MAX_ROWS,
    ] {
        let mut m = manifest(rows);
        // Select the upper half so truncating to the previous geometry cannot pass.
        while zakura_pir_query_server::receiver::snapshot::row_for(&m, &receiver(), 0).unwrap()
            < rows as usize / 2
        {
            m.salt[0] = m.salt[0].wrapping_add(1);
        }
        let records = [record(0, 2), record(1, 2)];
        let server = Server::new(Snapshot::build(m, &records).unwrap()).unwrap();
        let client = Client::new(server.manifest().clone(), server.public()).unwrap();
        assert_eq!(
            server.public().len(),
            zakura_pir_query_server::receiver::native::public_bytes(rows).unwrap()
        );
        for record in records {
            let q = client.prepare(receiver(), record.page).unwrap();
            assert_eq!(
                q.body().len(),
                zakura_pir_query_server::receiver::native::query_bytes(rows).unwrap()
            );
            let answer = server.respond(q.body()).unwrap();
            assert_eq!(
                answer.len(),
                zakura_pir_query_server::receiver::native::response_bytes(rows).unwrap()
            );
            assert_eq!(client.decode(q, &answer).unwrap(), Some(record));
        }
        let mut wrong_size = client.prepare(receiver(), 0).unwrap().body().to_vec();
        wrong_size
            .truncate(zakura_pir_query_server::receiver::native::query_bytes(MIN_ROWS).unwrap());
        assert!(matches!(server.respond(&wrong_size), Err(Error::Malformed)));
    }
}

#[test]
fn geometry_contract_accepts_only_supported_row_counts() {
    for rows in [0, 1, 4096, 8193, 131072, u32::MAX] {
        assert!(zakura_pir_query_server::receiver::session::validate_rows(rows).is_err());
        assert!(zakura_pir_query_server::receiver::native::query_bytes(rows).is_err());
    }
    for rows in [
        MIN_ROWS,
        16_384,
        32_768,
        zakura_pir_query_server::receiver::session::MAX_ROWS,
    ] {
        let m = zakura_pir_query_server::receiver::session::Manifest {
            protocol: zakura_pir_query_server::receiver::session::PROTOCOL.into(),
            directory: manifest(rows),
            public_digest: [0; 32],
        };
        m.validate().unwrap();
    }
}

/// A sealed shard of two segments covering the same blocks, as an oversized block
/// becomes, serves each receiver's pages from its own segment through the library
/// client; its filter is served by digest and matches both receivers; a chunk the
/// inventory lacks is 404 and an inventory not served is 410.
#[tokio::test]
async fn a_multi_segment_shard_and_its_filter_serve_through_the_library_client() {
    use zakura_pir_query_server::receiver::shards::{Coverage, ShardFilter};
    let other = common::seeded_receiver(1);
    let first = Snapshot::build(manifest(MIN_ROWS), &[record(0, 1)]).unwrap();
    let mut second_record = record(0, 1);
    second_record.receiver = other;
    (second_record.payment.position, second_record.payment.txid) = (201, [9; 32]);
    second_record.payment.tx_index = 2;
    let second = Snapshot::build(manifest(MIN_ROWS), &[second_record.clone()]).unwrap();
    let coverage = Coverage {
        start_height: 100,
        end_height: 101,
        end_hash: [3; 32],
    };
    let filter = ShardFilter::build([1; 32], 0, coverage, &[receiver(), other]).unwrap();
    let digest = filter.digest();
    let tables = vec![
        (first.clone(), prepared::placeholder(&first)),
        (second.clone(), prepared::placeholder(&second)),
    ];
    let publication = prepared::inventory(vec![(true, tables, Some(filter.clone()))]).unwrap();
    let inventory = hex::encode(publication.id());
    let server = serve_publication(publication).await;
    let session = connect(&server.origin, Http(http()), accepted())
        .await
        .unwrap();
    let client = &session.client;
    assert_eq!(client.inventory().shards()[0].segments.len(), 2);
    let ours = history::inventory_history(client, receiver())
        .await
        .unwrap();
    assert_eq!(ours, [record(0, 1).payment]);
    let theirs = history::inventory_history(client, other).await.unwrap();
    assert_eq!(theirs, [second_record.payment]);
    let served = http()
        .get(format!(
            "{}/v1/receiver/shard-filters/{}",
            server.origin,
            hex::encode(digest)
        ))
        .send()
        .await
        .unwrap();
    assert!(no_store(&served));
    assert_eq!(served.bytes().await.unwrap(), filter.bytes());
    let fetched = InventoryClient::new(
        &server.origin,
        Http(http()),
        client.inventory().clone(),
        true,
    );
    let fetched = fetched.filter(0).await.unwrap();
    assert_eq!(fetched.matches(&[receiver(), other]), [true, true]);
    let origin = server.origin.clone();
    let status = |path: String| {
        let origin = origin.clone();
        async move {
            http()
                .get(format!("{origin}{path}"))
                .send()
                .await
                .unwrap()
                .status()
        }
    };
    assert_eq!(
        status(format!("/v1/receiver/inventory/{inventory}/1")).await,
        reqwest::StatusCode::NOT_FOUND
    );
    let unknown = "00".repeat(32);
    assert_eq!(
        status(format!("/v1/receiver/inventory/{unknown}/0")).await,
        reqwest::StatusCode::GONE
    );
    assert_eq!(
        status(format!("/v1/receiver/shard-filters/{unknown}")).await,
        reqwest::StatusCode::GONE
    );
}

/// A stored shard filter keeps no bytes in the inventory: the first request reads its
/// file into the table budget, later ones reuse the cached copy, and a file that no
/// longer holds the digest is a server error rather than wrong bytes.
#[tokio::test]
async fn a_stored_shard_filter_is_read_into_the_table_budget() {
    use receiver_pir_server::tables::FilterSource;
    use std::sync::Arc;
    use zakura_pir_query_server::receiver::shards::{Coverage, ShardFilter};
    let snapshot = Snapshot::build(manifest(MIN_ROWS), &[record(0, 1)]).unwrap();
    let coverage = Coverage {
        start_height: 100,
        end_height: 101,
        end_hash: [3; 32],
    };
    let filter = ShardFilter::build([1; 32], 0, coverage, &[receiver()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("filter");
    std::fs::write(&path, filter.bytes()).unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(FilterSource::stored(
        filter.digest(),
        filter.bytes().len() as u32,
        filter.receivers(),
        {
            let (path, reads) = (path.clone(), reads.clone());
            move || {
                reads.fetch_add(1, Ordering::SeqCst);
                std::fs::read(&path).map_err(|e| Error::Transport(e.to_string()))
            }
        },
    ));
    assert_eq!(source.held_bytes(), 0);
    let segment = prepared::segment(snapshot.clone(), prepared::placeholder(&snapshot)).unwrap();
    let publication =
        Publication::assemble([1; 32], 0, vec![(true, vec![segment], Some(source))]).unwrap();
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    let tables = publications.tables();
    let resident = tables.resident();
    let server = serve_router(router_with_publications(publications)).await;
    let url = format!(
        "{}/v1/receiver/shard-filters/{}",
        server.origin,
        hex::encode(filter.digest())
    );
    for _ in 0..2 {
        let served = http().get(&url).send().await.unwrap();
        assert!(served.status().is_success());
        assert_eq!(served.bytes().await.unwrap(), filter.bytes());
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(tables.resident(), resident + filter.bytes().len());
    // A changed file is refused when it is next read.
    let other = ShardFilter::build([1; 32], 0, coverage, &[common::seeded_receiver(1)]).unwrap();
    std::fs::write(&path, other.bytes()).unwrap();
    let corrupt = Arc::new(FilterSource::stored(
        filter.digest(),
        filter.bytes().len() as u32,
        filter.receivers(),
        move || std::fs::read(&path).map_err(|e| Error::Transport(e.to_string())),
    ));
    let fresh = receiver_pir_server::tables::Tables::new(1 << 30);
    assert!(fresh.filter(&corrupt).is_err());
    assert_eq!(fresh.resident(), 0);
}

/// Wallets reach every receiver operation at the canonical `/receiver/v1/<operation>` and
/// at the deployed `/v1/receiver/<operation>`, with identical answers: the inventory head
/// and a chunk, both sessions' public setup, a shard filter, a payment and a witness
/// query, and the block-filter head and a batch. Operator routes stay outside the
/// canonical namespace.
#[tokio::test]
async fn canonical_and_legacy_receiver_routes_answer_identically() {
    use zakura_pir_query_server::receiver::blocks::{BlockFilter, BlockId};
    use zakura_pir_query_server::receiver::shards::{Coverage, ShardFilter};
    let snapshot = snapshot(2);
    let table = prepared::companion_table(&snapshot, &[[7; 512], [9; 512]]);
    let coverage = Coverage {
        start_height: 100,
        end_height: 101,
        end_hash: [3; 32],
    };
    let filter = ShardFilter::build([1; 32], 0, coverage, &[receiver()]).unwrap();
    let publication =
        prepared::inventory(vec![(true, vec![(snapshot, table)], Some(filter.clone()))]).unwrap();
    let inventory = hex::encode(publication.id());
    let segment = &publication.segments()[0];
    let (payments, witnesses) = (segment.payments.clone(), segment.witnesses.clone());
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    let tip = BlockId {
        height: 101,
        hash: [3; 32],
    };
    let block = BlockFilter::build(&[1; 32], tip, [2; 32], [&receiver()]).unwrap();
    let window = receiver_pir_server::blocks::BlockWindow::new([1; 32], 0, vec![block]).unwrap();
    assert!(publications.publish_filters(window, publications.epoch()));
    let server = serve_router(router_with_publications(publications)).await;

    let payment_query = Client::new(
        payments.payment_manifest().unwrap().clone(),
        payments.public(),
    )
    .unwrap()
    .prepare(receiver(), 0)
    .unwrap();
    let witness_query = zakura_pir_client::receiver::WitnessClient::new(
        witnesses.witness_manifest().unwrap().clone(),
        witnesses.public(),
    )
    .unwrap()
    .prepare(1)
    .unwrap();
    let gets = [
        "/inventory".to_owned(),
        format!("/inventory/{inventory}/0"),
        format!("/public/{}", hex::encode(payments.id())),
        format!("/public/{}", hex::encode(witnesses.id())),
        format!("/shard-filters/{}", hex::encode(filter.digest())),
        "/block-filters/head".to_owned(),
        "/block-filters/0/101/1".to_owned(),
    ];
    let answer = |response: reqwest::Response| async move {
        let status = response.status();
        let headers = [
            header_value(&response, "content-type"),
            header_value(&response, "cache-control"),
        ];
        (status, headers, response.bytes().await.unwrap())
    };
    for operation in &gets {
        let canonical = answer(
            http()
                .get(format!("{}/receiver/v1{operation}", server.origin))
                .send()
                .await
                .unwrap(),
        )
        .await;
        let legacy = answer(
            http()
                .get(format!("{}/v1/receiver{operation}", server.origin))
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(canonical.0, reqwest::StatusCode::OK, "{operation}");
        assert_eq!(canonical, legacy, "{operation}");
    }
    for body in [payment_query.body(), witness_query.body()] {
        let canonical = answer(
            http()
                .post(format!("{}/receiver/v1/query", server.origin))
                .body(body.to_vec())
                .send()
                .await
                .unwrap(),
        )
        .await;
        let legacy = answer(
            http()
                .post(format!("{}/v1/receiver/query", server.origin))
                .body(body.to_vec())
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(canonical.0, reqwest::StatusCode::OK);
        assert_eq!(canonical, legacy);
    }

    // Operator routes keep their own paths and never join the canonical namespace.
    for path in ["/v1/receiver/health", "/metrics"] {
        let response = http()
            .get(format!("{}{path}", server.origin))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK, "{path}");
    }
    for path in [
        "/receiver/v1/health",
        "/receiver/v1/metrics",
        "/receiver/v1/v1/receiver/health",
        "/receiver/v1/",
    ] {
        let response = http()
            .get(format!("{}{path}", server.origin))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND, "{path}");
    }

    // Both prefixes count under the same bounded labels.
    let metrics = http()
        .get(format!("{}/metrics", server.origin))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for (label, count) in [
        ("receiver_inventory", 4),
        ("receiver_public", 4),
        ("receiver_shard_filters", 2),
        ("receiver_block_filters", 4),
        ("receiver_query", 4),
    ] {
        let line = format!("pir_http_responses_total{{endpoint=\"{label}\",code=\"2xx\"}} {count}");
        assert!(metrics.contains(&line), "{line} in {metrics}");
    }
}

/// `name`'s value in `response`, if present.
fn header_value(response: &reqwest::Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .map(|value| value.to_str().unwrap().to_owned())
}
