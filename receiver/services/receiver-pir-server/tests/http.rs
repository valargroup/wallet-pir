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
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

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
    let mut manifest = manifest(rows);
    // Positions start at 200; a long history needs room after them.
    manifest.end_position = manifest.end_position.max(200 + u64::from(count));
    Snapshot::build(manifest, &records, &[]).unwrap()
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
    let mut old = connect(&server.origin, Http(http()), accepted(), 0)
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
    assert_eq!(old.lookup(receiver(), accepted()).await.unwrap().len(), 2);
    let mut anchor = accepted();
    anchor.height = 102;
    anchor.hash = [9; 32];
    let mut new = connect(&server.origin, Http(http()), anchor, 0)
        .await
        .unwrap();
    assert_eq!(new.lookup(receiver(), anchor).await.unwrap().len(), 2);

    let mut file = connect(&server.origin, Http(http()), anchor, 10_000)
        .await
        .unwrap();
    assert_eq!(file.lookup(receiver(), anchor).await.unwrap().len(), 2);

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
        new.lookup(receiver(), anchor).await,
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
    let mut recovered = connect(&server.origin, Http(http()), anchor, 0)
        .await
        .unwrap();
    assert!(recovered
        .lookup(receiver(), anchor)
        .await
        .unwrap()
        .is_empty());
}
/// A revocation aborts a session file still being sent, so a wallet in file mode
/// fails instead of installing it, while a file sent across an ordinary rotation
/// completes.
#[tokio::test]
async fn a_revocation_aborts_files_in_flight() {
    /// Reads the row file a chunk at a time, revoking after the first chunk once the
    /// server has filled the connection's buffers and paused.
    struct Revoking {
        http: Http,
        publications: Publications,
        received: AtomicUsize,
    }
    impl Transport for Revoking {
        async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
            if !url.contains("/rows/") {
                return Transport::get(&self.http, url, limit).await;
            }
            let failed = |e: reqwest::Error| Error::Transport(e.to_string());
            let mut response = self.http.0.get(url).send().await.map_err(failed)?;
            assert!(response.status().is_success());
            let mut body = Vec::new();
            body.extend(response.chunk().await.map_err(failed)?.unwrap());
            tokio::time::sleep(Duration::from_millis(200)).await;
            self.publications.revoke();
            let read = async {
                while let Some(chunk) = response.chunk().await? {
                    body.extend(chunk);
                }
                Ok(())
            };
            let result = read.await.map_err(failed);
            self.received.store(body.len(), Ordering::Relaxed);
            result.map(|()| body)
        }
        async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
            Transport::post(&self.http, url, body, limit).await
        }
    }
    let length = MIN_ROWS as usize * receiver_directory::snapshot::ROW_BYTES;
    let publications = Publications::default();
    assert!(publications.publish(
        Publication::new(Server::new(snapshot(2)).unwrap(), None).unwrap(),
        0
    ));
    let server = serve_router(receiver_pir_server::router_with_publications(
        publications.clone(),
    ))
    .await;
    let manifest = DirectoryClient::fetch_manifest(&server.origin, &Http(http()))
        .await
        .unwrap();
    let rows = format!(
        "{}/v1/receiver/rows/{}",
        server.origin,
        hex::encode(manifest.id().unwrap())
    );
    // A file in progress when its publication is displaced still completes.
    let mut response = http().get(&rows).send().await.unwrap();
    assert_eq!(response.content_length(), Some(length as u64));
    let mut received = response.chunk().await.unwrap().unwrap().len();
    let mut next = snapshot(2);
    next.manifest.end_height = 102;
    next.manifest.end_hash = [9; 32];
    assert!(publications.publish(
        Publication::new(Server::new(next).unwrap(), None).unwrap(),
        0
    ));
    while let Some(chunk) = response.chunk().await.unwrap() {
        received += chunk.len();
    }
    assert_eq!(received, length);
    // Revoking mid-transfer cuts the file short, and the wallet does not install it.
    let host = Revoking {
        http: Http(http()),
        publications: publications.clone(),
        received: AtomicUsize::new(0),
    };
    let result =
        DirectoryClient::connect_manifest(&server.origin, &host, accepted(), manifest, 10_000)
            .await;
    assert!(matches!(result, Err(Error::Transport(_))));
    let received = host.received.load(Ordering::Relaxed);
    assert!(
        received > 0 && received < length,
        "received {received} of {length}"
    );
}

/// Posts `body` to `url` and runs `during` while the test middleware of
/// [`a_revocation_stops_query_answers_not_yet_sent`] holds the successful response
/// between the handler's return and its body, then returns the complete answer the
/// client received, or its failure.
async fn held_query(
    url: &str,
    body: Vec<u8>,
    held: &(tokio::sync::Barrier, tokio::sync::Barrier),
    during: impl FnOnce(),
) -> reqwest::Result<Vec<u8>> {
    let url = url.to_owned();
    let answer = tokio::spawn(async move {
        let response = http().post(url).body(body).send().await?;
        Ok(response.error_for_status()?.bytes().await?.to_vec())
    });
    held.0.wait().await;
    during();
    held.1.wait().await;
    answer.await.unwrap()
}

/// A revocation after the query handler's own epoch check, but before the answer's
/// body is sent, still withholds the answer, even when the same publication is
/// republished meanwhile; an ordinary rotation does not.
#[tokio::test]
async fn a_revocation_stops_query_answers_not_yet_sent() {
    let held = std::sync::Arc::new((tokio::sync::Barrier::new(2), tokio::sync::Barrier::new(2)));
    let hold = {
        let held = held.clone();
        axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let held = held.clone();
                async move {
                    let query = request.uri().path() == "/v1/receiver/query";
                    let response = next.run(request).await;
                    if query && response.status().is_success() {
                        held.0.wait().await;
                        held.1.wait().await;
                    }
                    response
                }
            },
        )
    };
    let snapshot = snapshot(1);
    let pir = Server::new(snapshot.clone()).unwrap();
    let client = Client::new(pir.manifest().clone(), pir.public(), accepted()).unwrap();
    let publications = Publications::default();
    assert!(publications.publish(Publication::new(pir, None).unwrap(), 0));
    let server = serve_router(
        receiver_pir_server::router_with_publications(publications.clone()).layer(hold),
    )
    .await;
    let url = format!("{}/v1/receiver/query", server.origin);
    let republish = || {
        let publication = Publication::new(Server::new(snapshot.clone()).unwrap(), None);
        assert!(publications.publish(publication.unwrap(), publications.epoch()));
    };

    // A rotation keeps the epoch, so the displaced session's answer is delivered.
    let query = client.prepare(receiver(), 0).unwrap();
    let answer = held_query(&url, query.body().to_vec(), &held, || {
        let mut next = snapshot_rows(1, MIN_ROWS);
        next.manifest.end_height = 102;
        next.manifest.end_hash = [9; 32];
        let next = Publication::new(Server::new(next).unwrap(), None).unwrap();
        assert!(publications.publish(next, 0));
    })
    .await;
    assert_eq!(
        client.decode(query, &answer.unwrap()).unwrap(),
        Some(record(0, 1))
    );

    let query = client.prepare(receiver(), 0).unwrap();
    let answer = held_query(&url, query.body().to_vec(), &held, || publications.revoke()).await;
    assert!(answer.is_err());

    // Republishing the same session id does not rescue an answer selected before.
    republish();
    let query = client.prepare(receiver(), 0).unwrap();
    let answer = held_query(&url, query.body().to_vec(), &held, || {
        publications.revoke();
        republish();
    })
    .await;
    assert!(answer.is_err());

    // A query selected in the republished session's epoch is answered.
    let query = client.prepare(receiver(), 0).unwrap();
    let answer = held_query(&url, query.body().to_vec(), &held, || {}).await;
    assert_eq!(
        client.decode(query, &answer.unwrap()).unwrap(),
        Some(record(0, 1))
    );
}

/// Whether `response` forbids caching it.
fn no_store(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(reqwest::header::CACHE_CONTROL)
        .is_some_and(|value| value == "no-store")
}

/// Every refusal is `no-store`: a revoked session's 410 must not outlive the
/// republication of the same deterministic session id, which then serves again at the
/// same URL.
#[tokio::test]
async fn refusals_are_not_cached_and_a_republished_session_serves_again() {
    let snapshot = snapshot(1);
    let pir = Server::new(snapshot.clone()).unwrap();
    let client = Client::new(pir.manifest().clone(), pir.public(), accepted()).unwrap();
    let id = hex::encode(pir.manifest().id().unwrap());
    let publications = Publications::default();
    assert!(publications.publish(Publication::new(pir, None).unwrap(), 0));
    let server = serve_router(receiver_pir_server::router_with_publications(
        publications.clone(),
    ))
    .await;
    let origin = &server.origin;
    let get = |path: String| http().get(format!("{origin}{path}")).send();
    let query = |body: Vec<u8>| {
        http()
            .post(format!("{origin}/v1/receiver/query"))
            .body(body)
            .send()
    };
    let public = format!("/v1/receiver/public/{id}");
    let ok = get(public.clone()).await.unwrap();
    assert!(ok.status().is_success() && no_store(&ok));
    for (path, status) in [
        ("/v1/receiver/public/zz".to_owned(), 400),
        (format!("/v1/receiver/public/{}", "00".repeat(32)), 409),
        (format!("/v1/receiver/witness/{id}"), 503),
    ] {
        let refused = get(path.clone()).await.unwrap();
        assert_eq!(refused.status(), status, "{path}");
        assert!(no_store(&refused), "{path}");
    }
    let exact = client.prepare(receiver(), 0).unwrap().body().to_vec();
    let mut longer = exact.clone();
    longer.push(0);
    for (body, status) in [(vec![0; 8], 400), (longer, 413)] {
        let refused = query(body).await.unwrap();
        assert_eq!(refused.status(), status);
        assert!(no_store(&refused));
    }

    publications.revoke();
    let gone = get(public.clone()).await.unwrap();
    assert_eq!(gone.status(), reqwest::StatusCode::GONE);
    assert!(no_store(&gone));
    let gone = query(exact).await.unwrap();
    assert_eq!(gone.status(), reqwest::StatusCode::GONE);
    assert!(no_store(&gone));
    let unavailable = get("/v1/receiver/init".into()).await.unwrap();
    assert_eq!(
        unavailable.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(no_store(&unavailable));

    // The identical publication has the same session id and serves the same URL.
    let pir = Server::new(snapshot).unwrap();
    let expected = pir.public().to_vec();
    assert!(publications.publish(Publication::new(pir, None).unwrap(), publications.epoch()));
    let restored = get(public).await.unwrap();
    assert!(restored.status().is_success());
    assert_eq!(restored.bytes().await.unwrap(), expected);
    let query = client.prepare(receiver(), 0).unwrap();
    let answer = http()
        .post(format!("{origin}/v1/receiver/query"))
        .body(query.body().to_vec())
        .send()
        .await
        .unwrap();
    assert!(answer.status().is_success());
    let answer = answer.bytes().await.unwrap();
    assert_eq!(client.decode(query, &answer).unwrap(), Some(record(0, 1)));
}

/// Health serves the owner's latest report for monitoring.
#[tokio::test]
async fn health_serves_the_owners_report() {
    let publications = Publications::default();
    let server = serve_router(receiver_pir_server::router_with_publications(
        publications.clone(),
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
    assert!(health().await["indexer"].is_null());
    publications.set_report(serde_json::json!({"missing_payouts": 0}));
    assert_eq!(health().await["indexer"]["missing_payouts"], 0);
}
/// A displaced revision keeps its full grace: the next rotation waits for it, so two
/// quick rotations cannot strand a session that began on the older one.
#[test]
fn rotation_waits_for_the_previous_revisions_grace() {
    let publications = Publications::default();
    let publish = |height: u32| {
        let mut next = snapshot(1);
        next.manifest.end_height = height;
        next.manifest.end_hash = [height as u8; 32];
        publications.publish(
            Publication::new(Server::new(next).unwrap(), None).unwrap(),
            0,
        )
    };
    assert!(publish(101));
    assert!(publications.ready_at().is_none());
    assert!(publish(102));
    let ready = publications.ready_at().unwrap();
    assert!(ready > std::time::Instant::now() + Duration::from_secs(50));
    assert!(!publish(103));
    assert_eq!(publications.anchors(), [(102, [102; 32]), (101, [101; 32])]);
    publications.revoke();
    assert!(publications.ready_at().is_none());
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
    let manifest = DirectoryClient::fetch_manifest(&server.origin, &Http(http()))
        .await
        .unwrap();
    assert_eq!(manifest.directory.revision().unwrap(), revision);
    let metrics = http()
        .get(format!("{}/metrics", server.origin))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("pir_http_arrivals_total{endpoint=\"receiver_init\"} 1"));
    assert!(metrics.contains("pir_http_arrivals_total{endpoint=\"receiver_health\"} 1"));
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

/// A transport that counts requests and body bytes.
struct Counted {
    http: Http,
    up: AtomicUsize,
    down: AtomicUsize,
    posts: AtomicUsize,
    gets: AtomicUsize,
}

impl Counted {
    /// A counting transport over a fresh client.
    fn new() -> Self {
        Self {
            http: Http(http()),
            up: AtomicUsize::new(0),
            down: AtomicUsize::new(0),
            posts: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
        }
    }
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
async fn wallets_test_receivers_against_the_publication_filters() {
    /// Flips a bit of every filter file it fetches.
    struct Tampered(Http);
    impl Transport for Tampered {
        async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
            let mut bytes = Transport::get(&self.0, url, limit).await?;
            if url.contains("/filters/") {
                *bytes.last_mut().unwrap() ^= 1;
            }
            Ok(bytes)
        }
        async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
            Transport::post(&self.0, url, body, limit).await
        }
    }
    let server = serve(snapshot(1)).await;
    let transport = Http(http());
    let manifest = DirectoryClient::fetch_manifest(&server.origin, &transport)
        .await
        .unwrap();
    let filters = DirectoryClient::fetch_filters(&server.origin, &transport, &manifest)
        .await
        .unwrap();
    let key = manifest.directory.salt;
    let paid = filters.get(receiver_directory::filter::PAID).unwrap();
    assert_eq!(paid.matches(&key, &[receiver()]), [true]);
    assert_eq!(filters.iter().count(), 1);
    let tampered = Tampered(Http(http()));
    assert!(matches!(
        DirectoryClient::fetch_filters(&server.origin, &tampered, &manifest).await,
        Err(Error::Malformed)
    ));
}

#[tokio::test]
async fn retrieve_complete_history_and_enforce_limits_over_http() {
    let server = serve(snapshot(2)).await;
    let mut client = connect(&server.origin, Http(http()), accepted(), 0)
        .await
        .unwrap();
    let payments = client.lookup(receiver(), accepted()).await.unwrap();
    assert_eq!(payments.len(), 2);
    assert_eq!(payments[0].position, 200);
    assert_eq!(payments[1].position, 201);
    let mut wrong = accepted();
    wrong.hash[0] ^= 1;
    assert!(client.lookup(receiver(), wrong).await.is_err());
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
    let mut client = connect(&absent.origin, Http(http()), accepted(), 0)
        .await
        .unwrap();
    assert!(client
        .lookup(receiver(), accepted())
        .await
        .unwrap()
        .is_empty());
}

/// A query must be exactly its session's length: a longer one is 413 and a shorter
/// one 400, even below the largest supported query.
#[tokio::test]
async fn queries_must_match_their_sessions_length() {
    use receiver_pir::{query_bytes, MAX_ROWS};
    let pir = Server::new(snapshot(1)).unwrap();
    let client = Client::new(pir.manifest().clone(), pir.public(), accepted()).unwrap();
    let server = serve_publication(Publication::new(pir, None).unwrap()).await;
    let post = |body: Vec<u8>| {
        http()
            .post(format!("{}/v1/receiver/query", server.origin))
            .body(body)
            .send()
    };
    let query = client.prepare(receiver(), 0).unwrap();
    let exact = query.body().to_vec();
    assert_eq!(exact.len(), query_bytes(MIN_ROWS).unwrap());
    assert!(exact.len() + 1 < query_bytes(MAX_ROWS).unwrap());
    let mut longer = exact.clone();
    longer.push(0);
    assert_eq!(
        post(longer).await.unwrap().status(),
        reqwest::StatusCode::PAYLOAD_TOO_LARGE
    );
    let mut shorter = exact.clone();
    shorter.pop();
    assert_eq!(
        post(shorter).await.unwrap().status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    let answer = post(exact).await.unwrap();
    assert!(answer.status().is_success());
    let answer = answer.bytes().await.unwrap();
    assert_eq!(client.decode(query, &answer).unwrap(), Some(record(0, 1)));
}

#[tokio::test]
async fn long_histories_load_the_row_file() {
    use receiver_pir::transport::MAX_PIR_PAGES;
    let pir = MAX_PIR_PAGES as usize;
    for (count, posts, gets) in [(MAX_PIR_PAGES, pir, 2), (MAX_PIR_PAGES + 1, 1, 3)] {
        let server = serve(snapshot(count)).await;
        let host = Counted::new();
        let mut client = connect(&server.origin, &host, accepted(), 0).await.unwrap();
        let payments = client.lookup(receiver(), accepted()).await.unwrap();
        assert_eq!(payments.len(), count as usize);
        assert!(payments.windows(2).all(|p| p[0].position < p[1].position));
        // The manifest and PIR setup, plus the row file for the long history.
        assert_eq!(host.gets.load(Ordering::Relaxed), gets);
        assert_eq!(host.posts.load(Ordering::Relaxed), posts);
    }
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
        let mut client = connect(&server.origin, Http(http()), accepted(), 0)
            .await
            .unwrap();
        assert!(
            client.lookup(receiver(), accepted()).await.is_err(),
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
    // One served record at position 0, so the file must prove its commitment.
    let mut paid = record(0, 1);
    paid.payment.position = 0;
    paid.payment.cmx = [1; 32];
    let snapshot = Snapshot::build(manifest, &[paid], &[]).unwrap();
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
    let server = serve(snapshot(1)).await;
    for count in [50, 450, 10_000] {
        let host = Counted::new();
        let mut client = connect(&server.origin, &host, accepted(), count)
            .await
            .unwrap();
        assert_eq!(
            prefer_directory_file(client.manifest(), count).unwrap(),
            count >= 450
        );
        for _ in 0..count {
            assert_eq!(
                client.lookup(receiver(), accepted()).await.unwrap().len(),
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
        client.lookup(receiver(), accepted()).await.unwrap();
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
        async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
            Transport::post(&self.http, url, body, limit).await
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
                450
            )
            .await,
            Err(Error::Malformed)
        ));
    }
    // A PIR client that loads the file for a long history checks it the same way.
    let long = serve(snapshot(receiver_pir::transport::MAX_PIR_PAGES + 1)).await;
    let mut client = connect(
        &long.origin,
        Corrupt {
            http: Http(http()),
            truncate: false,
        },
        accepted(),
        0,
    )
    .await
    .unwrap();
    assert!(matches!(
        client.lookup(receiver(), accepted()).await,
        Err(Error::Malformed)
    ));
    let mut client = connect(&server.origin, Http(http()), accepted(), 450)
        .await
        .unwrap();
    assert_eq!(
        client.lookup(receiver(), accepted()).await.unwrap().len(),
        2
    );
}

#[test]
fn encrypted_publication_roundtrip_and_fail_closed() {
    let records = [record(0, 2), record(1, 2)];
    let server = Server::new(Snapshot::build(manifest(MIN_ROWS), &records, &[]).unwrap()).unwrap();
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
    malformed[0] ^= 1;
    assert!(matches!(server.respond(&malformed), Err(Error::Malformed)));
    println!(
        "public_bytes={} query_bytes={} response_bytes={}",
        server.public().len(),
        receiver_pir::query_bytes(MIN_ROWS).unwrap(),
        receiver_pir::response_bytes(MIN_ROWS).unwrap()
    );
}

#[test]
fn reject_corrupt_rows_before_preprocessing() {
    let mut snapshot = Snapshot::build(manifest(MIN_ROWS), &[], &[]).unwrap();
    snapshot.data[0] ^= 1;
    assert!(matches!(Server::new(snapshot), Err(Error::Malformed)));
    // The directory refuses a smaller table before the PIR profile sees it.
    assert!(Snapshot::build(manifest(MIN_ROWS / 2), &[], &[]).is_err());
}

/// A session manifest is valid up to exactly [`receiver_pir::MAX_MANIFEST_BYTES`] of
/// compact JSON, so a server refuses to prepare one with more provider sets than fit.
#[test]
fn session_manifests_are_bounded_by_their_serialized_size() {
    use receiver_directory::snapshot::{FilterSet, ProviderSet};
    use receiver_pir::MAX_MANIFEST_BYTES;
    let label = |i: usize, pad: usize| format!("p{i:03}{}/seen", "x".repeat(pad));
    let mut session = receiver_pir::Manifest {
        protocol: receiver_pir::PROTOCOL.into(),
        directory: manifest(MIN_ROWS),
        public_digest: [0; 32],
    };
    let size = |m: &receiver_pir::Manifest| serde_json::to_vec(m).unwrap().len();
    // Sets sort before `paid`. Add them until the manifest nears the bound, then
    // lengthen labels a byte at a time to reach it exactly.
    while size(&session) < MAX_MANIFEST_BYTES - 200 {
        let i = session.directory.filters.len() - 1;
        session.directory.filters.insert(
            i,
            FilterSet {
                label: label(i, 0),
                count: 0,
                window_secs: None,
                since_unix: Some(1),
                until_unix: Some(2),
            },
        );
    }
    let mut pads = vec![0; session.directory.filters.len() - 1];
    let mut next = 0;
    let mut lengthen = |session: &mut receiver_pir::Manifest| {
        let i = next % pads.len();
        next += 1;
        pads[i] += 1;
        session.directory.filters[i].label = label(i, pads[i]);
    };
    while size(&session) < MAX_MANIFEST_BYTES {
        lengthen(&mut session);
    }
    assert_eq!(size(&session), MAX_MANIFEST_BYTES);
    session.validate().unwrap();
    lengthen(&mut session);
    assert_eq!(size(&session), MAX_MANIFEST_BYTES + 1);
    assert!(matches!(session.validate(), Err(Error::Malformed)));
    // The directory accepts any number of sets, but a server refuses the session.
    let provider: Vec<_> = (0..300)
        .map(|i| ProviderSet {
            label: label(i, 0),
            window_secs: None,
            since_unix: 1,
            until_unix: 2,
            receivers: Vec::new(),
        })
        .collect();
    let snapshot = Snapshot::build(manifest(MIN_ROWS), &[], &provider).unwrap();
    assert!(matches!(Server::new(snapshot), Err(Error::Malformed)));
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
        let server = Server::new(Snapshot::build(m, &records, &[]).unwrap()).unwrap();
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
