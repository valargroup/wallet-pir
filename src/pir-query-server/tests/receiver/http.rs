//! Receiver serving over HTTP, as a wallet reaches it: inventory capture, directory
//! lookups routed by choice objects, companion reads at their admitted lengths, and the
//! serving contract's refusals.
#[path = "../../../receiver/receiver-store/tests/common/mod.rs"]
mod common;
#[path = "../../../receiver/receiver-pir-server/tests/common/lookup.rs"]
mod lookup;
#[path = "../../../receiver/receiver-pir-server/tests/common/prepared.rs"]
mod prepared;
#[path = "../../../receiver/receiver-pir-server/tests/common/router.rs"]
mod router;
use common::{paid, receiver, seeded_receiver};
use lookup::{query, Wallet};
use prepared::{coverage, tables, tables_at, Paid, Tables};
use receiver_pir_server::{tables::ObjectSource, Publication, Publications};
use reqwest::StatusCode;
use router::router_with_publications;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use zakura_pir_query_server::receiver::{
    server::Server,
    session::Error,
    shards::{Coverage as FilterCoverage, ShardFilter},
    Table as _, Tree,
};

/// `n` pages to [`receiver`] at positions 200 onward.
fn pages(n: u32) -> Vec<Paid> {
    (0..n).map(paid).collect()
}

/// One payment to another receiver at position 250 in its own transaction.
fn theirs() -> Paid {
    let mut p = paid(0);
    p.0 = seeded_receiver(1);
    (p.1.position, p.1.tx_index, p.1.txid) = (250, 50, [50; 32]);
    p
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

/// Serves one prepared publication through the production router.
async fn serve(publication: Publication) -> Running {
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    serve_router(router_with_publications(publications)).await
}

/// Serves `app` on a loopback port.
async fn serve_router(app: axum::Router) -> Running {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    Running { origin, task }
}

/// A one-shard publication of `tables`.
fn publication(tables: Tables) -> Publication {
    prepared::prepare(tables).unwrap()
}

fn http() -> reqwest::Client {
    reqwest::Client::new()
}

/// Whether `response` forbids caching it.
fn no_store(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(reqwest::header::CACHE_CONTROL)
        .is_some_and(|value| value == "no-store")
}

/// A wallet reads each receiver's records with one directory query per segment and one
/// companion query per record, a miss reads no companion, and the canonical and
/// deployed prefixes answer alike.
#[tokio::test]
async fn lookups_read_entries_and_slots_over_http() {
    let mut paid = pages(3);
    paid.push(theirs());
    let server = serve(publication(tables(coverage(3), &paid, 2048))).await;
    for prefix in ["/receiver/v1", "/v1/receiver"] {
        let wallet = Wallet::at(&server.origin, prefix);
        let ours = wallet.history(&receiver()).await.unwrap();
        assert_eq!(
            ours.iter()
                .map(|r| (r.page, r.total, r.payment.clone()))
                .collect::<Vec<_>>(),
            (0..3)
                .map(|page| (page, 3, paid[page as usize].1.clone()))
                .collect::<Vec<_>>()
        );
        assert_eq!(wallet.history(&seeded_receiver(1)).await.unwrap().len(), 1);
        assert!(wallet
            .history(&seeded_receiver(2))
            .await
            .unwrap()
            .is_empty());
    }
}

/// A companion query covers every row or exactly its `query_rows` prefix: both are
/// answered, and another prefix, a directory query sized for the companion and a body
/// too short for its header are refused before evaluation. A response is decoded only
/// when it echoes its own request's header.
#[tokio::test]
async fn companion_queries_are_admitted_at_full_or_trimmed_length() {
    let built = tables(coverage(3), &pages(2), 6144);
    assert_eq!(
        (
            built.companion.manifest.rows,
            built.companion.manifest.query_rows
        ),
        (6144, 2048)
    );
    let segment = prepared::segment(built).unwrap().publication();
    let server = serve(publication(tables(coverage(3), &pages(2), 6144))).await;
    let wallet = Wallet::new(&server.origin);
    let (d, c) = (&segment.directory, &segment.companion);
    for sent in [2048, 6144] {
        let row = wallet.read(c, 0, sent).await.unwrap();
        assert!(row.iter().any(|b| *b != 0), "{sent}");
    }
    assert_eq!(wallet.read(c, 0, 4096).await, Err(StatusCode::BAD_REQUEST));
    // A directory query is never trimmed or lengthened.
    let (_, long) = query(c, 0, 6144);
    let mut directory = long.clone();
    directory[4..36].copy_from_slice(&d.id().unwrap());
    assert_eq!(
        wallet.post(directory).await,
        Err(StatusCode::PAYLOAD_TOO_LARGE)
    );
    assert_eq!(
        wallet.post(long[..40].to_vec()).await,
        Err(StatusCode::BAD_REQUEST)
    );
    // Another session's answer is not this request's.
    let (_, first) = query(c, 0, 2048);
    let (_, second) = query(d, 0, 2048);
    let answer = wallet.post(first.clone()).await.unwrap();
    assert_eq!(answer[..52], first[..52]);
    assert_ne!(answer[..52], second[..52]);
}

/// A companion whose rows past `query_rows` are not zero, which preparation refuses
/// (wallet-libraries tests that), is never answered, even where its session was listed.
#[tokio::test]
async fn a_nonzero_omitted_row_is_refused_at_preparation() {
    use sha2::Digest;
    use zakura_pir_query_server::receiver::{Session, ROW_BYTES};
    let built = tables(coverage(3), &pages(2), 4096);
    let (directory, entries) = prepared::directory(built.directory.clone()).unwrap();
    let good = prepared::companion(
        built.companion.clone(),
        &directory.session().manifest,
        &entries,
    )
    .unwrap();
    let mut corrupt = built.companion.clone();
    corrupt.data[3000 * ROW_BYTES] = 1;
    corrupt.manifest.data_sha256 = sha2::Sha256::digest(&corrupt.data).into();
    let manifest = directory.session().manifest.clone();
    let session = Session {
        manifest: corrupt.manifest.clone(),
        public_digest: good.session().public_digest,
    };
    let loads = Arc::new(AtomicUsize::new(0));
    let source =
        receiver_pir_server::tables::Source::companion(session.clone(), good.public().to_vec(), {
            let loads = loads.clone();
            move || {
                loads.fetch_add(1, Ordering::SeqCst);
                Server::companion(corrupt.clone(), &manifest, &entries)
            }
        })
        .unwrap();
    let mut segment = prepared::segment(built).unwrap();
    segment.companion = Arc::new(source);
    let publication =
        Publication::assemble([1; 32], 0, vec![(false, vec![segment], None)]).unwrap();
    let server = serve(publication).await;
    let wallet = Wallet::new(&server.origin);
    let (_, body) = query(&session, 0, 2048);
    assert!(wallet.post(body).await.is_err());
    assert_eq!(loads.load(Ordering::SeqCst), 1);
}

/// A publication with no payments serves an empty directory and a companion whose
/// query covers one zero-filled block; any other length is refused.
#[tokio::test]
async fn a_zero_payment_publication_serves_empty_tables() {
    let built = prepared::empty(3);
    let c = &built.companion.manifest;
    assert_eq!((c.rows, c.query_rows, c.payments), (2048, 2048, 0));
    let segment = prepared::segment(built.clone()).unwrap().publication();
    let server = serve(publication(built)).await;
    let wallet = Wallet::new(&server.origin);
    assert!(wallet.history(&receiver()).await.unwrap().is_empty());
    assert!(wallet
        .read(&segment.companion, 0, 2048)
        .await
        .unwrap()
        .iter()
        .all(|b| *b == 0));
    let wider = prepared::segment(tables(coverage(3), &[], 4096))
        .unwrap()
        .publication();
    let (_, mut longer) = query(&wider.companion, 0, 4096);
    longer[4..36].copy_from_slice(&segment.companion.id().unwrap());
    assert_eq!(
        wallet.post(longer).await,
        Err(StatusCode::PAYLOAD_TOO_LARGE)
    );
}

/// Choice objects are served by digest with their exact bytes; a digest no live
/// inventory names is 410 and a malformed one 400. Once a displaced inventory's
/// retention ends, its choice object, public setups and queries are 410.
#[tokio::test]
async fn choice_objects_and_expired_sessions() {
    use sha2::Digest;
    let publications = Publications::with_retention(Duration::from_millis(1500));
    let built = tables(coverage(3), &pages(1), 2048);
    let choice = built.directory.choice.clone();
    let digest = built.directory.manifest.choice.digest;
    let segment = prepared::segment(built.clone()).unwrap().publication();
    assert!(publications.publish(publication(built), 0));
    let server = serve_router(router_with_publications(publications.clone())).await;
    let wallet = Wallet::new(&server.origin);
    let served = wallet
        .get(&format!("/choice/{}", hex::encode(digest)))
        .await
        .unwrap();
    assert_eq!(served, choice);
    assert_eq!(sha2::Sha256::digest(&served)[..], digest[..]);
    let response = http()
        .get(format!(
            "{}/receiver/v1/choice/{}",
            server.origin,
            "00".repeat(32)
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::GONE);
    assert!(no_store(&response));
    assert_eq!(wallet.get("/choice/zz").await, Err(StatusCode::BAD_REQUEST));
    let mut next = coverage(4);
    next.end_height = 102;
    assert!(publications.publish(publication(tables(next, &pages(1), 2048)), 0));
    // Within its retention the displaced inventory still serves.
    assert_eq!(wallet.lookup(&segment, &receiver()).await.unwrap().len(), 1);
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert_eq!(publications.anchors(), [(102, [4; 32])]);
    let path = format!("/choice/{}", hex::encode(digest));
    assert_eq!(wallet.get(&path).await, Err(StatusCode::GONE));
    let public = format!("/public/{}", hex::encode(segment.companion.id().unwrap()));
    assert_eq!(wallet.get(&public).await, Err(StatusCode::GONE));
    let (_, body) = query(&segment.directory, 0, 2048);
    assert_eq!(wallet.post(body).await, Err(StatusCode::GONE));
}

/// Every session not served now is 410, however many were retired since, and refusals
/// are `no-store`: a revoked session's 410 must not outlive the republication of the
/// same deterministic session id, which then serves again at the same URL.
#[tokio::test]
async fn refusals_are_not_cached_and_a_republished_session_serves_again() {
    let built = tables(coverage(3), &pages(1), 2048);
    let id = hex::encode(prepared::segment(built.clone()).unwrap().directory.id());
    let publications = Publications::default();
    assert!(publications.publish(publication(built.clone()), 0));
    let server = serve_router(router_with_publications(publications.clone())).await;
    let get = |path: String| {
        http()
            .get(format!("{}/v1/receiver{path}", server.origin))
            .send()
    };
    let unknown = get(format!("/public/{}", "00".repeat(32))).await.unwrap();
    assert_eq!(unknown.status(), StatusCode::GONE);
    assert!(no_store(&unknown));
    publications.revoke();
    // More retirements than the server ever remembered.
    for end in 10..19 {
        assert!(publications.publish(
            prepared::prepare(prepared::empty(end)).unwrap(),
            publications.epoch()
        ));
        publications.revoke();
    }
    let gone = get(format!("/public/{id}")).await.unwrap();
    assert_eq!(gone.status(), StatusCode::GONE);
    assert!(no_store(&gone));
    // The identical publication has the same session id and serves the same URL.
    let expected = prepared::segment(built.clone())
        .unwrap()
        .directory
        .public()
        .to_vec();
    assert!(publications.publish(publication(built), publications.epoch()));
    let restored = get(format!("/public/{id}")).await.unwrap();
    assert!(restored.status().is_success() && no_store(&restored));
    assert_eq!(restored.bytes().await.unwrap(), expected);
}

/// A reorg revokes every session of the inventory it replaced, including a captured
/// one's chunks, and the canonical replacement serves without the orphaned payment.
#[tokio::test]
async fn a_revocation_retires_captured_sessions_and_the_replacement_serves() {
    let publications = Publications::default();
    assert!(publications.publish(publication(tables(coverage(3), &pages(2), 2048)), 0));
    let server = serve_router(router_with_publications(publications.clone())).await;
    let wallet = Wallet::new(&server.origin);
    let (head, shards) = wallet.inventory().await.unwrap();
    let old = shards[0].segments[0].clone();
    assert_eq!(wallet.lookup(&old, &receiver()).await.unwrap().len(), 2);
    // Preparation started before the canonical guard detected the fork.
    let epoch = publications.epoch();
    let mut forked = coverage(9);
    forked.end_height = 102;
    let prepared = publication(tables(forked, &pages(2), 2048));
    publications.revoke();
    assert!(!publications.publish(prepared, epoch));
    assert_eq!(
        wallet.get("/inventory").await,
        Err(StatusCode::SERVICE_UNAVAILABLE)
    );
    let chunk = format!("/inventory/{}/0", hex::encode(head.inventory));
    assert_eq!(wallet.get(&chunk).await, Err(StatusCode::GONE));
    assert_eq!(
        wallet.lookup(&old, &receiver()).await.unwrap_err(),
        StatusCode::GONE
    );
    let mut replacement = coverage(10);
    replacement.end_height = 102;
    assert!(publications.publish(
        publication(tables(replacement, &[], 2048)),
        publications.epoch()
    ));
    assert!(wallet.history(&receiver()).await.unwrap().is_empty());
}

/// Health serves each publication's own report with its ID, and the owner's
/// process-local fields beside its own without letting them replace those.
#[tokio::test]
async fn health_serves_the_reports_and_fields() {
    let publications = Publications::default();
    let first =
        publication(prepared::empty(3)).with_report(serde_json::json!({"missing_payouts": 0}));
    assert!(publications.publish(first, 0));
    let reads = Arc::new(AtomicUsize::new(0));
    let fields: pir_query_server::receiver::HealthFields = {
        let reads = reads.clone();
        Arc::new(move || {
            let value = serde_json::json!({"reads": reads.load(Ordering::SeqCst), "serving": "x"});
            value.as_object().unwrap().clone()
        })
    };
    let server = serve_router(pir_query_server::receiver::router_with_health(
        publications.clone(),
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
    assert_eq!(health().await["indexer"]["missing_payouts"], 0);
    let mut next = coverage(9);
    next.end_height = 102;
    let next = publication(tables(next, &pages(1), 2048));
    let id = hex::encode(next.id());
    assert!(publications.publish(
        next.with_report(serde_json::json!({"missing_payouts": 3})),
        0
    ));
    reads.store(2, Ordering::SeqCst);
    let health = health().await;
    assert_eq!(health["serving"], id.as_str());
    assert_eq!(health["indexer"]["missing_payouts"], 3);
    assert_eq!(health["reads"], 2);
    let identity = pir_control::Identity::process();
    assert_eq!(health["identity"]["incarnation"], identity.incarnation);
}

/// A client at its cap of queries in flight, uploads included, is refused with 429 and
/// `Retry-After: 1`, as Enhance refuses overload; other clients are still admitted.
#[tokio::test]
async fn a_client_over_its_cap_is_told_to_retry() {
    use tokio::io::AsyncWriteExt;
    let server = serve(publication(prepared::empty(3))).await;
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
    assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(refused.headers()["retry-after"], "1");
    assert!(no_store(&refused));
    assert_eq!(
        query("10.0.0.2").await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    drop(uploads);
}

/// A sealed shard of two segments covering the same blocks, as an oversized block
/// becomes, serves each receiver's records from its own segment; its filter is served
/// by digest from a stored file read once into the table budget, and a file that no
/// longer holds the digest is refused. A chunk the inventory lacks is 404.
#[tokio::test]
async fn a_multi_segment_shard_and_its_stored_filter_serve() {
    let filter_coverage = FilterCoverage {
        start_height: 100,
        end_height: 101,
        end_hash: [3; 32],
    };
    let other = seeded_receiver(1);
    let filter = ShardFilter::build([1; 32], 0, filter_coverage, &[receiver(), other]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("filter");
    std::fs::write(&path, filter.bytes()).unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(ObjectSource::filter(
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
    let segments = vec![
        prepared::segment(tables(coverage(3), &pages(1), 2048)).unwrap(),
        prepared::segment(tables(coverage(3), &[theirs()], 2048)).unwrap(),
    ];
    let publication =
        Publication::assemble([1; 32], 0, vec![(true, segments, Some(source))]).unwrap();
    let inventory = hex::encode(publication.id());
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    let budget = publications.tables();
    let resident = budget.resident();
    let server = serve_router(router_with_publications(publications)).await;
    let wallet = Wallet::new(&server.origin);
    assert_eq!(wallet.history(&receiver()).await.unwrap().len(), 1);
    assert_eq!(wallet.history(&other).await.unwrap()[0].payment, theirs().1);
    let url = format!("/shard-filters/{}", hex::encode(filter.digest()));
    for _ in 0..2 {
        assert_eq!(wallet.get(&url).await.unwrap(), filter.bytes());
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert!(budget.resident() >= resident + filter.bytes().len());
    assert_eq!(
        wallet.get(&format!("/inventory/{inventory}/1")).await,
        Err(StatusCode::NOT_FOUND)
    );
    let replaced = ShardFilter::build([1; 32], 0, filter_coverage, &[receiver()]).unwrap();
    std::fs::write(&path, replaced.bytes()).unwrap();
    let corrupt =
        ObjectSource::filter(filter.digest(), filter.bytes().len() as u32, 2, move || {
            std::fs::read(&path).map_err(|e| Error::Transport(e.to_string()))
        });
    let fresh = receiver_pir_server::tables::Tables::new(1 << 30);
    assert!(fresh.object(&corrupt).is_err());
    assert_eq!(fresh.resident(), 0);
}

/// Wallets reach every receiver operation at the canonical `/receiver/v1/<operation>` and
/// at the deployed `/v1/receiver/<operation>` with identical answers, and both prefixes
/// count under one bounded label per operation. Operator routes stay outside the
/// canonical namespace.
#[tokio::test]
async fn canonical_and_legacy_receiver_routes_answer_identically() {
    use zakura_pir_query_server::receiver::blocks::HeightSet;
    let filter_coverage = FilterCoverage {
        start_height: 100,
        end_height: 101,
        end_hash: [3; 32],
    };
    let filter = ShardFilter::build([1; 32], 0, filter_coverage, &[receiver()]).unwrap();
    let built = tables(coverage(3), &pages(2), 2048);
    let choice = built.directory.manifest.choice.digest;
    let publication = prepared::inventory(vec![(true, vec![built], Some(filter.clone()))]).unwrap();
    let inventory = hex::encode(publication.id());
    let segment = publication.segments()[0].publication();
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    let set = HeightSet::build(&[1; 32], 101, [&receiver()])
        .unwrap()
        .unwrap();
    let window = receiver_pir_server::blocks::BlockWindow::new(
        [1; 32],
        0,
        101,
        [2; 32],
        vec![[3; 32]],
        vec![(101, set.bytes().to_vec())],
    )
    .unwrap();
    assert!(publications.publish_filters(window, publications.epoch()));
    let server = serve_router(router_with_publications(publications)).await;
    let gets = [
        "/inventory".to_owned(),
        format!("/inventory/{inventory}/0"),
        format!("/public/{}", hex::encode(segment.directory.id().unwrap())),
        format!("/public/{}", hex::encode(segment.companion.id().unwrap())),
        format!("/choice/{}", hex::encode(choice)),
        format!("/shard-filters/{}", hex::encode(filter.digest())),
        "/block-filters/head".to_owned(),
        "/block-filters/0/101/1".to_owned(),
    ];
    let answer = |response: reqwest::Response| async move {
        let status = response.status();
        let headers = [
            response.headers().get("content-type").cloned(),
            response.headers().get("cache-control").cloned(),
        ];
        (status, headers, response.bytes().await.unwrap())
    };
    for operation in &gets {
        let [canonical, legacy] = ["/receiver/v1", "/v1/receiver"].map(|prefix| {
            http()
                .get(format!("{}{prefix}{operation}", server.origin))
                .send()
        });
        let (canonical, legacy) = (
            answer(canonical.await.unwrap()).await,
            answer(legacy.await.unwrap()).await,
        );
        assert_eq!(canonical.0, StatusCode::OK, "{operation}");
        assert_eq!(canonical, legacy, "{operation}");
    }
    // Queries carry fresh encryption, so each prefix answers its own with status 200.
    for prefix in ["/receiver/v1", "/v1/receiver"] {
        let wallet = Wallet::at(&server.origin, prefix);
        let (_, d) = query(&segment.directory, 0, 2048);
        let (_, c) = query(&segment.companion, 0, 2048);
        for body in [d, c] {
            assert_eq!(
                wallet.post(body).await.unwrap().len(),
                zakura_pir_query_server::receiver::native::response_bytes()
            );
        }
    }
    for path in ["/v1/receiver/health", "/metrics"] {
        let response = http()
            .get(format!("{}{path}", server.origin))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
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
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
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
        ("receiver_choice", 2),
        ("receiver_shard_filters", 2),
        ("receiver_block_filters", 4),
        ("receiver_query", 4),
    ] {
        let line = format!("pir_http_responses_total{{endpoint=\"{label}\",code=\"2xx\"}} {count}");
        assert!(metrics.contains(&line), "{line} in {metrics}");
    }
    assert!(!metrics.contains(&hex::encode(segment.directory.id().unwrap())));
}

/// A companion built at a later tree than its directory's end keeps the directory
/// session, so a wallet holding the directory reads the new companion.
#[tokio::test]
async fn a_rebuilt_companion_keeps_the_directory_session() {
    let at = |height, hash: u8| Tree {
        height,
        hash: [hash; 32],
        size: 300,
    };
    let first = tables_at(coverage(3), &pages(1), 2048, at(101, 3));
    let second = tables_at(coverage(3), &pages(1), 2048, at(105, 5));
    assert_eq!(first.directory.manifest, second.directory.manifest);
    assert_ne!(
        first.companion.manifest.revision().unwrap(),
        second.companion.manifest.revision().unwrap()
    );
    let server = serve(publication(second)).await;
    let wallet = Wallet::new(&server.origin);
    let (_, shards) = wallet.inventory().await.unwrap();
    assert_eq!(shards[0].segments[0].companion.manifest.tree.height, 105);
    assert_eq!(wallet.history(&receiver()).await.unwrap().len(), 1);
}
