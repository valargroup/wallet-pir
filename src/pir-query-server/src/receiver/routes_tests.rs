use super::*;
use crate::receiver::tests::{common, lookup, prepared};

/// A one-page publication ending at block `[end; 32]`, at height 101 for the page's
/// own block `[3; 32]` and 102 otherwise.
fn publication(end: u8) -> Publication {
    let mut coverage = prepared::coverage(end);
    coverage.end_height += u32::from(end != 3);
    prepared::prepare(prepared::tables(coverage, &[common::paid(0)], 2048)).unwrap()
}

#[tokio::test]
async fn foreign_product_shaped_404_keeps_its_existing_metric_category() {
    let (query_url, task) = serve(
        Publications::default(),
        Arc::new(Queue::new(1, 1, WAIT, true)),
    )
    .await;
    let origin = query_url.strip_suffix("/v1/receiver/query").unwrap();
    let http = reqwest::Client::new();
    let refused = http
        .get(format!("{origin}/v2/shards/init"))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), StatusCode::NOT_FOUND);
    drop(refused);
    let metrics = http
        .get(format!("{origin}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("pir_http_responses_total{endpoint=\"init\",code=\"4xx\"} 1"));
    task.abort();
}

#[tokio::test]
async fn an_inventory_request_waits_for_the_first_publication() {
    let publications = Publications::default();
    let (query_url, task) =
        serve(publications.clone(), Arc::new(Queue::new(1, 1, WAIT, true))).await;
    let inventory = query_url.replace("/query", "/inventory");
    let asked = tokio::spawn(async move { reqwest::get(inventory).await.unwrap().status() });
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        !asked.is_finished(),
        "nothing published yet, so the request waits"
    );
    assert!(publications.publish(publication(3), 0));
    assert_eq!(asked.await.unwrap(), StatusCode::OK);
    task.abort();
}

#[tokio::test]
async fn query_response_retains_admission_and_publication_until_transport_drop() {
    let publication = publication(3);
    let session = publication.segments()[0].publication().directory;
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    let prepared = Arc::downgrade(&publications.current().unwrap());
    let queue = Arc::new(Queue::new(1, 1, WAIT, true));
    let clients = ClientSlots::new(1);
    let service = Service {
        publications: publications.clone(),
        queue: queue.clone(),
        clients: clients.clone(),
        metrics: Default::default(),
        health_fields: Arc::new(serde_json::Map::new),
        ranges: Arc::new(tokio::sync::Semaphore::new(MAX_FILTER_RANGES)),
        objects: Arc::new(tokio::sync::Semaphore::new(MAX_OBJECT_LOADS)),
    };
    let (_, request) = lookup::query(&session, 0, 2048);
    let response = query(
        State(service),
        Request::post("/v1/receiver/query")
            .body(axum::body::Body::from(request))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/octet-stream"
    );
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(queue.executing_available(), 0);
    assert!(clients.try_acquire("unknown").is_none());
    publications.revoke();
    assert!(prepared.upgrade().is_some());
    // Dropping an unconsumed response models transport cancellation. The
    // shared wrapper also tests the fully-polled end-of-stream case.
    drop(response);
    assert_eq!(queue.executing_available(), 1);
    assert!(clients.try_acquire("unknown").is_some());
    assert!(prepared.upgrade().is_none());
}

/// With every evaluation slot held, a query of the wrong length is still refused at
/// once, while one of the right length waits and is then told to retry.
#[tokio::test]
async fn wrong_lengths_are_refused_without_waiting_for_evaluation() {
    let publication = prepared::prepare(prepared::empty(1)).unwrap();
    let mut header = MAGIC.to_vec();
    header.extend(publication.segments()[0].directory.id());
    let exact = query_bytes(zakura_pir_query_server::receiver::DIRECTORY_ROWS);
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    let queue = Arc::new(Queue::new(EVALUATING, WAITING, WAIT, true));
    let _held = [
        queue.acquire(|| ()).await.unwrap(),
        queue.acquire(|| ()).await.unwrap(),
    ];
    let (url, server) = serve(publications, queue).await;
    let http = reqwest::Client::new();
    let post = |len: usize, client: &str| {
        let mut body = header.clone();
        body.resize(len, 0);
        http.post(&url)
            .header("x-forwarded-for", client)
            .body(body)
            .send()
    };
    for (len, status) in [
        (exact + 1, StatusCode::PAYLOAD_TOO_LARGE),
        (exact - 1, StatusCode::BAD_REQUEST),
    ] {
        let started = std::time::Instant::now();
        assert_eq!(post(len, "10.0.0.1").await.unwrap().status(), status);
        assert!(started.elapsed() < WAIT / 2);
    }
    let started = std::time::Instant::now();
    assert_eq!(
        post(exact, "10.0.0.2").await.unwrap().status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    assert!(started.elapsed() >= WAIT);
    server.abort();
}

/// Block hint ranges past the limit are refused with 429 while the others are
/// built or sent, and served again once one finishes.
#[tokio::test]
async fn block_hint_ranges_beyond_the_limit_are_refused() {
    let ranges = Arc::new(tokio::sync::Semaphore::new(MAX_FILTER_RANGES));
    let publications = Publications::default();
    assert!(publications.publish_filters(window(), 0));
    let app = router(Service {
        publications,
        queue: Arc::new(Queue::new(1, 1, WAIT, true)),
        clients: ClientSlots::new(PER_CLIENT),
        metrics: pir_observability::HttpMetrics::default(),
        health_fields: Arc::new(serde_json::Map::new),
        ranges: ranges.clone(),
        objects: Arc::new(tokio::sync::Semaphore::new(MAX_OBJECT_LOADS)),
    });
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/v1/receiver/block-filters/0/100/1",
        socket.local_addr().unwrap()
    );
    let server = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    let held = ranges
        .clone()
        .acquire_many_owned(MAX_FILTER_RANGES as u32)
        .await
        .unwrap();
    let refused = reqwest::get(&url).await.unwrap();
    assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
    drop(held);
    let served = reqwest::get(&url).await.unwrap();
    assert_eq!(served.status(), StatusCode::OK);
    served.bytes().await.unwrap();
    assert_eq!(ranges.available_permits(), MAX_FILTER_RANGES);
    server.abort();
}

/// A one-block window at height 100.
fn window() -> receiver_pir_server::blocks::BlockWindow {
    receiver_pir_server::blocks::BlockWindow::new([1; 32], 0, 100, [2; 32], vec![[3; 32]], vec![])
        .unwrap()
}

/// Hints revoked while a response is being built are refused as gone, and its permit
/// stays held until the blocking work finishes. The runtime's one blocking thread is
/// held so the request is selected before the revocation and built after it.
#[test]
fn a_revocation_during_hint_encoding_is_gone() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let ranges = Arc::new(tokio::sync::Semaphore::new(MAX_FILTER_RANGES));
        let publications = Publications::default();
        assert!(publications.publish_filters(window(), 0));
        let app = router(Service {
            publications: publications.clone(),
            queue: Arc::new(Queue::new(1, 1, WAIT, true)),
            clients: ClientSlots::new(PER_CLIENT),
            metrics: pir_observability::HttpMetrics::default(),
            health_fields: Arc::new(serde_json::Map::new),
            ranges: ranges.clone(),
            objects: Arc::new(tokio::sync::Semaphore::new(MAX_OBJECT_LOADS)),
        });
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/v1/receiver", socket.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let path = "block-filters/0/100/1";
        let (release, held) = std::sync::mpsc::channel::<()>();
        let blocker = tokio::task::spawn_blocking(move || held.recv());
        let response = tokio::spawn(reqwest::get(format!("{origin}/{path}")));
        tokio::time::timeout(WAIT, async {
            while ranges.available_permits() == MAX_FILTER_RANGES {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the request selected the hints and took a permit");
        publications.revoke();
        release.send(()).unwrap();
        blocker.await.unwrap().unwrap();
        let response = response.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::GONE, "{path}");
        assert_eq!(ranges.available_permits(), MAX_FILTER_RANGES);
        server.abort();
    });
}

/// Serves `publications` with `queue`, returning the query URL and the server task.
async fn serve(
    publications: Publications,
    queue: Arc<Queue>,
) -> (String, tokio::task::JoinHandle<()>) {
    let app = router(Service {
        publications,
        queue,
        clients: ClientSlots::new(PER_CLIENT),
        metrics: pir_observability::HttpMetrics::default(),
        health_fields: Arc::new(serde_json::Map::new),
        ranges: Arc::new(tokio::sync::Semaphore::new(MAX_FILTER_RANGES)),
        objects: Arc::new(tokio::sync::Semaphore::new(MAX_OBJECT_LOADS)),
    });
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1/receiver/query", socket.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    (url, server)
}

/// Posts `body` while every evaluation slot is held, runs `during` once the query
/// has been selected and waits for a slot, then frees the slots for its answer.
async fn held_query(
    url: &str,
    queue: &Queue,
    body: Vec<u8>,
    during: impl FnOnce(),
) -> reqwest::Response {
    let held = [
        queue.acquire(|| ()).await.unwrap(),
        queue.acquire(|| ()).await.unwrap(),
    ];
    let response = tokio::spawn(reqwest::Client::new().post(url).body(body).send());
    // A request refused before it queues fails here rather than hanging.
    tokio::time::timeout(WAIT, async {
        while queue.waiting_available() == WAITING {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the query was selected and queued");
    during();
    drop(held);
    response.await.unwrap().unwrap()
}

/// A query selected before a revocation and evaluated after it is answered 410,
/// `no-store`; one selected before an ordinary rotation is still answered.
#[tokio::test]
async fn a_revocation_after_selection_is_gone() {
    let first = publication(3);
    let session = first.segments()[0].publication().directory;
    let publications = Publications::default();
    assert!(publications.publish(first, 0));
    let queue = Arc::new(Queue::new(EVALUATING, WAITING, WAIT, true));
    let (url, task) = serve(publications.clone(), queue.clone()).await;

    let (_, query) = lookup::query(&session, 0, 2048);
    let rotated = held_query(&url, &queue, query.clone(), || {
        assert!(publications.publish(publication(4), 0));
    })
    .await;
    assert_eq!(rotated.status(), StatusCode::OK);
    assert_eq!(rotated.bytes().await.unwrap()[..52], query[..52]);

    let (_, query) = lookup::query(&session, 0, 2048);
    let revoked = held_query(&url, &queue, query, || publications.revoke()).await;
    assert_eq!(revoked.status(), StatusCode::GONE);
    assert_eq!(revoked.headers()[header::CACHE_CONTROL], "no-store");
    task.abort();
}

/// A shard filter whose load a revocation overtakes is 410, never the revoked
/// inventory's filter; choice objects share that path.
#[tokio::test]
async fn a_revocation_during_a_filter_load_is_gone() {
    use std::sync::mpsc;
    use zakura_pir_query_server::receiver::shards::{Coverage, ShardFilter};
    let coverage = Coverage {
        start_height: 100,
        end_height: 101,
        end_hash: [3; 32],
    };
    let filter = ShardFilter::build([1; 32], 0, coverage, &[common::receiver()]).unwrap();
    let (started, loading) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = std::sync::Mutex::new(released);
    let encoding = filter.bytes().to_vec();
    let source = Arc::new(tables::ObjectSource::filter(
        filter.digest(),
        encoding.len() as u32,
        filter.receivers(),
        move || {
            started.send(()).unwrap();
            released.lock().unwrap().recv().unwrap();
            Ok(encoding.clone())
        },
    ));
    let segment = prepared::segment(prepared::empty(1)).unwrap();
    let publication =
        Publication::assemble([1; 32], 0, vec![(true, vec![segment], Some(source))]).unwrap();
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    let (url, task) = serve(publications.clone(), Arc::new(Queue::new(1, 1, WAIT, true))).await;
    let url = url.replace(
        "/v1/receiver/query",
        &format!(
            "/v1/receiver/shard-filters/{}",
            hex::encode(filter.digest())
        ),
    );
    let response = tokio::spawn(reqwest::get(url));
    tokio::task::spawn_blocking(move || loading.recv().unwrap())
        .await
        .unwrap();
    publications.revoke();
    release.send(()).unwrap();
    let response = response.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::GONE);
    task.abort();
}

/// Filter and choice loads past the limit are refused with 429 before any blocking
/// work; a served object keeps its permit until its response is dropped.
#[tokio::test]
async fn object_loads_beyond_the_limit_are_refused() {
    use zakura_pir_query_server::receiver::shards::{Coverage, ShardFilter};
    let coverage = Coverage {
        start_height: 100,
        end_height: 101,
        end_hash: [3; 32],
    };
    let filter = ShardFilter::build([1; 32], 0, coverage, &[common::receiver()]).unwrap();
    let encoding = filter.bytes().to_vec();
    let source = Arc::new(tables::ObjectSource::filter(
        filter.digest(),
        encoding.len() as u32,
        filter.receivers(),
        move || Ok(encoding.clone()),
    ));
    let segment = prepared::segment(prepared::empty(1)).unwrap();
    let publication =
        Publication::assemble([1; 32], 0, vec![(true, vec![segment], Some(source))]).unwrap();
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    let objects = Arc::new(tokio::sync::Semaphore::new(MAX_OBJECT_LOADS));
    let service = Service {
        publications,
        queue: Arc::new(Queue::new(1, 1, WAIT, true)),
        clients: ClientSlots::new(PER_CLIENT),
        metrics: pir_observability::HttpMetrics::default(),
        health_fields: Arc::new(serde_json::Map::new),
        ranges: Arc::new(tokio::sync::Semaphore::new(MAX_FILTER_RANGES)),
        objects: objects.clone(),
    };
    let digest = hex::encode(filter.digest());
    let get = || {
        object(&service, &digest, Publications::with_filter, |p, d| {
            p.filter(d).cloned()
        })
    };
    let held = objects
        .clone()
        .acquire_many_owned(MAX_OBJECT_LOADS as u32)
        .await
        .unwrap();
    assert_eq!(get().await.status(), StatusCode::TOO_MANY_REQUESTS);
    drop(held);
    let served = get().await;
    assert_eq!(served.status(), StatusCode::OK);
    assert_eq!(objects.available_permits(), MAX_OBJECT_LOADS - 1);
    drop(served);
    assert_eq!(objects.available_permits(), MAX_OBJECT_LOADS);
}
