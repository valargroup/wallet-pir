//! Receiver PIR serving with immutable publications and atomic session revocation.
#[cfg(test)]
#[path = "../../../crates/receiver-store/tests/common/mod.rs"]
mod common;
#[cfg(test)]
extern crate self as receiver_pir_server;
#[cfg(test)]
#[path = "../tests/common/prepared.rs"]
mod prepared;
pub mod publication;
use axum::{
    body::Bytes,
    extract::{Path, Request, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use pir_control::admission::{client_key, read_body, BodyError, ClientSlots, Queue};
pub use publication::{Publication, Publications};
use receiver_pir::{query_bytes, Error, HEADER_BYTES, MAGIC, MAX_ROWS};
use std::{sync::Arc, time::Duration};

/// The receiver directory's route categories; session IDs never become labels.
fn endpoint(path: &str) -> Option<&'static str> {
    let route = path.strip_prefix("/v1/receiver/")?;
    Some(match route.split('/').next()? {
        "init" => "receiver_init",
        "public" => "receiver_public",
        "query" => "receiver_query",
        "rows" => "receiver_rows",
        "witness" => "receiver_witness",
        "filters" => "receiver_filters",
        "health" => "receiver_health",
        _ => return None,
    })
}

async fn observe(
    axum::extract::State(metrics): axum::extract::State<pir_observability::HttpMetrics>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    let label = endpoint(request.uri().path());
    pir_observability::observe(metrics, label, request, next).await
}

/// Queries evaluated at once.
const EVALUATING: usize = 2;
/// Queries that may wait for an evaluation slot.
const WAITING: usize = 8;
/// How long a query waits for an evaluation slot.
const WAIT: Duration = Duration::from_secs(2);
/// Queries one client may have in flight, uploads included. A wallet sends one at a
/// time; the margin is for clients sharing an address.
const PER_CLIENT: usize = 2;
/// How long a query's upload may take.
const UPLOAD: Duration = Duration::from_secs(15);

/// Process-local fields an owner adds to `/v1/receiver/health`, read on each request.
pub type HealthFields = Arc<dyn Fn() -> serde_json::Map<String, serde_json::Value> + Send + Sync>;

#[derive(Clone)]
struct Service {
    publications: Publications,
    queue: Arc<Queue>,
    clients: ClientSlots,
    metrics: pir_observability::HttpMetrics,
    health_fields: HealthFields,
}

/// The receiver categories `pir_observability` reports.
const ENDPOINTS: [&str; 7] = [
    "receiver_init",
    "receiver_public",
    "receiver_query",
    "receiver_rows",
    "receiver_witness",
    "receiver_filters",
    "receiver_health",
];

/// Serve the current publication while the owner prepares, validates and rotates
/// revisions, with the admission and refusals of `docs/serving-contract.md`.
/// Cancellation never frees a still-running CPU slot. `/v1/receiver/health` and
/// `/metrics` are for operators; a deployment's edge proxies only the wallet routes.
pub fn router_with_publications(publications: Publications) -> Router {
    router_with_health(publications, Arc::new(serde_json::Map::new))
}

/// [`router_with_publications`], with health also reporting `fields`. Health's own
/// fields (`identity`, `serving`, `epoch`, `indexer`) take precedence over theirs.
pub fn router_with_health(publications: Publications, fields: HealthFields) -> Router {
    router(Service {
        publications,
        queue: Arc::new(Queue::new(EVALUATING, WAITING, WAIT, true)),
        clients: ClientSlots::new(PER_CLIENT),
        metrics: pir_observability::HttpMetrics::default(),
        health_fields: fields,
    })
}

/// The routes of [`router_with_publications`] over `service`.
fn router(service: Service) -> Router {
    let metrics = service.metrics.clone();
    metrics.initialize(&ENDPOINTS);
    Router::new()
        .route("/v1/receiver/health", get(health))
        .route(
            "/metrics",
            get(|State(s): State<Service>| async move {
                ([(header::CONTENT_TYPE, "text/plain")], s.metrics.render())
            }),
        )
        .route(
            "/v1/receiver/init",
            get(|State(s): State<Service>| async move {
                match s.publications.select(None) {
                    Ok((p, _)) => (
                        [(header::CACHE_CONTROL, "no-store")],
                        Json(p.server.manifest().clone()),
                    )
                        .into_response(),
                    Err(status) => status.into_response(),
                }
            }),
        )
        .route(
            "/v1/receiver/public/:session",
            get(
                |State(s): State<Service>, Path(id): Path<String>| async move {
                    material(&s, &id, Material::Public)
                },
            ),
        )
        .route("/v1/receiver/query", post(query))
        .route(
            "/v1/receiver/rows/:session",
            get(
                |State(s): State<Service>, Path(id): Path<String>| async move {
                    material(&s, &id, Material::Rows)
                },
            ),
        )
        .route(
            "/v1/receiver/witness/:session",
            get(
                |State(s): State<Service>, Path(id): Path<String>| async move {
                    material(&s, &id, Material::Witness)
                },
            ),
        )
        .route(
            "/v1/receiver/filters/:session",
            get(
                |State(s): State<Service>, Path(id): Path<String>| async move {
                    material(&s, &id, Material::Filters)
                },
            ),
        )
        .layer(axum::middleware::map_response(no_store_errors))
        .layer(axum::middleware::from_fn_with_state(metrics, observe))
        .with_state(service)
}

/// Marks every error response `no-store`, leaving its status, other headers and body
/// as they are. Caches may keep a 410 heuristically, but session ids are deterministic
/// and an unserved one can be published again, so no refusal may outlive the state
/// that produced it.
async fn no_store_errors(mut response: Response) -> Response {
    let status = response.status();
    if status.is_client_error() || status.is_server_error() {
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    response
}

/// The process identity and the revision being served, for deploys and monitoring.
async fn health(State(s): State<Service>) -> Response {
    let (current, epoch) = s.publications.health();
    let (serving, report) = current.unzip();
    let mut body = (s.health_fields)();
    body.extend([
        (
            "identity".into(),
            serde_json::json!(pir_control::Identity::process()),
        ),
        ("serving".into(), serving.map(hex::encode).into()),
        ("epoch".into(), epoch.into()),
        ("indexer".into(), report.flatten().into()),
    ]);
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::Value::Object(body)),
    )
        .into_response()
}

enum Material {
    Public,
    Witness,
    Rows,
    Filters,
}

/// Serves one file of the publication whose hex id is `id`.
fn material(s: &Service, id: &str, material: Material) -> Response {
    let Some(id) = hex::decode(id).ok().and_then(|v| v.try_into().ok()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let p = match s.publications.select(Some(id)) {
        Ok((p, _)) => p,
        Err(status) => return status.into_response(),
    };
    match material {
        Material::Public => binary(p.server.public().to_vec()),
        Material::Rows => binary(Bytes::from_owner(p.server.rows())),
        Material::Filters => binary(Bytes::from_owner(p.server.filters())),
        Material::Witness => match &p.witnesses {
            Some(bytes) => binary(bytes.clone()),
            None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        },
    }
}

/// An uncached `application/octet-stream` response.
fn binary(body: impl IntoResponse) -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}

/// The refusal for a client at its cap or a full server, as Enhance answers overload.
fn overloaded() -> Response {
    (StatusCode::TOO_MANY_REQUESTS, [(header::RETRY_AFTER, "1")]).into_response()
}

/// Answers one PIR query against the publication its header names. The upload is
/// read before the query waits for an evaluation slot, so a slow upload holds only
/// its client's place, and a body longer or shorter than that session's query is
/// refused (413 or 400) without waiting.
async fn query(State(s): State<Service>, request: Request) -> Response {
    let Some(client) = s.clients.try_acquire(&client_key(request.headers(), None)) else {
        return overloaded();
    };
    let limit = query_bytes(MAX_ROWS).expect("supported maximum");
    let body = match read_body(request.into_body(), limit, UPLOAD).await {
        Ok(body) => body,
        Err(BodyError::Timeout) => return StatusCode::REQUEST_TIMEOUT.into_response(),
        Err(e) if e.over_limit() => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    if body.len() < HEADER_BYTES || &body[..4] != MAGIC {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let (publication, epoch) = match s.publications.select(Some(body[4..36].try_into().unwrap())) {
        Ok(p) => p,
        Err(status) => return status.into_response(),
    };
    let expected = query_bytes(publication.manifest().rows).expect("a served geometry");
    match body.len().cmp(&expected) {
        std::cmp::Ordering::Greater => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        std::cmp::Ordering::Less => return StatusCode::BAD_REQUEST.into_response(),
        std::cmp::Ordering::Equal => {}
    }
    let Ok(permit) = s.queue.acquire(|| ()).await else {
        return overloaded();
    };
    let result = pir_control::admission::spawn_admitted(
        (permit, client, publication),
        move |(_, _, publication)| publication.server.respond(&body),
    )
    .await;
    // A revocation during evaluation is 410; after it, wallets revalidate their anchor.
    if s.publications.epoch() != epoch {
        return StatusCode::GONE.into_response();
    }
    match result {
        Ok((Ok(body), guards)) => {
            let mut response = binary(body);
            let body = std::mem::replace(response.body_mut(), axum::body::Body::empty());
            *response.body_mut() = pir_control::http::retain(body, guards);
            response
        }
        Ok((Err(Error::Revision), _)) => StatusCode::GONE.into_response(),
        Ok((Err(Error::Malformed), _)) => StatusCode::BAD_REQUEST.into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn query_response_retains_admission_and_publication_until_transport_drop() {
        use receiver_directory::snapshot::Snapshot;
        use receiver_pir::{AcceptedCoverage, Client, MIN_ROWS};
        let snapshot =
            Snapshot::build(common::manifest(MIN_ROWS), &[common::record(0, 1)], &[]).unwrap();
        let publication = crate::prepared::prepare(snapshot, None).unwrap();
        let accepted = AcceptedCoverage {
            genesis: [1; 32],
            required_start: 100,
            height: 101,
            hash: [3; 32],
        };
        let client = Client::new(
            publication.server.manifest().clone(),
            publication.server.public(),
            accepted,
        )
        .unwrap();
        let publications = Publications::default();
        assert!(publications.publish(publication, 0));
        let prepared = Arc::downgrade(&publications.select(None).unwrap().0);
        let queue = Arc::new(Queue::new(1, 1, WAIT, true));
        let clients = ClientSlots::new(1);
        let service = Service {
            publications: publications.clone(),
            queue: queue.clone(),
            clients: clients.clone(),
            metrics: Default::default(),
            health_fields: Arc::new(serde_json::Map::new),
        };
        let request = client.prepare(common::receiver(), 0).unwrap();
        let response = query(
            State(service),
            Request::post("/v1/receiver/query")
                .body(axum::body::Body::from(request.body().to_vec()))
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
        let snapshot = receiver_directory::snapshot::Snapshot::build(
            common::manifest(receiver_pir::MIN_ROWS),
            &[],
            &[],
        )
        .unwrap();
        let publication = crate::prepared::prepare(snapshot, None).unwrap();
        let mut header = MAGIC.to_vec();
        header.extend(publication.id);
        let exact = query_bytes(receiver_pir::MIN_ROWS).unwrap();
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
        use receiver_directory::snapshot::Snapshot;
        use receiver_pir::{server::Server, AcceptedCoverage, Client, MIN_ROWS};
        let snapshot =
            Snapshot::build(common::manifest(MIN_ROWS), &[common::record(0, 1)], &[]).unwrap();
        let server = Server::new(snapshot).unwrap();
        let accepted = AcceptedCoverage {
            genesis: [1; 32],
            required_start: 100,
            height: 101,
            hash: [3; 32],
        };
        let client = Client::new(server.manifest().clone(), server.public(), accepted).unwrap();
        let publications = Publications::default();
        assert!(publications.publish(Publication::verified(server, None), 0));
        let queue = Arc::new(Queue::new(EVALUATING, WAITING, WAIT, true));
        let (url, task) = serve(publications.clone(), queue.clone()).await;

        let query = client.prepare(common::receiver(), 0).unwrap();
        let rotated = held_query(&url, &queue, query.body().to_vec(), || {
            let mut next = common::manifest(MIN_ROWS);
            (next.end_height, next.end_hash) = (102, [9; 32]);
            let next = Server::new(Snapshot::build(next, &[], &[]).unwrap()).unwrap();
            assert!(publications.publish(Publication::verified(next, None), 0));
        })
        .await;
        assert_eq!(rotated.status(), StatusCode::OK);
        let answer = rotated.bytes().await.unwrap();
        assert_eq!(
            client.decode(query, &answer).unwrap(),
            Some(common::record(0, 1))
        );

        let query = client.prepare(common::receiver(), 0).unwrap();
        let revoked = held_query(&url, &queue, query.body().to_vec(), || {
            publications.revoke()
        })
        .await;
        assert_eq!(revoked.status(), StatusCode::GONE);
        assert_eq!(revoked.headers()[header::CACHE_CONTROL], "no-store");
        task.abort();
    }
}

#[cfg(test)]
mod fixture_cache_tests {
    use super::*;
    use receiver_directory::snapshot::Snapshot;
    fn snapshot() -> Snapshot {
        Snapshot::build(common::manifest(receiver_pir::MIN_ROWS), &[], &[]).unwrap()
    }
    #[test]
    fn reused_public_rows_do_not_share_reports_or_hide_corruption() {
        let first = prepared::prepare(snapshot(), None).unwrap();
        let second = prepared::prepare(snapshot(), None).unwrap();
        assert!(std::ptr::eq(
            first.server.public().as_ptr(),
            second.server.public().as_ptr()
        ));
        let changed = first.with_report(serde_json::json!({"fixture": "changed"}));
        assert!(changed.report.is_some());
        assert!(second.report.is_none());
        let mut corrupt = snapshot();
        corrupt.data[0] ^= 1;
        assert!(prepared::prepare(corrupt, None).is_err());
        let mut corrupt = snapshot();
        corrupt.filters.push(1);
        assert!(prepared::prepare(corrupt, None).is_err());
        let mut corrupt = snapshot();
        corrupt.manifest.end_height = 0;
        assert!(prepared::prepare(corrupt, None).is_err());
    }
}
