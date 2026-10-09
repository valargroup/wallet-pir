//! Receiver PIR serving with immutable publications and atomic session revocation.
#[cfg(test)]
#[path = "../../../crates/receiver-directory/tests/common/mod.rs"]
mod common;
pub mod publication;
use axum::{
    body::{Body, Bytes},
    extract::{Path, Request, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use http_body::{Body as HttpBody, Frame, SizeHint};
use pir_control::admission::{client_key, read_body, BodyError, ClientSlots, Queue};
pub use publication::{Publication, Publications};
use receiver_pir::{query_bytes, Error, HEADER_BYTES, MAGIC, MAX_ROWS};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

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
/// Bytes of a file response sent between checks for a revocation.
const CHUNK: usize = 64 * 1024;

#[derive(Clone)]
struct Service {
    publications: Publications,
    queue: Arc<Queue>,
    clients: ClientSlots,
    metrics: pir_observability::HttpMetrics,
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

/// Serve the current publication while the owner prepares, validates and rotates revisions.
/// Queries are admitted with the shared `pir_control::admission` primitives: two in flight
/// per client, then a wait of up to 2 seconds behind at most eight others for one of two
/// evaluation slots. Overload is 429 with `Retry-After: 1`, as Enhance answers.
/// Cancellation never frees a still-running CPU slot. `/v1/receiver/health` reports the
/// process identity of `docs/serving-contract.md` and the owner's report, and `/metrics`
/// the shared HTTP observations; both are for operators, and a deployment's edge proxies
/// only the wallet routes. A revocation aborts the session files still being sent.
pub fn router_with_publications(publications: Publications) -> Router {
    router(Service {
        publications,
        queue: Arc::new(Queue::new(EVALUATING, WAITING, WAIT, true)),
        clients: ClientSlots::new(PER_CLIENT),
        metrics: pir_observability::HttpMetrics::default(),
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
        .layer(axum::middleware::from_fn_with_state(
            metrics,
            pir_observability::observe,
        ))
        .with_state(service)
}

/// Marks every error response `no-store`, leaving its status, other headers and body
/// as they are. Caches may keep a 410 heuristically, but a revoked session id is
/// deterministic and can be published again, so no refusal may outlive the state that
/// produced it.
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
    let serving = s
        .publications
        .select(None)
        .ok()
        .map(|(p, _)| hex::encode(p.id));
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "identity": pir_control::Identity::process(),
            "serving": serving,
            "epoch": s.publications.epoch(),
            "indexer": s.publications.report(),
        })),
    )
        .into_response()
}

enum Material {
    Public,
    Witness,
    Rows,
    Filters,
}

/// Serves one file of the publication whose hex id is `id`, as a [`FencedBody`].
fn material(s: &Service, id: &str, material: Material) -> Response {
    let Some(id) = hex::decode(id).ok().and_then(|v| v.try_into().ok()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let (p, epoch) = match s.publications.select(Some(id)) {
        Ok(selected) => selected,
        Err(status) => return status.into_response(),
    };
    let bytes = match material {
        Material::Public => Bytes::copy_from_slice(p.server.public()),
        Material::Rows => Bytes::from_owner(p.server.rows()),
        Material::Filters => Bytes::from_owner(p.server.filters()),
        Material::Witness => match &p.witnesses {
            Some(bytes) => bytes.clone(),
            None => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        },
    };
    binary(Body::new(FencedBody {
        bytes,
        publications: s.publications.clone(),
        epoch,
    }))
}

/// A file response sent in [`CHUNK`]-byte frames that fails, aborting the transfer,
/// once a revocation advances the epoch past the one its session was selected in.
/// The status and length are already sent, so the client sees a truncated body; bytes
/// already sent cannot be recalled, which is why wallets still check their anchor.
struct FencedBody {
    bytes: Bytes,
    publications: Publications,
    epoch: u64,
}

impl HttpBody for FencedBody {
    type Data = Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
        if self.bytes.is_empty() {
            return Poll::Ready(None);
        }
        if self.publications.epoch() != self.epoch {
            return Poll::Ready(Some(Err(std::io::Error::other("publication revoked"))));
        }
        let n = self.bytes.len().min(CHUNK);
        Poll::Ready(Some(Ok(Frame::data(self.bytes.split_to(n)))))
    }
    fn is_end_stream(&self) -> bool {
        self.bytes.is_empty()
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.bytes.len() as u64)
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
    let result = tokio::task::spawn_blocking(move || {
        // The blocking task outlives a disconnected request, so it owns admission until it finishes.
        let _admitted = (permit, client);
        publication.server.respond(&body)
    })
    .await;
    if s.publications.epoch() != epoch {
        return StatusCode::GONE.into_response();
    }
    match result {
        Ok(Ok(body)) => binary(body),
        Ok(Err(Error::Revision)) => StatusCode::CONFLICT.into_response(),
        Ok(Err(Error::Malformed)) => StatusCode::BAD_REQUEST.into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let publication =
            Publication::new(receiver_pir::server::Server::new(snapshot).unwrap(), None).unwrap();
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
        let app = router(Service {
            publications,
            queue,
            clients: ClientSlots::new(PER_CLIENT),
            metrics: pir_observability::HttpMetrics::default(),
        });
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1/receiver/query", socket.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
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
}
