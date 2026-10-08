//! Receiver PIR serving with immutable publications and atomic session revocation.
pub mod publication;
use axum::{
    extract::{Path, Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use pir_control::admission::{client_key, read_body, BodyError, ClientSlots, Queue};
pub use publication::{Publication, Publications};
use receiver_pir::{query_bytes, Error, HEADER_BYTES, MAGIC, MAX_ROWS};
use std::{sync::Arc, time::Duration};

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
/// only the wallet routes.
pub fn router_with_publications(publications: Publications) -> Router {
    let metrics = pir_observability::HttpMetrics::default();
    metrics.initialize(&ENDPOINTS);
    let service = Service {
        publications,
        queue: Arc::new(Queue::new(EVALUATING, WAITING, WAIT, true)),
        clients: ClientSlots::new(PER_CLIENT),
        metrics: metrics.clone(),
    };
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
        .layer(axum::middleware::from_fn_with_state(
            metrics,
            pir_observability::observe,
        ))
        .with_state(service)
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
        Material::Rows => binary(axum::body::Bytes::from_owner(p.server.rows())),
        Material::Filters => binary(axum::body::Bytes::from_owner(p.server.filters())),
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
/// its client's place.
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
