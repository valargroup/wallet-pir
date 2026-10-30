//! Receiver HTTP, admission and response lifetime owned by the root query host.
use axum::{
    extract::{Path, Request, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use pir_control::admission::{client_key, read_body, BodyError, ClientSlots, Queue};
use receiver_pir_server::{tables, Publication, Publications};
use std::{sync::Arc, time::Duration};
use zakura_pir_query_server::receiver::session::{Error, HEADER_BYTES};
use zakura_pir_query_server::receiver::Hash;
use zakura_pir_query_server::receiver::{
    native::{query_bytes, MAGIC},
    MAX_COMPANION_ROWS,
};

// Preserve the historical bounded categories for unmatched product-shaped
// paths as well as this service's registered routes.
/// Only a fixed endpoint category survives classification. No dynamic path segment is retained.
fn endpoint(path: &str) -> Option<&'static str> {
    if path == "/v2/shards/init" {
        Some("init")
    } else if path == "/v2/shards" || path == "/v2/filters/shards" {
        Some("map")
    } else if path.starts_with("/v2/filters/") || path.starts_with("/v1/filters/") {
        Some("filter")
    } else if path.starts_with("/v2/shards/") && path.ends_with("/manifest") {
        Some("manifest")
    } else if path.starts_with("/v2/shards/") && path.contains("/setup/") {
        Some("setup")
    } else if let Some(route) = path
        .strip_prefix("/receiver/v1/")
        .or_else(|| path.strip_prefix("/v1/receiver/"))
    {
        receiver_endpoint(route)
    } else if path.starts_with("/v2/shards/") && path.contains("/query/") {
        if path.ends_with("/directory") {
            Some("query_directory")
        } else if path.ends_with("/pages") {
            Some("query_pages")
        } else {
            Some("query_other")
        }
    } else {
        None
    }
}

/// The receiver directory's route categories below either public prefix; session IDs
/// never become labels.
fn receiver_endpoint(route: &str) -> Option<&'static str> {
    Some(match route.split('/').next()? {
        "inventory" => "receiver_inventory",
        "shard-filters" => "receiver_shard_filters",
        "choice" => "receiver_choice",
        "public" => "receiver_public",
        "query" => "receiver_query",
        "block-filters" => "receiver_block_filters",
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
    /// Block hint ranges being encoded or sent; see [`MAX_FILTER_RANGES`].
    ranges: Arc<tokio::sync::Semaphore>,
    /// Filter and choice objects being loaded or sent; see [`MAX_OBJECT_LOADS`].
    objects: Arc<tokio::sync::Semaphore>,
}

/// Most block hint ranges built or sent at once. Each holds up to twice
/// `MAX_RANGE_BYTES` (its sets and their encoding); more are refused with 429.
const MAX_FILTER_RANGES: usize = 8;

/// Most filter and choice objects loaded or sent at once; more are refused with 429.
const MAX_OBJECT_LOADS: usize = 16;

/// The receiver categories `pir_observability` reports.
const ENDPOINTS: [&str; 7] = [
    "receiver_inventory",
    "receiver_shard_filters",
    "receiver_choice",
    "receiver_public",
    "receiver_query",
    "receiver_block_filters",
    "receiver_health",
];

/// Serve the current publication while the owner prepares, validates and rotates
/// revisions, with the admission and refusals of `docs/serving-contract.md`.
/// Cancellation never frees a still-running CPU slot. Wallet routes are served at the
/// canonical `/receiver/v1/<operation>` and at the deployed `/v1/receiver/<operation>`.
/// `/v1/receiver/health` and `/metrics` are for operators, outside the canonical
/// namespace; a deployment's edge proxies only the wallet routes.
/// Health also reports `fields`, though its own (`identity`, `serving`, `epoch`,
/// `indexer`) take precedence over theirs.
pub fn router_with_health(publications: Publications, fields: HealthFields) -> Router {
    router(Service {
        publications,
        queue: Arc::new(Queue::new(EVALUATING, WAITING, WAIT, true)),
        clients: ClientSlots::new(PER_CLIENT),
        metrics: pir_observability::HttpMetrics::default(),
        health_fields: fields,
        ranges: Arc::new(tokio::sync::Semaphore::new(MAX_FILTER_RANGES)),
        objects: Arc::new(tokio::sync::Semaphore::new(MAX_OBJECT_LOADS)),
    })
}

/// The routes of [`router_with_health`] over `service`: the operator routes, and the
/// wallet operations mounted once at the canonical prefix and once at the deployed
/// legacy prefix, reaching the same handlers.
fn router(service: Service) -> Router {
    let metrics = service.metrics.clone();
    metrics.initialize(&ENDPOINTS);
    let wallet = wallet_routes();
    Router::new()
        .route("/v1/receiver/health", get(health))
        .route(
            "/metrics",
            get(|State(s): State<Service>| async move {
                ([(header::CONTENT_TYPE, "text/plain")], s.metrics.render())
            }),
        )
        .nest("/receiver/v1", wallet.clone())
        .nest("/v1/receiver", wallet)
        .layer(axum::middleware::map_response(no_store_errors))
        .layer(axum::middleware::from_fn_with_state(metrics, observe))
        .with_state(service)
}

/// The wallet-facing receiver operations, relative to a public prefix. Each is defined
/// only here; operator routes never join them, so no prefix exposes those.
fn wallet_routes() -> Router<Service> {
    Router::new()
        .route(
            "/inventory",
            get(|State(s): State<Service>| async move {
                // Nothing is served for seconds after this server starts or the
                // publisher restarts; the inventory names no view, so it waits.
                match unavailable_held(|| s.publications.current()).await {
                    Ok(p) => json(p.head_bytes().to_vec()),
                    Err(status) => status.into_response(),
                }
            }),
        )
        .route(
            "/inventory/:inventory/:chunk",
            get(
                |State(s): State<Service>, Path((id, chunk)): Path<(String, usize)>| async move {
                    let Some(id) = hash(&id) else {
                        return StatusCode::BAD_REQUEST.into_response();
                    };
                    match s.publications.inventory(&id) {
                        Ok((p, _)) => match p.chunk(chunk) {
                            Some(bytes) => json(bytes.to_vec()),
                            None => StatusCode::NOT_FOUND.into_response(),
                        },
                        Err(status) => status.into_response(),
                    }
                },
            ),
        )
        .route(
            "/shard-filters/:digest",
            get(|State(s): State<Service>, Path(digest): Path<String>| async move {
                object(&s, &digest, Publications::with_filter, |p, d| p.filter(d).cloned()).await
            }),
        )
        .route(
            "/choice/:digest",
            get(|State(s): State<Service>, Path(digest): Path<String>| async move {
                object(&s, &digest, Publications::with_choice, |p, d| p.choice(d).cloned()).await
            }),
        )
        .route(
            "/public/:session",
            get(|State(s): State<Service>, Path(id): Path<String>| async move { public(&s, &id) }),
        )
        .route("/query", post(query))
        .route("/block-filters/head", get(|State(s): State<Service>| async move {
            match unavailable_held(|| s.publications.filters_in_epoch().map(|(window, _)| window)).await {
                Ok(window) => ([(header::CACHE_CONTROL, "no-store")], Json(window.head().clone())).into_response(),
                Err(status) => status.into_response(),
            }
        }))
        .route("/block-filters/:revision/:from/:limit", get(
            |State(s): State<Service>, Path((revision, from, limit)): Path<(u64, u32, u32)>| async move {
                match with_hints(&s, move |window| window.range(revision, from, limit)).await {
                    Ok((bytes, permit)) => {
                        // The permit is held until the encoded range is sent.
                        let mut response = binary(bytes);
                        let body = std::mem::replace(response.body_mut(), axum::body::Body::empty());
                        *response.body_mut() = pir_control::http::retain(body, permit);
                        response
                    }
                    Err(response) => response,
                }
            }
        ))
}

/// Runs `work` on the current block hints off the async runtime under one of the
/// [`MAX_FILTER_RANGES`] permits, which the blocking job owns, so a cancelled request
/// cannot admit more work while its own still runs; the permit is returned with the
/// result. Hints revoked or invalidated meanwhile could wrongly rule a payment out, so,
/// as for shard filters, that result is refused as gone.
async fn with_hints<T: Send + 'static>(
    s: &Service,
    work: impl FnOnce(&receiver_pir_server::blocks::BlockWindow) -> Result<T, StatusCode>
        + Send
        + 'static,
) -> Result<(T, tokio::sync::OwnedSemaphorePermit), Response> {
    let (window, epoch) = s
        .publications
        .filters_in_epoch()
        .map_err(IntoResponse::into_response)?;
    let Ok(permit) = s.ranges.clone().try_acquire_owned() else {
        return Err(overloaded());
    };
    let (result, (permit, window)) =
        pir_control::admission::spawn_admitted((permit, window), move |(_, window)| work(window))
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;
    if s.publications.epoch() != epoch || !window.authorized() {
        return Err(StatusCode::GONE.into_response());
    }
    drop(window);
    result
        .map(|value| (value, permit))
        .map_err(IntoResponse::into_response)
}

/// `read`'s answer, re-read while it is 503 for up to [`crate::hold::WINDOW`].
async fn unavailable_held<T>(read: impl Fn() -> Result<T, StatusCode>) -> Result<T, StatusCode> {
    crate::hold::until(
        crate::hold::WINDOW,
        |status: &StatusCode| *status == StatusCode::SERVICE_UNAVAILABLE,
        || std::future::ready(read()),
        || StatusCode::SERVICE_UNAVAILABLE,
    )
    .await
}

/// A live inventory a request selected, with the recovery epoch, or the refusal.
type Selected = Result<(Arc<Publication>, u64), StatusCode>;

/// Serves the filter or choice object with hex digest `digest` from the live inventory
/// `select` finds, whose source `source` gives, under one of the [`MAX_OBJECT_LOADS`]
/// permits, which the blocking load owns and the response keeps until sent. Its bytes
/// are read into the table budget and stay counted until the response is sent; holding the selected inventory
/// meanwhile keeps its file (see `Publications::references`). A revocation during the
/// load is 410, as for queries: a revoked inventory's object could wrongly rule a
/// payment out.
async fn object(
    s: &Service,
    digest: &str,
    select: fn(&Publications, &Hash) -> Selected,
    source: fn(&Publication, &Hash) -> Option<Arc<tables::ObjectSource>>,
) -> Response {
    let Some(digest) = hash(digest) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let (publication, epoch) = match select(&s.publications, &digest) {
        Ok(selected) => selected,
        Err(status) => return status.into_response(),
    };
    let Ok(permit) = s.objects.clone().try_acquire_owned() else {
        return overloaded();
    };
    let authorization = publication.authorization();
    let source = source(&publication, &digest).expect("selected by digest");
    let tables = s.publications.tables();
    let loaded =
        pir_control::admission::spawn_admitted(permit, move |_| tables.object(&source)).await;
    if s.publications.epoch() != epoch || !authorization.load(std::sync::atomic::Ordering::Acquire)
    {
        return StatusCode::GONE.into_response();
    }
    drop(publication);
    match loaded {
        Ok((Ok(resident), permit)) => {
            let mut response = binary(axum::body::Bytes::from_owner(Object(resident)));
            let body = std::mem::replace(response.body_mut(), axum::body::Body::empty());
            *response.body_mut() = pir_control::http::retain(body, permit);
            response
        }
        Ok((Err(tables::LoadError::Busy), _)) => overloaded(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// A resident object's encoding as a response body, holding its budget until sent.
struct Object(Arc<tables::Resident>);

impl AsRef<[u8]> for Object {
    /// The encoding the resident entry holds.
    fn as_ref(&self) -> &[u8] {
        self.0.object().expect("an object entry")
    }
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

/// A 32-byte hash from its hex route segment.
fn hash(hex: &str) -> Option<zakura_pir_query_server::receiver::Hash> {
    hex::decode(hex).ok()?.try_into().ok()
}

/// An uncached JSON response of already serialized `body`.
fn json(body: Vec<u8>) -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}

/// Serves the public PIR setup of the directory or companion session whose hex id is `id`.
fn public(s: &Service, id: &str) -> Response {
    let Some(id) = hash(id) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    match s.publications.select(&id) {
        Ok((p, _)) => binary(p.table(&id).expect("selected by id").public().to_vec()),
        Err(status) => status.into_response(),
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

/// Answers one PIR query against the directory or companion whose session its header
/// names. The upload is read before the query waits for an evaluation slot, so a slow
/// upload holds only its client's place. A directory query covers every row; a
/// companion query every row or exactly its `query_rows` prefix. Any other length is
/// refused (413 past the full length, else 400) without waiting.
async fn query(State(s): State<Service>, request: Request) -> Response {
    let Some(client) = s.clients.try_acquire(&client_key(request.headers(), None)) else {
        return overloaded();
    };
    let limit = query_bytes(MAX_COMPANION_ROWS);
    let body = match read_body(request.into_body(), limit, UPLOAD).await {
        Ok(body) => body,
        Err(BodyError::Timeout) => return StatusCode::REQUEST_TIMEOUT.into_response(),
        Err(e) if e.over_limit() => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    if body.len() < HEADER_BYTES || &body[..4] != MAGIC {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let id: zakura_pir_query_server::receiver::Hash = body[4..36].try_into().unwrap();
    let (publication, epoch) = match s.publications.select(&id) {
        Ok(p) => p,
        Err(status) => return status.into_response(),
    };
    let source = publication.table(&id).expect("selected by id").clone();
    let (full, trimmed) = (query_bytes(source.rows()), query_bytes(source.query_rows()));
    if body.len() > full {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    if body.len() != full && body.len() != trimmed {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(permit) = s.queue.acquire(|| ()).await else {
        return overloaded();
    };
    let authorization = publication.authorization();
    let tables = s.publications.tables();
    let result =
        pir_control::admission::spawn_admitted(
            (permit, client, publication),
            move |_| match tables.get(&source) {
                Ok(resident) => resident.respond(&body).map_err(Some),
                // Tables in use fill the memory budget; the client retries.
                Err(tables::LoadError::Busy) => Err(None),
                Err(tables::LoadError::Load(error)) => Err(Some(error)),
            },
        )
        .await;
    // A revocation during evaluation is 410; after it, wallets revalidate their anchor.
    if s.publications.epoch() != epoch || !authorization.load(std::sync::atomic::Ordering::Acquire)
    {
        return StatusCode::GONE.into_response();
    }
    match result {
        Ok((Ok(body), guards)) => {
            let mut response = binary(body);
            let body = std::mem::replace(response.body_mut(), axum::body::Body::empty());
            *response.body_mut() = pir_control::http::retain(body, guards);
            response
        }
        Ok((Err(Some(Error::Revision)), _)) => StatusCode::GONE.into_response(),
        Ok((Err(Some(Error::Malformed)), _)) => StatusCode::BAD_REQUEST.into_response(),
        Ok((Err(None), _)) => overloaded(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[cfg(test)]
#[path = "routes_tests.rs"]
mod tests;
