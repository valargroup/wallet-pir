//! The public Enhance query surface: manifest and session reads, the
//! compatibility query endpoint, health, readiness and metrics.
//!
//! The gateway owns admission, response guards and the single shared
//! generation-view lock. The publisher installs immutable prepared views under
//! that lock and answers [`PublisherStatus`] from its current durable state;
//! the gateway never prepares, commits, recovers or mutates publication state.
//! Lock order is the view lock, then any publisher state the status reads.
use crate::{
    http_metrics::HttpMetrics, packing_budget::PackingBudget, protocol::*,
    runtime::PublishedPacking, worker,
};
use axum::{
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::{RwLock, Semaphore};

// Two replicas each admit two evaluations. Keep the coordinator aligned with
// that aggregate worker capacity so it does not queue behind idle replicas.
const QUERY_ACTIVE_LIMIT: usize = 4;
const QUERY_WAIT_LIMIT: usize = 16;
const QUERY_WAIT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(2);
const QUERY_BODY_LIMIT: usize = 512 * 1024;
/// Concurrent public manifest/session reads; a flood cannot pin the snapshot lock.
const PUBLIC_READ_LIMIT: usize = 64;

#[derive(Default)]
struct QueryStats {
    route_cursor: AtomicU64,
    wait_count: AtomicU64,
    wait_micros: AtomicU64,
    rejected: AtomicU64,
    primary_success: AtomicU64,
    fallback_success: AtomicU64,
    worker_busy: AtomicU64,
    worker_failure: AtomicU64,
}

#[derive(Debug)]
struct QueryError(StatusCode, String);

impl From<(StatusCode, String)> for QueryError {
    fn from((status, message): (StatusCode, String)) -> Self {
        Self(status, message)
    }
}

impl IntoResponse for QueryError {
    fn into_response(self) -> Response {
        crate::query_serving::public_error((self.0, self.1))
    }
}

/// A retained generation's saved routing and packing metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SavedSnapshot {
    pub manifest: Manifest,
    pub routes: BTreeMap<u64, Vec<String>>,
    #[serde(default)]
    pub prepared: BTreeMap<u64, crate::prepared_packing::Artifact>,
    pub hints: BTreeMap<u64, String>,
    pub domain_keys: BTreeMap<u64, String>,
}

/// An immutable prepared view of one retained generation.
pub struct Snapshot {
    pub saved: SavedSnapshot,
    pub packing: BTreeMap<u64, Arc<PublishedPacking>>,
}

/// Retained generations, newest first, behind the one generation-view lock.
/// Admitted queries pin it; only the publisher writes it.
pub type Views = Arc<RwLock<Vec<Arc<Snapshot>>>>;

/// Publisher fields of the health document, already projected for JSON.
#[derive(Default)]
pub struct PublisherHealth {
    pub registered_groups: usize,
    pub registered_workers: usize,
    pub pool: serde_json::Value,
    pub capacity: serde_json::Value,
    pub published_replica_counts: serde_json::Value,
    pub pending_commit_notifications: usize,
    pub pending_abort_notifications: usize,
    pub operation: serde_json::Value,
    pub blocked_reason: Option<String>,
}

/// Synchronous, read-only publisher state consulted by the gateway. Every
/// call reads the publisher's current state; implementations must not cache
/// revocations. No method may prepare, commit, recover or mutate publication.
pub trait PublisherStatus: Send + Sync + 'static {
    /// Whether the durable recovery state currently revokes this session ID.
    fn revoked(&self, session_id: &str) -> bool;
    /// Whether exact-session packing runs on remote packing routers.
    fn remote_packing(&self) -> bool;
    /// Whether remote packing routers have activated this generation.
    fn public_ready(&self, generation: u64) -> bool;
    /// Publisher fields of `/v1/health` for the current view.
    fn health(&self, current: Option<&Snapshot>) -> PublisherHealth;
    /// Publisher Prometheus families for the current manifest and route counts.
    fn metrics(&self, manifest: Option<&Manifest>, routes: &BTreeMap<u64, usize>) -> String;
}

/// A publisher with no publication, inventory or remote packing: the gateway
/// it backs answers readiness probes as unavailable.
pub struct Unpublished;

impl PublisherStatus for Unpublished {
    fn revoked(&self, _: &str) -> bool {
        false
    }
    fn remote_packing(&self) -> bool {
        false
    }
    fn public_ready(&self, _: u64) -> bool {
        false
    }
    fn health(&self, _: Option<&Snapshot>) -> PublisherHealth {
        PublisherHealth {
            published_replica_counts: serde_json::json!({}),
            ..Default::default()
        }
    }
    fn metrics(&self, _: Option<&Manifest>, _: &BTreeMap<u64, usize>) -> String {
        String::new()
    }
}

#[derive(Clone)]
pub struct QueryGateway {
    packing_budget: PackingBudget,
    views: Views,
    queries: Arc<Semaphore>,
    query_waiters: Arc<Semaphore>,
    public_reads: Arc<Semaphore>,
    query_stats: Arc<QueryStats>,
    http_metrics: HttpMetrics,
    http: reqwest::Client,
    status: Arc<dyn PublisherStatus>,
}

impl QueryGateway {
    pub fn new(
        status: Arc<dyn PublisherStatus>,
        packing_budget: PackingBudget,
        views: Vec<Arc<Snapshot>>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            packing_budget,
            views: Arc::new(RwLock::new(views)),
            queries: Arc::new(Semaphore::new(QUERY_ACTIVE_LIMIT)),
            query_waiters: Arc::new(Semaphore::new(QUERY_WAIT_LIMIT)),
            public_reads: Arc::new(Semaphore::new(PUBLIC_READ_LIMIT)),
            query_stats: Arc::new(QueryStats::default()),
            http_metrics: HttpMetrics::default(),
            http,
            status,
        }
    }

    /// An empty gateway for a deliberately unready Enhance service.
    pub fn unpublished() -> Self {
        Self::new(
            Arc::new(Unpublished),
            PackingBudget::coordinator(),
            Vec::new(),
            reqwest::Client::new(),
        )
    }

    /// The shared generation-view lock. Only the publisher writes it.
    pub fn views(&self) -> &Views {
        &self.views
    }

    pub fn packing_budget(&self) -> &PackingBudget {
        &self.packing_budget
    }

    pub async fn manifest(&self) -> Option<Manifest> {
        self.views
            .read()
            .await
            .first()
            .map(|s| s.saved.manifest.clone())
    }

    pub fn router(self) -> Router {
        self.http_metrics.endpoint("init");
        self.http_metrics.endpoint("query");
        Router::new()
            .route("/v1/enhance/init", get(init))
            .route("/v1/enhance/sessions/:generation/:shard", get(session))
            .route("/v1/enhance/session/:session_id", get(session_by_id))
            .route("/v1/enhance/query", post(query))
            .route("/v1/health", get(health))
            .route("/ready", get(ready))
            .route("/metrics", get(metrics))
            .layer(axum::middleware::from_fn_with_state(
                self.http_metrics.clone(),
                crate::http_metrics::measure,
            ))
            .with_state(self)
    }
}

type ApiResult<T> = Result<T, (StatusCode, String)>;
type QueryResult<T> = Result<T, QueryError>;

async fn query_body(request: Request) -> QueryResult<axum::body::Bytes> {
    use crate::admission::BodyError;
    crate::admission::read_body(
        request.into_body(),
        QUERY_BODY_LIMIT,
        std::time::Duration::from_secs(30),
    )
    .await
    .map_err(|e| match e {
        BodyError::Timeout => QueryError(StatusCode::REQUEST_TIMEOUT, e.to_string()),
        BodyError::Read(_) => QueryError(StatusCode::PAYLOAD_TOO_LARGE, e.to_string()),
    })
}

/// Records one admission wait into the query stats when dropped.
struct WaitStats<'a> {
    stats: &'a QueryStats,
    started: std::time::Instant,
}
impl<'a> WaitStats<'a> {
    fn start(stats: &'a QueryStats) -> Self {
        Self {
            stats,
            started: std::time::Instant::now(),
        }
    }
}
impl Drop for WaitStats<'_> {
    fn drop(&mut self) {
        self.stats.wait_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .wait_micros
            .fetch_add(self.started.elapsed().as_micros() as u64, Ordering::Relaxed);
    }
}

/// Callers have already read the bounded body, so the public reverse proxy is
/// never left writing to a closed upstream connection.
fn reject_query(c: &QueryGateway) -> QueryResult<Response> {
    c.query_stats.rejected.fetch_add(1, Ordering::Relaxed);
    Err(QueryError(
        StatusCode::TOO_MANY_REQUESTS,
        "query limit".into(),
    ))
}

fn public_read(c: &QueryGateway) -> ApiResult<tokio::sync::OwnedSemaphorePermit> {
    c.public_reads
        .clone()
        .try_acquire_owned()
        .map_err(|_| (StatusCode::TOO_MANY_REQUESTS, "overloaded".into()))
}

/// Publication inserts a generation before its packing routers are activated;
/// init waits out that short window instead of answering a transient 503.
const INIT_ACTIVATION_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

async fn init(State(c): State<QueryGateway>) -> ApiResult<Json<Manifest>> {
    let _read = public_read(&c)?;
    let deadline = tokio::time::Instant::now() + INIT_ACTIVATION_WAIT;
    let manifest = loop {
        let manifest = c.manifest().await;
        let unready = c.status.remote_packing()
            && manifest
                .as_ref()
                .is_none_or(|m| !c.status.public_ready(m.generation));
        if !unready || manifest.is_none() {
            break manifest;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "packing routers not activated".into(),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    if c.status.remote_packing() && manifest.is_none() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "packing routers not activated".into(),
        ));
    }
    manifest.map(Json).ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "no published generation".into(),
    ))
}
async fn session(
    State(c): State<QueryGateway>,
    Path((generation, shard)): Path<(u64, u64)>,
) -> ApiResult<Json<ShardSession>> {
    let _read = public_read(&c)?;
    let snapshots = c.views.read().await;
    let s = snapshots
        .iter()
        .find(|s| s.saved.manifest.generation == generation)
        .ok_or((StatusCode::GONE, "expired session".into()))?;
    let id = s
        .saved
        .manifest
        .session_id(shard)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    if c.status.revoked(&hex::encode(id)) {
        return Err((StatusCode::GONE, "noncanonical_session".into()));
    }
    let pack = s
        .packing
        .get(&shard)
        .ok_or((StatusCode::BAD_REQUEST, "wrong shard".into()))?;
    Ok(Json(pack.session(&s.saved.manifest, shard)))
}
async fn session_by_id(
    State(c): State<QueryGateway>,
    Path(id): Path<String>,
) -> QueryResult<Json<ShardSession>> {
    let _read = public_read(&c)?;
    if !crate::protocol::canonical_hash(&id) {
        return Err(QueryError(
            StatusCode::BAD_REQUEST,
            "invalid session id".into(),
        ));
    }
    let snapshots = c.views.read().await;
    if c.status.revoked(&id) {
        return Err(QueryError(StatusCode::GONE, "noncanonical_session".into()));
    }
    for snapshot in snapshots.iter() {
        let manifest = &snapshot.saved.manifest;
        for shard in &manifest.coverage.shards {
            if manifest
                .session_id(shard.id)
                .is_ok_and(|hash| hex::encode(hash) == id)
            {
                return Ok(Json(
                    snapshot.packing[&shard.id].session(manifest, shard.id),
                ));
            }
        }
    }
    Err(QueryError(StatusCode::GONE, "session_unavailable".into()))
}

async fn query(State(c): State<QueryGateway>, request: Request) -> QueryResult<Response> {
    if c.status.remote_packing() {
        return Err(QueryError(
            StatusCode::SERVICE_UNAVAILABLE,
            "query serving moved to packing router".into(),
        ));
    }
    // Read the bounded body before any wait or admission permit is charged.
    let body = query_body(request).await?;
    // Every query takes a waiting slot; the wait is recorded whether or not
    // it ends in a permit.
    let acquired = crate::admission::Queue::with_permits(
        c.queries.clone(),
        c.query_waiters.clone(),
        QUERY_WAIT_DEADLINE,
        false,
    )
    .acquire(|| WaitStats::start(&c.query_stats))
    .await;
    let permit = match acquired {
        Ok(permit) => permit,
        Err(_) => return reject_query(&c),
    };
    let binding = QueryBinding::decode(&body).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let generation_pin = c.views.clone().read_owned().await;
    if c.status.revoked(&hex::encode(binding.session_id)) {
        return Err(QueryError(StatusCode::GONE, "noncanonical_session".into()));
    }
    let snapshot = generation_pin
        .first()
        .cloned()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no routing view".into()))?;
    let manifest = &snapshot.saved.manifest;
    let query_rows = crate::query_serving::validate_binding(manifest, binding)?;
    let pack = snapshot
        .packing
        .get(&binding.shard_id)
        .cloned()
        .ok_or((StatusCode::BAD_REQUEST, "wrong shard".into()))?;
    // The admitted task owns capacity even if its HTTP caller disconnects.
    crate::query_serving::admitted(async move {
        let coefficients = pack
            .query_coefficients(&body, binding, query_rows)
            .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
        let request = worker::Evaluate {
            binding: Some(binding.encode()),
            generation: binding.generation,
            shard_id: binding.shard_id,
            epoch: hex::encode(binding.epoch),
            session_id: hex::encode(binding.session_id),
            coefficients,
        };
        let routes = snapshot
            .saved
            .routes
            .get(&binding.shard_id)
            .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no replica route".into()))?;
        let answer = evaluate_query(&c, &request, routes).await?;
        let (response, guards) = tokio::task::spawn_blocking(move || {
            let response = pack.pack(&body, &answer, query_rows)?;
            Ok::<_, String>((response, (permit, snapshot, generation_pin, pack)))
        })
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
        guarded_query_response(c, hex::encode(binding.session_id), response, guards)
    })
    .await
    .map_err(QueryError::from)?
}

async fn evaluate_query(
    c: &QueryGateway,
    request: &worker::Evaluate,
    routes: &[String],
) -> QueryResult<Vec<u64>> {
    if routes.is_empty() {
        return Err(QueryError(
            StatusCode::SERVICE_UNAVAILABLE,
            "no replica route".into(),
        ));
    }
    let first = c.query_stats.route_cursor.fetch_add(1, Ordering::Relaxed) as usize % routes.len();
    let mut last_busy = false;
    let answer = crate::query_serving::evaluate_observed(
        &c.http,
        request,
        routes.len(),
        |excluded| {
            let url = (0..routes.len())
                .map(|offset| &routes[(first + offset) % routes.len()])
                .find(|url| Some(url.as_str()) != excluded)
                .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no replica route".into()))?;
            Ok((url.clone(), ()))
        },
        |failure| {
            last_busy = matches!(failure, crate::query_serving::AttemptFailure::Busy);
            if last_busy {
                c.query_stats.worker_busy.fetch_add(1, Ordering::Relaxed);
            } else {
                c.query_stats.worker_failure.fetch_add(1, Ordering::Relaxed);
            }
        },
    )
    .await
    .map_err(|error| {
        // Preserve the compatibility endpoint's worker-overload response, without
        // replaying evaluations whose acceptance is unknown.
        if last_busy {
            c.query_stats.rejected.fetch_add(1, Ordering::Relaxed);
            QueryError(StatusCode::TOO_MANY_REQUESTS, "workers not admitted".into())
        } else {
            QueryError::from(error)
        }
    })?;
    if answer.attempts == 1 {
        c.query_stats
            .primary_success
            .fetch_add(1, Ordering::Relaxed);
    } else {
        c.query_stats
            .fallback_success
            .fetch_add(1, Ordering::Relaxed);
    }
    Ok(answer.coefficients)
}

fn guarded_query_response<G: Send + Unpin + 'static>(
    c: QueryGateway,
    session: String,
    response: Vec<u8>,
    guards: G,
) -> QueryResult<Response> {
    let fence = move || {
        if c.status.revoked(&session) {
            Err("noncanonical_session".to_string())
        } else {
            Ok(())
        }
    };
    fence().map_err(|e| (StatusCode::GONE, e))?;
    Ok(crate::response_body::guarded(response, guards, fence).into_response())
}

async fn health(State(c): State<QueryGateway>) -> Json<serde_json::Value> {
    let snapshots = c.views.read().await;
    let manifest = snapshots.first().map(|s| &s.saved.manifest);
    let packing_ready = !c.status.remote_packing()
        || manifest
            .as_ref()
            .is_some_and(|m| c.status.public_ready(m.generation));
    let publisher = c.status.health(snapshots.first().map(|s| s.as_ref()));
    Json(
        serde_json::json!({"packing_charged_bytes":c.packing_budget.charged_bytes(),"protocol":PROTOCOL_REVISION,"generation":manifest.as_ref().map(|m|m.generation),
        "anchor_height":manifest.as_ref().map(|m|m.anchor_height),"placement_revision":manifest.as_ref().map(|m|m.placement_revision),
        "registered_groups":publisher.registered_groups,
        "registered_workers":publisher.registered_workers,
        "pool":publisher.pool,
        "remote_packing":c.status.remote_packing(),
        "resident_packing_objects":snapshots.iter().flat_map(|s|s.packing.values()).filter(|p|p.is_serving()).map(|p|Arc::as_ptr(p) as usize).collect::<BTreeSet<_>>().len(),
        "packing_ready":packing_ready,
        "capacity":publisher.capacity,
        "published_replica_counts":publisher.published_replica_counts,
        "pending_commit_notifications":publisher.pending_commit_notifications,
        "pending_abort_notifications":publisher.pending_abort_notifications,
        "operation":publisher.operation,"blocked_reason":publisher.blocked_reason}),
    )
}
async fn ready(State(c): State<QueryGateway>) -> StatusCode {
    if c.manifest().await.is_some() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn metrics(
    State(c): State<QueryGateway>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    let snapshots = c.views.read().await;
    let manifest = snapshots.first().map(|s| &s.saved.manifest);
    let routes = snapshots
        .first()
        .map(|s| {
            s.saved
                .routes
                .iter()
                .map(|(id, replicas)| (*id, replicas.len()))
                .collect()
        })
        .unwrap_or_default();
    let mut body = c.status.metrics(manifest, &routes);
    body.push_str(&c.http_metrics.render());
    body.push_str(&crate::prepared_packing::metrics());
    for (name, value) in [
        (
            "enhance_query_active",
            (QUERY_ACTIVE_LIMIT - c.queries.available_permits()) as u64,
        ),
        (
            "enhance_query_waiting",
            (QUERY_WAIT_LIMIT - c.query_waiters.available_permits()) as u64,
        ),
        (
            "enhance_query_wait_count_total",
            c.query_stats.wait_count.load(Ordering::Relaxed),
        ),
        (
            "enhance_query_wait_microseconds_total",
            c.query_stats.wait_micros.load(Ordering::Relaxed),
        ),
        (
            "enhance_query_rejected_total",
            c.query_stats.rejected.load(Ordering::Relaxed),
        ),
        (
            "enhance_query_primary_success_total",
            c.query_stats.primary_success.load(Ordering::Relaxed),
        ),
        (
            "enhance_query_fallback_success_total",
            c.query_stats.fallback_success.load(Ordering::Relaxed),
        ),
        (
            "enhance_query_worker_busy_total",
            c.query_stats.worker_busy.load(Ordering::Relaxed),
        ),
        (
            "enhance_query_worker_failure_total",
            c.query_stats.worker_failure.load(Ordering::Relaxed),
        ),
    ] {
        let kind = if name.ends_with("_total") {
            "counter"
        } else {
            "gauge"
        };
        body.push_str(&format!("# TYPE {name} {kind}\n{name} {value}\n"));
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use std::sync::Mutex;

    /// Publisher state a test can revoke while a response is streaming.
    #[derive(Default)]
    struct Revocations(Mutex<BTreeSet<String>>);

    impl PublisherStatus for Revocations {
        fn revoked(&self, session_id: &str) -> bool {
            self.0.lock().unwrap().contains(session_id)
        }
        fn remote_packing(&self) -> bool {
            false
        }
        fn public_ready(&self, _: u64) -> bool {
            false
        }
        fn health(&self, _: Option<&Snapshot>) -> PublisherHealth {
            PublisherHealth::default()
        }
        fn metrics(&self, _: Option<&Manifest>, _: &BTreeMap<u64, usize>) -> String {
            String::new()
        }
    }

    fn test_gateway() -> QueryGateway {
        QueryGateway::unpublished()
    }

    fn empty_query() -> axum::extract::Request {
        axum::http::Request::builder()
            .uri("/v1/enhance/query")
            .body(axum::body::Body::empty())
            .unwrap()
    }

    async fn occupy_query_slots(gateway: &QueryGateway) -> Vec<tokio::sync::OwnedSemaphorePermit> {
        let mut permits = Vec::new();
        for _ in 0..QUERY_ACTIVE_LIMIT {
            permits.push(gateway.queries.clone().acquire_owned().await.unwrap());
        }
        permits
    }

    #[tokio::test]
    async fn public_reads_are_bounded_and_released() {
        use axum::{extract::Path, http::StatusCode};
        let gateway = test_gateway();
        let mut held = Vec::new();
        for _ in 0..PUBLIC_READ_LIMIT {
            held.push(gateway.public_reads.clone().acquire_owned().await.unwrap());
        }
        assert_eq!(
            init(State(gateway.clone())).await.unwrap_err().0,
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            session(State(gateway.clone()), Path((1, 0)))
                .await
                .unwrap_err()
                .0,
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            session_by_id(State(gateway.clone()), Path("aa".repeat(32)))
                .await
                .unwrap_err()
                .0,
            StatusCode::TOO_MANY_REQUESTS
        );
        drop(held);
        assert_eq!(
            init(State(gateway.clone())).await.unwrap_err().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(gateway.public_reads.available_permits(), PUBLIC_READ_LIMIT);
    }

    #[tokio::test]
    async fn queue_waits_for_a_slot_and_releases_it() {
        let gateway = test_gateway();
        let mut active = occupy_query_slots(&gateway).await;
        let waiting = tokio::spawn(query(State(gateway.clone()), empty_query()));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            gateway.query_waiters.available_permits(),
            QUERY_WAIT_LIMIT - 1
        );
        drop(active.pop());
        let error = waiting.await.unwrap().unwrap_err();
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert_eq!(gateway.query_waiters.available_permits(), QUERY_WAIT_LIMIT);
        drop(active);
    }

    #[tokio::test]
    async fn full_queue_and_expired_wait_return_retryable_429() {
        let gateway = test_gateway();
        let _active = occupy_query_slots(&gateway).await;
        let mut waiters = Vec::new();
        for _ in 0..QUERY_WAIT_LIMIT {
            waiters.push(tokio::spawn(query(State(gateway.clone()), empty_query())));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(gateway.query_waiters.available_permits(), 0);
        let response = query(State(gateway.clone()), empty_query())
            .await
            .unwrap_err()
            .into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
        for waiter in waiters {
            let response = waiter.await.unwrap().unwrap_err().into_response();
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        }
        assert_eq!(gateway.query_waiters.available_permits(), QUERY_WAIT_LIMIT);
    }

    #[tokio::test]
    async fn cancelled_waiter_releases_capacity() {
        let gateway = test_gateway();
        let _active = occupy_query_slots(&gateway).await;
        let waiting = tokio::spawn(query(State(gateway.clone()), empty_query()));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            gateway.query_waiters.available_permits(),
            QUERY_WAIT_LIMIT - 1
        );
        waiting.abort();
        let _ = waiting.await;
        assert_eq!(gateway.query_waiters.available_permits(), QUERY_WAIT_LIMIT);
    }

    #[tokio::test]
    async fn compatibility_evaluation_preserves_attempt_metrics_without_ambiguous_replay() {
        let gateway = test_gateway();
        let request = worker::Evaluate {
            binding: Some(vec![7]),
            generation: 1,
            shard_id: 0,
            epoch: "epoch".into(),
            session_id: "session".into(),
            coefficients: vec![1],
        };
        for explicit in [true, false] {
            let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let second = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let routes = vec![
                format!("http://{}", first.local_addr().unwrap()),
                format!("http://{}", second.local_addr().unwrap()),
            ];
            let second_calls = Arc::new(AtomicU64::new(0));
            let calls = second_calls.clone();
            let busy = Router::new().route(
                "/internal/evaluate",
                post(move || async move {
                    let mut response = StatusCode::TOO_MANY_REQUESTS.into_response();
                    if explicit {
                        response
                            .headers_mut()
                            .insert("x-enhance-evaluation", "not-accepted".parse().unwrap());
                    }
                    response
                }),
            );
            let ready = Router::new().route(
                "/internal/evaluate",
                post(move || {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::Relaxed);
                        Json(worker::Intermediate {
                            binding: Some(vec![7]),
                            generation: 1,
                            shard_id: 0,
                            epoch: "epoch".into(),
                            coefficients: vec![9],
                        })
                    }
                }),
            );
            let a = tokio::spawn(async move { axum::serve(first, busy).await.unwrap() });
            let b = tokio::spawn(async move { axum::serve(second, ready).await.unwrap() });
            gateway.query_stats.route_cursor.store(0, Ordering::Relaxed);
            let before_busy = gateway.query_stats.worker_busy.load(Ordering::Relaxed);
            let before_fallback = gateway.query_stats.fallback_success.load(Ordering::Relaxed);
            let before_rejected = gateway.query_stats.rejected.load(Ordering::Relaxed);
            let result = evaluate_query(&gateway, &request, &routes).await;
            assert_eq!(
                gateway.query_stats.worker_busy.load(Ordering::Relaxed),
                before_busy + 1
            );
            if explicit {
                assert_eq!(result.unwrap(), vec![9]);
                assert_eq!(
                    gateway.query_stats.fallback_success.load(Ordering::Relaxed),
                    before_fallback + 1
                );
                assert_eq!(second_calls.load(Ordering::Relaxed), 1);
            } else {
                assert_eq!(result.unwrap_err().0, StatusCode::TOO_MANY_REQUESTS);
                assert_eq!(
                    gateway.query_stats.rejected.load(Ordering::Relaxed),
                    before_rejected + 1
                );
                assert_eq!(second_calls.load(Ordering::Relaxed), 0);
            }
            a.abort();
            b.abort();
        }
    }

    #[tokio::test]
    async fn compatibility_response_holds_admission_and_rechecks_current_revocation() {
        let status = Arc::new(Revocations::default());
        let gateway = QueryGateway::new(
            status.clone(),
            PackingBudget::coordinator(),
            Vec::new(),
            reqwest::Client::new(),
        );
        let permit = gateway.queries.clone().acquire_owned().await.unwrap();
        let response =
            guarded_query_response(gateway.clone(), "session".into(), vec![7], permit).unwrap();
        assert_eq!(gateway.queries.available_permits(), QUERY_ACTIVE_LIMIT - 1);
        status.0.lock().unwrap().insert("session".into());
        assert!(axum::body::to_bytes(response.into_body(), 10)
            .await
            .is_err());
        assert_eq!(gateway.queries.available_permits(), QUERY_ACTIVE_LIMIT);
        let rejected = guarded_query_response(gateway, "session".into(), vec![7], ()).unwrap_err();
        assert_eq!(rejected.0, StatusCode::GONE);
    }
}
