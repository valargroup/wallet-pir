//! Request handling the history and display workers share.
//!
//! Both serve the same kind of table through the same runtime cache, admission
//! and work-memory bounds; they differ in how a request names its revision and
//! table. Each worker's snapshot resolves that address into a
//! [`PreparedQuery`], then hands it to [`answer_query`]: the declared-length
//! check, admission, the body read, the runtime handles, the memory
//! reservation and evaluation. Refusals are one
//! [`RequestError`], so a wallet sees the same status codes, retry delays and
//! body shapes from either.

use crate::transparent::admission::{Admission, AdmissionError};
use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use transparent_shared::runtime::memory::WorkMemory;
use transparent_shared::runtime::metrics::Metrics;
use transparent_shared::runtime::CacheError;
use transparent_shared::serving::{PreparedQuery, ServingError};

/// Why a request could not be answered.
pub enum RequestError {
    Bad(String),
    /// The revision named is no longer served. Retryable after a map refresh;
    /// `retry` says which map.
    Stale {
        message: String,
        retry: &'static str,
    },
    /// No cache capacity could be freed. Retryable as-is.
    Overloaded,
    /// A query arrived without saying how long it is.
    LengthRequired,
    /// The worker is at a bound it will not exceed. Retryable as-is; the body
    /// names which bound.
    Busy(&'static str),
    /// The body did not arrive in time. The client is the slow party.
    UploadTimeout,
    /// The request reached a worker that does not serve what it names: an
    /// unassigned shard, the wrong tier or the wrong role. A routing fault;
    /// not retryable here and not a reason to refresh the map.
    Misdirected(String),
}

impl From<AdmissionError> for RequestError {
    fn from(error: AdmissionError) -> Self {
        match error {
            AdmissionError::QueueFull => RequestError::Busy("every waiting place is taken"),
            AdmissionError::BodyBudget => RequestError::Busy("the body budget is full"),
            AdmissionError::DeadlineExceeded => {
                RequestError::Busy("the request waited its whole deadline without a slot")
            }
            AdmissionError::UploadTimeout => RequestError::UploadTimeout,
            AdmissionError::ShuttingDown => RequestError::Bad("server is shutting down".into()),
        }
    }
}

impl From<CacheError> for RequestError {
    fn from(error: CacheError) -> Self {
        match error {
            CacheError::Overloaded => RequestError::Overloaded,
            CacheError::Failed(message) => RequestError::Bad(message),
        }
    }
}

impl From<ServingError> for RequestError {
    fn from(error: ServingError) -> Self {
        match error {
            ServingError::Invalid(message) => RequestError::Bad(message),
            ServingError::Stale { message, retry } => RequestError::Stale { message, retry },
            ServingError::WrongHoldings(message) => RequestError::Misdirected(message),
            ServingError::Capacity => RequestError::Overloaded,
        }
    }
}

impl RequestError {
    pub fn into_response(self, map_digest: &str) -> Response {
        // The same shape for every retryable refusal: 503 with a delay, which
        // is what the wallet keys its retry on. The body says which bound.
        let retry = |reason: &str| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [("content-type", "application/json"), ("retry-after", "1")],
                serde_json::json!({"error": reason, "retry": "retry shortly"}).to_string(),
            )
                .into_response()
        };
        match self {
            RequestError::Bad(message) => json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({"error": message}),
            ),
            RequestError::LengthRequired => json(
                StatusCode::LENGTH_REQUIRED,
                serde_json::json!({"error": "a query must declare its length"}),
            ),
            RequestError::UploadTimeout => json(
                StatusCode::REQUEST_TIMEOUT,
                serde_json::json!({"error": "the query body did not arrive in time"}),
            ),
            // 421: the request reached a server that is not configured to
            // answer it. No retry delay, so a wallet does not spin on it, and
            // the map digest so an operator can see which set the worker holds.
            RequestError::Misdirected(message) => json(
                StatusCode::MISDIRECTED_REQUEST,
                serde_json::json!({"error": message, "map_sha256": map_digest}),
            ),
            RequestError::Busy(reason) => retry(reason),
            RequestError::Overloaded => retry("no cache capacity is free"),
            // 409 rather than 404: the shard exists and the request is well
            // formed, but it is addressed to a publication that has been
            // replaced. The current map digest is included so the client can
            // tell whether refreshing will actually help.
            RequestError::Stale { message, retry } => json(
                StatusCode::CONFLICT,
                serde_json::json!({
                    "error": message,
                    "retry": retry,
                    "map_sha256": map_digest,
                }),
            ),
        }
    }
}

pub fn json(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        [("content-type", "application/json")],
        body.to_string(),
    )
        .into_response()
}

/// Awaits `work` within `remaining`, counting a timeout as an exceeded
/// deadline. The refusal is the admission's, so it is retryable.
pub async fn within<T>(
    remaining: Duration,
    metrics: &Metrics,
    work: impl Future<Output = Result<T, RequestError>>,
) -> Result<T, RequestError> {
    match tokio::time::timeout(remaining, work).await {
        Ok(result) => result,
        Err(_) => {
            Metrics::incr(&metrics.deadline_exceeded);
            Err(RequestError::from(AdmissionError::DeadlineExceeded))
        }
    }
}

/// The parts of a worker a request passes through, borrowed from whichever
/// worker received it.
pub struct Worker<'a> {
    pub admission: &'a Admission,
    pub work_memory: &'a Arc<WorkMemory>,
    pub metrics: &'a Arc<Metrics>,
    /// The longest body any table this worker serves can legitimately send.
    pub max_query_bytes: usize,
    pub map_digest: &'a str,
}

impl Worker<'_> {
    /// Sends a query refusal decided before the body was read, after reading
    /// and discarding the body.
    ///
    /// A proxy streams the upload while the worker answers. Answering and
    /// closing with the upload unread tears the proxy's upload down mid-body
    /// ("broken pipe"): the wallet then gets the proxy's 502 rather than this
    /// refusal and its retry delay, and the proxy counts an error against the
    /// worker's health. On the 2026-09-27 bench fleet such errors from
    /// saturated workers ejected the whole pool.
    ///
    /// Discarding costs bandwidth, not memory: frames are dropped as they
    /// arrive, at most `max_query_bytes` are accepted, and the wait is
    /// bounded. A body that is longer, slower or broken is abandoned, and the
    /// connection closes as it did before.
    pub async fn refuse_unread(&self, request: Request, refused: Response) -> Response {
        use http_body::Body as _;
        let limit = self.max_query_bytes;
        let wait = self
            .admission
            .config()
            .upload_deadline
            .min(std::time::Duration::from_secs(5));
        let mut body = request.into_body();
        let drained = tokio::time::timeout(wait, async {
            let mut seen = 0usize;
            loop {
                match std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await
                {
                    None => return true,
                    Some(Ok(frame)) => {
                        seen += frame.data_ref().map_or(0, |data| data.len());
                        if seen > limit {
                            return false;
                        }
                    }
                    Some(Err(_)) => return false,
                }
            }
        })
        .await;
        if !matches!(drained, Ok(true)) {
            Metrics::incr(&self.metrics.refusals_unread);
        }
        refused
    }
}

/// Answers a query the snapshot resolved: everything from the declared
/// length to the evaluated answer.
///
/// Every segment's runtime is taken once a slot is held, each bounded by what
/// is left of the request's deadline.
pub async fn answer_query(worker: Worker<'_>, request: Request, query: PreparedQuery) -> Response {
    let metrics = worker.metrics;
    let map_digest = worker.map_digest;
    let table = query.table().to_string();
    let revision = query.revision().to_string();

    // The exact lengths this table's query may have — each width its profile
    // accepts, over every row or, for a growing tail's pages, over only the
    // rows its manifest says can hold data — are known before a single body
    // byte is read, so a body of any other length is refused here: before it
    // is buffered, before it queues, before any runtime is built for it. The
    // global body limit is the ceiling for the widest geometry; this is the
    // check for the one actually addressed.
    let declared = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .or_else(|| {
            use http_body::Body as _;
            request.body().size_hint().exact()
        });
    let Some(declared) = declared else {
        // Not discarded: with no declared length there is no bound to read to.
        Metrics::incr(&metrics.query_length_rejections);
        return RequestError::LengthRequired.into_response(map_digest);
    };
    let Some(expected) = usize::try_from(declared)
        .ok()
        .filter(|&len| query.accepts_len(len))
    else {
        Metrics::incr(&metrics.query_length_rejections);
        let refused = RequestError::Bad(format!(
            "a {table} query for geometry {} must be exactly {} bytes, not {declared}",
            query.geometry(),
            query.lengths(),
        ))
        .into_response(map_digest);
        return worker.refuse_unread(request, refused).await;
    };

    // Counted, and its bytes budgeted, before the body is read. A worker that
    // buffered first and counted afterwards would be bounded only by how many
    // clients chose to send at once.
    let pending = match worker.admission.try_enter(Some(expected)) {
        Ok(pending) => pending,
        Err(error) => {
            Metrics::incr(&metrics.query_errors);
            // The capacity refusal is the one a saturated worker sends most,
            // and the one whose retry delay matters most to reach the wallet.
            let refused = RequestError::from(error).into_response(map_digest);
            return worker.refuse_unread(request, refused).await;
        }
    };
    let upload_deadline = worker.admission.config().upload_deadline;
    let body =
        match pir_control::admission::read_body(request.into_body(), expected, upload_deadline)
            .await
        {
            Ok(body) => body,
            Err(pir_control::admission::BodyError::Read(error)) => {
                Metrics::incr(&metrics.query_errors);
                return RequestError::Bad(format!("reading the query body: {error}"))
                    .into_response(map_digest);
            }
            Err(pir_control::admission::BodyError::Timeout) => {
                Metrics::incr(&metrics.upload_timeouts);
                Metrics::incr(&metrics.query_errors);
                return RequestError::UploadTimeout.into_response(map_digest);
            }
        };
    // A declared length is a claim; the bytes that arrived are the fact.
    if body.len() != expected {
        Metrics::incr(&metrics.query_length_rejections);
        Metrics::incr(&metrics.query_errors);
        return RequestError::Bad(format!(
            "the query body is {} bytes where {expected} were declared",
            body.len()
        ))
        .into_response(map_digest);
    }

    // Admission before work: bounded evaluation is what keeps a burst from
    // pinning every runtime in the cache against eviction at once. The wait is
    // bounded by the request's deadline, and a request that outwaits it is
    // refused retryably rather than kept.
    let query_stage = std::time::Instant::now();
    let admitted = match worker.admission.wait_slot(pending).await {
        Ok(admitted) => admitted,
        Err(error) => {
            Metrics::incr(&metrics.query_errors);
            return RequestError::from(error).into_response(map_digest);
        }
    };

    tracing::debug!(revision = %revision, table = %table, seconds = query_stage.elapsed().as_secs_f64(), stage = "query_admission", "query stage");
    let query_stage = std::time::Instant::now();

    // Every segment's runtime is acquired *before* any of them is evaluated,
    // and all the handles are held until the last one is done. Acquiring them
    // one at a time would let a tight budget evict segment 0 while segment 1
    // builds, and the next query would rebuild what this one just discarded —
    // a multi-segment table would thrash rather than be answered. Holding them
    // together also means a table whose segments do not fit the budget is
    // refused once, retryably, instead of making progress it cannot keep.
    let mut handles = Vec::with_capacity(query.segments());
    for segment in 0..query.segments() {
        let runtime = async { query.runtime(segment).await.map_err(RequestError::from) };
        match within(admitted.remaining(), metrics, runtime).await {
            Ok(handle) => handles.push(handle),
            Err(error) => {
                Metrics::incr(&metrics.query_errors);
                return error.into_response(map_digest);
            }
        }
    }

    tracing::debug!(revision = %revision, table = %table, seconds = query_stage.elapsed().as_secs_f64(), stage = "query_runtime", "query stage");

    // Evaluated off the async runtime, holding the slot and every handle for
    // exactly as long as the work runs. The admission moves into the closure:
    // if the client goes away meanwhile the evaluation still finishes and is
    // discarded, but the slot is released when it does, not when the dropped
    // future would have been polled.
    let memory_started = std::time::Instant::now();
    let memory = match worker
        .work_memory
        .reserve_query(query.work_bytes(), admitted.remaining())
        .await
    {
        Some(memory) => memory,
        None => {
            tracing::debug!(revision = %revision, table = %table, seconds = memory_started.elapsed().as_secs_f64(), admitted = false, stage = "query_memory_admission", "query stage");
            if admitted.remaining().is_zero() {
                Metrics::incr(&metrics.deadline_exceeded);
                Metrics::incr(&metrics.query_errors);
                return RequestError::from(AdmissionError::DeadlineExceeded)
                    .into_response(map_digest);
            }
            Metrics::incr(&metrics.overloads);
            return RequestError::Overloaded.into_response(map_digest);
        }
    };
    tracing::debug!(revision = %revision, table = %table, seconds = memory_started.elapsed().as_secs_f64(), admitted = true, stage = "query_memory_admission", "query stage");
    let evaluation_metrics = metrics.clone();
    let evaluator = query.evaluator();
    drop(query);
    let query_stage = std::time::Instant::now();
    let evaluated = pir_control::admission::spawn_admitted((handles, memory), move |(handles, _)| {
        tracing::debug!(revision = %revision, table = %table, seconds = query_stage.elapsed().as_secs_f64(), stage = "query_dispatch", "query stage");
        let query_stage = std::time::Instant::now();
        let _timer = evaluation_metrics.evaluation_seconds.timer();
        let answer = evaluator.evaluate(handles, &body)?;
        tracing::debug!(revision = %revision, table = %table, seconds = query_stage.elapsed().as_secs_f64(), stage = "query_evaluate", "query stage");
        admitted.complete();
        Ok::<_, String>(answer)
    })
    .await;
    match evaluated {
        Ok((Ok(answer), (handles, memory))) => {
            Metrics::incr(&metrics.queries);
            (
                StatusCode::OK,
                [("content-type", "application/octet-stream")],
                pir_control::http::retain(axum::body::Body::from(answer), (handles, memory)),
            )
                .into_response()
        }
        Ok((Err(error), _)) => {
            Metrics::incr(&metrics.query_errors);
            RequestError::Bad(error).into_response(map_digest)
        }
        Err(error) => {
            Metrics::incr(&metrics.query_errors);
            RequestError::Bad(format!("evaluation task failed: {error}")).into_response(map_digest)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn sent(error: RequestError) -> (u16, Option<String>, String) {
        let response = error.into_response("cafe");
        let retry = response
            .headers()
            .get("retry-after")
            .map(|v| v.to_str().unwrap().to_string());
        assert_eq!(response.headers()["content-type"], "application/json");
        let status = response.status().as_u16();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, retry, String::from_utf8(body.to_vec()).unwrap())
    }

    /// The refusals the integration transcripts cannot reach without a
    /// saturated worker, pinned byte for byte. Wallets key their retries on
    /// the status and the delay.
    #[tokio::test]
    async fn retryable_refusals_are_pinned() {
        let one = Some("1".to_string());
        assert_eq!(
            sent(AdmissionError::QueueFull.into()).await,
            (
                503,
                one.clone(),
                r#"{"error":"every waiting place is taken","retry":"retry shortly"}"#.into()
            )
        );
        assert_eq!(
            sent(AdmissionError::BodyBudget.into()).await,
            (
                503,
                one.clone(),
                r#"{"error":"the body budget is full","retry":"retry shortly"}"#.into()
            )
        );
        assert_eq!(
            sent(AdmissionError::DeadlineExceeded.into()).await,
            (
                503,
                one.clone(),
                r#"{"error":"the request waited its whole deadline without a slot","retry":"retry shortly"}"#.into()
            )
        );
        assert_eq!(
            sent(CacheError::Overloaded.into()).await,
            (
                503,
                one,
                r#"{"error":"no cache capacity is free","retry":"retry shortly"}"#.into()
            )
        );
        assert_eq!(
            sent(AdmissionError::UploadTimeout.into()).await,
            (
                408,
                None,
                r#"{"error":"the query body did not arrive in time"}"#.into()
            )
        );
        assert_eq!(
            sent(AdmissionError::ShuttingDown.into()).await,
            (400, None, r#"{"error":"server is shutting down"}"#.into())
        );
    }
}
