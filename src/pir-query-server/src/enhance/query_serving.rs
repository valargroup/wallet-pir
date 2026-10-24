//! Shared worker-evaluation contract for remote and compatibility serving.
use super::evaluation::{Evaluate, Intermediate};
use crate::enhance::{peers::WorkerEndpoint, worker_sync::EvaluationFailure};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use enhance_shared::protocol::{Manifest, QueryBinding};

pub(crate) type Error = (StatusCode, String);
fn unavailable(message: impl ToString) -> Error {
    (StatusCode::SERVICE_UNAVAILABLE, message.to_string())
}

/// Checks the binding against the current manifest and returns the rows its
/// domain's queries select over. The session ID binds the domain's records,
/// so the client derived the same count from the manifest it accepted.
pub(crate) fn validate_binding(manifest: &Manifest, binding: QueryBinding) -> Result<usize, Error> {
    if binding.generation != manifest.generation
        || binding.recovery_epoch != manifest.recovery_epoch
    {
        return Err((StatusCode::CONFLICT, "stale_routing".into()));
    }
    if manifest
        .session_id(binding.shard_id)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?
        != binding.session_id
        || hex::encode(binding.anchor_hash) != manifest.anchor_block_hash
    {
        return Err((StatusCode::GONE, "session_unavailable".into()));
    }
    manifest
        .coverage
        .shards
        .iter()
        .find(|s| s.id == binding.shard_id)
        .ok_or("unknown domain".to_string())
        .and_then(|s| s.query_rows(manifest.geometry))
        .map(|rows| rows as usize)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))
}

/// Once evaluation is admitted, disconnecting the caller cannot release its resources.
pub(crate) async fn admitted<T: Send + 'static>(
    work: impl std::future::Future<Output = T> + Send + 'static,
) -> Result<T, Error> {
    tokio::spawn(work).await.map_err(unavailable)
}

pub(crate) struct Answer {
    pub coefficients: Vec<u64>,
    pub worker_time: Option<std::time::Duration>,
}

/// Retry only when the previous worker demonstrably did not accept evaluation.
/// The selected lease remains charged until the complete worker response is read.
/// Autonomous peers also bind acceptance to the process that promised readiness.
/// A worker in this process is called directly; only its typed `NotAccepted`
/// refusal counts as nonacceptance, and a failure or panic after admission is
/// as final as an HTTP worker that may have received the request.
pub(crate) async fn evaluate_exact<G>(
    http: &reqwest::Client,
    request: &Evaluate,
    route_count: usize,
    mut select: impl FnMut(Option<&str>) -> Result<(WorkerEndpoint, String, G), Error>,
) -> Result<Answer, Error> {
    let mut excluded = None;
    for attempt in 0..2 {
        let (worker, incarnation, _lease) = select(excluded.as_deref())?;
        let retry = attempt == 0 && route_count > 1;
        let url = match &worker {
            WorkerEndpoint::Http(url) => url,
            WorkerEndpoint::Local(local) => {
                match local.evaluate_local(&incarnation, request.clone()).await {
                    Ok((result, worker_time)) => {
                        return answer(request, result, Some(worker_time));
                    }
                    Err(EvaluationFailure::NotAccepted((status, _))) => {
                        if refusal_allows_replay(status) && retry {
                            excluded = Some(worker.key().to_string());
                            continue;
                        }
                        return Err(refusal(status, true));
                    }
                    Err(EvaluationFailure::Failed((_, error))) => {
                        tracing::warn!(%error, "local worker evaluation failed; no replay");
                        return Err(unavailable("worker evaluation failed; no replay"));
                    }
                }
            }
        };
        let builder = http
            .post(format!("{url}/internal/evaluate"))
            .timeout(std::time::Duration::from_secs(30))
            .json(request)
            .header("x-enhance-worker-incarnation", incarnation);
        let response = builder.send().await;
        match response {
            Err(e) if e.is_connect() && retry => {
                excluded = Some(url.clone());
            }
            Err(e) => {
                tracing::warn!(error = %e, "worker evaluation transport failed; no replay");
                return Err(unavailable("evaluation acceptance unknown"));
            }
            Ok(response) => {
                let status = response.status();
                let not_accepted = response
                    .headers()
                    .get("x-enhance-evaluation")
                    .is_some_and(|v| v == "not-accepted");
                if not_accepted && refusal_allows_replay(status) && retry {
                    excluded = Some(url.clone());
                    continue;
                }
                if !status.is_success() {
                    return Err(refusal(status, not_accepted));
                }
                let worker_time = response
                    .headers()
                    .get("x-enhance-matvec-microseconds")
                    .and_then(|h| h.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(std::time::Duration::from_micros);
                let bytes = bounded(response, 1024 * 1024).await.map_err(unavailable)?;
                let result: Intermediate = serde_json::from_slice(&bytes).map_err(unavailable)?;
                return answer(request, result, worker_time);
            }
        }
    }
    Err(unavailable("no admitted evaluation"))
}

/// Refusals another replica may still accept.
fn refusal_allows_replay(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::GONE | StatusCode::SERVICE_UNAVAILABLE
    )
}

fn refusal(status: StatusCode, not_accepted: bool) -> Error {
    if not_accepted && status == StatusCode::TOO_MANY_REQUESTS {
        (StatusCode::TOO_MANY_REQUESTS, "workers not admitted".into())
    } else {
        unavailable("worker evaluation failed; no replay")
    }
}

fn answer(
    request: &Evaluate,
    result: Intermediate,
    worker_time: Option<std::time::Duration>,
) -> Result<Answer, Error> {
    if result.binding != request.binding
        || result.generation != request.generation
        || result.shard_id != request.shard_id
        || result.epoch != request.epoch
    {
        return Err(unavailable("worker binding mismatch"));
    }
    Ok(Answer {
        coefficients: result.coefficients,
        worker_time,
    })
}

pub async fn bounded(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>, String> {
    if response.content_length().is_some_and(|n| n > limit as u64) {
        return Err("oversized response".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
        if chunk.len() > limit - bytes.len() {
            return Err("oversized response".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

const PUBLIC_CODES: [&str; 5] = [
    "stale_routing",
    "noncanonical_session",
    "session_unavailable",
    "overloaded",
    "temporarily_unavailable",
];

pub(crate) fn public_error(error: Error) -> Response {
    let code = match error.0 {
        StatusCode::CONFLICT => "stale_routing",
        StatusCode::GONE if error.1 == "noncanonical_session" => "noncanonical_session",
        StatusCode::GONE => "session_unavailable",
        StatusCode::TOO_MANY_REQUESTS => "overloaded",
        StatusCode::SERVICE_UNAVAILABLE | StatusCode::BAD_GATEWAY => "temporarily_unavailable",
        _ => "invalid_request",
    };
    // Overload and upstream failures never echo transport text, URLs or
    // private addresses; only the fixed public vocabulary is returned.
    let message = if (error.0.is_server_error() || error.0 == StatusCode::TOO_MANY_REQUESTS)
        && !PUBLIC_CODES.contains(&error.1.as_str())
    {
        if error.0 == StatusCode::TOO_MANY_REQUESTS {
            "too many requests".to_string()
        } else {
            "service temporarily unavailable".to_string()
        }
    } else {
        error.1
    };
    let mut response = (
        error.0,
        Json(serde_json::json!({"code":code,"message":message})),
    )
        .into_response();
    if error.0 == StatusCode::TOO_MANY_REQUESTS {
        response
            .headers_mut()
            .insert("retry-after", "1".parse().unwrap());
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Json, Router};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    fn request() -> Evaluate {
        Evaluate {
            binding: Some(vec![7]),
            generation: 3,
            shard_id: 1,
            epoch: "epoch".into(),
            session_id: "session".into(),
            coefficients: vec![1],
        }
    }
    async fn server(
        status: StatusCode,
        rejected: bool,
        binding: Vec<u8>,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        server_timing(status, rejected, binding, None).await
    }
    async fn server_timing(
        status: StatusCode,
        rejected: bool,
        binding: Vec<u8>,
        timing: Option<&'static str>,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let app = Router::new().route(
            "/internal/evaluate",
            post(move || {
                let calls = calls.clone();
                let binding = binding.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let mut headers = axum::http::HeaderMap::new();
                    if rejected {
                        headers.insert("x-enhance-evaluation", "not-accepted".parse().unwrap());
                    }
                    if let Some(timing) = timing {
                        headers.insert("x-enhance-matvec-microseconds", timing.parse().unwrap());
                    }
                    (
                        status,
                        headers,
                        Json(Intermediate {
                            binding: Some(binding),
                            generation: 3,
                            shard_id: 1,
                            epoch: "epoch".into(),
                            coefficients: vec![9],
                        }),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, count, task)
    }
    async fn pair(
        first_status: StatusCode,
        rejected: bool,
        binding: Vec<u8>,
    ) -> (Result<Answer, Error>, usize, usize) {
        let (a, ac, at) = server(first_status, rejected, binding).await;
        let (b, bc, bt) = server(StatusCode::OK, false, vec![7]).await;
        let result = evaluate_exact(&reqwest::Client::new(), &request(), 2, |excluded| {
            Ok((
                WorkerEndpoint::Http(if excluded.is_none() {
                    a.clone()
                } else {
                    b.clone()
                }),
                "01".repeat(16),
                (),
            ))
        })
        .await;
        let counts = (ac.load(Ordering::SeqCst), bc.load(Ordering::SeqCst));
        at.abort();
        bt.abort();
        (result, counts.0, counts.1)
    }
    #[tokio::test]
    async fn public_errors_never_echo_upstream_transport_text() {
        for status in [
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::BAD_GATEWAY,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            let response = public_error((
                status,
                "error sending request for url (http://10.1.2.3:8092/v1/enhance/query)".into(),
            ));
            assert_eq!(response.status(), status);
            let body = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let text = String::from_utf8(body.to_vec()).unwrap();
            assert!(!text.contains("http://10."), "{text}");
            assert!(!text.contains("reqwest"), "{text}");
        }
        let response = public_error((StatusCode::SERVICE_UNAVAILABLE, "overloaded".into()));
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        assert!(String::from_utf8(body.to_vec())
            .unwrap()
            .contains("\"message\":\"overloaded\""));
        let response = public_error((StatusCode::BAD_REQUEST, "wrong shard".into()));
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        assert!(String::from_utf8(body.to_vec())
            .unwrap()
            .contains("wrong shard"));
    }
    #[tokio::test]
    async fn only_explicit_nonacceptance_allows_replay() {
        for status in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::GONE,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let (result, first, second) = pair(status, true, vec![7]).await;
            let answer = result.unwrap();
            assert_eq!(answer.coefficients, vec![9]);
            assert_eq!((first, second), (1, 1));
            let (result, first, second) = pair(status, false, vec![7]).await;
            assert!(result.is_err());
            assert_eq!((first, second), (1, 0));
        }
    }
    #[tokio::test]
    async fn binding_mismatch_never_replays_even_with_matching_metadata() {
        let (result, first, second) = pair(StatusCode::OK, false, vec![8]).await;
        assert_eq!(result.err().unwrap().1, "worker binding mismatch");
        assert_eq!((first, second), (1, 0));
    }
    #[tokio::test]
    async fn cancelled_caller_keeps_admitted_capacity_until_work_completes() {
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = slots.clone().acquire_owned().await.unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(admitted(async move {
            started_tx.send(()).unwrap();
            finish_rx.await.unwrap();
            drop(permit);
            done_tx.send(()).unwrap();
        }));
        started_rx.await.unwrap();
        caller.abort();
        let _ = caller.await;
        assert_eq!(slots.available_permits(), 0);
        finish_tx.send(()).unwrap();
        done_rx.await.unwrap();
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn malformed_or_oversized_success_never_replays() {
        for bytes in [b"not json".to_vec(), vec![b'x'; 1024 * 1024 + 1]] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let first = format!("http://{}", listener.local_addr().unwrap());
            let app = Router::new().route(
                "/internal/evaluate",
                post(move || {
                    let bytes = bytes.clone();
                    async move { bytes }
                }),
            );
            let first_task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let (second, calls, second_task) = server(StatusCode::OK, false, vec![7]).await;
            let result = evaluate_exact(&reqwest::Client::new(), &request(), 2, |excluded| {
                Ok((
                    WorkerEndpoint::Http(if excluded.is_none() {
                        first.clone()
                    } else {
                        second.clone()
                    }),
                    "01".repeat(16),
                    (),
                ))
            })
            .await;
            assert!(result.is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            first_task.abort();
            second_task.abort();
        }
    }

    #[tokio::test]
    async fn disconnect_after_receiving_request_never_replays() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first = format!("http://{}", listener.local_addr().unwrap());
        let first_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 4096];
            assert!(stream.read(&mut bytes).await.unwrap() > 0);
            // The request may have been accepted: no response is permission to replay.
        });
        let (second, calls, second_task) = server(StatusCode::OK, false, vec![7]).await;
        let result = evaluate_exact(&reqwest::Client::new(), &request(), 2, |excluded| {
            Ok((
                WorkerEndpoint::Http(if excluded.is_none() {
                    first.clone()
                } else {
                    second.clone()
                }),
                "01".repeat(16),
                (),
            ))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        first_task.await.unwrap();
        second_task.abort();
    }

    #[tokio::test]
    async fn worker_timing_is_optional_and_comes_from_the_successful_attempt() {
        for (header, expected) in [
            (Some("1234"), Some(std::time::Duration::from_micros(1234))),
            (None, None),
            (Some("invalid"), None),
        ] {
            let (first, _, first_task) =
                server_timing(StatusCode::TOO_MANY_REQUESTS, true, vec![7], Some("9999")).await;
            let (second, _, second_task) =
                server_timing(StatusCode::OK, false, vec![7], header).await;
            let answer = evaluate_exact(&reqwest::Client::new(), &request(), 2, |excluded| {
                Ok((
                    WorkerEndpoint::Http(if excluded.is_none() {
                        first.clone()
                    } else {
                        second.clone()
                    }),
                    "01".repeat(16),
                    (),
                ))
            })
            .await
            .unwrap();
            assert_eq!(answer.worker_time, expected);
            first_task.abort();
            second_task.abort();
        }
    }

    #[tokio::test]
    async fn connection_refusal_allows_one_retry() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let (available, calls, task) = server(StatusCode::OK, false, vec![7]).await;
        let mut selections = Vec::new();
        evaluate_exact(&reqwest::Client::new(), &request(), 2, |excluded| {
            selections.push(excluded.map(str::to_owned));
            Ok((
                WorkerEndpoint::Http(if excluded.is_none() {
                    unavailable.clone()
                } else {
                    available.clone()
                }),
                "01".repeat(16),
                (),
            ))
        })
        .await
        .unwrap();
        assert_eq!(selections, [None, Some(unavailable.clone())]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        task.abort();
    }
}
