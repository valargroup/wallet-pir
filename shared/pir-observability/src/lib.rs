//! Bounded aggregate HTTP observations. Paths and client data never become labels.
use axum::{
    body::{Body, Bytes},
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use http_body::{Body as HttpBody, Frame, SizeHint};
use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

pub const BOUNDS: [f64; 16] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1., 2., 5., 10., 30., 60., 120., 600.,
];

#[derive(Default)]
pub struct Distribution(Mutex<([u64; 17], f64)>);
impl Distribution {
    pub fn observe(&self, seconds: f64) {
        if !seconds.is_finite() || seconds < 0. {
            return;
        }
        let mut value = self.0.lock().unwrap();
        value.0[BOUNDS.iter().position(|b| seconds <= *b).unwrap_or(16)] += 1;
        value.1 += seconds;
    }
    pub fn render(&self, name: &str) -> String {
        let value = self.0.lock().unwrap();
        let mut total = 0;
        let mut out = String::new();
        for (i, count) in value.0.iter().enumerate() {
            total += count;
            let bound = BOUNDS
                .get(i)
                .map(|n| n.to_string())
                .unwrap_or_else(|| "+Inf".into());
            out.push_str(&format!("{name}_bucket{{le=\"{bound}\"}} {total}\n"));
        }
        out.push_str(&format!("{name}_count {total}\n{name}_sum {}\n", value.1));
        out
    }
}

#[derive(Default, Debug)]
struct Operation {
    arrivals: u64,
    inflight: u64,
    responses: [u64; 5],
    cancelled: u64,
    upload: u64,
    download: u64,
    body_errors: u64,
    incomplete_responses: u64,
    durations: [u64; 17],
    duration_sum: f64,
}

#[derive(Clone, Debug)]
pub struct HttpMetrics {
    inner: Arc<Mutex<BTreeMap<&'static str, Operation>>>,
    started: f64,
}
impl Default for HttpMetrics {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            started: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        }
    }
}

/// Only a fixed endpoint category survives classification. No dynamic path segment is retained.
pub fn endpoint(path: &str) -> Option<&'static str> {
    if path == "/v1/shards/init" {
        Some("init")
    } else if path == "/v1/shards" || path == "/v1/filters/shards" {
        Some("map")
    } else if path.starts_with("/v1/filters/") {
        Some("filter")
    } else if path.starts_with("/v1/shards/") && path.ends_with("/manifest") {
        Some("manifest")
    } else if path.starts_with("/v1/shards/") && path.contains("/setup/") {
        Some("setup")
    } else if let Some(route) = path.strip_prefix("/v1/receiver/") {
        receiver_endpoint(route)
    } else if path.starts_with("/v1/shards/") && path.contains("/query/") {
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

/// The receiver directory's route categories; session IDs never become labels.
fn receiver_endpoint(route: &str) -> Option<&'static str> {
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

impl HttpMetrics {
    pub fn initialize(&self, endpoints: &[&'static str]) {
        let mut ops = self.inner.lock().unwrap();
        for endpoint in endpoints {
            ops.entry(endpoint).or_default();
        }
    }
    pub fn render(&self) -> String {
        let ops = self.inner.lock().unwrap();
        let mut out = format!(
            "pir_http_observation_version 1\npir_http_process_start_time_seconds {}\n",
            self.started
        );
        for (endpoint, op) in ops.iter() {
            for (name, value) in [
                ("arrivals_total", op.arrivals),
                ("inflight", op.inflight),
                ("cancelled_total", op.cancelled),
                ("request_body_bytes_total", op.upload),
                ("response_body_bytes_total", op.download),
                ("body_errors_total", op.body_errors),
                ("incomplete_responses_total", op.incomplete_responses),
            ] {
                out.push_str(&format!(
                    "pir_http_{name}{{endpoint=\"{endpoint}\"}} {value}\n"
                ));
            }
            for (i, value) in op.responses.iter().enumerate() {
                out.push_str(&format!(
                    "pir_http_responses_total{{endpoint=\"{endpoint}\",code=\"{}xx\"}} {value}\n",
                    i + 1
                ));
            }
            let mut cumulative = 0;
            for (i, count) in op.durations.iter().enumerate() {
                cumulative += count;
                let bound = BOUNDS
                    .get(i)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "+Inf".into());
                out.push_str(&format!("pir_http_duration_seconds_bucket{{endpoint=\"{endpoint}\",le=\"{bound}\"}} {cumulative}\n"));
            }
            out.push_str(&format!("pir_http_duration_seconds_count{{endpoint=\"{endpoint}\"}} {cumulative}\npir_http_duration_seconds_sum{{endpoint=\"{endpoint}\"}} {}\n",op.duration_sum));
        }
        out
    }
}

struct RequestGuard {
    metrics: HttpMetrics,
    endpoint: &'static str,
    began: Instant,
    code: Option<u16>,
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        let elapsed = self.began.elapsed().as_secs_f64();
        let mut ops = self.metrics.inner.lock().unwrap();
        let op = ops.entry(self.endpoint).or_default();
        op.inflight = op.inflight.saturating_sub(1);
        if let Some(code) = self.code {
            op.responses[(usize::from(code / 100).clamp(1, 5)) - 1] += 1;
        } else {
            op.cancelled += 1;
        }
        op.durations[BOUNDS.iter().position(|b| elapsed <= *b).unwrap_or(16)] += 1;
        op.duration_sum += elapsed;
    }
}
struct CountedBody {
    body: Body,
    metrics: HttpMetrics,
    endpoint: &'static str,
    response: bool,
    ended: bool,
}
impl Drop for CountedBody {
    fn drop(&mut self) {
        if self.response && !self.ended && !self.body.is_end_stream() {
            self.metrics
                .inner
                .lock()
                .unwrap()
                .entry(self.endpoint)
                .or_default()
                .incomplete_responses += 1;
        }
    }
}
impl HttpBody for CountedBody {
    type Data = Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let result = Pin::new(&mut self.body).poll_frame(cx);
        match &result {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(bytes) = frame.data_ref() {
                    let mut ops = self.metrics.inner.lock().unwrap();
                    let op = ops.entry(self.endpoint).or_default();
                    if self.response {
                        op.download += bytes.len() as u64;
                    } else {
                        op.upload += bytes.len() as u64;
                    }
                }
            }
            Poll::Ready(Some(Err(_))) => {
                self.metrics
                    .inner
                    .lock()
                    .unwrap()
                    .entry(self.endpoint)
                    .or_default()
                    .body_errors += 1;
            }
            Poll::Ready(None) => self.ended = true,
            Poll::Pending => {}
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

/// Timer covers body extraction, admission, errors, and cancellation until response construction.
/// Response bytes are counted only as the server polls them; this is not client receipt or decoding.
pub async fn observe(
    State(metrics): State<HttpMetrics>,
    mut request: Request,
    next: Next,
) -> Response {
    #[derive(Clone)]
    struct Observed;
    if request.extensions().get::<Observed>().is_some() {
        return next.run(request).await;
    }
    let Some(endpoint) = endpoint(request.uri().path()) else {
        return next.run(request).await;
    };
    request.extensions_mut().insert(Observed);
    {
        let mut ops = metrics.inner.lock().unwrap();
        let op = ops.entry(endpoint).or_default();
        op.arrivals += 1;
        op.inflight += 1;
    }
    let mut guard = RequestGuard {
        metrics: metrics.clone(),
        endpoint,
        began: Instant::now(),
        code: None,
    };
    let (parts, body) = request.into_parts();
    let body = Body::new(CountedBody {
        body,
        metrics: metrics.clone(),
        endpoint,
        response: false,
        ended: false,
    });
    let response = next.run(Request::from_parts(parts, body)).await;
    guard.code = Some(response.status().as_u16());
    drop(guard);
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(CountedBody {
            body,
            metrics,
            endpoint,
            response: true,
            ended: false,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{http::StatusCode, routing::post, Router};
    use tower::ServiceExt;
    #[tokio::test]
    async fn outcomes_include_early_rejections_and_actual_bytes() {
        let metrics = HttpMetrics::default();
        let app = Router::new()
            .route(
                "/v1/shards/init",
                post(|| async { (StatusCode::SERVICE_UNAVAILABLE, "busy") }),
            )
            .layer(axum::middleware::from_fn_with_state(
                metrics.clone(),
                observe,
            ));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/shards/init")
                    .body(Body::from("unread"))
                    .unwrap(),
            )
            .await
            .unwrap();
        axum::body::to_bytes(response.into_body(), 100)
            .await
            .unwrap();
        let text = metrics.render();
        assert!(text.contains("pir_http_responses_total{endpoint=\"init\",code=\"5xx\"} 1"));
        assert!(text.contains("pir_http_request_body_bytes_total{endpoint=\"init\"} 0"));
        assert!(text.contains("pir_http_response_body_bytes_total{endpoint=\"init\"} 4"));
    }
    #[tokio::test]
    async fn nested_layers_count_once_and_unconsumed_response_is_incomplete() {
        let metrics = HttpMetrics::default();
        let app = Router::new()
            .route("/v1/shards/init", post(|| async { "payload" }))
            .layer(axum::middleware::from_fn_with_state(
                metrics.clone(),
                observe,
            ))
            .layer(axum::middleware::from_fn_with_state(
                metrics.clone(),
                observe,
            ));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/shards/init")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        drop(response);
        let text = metrics.render();
        assert!(text.contains("pir_http_arrivals_total{endpoint=\"init\"} 1"));
        assert!(text.contains("pir_http_incomplete_responses_total{endpoint=\"init\"} 1"));
        assert!(text.contains("pir_http_response_body_bytes_total{endpoint=\"init\"} 0"));
    }
    #[tokio::test]
    async fn cancelled_future_releases_inflight() {
        let metrics = HttpMetrics::default();
        let app = Router::new()
            .route(
                "/v1/shards/init",
                post(|| async {
                    std::future::pending::<()>().await;
                    "unreachable"
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                metrics.clone(),
                observe,
            ));
        let request = app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/shards/init")
                .body(Body::empty())
                .unwrap(),
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(5), request)
                .await
                .is_err()
        );
        let text = metrics.render();
        assert!(text.contains("pir_http_inflight{endpoint=\"init\"} 0"));
        assert!(text.contains("pir_http_cancelled_total{endpoint=\"init\"} 1"));
    }
    #[test]
    fn dropped_handler_is_a_cancellation_and_labels_are_bounded() {
        let metrics = HttpMetrics::default();
        metrics.initialize(&["init"]);
        drop(RequestGuard {
            metrics: metrics.clone(),
            endpoint: "init",
            began: Instant::now(),
            code: None,
        });
        assert!(metrics
            .render()
            .contains("pir_http_cancelled_total{endpoint=\"init\"} 1"));
        assert_eq!(
            endpoint("/v1/shards/123/revisions/private/query/pages"),
            Some("query_pages")
        );
        assert_eq!(endpoint("/metrics"), None);
        assert!(!metrics.render().contains("private"));
    }
}
