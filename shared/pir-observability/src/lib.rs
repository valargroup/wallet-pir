//! Bounded aggregate HTTP observations. Paths and client data never become labels.
use axum::{
    body::{Body, Bytes},
    extract::Request,
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
        self.render_labeled(name, "")
    }
    /// Renders with `labels` (`key="value"` pairs, comma separated, or empty)
    /// on every series.
    pub fn render_labeled(&self, name: &str, labels: &str) -> String {
        let value = self.0.lock().unwrap();
        let (comma, braces) = if labels.is_empty() {
            ("", String::new())
        } else {
            (",", format!("{{{labels}}}"))
        };
        let mut total = 0;
        let mut out = String::new();
        for (i, count) in value.0.iter().enumerate() {
            total += count;
            let bound = BOUNDS
                .get(i)
                .map(|n| n.to_string())
                .unwrap_or_else(|| "+Inf".into());
            out.push_str(&format!(
                "{name}_bucket{{{labels}{comma}le=\"{bound}\"}} {total}\n"
            ));
        }
        out.push_str(&format!(
            "{name}_count{braces} {total}\n{name}_sum{braces} {}\n",
            value.1
        ));
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
/// Product callbacks for streamed body observations. Completion includes the
/// last successful frame when the inner body advertises end-of-stream.
pub trait BodyObserver {
    fn data(&mut self, _bytes: usize) {}
    fn error(&mut self) {}
    fn complete(&mut self) {}
    fn incomplete(&mut self) {}
}

/// Observe frames without buffering or changing the inner body's ownership,
/// framing, errors or transport lifetime.
pub struct ObservedBody<O: BodyObserver> {
    body: Body,
    observer: O,
    ended: bool,
}
impl<O: BodyObserver> ObservedBody<O> {
    pub fn new(body: Body, observer: O) -> Self {
        Self {
            body,
            observer,
            ended: false,
        }
    }
}
impl<O: BodyObserver> Drop for ObservedBody<O> {
    fn drop(&mut self) {
        if !self.ended && !self.body.is_end_stream() {
            self.observer.incomplete();
        }
    }
}
impl<O: BodyObserver + Unpin> HttpBody for ObservedBody<O> {
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
                    self.observer.data(bytes.len());
                }
            }
            Poll::Ready(Some(Err(_))) => self.observer.error(),
            _ => {}
        }
        if !self.ended
            && (matches!(result, Poll::Ready(None))
                || (matches!(result, Poll::Ready(Some(Ok(_)))) && self.body.is_end_stream()))
        {
            self.ended = true;
            self.observer.complete();
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

struct Counters {
    metrics: HttpMetrics,
    endpoint: &'static str,
    response: bool,
}
impl BodyObserver for Counters {
    fn data(&mut self, bytes: usize) {
        let mut ops = self.metrics.inner.lock().unwrap();
        let op = ops.entry(self.endpoint).or_default();
        if self.response {
            op.download += bytes as u64;
        } else {
            op.upload += bytes as u64;
        }
    }
    fn error(&mut self) {
        self.metrics
            .inner
            .lock()
            .unwrap()
            .entry(self.endpoint)
            .or_default()
            .body_errors += 1;
    }
    fn incomplete(&mut self) {
        if self.response {
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

/// Timer covers body extraction, admission, errors, and cancellation until response construction.
/// Response bytes are counted only as the server polls them; this is not client receipt or decoding.
pub async fn observe(
    metrics: HttpMetrics,
    endpoint: Option<&'static str>,
    mut request: Request,
    next: Next,
) -> Response {
    #[derive(Clone)]
    struct Observed;
    if request.extensions().get::<Observed>().is_some() {
        return next.run(request).await;
    }
    let Some(endpoint) = endpoint else {
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
    let body = Body::new(ObservedBody::new(
        body,
        Counters {
            metrics: metrics.clone(),
            endpoint,
            response: false,
        },
    ));
    let response = next.run(Request::from_parts(parts, body)).await;
    guard.code = Some(response.status().as_u16());
    drop(guard);
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(ObservedBody::new(
            body,
            Counters {
                metrics,
                endpoint,
                response: true,
            },
        )),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::{http::StatusCode, routing::post, Router};
    use tower::ServiceExt;
    async fn classified(
        State(metrics): State<HttpMetrics>,
        request: Request,
        next: Next,
    ) -> Response {
        observe(metrics, Some("init"), request, next).await
    }
    #[test]
    fn labels_reach_every_distribution_series() {
        let distribution = Distribution::default();
        distribution.observe(0.5);
        distribution.observe(3.);
        let text = distribution.render_labeled("d", "stage=\"build\"");
        assert!(text.contains("d_bucket{stage=\"build\",le=\"0.5\"} 1\n"));
        assert!(text.contains("d_bucket{stage=\"build\",le=\"+Inf\"} 2\n"));
        assert!(text.contains("d_count{stage=\"build\"} 2\nd_sum{stage=\"build\"} 3.5\n"));
        let bare = distribution.render("d");
        assert!(bare.contains("d_bucket{le=\"0.5\"} 1\n"));
        assert!(bare.contains("d_count 2\nd_sum 3.5\n"));
    }
    #[tokio::test]
    async fn outcomes_include_early_rejections_and_actual_bytes() {
        let metrics = HttpMetrics::default();
        let app = Router::new()
            .route(
                "/test",
                post(|| async { (StatusCode::SERVICE_UNAVAILABLE, "busy") }),
            )
            .layer(axum::middleware::from_fn_with_state(
                metrics.clone(),
                classified,
            ));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/test")
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
            .route("/test", post(|| async { "payload" }))
            .layer(axum::middleware::from_fn_with_state(
                metrics.clone(),
                classified,
            ))
            .layer(axum::middleware::from_fn_with_state(
                metrics.clone(),
                classified,
            ));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/test")
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
                "/test",
                post(|| async {
                    std::future::pending::<()>().await;
                    "unreachable"
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                metrics.clone(),
                classified,
            ));
        let request = app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/test")
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
        assert!(!metrics.render().contains("private"));
    }
}
