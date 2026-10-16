//! Aggregate public-entrypoint telemetry. Bodies remain streaming and retain
//! their original ownership (including admission guards) until transport drop.
use axum::{
    body::{Body, Bytes},
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use http_body::{Body as HttpBody, Frame, SizeHint};
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts, Registry, TextEncoder,
};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Clone)]
pub(crate) struct HttpMetrics {
    registry: Registry,
    processing: HistogramVec,
    requests: IntCounterVec,
    upload: IntCounterVec,
    download: IntCounterVec,
    started: f64,
    clock: Instant,
    arrivals: IntCounterVec,
    arrival_windows: Arc<Mutex<std::collections::BTreeMap<String, ArrivalWindow>>>,
}
impl Default for HttpMetrics {
    fn default() -> Self {
        let registry = Registry::new();
        let processing = HistogramVec::new(
            HistogramOpts::new(
                "enhance_http_request_processing_duration_seconds",
                "Time from complete request body to response ready",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1., 2.5,
                5., 10., 30., 60., 120.,
            ]),
            &["endpoint"],
        )
        .unwrap();
        let requests = IntCounterVec::new(
            Opts::new(
                "enhance_http_requests_total",
                "Responses by public entrypoint and status",
            ),
            &["endpoint", "status"],
        )
        .unwrap();
        let upload = IntCounterVec::new(
            Opts::new(
                "enhance_http_request_body_bytes_total",
                "Consumed public request payload bytes",
            ),
            &["endpoint"],
        )
        .unwrap();
        let download = IntCounterVec::new(
            Opts::new(
                "enhance_http_response_body_bytes_total",
                "Emitted public response payload bytes",
            ),
            &["endpoint"],
        )
        .unwrap();
        let arrivals = IntCounterVec::new(
            Opts::new(
                "enhance_http_arrivals_total",
                "Public requests arriving, including incomplete and rejected requests",
            ),
            &["endpoint"],
        )
        .unwrap();
        registry.register(Box::new(arrivals.clone())).unwrap();
        registry.register(Box::new(processing.clone())).unwrap();
        registry.register(Box::new(requests.clone())).unwrap();
        registry.register(Box::new(upload.clone())).unwrap();
        registry.register(Box::new(download.clone())).unwrap();
        Self {
            arrivals,
            clock: Instant::now(),
            arrival_windows: Default::default(),
            registry,
            processing,
            requests,
            upload,
            download,
            started: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        }
    }
}
impl HttpMetrics {
    /// Initialize only routes served by this process, including idle counters.
    pub(crate) fn endpoint(&self, endpoint: &str) {
        self.arrivals.with_label_values(&[endpoint]);
        self.arrival_windows
            .lock()
            .unwrap()
            .entry(endpoint.into())
            .or_default();
        self.processing.with_label_values(&[endpoint]);
        self.upload.with_label_values(&[endpoint]);
        self.download.with_label_values(&[endpoint]);
    }
    pub(crate) fn render(&self) -> String {
        let mut bytes = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut bytes)
            .unwrap();
        let mut text = format!(
            "{}\nprocess_start_time_seconds {}\n",
            String::from_utf8(bytes).unwrap(),
            self.started
        );
        let now = self.clock.elapsed().as_secs();
        for (endpoint, window) in self.arrival_windows.lock().unwrap().iter() {
            if let Some(count) = window.last_ten(now) {
                text.push_str(&format!(
                    "enhance_http_arrivals_last_10_seconds{{endpoint=\"{endpoint}\"}} {count}\n"
                ));
            }
        }
        text
    }
}

/// Eleven slots retain the current second plus ten complete seconds. No
/// per-request history or wall-clock adjustment can grow or skew this window.
#[derive(Default)]
struct ArrivalWindow {
    seconds: [(u64, u64); 11],
}
impl ArrivalWindow {
    fn record(&mut self, second: u64) {
        let slot = &mut self.seconds[(second % 11) as usize];
        if slot.0 != second {
            *slot = (second, 0);
        }
        slot.1 = slot.1.saturating_add(1);
    }
    fn last_ten(&self, now: u64) -> Option<u64> {
        let beginning = now.checked_sub(10)?;
        Some(
            self.seconds
                .iter()
                .filter(|(at, _)| *at >= beginning && *at < now)
                .map(|(_, n)| *n)
                .sum(),
        )
    }
}

struct CountedBody {
    inner: Body,
    bytes: IntCounter,
    completed: Option<Arc<Mutex<Option<Instant>>>>,
}
impl HttpBody for CountedBody {
    type Data = Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &result {
            if let Some(data) = frame.data_ref() {
                self.bytes.inc_by(data.len() as u64);
            }
        }
        // A failed/truncated body never starts the processing timer.
        if matches!(result, Poll::Ready(None))
            || (matches!(result, Poll::Ready(Some(Ok(_)))) && self.inner.is_end_stream())
        {
            if let Some(completed) = self.completed.take() {
                *completed.lock().unwrap() = Some(Instant::now());
            }
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

pub(crate) async fn measure(
    State(metrics): State<HttpMetrics>,
    request: Request,
    next: Next,
) -> Response {
    let endpoint = match (request.method().as_str(), request.uri().path()) {
        ("GET", "/v1/enhance/init") => "init",
        ("POST", "/v1/enhance/query") => "query",
        _ => return next.run(request).await,
    };
    metrics.arrivals.with_label_values(&[endpoint]).inc();
    metrics
        .arrival_windows
        .lock()
        .unwrap()
        .entry(endpoint.into())
        .or_default()
        .record(metrics.clock.elapsed().as_secs());
    let (parts, body) = request.into_parts();
    let completed = Arc::new(Mutex::new(if endpoint == "init" || body.is_end_stream() {
        Some(Instant::now())
    } else {
        None
    }));
    let body = Body::new(CountedBody {
        inner: body,
        bytes: metrics.upload.with_label_values(&[endpoint]),
        completed: Some(completed.clone()),
    });
    let response = next.run(Request::from_parts(parts, body)).await;
    if let Some(start) = *completed.lock().unwrap() {
        metrics
            .processing
            .with_label_values(&[endpoint])
            .observe(start.elapsed().as_secs_f64());
    }
    metrics
        .requests
        .with_label_values(&[endpoint, response.status().as_str()])
        .inc();
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(CountedBody {
            inner: body,
            bytes: metrics.download.with_label_values(&[endpoint]),
            completed: None,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        routing::{get, post},
        Router,
    };
    use std::time::Duration;
    use tower::ServiceExt;

    async fn echo(request: Request) -> Body {
        let bytes = axum::body::to_bytes(request.into_body(), 1024)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        Body::from(bytes)
    }
    fn app(metrics: &HttpMetrics) -> Router {
        metrics.endpoint("init");
        metrics.endpoint("query");
        Router::new()
            .route("/v1/enhance/query", post(echo))
            .route("/v1/enhance/init", get(|| async { "manifest" }))
            .layer(axum::middleware::from_fn_with_state(
                metrics.clone(),
                measure,
            ))
    }
    #[tokio::test]
    async fn slow_upload_is_excluded_and_bytes_are_streamed() {
        let metrics = HttpMetrics::default();
        let app = app(&metrics);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let body = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
        let pending =
            tokio::spawn(app.oneshot(Request::post("/v1/enhance/query").body(body).unwrap()));
        tx.send(Ok::<_, std::io::Error>(Bytes::from_static(b"abc")))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(metrics.upload.with_label_values(&["query"]).get(), 3);
        assert_eq!(metrics.arrivals.with_label_values(&["query"]).get(), 1);
        assert_eq!(
            metrics
                .processing
                .with_label_values(&["query"])
                .get_sample_count(),
            0
        );
        tx.send(Ok(Bytes::from_static(b"de"))).await.unwrap();
        drop(tx);
        let response = pending.await.unwrap().unwrap();
        let histogram = metrics.processing.with_label_values(&["query"]);
        assert_eq!(histogram.get_sample_count(), 1);
        assert!(histogram.get_sample_sum() >= 0.02);
        assert!(
            histogram.get_sample_sum() < 0.25,
            "upload time leaked into processing latency"
        );
        assert_eq!(metrics.download.with_label_values(&["query"]).get(), 0);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap()
                .as_ref(),
            b"abcde"
        );
        assert_eq!(metrics.upload.with_label_values(&["query"]).get(), 5);
        assert_eq!(metrics.download.with_label_values(&["query"]).get(), 5);
    }
    #[tokio::test]
    async fn init_and_unmatched_paths_have_bounded_labels() {
        let metrics = HttpMetrics::default();
        let app = app(&metrics);
        let response = app
            .clone()
            .oneshot(
                Request::get("/v1/enhance/init")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        app.oneshot(
            Request::get("/session/private-id")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            metrics
                .processing
                .with_label_values(&["init"])
                .get_sample_count(),
            1
        );
        assert_eq!(metrics.download.with_label_values(&["init"]).get(), 8);
        assert_eq!(metrics.upload.with_label_values(&["init"]).get(), 0);
        assert!(!metrics.render().contains("private-id"));
    }
    #[tokio::test]
    async fn early_rejection_has_no_processing_sample_and_no_invented_upload() {
        let metrics = HttpMetrics::default();
        metrics.endpoint("query");
        let app = Router::new()
            .route(
                "/v1/enhance/query",
                post(|| async { axum::http::StatusCode::TOO_MANY_REQUESTS }),
            )
            .layer(axum::middleware::from_fn_with_state(
                metrics.clone(),
                measure,
            ));
        let response = app
            .oneshot(
                Request::post("/v1/enhance/query")
                    .body(Body::from("unread"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 429);
        assert_eq!(metrics.arrivals.with_label_values(&["query"]).get(), 1);
        assert_eq!(
            metrics
                .processing
                .with_label_values(&["query"])
                .get_sample_count(),
            0
        );
        assert_eq!(metrics.upload.with_label_values(&["query"]).get(), 0);
        assert_eq!(
            metrics.requests.with_label_values(&["query", "429"]).get(),
            1
        );
    }
    #[tokio::test]
    async fn response_guard_survives_end_of_stream_until_drop() {
        let metrics = HttpMetrics::default();
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let body = crate::response_body::guarded(
            vec![1, 2, 3],
            slots.clone().acquire_owned().await.unwrap(),
            || Ok(()),
        );
        let mut counted = CountedBody {
            inner: body,
            bytes: metrics.download.with_label_values(&["query"]),
            completed: None,
        };
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut counted).poll_frame(cx))
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut counted).poll_frame(cx))
                .await
                .is_none()
        );
        assert_eq!(slots.available_permits(), 0);
        drop(counted);
        assert_eq!(slots.available_permits(), 1);
        assert_eq!(metrics.download.with_label_values(&["query"]).get(), 3);
    }
    #[tokio::test]
    async fn errored_upload_is_not_a_completed_body() {
        let metrics = HttpMetrics::default();
        let completed = Arc::new(Mutex::new(None));
        let body = Body::from_stream(tokio_stream::iter(vec![
            Ok(Bytes::from_static(b"abc")),
            Err(std::io::Error::other("truncated")),
        ]));
        let body = Body::new(CountedBody {
            inner: body,
            bytes: metrics.upload.with_label_values(&["query"]),
            completed: Some(completed.clone()),
        });
        assert!(axum::body::to_bytes(body, 1024).await.is_err());
        assert!(completed.lock().unwrap().is_none());
        assert_eq!(metrics.upload.with_label_values(&["query"]).get(), 3);
    }
}

#[cfg(test)]
mod arrival_tests {
    use super::*;
    #[test]
    fn complete_seconds_cold_start_wrap_and_idle() {
        let mut window = ArrivalWindow::default();
        window.record(0);
        assert_eq!(window.last_ten(9), None);
        window.record(9);
        window.record(9);
        window.record(10);
        assert_eq!(window.last_ten(10), Some(3));
        assert_eq!(window.last_ten(11), Some(3));
        window.record(20);
        assert_eq!(window.last_ten(20), Some(1));
        assert_eq!(window.last_ten(21), Some(1));
        assert_eq!(window.last_ten(31), Some(0));
    }
}
