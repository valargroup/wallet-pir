//! Host-only Axum middleware and streamed-body observation for [`HttpMetrics`].
use crate::{HttpMetrics, RequestGuard};
use axum::{
    body::{Body, Bytes},
    extract::Request,
    middleware::Next,
    response::Response,
};
use http_body::{Body as HttpBody, Frame, SizeHint};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

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
    use crate::HttpMetrics;
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
}
