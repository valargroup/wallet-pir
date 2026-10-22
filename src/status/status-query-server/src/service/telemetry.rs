//! HTTP observation and loopback management routes over the Status core's
//! aggregate counters. No query identifiers enter labels.
use axum::{
    body::HttpBody,
    extract::Request,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use status_material::telemetry;
use std::time::Instant;

pub async fn observe(role: &'static str, request: Request, next: Next) -> Response {
    let Some(operation) = telemetry::operation(role, request.uri().path()) else {
        return next.run(request).await;
    };
    let upload = content_length(request.headers()).unwrap_or(0);
    telemetry::arrival(operation);
    let start = Instant::now();
    let response = next.run(request).await;
    let download = content_length(response.headers())
        .or_else(|| response.body().size_hint().exact())
        .unwrap_or(0);
    telemetry::outcome(
        operation,
        response.status().as_u16(),
        upload,
        download,
        start.elapsed(),
    );
    response
}

fn content_length(headers: &axum::http::HeaderMap) -> Option<u64> {
    headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
}

async fn snapshot() -> Json<telemetry::Telemetry> {
    Json(
        tokio::task::spawn_blocking(telemetry::snapshot)
            .await
            .unwrap_or_default(),
    )
}

/// Bind this router only to a loopback management listener.
pub fn routes() -> Router {
    Router::new()
        .route("/internal/status-apm", get(snapshot))
        .route(
            "/internal/metrics",
            get(|| async {
                (
                    [(
                        axum::http::header::CONTENT_TYPE,
                        "text/plain; version=0.0.4",
                    )],
                    telemetry::prometheus(),
                )
                    .into_response()
            }),
        )
        .route("/internal/health", get(|| async { "ok\n" }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn counts_one_ingress_and_exports_no_request_identity() {
        let app = Router::new()
            .route("/v1/status/init", get(|| async { "xx" }))
            .layer(axum::middleware::from_fn(|req, next| {
                observe("router", req, next)
            }));
        let before = telemetry::snapshot().operations["init"].clone();
        let response = app
            .oneshot(Request::get("/v1/status/init").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = routes()
            .oneshot(
                Request::builder()
                    .uri("/internal/status-apm")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = to_bytes(body.into_body(), 64 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let init = &value["operations"]["init"];
        assert_eq!(init["arrivals"].as_u64().unwrap() - before.arrivals, 1);
        assert_eq!(
            init["download_bytes"].as_u64().unwrap() - before.download_bytes,
            2
        );
        assert!(String::from_utf8_lossy(&bytes).find("xx").is_none());
    }
}
