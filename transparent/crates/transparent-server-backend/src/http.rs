use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use pir_observability::HttpMetrics;

/// Product-owned route classification; shared observation sees only fixed labels.
pub async fn observe(State(metrics): State<HttpMetrics>, request: Request, next: Next) -> Response {
    let label = endpoint(request.uri().path());
    pir_observability::observe(metrics, label, request, next).await
}

/// Only a fixed endpoint category survives classification. No dynamic path segment is retained.
pub fn endpoint(path: &str) -> Option<&'static str> {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn route_categories_preserve_bounded_labels() {
        for (path, label) in [
            ("/v2/shards/init", "init"),
            ("/v2/shards", "map"),
            ("/v2/filters/shards", "map"),
            ("/v1/filters/private", "filter"),
            ("/v2/shards/private/revisions/digest/manifest", "manifest"),
            ("/v2/shards/private/revisions/digest/setup/pages/0", "setup"),
            (
                "/v2/shards/private/revisions/digest/query/pages",
                "query_pages",
            ),
            (
                "/v2/shards/private/revisions/digest/query/directory",
                "query_directory",
            ),
            (
                "/v2/shards/private/revisions/digest/query/unknown",
                "query_other",
            ),
        ] {
            assert_eq!(endpoint(path), Some(label));
        }
        assert_eq!(endpoint("/metrics"), None);
        assert_eq!(endpoint("/v1/receiver/query"), None);
    }

    #[tokio::test]
    async fn transparent_route_exports_existing_metric_families_and_labels() {
        use axum::{body::Body, routing::post, Router};
        use tower::ServiceExt;
        let metrics = HttpMetrics::default();
        let app = Router::new()
            .route(
                "/v2/shards/:id/revisions/:digest/query/pages",
                post(|| async { "answer" }),
            )
            .layer(axum::middleware::from_fn_with_state(
                metrics.clone(),
                observe,
            ));
        let response = app
            .oneshot(
                Request::post("/v2/shards/private/revisions/secret/query/pages")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        axum::body::to_bytes(response.into_body(), 100)
            .await
            .unwrap();
        let text = metrics.render();
        for family in [
            "arrivals_total",
            "inflight",
            "cancelled_total",
            "request_body_bytes_total",
            "response_body_bytes_total",
            "body_errors_total",
            "incomplete_responses_total",
        ] {
            assert!(
                text.contains(&format!("pir_http_{family}{{endpoint=\"query_pages\"}}")),
                "{family}"
            );
        }
        assert!(text.contains("pir_http_responses_total{endpoint=\"query_pages\",code=\"2xx\"} 1"));
        assert!(text.contains("pir_http_response_body_bytes_total{endpoint=\"query_pages\"} 6"));
        assert!(text
            .contains("pir_http_duration_seconds_bucket{endpoint=\"query_pages\",le=\"0.001\"}"));
        assert!(text.contains("pir_http_duration_seconds_count{endpoint=\"query_pages\"} 1"));
        assert!(!text.contains("private"));
        assert!(!text.contains("secret"));
    }
}
