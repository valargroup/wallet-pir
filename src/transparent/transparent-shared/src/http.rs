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

/// Canonical namespace of every wallet-facing Transparent operation.
pub const CANONICAL_PREFIX: &str = "/transparent/v1";
/// Deployed alias of the shard, filter-shard and transaction-detail
/// operations (`/shards/...`, `/filters/shards/...`, `/txid/...`).
pub const LEGACY_PREFIX: &str = "/v2";
/// Deployed alias of the compact-filter operations (`/filters/info`,
/// `/filters/chain`, `/filters/digests`, `/filters/range`). Only `/filters/*`
/// suffixes are public under it; `/v1/health` and `/v1/ready` stay private.
pub const LEGACY_FILTERS_PREFIX: &str = "/v1";

fn suffix_under<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    path.strip_prefix(prefix).filter(|s| s.starts_with('/'))
}

/// The shard, filter-shard or txid operation suffix of a path under
/// [`CANONICAL_PREFIX`] or [`LEGACY_PREFIX`], e.g. `/shards/init`.
pub fn shard_operation(path: &str) -> Option<&str> {
    suffix_under(path, CANONICAL_PREFIX).or_else(|| suffix_under(path, LEGACY_PREFIX))
}

/// The operation suffix of any wallet-facing Transparent path: a
/// [`shard_operation`] or a `/filters/*` operation under
/// [`LEGACY_FILTERS_PREFIX`].
pub fn public_operation(path: &str) -> Option<&str> {
    shard_operation(path).or_else(|| {
        suffix_under(path, LEGACY_FILTERS_PREFIX).filter(|s| s.starts_with("/filters/"))
    })
}

// Preserve historical classification even for product-shaped 404s. These
// mappings are product policy; the shared observer receives only fixed labels.
// Canonical and legacy paths of one operation share its label.
/// Only a fixed endpoint category survives classification. No dynamic path segment is retained.
fn endpoint(path: &str) -> Option<&'static str> {
    let operation = public_operation(path).unwrap_or_default();
    if operation == "/shards/init" {
        Some("init")
    } else if operation == "/shards" || operation == "/filters/shards" {
        Some("map")
    } else if operation.starts_with("/filters/") {
        Some("filter")
    } else if operation.starts_with("/shards/") && operation.ends_with("/manifest") {
        Some("manifest")
    } else if operation.starts_with("/shards/") && operation.contains("/setup/") {
        Some("setup")
    } else if let Some(route) = path.strip_prefix("/v1/receiver/") {
        receiver_endpoint(route)
    } else if operation.starts_with("/shards/") && operation.contains("/query/") {
        if operation.ends_with("/directory") {
            Some("query_directory")
        } else if operation.ends_with("/pages") {
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
        "witness" => "receiver_witness",
        "health" => "receiver_health",
        _ => return None,
    })
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
            let canonical = public_operation(path).map(|op| format!("{CANONICAL_PREFIX}{op}"));
            assert_eq!(endpoint(&canonical.unwrap()), Some(label), "{path}");
        }
        assert_eq!(endpoint("/transparent/v1/filters/info"), Some("filter"));
        assert_eq!(endpoint("/metrics"), None);
        assert_eq!(endpoint("/v1/health"), None);
        assert_eq!(endpoint("/transparent/v1x/shards/init"), None);
        assert_eq!(endpoint("/v1/receiver/query"), Some("receiver_query"));
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
            .clone()
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
        let foreign = app
            .oneshot(
                Request::get("/v1/receiver/query")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(foreign.status(), axum::http::StatusCode::NOT_FOUND);
        drop(foreign);
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
        assert!(
            text.contains("pir_http_responses_total{endpoint=\"receiver_query\",code=\"4xx\"} 1")
        );
        assert!(!text.contains("private"));
        assert!(!text.contains("secret"));
    }
}
