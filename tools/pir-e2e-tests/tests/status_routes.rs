//! The Enhance publisher answers only operator routes on its own listener.
//! Wallets reach query ingress, and Status is a separate process.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use enhance_publisher::{publisher::Publisher, zakura::ZakuraClient};
use tower::ServiceExt;

#[tokio::test]
async fn unready_publisher_serves_operator_routes_and_never_wallets() {
    let root = tempfile::tempdir().unwrap();
    let cookie = root.path().join("cookie");
    std::fs::write(&cookie, "fixture:fixture").unwrap();
    let publisher = Publisher::open(
        &root.path().join("publisher"),
        ZakuraClient::from_cookie_file("http://127.0.0.1:1", cookie).unwrap(),
    )
    .unwrap();
    let routes = publisher.router();
    let request = |method: &str, uri: &str| {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    };
    let get = |uri: &str| request("GET", uri);

    let response = routes.clone().oneshot(get("/ready")).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let session = format!("/v1/enhance/session/{}", "ab".repeat(32));
    for (method, uri) in [
        ("GET", "/v1/enhance/init"),
        ("GET", "/v1/enhance/sessions/1/0"),
        ("GET", session.as_str()),
        ("POST", "/v1/enhance/query"),
        ("GET", "/v1/status/init"),
    ] {
        let response = routes.clone().oneshot(request(method, uri)).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {uri}");
    }
    let response = routes.clone().oneshot(get("/v1/health")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let health: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(health.get("registered_groups").is_none());
    assert!(health.get("registered_workers").is_none());
    assert_eq!(health["eligible"], false);
    assert_eq!(health["generation"], serde_json::Value::Null);
}
