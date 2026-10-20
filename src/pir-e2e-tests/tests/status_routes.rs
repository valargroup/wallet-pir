//! The Enhance coordinator answers only operator routes on its own listener.
//! Wallets reach query ingress, and Status is a separate process.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use enhance_publisher::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
};
use tower::ServiceExt;

#[tokio::test]
async fn unready_coordinator_serves_operator_routes_and_never_wallets() {
    let root = tempfile::tempdir().unwrap();
    let groups = vec![Group {
        placement_policy: Default::default(),
        id: "g0".into(),
        sequence: 0,
        replicas: (0..2)
            .map(|i| Replica {
                name: format!("r{i}"),
                url: format!("http://127.0.0.1:{}", 9 + i),
                incarnation: String::new(),
                ledger: Ledger::default(),
            })
            .collect(),
    }];
    let coordinator = Coordinator::open(&root.path().join("coordinator"), groups).unwrap();
    let routes = coordinator.router();
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
    assert_eq!(health["registered_groups"], 1);
    assert_eq!(health["registered_workers"], 2);
    assert_eq!(health["generation"], serde_json::Value::Null);
}
