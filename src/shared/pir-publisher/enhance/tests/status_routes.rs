//! Enhance publication routes answer on their own listener. Status is a separate
//! process and is not merged into this router.
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
async fn unready_enhance_owner_serves_its_own_routes() {
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
    let get = |uri: &str| Request::builder().uri(uri).body(Body::empty()).unwrap();

    for uri in ["/v1/enhance/init", "/ready"] {
        let response = routes.clone().oneshot(get(uri)).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{uri}");
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
    let response = routes.oneshot(get("/v1/status/init")).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
