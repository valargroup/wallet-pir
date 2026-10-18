//! The real publication owner and an embedded Status publisher share one public
//! listener, as `enhance-pir-server coordinator --status-config` composes them.
//! Status routes stay reachable while Enhance has no published generation.
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
async fn unready_enhance_owner_and_status_routes_coexist() {
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
    let status = status_publisher::publisher::Publisher::new(status_publisher::publisher::Config {
        state_dir: root.path().join("status"),
        salt_hex: hex::encode([1; 32]),
        window_blocks: 1,
        worker_origin: "http://127.0.0.1:9".into(),
        router_origin: "http://127.0.0.1:10".into(),
        query_router_origin: None,
        private_listen: "127.0.0.1:0".parse().unwrap(),
        public_enabled: true,
    })
    .unwrap();
    let routes = coordinator.router().merge(status.routes());
    let get = |uri: &str| Request::builder().uri(uri).body(Body::empty()).unwrap();

    for uri in ["/v1/enhance/init", "/ready"] {
        let response = routes.clone().oneshot(get(uri)).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{uri}");
    }
    // Health is projected from the publisher's durable inventory.
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
    // Status answers its own routes; none fall through to a missing route.
    for request in [
        get("/v1/status/init"),
        Request::builder()
            .method("POST")
            .uri("/v1/status/query")
            .body(Body::empty())
            .unwrap(),
    ] {
        let response = routes.clone().oneshot(request).await.unwrap();
        assert_ne!(response.status(), StatusCode::NOT_FOUND);
    }
}
