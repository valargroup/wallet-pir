use super::*;
use tower::ServiceExt;
#[tokio::test]
async fn discovery_serves_the_shared_contract_version_uncached() {
    let response = discovery()
        .oneshot(
            axum::http::Request::get(pir_service_discovery::PATH)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["content-type"], "application/json");
    let body = axum::body::to_bytes(response.into_body(), 1 << 16)
        .await
        .unwrap();
    assert_eq!(
        &body[..],
        br#"{"service":"pir-publisher","version":"1.0.0"}"#
    );
    assert_eq!(
        pir_service_discovery::check_publisher(&body).unwrap(),
        pir_service_discovery::version()
    );
    let other = discovery()
        .oneshot(
            axum::http::Request::get("/.well-known/other")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(other.status(), StatusCode::NOT_FOUND);
}
