//! HTTP fixtures exercise the readiness contract without starting product libraries.
use super::*;
use crate::{host::status::ProductPhase, publisher::PublisherProduct};
use axum::response::Redirect;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{net::TcpListener, task::JoinHandle};

struct Server {
    address: SocketAddr,
    task: JoinHandle<()>,
}
impl Server {
    async fn start(routes: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, routes).await.unwrap();
        });
        Self { address, task }
    }
    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.address)
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn readiness(rule: ReadinessRule, routes: Router) -> (StatusCode, Value) {
    let product = Server::start(routes).await;
    let status = PublisherStatus::default();
    status.register(
        PublisherProduct::TransparentHistory,
        vec![ReadinessProbe {
            address: product.address,
            path: "/probe",
            rule,
        }],
    );
    let management = Server::start(router(status).unwrap()).await;
    let response = reqwest::Client::new()
        .get(management.url("/ready"))
        .timeout(Duration::from_secs(4))
        .send()
        .await
        .unwrap();
    (response.status(), response.json().await.unwrap())
}

fn body_route(status: StatusCode, body: impl Into<String>) -> Router {
    let body = body.into();
    Router::new().route("/probe", get(move || async move { (status, body) }))
}

#[tokio::test]
async fn readiness_preserves_each_product_response_predicate() {
    let cases = [
        (ReadinessRule::HttpSuccess, "not json", true),
        (ReadinessRule::ServingPhase, r#"{"phase":"serving"}"#, true),
        (ReadinessRule::ServingPhase, r#"{"phase":"running"}"#, false),
        (ReadinessRule::ServingPhase, "not json", false),
        (
            ReadinessRule::TransactionDetailsPublished,
            r#"{"phase":"running","halted":null,"stalled":false,"published":{}}"#,
            true,
        ),
        (
            ReadinessRule::TransactionDetailsPublished,
            r#"{"phase":"running","stalled":false,"published":{}}"#,
            true,
        ),
        (
            ReadinessRule::TransactionDetailsPublished,
            r#"{"phase":"running","halted":"source failed","stalled":false,"published":{}}"#,
            false,
        ),
        (
            ReadinessRule::TransactionDetailsPublished,
            r#"{"phase":"running","stalled":true,"published":{}}"#,
            false,
        ),
        (
            ReadinessRule::TransactionDetailsPublished,
            r#"{"phase":"running","stalled":false,"published":null}"#,
            false,
        ),
        (
            ReadinessRule::TransactionDetailsPublished,
            r#"{"phase":"running","published":{}}"#,
            false,
        ),
        (
            ReadinessRule::TransactionDetailsPublished,
            r#"{"phase":"serving","stalled":false,"published":{}}"#,
            false,
        ),
    ];
    for (rule, body, expected) in cases {
        let (status, report) = readiness(rule, body_route(StatusCode::OK, body)).await;
        assert_eq!(
            status,
            if expected {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            "{body}"
        );
        assert_eq!(
            report,
            json!({"ready":expected,"products":{"transparent_history":{"phase":"running","ready":expected}}})
        );
    }
}

#[tokio::test]
async fn readiness_requires_success_and_never_follows_redirects() {
    for status in [StatusCode::SERVICE_UNAVAILABLE, StatusCode::NOT_FOUND] {
        let (status, _) = readiness(ReadinessRule::HttpSuccess, body_route(status, "")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }
    let visits = Arc::new(AtomicUsize::new(0));
    let observed = visits.clone();
    let routes = Router::new()
        .route("/probe", get(|| async { Redirect::temporary("/target") }))
        .route(
            "/target",
            get(move || async move {
                observed.fetch_add(1, Ordering::SeqCst);
                "ready"
            }),
        );
    let (status, _) = readiness(ReadinessRule::HttpSuccess, routes).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(visits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn only_json_probes_enforce_the_body_limit() {
    let mut body = r#"{"phase":"serving"}"#.to_owned();
    body.extend(std::iter::repeat_n(' ', MAX_PROBE_BYTES - body.len()));
    let (status, _) = readiness(
        ReadinessRule::ServingPhase,
        body_route(StatusCode::OK, body.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    body.push(' ');
    let (status, _) = readiness(
        ReadinessRule::ServingPhase,
        body_route(StatusCode::OK, body.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let (status, _) = readiness(ReadinessRule::HttpSuccess, body_route(StatusCode::OK, body)).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_stalled_probe_is_bounded_by_the_request_timeout() {
    let routes = Router::new().route(
        "/probe",
        get(|| async { std::future::pending::<&'static str>().await }),
    );
    let (status, report) = tokio::time::timeout(
        Duration::from_secs(4),
        readiness(ReadinessRule::HttpSuccess, routes),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(report["ready"], false);
}

#[tokio::test]
async fn every_probe_is_required_and_failed_products_are_not_probed() {
    let probes = Server::start(
        Router::new()
            .route("/first", get(|| async { r#"{"phase":"serving"}"# }))
            .route("/second", get(|| async { StatusCode::SERVICE_UNAVAILABLE })),
    )
    .await;
    let status = PublisherStatus::default();
    status.register(
        PublisherProduct::TransparentHistory,
        vec![
            ReadinessProbe {
                address: probes.address,
                path: "/first",
                rule: ReadinessRule::ServingPhase,
            },
            ReadinessProbe {
                address: probes.address,
                path: "/second",
                rule: ReadinessRule::HttpSuccess,
            },
        ],
    );
    let management = Server::start(router(status.clone()).unwrap()).await;
    let client = reqwest::Client::new();
    let response = client.get(management.url("/ready")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    status.set_phase(PublisherProduct::TransparentHistory, ProductPhase::Failed);
    let response = client.get(management.url("/health")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({"ready":false,"products":{"transparent_history":{"phase":"failed","ready":false}}})
    );
}

#[tokio::test]
async fn a_stalled_json_body_is_also_bounded() {
    // Send headers immediately, then keep the body open without producing bytes.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let received = stream.read(&mut request).await.unwrap();
        assert!(received > 0, "client must send the probe request");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });
    let _server = Server { address, task };
    let state = Management {
        status: PublisherStatus::default(),
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
    };
    let probe = ReadinessProbe {
        address,
        path: "/probe",
        rule: ReadinessRule::ServingPhase,
    };
    assert!(
        !tokio::time::timeout(Duration::from_secs(4), state.probe(&probe))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn management_preserves_product_and_probe_order_and_skips_failed_owners() {
    let visits = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = visits.clone();
    let probes = Server::start(Router::new().fallback(get(
        move |request: axum::extract::Request| async move {
            observed
                .lock()
                .unwrap()
                .push(request.uri().path().to_owned());
            StatusCode::OK
        },
    )))
    .await;
    let status = PublisherStatus::default();
    for (product, paths) in [
        (
            PublisherProduct::TransparentHistory,
            vec!["/history-status", "/history-init"],
        ),
        (PublisherProduct::Enhance, vec!["/enhance"]),
        (PublisherProduct::Receiver, vec!["/receiver"]),
    ] {
        status.register(
            product,
            paths
                .into_iter()
                .map(|path| ReadinessProbe {
                    address: probes.address,
                    path,
                    rule: ReadinessRule::HttpSuccess,
                })
                .collect(),
        );
    }
    let management = Server::start(router(status.clone()).unwrap()).await;
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(management.url("/ready"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        *visits.lock().unwrap(),
        ["/enhance", "/receiver", "/history-status", "/history-init"]
    );
    visits.lock().unwrap().clear();
    status.set_phase(PublisherProduct::TransparentHistory, ProductPhase::Failed);
    assert_eq!(
        client
            .get(management.url("/ready"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(*visits.lock().unwrap(), ["/enhance", "/receiver"]);
}
