//! Real loopback listeners exercise the host without chain or worker fixtures.
use pir_pipeline::service::Shutdown;
use pir_publisher::run;
use serde_json::{json, Value};
use std::{net::SocketAddr, time::Duration};
fn port() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
#[cfg(all(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
fn example() -> Value {
    serde_json::from_str(include_str!(
        "../../../tools/deploy/publisher/publisher.example.json"
    ))
    .unwrap()
}
#[cfg(all(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
#[tokio::test]
async fn bind_failure_cannot_start_a_publisher_or_create_its_state() {
    let temp = tempfile::tempdir().unwrap();
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut value = example();
    value["management_listen"] = json!(port().to_string());
    for name in ["status", "transparent_history", "receiver"] {
        value[name]["enabled"] = json!(false);
    }
    value["enhance"]["settings"]["data_dir"] = json!(temp.path().join("enhance"));
    value["enhance"]["settings"]["listen"] = json!(port().to_string());
    value["enhance"]["settings"]["artifact_listen"] = json!(port().to_string());
    value["transaction_details"]["settings"]["status_listen"] =
        json!(occupied.local_addr().unwrap().to_string());
    let config = serde_json::from_value(value).unwrap();
    assert!(run(config, Shutdown::default()).await.is_err());
    assert!(!temp.path().join("enhance").exists());
}
async fn report(url: &str) -> Value {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    for _ in 0..100 {
        if let Ok(response) = client.get(url).send().await {
            return response.json().await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("management listener did not start");
}
#[tokio::test]
async fn disabled_products_start_no_work_and_shutdown_releases_the_listener() {
    let address = port();
    let stop = Shutdown::default();
    let mut value = json!({"management_listen":address.to_string()});
    for (name, compiled) in [
        ("enhance", cfg!(feature = "enhance")),
        ("status", cfg!(feature = "status")),
        ("transparent_history", cfg!(feature = "transparent-history")),
        ("transparent_filters", cfg!(feature = "transparent-history")),
        ("transaction_details", cfg!(feature = "transparent-detail")),
        ("receiver", cfg!(feature = "receiver")),
    ] {
        if compiled {
            value[name] = json!({"enabled":false});
        }
    }
    let config = serde_json::from_value(value).unwrap();
    let task = tokio::spawn(run(config, stop.clone()));
    let value = report(&format!("http://{address}/ready")).await;
    assert_eq!(value["ready"], true);
    assert_eq!(value["products"], json!({}));
    stop.request();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::task::yield_now().await;
    let _rebound = tokio::net::TcpListener::bind(address).await.unwrap();
}
#[tokio::test]
async fn management_alone_becomes_ready_and_releases_its_listener() {
    let address = port();
    let stop = Shutdown::default();
    let config = serde_json::from_value(json!({"management_listen": address.to_string()})).unwrap();
    let task = tokio::spawn(run(config, stop.clone()));
    let value = report(&format!("http://{address}/ready")).await;
    assert_eq!(value["ready"], true);
    assert_eq!(value["products"], json!({}));
    stop.request();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::task::yield_now().await;
    let _rebound = tokio::net::TcpListener::bind(address).await.unwrap();
}
#[cfg(all(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
#[tokio::test]
async fn product_startup_failure_is_visible_without_stopping_management() {
    let temp = tempfile::tempdir().unwrap();
    let mut value = example();
    let address = port();
    value["management_listen"] = json!(address.to_string());
    for name in [
        "enhance",
        "transparent_history",
        "transaction_details",
        "receiver",
    ] {
        value[name]["enabled"] = json!(false);
    }
    value["status"]["settings"]["publisher"]["state_dir"] = json!(temp.path().join("state"));
    value["status"]["settings"]["publisher"]["private_listen"] = json!(port().to_string());
    value["status"]["settings"]["cookie"] = json!(temp.path().join("absent-cookie"));
    let stop = Shutdown::default();
    let task = tokio::spawn(run(serde_json::from_value(value).unwrap(), stop.clone()));
    let url = format!("http://{address}/health");
    let mut observed = false;
    for _ in 0..100 {
        let value = report(&url).await;
        if value["products"]["status"]["phase"] == "failed" {
            observed = true;
            assert_eq!(value["ready"], false);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.request();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(observed);
}

/// A product unknown to the application runs through the real host without any
/// compiled product feature, controller import or product-specific match arm.
#[tokio::test]
async fn a_registered_workflow_runs_without_any_builtin_product() {
    use pir_pipeline::publisher::{
        Control, PublicationPass, PublisherDefinition, PublisherResources, PublisherService,
        ReadinessProbe, ReadinessRule, Workflow,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    struct FakePublication(Arc<AtomicUsize>);
    impl Workflow for FakePublication {
        type Error = pir_pipeline::publisher::Error;
        async fn step(&mut self) -> Result<Control, Self::Error> {
            PublicationPass::new(
                async { Ok::<_, Self::Error>(pir_pipeline::Input::Work(3)) },
                |work| async move { Ok(pir_pipeline::Input::Work(work + 1)) },
                |candidate| async move {
                    self.0.store(candidate, Ordering::SeqCst);
                    Ok(())
                },
            )
            .run(&mut ())
            .await?;
            Ok(Control::Continue)
        }
        async fn wait(&mut self) -> Result<(), Self::Error> {
            std::future::pending().await
        }
    }
    let management = port();
    let product = port();
    let published = Arc::new(AtomicUsize::new(0));
    let observed = published.clone();
    let definition = PublisherDefinition::new(
        "example_publisher",
        PublisherResources {
            listeners: vec![product],
            writable_paths: vec![],
            probes: vec![ReadinessProbe {
                address: product,
                path: "/product-ready",
                rule: ReadinessRule::HttpSuccess,
            }],
        },
        move |context| async move {
            let serving = tokio::spawn(
                context.http.serve(
                    product,
                    axum::Router::new()
                        .route("/product-ready", axum::routing::get(|| async { "ready" })),
                ),
            );
            let mut tasks = pir_pipeline::service::Tasks::default();
            tasks.own(&serving);
            PublisherService::new(FakePublication(published))
                .run(context.shutdown.cancelled())
                .await
        },
    );
    let shutdown = Shutdown::default();
    let task = tokio::spawn(pir_publisher::run_registered(
        management,
        vec![definition],
        shutdown.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let value = report(&format!("http://{management}/ready")).await;
            if value["ready"] == true {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(observed.load(Ordering::SeqCst), 4);
    shutdown.request();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::task::yield_now().await;
    tokio::net::TcpListener::bind(product).await.unwrap();
    tokio::net::TcpListener::bind(management).await.unwrap();
}
