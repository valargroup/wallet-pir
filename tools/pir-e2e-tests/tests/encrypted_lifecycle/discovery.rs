//! Publisher service discovery gates autonomous Enhance roles.
//!
//! The publisher stub serves only discovery; every product route is refused, so
//! no publication is prepared. The TLS terminator logs discovery and product
//! requests separately and applies `discovery-*` faults.
use super::encrypted::serve;
use pir_e2e_tests::enhance_fixture::tls;
use pir_query_server::enhance::{
    ingress_sync, router_sync, service::ServingRole, synchronization, worker_sync,
};
use pir_pipeline::consumer::polling::SyncTask;
use pir_sync_manager::DiscoveryError;
use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

const INCOMPATIBLE: &str = r#"{"service":"pir-publisher","version":"2.0.0"}"#;

/// Answer discovery compatibly `compatible` times, then as major version 2.
fn stub(compatible: usize) -> axum::Router {
    let answered = Arc::new(AtomicUsize::new(0));
    axum::Router::new().route(
        pir_service_discovery::PATH,
        axum::routing::get(move || {
            let answered = answered.clone();
            async move {
                if answered.fetch_add(1, Ordering::SeqCst) < compatible {
                    pir_service_discovery::Document::publisher().to_json()
                } else {
                    INCOMPATIBLE.as_bytes().to_vec()
                }
            }
        }),
    )
}

fn source(root: &Path, origin: &str) -> synchronization::Config {
    synchronization::Config {
        publisher_origin: origin.into(),
        trusted_ca: Some(root.join("ca.pem")),
        download_budget_bytes: 8 * 1024 * 1024,
        cache_limit_bytes: 8 * 1024 * 1024,
    }
}

fn free() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn lines(path: &Path) -> usize {
    std::fs::read_to_string(path).map_or(0, |log| log.lines().count())
}

/// Host JSON exercises the same tagged configuration boundary as the binary.
fn host_json(root: &Path, origin: &str, role: &str) -> serde_json::Value {
    let source = serde_json::to_value(source(root, origin)).unwrap();
    let mut synchronization = source.as_object().unwrap().clone();
    let assignment = match role {
        "worker" => ("shards", serde_json::json!([0])),
        "packing-router" => ("workers", serde_json::json!({"0": ["http://127.0.0.1:1"]})),
        "query-ingress" => (
            "routers",
            serde_json::json!({"0": [{"control_origin":"http://127.0.0.1:1", "query_origin":"http://127.0.0.1:2"}]}),
        ),
        _ => panic!("unsupported role"),
    };
    synchronization.insert(assignment.0.into(), assignment.1);
    let mut settings = serde_json::json!({"role": role, "data_dir": root.join("state"),
        "synchronization": synchronization});
    let mut host = serde_json::json!({"management_listen": free()});
    if role == "query-ingress" {
        host["public_listen"] = serde_json::to_value(free()).unwrap();
    } else {
        settings["listen"] = serde_json::to_value(free()).unwrap();
    }
    if role != "worker" {
        settings["control_listen"] = serde_json::to_value(free()).unwrap();
    }
    host["enhance"] = serde_json::json!({"enabled": true, "settings": settings});
    host
}

fn host(root: &Path, origin: &str, role: &str) -> pir_query_server::config::Config {
    serde_json::from_str(&host_json(root, origin, role).to_string()).unwrap()
}

#[test]
fn ingress_host_configuration_parses_domain_keys_and_rejects_invalid_assignments() {
    let root = Path::new("/unused");
    let mut json = host_json(root, "https://publisher.example", "query-ingress");
    let config = host(root, "https://publisher.example", "query-ingress");
    let role = config.enhance.unwrap().settings.unwrap();
    role.validate().unwrap();
    let ServingRole::QueryIngress(config) = role else {
        panic!("expected autonomous ingress");
    };
    assert_eq!(
        config.synchronization.routers[&0][0].control_origin,
        "http://127.0.0.1:1"
    );
    assert_eq!(
        config.synchronization.routers[&0][0].query_origin,
        "http://127.0.0.1:2"
    );
    for key in ["invalid", "18446744073709551616"] {
        json["enhance"]["settings"]["synchronization"]["routers"] = serde_json::json!({key: [{"control_origin":"http://127.0.0.1:1", "query_origin":"http://127.0.0.1:2"}]});
        assert!(
            serde_json::from_str::<pir_query_server::config::Config>(&json.to_string()).is_err()
        );
    }
    json["enhance"]["settings"]["synchronization"]["routers"] = serde_json::json!({});
    let empty: pir_query_server::config::Config = serde_json::from_str(&json.to_string()).unwrap();
    assert!(empty.enhance.unwrap().settings.unwrap().validate().is_err());
}

async fn run_host(config: pir_query_server::config::Config) -> pir_query_server::Result<()> {
    tokio::time::timeout(
        Duration::from_secs(30),
        pir_query_server::run(config, Default::default()),
    )
    .await
    .expect("host did not stop on a discovery refusal")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autonomous_roles_refuse_an_incompatible_or_missing_publisher_before_listening() {
    for role in ["worker", "packing-router", "query-ingress"] {
        for (fault, mismatch) in [
            (format!("discovery-body:{INCOMPATIBLE}"), true),
            ("discovery-missing".to_string(), false),
        ] {
            let scratch = tempfile::tempdir().unwrap();
            let upstream = serve(stub(usize::MAX)).await;
            let transport = tls(scratch.path(), &upstream.url).await;
            let control = scratch.path().join("fault");
            std::fs::write(&control, &fault).unwrap();
            let config = host(scratch.path(), &transport.origin, role);
            let error = run_host(config).await.unwrap_err();
            let discovery = error
                .downcast_ref::<DiscoveryError>()
                .unwrap_or_else(|| panic!("untyped refusal: {error}"));
            assert_eq!(discovery.incompatible().is_some(), mismatch, "{error}");
            assert_eq!(lines(&control.with_extension("discovery")), 1);
            assert_eq!(
                lines(&control.with_extension("requests")),
                0,
                "no product request"
            );
            assert!(!scratch.path().join("state").exists(), "no product state");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_later_major_mismatch_stops_the_whole_host_with_its_typed_error() {
    for role in ["worker", "packing-router", "query-ingress"] {
        let scratch = tempfile::tempdir().unwrap();
        // Startup accepts the publisher; the first pass sees major version 2.
        let upstream = serve(stub(1)).await;
        let transport = tls(scratch.path(), &upstream.url).await;
        let control = scratch.path().join("fault");
        let config = host(scratch.path(), &transport.origin, role);
        let error = run_host(config).await.unwrap_err();
        assert!(pir_sync_manager::is_incompatible(error.as_ref()), "{error}");
        assert!(
            scratch.path().join("state").exists(),
            "startup accepted the publisher"
        );
        assert_eq!(lines(&control.with_extension("discovery")), 2);
        assert_eq!(
            lines(&control.with_extension("requests")),
            0,
            "no product request"
        );
    }
}

/// Run one pass and report whether it reached the product endpoints and
/// whether its failure stops synchronization.
async fn pass<T: SyncTask<Error = pir_sync_manager::Error>>(
    task: &mut T,
    control: &Path,
) -> (bool, bool) {
    let before = lines(&control.with_extension("requests"));
    let error = task
        .synchronize()
        .await
        .err()
        .expect("refused product routes");
    (
        lines(&control.with_extension("requests")) > before,
        task.terminal(&error),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_pass_checks_discovery_and_retries_only_transient_failures() {
    let scratch = tempfile::tempdir().unwrap();
    let upstream = serve(stub(usize::MAX)).await;
    let transport = tls(scratch.path(), &upstream.url).await;
    let control = scratch.path().join("fault");
    let worker = worker_sync::Worker::open(
        &scratch.path().join("worker"),
        worker_sync::Config {
            source: source(scratch.path(), &transport.origin),
            shards: [0].into(),
        },
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let router = router_sync::PackingRouter::open(
        &scratch.path().join("router"),
        router_sync::Config {
            source: source(scratch.path(), &transport.origin),
            workers: [(0, vec!["http://127.0.0.1:1".into()])].into(),
            preferred: Default::default(),
        },
        1,
        1,
    )
    .unwrap();
    let ingress = ingress_sync::QueryIngress::open(
        &scratch.path().join("ingress"),
        ingress_sync::Config {
            source: source(scratch.path(), &transport.origin),
            routers: [(
                0,
                vec![ingress_sync::RouterPeer {
                    control_origin: "http://127.0.0.1:1".into(),
                    query_origin: "http://127.0.0.1:2".into(),
                }],
            )]
            .into(),
        },
        1,
        4,
    )
    .unwrap();
    let mut worker = worker.synchronizer().unwrap();
    let mut router = router.synchronizer().unwrap();
    let mut ingress = ingress.synchronizer().unwrap();
    for (fault, product, terminal) in [
        ("", true, false),
        // An unchanged publisher is checked again on the next pass.
        ("", true, false),
        ("discovery-unavailable", false, false),
        ("discovery-missing", false, false),
        (&*format!("discovery-body:{INCOMPATIBLE}"), false, true),
    ] {
        std::fs::write(&control, fault).unwrap();
        let discovered = lines(&control.with_extension("discovery"));
        assert_eq!(
            pass(&mut worker, &control).await,
            (product, terminal),
            "worker {fault:?}"
        );
        assert_eq!(
            pass(&mut router, &control).await,
            (product, terminal),
            "router {fault:?}"
        );
        assert_eq!(
            pass(&mut ingress, &control).await,
            (product, terminal),
            "ingress {fault:?}"
        );
        assert_eq!(lines(&control.with_extension("discovery")), discovered + 3);
    }
}

#[test]
fn worker_configuration_requires_its_publisher_source() {
    assert!(
        serde_json::from_value::<ServingRole>(serde_json::json!({"role": "worker",
        "listen": "127.0.0.1:1", "data_dir": "/unused"}))
        .is_err()
    );
}
