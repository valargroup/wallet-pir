//! The independent wallet library (zakura-pir-enhance, native feature) must
//! decode real answers through publisher HTTPS, autonomous workers, packing
//! routers and ingress: opened in this process, as a spawned host's combined
//! role, and as three spawned hosts running the distributed roles.
#![cfg(feature = "native-reinspiring")]
use axum::{
    extract::Request,
    routing::{get, post},
    Router,
};
use pir_e2e_tests::enhance_fixture::{Publication, System};
use zakura_pir_client::enhance as wallet;
#[path = "support/enhance_wallet.rs"]
mod enhance_wallet;
use enhance_wallet::{record, wallet_decodes};
#[path = "support/query_host.rs"]
mod query_host;
use query_host::{free, Host};

async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    (
        origin,
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wallet_library_decodes_native_two_mask_answers_over_the_distributed_path() {
    assert_eq!(
        wallet::PROTOCOL_REVISION,
        enhance_shared::protocol::PROTOCOL_REVISION
    );
    assert_eq!(
        wallet::parameter_id(32768).unwrap(),
        enhance_shared::protocol::parameter_id(32768).unwrap()
    );
    for rows in [2048, 4096, 8192] {
        assert_eq!(
            wallet::unit_parameter_id(rows).unwrap(),
            enhance_shared::protocol::unit_parameter_id(rows).unwrap()
        );
    }
    let records: u64 = 67;
    let anchor_hash = "01".repeat(32);
    let system =
        System::with_backend((0..records).map(record).collect(), Default::default(), 1, 2).await;
    let ingress_query = system.origin.clone();
    let mut tasks = Vec::new();
    // Minimal Caddy equivalent: every wallet route goes to query ingress.
    let query_origin = ingress_query.clone();
    let metadata_origin = ingress_query.clone();
    let proxy = Router::new()
        .route(
            "/v1/enhance/query",
            post(move |request: Request| {
                let origin = query_origin.clone();
                async move {
                    let body = axum::body::to_bytes(request.into_body(), 512 * 1024)
                        .await
                        .unwrap();
                    let response = reqwest::Client::new()
                        .post(format!("{origin}/v1/enhance/query"))
                        .header("x-forwarded-for", "203.0.113.7")
                        .body(body)
                        .send()
                        .await
                        .unwrap();
                    (response.status(), response.bytes().await.unwrap())
                }
            }),
        )
        .fallback(get(move |request: Request| {
            let origin = metadata_origin.clone();
            async move {
                let response = reqwest::get(format!("{origin}{}", request.uri()))
                    .await
                    .unwrap();
                (response.status(), response.bytes().await.unwrap())
            }
        }));
    let (public, task) = serve(proxy).await;
    tasks.push(task);
    wallet_decodes(&public, records, &anchor_hash).await;
    for task in tasks {
        task.abort();
    }
}

fn source(publication: &Publication) -> serde_json::Value {
    serde_json::to_value(&publication.source).unwrap()
}

async fn status(url: String) -> u16 {
    reqwest::get(url).await.unwrap().status().as_u16()
}

/// Enhance namespaces on the root wallet listener: both prefixes serve the
/// same manifest; private, management and disabled product routes are absent.
async fn public_surface(public: &str) {
    let canonical = reqwest::get(format!("{public}/enhance/v1/init"))
        .await
        .unwrap();
    assert_eq!(canonical.status(), 200);
    let canonical = canonical.bytes().await.unwrap();
    let deployed = reqwest::get(format!("{public}/v1/enhance/init"))
        .await
        .unwrap();
    assert_eq!(deployed.status(), 200);
    assert_eq!(deployed.bytes().await.unwrap(), canonical);
    for path in [
        "/internal/health",
        "/internal/metrics",
        "/internal/readiness",
        "/internal/evaluate",
        "/health",
        "/ready",
        "/metrics",
        "/v1/status/init",
        "/v2/shards",
        "/v2/txid/init",
    ] {
        assert_eq!(status(format!("{public}{path}")).await, 404, "{path}");
    }
}

/// A spawned host runs the combined Enhance role behind its root
/// `public_listen`; ingress, packing and evaluation call each other directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wallet_decodes_native_answers_from_a_spawned_combined_host() {
    let records: u64 = 67;
    let anchor_hash = "01".repeat(32);
    let publication = Publication::start(
        (0..records).map(record).collect(),
        enhance_shared::ACTIVATION_HEIGHT,
        anchor_hash.clone(),
    )
    .await;
    let dir = publication.root.path();
    let public = free();
    let control = free();
    let mut synchronization = source(&publication);
    synchronization["shards"] = serde_json::json!([0]);
    let mut host = Host::start(
        dir,
        "combined",
        serde_json::json!({
            "management_listen": free(),
            "public_listen": public,
            "enhance": {"enabled": true, "settings": {
                "role": "combined",
                "data_dir": dir.join("combined"),
                "control_listen": control,
                "synchronization": synchronization,
            }},
        }),
    )
    .await;
    host.ready().await;
    let public = format!("http://{public}");
    wallet_decodes(&public, records, &anchor_hash).await;
    public_surface(&public).await;
    // The private control listener keeps the role's health and metrics.
    assert_eq!(
        status(format!("http://{control}/internal/health")).await,
        200
    );
    assert_eq!(
        status(format!("http://{control}/internal/metrics")).await,
        200
    );
    host.stop().await;
}

/// Worker, packing router and query ingress each run in their own spawned
/// host and reach each other over their configured private listeners; only
/// the ingress host has a wallet listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wallet_decodes_native_answers_from_spawned_distributed_hosts() {
    let records: u64 = 67;
    let anchor_hash = "01".repeat(32);
    let publication = Publication::start(
        (0..records).map(record).collect(),
        enhance_shared::ACTIVATION_HEIGHT,
        anchor_hash.clone(),
    )
    .await;
    let dir = publication.root.path();
    let (worker, router, router_control, ingress_control, public) =
        (free(), free(), free(), free(), free());
    let mut worker_sync = source(&publication);
    worker_sync["shards"] = serde_json::json!([0]);
    let mut router_sync = source(&publication);
    router_sync["workers"] = serde_json::json!({"0": [format!("http://{worker}")]});
    let mut ingress_sync = source(&publication);
    ingress_sync["routers"] = serde_json::json!({"0": [{
        "control_origin": format!("http://{router_control}"),
        "query_origin": format!("http://{router}"),
    }]});
    let mut hosts = Vec::new();
    for (name, extra, settings) in [
        (
            "worker",
            serde_json::json!({}),
            serde_json::json!({"role": "worker", "listen": worker, "synchronization": worker_sync}),
        ),
        (
            "router",
            serde_json::json!({}),
            serde_json::json!({"role": "packing-router", "listen": router,
                "control_listen": router_control, "synchronization": router_sync}),
        ),
        (
            "ingress",
            serde_json::json!({"public_listen": public}),
            serde_json::json!({"role": "query-ingress", "control_listen": ingress_control,
                "synchronization": ingress_sync}),
        ),
    ] {
        let mut settings = settings;
        settings["data_dir"] = dir.join(name).to_str().unwrap().into();
        let mut document = serde_json::json!({
            "management_listen": free(),
            "enhance": {"enabled": true, "settings": settings},
        });
        if let Some(public) = extra.get("public_listen") {
            document["public_listen"] = public.clone();
        }
        hosts.push(Host::start(dir, name, document).await);
    }
    for host in &mut hosts {
        host.ready().await;
    }
    let public = format!("http://{public}");
    wallet_decodes(&public, records, &anchor_hash).await;
    public_surface(&public).await;
    // The packing router's query listener is private to ingress: it never
    // carries the wallet's manifest route.
    assert_eq!(
        status(format!("http://{router}/v1/enhance/init")).await,
        404
    );
    for host in hosts {
        host.stop().await;
    }
}

/// A host owns its state root until it has stopped: a second host configured
/// with the same root cannot open it while the first serves and does not
/// disturb it, and serves from it once the first has drained and exited.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_state_root_is_owned_until_its_host_has_stopped() {
    let records: u64 = 67;
    let anchor_hash = "01".repeat(32);
    let publication = Publication::start(
        (0..records).map(record).collect(),
        enhance_shared::ACTIVATION_HEIGHT,
        anchor_hash.clone(),
    )
    .await;
    let dir = publication.root.path();
    let state = dir.join("combined");
    let document = |public: &str| {
        let mut synchronization = source(&publication);
        synchronization["shards"] = serde_json::json!([0]);
        serde_json::json!({
            "management_listen": free(),
            "public_listen": public,
            "enhance": {"enabled": true, "settings": {
                "role": "combined",
                "data_dir": state,
                "control_listen": free(),
                "synchronization": synchronization,
            }},
        })
    };
    let (first_public, second_public) = (free(), free());
    let mut first = Host::start(dir, "first", document(&first_public)).await;
    first.ready().await;
    let first_origin = format!("http://{first_public}");
    assert!(state.join("worker").exists());
    wallet_decodes(&first_origin, records, &anchor_hash).await;

    // The second host's Enhance product fails to open the owned root: it is
    // isolated as failed, never ready, and serves no wallet generation.
    let refused = Host::start(dir, "refused", document(&second_public)).await;
    let management = refused.management.clone();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if let Ok(response) = reqwest::get(format!("http://{management}/health")).await {
                let report: serde_json::Value = response.json().await.unwrap_or_default();
                if report["products"]["enhance"]["phase"] == "failed" {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("second host took owned state: {}", refused.log()));
    assert!(refused.log().contains("already owned"), "{}", refused.log());
    assert_eq!(status(format!("http://{management}/ready")).await, 503);
    assert_ne!(
        status(format!("http://{second_public}/enhance/v1/init")).await,
        200
    );
    refused.stop().await;
    // The refused host left the serving host's view intact.
    wallet_decodes(&first_origin, records, &anchor_hash).await;

    let status = first.terminate().await;
    assert!(status.success(), "{status}");
    let mut second = Host::start(dir, "second", document(&second_public)).await;
    second.ready().await;
    wallet_decodes(&format!("http://{second_public}"), records, &anchor_hash).await;
    second.stop().await;
}
