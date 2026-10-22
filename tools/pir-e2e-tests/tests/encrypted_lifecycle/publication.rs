//! The real publisher distribution router, node RPC and TLS transport.
use super::encrypted::{record, serve};
use crate::publication_source::Published;
use enhance_publisher::{chain_source::ZakuraClient, distribution::PublicationDistribution};
use enhance_query_server::{
    protocol::{self, Geometry, Lifecycle, Manifest},
    publication::{AuthorityRequest, AuthorityResponse, Inventory, Object, Revocation, PREFIX},
};
use pir_e2e_tests::enhance_fixture::{tls, Tls};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

/// Every deployed-geometry case prepares, copies and hashes about 1 GB, so the
/// qualification lane runs them one at a time. The bound covers the shared cold
/// preparation (24-34s on 3 CPUs) plus the longest body measured there (60s).
/// Wall-clock time `offset_ms` after the response's source observation.
fn observed_at(response: &AuthorityResponse, offset_ms: i64) -> std::time::SystemTime {
    let at = response
        .source
        .observed_unix_ms
        .checked_add_signed(offset_ms)
        .unwrap();
    std::time::UNIX_EPOCH + Duration::from_millis(at)
}

const QUALIFICATION_BUDGET: Duration = Duration::from_secs(180);
static DEPLOYED: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    not(feature = "qualification-tests"),
    ignore = "deployed geometry: qualification-tests"
)]
async fn autonomous_ingress_keeps_wallet_bindings_during_independent_router_updates() {
    use enhance_query_server::{
        client::QuerySession, ingress_sync, protocol::ShardSession, router_sync, worker_sync,
        RECORD_BYTES,
    };
    let _serial = DEPLOYED.lock().await;
    tokio::time::timeout(QUALIFICATION_BUDGET, async {
        let fixture = published().await;
        let worker = worker_sync::Worker::open(
            &fixture.scratch.path().join("worker"),
            worker_sync::Config {
                source: fixture.source(),
                shards: [0, 1].into(),
            },
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let mut worker_sync = worker.synchronizer().unwrap();
        let worker_http = serve(worker.router()).await;
        let router_root = fixture.scratch.path().join("router");
        let router_config = router_sync::Config {
            source: fixture.source(),
            workers: [
                (0, vec![worker_http.url.clone()]),
                (1, vec![worker_http.url.clone()]),
            ]
            .into(),
            preferred: Default::default(),
        };
        let router =
            router_sync::PackingRouter::open(&router_root, router_config.clone(), 6, 4).unwrap();
        let mut router_sync = router.synchronizer().unwrap();
        let pause = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let released = Arc::new(tokio::sync::Notify::new());
        let pausing = pause.clone();
        let entering = entered.clone();
        let releasing = released.clone();
        let router_http = serve(router.public_router().merge(router.control_router()).layer(
            axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let pause = request.uri().path() == "/v1/enhance/query"
                        && pausing.swap(false, Ordering::AcqRel);
                    let entering = entering.clone();
                    let releasing = releasing.clone();
                    async move {
                        if pause {
                            entering.notify_one();
                            releasing.notified().await;
                        }
                        next.run(request).await
                    }
                },
            ),
        ))
        .await;
        let ingress_root = fixture.scratch.path().join("ingress");
        let ingress_config = ingress_sync::Config {
            source: fixture.source(),
            routers: [
                (
                    0,
                    vec![ingress_sync::RouterPeer {
                        control_origin: router_http.url.clone(),
                        query_origin: router_http.url.clone(),
                    }],
                ),
                (
                    1,
                    vec![ingress_sync::RouterPeer {
                        control_origin: router_http.url.clone(),
                        query_origin: router_http.url.clone(),
                    }],
                ),
            ]
            .into(),
        };
        let ingress =
            ingress_sync::QueryIngress::open(&ingress_root, ingress_config.clone(), 16, 256)
                .unwrap();
        assert!(
            ingress_sync::QueryIngress::open(&ingress_root, ingress_config.clone(), 16, 256)
                .is_err()
        );
        let mut ingress_sync = ingress.synchronizer().unwrap();
        let ingress_http = serve(ingress.public_router()).await;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        let init_url = format!("{}/v1/enhance/init", ingress_http.url);
        let endpoint = format!("{}/v1/enhance/query", ingress_http.url);
        ingress_sync.poll_once().await.unwrap();
        assert!(!http
            .get(&init_url)
            .send()
            .await
            .unwrap()
            .status()
            .is_success());
        worker_sync.poll_once().await.unwrap();
        ingress_sync.poll_once().await.unwrap();
        assert!(
            !http
                .get(&init_url)
                .send()
                .await
                .unwrap()
                .status()
                .is_success(),
            "workers alone do not authorize ingress"
        );
        router_sync.poll_once().await.unwrap();
        ingress_sync.poll_once().await.unwrap();
        let initial: Manifest = http
            .get(&init_url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(initial, fixture.manifest);
        let session: ShardSession = http
            .get(format!("{}/v1/enhance/sessions/1/0", ingress_http.url))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let session = QuerySession::new(&initial, session).unwrap();
        // An admitted query keeps an immutable five-second authority guard.
        // Hash and load the next generation first, so that only the router's
        // activation happens while the query is in flight.
        let mut advanced = initial.clone();
        advanced.generation += 1;
        fixture
            .distribution
            .publish(
                advanced.clone(),
                fixture.inventory.clone(),
                pir_publisher::publisher::PublisherEngine,
            )
            .await
            .unwrap();
        worker_sync.poll_once().await.unwrap();
        let (query, slot) = session.prepare_position(66).unwrap();
        let running_http = http.clone();
        let running_endpoint = endpoint.clone();
        let body = query.body().to_vec();
        pause.store(true, Ordering::Release);
        let in_flight = tokio::spawn(async move {
            running_http
                .post(running_endpoint)
                .body(body)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .bytes()
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        router_sync.poll_once().await.unwrap();
        let before: Manifest = http
            .get(&init_url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(before, initial, "ingress advertises its own ready view");
        // This reaches a router that already advanced: the ingress promise must
        // retain the old view before the router has admitted the query itself.
        let (old_query, old_slot) = session.prepare_position(0).unwrap();
        let answer = http
            .post(&endpoint)
            .body(old_query.body().to_vec())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let row = session.decode(old_query, &answer).unwrap();
        assert_eq!(
            &row[old_slot * RECORD_BYTES..(old_slot + 1) * RECORD_BYTES],
            record(0)
        );
        ingress_sync.poll_once().await.unwrap();
        let after: Manifest = http
            .get(&init_url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(after, advanced);
        released.notify_one();
        let row = session.decode(query, &in_flight.await.unwrap()).unwrap();
        assert_eq!(
            &row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES],
            record(66)
        );
        let (stale, _) = session.prepare_position(0).unwrap();
        assert_eq!(
            http.post(&endpoint)
                .body(stale.body().to_vec())
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::CONFLICT
        );

        let session_doc: ShardSession = http
            .get(format!(
                "{}/v1/enhance/session/{}",
                ingress_http.url,
                hex::encode(advanced.session_id(0).unwrap())
            ))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let session = QuerySession::new(&advanced, session_doc).unwrap();
        let (query, slot) = session.prepare_position(33).unwrap();
        let answer = http
            .post(&endpoint)
            .body(query.body().to_vec())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let row = session.decode(query, &answer).unwrap();
        assert_eq!(
            &row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES],
            record(33)
        );

        // Advance past the retained-generation limit. A released promise must
        // free downstream capacity even while its old source lease is young.
        let mut latest = advanced.clone();
        for _ in 0..6 {
            latest.generation += 1;
            fixture
                .distribution
                .publish(
                    latest.clone(),
                    fixture.inventory.clone(),
                    pir_publisher::publisher::PublisherEngine,
                )
                .await
                .unwrap();
            for _ in 0..3 {
                worker_sync.poll_once().await.unwrap();
                router_sync.poll_once().await.unwrap();
                ingress_sync.poll_once().await.unwrap();
            }
            let current: Manifest = http
                .get(&init_url)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(
                current, latest,
                "released promises must allow continued publication"
            );
        }

        let mut session = session;
        session.rebind(&latest).unwrap();

        let router_address = router_http
            .url
            .strip_prefix("http://")
            .unwrap()
            .parse()
            .unwrap();
        router_http.stop().await;
        drop(router_sync);
        drop(router);
        let router = router_sync::PackingRouter::open(&router_root, router_config, 6, 4).unwrap();
        let mut router_sync = router.synchronizer().unwrap();
        router_sync.poll_once().await.unwrap();
        let _router_http = super::encrypted::serve_at(
            router.public_router().merge(router.control_router()),
            router_address,
        )
        .await;
        let (query, _) = session.prepare_position(0).unwrap();
        assert!(
            !http
                .post(&endpoint)
                .body(query.body().to_vec())
                .send()
                .await
                .unwrap()
                .status()
                .is_success(),
            "ingress must learn the new router incarnation"
        );
        ingress_sync.poll_once().await.unwrap();
        let (query, slot) = session.prepare_position(0).unwrap();
        let answer = http
            .post(&endpoint)
            .body(query.body().to_vec())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let row = session.decode(query, &answer).unwrap();
        assert_eq!(
            &row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES],
            record(0)
        );

        ingress_http.stop().await;
        drop(ingress_sync);
        drop(ingress);
        let ingress =
            ingress_sync::QueryIngress::open(&ingress_root, ingress_config, 16, 256).unwrap();
        let mut ingress_sync = ingress.synchronizer().unwrap();
        let listener = serve(ingress.public_router()).await;
        let init_url = format!("{}/v1/enhance/init", listener.url);
        assert!(
            !http
                .get(&init_url)
                .send()
                .await
                .unwrap()
                .status()
                .is_success(),
            "cached metadata cannot authorize ingress restart"
        );
        ingress_sync.poll_once().await.unwrap();
        assert!(http
            .get(&init_url)
            .send()
            .await
            .unwrap()
            .status()
            .is_success());
        fixture
            .distribution
            .withdraw(&Revocation::default())
            .unwrap();
        assert!(ingress_sync.poll_once().await.is_err());
        assert!(!http
            .get(&init_url)
            .send()
            .await
            .unwrap()
            .status()
            .is_success());
    })
    .await
    .expect("autonomous ingress wallet flow exceeded qualification budget");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    not(feature = "qualification-tests"),
    ignore = "deployed geometry: qualification-tests"
)]
async fn invalidated_router_releases_obsolete_packing_before_loading_recovery() {
    use enhance_query_server::{router_sync, worker_sync};
    let _serial = DEPLOYED.lock().await;
    tokio::time::timeout(QUALIFICATION_BUDGET, async {
        let fixture = published().await;
        let worker = worker_sync::Worker::open(
            &fixture.scratch.path().join("worker"),
            worker_sync::Config {
                source: fixture.source(),
                shards: [0].into(),
            },
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let mut worker_sync = worker.synchronizer().unwrap();
        let worker_http = serve(worker.router()).await;
        worker_sync.poll_once().await.unwrap();
        let router = router_sync::PackingRouter::open(
            &fixture.scratch.path().join("router"),
            router_sync::Config {
                source: fixture.source(),
                workers: [(0, vec![worker_http.url.clone()])].into(),
                preferred: Default::default(),
            },
            1,
            1,
        )
        .unwrap();
        let mut router_sync = router.synchronizer().unwrap();
        let control = serve(router.control_router()).await;
        let http = enhance_query_server::internal_auth::client_builder()
            .build()
            .unwrap();
        let health = format!("{}/internal/health", control.url);
        router_sync.poll_once().await.unwrap();
        let before: serde_json::Value = http
            .get(&health)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(before["packing_charged_bytes"].as_u64().unwrap() > 0);
        fixture
            .distribution
            .withdraw(&Revocation {
                recovery_epoch: 1,
                sessions: [hex::encode(fixture.manifest.session_id(0).unwrap())].into(),
            })
            .unwrap();
        assert!(router_sync.poll_once().await.is_err());
        let after: serde_json::Value = http
            .get(&health)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(after["active"], serde_json::Value::Null);
        assert_eq!(
            after["packing_charged_bytes"], 0,
            "obsolete active ownership must not prevent recovery admission"
        );
        let mut inventory = fixture.inventory.clone();
        for domain in &mut inventory.domains {
            for unit in &mut domain.plan.units {
                unit.recovery_epoch = 1;
            }
        }
        let mut recovery = fixture.manifest.clone();
        recovery.generation += 1;
        recovery.recovery_epoch = 1;
        recovery.domain_recovery_epochs.insert(0, "1".into());
        recovery
            .unit_identities
            .insert(0, inventory.domains[0].plan.units.clone());
        fixture
            .distribution
            .publish(
                recovery.clone(),
                inventory,
                pir_publisher::publisher::PublisherEngine,
            )
            .await
            .unwrap();
        worker_sync.poll_once().await.unwrap();
        router_sync.poll_once().await.unwrap();
        let after: serde_json::Value = http
            .get(&health)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(after["active"], protocol::digest(&recovery));
        assert!(after["packing_charged_bytes"].as_u64().unwrap() > 0);
    })
    .await
    .expect("router recovery exceeded qualification budget");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    not(feature = "qualification-tests"),
    ignore = "deployed geometry: qualification-tests"
)]
async fn autonomous_router_waits_for_workers_and_keeps_admitted_query_bound_during_activation() {
    use enhance_query_server::{
        client::QuerySession, protocol::ShardSession, router_sync, worker_sync, RECORD_BYTES,
    };
    let _serial = DEPLOYED.lock().await;
    tokio::time::timeout(QUALIFICATION_BUDGET, async {
        let fixture = published().await;
        let worker_root = fixture.scratch.path().join("worker");
        let worker_config = worker_sync::Config {
            source: fixture.source(),
            shards: [0, 1].into(),
        };
        let worker = worker_sync::Worker::open(
            &worker_root,
            worker_config.clone(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let mut worker_sync = worker.synchronizer().unwrap();
        let pause = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let released = Arc::new(tokio::sync::Notify::new());
        // A loaded candidate whose readiness is withheld, as an unloaded one
        // would answer, so activation can happen at a chosen moment.
        let withheld = Arc::new(std::sync::Mutex::new(None::<String>));
        let pausing = pause.clone();
        let entering = entered.clone();
        let releasing = released.clone();
        let withholding = withheld.clone();
        let worker_http = serve(worker.clone().router().layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let should_pause = request.uri().path() == "/internal/evaluate"
                    && pausing.swap(false, Ordering::AcqRel);
                let withheld = (request.uri().path() == "/internal/readiness")
                    .then(|| withholding.lock().unwrap().clone())
                    .flatten();
                let entering = entering.clone();
                let releasing = releasing.clone();
                async move {
                    use axum::response::IntoResponse;
                    let request = if let Some(withheld) = withheld {
                        let (parts, body) = request.into_parts();
                        let bytes = axum::body::to_bytes(body, 1 << 20).await.unwrap();
                        let readiness: worker_sync::ReadinessRequest =
                            serde_json::from_slice(&bytes).unwrap();
                        if readiness.manifest_sha256 == withheld {
                            return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
                        }
                        axum::extract::Request::from_parts(parts, axum::body::Body::from(bytes))
                    } else {
                        request
                    };
                    let response = next.run(request).await;
                    if should_pause {
                        entering.notify_one();
                        releasing.notified().await;
                    }
                    response
                }
            },
        )))
        .await;
        let router = router_sync::PackingRouter::open(
            &fixture.scratch.path().join("router"),
            router_sync::Config {
                source: fixture.source(),
                workers: [
                    (0, vec![worker_http.url.clone()]),
                    (1, vec![worker_http.url.clone()]),
                ]
                .into(),
                preferred: Default::default(),
            },
            6,
            4,
        )
        .unwrap();
        let mut router_sync = router.synchronizer().unwrap();
        let public = serve(router.public_router()).await;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        let session_url = format!(
            "{}/v1/enhance/session/{}",
            public.url,
            hex::encode(fixture.manifest.session_id(0).unwrap())
        );
        router_sync.poll_once().await.unwrap();
        assert!(
            !http
                .get(&session_url)
                .send()
                .await
                .unwrap()
                .status()
                .is_success(),
            "publisher finalization does not imply worker readiness"
        );
        worker_sync.poll_once().await.unwrap();
        router_sync.poll_once().await.unwrap();
        let session: ShardSession = http
            .get(&session_url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let session = QuerySession::new(&fixture.manifest, session).unwrap();
        // An admitted query keeps an immutable five-second authority guard.
        // Hash and load the next generation first, so that only activation of
        // the prepared candidate happens while the query is in flight.
        let mut advanced = fixture.manifest.clone();
        advanced.generation += 1;
        fixture
            .distribution
            .publish(
                advanced.clone(),
                fixture.inventory.clone(),
                pir_publisher::publisher::PublisherEngine,
            )
            .await
            .unwrap();
        router_sync.poll_once().await.unwrap();
        let before: ShardSession = http
            .get(&session_url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            before.generation, 1,
            "router keeps its old view while the worker loads"
        );
        *withheld.lock().unwrap() = Some(protocol::digest(&advanced));
        worker_sync.poll_once().await.unwrap();
        router_sync.poll_once().await.unwrap();
        let staged: ShardSession = http
            .get(&session_url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            staged.generation, 1,
            "router keeps its old view until the worker reports readiness"
        );
        let (query, slot) = session.prepare_position(66).unwrap();
        pause.store(true, Ordering::Release);
        let endpoint = format!("{}/v1/enhance/query", public.url);
        let body = query.body().to_vec();
        let running_http = http.clone();
        let running_endpoint = endpoint.clone();
        let in_flight = tokio::spawn(async move {
            running_http
                .post(running_endpoint)
                .body(body)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .bytes()
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        withheld.lock().unwrap().take();
        router_sync.poll_once().await.unwrap();
        released.notify_one();
        let row = session.decode(query, &in_flight.await.unwrap()).unwrap();
        assert_eq!(
            &row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES],
            record(66)
        );
        let session: ShardSession = http
            .get(&session_url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(session.generation, 2);
        let session = QuerySession::new(&advanced, session).unwrap();
        let (query, slot) = session.prepare_position(33).unwrap();
        let response = http
            .post(&endpoint)
            .body(query.body().to_vec())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let row = session.decode(query, &response).unwrap();
        assert_eq!(
            &row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES],
            record(33)
        );
        let worker_address = worker_http
            .url
            .strip_prefix("http://")
            .unwrap()
            .parse()
            .unwrap();
        worker_http.stop().await;
        drop(worker_sync);
        drop(worker);
        let worker = worker_sync::Worker::open(
            &worker_root,
            worker_config,
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let mut worker_sync = worker.synchronizer().unwrap();
        worker_sync.poll_once().await.unwrap();
        let _worker_http = super::encrypted::serve_at(worker.router(), worker_address).await;
        let (query, _) = session.prepare_position(0).unwrap();
        assert!(
            !http
                .post(&endpoint)
                .body(query.body().to_vec())
                .send()
                .await
                .unwrap()
                .status()
                .is_success(),
            "a changed worker process needs fresh readiness"
        );
        router_sync.poll_once().await.unwrap();
        let (query, slot) = session.prepare_position(0).unwrap();
        let response = http
            .post(&endpoint)
            .body(query.body().to_vec())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let row = session.decode(query, &response).unwrap();
        assert_eq!(
            &row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES],
            record(0)
        );
    })
    .await
    .expect("autonomous router handover exceeded qualification budget");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    not(feature = "qualification-tests"),
    ignore = "deployed geometry: qualification-tests"
)]
async fn autonomous_worker_pulls_rows_and_answers_after_update_and_restart() {
    use enhance_query_server::{
        client::QuerySession,
        prepared_packing,
        protocol::{digest, QueryBinding},
        synchronization::Config as SourceConfig,
        worker::{Evaluate, Intermediate},
        worker_sync::{self, Readiness, ReadinessRequest},
        PackingBudget, RECORD_BYTES,
    };
    let _serial = DEPLOYED.lock().await;
    tokio::time::timeout(QUALIFICATION_BUDGET, async {
        let scratch = tempfile::tempdir().unwrap();
        let root = scratch.path().join("publication");
        let node = serve(axum::Router::new().route("/", axum::routing::post(|axum::Json(request): axum::Json<serde_json::Value>| async move {
            axum::Json(serde_json::json!({"id": request["id"], "error": null, "result": "01".repeat(32)}))
        }))).await;
        let cookie = scratch.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let distribution = PublicationDistribution::open(&root, ZakuraClient::from_cookie_file(&node.url, cookie).unwrap()).unwrap();
        let upstream = serve(pir_e2e_tests::enhance_fixture::distribution_router(&distribution)).await;
        let transport = tls(scratch.path(), &upstream.url).await;
        let preparing = root.clone();
        let inventory = tokio::task::spawn_blocking(move || prepare_publication(&preparing, 67)).await.unwrap().inventory().clone();
        let domain = &inventory.domains[0];
        let manifest = Manifest {
            recovery_epoch: 0, placement_revision: 1, domain_recovery_epochs: [(0, "0".into())].into(),
            schema_version: protocol::SCHEMA_VERSION, protocol_revision: protocol::PROTOCOL_REVISION.into(),
            network: "main".into(), pool: "ironwood".into(), generation: 1, anchor_height: 3428143,
            anchor_block_hash: "01".repeat(32), geometry: Geometry::default(), coverage: inventory.coverage.clone(),
            sessions: vec![domain.reference().unwrap()], unit_identities: [(0, domain.plan.units.clone())].into(),
        };
        distribution.publish(manifest.clone(), inventory.clone(), pir_publisher::publisher::PublisherEngine).await.unwrap();
        let config = worker_sync::Config { source: SourceConfig { publisher_origin: transport.origin.clone(),
            trusted_ca: Some(scratch.path().join("ca.pem")), download_budget_bytes: 128 * 1024 * 1024,
            cache_limit_bytes: 128 * 1024 * 1024 }, shards: [0].into() };
        let worker_root = scratch.path().join("worker");
        let worker = worker_sync::Worker::open(&worker_root, config.clone(), Default::default(), Default::default()).unwrap();
        assert!(worker_sync::Worker::open(&worker_root, config.clone(), Default::default(), Default::default()).is_err());
        let mut synchronizer = worker.synchronizer().unwrap();
        assert!(worker.synchronizer().is_err(), "only one synchronization pass owner may open a worker");
        let http = enhance_query_server::internal_auth::client_builder().build().unwrap();
        let listener = serve(worker.clone().router()).await;
        let readiness = format!("{}/internal/readiness", listener.url);
        let request = ReadinessRequest { manifest_sha256: digest(&manifest), holder: None };
        assert!(!http.post(&readiness).json(&request).send().await.unwrap().status().is_success());
        for fault in ["corrupt", "truncated", "missing"] {
            std::fs::write(scratch.path().join("fault"), fault).unwrap();
            assert!(synchronizer.poll_once().await.is_err(), "damaged source must not activate");
            // A failed transfer leaves no partial file: only promoted digest names remain.
            assert!(std::fs::read_dir(worker_root.join("objects")).unwrap().all(|entry| protocol::canonical_hash(&entry.unwrap().file_name().to_string_lossy())));
        }
        std::fs::write(scratch.path().join("fault"), "").unwrap();
        synchronizer.poll_once().await.unwrap();
        let ready: Readiness = http.post(&readiness).json(&request).send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
        assert_eq!(ready.shards, [0].into());
        assert_eq!(ready.manifest, manifest);
        assert!(!worker_root.join("artifacts-9").exists(), "worker must not generate hint artifacts");
        let requests = std::fs::read_to_string(scratch.path().join("fault.requests")).unwrap();
        for object in [&domain.hint, &domain.public, &domain.packing_object()] {
            assert!(!requests.contains(&object.sha256), "worker must download only its rows");
        }
        let packing = prepared_packing::load(&domain.packing_object().path(&root).unwrap(), &domain.packing,
            domain.plan.shard.logical_rows, &PackingBudget::coordinator()).unwrap();
        for position in [0, 32, 33, 66] {
            let session = QuerySession::new(&manifest, packing.session(&manifest, 0)).unwrap();
            let (query, slot) = session.prepare_position(position).unwrap();
            let binding = QueryBinding::decode(query.body()).unwrap();
            let request = Evaluate { binding: Some(binding.encode()), session_id: hex::encode(binding.session_id),
                generation: manifest.generation, shard_id: 0, epoch: hex::encode(binding.epoch),
                coefficients: packing.query_coefficients(query.body(), binding, session.query_rows()).unwrap() };
            if position == 0 {
                let mut malformed = request.clone();
                malformed.coefficients.clear();
                assert_eq!(http.post(format!("{}/internal/evaluate", listener.url)).json(&malformed).send().await.unwrap().status(), reqwest::StatusCode::BAD_REQUEST);
            }
            let intermediate: Intermediate = http.post(format!("{}/internal/evaluate", listener.url)).json(&request).send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
            let answer = packing.pack(query.body(), &intermediate.coefficients, session.query_rows()).unwrap();
            let row = session.decode(query, &answer).unwrap();
            assert_eq!(&row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES], record(position));
        }
        // A shallow withdrawal does not revoke unchanged material, but it does
        // invalidate authorization observed before that withdrawal.
        let safety_root = scratch.path().join("independent-consumer");
        let safety = enhance_query_server::synchronization::Safety::open(&safety_root, &transport.origin).unwrap();
        let current_url = format!("{}{PREFIX}current", transport.origin);
        let initial: AuthorityResponse = transport.client.get(&current_url).send().await.unwrap().json().await.unwrap();
        let old_lease = enhance_query_server::synchronization::Lease::from_response(initial.clone(), &manifest, &safety).unwrap();
        distribution.withdraw(&Revocation::default()).unwrap();
        distribution.publish(manifest.clone(), inventory.clone(), pir_publisher::publisher::PublisherEngine).await.unwrap();
        let republished: AuthorityResponse = transport.client.get(&current_url).send().await.unwrap().json().await.unwrap();
        assert_eq!(republished.current.recovery_epoch, initial.current.recovery_epoch);
        assert_eq!(republished.current.revoked, initial.current.revoked);
        assert!(republished.current.withdrawal_revision > initial.current.withdrawal_revision);
        safety.observe(&republished.current).unwrap();
        assert!(old_lease.check(&manifest, &safety).is_err());
        let authorized: AuthorityResponse = transport.client.post(format!("{}{PREFIX}authority", transport.origin))
            .json(&AuthorityRequest { manifest_sha256: digest(&manifest) }).send().await.unwrap().json().await.unwrap();
        enhance_query_server::synchronization::Lease::from_response(authorized, &manifest, &safety).unwrap().check(&manifest, &safety).unwrap();
        synchronizer.poll_once().await.unwrap();
        let mut advanced = manifest.clone();
        advanced.generation += 1;
        distribution.publish(advanced.clone(), inventory.clone(), pir_publisher::publisher::PublisherEngine).await.unwrap();
        let before = std::fs::read_to_string(scratch.path().join("fault.requests")).unwrap();
        synchronizer.poll_once().await.unwrap();
        let after = std::fs::read_to_string(scratch.path().join("fault.requests")).unwrap();
        assert!(!after[before.len()..].contains("/objects/"), "unchanged units are reused");
        let advanced_request = ReadinessRequest { manifest_sha256: digest(&advanced), holder: None };
        assert!(http.post(&readiness).json(&advanced_request).send().await.unwrap().status().is_success());
        let old_incarnation = worker.incarnation.clone();
        listener.stop().await;
        drop(synchronizer);
        drop(worker);
        let restarted = worker_sync::Worker::open(&worker_root, config, Default::default(), Default::default()).unwrap();
        assert_ne!(old_incarnation, restarted.incarnation);
        let listener = serve(restarted.clone().router()).await;
        let readiness = format!("{}/internal/readiness", listener.url);
        assert!(!http.post(&readiness).json(&advanced_request).send().await.unwrap().status().is_success(), "cached rows do not authorize restart");
        let mut synchronizer = restarted.synchronizer().unwrap();
        synchronizer.poll_once().await.unwrap();
        assert!(http.post(&readiness).json(&advanced_request).send().await.unwrap().status().is_success());
        distribution.withdraw(&Revocation { recovery_epoch: 1, sessions: [hex::encode(manifest.session_id(0).unwrap())].into() }).unwrap();
        assert!(synchronizer.poll_once().await.is_err());
        assert!(!http.post(&readiness).json(&advanced_request).send().await.unwrap().status().is_success(), "revocation closes loaded material");
    }).await.expect("autonomous worker lifecycle exceeded qualification budget");
}

async fn check_object(tls: &Tls, path: &str, object: &Object) {
    let mut response = tls
        .client
        .get(format!("{}{PREFIX}{path}/{}", tls.origin, object.sha256))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let mut bytes = 0u64;
    let mut hash = Sha256::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        bytes += chunk.len() as u64;
        assert!(bytes <= object.bytes);
        hash.update(&chunk);
    }
    assert_eq!(bytes, object.bytes);
    assert_eq!(hex::encode(hash.finalize()), object.sha256);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    not(feature = "qualification-tests"),
    ignore = "deployed geometry: qualification-tests"
)]
async fn complete_publication_crosses_tls_without_consumers_and_source_loss_cannot_renew_authority()
{
    let _serial = DEPLOYED.lock().await;
    tokio::time::timeout(QUALIFICATION_BUDGET, async {
        let scratch = tempfile::tempdir().unwrap();
        let root = scratch.path().join("publication");
        let canonical = Arc::new(AtomicBool::new(true));
        let node_state = canonical.clone();
        let pause = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let node_pause = pause.clone();
        let node_entered = entered.clone();
        let node_release = release.clone();
        let node = serve(axum::Router::new().route("/", axum::routing::post(move |axum::Json(request): axum::Json<serde_json::Value>| {
            let node_state = node_state.clone();
            let node_pause = node_pause.clone();
            let node_entered = node_entered.clone();
            let node_release = node_release.clone();
            async move {
                assert_eq!(request["method"], "getblockhash");
                if node_pause.swap(false, Ordering::AcqRel) {
                    node_entered.notify_one();
                    node_release.notified().await;
                }
                axum::Json(serde_json::json!({"id": request["id"], "error": null, "result": if node_state.load(Ordering::Acquire) { "01".repeat(32) } else { "02".repeat(32) }}))
            }
        }))).await;
        let cookie = scratch.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let rpc = ZakuraClient::from_cookie_file(&node.url, cookie).unwrap();
        let distribution = PublicationDistribution::open(&root, rpc.clone()).unwrap();
        let upstream = serve(pir_e2e_tests::enhance_fixture::distribution_router(&distribution)).await;
        let transport = tls(scratch.path(), &upstream.url).await;
        let current_url = format!("{}{PREFIX}current", transport.origin);
        let before: AuthorityResponse = transport.client.get(&current_url).send().await.unwrap().json().await.unwrap();
        assert!(!before.current.eligible);
        assert_eq!(before.source.observed_unix_ms, 0);
        let prepare_root = root.clone();
        let inventory = tokio::task::spawn_blocking(move || prepare_publication(&prepare_root, 67)).await.unwrap().inventory().clone();
        let domain = &inventory.domains[0];
        let manifest = Manifest {
            recovery_epoch: 0, placement_revision: 1, domain_recovery_epochs: [(0, "0".into())].into(),
            schema_version: protocol::SCHEMA_VERSION, protocol_revision: protocol::PROTOCOL_REVISION.into(),
            network: "main".into(), pool: "ironwood".into(), generation: 1, anchor_height: 3428143,
            anchor_block_hash: "01".repeat(32), geometry: Geometry::default(), coverage: inventory.coverage.clone(),
            sessions: vec![domain.reference().unwrap()], unit_identities: [(0, domain.plan.units.clone())].into(),
        };
        distribution.publish(manifest.clone(), inventory.clone(), pir_publisher::publisher::PublisherEngine).await.unwrap();
        let current: AuthorityResponse = transport.client.get(&current_url).send().await.unwrap().json().await.unwrap();
        assert!(current.current.eligible);
        assert!(current.current.succeeds(&before.current));
        assert_eq!(current.current.withdrawal_revision, before.current.withdrawal_revision);
        assert_ne!(current.source.observed_unix_ms, 0);
        current.remaining_at(observed_at(&current, 0)).unwrap();
        current.remaining_at(observed_at(&current, 5000)).unwrap();
        assert!(current.remaining_at(observed_at(&current, 5001)).is_err());
        assert!(current.remaining_at(observed_at(&current, -1)).is_err());
        let mut replay = current.current.clone();
        replay.revision -= 1;
        assert!(!replay.succeeds(&current.current));
        replay = current.current.clone();
        replay.version += 1;
        assert!(replay.validate().is_err());
        pause.store(true, Ordering::Release);
        let delayed_client = transport.client.clone();
        let delayed_url = current_url.clone();
        let delayed = tokio::spawn(async move {
            delayed_client.get(delayed_url).send().await.unwrap().json::<AuthorityResponse>().await.unwrap()
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified()).await.unwrap();
        let observed_before_delay = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        tokio::time::sleep(Duration::from_millis(30)).await;
        release.notify_one();
        assert!(delayed.await.unwrap().source.observed_unix_ms <= observed_before_delay, "source response delay cannot renew the observation time");
        let descriptor = current.current.inventory.as_ref().unwrap();
        check_object(&transport, "inventories", descriptor).await;
        let fetched: Inventory = transport.client.get(format!("{}{PREFIX}inventories/{}", transport.origin, descriptor.sha256)).send().await.unwrap().json().await.unwrap();
        assert!(fetched.supports(&manifest).unwrap());
        for domain in &fetched.domains {
            for object in domain.rows.iter().chain([&domain.hint, &domain.public, &domain.packing_object()]) {
                check_object(&transport, "objects", object).await;
            }
        }
        let mut broken = inventory.clone();
        broken.domains[0].rows[0].bytes -= 1;
        let mut later = manifest.clone();
        later.generation += 1;
        assert!(distribution.publish(later, broken, pir_publisher::publisher::PublisherEngine).await.is_err());
        let unchanged: AuthorityResponse = transport.client.get(&current_url).send().await.unwrap().json().await.unwrap();
        assert_eq!(current.current, unchanged.current, "failed publication cannot change current");
        canonical.store(false, Ordering::Release);
        assert_eq!(transport.client.get(&current_url).header("If-None-Match", "unchanged").send().await.unwrap().status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        canonical.store(true, Ordering::Release);
        let request = AuthorityRequest { manifest_sha256: protocol::digest(&manifest) };
        let authority_url = format!("{}{PREFIX}authority", transport.origin);
        assert!(transport.client.post(&authority_url).json(&request).send().await.unwrap().status().is_success());
        pause.store(true, Ordering::Release);
        let pending_distribution = distribution.clone();
        let mut pending_manifest = manifest.clone();
        pending_manifest.generation += 1;
        let pending_inventory = inventory.clone();
        let publication = tokio::spawn(async move {
            pending_distribution.publish(pending_manifest, pending_inventory, pir_publisher::publisher::PublisherEngine).await
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified()).await.unwrap();
        distribution.withdraw(&Revocation::default()).unwrap();
        release.notify_one();
        assert!(publication.await.unwrap().is_err(), "an in-flight publication cannot undo a source withdrawal");
        let withdrawn: AuthorityResponse = transport.client.get(&current_url).send().await.unwrap().json().await.unwrap();
        assert!(!withdrawn.current.eligible);
        assert_eq!(withdrawn.source.observed_unix_ms, 0);
        distribution.publish(manifest.clone(), inventory.clone(), pir_publisher::publisher::PublisherEngine).await.unwrap();
        distribution.withdraw(&Revocation { recovery_epoch: 1, sessions: [hex::encode(manifest.session_id(0).unwrap())].into() }).unwrap();
        assert_eq!(transport.client.post(&authority_url).json(&request).send().await.unwrap().status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert!(distribution.publish(manifest, inventory, pir_publisher::publisher::PublisherEngine).await.is_err());
        upstream.stop().await;
        drop(distribution);
        let reopened = PublicationDistribution::open(&root, rpc).unwrap();
        let listener = serve(reopened.router()).await;
        let restored: AuthorityResponse = reqwest::get(format!("{}{PREFIX}current", listener.url)).await.unwrap().json().await.unwrap();
        assert!(!restored.current.eligible);
        assert!(restored.current.withdrawal_revision > current.current.withdrawal_revision);
        assert_eq!(restored.current.recovery_epoch, 1);
        assert_eq!(restored.current.revoked.len(), 1);
        assert_eq!(restored.source.observed_unix_ms, 0);
        assert!(!current.current.succeeds(&restored.current), "restart fence cannot replay the predecessor");
    }).await.expect("complete Enhance TLS publication exceeded qualification budget");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    not(feature = "qualification-tests"),
    ignore = "deployed geometry: qualification-tests"
)]
async fn collection_keeps_retained_material_and_open_files_and_removes_interrupted_candidates() {
    use std::io::Read;
    let _serial = DEPLOYED.lock().await;
    tokio::time::timeout(QUALIFICATION_BUDGET, async {
        let scratch = tempfile::tempdir().unwrap();
        let root = scratch.path().join("publication");
        let node = serve(axum::Router::new().route("/", axum::routing::post(
            |axum::Json(request): axum::Json<serde_json::Value>| async move {
                axum::Json(serde_json::json!({"id": request["id"], "error": null, "result": "01".repeat(32)}))
            }
        ))).await;
        let cookie = scratch.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let distribution = PublicationDistribution::open(&root, ZakuraClient::from_cookie_file(&node.url, cookie).unwrap()).unwrap();
        let prepare_root = root.clone();
        let initial = tokio::task::spawn_blocking(move || prepare_publication(&prepare_root, 67)).await.unwrap().inventory().clone();
        let orphan = hex::encode(Sha256::digest(b"interrupted candidate"));
        let orphan_path = root.join("objects").join(orphan);
        std::fs::write(&orphan_path, b"interrupted candidate").unwrap();
        let partial_path = root.join("objects").join(format!("{}.tmp", "ab".repeat(32)));
        std::fs::write(&partial_path, b"unfinished write").unwrap();
        let mut pinned = std::fs::File::open(&orphan_path).unwrap();
        std::fs::write(root.join("objects/operator-note"), b"not collector-owned").unwrap();
        for epoch in 0..7 {
            let mut inventory = initial.clone();
            for unit in &mut inventory.domains[0].plan.units { unit.recovery_epoch = epoch; }
            let domain = &inventory.domains[0];
            let manifest = Manifest {
                recovery_epoch: epoch, placement_revision: 1, domain_recovery_epochs: [(0, epoch.to_string())].into(),
                schema_version: protocol::SCHEMA_VERSION, protocol_revision: protocol::PROTOCOL_REVISION.into(),
                network: "main".into(), pool: "ironwood".into(), generation: epoch + 1, anchor_height: 3428143,
                anchor_block_hash: "01".repeat(32), geometry: Geometry::default(), coverage: inventory.coverage.clone(),
                sessions: vec![domain.reference().unwrap()], unit_identities: [(0, domain.plan.units.clone())].into(),
            };
            std::fs::write(root.join("domains").join(format!("{}.json", protocol::digest(&domain.plan))), serde_json::to_vec(domain).unwrap()).unwrap();
            // Only retention uses these markers. Runtime loading is covered
            // independently without rebuilding identical bytes for each epoch.
            let runtime = root.join("preparation/artifacts-9").join(domain.plan.units[0].digest());
            std::fs::create_dir_all(&runtime).unwrap();
            std::fs::write(runtime.join("retention-marker"), b"runtime identity").unwrap();
            distribution.publish(manifest, inventory, pir_publisher::publisher::PublisherEngine).await.unwrap();
        }
        assert_eq!(std::fs::read_dir(root.join("inventories")).unwrap().count(), protocol::RETAINED_GENERATIONS);
        assert_eq!(std::fs::read_dir(root.join("domains")).unwrap().count(), protocol::RETAINED_GENERATIONS);
        assert_eq!(std::fs::read_dir(root.join("preparation/artifacts-9")).unwrap().count(), protocol::RETAINED_GENERATIONS);
        assert!(!orphan_path.exists());
        assert!(!partial_path.exists());
        let mut bytes = String::new();
        pinned.read_to_string(&mut bytes).unwrap();
        assert_eq!(bytes, "interrupted candidate", "already-open streams survive collection");
        assert!(root.join("objects/operator-note").exists());
        enhance_publisher::publication::verify_publication(&root, initial).unwrap();
        assert!(root.join("current.json").exists());
        assert!(root.join("distribution.lock").exists());
        assert!(root.join("prepare.lock").exists());
    }).await.expect("Enhance retained publication collection exceeded qualification budget");
}

fn fixture_records(start: u64, count: usize) -> Result<Vec<u8>, String> {
    Ok((start..start + count as u64).flat_map(record).collect())
}

fn fixture_coverage(count: u64) -> protocol::Coverage {
    Lifecycle::default()
        .coverage(count, Geometry::default())
        .unwrap()
}

// Without WALLET_PIR_TEST_RUNTIME_CACHE the shared preparation lives in a
// process-owned directory that is removed after the test threads exit.
static LOCAL_CACHE: std::sync::Mutex<Option<tempfile::TempDir>> = std::sync::Mutex::new(None);

extern "C" fn remove_local_cache() {
    LOCAL_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();
}

fn cache() -> std::path::PathBuf {
    if let Some(root) = std::env::var_os("WALLET_PIR_TEST_RUNTIME_CACHE") {
        return root.into();
    }
    let mut local = LOCAL_CACHE.lock().unwrap();
    local
        .get_or_insert_with(|| {
            // SAFETY: only an owned test directory is dropped after test threads exit.
            assert_eq!(unsafe { libc::atexit(remove_local_cache) }, 0);
            tempfile::tempdir().unwrap()
        })
        .path()
        .to_path_buf()
}

fn copy_tree(source: &Path, target: &Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let destination = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &destination);
        } else {
            std::fs::copy(entry.path(), destination).unwrap();
        }
    }
}

/// One complete publisher preparation of `count` fixture records, built once
/// per process (or per `WALLET_PIR_TEST_RUNTIME_CACHE`) and keyed by its unit
/// identities. A cold build is the publisher's whole deployed-geometry
/// preparation, including packing; its time is reported separately.
fn shared_publication(count: u64) -> std::path::PathBuf {
    let coverage = fixture_coverage(count);
    let plan =
        enhance_query_server::runtime::plan(coverage.shards[0].clone(), fixture_records).unwrap();
    let cache = cache();
    std::fs::create_dir_all(&cache).unwrap();
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(cache.join("enhance-publications.lock"))
        .unwrap();
    lock.lock().unwrap();
    let prepared = cache.join(format!(
        "enhance-publication-{count}-{}-{}-{}",
        cfg!(feature = "native-reinspiring"),
        cfg!(feature = "cuda"),
        hex::encode(Sha256::digest(serde_json::to_vec(&plan.units).unwrap()))
    ));
    if !prepared.exists() {
        let started = std::time::Instant::now();
        let temporary = tempfile::tempdir_in(&cache).unwrap();
        enhance_publisher::publication::prepare_publication(
            temporary.path(),
            coverage,
            &[(0, "0".into())].into(),
            fixture_records,
        )
        .unwrap();
        std::fs::rename(temporary.path(), &prepared).unwrap();
        eprintln!(
            "cold shared {count}-record publication: {:?}",
            started.elapsed()
        );
    }
    prepared
}

/// Prepares a publication of `count` fixture records through the real
/// publisher. The case's objects and domain records start as copies of the
/// shared preparation, so the publisher takes its retained-domain path: it
/// reinstalls the rows, checks the retained domain identity and hashes every
/// object.
fn prepare_publication(
    root: &Path,
    count: u64,
) -> enhance_publisher::publication::PreparedPublication {
    let shared = shared_publication(count);
    for directory in ["objects", "domains"] {
        copy_tree(&shared.join(directory), &root.join(directory));
    }
    enhance_publisher::publication::prepare_publication(
        root,
        fixture_coverage(count),
        &[(0, "0".into())].into(),
        fixture_records,
    )
    .unwrap()
}

/// The shared 67-record publication, published through the real distribution.
async fn published() -> Published {
    Published::prepared_by(|root| prepare_publication(root, 67)).await
}

/// The ordinary-lane authority boundary needs no prepared material: an empty
/// distribution grants nothing over TLS, and its withdrawal fence survives a
/// restart without letting the predecessor replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unpublished_distribution_refuses_authority_and_keeps_its_fence_across_restart() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let scratch = tempfile::tempdir().unwrap();
        let root = scratch.path().join("publication");
        let node = serve(axum::Router::new().route("/", axum::routing::post(
            |axum::Json(request): axum::Json<serde_json::Value>| async move {
                axum::Json(serde_json::json!({"id": request["id"], "error": null, "result": "01".repeat(32)}))
            }
        ))).await;
        let cookie = scratch.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let rpc = ZakuraClient::from_cookie_file(&node.url, cookie).unwrap();
        let distribution = PublicationDistribution::open(&root, rpc.clone()).unwrap();
        let upstream = serve(pir_e2e_tests::enhance_fixture::distribution_router(&distribution)).await;
        let transport = tls(scratch.path(), &upstream.url).await;
        let current_url = format!("{}{PREFIX}current", transport.origin);
        let authority_url = format!("{}{PREFIX}authority", transport.origin);
        let before: AuthorityResponse = transport.client.get(&current_url).send().await.unwrap().json().await.unwrap();
        assert!(!before.current.eligible);
        assert_eq!(before.source.observed_unix_ms, 0);
        assert!(before.authorized_manifest_sha256.is_empty());
        for manifest_sha256 in ["11".repeat(32), "../escape".into()] {
            let refused = transport.client.post(&authority_url).json(&AuthorityRequest { manifest_sha256 }).send().await.unwrap();
            assert_eq!(refused.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE, "nothing published grants no authority");
        }
        let session = "ab".repeat(32);
        distribution.withdraw(&Revocation { recovery_epoch: 1, sessions: [session.clone()].into() }).unwrap();
        assert!(distribution.withdraw(&Revocation::default()).is_err(), "a fence cannot move backwards");
        let withdrawn: AuthorityResponse = transport.client.get(&current_url).send().await.unwrap().json().await.unwrap();
        assert!(!withdrawn.current.eligible);
        assert_eq!(withdrawn.source.observed_unix_ms, 0);
        assert!(withdrawn.current.succeeds(&before.current));
        assert!(withdrawn.current.withdrawal_revision > before.current.withdrawal_revision);
        assert_eq!(withdrawn.current.recovery_epoch, 1);
        assert_eq!(withdrawn.current.revoked, [session].into());
        upstream.stop().await;
        drop(distribution);
        let reopened = PublicationDistribution::open(&root, rpc).unwrap();
        let listener = serve(reopened.router()).await;
        let restored: AuthorityResponse = reqwest::get(format!("{}{PREFIX}current", listener.url)).await.unwrap().json().await.unwrap();
        assert!(!restored.current.eligible);
        assert!(restored.current.succeeds(&withdrawn.current));
        assert!(restored.current.withdrawal_revision > withdrawn.current.withdrawal_revision);
        assert_eq!(restored.current.recovery_epoch, withdrawn.current.recovery_epoch);
        assert_eq!(restored.current.revoked, withdrawn.current.revoked);
        assert_eq!(restored.source.observed_unix_ms, 0);
        assert!(!before.current.succeeds(&restored.current), "restart fence cannot replay the predecessor");
    }).await.expect("Enhance authority refusal exceeded thirty seconds");
}
