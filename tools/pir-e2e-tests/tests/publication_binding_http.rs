//! A real wallet body cannot select a different publication, setup or anchor.
use enhance_query_server::{
    client::QuerySession,
    protocol::{QueryBinding, ShardSession, HEADER_BYTES},
};
use pir_e2e_tests::enhance_fixture::{record, System};
#[path = "support/publication.rs"]
mod publication_source;

async fn advance_during_authorization(
    source: &publication_source::Published,
    generation: u64,
    poll: impl std::future::Future<Output = Result<(), enhance_query_server::synchronization::Error>>,
    withdraw: bool,
) -> Result<(), enhance_query_server::synchronization::Error> {
    let fault = source.scratch.path().join("fault");
    let entered = source.scratch.path().join("fault.entered");
    if entered.exists() {
        std::fs::remove_file(&entered).unwrap();
    }
    std::fs::write(&fault, "pause-authority").unwrap();
    let change = async {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !entered.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("candidate did not request fresh authority");
        if withdraw {
            source
                .distribution
                .withdraw(&enhance_query_server::worker::Revocation {
                    recovery_epoch: 0,
                    sessions: Default::default(),
                })
                .unwrap();
        }
        let mut next = source.manifest.clone();
        next.generation = generation;
        source
            .distribution
            .publish(next, source.inventory.clone())
            .await
            .unwrap();
        std::fs::write(&fault, "").unwrap();
    };
    let (result, ()) = tokio::join!(poll, change);
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_publication_advancement_allows_retained_candidates_but_withdrawal_refuses_them() {
    use enhance_query_server::{ingress_sync, router_sync, worker_sync};
    use pir_e2e_tests::enhance_fixture::serve;
    let fixture = publication_source::Published::new().await;
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
    advance_during_authorization(&fixture, 2, worker_sync.poll_once(), false)
        .await
        .unwrap();
    worker_sync.poll_once().await.unwrap();
    let router = router_sync::PackingRouter::open(
        &fixture.scratch.path().join("router"),
        router_sync::Config {
            source: fixture.source(),
            workers: [(0, vec![worker_http.url.clone()])].into(),
            preferred: Default::default(),
        },
        6,
        4,
    )
    .unwrap();
    let mut router_sync = router.synchronizer().unwrap();
    let router_http = serve(router.public_router()).await;
    let router_control = serve(router.control_router()).await;
    advance_during_authorization(&fixture, 3, router_sync.poll_once(), false)
        .await
        .unwrap();
    worker_sync.poll_once().await.unwrap();
    router_sync.poll_once().await.unwrap();
    let ingress = ingress_sync::QueryIngress::open(
        &fixture.scratch.path().join("ingress"),
        ingress_sync::Config {
            source: fixture.source(),
            routers: [(
                0,
                vec![ingress_sync::RouterPeer {
                    control_origin: router_control.url.clone(),
                    query_origin: router_http.url.clone(),
                }],
            )]
            .into(),
        },
        16,
        256,
    )
    .unwrap();
    let mut ingress_sync = ingress.synchronizer().unwrap();
    advance_during_authorization(&fixture, 4, ingress_sync.poll_once(), false)
        .await
        .unwrap();
    let public = serve(ingress.public_router()).await;
    let init: serde_json::Value = reqwest::get(format!("{}/v1/enhance/init", public.url))
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(init["generation"], 3, "the retained candidate became ready");
    assert!(
        advance_during_authorization(&fixture, 5, worker_sync.poll_once(), true)
            .await
            .is_err(),
        "a newer withdrawal boundary cannot authorize the candidate"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wallet_binding_mismatches_are_refused_before_forwarding() {
    let system = System::start((0..67).map(record).collect()).await;
    let http = reqwest::Client::new();
    let session: ShardSession = http
        .get(format!(
            "{}/v1/enhance/sessions/{}/0",
            system.origin, system.manifest.generation
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let session = QuerySession::new(&system.manifest, session).unwrap();
    let (query, _) = session.prepare_position(66).unwrap();
    let initial = QueryBinding::decode(query.body()).unwrap();
    for fault in ["generation", "recovery", "anchor", "session"] {
        let mut binding = initial;
        match fault {
            "generation" => binding.generation += 1,
            "recovery" => binding.recovery_epoch += 1,
            "anchor" => binding.anchor_hash[0] ^= 1,
            _ => binding.session_id[0] ^= 1,
        }
        let mut body = binding.encode();
        body.extend_from_slice(&query.body()[HEADER_BYTES..]);
        let response = http
            .post(format!("{}/v1/enhance/query", system.origin))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if matches!(fault, "generation" | "recovery") {
                reqwest::StatusCode::CONFLICT
            } else {
                reqwest::StatusCode::GONE
            },
            "{fault}"
        );
    }
}
