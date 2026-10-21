//! Encrypted answers through autonomous serving roles and real publisher TLS.
use enhance_query_server::{client::EnhancePirClient, matvec::MatvecConfig};
use pir_e2e_tests::enhance_fixture::{record, System};
async fn round_trip(backend: MatvecConfig) {
    let mut system = System::with_backend((0..67).map(record).collect(), backend, 1, 2).await;
    let mut client = EnhancePirClient::connect(&system.origin).await.unwrap();
    for position in [0, 32, 33, 66] {
        assert_eq!(
            client
                .query_position_with_timing(position)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(position)
        );
    }
    let before = client.manifest().generation;
    system.append(vec![record(67)]).await;
    // An existing covered request observes the stale generation and refreshes.
    client.query_position_with_timing(0).await.unwrap();
    assert_eq!(
        client
            .query_position_with_timing(67)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(67)
    );
    assert!(client.manifest().generation > before);
    system.publisher.revoke_after(0).unwrap();
    for worker in &mut system.workers {
        assert!(worker.poll_once().await.is_err());
    }
    for router in &mut system.routers {
        assert!(router.poll_once().await.is_err());
    }
    assert!(system.ingress.poll_once().await.is_err());
    assert!(!reqwest::get(format!("{}/v1/enhance/init", system.origin))
        .await
        .unwrap()
        .status()
        .is_success());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_consumers_answer_updates_and_close_on_withdrawal() {
    round_trip(Default::default()).await;
}
#[cfg(feature = "cuda")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an isolated CUDA qualification host"]
async fn cuda_distributed_round_trip_retention_failover_and_restart() {
    round_trip(MatvecConfig {
        matvec_backend: enhance_query_server::matvec::Backend::Cuda,
        cuda_device: None,
    })
    .await;
}
