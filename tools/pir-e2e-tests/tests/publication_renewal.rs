//! Real source observations for responses longer than one authority lifetime.
#[path = "support/publication.rs"]
mod publication_source;
use enhance_shared::publication::Revocation;
use publication_source::Published;
use std::time::Duration;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_response_renews_real_authority_but_cannot_cross_withdrawal_or_source_loss() {
    use pir_query_server::enhance::synchronization::{Authority, Safety};
    // The cold publisher preparation (26-32s on a three-CPU host) is fixture
    // setup, not renewal behavior, so it runs before the case budget starts.
    let fixture = Published::new().await;
    tokio::time::timeout(Duration::from_secs(30), async {
        let source = fixture.source();
        let root = fixture.scratch.path().join("delayed-response");
        let authority = Authority::new(
            &root,
            &source,
            Safety::open(&root, &source.publisher_origin).unwrap(),
        )
        .unwrap();
        let admitted = authority.fresh(&fixture.manifest).await.unwrap();
        let requests = fixture.scratch.path().join("fault.requests");
        let calls = || {
            std::fs::read_to_string(&requests)
                .unwrap()
                .lines()
                .filter(|line| line.ends_with("/authority"))
                .count()
        };
        let before = calls();
        tokio::time::sleep(Duration::from_millis(5200)).await;
        assert!(
            admitted.check().is_err(),
            "the original five-second observation expired"
        );
        let renewed = admitted.fresh_for_release(&authority).await.unwrap();
        renewed.check().unwrap();
        assert!(
            calls() > before,
            "renewal must cross real HTTPS, not extend a timer"
        );

        // The consumer deliberately misses the ineligible snapshot. A fresh
        // observation can authorize a new request, but cannot revive this one.
        fixture
            .distribution
            .withdraw(&Revocation {
                recovery_epoch: 0,
                sessions: Default::default(),
            })
            .unwrap();
        fixture
            .distribution
            .publish(
                fixture.manifest.clone(),
                fixture.inventory.clone(),
                pir_publisher::publisher::PublisherEngine,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5200)).await;
        assert!(renewed.fresh_for_release(&authority).await.is_err());
        let new_admission = authority.fresh(&fixture.manifest).await.unwrap();
        new_admission.check().unwrap();

        fixture._node.stop().await;
        tokio::time::sleep(Duration::from_millis(5200)).await;
        assert!(new_admission.check().is_err());
        assert!(
            new_admission.fresh_for_release(&authority).await.is_err(),
            "source loss cannot grant a new observation"
        );
    })
    .await
    .unwrap();
}
