use super::*;

#[tokio::test]
async fn publication_metrics_report_failures_without_claiming_advancement() {
    use tower::ServiceExt;
    let root = tempfile::tempdir().unwrap();
    let publisher = EnhancePublisher::open(
        &root.path().join("publisher"),
        rpc(root.path(), "http://127.0.0.1:1"),
    )
    .unwrap();
    async fn metrics(p: &EnhancePublisher) -> String {
        let response = p
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/metrics")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        String::from_utf8(
            axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap()
    }
    let text = metrics(&publisher).await;
    assert!(!text.contains("last_advancement"));
    assert!(text.contains("enhance_publication_blocked 1\n"));
    for _ in 0..3 {
        publisher.publication_finished(Some("preparation failed".into()));
    }
    let text = metrics(&publisher).await;
    assert!(text.contains("enhance_publication_consecutive_failures 3\n"));
    assert!(!text.contains("last_advancement"));
    publisher.state.lock().unwrap().last_advancement =
        Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(100));
    publisher.publication_finished(None);
    let text = metrics(&publisher).await;
    assert!(text.contains("enhance_publication_consecutive_failures 0\n"));
    assert!(text.contains("enhance_publication_last_advancement_unix_seconds 100\n"));
    // A successful completion does not authorize a withdrawn publication.
    assert!(text.contains("enhance_publication_blocked 1\n"));
    drop(publisher);
    let reopened = EnhancePublisher::open(
        &root.path().join("publisher"),
        rpc(root.path(), "http://127.0.0.1:1"),
    )
    .unwrap();
    assert!(!metrics(&reopened).await.contains("last_advancement"));
}
use enhance_shared::types::{DatabaseId, ENHANCE_LAYOUT};
use std::fs;
fn journal(root: &Path) -> RecordJournal {
    let mut journal =
        RecordJournal::open(root.join("records"), DatabaseId::Enhance, ENHANCE_LAYOUT).unwrap();
    journal
        .append_block(
            1,
            format!("{:064x}", 1),
            &vec![vec![0u8; enhance_shared::RECORD_BYTES]; 67],
        )
        .unwrap();
    journal
}
fn rpc(root: &Path, origin: &str) -> ZakuraClient {
    let cookie = root.join("cookie");
    fs::write(&cookie, "fixture:fixture").unwrap();
    ZakuraClient::from_cookie_file(origin, cookie).unwrap()
}
/// Withdrawn work is rejected before preparation. The reference packing
/// artifact is larger than 512 MiB, so this case must not build one.
#[tokio::test]
async fn withdrawal_rejects_bound_publication_before_a_runtime_exists() {
    let root = tempfile::tempdir().unwrap();
    let rpc = rpc(root.path(), "http://127.0.0.1:1");
    let publisher_root = root.path().join("publication");
    let publisher = EnhancePublisher::open(&publisher_root, rpc.clone()).unwrap();
    assert!(EnhancePublisher::open(&publisher_root, rpc.clone()).is_err());
    let journal = journal(root.path());
    let stale = publisher.work(&journal).unwrap().unwrap();
    let before = publisher.distribution.durable_current();
    publisher.revoke_after(0).unwrap();
    assert!(
        publisher.distribution.durable_current().withdrawal_revision > before.withdrawal_revision,
        "withdrawal must fence preparation even before the first eligible publication"
    );
    assert!(publisher
        .publish(stale, crate::host_engine::PublisherEngine)
        .await
        .is_err());
    assert!(
        !publisher_root.join("objects").exists(),
        "withdrawn work must not prepare a runtime"
    );
    let again = publisher.work(&journal).unwrap().unwrap();
    publisher.revoke_after(0).unwrap();
    assert!(publisher
        .publish(again, crate::host_engine::PublisherEngine)
        .await
        .is_err());
    drop(publisher);
    let publisher = EnhancePublisher::open(&publisher_root, rpc).unwrap();
    assert!(!publisher.distribution.durable_current().eligible);
    let fresh = publisher.work(&journal).unwrap().unwrap();
    assert!(
        !publisher.distribution.durable_current().eligible,
        "reserving restart work cannot authorize service"
    );
    publisher.revoke_after(0).unwrap();
    assert!(
        publisher
            .publish(fresh, crate::host_engine::PublisherEngine)
            .await
            .is_err(),
        "restart work must also obey a later withdrawal"
    );
    assert!(!publisher_root.join("objects").exists());
}
/// Full publication, including the reference packing artifact, then restart.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_publication_needs_no_consumers_and_never_reuses_prewithdrawal_work() {
    let root = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new().route("/", axum::routing::post(|axum::Json(request): axum::Json<serde_json::Value>| async move {
        assert_eq!(request["method"], "getblockhash");
        axum::Json(serde_json::json!({"id": request["id"], "error": null, "result": format!("{:064x}", request["params"][0].as_u64().unwrap())}))
    }));
    let node = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let rpc = rpc(root.path(), &origin);
    let publisher_root = root.path().join("publication");
    let publisher = EnhancePublisher::open(&publisher_root, rpc.clone()).unwrap();
    assert!(EnhancePublisher::open(&publisher_root, rpc.clone()).is_err());
    let journal = journal(root.path());
    let stale = publisher.work(&journal).unwrap().unwrap();
    let before = publisher.distribution.durable_current();
    publisher.revoke_after(0).unwrap();
    assert!(
        publisher.distribution.durable_current().withdrawal_revision > before.withdrawal_revision,
        "withdrawal must fence preparation even before the first eligible publication"
    );
    assert!(publisher
        .publish(stale, crate::host_engine::PublisherEngine)
        .await
        .is_err());
    let work = publisher.work(&journal).unwrap().unwrap();
    publisher
        .publish(work, crate::host_engine::PublisherEngine)
        .await
        .unwrap();
    let first = publisher.distribution.durable_current();
    let manifest = first.manifest.as_ref().unwrap();
    assert!(first.eligible);
    let inventory: enhance_shared::publication::Inventory = crate::publication::read_metadata(
        &publisher
            .distribution
            .root()
            .join("inventories")
            .join(format!("{}.json", first.inventory.as_ref().unwrap().sha256)),
    )
    .unwrap();
    assert!(inventory.supports(manifest).unwrap());
    crate::publication::verify_publication(publisher.distribution.root(), inventory).unwrap();
    assert!(publisher.work(&journal).unwrap().is_none());
    let before_restart = publisher.distribution.durable_current();
    let old_work = publisher.work(&journal).unwrap();
    assert!(old_work.is_none());
    drop(publisher);
    let engine = || crate::host_engine::PublisherEngine;
    // A restart withdraws the publication; one whose anchor cannot be checked stays so.
    let unchecked = EnhancePublisher::open(
        &publisher_root,
        self::rpc(root.path(), "http://127.0.0.1:1"),
    )
    .unwrap();
    assert!(!unchecked.distribution.durable_current().eligible);
    assert!(unchecked
        .restore(engine(), Default::default())
        .await
        .is_err());
    assert!(!unchecked.distribution.durable_current().eligible);
    drop(unchecked);
    // Checked again, the same publication serves under the restart's withdrawal fence,
    // so leases granted before the restart stay void, and its prefix needs no new work.
    let publisher = EnhancePublisher::open(&publisher_root, rpc.clone()).unwrap();
    assert!(publisher
        .restore(engine(), Default::default())
        .await
        .unwrap());
    let restored = publisher.distribution.durable_current();
    assert!(restored.eligible);
    assert_eq!(restored.manifest, before_restart.manifest);
    assert!(restored.withdrawal_revision > before_restart.withdrawal_revision);
    assert!(publisher.work(&journal).unwrap().is_none());
    assert!(!publisher
        .restore(engine(), Default::default())
        .await
        .unwrap());
    drop(publisher);
    // An object whose bytes changed keeps the publication withdrawn.
    let objects = publisher_root.join("https").join("objects");
    let changed = fs::read_dir(&objects)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .min_by_key(|path| fs::metadata(path).unwrap().len())
        .unwrap();
    let original = fs::read(&changed).unwrap();
    let mut bytes = original.clone();
    *bytes.last_mut().unwrap() ^= 1;
    let mut permissions = fs::metadata(&changed).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(&changed, permissions.clone()).unwrap();
    fs::write(&changed, &bytes).unwrap();
    let publisher = EnhancePublisher::open(&publisher_root, rpc.clone()).unwrap();
    assert!(publisher
        .restore(engine(), Default::default())
        .await
        .is_err());
    assert!(!publisher.distribution.durable_current().eligible);
    drop(publisher);
    fs::write(&changed, &original).unwrap();
    // A recovery fence raised after the publication keeps it withdrawn too.
    let publisher = EnhancePublisher::open(&publisher_root, rpc.clone()).unwrap();
    let current = publisher.distribution.durable_current();
    publisher
        .distribution
        .withdraw(&enhance_shared::publication::Revocation {
            recovery_epoch: current.recovery_epoch + 1,
            sessions: current.revoked.clone(),
        })
        .unwrap();
    drop(publisher);
    let publisher = EnhancePublisher::open(&publisher_root, rpc).unwrap();
    assert!(publisher
        .restore(engine(), Default::default())
        .await
        .is_err());
    assert!(!publisher.distribution.durable_current().eligible);
    assert!(
        publisher.distribution.durable_current().withdrawal_revision
            > before_restart.withdrawal_revision
    );
    let fresh = publisher.work(&journal).unwrap().unwrap();
    assert!(fresh.manifest.generation > before_restart.manifest.unwrap().generation);
    assert!(
        !publisher.distribution.durable_current().eligible,
        "reserving restart work cannot authorize service"
    );
    publisher.revoke_after(0).unwrap();
    assert!(
        publisher
            .publish(fresh, crate::host_engine::PublisherEngine)
            .await
            .is_err(),
        "restart work must also obey a later withdrawal"
    );
    node.abort();
}
#[tokio::test]
async fn failed_publisher_persistence_cannot_grant_authority_after_storage_recovers() {
    let root = tempfile::tempdir().unwrap();
    let publisher_root = root.path().join("publication");
    let publisher =
        EnhancePublisher::open(&publisher_root, rpc(root.path(), "http://127.0.0.1:1")).unwrap();
    let journal = journal(root.path());
    fs::rename(
        publisher_root.join("publisher.json"),
        publisher_root.join("saved.json"),
    )
    .unwrap();
    fs::create_dir(publisher_root.join("publisher.json")).unwrap();
    assert!(publisher.work(&journal).is_err());
    assert!(publisher.distribution.authority(None).await.is_err());
    fs::remove_dir(publisher_root.join("publisher.json")).unwrap();
    fs::rename(
        publisher_root.join("saved.json"),
        publisher_root.join("publisher.json"),
    )
    .unwrap();
    assert!(publisher.work(&journal).is_err());
    assert!(publisher.distribution.authority(None).await.is_err());
}
