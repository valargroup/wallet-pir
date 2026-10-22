use super::*;
use enhance_query_server::types::{DatabaseId, ENHANCE_LAYOUT};
use std::fs;
fn journal(root: &Path) -> RecordJournal {
    let mut journal =
        RecordJournal::open(root.join("records"), DatabaseId::Enhance, ENHANCE_LAYOUT).unwrap();
    journal
        .append_block(
            1,
            format!("{:064x}", 1),
            &vec![vec![0u8; enhance_query_server::RECORD_BYTES]; 67],
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
    assert!(publisher.publish(stale).await.is_err());
    assert!(
        !publisher_root
            .join(pir_artifact::namespace("objects"))
            .exists(),
        "withdrawn work must not prepare a runtime"
    );
    let again = publisher.work(&journal).unwrap().unwrap();
    publisher.revoke_after(0).unwrap();
    assert!(publisher.publish(again).await.is_err());
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
        publisher.publish(fresh).await.is_err(),
        "restart work must also obey a later withdrawal"
    );
    assert!(!publisher_root
        .join(pir_artifact::namespace("objects"))
        .exists());
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
    assert!(publisher.publish(stale).await.is_err());
    let work = publisher.work(&journal).unwrap().unwrap();
    publisher.publish(work).await.unwrap();
    let first = publisher.distribution.durable_current();
    let manifest = first.manifest.as_ref().unwrap();
    assert!(first.eligible);
    let inventory: enhance_query_server::publication::Inventory =
        crate::publication::read_metadata(
            &publisher
                .distribution
                .root()
                .join(pir_artifact::namespace("inventories"))
                .join(format!(
                    "{}.json",
                    first.inventory.as_ref().unwrap().checksum
                )),
        )
        .unwrap();
    assert!(inventory.supports(manifest).unwrap());
    crate::publication::verify_publication(publisher.distribution.root(), inventory).unwrap();
    assert!(publisher.work(&journal).unwrap().is_none());
    let before_restart = publisher.distribution.durable_current();
    let old_work = publisher.work(&journal).unwrap();
    assert!(old_work.is_none());
    drop(publisher);
    let publisher = EnhancePublisher::open(&publisher_root, rpc).unwrap();
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
        publisher.publish(fresh).await.is_err(),
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
