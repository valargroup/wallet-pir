//! Encrypted publication and restart through real listeners.
#[path = "support/encrypted.rs"]
mod encrypted;
use encrypted::{record, serve, workers};
use enhance_publisher::{coordinator::Coordinator, store::RecordJournal};
use enhance_query_server::{
    client::EnhancePirClient,
    types::{DatabaseId, ENHANCE_LAYOUT},
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_publication_advances_with_a_pending_commit_and_survives_restart() {
    let phase = std::cell::Cell::new("starting listeners");
    tokio::time::timeout(Duration::from_secs(30), async {
        let root = tempfile::tempdir().unwrap();
        let reject_commit = Arc::new(AtomicBool::new(false));
        let (_listeners, groups) = workers(root.path(), 2, reject_commit.clone()).await;
        let state_dir = root.path().join("coordinator");
        let coordinator = Coordinator::open(&state_dir, groups.clone()).unwrap();
        let mut journal = RecordJournal::open(
            root.path().join("journal"),
            DatabaseId::Enhance,
            ENHANCE_LAYOUT,
        )
        .unwrap();
        journal
            .append_block(
                3428143,
                "01".repeat(32),
                &(0..67).map(record).collect::<Vec<_>>(),
            )
            .unwrap();
        // Both workers prepare the first assignment, but one loses its commit
        // acknowledgement. Its healthy peer must still publish the next block.
        phase.set("publishing the first block");
        reject_commit.store(true, Ordering::SeqCst);
        coordinator
            .publish(&journal, 3428143, "01".repeat(32))
            .await
            .unwrap();
        let http = serve(coordinator.clone().router()).await;
        // Keep one real commit unacknowledged while its healthy peer publishes
        // again. This bridges the small decision-delivery tests to real worker
        // preparation, publication selection and encrypted wallet answers.
        let read_state = || -> enhance_publisher::control::State {
            serde_json::from_slice(&std::fs::read(state_dir.join("controller.json")).unwrap())
                .unwrap()
        };
        let pending = read_state().pending_commits;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].replica, "worker-0");
        journal
            .append_block(3428144, "02".repeat(32), &[record(67)])
            .unwrap();
        phase.set("publishing while a commit is pending");
        coordinator
            .publish(&journal, 3428144, "02".repeat(32))
            .await
            .unwrap();
        assert_eq!(read_state().pending_commits, pending);
        phase.set("querying the new publication");
        let mut current = EnhancePirClient::connect(&http.url).await.unwrap();
        assert_eq!(
            current
                .query_position_with_timing(67)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(67)
        );
        phase.set("retrying the pending commit");
        reject_commit.store(false, Ordering::SeqCst);
        coordinator.reconcile().await.unwrap();
        assert!(read_state().pending_commits.is_empty());
        let malformed = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap()
            .post(format!("{}/v1/enhance/query", http.url))
            .body(vec![0; 8])
            .send()
            .await
            .unwrap();
        assert!(malformed.status().is_client_error());
        let _ = malformed.bytes().await.unwrap();
        drop(current);
        http.stop().await;
        drop(coordinator);
        // A real process restart waits for every old request-owned view to drop.
        // The in-process server can finish draining before Tokio drops its last
        // completed connection task, so observe exclusive ownership explicitly.
        phase.set("waiting for the old controller's request views to close");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match enhance_publisher::control::Store::open(&state_dir) {
                    Ok(store) => {
                        drop(store);
                        break;
                    }
                    Err(error) if error.starts_with("controller already running:") => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("cannot reopen controller state: {error}"),
                }
            }
        })
        .await
        .expect("old HTTP request views retained the controller lock");
        phase.set("restoring the coordinator from disk");
        let restored = Coordinator::open(&state_dir, groups).unwrap();
        let restored_http = serve(restored.router()).await;
        phase.set("querying after restart with a fresh wallet");
        let mut client = EnhancePirClient::connect(&restored_http.url).await.unwrap();
        // These positions share a row. One encrypted request checks that the
        // update preserved the old slot beside the appended record, and that
        // a fresh wallet can recover both after reopening durable state.
        let recovered = client.query_positions(&[66, 67]).await.unwrap();
        assert_eq!(recovered[0].as_ref(), record(66));
        assert_eq!(recovered[1].as_ref(), record(67));
        drop(client);
        restored_http.stop().await;
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "encrypted HTTP smoke exceeded its deadline while {}",
            phase.get()
        )
    });
}
