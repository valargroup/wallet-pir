//! A bounded encrypted round trip through real listeners, including a durable restart.
use enhance_pir::{
    client::EnhancePirClient,
    types::{EnhanceRecord, EnhanceRecordParts},
};
use enhance_serving::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
    store::RecordJournal,
    types::{DatabaseId, ENHANCE_LAYOUT},
    worker::Worker,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

struct Listening {
    url: String,
    task: tokio::task::JoinHandle<()>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Listening {
    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(5), &mut self.task)
            .await
            .expect("HTTP connections did not drain")
            .unwrap();
    }
}
impl Drop for Listening {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn serve(router: axum::Router) -> Listening {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", socket.local_addr().unwrap());
    let (stop, done) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(socket, router)
            .with_graceful_shutdown(async {
                let _ = done.await;
            })
            .await
            .unwrap()
    });
    Listening {
        url,
        task,
        stop: Some(stop),
    }
}
fn record(position: u64) -> Vec<u8> {
    EnhanceRecord::from_parts(EnhanceRecordParts {
        enc_ciphertext_suffix: [position as u8; 528],
        cv_net: [position.wrapping_add(1) as u8; 32],
        out_ciphertext: [position.wrapping_add(2) as u8; 80],
        has_transparent_inputs: true,
        has_transparent_outputs: false,
        metadata: enhance_pir::EnhanceTransactionMetadata::new(position as u32, Some(position))
            .unwrap(),
    })
    .as_bytes()
    .to_vec()
}

async fn workers(
    root: &std::path::Path,
    count: usize,
    reject_commit: Arc<AtomicBool>,
) -> (Vec<Listening>, Vec<Group>) {
    let mut listeners = Vec::new();
    let mut replicas = Vec::new();
    for index in 0..count {
        let worker = Worker::open(&root.join(format!("worker-{index}"))).unwrap();
        let rejected = reject_commit.clone();
        let router = worker.router().layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let rejected = rejected.clone();
                async move {
                    use axum::response::IntoResponse;
                    if index == 0
                        && request.uri().path() == "/internal/commit"
                        && rejected.load(Ordering::SeqCst)
                    {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                    } else {
                        next.run(request).await
                    }
                }
            },
        ));
        let listening = serve(router).await;
        replicas.push(Replica {
            name: format!("worker-{index}"),
            url: listening.url.clone(),
            incarnation: String::new(),
            ledger: Ledger::default(),
        });
        listeners.push(listening);
    }
    let groups = vec![Group {
        placement_policy: Default::default(),
        id: "smoke".into(),
        sequence: 0,
        replicas,
    }];
    (listeners, groups)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_publication_advances_while_another_commit_is_pending() {
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
        let read_state = || -> enhance_serving::control::State {
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
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "encrypted HTTP smoke exceeded its deadline while {}",
            phase.get()
        )
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_http_answers_survive_coordinator_restart() {
    let phase = std::cell::Cell::new("publishing the initial block");
    tokio::time::timeout(Duration::from_secs(30), async {
        let root = tempfile::tempdir().unwrap();
        let (_listeners, groups) = workers(root.path(), 2, Arc::new(AtomicBool::new(false))).await;
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
        coordinator
            .publish(&journal, 3428143, "01".repeat(32))
            .await
            .unwrap();
        let http = serve(coordinator.clone().router()).await;
        phase.set("querying before restart");
        let mut client = EnhancePirClient::connect(&http.url).await.unwrap();
        assert_eq!(
            client
                .query_position_with_timing(66)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(66)
        );
        drop(client);
        http.stop().await;
        drop(coordinator);
        phase.set("restoring the coordinator from disk");
        let restored = Coordinator::open(&state_dir, groups).unwrap();
        let restored_http = serve(restored.router()).await;
        phase.set("querying after restart with a fresh wallet");
        let mut client = EnhancePirClient::connect(&restored_http.url).await.unwrap();
        assert_eq!(
            client
                .query_position_with_timing(0)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(0)
        );
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "encrypted restart exceeded its deadline while {}",
            phase.get()
        )
    });
}
