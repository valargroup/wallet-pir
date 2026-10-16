//! A bounded encrypted round trip through real listeners, including a durable restart.
use enhance_pir::{
    client::EnhancePirClient,
    types::{EnhanceRecord, EnhanceRecordParts},
};
use enhance_pir_server::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
    store::RecordJournal,
    types::{DatabaseId, ENHANCE_LAYOUT},
    worker::Worker,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_http_answers_survive_restart_and_reject_malformed_queries() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let root = tempfile::tempdir().unwrap();
        let mut listeners = Vec::new();
        let mut replicas = Vec::new();
        for index in 0..2 {
            let worker = Worker::open(&root.path().join(format!("worker-{index}"))).unwrap();
            let listening = serve(worker.router()).await;
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
        drop(client);
        http.stop().await;
        drop(coordinator);
        let restored = Coordinator::open(&state_dir, groups).unwrap();
        let restored_http = serve(restored.router()).await;
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
    .expect("encrypted HTTP smoke exceeded its deadline");
}
