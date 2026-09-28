//! Loopback-only immutable Enhance fixture server for the wallet recovery benchmark.
use enhance_pir::RECORD_BYTES;
use enhance_pir_server::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
    store::RecordJournal,
    types::{DatabaseId, ENHANCE_LAYOUT},
    worker::Worker,
};
use std::path::PathBuf;
async fn serve(app: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", socket.local_addr().unwrap());
    (
        origin,
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() }),
    )
}
#[tokio::main]
async fn main() {
    let input = PathBuf::from(std::env::args().nth(1).expect("fixture JSON"));
    let ready = PathBuf::from(std::env::args().nth(2).expect("ready JSON"));
    assert!(!ready.exists());
    let fixture: serde_json::Value =
        serde_json::from_slice(&std::fs::read(input).unwrap()).unwrap();
    let root = PathBuf::from(
        std::env::args()
            .nth(3)
            .expect("disposable storage directory"),
    );
    let mut tasks = Vec::new();
    let mut replicas = Vec::new();
    for i in 0..2 {
        let (url, task) = serve(
            Worker::open(&root.join(format!("worker-{i}")))
                .unwrap()
                .router(),
        )
        .await;
        tasks.push(task);
        replicas.push(Replica {
            name: format!("r{i}"),
            url,
            incarnation: String::new(),
            ledger: Ledger::default(),
        });
    }
    let coordinator = Coordinator::open(
        &root.join("control"),
        vec![Group {
            placement_policy: Default::default(),
            id: "g0".into(),
            sequence: 0,
            replicas,
        }],
    )
    .unwrap();
    let (origin, task) = serve(coordinator.clone().router()).await;
    tasks.push(task);
    let mut journal =
        RecordJournal::open(root.join("journal"), DatabaseId::Enhance, ENHANCE_LAYOUT).unwrap();
    for block in fixture["blocks"].as_array().unwrap() {
        let records: Vec<Vec<u8>> = block["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                let bytes = hex::decode(r.as_str().unwrap()).unwrap();
                assert_eq!(bytes.len(), RECORD_BYTES);
                bytes
            })
            .collect();
        journal
            .append_block(
                block["height"].as_u64().unwrap(),
                block["hash"].as_str().unwrap().to_owned(),
                &records,
            )
            .unwrap();
    }
    let last = fixture["blocks"].as_array().unwrap().last().unwrap();
    coordinator
        .publish(
            &journal,
            last["height"].as_u64().unwrap(),
            last["hash"].as_str().unwrap().to_owned(),
        )
        .await
        .unwrap();
    std::fs::write(
        ready,
        serde_json::to_vec(&serde_json::json!({"origin":origin})).unwrap(),
    )
    .unwrap();
    tokio::signal::ctrl_c().await.unwrap();
    for task in tasks {
        task.abort();
    }
}
