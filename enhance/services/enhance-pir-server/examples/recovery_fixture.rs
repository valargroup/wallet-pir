//! Loopback-only immutable Enhance fixture server for the wallet recovery benchmark.
use enhance_pir::RECORD_BYTES;
use enhance_pir_server::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
    store::{BlockEntry, RecordJournal},
    types::{DatabaseId, ENHANCE_LAYOUT},
    worker::Worker,
};
use std::{
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
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
    let journal = fixture_journal(&root.join("journal"), &fixture);
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

// This is fixture preparation, not an ingestion benchmark. Seed an empty journal's
// format once, retaining real block boundaries for sealed-shard coverage. Appending
// tens of thousands of blocks individually would repeatedly rewrite its manifest.
fn fixture_journal(path: &Path, fixture: &serde_json::Value) -> RecordJournal {
    assert!(!path.exists());
    drop(RecordJournal::open(path, DatabaseId::Enhance, ENHANCE_LAYOUT).unwrap());
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path.join("manifest.json")).unwrap()).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path.join("records.bin"))
        .unwrap();
    let mut position = 0u64;
    let mut blocks: Vec<BlockEntry> = Vec::new();
    for block in fixture["blocks"].as_array().unwrap() {
        let count = block["count"].as_u64().unwrap();
        let height = block["height"].as_u64().unwrap();
        if let Some(previous) = blocks.last() {
            assert_eq!(height, previous.height + 1);
        }
        for record in block["records"].as_array().unwrap() {
            let offset = record["offset"].as_u64().unwrap();
            assert!(offset < count);
            let bytes = hex::decode(record["hex"].as_str().unwrap()).unwrap();
            assert_eq!(bytes.len(), RECORD_BYTES);
            file.seek(SeekFrom::Start((position + offset) * RECORD_BYTES as u64))
                .unwrap();
            file.write_all(&bytes).unwrap();
        }
        blocks.push(BlockEntry {
            height,
            hash: block["hash"].as_str().unwrap().into(),
            first_position: position,
            action_count: count,
        });
        position = position.checked_add(count).unwrap();
    }
    file.set_len(position.checked_mul(RECORD_BYTES as u64).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    manifest["tree_size"] = position.into();
    manifest["blocks"] = serde_json::to_value(&blocks).unwrap();
    std::fs::write(
        path.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let journal = RecordJournal::open(path, DatabaseId::Enhance, ENHANCE_LAYOUT).unwrap();
    assert_eq!(journal.tree_size(), position);
    assert_eq!(journal.blocks(), blocks);
    journal
}
