//! Repairing fee-less history: an old journal (producer output with the old
//! binary's missing fees), `rebuild-journal` against a trusted-RPC double with
//! an interruption, then adoption by the real coordinator and HTTP PIR queries.
mod support;

use enhance_pir::client::EnhancePirClient;
use enhance_pir::protocol::Manifest;
use enhance_pir::types::{
    EnhanceRecord, FLAG_HAS_FEE, FLAG_HAS_TRANSPARENT_INPUTS, FLAG_HAS_TRANSPARENT_OUTPUTS,
    RECORD_BYTES, RECORD_FEE_OFFSET, RECORD_FLAGS_OFFSET,
};
use enhance_pir::ACTIVATION_HEIGHT;
use enhance_pir_server::ingest::EnhanceJournal;
use enhance_pir_server::rebuild::{adopt_staged, Adoption, Receipt};
use enhance_pir_server::worker::Worker;
use enhance_pir_server::zakura::{Prevouts, ZakuraClient};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::*;

const H: u64 = ACTIVATION_HEIGHT;

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_enhance-pir-server"))
}

fn log(root: &Path) -> std::fs::File {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("coordinator.log"))
        .unwrap()
}

fn read_log(root: &Path) -> String {
    std::fs::read_to_string(root.join("coordinator.log")).unwrap_or_default()
}

fn start(root: &Path, address: &str, rpc: &str) -> Process {
    let log = log(root);
    Process(
        binary()
            .env("RUST_LOG", "info")
            .args([
                "coordinator",
                "--listen",
                address,
                "--poll-seconds",
                "1",
                "--zakura-rpc-url",
                rpc,
            ])
            .arg("--data-dir")
            .arg(root.join("data"))
            .arg("--worker-config")
            .arg(root.join("workers.json"))
            .arg("--zakura-cookie")
            .arg(root.join("cookie"))
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}

fn rebuild_command(root: &Path, rpc: &str) -> Command {
    let mut command = binary();
    command
        .args([
            "rebuild-journal",
            "--zakura-rpc-url",
            rpc,
            "--concurrency",
            "4",
            "--cache-outputs",
            "1000",
        ])
        .arg("--source")
        .arg(root.join("data"))
        .arg("--output")
        .arg(root.join("data/enhance-staged"))
        .arg("--zakura-cookie")
        .arg(root.join("cookie"));
    command
}

async fn published(process: &mut Process, root: &Path, origin: &str, hash: &str) -> Manifest {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    loop {
        assert!(
            process.0.try_wait().unwrap().is_none(),
            "coordinator stopped: {}",
            read_log(root)
        );
        if let Ok(response) = client.get(format!("{origin}/v1/enhance/init")).send().await {
            if let Ok(manifest) = response.json::<Manifest>().await {
                if manifest.anchor_block_hash == hash {
                    return manifest;
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "publication timeout: {}",
            read_log(root)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_log(process: &mut Process, root: &Path, needle: &str, after: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            process.0.try_wait().unwrap().is_none(),
            "coordinator stopped: {}",
            read_log(root)
        );
        if read_log(root)[after..].contains(needle) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "missing {needle}: {}",
            read_log(root)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn digest(path: &Path) -> String {
    hex::encode(Sha256::digest(std::fs::read(path).unwrap()))
}

fn journal_digests(dir: &Path) -> (String, String) {
    (
        digest(&dir.join("records.bin")),
        digest(&dir.join("manifest.json")),
    )
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

fn read_record(dir: &Path, position: u64) -> EnhanceRecord {
    let bytes = std::fs::read(dir.join("records.bin")).unwrap();
    let start = position as usize * RECORD_BYTES;
    EnhanceRecord::from_bytes(bytes[start..start + RECORD_BYTES].try_into().unwrap()).unwrap()
}

fn receipt(dir: &Path) -> Receipt {
    serde_json::from_slice(&std::fs::read(dir.join("rebuild.json")).unwrap()).unwrap()
}

fn write_receipt(dir: &Path, receipt: &Receipt) {
    std::fs::write(
        dir.join("rebuild.json"),
        serde_json::to_vec(receipt).unwrap(),
    )
    .unwrap();
}

/// Expected fee and transparent flags by position, from hand-set values:
/// 0..1 the public pure-Ironwood fixture; 2..3 420,000 -> 400,000 Ironwood;
/// 4..5 99,000 + 300,000 -> 100,000 change + 279,000 Ironwood; 6..7
/// 200,000 -> 180,000 Ironwood; 8..9 a coinbase, which has no fee.
fn expected(position: u64) -> (Option<u64>, bool, bool) {
    match position {
        0 | 1 => (Some(10_000), false, false),
        2..=3 | 6..=7 => (Some(20_000), true, false),
        4..=5 => (Some(20_000), true, true),
        8..=9 => (None, true, true),
        10 | 11 => (Some(10_000), false, false),
        _ => unreachable!(),
    }
}

fn check(record: &EnhanceRecord, position: u64, repaired: bool) {
    assert_fixture_action(record, position as usize % 2);
    let flags = record.as_bytes()[RECORD_FLAGS_OFFSET];
    let (fee, inputs, outputs) = expected(position);
    assert_eq!(
        flags & FLAG_HAS_TRANSPARENT_INPUTS != 0,
        inputs,
        "{position}"
    );
    assert_eq!(
        flags & FLAG_HAS_TRANSPARENT_OUTPUTS != 0,
        outputs,
        "{position}"
    );
    // The old binary published fees only for pure-Ironwood transactions.
    let fee = if repaired || !inputs { fee } else { None };
    assert_eq!(record.metadata().fee_zatoshis(), fee, "{position}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebuild_resumes_and_coordinator_adopts_only_fee_repairs() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    let live = data.join("enhance");
    let staged = data.join("enhance-staged");
    let chain: SharedChain = Arc::new(Mutex::new(Chain::default()));
    let earlier = funding(1, &[300_000]);
    let same = funding(2, &[420_000, 99_000]);
    let later = funding(3, &[200_000]);
    let hashes = {
        let mut c = chain.lock().unwrap();
        c.add_transaction(&earlier);
        let spend_same = Mixed::new(vec![spend(&same, 0)], &[], -400_000).build();
        let change = Mixed::new(
            vec![spend(&same, 1), spend(&earlier, 0)],
            &[100_000],
            -279_000,
        )
        .build();
        let spend_later = Mixed::new(vec![spend(&later, 0)], &[], -180_000).build();
        [
            c.push_block(H, &[fixture(), same.clone(), spend_same], 0),
            c.push_block(H + 1, &[later.clone()], 1),
            c.push_block(H + 2, &[change, spend_later], 2),
            c.push_block(H + 3, &[ironwood_coinbase((H + 3) as u32)], 3),
        ]
    };
    let (rpc_url, rpc_task) = serve_chain(chain.clone()).await;
    let cookie = cookie(root.path());

    // The old journal: today's producer output with the old binary's rule
    // applied, so only pure-Ironwood records keep a fee.
    {
        let client = ZakuraClient::from_cookie_file(&rpc_url, &cookie).unwrap();
        let prevouts = Prevouts::new(1000);
        let mut journal = EnhanceJournal::open(&data).unwrap();
        for height in H..=H + 3 {
            let mut block = client.block(height, &prevouts).await.unwrap();
            for record in &mut block.records {
                let mut bytes = *record.as_bytes();
                if bytes[RECORD_FLAGS_OFFSET] & FLAG_HAS_TRANSPARENT_INPUTS != 0 {
                    bytes[RECORD_FLAGS_OFFSET] &= !FLAG_HAS_FEE;
                    bytes[RECORD_FEE_OFFSET..].fill(0);
                }
                *record = EnhanceRecord::from_bytes(bytes).unwrap();
            }
            journal.append_block(&block).unwrap();
        }
        assert_eq!(journal.records.tree_size(), 10);
    }
    for position in 0..10 {
        check(&read_record(&live, position), position, false);
    }

    let mut workers = Vec::new();
    let mut replicas = Vec::new();
    for i in 0..2 {
        let (url, task) = serve(
            Worker::open(&root.path().join(format!("worker-{i}")))
                .unwrap()
                .router(),
        )
        .await;
        workers.push(task);
        replicas.push(json!({"name": format!("r{i}"), "url": url}));
    }
    std::fs::write(
        root.path().join("workers.json"),
        serde_json::to_vec(&json!({"groups": [{"name": "g0", "replicas": replicas}]})).unwrap(),
    )
    .unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap().to_string();
    drop(socket);
    let origin = format!("http://{address}");
    let mut process = start(root.path(), &address, &rpc_url);
    published(&mut process, root.path(), &origin, &hashes[3]).await;
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    for position in [0, 2, 4, 8] {
        let answer = client.query_position_with_timing(position).await.unwrap().0;
        check(
            &EnhanceRecord::from_bytes(answer.as_ref().try_into().unwrap()).unwrap(),
            position,
            false,
        );
    }

    // Interrupted rebuild, against the journal the running coordinator holds.
    chain.lock().unwrap().hold = Some(H + 2);
    let mut interrupted = Process(
        rebuild_command(root.path(), &rpc_url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let blocks = std::fs::read(staged.join("manifest.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .map_or(0, |m| m["blocks"].as_array().unwrap().len());
        if blocks == 2 {
            break;
        }
        assert!(interrupted.0.try_wait().unwrap().is_none());
        assert!(
            tokio::time::Instant::now() < deadline,
            "rebuild did not progress"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(interrupted);
    assert!(!staged.join("rebuild.json").exists());
    {
        let mut c = chain.lock().unwrap();
        c.hold = None;
        c.getblocks.clear();
    }
    let output = tokio::task::spawn_blocking({
        let mut command = rebuild_command(root.path(), &rpc_url);
        move || command.output().unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let printed: Receipt = serde_json::from_slice(&output.stdout).unwrap();
    let staged_receipt = receipt(&staged);
    assert_eq!(printed, staged_receipt);
    {
        let c = chain.lock().unwrap();
        // Resumed above the staged tip: H and H+1 were not fetched again, and
        // only the final tree-size check reads verbose data. (The interrupted
        // run's held H+2 request may also be answered after release.)
        assert!(
            c.getblocks.iter().all(|(h, _)| *h >= H + 2),
            "{:?}",
            c.getblocks
        );
        assert!(c.getblocks.contains(&(H + 2, 0)) && c.getblocks.contains(&(H + 3, 0)));
        assert_eq!(c.getblocks.iter().filter(|(_, v)| *v == 2).count(), 1);
    }
    assert_eq!(staged_receipt.height, H + 3);
    assert_eq!(staged_receipt.block_hash, hashes[3]);
    assert_eq!(staged_receipt.tree_size, 10);
    assert_eq!(staged_receipt.changed_records, 6);
    assert_eq!(staged_receipt.absent_fee_records, 2);
    assert_eq!(
        staged_receipt.records_sha256,
        digest(&staged.join("records.bin"))
    );
    assert_eq!(
        staged_receipt.manifest_sha256,
        digest(&staged.join("manifest.json"))
    );
    for position in 0..10 {
        check(&read_record(&staged, position), position, true);
    }
    // A completed output is never overwritten.
    let again = tokio::task::spawn_blocking({
        let mut command = rebuild_command(root.path(), &rpc_url);
        move || command.output().unwrap()
    })
    .await
    .unwrap();
    assert!(!again.status.success());
    let good = root.path().join("good-staged");
    std::fs::rename(&staged, &good).unwrap();

    // Library-level adoption on copies: a sealed shard covering a changed
    // record refuses, and a crash between the two renames completes.
    for sealed in [true, false] {
        let copy = root.path().join(format!("copy-{sealed}"));
        copy_dir(&live, &copy.join("enhance"));
        copy_dir(&good, &copy.join("enhance-staged"));
        let before = journal_digests(&copy.join("enhance"));
        if sealed {
            std::fs::create_dir_all(copy.join("control")).unwrap();
            std::fs::write(
                copy.join("control/controller.json"),
                serde_json::to_vec(&json!({"recovery": {"sealed": {"0": [H, hashes[0]]}}}))
                    .unwrap(),
            )
            .unwrap();
            let Adoption::Rejected { reason, moved_to } = adopt_staged(&copy).unwrap() else {
                panic!("sealed shard must refuse");
            };
            assert!(reason.contains("sealed shard 0"), "{reason}");
            assert!(moved_to.join("rebuild.json").exists());
            assert_eq!(journal_digests(&copy.join("enhance")), before);
        } else {
            let previous = copy.join(format!(
                "enhance.before-rebuild-{}",
                staged_receipt.receipt_id
            ));
            std::fs::rename(copy.join("enhance"), &previous).unwrap();
            let adopted = adopt_staged(&copy).unwrap();
            assert!(matches!(
                adopted,
                Adoption::Adopted {
                    changed_records: 6,
                    ..
                }
            ));
            assert_eq!(journal_digests(&previous), before);
            assert_eq!(
                journal_digests(&copy.join("enhance")),
                journal_digests(&good)
            );
            assert_eq!(adopt_staged(&copy).unwrap(), Adoption::None);
        }
    }

    // Refused adoptions through the real coordinator: the live journal stays,
    // the staged directory is set aside, and the coordinator keeps serving.
    type Tamper = fn(&Path);
    let tampers: [(&str, Tamper); 3] = [
        ("corrupt receipt", |dir| {
            let mut r = receipt(dir);
            r.records_sha256 = "00".repeat(32);
            write_receipt(dir, &r);
        }),
        ("non-fee byte", |dir| {
            let path = dir.join("records.bin");
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[2 * RECORD_BYTES] ^= 1;
            std::fs::write(&path, &bytes).unwrap();
            let mut r = receipt(dir);
            r.records_sha256 = digest(&path);
            write_receipt(dir, &r);
        }),
        ("not a prefix", |dir| {
            let path = dir.join("manifest.json");
            let mut manifest: Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            manifest["blocks"][1]["hash"] = json!("11".repeat(32));
            std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
            let mut r = receipt(dir);
            r.manifest_sha256 = digest(&path);
            write_receipt(dir, &r);
        }),
    ];
    for (name, tamper) in tampers {
        drop(process);
        copy_dir(&good, &staged);
        tamper(&staged);
        let before = journal_digests(&live);
        let offset = read_log(root.path()).len();
        process = start(root.path(), &address, &rpc_url);
        wait_for_log(
            &mut process,
            root.path(),
            "rejected staged Enhance journal rebuild",
            offset,
        )
        .await;
        assert!(!staged.exists(), "{name}");
        assert_eq!(journal_digests(&live), before, "{name}");
        published(&mut process, root.path(), &origin, &hashes[3]).await;
    }
    let rejected = std::fs::read_dir(&data)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("enhance-staged.rejected-")
        })
        .count();
    assert_eq!(rejected, 3);

    // Adoption, then the normal loop publishes the next block.
    drop(process);
    std::fs::rename(&good, &staged).unwrap();
    let old_live = journal_digests(&live);
    let offset = read_log(root.path()).len();
    let mut process = start(root.path(), &address, &rpc_url);
    wait_for_log(
        &mut process,
        root.path(),
        "adopted staged Enhance journal rebuild",
        offset,
    )
    .await;
    assert!(read_log(root.path())[offset..].contains(&staged_receipt.receipt_id));
    let previous: PathBuf = data.join(format!(
        "enhance.before-rebuild-{}",
        staged_receipt.receipt_id
    ));
    assert_eq!(
        journal_digests(&previous),
        old_live,
        "rollback copy is the old journal"
    );
    assert!(live.join("rebuild.json").exists());
    let next = chain.lock().unwrap().push_block(H + 4, &[fixture()], 4);
    published(&mut process, root.path(), &origin, &next).await;
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    for position in 0..12 {
        let answer = client.query_position_with_timing(position).await.unwrap().0;
        check(
            &EnhanceRecord::from_bytes(answer.as_ref().try_into().unwrap()).unwrap(),
            position,
            true,
        );
    }
    drop(process);
    rpc_task.abort();
    for worker in workers {
        worker.abort();
    }
}
