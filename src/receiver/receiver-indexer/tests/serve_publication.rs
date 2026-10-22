//! `receiver-directory serve-publication`: the real binary, real files and real HTTP,
//! against a JSON-RPC node the test controls.
#[path = "common/bound.rs"]
mod bound;
#[path = "../../receiver-store/tests/common/mod.rs"]
mod common;
#[path = "../../receiver-pir-server/tests/common/history.rs"]
mod history;
use axum::{extract::State, routing::post, Json, Router};
use receiver_directory::witness;
use receiver_directory::{snapshot::Snapshot, Hash};
use receiver_pir::{transport::Transport, Error, MIN_ROWS};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

fn activation() -> u32 {
    receiver_indexer::blocks::ironwood_activation()
}

fn mainnet() -> Hash {
    zakura_server_chain::Network::Mainnet.genesis_hash()
}

/// The node: its tip and the hash it reports at each height, and the Ironwood tree size
/// after a block where it differs from the fixtures' 8.
#[derive(Clone, Default)]
struct Node(
    Arc<Mutex<(u64, HashMap<u64, Hash>)>>,
    Arc<Mutex<Option<RecordReplacement>>>,
    Arc<Mutex<HashMap<u64, u64>>>,
);

struct RecordReplacement {
    height: u64,
    path: PathBuf,
    value: Value,
}

impl Node {
    fn set(&self, height: u32, hash: Hash) {
        let mut state = self.0.lock().unwrap();
        state.0 = state.0.max(u64::from(height) + 5);
        state.1.insert(u64::from(height), hash);
    }
}

async fn rpc(State(node): State<Node>, Json(request): Json<Value>) -> Json<Value> {
    let (tip, hashes) = node.0.lock().unwrap().clone();
    let result = match request["method"].as_str().unwrap() {
        "getblockcount" => json!(tip),
        "getblockhash" => {
            let height = request["params"][0].as_u64().unwrap();
            if height == 0 {
                json!(zakura_server_chain::hash::display(&mainnet()))
            } else {
                let mut replacement = node.1.lock().unwrap();
                if replacement.as_ref().is_some_and(|r| r.height == height) {
                    let replacement = replacement.take().unwrap();
                    point(&replacement.path, replacement.value);
                }
                let hash = hashes.get(&height).copied().unwrap_or([0xee; 32]);
                json!(zakura_server_chain::hash::display(&hash))
            }
        }
        "getblock" => {
            let named =
                zakura_server_chain::hash::parse_display(request["params"][0].as_str().unwrap())
                    .unwrap();
            let height = hashes.iter().find(|(_, h)| **h == named).map(|(h, _)| *h);
            let size = height.and_then(|h| node.2.lock().unwrap().get(&h).copied());
            json!({"trees": {"ironwood": {"size": size.unwrap_or(8)}}})
        }
        method => panic!("serve-publication must not call {method}"),
    };
    Json(json!({"result": result, "error": null, "id": request["id"]}))
}

/// A publication ending `offset` blocks after activation at `hash`, with two payments
/// to the fixture receiver and their companion witness table.
fn publication(offset: u32, hash: Hash) -> (Snapshot, witness::Table) {
    corrected_publication(offset, hash, 0)
}

/// [`publication`] under history correction revision `revision`.
fn corrected_publication(offset: u32, hash: Hash, revision: u64) -> (Snapshot, witness::Table) {
    let mut manifest = common::manifest(MIN_ROWS);
    manifest.history_revision = revision;
    manifest.genesis = mainnet();
    manifest.start_height = activation();
    manifest.end_height = activation() + offset;
    manifest.end_hash = hash;
    manifest.start_position = 0;
    manifest.end_position = 8;
    let records: Vec<_> = [0u8, 5]
        .into_iter()
        .enumerate()
        .map(|(page, position)| {
            let mut record = common::record(page as u32, 2);
            record.payment.height = manifest.end_height;
            record.payment.block_hash = hash;
            record.payment.position = position.into();
            record.payment.cmx = [position + 1; 32];
            record
        })
        .collect();
    let snapshot = Snapshot::build(manifest, &records).unwrap();
    let tree: Vec<Hash> = (1..=8).map(|i| [i; 32]).collect();
    let payloads = zakura_receiver_crypto::witness::payloads(&tree, &[0, 5]).unwrap();
    let table = receiver_indexer::witness::table(&snapshot, &payloads).unwrap();
    (snapshot, table)
}

/// Stages `snapshot` as a complete one-shard publication directory, in the indexer's
/// file layout, and returns the directory and layout identity.
fn stage(
    root: &Path,
    name: &str,
    snapshot: &Snapshot,
    witness: Option<&witness::Table>,
) -> (PathBuf, String) {
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    let companion = witness.map_or([0; 32], |w| w.manifest.revision().unwrap());
    let layout = receiver_indexer::application::Layout {
        genesis: snapshot.manifest.genesis,
        history_revision: snapshot.manifest.history_revision,
        shards: vec![receiver_indexer::application::LayoutShard {
            sealed: false,
            segments: vec![receiver_indexer::application::LayoutSegment {
                payments: snapshot.manifest.revision().unwrap(),
                witnesses: companion,
            }],
            filter: None,
        }],
    };
    let (id, bytes) = layout.encode().unwrap();
    std::fs::write(
        dir.join(format!("{}.inventory.json", hex::encode(id))),
        bytes,
    )
    .unwrap();
    let revision = hex::encode(snapshot.manifest.revision().unwrap());
    std::fs::write(
        dir.join(format!("{revision}.json")),
        serde_json::to_vec_pretty(&snapshot.manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.join(format!("{revision}.rows")), &snapshot.data).unwrap();
    if let Some(witness) = witness {
        let companion = hex::encode(companion);
        std::fs::write(dir.join(format!("{companion}.witness")), &witness.data).unwrap();
        std::fs::write(
            dir.join(format!("{companion}.witness.json")),
            serde_json::to_vec_pretty(&witness.manifest).unwrap(),
        )
        .unwrap();
    }
    (dir, hex::encode(id))
}

/// The layout identity of the inventory `health` reports serving, if any.
fn serving(health: &Value) -> Option<&str> {
    health["serving"].as_str()?;
    health["indexer"]["revision"].as_str()
}

/// Replaces the record atomically, as an operator does.
fn point(record: &Path, value: Value) {
    let temp = record.with_extension("tmp");
    std::fs::write(&temp, value.to_string()).unwrap();
    std::fs::rename(temp, record).unwrap();
}

struct Worker(std::process::Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Http(reqwest::Client);
impl Transport for Http {
    async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
        read(self.0.get(url), limit).await
    }
    async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
        read(self.0.post(url).body(body), limit).await
    }
}
async fn read(request: reqwest::RequestBuilder, limit: usize) -> Result<Vec<u8>, Error> {
    let response = request
        .send()
        .await
        .map_err(|e| Error::Transport(e.to_string()))?;
    match response.status() {
        reqwest::StatusCode::GONE => return Err(Error::Revision),
        status if !status.is_success() => return Err(Error::Transport(status.to_string())),
        _ => {}
    }
    let body = response
        .bytes()
        .await
        .map_err(|e| Error::Transport(e.to_string()))?;
    if body.len() > limit {
        return Err(Error::Malformed);
    }
    Ok(body.to_vec())
}

struct World {
    root: tempfile::TempDir,
    node: Node,
    record: PathBuf,
    config: PathBuf,
    origin: String,
    client: reqwest::Client,
}

impl World {
    async fn new() -> Self {
        let node = Node::default();
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc = format!("http://{}", socket.local_addr().unwrap());
        let app = Router::new()
            .route("/", post(rpc_handler))
            .with_state(node.clone());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let root = tempfile::tempdir().unwrap();
        let record = root.path().join("input/publication.json");
        std::fs::create_dir_all(record.parent().unwrap()).unwrap();
        let config = root.path().join("input/config.json");
        std::fs::write(
            &config,
            json!({
                "publication_record": record,
                "bind": "127.0.0.1:0",
                "state_dir": root.path().join("state"),
                "rpc_urls": [rpc],
                "no_auth": true,
                "poll_seconds": 1,
            })
            .to_string(),
        )
        .unwrap();
        Self {
            root,
            node,
            record,
            config,
            origin: String::new(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
        }
    }

    /// Starts the worker on a port of its own choosing and points `origin` at it.
    async fn start(&mut self) -> Worker {
        let log = self.root.path().join("worker.log");
        let from = std::fs::metadata(&log).map_or(0, |m| m.len() as usize);
        let worker = Worker(
            std::process::Command::new(env!("CARGO_BIN_EXE_receiver-directory"))
                .args(["serve-publication", "--config"])
                .arg(&self.config)
                // A partner key in the environment is never read in this mode.
                .env("NEAR_INTENTS_EXPLORER", "must-not-be-read")
                .env("RUST_LOG", "info")
                .stdout(std::process::Stdio::null())
                .stderr(
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&log)
                        .unwrap(),
                )
                .spawn()
                .unwrap(),
        );
        let address = bound::address(&log, from, "serve-publication listening").await;
        self.origin = format!("http://{address}");
        worker
    }

    async fn health(&self) -> Option<Value> {
        let response = self
            .client
            .get(format!("{}/v1/receiver/health", self.origin))
            .send()
            .await
            .ok()?;
        response.json().await.ok()
    }

    /// Polls health until `done` holds, recording every layout served meanwhile.
    async fn until(
        &self,
        what: &str,
        served: &mut Vec<String>,
        done: impl Fn(&Value) -> bool,
    ) -> Value {
        let deadline = std::time::Instant::now() + Duration::from_secs(150);
        loop {
            if let Some(health) = self.health().await {
                if let Some(id) = serving(&health) {
                    if served.last().map(String::as_str) != Some(id) {
                        served.push(id.to_string());
                    }
                }
                if done(&health) {
                    return health;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "{what}: {health:#}\n{}",
                    std::fs::read_to_string(self.root.path().join("worker.log"))
                        .unwrap_or_default()
                );
            } else {
                assert!(std::time::Instant::now() < deadline, "{what}: no health");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Points the record at a staged publication.
    fn assign(&self, dir: &Path, revision: &str) {
        point(
            &self.record,
            json!({"directory": dir, "revision": revision}),
        );
    }

    /// Looks the fixture receiver up over encrypted PIR at the served anchor.
    async fn lookup(&self, height: u32, hash: Hash) -> Vec<receiver_directory::Payment> {
        let accepted = history::Accepted {
            genesis: mainnet(),
            required_start: activation(),
            height,
            hash,
        };
        let http = Http(self.client.clone());
        let session = history::connect(&self.origin, http, accepted)
            .await
            .unwrap();
        history::inventory_history(&session.client, common::receiver())
            .await
            .unwrap()
    }
}

async fn rpc_handler(state: State<Node>, request: Json<Value>) -> Json<Value> {
    rpc(state, request).await
}

/// Payment session id of a staged publication's one segment.
fn session(snapshot: &Snapshot) -> String {
    hex::encode(
        receiver_pir::server::Server::new(snapshot.clone())
            .unwrap()
            .id(),
    )
}

fn refusal(health: &Value) -> (&str, &str) {
    let refusal = &health["publication"]["refusal"];
    (
        refusal["kind"].as_str().unwrap_or(""),
        refusal["message"].as_str().unwrap_or(""),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacing_the_record_during_the_canonical_rpc_prevents_activation() {
    let mut world = World::new().await;
    let (snapshot, witness) = publication(1, [0xa1; 32]);
    world.node.set(activation() + 1, [0xa1; 32]);
    let (dir, revision) = stage(world.root.path(), "candidate", &snapshot, Some(&witness));
    world.assign(&dir, &revision);
    *world.node.1.lock().unwrap() = Some(RecordReplacement {
        height: u64::from(activation() + 1),
        path: world.record.clone(),
        value: json!({"directory": dir, "revision": "invalid"}),
    });
    let _worker = world.start().await;
    let mut served = Vec::new();
    let health = world
        .until("replacement refused", &mut served, |h| {
            refusal(h).0 == "invalid_source"
        })
        .await;
    assert!(
        world.node.1.lock().unwrap().is_none(),
        "the record changed inside the canonical RPC"
    );
    assert!(
        health["serving"].is_null(),
        "the superseded publication must never activate"
    );
    assert!(
        served.is_empty(),
        "the superseded publication must never be observed serving"
    );
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StagedCase {
    Payments,
    WrongRevision,
    Partial,
    Network,
    Noncanonical,
    Witness,
    WitnessContext,
    WitnessSize,
    Rows,
    Relative,
    Rotation,
    LatestRecord,
    Restart,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_publication_serves_exact_encrypted_payments() {
    check_staged_publication(StagedCase::Payments).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_revision_keeps_the_valid_publication_serving() {
    check_staged_publication(StagedCase::WrongRevision).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_staging_keeps_the_valid_publication_serving() {
    check_staged_publication(StagedCase::Partial).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_network_keeps_the_valid_publication_serving() {
    check_staged_publication(StagedCase::Network).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_noncanonical_candidate_keeps_the_valid_publication_serving() {
    check_staged_publication(StagedCase::Noncanonical).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_witness_keeps_the_valid_publication_serving() {
    check_staged_publication(StagedCase::Witness).await;
}

/// A companion constructed at a later block than its payment table is activated only
/// once that block is canonical too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_noncanonical_witness_context_keeps_the_valid_publication_serving() {
    check_staged_publication(StagedCase::WitnessContext).await;
}

/// A companion whose construction block is canonical but whose claimed Ironwood tree
/// size after it is not the node's is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_witness_tree_size_keeps_the_valid_publication_serving() {
    check_staged_publication(StagedCase::WitnessSize).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_rows_keep_the_valid_publication_serving() {
    check_staged_publication(StagedCase::Rows).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relative_record_keeps_the_valid_publication_serving() {
    check_staged_publication(StagedCase::Relative).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_valid_rotation_retains_its_predecessor() {
    check_staged_publication(StagedCase::Rotation).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_latest_record_activates_after_a_reorg_during_retention() {
    check_staged_publication(StagedCase::LatestRecord).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_rechecks_the_record_and_reorgs_revoke_serving() {
    check_staged_publication(StagedCase::Restart).await;
}

/// A correction record that raises the history revision and then fails its rows is
/// refused once. Raising the revision revoked the served publication, and that
/// self-revocation is not mistaken for an outside one, so the same invalid record is
/// not loaded again on every poll.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_correction_is_loaded_once() {
    let mut world = World::new().await;
    let root = world.root.path().join("publications");
    let mut served = Vec::new();
    let (a, a_witness) = publication(1, [0xa1; 32]);
    world.node.set(activation() + 1, [0xa1; 32]);
    let (a_dir, a_rev) = stage(&root, "a", &a, Some(&a_witness));
    world.assign(&a_dir, &a_rev);
    let _worker = world.start().await;
    world
        .until("first publication", &mut served, |h| {
            serving(h) == Some(a_rev.as_str())
        })
        .await;
    let mut manifest = a.manifest.clone();
    manifest.history_revision = 1;
    let corrected = Snapshot {
        manifest,
        data: a.data.clone(),
    };
    let (dir, rev) = stage(&root, "corrected", &corrected, Some(&a_witness));
    let payments = hex::encode(corrected.manifest.revision().unwrap());
    let rows = dir.join(format!("{payments}.rows"));
    let mut bytes = std::fs::read(&rows).unwrap();
    bytes[0] ^= 1;
    std::fs::write(&rows, bytes).unwrap();
    world.assign(&dir, &rev);
    world
        .until("refused correction", &mut served, |h| {
            h["publication"]["assigned"]["revision"] == rev.as_str()
                && refusal(h).0 == "invalid_source"
        })
        .await;
    // Several record polls later the record has still been prepared only once.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let log = std::fs::read_to_string(world.root.path().join("worker.log")).unwrap();
    let prepared = log
        .lines()
        .filter(|l| l.contains("publication prepared") && l.contains(rev.as_str()))
        .count();
    assert_eq!(prepared, 1, "{log}");
}

/// A correction whose floor cannot be recorded yet is deferred, not refused: once the
/// state directory takes the floor again, the next poll activates it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unrecordable_floor_defers_the_correction() {
    let world = World::new().await;
    let root = world.root.path().join("publications");
    let mut served = Vec::new();
    let (a, a_witness) = publication(1, [0xa1; 32]);
    world.node.set(activation() + 1, [0xa1; 32]);
    let (a_dir, a_rev) = stage(&root, "a", &a, Some(&a_witness));
    world.assign(&a_dir, &a_rev);
    let _worker = world.start();
    world
        .until("first publication", &mut served, |h| {
            h["serving"] == session(&a)
        })
        .await;
    // A directory where the floor file goes makes recording it fail.
    let floor = world
        .root
        .path()
        .join("state")
        .join(receiver_indexer::serve_publication::HISTORY_FILE);
    std::fs::create_dir(&floor).unwrap();
    let (b, b_witness) = corrected_publication(2, [0xb2; 32], 1);
    world.node.set(activation() + 2, [0xb2; 32]);
    let (b_dir, b_rev) = stage(&root, "b", &b, Some(&b_witness));
    world.assign(&b_dir, &b_rev);
    world
        .until("deferred correction", &mut served, |h| {
            h["publication"]["assigned"]["revision"] == b_rev.as_str()
                && refusal(h).0 == "unavailable"
        })
        .await;
    std::fs::remove_dir(&floor).unwrap();
    world
        .until("corrected publication", &mut served, |h| {
            h["serving"] == session(&b)
        })
        .await;
}

async fn check_staged_publication(case: StagedCase) {
    let mut world = World::new().await;
    let root = world.root.path().join("publications");
    let mut served = Vec::new();

    // Fresh start: nothing is served until the recorded publication verifies and its
    // anchor is canonical.
    let (a, a_witness) = publication(1, [0xa1; 32]);
    world.node.set(activation() + 1, [0xa1; 32]);
    let (a_dir, a_rev) = stage(&root, "a", &a, Some(&a_witness));
    world.assign(&a_dir, &a_rev);
    let worker = world.start().await;
    let a_id = session(&a);
    // The record is reported loaded just after the publication starts serving, so
    // health can briefly show one without the other.
    let health = world
        .until("first publication", &mut served, |h| {
            serving(h) == Some(a_rev.as_str())
                && h["publication"]["loaded"]["revision"] == a_rev.as_str()
        })
        .await;
    assert_eq!(health["indexer"]["mode"], "serve-publication");
    if case == StagedCase::Payments {
        assert!(!a_dir.join(format!("{a_id}.filters")).exists());
        assert_eq!(
            world
                .client
                .get(format!("{}/v1/receiver/block-filters/head", world.origin))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        );
        let mut found = world.lookup(activation() + 1, [0xa1; 32]).await;
        found.sort_by_key(|payment| payment.position);
        let expected: Vec<_> = (0..2)
            .map(|page| {
                let mut payment = common::record(page, 2).payment;
                payment.height = activation() + 1;
                payment.block_hash = [0xa1; 32];
                payment.position = [0u64, 5][page as usize];
                payment.cmx = [[1u8; 32], [6u8; 32]][page as usize];
                payment
            })
            .collect();
        assert_eq!(found, expected, "the exact payments over encrypted PIR");

        return;
    }

    if case == StagedCase::Restart {
        drop(worker);
        std::fs::write(
            world.root.path().join("state/serve-publication.json"),
            json!({"loaded": {"directory": root.join("stale"), "revision": "00".repeat(32)}})
                .to_string(),
        )
        .unwrap();
        let _worker = world.start().await;
        world
            .until("restart", &mut served, |h| {
                serving(h) == Some(a_rev.as_str())
                    && h["publication"]["loaded"]["revision"] == a_rev.as_str()
            })
            .await;
        world.node.set(activation() + 1, [0x0a; 32]);
        let health = world
            .until("reorg after restart", &mut served, |h| {
                h["serving"].is_null() && refusal(h).0 == "noncanonical"
            })
            .await;
        assert!(health["epoch"].as_u64().unwrap() >= 1);
        let init = world
            .client
            .get(format!("{}/v1/receiver/inventory", world.origin))
            .send()
            .await
            .unwrap();
        assert_eq!(init.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        return;
    }

    // Every bad record is refused while the valid publication keeps serving.
    let refuse =
        |name: &'static str, kind: &'static str, contains: &'static str| (name, kind, contains);
    let (b, b_witness) = publication(2, [0xb2; 32]);
    world.node.set(activation() + 2, [0xb2; 32]);
    let (b_dir, b_rev) = stage(&root, "b", &b, Some(&b_witness));
    if !matches!(case, StagedCase::Rotation | StagedCase::LatestRecord) {
        // The wrong revision: B's files staged under a name the record expects.
        let wrong = root.join("wrong-revision");
        std::fs::create_dir_all(&wrong).unwrap();
        let other_rev = "cd".repeat(32);
        let b_segment = hex::encode(b.manifest.revision().unwrap());
        for entry in std::fs::read_dir(&b_dir).unwrap() {
            let path = entry.unwrap().path();
            std::fs::copy(&path, wrong.join(path.file_name().unwrap())).unwrap();
        }
        std::fs::copy(
            b_dir.join(format!("{b_rev}.inventory.json")),
            wrong.join(format!("{other_rev}.inventory.json")),
        )
        .unwrap();
        // Partial staging: the manifest without its rows.
        let partial = root.join("partial");
        std::fs::create_dir_all(&partial).unwrap();
        for name in [
            format!("{b_rev}.inventory.json"),
            format!("{b_segment}.json"),
        ] {
            std::fs::copy(b_dir.join(&name), partial.join(&name)).unwrap();
        }
        // Another network.
        let (mut other_net, _) = publication(2, [0xb2; 32]);
        other_net.manifest.genesis = [1; 32];
        let other_net = Snapshot::build(other_net.manifest, &[]).unwrap();
        let (net_dir, net_rev) = stage(&root, "network", &other_net, None);
        // An anchor the node does not have.
        let (fork, fork_witness) = publication(3, [0xf0; 32]);
        world.node.set(activation() + 3, [0xc3; 32]);
        let (fork_dir, fork_rev) = stage(&root, "fork", &fork, Some(&fork_witness));
        // A corrupt witness.
        let mut bad_witness = b_witness.clone();
        let last = bad_witness.data.len() - 1;
        bad_witness.data[last] ^= 1;
        let (witness_dir, witness_rev) = stage(&root, "witness", &b, Some(&bad_witness));
        // A companion constructed at a later block the node does not have.
        let mut later = b_witness.manifest.clone();
        (later.tree_height, later.tree_hash) = (activation() + 3, [0xe3; 32]);
        let payloads = b_witness.data[..2 * 512]
            .chunks(512)
            .map(|p| p.try_into().unwrap())
            .collect::<Vec<[u8; 512]>>();
        let late = witness::Table::build(later, &payloads).unwrap();
        let (late_dir, late_rev) = stage(&root, "late", &b, Some(&late));
        // A companion at a canonical later block, claiming a tree size the node does not
        // have after it.
        let mut sized = b_witness.manifest.clone();
        (sized.tree_height, sized.tree_hash) = (activation() + 3, [0xc3; 32]);
        world
            .node
            .2
            .lock()
            .unwrap()
            .insert(u64::from(activation() + 3), 9);
        let sized = witness::Table::build(sized, &payloads).unwrap();
        let (sized_dir, sized_rev) = stage(&root, "sized", &b, Some(&sized));
        // A same-length corrupt rows file.
        let (rows_dir, rows_rev) = stage(&root, "rows", &b, Some(&b_witness));
        let rows_path = rows_dir.join(format!("{b_segment}.rows"));
        let mut rows = std::fs::read(&rows_path).unwrap();
        rows[0] ^= 1;
        std::fs::write(&rows_path, rows).unwrap();
        let mut checked = false;
        for (variant, (dir, rev), (name, kind, contains)) in [
            (
                StagedCase::WrongRevision,
                (wrong.clone(), other_rev.clone()),
                refuse("wrong revision", "invalid_source", "record expects"),
            ),
            (
                StagedCase::Partial,
                (partial.clone(), b_rev.clone()),
                refuse("partial staging", "invalid_source", "rows"),
            ),
            (
                StagedCase::Network,
                (net_dir, net_rev),
                refuse("wrong network", "invalid_source", "not for mainnet"),
            ),
            (
                StagedCase::Noncanonical,
                (fork_dir, fork_rev),
                refuse("noncanonical anchor", "noncanonical", "canonical chain"),
            ),
            (
                StagedCase::Witness,
                (witness_dir, witness_rev),
                refuse("corrupt witness", "invalid_source", "witness"),
            ),
            (
                StagedCase::WitnessContext,
                (late_dir, late_rev),
                refuse(
                    "noncanonical witness context",
                    "noncanonical",
                    "canonical chain",
                ),
            ),
            (
                StagedCase::WitnessSize,
                (sized_dir, sized_rev),
                refuse("wrong witness tree size", "invalid_source", "tree size"),
            ),
            (
                StagedCase::Rows,
                (rows_dir, rows_rev),
                refuse("corrupt rows", "invalid_source", "publication"),
            ),
        ] {
            if case != variant {
                continue;
            }
            checked = true;
            world.assign(&dir, &rev);
            let health = world
                .until(name, &mut served, |h| {
                    h["publication"]["assigned"]["revision"] == rev.as_str()
                        && h["publication"]["assigned"]["directory"] == dir.to_str().unwrap()
                        && !refusal(h).0.is_empty()
                })
                .await;
            assert_eq!(refusal(&health).0, kind, "{name}: {health:#}");
            assert!(refusal(&health).1.contains(contains), "{name}: {health:#}");
            assert_eq!(
                serving(&health),
                Some(a_rev.as_str()),
                "{name} left the valid publication serving"
            );
        }
        if case == StagedCase::Relative {
            checked = true;
            point(
                &world.record,
                json!({"directory": "relative", "revision": a_rev}),
            );
            world
                .until("malformed record", &mut served, |h| {
                    refusal(h).1.contains("absolute") && serving(h) == Some(a_rev.as_str())
                })
                .await;
        }
        assert!(checked, "a refusal scenario must exercise its bad record");
        return;
    }

    // A valid update activates; the displaced inventory is retained.
    world.assign(&b_dir, &b_rev);
    let b_id = session(&b);
    world
        .until("update", &mut served, |h| {
            serving(h) == Some(b_rev.as_str())
        })
        .await;
    let old = world
        .client
        .get(format!("{}/v1/receiver/public/{a_id}", world.origin))
        .send()
        .await
        .unwrap();
    assert!(
        old.status().is_success(),
        "the predecessor serves while retained"
    );
    if case == StagedCase::Rotation {
        assert_eq!(world.lookup(activation() + 2, [0xb2; 32]).await.len(), 2);
        return;
    }

    // Rapid replacement while B's predecessor is retained: C is superseded by D
    // before it can activate. A confirmed reorg of B, found by the independent guard
    // while D waits, revokes everything; D, still canonical, then activates.
    let (c, c_witness) = publication(3, [0xc3; 32]);
    world.node.set(activation() + 3, [0xc3; 32]);
    let (c_dir, c_rev) = stage(&root, "c", &c, Some(&c_witness));
    let (d, d_witness) = publication(4, [0xd4; 32]);
    world.node.set(activation() + 4, [0xd4; 32]);
    let (d_dir, d_rev) = stage(&root, "d", &d, Some(&d_witness));
    world.assign(&c_dir, &c_rev);
    world.assign(&d_dir, &d_rev);
    let pending = world
        .until("D pending", &mut served, |h| {
            h["publication"]["preparing"]["revision"] == d_rev.as_str()
                || h["publication"]["assigned"]["revision"] == d_rev.as_str()
        })
        .await;
    world.node.set(activation() + 2, [0x0b; 32]);
    // D's own anchors do not include B's block, so D may activate before the guard
    // finds the reorg; it then serves again once that revocation has run.
    let epoch = pending["epoch"].as_u64().unwrap();
    world
        .until("reorg then D", &mut served, |h| {
            serving(h) == Some(d_rev.as_str()) && h["epoch"].as_u64() > Some(epoch)
        })
        .await;
    assert!(
        !served.contains(&c_rev),
        "a superseded record never activates"
    );
    let gone = world
        .client
        .get(format!("{}/v1/receiver/public/{b_id}", world.origin))
        .send()
        .await
        .unwrap();
    assert_eq!(
        gone.status(),
        reqwest::StatusCode::GONE,
        "the reorged revision is revoked"
    );

    drop(worker);
}

#[tokio::test]
async fn check_mode_verifies_without_serving() {
    let world = World::new().await;
    let (a, a_witness) = publication(1, [0xa1; 32]);
    let (a_dir, a_rev) = stage(&world.root.path().join("p"), "a", &a, Some(&a_witness));
    world.assign(&a_dir, &a_rev);
    let run = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_receiver-directory"))
            .args(["serve-publication", "--check", "--config"])
            .arg(&world.config)
            .output()
            .unwrap()
    };
    let output = run();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["revision"], a_rev);
    assert!(
        !world.root.path().join("state").exists(),
        "check writes nothing"
    );
    let companion = hex::encode(a_witness.manifest.revision().unwrap());
    std::fs::remove_file(a_dir.join(format!("{companion}.witness"))).unwrap();
    let output = run();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("witness: "));
}
