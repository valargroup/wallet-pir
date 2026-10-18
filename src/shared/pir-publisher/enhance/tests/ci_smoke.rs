//! A bounded encrypted round trip through real listeners, including a durable restart.
use enhance_publisher::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
    store::RecordJournal,
};
use enhance_query_server::{client::EnhancePirClient, EnhanceRecord, EnhanceRecordParts};
use enhance_query_server::{
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
        metadata: enhance_query_server::EnhanceTransactionMetadata::new(
            position as u32,
            Some(position),
        )
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
        let worker_root = root.join(format!("worker-{index}"));
        let worker = Worker::open(&worker_root).unwrap();
        let rejected = reject_commit.clone();
        let router = worker.router().layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let rejected = rejected.clone();
                let worker_root = worker_root.clone();
                async move {
                    use axum::response::IntoResponse;
                    if index == 0
                        && request.uri().path() == "/internal/commit"
                        && rejected.load(Ordering::SeqCst)
                    {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                    } else if request.uri().path() == "/internal/prepare" {
                        let (parts, body) = request.into_parts();
                        let bytes = axum::body::to_bytes(body, 1 << 20).await.unwrap();
                        let candidate: enhance_query_server::worker::Candidate =
                            serde_json::from_slice(&bytes).unwrap();
                        for plan in candidate.plans {
                            let root = worker_root.clone();
                            tokio::task::spawn_blocking(move || {
                                seed_hint_fixture(&root, plan.shard.records, &plan);
                            })
                            .await
                            .unwrap();
                        }
                        next.run(axum::extract::Request::from_parts(
                            parts,
                            axum::body::Body::from(bytes),
                        ))
                        .await
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
        assert_eq!(
            current
                .query_position_with_timing(66)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(66)
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
        assert_eq!(
            client
                .query_position_with_timing(0)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(0)
        );
        assert_eq!(
            client
                .query_position_with_timing(67)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(67)
        );
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

// The original record fixture above also drives the independent generic-hint
// oracle. Each worker and oracle has its own writable artifact directory.
static HINT_FIXTURES: std::sync::Mutex<Vec<(u64, tempfile::TempDir)>> =
    std::sync::Mutex::new(Vec::new());

extern "C" fn cleanup_hint_fixtures() {
    HINT_FIXTURES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

fn copy_hint_fixture(source: &std::path::Path, target: &std::path::Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let destination = target.join(entry.file_name());
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            copy_hint_fixture(&entry.path(), &destination);
        } else {
            assert!(kind.is_file(), "hint fixtures contain only regular files");
            // Never replace a file a live runtime may have mapped, or repair a
            // deliberately damaged file before the production loader sees it.
            if !destination.exists() {
                std::fs::copy(entry.path(), destination).unwrap();
            }
        }
    }
}

fn seed_hint_fixture(
    root: &std::path::Path,
    count: u64,
    plan: &enhance_query_server::runtime::DomainPlan,
) {
    static CLEANUP: std::sync::Once = std::sync::Once::new();
    CLEANUP.call_once(|| {
        // SAFETY: only owned test directories are dropped, after test threads exit.
        assert_eq!(unsafe { libc::atexit(cleanup_hint_fixtures) }, 0);
    });
    let mut fixtures = HINT_FIXTURES.lock().unwrap();
    if !fixtures.iter().any(|(records, _)| *records == count) {
        let fixture = tempfile::tempdir().unwrap();
        let (_, outcome) = enhance_query_server::runtime::Engine::new(fixture.path())
            .prepare(plan.clone(), |start, n| {
                Ok((start..start + n as u64).flat_map(record).collect())
            })
            .unwrap();
        assert_eq!(outcome.built, plan.units.len());
        fixtures.push((count, fixture));
    }
    let source = &fixtures
        .iter()
        .find(|(records, _)| *records == count)
        .unwrap()
        .1;
    // Copy bytes, never handles or verification results. The real worker loader
    // verifies its own private copy, and the generic oracle cold-builds separately.
    copy_hint_fixture(
        &source.path().join("artifacts-9"),
        &root.join("artifacts-9"),
    );
}

async fn prepare_hint_fixture(
    root: &std::path::Path,
    count: u64,
) -> (Listening, enhance_query_server::worker::Candidate) {
    use enhance_query_server::protocol::{Geometry, Lifecycle};
    use enhance_query_server::{runtime, worker::Candidate};
    let (mut listeners, _) = workers(root, 1, Arc::new(AtomicBool::new(false))).await;
    let listening = listeners.pop().unwrap();
    let shard = Lifecycle::default()
        .coverage(count, Geometry::default())
        .unwrap()
        .shards[0]
        .clone();
    let source = |start, n| Ok((start..start + n as u64).flat_map(record).collect());
    let plan = runtime::plan(shard, source).unwrap();
    let candidate = Candidate {
        placement_policy: Default::default(),
        operation: "hint-proof".into(),
        attempt: 1,
        epoch: 1,
        expected_revision: 0,
        generation: 1,
        plans: vec![plan.clone()],
    };
    let http = reqwest::Client::new();
    hint_command(&http, &listening.url, "reserve", &candidate).await;
    for (spec, identity) in plan.shard.units.iter().zip(&plan.units) {
        let rows = runtime::unit_rows(&plan.shard, spec, &mut { source }).unwrap();
        http.put(format!(
            "{}/internal/rows/{}",
            listening.url,
            identity.digest()
        ))
        .header("x-enhance-epoch", "1")
        .header("x-enhance-attempt", "1")
        .header("x-enhance-operation", &candidate.operation)
        .body(rows)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    }
    hint_command(&http, &listening.url, "prepare", &candidate).await;
    (listening, candidate)
}

async fn hint_command(
    http: &reqwest::Client,
    url: &str,
    command: &str,
    body: &impl serde::Serialize,
) {
    http.post(format!("{url}/internal/{command}"))
        .json(body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

async fn hint_matches_generic(count: u64) {
    use enhance_query_server::{runtime::Engine, wire::encode_crs_blocks};
    #[cfg(not(feature = "native-reinspiring"))]
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir().unwrap();
    let (worker, candidate) = prepare_hint_fixture(root.path(), count).await;
    let oracle_root = tempfile::tempdir().unwrap();
    let oracle = Engine::new(oracle_root.path())
        .prepare(candidate.plans[0].clone(), |start, n| {
            Ok((start..start + n as u64).flat_map(record).collect())
        })
        .unwrap()
        .0;
    // Exercise the unchanged decoder/combiner/writer on independently built
    // material, rather than comparing a publication with its own serialization.
    let expected = encode_crs_blocks(&oracle.hint().unwrap());
    let http = reqwest::Client::new();
    let response = http
        .get(format!("{}/internal/hint/0", worker.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert!(response.headers().get("content-length").is_none());
    let actual = response.bytes().await.unwrap();
    assert_eq!(actual.as_ref(), expected);
    #[cfg(not(feature = "native-reinspiring"))]
    assert_eq!(
        hex::encode(Sha256::digest(&actual)),
        match count {
            // Immutable original 67/68 smoke material retained by the timing
            // diagnosis, before the transport optimization.
            67 => "c6020b03b132c41dbb3d7f85b97c1dcc6381e8f2ec2db8c98ac89f8db7e5b70a",
            68 => "12f1a4de5fa69d4f37d86c98c8e7e2b7f37ec3d2613b638d90075437e7825fc0",
            _ => unreachable!(),
        }
    );
    assert_eq!(
        std::fs::read_dir(root.path().join("worker-0/hints"))
            .unwrap()
            .count(),
        0
    );
    drop(http);
    worker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_unit_hint_matches_independent_generic_67() {
    hint_matches_generic(67).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_unit_hint_matches_independent_generic_68() {
    hint_matches_generic(68).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_unit_hint_rejects_damage_and_pins_retired_file() {
    use enhance_query_server::{
        ipir::shard_artifact_dir,
        worker::{Abort, Retain},
    };
    use std::io::{Seek, SeekFrom, Write};
    let root = tempfile::tempdir().unwrap();
    let (worker, mut candidate) = prepare_hint_fixture(root.path(), 67).await;
    let worker_root = root.path().join("worker-0");
    let artifact_root = worker_root
        .join("artifacts-9")
        .join(candidate.plans[0].units[0].digest());
    let path = shard_artifact_dir(&artifact_root, DatabaseId::Enhance, 0).join("partial-crs.bin");
    let original = std::fs::read(&path).unwrap();
    let http = reqwest::Client::new();
    let url = format!("{}/internal/hint/0", worker.url);
    // Damage the real owning worker's pinned material after successful prepare.
    // Each retry restores immutable material and obtains a fresh verified handle.
    for fault in ["corrupt", "truncated", "shape", "trailing", "format"] {
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        match fault {
            "truncated" => file.set_len(original.len() as u64 - 1).unwrap(),
            "trailing" => {
                file.seek(SeekFrom::End(0)).unwrap();
                file.write_all(&[0]).unwrap();
            }
            _ => {
                let offset = match fault {
                    "shape" => 8,
                    "format" => 0,
                    _ => original.len() - 1,
                };
                file.seek(SeekFrom::Start(offset as u64)).unwrap();
                file.write_all(&[original[offset] ^ 1]).unwrap();
            }
        }
        drop(file);
        let response = http.get(&url).send().await.unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "{fault}"
        );
        let _ = response.bytes().await.unwrap();
        hint_command(
            &http,
            &worker.url,
            "abort",
            &Abort {
                epoch: candidate.epoch,
                operation: candidate.operation.clone(),
                attempt: candidate.attempt,
            },
        )
        .await;
        // Atomic replacement, never mutate the diagnostic author's raw inputs.
        let replacement = path.with_extension("replacement");
        std::fs::write(&replacement, &original).unwrap();
        std::fs::rename(replacement, &path).unwrap();
        candidate.attempt += 1;
        hint_command(&http, &worker.url, "reserve", &candidate).await;
        hint_command(&http, &worker.url, "prepare", &candidate).await;
    }
    // Disconnect does not poison healthy shared validation state.
    drop(http.get(&url).send().await.unwrap());
    // A pinned, verified unit remains readable without its original pathname.
    std::fs::remove_file(&path).unwrap();
    let response = http.get(&url).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    hint_command(
        &http,
        &worker.url,
        "abort",
        &Abort {
            epoch: candidate.epoch,
            operation: candidate.operation.clone(),
            attempt: candidate.attempt,
        },
    )
    .await;
    // Abort drops the candidate runtime; the response owns only its pinned file.
    // The manual unlink above must not stand in for retention's directory GC.
    assert!(
        artifact_root.is_dir(),
        "unit directory must exist before GC"
    );
    hint_command(
        &http,
        &worker.url,
        "retain",
        &Retain {
            epoch: 1,
            generations: vec![1],
        },
    )
    .await;
    assert!(
        !artifact_root.exists(),
        "retirement must reclaim the unit directory"
    );
    assert_eq!(response.bytes().await.unwrap().as_ref(), original);
    // A saved path alone cannot authorize serving, even if it contains valid bytes.
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, &original).unwrap();
    let response = http.get(&url).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let _ = response.bytes().await.unwrap();
    // Corruption, unlinking and retirement touched only this worker's copy.
    {
        let fixtures = HINT_FIXTURES.lock().unwrap();
        let source = &fixtures.iter().find(|(count, _)| *count == 67).unwrap().1;
        let retained = source.path().join(path.strip_prefix(&worker_root).unwrap());
        assert_eq!(std::fs::read(retained).unwrap(), original);
    }
    drop(http);
    worker.stop().await;
}
