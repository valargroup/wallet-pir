//! Public preparation bytes shared by the encrypted HTTP and hint checks.
use enhance_publisher::control::{Group, Ledger, Replica};
use enhance_query_server::{worker::Worker, EnhanceRecord, EnhanceRecordParts};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

pub(super) struct Listening {
    pub(super) url: String,
    task: tokio::task::JoinHandle<()>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Listening {
    pub(super) async fn stop(mut self) {
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
pub(super) async fn serve(router: axum::Router) -> Listening {
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
pub(super) fn record(position: u64) -> Vec<u8> {
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

pub(super) async fn workers(
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

// The original record fixture above also drives the independent generic-hint
// oracle. Each worker and oracle has its own writable artifact directory.
pub(super) static HINT_FIXTURES: std::sync::Mutex<Vec<(u64, tempfile::TempDir)>> =
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
    if let Some(prepared) = shared_hint_fixture(plan) {
        let cache = prepared.parent().unwrap();
        // Publish complete immutable material atomically. Workers copy the
        // bytes and verify their own files; the independent oracle cold-builds.
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(cache.join("enhance-hints.lock"))
            .unwrap();
        lock.lock().unwrap();
        if !prepared.exists() {
            let temporary = tempfile::tempdir_in(cache).unwrap();
            build_hint_fixture(temporary.path(), plan);
            std::fs::rename(temporary.path(), &prepared).unwrap();
        }
        copy_hint_fixture(&prepared.join("artifacts-9"), &root.join("artifacts-9"));
        return;
    }
    static CLEANUP: std::sync::Once = std::sync::Once::new();
    CLEANUP.call_once(|| {
        // SAFETY: only owned test directories are dropped, after test threads exit.
        assert_eq!(unsafe { libc::atexit(cleanup_hint_fixtures) }, 0);
    });
    let mut fixtures = HINT_FIXTURES.lock().unwrap();
    if !fixtures.iter().any(|(records, _)| *records == count) {
        let fixture = tempfile::tempdir().unwrap();
        build_hint_fixture(fixture.path(), plan);
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

pub(super) fn shared_hint_fixture(
    plan: &enhance_query_server::runtime::DomainPlan,
) -> Option<std::path::PathBuf> {
    use sha2::{Digest, Sha256};
    std::env::var_os("WALLET_PIR_TEST_RUNTIME_CACHE").map(|root| {
        std::path::PathBuf::from(root).join(format!(
            "enhance-hints-{}-{}-{}",
            cfg!(feature = "native-reinspiring"),
            cfg!(feature = "cuda"),
            hex::encode(Sha256::digest(serde_json::to_vec(plan).unwrap()))
        ))
    })
}

fn build_hint_fixture(root: &std::path::Path, plan: &enhance_query_server::runtime::DomainPlan) {
    let (_, outcome) = enhance_query_server::runtime::Engine::new(root)
        .prepare(plan.clone(), |start, n| {
            Ok((start..start + n as u64).flat_map(record).collect())
        })
        .unwrap();
    assert_eq!(outcome.built, plan.units.len());
}
