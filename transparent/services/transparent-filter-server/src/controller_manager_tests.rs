//! Real publisher/TLS/manager/owner/recent-worker boundaries, with source rows
//! as an independent answer oracle. These do not qualify a complete provider.
use super::*;
use transparent_shard_server::{live::Command, manager::Manager, service::ServiceConfig};

// Prepare immutable producer bytes once per test process. Each test receives
// separate copies: corruption, missing-file and restart cases must stay isolated.
// Static destructors do not run at process exit. Register only roots we own,
// and drop them after libtest joins its test threads (also on a failed test).
// SIGKILL/abort still requires the runner's enclosing scratch-directory cleanup.
static PREPARED_FIXTURES: std::sync::Mutex<Vec<tempfile::TempDir>> =
    std::sync::Mutex::new(Vec::new());

extern "C" fn cleanup_prepared_fixtures() {
    PREPARED_FIXTURES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

fn copy_fixture_tree(source: &Path, target: &Path) {
    std::fs::create_dir(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = target.join(entry.file_name());
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            copy_fixture_tree(&entry.path(), &target);
        } else {
            assert!(
                kind.is_file(),
                "producer fixture must contain regular files"
            );
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

// Share the owner and exit registration, not construction or verification results.
// The cleanup controls below exercise this helper with tiny ordinary files.
fn owned_fixture(build: impl FnOnce(&Path)) -> PathBuf {
    static CLEANUP: std::sync::Once = std::sync::Once::new();
    CLEANUP.call_once(|| {
        // SAFETY: the callback has C ABI, cannot unwind, and only drops
        // registered TempDirs after libtest joins its threads.
        assert_eq!(unsafe { libc::atexit(cleanup_prepared_fixtures) }, 0);
    });
    let root = tempfile::Builder::new()
        .prefix("provider-prepared-fixture-")
        .tempdir()
        .unwrap();
    build(root.path());
    let path = root.path().to_path_buf();
    PREPARED_FIXTURES.lock().unwrap().push(root);
    path
}

fn prepared_fixture(through: u64) -> &'static Path {
    fn build(through: u64) -> PathBuf {
        owned_fixture(|root| {
            let (directory, set) =
                publication_fixture(root, through, !cfg!(feature = "qualification-tests"));
            prepare_recent(&directory, &set);
        })
    }
    static FIRST: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| build(2));
    static NEXT: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| build(3));
    match through {
        2 => &FIRST,
        3 => &NEXT,
        _ => panic!("no prepared publication fixture through height {through}"),
    }
}

// Reuse only the immutable publisher side. Every manager still owns a fresh
// consumer directory and verifies all bytes over its own real HTTP boundary.
// Corruption and changing-holdings cases use independently copied files below.
struct ProducerFixture {
    directory: PathBuf,
    set: Arc<ShardSet>,
    distribution: Finalized,
}
pub(super) fn immutable_publication(through: u64) -> (PathBuf, Arc<ShardSet>, Finalized) {
    fn build(through: u64) -> ProducerFixture {
        let directory = prepared_fixture(through).join("publication");
        let set = ShardSet::open(&directory, 0).unwrap();
        let distribution = Finalized::prepare(&directory, &set, 2).unwrap();
        ProducerFixture {
            directory,
            set: Arc::new(set),
            distribution,
        }
    }
    static FIRST: std::sync::LazyLock<ProducerFixture> = std::sync::LazyLock::new(|| build(2));
    static NEXT: std::sync::LazyLock<ProducerFixture> = std::sync::LazyLock::new(|| build(3));
    let fixture = match through {
        2 => &*FIRST,
        3 => &*NEXT,
        _ => panic!("unknown producer height"),
    };
    (
        fixture.directory.clone(),
        fixture.set.clone(),
        fixture.distribution.clone(),
    )
}

// Use the real helper in a fresh libtest process, with no runner-owned TMPDIR
// cleanup. Observe the directory only after the process and exit callback finish.
fn fixture_exit_control(both: bool, fail: bool) {
    let child = format!(
        "{}::prepared_fixtures_exit_child",
        module_path!().split_once("::").unwrap().1
    );
    let scratch = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([&child, "--exact", "--test-threads=1", "--nocapture"])
        .env(
            "WALLET_PIR_FIXTURE_EXIT_CHILD",
            if fail {
                "fail"
            } else if both {
                "both"
            } else {
                "first"
            },
        )
        .env("TMPDIR", scratch.path())
        .env("TMP", scratch.path())
        .env("TEMP", scratch.path())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(if fail { 101 } else { 0 }),
        "child output: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains(if fail {
        "1 failed"
    } else {
        "1 passed"
    }));
    assert!(String::from_utf8_lossy(&output.stdout)
        .contains(&format!("prepared fixtures: {}", if both { 2 } else { 1 })));
    if fail {
        assert!(String::from_utf8_lossy(&output.stderr)
            .contains("intentional child failure after preparing fixtures"));
    }
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

// Only an explicitly selected private entry consumes the child mode. Public
// controls always spawn a fresh process and check its scratch after exit, even
// when their caller inherited the internal environment variable.
#[test]
fn prepared_fixtures_exit_child() {
    let name = format!(
        "{}::prepared_fixtures_exit_child",
        module_path!().split_once("::").unwrap().1
    );
    let args: Vec<_> = std::env::args().collect();
    // The private caller uses --option=value switches. A runner partition can
    // also use --exact, but its multiple case filters are not child entry mode.
    let filters = args
        .iter()
        .skip(1)
        .filter(|arg| !arg.starts_with('-'))
        .collect::<Vec<_>>();
    if !args.iter().any(|arg| arg == "--exact") || filters != [&name] {
        return;
    }
    let (both, fail) = match std::env::var("WALLET_PIR_FIXTURE_EXIT_CHILD").as_deref() {
        Ok("first") => (false, false),
        Ok("both") => (true, false),
        Ok("fail") => (true, true),
        other => panic!("missing child mode: {other:?}"),
    };
    fn fixture(through: u64) -> &'static Path {
        fn build(through: u64) -> PathBuf {
            owned_fixture(|root| {
                let publication = root.join("publication");
                std::fs::create_dir(&publication).unwrap();
                std::fs::write(publication.join("shards.json"), through.to_string()).unwrap();
            })
        }
        static FIRST: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| build(2));
        static NEXT: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| build(3));
        if through == 2 {
            &FIRST
        } else {
            &NEXT
        }
    }
    let first = tempfile::tempdir().unwrap();
    let directory = first.path().join("publication");
    copy_fixture_tree(&fixture(2).join("publication"), &directory);
    let copy = tempfile::tempdir().unwrap();
    let copy_directory = copy.path().join("publication");
    copy_fixture_tree(&fixture(2).join("publication"), &copy_directory);
    let map = std::fs::read(copy_directory.join("shards.json")).unwrap();
    std::fs::write(directory.join("shards.json"), b"corrupt").unwrap();
    assert_eq!(
        std::fs::read(copy_directory.join("shards.json")).unwrap(),
        map
    );
    let fresh = tempfile::tempdir().unwrap();
    let fresh_directory = fresh.path().join("publication");
    copy_fixture_tree(&fixture(2).join("publication"), &fresh_directory);
    assert_eq!(
        std::fs::read(fresh_directory.join("shards.json")).unwrap(),
        map
    );
    if both {
        assert_ne!(
            std::fs::read(fixture(3).join("publication/shards.json")).unwrap(),
            map
        );
    }
    assert_eq!(
        PREPARED_FIXTURES.lock().unwrap().len(),
        if both { 2 } else { 1 }
    );
    println!("prepared fixtures: {}", if both { 2 } else { 1 });
    assert!(!fail, "intentional child failure after preparing fixtures");
}

#[test]
fn prepared_fixture_child_mode_requires_a_single_explicit_entry() {
    let prefix = module_path!().split_once("::").unwrap().1;
    let child = format!("{prefix}::prepared_fixtures_exit_child");
    let control = format!("{prefix}::prepared_fixtures_exit_first_only");
    for exact_first in [true, false] {
        let scratch = tempfile::tempdir().unwrap();
        let filters = if exact_first {
            vec!["--exact", child.as_str(), control.as_str()]
        } else {
            vec![child.as_str(), control.as_str(), "--exact"]
        };
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(filters)
            .args(["--test-threads=1", "--nocapture"])
            .env("WALLET_PIR_FIXTURE_EXIT_CHILD", "fail")
            .env("TMPDIR", scratch.path())
            .env("TMP", scratch.path())
            .env("TEMP", scratch.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("2 passed"));
        assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    }
}

#[test]
fn prepared_fixtures_exit_first_only() {
    fixture_exit_control(false, false);
}

#[test]
fn prepared_fixtures_exit_both() {
    fixture_exit_control(true, false);
}

#[test]
fn prepared_fixtures_exit_failed_test() {
    fixture_exit_control(true, true);
}

#[test]
fn fixture_copies_are_independent_and_refuse_symlinks() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("bytes"), b"immutable").unwrap();
    let first = root.path().join("first");
    let second = root.path().join("second");
    copy_fixture_tree(&source, &first);
    copy_fixture_tree(&source, &second);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = |path: &Path| std::fs::metadata(path.join("bytes")).unwrap();
        assert_ne!(metadata(&source).ino(), metadata(&first).ino());
        assert_ne!(metadata(&first).ino(), metadata(&second).ino());
        assert_eq!(metadata(&first).nlink(), 1);
    }
    std::fs::write(first.join("bytes"), b"corrupt").unwrap();
    assert_eq!(std::fs::read(source.join("bytes")).unwrap(), b"immutable");
    assert_eq!(std::fs::read(second.join("bytes")).unwrap(), b"immutable");
    std::fs::remove_file(first.join("bytes")).unwrap();
    assert_eq!(std::fs::read(second.join("bytes")).unwrap(), b"immutable");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(source.join("bytes"), source.join("link")).unwrap();
        assert!(std::panic::catch_unwind(|| {
            copy_fixture_tree(&source, &root.path().join("refused"));
        })
        .is_err());
    }
}

fn manager(tls: &Tls, root: &Path) -> Manager {
    Manager::new(
        config(tls, root.to_path_buf()),
        ServiceConfig {
            cache_bytes: 4 << 30,
            ..ServiceConfig::default()
        },
    )
    .unwrap()
}
async fn restart_manager(tls: &Tls, root: &Path) -> Manager {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    loop {
        match Manager::new(
            config(tls, root.to_path_buf()),
            ServiceConfig {
                cache_bytes: 4 << 30,
                ..ServiceConfig::default()
            },
        ) {
            Ok(manager) => return manager,
            Err(error)
                if error.to_string() == "another publication consumer owns this directory"
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            Err(error) => panic!("retired HTTP owner did not release its directory: {error}"),
        }
    }
}
async fn worker(
    live: transparent_shard_server::live::LiveService,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, live.router()).await.unwrap();
    });
    (origin, task)
}
async fn get(origin: &str, path: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("{origin}{path}"))
        .send()
        .await
        .unwrap()
}
fn artifact_requests(tls: &Tls) -> usize {
    std::fs::read_to_string(tls.control.with_extension("requests"))
        .unwrap()
        .lines()
        .filter(|s| s.contains("/artifacts/"))
        .count()
}
fn advance(owner: &Authority, directory: &Path, distribution: Finalized) {
    let mut public = read_public(directory.to_path_buf(), Vec::new()).unwrap();
    public.distribution = Some(distribution);
    let _gate = owner.0.publication_gate.lock().unwrap();
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    *owner.0.active.write().unwrap() = Arc::new(public);
}
// This asks for an occupied producer row, re-deriving the native profile
// independently and comparing decrypted bytes to the original publication.
async fn query(origin: &str, set: &ShardSet) {
    drop(query_and_upload(origin, set).await);
}
async fn query_and_upload(origin: &str, set: &ShardSet) -> Vec<u8> {
    query_with_reply(origin, set).await.0
}
async fn query_with_reply(origin: &str, set: &ShardSet) -> (Vec<u8>, Vec<u8>) {
    use transparent_server_backend::shardset::{Table, TableRef};
    let shard = set.get(1).unwrap();
    let source = shard
        .segment(TableRef::Directory, 0)
        .unwrap()
        .load()
        .unwrap();
    let width = shard.geometry.directory_row_bytes;
    let row = source
        .chunks_exact(width)
        .position(|r| r.iter().any(|b| *b != 0))
        .unwrap();
    let profile = transparent_native::TableProfile::new(
        transparent_shard::manifest::SCHEMA,
        shard.geometry.name,
        "directory",
        Table::Directory.rows(shard.geometry),
        width as u32,
    )
    .unwrap();
    let setup: serde_json::Value = get(
        origin,
        &format!("/v2/shards/1/revisions/{}/setup/directory/0", shard.digest),
    )
    .await
    .json()
    .await
    .unwrap();
    let params = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        setup["public_params"].as_str().unwrap(),
    )
    .unwrap();
    let (secret, upload) = profile.prepare(row).unwrap();
    let binding = transparent_shard::manifest::query_binding(&shard.digest, "directory");
    let mut body = binding.to_vec();
    body.extend(upload);
    let response = reqwest::Client::new()
        .post(format!(
            "{origin}/v2/shards/1/revisions/{}/query/directory",
            shard.digest
        ))
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.bytes().await.unwrap();
    assert_eq!(&bytes[..8], &binding);
    let decoded = profile.decode(&secret, &params, &bytes[16..]).unwrap();
    assert_eq!(decoded, source[row * width..(row + 1) * width]);
    (body, bytes.to_vec())
}

// A fresh query is decoded against producer bytes before it can be repeated.
// Reuse stays within one test and one immutable revision; every check still
// sends an actual HTTP request and compares the complete encrypted answer.
struct CheckedAnswer {
    path: String,
    body: Vec<u8>,
    reply: Vec<u8>,
}
impl CheckedAnswer {
    async fn new(origin: &str, set: &ShardSet) -> Self {
        let (body, reply) = query_with_reply(origin, set).await;
        Self {
            path: format!(
                "/v2/shards/1/revisions/{}/query/directory",
                set.get(1).unwrap().digest
            ),
            body,
            reply,
        }
    }
    async fn check(&self, origin: &str) {
        let response = reqwest::Client::new()
            .post(format!("{origin}{}", self.path))
            .header(reqwest::header::CONNECTION, "close")
            .body(self.body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            self.reply.as_slice()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_reuses_immutable_transfers_and_serves_through_failed_update() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set, distribution) = immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(distribution), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let state = root.path().join("provider");
    let mut follow = manager(&tls, &state);
    let first = follow.poll_once().await.unwrap();
    assert!(first.downloaded_files > 0);
    let live = follow.live().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let app = follow.router().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let observed: serde_json::Value = get(&origin, "/v1/provider-status")
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(observed["synchronization"]["state"], "synchronized");
    assert_eq!(
        observed["verified_publication"]["map_sha256"],
        set.map_digest
    );
    assert_eq!(observed["runtime_readiness"]["required"], true);
    assert_eq!(observed["archive_reachability"], serde_json::Value::Null);
    let original_answer = CheckedAnswer::new(&origin, &set).await;
    let count = artifact_requests(&tls);
    let unchanged = follow.poll_once().await.unwrap();
    assert!(unchanged.unchanged);
    assert_eq!(unchanged.downloaded_bytes, 0);
    assert_eq!(count, artifact_requests(&tls));
    // Missing many revisions does not require a retained delta base.
    owner.0.distribution_revision.store(40, Ordering::Release);
    let reused = follow.poll_once().await.unwrap();
    assert_eq!(reused.revision, 40);
    assert_eq!(reused.downloaded_files, 0);
    assert_eq!(count, artifact_requests(&tls));
    let next_root = root.path().join("next");
    std::fs::create_dir(&next_root).unwrap();
    let (next_dir, next_set, next_distribution) = immutable_publication(3);
    advance(&owner, &next_dir, next_distribution);
    for fault in ["truncated", "corrupt", "missing"] {
        std::fs::write(&tls.control, fault).unwrap();
        assert!(follow.poll_once().await.is_err(), "{fault}");
        let observed: serde_json::Value = get(&origin, "/v1/provider-status")
            .await
            .json()
            .await
            .unwrap();
        assert_eq!(observed["synchronization"]["state"], "refused");
        assert_eq!(
            observed["verified_publication"]["map_sha256"],
            set.map_digest
        );
        assert_eq!(observed["runtime_readiness"]["warm"], true);
        assert_eq!(
            live.command(Command::Status).await.unwrap()["active"]["map_sha256"],
            set.map_digest
        );
        original_answer.check(&origin).await;
    }
    // A restart during a failed update recovers the complete predecessor only
    // after live source checks; failure of the new transfer still leaves it up.
    task.abort();
    let _ = task.await;
    drop(live);
    drop(follow);
    std::fs::write(&tls.control, "missing").unwrap();
    let mut follow = restart_manager(&tls, &state).await;
    assert!(follow.poll_once().await.is_err());
    let live = follow
        .live()
        .expect("freshly checked predecessor survives restart/update failure");
    let (origin, task) = worker(live.clone()).await;
    original_answer.check(&origin).await;
    std::fs::write(&tls.control, "").unwrap();
    let updated = follow.poll_once().await.unwrap();
    assert!(updated.downloaded_files > 0);
    assert!(
        updated.downloaded_files < first.downloaded_files,
        "unchanged archive metadata is reused"
    );
    assert_eq!(
        live.command(Command::Status).await.unwrap()["active"]["map_sha256"],
        next_set.map_digest
    );
    let next_answer = CheckedAnswer::new(&origin, &next_set).await;
    // Old exact sessions retain their source after normal handover.
    original_answer.check(&origin).await;
    // The newest range is not cumulative: the old retained tail can be
    // orphaned while its newer replacement is positively canonical.
    owner.0.distribution_revision.fetch_add(5, Ordering::AcqRel);
    owner.0.invalidated_from.store(4, Ordering::Release);
    *rpc.hash.write().unwrap() = Some("77".repeat(32));
    follow.poll_once().await.unwrap();
    next_answer.check(&origin).await;
    assert_eq!(
        get(
            &origin,
            &format!(
                "/v2/shards/1/revisions/{}/setup/directory/0",
                set.get(1).unwrap().digest
            )
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    // A later good same-revision answer cannot resurrect that retired view.
    *rpc.hash.write().unwrap() = Some(set.map.shards.last().unwrap().terminal_block_hash.clone());
    assert!(follow.poll_once().await.unwrap().unchanged);
    assert_eq!(
        get(
            &origin,
            &format!(
                "/v2/shards/1/revisions/{}/setup/directory/0",
                set.get(1).unwrap().digest
            )
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    let status = live.command(Command::Status).await.unwrap();
    assert_eq!(status["runtimes"]["built"], 0);
    assert_eq!(status["runtimes"]["shipped_fallbacks"], 0);
    rpc.available.store(false, Ordering::Release);
    assert!(follow.poll_once().await.is_err());
    next_answer.check(&origin).await;
    std::fs::write(&tls.control, "refused-body").unwrap();
    assert!(follow.poll_once().await.is_err());
    next_answer.check(&origin).await;
    task.abort();
    let _ = task.await;
    drop(live);
    drop(follow);
    std::fs::write(&tls.control, "").unwrap();
    rpc.available.store(true, Ordering::Release);
    std::fs::write(state.join("active.invalid.json"), b"[]").unwrap();
    let mut restarted = restart_manager(&tls, &state).await;
    assert!(restarted.live().is_none());
    let restored = restarted.poll_once().await.unwrap();
    assert_eq!(restored.downloaded_files, 0);
    let status = restarted
        .live()
        .unwrap()
        .command(Command::Status)
        .await
        .unwrap();
    assert_eq!(status["active"]["map_sha256"], next_set.map_digest);
    assert!(status["revoked_digests"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!(set.get(1).unwrap().digest)));
    assert_eq!(status["runtimes"]["built"], 0);
    assert_eq!(status["runtimes"]["shipped_fallbacks"], 0);
    let (origin, task) = worker(restarted.live().unwrap()).await;
    next_answer.check(&origin).await;
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_persists_invalidation_and_refuses_equal_revision_reenable_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set, distribution) = immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(distribution), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let state = root.path().join("provider");
    let mut follow = manager(&tls, &state);
    follow.poll_once().await.unwrap();
    let live = follow.live().unwrap();
    let (origin, task) = worker(live.clone()).await;
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    owner.0.invalidated_from.store(2, Ordering::Release);
    owner.0.withdrawn.store(true, Ordering::Release);
    // An intent-write failure must still fence memory and the owner's own
    // durable record. Startup then reconciles the two records conservatively.
    std::fs::create_dir(state.join("revocation-floor.tmp")).unwrap();
    assert!(follow.poll_once().await.is_err());
    assert_eq!(
        get(&origin, "/v2/shards/init").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(state.join("active.invalid.json").exists());
    let intent: serde_json::Value =
        serde_json::from_slice(&std::fs::read(state.join("revocation-floor.json")).unwrap())
            .unwrap();
    assert_eq!(intent["revision"], 0);
    std::fs::remove_dir(state.join("revocation-floor.tmp")).unwrap();
    task.abort();
    let _ = task.await;
    drop(live);
    drop(follow);
    owner.0.withdrawn.store(false, Ordering::Release);
    owner.0.invalidated_from.store(u64::MAX, Ordering::Release);
    let mut restarted = manager(&tls, &state);
    assert!(restarted.poll_once().await.is_err());
    assert!(restarted.live().is_none());
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    restarted.poll_once().await.unwrap();
    let live = restarted.live().unwrap();
    let (origin, task) = worker(live.clone()).await;
    query(&origin, &set).await;
    // A later range above the retained anchor cannot hide a missed reorg.
    owner.0.distribution_revision.fetch_add(5, Ordering::AcqRel);
    owner.0.invalidated_from.store(3, Ordering::Release);
    *rpc.hash.write().unwrap() = Some("77".repeat(32));
    assert!(restarted.poll_once().await.is_err());
    assert_eq!(
        get(&origin, "/v2/shards/init").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(live.command(Command::Status).await.unwrap()["invalidated"]
        .as_bool()
        .unwrap());
    task.abort();
    let _ = task.await;
}

async fn entered(tls: &Tls) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !tls.control.with_extension("entered").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_restarts_interrupted_staging_and_rejects_superseded_activation() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set, distribution) = immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(distribution), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let state = root.path().join("provider");
    std::fs::write(&tls.control, "pause-artifact").unwrap();
    let mut follow = manager(&tls, &state);
    let work = tokio::spawn(async move { follow.poll_once().await });
    entered(&tls).await;
    assert!(!state.join("active.json").exists());
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    std::fs::write(&tls.control, "").unwrap();
    let mut restarted = manager(&tls, &state);
    restarted.poll_once().await.unwrap();
    let live = restarted.live().unwrap();
    let (origin, task) = worker(live.clone()).await;
    query(&origin, &set).await;
    let next_root = root.path().join("next");
    std::fs::create_dir(&next_root).unwrap();
    let (next_dir, next_set, next_distribution) = immutable_publication(3);
    advance(&owner, &next_dir, next_distribution);
    let _ = std::fs::remove_file(tls.control.with_extension("entered"));
    std::fs::write(&tls.control, "pause-artifact").unwrap();
    let work = tokio::spawn(async move {
        let result = restarted.poll_once().await;
        (restarted, result)
    });
    entered(&tls).await;
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    query(&origin, &set).await;
    std::fs::write(&tls.control, "").unwrap();
    let (mut restarted, result) = work.await.unwrap();
    assert!(result.unwrap_err().to_string().contains("superseded"));
    assert_eq!(
        live.command(Command::Status).await.unwrap()["active"]["map_sha256"],
        set.map_digest
    );
    query(&origin, &set).await;
    // A durable pointer failure exercises the real activation path after all
    // fetching, strict loading and authority checks, leaving the old view live.
    std::fs::remove_file(state.join("active.json")).unwrap();
    std::fs::create_dir(state.join("active.json")).unwrap();
    assert!(restarted.poll_once().await.is_err());
    assert_eq!(
        live.command(Command::Status).await.unwrap()["active"]["map_sha256"],
        set.map_digest
    );
    query(&origin, &set).await;
    std::fs::remove_dir(state.join("active.json")).unwrap();
    let _ = std::fs::remove_file(tls.control.with_extension("entered"));
    std::fs::write(&tls.control, "pause-activation").unwrap();
    let work = tokio::spawn(async move { restarted.poll_once().await });
    entered(&tls).await;
    assert!(live.command(Command::Status).await.unwrap()["candidate"].is_object());
    query(&origin, &set).await;
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    task.abort();
    let _ = task.await;
    drop(live);
    std::fs::write(&tls.control, "").unwrap();
    // Saved staging/candidate state is not authority. Source outage on restart
    // cannot expose it, even though every immutable object is now cached.
    let mut restarted = manager(&tls, &state);
    rpc.available.store(false, Ordering::Release);
    assert!(restarted.poll_once().await.is_err());
    assert!(restarted.live().is_none());
    rpc.available.store(true, Ordering::Release);
    let done = restarted.poll_once().await.unwrap();
    assert_eq!(done.downloaded_files, 0);
    let (origin, task) = worker(restarted.live().unwrap()).await;
    query(&origin, &next_set).await;
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_checks_inventory_and_required_files_on_unchanged_maps() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set, distribution) = immutable_publication(2);
    let original = distribution;
    let runtime = original
        .inventory()
        .artifacts
        .iter()
        .find(|a| a.kind == Kind::RecentRuntime)
        .unwrap()
        .clone();
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(original), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let state = root.path().join("provider");
    let mut follow = manager(&tls, &state);
    follow.poll_once().await.unwrap();
    let live = follow.live().unwrap();
    let (origin, task) = worker(live.clone()).await;
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    std::fs::write(&tls.control, "missing-runtime").unwrap();
    assert!(follow.poll_once().await.is_err());
    query(&origin, &set).await;
    let status = live.command(Command::Status).await.unwrap();
    assert_eq!(status["last_preparation"]["outcome"], "runtime");
    assert_eq!(status["runtimes"]["built"], 0);
    assert_eq!(status["runtimes"]["shipped_fallbacks"], 0);
    std::fs::write(&tls.control, "").unwrap();
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    follow.poll_once().await.unwrap();
    // Rehash cached files, then charge the actual replacement transfer. A bad
    // cache object plus bad download cannot borrow a resident runtime to pass.
    std::fs::write(state.join("objects").join(&runtime.sha256), b"corrupt").unwrap();
    std::fs::write(&tls.control, "corrupt").unwrap();
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    assert!(follow.poll_once().await.is_err());
    query(&origin, &set).await;
    std::fs::write(&tls.control, "").unwrap();
    let repaired = follow.poll_once().await.unwrap();
    assert_eq!(repaired.downloaded_files, 1);
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_refuses_changed_holdings_on_an_unchanged_map() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set) = publication(root.path());
    prepare_recent(&directory, &set);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(
        &directory,
        Some(Finalized::prepare(&directory, &set, 2).unwrap()),
        rpc.client.clone(),
    );
    let tls = tls(root.path(), owner.clone()).await;
    let state = root.path().join("provider");
    let mut follow = manager(&tls, &state);
    follow.poll_once().await.unwrap();
    let live = follow.live().unwrap();
    let (origin, task) = worker(live.clone()).await;
    // Changing only holdings is a full inventory change, never an unchanged map
    // shortcut. PR2 preserves the existing owner assignment fence.
    let prepared =
        transparent_server_backend::prebuild::prepare_publication(&set, &directory, &[], &|| false)
            .unwrap();
    assert!(prepared.failures.is_empty());
    let mut public = read_public(directory.clone(), Vec::new()).unwrap();
    public.distribution = Some(Finalized::prepare(&directory, &set, 0).unwrap());
    *owner.0.active.write().unwrap() = Arc::new(public);
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    let failure = follow.poll_once().await.unwrap_err();
    assert!(
        failure
            .to_string()
            .contains(transparent_shard_server::live::ASSIGNMENT_CHANGED),
        "{failure}"
    );
    query(&origin, &set).await;
    task.abort();
    let _ = task.await;
}

// A validated withdrawal must reach the owner even when any one control-record
// write fails. Run the same probe at the ordinary and both activation polls.
async fn control_write_failure(pause_at: Option<u32>, failed_record: &str) {
    let high_water_fault = failed_record.split_once(':');
    let failed_records = if let Some((_, record)) = high_water_fault {
        vec![record]
    } else if failed_record == "both" {
        vec![
            "publisher-high-water.tmp",
            "publisher-high-water-fallback.tmp",
            "revocation-floor.tmp",
        ]
    } else if failed_record == "malformed" {
        vec!["revocation-floor.json"]
    } else {
        vec![failed_record]
    };
    let root = tempfile::tempdir().unwrap();
    let (directory, set, distribution) = immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(distribution), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let state = root.path().join("provider");
    if high_water_fault.is_some() {
        owner.0.distribution_revision.store(10, Ordering::Release);
    }
    let mut follow = manager(&tls, &state);
    follow.poll_once().await.unwrap();
    let live = follow.live().unwrap();
    let (origin, task) = worker(live.clone()).await;
    let answer = CheckedAnswer::new(&origin, &set).await;
    let saved_high_water = std::fs::read(state.join("publisher-high-water.json")).unwrap();
    if let Some((_, record)) = high_water_fault {
        if record.contains("fallback") {
            std::fs::write(state.join(record), &saved_high_water).unwrap();
        }
    }
    if failed_record == "both" {
        owner.0.distribution_revision.store(99, Ordering::Release);
    }
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    let result = if let Some(at) = pause_at {
        std::fs::write(&tls.control, format!("pause-current-{at}")).unwrap();
        let work = tokio::spawn(async move {
            let result = follow.poll_once().await;
            (follow, result)
        });
        entered(&tls).await;
        for record in &failed_records {
            if failed_record == "malformed"
                || high_water_fault.is_some_and(|(kind, _)| kind == "parse")
            {
                std::fs::write(state.join(record), b"{").unwrap();
                continue;
            }
            if record.ends_with(".json") {
                std::fs::remove_file(state.join(record)).unwrap();
            }
            std::fs::create_dir(state.join(record)).unwrap();
        }
        owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
        // Exercise withdrawal alone at the first guard and an applicable range
        // while the publication remains eligible at the final guard.
        owner.0.withdrawn.store(at == 2, Ordering::Release);
        owner
            .0
            .invalidated_from
            .store(if at == 2 { u64::MAX } else { 2 }, Ordering::Release);
        std::fs::write(&tls.control, "").unwrap();
        let (returned, result) = work.await.unwrap();
        follow = returned;
        result
    } else {
        for record in &failed_records {
            if failed_record == "malformed"
                || high_water_fault.is_some_and(|(kind, _)| kind == "parse")
            {
                std::fs::write(state.join(record), b"{").unwrap();
                continue;
            }
            if record.ends_with(".json") {
                std::fs::remove_file(state.join(record)).unwrap();
            }
            std::fs::create_dir(state.join(record)).unwrap();
        }
        if let Some((_, failed)) = high_water_fault {
            // A broken safety file is not permission to call every error a
            // revocation: outage, malformed transport, wrong network, replay,
            // and a contradictory READABLE companion all preserve active A.
            rpc.available.store(false, Ordering::Release);
            assert!(follow.poll_once().await.is_err());
            rpc.available.store(true, Ordering::Release);
            assert_eq!(
                get(&origin, "/v2/shards/init").await.status(),
                StatusCode::OK
            );
            std::fs::write(&tls.control, "malformed-current").unwrap();
            assert!(follow.poll_once().await.is_err());
            std::fs::write(&tls.control, "").unwrap();
            assert_eq!(
                get(&origin, "/v2/shards/init").await.status(),
                StatusCode::OK
            );
            *rpc.genesis.write().unwrap() = "ff".repeat(32);
            assert!(follow.poll_once().await.is_err());
            *rpc.genesis.write().unwrap() = set.map.genesis_hash.clone();
            assert_eq!(
                get(&origin, "/v2/shards/init").await.status(),
                StatusCode::OK
            );
            owner.0.distribution_revision.store(9, Ordering::Release);
            owner.0.withdrawn.store(true, Ordering::Release);
            assert!(follow.poll_once().await.is_err());
            assert_eq!(
                get(&origin, "/v2/shards/init").await.status(),
                StatusCode::OK
            );
            owner.0.distribution_revision.store(11, Ordering::Release);
            let other = if failed.contains("fallback") {
                "publisher-high-water.json"
            } else {
                "publisher-high-water-fallback.json"
            };
            let mut contradiction: serde_json::Value =
                serde_json::from_slice(&saved_high_water).unwrap();
            contradiction["current"]["revision"] = serde_json::json!(12);
            std::fs::write(
                state.join(other),
                serde_json::to_vec(&contradiction).unwrap(),
            )
            .unwrap();
            assert!(follow.poll_once().await.is_err());
            assert_eq!(
                get(&origin, "/v2/shards/init").await.status(),
                StatusCode::OK
            );
            // A readable origin mismatch also remains a refusal, never evidence.
            contradiction["current"]["revision"] = serde_json::json!(10);
            contradiction["origin"] = serde_json::json!("https://another.invalid/");
            std::fs::write(
                state.join(other),
                serde_json::to_vec(&contradiction).unwrap(),
            )
            .unwrap();
            assert!(follow.poll_once().await.is_err());
            assert_eq!(
                get(&origin, "/v2/shards/init").await.status(),
                StatusCode::OK
            );
            std::fs::write(state.join(other), &saved_high_water).unwrap();
            // Reuse an independently validated encrypted answer while storage
            // is still broken, proving the controls did not silently fence it.
            answer.check(&origin).await;
        }
        owner.0.withdrawn.store(true, Ordering::Release);
        owner.0.invalidated_from.store(2, Ordering::Release);
        follow.poll_once().await
    };
    let error = result.unwrap_err().to_string();
    if let Some((kind, record)) = high_water_fault {
        assert!(
            error.contains(if kind == "read" { "directory" } else { "EOF" }),
            "original high-water storage error: {error}"
        );
        // A new Source has no trusted in-process baseline. The same malformed
        // or unreadable record must refuse before constructing any owner.
        let startup_root = root.path().join("startup-provider");
        std::fs::create_dir(&startup_root).unwrap();
        if kind == "read" {
            std::fs::create_dir(startup_root.join(record)).unwrap();
        } else {
            std::fs::write(startup_root.join(record), b"{").unwrap();
        }
        let mut startup = manager(&tls, &startup_root);
        assert!(startup.poll_once().await.is_err());
        assert!(startup.live().is_none());
    }
    assert_eq!(
        get(&origin, "/v2/shards/init").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    // Reuse the exact valid encrypted request whose answer was independently
    // checked before invalidation. A binding/framing error cannot fake refusal.
    let response = reqwest::Client::new()
        .post(format!(
            "{origin}/v2/shards/1/revisions/{}/query/directory",
            set.get(1).unwrap().digest
        ))
        .body(answer.body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    for record in &failed_records {
        if failed_record == "malformed" || high_water_fault.is_some_and(|(kind, _)| kind == "parse")
        {
            std::fs::remove_file(state.join(record)).unwrap();
        } else {
            std::fs::remove_dir(state.join(record)).unwrap();
        }
        if high_water_fault.is_some() {
            std::fs::write(state.join(record), &saved_high_water).unwrap();
        }
    }
    owner.0.withdrawn.store(false, Ordering::Release);
    owner.0.invalidated_from.store(u64::MAX, Ordering::Release);
    assert!(follow.poll_once().await.is_err(), "same-revision reenable");
    task.abort();
    let _ = task.await;
    drop(live);
    drop(follow);
    let mut restarted = manager(&tls, &state);
    assert!(
        restarted.poll_once().await.is_err(),
        "restart cannot erase revocation"
    );
    assert!(restarted.live().is_none());
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    restarted.poll_once().await.unwrap();
    let (origin, task) = worker(restarted.live().unwrap()).await;
    answer.check(&origin).await;
    task.abort();
    let _ = task.await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_high_water_write_failure_still_fences_http() {
    control_write_failure(None, "publisher-high-water.tmp").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_activation_poll_write_failure_still_fences_http() {
    control_write_failure(Some(2), "publisher-high-water.tmp").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_final_poll_write_failure_still_fences_http() {
    control_write_failure(Some(3), "publisher-high-water.tmp").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_owner_fence_write_failure_still_fences_http() {
    control_write_failure(None, "active.invalid.tmp").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_bounds_memory_while_retaining_admitted_upload() {
    use tokio::io::AsyncWriteExt;
    use tower::ServiceExt;
    use transparent_server_backend::shardset::{Table, TableRef};
    let root = tempfile::tempdir().unwrap();
    let (directory, set, distribution) = immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(distribution), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let state = root.path().join("provider");
    let mut follow = manager(&tls, &state);
    follow.poll_once().await.unwrap();
    let live = follow.live().unwrap();
    let (origin, task) = worker(live.clone()).await;
    let shard = set.get(1).unwrap();
    let width = shard.geometry.directory_row_bytes;
    let profile = transparent_native::TableProfile::new(
        transparent_shard::manifest::SCHEMA,
        shard.geometry.name,
        "directory",
        Table::Directory.rows(shard.geometry),
        width as u32,
    )
    .unwrap();
    let setup: serde_json::Value = get(
        &origin,
        &format!("/v2/shards/1/revisions/{}/setup/directory/0", shard.digest),
    )
    .await
    .json()
    .await
    .unwrap();
    let params = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        setup["public_params"].as_str().unwrap(),
    )
    .unwrap();
    let (secret, upload) = profile.prepare(0).unwrap();
    let binding = transparent_shard::manifest::query_binding(&shard.digest, "directory");
    let mut body = binding.to_vec();
    body.extend(upload);
    let (mut writer, reader) = tokio::io::duplex(body.len());
    let request = axum::http::Request::post(format!(
        "/v2/shards/1/revisions/{}/query/directory",
        shard.digest
    ))
    .header("content-length", body.len())
    .body(axum::body::Body::from_stream(
        tokio_util::io::ReaderStream::new(reader),
    ))
    .unwrap();
    let mut admitted = Box::pin(live.router().oneshot(request));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut admitted)
            .await
            .is_err()
    );
    // Same immutable map, new complete owner snapshots: no expensive fixture
    // growth, and no helper-only fake activation or explicit Collect command.
    for _ in 0..7 {
        owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
        follow.poll_once().await.unwrap();
        let status = live.command(Command::Status).await.unwrap();
        assert!(status["retired_snapshots"].as_u64().unwrap() <= 4);
    }
    assert_eq!(
        live.command(Command::Status).await.unwrap()["retired_snapshots"],
        4,
        "held oldest plus three recent"
    );
    writer.write_all(&body).await.unwrap();
    drop(writer);
    let response = admitted.await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decoded = profile.decode(&secret, &params, &bytes[16..]).unwrap();
    let source = shard
        .segment(TableRef::Directory, 0)
        .unwrap()
        .load()
        .unwrap();
    assert_eq!(decoded, source[..width]);
    assert!(follow.poll_once().await.unwrap().unchanged);
    assert_eq!(
        live.command(Command::Status).await.unwrap()["retired_snapshots"],
        3
    );
    query(&origin, &set).await;
    assert!(state.join("objects").is_dir());
    assert!(
        std::fs::read_dir(&state)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with("staging-"))
            .count()
            >= 8,
        "memory collection does not delete disk trees"
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_unreadable_revocation_floor_still_fences_http() {
    control_write_failure(None, "revocation-floor.json").await;
}

async fn recovery_poll_write_failure(pause_at: u32) {
    let root = tempfile::tempdir().unwrap();
    let (directory, set, distribution) = immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(distribution), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let state = root.path().join("provider");
    let mut follow = manager(&tls, &state);
    follow.poll_once().await.unwrap();
    drop(follow);
    let mut restarted = manager(&tls, &state);
    std::fs::write(&tls.control, format!("pause-current-{pause_at}")).unwrap();
    let work = tokio::spawn(async move {
        let result = restarted.poll_once().await;
        (restarted, result)
    });
    entered(&tls).await;
    std::fs::create_dir(state.join("publisher-high-water.tmp")).unwrap();
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    owner.0.withdrawn.store(true, Ordering::Release);
    owner.0.invalidated_from.store(2, Ordering::Release);
    std::fs::write(&tls.control, "").unwrap();
    let (restarted, result) = work.await.unwrap();
    assert!(result.is_err());
    assert!(
        restarted.live().is_none(),
        "recovery cannot expose the saved view"
    );
    let floor: serde_json::Value =
        serde_json::from_slice(&std::fs::read(state.join("revocation-floor.json")).unwrap())
            .unwrap();
    assert_eq!(floor["revision"], 2);
    assert!(floor["digests"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!(set.get(1).unwrap().digest)));
    drop(restarted);
    std::fs::remove_dir(state.join("publisher-high-water.tmp")).unwrap();
    owner.0.withdrawn.store(false, Ordering::Release);
    owner.0.invalidated_from.store(u64::MAX, Ordering::Release);
    let mut restarted = manager(&tls, &state);
    assert!(restarted.poll_once().await.is_err());
    assert!(restarted.live().is_none());
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    restarted.poll_once().await.unwrap();
    let (origin, task) = worker(restarted.live().unwrap()).await;
    query(&origin, &set).await;
    task.abort();
    let _ = task.await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_recovery_poll_write_failure_keeps_saved_view_unexposed() {
    recovery_poll_write_failure(2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_recovery_final_poll_write_failure_keeps_saved_view_unexposed() {
    recovery_poll_write_failure(3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_missing_durable_revision_cannot_reenable_revoked_view() {
    // Lose both exact-revision writes, retaining only the owner's digest fence.
    // A large missed-revision jump prevents an old high-water-plus-one shortcut.
    control_write_failure(None, "both").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_malformed_revocation_floor_still_fences_http() {
    control_write_failure(None, "malformed").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_rejects_replayed_different_map_below_durable_revocation_floor() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set, distribution) = immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(distribution), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let state = root.path().join("provider");
    let mut follow = manager(&tls, &state);
    follow.poll_once().await.unwrap();
    let (origin, task) = worker(follow.live().unwrap()).await;
    std::fs::create_dir(state.join("publisher-high-water.tmp")).unwrap();
    std::fs::create_dir(state.join("publisher-high-water-fallback.tmp")).unwrap();
    std::fs::create_dir(state.join("revocation-floor.tmp")).unwrap();
    owner.0.distribution_revision.store(100, Ordering::Release);
    owner.0.withdrawn.store(true, Ordering::Release);
    owner.0.invalidated_from.store(2, Ordering::Release);
    assert!(follow.poll_once().await.is_err());
    assert_eq!(
        get(&origin, "/v2/shards/init").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    task.abort();
    let _ = task.await;
    drop(follow);
    std::fs::remove_dir(state.join("publisher-high-water.tmp")).unwrap();
    std::fs::remove_dir(state.join("publisher-high-water-fallback.tmp")).unwrap();
    std::fs::remove_dir(state.join("revocation-floor.tmp")).unwrap();
    let next_root = root.path().join("next");
    std::fs::create_dir(&next_root).unwrap();
    let (next_dir, next_set, next_distribution) = immutable_publication(3);
    advance(&owner, &next_dir, next_distribution);
    owner.0.distribution_revision.store(40, Ordering::Release);
    owner.0.withdrawn.store(false, Ordering::Release);
    owner.0.invalidated_from.store(u64::MAX, Ordering::Release);
    let mut restarted = manager(&tls, &state);
    for revision in [40, 100] {
        owner
            .0
            .distribution_revision
            .store(revision, Ordering::Release);
        assert!(
            restarted.poll_once().await.is_err(),
            "a different map cannot bypass the saved revision100 floor at{revision}"
        );
        assert!(restarted.live().is_none());
    }
    owner.0.distribution_revision.store(101, Ordering::Release);
    restarted.poll_once().await.unwrap();
    let (origin, task) = worker(restarted.live().unwrap()).await;
    query(&origin, &next_set).await;
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_primary_read_failure_at_poll_still_fences_http() {
    control_write_failure(None, "read:publisher-high-water.json").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_primary_read_failure_at_guard_still_fences_http() {
    control_write_failure(Some(2), "read:publisher-high-water.json").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_primary_parse_failure_at_poll_still_fences_http() {
    control_write_failure(None, "parse:publisher-high-water.json").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_primary_parse_failure_at_guard_still_fences_http() {
    control_write_failure(Some(3), "parse:publisher-high-water.json").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_fallback_read_failure_at_poll_still_fences_http() {
    control_write_failure(None, "read:publisher-high-water-fallback.json").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_fallback_read_failure_at_guard_still_fences_http() {
    control_write_failure(Some(2), "read:publisher-high-water-fallback.json").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_fallback_parse_failure_at_poll_still_fences_http() {
    control_write_failure(None, "parse:publisher-high-water-fallback.json").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_fallback_parse_failure_at_guard_still_fences_http() {
    control_write_failure(Some(3), "parse:publisher-high-water-fallback.json").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_double_intent_write_failure_retains_exact_negative_revision() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set, distribution) = immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(distribution), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let state = root.path().join("provider");
    let mut follow = manager(&tls, &state);
    follow.poll_once().await.unwrap();
    let (origin, task) = worker(follow.live().unwrap()).await;
    let encrypted = query_and_upload(&origin, &set).await;
    for name in ["revocation-floor.tmp", "revocation-fallback.tmp"] {
        std::fs::create_dir(state.join(name)).unwrap();
    }
    owner.0.distribution_revision.store(100, Ordering::Release);
    std::fs::write(&tls.control, "pause-anchor").unwrap();
    let work = tokio::spawn(async move {
        let result = follow.poll_once().await;
        (follow, result)
    });
    entered(&tls).await;
    // The actual producer already returned eligible Current100. Its delegated
    // source now rejects the exact anchor without changing that revision.
    *rpc.hash.write().unwrap() = Some("ff".repeat(32));
    std::fs::write(&tls.control, "").unwrap();
    let (mut follow, result) = work.await.unwrap();
    let error = result.unwrap_err().to_string();
    assert!(
        error.contains("directory"),
        "original intent storage error: {error}"
    );
    let high_water: serde_json::Value =
        serde_json::from_slice(&std::fs::read(state.join("publisher-high-water.json")).unwrap())
            .unwrap();
    assert_eq!(high_water["current"]["revision"], 100);
    assert_eq!(high_water["current"]["eligibility"], "canonical");
    let intent: serde_json::Value =
        serde_json::from_slice(&std::fs::read(state.join("revocation-floor.json")).unwrap())
            .unwrap();
    assert_eq!(
        intent["revision"], 0,
        "both exact intent writes really failed"
    );
    assert_eq!(
        get(&origin, "/v2/shards/init").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    for name in ["revocation-floor.tmp", "revocation-fallback.tmp"] {
        std::fs::remove_dir(state.join(name)).unwrap();
    }
    *rpc.hash.write().unwrap() = Some(set.map.shards.last().unwrap().terminal_block_hash.clone());
    assert!(
        follow.poll_once().await.is_err(),
        "same revision cannot erase exact-anchor negative"
    );
    assert_eq!(
        get(&origin, "/v2/shards/init").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let response = reqwest::Client::new()
        .post(format!(
            "{origin}/v2/shards/1/revisions/{}/query/directory",
            set.get(1).unwrap().digest
        ))
        .body(encrypted)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    owner.0.distribution_revision.store(101, Ordering::Release);
    follow.poll_once().await.unwrap();
    query(&origin, &set).await;
    task.abort();
    let _ = task.await;
}

/// Standard controller/followed-journal adoption with the production sealing
/// policy, original mixed geometries, and an independent native row oracle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standard_history_lifecycle_queries_advance_and_reconciles_lost_activation() {
    use std::os::unix::fs::PermissionsExt;
    use transparent_shard_server::{
        live::{LiveService, Publication},
        service::ServiceState,
        shardset::LoadScope,
    };
    let root = tempfile::tempdir().unwrap();
    let (directory, set) = publication_through(root.path(), 2);
    let service_config = ServiceConfig {
        cache_bytes: 4 << 30,
        ..ServiceConfig::default()
    };
    let mut options = recent_options(&set);
    options.retain_revisions = transparent_shard_server::shardset::DEFAULT_RETAIN_REVISIONS;
    let state = ServiceState::build(
        ShardSet::open_with(&directory, &options).unwrap(),
        service_config,
    )
    .unwrap();
    state.spawn_prewarm().await.unwrap();
    let live = LiveService::new(
        state,
        Publication {
            directory: directory.clone(),
            assignment: None,
            map_sha256: set.map_digest.clone(),
            runtimes: None,
        },
        service_config,
        options,
        root.path().join("worker-active.json"),
    )
    .unwrap();
    type FleetState = (LiveService, Arc<AtomicBool>);
    async fn fleet_http(
        State((live, lose_reply)): State<FleetState>,
        axum::Json(request): axum::Json<serde_json::Value>,
    ) -> axum::Json<serde_json::Value> {
        let expected = live.command(Command::Status).await.unwrap()["active"]["map_sha256"]
            .as_str()
            .unwrap()
            .to_string();
        let command = match request["operation"].as_str().unwrap() {
            "prepare" => {
                let directory = PathBuf::from(request["directory"].as_str().unwrap());
                let set = ShardSet::open(
                    &directory,
                    transparent_shard_server::shardset::DEFAULT_RETAIN_REVISIONS,
                )
                .unwrap();
                let LoadScope::Assigned { assignment, .. } = recent_options(&set).scope else {
                    unreachable!()
                };
                let path = directory.join("fixture-assignment.json");
                std::fs::write(&path, serde_json::to_vec(assignment.as_ref()).unwrap()).unwrap();
                Command::Prepare {
                    expected,
                    publication: Publication {
                        directory,
                        assignment: Some(path),
                        map_sha256: request["map_sha256"].as_str().unwrap().into(),
                        runtimes: None,
                    },
                }
            }
            "activate" => Command::Activate {
                expected,
                map_sha256: request["map_sha256"].as_str().unwrap().into(),
            },
            other => panic!("unexpected fleet operation {other}"),
        };
        let result = live.command(command).await;
        if request["operation"] == "activate" && lose_reply.swap(false, Ordering::AcqRel) {
            assert!(result.is_ok());
            return axum::Json(
                serde_json::json!({"ok":false,"error":"lost committed activation reply"}),
            );
        }
        axum::Json(match result {
            Ok(_) => serde_json::json!({"ok":true,"upstreams":[],"workers":{}}),
            Err(error) => serde_json::json!({"ok":false,"error":error}),
        })
    }
    let lose_reply = Arc::new(AtomicBool::new(true));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new()
        .route("/fleet", axum::routing::post(fleet_http))
        .with_state((live.clone(), lose_reply))
        .merge(live.router());
    let worker_task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    struct Tasks(Vec<tokio::task::JoinHandle<()>>);
    impl Drop for Tasks {
        fn drop(&mut self) {
            for task in &self.0 {
                task.abort();
            }
        }
    }
    let mut tasks = Tasks(vec![worker_task]);
    let (upload, expected_response) = query_with_reply(&origin, &set).await;
    use tower::ServiceExt;
    let retained = live
        .router()
        .oneshot(
            axum::http::Request::post(format!(
                "/v2/shards/1/revisions/{}/query/directory",
                set.get(1).unwrap().digest
            ))
            .body(axum::body::Body::from(upload))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(retained.status(), StatusCode::OK);
    // Keep the already admitted response unconsumed through activation. Its
    // exact encrypted bytes were just decrypted against the original row oracle.
    let tip = Arc::new(AtomicU64::new(2));
    let node_tip = tip.clone();
    struct EntryPause {
        enabled: AtomicBool,
        entered: tokio::sync::Notify,
        resume: tokio::sync::Notify,
    }
    let pause = Arc::new(EntryPause {
        enabled: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    let node_pause = pause.clone();
    let rpc_app = axum::Router::new().route(
        "/",
        axum::routing::post(move |axum::Json(request): axum::Json<serde_json::Value>| {
            let tip = node_tip.clone();
            let pause = node_pause.clone();
            async move {
                let value = match request["method"].as_str().unwrap() {
                    "getblockcount" => serde_json::json!(tip.load(Ordering::Acquire)),
                    "getblockhash" => {
                        let h = request["params"][0].as_u64().unwrap();
                        if h == 3 && pause.enabled.swap(false, Ordering::AcqRel) {
                            pause.entered.notify_one();
                            pause.resume.notified().await;
                        }
                        serde_json::json!(if h == 0 {
                            transparent_filter::MAINNET_GENESIS_DISPLAY.to_string()
                        } else {
                            BlockHash::from_internal_bytes([h as u8; 32]).to_display_hex()
                        })
                    }
                    other => panic!("unexpected source method {other}"),
                };
                axum::Json(serde_json::json!({"result":value,"error":null,"id":request["id"]}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc_origin = format!("http://{}", listener.local_addr().unwrap());
    tasks.0.push(tokio::spawn(async move {
        axum::serve(listener, rpc_app).await.unwrap()
    }));
    let cookie = root.path().join("cookie");
    std::fs::write(&cookie, "fixture:fixture").unwrap();
    let rpc = crate::zakura::connect(&rpc_origin, &cookie).unwrap();
    let owner = authority(&directory, None, rpc.clone());
    let hook = root.path().join("fleet.py");
    std::fs::write(&hook, r#"#!/usr/bin/env python3
import sys,urllib.request
from pathlib import Path
r=urllib.request.Request(Path(sys.argv[1]).read_text(),data=sys.stdin.buffer.read(),headers={'Content-Type':'application/json'})
with urllib.request.urlopen(r,timeout=20) as response: sys.stdout.buffer.write(response.read())
"#).unwrap();
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o700)).unwrap();
    let fleet_config = root.path().join("fleet-url");
    std::fs::write(&fleet_config, format!("{origin}/fleet")).unwrap();
    let publications = root.path().join("publications");
    std::fs::create_dir(&publications).unwrap();
    let config = Config {
        data_dir: root.path().join("journal"),
        publication_root: publications,
        initial_publication: directory.clone(),
        recent_from: 2,
        recent_geometry: "decoupled-2k-8k".into(),
        archive_geometry: "decoupled-32k-32k".into(),
        rpc_url: rpc_origin,
        rpc_cookie: cookie,
        fleet_command: hook,
        fleet_config,
        listen: "127.0.0.1:0".parse().unwrap(),
        source_sha: "fixture".into(),
        shadow: false,
        directory_choice: publication::DirectoryChoice::Off,
        range_profile: transparent_filter::RANGE_PROFILE.into(),
        follow_journal: true,
        snapshot_dir: None,
        ship_runtimes: false,
        distribute_prepared: false,
    };
    let reader = Arc::new(tokio::sync::Mutex::new(
        EventStore::open_existing(&config.data_dir).unwrap(),
    ));
    let mut writer = EventStore::open(
        &config.data_dir,
        transparent_filter::MAINNET_GENESIS_DISPLAY,
        0,
    )
    .unwrap();
    let mut added = writer.events_at(2).unwrap().unwrap();
    let TransparentEvent::Receive(event) = &mut added[0].1 else {
        unreachable!()
    };
    event.height = 3;
    event.txid = transparent_events::Txid([8; 32]);
    event.value = 60_000;
    writer
        .append_block(3, BlockHash::from_internal_bytes([3; 32]), &added)
        .unwrap();
    writer.commit().unwrap();
    tip.store(3, Ordering::Release);
    let (notify, _) = tokio::sync::watch::channel(0);
    let mut input = HistoryInputTask {
        config: config.clone(),
        rpc: rpc.clone(),
        store: reader.clone(),
        authority: owner.clone(),
        notify,
        cache: OutputCache::new(100),
        pending_invalidation: None,
    };
    pir_pipeline::Task::run(&mut input).await.unwrap();
    assert_eq!(reader.lock().await.covered_through(), Some(3));
    let Input::Work(work) = acquire_history(&config, &rpc, &reader, &owner)
        .await
        .unwrap()
    else {
        panic!("committed work expected");
    };
    let Input::Work(ready) = prepare_history(&config, &rpc, &owner, work).await.unwrap() else {
        panic!("prepared work expected");
    };
    pause.enabled.store(true, Ordering::Release);
    {
        let activation = activate_history(&config, &rpc, &owner, ready);
        tokio::pin!(activation);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::select! {
                result = &mut activation => panic!("owner check did not pause: {result:?}"),
                _ = pause.entered.notified() => {},
            }
        })
        .await
        .unwrap();
        // A valid RPC answer cannot clear an invalidation that arrived while
        // the owner awaited it. No durable intent or fleet activation may occur.
        owner.0.epoch.fetch_add(1, Ordering::AcqRel);
        pause.resume.notify_one();
        assert!(activation
            .await
            .unwrap_err()
            .to_string()
            .contains("invalidated before activation"));
    }
    assert!(!config.publication_root.join("activation.json").exists());
    assert_eq!(
        live.command(Command::Status).await.unwrap()["active"]["map_sha256"],
        set.map_digest
    );
    assert!(publish_once(&config, &rpc, &reader, &owner).await.is_err());
    assert_eq!(
        owner.0.active.read().unwrap().publication.map_sha256,
        set.map_digest
    );
    assert!(config.publication_root.join("activation.json").exists());
    let activated: ActivePublication = serde_json::from_slice(
        &std::fs::read(config.publication_root.join("activation.json")).unwrap(),
    )
    .unwrap();
    let next_set = ShardSet::open(
        &activated.directory,
        transparent_shard_server::shardset::DEFAULT_RETAIN_REVISIONS,
    )
    .unwrap();
    query(&origin, &next_set).await;
    let old_rows = set
        .get(1)
        .unwrap()
        .segment(transparent_server_backend::shardset::TableRef::Directory, 0)
        .unwrap()
        .load()
        .unwrap();
    let new_rows = next_set
        .get(1)
        .unwrap()
        .segment(transparent_server_backend::shardset::TableRef::Directory, 0)
        .unwrap()
        .load()
        .unwrap();
    let width = set.get(1).unwrap().geometry.directory_row_bytes;
    let selected = old_rows
        .chunks_exact(width)
        .position(|row| row.iter().any(|b| *b != 0))
        .unwrap();
    let selected = selected * width..(selected + 1) * width;
    assert_ne!(
        &old_rows[selected.clone()],
        &new_rows[selected],
        "the admitted query's selected row must change across handover"
    );
    let retained_bytes = axum::body::to_bytes(retained.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        retained_bytes.as_ref(),
        expected_response,
        "no refresh or new-generation response is permitted"
    );
    // Reopen the controller's independent read/checkpoint and authority state,
    // as startup does with no committed active.json. The worker still holds the
    // activation whose reply was lost, and the durable intent still protects it.
    drop(input);
    drop(owner);
    drop(reader);
    let owner = authority(&directory, None, rpc.clone());
    owner.0.withdrawn.store(true, Ordering::Release);
    let reader = Arc::new(tokio::sync::Mutex::new(
        EventStore::open_existing(&config.data_dir).unwrap(),
    ));
    assert_eq!(reader.lock().await.covered_through(), Some(3));
    assert!(config.publication_root.join("activation.json").exists());
    // The existing fleet owner reconciles the actual predecessor; shared
    // advance never repeats an uncertain activation on its own.
    publish_once(&config, &rpc, &reader, &owner).await.unwrap();
    assert!(!config.publication_root.join("activation.json").exists());
    assert_eq!(owner.0.active.read().unwrap().publication.height, 3);
    let current = owner.0.active.read().unwrap().publication.clone();
    query(
        &origin,
        &ShardSet::open(
            &current.directory,
            transparent_shard_server::shardset::DEFAULT_RETAIN_REVISIONS,
        )
        .unwrap(),
    )
    .await;
}
