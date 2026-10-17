//! Real publisher/TLS/manager/owner/recent-worker boundaries, with source rows
//! as an independent answer oracle. These do not qualify a complete provider.
use super::*;
use transparent_shard_server::{live::Command, manager::Manager, service::ServiceConfig};

// Prepare immutable producer bytes once per test process. Each test receives
// separate copies: corruption, missing-file and restart cases must stay isolated.
fn prepared_publication(root: &Path, through: u64) -> (PathBuf, ShardSet) {
    fn build(through: u64) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        let (directory, set) = publication_through(root.path(), through);
        prepare_recent(&directory, &set);
        root
    }
    static FIRST: std::sync::LazyLock<tempfile::TempDir> = std::sync::LazyLock::new(|| build(2));
    static NEXT: std::sync::LazyLock<tempfile::TempDir> = std::sync::LazyLock::new(|| build(3));
    let fixture = match through {
        2 => &*FIRST,
        3 => &*NEXT,
        _ => panic!("no prepared publication fixture through height {through}"),
    };
    fn copy_tree(source: &Path, target: &Path) {
        std::fs::create_dir(target).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let target = target.join(entry.file_name());
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                assert!(
                    kind.is_file(),
                    "producer fixture must contain regular files"
                );
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    let directory = root.join("publication");
    copy_tree(&fixture.path().join("publication"), &directory);
    let set = ShardSet::open(&directory, 0).unwrap();
    (directory, set)
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
fn advance(owner: &Authority, directory: &Path, set: &ShardSet) {
    let mut public = read_public(directory.to_path_buf(), Vec::new()).unwrap();
    public.distribution = Some(Finalized::prepare(directory, set, 2).unwrap());
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
    body
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_reuses_immutable_transfers_and_serves_through_failed_update() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set) = prepared_publication(root.path(), 2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(
        &directory,
        Some(Finalized::prepare(&directory, &set, 2).unwrap()),
        rpc.client.clone(),
    );
    let tls = tls(root.path(), owner.clone()).await;
    let state = root.path().join("provider");
    let mut follow = manager(&tls, &state);
    let first = follow.poll_once().await.unwrap();
    assert!(first.downloaded_files > 0);
    let live = follow.live().unwrap();
    let (origin, task) = worker(live.clone()).await;
    query(&origin, &set).await;
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
    let (next_dir, next_set) = prepared_publication(&next_root, 3);
    advance(&owner, &next_dir, &next_set);
    for fault in ["truncated", "corrupt", "missing"] {
        std::fs::write(&tls.control, fault).unwrap();
        assert!(follow.poll_once().await.is_err(), "{fault}");
        assert_eq!(
            live.command(Command::Status).await.unwrap()["active"]["map_sha256"],
            set.map_digest
        );
        query(&origin, &set).await;
    }
    // A restart during a failed update recovers the complete predecessor only
    // after live source checks; failure of the new transfer still leaves it up.
    task.abort();
    let _ = task.await;
    drop(live);
    drop(follow);
    std::fs::write(&tls.control, "missing").unwrap();
    let mut follow = manager(&tls, &state);
    assert!(follow.poll_once().await.is_err());
    let live = follow
        .live()
        .expect("freshly checked predecessor survives restart/update failure");
    let (origin, task) = worker(live.clone()).await;
    query(&origin, &set).await;
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
    query(&origin, &next_set).await;
    // Old exact sessions retain their source after normal handover.
    query(&origin, &set).await;
    // The newest range is not cumulative: the old retained tail can be
    // orphaned while its newer replacement is positively canonical.
    owner.0.distribution_revision.fetch_add(5, Ordering::AcqRel);
    owner.0.invalidated_from.store(4, Ordering::Release);
    *rpc.hash.write().unwrap() = Some("77".repeat(32));
    follow.poll_once().await.unwrap();
    query(&origin, &next_set).await;
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
    query(&origin, &next_set).await;
    std::fs::write(&tls.control, "refused-body").unwrap();
    assert!(follow.poll_once().await.is_err());
    query(&origin, &next_set).await;
    task.abort();
    let _ = task.await;
    drop(live);
    drop(follow);
    std::fs::write(&tls.control, "").unwrap();
    rpc.available.store(true, Ordering::Release);
    std::fs::write(state.join("active.invalid.json"), b"[]").unwrap();
    let mut restarted = manager(&tls, &state);
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
    query(&origin, &next_set).await;
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_persists_invalidation_and_refuses_equal_revision_reenable_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set) = prepared_publication(root.path(), 2);
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
    let (directory, set) = prepared_publication(root.path(), 2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(
        &directory,
        Some(Finalized::prepare(&directory, &set, 2).unwrap()),
        rpc.client.clone(),
    );
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
    let (next_dir, next_set) = prepared_publication(&next_root, 3);
    advance(&owner, &next_dir, &next_set);
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
    let (directory, set) = prepared_publication(root.path(), 2);
    let original = Finalized::prepare(&directory, &set, 2).unwrap();
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
    // Assignment identity is independent of table geometry. Keep both tiers
    // small here so expanding holdings does not build an unrelated 32k runtime.
    let (directory, set) = publication_with_archive(root.path(), 2, "decoupled-2k-8k");
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
    let (directory, set) = prepared_publication(root.path(), 2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(
        &directory,
        Some(Finalized::prepare(&directory, &set, 2).unwrap()),
        rpc.client.clone(),
    );
    let tls = tls(root.path(), owner.clone()).await;
    let state = root.path().join("provider");
    if high_water_fault.is_some() {
        owner.0.distribution_revision.store(10, Ordering::Release);
    }
    let mut follow = manager(&tls, &state);
    follow.poll_once().await.unwrap();
    let live = follow.live().unwrap();
    let (origin, task) = worker(live.clone()).await;
    let encrypted = query_and_upload(&origin, &set).await;
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
            query(&origin, &set).await;
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
        .body(encrypted)
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
    query(&origin, &set).await;
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
    let (directory, set) = prepared_publication(root.path(), 2);
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
    let (directory, set) = prepared_publication(root.path(), 2);
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
    let (directory, set) = prepared_publication(root.path(), 2);
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
    let (next_dir, next_set) = prepared_publication(&next_root, 3);
    advance(&owner, &next_dir, &next_set);
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
    let (directory, set) = prepared_publication(root.path(), 2);
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
