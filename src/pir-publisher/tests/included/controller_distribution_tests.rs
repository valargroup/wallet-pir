//! Real producer → TLS termination → strict production-loader bootstrap.
use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_publisher::transparent::filter as transparent_filter;

use super::*;
use pir_query_server::transparent::routing::bootstrap;
use tokio::io::{AsyncBufReadExt, BufReader};
use transparent_events::TransparentEvent;
use transparent_filter::ScriptBytes;
use transparent_shared::history::shardset::ShardSet;
use transparent_shared::publication::{Finalized, Kind};

fn authority(directory: &Path, distribution: Option<Finalized>, rpc: NodeClient) -> Authority {
    let mut public = read_public(directory.to_path_buf(), Vec::new()).unwrap();
    public.distribution = distribution;
    Authority(Arc::new(Inner {
        cycle_duration: Default::default(),
        publication_freshness: Default::default(),
        telemetry: Default::default(),
        publication_attempts: AtomicU64::new(0),
        publication_failures: AtomicU64::new(0),
        last_publication: AtomicU64::new(0),
        active: RwLock::new(Arc::new(public)),
        withdrawn: AtomicBool::new(false),
        epoch: AtomicU64::new(0),
        distribution_revision: AtomicU64::new(1),
        distribution_slots: Arc::new(tokio::sync::Semaphore::new(4)),
        canonicality: Some(rpc),
        invalidated_from: AtomicU64::new(u64::MAX),
        publication_gate: std::sync::Mutex::new(()),
        status: RwLock::new(serde_json::json!({})),
        observed: RwLock::new(BTreeMap::new()),
        prepared_set: std::sync::Mutex::new(None),
        shutdown: Default::default(),
        checked_runtimes: Default::default(),
        runtime_preparations: AtomicU64::new(0),
        runtime_preparation_failures: AtomicU64::new(0),
        validation: Default::default(),
        integrity_failed: AtomicBool::new(false),
        foreground: AtomicU64::new(0),
    }))
}

#[cfg(feature = "qualification-tests")]
fn publication(root: &Path) -> (PathBuf, ShardSet) {
    publication_fixture(root, 2, false)
}
fn publication_fixture(root: &Path, through: u64, bounded: bool) -> (PathBuf, ShardSet) {
    let journal_dir = root.join("journal");
    let mut journal =
        EventStore::open(&journal_dir, transparent_filter::MAINNET_GENESIS_DISPLAY, 0).unwrap();
    for h in 0..=through {
        // Keep the occupied recent row used by the independent answer oracle.
        // Full qualification retains the original mixed deployed geometries.
        let mut events = if h == 2 || (bounded && h == 1) {
            vec![(
                ScriptBytes::new(vec![
                    0x76, 0xa9, 0x14, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0x88, 0xac,
                ]),
                TransparentEvent::Receive(transparent_events::ReceiveEvent {
                    metadata: Some(transparent_events::TransactionMetadata {
                        fee: transparent_events::FeeState::Exact(1000),
                        transparent_input_count: 1,
                        has_shielded_components: false,
                    }),
                    height: h as u32,
                    txid: transparent_events::Txid([h as u8 + 5; 32]),
                    transaction_index: 0,
                    output_index: 0,
                    value: 50_000,
                    coinbase: false,
                }),
            )]
        } else {
            Vec::new()
        };
        if bounded && h == 1 {
            // Two archive scripts close the first real shard at height one.
            // The single recent script stays below this small sealing target.
            let mut other = events[0].clone();
            other.0 = ScriptBytes::new(vec![0x51]);
            let TransparentEvent::Receive(event) = &mut other.1 else {
                unreachable!()
            };
            event.output_index = 1;
            events.push(other);
        }
        journal
            .append_block(
                h,
                if h == 0 {
                    BlockHash::from_display_hex(transparent_filter::MAINNET_GENESIS_DISPLAY)
                        .unwrap()
                } else {
                    BlockHash::from_internal_bytes([h as u8; 32])
                },
                &events,
            )
            .unwrap();
    }
    journal.commit().unwrap();
    let output = root.join("publication");
    publication::publish_with(
        &PublishOptions {
            data_dir: journal_dir,
            output: output.clone(),
            previous: None,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: (!bounded).then(|| "decoupled-32k-32k".into()),
            recent_from: (!bounded).then_some(2),
            zakura_rpc_url: String::new(),
            zakura_cookie: root.join("unused"),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
        },
        &journal,
        BlockHash::from_internal_bytes([0; 32]),
        &|geometry| {
            let mut policy = transparent_shard::seal::DecoupledPolicy::for_geometry(geometry);
            if bounded {
                policy.scripts.target = 2;
            }
            policy
        },
    )
    .unwrap();
    let set = ShardSet::open(&output, 0).unwrap();
    assert_eq!(set.map.shards.len(), 2);
    assert_eq!(set.map.shards[0].end_height, 1);
    assert_eq!(set.map.shards[1].start_height, 2);
    (output, set)
}
fn recent_options(set: &ShardSet) -> transparent_shared::history::shardset::LoadOptions {
    use pir_query_server::transparent::routing::holdings;
    use transparent_shared::history::assignment::GeneratedBy;
    use transparent_shared::history::shardset::{LoadOptions, LoadScope};
    let assignment = holdings::local(
        &set.map,
        &set.map_digest,
        1,
        4 << 30,
        0.15,
        GeneratedBy {
            tool: "fixture".into(),
            source_sha: None,
            generated_at: "fixture".into(),
        },
    )
    .unwrap();
    LoadOptions {
        scope: LoadScope::Assigned {
            assignment: Arc::new(assignment),
            worker_id: holdings::PROVIDER.into(),
        },
        ..LoadOptions::whole(0)
    }
}
fn prepare_recent(directory: &Path, set: &ShardSet) {
    let recent = ShardSet::open_with(directory, &recent_options(set)).unwrap();
    let prepared = transparent_shared::runtime::prebuild::prepare_publication(
        &recent,
        directory,
        &[],
        &|| false,
    )
    .unwrap();
    assert!(prepared.failures.is_empty());
}

struct CanonicalRpc {
    client: NodeClient,
    url: String,
    // Qualification campaigns mutate the served tip. Fast cases leave it at
    // the publication hash, so this field has no reader in that build.
    #[cfg_attr(not(feature = "qualification-tests"), allow(dead_code))]
    hash: Arc<RwLock<Option<String>>>,
    genesis: Arc<RwLock<String>>,
    available: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for CanonicalRpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn canonical_rpc(root: &Path, set: &ShardSet) -> CanonicalRpc {
    let hash = Arc::new(RwLock::new(Some(
        set.map.shards.last().unwrap().terminal_block_hash.clone(),
    )));
    let genesis = Arc::new(RwLock::new(set.map.genesis_hash.clone()));
    let available = Arc::new(AtomicBool::new(true));
    let state = (hash.clone(), genesis.clone(), available.clone());
    type RpcState = (
        Arc<RwLock<Option<String>>>,
        Arc<RwLock<String>>,
        Arc<AtomicBool>,
    );
    async fn rpc(
        State((hash, genesis, available)): State<RpcState>,
        axum::Json(request): axum::Json<serde_json::Value>,
    ) -> impl IntoResponse {
        assert_eq!(request["method"], "getblockhash");
        let result = if !available.load(Ordering::Acquire) {
            None
        } else if request["params"][0] == 0 {
            Some(genesis.read().unwrap().clone())
        } else if request["params"][0] == 1 {
            Some(BlockHash::from_internal_bytes([1; 32]).to_display_hex())
        } else if request["params"][0] == 2 {
            hash.read().unwrap().clone()
        } else {
            request["params"][0]
                .as_u64()
                .map(|h| BlockHash::from_internal_bytes([h as u8; 32]).to_display_hex())
        };
        axum::Json(match result {
            Some(hash) => serde_json::json!({"id":request["id"],"result":hash,"error":null}),
            None => {
                serde_json::json!({"id":request["id"],"result":null,"error":{"code":-8,"message":"node unavailable"}})
            }
        })
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/", axum::routing::post(rpc))
                .with_state(state),
        )
        .await
        .unwrap();
    });
    let cookie = root.join("node-cookie");
    std::fs::write(&cookie, "fixture:fixture").unwrap();
    CanonicalRpc {
        client: crate::ingest::zakura::connect(format!("http://{address}"), cookie).unwrap(),
        url: format!("http://{address}"),
        hash,
        genesis,
        available,
        task,
    }
}

pub(crate) struct Tls {
    pub(crate) origin: String,
    pub(crate) cert: reqwest::Certificate,
    pub(crate) control: PathBuf,
    _child: tokio::process::Child,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Tls {
    fn drop(&mut self) {
        self.server.abort();
    }
}
async fn tls(root: &Path, authority: Authority) -> Tls {
    tls_router(root, authority.router()).await
}

/// The publisher's service discovery, which the unified host serves on its
/// management router and an operator proxies at this exact public path.
pub(crate) fn with_discovery(router: Router) -> Router {
    router.route(
        pir_service_discovery::PATH,
        axum::routing::get(|| async { pir_service_discovery::Document::publisher().to_json() }),
    )
}

pub(crate) async fn tls_router(root: &Path, router: Router) -> Tls {
    let router = with_discovery(router);
    let cert = root.join("ca.pem");
    let key = root.join("key.pem");
    static MATERIAL: std::sync::LazyLock<(Vec<u8>, Vec<u8>)> = std::sync::LazyLock::new(|| {
        let root = tempfile::tempdir().unwrap();
        let cert = root.path().join("ca.pem");
        let key = root.path().join("key.pem");
        assert!(std::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost",
                "-addext",
                "basicConstraints=critical,CA:FALSE",
                "-keyout"
            ])
            .arg(&key)
            .arg("-out")
            .arg(&cert)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success());
        (std::fs::read(cert).unwrap(), std::fs::read(key).unwrap())
    });
    // Only immutable generated bytes are shared. Trust checks, listeners and
    // fault controls remain independently owned by each test.
    std::fs::write(&cert, &MATERIAL.0).unwrap();
    std::fs::write(&key, &MATERIAL.1).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let control = root.join("fault");
    std::fs::write(&control, "").unwrap();
    let mut child = tokio::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../pir-publisher/tests/support/distribution_tls.py"
        ))
        .arg(address.to_string())
        .arg(&cert)
        .arg(&key)
        .arg(&control)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .await
        .unwrap();
    let port: u16 = line.trim().parse().unwrap();
    Tls {
        origin: format!("https://localhost:{port}"),
        cert: reqwest::Certificate::from_pem(&std::fs::read(cert).unwrap()).unwrap(),
        control,
        _child: child,
        server,
    }
}
fn config(tls: &Tls, directory: PathBuf) -> bootstrap::Config {
    bootstrap::Config {
        publisher: tls.origin.clone(),
        network: "main".into(),
        genesis_hash: transparent_filter::MAINNET_GENESIS_DISPLAY.into(),
        directory,
        cache_bytes: 4 << 30,
        max_download_bytes: 4 << 30,
        roots: vec![tls.cert.clone()],
    }
}

/// Finalization lists the verified set's digests for unchanged tables, lists
/// what is actually present for a table changed since, and stops on request
/// without producing an inventory.
#[test]
fn finalization_lists_present_bytes_and_stops_when_asked() {
    use std::os::unix::fs::FileExt;
    use transparent_shared::publication::{digest, CheckedRuntimes, Stopped};
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("publication");
    manager_tests::copy_prepared_publication(2, &directory);
    // Settle the copies so the set's verification leaves reusable evidence.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let set = ShardSet::open(&directory, 0).unwrap();
    let error = Finalized::prepare_reusing_until(
        &directory,
        &set,
        2,
        false,
        &mut CheckedRuntimes::default(),
        &|| true,
    )
    .err()
    .unwrap();
    assert!(error.is::<Stopped>(), "{error}");
    let asked = std::sync::atomic::AtomicUsize::new(0);
    let error = Finalized::prepare_reusing_until(
        &directory,
        &set,
        2,
        false,
        &mut CheckedRuntimes::default(),
        &|| asked.fetch_add(1, std::sync::atomic::Ordering::Relaxed) >= 4,
    )
    .err()
    .unwrap();
    assert!(error.is::<Stopped>(), "{error}");
    let finalized = Finalized::prepare(&directory, &set, 2).unwrap();
    let mut tables = 0;
    for artifact in &finalized.inventory().artifacts {
        let bytes = std::fs::read(directory.join(&artifact.path)).unwrap();
        assert_eq!(artifact.sha256, digest(&bytes), "{}", artifact.path);
        assert_eq!(artifact.bytes, bytes.len() as u64);
        tables += usize::from(matches!(
            artifact.kind,
            Kind::RecentTable | Kind::ArchiveTable
        ));
    }
    assert!(tables > 0);
    // Same-size change after the set verified it, modification time restored:
    // the stale verified digest is never listed, so finalization refuses it.
    for kind in [Kind::RecentTable, Kind::ArchiveTable] {
        let table = finalized
            .inventory()
            .artifacts
            .iter()
            .find(|a| a.kind == kind)
            .unwrap()
            .clone();
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .open(directory.join(&table.path))
            .unwrap();
        let modified = file.metadata().unwrap().modified().unwrap();
        let mut first = [0u8; 1];
        file.read_exact_at(&mut first, 0).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        file.write_all_at(&[first[0] ^ 1], 0).unwrap();
        file.set_modified(modified).unwrap();
        assert!(Finalized::prepare(&directory, &set, 2).is_err(), "{kind:?}");
        file.write_all_at(&first, 0).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn https_bootstrap_verifies_real_prepared_recent_without_archive_tables() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("publication");
    // The process already published this tree and prepared its recent runtimes.
    manager_tests::copy_prepared_publication(2, &directory);
    let set = ShardSet::open(&directory, 0).unwrap();
    // Incomplete preparation must fail finalization, never produce a current.
    let saved: Vec<(PathBuf, Vec<u8>)> = std::fs::read_dir(&directory)
        .unwrap()
        .map(Result::unwrap)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "runtime"))
        .map(|path| {
            let bytes = std::fs::read(&path).unwrap();
            std::fs::remove_file(&path).unwrap();
            (path, bytes)
        })
        .collect();
    assert!(!saved.is_empty());
    assert!(Finalized::prepare(&directory, &set, 2).is_err());
    for (path, bytes) in &saved {
        std::fs::write(path, bytes).unwrap();
    }
    // A full-length native file with a valid identity/header but corrupted
    // body cannot be finalized just because preprocessing previously completed.
    let native = std::fs::read_dir(&directory)
        .unwrap()
        .map(Result::unwrap)
        .find(|e| e.path().extension().is_some_and(|x| x == "runtime"))
        .unwrap()
        .path();
    let good = std::fs::read(&native).unwrap();
    let mut bad = good.clone();
    bad[64] ^= 1;
    publication::write_atomic(&native, &bad).unwrap();
    assert!(Finalized::prepare(&directory, &set, 2).is_err());
    publication::write_atomic(&native, &good).unwrap();
    drop((good, bad));
    let finalized = Finalized::prepare(&directory, &set, 2).unwrap();
    let expected = finalized.inventory().clone();
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(finalized), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let report = bootstrap::verify(config(&tls, root.path().join("provider")))
        .await
        .unwrap();
    assert!(!report.serving);
    assert_eq!(report.runtime_preparation["shipped_fallbacks"], 0);
    assert_eq!(report.runtime_preparation["built"], 0);
    assert_eq!(report.runtime_preparation["restored"], 0);
    // Bootstrap checks required prepared runtimes by size and loads them on
    // first use, so none are loaded yet; the byte comparison below proves the
    // shipped runtimes themselves arrived intact.
    assert_eq!(report.runtime_preparation["runtimes_offered"], true);
    assert!(expected
        .artifacts
        .iter()
        .any(|f| f.kind.local() && f.path.ends_with(".runtime")));
    assert_eq!(report.map_sha256, set.map_digest);
    assert_eq!(report.archive_tables_downloaded, 0);
    assert!(expected
        .artifacts
        .iter()
        .any(|f| f.kind == Kind::ArchiveTable));
    // Independent expected bytes are read from the original producer fixture,
    // never reconstructed from the downloaded inventory or consumer report.
    for file in &expected.artifacts {
        if file.kind.local() {
            assert_eq!(
                std::fs::read(directory.join(&file.path)).unwrap(),
                std::fs::read(report.directory.join(&file.path)).unwrap(),
                "{}",
                file.path
            );
        } else {
            assert!(!report.directory.join(&file.path).exists());
        }
    }
    // A persisted verified tree is not an offline authority on restart.
    owner.0.withdrawn.store(true, Ordering::Release);
    let error = bootstrap::verify(config(&tls, root.path().join("provider")))
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("withdrawn"));
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn https_bootstrap_fails_closed_on_origin_network_and_transport_errors() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set, finalized) = manager_tests::immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(finalized), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let mut wrong = config(&tls, root.path().join("wrong-network"));
    wrong.network = "test".into();
    assert!(bootstrap::verify(wrong)
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("chain identity"));
    let mut untrusted = config(&tls, root.path().join("untrusted"));
    untrusted.roots.clear();
    assert!(bootstrap::verify(untrusted).await.is_err());
    let mut plain = config(&tls, root.path().join("plain"));
    plain.publisher = plain.publisher.replace("https:", "http:");
    assert!(bootstrap::verify(plain)
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("HTTPS origin"));
    for fault in ["redirect", "missing", "corrupt", "truncated"] {
        std::fs::write(&tls.control, fault).unwrap();
        assert!(
            bootstrap::verify(config(&tls, root.path().join(fault)))
                .await
                .is_err(),
            "{fault}"
        );
    }
    std::fs::write(&tls.control, "").unwrap();
    for fault in ["runtime", "unsafe-path", "missing-runtime"] {
        std::fs::write(&tls.control, fault).unwrap();
        assert!(
            bootstrap::verify(config(&tls, root.path().join(fault)))
                .await
                .is_err(),
            "{fault}"
        );
    }
    std::fs::write(&tls.control, "").unwrap();
    // Race an actual producer pointer change against a real artifact transfer.
    std::fs::write(&tls.control, "pause-artifact").unwrap();
    let attempt = tokio::spawn(bootstrap::verify(config(
        &tls,
        root.path().join("superseded"),
    )));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !tls.control.with_extension("entered").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    std::fs::write(&tls.control, "").unwrap();
    assert!(attempt
        .await
        .unwrap()
        .err()
        .unwrap()
        .to_string()
        .contains("superseded"));
    // Replaying a formerly observed current record is refused after restart,
    // without deleting or trusting the already verified files.
    owner.0.distribution_revision.store(20, Ordering::Release);
    let restart_dir = root.path().join("restart");
    bootstrap::verify(config(&tls, restart_dir.clone()))
        .await
        .unwrap();
    owner.0.distribution_revision.store(1, Ordering::Release);
    assert!(bootstrap::verify(config(&tls, restart_dir))
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("replay"));
}

/// One batched canonicality request answers every exact anchor for exactly the
/// requested snapshot, or answers for none of them.
#[tokio::test(flavor = "multi_thread")]
async fn batched_anchor_checks_bind_the_snapshot_and_fail_closed() {
    use transparent_shared::publication::{
        AnchorBatch, AnchorBatchRequest, AnchorPair, Current, MAX_ANCHOR_BATCH,
        MAX_ANCHOR_BATCH_BYTES,
    };
    let root = tempfile::tempdir().unwrap();
    let (directory, set, finalized) = manager_tests::immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(finalized), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let client = reqwest::Client::builder()
        .add_root_certificate(tls.cert.clone())
        .https_only(true)
        .build()
        .unwrap();
    let url = format!("{}/provider/v1/anchors/check", tls.origin);
    let current: Current = client
        .get(format!("{}/provider/v1/current", tls.origin))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let older = BlockHash::from_internal_bytes([1; 32]).to_display_hex();
    let tip = set.map.shards.last().unwrap();
    let anchors = vec![
        AnchorPair {
            height: 1,
            hash: older.clone(),
        },
        AnchorPair {
            height: 1,
            hash: "99".repeat(32),
        },
        AnchorPair {
            height: tip.end_height,
            hash: tip.terminal_block_hash.clone(),
        },
    ];
    let request = AnchorBatchRequest::new(&current, anchors.clone()).unwrap();
    let post = |body: Vec<u8>| client.post(&url).body(body).send();
    let response = post(serde_json::to_vec(&request).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let batch: AnchorBatch = response.json().await.unwrap();
    let answers = batch.answers(&request, &current.genesis_hash).unwrap();
    assert!(answers[&anchors[0]]);
    assert!(!answers[&anchors[1]], "a same-network mismatch is negative");
    assert!(answers[&anchors[2]]);

    // Requests are POST-only, bounded and well formed.
    assert_eq!(
        client.get(&url).send().await.unwrap().status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        post(vec![b' '; MAX_ANCHOR_BATCH_BYTES + 1])
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let mut invalid = request.clone();
    invalid.anchors = (0..=MAX_ANCHOR_BATCH as u64)
        .map(|height| AnchorPair {
            height,
            hash: older.clone(),
        })
        .collect();
    let mut duplicate = request.clone();
    duplicate.anchors.push(anchors[0].clone());
    for body in [invalid, duplicate] {
        assert_eq!(
            post(serde_json::to_vec(&body).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    // Never an answer for a snapshot the consumer did not ask about.
    for mutate in [
        |r: &mut AnchorBatchRequest| r.revision += 1,
        |r: &mut AnchorBatchRequest| r.recovery_epoch += 1,
        |r: &mut AnchorBatchRequest| r.inventory_sha256 = "33".repeat(32),
    ] {
        let mut other = request.clone();
        mutate(&mut other);
        assert_eq!(
            post(serde_json::to_vec(&other).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
    }
    // An outage or a repointed node proves nothing about any anchor.
    rpc.available.store(false, Ordering::Release);
    assert_eq!(
        post(serde_json::to_vec(&request).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    rpc.available.store(true, Ordering::Release);
    *rpc.genesis.write().unwrap() = "55".repeat(32);
    assert_eq!(
        post(serde_json::to_vec(&request).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    *rpc.genesis.write().unwrap() = set.map.genesis_hash.clone();
    // A revision committed after the request's snapshot is refused.
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    assert_eq!(
        post(serde_json::to_vec(&request).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn https_bootstrap_consumes_canonicality_withdrawal_and_recovery_state() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set, finalized) = manager_tests::immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(finalized), rpc.client.clone());
    let tls = tls(root.path(), owner.clone()).await;
    let provider = root.path().join("provider");
    // The same endpoint can revalidate an older retained public anchor without
    // publisher disk access or an archive evaluation table.
    let client = reqwest::Client::builder()
        .add_root_certificate(tls.cert.clone())
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let older = BlockHash::from_internal_bytes([1; 32]).to_display_hex();
    let checked: transparent_shared::publication::AnchorCheck = client
        .get(format!("{}/provider/v1/anchors/1/{older}", tls.origin))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(checked.canonical);
    assert_eq!(checked.height, 1);
    assert_eq!(checked.hash, older);
    let wrong: transparent_shared::publication::AnchorCheck = client
        .get(format!(
            "{}/provider/v1/anchors/1/{}",
            tls.origin,
            "99".repeat(32)
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!wrong.canonical);
    // Same production NodeClient/genesis/anchor checks as controller activation;
    // provider traffic still uses only the authenticated publisher HTTPS origin.
    *rpc.hash.write().unwrap() = Some("77".repeat(32));
    assert!(bootstrap::verify(config(&tls, provider.clone()))
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("noncanonical"));
    *rpc.hash.write().unwrap() = Some(set.map.shards.last().unwrap().terminal_block_hash.clone());
    // A later good answer cannot undo an observed bad equal revision.
    assert!(bootstrap::verify(config(&tls, provider.clone()))
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("replay"));
    owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
    let recovered = bootstrap::verify(config(&tls, provider.clone()))
        .await
        .unwrap();
    assert!(!recovered.serving);
    // Explicit invalidation is consumed and retained even before downloading.
    {
        let _gate = owner.0.publication_gate.lock().unwrap();
        owner.0.epoch.fetch_add(1, Ordering::AcqRel);
        owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
        owner.0.invalidated_from.store(1, Ordering::Release);
        owner.0.withdrawn.store(true, Ordering::Release);
    }
    assert!(bootstrap::verify(config(&tls, provider.clone()))
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("withdrawn"));
    let water: serde_json::Value =
        serde_json::from_slice(&std::fs::read(provider.join("publisher-high-water.json")).unwrap())
            .unwrap();
    assert_eq!(water["current"]["invalidated_from"], 1);
    assert_eq!(water["current"]["eligibility"], "withdrawn");
    // The controller can retain an older canonical anchor when only a later
    // journal suffix forked. Recovery advances epoch/revision before reoffering.
    {
        let _gate = owner.0.publication_gate.lock().unwrap();
        owner.0.epoch.fetch_add(1, Ordering::AcqRel);
        owner.0.distribution_revision.fetch_add(1, Ordering::AcqRel);
        owner.0.invalidated_from.store(3, Ordering::Release);
        owner.0.withdrawn.store(false, Ordering::Release);
    }
    let retained = bootstrap::verify(config(&tls, provider.clone()))
        .await
        .unwrap();
    assert_eq!(retained.current.invalidated_from, Some(3));
    assert!(retained.current.eligible());
    // Node/source failure or a wrong-network canonicality source cannot verify
    // an otherwise intact candidate; failure is not a freshness renewal.
    *rpc.hash.write().unwrap() = None;
    assert!(
        bootstrap::verify(config(&tls, root.path().join("source-outage")))
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("503")
    );
    *rpc.hash.write().unwrap() = Some(set.map.shards.last().unwrap().terminal_block_hash.clone());
    *rpc.genesis.write().unwrap() = "88".repeat(32);
    assert!(
        bootstrap::verify(config(&tls, root.path().join("source-network")))
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("503")
    );
}

#[test]
fn distribution_revisions_survive_opens_and_refuse_corrupt_state() {
    let root = tempfile::tempdir().unwrap();
    assert_eq!(advance_distribution_revision(root.path()).unwrap(), 1);
    assert_eq!(advance_distribution_revision(root.path()).unwrap(), 2);
    assert_eq!(advance_distribution_revision(root.path()).unwrap(), 3);
    std::fs::write(
        root.path().join("distribution-revision.json"),
        b"not a counter",
    )
    .unwrap();
    assert!(advance_distribution_revision(root.path()).is_err());
}

#[test]
fn distribution_revision_refuses_directory_sync_failure_before_acknowledgement() {
    let root = tempfile::tempdir().unwrap();
    let mut offered = advance_distribution_revision(root.path()).unwrap();
    let attempted = advance_distribution_revision_with_sync(root.path(), |_| {
        Err(std::io::Error::other("injected directory sync failure"))
    });
    // Same propagation/store order used at the controller's authority boundary:
    // failure cannot replace the acknowledged in-memory revision.
    if let Ok(revision) = &attempted {
        offered = *revision;
    }
    assert!(attempted
        .unwrap_err()
        .to_string()
        .contains("injected directory sync failure"));
    assert_eq!(offered, 1);
    // Rename may have landed even though durability is uncertain. Never roll
    // back or reuse that reservation in a subsequent successful open.
    assert_eq!(advance_distribution_revision(root.path()).unwrap(), 3);
    let missing = root.path().join("missing");
    assert!(advance_distribution_revision(&missing).is_err());
    assert!(!missing.exists());
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn https_bootstrap_charges_all_response_bytes_at_exact_budget_boundary() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set, finalized) = manager_tests::immutable_publication(2);
    // Independent producer-byte oracle, including both live current responses
    // and the canonicality attestation. Never derive the cap from Report.
    let material_bytes = finalized.bytes().len() as u64
        + finalized
            .inventory()
            .artifacts
            .iter()
            .filter(|f| f.kind.local())
            .map(|f| std::fs::metadata(directory.join(&f.path)).unwrap().len())
            .sum::<u64>();
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(finalized), rpc.client.clone());
    let tls = tls(root.path(), owner).await;
    let client = reqwest::Client::builder()
        .add_root_certificate(tls.cert.clone())
        .build()
        .unwrap();
    let current_bytes = client
        .get(format!("{}/provider/v1/current", tls.origin))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let current: transparent_shared::publication::Current =
        serde_json::from_slice(&current_bytes).unwrap();
    let anchor_bytes = client
        .get(format!(
            "{}/provider/v1/anchors/{}/{}",
            tls.origin, current.anchor_height, current.anchor_hash
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let exact = material_bytes + 2 * current_bytes.len() as u64 + anchor_bytes.len() as u64;
    for cap in [
        0,
        1,
        current_bytes.len() as u64,
        current_bytes.len() as u64 + 1,
        exact - 1,
    ] {
        std::fs::write(tls.control.with_extension("requests"), "").unwrap();
        let mut limited = config(&tls, root.path().join(format!("cap-{cap}")));
        limited.max_download_bytes = cap;
        assert!(
            bootstrap::verify(limited)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("download budget"),
            "cap={cap}"
        );
        if cap <= current_bytes.len() as u64 {
            let requests = std::fs::read_to_string(tls.control.with_extension("requests")).unwrap();
            assert!(
                !requests.contains("/inventories/"),
                "tiny cap fetched inventory: {requests}"
            );
            if cap == 0 {
                assert!(requests.is_empty());
            }
        }
    }
    // Even a refused HTTP body consumes the budget before status refusal.
    std::fs::write(&tls.control, "refused-body").unwrap();
    let mut refused = config(&tls, root.path().join("refused-body"));
    refused.max_download_bytes = 64;
    assert!(bootstrap::verify(refused)
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("download budget"));
    std::fs::write(&tls.control, "").unwrap();
    let mut limited = config(&tls, root.path().join("exact"));
    limited.max_download_bytes = exact;
    let report = bootstrap::verify(limited).await.unwrap();
    assert_eq!(report.downloaded_bytes, exact);
    assert!(!report.serving);
}

#[path = "controller_manager_tests.rs"]
mod manager_tests;

/// Full holdings require archive bytes and native files, not an archive address.
#[tokio::test(flavor = "multi_thread")]
async fn full_history_http_holdings_include_archive_runtimes() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set) = if cfg!(feature = "qualification-tests") {
        let (directory, set) = publication_fixture(root.path(), 2, true);
        let prepared = transparent_shared::runtime::prebuild::prepare_publication(
            &set,
            &directory,
            &[],
            &|| false,
        )
        .unwrap();
        assert!(prepared.failures.is_empty());
        (directory, set)
    } else {
        // Built once in this process, from a copy that had recent files and no
        // archive runtimes. Recent files were linked; archive files were built.
        let build = manager_tests::archive_runtime_build(2);
        assert!(build.recent_files > 0);
        assert_eq!(build.linked, build.recent_files);
        assert!(build.built > 0, "archive runtimes must still be prepared");
        let directory = root.path().join("publication");
        manager_tests::copy_archive_publication(2, &directory);
        let set = ShardSet::open(&directory, 0).unwrap();
        (directory, set)
    };
    let finalized = Finalized::prepare_full(&directory, &set, 2).unwrap();
    assert!(finalized
        .inventory()
        .artifacts
        .iter()
        .any(|f| f.kind == Kind::ArchiveRuntime));
    let rpc = canonical_rpc(root.path(), &set).await;
    let authority = authority(&directory, Some(finalized), rpc.client.clone());
    let tls = tls(root.path(), authority).await;
    let mut manager = pir_query_server::transparent::routing::manager::Manager::new(
        config(&tls, root.path().join("consumer")),
        pir_query_server::transparent::config::ServiceConfig {
            cache_bytes: 4 << 30,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_full_holdings();
    manager.poll_once().await.unwrap();
    let status = manager
        .live()
        .unwrap()
        .command(pir_query_server::transparent::history::live::Command::Status)
        .await
        .unwrap();
    assert_eq!(status["runtimes"]["built"], 0);
    assert_eq!(status["runtimes"]["shipped_fallbacks"], 0);
    assert!(status["runtimes"]["shipped"].as_u64().unwrap() > 0);
    assert!(status["revisions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["end_height"] == 1));
    drop(manager);
    let mut restarted = pir_query_server::transparent::routing::manager::Manager::new(
        config(&tls, root.path().join("consumer")),
        pir_query_server::transparent::config::ServiceConfig {
            cache_bytes: 4 << 30,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_full_holdings();
    let progress = restarted.poll_once().await.unwrap();
    assert_eq!(
        progress.downloaded_files, 0,
        "restart reuses completed files"
    );
    // The freshly checked predecessor is the current publication: it was
    // loaded and warmed once, and nothing was restaged or prepared again.
    assert!(progress.unchanged, "{progress:?}");
    let entries = |root: &std::path::Path| {
        std::fs::read_dir(root)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|name| name.starts_with("staging-"))
            .count()
    };
    assert_eq!(
        entries(&root.path().join("consumer")),
        1,
        "no new staging tree"
    );
    let status = restarted
        .live()
        .unwrap()
        .command(pir_query_server::transparent::history::live::Command::Status)
        .await
        .unwrap();
    assert_eq!(status["runtimes"]["built"], 0);
    assert_eq!(status["runtimes"]["shipped_fallbacks"], 0);
    let again = restarted.poll_once().await.unwrap();
    assert!(again.unchanged && again.downloaded_files == 0);
    drop(restarted);

    // A damaged verified inventory never infers the recovered map is Current:
    // the restart updates through a full staging pass instead.
    let consumer = root.path().join("consumer");
    let active: serde_json::Value =
        serde_json::from_slice(&std::fs::read(consumer.join("active.json")).unwrap()).unwrap();
    let staged = std::path::PathBuf::from(active["directory"].as_str().unwrap());
    std::fs::write(staged.join("verified-inventory.json"), b"{").unwrap();
    let mut damaged = pir_query_server::transparent::routing::manager::Manager::new(
        config(&tls, consumer.clone()),
        pir_query_server::transparent::config::ServiceConfig {
            cache_bytes: 4 << 30,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_full_holdings();
    let progress = damaged.poll_once().await.unwrap();
    assert!(!progress.unchanged, "{progress:?}");
    assert_eq!(progress.downloaded_files, 0, "objects are still reusable");
    assert!(damaged.live().is_some());
}

/// A unified query server serving both Transparent products stops entirely,
/// with the original typed error, when one publisher's contract major changes
/// at runtime: management and every wallet listener close. A missing discovery
/// document writes no consumer state, and a recovered active view cannot mask
/// the mismatch.
#[tokio::test(flavor = "multi_thread")]
async fn unified_host_stops_every_product_when_a_publisher_major_changes() {
    use crate::tx_detail::{distribution::Distribution, fixture, publisher::bootstrap};
    use pir_query_server::transparent::history::process::{run, run_with, HistoryProcess};
    use transparent_shared::journal::events::EventStore;
    let root = tempfile::tempdir().unwrap();

    // History publisher.
    let (directory, set, finalized) = manager_tests::immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let history_tls_root = root.path().join("history-tls");
    std::fs::create_dir(&history_tls_root).unwrap();
    let history = tls(
        &history_tls_root,
        authority(&directory, Some(finalized), rpc.client.clone()),
    )
    .await;
    let history_consumer = root.path().join("history-consumer");
    let history_process = |shard_dir: &Path, verify_only: bool| -> HistoryProcess {
        serde_json::from_value(serde_json::json!({
            "publisher": history.origin,
            "publisher_network": "main",
            "publisher_genesis": transparent_filter::MAINNET_GENESIS_DISPLAY,
            "publisher_ca": history_tls_root.join("ca.pem"),
            "shard_dir": shard_dir,
            "cache_bytes": 4u64 << 30,
            "bootstrap_max_bytes": 4u64 << 30,
            "verify_only": verify_only,
        }))
        .unwrap()
    };

    // Verification refuses a publisher without discovery before any state.
    let absent = root.path().join("absent-consumer");
    std::fs::write(&history.control, "discovery-missing").unwrap();
    let error = run(history_process(&absent, true)).await.unwrap_err();
    assert!(error.to_string().contains("HTTP 404"), "{error}");
    assert!(!pir_query_server::transparent::is_incompatible(&error));
    assert!(!absent.exists(), "verification opened consumer state");
    std::fs::write(&history.control, "").unwrap();

    // Display publisher.
    let journal = root.path().join("journal");
    fixture::write_journal(&journal, 0, 8, 0);
    let store = EventStore::open_existing(&journal).unwrap();
    let display_root = root.path().join("display-publisher");
    let initial = bootstrap(&store, &display_root, &fixture::layout(&store, 1, 16), 7).unwrap();
    let node = axum::Router::new().route(
        "/",
        axum::routing::post(
            |axum::Json(request): axum::Json<serde_json::Value>| async move {
                let height = request["params"][0].as_u64().unwrap();
                let hash = if height == 0 {
                    fixture::GENESIS.to_string()
                } else {
                    fixture::block_hash(0, height).to_display_hex()
                };
                axum::Json(serde_json::json!({"id":request["id"],"result":hash,"error":null}))
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_address = listener.local_addr().unwrap();
    let _node = tokio::spawn(async move { axum::serve(listener, node).await.unwrap() });
    let cookie = root.path().join("cookie");
    std::fs::write(&cookie, "fixture:fixture").unwrap();
    let display_rpc =
        crate::ingest::zakura::connect(format!("http://{node_address}"), cookie).unwrap();
    let distribution = Distribution::new(&display_root, display_rpc).unwrap();
    distribution
        .restore(
            crate::host_engine::PublisherEngine,
            initial.candidate.clone(),
            vec![],
        )
        .await
        .unwrap();
    let display_tls_root = root.path().join("display-tls");
    std::fs::create_dir(&display_tls_root).unwrap();
    let display = tls_router(&display_tls_root, distribution.router()).await;

    let addrs: Vec<std::net::SocketAddr> = (0..3)
        .map(|_| {
            let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            held.local_addr().unwrap()
        })
        .collect();
    let config: pir_query_server::config::Config = serde_json::from_value(serde_json::json!({
        "management_listen": addrs[0],
        "transparent_history": {"enabled": true, "settings": {
            "publisher": history.origin,
            "publisher_ca": history_tls_root.join("ca.pem"),
            "publisher_network": "main",
            "publisher_genesis": transparent_filter::MAINNET_GENESIS_DISPLAY,
            "cache_bytes": 4u64 << 30,
            "bootstrap_max_bytes": 4u64 << 30,
            "listen": addrs[1],
            "shard_dir": history_consumer,
        }},
        "transaction_details": {"enabled": true, "settings": {
            "publisher": display.origin,
            "publisher_ca": display_tls_root.join("ca.pem"),
            "publisher_network": "main",
            "publisher_genesis": fixture::GENESIS,
            "cache_bytes": 4u64 << 30,
            "role": "combined",
            "listen": addrs[2],
            "state_dir": root.path().join("display-consumer"),
        }},
    }))
    .unwrap();
    // Seed checked display state so the host also exercises rediscovery on restart.
    let mut details = config
        .transaction_details
        .as_ref()
        .unwrap()
        .settings
        .clone()
        .unwrap();
    details.verify_only = true;
    pir_query_server::transparent::tx_detail::process::run(details)
        .await
        .unwrap();
    let running = tokio::spawn(pir_query_server::run(
        config,
        pir_pipeline::service::Shutdown::default(),
    ));

    // Both products serve before the publisher changes.
    let ready = format!("http://{}/ready", addrs[0]);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        assert!(!running.is_finished(), "host stopped before serving");
        let response = client.get(&ready).send().await;
        if response
            .as_ref()
            .is_ok_and(|response| response.status().is_success())
        {
            break;
        }
        let state = match response {
            Ok(response) => response.text().await.unwrap_or_default(),
            Err(error) => error.to_string(),
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "both products never became ready: {state}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    std::fs::write(
        &display.control,
        r#"discovery-body:{"service":"pir-publisher","version":"2.0.0"}"#,
    )
    .unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(60), running)
        .await
        .expect("an incompatible publisher must stop the unified host")
        .unwrap()
        .unwrap_err();
    assert!(
        pir_query_server::transparent::is_incompatible(&error),
        "{error}"
    );
    let text = error.to_string();
    assert!(
        text.contains("2.0.0") && text.contains(&display.origin),
        "{text}"
    );
    for addr in addrs {
        assert!(
            tokio::net::TcpStream::connect(addr).await.is_err(),
            "{addr} outlived an incompatible publisher"
        );
    }

    // The recovered active view on disk cannot mask a major mismatch at restart.
    assert!(history_consumer.join("active.json").exists());
    std::fs::write(
        &history.control,
        r#"discovery-body:{"service":"pir-publisher","version":"2.0.0"}"#,
    )
    .unwrap();
    let error = run_with(history_process(&history_consumer, false), None)
        .await
        .unwrap_err();
    assert!(
        pir_query_server::transparent::is_incompatible(&error),
        "{error}"
    );
}

/// A source change during the final check stays private. Clearing a withdrawal
/// is not enough: that map is served again only after the source revision rises.
#[tokio::test(flavor = "multi_thread")]
async fn cold_history_bootstrap_keeps_checked_inputs_private_until_the_source_advances() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("publication");
    manager_tests::copy_archive_publication(2, &directory);
    let set = ShardSet::open(&directory, 0).unwrap();
    let finalized = Finalized::prepare_full(&directory, &set, 2).unwrap();
    let rpc = canonical_rpc(root.path(), &set).await;
    let authority = authority(&directory, Some(finalized), rpc.client.clone());
    let tls = tls(root.path(), authority.clone()).await;
    cold_history_bootstrap_retry(&tls, &authority, &root.path().join("change"), false).await;
    cold_history_bootstrap_retry(&tls, &authority, &root.path().join("withdraw"), true).await;
}

async fn cold_history_bootstrap_retry(
    tls: &Tls,
    authority: &Authority,
    consumer_root: &Path,
    withdraw: bool,
) {
    let _ = std::fs::remove_file(tls.control.with_extension("current-count"));
    let _ = std::fs::remove_file(tls.control.with_extension("entered"));
    std::fs::write(&tls.control, "pause-current-2").unwrap();
    let mut manager = pir_query_server::transparent::routing::manager::Manager::new(
        config(tls, consumer_root.to_path_buf()),
        pir_query_server::transparent::config::ServiceConfig {
            cache_bytes: 4 << 30,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_full_holdings();
    let checking = tokio::spawn(async move {
        let result = manager.poll_once().await;
        (manager, result)
    });
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while !tls.control.with_extension("entered").exists() && !checking.is_finished() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    if checking.is_finished() {
        let (_, result) = checking.await.unwrap();
        panic!("bootstrap stopped before the final source check: {result:?}");
    }
    assert!(tls.control.with_extension("entered").exists());
    authority
        .0
        .distribution_revision
        .fetch_add(1, Ordering::AcqRel);
    authority.0.withdrawn.store(withdraw, Ordering::Release);
    std::fs::write(&tls.control, "").unwrap();
    let (mut manager, result) = checking.await.unwrap();
    assert!(
        result.is_err(),
        "a source change must reject the first activation"
    );
    assert!(
        manager.live().is_none(),
        "unpublished checked data must remain private"
    );
    assert!(!consumer_root.join("active.json").exists());
    if withdraw {
        authority.0.withdrawn.store(false, Ordering::Release);
        assert!(
            manager.poll_once().await.is_err(),
            "restoring a withdrawn map requires a higher source revision"
        );
        authority
            .0
            .distribution_revision
            .fetch_add(1, Ordering::AcqRel);
    }
    manager.poll_once().await.unwrap();
    assert!(manager.live().is_some());
    assert!(
        consumer_root.join("active.json").exists(),
        "retry must durably activate the initial view"
    );
}

/// A completed publication's record and the expectation its restorer derives
/// from the saved publication.
fn restore_fixture(
    directory: &Path,
) -> (
    ShardSet,
    Finalized,
    transparent_shared::publication::RestoreRecord,
    transparent_shared::publication::RestoreExpectation,
) {
    manager_tests::copy_prepared_publication(2, directory);
    let set = ShardSet::open(directory, 0).unwrap();
    let finalized = Finalized::prepare(directory, &set, 2).unwrap();
    let end = set.map.shards.last().unwrap();
    let record = finalized
        .restore_record(false, 2, end.end_height, &end.terminal_block_hash)
        .unwrap();
    let expected = transparent_shared::publication::RestoreExpectation {
        network: set.map.network.clone(),
        genesis_hash: set.map.genesis_hash.clone(),
        map_sha256: set.map_digest.clone(),
        full: false,
        recent_from_height: 2,
        anchor_height: end.end_height,
        anchor_hash: end.terminal_block_hash.clone(),
    };
    (set, finalized, record, expected)
}

fn with_inventory(
    record: &transparent_shared::publication::RestoreRecord,
    inventory: &transparent_shared::publication::Inventory,
) -> Vec<u8> {
    let mut record = record.clone();
    record.inventory = serde_json::to_string(inventory).unwrap();
    record.inventory_sha256 = transparent_shared::publication::digest(record.inventory.as_bytes());
    serde_json::to_vec(&record).unwrap()
}

/// A restored fixture publication, its record bytes and expectation.
struct RestoreCase {
    root: tempfile::TempDir,
    directory: PathBuf,
    finalized: Finalized,
    record: transparent_shared::publication::RestoreRecord,
    bytes: Vec<u8>,
    expected: transparent_shared::publication::RestoreExpectation,
}
impl RestoreCase {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("publication");
        let (_set, finalized, record, expected) = restore_fixture(&directory);
        let bytes = serde_json::to_vec(&record).unwrap();
        Self {
            root,
            directory,
            finalized,
            record,
            bytes,
            expected,
        }
    }
    fn restore(&self) -> transparent_shared::publication::Restored {
        Finalized::restore_completed(&self.directory, &self.bytes, &self.expected).unwrap()
    }
    fn inventory(&self) -> transparent_shared::publication::Inventory {
        self.finalized.inventory().clone()
    }
    fn artifact(&self, kind: Kind) -> transparent_shared::publication::Artifact {
        self.finalized
            .inventory()
            .artifacts
            .iter()
            .find(|a| a.kind == kind)
            .unwrap()
            .clone()
    }
    #[track_caller]
    fn refuse(
        &self,
        bytes: &[u8],
        expected: &transparent_shared::publication::RestoreExpectation,
        why: &str,
    ) {
        assert!(
            Finalized::restore_completed(&self.directory, bytes, expected).is_err(),
            "{why}"
        );
    }
    fn memory() -> Arc<transparent_shared::runtime::memory::WorkMemory> {
        Arc::new(transparent_shared::runtime::memory::WorkMemory::default())
    }
}

/// Replace `path` with a new inode holding the same bytes.
fn replace_with_same_bytes(path: &Path) {
    let bytes = std::fs::read(path).unwrap();
    let next = path.with_extension("replacement");
    std::fs::write(&next, &bytes).unwrap();
    std::fs::rename(&next, path).unwrap();
}

#[test]
fn restore_offers_the_exact_prepared_inventory_and_its_bodies_validate() {
    let case = RestoreCase::new();
    let restored = case.restore();
    // The exact bytes and digest offered over HTTPS, and only listed local files.
    assert_eq!(restored.finalized.bytes(), case.finalized.bytes());
    assert_eq!(restored.finalized.sha256(), case.finalized.sha256());
    for artifact in &case.finalized.inventory().artifacts {
        assert_eq!(
            restored
                .finalized
                .open_file(&artifact.sha256)
                .unwrap()
                .map(|f| {
                    use std::os::unix::fs::MetadataExt;
                    let m = f.metadata().unwrap();
                    (m.dev(), m.ino(), m.len())
                }),
            case.finalized
                .open_file(&artifact.sha256)
                .unwrap()
                .map(|f| {
                    use std::os::unix::fs::MetadataExt;
                    let m = f.metadata().unwrap();
                    (m.dev(), m.ino(), m.len())
                }),
            "{}",
            artifact.path
        );
    }
    let inventory = case.inventory();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let verified = restored
        .validation
        .integrity(&inventory, None, &|| false)
        .unwrap();
    assert!(verified.hashed > 0);
    let checked = restored
        .validation
        .correctness(&inventory, &verified, &RestoreCase::memory(), &|| false)
        .unwrap();
    assert!(checked.loaded > 0);
    // Unchanged files verified by an earlier pass are not read again.
    let again = restored
        .validation
        .integrity(&inventory, Some(&verified), &|| false)
        .unwrap();
    assert_eq!(again.hashed, 0);
    // A stop ends the pass between reads.
    assert!(matches!(
        restored.validation.integrity(&inventory, None, &|| true),
        Err(transparent_shared::publication::IntegrityError::Stopped)
    ));
}

/// Importing a served publication into the publisher stores leaves its
/// inventory, logical paths and bytes exactly as wallets fetched them: the
/// saved record restores unchanged, its bodies validate from the stores, and
/// a response already reading a stored file completes after collection.
#[test]
fn an_imported_stored_publication_restores_unchanged_and_serves_in_flight_reads() {
    use std::io::Read;
    let case = RestoreCase::new();
    let base = case.directory.parent().unwrap().to_path_buf();
    let never = || false;
    let imported = crate::history::store::adopt_segments(&case.directory, &never).unwrap();
    assert!(
        imported.installed > 0 && imported.bytes_hashed > 0,
        "{imported:?}"
    );
    crate::history::store::adopt_runtimes(&case.directory, &never).unwrap();
    // Imported once: a second adoption links everything already.
    let again = crate::history::store::adopt_segments(&case.directory, &never).unwrap();
    assert_eq!((again.installed, again.bytes_hashed), (0, 0));
    let restored = case.restore();
    assert_eq!(restored.finalized.bytes(), case.finalized.bytes());
    assert_eq!(restored.finalized.sha256(), case.finalized.sha256());
    let inventory = case.inventory();
    for artifact in &inventory.artifacts {
        if let Some(mut file) = restored.finalized.open_file(&artifact.sha256).unwrap() {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            assert_eq!(
                transparent_shared::publication::digest(&bytes),
                artifact.sha256,
                "{}",
                artifact.path
            );
        }
    }
    let verified = restored
        .validation
        .integrity(&inventory, None, &|| false)
        .unwrap();
    restored
        .validation
        .correctness(&inventory, &verified, &RestoreCase::memory(), &|| false)
        .unwrap();
    // Collection keeps everything a remaining publication links.
    let kept = crate::history::store::collect(&base).unwrap();
    assert_eq!(kept, Default::default());
    // A response reading a stored runtime keeps its bytes when the
    // publication is retired and its objects are collected.
    let runtime = case.artifact(Kind::RecentRuntime);
    let path = case.directory.join(&runtime.path).canonicalize().unwrap();
    let mut in_flight = restored
        .finalized
        .open_file(&runtime.sha256)
        .unwrap()
        .unwrap();
    let mut head = vec![0u8; 64];
    in_flight.read_exact(&mut head).unwrap();
    std::fs::remove_dir_all(&case.directory).unwrap();
    let collected = crate::history::store::collect(&base).unwrap();
    assert!(collected.objects > 0, "{collected:?}");
    assert!(!path.exists());
    in_flight.read_to_end(&mut head).unwrap();
    assert_eq!(
        transparent_shared::publication::digest(&head),
        runtime.sha256
    );
}

#[test]
fn restore_refuses_unsupported_or_foreign_evidence() {
    let case = RestoreCase::new();
    let encode =
        |r: &transparent_shared::publication::RestoreRecord| serde_json::to_vec(r).unwrap();
    let mut changed = case.record.clone();
    changed.schema = "transparent-history-restore-v0".into();
    case.refuse(&encode(&changed), &case.expected, "unknown schema");
    let mut changed = case.record.clone();
    changed.profile = "other".into();
    case.refuse(&encode(&changed), &case.expected, "unknown profile");
    let mut changed = case.record.clone();
    changed.completed.pop();
    case.refuse(&encode(&changed), &case.expected, "incomplete preparation");
    let mut changed = case.record.clone();
    changed.inventory.push(' ');
    case.refuse(&encode(&changed), &case.expected, "inventory digest");
    case.refuse(b"{", &case.expected, "malformed record");
    for field in [
        "anchor", "height", "cutoff", "full", "network", "genesis", "map",
    ] {
        let mut other = case.expected.clone();
        match field {
            "anchor" => other.anchor_hash = "00".repeat(32),
            "height" => other.anchor_height += 1,
            "cutoff" => other.recent_from_height = 0,
            "full" => other.full = true,
            "network" => other.network = "testnet".into(),
            "genesis" => other.genesis_hash = "00".repeat(32),
            _ => other.map_sha256 = "00".repeat(32),
        }
        case.refuse(&case.bytes, &other, field);
    }
}

#[test]
fn restore_refuses_an_inventory_that_is_not_exactly_the_publication() {
    let case = RestoreCase::new();
    let mut omitted = case.inventory();
    omitted.artifacts.retain(|a| a.kind != Kind::RecentRuntime);
    case.refuse(
        &with_inventory(&case.record, &omitted),
        &case.expected,
        "omitted runtime",
    );
    let mut longer = case.inventory();
    longer
        .artifacts
        .iter_mut()
        .find(|a| a.kind == Kind::ArchiveTable)
        .unwrap()
        .bytes += 1;
    case.refuse(
        &with_inventory(&case.record, &longer),
        &case.expected,
        "wrong length",
    );
    let mut wrong = case.inventory();
    wrong
        .artifacts
        .iter_mut()
        .find(|a| a.kind == Kind::RecentTable)
        .unwrap()
        .sha256 = "11".repeat(32);
    case.refuse(
        &with_inventory(&case.record, &wrong),
        &case.expected,
        "source differs from manifest",
    );
    let mut renamed = case.inventory();
    renamed
        .artifacts
        .iter_mut()
        .find(|a| a.kind == Kind::RecentRuntime)
        .unwrap()
        .path = "../escape.runtime".into();
    case.refuse(
        &with_inventory(&case.record, &renamed),
        &case.expected,
        "unsafe path",
    );
}

#[test]
fn restore_refuses_missing_truncated_relabelled_or_linked_runtimes() {
    let case = RestoreCase::new();
    let path = case.directory.join(case.artifact(Kind::RecentRuntime).path);
    let good = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    case.refuse(&case.bytes, &case.expected, "missing runtime");
    std::fs::write(&path, &good[..good.len() - 1]).unwrap();
    case.refuse(&case.bytes, &case.expected, "truncated runtime");
    let mut header = good.clone();
    header[0] ^= 1;
    std::fs::write(&path, &header).unwrap();
    case.refuse(&case.bytes, &case.expected, "wrong runtime identity header");
    std::fs::remove_file(&path).unwrap();
    let elsewhere = case.root.path().join("elsewhere.runtime");
    std::fs::write(&elsewhere, &good).unwrap();
    std::os::unix::fs::symlink(&elsewhere, &path).unwrap();
    case.refuse(&case.bytes, &case.expected, "symbolic link");
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    case.refuse(&case.bytes, &case.expected, "not a regular file");
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(&path, &good).unwrap();
    case.restore();
}

#[test]
fn integrity_confirms_same_size_body_corruption_the_restore_cannot_see() {
    use std::os::unix::fs::FileExt;
    let case = RestoreCase::new();
    let table = case.artifact(Kind::ArchiveTable);
    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .open(case.directory.join(&table.path))
        .unwrap();
    let mut first = [0u8; 1];
    file.read_exact_at(&mut first, 0).unwrap();
    file.write_all_at(&[first[0] ^ 1], 0).unwrap();
    let restored = case.restore();
    match restored
        .validation
        .integrity(&case.inventory(), None, &|| false)
    {
        Err(transparent_shared::publication::IntegrityError::Mismatch {
            path,
            found_sha256,
            ..
        }) => {
            assert_eq!(path, table.path);
            assert!(found_sha256.is_some());
        }
        other => panic!("expected a mismatch, got {other:?}"),
    }
}

#[test]
fn integrity_never_passes_a_file_replaced_while_it_is_read() {
    let case = RestoreCase::new();
    let restored = case.restore();
    let table = case.directory.join(case.artifact(Kind::ArchiveTable).path);
    let reads = std::cell::Cell::new(0);
    // The first read of the first artifact replaces one table: the pass
    // cannot vouch for either version and reports nothing about its bytes.
    let pause = || {
        reads.set(reads.get() + 1);
        if reads.get() == 1 {
            replace_with_same_bytes(&table);
        }
        false
    };
    let mut inventory = case.inventory();
    let index = inventory
        .artifacts
        .iter()
        .position(|a| case.directory.join(&a.path) == table)
        .unwrap();
    inventory.artifacts.swap(0, index);
    assert!(matches!(
        restored.validation.integrity(&inventory, None, &pause),
        Err(transparent_shared::publication::IntegrityError::Unavailable(_))
    ));
    // Read again, the replacement's own unchanged bytes pass.
    restored
        .validation
        .integrity(&inventory, None, &|| false)
        .unwrap();
}

#[test]
fn correctness_checks_only_the_inodes_integrity_verified() {
    use transparent_shared::publication::CorrectnessError;
    let case = RestoreCase::new();
    let restored = case.restore();
    let inventory = case.inventory();
    let memory = RestoreCase::memory();
    for kind in [Kind::RecentRuntime, Kind::RecentTable] {
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let verified = restored
            .validation
            .integrity(&inventory, None, &|| false)
            .unwrap();
        replace_with_same_bytes(&case.directory.join(case.artifact(kind).path));
        assert!(
            matches!(
                restored
                    .validation
                    .correctness(&inventory, &verified, &memory, &|| false),
                Err(CorrectnessError::Unavailable(_))
            ),
            "{kind:?} replaced after integrity"
        );
        // Earlier evidence does not cover the new inode: it is hashed again.
        let again = restored
            .validation
            .integrity(&inventory, Some(&verified), &|| false)
            .unwrap();
        assert_eq!(again.hashed, case.artifact(kind).bytes, "{kind:?}");
        restored
            .validation
            .correctness(&inventory, &again, &memory, &|| false)
            .unwrap();
    }
    // Nothing integrity did not verify is checked.
    assert!(matches!(
        restored
            .validation
            .correctness(&inventory, &Default::default(), &memory, &|| false),
        Err(CorrectnessError::Unavailable(_))
    ));
}

#[test]
fn correctness_fails_a_runtime_whose_listed_digest_names_corrupt_bytes() {
    let case = RestoreCase::new();
    let runtime = case.artifact(Kind::RecentRuntime);
    let path = case.directory.join(&runtime.path);
    let mut bad = std::fs::read(&path).unwrap();
    bad[64] ^= 1;
    publication::write_atomic(&path, &bad).unwrap();
    let mut corrupt = case.inventory();
    corrupt
        .artifacts
        .iter_mut()
        .find(|a| a.path == runtime.path)
        .unwrap()
        .sha256 = transparent_shared::publication::digest(&bad);
    let restored = Finalized::restore_completed(
        &case.directory,
        &with_inventory(&case.record, &corrupt),
        &case.expected,
    )
    .unwrap();
    let verified = restored
        .validation
        .integrity(&corrupt, None, &|| false)
        .unwrap();
    assert!(matches!(
        restored
            .validation
            .correctness(&corrupt, &verified, &RestoreCase::memory(), &|| false),
        Err(transparent_shared::publication::CorrectnessError::Failed(_))
    ));
}

#[test]
fn a_newer_job_or_cancellation_stops_obsolete_validation_and_releases_its_input() {
    use crate::history::validator::{Event, Identity, Job, Queue, Validator};
    let first = RestoreCase::new();
    let second = RestoreCase::new();
    let job = |case: &RestoreCase| Job {
        identity: Identity {
            directory: case.directory.clone(),
            map_sha256: case.expected.map_sha256.clone(),
            inventory_sha256: case.finalized.sha256().to_string(),
            height: case.expected.anchor_height,
            hash: case.expected.anchor_hash.clone(),
            epoch: 0,
        },
        inventory: case.inventory(),
        validation: case.restore().validation,
    };
    // Foreground work keeps the validator waiting between reads.
    let busy = Arc::new(AtomicBool::new(true));
    let events = Arc::new(std::sync::Mutex::new(Vec::<(PathBuf, String)>::new()));
    let queue = Queue::default();
    let validator = Validator::spawn(
        queue.clone(),
        RestoreCase::memory(),
        {
            let busy = busy.clone();
            Box::new(move || busy.load(Ordering::Acquire))
        },
        {
            let events = events.clone();
            Box::new(move |identity: &Identity, event| {
                let name = match event {
                    Event::Started(phase) => format!("started {phase}"),
                    Event::Progress(..) => return,
                    Event::Integrity(Ok(_)) => "integrity passed".into(),
                    Event::Integrity(Err(e)) => format!("integrity {e}"),
                    Event::Correctness(Ok(_)) => "correctness passed".into(),
                    Event::Correctness(Err(e)) => format!("correctness {e}"),
                };
                events
                    .lock()
                    .unwrap()
                    .push((identity.directory.clone(), name));
            })
        },
    )
    .unwrap();
    let wait = |what: &str, done: &dyn Fn() -> bool| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "{what}");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    };
    let saw = |directory: &Path, name: &str| {
        events
            .lock()
            .unwrap()
            .iter()
            .any(|(d, n)| d == directory && n == name)
    };
    queue.submit(job(&first));
    wait("first started", &|| {
        saw(&first.directory, "started integrity")
    });
    assert_eq!(queue.held(), vec![first.directory.clone()]);
    // A newer publication stops the running job while it is still blocked;
    // only the newest one remains held.
    queue.submit(job(&second));
    wait("first stopped", &|| {
        saw(&first.directory, "integrity validation stopped")
    });
    wait("second started", &|| {
        saw(&second.directory, "started integrity")
    });
    assert_eq!(queue.held(), vec![second.directory.clone()]);
    // Withdrawal cancels it too and releases its directory.
    queue.cancel();
    wait("second stopped", &|| {
        saw(&second.directory, "integrity validation stopped")
    });
    wait("released", &|| queue.held().is_empty());
    assert!(!saw(&first.directory, "integrity passed"));
    // A later job runs to completion once foreground work ends.
    busy.store(false, Ordering::Release);
    queue.submit(job(&first));
    wait("passed", &|| saw(&first.directory, "correctness passed"));
    drop(validator);
    assert!(queue.held().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn restored_history_serves_before_validation_and_withdraws_on_a_body_mismatch() {
    use crate::history::validator::{Identity, Job, Queue, Validator};
    use std::os::unix::fs::FileExt;
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("publication");
    let (set, _finalized, record, expected) = restore_fixture(&directory);
    let restored =
        Finalized::restore_completed(&directory, &serde_json::to_vec(&record).unwrap(), &expected)
            .unwrap();
    let inventory = restored.finalized.inventory().clone();
    let sha256 = restored.finalized.sha256().to_string();
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(restored.finalized), rpc.client.clone());
    let failures = root.path().join("state");
    std::fs::create_dir(&failures).unwrap();
    // The validator stays blocked, as behind foreground work, until released.
    let blocked = Arc::new(AtomicBool::new(true));
    let queue = Queue::default();
    let validator = Validator::spawn(
        queue.clone(),
        Arc::new(transparent_shared::runtime::memory::WorkMemory::default()),
        {
            let blocked = blocked.clone();
            Box::new(move || blocked.load(Ordering::Acquire))
        },
        {
            let owner = owner.clone();
            let failures = failures.clone();
            Box::new(move |identity, event| validation_event(&failures, &owner, identity, event))
        },
    )
    .unwrap();
    queue.submit(Job {
        identity: Identity {
            directory: directory.clone(),
            map_sha256: set.map_digest.clone(),
            inventory_sha256: sha256.clone(),
            height: expected.anchor_height,
            hash: expected.anchor_hash.clone(),
            epoch: owner.0.epoch.load(Ordering::Acquire),
        },
        inventory: inventory.clone(),
        validation: restored.validation,
    });
    let status = |key: &str| owner.0.status.read().unwrap()[key].clone();
    let wait = |what: &str, done: &dyn Fn() -> bool| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "{what}");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    };
    wait("validation started", &|| {
        status("integrity_validation")["state"] == "pending"
    });
    assert_eq!(
        queue.held(),
        vec![directory.clone()],
        "collection keeps the source"
    );
    // Real HTTPS: the restored inventory is retrievable while validation waits.
    let tls = tls(root.path(), owner.clone()).await;
    let report = bootstrap::verify(config(&tls, root.path().join("provider")))
        .await
        .unwrap();
    assert_eq!(report.map_sha256, set.map_digest);
    assert_eq!(status("integrity_validation")["state"], "pending");
    assert_eq!(status("integrity_validation")["inventory_sha256"], sha256);

    // Same-size corruption found after start withdraws the publication and
    // is recorded for the next process.
    let table = inventory
        .artifacts
        .iter()
        .find(|a| a.kind == Kind::ArchiveTable)
        .unwrap();
    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .open(directory.join(&table.path))
        .unwrap();
    let mut first = [0u8; 1];
    file.read_exact_at(&mut first, 0).unwrap();
    file.write_all_at(&[first[0] ^ 1], 0).unwrap();
    blocked.store(false, Ordering::Release);
    wait("integrity failure", &|| {
        status("integrity_validation")["state"] == "failed"
    });
    assert!(owner.0.withdrawn.load(Ordering::Acquire));
    assert!(owner.0.integrity_failed.load(Ordering::Acquire));
    assert_eq!(status("phase"), "integrity_failed");
    assert_eq!(
        status("correctness_validation")["state"],
        serde_json::Value::Null
    );
    let recorded = integrity_failures(&failures).unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].path, table.path);
    // The record survives a restart: the same inventory is not restored.
    assert!(lists_failed(&recorded, &inventory));
    let error = bootstrap::verify(config(&tls, root.path().join("provider-2")))
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("withdrawn"), "{error}");
    // Shutdown joins the validator.
    drop(validator);
    assert!(queue.held().is_empty());
    file.write_all_at(&first, 0).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_result_about_a_superseded_publication_changes_nothing() {
    use crate::history::validator::{Event, Identity};
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("publication");
    let (set, _finalized, record, expected) = restore_fixture(&directory);
    let restored =
        Finalized::restore_completed(&directory, &serde_json::to_vec(&record).unwrap(), &expected)
            .unwrap();
    let sha256 = restored.finalized.sha256().to_string();
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(restored.finalized), rpc.client.clone());
    let state = root.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let identity = Identity {
        directory: directory.clone(),
        map_sha256: set.map_digest.clone(),
        inventory_sha256: sha256,
        height: expected.anchor_height,
        hash: expected.anchor_hash.clone(),
        epoch: owner.0.epoch.load(Ordering::Acquire),
    };
    let before = owner.0.status.read().unwrap().clone();
    // A reorg or activation advanced the epoch after the job was submitted.
    owner.0.epoch.fetch_add(1, Ordering::AcqRel);
    validation_event(&state, &owner, &identity, Event::Started("integrity"));
    validation_event(&state, &owner, &identity, Event::Integrity(Ok(1)));
    validation_event(
        &state,
        &owner,
        &identity,
        Event::Correctness(Err(
            transparent_shared::publication::CorrectnessError::Failed("stale".into()),
        )),
    );
    assert_eq!(*owner.0.status.read().unwrap(), before);
    // A different inventory under the current epoch is equally stale.
    let other = Identity {
        inventory_sha256: "00".repeat(32),
        epoch: owner.0.epoch.load(Ordering::Acquire),
        ..identity
    };
    validation_event(&state, &owner, &other, Event::Integrity(Ok(1)));
    assert_eq!(*owner.0.status.read().unwrap(), before);

    // A confirmed mismatch of bytes the current publication lists withdraws it
    // even when reported by a stale job, and an unreadable failure record is
    // kept as is rather than replaced.
    std::fs::write(state.join(INTEGRITY_FAILURES), b"not json").unwrap();
    let artifact = owner
        .0
        .active
        .read()
        .unwrap()
        .distribution
        .as_ref()
        .unwrap()
        .inventory()
        .artifacts[0]
        .clone();
    validation_event(
        &state,
        &owner,
        &other,
        Event::Integrity(Err(
            transparent_shared::publication::IntegrityError::Mismatch {
                path: artifact.path.clone(),
                expected_sha256: artifact.sha256.clone(),
                found_sha256: Some("11".repeat(32)),
                reason: "changed".into(),
            },
        )),
    );
    assert!(owner.0.withdrawn.load(Ordering::Acquire));
    assert!(owner.0.integrity_failed.load(Ordering::Acquire));
    assert_eq!(
        std::fs::read(state.join(INTEGRITY_FAILURES)).unwrap(),
        b"not json"
    );
}

/// A controller root whose saved active publication carries full completion
/// evidence, as a previous process left it, and the fixture node.
struct Startup {
    root: tempfile::TempDir,
    config: Config,
    directory: PathBuf,
    record: transparent_shared::publication::RestoreRecord,
    finalized: Finalized,
    _rpc: CanonicalRpc,
}
impl Startup {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let journal = root.path().join("journal");
        manager_tests::copy_fixture_tree(
            &manager_tests::prepared_fixture(2).join("journal"),
            &journal,
        );
        let publications = root.path().join("publications");
        std::fs::create_dir(&publications).unwrap();
        let directory = publications.join("candidate-restored");
        manager_tests::copy_archive_publication(2, &directory);
        let set = ShardSet::open(&directory, 0).unwrap();
        let finalized = Finalized::prepare_full(&directory, &set, 2).unwrap();
        let end = set.map.shards.last().unwrap();
        let record = finalized
            .restore_record(true, 2, end.end_height, &end.terminal_block_hash)
            .unwrap();
        let rpc = canonical_rpc(root.path(), &set).await;
        let config = Config {
            data_dir: journal,
            publication_root: publications.clone(),
            initial_publication: directory.clone(),
            recent_from: 2,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: "decoupled-32k-32k".into(),
            rpc_url: rpc.url.clone(),
            rpc_cookie: root.path().join("node-cookie"),
            listen: "127.0.0.1:0".parse().unwrap(),
            source_sha: "fixture".into(),
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
            // The fixture node answers block hashes only; the saved journal
            // already covers the publication.
            follow_journal: true,
            snapshot_dir: None,
        };
        let saved = read_public(directory.clone(), Vec::new())
            .unwrap()
            .publication;
        publication::write_atomic(
            &publications.join("active.json"),
            &serde_json::to_vec(&saved).unwrap(),
        )
        .unwrap();
        let startup = Self {
            root,
            config,
            directory,
            record,
            finalized,
            _rpc: rpc,
        };
        startup.write_record(&serde_json::to_vec(&startup.record).unwrap());
        startup
    }
    fn write_record(&self, bytes: &[u8]) {
        publication::write_atomic(
            &self
                .directory
                .join(transparent_shared::publication::RESTORE_FILE),
            bytes,
        )
        .unwrap();
    }
    /// Start the controller with its validator held; returns its HTTPS front,
    /// task and stop handle once its router exists.
    async fn start(
        &self,
        name: &str,
    ) -> (
        Tls,
        tokio::task::JoinHandle<Result<(), BoxError>>,
        pir_pipeline::service::Shutdown,
    ) {
        crate::history::validator::HOLD
            .lock()
            .unwrap()
            .insert(self.config.publication_root.clone());
        let (send, routed) = tokio::sync::oneshot::channel();
        let send = Arc::new(std::sync::Mutex::new(Some(send)));
        let http = pir_pipeline::service::Http::new(move |_, router: Router| {
            let send = send.clone();
            async move {
                if let Some(send) = send.lock().unwrap().take() {
                    let _ = send.send(router);
                }
                std::future::pending::<std::io::Result<()>>().await
            }
        });
        let shutdown = pir_pipeline::service::Shutdown::default();
        let task = tokio::spawn(run_managed(
            self.config.clone(),
            crate::host_engine::PublisherEngine,
            http,
            shutdown.clone(),
        ));
        let router = tokio::time::timeout(std::time::Duration::from_secs(20), routed)
            .await
            .expect("router")
            .expect("controller started");
        let front = self.root.path().join(name);
        std::fs::create_dir_all(&front).unwrap();
        (tls_router(&front, router).await, task, shutdown)
    }
    fn release(&self) {
        crate::history::validator::HOLD
            .lock()
            .unwrap()
            .remove(&self.config.publication_root);
    }
}

async fn status_of(tls: &Tls) -> serde_json::Value {
    reqwest::Client::builder()
        .add_root_certificate(tls.cert.clone())
        .build()
        .unwrap()
        .get(format!("{}/v1/status", tls.origin))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn wait_status(
    tls: &Tls,
    what: &str,
    done: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let status = status_of(tls).await;
        if done(&status) {
            return status;
        }
        assert!(std::time::Instant::now() < deadline, "{what}: {status}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Flip one byte of `path` in place, keeping its length.
fn flip_first_byte(path: &Path) {
    use std::os::unix::fs::FileExt;
    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut first = [0u8; 1];
    file.read_exact_at(&mut first, 0).unwrap();
    file.write_all_at(&[first[0] ^ 1], 0).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn restarted_controller_serves_before_validation_and_keeps_serving_a_correctness_failure() {
    let startup = Startup::new().await;
    // The previous process listed a runtime whose body is corrupt under its
    // own digest: integrity passes and only the runtime check can fail it.
    let runtime = startup
        .finalized
        .inventory()
        .artifacts
        .iter()
        .find(|a| a.kind == Kind::RecentRuntime)
        .unwrap()
        .clone();
    let path = startup.directory.join(&runtime.path);
    let mut bad = std::fs::read(&path).unwrap();
    bad[64] ^= 1;
    publication::write_atomic(&path, &bad).unwrap();
    let mut inventory = startup.finalized.inventory().clone();
    inventory
        .artifacts
        .iter_mut()
        .find(|a| a.path == runtime.path)
        .unwrap()
        .sha256 = transparent_shared::publication::digest(&bad);
    startup.write_record(&with_inventory(&startup.record, &inventory));
    let (tls, task, shutdown) = startup.start("front").await;
    // Serving, eligible and retrievable while validation is held: nothing
    // loaded that runtime, or the restore would have failed.
    let status = wait_status(&tls, "restored serving", |s| s["phase"] == "serving").await;
    assert_eq!(status["restored"], true);
    assert_eq!(status["integrity_validation"]["state"], "pending");
    assert_eq!(status["correctness_validation"]["state"], "pending");
    let report = bootstrap::verify(config(&tls, startup.root.path().join("provider")))
        .await
        .unwrap();
    assert_eq!(report.map_sha256, startup.record.map_sha256);
    startup.release();
    let status = wait_status(&tls, "correctness failed", |s| {
        s["correctness_validation"]["state"] == "failed"
    })
    .await;
    assert_eq!(status["integrity_validation"]["state"], "passed");
    assert_eq!(status["phase"], "serving");
    assert_eq!(
        status["correctness_validation"]["inventory_sha256"],
        transparent_shared::publication::digest(&serde_json::to_vec(&inventory).unwrap())
    );
    // Still the same process, still serving the same inventory.
    bootstrap::verify(config(&tls, startup.root.path().join("provider-2")))
        .await
        .unwrap();
    shutdown.request();
    tokio::time::timeout(std::time::Duration::from_secs(20), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn restarted_controller_withdraws_on_body_corruption_and_stays_withdrawn_after_restart() {
    let startup = Startup::new().await;
    let table = startup
        .finalized
        .inventory()
        .artifacts
        .iter()
        .find(|a| a.kind == Kind::ArchiveTable)
        .unwrap()
        .clone();
    // Same-size corruption the restore cannot see without hashing bodies.
    flip_first_byte(&startup.directory.join(&table.path));
    let (tls, task, shutdown) = startup.start("front").await;
    wait_status(&tls, "restored serving", |s| s["phase"] == "serving").await;
    bootstrap::verify(config(&tls, startup.root.path().join("provider")))
        .await
        .unwrap();
    startup.release();
    let status = wait_status(&tls, "integrity failed", |s| {
        s["integrity_validation"]["state"] == "failed"
    })
    .await;
    assert_eq!(status["phase"], "integrity_failed");
    assert_eq!(status["integrity_validation"]["recorded"], true);
    assert!(
        bootstrap::verify(config(&tls, startup.root.path().join("provider-2")))
            .await
            .is_err()
    );
    // Shutdown joins the validator and releases the controller lock.
    shutdown.request();
    tokio::time::timeout(std::time::Duration::from_secs(20), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // A new process with the same evidence stays withdrawn and alive;
    // deleting the completion record would not clear the failure either.
    let (tls, task, shutdown) = startup.start("front-2").await;
    let status = wait_status(&tls, "restart withdrawn", |s| {
        s["phase"] == "integrity_failed"
    })
    .await;
    assert_eq!(
        status["integrity_validation"]["artifact"]["path"],
        table.path
    );
    assert!(
        bootstrap::verify(config(&tls, startup.root.path().join("provider-3")))
            .await
            .is_err()
    );
    assert!(!task.is_finished());
    shutdown.request();
    tokio::time::timeout(std::time::Duration::from_secs(20), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    startup.release();
    assert!(integrity_failures(&startup.config.publication_root)
        .unwrap()
        .iter()
        .any(|f| f.path == table.path));
}

#[tokio::test(flavor = "multi_thread")]
async fn restarted_controller_refuses_unusable_evidence_without_suggesting_its_deletion() {
    let startup = Startup::new().await;
    startup.write_record(b"{");
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        run_managed(
            startup.config.clone(),
            crate::host_engine::PublisherEngine,
            pir_pipeline::service::Http::new(|_, _: Router| std::future::pending()),
            Default::default(),
        ),
    )
    .await
    .unwrap()
    .unwrap_err()
    .to_string();
    assert!(error.contains("cannot be restored"), "{error}");
    assert!(error.contains("does not clear"), "{error}");
    assert!(!error.contains("remove "), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn restarted_controller_does_not_serve_a_restored_anchor_the_node_replaced() {
    let startup = Startup::new().await;
    *startup._rpc.hash.write().unwrap() = Some("00".repeat(32));
    let (tls, task, shutdown) = startup.start("front").await;
    let status = wait_status(&tls, "publication refused", |s| {
        !s["publication_error"].is_null() || s["phase"] != "starting"
    })
    .await;
    assert_ne!(status["phase"], "serving", "{status}");
    assert!(status["restored"].is_null(), "{status}");
    assert!(
        bootstrap::verify(config(&tls, startup.root.path().join("provider")))
            .await
            .is_err()
    );
    shutdown.request();
    tokio::time::timeout(std::time::Duration::from_secs(20), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    startup.release();
}

#[tokio::test(flavor = "multi_thread")]
async fn without_completion_evidence_corrupt_bytes_are_prepared_in_full_and_never_served() {
    let startup = Startup::new().await;
    let table = startup
        .finalized
        .inventory()
        .artifacts
        .iter()
        .find(|a| a.kind == Kind::ArchiveTable)
        .unwrap()
        .clone();
    flip_first_byte(&startup.directory.join(&table.path));
    std::fs::remove_file(
        startup
            .directory
            .join(transparent_shared::publication::RESTORE_FILE),
    )
    .unwrap();
    let failures = startup.config.publication_root.join(INTEGRITY_FAILURES);
    let recorded = serde_json::json!([{"path":table.path,"expected_sha256":table.sha256,
        "found_sha256":null,"inventory_sha256":"00".repeat(32),
        "map_sha256":startup.record.map_sha256,"reason":"recorded by an earlier process"}]);
    std::fs::write(&failures, serde_json::to_vec(&recorded).unwrap()).unwrap();
    let (tls, task, shutdown) = startup.start("front").await;
    let status = wait_status(&tls, "full preparation refused", |s| {
        !s["publication_error"].is_null()
    })
    .await;
    assert_ne!(status["phase"], "serving", "{status}");
    assert!(
        bootstrap::verify(config(&tls, startup.root.path().join("provider")))
            .await
            .is_err()
    );
    shutdown.request();
    tokio::time::timeout(std::time::Duration::from_secs(20), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    startup.release();
    // The cold path neither served nor discarded the durable failure.
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&std::fs::read(&failures).unwrap()).unwrap(),
        recorded
    );
}
