//! Real producer → TLS termination → strict production-loader bootstrap.
use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_publisher::transparent::filter as transparent_filter;

use super::*;
use tokio::io::{AsyncBufReadExt, BufReader};
use transparent_events::TransparentEvent;
use transparent_filter::ScriptBytes;
use transparent_query_server::routing::bootstrap;
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
        last_init: std::sync::Mutex::new(None),
        status: RwLock::new(serde_json::json!({})),
        http: reqwest::Client::new(),
        observed: RwLock::new(BTreeMap::new()),
        membership: None,
        prepared_set: std::sync::Mutex::new(None),
        runtime_preparations: AtomicU64::new(0),
        runtime_preparation_failures: AtomicU64::new(0),
    }))
}

fn publication(root: &Path) -> (PathBuf, ShardSet) {
    publication_fixture(root, 2, !cfg!(feature = "qualification-tests"))
}
#[cfg(feature = "qualification-tests")]
fn publication_through(root: &Path, through: u64) -> (PathBuf, ShardSet) {
    publication_fixture(root, through, false)
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
    use transparent_query_server::routing::router::{plan, RosterEntry};
    use transparent_shared::history::assignment::{GeneratedBy, WorkerRole};
    use transparent_shared::history::shardset::{LoadOptions, LoadScope};
    let roster = [
        RosterEntry {
            id: "recent".into(),
            role: WorkerRole::RecentReplica,
            replica_group: Some("recent".into()),
            ssh_host: String::new(),
            upstream: "127.0.0.1:0".into(),
            cache_bytes: 4 << 30,
            archive_range: None,
        },
        RosterEntry {
            id: "archive".into(),
            role: WorkerRole::ArchiveOwner,
            replica_group: None,
            ssh_host: String::new(),
            upstream: "127.0.0.1:0".into(),
            cache_bytes: u64::MAX,
            archive_range: None,
        },
    ];
    let assignment = plan(
        &set.map,
        &set.map_digest,
        &roster,
        1,
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
            worker_id: "recent".into(),
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

#[tokio::test(flavor = "multi_thread")]
async fn https_bootstrap_verifies_real_prepared_recent_without_archive_tables() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set) = publication(root.path());
    // Incomplete preparation must fail finalization, never produce a current.
    assert!(Finalized::prepare(&directory, &set, 2).is_err());
    prepare_recent(&directory, &set);
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
    assert!(report.runtime_preparation["shipped"].as_u64().unwrap() > 0);
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
        publication_fixture(root.path(), 2, true)
    } else {
        let directory = root.path().join("publication");
        manager_tests::copy_prepared_publication(2, &directory);
        let set = ShardSet::open(&directory, 0).unwrap();
        (directory, set)
    };
    let recent_files: std::collections::BTreeSet<_> = std::fs::read_dir(&directory)
        .unwrap()
        .map(Result::unwrap)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "runtime"))
        .map(|entry| entry.file_name().into_string().unwrap())
        .collect();
    let prepared =
        transparent_shared::runtime::prebuild::prepare_publication(&set, &directory, &[], &|| {
            false
        })
        .unwrap();
    assert!(prepared.failures.is_empty());
    if !cfg!(feature = "qualification-tests") {
        assert!(!recent_files.is_empty());
        assert_eq!(prepared.linked(), recent_files.len());
        assert!(
            prepared.built() > 0,
            "archive runtimes must still be prepared"
        );
        for file in &prepared.files {
            assert_eq!(file.linked, recent_files.contains(&file.name));
        }
    }
    let finalized = Finalized::prepare_full(&directory, &set, 2).unwrap();
    assert!(finalized
        .inventory()
        .artifacts
        .iter()
        .any(|f| f.kind == Kind::ArchiveRuntime));
    let rpc = canonical_rpc(root.path(), &set).await;
    let authority = authority(&directory, Some(finalized), rpc.client.clone());
    let tls = tls(root.path(), authority).await;
    let mut manager = transparent_query_server::routing::manager::Manager::new(
        config(&tls, root.path().join("consumer")),
        transparent_query_server::config::ServiceConfig {
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
        .command(transparent_query_server::history::live::Command::Status)
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
    let mut restarted = transparent_query_server::routing::manager::Manager::new(
        config(&tls, root.path().join("consumer")),
        transparent_query_server::config::ServiceConfig {
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
        .command(transparent_query_server::history::live::Command::Status)
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
    let mut damaged = transparent_query_server::routing::manager::Manager::new(
        config(&tls, consumer.clone()),
        transparent_query_server::config::ServiceConfig {
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

/// Publisher service discovery gates one-shot verification and supervises a
/// running real HTTPS history process. Only a confirmed major version mismatch
/// stops it, and a recovered predecessor cannot mask that mismatch at restart.
#[tokio::test(flavor = "multi_thread")]
async fn history_process_checks_publisher_discovery_and_stops_on_a_major_change() {
    use transparent_query_server::history::process::{run, run_with, HistoryProcess};
    let root = tempfile::tempdir().unwrap();
    let (directory, set, finalized) = manager_tests::immutable_publication(2);
    let rpc = canonical_rpc(root.path(), &set).await;
    let owner = authority(&directory, Some(finalized), rpc.client.clone());
    let tls = tls(root.path(), owner).await;
    let consumer = root.path().join("consumer");
    let process = |verify_only: bool| -> HistoryProcess {
        serde_json::from_value(serde_json::json!({
            "publisher": tls.origin,
            "publisher_network": "main",
            "publisher_genesis": transparent_filter::MAINNET_GENESIS_DISPLAY,
            "publisher_ca": root.path().join("ca.pem"),
            "shard_dir": consumer,
            "cache_bytes": 4u64 << 30,
            "bootstrap_max_bytes": 4u64 << 30,
            "verify_only": verify_only,
        }))
        .unwrap()
    };

    // Verification refuses a publisher without discovery before any state.
    std::fs::write(&tls.control, "discovery-missing").unwrap();
    let error = run(process(true)).await.unwrap_err();
    assert!(error.to_string().contains("HTTP 404"), "{error}");
    assert!(!transparent_query_server::is_incompatible(&error));
    assert!(!consumer.exists(), "verification opened consumer state");

    std::fs::write(&tls.control, "").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let serving = listener.local_addr().unwrap();
    let running = tokio::spawn(run_with(process(false), Some(listener)));
    let health = format!("http://{serving}/v1/health");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    while reqwest::get(&health).await.is_err() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "process never served"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    std::fs::write(
        &tls.control,
        r#"discovery-body:{"service":"pir-publisher","version":"2.0.0"}"#,
    )
    .unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(60), running)
        .await
        .expect("an incompatible publisher must stop the history process")
        .unwrap()
        .unwrap_err();
    assert!(transparent_query_server::is_incompatible(&error), "{error}");
    assert!(error.to_string().contains("2.0.0"), "{error}");
    assert!(
        tokio::net::TcpStream::connect(serving).await.is_err(),
        "wallet serving outlived an incompatible publisher"
    );

    // The recovered active view on disk cannot mask the mismatch at restart.
    assert!(consumer.join("active.json").exists());
    let error = run_with(process(false), None).await.unwrap_err();
    assert!(transparent_query_server::is_incompatible(&error), "{error}");
}
