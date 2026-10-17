//! Real producer → TLS termination → strict production-loader bootstrap.
use super::*;
use tokio::io::{AsyncBufReadExt, BufReader};
use transparent_server_backend::distribution::{Finalized, Kind};
use transparent_server_backend::shardset::ShardSet;
use transparent_shard_server::bootstrap;

fn authority(directory: &Path, distribution: Option<Finalized>) -> Authority {
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
    let journal_dir = root.join("journal");
    let mut journal =
        EventStore::open(&journal_dir, transparent_filter::MAINNET_GENESIS_DISPLAY, 0).unwrap();
    for h in 0..3 {
        // One real recent receive; the archive has no private records. This
        // keeps native correctness cases small without changing any geometry.
        let events = if h == 2 {
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
                    height: 2,
                    txid: transparent_events::Txid([7; 32]),
                    transaction_index: 0,
                    output_index: 0,
                    value: 50_000,
                    coinbase: false,
                }),
            )]
        } else {
            Vec::new()
        };
        journal
            .append_block(h, BlockHash::from_internal_bytes([h as u8; 32]), &events)
            .unwrap();
    }
    journal.commit().unwrap();
    let output = root.join("publication");
    publication::publish(
        &PublishOptions {
            data_dir: journal_dir,
            output: output.clone(),
            previous: None,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: Some("decoupled-32k-32k".into()),
            recent_from: Some(2),
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
    )
    .unwrap();
    let set = ShardSet::open(&output, 0).unwrap();
    (output, set)
}
fn prepare_recent(directory: &Path, set: &ShardSet) {
    use transparent_shard_server::{
        assignment::{GeneratedBy, WorkerRole},
        router::{plan, RosterEntry},
        shardset::{LoadOptions, LoadScope},
    };
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
    let recent = ShardSet::open_with(
        directory,
        &LoadOptions {
            scope: LoadScope::Assigned {
                assignment: Arc::new(assignment),
                worker_id: "recent".into(),
            },
            ..LoadOptions::whole(0)
        },
    )
    .unwrap();
    let prepared =
        transparent_server_backend::prebuild::prepare_publication(&recent, directory, &[], &|| {
            false
        })
        .unwrap();
    assert!(prepared.failures.is_empty());
}

struct Tls {
    origin: String,
    cert: reqwest::Certificate,
    control: PathBuf,
    _child: tokio::process::Child,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Tls {
    fn drop(&mut self) {
        self.server.abort();
    }
}
async fn tls(root: &Path, authority: Authority) -> Tls {
    let cert = root.join("ca.pem");
    let key = root.join("key.pem");
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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, authority.router()).await.unwrap();
    });
    let control = root.join("fault");
    std::fs::write(&control, "").unwrap();
    let mut child = tokio::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/distribution_tls.py"
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
    let finalized = Finalized::prepare(&directory, &set, 2).unwrap();
    let expected = finalized.inventory().clone();
    let owner = authority(&directory, Some(finalized));
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
    assert!(error.to_string().contains("503"));
}

#[tokio::test(flavor = "multi_thread")]
async fn https_bootstrap_fails_closed_on_origin_network_and_transport_errors() {
    let root = tempfile::tempdir().unwrap();
    let (directory, set) = publication(root.path());
    prepare_recent(&directory, &set);
    let owner = authority(
        &directory,
        Some(Finalized::prepare(&directory, &set, 2).unwrap()),
    );
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
