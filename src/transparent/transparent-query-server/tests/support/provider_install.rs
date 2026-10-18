//! Artifact consumer: all runtime commands run in a new extracted tree without PYTHONPATH.
use super::*;
use std::{
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use transparent_shared::publication::{self, Finalized};

async fn command(args: &[&str], cwd: &Path) -> Vec<u8> {
    let out = tokio::process::Command::new(args[0])
        .args(&args[1..])
        .current_dir(cwd)
        .env("PYTHONPATH", "")
        .output()
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}
async fn tls(root: &Path, upstream: &str) -> (String, tokio::process::Child) {
    let mut child = tokio::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/provider_tls.py"
        ))
        .arg(upstream.strip_prefix("http://").unwrap())
        .arg(root.join("ca.pem"))
        .arg(root.join("key.pem"))
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .await
        .unwrap();
    (
        format!("https://localhost:{}", line.trim().parse::<u16>().unwrap()),
        child,
    )
}
async fn row_query(origin: &str, set: &ShardSet, id: u64) {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use transparent_shared::history::shardset::TableRef;
    let shard = set.get(id).unwrap();
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
        shard.geometry.directory_rows,
        width as u32,
    )
    .unwrap();
    let setup: serde_json::Value = reqwest::get(format!(
        "{origin}/v2/shards/{id}/revisions/{}/setup/directory/0",
        shard.digest
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    let params = STANDARD
        .decode(setup["public_params"].as_str().unwrap())
        .unwrap();
    let (secret, upload) = profile.prepare(row).unwrap();
    let binding = transparent_shard::manifest::query_binding(&shard.digest, "directory");
    let mut body = binding.to_vec();
    body.extend(upload);
    let response = reqwest::Client::new()
        .post(format!(
            "{origin}/v2/shards/{id}/revisions/{}/query/directory",
            shard.digest
        ))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let answer = response.bytes().await.unwrap();
    assert_eq!(&answer[..8], &binding);
    assert_eq!(
        profile.decode(&secret, &params, &answer[16..]).unwrap(),
        source[row * width..(row + 1) * width]
    );
}
async fn status(origin: &str) -> serde_json::Value {
    reqwest::get(format!("{origin}/v1/provider-status"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}
async fn start(binary: &Path, args: &[String], cwd: &Path, origin: &str) -> tokio::process::Child {
    let log = std::fs::File::create(cwd.join("provider.log")).unwrap();
    let mut child = tokio::process::Command::new(binary)
        .args(args)
        .current_dir(cwd)
        .env("PYTHONPATH", "")
        .kill_on_drop(true)
        .stderr(log)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    for _ in 0..400 {
        if reqwest::get(format!("{origin}/v1/ready"))
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            return child;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "provider exited: {}",
            std::fs::read_to_string(cwd.join("provider.log")).unwrap()
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("provider did not start")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkout_free_artifacts_support_queries_and_restart() {
    installation_case(Case::Restart).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkout_free_artifacts_refuse_missing_and_incompatible_runtimes() {
    installation_case(Case::Invalid).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_processes_recover_during_peer_restart_and_outages() {
    installation_case(Case::Independent).await;
}
enum Case {
    Restart,
    Invalid,
    Independent,
}
async fn installation_case(case: Case) {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let source = root.join("publisher-data");
    std::fs::create_dir(&source).unwrap();
    let map = fixture(&source, &DECOUPLED_2K_8K);
    let set = ShardSet::open(&source, 0).unwrap();
    let prepared =
        transparent_shared::runtime::prebuild::prepare_publication(&set, &source, &[], &|| false)
            .unwrap();
    assert!(prepared.failures.is_empty());
    let finalized = Finalized::prepare(&source, &set, FIRST + 2).unwrap();
    let current = publication::Current {
        schema: publication::SCHEMA.into(),
        revision: 1,
        recovery_epoch: 0,
        inventory_sha256: finalized.sha256().into(),
        network: map.network.clone(),
        genesis_hash: map.genesis_hash.clone(),
        anchor_height: FIRST + 3,
        anchor_hash: map.shards[1].terminal_block_hash.clone(),
        eligibility: publication::Eligibility::Canonical,
        invalidated_from: None,
    };
    if matches!(case, Case::Independent) {
        // Keep the actual prepared producer inputs and pointer/inventory, not
        // merely source rows or hashes of temporary archives after teardown.
        retain_fixture("independent-prepared-inputs", &source);
        let records = root.join("publication-records");
        std::fs::create_dir(&records).unwrap();
        std::fs::write(
            records.join("current.json"),
            serde_json::to_vec(&current).unwrap(),
        )
        .unwrap();
        std::fs::write(records.join("inventory.json"), finalized.bytes()).unwrap();
        retain_fixture("independent-publication-records", &records);
    }
    let mut replies = BTreeMap::<String, Vec<u8>>::new();
    replies.insert(
        "/provider/v1/current".into(),
        serde_json::to_vec(&current).unwrap(),
    );
    replies.insert(
        format!("/provider/v1/inventories/{}", finalized.sha256()),
        finalized.bytes().to_vec(),
    );
    for file in &finalized.inventory().artifacts {
        if file.kind.local() {
            replies.insert(
                format!("/provider/v1/artifacts/{}", file.sha256),
                std::fs::read(finalized.file(&file.sha256).unwrap()).unwrap(),
            );
        }
    }
    for shard in &map.shards {
        let anchor = publication::AnchorCheck {
            schema: publication::SCHEMA.into(),
            revision: 1,
            recovery_epoch: 0,
            genesis_hash: map.genesis_hash.clone(),
            height: shard.end_height,
            hash: shard.terminal_block_hash.clone(),
            canonical: true,
        };
        replies.insert(
            format!(
                "/provider/v1/anchors/{}/{}",
                shard.end_height, shard.terminal_block_hash
            ),
            serde_json::to_vec(&anchor).unwrap(),
        );
    }
    let mut alternate_current = Vec::new();
    for missing in [true, false] {
        let mut inventory = finalized.inventory().clone();
        if missing {
            inventory
                .artifacts
                .retain(|f| f.kind != publication::Kind::RecentRuntime);
        } else {
            inventory.runtime.push_str("-incompatible");
        }
        let bytes = serde_json::to_vec(&inventory).unwrap();
        let mut pointer = current.clone();
        pointer.inventory_sha256 = publication::digest(&bytes);
        replies.insert(
            format!("/provider/v1/inventories/{}", pointer.inventory_sha256),
            bytes,
        );
        alternate_current.push(serde_json::to_vec(&pointer).unwrap());
    }
    let alternate_current = Arc::new(alternate_current);
    let inventory_fault = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let fault = inventory_fault.clone();
    let runtime_path = format!(
        "/provider/v1/artifacts/{}",
        finalized
            .inventory()
            .artifacts
            .iter()
            .find(|f| f.kind == publication::Kind::RecentRuntime)
            .unwrap()
            .sha256
    );
    let corrupt_runtime = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let corrupt = corrupt_runtime.clone();
    let replies = Arc::new(replies);
    let token = format!(
        "\"{}\"",
        publication::digest(&serde_json::to_vec(&current).unwrap())
    );
    let app = axum::Router::new().fallback(axum::routing::get(
        move |uri: axum::http::Uri, headers: axum::http::HeaderMap| {
            let mode = fault.load(std::sync::atomic::Ordering::Acquire);
            let unchanged = mode == 0
                && uri.path() == "/provider/v1/current"
                && headers
                    .get("if-none-match")
                    .is_some_and(|h| h.as_bytes() == token.as_bytes());
            let body = if mode > 0 && uri.path() == "/provider/v1/current" {
                Some(alternate_current[mode - 1].clone())
            } else if uri.path() == runtime_path
                && corrupt.load(std::sync::atomic::Ordering::Acquire)
            {
                Some(vec![0])
            } else {
                replies.get(uri.path()).cloned()
            };
            async move {
                if unchanged {
                    return (axum::http::StatusCode::NOT_MODIFIED, Vec::new());
                }
                body.map_or((axum::http::StatusCode::NOT_FOUND, Vec::new()), |b| {
                    (axum::http::StatusCode::OK, b)
                })
            }
        },
    ));
    let (publisher, publisher_server) = serve_app(app).await;
    command(
        &[
            "openssl",
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
            "-keyout",
            root.join("key.pem").to_str().unwrap(),
            "-out",
            root.join("ca.pem").to_str().unwrap(),
        ],
        root,
    )
    .await;
    let (publisher, mut publisher_tls) = tls(root, &publisher).await;
    // Reuse the exact publisher-built material through the production loader;
    // the terminal must not rebuild the fixture's runtimes a second time.
    let terminal = terminal_prepared(
        &source,
        &map,
        Some(transparent_shared::runtime::disk::ShippedRuntimes::required(source.clone())),
    );
    terminal.spawn_prewarm().await.unwrap();
    let (archive, archive_server) = serve(terminal).await;
    let (archive, mut archive_tls) = tls(root, &archive).await;
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .unwrap()
        .to_path_buf();
    // Package using existing release kinds, then use only the extracted consumer.
    let build = root.join("build/release");
    std::fs::create_dir_all(&build).unwrap();
    for (name, path) in [
        (
            "transparent-query-server",
            env!("CARGO_BIN_EXE_transparent-query-server"),
        ),
        ("shard-assign", env!("CARGO_BIN_EXE_shard-assign")),
        ("shard-prune", env!("CARGO_BIN_EXE_shard-prune")),
    ] {
        std::fs::copy(path, build.join(name)).unwrap();
    }
    let sha = String::from_utf8(command(&["git", "rev-parse", "HEAD"], &repo).await).unwrap();
    let sha = sha.trim();
    let package = format!("import sys; from pathlib import Path; sys.path.insert(0,{:?}); from wallet_pir_ops import release,bundle; r=Path({:?}); release.assemble({:?},r/'build',r/'artifacts','transparent-shard'); release.extract(r/'artifacts/transparent-shard.tar.gz',r/'binary',{:?},'transparent-shard'); bundle.assemble(r/'ops.tar.gz',{:?}); bundle.extract(r/'ops.tar.gz',r/'ops',{:?})",repo.join("tools/lib").to_str().unwrap(),root.to_str().unwrap(),sha,sha,sha,sha);
    command(&["python3", "-B", "-c", &package], root).await;
    if matches!(case, Case::Independent) {
        retain_fixture("independent-service-release", &root.join("artifacts"));
        retain_file("independent-operations.tar.gz", &root.join("ops.tar.gz"));
    }
    let renderer = root.join("ops/tools/scripts/wallet-pir-roles.py");
    command(&["python3", renderer.to_str().unwrap(), "--help"], root).await;
    assert!(!root.join("ops/.git").exists());
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = socket.local_addr().unwrap();
    drop(socket);
    let origin = format!("http://{addr}");
    let settings = serde_json::json!({"listen":addr.to_string(),"state_dir":root.join("state"),"publisher":publisher,
        "publisher_network":map.network,"publisher_genesis":map.genesis_hash,"archive_upstream":archive,"publisher_ca":root.join("ca.pem"),"archive_ca":root.join("ca.pem"),"cache_bytes":4u64<<30});
    let settings_path = root.join("settings.json");
    std::fs::write(&settings_path, serde_json::to_vec(&settings).unwrap()).unwrap();
    let rendered: serde_json::Value = serde_json::from_slice(
        &command(
            &[
                "python3",
                renderer.to_str().unwrap(),
                "--service",
                "transparent-history",
                "--role",
                "provider",
                "--settings",
                settings_path.to_str().unwrap(),
                "--release",
                root.join("binary").to_str().unwrap(),
            ],
            root,
        )
        .await,
    )
    .unwrap();
    let check: Vec<String> = rendered["check"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    let binary = root.join("binary/transparent-query-server");
    if matches!(case, Case::Invalid) {
        corrupt_runtime.store(false, std::sync::atomic::Ordering::Release);
        for (mode, name) in [(1, "missing"), (2, "incompatible")] {
            inventory_fault.store(mode, std::sync::atomic::Ordering::Release);
            let state = root.join(format!("state-{name}"));
            let mut arguments = check.clone();
            let at = arguments.iter().position(|a| a == "--shard-dir").unwrap() + 1;
            arguments[at] = state.to_str().unwrap().to_string();
            for verify_only in [true, false] {
                let args = if verify_only {
                    &arguments[..]
                } else {
                    &arguments[..arguments.len() - 1]
                };
                let refused = tokio::process::Command::new(&binary)
                    .args(args)
                    .current_dir(root)
                    .kill_on_drop(true)
                    .output()
                    .await
                    .unwrap();
                let error = String::from_utf8_lossy(&refused.stderr);
                assert!(
                    !refused.status.success(),
                    "{name} material unexpectedly accepted"
                );
                assert!(
                    error.contains("runtime"),
                    "{name} refused for another reason: {error}"
                );
                assert!(!state.join("active.json").exists(), "{name} became active");
            }
        }
        inventory_fault.store(0, std::sync::atomic::Ordering::Release);
        let control = tokio::process::Command::new(&binary)
            .args(&check)
            .current_dir(root)
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        assert!(
            control.status.success(),
            "valid material control failed: {}",
            String::from_utf8_lossy(&control.stderr)
        );
        let loaded: serde_json::Value = serde_json::from_slice(&control.stdout).unwrap();
        assert_eq!(loaded["runtimes"]["built"], 0);
        publisher_tls.kill().await.unwrap();
        publisher_tls.wait().await.unwrap();
        archive_tls.kill().await.unwrap();
        archive_tls.wait().await.unwrap();
        publisher_server.stop().await;
        archive_server.stop().await;
        return;
    }
    let args = &check[..check.len() - 1];
    if matches!(case, Case::Independent) {
        // Startup performs strict verification itself. The separate installation
        // cases above/below own negative and positive verify-only coverage.
        corrupt_runtime.store(false, std::sync::atomic::Ordering::Release);
        independent_processes(
            root,
            &binary,
            args,
            &origin,
            &set,
            (&mut publisher_tls, &mut archive_tls),
        )
        .await;
        publisher_server.stop().await;
        archive_server.stop().await;
        return;
    }
    // Real install/start preflight, then process startups from those same arguments.
    let refused = tokio::process::Command::new(&binary)
        .args(&check)
        .current_dir(root)
        .output()
        .await
        .unwrap();
    assert!(
        !refused.status.success(),
        "bad prepared bytes must refuse installation"
    );
    assert!(!root.join("state/active.json").exists());
    corrupt_runtime.store(false, std::sync::atomic::Ordering::Release);
    let preflight = tokio::process::Command::new(&binary)
        .args(&check)
        .current_dir(root)
        .output()
        .await
        .unwrap();
    assert!(
        preflight.status.success(),
        "{}",
        String::from_utf8_lossy(&preflight.stderr)
    );
    for _ in 0..2 {
        let mut child = start(&binary, args, root, &origin).await;
        let before = status(&origin).await;
        assert_eq!(before["synchronization"]["state"], "synchronized");
        assert_eq!(before["verified_publication"]["map_sha256"], set.map_digest);
        assert_eq!(before["runtime_readiness"]["required"], true);
        assert_eq!(before["runtime_readiness"]["preparation"]["built"], 0);
        assert_eq!(before["active_coverage"][0]["local"], false);
        assert_eq!(before["active_coverage"][1]["local"], true);
        if std::env::var_os("WALLET_PIR_FROZEN_CLIENT").is_some() {
            let recovered = frozen(origin.clone(), 1, FIRST).await;
            assert_eq!(
                recovered,
                serde_json::json!({
                    "covered_through":FIRST+3,"matched_shards":[0,1],"utxos":[],"unresolved":0,
                    "spends":[{"spent_txid":"01".repeat(32),"spent_index":0,
                        "spending_txid":"02".repeat(32),"input_index":0,"height":FIRST+2,
                        "value":123456,"script":hex::encode(script(1).as_slice())}],
                    "history":[{"txid":"01".repeat(32),"height":FIRST,"received":123456,"spent":0},
                        {"txid":"02".repeat(32),"height":FIRST+2,"received":0,"spent":123456}]
                })
            );
        } else {
            // Ordinary CPU runs compare exact producer rows; explicit original
            // client qualification compares the full independently derived ledger.
            row_query(&origin, &set, 0).await;
            row_query(&origin, &set, 1).await;
        }
        assert_eq!(
            status(&origin).await["archive_reachability"]["state"],
            "identity-checked"
        );
        child.kill().await.unwrap();
        child.wait().await.unwrap();
    }
    archive_tls.kill().await.unwrap();
    archive_tls.wait().await.unwrap();
    let mut child = start(&binary, args, root, &origin).await;
    let digest = &map.shards[0].manifest_digest;
    let refused = reqwest::get(format!(
        "{origin}/v2/shards/0/revisions/{digest}/setup/directory/0"
    ))
    .await
    .unwrap();
    assert_eq!(refused.status(), 503);
    let observed = status(&origin).await;
    assert_eq!(observed["archive_reachability"]["state"], "refused");
    assert_eq!(observed["runtime_readiness"]["warm"], true);
    row_query(&origin, &set, 1).await;
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    // Persisted bytes cannot permit an offline restart, nor can a configuration
    // origin assert archive readiness without a real runtime/identity check.
    publisher_tls.kill().await.unwrap();
    publisher_tls.wait().await.unwrap();
    let refused = tokio::process::Command::new(&binary)
        .args(&check)
        .current_dir(root)
        .output()
        .await
        .unwrap();
    assert!(!refused.status.success());
    publisher_server.stop().await;
    archive_server.stop().await;
}

async fn independent_processes(
    root: &Path,
    binary: &Path,
    args: &[String],
    origin: &str,
    set: &ShardSet,
    transports: (&mut tokio::process::Child, &mut tokio::process::Child),
) {
    let second = root.join("second");
    std::fs::create_dir(&second).unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = socket.local_addr().unwrap();
    drop(socket);
    let second_origin = format!("http://{addr}");
    let mut second_args = args.to_vec();
    for (flag, value) in [
        ("--listen", addr.to_string()),
        ("--shard-dir", second.join("state").display().to_string()),
    ] {
        let at = second_args.iter().position(|a| a == flag).unwrap() + 1;
        second_args[at] = value;
    }
    let (mut first, mut other) = tokio::join!(
        start(binary, args, root, origin),
        start(binary, &second_args, &second, &second_origin)
    );
    assert_ne!(first.id(), other.id(), "independent OS processes");
    assert!(root.join("state/active.json").is_file());
    assert!(second.join("state/active.json").is_file());
    for endpoint in [origin, second_origin.as_str()] {
        let observed = status(endpoint).await;
        assert_eq!(
            observed["verified_publication"]["map_sha256"],
            set.map_digest
        );
        assert_eq!(observed["runtime_readiness"]["required"], true);
        assert_eq!(observed["runtime_readiness"]["preparation"]["built"], 0);
        if std::env::var_os("WALLET_PIR_FROZEN_CLIENT").is_some() {
            let recovered = frozen(endpoint.into(), 1, FIRST).await;
            // Source-event oracle, independent of native table generation.
            assert_eq!(
                recovered,
                serde_json::json!({
                    "covered_through": FIRST+3, "matched_shards": [0,1], "utxos": [], "unresolved": 0,
                    "spends": [{"spent_txid":"01".repeat(32),"spent_index":0,
                        "spending_txid":"02".repeat(32),"input_index":0,"height":FIRST+2,
                        "value":123456,"script":hex::encode(script(1).as_slice())}],
                    "history":[{"txid":"01".repeat(32),"height":FIRST,"received":123456,"spent":0},
                        {"txid":"02".repeat(32),"height":FIRST+2,"received":0,"spent":123456}]
                })
            );
        } else {
            row_query(endpoint, set, 0).await;
            row_query(endpoint, set, 1).await;
        }
    }
    // A fresh process must recover its own durable state; the other provider
    // answers real encrypted local work throughout that restart.
    first.kill().await.unwrap();
    first.wait().await.unwrap();
    let ready = std::sync::atomic::AtomicBool::new(false);
    let (restarted, queries) = tokio::join!(
        async {
            let child = start(binary, args, root, origin).await;
            ready.store(true, std::sync::atomic::Ordering::Release);
            child
        },
        async {
            let mut queries = 0;
            loop {
                row_query(&second_origin, set, 1).await;
                queries += 1;
                if ready.load(std::sync::atomic::Ordering::Acquire) {
                    break queries;
                }
            }
        }
    );
    assert!(
        queries > 0,
        "one-at-a-time encrypted load spans peer restart"
    );
    first = restarted;
    if std::env::var_os("WALLET_PIR_FROZEN_CLIENT").is_some() {
        assert_eq!(
            frozen(origin.into(), 1, FIRST).await,
            frozen(second_origin.clone(), 1, FIRST).await,
            "restarted provider recovers the same independently checked ledger"
        );
    }
    row_query(origin, set, 0).await;
    row_query(origin, set, 1).await;
    // Publisher transport loss cannot revoke the valid active view. Losing
    // archive transport subsequently refuses historical work, never an empty
    // recovery, while each provider's valid recent worker stays independent.
    transports.0.kill().await.unwrap();
    transports.0.wait().await.unwrap();
    for endpoint in [origin, second_origin.as_str()] {
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            while status(endpoint).await["synchronization"]["state"] != "refused" {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("publisher outage must reach the real manager poll");
        row_query(endpoint, set, 0).await;
        row_query(endpoint, set, 1).await;
    }
    transports.1.kill().await.unwrap();
    transports.1.wait().await.unwrap();
    for endpoint in [origin, second_origin.as_str()] {
        let digest = &set.map.shards[0].manifest_digest;
        assert_eq!(
            reqwest::get(format!(
                "{endpoint}/v2/shards/0/revisions/{digest}/setup/directory/0"
            ))
            .await
            .unwrap()
            .status(),
            503
        );
        row_query(endpoint, set, 1).await;
    }
    first.kill().await.unwrap();
    first.wait().await.unwrap();
    other.kill().await.unwrap();
    other.wait().await.unwrap();
}
