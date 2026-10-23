//! Artifact consumer: all runtime commands run in a new extracted tree without PYTHONPATH.
//! The packaged `pir-query-server` runs the rendered provider configuration.
use zakura_pir_client::transparent::native::TableClientExt as _;

use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_query_server::transparent::native as transparent_native;

use super::*;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use transparent_shard::layout::DECOUPLED_2K_8K;
use transparent_shared::history::shardset::ShardSet;
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
            "/../../src/pir-query-server/tests/transparent/support/provider_tls.py"
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
/// The one host configuration the renderer installs for this role.
fn rendered_config(rendered: &serde_json::Value) -> serde_json::Value {
    let files = rendered["files"].as_object().unwrap();
    assert_eq!(files.len(), 1, "provider renders one configuration file");
    let (path, text) = files.iter().next().unwrap();
    let check = rendered["check"].as_array().unwrap();
    assert_eq!(
        check,
        &[
            serde_json::json!("--config"),
            serde_json::json!(path),
            serde_json::json!("--verify-only")
        ]
    );
    let mut document: serde_json::Value = serde_json::from_str(text.as_str().unwrap()).unwrap();
    let object = document.as_object().unwrap();
    assert_eq!(object.len(), 2, "management and one Transparent section");
    let settings = &document["transparent_history"]["settings"];
    assert!(settings.get("verify_only").is_none());
    let management = document["management_listen"].as_str().unwrap();
    assert!(management.starts_with("127.0.0.1:"));
    assert_ne!(settings["listen"].as_str().unwrap(), management);
    // The role's fixed host port would collide between concurrent test cases.
    document["management_listen"] = free_address().into();
    document
}
/// Installs a rendered configuration at a test-owned path.
fn write_config(dir: &Path, name: &str, document: &serde_json::Value) -> PathBuf {
    let path = dir.join(format!("{name}.json"));
    std::fs::write(&path, serde_json::to_vec_pretty(document).unwrap()).unwrap();
    path
}
fn config_args(path: &Path, verify_only: bool) -> Vec<String> {
    let mut args = vec!["--config".to_string(), path.display().to_string()];
    if verify_only {
        args.push("--verify-only".into());
    }
    args
}
fn free_address() -> String {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().to_string()
}
/// Starts the host on material its product must refuse; returns the product's
/// logged refusal once management reports it failed and never ready.
async fn refused_start(
    binary: &Path,
    config: &Path,
    document: &serde_json::Value,
    cwd: &Path,
) -> String {
    let log = cwd.join("refused-start.log");
    let file = std::fs::File::create(&log).unwrap();
    // The serving host logs to stdout; capture both streams at the level the
    // product's diagnostics are written.
    let mut child = tokio::process::Command::new(binary)
        .args(config_args(config, false))
        .current_dir(cwd)
        .env("RUST_LOG", "info")
        .kill_on_drop(true)
        .stderr(file.try_clone().unwrap())
        .stdout(file)
        .spawn()
        .unwrap();
    let management = format!(
        "http://{}/health",
        document["management_listen"].as_str().unwrap()
    );
    let mut failed = false;
    for _ in 0..3000 {
        if let Ok(response) = reqwest::get(&management).await {
            let health: serde_json::Value = response.json().await.unwrap();
            if health["products"]["transparent_history"]["phase"] == "failed" {
                assert_eq!(health["ready"], false);
                failed = true;
                break;
            }
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "host exited: {}",
            std::fs::read_to_string(&log).unwrap()
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    assert!(failed, "refused product was not reported failed");
    std::fs::read_to_string(&log).unwrap()
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
/// One spawned host serves Transparent history and a combined Status role
/// to independent wallet clients through one `public_listen`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_public_listener_serves_history_and_status_wallets() {
    installation_case(Case::SharedListener).await;
}
enum Case {
    Restart,
    Invalid,
    Independent,
    SharedListener,
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
        pir_service_discovery::PATH.into(),
        pir_service_discovery::Document::publisher().to_json(),
    );
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
            use std::io::Read;
            let mut bytes = Vec::new();
            finalized
                .open_file(&file.sha256)
                .unwrap()
                .unwrap()
                .read_to_end(&mut bytes)
                .unwrap();
            replies.insert(format!("/provider/v1/artifacts/{}", file.sha256), bytes);
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
    let canonical: std::collections::BTreeSet<(u64, String)> = map
        .shards
        .iter()
        .map(|shard| (shard.end_height, shard.terminal_block_hash.clone()))
        .collect();
    let genesis = map.genesis_hash.clone();
    let batch = move |axum::Json(request): axum::Json<publication::AnchorBatchRequest>| {
        let answer = publication::AnchorBatch {
            schema: publication::SCHEMA.into(),
            revision: request.revision,
            recovery_epoch: request.recovery_epoch,
            inventory_sha256: request.inventory_sha256.clone(),
            genesis_hash: genesis.clone(),
            results: request
                .anchors
                .iter()
                .map(|anchor| publication::AnchorResult {
                    height: anchor.height,
                    hash: anchor.hash.clone(),
                    canonical: canonical.contains(&(anchor.height, anchor.hash.clone())),
                })
                .collect(),
        };
        async move { axum::Json(answer) }
    };
    // The publisher edge proxies exactly this path to its management listener.
    let app = axum::Router::new()
        .route(
            pir_service_discovery::PATH,
            axum::routing::get(|| async { pir_service_discovery::Document::publisher().to_json() }),
        )
        .route("/provider/v1/anchors/check", axum::routing::post(batch))
        .fallback(axum::routing::get(
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
    // Compiled into pir-e2e-tests: tools/pir-e2e-tests is two levels below the root.
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap()
        .to_path_buf();
    // Package using existing release kinds, then use only the extracted consumer.
    let build = root.join("build/release");
    std::fs::create_dir_all(&build).unwrap();
    for (name, path) in [
        (
            "pir-query-server",
            env!("CARGO_BIN_EXE_pir-query-server-test"),
        ),
        ("shard-assign", env!("CARGO_BIN_EXE_shard-assign-test")),
        ("shard-prune", env!("CARGO_BIN_EXE_shard-prune-test")),
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
    let document = rendered_config(&rendered);
    let check = config_args(&write_config(root, "provider", &document), true);
    let binary = root.join("binary/pir-query-server");
    if matches!(case, Case::Invalid) {
        corrupt_runtime.store(false, std::sync::atomic::Ordering::Release);
        for (mode, name) in [(1, "missing"), (2, "incompatible")] {
            inventory_fault.store(mode, std::sync::atomic::Ordering::Release);
            let state = root.join(format!("state-{name}"));
            let mut variant = document.clone();
            variant["transparent_history"]["settings"]["shard_dir"] =
                state.display().to_string().into();
            let config = write_config(root, name, &variant);
            let refused = tokio::process::Command::new(&binary)
                .args(config_args(&config, true))
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
            // Ordinary startup refuses the same material: the product fails
            // and is never ready, while the host reports it as failed.
            let error = refused_start(&binary, &config, &variant, root).await;
            assert!(
                error.contains("runtime"),
                "{name} start refused for another reason: {error}"
            );
            assert!(!state.join("active.json").exists(), "{name} became active");
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
    if matches!(case, Case::SharedListener) {
        corrupt_runtime.store(false, std::sync::atomic::Ordering::Release);
        shared_listener(root, &binary, &document, &origin, &set).await;
        publisher_server.stop().await;
        archive_server.stop().await;
        return;
    }
    if matches!(case, Case::Independent) {
        // Startup performs strict verification itself. The separate installation
        // cases above/below own negative and positive verify-only coverage.
        corrupt_runtime.store(false, std::sync::atomic::Ordering::Release);
        independent_processes(
            root,
            &binary,
            &document,
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
    document: &serde_json::Value,
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
    let args = config_args(&root.join("provider.json"), false);
    // A separate process: its own state, wallet listener and management address.
    let mut second_document = document.clone();
    let settings = &mut second_document["transparent_history"]["settings"];
    settings["listen"] = addr.to_string().into();
    settings["shard_dir"] = second.join("state").display().to_string().into();
    second_document["management_listen"] = free_address().into();
    let second_args = config_args(&write_config(&second, "provider", &second_document), false);
    let args = &args[..];
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

/// Status and Transparent history wallets share one spawned host's
/// `public_listen`: real encrypted answers decode through canonical and
/// deployed paths, while private, management and disabled product routes stay
/// off that listener.
async fn shared_listener(
    root: &Path,
    binary: &Path,
    document: &serde_json::Value,
    origin: &str,
    set: &ShardSet,
) {
    use super::status_publisher::StatusPublisher;
    let publisher = StatusPublisher::start().await;
    let public = free_address();
    let status_listen = free_address();
    let mut document = document.clone();
    document["public_listen"] = public.clone().into();
    document["status"] = serde_json::json!({"enabled": true, "settings": {
        "role": "combined",
        "network_hex": hex::encode(status_shared::fixture::NETWORK),
        "state_dir": root.join("status-state"),
        "listen": status_listen,
        "publisher": publisher.origin,
        "publisher_ca": publisher.certificate,
    }});
    let config = write_config(root, "shared", &document);
    let mut child = start(binary, &config_args(&config, false), root, origin).await;
    let management = document["management_listen"].as_str().unwrap().to_string();
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        loop {
            if let Ok(response) = reqwest::get(format!("http://{management}/ready")).await {
                if response.status().is_success() {
                    break;
                }
            }
            assert!(child.try_wait().unwrap().is_none(), "host exited");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("both products become ready on the shared host");
    let public_origin = format!("http://{public}");
    // Transparent history: exact producer rows decode through the shared
    // listener's deployed alias, and the canonical namespace is served too.
    for id in [0, 1] {
        let digest = &set.map.shards[id].manifest_digest;
        let setup = reqwest::get(format!(
            "{public_origin}/v2/shards/{id}/revisions/{digest}/setup/directory/0"
        ))
        .await
        .unwrap();
        let code = setup.status();
        assert_eq!(code, 200, "shard {id}: {}", setup.text().await.unwrap());
    }
    row_query(&public_origin, set, 0).await;
    row_query(&public_origin, set, 1).await;
    for path in ["/v2/shards", "/transparent/v1/shards"] {
        let response = reqwest::get(format!("{public_origin}{path}"))
            .await
            .unwrap();
        assert_eq!(response.status(), 200, "{path}");
    }
    // Status: the independent wallet accepts the served anchor and decrypts
    // every fixture case from the combined role through the same listener.
    status_wallet(
        &public_origin,
        &publisher.snapshot,
        &root.join("provider.log"),
    )
    .await;
    let http = reqwest::Client::new();
    for prefix in ["/v1/status", "/status/v1"] {
        let malformed = http
            .post(format!("{public_origin}{prefix}/query"))
            .body(vec![0; 8])
            .send()
            .await
            .unwrap();
        assert_eq!(malformed.status(), 400, "{prefix}");
    }
    // Private product, management and disabled-product routes are absent.
    for path in [
        "/v1/provider-status",
        "/v1/ready",
        "/v1/health",
        "/health",
        "/ready",
        "/metrics",
        "/control/health",
        "/internal/health",
        "/v2/txid/init",
        "/transparent/v1/txid/init",
        "/enhance/v1/init",
    ] {
        let response = reqwest::get(format!("{public_origin}{path}"))
            .await
            .unwrap();
        assert_eq!(response.status(), 404, "{path}");
    }
    // The products' private listeners keep their own routes.
    assert_eq!(
        status(origin).await["verified_publication"]["map_sha256"],
        set.map_digest
    );
    let control = reqwest::get(format!("http://{status_listen}/control/health"))
        .await
        .unwrap();
    assert!(control.status().is_success());
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}

async fn status_wallet(origin: &str, snapshot: &status_shared::index::Snapshot, log: &Path) {
    use super::status_http::Loopback;
    use status_shared::protocol::Observation;
    use zakura_pir_client::status as wallet;
    let transport = Loopback {
        origin: origin.into(),
        http: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap(),
        corrupt_answer: Default::default(),
    };
    let anchor = wallet::AcceptedAnchor {
        network: snapshot.network,
        height: snapshot.height,
        hash: snapshot.anchor,
    };
    // Host readiness probes the Status control listener; its wallet routes
    // answer once the combined role has committed the publication.
    let pending = tokio::time::timeout(std::time::Duration::from_secs(120), async {
        loop {
            match wallet::transport::PendingClient::fetch(
                &transport,
                "https://status-wallet.invalid",
                status_shared::now_ms as fn() -> u64,
            )
            .await
            {
                Ok(pending) => break pending,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Status wallet routes never served the publication: {}",
            std::fs::read_to_string(log).unwrap_or_default()
        )
    });
    let client = pending.accept(&transport, &anchor).await.unwrap();
    let coverage = wallet::LocalCoverageContext {
        earliest_possible_inclusion: Some(snapshot.start),
        required_through: Some(snapshot.height),
    };
    for (txid, expected) in status_shared::fixture::cases(super::status_publisher::ENTRIES, false) {
        let expected = match expected {
            Observation::Mined(height) => wallet::Observation::Mined(height),
            Observation::Mempool => wallet::Observation::Mempool,
            Observation::Forked => wallet::Observation::Forked,
            Observation::NotFound => wallet::Observation::NotFound,
        };
        assert_eq!(
            client.observe(&transport, &txid, coverage).await.unwrap(),
            expected
        );
    }
}
