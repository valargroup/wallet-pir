//! Operator commands on the query-server binary: role config, one-shot
//! Transparent verification, evaluator check, row repair flags.
use std::process::Command;

#[cfg(all(feature = "enhance", feature = "status"))]
#[path = "support/status_tls.rs"]
mod contract_tls;

fn binary() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pir-query-server"));
    command
        .env_remove("ENHANCE_MATVEC_BACKEND")
        .env_remove("ENHANCE_CUDA_DEVICE");
    command
}

/// Exercise the real binary through a real TLS discovery endpoint while the
/// publisher moves to an incompatible major and returns to its predecessor.
#[cfg(all(feature = "enhance", feature = "status"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contract_cutover_and_rollback_gate_distributed_and_unified_startup() {
    use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
    use std::sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;
    type Counters = Arc<(AtomicU64, AtomicUsize)>;
    let current = pir_sync_manager::service_discovery::version().major;
    let counters: Counters = Arc::new((AtomicU64::new(current), AtomicUsize::new(0)));
    let app = Router::new()
        .route(
            pir_sync_manager::service_discovery::PATH,
            get(|State(counters): State<Counters>| async move {
                Json(serde_json::json!({"service": "pir-publisher",
                    "version": format!("{}.0.0", counters.0.load(Ordering::SeqCst))}))
            }),
        )
        .fallback(|State(counters): State<Counters>| async move {
            counters.1.fetch_add(1, Ordering::SeqCst);
            StatusCode::SERVICE_UNAVAILABLE
        })
        .with_state(counters.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let tls = contract_tls::Tls::start(&upstream).await;
    for unified in [false, true] {
        // The same executable must refuse the new publisher, then be allowed
        // through discovery after publisher rollback. No old process is reused.
        for advertised in [current + 1, current] {
            counters.0.store(advertised, Ordering::SeqCst);
            counters.1.store(0, Ordering::SeqCst);
            let temp = tempfile::tempdir().unwrap();
            let socket = || {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                listener.local_addr().unwrap().to_string()
            };
            let mut config = serde_json::json!({
                "management_listen": socket(),
                "status": {"enabled": true, "settings": {
                    "role": if unified {"combined"} else {"worker"},
                    "network_hex": "01".repeat(32), "state_dir": temp.path().join("status"),
                    "listen": socket(), "publisher": tls.origin, "publisher_ca": tls.certificate,
                }},
            });
            if unified {
                config["public_listen"] = socket().into();
                config["enhance"] = serde_json::json!({"enabled": true, "settings": {
                    "role": "combined", "control_listen": socket(),
                    "data_dir": temp.path().join("enhance"),
                    "synchronization": {"publisher_origin": tls.origin, "trusted_ca": tls.certificate,
                        "download_budget_bytes": 8u64 << 30, "cache_limit_bytes": 8u64 << 30,
                        "shards": [0]},
                }});
            }
            let path = write(temp.path(), config);
            let mut command = tokio::process::Command::from(binary());
            command
                .arg("--config")
                .arg(path)
                .env("RUST_LOG", "off")
                .kill_on_drop(true);
            if advertised != current {
                let output = tokio::time::timeout(Duration::from_secs(5), command.output())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(!output.status.success());
                assert!(String::from_utf8_lossy(&output.stderr).contains("incompatible"));
                assert_eq!(
                    counters.1.load(Ordering::SeqCst),
                    0,
                    "product metadata requested before compatibility was established"
                );
                assert!(!temp.path().join("status").exists());
                assert!(!temp.path().join("enhance").exists());
            } else {
                let mut child = command
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .spawn()
                    .unwrap();
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                while counters.1.load(Ordering::SeqCst) == 0 {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "compatible publisher did not allow product startup"
                    );
                    assert!(
                        child.try_wait().unwrap().is_none(),
                        "query process exited before product startup"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                child.kill().await.unwrap();
                child.wait().await.unwrap();
            }
        }
    }
    server.abort();
}

#[test]
fn config_check_validates_a_role_without_opening_state() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("query-server.json");
    std::fs::write(&path, r#"{"management_listen":"127.0.0.1:18080"}"#).unwrap();
    let output = binary()
        .arg("--config")
        .arg(&path)
        .arg("--check")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout)
        .contains("Query server configuration is valid; runtime readiness was not checked"));
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);

    let missing = binary().arg("--check").output().unwrap();
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("--config"));
}

#[test]
fn contract_info_reports_compiled_products_without_configuration() {
    let temp = tempfile::tempdir().unwrap();
    // A GPU backend request in the environment must not be initialized.
    let output = binary()
        .current_dir(temp.path())
        .env("ENHANCE_MATVEC_BACKEND", "cuda")
        .arg("--contract-info")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let info: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(info["schema"], "wallet-pir-contract-info/1");
    assert_eq!(info["executable"], "pir-query-server");
    let version = info["contract_version"].as_str().unwrap();
    assert_eq!(
        info["contract_major"].as_u64().unwrap().to_string(),
        version.split('.').next().unwrap()
    );
    let products: Vec<&str> = info["products"]
        .as_array()
        .unwrap()
        .iter()
        .map(|name| name.as_str().unwrap())
        .collect();
    // Cargo's feature builds can replace the CLI independently of this test
    // executable. Validate the operator report rather than this harness's flags.
    let distinct: std::collections::BTreeSet<_> = products.iter().copied().collect();
    assert_eq!(distinct.len(), products.len(), "duplicate product names");
    assert!(distinct.iter().all(|product| [
        "enhance",
        "status",
        "transparent_history",
        "transaction_details",
        "receiver",
    ]
    .contains(product)));
    let backends = info["backends"].as_array().unwrap();
    assert!(backends.len() <= 1);
    assert!(backends.iter().all(|backend| backend == "cuda"));
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);

    let mixed = binary()
        .args(["--contract-info", "--config", "missing.json"])
        .output()
        .unwrap();
    assert!(!mixed.status.success());
}

#[cfg(feature = "enhance")]
#[test]
fn check_backend_validates_matvec_before_any_worker_state() {
    let temp = tempfile::tempdir().unwrap();
    let output = binary()
        .current_dir(temp.path())
        .args(["check-backend", "--cuda-device=0"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --matvec-backend cuda"));
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);

    let output = binary()
        .current_dir(temp.path())
        .env("ENHANCE_MATVEC_BACKEND", "cuda")
        .env("ENHANCE_CUDA_DEVICE", "not-a-number")
        .args(["check-backend", "--matvec-backend=cpu", "--cuda-device=0"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --matvec-backend cuda"));
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[cfg(feature = "enhance")]
#[test]
fn environment_cuda_selection_fails_explicitly_for_invalid_device_or_build() {
    let temp = tempfile::tempdir().unwrap();
    let output = binary()
        .current_dir(temp.path())
        .env("ENHANCE_MATVEC_BACKEND", "cuda")
        .env("ENHANCE_CUDA_DEVICE", usize::MAX.to_string())
        .arg("check-backend")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    #[cfg(feature = "cuda")]
    assert!(error.contains("ordinal"), "{error}");
    #[cfg(not(feature = "cuda"))]
    assert!(error.contains("--features cuda"), "{error}");
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

fn write(dir: &std::path::Path, config: serde_json::Value) -> std::path::PathBuf {
    let path = dir.join("query-server.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    path
}

#[test]
fn verify_only_needs_one_compiled_transparent_section_and_opens_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let path = write(
        temp.path(),
        serde_json::json!({"management_listen": "127.0.0.1:18080"}),
    );
    let output = binary()
        .arg("--config")
        .arg(&path)
        .arg("--verify-only")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("exactly one"));
    assert!(
        output.stdout.is_empty(),
        "stdout is reserved for the report"
    );
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
    // A separate mode from --check.
    let output = binary()
        .arg("--config")
        .arg(&path)
        .args(["--check", "--verify-only"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot be used with"));
}

#[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
#[test]
fn transparent_verification_refuses_ordinary_flags_and_extra_products() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let (section, settings) = if cfg!(feature = "transparent-history") {
        (
            "transparent_history",
            serde_json::json!({"listen": "127.0.0.1:18092", "shard_dir": state}),
        )
    } else {
        (
            "transaction_details",
            serde_json::json!({"listen": "127.0.0.1:18095", "role": "combined",
                "publication_dir": state}),
        )
    };
    // Ordinary startup refuses the product-level flag before binding anything.
    let mut flagged = settings.clone();
    flagged["verify_only"] = true.into();
    let path = write(
        temp.path(),
        serde_json::json!({"management_listen": "127.0.0.1:18080",
            section: {"enabled": true, "settings": flagged}}),
    );
    for mode in [None, Some("--check")] {
        let mut command = binary();
        command.arg("--config").arg(&path);
        command.args(mode);
        let output = command.output().unwrap();
        assert!(!output.status.success(), "{mode:?}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("--verify-only"));
    }
    // A disabled section is still a typed section, and does no work.
    let path = write(
        temp.path(),
        serde_json::json!({"management_listen": "127.0.0.1:18080",
            section: {"enabled": false, "settings": settings}}),
    );
    let output = binary()
        .arg("--config")
        .arg(&path)
        .arg("--verify-only")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("exactly one"));
    assert!(!state.exists(), "refused verification opened no state");
}

#[cfg(all(
    feature = "transparent-history",
    any(feature = "enhance", feature = "status")
))]
#[test]
fn transparent_verification_refuses_other_enabled_products() {
    let temp = tempfile::tempdir().unwrap();
    let other = if cfg!(feature = "enhance") {
        "enhance"
    } else {
        "status"
    };
    let path = write(
        temp.path(),
        serde_json::json!({"management_listen": "127.0.0.1:18080",
            other: {"enabled": true},
            "transparent_history": {"enabled": true, "settings": {"shard_dir": temp.path().join("state")}}}),
    );
    let output = binary()
        .arg("--config")
        .arg(&path)
        .arg("--verify-only")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains(other));
}

#[cfg(feature = "enhance")]
#[test]
fn every_enhance_role_checks_real_json_without_opening_state_or_certificates() {
    let temp = tempfile::tempdir().unwrap();
    let source = serde_json::json!({"publisher_origin":"https://publisher.example", "trusted_ca":temp.path().join("missing.pem"),
        "download_budget_bytes": 8u64 << 30, "cache_limit_bytes": 8u64 << 30});
    for (role, responsibility) in [
        ("worker", serde_json::json!({"shards":[0,1]})),
        (
            "packing-router",
            serde_json::json!({"workers":{"0":["http://127.0.0.1:8091"]},"preferred":{"0":["http://127.0.0.1:8091"]}}),
        ),
        (
            "query-ingress",
            serde_json::json!({"routers":{"0":[{"control_origin":"http://127.0.0.1:8094", "query_origin":"http://127.0.0.1:8093"}]}}),
        ),
        // Rendered by tools/enhance/lib/roles.py:combined.
        ("combined", serde_json::json!({"shards":[0,1]})),
    ] {
        let mut synchronization = source.clone();
        synchronization
            .as_object_mut()
            .unwrap()
            .extend(responsibility.as_object().unwrap().clone());
        let mut settings = serde_json::json!({"role":role, "data_dir":temp.path().join(role), "synchronization":synchronization});
        let mut host = serde_json::json!({"management_listen":"127.0.0.1:18081"});
        if role == "query-ingress" || role == "combined" {
            // The ingress answers wallets on the host's shared listener.
            host["public_listen"] = serde_json::json!("127.0.0.1:8091");
        } else {
            settings["listen"] = serde_json::json!("127.0.0.1:8091");
        }
        if role == "combined" {
            settings["matvec"] = serde_json::json!({"matvec_backend":"cpu","cuda_device":null});
            settings["packing_requests"] = serde_json::json!(2);
        }
        if role != "worker" {
            settings["control_listen"] = serde_json::json!("127.0.0.1:8094");
        }
        host["enhance"] = serde_json::json!({"enabled":true,"settings":settings});
        let path = write(temp.path(), host);
        let output = binary()
            .arg("--config")
            .arg(path)
            .arg("--check")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{role}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!temp.path().join(role).exists());
    }
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
}

/// The binary accepts a combined Status role without a worker or token,
/// refuses one naming a peer, and stops before opening state or listeners when
/// its publisher cannot be discovered.
#[cfg(feature = "status")]
#[test]
fn combined_status_role_checks_and_refuses_an_undiscoverable_publisher() {
    let temp = tempfile::tempdir().unwrap();
    let port = |listener: std::net::TcpListener| listener.local_addr().unwrap().port();
    let management = port(std::net::TcpListener::bind("127.0.0.1:0").unwrap());
    let listen = port(std::net::TcpListener::bind("127.0.0.1:0").unwrap());
    let query = port(std::net::TcpListener::bind("127.0.0.1:0").unwrap());
    let publisher = port(std::net::TcpListener::bind("127.0.0.1:0").unwrap());
    let state = temp.path().join("combined");
    let settings = |worker_origin: &str| {
        serde_json::json!({
            "management_listen": format!("127.0.0.1:{management}"),
            "public_listen": format!("127.0.0.1:{query}"),
            "status": {"enabled": true, "settings": {
                "role": "combined", "network_hex": "01".repeat(32),
                "state_dir": state, "listen": format!("127.0.0.1:{listen}"),
                "publisher": format!("https://localhost:{publisher}"),
                "worker_origin": worker_origin,
            }},
        })
    };
    let configs = [
        temp.path().join("combined-config"),
        temp.path().join("peer-config"),
    ];
    configs
        .iter()
        .for_each(|dir| std::fs::create_dir(dir).unwrap());
    let combined = write(&configs[0], settings(""));
    let output = binary()
        .arg("--config")
        .arg(&combined)
        .arg("--check")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let peer = write(&configs[1], settings("http://127.0.0.1:8381"));
    let output = binary()
        .arg("--config")
        .arg(&peer)
        .arg("--check")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("names no worker"));

    let output = binary()
        .arg("--config")
        .arg(&combined)
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("discover"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!state.exists(), "state opened before discovery");
}

/// What the deployment renderer writes for a combined Status or Enhance role
/// is a configuration this binary accepts, without opening its state.
#[cfg(any(feature = "enhance", feature = "status"))]
#[test]
fn rendered_combined_roles_pass_the_binary_check() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let mut roles = Vec::new();
    #[cfg(feature = "status")]
    roles.push((
        "status",
        "/etc/status-pir/combined.json",
        serde_json::json!({"network_hex": "01".repeat(32), "state_dir": state,
            "listen": "127.0.0.1:8481", "query_listen": "127.0.0.1:8090",
            "publisher": "https://publisher.example"}),
    ));
    #[cfg(feature = "enhance")]
    roles.push((
        "enhance",
        "/etc/enhance-pir/combined.json",
        serde_json::json!({"listen": "127.0.0.1:8090", "control_listen": "127.0.0.1:8096",
            "data_dir": state, "requests": 8, "uploads": 64, "max_objects": 2,
            "synchronization": {"publisher_origin": "https://publisher.example",
                "trusted_ca": temp.path().join("missing.pem"),
                "download_budget_bytes": 8u64 << 30, "cache_limit_bytes": 8u64 << 30,
                "shards": [0, 1]}}),
    ));
    for (service, path, settings) in roles {
        let input = temp.path().join(format!("{service}-settings.json"));
        std::fs::write(&input, serde_json::to_vec(&settings).unwrap()).unwrap();
        let rendered = Command::new("python3")
            .arg(root.join("tools/scripts/wallet-pir-roles.py"))
            .args(["--service", service, "--role", "combined", "--settings"])
            .arg(&input)
            .args(["--release", "/opt/example/release"])
            .output()
            .unwrap();
        assert!(
            rendered.status.success(),
            "{service}: {}",
            String::from_utf8_lossy(&rendered.stderr)
        );
        let described: serde_json::Value = serde_json::from_slice(&rendered.stdout).unwrap();
        assert_eq!(
            described["check"],
            serde_json::json!(["--config", path, "--check"])
        );
        let config = temp.path().join(format!("{service}.json"));
        std::fs::write(&config, described["files"][path].as_str().unwrap()).unwrap();
        let output = binary()
            .arg("--config")
            .arg(&config)
            .arg("--check")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{service}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!state.exists(), "{service}: state opened by a check");
    }
}
