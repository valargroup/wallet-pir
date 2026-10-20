//! The real publisher distribution router, node RPC and TLS transport.
use super::encrypted::{record, serve};
use enhance_publisher::{distribution::Distribution, zakura::ZakuraClient};
use enhance_query_server::{
    protocol::{self, Geometry, Lifecycle, Manifest},
    publication::{AuthorityRequest, AuthorityResponse, Inventory, Object, PREFIX},
    worker::Revocation,
};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, BufReader};

struct Tls {
    origin: String,
    client: reqwest::Client,
    _child: tokio::process::Child,
}

async fn tls(root: &Path, upstream: &str) -> Tls {
    let cert = root.join("ca.pem");
    let key = root.join("key.pem");
    assert!(tokio::process::Command::new("openssl")
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
            "-keyout",
        ])
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .unwrap()
        .success());
    let control = root.join("fault");
    std::fs::write(&control, "").unwrap();
    let mut child = tokio::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../src/pir-publisher/tests/support/distribution_tls.py"
        ))
        .arg(upstream.strip_prefix("http://").unwrap())
        .arg(&cert)
        .arg(key)
        .arg(control)
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut port = String::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        BufReader::new(child.stdout.take().unwrap()).read_line(&mut port),
    )
    .await
    .unwrap()
    .unwrap();
    let certificate = reqwest::Certificate::from_pem(&std::fs::read(cert).unwrap()).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(certificate)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    Tls {
        origin: format!("https://localhost:{}", port.trim()),
        client,
        _child: child,
    }
}

async fn check_object(tls: &Tls, path: &str, object: &Object) {
    let mut response = tls
        .client
        .get(format!("{}{PREFIX}{path}/{}", tls.origin, object.sha256))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let mut bytes = 0u64;
    let mut hash = Sha256::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        bytes += chunk.len() as u64;
        assert!(bytes <= object.bytes);
        hash.update(&chunk);
    }
    assert_eq!(bytes, object.bytes);
    assert_eq!(hex::encode(hash.finalize()), object.sha256);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_publication_crosses_tls_without_consumers_and_source_loss_cannot_renew_authority()
{
    tokio::time::timeout(Duration::from_secs(30), async {
        let scratch = tempfile::tempdir().unwrap();
        let root = scratch.path().join("publication");
        let canonical = Arc::new(AtomicBool::new(true));
        let node_state = canonical.clone();
        let pause = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let node_pause = pause.clone();
        let node_entered = entered.clone();
        let node_release = release.clone();
        let node = serve(axum::Router::new().route("/", axum::routing::post(move |axum::Json(request): axum::Json<serde_json::Value>| {
            let node_state = node_state.clone();
            let node_pause = node_pause.clone();
            let node_entered = node_entered.clone();
            let node_release = node_release.clone();
            async move {
                assert_eq!(request["method"], "getblockhash");
                if node_pause.swap(false, Ordering::AcqRel) {
                    node_entered.notify_one();
                    node_release.notified().await;
                }
                axum::Json(serde_json::json!({"id": request["id"], "error": null, "result": if node_state.load(Ordering::Acquire) { "01".repeat(32) } else { "02".repeat(32) }}))
            }
        }))).await;
        let cookie = scratch.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let rpc = ZakuraClient::from_cookie_file(&node.url, cookie).unwrap();
        let distribution = Distribution::open(&root, rpc.clone()).unwrap();
        let upstream = serve(distribution.router()).await;
        let transport = tls(scratch.path(), &upstream.url).await;
        let current_url = format!("{}{PREFIX}current", transport.origin);
        let before: AuthorityResponse = transport.client.get(&current_url).send().await.unwrap().json().await.unwrap();
        assert!(!before.current.eligible);
        assert_eq!(before.observed_unix_ms, 0);
        let prepare_root = root.clone();
        let inventory = tokio::task::spawn_blocking(move || {
            enhance_publisher::publication::prepare(&prepare_root,
                Lifecycle::default().coverage(67, Geometry::default()).unwrap(), &[(0, "0".into())].into(),
                |start, count| Ok((start..start + count as u64).flat_map(record).collect()))
        }).await.unwrap().unwrap();
        let domain = &inventory.domains[0];
        let manifest = Manifest {
            recovery_epoch: 0, placement_revision: 1, domain_recovery_epochs: [(0, "0".into())].into(),
            schema_version: protocol::SCHEMA_VERSION, protocol_revision: protocol::PROTOCOL_REVISION.into(),
            network: "main".into(), pool: "ironwood".into(), generation: 1, anchor_height: 3428143,
            anchor_block_hash: "01".repeat(32), geometry: Geometry::default(), coverage: inventory.coverage.clone(),
            sessions: vec![domain.reference().unwrap()], unit_identities: [(0, domain.plan.units.clone())].into(),
        };
        distribution.publish(manifest.clone(), inventory.clone()).await.unwrap();
        let current: AuthorityResponse = transport.client.get(&current_url).send().await.unwrap().json().await.unwrap();
        assert!(current.current.eligible);
        assert!(current.current.succeeds(&before.current));
        assert_ne!(current.observed_unix_ms, 0);
        current.fresh(current.observed_unix_ms).unwrap();
        current.fresh(current.observed_unix_ms + 5000).unwrap();
        assert!(current.fresh(current.observed_unix_ms + 5001).is_err());
        assert!(current.fresh(current.observed_unix_ms - 1).is_err());
        let mut replay = current.current.clone();
        replay.revision -= 1;
        assert!(!replay.succeeds(&current.current));
        replay = current.current.clone();
        replay.version += 1;
        assert!(replay.validate().is_err());
        pause.store(true, Ordering::Release);
        let delayed_client = transport.client.clone();
        let delayed_url = current_url.clone();
        let delayed = tokio::spawn(async move {
            delayed_client.get(delayed_url).send().await.unwrap().json::<AuthorityResponse>().await.unwrap()
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified()).await.unwrap();
        let observed_before_delay = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        tokio::time::sleep(Duration::from_millis(30)).await;
        release.notify_one();
        assert!(delayed.await.unwrap().observed_unix_ms <= observed_before_delay, "source response delay cannot renew the observation time");
        let descriptor = current.current.inventory.as_ref().unwrap();
        check_object(&transport, "inventories", descriptor).await;
        let fetched: Inventory = transport.client.get(format!("{}{PREFIX}inventories/{}", transport.origin, descriptor.sha256)).send().await.unwrap().json().await.unwrap();
        assert!(fetched.supports(&manifest).unwrap());
        for domain in &fetched.domains {
            for object in domain.rows.iter().chain([&domain.hint, &domain.public, &domain.packing_object()]) {
                check_object(&transport, "objects", object).await;
            }
        }
        let mut broken = inventory.clone();
        broken.domains[0].rows[0].bytes -= 1;
        let mut later = manifest.clone();
        later.generation += 1;
        assert!(distribution.publish(later, broken).await.is_err());
        let unchanged: AuthorityResponse = transport.client.get(&current_url).send().await.unwrap().json().await.unwrap();
        assert_eq!(current.current, unchanged.current, "failed publication cannot change current");
        canonical.store(false, Ordering::Release);
        assert_eq!(transport.client.get(&current_url).header("If-None-Match", "unchanged").send().await.unwrap().status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        canonical.store(true, Ordering::Release);
        let request = AuthorityRequest { manifest_sha256: protocol::digest(&manifest) };
        let authority_url = format!("{}{PREFIX}authority", transport.origin);
        assert!(transport.client.post(&authority_url).json(&request).send().await.unwrap().status().is_success());
        pause.store(true, Ordering::Release);
        let pending_distribution = distribution.clone();
        let mut pending_manifest = manifest.clone();
        pending_manifest.generation += 1;
        let pending_inventory = inventory.clone();
        let publication = tokio::spawn(async move {
            pending_distribution.publish(pending_manifest, pending_inventory).await
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified()).await.unwrap();
        distribution.withdraw(&Revocation::default()).unwrap();
        release.notify_one();
        assert!(publication.await.unwrap().is_err(), "an in-flight publication cannot undo a source withdrawal");
        let withdrawn: AuthorityResponse = transport.client.get(&current_url).send().await.unwrap().json().await.unwrap();
        assert!(!withdrawn.current.eligible);
        assert_eq!(withdrawn.observed_unix_ms, 0);
        distribution.publish(manifest.clone(), inventory.clone()).await.unwrap();
        distribution.withdraw(&Revocation { recovery_epoch: 1, sessions: [hex::encode(manifest.session_id(0).unwrap())].into() }).unwrap();
        assert_eq!(transport.client.post(&authority_url).json(&request).send().await.unwrap().status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert!(distribution.publish(manifest, inventory).await.is_err());
        drop(upstream);
        drop(distribution);
        let reopened = Distribution::open(&root, rpc).unwrap();
        let listener = serve(reopened.router()).await;
        let restored: AuthorityResponse = reqwest::get(format!("{}{PREFIX}current", listener.url)).await.unwrap().json().await.unwrap();
        assert!(!restored.current.eligible);
        assert_eq!(restored.current.recovery_epoch, 1);
        assert_eq!(restored.current.revoked.len(), 1);
        assert_eq!(restored.observed_unix_ms, 0);
        assert!(!current.current.succeeds(&restored.current), "restart fence cannot replay the predecessor");
    }).await.expect("complete Enhance TLS publication exceeded thirty seconds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collection_keeps_retained_material_and_open_files_and_removes_interrupted_candidates() {
    use std::io::Read;
    tokio::time::timeout(Duration::from_secs(30), async {
        let scratch = tempfile::tempdir().unwrap();
        let root = scratch.path().join("publication");
        let node = serve(axum::Router::new().route("/", axum::routing::post(
            |axum::Json(request): axum::Json<serde_json::Value>| async move {
                axum::Json(serde_json::json!({"id": request["id"], "error": null, "result": "01".repeat(32)}))
            }
        ))).await;
        let cookie = scratch.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let distribution = Distribution::open(&root, ZakuraClient::from_cookie_file(&node.url, cookie).unwrap()).unwrap();
        let prepare_root = root.clone();
        let initial = tokio::task::spawn_blocking(move || enhance_publisher::publication::prepare(
            &prepare_root, Lifecycle::default().coverage(1, Geometry::default()).unwrap(),
            &[(0, "0".into())].into(), |start, count| Ok((start..start + count as u64).flat_map(record).collect()),
        )).await.unwrap().unwrap();
        let orphan = hex::encode(Sha256::digest(b"interrupted candidate"));
        let orphan_path = root.join("objects").join(orphan);
        std::fs::write(&orphan_path, b"interrupted candidate").unwrap();
        let partial_path = root.join("objects").join(format!("{}.tmp", "ab".repeat(32)));
        std::fs::write(&partial_path, b"unfinished write").unwrap();
        let mut pinned = std::fs::File::open(&orphan_path).unwrap();
        std::fs::write(root.join("objects/operator-note"), b"not collector-owned").unwrap();
        for epoch in 0..7 {
            let mut inventory = initial.clone();
            for unit in &mut inventory.domains[0].plan.units { unit.recovery_epoch = epoch; }
            let domain = &inventory.domains[0];
            let manifest = Manifest {
                recovery_epoch: epoch, placement_revision: 1, domain_recovery_epochs: [(0, epoch.to_string())].into(),
                schema_version: protocol::SCHEMA_VERSION, protocol_revision: protocol::PROTOCOL_REVISION.into(),
                network: "main".into(), pool: "ironwood".into(), generation: epoch + 1, anchor_height: 3428143,
                anchor_block_hash: "01".repeat(32), geometry: Geometry::default(), coverage: inventory.coverage.clone(),
                sessions: vec![domain.reference().unwrap()], unit_identities: [(0, domain.plan.units.clone())].into(),
            };
            std::fs::write(root.join("domains").join(format!("{}.json", protocol::digest(&domain.plan))), serde_json::to_vec(domain).unwrap()).unwrap();
            // Only retention uses these markers. Runtime loading is covered
            // independently without rebuilding identical bytes for each epoch.
            let runtime = root.join("preparation/artifacts-9").join(domain.plan.units[0].digest());
            std::fs::create_dir_all(&runtime).unwrap();
            std::fs::write(runtime.join("retention-marker"), b"runtime identity").unwrap();
            distribution.publish(manifest, inventory).await.unwrap();
        }
        assert_eq!(std::fs::read_dir(root.join("inventories")).unwrap().count(), protocol::RETAINED_GENERATIONS);
        assert_eq!(std::fs::read_dir(root.join("domains")).unwrap().count(), protocol::RETAINED_GENERATIONS);
        assert_eq!(std::fs::read_dir(root.join("preparation/artifacts-9")).unwrap().count(), protocol::RETAINED_GENERATIONS);
        assert!(!orphan_path.exists());
        assert!(!partial_path.exists());
        let mut bytes = String::new();
        pinned.read_to_string(&mut bytes).unwrap();
        assert_eq!(bytes, "interrupted candidate", "already-open streams survive collection");
        assert!(root.join("objects/operator-note").exists());
        initial.verify(&root).unwrap();
        assert!(root.join("current.json").exists());
        assert!(root.join("distribution.lock").exists());
        assert!(root.join("prepare.lock").exists());
    }).await.expect("Enhance retained publication collection exceeded thirty seconds");
}
