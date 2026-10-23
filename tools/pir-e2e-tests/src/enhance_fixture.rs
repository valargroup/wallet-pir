//! Isolated current ownership boundary: local publisher, real HTTPS, and
//! independently loaded serving roles. No publisher-to-consumer control exists.
use enhance_publisher::{
    chain_source::ZakuraClient, journal::RecordJournal, publisher::EnhancePublisher,
};
use enhance_shared::{
    protocol::Manifest,
    publication::{AuthorityResponse, PREFIX},
    types::{DatabaseId, ENHANCE_LAYOUT},
    EnhanceRecord, EnhanceRecordParts,
};
use pir_query_server::enhance::{
    ingress_sync, router_sync, synchronization::Config as SourceConfig, worker_sync,
};
use std::{
    path::Path,
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, BufReader};
pub struct Listening {
    pub url: String,
    task: tokio::task::JoinHandle<()>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Listening {
    #[allow(dead_code)]
    pub async fn stop(mut self) {
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
pub fn distribution_router(
    distribution: &enhance_publisher::distribution::PublicationDistribution,
) -> axum::Router {
    distribution.router().route(
        pir_service_discovery::PATH,
        axum::routing::get(|| async { pir_service_discovery::Document::publisher().to_json() }),
    )
}

pub async fn serve(router: axum::Router) -> Listening {
    serve_at(router, "127.0.0.1:0".parse().unwrap()).await
}
pub async fn serve_at(router: axum::Router, address: std::net::SocketAddr) -> Listening {
    let socket = tokio::net::TcpListener::bind(address).await.unwrap();
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
pub fn record(position: u64) -> Vec<u8> {
    EnhanceRecord::from_parts(EnhanceRecordParts {
        enc_ciphertext_suffix: [position as u8; 528],
        cv_net: [position.wrapping_add(1) as u8; 32],
        out_ciphertext: [position.wrapping_add(2) as u8; 80],
        has_transparent_inputs: true,
        has_transparent_outputs: false,
        metadata: enhance_shared::EnhanceTransactionMetadata::new(position as u32, Some(position))
            .unwrap(),
    })
    .as_bytes()
    .to_vec()
}

pub struct Tls {
    pub origin: String,
    pub client: reqwest::Client,
    _child: tokio::process::Child,
}
pub async fn tls(root: &Path, upstream: &str) -> Tls {
    tls_with_discovery(root, upstream, None).await
}
/// [`tls`] for a unified publisher, which serves service discovery on its
/// management listener: the edge proxies that exact path to `discovery`
/// (`host:port`), as a production edge does.
pub async fn tls_with_discovery(root: &Path, upstream: &str, discovery: Option<&str>) -> Tls {
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
    let mut command = tokio::process::Command::new("python3");
    if let Some(discovery) = discovery {
        command.env("DISCOVERY_UPSTREAM", discovery);
    }
    let mut child = command
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
        .stderr(Stdio::null())
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

pub struct System {
    pub root: tempfile::TempDir,
    pub publisher: EnhancePublisher,
    pub journal: RecordJournal,
    pub manifest: Manifest,
    pub origin: String,
    pub source: SourceConfig,
    pub workers: Vec<worker_sync::Synchronizer>,
    pub routers: Vec<router_sync::Synchronizer>,
    pub ingress: ingress_sync::Synchronizer,
    /// The one-process role, when the system serves through it.
    pub combined: Option<pir_query_server::enhance::combined::Combined>,
    canonical: Arc<Mutex<std::collections::BTreeMap<u64, String>>>,
    transport: Tls,
    _listeners: Vec<Listening>,
}
impl System {
    pub async fn start(records: Vec<Vec<u8>>) -> Self {
        Self::with_backend(records, Default::default(), 1, 1).await
    }
    pub async fn with_backend(
        records: Vec<Vec<u8>>,
        backend: enhance_shared::matvec::MatvecConfig,
        maximum_domains: u64,
        replicas: usize,
    ) -> Self {
        Self::at_anchor(
            records,
            backend,
            maximum_domains,
            replicas,
            enhance_shared::ACTIVATION_HEIGHT,
            "01".repeat(32),
        )
        .await
    }
    pub async fn at_anchor(
        records: Vec<Vec<u8>>,
        backend: enhance_shared::matvec::MatvecConfig,
        maximum_domains: u64,
        replicas: usize,
        height: u64,
        hash: String,
    ) -> Self {
        Self::build(
            records,
            backend,
            maximum_domains,
            replicas,
            height,
            hash,
            false,
        )
        .await
    }
    /// The same publisher, served by one combined Enhance role whose ingress,
    /// packing router and worker call each other directly.
    pub async fn combined(
        records: Vec<Vec<u8>>,
        backend: enhance_shared::matvec::MatvecConfig,
    ) -> Self {
        Self::build(
            records,
            backend,
            1,
            1,
            enhance_shared::ACTIVATION_HEIGHT,
            "01".repeat(32),
            true,
        )
        .await
    }
    async fn build(
        records: Vec<Vec<u8>>,
        backend: enhance_shared::matvec::MatvecConfig,
        maximum_domains: u64,
        replicas: usize,
        height: u64,
        hash: String,
        combined: bool,
    ) -> Self {
        assert!((1..=enhance_shared::protocol::MAX_QUERY_SHARDS).contains(&maximum_domains));
        assert!((1..=2).contains(&replicas));
        let root = tempfile::tempdir().unwrap();
        let canonical = Arc::new(Mutex::new(
            [(height, hash.clone())]
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>(),
        ));
        let node_state = canonical.clone();
        let node = serve(axum::Router::new().route(
            "/",
            axum::routing::post(move |axum::Json(request): axum::Json<serde_json::Value>| {
                let state = node_state.clone();
                async move {
                    assert_eq!(request["method"], "getblockhash");
                    let height = request["params"][0].as_u64().unwrap();
                    let result = state.lock().unwrap().get(&height).cloned().unwrap();
                    axum::Json(
                        serde_json::json!({"id": request["id"], "error": null, "result": result}),
                    )
                }
            }),
        ))
        .await;
        let cookie = root.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let publisher = EnhancePublisher::open(
            &root.path().join("publisher"),
            ZakuraClient::from_cookie_file(&node.url, cookie).unwrap(),
        )
        .unwrap();
        let mut journal = RecordJournal::open(
            root.path().join("journal"),
            DatabaseId::Enhance,
            ENHANCE_LAYOUT,
        )
        .unwrap();
        journal.append_block(height, hash, &records).unwrap();
        publisher
            .publish(
                publisher.work(&journal).unwrap().unwrap(),
                pir_publisher::publisher::PublisherEngine,
            )
            .await
            .unwrap();
        let publisher_http = serve(distribution_router(publisher.distribution())).await;
        let transport = tls(root.path(), &publisher_http.url).await;
        let source = SourceConfig {
            publisher_origin: transport.origin.clone(),
            trusted_ca: Some(root.path().join("ca.pem")),
            download_budget_bytes: 64 * 1024 * 1024 * 1024,
            cache_limit_bytes: 64 * 1024 * 1024 * 1024,
        };
        let response: AuthorityResponse = transport
            .client
            .get(format!("{}{PREFIX}current", transport.origin))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let manifest = response.current.manifest.unwrap();
        let mut listeners = vec![node, publisher_http];
        if combined {
            let role = pir_query_server::enhance::combined::Combined::open(
                &root.path().join("combined"),
                worker_sync::Config {
                    source: source.clone(),
                    shards: (0..maximum_domains).collect(),
                },
                pir_query_server::enhance::combined::Limits {
                    sealed_shards: 6,
                    requests: 16,
                    uploads: 256,
                    max_objects: 6,
                    packing_requests: 4,
                },
                backend,
            )
            .unwrap();
            let synchronizers = role.synchronizers().unwrap();
            let public = serve(role.public_router()).await;
            let origin = public.url.clone();
            listeners.push(public);
            let mut system = Self {
                root,
                publisher,
                journal,
                manifest,
                origin,
                source,
                workers: vec![synchronizers.worker],
                routers: vec![synchronizers.router],
                ingress: synchronizers.ingress,
                combined: Some(role),
                canonical,
                transport,
                _listeners: listeners,
            };
            system.synchronize().await;
            return system;
        }
        let mut workers = Vec::new();
        let mut worker_origins: std::collections::BTreeMap<u64, Vec<String>> = Default::default();
        for first in (0..maximum_domains).step_by(5) {
            let shards = (first..(first + 5).min(maximum_domains))
                .collect::<std::collections::BTreeSet<_>>();
            for replica in 0..replicas {
                let worker = worker_sync::Worker::open(
                    &root.path().join(format!("worker-{first}-{replica}")),
                    worker_sync::Config {
                        source: source.clone(),
                        shards: shards.clone(),
                    },
                    Default::default(),
                    backend,
                )
                .unwrap();
                workers.push(worker.synchronizer().unwrap());
                let http = serve(worker.router()).await;
                for id in &shards {
                    worker_origins
                        .entry(*id)
                        .or_default()
                        .push(http.url.clone());
                }
                listeners.push(http);
            }
        }
        let mut routers = Vec::new();
        let mut router_origins: std::collections::BTreeMap<u64, Vec<ingress_sync::RouterPeer>> =
            Default::default();
        for first in (0..maximum_domains).step_by(6) {
            let configured = worker_origins
                .iter()
                .filter(|(id, _)| **id >= first && **id < first + 6)
                .map(|(id, origins)| (*id, origins.clone()))
                .collect();
            let router = router_sync::PackingRouter::open(
                &root.path().join(format!("router-{first}")),
                router_sync::Config {
                    source: source.clone(),
                    workers: configured,
                    preferred: Default::default(),
                },
                6,
                4,
            )
            .unwrap();
            routers.push(router.synchronizer().unwrap());
            let http = serve(router.public_router()).await;
            let control = serve(router.control_router()).await;
            for id in first..(first + 6).min(maximum_domains) {
                router_origins.insert(
                    id,
                    vec![ingress_sync::RouterPeer {
                        control_origin: control.url.clone(),
                        query_origin: http.url.clone(),
                    }],
                );
            }
            listeners.extend([http, control]);
        }
        let ingress = ingress_sync::QueryIngress::open(
            &root.path().join("ingress"),
            ingress_sync::Config {
                source: source.clone(),
                routers: router_origins,
            },
            16,
            256,
        )
        .unwrap();
        let ingress_sync = ingress.synchronizer().unwrap();
        let public = serve(ingress.public_router()).await;
        let origin = public.url.clone();
        listeners.push(public);
        let mut system = Self {
            root,
            publisher,
            journal,
            manifest,
            origin,
            source,
            workers,
            routers,
            ingress: ingress_sync,
            combined: None,
            canonical,
            transport,
            _listeners: listeners,
        };
        system.synchronize().await;
        system
    }
    pub async fn synchronize(&mut self) {
        for worker in &mut self.workers {
            worker.poll_once().await.unwrap();
        }
        for router in &mut self.routers {
            router.poll_once().await.unwrap();
        }
        self.ingress.poll_once().await.unwrap();
    }
    pub async fn append(&mut self, records: Vec<Vec<u8>>) {
        let height = self.manifest.anchor_height + 1;
        let hash = format!("{height:064x}");
        self.append_at(height, hash, records).await;
    }
    pub async fn append_at(&mut self, height: u64, hash: String, records: Vec<Vec<u8>>) {
        self.canonical.lock().unwrap().insert(height, hash.clone());
        self.journal.append_block(height, hash, &records).unwrap();
        self.publish().await;
    }
    pub async fn publish(&mut self) {
        let anchor = self.journal.last_block().unwrap();
        self.canonical
            .lock()
            .unwrap()
            .insert(anchor.height, anchor.hash.clone());
        if let Some(work) = self.publisher.work(&self.journal).unwrap() {
            self.publisher
                .publish(work, pir_publisher::publisher::PublisherEngine)
                .await
                .unwrap();
        }
        let response: AuthorityResponse = self
            .transport
            .client
            .get(format!("{}{PREFIX}current", self.transport.origin))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        self.manifest = response.current.manifest.unwrap();
        self.synchronize().await;
    }
}
