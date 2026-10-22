//! Current serving roles for a separately running publisher process. Tests drive
//! sequential consumer passes; the publisher receives no consumer acknowledgments.
use enhance_query_server::{
    ingress_sync, router_sync, synchronization::Config as SourceConfig, worker_sync,
};
use pir_e2e_tests::enhance_fixture::{serve, tls_with_discovery, Listening, Tls};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
struct Consumers {
    workers: Vec<worker_sync::Synchronizer>,
    router: router_sync::Synchronizer,
    ingress: ingress_sync::Synchronizer,
}
pub struct Roles {
    pub origin: String,
    artifact_listen: std::net::SocketAddr,
    management_listen: std::net::SocketAddr,
    consumers: Arc<tokio::sync::Mutex<Consumers>>,
    _listeners: Vec<Listening>,
    _tls: Tls,
    _root: PathBuf,
    minimum_generation: std::sync::atomic::AtomicU64,
}
fn address() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
impl Roles {
    pub async fn for_process(root: &Path) -> Result<Self, String> {
        std::fs::create_dir_all(root).map_err(|e| e.to_string())?;
        let artifact_listen = address();
        // The publisher serves discovery on its management listener, which
        // keeps one address across restarts like the artifact listener.
        let management_listen = address();
        let transport = tls_with_discovery(
            root,
            &format!("http://{artifact_listen}"),
            Some(&management_listen.to_string()),
        )
        .await;
        // The reference packing is larger than 512 MiB even for the smallest
        // domain. Bound scratch space by its actual format plus two row files;
        // this fixture does not change any role's resident admission budget.
        let scratch = enhance_query_server::prepared_packing::expected_len(4096)
            .unwrap()
            .max(400 * 1024 * 1024)
            + 2 * 2048 * enhance_query_server::ROW_BYTES as u64
            + 3 * enhance_query_server::publication::METADATA_LIMIT;
        let source = SourceConfig {
            publisher_origin: transport.origin.clone(),
            trusted_ca: Some(root.join("ca.pem")),
            download_budget_bytes: scratch,
            // Handover retains the serving generation while downloading its
            // successor. Both complete packing objects must fit in the fixture.
            cache_limit_bytes: scratch * 2,
        };
        let mut workers = Vec::new();
        let mut origins = Vec::new();
        let mut listeners = Vec::new();
        for index in 0..2 {
            let worker = worker_sync::Worker::open(
                &root.join(format!("worker-{index}")),
                worker_sync::Config {
                    source: source.clone(),
                    shards: [0].into(),
                },
                Default::default(),
                Default::default(),
            )
            .map_err(|e| e.to_string())?;
            workers.push(worker.synchronizer().map_err(|e| e.to_string())?);
            let http = serve(worker.router()).await;
            origins.push(http.url.clone());
            listeners.push(http);
        }
        let router = router_sync::PackingRouter::open(
            &root.join("router"),
            router_sync::Config {
                source: source.clone(),
                workers: [(0, origins)].into(),
                preferred: Default::default(),
            },
            6,
            4,
        )
        .map_err(|e| e.to_string())?;
        let router_sync = router.synchronizer().map_err(|e| e.to_string())?;
        let router_http = serve(router.public_router()).await;
        let router_control = serve(router.control_router()).await;
        let ingress = ingress_sync::QueryIngress::open(
            &root.join("ingress"),
            ingress_sync::Config {
                source,
                routers: [(
                    0,
                    vec![ingress_sync::RouterPeer {
                        control_origin: router_control.url.clone(),
                        query_origin: router_http.url.clone(),
                    }],
                )]
                .into(),
            },
            16,
            256,
        )
        .map_err(|e| e.to_string())?;
        let ingress_sync = ingress.synchronizer().map_err(|e| e.to_string())?;
        let public = serve(ingress.public_router()).await;
        let origin = public.url.clone();
        listeners.extend([router_http, router_control, public]);
        Ok(Self {
            origin,
            artifact_listen,
            management_listen,
            consumers: Arc::new(tokio::sync::Mutex::new(Consumers {
                workers,
                router: router_sync,
                ingress: ingress_sync,
            })),
            _listeners: listeners,
            _tls: transport,
            _root: root.into(),
            minimum_generation: std::sync::atomic::AtomicU64::new(1),
        })
    }
    pub async fn synchronize(&self) {
        let mut consumers = self.consumers.lock().await;
        for worker in &mut consumers.workers {
            if let Err(error) = worker.poll_once().await {
                eprintln!("worker synchronization: {error}");
            }
        }
        if let Err(error) = consumers.router.poll_once().await {
            eprintln!("router synchronization: {error}");
        }
        if let Err(error) = consumers.ingress.poll_once().await {
            eprintln!("ingress synchronization: {error}");
        }
    }
    pub fn accepts(&self, manifest: &enhance_query_server::protocol::Manifest) -> bool {
        manifest.generation
            >= self
                .minimum_generation
                .load(std::sync::atomic::Ordering::Acquire)
    }
    pub fn publisher_config(
        &self,
        root: &Path,
        listen: &str,
        rpc: &str,
    ) -> Result<serde_json::Value, String> {
        let durable = root.join("data/publication/publisher.json");
        let minimum = if durable.exists() {
            let state: serde_json::Value =
                serde_json::from_slice(&std::fs::read(durable).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            state["next_generation"]
                .as_u64()
                .ok_or("missing generation reservation")?
        } else {
            1
        };
        self.minimum_generation
            .store(minimum, std::sync::atomic::Ordering::Release);
        Ok(
            serde_json::json!({ "management_listen": self.management_listen, "enhance": { "enabled": true, "settings": {
                "listen": listen, "artifact_listen": self.artifact_listen, "data_dir": root.join("data"),
                "zakura_rpc_url": rpc, "zakura_cookie": root.join("cookie"), "poll_seconds": 1, "prevout_cache_outputs": 1000
            }}}),
        )
    }
}
