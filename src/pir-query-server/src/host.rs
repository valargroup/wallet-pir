#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail"
))]
use crate::config::active;
use crate::{
    config::{Config, Probe},
    Result,
};
use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde_json::{json, Value};
#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail"
))]
use std::future::Future;
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    task::{Id, JoinSet},
};

#[derive(Clone)]
struct Management {
    phases: Arc<Mutex<BTreeMap<&'static str, &'static str>>>,
    probes: Arc<BTreeMap<&'static str, Vec<Probe>>>,
    client: reqwest::Client,
}

impl Management {
    async fn snapshot(&self) -> (bool, Value) {
        let phases = self.phases.lock().unwrap().clone();
        let mut products = BTreeMap::new();
        let mut all_ready = true;
        for (name, phase) in phases {
            let mut ready = phase == "running";
            if ready {
                for probe in &self.probes[name] {
                    ready &= self.probe(probe).await;
                }
            }
            all_ready &= ready;
            products.insert(name, json!({"phase": phase, "ready": ready}));
        }
        (all_ready, json!({"ready": all_ready, "products": products}))
    }

    async fn probe(&self, probe: &Probe) -> bool {
        let mut request = self
            .client
            .get(format!("http://{}{}", probe.address, probe.path));
        if let Ok(token) = std::env::var(enhance_token()) {
            if !token.is_empty() {
                request = request.bearer_auth(token);
            }
        }
        let Ok(response) = request.send().await else {
            return false;
        };
        response.status().is_success()
    }
}

fn enhance_token() -> &'static str {
    #[cfg(feature = "enhance")]
    {
        enhance_query_server::internal_auth::ENV
    }
    #[cfg(not(feature = "enhance"))]
    {
        "ENHANCE_INTERNAL_TOKEN"
    }
}

fn http(listeners: Arc<Mutex<BTreeMap<SocketAddr, TcpListener>>>, stop: Shutdown) -> Http<Router> {
    Http::new(move |addr, router| {
        let listener = listeners.lock().unwrap().remove(&addr);
        let stop = stop.clone();
        async move {
            let listener = listener.ok_or_else(|| {
                std::io::Error::other("listener was not reserved or is already in use")
            })?;
            axum::serve(listener, router)
                .with_graceful_shutdown(async move { stop.cancelled().await })
                .await
        }
    })
}

#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail"
))]
fn take(
    listeners: &Mutex<BTreeMap<SocketAddr, TcpListener>>,
    addr: SocketAddr,
) -> Result<TcpListener> {
    listeners
        .lock()
        .unwrap()
        .remove(&addr)
        .ok_or_else(|| "listener was not reserved".into())
}

#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail"
))]
fn spawn<F>(
    name: &'static str,
    future: F,
    stop: Shutdown,
    group: &mut JoinSet<(&'static str, Result<()>)>,
    owners: &mut HashMap<Id, (&'static str, Shutdown)>,
) where
    F: Future<Output = Result<()>> + Send + 'static,
{
    let id = group.spawn(async move { (name, future.await) }).id();
    owners.insert(id, (name, stop));
}

#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail"
))]
async fn stopped(local: Shutdown, process: Shutdown) {
    tokio::select! {
        _ = local.cancelled() => {}
        _ = process.cancelled() => {}
    }
}

/// Load and verify the one enabled Transparent section through its existing
/// one-shot path, print its report and return. No management or wallet
/// listener is bound.
pub async fn verify(config: Config) -> Result<()> {
    match config.verification()? {
        #[cfg(feature = "transparent-history")]
        crate::config::Verification::History(worker) => {
            let mut worker = worker.clone();
            worker.verify_only = true;
            transparent_query_server::history::process::run(worker).await
        }
        #[cfg(feature = "transparent-detail")]
        crate::config::Verification::Detail(worker) => {
            let mut worker = worker.clone();
            worker.verify_only = true;
            transparent_query_server::tx_detail::process::run(worker).await
        }
        crate::config::Verification::_Unused(_) => unreachable!(),
    }
}

/// Check publisher service discovery for every enabled HTTPS consumer. Runs
/// before any listener is bound, product state is opened or task is started;
/// any failure stops the whole query server. Legacy roles are not checked.
async fn discover(config: &Config) -> Result<()> {
    #[cfg(feature = "enhance")]
    if let Some(role) = active(&config.enhance)? {
        role.discover().await?;
    }
    #[cfg(feature = "transparent-history")]
    if let Some(worker) = active(&config.transparent_history)? {
        worker.discover().await?;
    }
    #[cfg(feature = "transparent-detail")]
    if let Some(worker) = active(&config.transaction_details)? {
        worker.discover().await?;
    }
    let _ = config;
    Ok(())
}

/// Prebind every listener, then run one role of each enabled product until shutdown.
/// A publisher contract version mismatch in any product, or a discovery failure
/// that ends a product's startup, stops every product.
pub async fn run(config: Config, shutdown: Shutdown) -> Result<()> {
    let layout = config.validate()?;
    discover(&config).await?;
    let mut listeners = BTreeMap::new();
    for addr in layout.listeners.keys() {
        listeners.insert(*addr, TcpListener::bind(addr).await?);
    }
    let listeners = Arc::new(Mutex::new(listeners));
    let state = Management {
        phases: Arc::default(),
        probes: Arc::new(layout.probes),
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
    };
    let routes = Router::new()
        .route(
            "/health",
            get(|State(s): State<Management>| async move { Json(s.snapshot().await.1) }),
        )
        .route(
            "/ready",
            get(|State(s): State<Management>| async move {
                let (ready, report) = s.snapshot().await;
                (
                    if ready {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    },
                    Json(report),
                )
            }),
        )
        .with_state(state.clone());
    let mut management = tokio::spawn(
        http(listeners.clone(), shutdown.clone()).serve(config.management_listen, routes),
    );
    let mut tasks = Tasks::default();
    tasks.own(&management);
    let mut group: JoinSet<(&'static str, Result<()>)> = JoinSet::new();
    let mut owners: HashMap<Id, (&'static str, Shutdown)> = HashMap::new();
    #[cfg(feature = "enhance")]
    if let Some(role) = active(&config.enhance)? {
        let role = role.clone();
        let bound = role
            .listens()
            .into_iter()
            .map(|addr| take(&listeners, addr))
            .collect::<Result<Vec<_>>>()?;
        let stop = Shutdown::default();
        state.phases.lock().unwrap().insert("enhance", "running");
        spawn(
            "enhance",
            enhance_query_server::service::run_serving(
                role,
                bound,
                stopped(stop.clone(), shutdown.clone()),
            ),
            stop,
            &mut group,
            &mut owners,
        );
    }
    #[cfg(feature = "status")]
    if let Some(role) = active(&config.status)? {
        let role = role.clone();
        let listen = take(&listeners, role.listen)?;
        let query = match role.query_listen {
            Some(addr) => Some(take(&listeners, addr)?),
            None => None,
        };
        let stop = Shutdown::default();
        let local = stop.clone();
        let process = shutdown.clone();
        state.phases.lock().unwrap().insert("status", "running");
        spawn(
            "status",
            async move {
                tokio::select! {
                    result = crate::status::serve_role_on(role, listen, query) => result,
                    _ = stopped(local, process) => Ok(()),
                }
            },
            stop,
            &mut group,
            &mut owners,
        );
    }
    #[cfg(feature = "transparent-history")]
    if let Some(worker) = active(&config.transparent_history)? {
        let worker = worker.clone();
        let listen = take(&listeners, worker.listen)?;
        let stop = Shutdown::default();
        let local = stop.clone();
        let process = shutdown.clone();
        state
            .phases
            .lock()
            .unwrap()
            .insert("transparent_history", "running");
        spawn(
            "transparent_history",
            async move {
                tokio::select! {
                    result = transparent_query_server::history::process::run_with(worker, Some(listen)) => result,
                    _ = stopped(local, process) => Ok(()),
                }
            },
            stop,
            &mut group,
            &mut owners,
        );
    }
    #[cfg(feature = "transparent-detail")]
    if let Some(worker) = active(&config.transaction_details)? {
        let worker = worker.clone();
        let listen = take(&listeners, worker.listen)?;
        let stop = Shutdown::default();
        let local = stop.clone();
        let process = shutdown.clone();
        state
            .phases
            .lock()
            .unwrap()
            .insert("transaction_details", "running");
        spawn(
            "transaction_details",
            async move {
                tokio::select! {
                    result = transparent_query_server::tx_detail::process::run_with(worker, Some(listen)) => result,
                    _ = stopped(local, process) => Ok(()),
                }
            },
            stop,
            &mut group,
            &mut owners,
        );
    }
    let mut fatal: Option<Box<dyn std::error::Error + Send + Sync>> = None;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = &mut management => { fatal = Some("management HTTP service stopped".into()); break; },
            result = group.join_next_with_id(), if !group.is_empty() => {
                match result.unwrap() {
                    Ok((id, (name, result))) => {
                        let (_, stop) = owners.remove(&id).unwrap();
                        stop.request();
                        state.phases.lock().unwrap().insert(name, "failed");
                        match result {
                            // Products retry ordinary discovery failures while
                            // running, so one that ends a product is either a
                            // version mismatch or a startup refusal after the
                            // preflight above; both stop the whole host.
                            Err(error) if error.downcast_ref::<pir_sync_manager::DiscoveryError>().is_some() => {
                                tracing::error!(product = name, %error, "publisher service discovery failed; stopping the query server");
                                fatal = Some(error);
                                break;
                            }
                            Err(error) => {
                                tracing::error!(product = name, %error, "query server stopped; other products continue");
                            }
                            Ok(()) => {}
                        }
                    }
                    Err(_) => {
                        fatal = Some("query server task panicked; stopping all owners safely".into());
                        break;
                    }
                }
            }
        }
    }
    for (_, stop) in owners.values() {
        stop.request();
    }
    while let Some(result) = group.join_next().await {
        if let Ok((name, _)) = result {
            state.phases.lock().unwrap().insert(name, "stopped");
        }
    }
    shutdown.request();
    // Cancellation alone can leave the management listener bound until the
    // task is polled again. Join it before reporting that the host stopped.
    if !management.is_finished() {
        management.abort();
        let _ = management.await;
    }
    if let Some(error) = fatal {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_releases_management_listener_before_returning() {
        let address = {
            let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            held.local_addr().unwrap()
        };
        let config: Config = serde_json::from_value(serde_json::json!({
            "management_listen": address,
        }))
        .unwrap();
        // Stop before the spawned management task gets its first poll. Its
        // reserved socket must still be released before the host returns.
        let shutdown = Shutdown::default();
        shutdown.request();
        run(config, shutdown).await.unwrap();
        assert!(
            std::net::TcpListener::bind(address).is_ok(),
            "management listener remained bound after the host returned"
        );
    }
}

#[cfg(all(test, feature = "transparent-history", feature = "transparent-detail"))]
mod tests {
    use super::*;

    /// Discovery runs before any listener, state directory or product task:
    /// every configured address is held here, so binding one would fail with
    /// "address in use" instead of the publisher's discovery error.
    #[tokio::test]
    async fn unreachable_publisher_stops_every_product_before_any_listener_or_state() {
        let root = tempfile::tempdir().unwrap();
        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(std::net::TcpListener::bind("127.0.0.1:0").unwrap());
        }
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let publisher = format!("https://localhost:{}", closed.local_addr().unwrap().port());
        drop(closed);
        let addr = |i: usize| held[i].local_addr().unwrap().to_string();
        let config: Config = serde_json::from_value(serde_json::json!({
            "management_listen": addr(0),
            "transparent_history": {"enabled": true, "settings": {
                "publisher": publisher,
                "publisher_network": "main",
                "publisher_genesis": "00".repeat(32),
                "listen": addr(1),
                "shard_dir": root.path().join("history"),
            }},
            "transaction_details": {"enabled": true, "settings": {
                "publisher": publisher,
                "publisher_network": "main",
                "publisher_genesis": "00".repeat(32),
                "state_dir": root.path().join("details"),
                "role": "combined",
                "listen": addr(2),
            }},
        }))
        .unwrap();
        config.validate().unwrap();
        let error = run(config, Shutdown::default()).await.unwrap_err();
        let text = error.to_string();
        assert!(text.contains("service discovery failed"), "{text}");
        assert!(text.contains(&publisher), "{text}");
        assert!(!pir_sync_manager::is_incompatible(error.as_ref()));
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    /// One-shot verification of either Transparent section checks discovery
    /// before it creates any consumer state.
    #[tokio::test]
    async fn verify_only_refuses_an_unreachable_publisher_before_any_state() {
        let root = tempfile::tempdir().unwrap();
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let publisher = format!("https://localhost:{}", closed.local_addr().unwrap().port());
        drop(closed);
        let common = serde_json::json!({
            "publisher": publisher,
            "publisher_network": "main",
            "publisher_genesis": "00".repeat(32),
            "listen": "127.0.0.1:1",
        });
        let mut history = common.clone();
        history["shard_dir"] = root.path().join("history").display().to_string().into();
        let mut details = common;
        details["state_dir"] = root.path().join("details").display().to_string().into();
        details["role"] = "combined".into();
        for (section, settings) in [
            ("transparent_history", history),
            ("transaction_details", details),
        ] {
            let config: Config = serde_json::from_value(serde_json::json!({
                "management_listen": "127.0.0.1:2",
                section: {"enabled": true, "settings": settings},
            }))
            .unwrap();
            let error = verify(config).await.unwrap_err();
            assert!(
                error
                    .downcast_ref::<pir_sync_manager::DiscoveryError>()
                    .is_some(),
                "{section}: {error}"
            );
            assert!(error.to_string().contains(&publisher), "{section}: {error}");
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
        }
    }

    /// A publisher that passes the host preflight and then refuses a product's
    /// own startup discovery stops the whole host with that original error:
    /// management and every product listener close.
    #[tokio::test(flavor = "multi_thread")]
    async fn discovery_refused_after_the_preflight_stops_the_whole_host() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let root = tempfile::tempdir().unwrap();
        let cert = root.path().join("ca.pem");
        let key = root.path().join("key.pem");
        assert!(std::process::Command::new("openssl")
            .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1"])
            .args([
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost"
            ])
            .args(["-addext", "basicConstraints=critical,CA:FALSE", "-keyout"])
            .arg(&key)
            .arg("-out")
            .arg(&cert)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success());
        // The two preflight requests succeed; every later one is refused.
        let served = Arc::new(AtomicUsize::new(0));
        let count = served.clone();
        let upstream = Router::new().route(
            pir_sync_manager::service_discovery::PATH,
            get(move || async move {
                if count.fetch_add(1, Ordering::SeqCst) < 2 {
                    let document = pir_sync_manager::service_discovery::Document::publisher();
                    (StatusCode::OK, document.to_json())
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, b"unavailable".to_vec())
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
        let control = root.path().join("fault");
        std::fs::write(&control, "").unwrap();
        let mut proxy = tokio::process::Command::new("python3")
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../pir-publisher/tests/support/distribution_tls.py"
            ))
            .arg(upstream_addr.to_string())
            .args([&cert, &key, &control])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut line = String::new();
        tokio::io::AsyncBufReadExt::read_line(
            &mut tokio::io::BufReader::new(proxy.stdout.take().unwrap()),
            &mut line,
        )
        .await
        .unwrap();
        let publisher = format!("https://localhost:{}", line.trim());
        let addrs: Vec<SocketAddr> = (0..3)
            .map(|_| {
                let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                held.local_addr().unwrap()
            })
            .collect();
        let config: Config = serde_json::from_value(serde_json::json!({
            "management_listen": addrs[0],
            "transparent_history": {"enabled": true, "settings": {
                "publisher": publisher,
                "publisher_ca": cert,
                "publisher_network": "main",
                "publisher_genesis": "00".repeat(32),
                "listen": addrs[1],
                "shard_dir": root.path().join("history"),
            }},
            "transaction_details": {"enabled": true, "settings": {
                "publisher": publisher,
                "publisher_ca": cert,
                "publisher_network": "main",
                "publisher_genesis": "00".repeat(32),
                "state_dir": root.path().join("details"),
                "role": "combined",
                "listen": addrs[2],
            }},
        }))
        .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(60), run(config, Shutdown::default()))
            .await
            .expect("the host kept running after a product's discovery was refused")
            .unwrap_err();
        assert!(served.load(Ordering::SeqCst) > 2);
        let discovery = error
            .downcast_ref::<pir_sync_manager::DiscoveryError>()
            .unwrap_or_else(|| panic!("discovery error lost its type: {error}"));
        assert_eq!(discovery.origin(), publisher);
        assert!(discovery.incompatible().is_none());
        assert!(error.to_string().contains("HTTP 503"), "{error}");
        for addr in addrs {
            assert!(
                tokio::net::TcpStream::connect(addr).await.is_err(),
                "{addr} outlived the host"
            );
        }
    }
}
