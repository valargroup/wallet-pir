#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
use crate::config::active;
use crate::{config::Config, readiness::Readiness, Result};
use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde_json::{json, Value};
#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
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
    readiness: Arc<BTreeMap<&'static str, Readiness>>,
}

impl Management {
    /// A product is ready only while it runs and its own typed check reports
    /// an authorized, usable serving view; the host is ready when all are.
    async fn snapshot(&self) -> (bool, Value) {
        let phases = self.phases.lock().unwrap().clone();
        let mut products = BTreeMap::new();
        let mut all_ready = true;
        for (name, phase) in phases {
            let ready = phase == "running" && self.readiness[name].ready().await;
            all_ready &= ready;
            products.insert(name, json!({"phase": phase, "ready": ready}));
        }
        (all_ready, json!({"ready": all_ready, "products": products}))
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
    feature = "transparent-detail",
    feature = "receiver"
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

#[cfg(feature = "enhance")]
async fn stopped(local: Shutdown) {
    local.cancelled().await
}

/// Upper bound on draining wallet requests the public listener already accepted.
const PUBLIC_DRAIN: Duration = Duration::from_secs(60);

/// Stop the host in admission order. Readiness first reports every running
/// product as stopping and its wallet namespaces refuse new requests; the
/// public listener then drains requests it already accepted while every
/// product still owns its state, permits and durable transactions; only then
/// does each product stop and release its listeners, locks and tasks.
async fn drain(
    phases: &Mutex<BTreeMap<&'static str, &'static str>>,
    slots: &BTreeMap<&'static str, crate::public::Slot>,
    public: Option<(Shutdown, tokio::task::JoinHandle<std::io::Result<()>>)>,
    owners: &HashMap<Id, (&'static str, Shutdown)>,
    group: &mut JoinSet<(&'static str, Result<()>)>,
) {
    for phase in phases.lock().unwrap().values_mut() {
        if *phase == "running" {
            *phase = "stopping";
        }
    }
    for slot in slots.values() {
        slot.withdraw();
    }
    if let Some((stop, mut task)) = public {
        stop.request();
        if tokio::time::timeout(PUBLIC_DRAIN, &mut task).await.is_err() {
            tracing::warn!("wallet requests outlasted the shutdown drain; closing the listener");
            task.abort();
            let _ = task.await;
        }
    }
    for (_, stop) in owners.values() {
        stop.request();
    }
    while let Some(result) = group.join_next().await {
        if let Ok((name, _)) = result {
            phases.lock().unwrap().insert(name, "stopped");
        }
    }
}

/// The wallet namespaces of every product validated onto `public_listen`.
fn public_mounts(products: &[&'static str]) -> Vec<crate::public::Mount> {
    products
        .iter()
        .filter_map(|product| match *product {
            #[cfg(feature = "receiver")]
            "receiver" => Some(crate::public::receiver()),
            #[cfg(feature = "enhance")]
            "enhance" => Some(crate::public::enhance()),
            #[cfg(feature = "status")]
            "status" => Some(crate::public::status()),
            #[cfg(feature = "transparent-history")]
            "transparent_history" => Some(crate::public::transparent_history()),
            #[cfg(feature = "transparent-detail")]
            "transaction_details" => Some(crate::public::transaction_details()),
            _ => None,
        })
        .collect()
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
            crate::transparent::history::process::run(worker).await
        }
        #[cfg(feature = "transparent-detail")]
        crate::config::Verification::Detail(worker) => {
            let mut worker = worker.clone();
            worker.verify_only = true;
            crate::transparent::tx_detail::process::run(worker).await
        }
        crate::config::Verification::_Unused(_) => unreachable!(),
    }
}

/// Check publisher service discovery for every enabled HTTPS consumer. Runs
/// before any listener is bound, product state is opened or task is started;
/// any failure stops the whole query server. Legacy roles are not checked.
async fn discover(config: &Config) -> Result<()> {
    #[cfg(feature = "receiver")]
    if let Some(role) = active(&config.receiver)? {
        role.source(true)?.discover().await?;
    }
    #[cfg(feature = "status")]
    if let Some(role) = active(&config.status)? {
        crate::status::discover(role).await?;
    }
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
    let readiness: BTreeMap<&'static str, Readiness> = layout
        .products
        .iter()
        .map(|product| (*product, Readiness::default()))
        .collect();
    let state = Management {
        phases: Arc::default(),
        readiness: Arc::new(readiness),
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
    // The host stops each listener and product itself, in drain order, once
    // `shutdown` is requested or a fatal failure ends the loop below.
    let management_stop = Shutdown::default();
    let public_stop = Shutdown::default();
    let mut management = tokio::spawn(
        http(listeners.clone(), management_stop.clone()).serve(config.management_listen, routes),
    );
    let mut tasks = Tasks::default();
    tasks.own(&management);
    // The one wallet-facing listener. Each product's namespaces answer 503
    // until the product installs its wallet routes.
    let mut slots: BTreeMap<&'static str, crate::public::Slot> = BTreeMap::new();
    let mut public = match config.public_listen {
        Some(addr) => {
            let mounts = public_mounts(&layout.public);
            for mount in &mounts {
                slots.insert(mount.product, mount.slot.clone());
            }
            let app =
                crate::public::router(&mounts)?.into_make_service_with_connect_info::<SocketAddr>();
            let listener = listeners
                .lock()
                .unwrap()
                .remove(&addr)
                .ok_or("public listener was not reserved")?;
            let stop = public_stop.clone();
            let task = tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move { stop.cancelled().await })
                    .await
            });
            tasks.own(&task);
            Some(task)
        }
        None => None,
    };
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
            crate::enhance::service::run_serving(
                role,
                bound,
                slots.get("enhance").cloned(),
                state.readiness["enhance"].clone(),
                stopped(stop.clone()),
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
        let query = slots.get("status").cloned();
        let readiness = state.readiness["status"].clone();
        let stop = Shutdown::default();
        let local = stop.clone();
        state.phases.lock().unwrap().insert("status", "running");
        spawn(
            "status",
            crate::status::serve_role_on(role, listen, query, readiness, local),
            stop,
            &mut group,
            &mut owners,
        );
    }
    #[cfg(feature = "transparent-history")]
    if let Some(worker) = active(&config.transparent_history)? {
        let worker = worker.clone();
        let listen = take(&listeners, worker.listen)?;
        let wallet = slots.get("transparent_history").cloned();
        let readiness = state.readiness["transparent_history"].clone();
        let stop = Shutdown::default();
        let local = stop.clone();
        state
            .phases
            .lock()
            .unwrap()
            .insert("transparent_history", "running");
        spawn(
            "transparent_history",
            crate::transparent::history::process::run_with(
                worker,
                Some(listen),
                wallet,
                readiness,
                local,
            ),
            stop,
            &mut group,
            &mut owners,
        );
    }
    #[cfg(feature = "transparent-detail")]
    if let Some(worker) = active(&config.transaction_details)? {
        let worker = worker.clone();
        let listen = take(&listeners, worker.listen)?;
        let wallet = slots.get("transaction_details").cloned();
        let readiness = state.readiness["transaction_details"].clone();
        let stop = Shutdown::default();
        let local = stop.clone();
        state
            .phases
            .lock()
            .unwrap()
            .insert("transaction_details", "running");
        spawn(
            "transaction_details",
            crate::transparent::tx_detail::process::run_with(
                worker,
                Some(listen),
                wallet,
                readiness,
                local,
            ),
            stop,
            &mut group,
            &mut owners,
        );
    }
    #[cfg(feature = "receiver")]
    if let Some(role) = active(&config.receiver)? {
        let stop = Shutdown::default();
        state.phases.lock().unwrap().insert("receiver", "running");
        spawn(
            "receiver",
            crate::receiver::run(
                role.clone(),
                slots["receiver"].clone(),
                state.readiness["receiver"].clone(),
                stop.clone(),
            ),
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
            _ = async { public.as_mut().unwrap().await }, if public.is_some() => {
                public = None;
                fatal = Some("public wallet HTTP service stopped".into());
                break;
            },
            result = group.join_next_with_id(), if !group.is_empty() => {
                match result.unwrap() {
                    Ok((id, (name, result))) => {
                        let (_, stop) = owners.remove(&id).unwrap();
                        stop.request();
                        // A stopped product no longer answers wallets.
                        if let Some(slot) = slots.get(name) {
                            slot.withdraw();
                        }
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
    drain(
        &state.phases,
        &slots,
        public.map(|task| (public_stop, task)),
        &owners,
        &mut group,
    )
    .await;
    shutdown.request();
    management_stop.request();
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

    /// Aggregate readiness asks each product's typed check, and a product
    /// that is not running is never ready.
    #[tokio::test]
    async fn aggregate_readiness_requires_product_eligibility() {
        let ready = Arc::new(Mutex::new(false));
        let flag = ready.clone();
        let enhance = Readiness::default();
        enhance.register(move || std::future::ready(*flag.lock().unwrap()));
        let management = Management {
            phases: Arc::new(Mutex::new(BTreeMap::from([
                ("enhance", "running"),
                ("status", "running"),
            ]))),
            readiness: Arc::new(BTreeMap::from([
                ("enhance", enhance),
                ("status", Readiness::default()),
            ])),
        };
        let (aggregate, report) = management.snapshot().await;
        assert!(!aggregate, "{report}");
        assert_eq!(report["products"]["enhance"]["ready"], json!(false));
        *ready.lock().unwrap() = true;
        let (aggregate, report) = management.snapshot().await;
        assert!(!aggregate, "an unregistered product is not ready");
        assert_eq!(report["products"]["enhance"]["ready"], json!(true));
        management.readiness["status"].register(|| std::future::ready(true));
        assert!(management.snapshot().await.0);
        management
            .phases
            .lock()
            .unwrap()
            .insert("status", "stopping");
        let (aggregate, report) = management.snapshot().await;
        assert!(!aggregate);
        assert_eq!(report["products"]["status"]["ready"], json!(false));
    }

    /// Shutdown stops admission, then drains an accepted wallet request while
    /// its product still runs, and only then stops the product.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_accepted_wallet_requests_before_stopping_products() {
        use crate::public::{Mount, Slot};
        use tokio::sync::oneshot;
        use tower::ServiceExt;
        let events = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let (entered, accepted) = oneshot::channel::<()>();
        let (release, released) = oneshot::channel::<()>();
        let gates = Arc::new(Mutex::new(Some((entered, released))));
        let log = events.clone();
        let slot = Slot::default();
        slot.install(Router::new().route(
            "/status/v1/query",
            get(move || {
                let (entered, released) = gates.lock().unwrap().take().unwrap();
                let log = log.clone();
                async move {
                    entered.send(()).unwrap();
                    released.await.unwrap();
                    log.lock().unwrap().push("request answered");
                    "answered"
                }
            }),
        ));
        let mount = || Mount {
            product: "status",
            paths: vec!["/status/v1/*operation".into()],
            slot: slot.clone(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let public_stop = Shutdown::default();
        let stop = public_stop.clone();
        let app = crate::public::router(&[mount()]).unwrap();
        let public = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { stop.cancelled().await })
                .await
        });
        let mut group = JoinSet::new();
        let mut owners = HashMap::new();
        let product = Shutdown::default();
        let (local, log) = (product.clone(), events.clone());
        let id = group
            .spawn(async move {
                local.cancelled().await;
                log.lock().unwrap().push("product stopped");
                ("status", Ok(()))
            })
            .id();
        owners.insert(id, ("status", product));
        let phases = Mutex::new(BTreeMap::from([("status", "running")]));
        let slots = BTreeMap::from([("status", slot.clone())]);
        let request = tokio::spawn(reqwest::get(format!("http://{address}/status/v1/query")));
        accepted.await.unwrap();

        let drained = drain(
            &phases,
            &slots,
            Some((public_stop, public)),
            &owners,
            &mut group,
        );
        tokio::pin!(drained);
        tokio::select! {
            _ = &mut drained => panic!("shutdown finished before the accepted request"),
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        assert_eq!(phases.lock().unwrap()["status"], "stopping");
        let refused = crate::public::router(&[mount()])
            .unwrap()
            .oneshot(
                axum::extract::Request::get("/status/v1/query")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            events.lock().unwrap().is_empty(),
            "product stopped while a wallet request ran"
        );
        release.send(()).unwrap();
        drained.await;
        let response = request.await.unwrap().unwrap();
        assert_eq!(response.text().await.unwrap(), "answered");
        assert_eq!(
            *events.lock().unwrap(),
            ["request answered", "product stopped"]
        );
        assert_eq!(phases.lock().unwrap()["status"], "stopped");
    }

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

#[cfg(all(test, feature = "status"))]
mod status_discovery_tests {
    use super::*;

    #[tokio::test]
    async fn status_discovery_runs_before_listeners_and_state_are_opened() {
        let root = tempfile::tempdir().unwrap();
        let management = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let status = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let publisher = format!("https://localhost:{}", closed.local_addr().unwrap().port());
        drop(closed);
        let config: Config = serde_json::from_value(serde_json::json!({
            "management_listen": management.local_addr().unwrap(),
            "status": {"enabled": true, "settings": {
                "role": "worker", "network_hex": "01".repeat(32),
                "state_dir": root.path().join("worker"),
                "listen": status.local_addr().unwrap(), "publisher": publisher,
                "worker_origin": "",
            }},
        }))
        .unwrap();
        config.validate().unwrap();
        let error = run(config, Shutdown::default()).await.unwrap_err();
        assert!(
            error
                .downcast_ref::<pir_sync_manager::DiscoveryError>()
                .is_some(),
            "{error}"
        );
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
