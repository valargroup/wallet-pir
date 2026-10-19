#[cfg(any(feature = "enhance", feature = "status", feature = "transparent"))]
use crate::config::active;
use crate::{
    config::{Config, Probe},
    Result,
};
use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde_json::{json, Value};
#[cfg(any(feature = "enhance", feature = "status", feature = "transparent"))]
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

#[cfg(any(feature = "enhance", feature = "status", feature = "transparent"))]
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

#[cfg(any(feature = "enhance", feature = "status", feature = "transparent"))]
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

#[cfg(any(feature = "enhance", feature = "status", feature = "transparent"))]
async fn stopped(local: Shutdown, process: Shutdown) {
    tokio::select! {
        _ = local.cancelled() => {}
        _ = process.cancelled() => {}
    }
}

/// Prebind every listener, then run one role of each enabled product until shutdown.
pub async fn run(config: Config, shutdown: Shutdown) -> Result<()> {
    let layout = config.validate()?;
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
                    result = status_query_server::distributed::serve_role_on(role, listen, query) => result,
                    _ = stopped(local, process) => Ok(()),
                }
            },
            stop,
            &mut group,
            &mut owners,
        );
    }
    #[cfg(feature = "transparent")]
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
    #[cfg(feature = "transparent")]
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
    let mut fatal = None;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = &mut management => { fatal = Some("management HTTP service stopped"); break; },
            result = group.join_next_with_id(), if !group.is_empty() => {
                match result.unwrap() {
                    Ok((id, (name, result))) => {
                        let (_, stop) = owners.remove(&id).unwrap();
                        stop.request();
                        state.phases.lock().unwrap().insert(name, "failed");
                        if let Err(error) = result {
                            tracing::error!(product = name, %error, "query server stopped; other products continue");
                        }
                    }
                    Err(_) => {
                        fatal = Some("query server task panicked; stopping all owners safely");
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
    if let Some(error) = fatal {
        return Err(error.into());
    }
    Ok(())
}
