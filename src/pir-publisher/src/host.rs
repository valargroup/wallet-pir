#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent",
    feature = "receiver"
))]
use crate::config::active;
use crate::{
    config::{Config, Probe},
    Result,
};
use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
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
            products.insert(name, json!({"phase":phase,"ready":ready}));
        }
        (all_ready, json!({"ready":all_ready,"products":products}))
    }
    async fn probe(&self, probe: &Probe) -> bool {
        let response = self
            .client
            .get(format!("http://{}{}", probe.address, probe.path))
            .send()
            .await;
        let Ok(mut response) = response else {
            return false;
        };
        if !response.status().is_success() {
            return false;
        }
        if probe.kind == "status" {
            return true;
        }
        let mut bytes = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) if bytes.len() + chunk.len() <= 65536 => {
                    bytes.extend_from_slice(&chunk)
                }
                Ok(None) => break,
                _ => return false,
            }
        }
        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
            return false;
        };
        match probe.kind {
            "history" => value["phase"] == "serving",
            "display" => {
                value["phase"] == "running"
                    && value["halted"].is_null()
                    && value["stalled"] == false
                    && !value["published"].is_null()
            }
            _ => false,
        }
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
#[cfg_attr(
    not(any(
        feature = "enhance",
        feature = "status",
        feature = "transparent",
        feature = "receiver"
    )),
    allow(dead_code)
)]
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
/// Prebind every listener, then run independent product owners until operator shutdown.
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
    #[cfg(any(
        feature = "enhance",
        feature = "status",
        feature = "transparent",
        feature = "receiver"
    ))]
    macro_rules! product {
        ($field:ident, $run:path) => {
            if let Some(settings) = active(&config.$field)? {
                let name = stringify!($field);
                let stop = Shutdown::default();
                state.phases.lock().unwrap().insert(name, "running");
                spawn(
                    name,
                    $run(
                        settings.clone(),
                        http(listeners.clone(), stop.clone()),
                        stop.clone(),
                    ),
                    stop,
                    &mut group,
                    &mut owners,
                );
            }
        };
    }
    #[cfg(feature = "enhance")]
    product!(enhance, enhance_publisher::service::run);
    #[cfg(feature = "status")]
    product!(status, status_publisher::service::run);
    #[cfg(feature = "transparent")]
    product!(
        transparent_history,
        transparent_publisher::history::controller::run_managed
    );
    #[cfg(feature = "transparent")]
    product!(
        transaction_details,
        transparent_publisher::tx_detail::service::run
    );
    #[cfg(feature = "receiver")]
    product!(receiver, receiver_indexer::service::run_isolated);
    let mut fatal = None;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = &mut management => { fatal = Some("management HTTP service stopped"); break; },
            result = group.join_next_with_id(), if !group.is_empty() => {
                match result.unwrap() {
                    Ok((id, (name, result))) => {
                        let (_, stop) = owners.remove(&id).unwrap(); stop.request();
                        state.phases.lock().unwrap().insert(name, "failed");
                        if let Err(error) = result { tracing::error!(product=name, %error, "publisher stopped; other products continue"); }
                    }
                    Err(_) => {
                        // A panic may leave an unobserved blocking operation. Do not reopen its state.
                        fatal = Some("publisher task panicked; stopping all owners safely"); break;
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

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn failure_does_not_stop_another_owner() {
        let mut group = JoinSet::new();
        let mut owners = HashMap::new();
        let first = Shutdown::default();
        let second = Shutdown::default();
        let held = second.clone();
        spawn(
            "one",
            async { Err("expected failure".into()) },
            first,
            &mut group,
            &mut owners,
        );
        spawn(
            "two",
            async move {
                held.cancelled().await;
                Ok(())
            },
            second.clone(),
            &mut group,
            &mut owners,
        );
        let (id, (name, result)) = group.join_next_with_id().await.unwrap().unwrap();
        assert_eq!(name, "one");
        assert!(result.is_err());
        owners.remove(&id).unwrap().1.request();
        assert!(!second.requested());
        second.request();
        assert!(group.join_next().await.unwrap().unwrap().1.is_ok());
    }
}
