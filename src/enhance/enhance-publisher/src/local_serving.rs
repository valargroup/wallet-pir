//! In-process Enhance serving roles for tools and tests: one packing router and
//! one query ingress beside a coordinator. Wallets use the ingress origin; the
//! coordinator answers only its operator routes. Production runs each role as
//! its own `pir-query-server` process. The roles outlive coordinator restarts,
//! as they do in production.
use crate::{control::Group, coordinator::Coordinator};
use enhance_query_server::{
    packing_router::{PackingRouter, RouterRegistration},
    query_ingress::QueryIngress,
};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::{net::TcpListener, task::JoinHandle};

type Result<T> = std::result::Result<T, String>;

const EPHEMERAL: &str = "127.0.0.1:0";

async fn serve(router: axum::Router) -> Result<(String, JoinHandle<()>)> {
    serve_on(EPHEMERAL.parse().unwrap(), router).await
}

async fn serve_on(listen: SocketAddr, router: axum::Router) -> Result<(String, JoinHandle<()>)> {
    let listener = TcpListener::bind(listen).await.map_err(|e| e.to_string())?;
    let mut address = listener.local_addr().map_err(|e| e.to_string())?;
    if address.ip().is_unspecified() {
        address.set_ip(match address {
            SocketAddr::V4(_) => std::net::Ipv4Addr::LOCALHOST.into(),
            SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
        });
    }
    let origin = format!("http://{address}");
    Ok((
        origin,
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        }),
    ))
}

pub struct Roles {
    /// Wallet origin: the query ingress public routes.
    pub origin: String,
    /// Address the coordinator serves its private artifacts on.
    pub artifact_listen: String,
    pub registrations: Vec<RouterRegistration>,
    pub ingress_control: String,
    tasks: Vec<JoinHandle<()>>,
}

impl Roles {
    /// Starts the roles under `root`, pinning the packing router's artifact
    /// origin to `artifact`, which the coordinator serves. Wallets reach the
    /// ingress on `wallet_listen`.
    async fn open(root: &Path, artifact: SocketAddr, wallet_listen: SocketAddr) -> Result<Self> {
        let packing =
            PackingRouter::open(&root.join("packing"), &format!("http://{artifact}"), 6, 4)?;
        packing.start_worker_health_monitor();
        let ingress = QueryIngress::open(&root.join("ingress"), 16)?;
        let mut tasks = Vec::new();
        let (control, task) = serve(packing.control_router()).await?;
        tasks.push(task);
        let (query, task) = serve(packing.public_router()).await?;
        tasks.push(task);
        let (ingress_control, task) = serve(ingress.control_router()).await?;
        tasks.push(task);
        let (origin, task) = serve_on(wallet_listen, ingress.public_router()).await?;
        tasks.push(task);
        Ok(Self {
            origin,
            artifact_listen: artifact.to_string(),
            registrations: vec![RouterRegistration {
                name: "packing".into(),
                url: control,
                query_url: query,
                domains: Default::default(),
            }],
            ingress_control,
            tasks,
        })
    }

    /// Roles for a coordinator process, which binds the artifact listener itself.
    pub async fn for_process(root: &Path) -> Result<Self> {
        let reserved = std::net::TcpListener::bind(EPHEMERAL).map_err(|e| e.to_string())?;
        let artifact = reserved.local_addr().map_err(|e| e.to_string())?;
        drop(reserved);
        Self::open(root, artifact, EPHEMERAL.parse().unwrap()).await
    }

    /// Writes the packing inventory and returns the root publisher configuration
    /// used by the real-process tests.
    pub fn publisher_config(
        &self,
        root: &Path,
        listen: &str,
        rpc: &str,
    ) -> Result<serde_json::Value> {
        self.coordinator_args(root)?;
        let example: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tools/deploy/publisher/publisher.example.json"
        ))
        .map_err(|e| e.to_string())?;
        let mut settings = example["enhance"]["settings"].clone();
        settings["listen"] = serde_json::json!(listen);
        settings["poll_seconds"] = serde_json::json!(1);
        settings["zakura_rpc_url"] = serde_json::json!(rpc);
        settings["data_dir"] = serde_json::json!(root.join("data"));
        settings["worker_config"] = serde_json::json!(root.join("workers.json"));
        settings["zakura_cookie"] = serde_json::json!(root.join("cookie"));
        settings["packing_router_config"] = serde_json::json!(root.join("packing-routers.json"));
        settings["query_ingress"] = serde_json::json!([self.ingress_control.clone()]);
        settings["artifact_listen"] = serde_json::json!(self.artifact_listen);
        let management = std::net::TcpListener::bind(EPHEMERAL)
            .map_err(|e| e.to_string())?
            .local_addr()
            .map_err(|e| e.to_string())?;
        Ok(
            serde_json::json!({"management_listen":management.to_string(),"enhance":{"enabled":true,"settings":settings}}),
        )
    }

    /// Writes the packing-router inventory under `root` and returns the
    /// coordinator flags that connect a coordinator process to these roles.
    pub fn coordinator_args(&self, root: &Path) -> Result<Vec<std::ffi::OsString>> {
        let config = root.join("packing-routers.json");
        let inventory = serde_json::to_vec(&self.registrations).map_err(|e| e.to_string())?;
        std::fs::write(&config, inventory).map_err(|e| e.to_string())?;
        Ok(vec![
            "--packing-router-config".into(),
            config.into(),
            "--query-ingress".into(),
            self.ingress_control.clone().into(),
            "--artifact-listen".into(),
            self.artifact_listen.clone().into(),
        ])
    }
}

impl Drop for Roles {
    fn drop(&mut self) {
        self.tasks.iter().for_each(JoinHandle::abort);
    }
}

/// An in-process coordinator with its serving roles.
pub struct Serving {
    coordinator: Option<Coordinator>,
    operator_listen: SocketAddr,
    /// Wallet origin: the query ingress.
    pub origin: String,
    /// The coordinator's private health, readiness and metrics routes.
    pub operator: String,
    pub roles: Roles,
    root: PathBuf,
    artifacts: JoinHandle<()>,
    coordinator_tasks: Vec<JoinHandle<()>>,
}

impl Serving {
    /// Opens a coordinator at `coordinator_root` with roles under `root`, all
    /// on ephemeral loopback ports.
    pub async fn start(root: &Path, coordinator_root: &Path, groups: Vec<Group>) -> Result<Self> {
        let ephemeral = EPHEMERAL.parse().unwrap();
        Self::start_on(root, coordinator_root, groups, ephemeral, ephemeral).await
    }

    /// As [`Serving::start`], with the coordinator's operator routes on
    /// `operator_listen` and wallet routes on `wallet_listen`.
    pub async fn start_on(
        root: &Path,
        coordinator_root: &Path,
        groups: Vec<Group>,
        operator_listen: SocketAddr,
        wallet_listen: SocketAddr,
    ) -> Result<Self> {
        let listener = TcpListener::bind(EPHEMERAL)
            .await
            .map_err(|e| e.to_string())?;
        let artifact = listener.local_addr().map_err(|e| e.to_string())?;
        let roles = Roles::open(root, artifact, wallet_listen).await?;
        let coordinator = Coordinator::open_with_serving(
            coordinator_root,
            groups,
            roles.registrations.clone(),
            vec![roles.ingress_control.clone()],
        )?;
        // The artifact routes read only the coordinator's directory, so one
        // listener serves every coordinator reopened there.
        let artifacts = coordinator.artifact_router();
        let artifacts = tokio::spawn(async move {
            let _ = axum::serve(listener, artifacts).await;
        });
        let mut serving = Self {
            operator_listen,
            origin: roles.origin.clone(),
            operator: String::new(),
            coordinator: None,
            roles,
            root: coordinator_root.into(),
            artifacts,
            coordinator_tasks: Vec::new(),
        };
        serving.serve(coordinator).await?;
        Ok(serving)
    }

    pub fn coordinator(&self) -> Coordinator {
        self.coordinator.clone().expect("a served coordinator")
    }

    /// Opens another coordinator over the same directory and roles.
    fn reopen(&self, groups: Vec<Group>) -> Result<Coordinator> {
        Coordinator::open_with_serving(
            &self.root,
            groups,
            self.roles.registrations.clone(),
            vec![self.roles.ingress_control.clone()],
        )
    }

    /// Stops the current coordinator's routes and serves a reopened one,
    /// keeping the roles running. Callers must drop their clones first.
    pub async fn restart(&mut self, groups: Vec<Group>) -> Result<Coordinator> {
        for task in self.coordinator_tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
        self.coordinator = None;
        // Finished connection tasks may still hold the old controller lock
        // briefly; a process restart waits for them the same way.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let coordinator = loop {
            match self.reopen(groups.clone()) {
                Ok(coordinator) => break coordinator,
                Err(error)
                    if error.starts_with("controller already running:")
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await
                }
                Err(error) => return Err(error),
            }
        };
        self.serve(coordinator.clone()).await?;
        Ok(coordinator)
    }

    async fn serve(&mut self, coordinator: Coordinator) -> Result<()> {
        // Startup reconciliation activates restored publications on the roles.
        if coordinator.manifest().await.is_some() {
            coordinator.reconcile().await?;
        }
        let (operator, task) = serve_on(self.operator_listen, coordinator.clone().router()).await?;
        self.operator = operator;
        self.coordinator = Some(coordinator.clone());
        let heartbeat = tokio::spawn(async move {
            loop {
                coordinator.refresh_routers().await;
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        });
        self.coordinator_tasks = vec![task, heartbeat];
        Ok(())
    }
}

impl Drop for Serving {
    fn drop(&mut self) {
        self.artifacts.abort();
        self.coordinator_tasks.iter().for_each(JoinHandle::abort);
    }
}
