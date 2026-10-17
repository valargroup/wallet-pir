//! Coordinator-owned preparation and acknowledgment of packing-router views.
use crate::packing_router::{self, Ack, Activation, RouterRegistration, ServingSnapshot, View};
use crate::worker::Revocation;
use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use enhance_pir::protocol::{canonical_hash, digest, RETAINED_GENERATIONS};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self},
    path::{Path as FsPath, PathBuf},
    sync::Mutex,
};
use tokio::io::AsyncReadExt;

#[derive(Clone, Serialize, Deserialize)]
struct Decision {
    generation: u64,
    acks: BTreeMap<String, Ack>,
}

pub(crate) struct ServingControl {
    root: PathBuf,
    pub routers: Vec<RouterRegistration>,
    ingresses: Vec<String>,
    active: Mutex<Option<Decision>>,
    acknowledged_fence: Mutex<Option<String>>,
    pub ready: std::sync::atomic::AtomicBool,
    http: reqwest::Client,
}
impl ServingControl {
    pub fn open(
        root: &FsPath,
        routers: Vec<RouterRegistration>,
        ingresses: Vec<String>,
    ) -> Result<Self, String> {
        let mut names = BTreeSet::new();
        let mut urls = BTreeSet::new();
        for router in &routers {
            packing_router::valid_origin(&router.url)?;
            packing_router::valid_origin(&router.query_url)?;
            if router.name.is_empty() || !names.insert(&router.name) || !urls.insert(&router.url) {
                return Err("duplicate packing router identity".into());
            }
        }
        for origin in &ingresses {
            packing_router::valid_origin(origin)?;
        }
        fs::create_dir_all(root.join("serving")).map_err(|e| e.to_string())?;
        let path = root.join("serving/routers.json");
        if path.exists() {
            let previous: Vec<RouterRegistration> =
                serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            for old in previous {
                if !routers
                    .iter()
                    .any(|r| r.name == old.name && r.url == old.url)
                {
                    return Err(
                        "registered routers cannot be removed or replaced without explicit fencing"
                            .into(),
                    );
                }
            }
        }
        crate::artifact::write_atomic(&root.join("serving"), "routers.json", |f| {
            serde_json::to_writer(f, &routers).map_err(std::io::Error::other)
        })
        .map_err(|e| e.to_string())?;
        Ok(Self {
            root: root.into(),
            routers,
            ingresses,
            active: Mutex::new(None),
            acknowledged_fence: Mutex::new(None),
            ready: std::sync::atomic::AtomicBool::new(false),
            http: crate::internal_auth::client_builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()
                .map_err(|e| e.to_string())?,
        })
    }
    pub async fn prepare(
        &self,
        epoch: u64,
        snapshots: Vec<ServingSnapshot>,
        fence: Revocation,
    ) -> Result<Vec<(RouterRegistration, Ack)>, String> {
        for snapshot in &snapshots {
            for id in snapshot.artifacts.keys() {
                if !self
                    .routers
                    .iter()
                    .any(|r| r.domains.is_empty() || r.domains.contains(id))
                {
                    return Err(format!("domain {id} lacks a packing-router assignment"));
                }
            }
        }
        let mut prepared = Vec::new();
        for router in &self.routers {
            let mut assigned = snapshots.clone();
            assigned.truncate(RETAINED_GENERATIONS);
            for snapshot in &mut assigned {
                snapshot
                    .artifacts
                    .retain(|id, _| router.domains.is_empty() || router.domains.contains(id));
                snapshot
                    .routes
                    .retain(|id, _| snapshot.artifacts.contains_key(id));
                snapshot
                    .preferred
                    .retain(|id, _| snapshot.artifacts.contains_key(id));
            }
            let view = View {
                version: packing_router::CONTROL_VERSION,
                controller_epoch: epoch,
                snapshots: assigned,
                revocation: fence.clone(),
            };
            let expected = digest(&view);
            let response = self
                .http
                .post(format!("{}/internal/prepare", router.url))
                .timeout(std::time::Duration::from_secs(600))
                .json(&view)
                .send()
                .await
                .map_err(|e| e.to_string())?
                .error_for_status()
                .map_err(|e| e.to_string())?;
            let ack: Ack = serde_json::from_slice(&packing_router::bounded(response, 65536).await?)
                .map_err(|e| e.to_string())?;
            if ack.digest != expected || ack.controller_epoch != epoch || ack.incarnation.is_empty()
            {
                return Err("packing-router readiness mismatch".into());
            }
            prepared.push((router.clone(), ack));
        }
        let mut routes: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for snapshot in &snapshots {
            for id in snapshot.artifacts.keys() {
                let session = hex::encode(snapshot.manifest.session_id(*id)?);
                let assigned = routes.entry(session).or_default();
                for router in &self.routers {
                    if (router.domains.is_empty() || router.domains.contains(id))
                        && !assigned.contains(&router.query_url)
                    {
                        assigned.push(router.query_url.clone());
                    }
                }
            }
        }
        let view = crate::query_ingress::IngressView {
            version: packing_router::CONTROL_VERSION,
            controller_epoch: epoch,
            generation: snapshots[0].manifest.generation,
            revocation: fence,
            routes,
        };
        let expected = digest(&view);
        for url in &self.ingresses {
            let response = self
                .http
                .post(format!("{url}/internal/prepare"))
                .json(&view)
                .send()
                .await
                .map_err(|e| e.to_string())?
                .error_for_status()
                .map_err(|e| e.to_string())?;
            let ack: Ack = serde_json::from_slice(&packing_router::bounded(response, 65536).await?)
                .map_err(|e| e.to_string())?;
            if ack.digest != expected || ack.controller_epoch != epoch || ack.incarnation.is_empty()
            {
                return Err("ingress readiness mismatch".into());
            }
            prepared.push((Self::ingress_registration(url), ack));
        }
        let decision = Decision {
            generation: snapshots[0].manifest.generation,
            acks: prepared
                .iter()
                .map(|(r, a)| (r.name.clone(), a.clone()))
                .collect(),
        };
        crate::artifact::write_atomic(&self.root.join("serving"), "prepared.json", |f| {
            serde_json::to_writer(f, &decision).map_err(std::io::Error::other)
        })
        .map_err(|e| e.to_string())?;
        Ok(prepared)
    }
    pub async fn activate(
        &self,
        generation: u64,
        prepared: Vec<(RouterRegistration, Ack)>,
    ) -> Result<(), String> {
        self.ready.store(false, std::sync::atomic::Ordering::SeqCst);
        for (router, ack) in &prepared {
            self.http
                .post(format!("{}/internal/activate", router.url))
                .json(&Activation {
                    incarnation: ack.incarnation.clone(),
                    digest: ack.digest.clone(),
                    controller_epoch: ack.controller_epoch,
                })
                .send()
                .await
                .map_err(|e| e.to_string())?
                .error_for_status()
                .map_err(|e| e.to_string())?;
        }
        let decision = Decision {
            generation,
            acks: prepared.into_iter().map(|(r, a)| (r.name, a)).collect(),
        };
        crate::artifact::write_atomic(&self.root.join("serving"), "active.json", |f| {
            serde_json::to_writer(f, &decision).map_err(std::io::Error::other)
        })
        .map_err(|e| e.to_string())?;
        *self.active.lock().unwrap() = Some(decision);
        self.ready.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn ingress_registration(url: &str) -> RouterRegistration {
        RouterRegistration {
            name: format!("ingress:{url}"),
            url: url.into(),
            query_url: url.into(),
            domains: BTreeSet::new(),
        }
    }
    fn participants(&self) -> Vec<RouterRegistration> {
        self.routers
            .iter()
            .cloned()
            .chain(
                self.ingresses
                    .iter()
                    .map(|url| Self::ingress_registration(url)),
            )
            .collect()
    }
    pub async fn refresh(&self) -> Result<(), String> {
        let decision = self
            .active
            .lock()
            .unwrap()
            .clone()
            .ok_or("no activated serving view")?;
        let mut tasks = tokio::task::JoinSet::new();
        for router in self.participants() {
            let ack = decision
                .acks
                .get(&router.name)
                .ok_or("missing serving acknowledgment")?
                .clone();
            let http = self.http.clone();
            tasks.spawn(async move {
                http.post(format!("{}/internal/refresh", router.url))
                    .json(&Activation {
                        incarnation: ack.incarnation,
                        digest: ack.digest,
                        controller_epoch: ack.controller_epoch,
                    })
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
            });
        }
        let mut healthy = true;
        while let Some(result) = tasks.join_next().await {
            healthy &= result.unwrap_or(false);
        }
        if !healthy {
            self.ready.store(false, std::sync::atomic::Ordering::SeqCst);
            return Err("router refresh failed; readiness requires reconciliation".into());
        }
        // Every participant acknowledged the activated view again, so a transient
        // refresh failure no longer leaves the public manifest unavailable.
        self.ready.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    pub async fn revoke(&self, fence: &Revocation) -> Result<(), String> {
        let hash = digest(fence);
        if self.acknowledged_fence.lock().unwrap().as_ref() == Some(&hash) {
            return Ok(());
        }
        self.ready.store(false, std::sync::atomic::Ordering::SeqCst);
        for router in self.participants() {
            self.http
                .post(format!("{}/internal/revoke", router.url))
                .json(fence)
                .send()
                .await
                .map_err(|e| e.to_string())?
                .error_for_status()
                .map_err(|e| e.to_string())?;
        }
        *self.acknowledged_fence.lock().unwrap() = Some(hash);
        Ok(())
    }
    pub fn public_ready(&self, generation: u64) -> bool {
        self.ready.load(std::sync::atomic::Ordering::SeqCst)
            && self
                .active
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|d| d.generation == generation)
    }
    pub fn artifact_router(root: PathBuf) -> Router {
        Router::new()
            .route("/internal/prepared-packing-artifact/:name", get(artifact))
            .route("/internal/packing-artifact/:name", get(legacy_artifact))
            .with_state(root)
    }
}
async fn artifact(
    State(root): State<PathBuf>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !name.strip_suffix(".bin").is_some_and(canonical_hash) {
        return Err((StatusCode::BAD_REQUEST, "invalid artifact".into()));
    }
    stream_artifact(root.join(crate::prepared_packing::DIRECTORY).join(name)).await
}
async fn legacy_artifact(
    State(root): State<PathBuf>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !name.strip_suffix(".bin").is_some_and(canonical_hash) {
        return Err((StatusCode::BAD_REQUEST, "invalid artifact".into()));
    }
    stream_artifact(root.join("hints").join(name)).await
}
async fn stream_artifact(path: PathBuf) -> Result<impl IntoResponse, (StatusCode, String)> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
    // Stream bounded chunks; large immutable hints never require another coordinator copy.
    let stream = tokio_stream::wrappers::ReceiverStream::new({
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let mut file = file;
            loop {
                let mut bytes = vec![0; 65536];
                match file.read(&mut bytes).await {
                    Ok(0) => break,
                    Ok(n) => {
                        bytes.truncate(n);
                        if sender.send(Ok::<_, std::io::Error>(bytes)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = sender.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
        receiver
    });
    Ok(Body::from_stream(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::post;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn successful_refresh_restores_readiness() {
        let accept = std::sync::Arc::new(AtomicBool::new(false));
        let gate = accept.clone();
        let app = Router::new().route(
            "/internal/refresh",
            post(move || {
                let gate = gate.clone();
                async move {
                    if gate.load(Ordering::SeqCst) {
                        StatusCode::NO_CONTENT
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let root = tempfile::tempdir().unwrap();
        let control = ServingControl::open(root.path(), Vec::new(), vec![url.clone()]).unwrap();
        let ack = Ack {
            incarnation: "i".into(),
            digest: "d".into(),
            controller_epoch: 1,
        };
        *control.active.lock().unwrap() = Some(Decision {
            generation: 7,
            acks: [(ServingControl::ingress_registration(&url).name, ack)].into(),
        });
        assert!(control.refresh().await.is_err());
        assert!(!control.public_ready(7));
        accept.store(true, Ordering::SeqCst);
        control.refresh().await.unwrap();
        assert!(control.public_ready(7));
        assert!(!control.public_ready(8));
        server.abort();
    }
}
