//! `validate-distributed`: encrypted oracle validation of the synthetic fixture
//! across real `pir-query-server` worker and router processes. It never serves
//! wallets itself.
use super::{require, AnyError};
use crate::status::{self, fixture};
use serde_json::json;
use status_query_server::protocol::*;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::net::TcpListener;

/// Distinct free loopback ports for child listeners, which need fixed ports.
async fn free_loopback_ports<const N: usize>() -> Result<[SocketAddr; N], AnyError> {
    let mut held = Vec::new();
    for _ in 0..N {
        held.push(TcpListener::bind("127.0.0.1:0").await?);
    }
    let mut ports = [SocketAddr::from(([127, 0, 0, 1], 0)); N];
    for (port, listener) in ports.iter_mut().zip(&held) {
        *port = listener.local_addr()?;
    }
    Ok(ports)
}

async fn lookup(
    http: &reqwest::Client,
    origin: &str,
    client: &Client,
    txid: Hash,
    earliest: Option<u32>,
) -> Result<Observation, AnyError> {
    let q = client.prepare(&txid, earliest, status::now_ms())?;
    let response = http
        .post(format!("{origin}/v1/status/query"))
        .body(q.body.clone())
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    Ok(client.decode(q, &response, status::now_ms())?)
}

/// Run the worker and router as `pir-query-server` processes, drive them as
/// the publisher does, and check every answer against the fixture oracle.
pub async fn validate_distributed(
    entries: usize,
    cuda: bool,
    state_dir: PathBuf,
    query_server: Option<PathBuf>,
    existing_worker: Option<String>,
    artifact_listen: SocketAddr,
    request_timeout_seconds: u64,
) -> Result<(), AnyError> {
    use status::distributed::{Activate, Binding, Health, Prepare, Ready};
    struct Children(Vec<std::process::Child>);
    impl Drop for Children {
        fn drop(&mut self) {
            for child in &mut self.0 {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
    let snapshot = fixture::snapshot(entries, false)?;
    let rows = Arc::new(snapshot.rows.clone());
    let digest = snapshot.digest;
    require(
        artifact_listen.ip().is_loopback(),
        "fixture artifacts require loopback",
    )?;
    if let Some(origin) = &existing_worker {
        let url = reqwest::Url::parse(origin)?;
        require(
            url.scheme() == "http"
                && url
                    .host_str()
                    .and_then(|h| h.parse::<std::net::IpAddr>().ok())
                    .is_some_and(|ip| ip.is_loopback())
                && url.path() == "/"
                && url.query().is_none()
                && url.fragment().is_none()
                && url.username().is_empty()
                && url.password().is_none(),
            "an existing fixture worker must be an HTTP loopback origin",
        )?;
    }
    require(
        (1..=900).contains(&request_timeout_seconds),
        "invalid diagnostic request timeout",
    )?;
    let artifact_listener = TcpListener::bind(artifact_listen).await?;
    let artifact_origin = format!("http://{}", artifact_listener.local_addr()?);
    let artifacts = axum::Router::new().route(
        "/artifact/:id",
        axum::routing::get(
            move |axum::extract::Path(id): axum::extract::Path<String>| {
                let rows = rows.clone();
                async move {
                    if id == hex::encode(digest) {
                        Ok(rows.as_ref().clone())
                    } else {
                        Err(axum::http::StatusCode::NOT_FOUND)
                    }
                }
            },
        ),
    );
    let artifact_job = tokio::spawn(async move { axum::serve(artifact_listener, artifacts).await });
    let query_server = match query_server {
        Some(path) => path,
        None => std::env::current_exe()?
            .parent()
            .ok_or("status-pir has no parent directory")?
            .join("pir-query-server"),
    };
    require(
        query_server.is_file(),
        "pir-query-server (built with the status feature) not found; pass --query-server",
    )?;
    std::fs::create_dir_all(&state_dir)?;
    let state_dir = state_dir.canonicalize()?;
    let [wa, ra, worker_management, router_management] = free_loopback_ports().await?;
    let external_worker = existing_worker.is_some();
    let worker = existing_worker.unwrap_or_else(|| format!("http://{wa}"));
    let router = format!("http://{ra}");
    let mut children = Children(Vec::new());
    for (role, addr, management, origin) in [
        ("worker", wa, worker_management, artifact_origin),
        ("router", ra, router_management, worker.clone()),
    ] {
        if role == "worker" && external_worker {
            continue;
        }
        let config = state_dir.join(format!("{role}.json"));
        std::fs::write(
            &config,
            serde_json::to_vec(&json!({
                "management_listen": management.to_string(),
                "status": {"enabled": true, "settings": {
                    "role": role,
                    "network_hex": hex::encode(snapshot.network),
                    "state_dir": state_dir.join(role),
                    "listen": addr.to_string(),
                    "artifact_origin": origin,
                    "worker_origin": worker,
                    "cuda": cuda && role == "worker",
                }},
            }))?,
        )?;
        children.0.push(
            std::process::Command::new(&query_server)
                .arg("--config")
                .arg(config)
                .spawn()?,
        );
    }
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(request_timeout_seconds))
        .build()?;
    let mut bindings = Vec::new();
    let mut healths = Vec::new();
    for origin in [&worker, &router] {
        let mut health = None;
        for _ in 0..100 {
            if let Ok(response) = http.get(format!("{origin}/control/health")).send().await {
                health = Some(response.error_for_status()?.json::<Health>().await?);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let health = health.ok_or("role failed to start")?;
        healths.push(health);
    }
    let epoch = healths.iter().map(|h| h.binding.epoch).max().unwrap() + 1;
    for (origin, health) in [&worker, &router].into_iter().zip(healths) {
        let b = Binding {
            epoch,
            incarnation: health.binding.incarnation,
        };
        let ack: Binding = http
            .post(format!("{origin}/control/fence"))
            .json(&b)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        require(ack == b, "fence mismatch")?;
        bindings.push(b);
    }
    require(
        bindings[0].epoch == bindings[1].epoch,
        "fixture role epochs disagree",
    )?;
    let m = Manifest {
        protocol: PROTOCOL.into(),
        network: snapshot.network,
        salt: snapshot.salt,
        generation: status::now_ms(),
        recovery_epoch: bindings[0].epoch,
        coverage_start: snapshot.start,
        anchor_height: snapshot.height,
        anchor_hash: snapshot.anchor,
        observed_ms: status::now_ms(),
        entries: snapshot.entries,
        rows_digest: snapshot.digest,
        public_digest: [0; 32],
    };
    let w: Ready = http
        .post(format!("{worker}/control/prepare"))
        .json(&Prepare {
            binding: bindings[0].clone(),
            manifest: m.clone(),
            artifact_digest: snapshot.digest,
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let r: Ready = http
        .post(format!("{router}/control/prepare"))
        .json(&Prepare {
            binding: bindings[1].clone(),
            manifest: m.clone(),
            artifact_digest: w.artifact_digest,
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let public = http
        .get(format!(
            "{router}/public/{}",
            hex::encode(r.manifest.public_digest)
        ))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?
        .to_vec();
    for (origin, binding) in [(&worker, &bindings[0]), (&router, &bindings[1])] {
        let _: Ready = http
            .post(format!("{origin}/control/activate"))
            .json(&Activate {
                binding: binding.clone(),
                manifest: r.manifest.clone(),
            })
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
    }
    let manifest = r.manifest.clone();
    let heartbeat_http = http.clone();
    let peers = [
        (worker.clone(), bindings[0].clone()),
        (router.clone(), bindings[1].clone()),
    ];
    let heartbeat = tokio::spawn(async move {
        loop {
            for (origin, binding) in &peers {
                let _ = heartbeat_http
                    .post(format!("{origin}/control/heartbeat"))
                    .json(&Activate {
                        binding: binding.clone(),
                        manifest: manifest.clone(),
                    })
                    .send()
                    .await;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    let anchor = AcceptedAnchor {
        network: snapshot.network,
        height: snapshot.height,
        hash: snapshot.anchor,
    };
    let client = Client::new(r.manifest.clone(), &public, &anchor)?;
    // A missing artifact must not replace the active publication. Verify exact
    // answers below after that failed update, against the original fixture.
    let mut invalid = m.clone();
    invalid.generation += 1;
    let refused = http
        .post(format!("{worker}/control/prepare"))
        .json(&Prepare {
            binding: bindings[0].clone(),
            manifest: invalid,
            artifact_digest: [0xff; 32],
        })
        .send()
        .await?;
    require(
        !refused.status().is_success(),
        "missing fixture artifact was accepted",
    )?;
    for (txid, expected) in fixture::cases(entries, false) {
        let actual = lookup(&http, &router, &client, txid, Some(snapshot.height)).await?;
        require(actual == expected, "distributed encrypted oracle mismatch")?;
    }
    // An authenticated fence must invalidate both new admission and already-held identities.
    let fence = Binding {
        epoch: bindings[1].epoch + 1,
        incarnation: bindings[1].incarnation.clone(),
    };
    http.post(format!("{router}/control/fence"))
        .json(&fence)
        .send()
        .await?
        .error_for_status()?;
    let query = client.prepare(&fixture::txid(0), Some(snapshot.height), status::now_ms())?;
    let response = http
        .post(format!("{router}/v1/status/query"))
        .body(query.body)
        .send()
        .await?;
    require(response.status() == 503, "fenced router still serves")?;
    heartbeat.abort();
    artifact_job.abort();
    let _ = artifact_job.await;
    println!(
        "{}",
        json!({"phase":"distributed_validation","passed":true,"processes":3,"external_worker":external_worker,"cuda":cuda,
        "entries":entries,"cases":["mined","mempool","forked","not_found","refused_update","remote_fence"],"protocol":PROTOCOL,"rows":ROWS,"slots":SLOTS,"slot_bytes":SLOT_BYTES,"columns":ROW_BYTES/2,"row_bytes":ROW_BYTES,"database_bytes":ROWS*ROW_BYTES,"production_qualified":false})
    );
    Ok(())
}
