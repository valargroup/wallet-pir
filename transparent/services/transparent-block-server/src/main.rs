//! Read-only, immutable compact-block benchmark service. Metrics belong on a private listener.
use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use clap::Parser;
use futures_util::StreamExt;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};
use transparent_blocks::dataset::{file_name, Manifest};
#[derive(Parser)]
struct Cli {
    #[arg(long)]
    dataset: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8096")]
    listen: String,
    #[arg(long, default_value = "127.0.0.1:8097")]
    metrics_listen: String,
}
struct App {
    root: PathBuf,
    manifest: Manifest,
    id: String,
    requests: AtomicU64,
    failures: AtomicU64,
    bytes: AtomicU64,
    nanos: AtomicU64,
    active: AtomicU64,
}
struct ActiveRequest {
    app: Arc<App>,
    started: Instant,
}
impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.app.active.fetch_sub(1, Ordering::Relaxed);
        self.app
            .nanos
            .fetch_add(self.started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
}
async fn batch(
    State(app): State<Arc<App>>,
    Path((id, variant)): Path<(usize, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    app.requests.fetch_add(1, Ordering::Relaxed);
    app.active.fetch_add(1, Ordering::Relaxed);
    let active = ActiveRequest {
        app: app.clone(),
        started: Instant::now(),
    };
    let encoding = query.get("encoding").map_or("gzip", String::as_str);
    let result = async {
        let name = file_name(id, &variant, encoding).map_err(|_| StatusCode::BAD_REQUEST)?;
        let batch = app.manifest.batches.get(id).ok_or(StatusCode::NOT_FOUND)?;
        let expected = &batch.artifacts[&format!("{variant}.{encoding}")];
        let file = tokio::fs::File::open(app.root.join(name))
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if file
            .metadata()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .len()
            != expected.bytes
        {
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        let length = expected.bytes.to_string();
        // Retain only a small read buffer per response, including slow clients.
        // The guard lives through streaming and is released on cancellation too.
        let stream =
            tokio_util::io::ReaderStream::with_capacity(file, 64 * 1024).map(move |chunk| {
                match &chunk {
                    Ok(bytes) => {
                        active
                            .app
                            .bytes
                            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    }
                    Err(_) => {
                        active.app.failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
                chunk
            });
        Ok((
            [
                ("content-type", "application/octet-stream"),
                ("content-encoding", encoding),
                ("content-length", length.as_str()),
                ("x-dataset-id", &app.id),
                ("cache-control", "public, max-age=31536000, immutable"),
            ],
            axum::body::Body::from_stream(stream),
        )
            .into_response())
    }
    .await;
    result.unwrap_or_else(|status| {
        app.failures.fetch_add(1, Ordering::Relaxed);
        status.into_response()
    })
}
async fn range_manifest(
    State(app): State<Arc<App>>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let Some(from) = query.get("from").and_then(|s| s.parse::<u64>().ok()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(index) = app.manifest.batches.iter().position(|b| b.end >= from) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if from < app.manifest.start {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let mut view = app.manifest.clone();
    view.batches = view.batches.split_off(index);
    view.start = view.batches[0].start;
    axum::Json(serde_json::json!({"dataset_id":app.id,"first_batch_index":index,"manifest":view}))
        .into_response()
}

async fn metrics(State(app): State<Arc<App>>) -> String {
    let mut system = sysinfo::System::new();
    let pid = sysinfo::get_current_pid().unwrap();
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    let (rss, cpu, started) = system.process(pid).map_or((0, 0.0, 0), |p| {
        (
            p.memory(),
            p.accumulated_cpu_time() as f64 / 1000.0,
            p.start_time(),
        )
    });
    format!("transparent_blocks_batch_requests_total {}\ntransparent_blocks_batch_failures_total {}\ntransparent_blocks_batch_response_bytes_total {}\ntransparent_blocks_batch_request_seconds_sum {}\ntransparent_blocks_active_batch_requests {}\nprocess_resident_memory_bytes {}\nprocess_cpu_seconds_total {}\nprocess_start_time_seconds {}\n",app.requests.load(Ordering::Relaxed),app.failures.load(Ordering::Relaxed),app.bytes.load(Ordering::Relaxed),app.nanos.load(Ordering::Relaxed) as f64/1e9,app.active.load(Ordering::Relaxed),rss,cpu,started)
}
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let manifest = Manifest::load(&cli.dataset, true)?;
    // A complete startup verification makes missing/corrupt artifacts a readiness failure.
    for i in 0..manifest.batches.len() {
        if i % 100 == 0 {
            eprintln!("Verifying dataset: {i}/{} batches", manifest.batches.len());
        }
        for v in transparent_blocks::dataset::VARIANTS {
            for e in ["identity", "gzip"] {
                transparent_blocks::dataset::read_batch(&cli.dataset, &manifest, i, v, e)?;
            }
        }
    }
    let id = manifest.id()?;
    let app = Arc::new(App {
        root: cli.dataset,
        manifest,
        id,
        requests: 0.into(),
        failures: 0.into(),
        bytes: 0.into(),
        nanos: 0.into(),
        active: 0.into(),
    });
    let router = Router::new()
        .route("/health", get(|| async { "ready" }))
        .route(
            "/identity",
            get(|State(a): State<Arc<App>>| async move {
                axum::Json(serde_json::json!({"dataset_id":a.id}))
            }),
        )
        .route(
            "/manifest",
            get(|State(a): State<Arc<App>>| async move { axum::Json(a.manifest.clone()) }),
        )
        .route("/range-manifest", get(range_manifest))
        .route("/batch/:id/:variant", get(batch))
        .with_state(app.clone());
    let metrics = Router::new()
        .route("/metrics", get(metrics))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    let private = tokio::net::TcpListener::bind(cli.metrics_listen).await?;
    tokio::try_join!(async { axum::serve(listener, router).await }, async {
        axum::serve(private, metrics).await
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn streamed_bytes_are_counted_and_cancellation_releases_activity() {
        let dir = tempfile::tempdir().unwrap();
        let blocks = vec![transparent_blocks::proto::CompactBlock {
            height: 0,
            hash: vec![1; 32],
            prev_hash: vec![0; 32],
            ..Default::default()
        }];
        let part = transparent_blocks::dataset::write_batch(dir.path(), 0, &blocks).unwrap();
        let size = part.artifacts["transparent.gzip"].bytes;
        let mut manifest = Manifest::new("01".repeat(32), 0, 0, "01".repeat(32), "test".into());
        manifest.batches.push(part);
        manifest.complete = true;
        let app = Arc::new(App {
            root: dir.path().to_owned(),
            id: manifest.id().unwrap(),
            manifest,
            requests: 0.into(),
            failures: 0.into(),
            bytes: 0.into(),
            nanos: 0.into(),
            active: 0.into(),
        });
        let response = batch(
            State(app.clone()),
            Path((0, "transparent".into())),
            Query(HashMap::new()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-encoding"], "gzip");
        assert_eq!(app.active.load(Ordering::Relaxed), 1);
        assert_eq!(app.bytes.load(Ordering::Relaxed), 0);
        let body = axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap();
        assert_eq!(body.len() as u64, size);
        assert_eq!(app.bytes.load(Ordering::Relaxed), size);
        assert_eq!(app.active.load(Ordering::Relaxed), 0);
        let response = batch(
            State(app.clone()),
            Path((0, "transparent".into())),
            Query(HashMap::new()),
        )
        .await;
        assert_eq!(app.active.load(Ordering::Relaxed), 1);
        drop(response);
        assert_eq!(app.active.load(Ordering::Relaxed), 0);
        assert_eq!(app.bytes.load(Ordering::Relaxed), size);
    }
}
