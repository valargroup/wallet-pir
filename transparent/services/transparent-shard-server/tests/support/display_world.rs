//! The in-process tiered display deployment the txid tests share: an
//! archive-owner and a recent-replica worker behind an edge that routes
//! `/v1/txid/archive/` to the owner, and the published fixtures.
#![allow(dead_code)]
use super::*;
pub const QUERY_BYTES: u64 = 40_200;

pub fn params(buckets: u32) -> DisplaySealParams {
    DisplaySealParams {
        n_archive: buckets,
        n_recent: buckets,
        archive_target: 1,
        recent_floor: 1,
        reorg_margin: 1,
    }
}

pub fn spec(shard_id: u64, start: u64, end: u64, sealed: bool, parent: &str) -> ShardSpec {
    ShardSpec {
        shard_id,
        start_height: start,
        end_height: end,
        sealed,
        revision: 0,
        supersedes: String::new(),
        parent_manifest_digest: parent.to_string(),
        n_buckets: 1,
        archive_target: 1,
        geometry: &TXID_2K,
    }
}

/// An inline record and records of one, two and five pages.
pub fn classes(seed: u64) -> Vec<TransparentDisplayRecord> {
    vec![
        synth::record(seed, 1, 25, 0),
        synth::record(seed, 2, 1_000, 0),
        synth::record(seed, 3, 5_000, 0),
        synth::record(seed, 4, 17_000, 0),
    ]
}

/// A record of `seed` whose two candidate rows coincide in `shard_id`.
pub fn coincident(seed: u64, shard_id: u64) -> TransparentDisplayRecord {
    let index = (1_000u64..)
        .find(|index| {
            let [a, b] = display::candidate_rows(&synth::txid(seed, *index), shard_id, 0, 2_048);
            a == b
        })
        .unwrap();
    synth::record(seed, index, 25, 0)
}

pub fn records(seed: u64, extra: &[TransparentDisplayRecord]) -> Vec<TransparentDisplayRecord> {
    let mut records = synth::records(40, seed + 1_000);
    records.extend(classes(seed));
    records.extend_from_slice(extra);
    records
}

pub fn config() -> ServiceConfig {
    ServiceConfig {
        cache_bytes: 2 << 30,
        readiness: ReadinessMode::Warm,
        ..ServiceConfig::default()
    }
}

pub fn loaded_only() -> ServiceConfig {
    ServiceConfig {
        readiness: ReadinessMode::LoadedOnly,
        ..config()
    }
}

/// Faults the edge injects in front of the workers.
#[derive(Clone, Default)]
pub struct Faults {
    /// Fail this many page queries with 503.
    pub pages: Arc<AtomicUsize>,
    /// Fail this many queries of any table with 503.
    pub queries: Arc<AtomicUsize>,
    /// Publish an init document naming a codec this client does not know.
    pub foreign_codec: Arc<AtomicBool>,
}

pub fn take(counter: &AtomicUsize) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
        .is_ok()
}

#[derive(Clone)]
pub struct Edge {
    pub archive: Router,
    pub recent: Router,
    pub faults: Faults,
}

pub async fn edge(State(edge): State<Edge>, request: Request) -> Response {
    let path = request.uri().path().to_string();
    let fail = path.contains("/query/")
        && (take(&edge.faults.queries)
            || path.ends_with("/query/pages") && take(&edge.faults.pages));
    if fail {
        let _ = axum::body::to_bytes(request.into_body(), usize::MAX).await;
        return (StatusCode::SERVICE_UNAVAILABLE, "injected").into_response();
    }
    let target = if path.starts_with("/v1/txid/archive/") {
        edge.archive
    } else {
        edge.recent
    };
    let response = target.oneshot(request).await.unwrap();
    if path == "/v1/txid/init" && edge.faults.foreign_codec.load(Ordering::Acquire) {
        let (parts, body) = response.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let mut init: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        init["codec"] = "transparent-txid-display-v9".into();
        let mut parts = parts;
        parts.headers.remove("content-length");
        return Response::from_parts(parts, Body::from(init.to_string()));
    }
    response
}

pub struct Worker {
    pub live: DisplayLive,
    pub runtime: Arc<DisplayRuntime>,
}

impl Worker {
    pub fn new(
        root: &Path,
        role: WorkerRole,
        config: ServiceConfig,
        collect: Vec<PathBuf>,
    ) -> Self {
        let runtime = DisplayRuntime::new(config, None);
        let live = DisplayLive::new(
            runtime.clone(),
            role,
            3,
            root.join(format!("{}.active.json", role.as_str())),
            collect,
            None,
        )
        .unwrap();
        Self { live, runtime }
    }

    pub fn builds(&self) -> u64 {
        self.runtime.metrics.builds.load(Ordering::Relaxed)
    }

    pub async fn command(&self, command: DisplayCommand) -> Result<serde_json::Value, String> {
        self.live
            .command(command)
            .await
            .map(serde_json::Value::Object)
    }

    /// Prepares and activates a publication; returns the prepare reply.
    pub async fn publish(&self, directory: &Path, map_sha256: &str) -> serde_json::Value {
        let expected = self.live.expected();
        let prepared = self
            .command(DisplayCommand::Prepare {
                expected: expected.clone(),
                publication: DisplayPublication {
                    directory: directory.to_path_buf(),
                    map_sha256: map_sha256.to_string(),
                },
            })
            .await
            .unwrap();
        self.command(DisplayCommand::Activate {
            expected,
            map_sha256: map_sha256.to_string(),
        })
        .await
        .unwrap();
        prepared
    }
}

pub struct World {
    pub root: tempfile::TempDir,
    pub archive: Worker,
    pub recent: Worker,
    pub faults: Faults,
    pub url: String,
    pub server: tokio::task::JoinHandle<()>,
    /// Archive shard 0 over 100..=199 and recent shard 1 over 200..=260.
    pub a0: Published,
    pub r0: Published,
    pub a0_records: Vec<TransparentDisplayRecord>,
    pub r0_records: Vec<TransparentDisplayRecord>,
    pub p0: (PathBuf, String),
}

impl World {
    pub async fn start() -> Self {
        Self::start_with(config()).await
    }

    /// The owner collects `<root>/sealed`, where shards are written, as a
    /// worker collects the root sealed revisions are shipped to for staging.
    pub async fn start_with(config: ServiceConfig) -> Self {
        let root = tempfile::tempdir().unwrap();
        let a0_records = records(10, &[]);
        let r0_records = records(20, &[coincident(20, 1)]);
        let a0 =
            synth::write_shard(root.path(), &spec(0, 100, 199, true, ""), &a0_records).unwrap();
        let r0 = synth::write_shard(
            root.path(),
            &spec(1, 200, 260, false, &a0.digest),
            &r0_records,
        )
        .unwrap();
        let p0 = synth::write_candidate(root.path(), &params(1), &[a0.clone(), r0.clone()], "p0")
            .unwrap();
        let archive = Worker::new(
            root.path(),
            WorkerRole::ArchiveOwner,
            config,
            vec![root.path().join("sealed")],
        );
        let recent = Worker::new(root.path(), WorkerRole::RecentReplica, config, Vec::new());
        // Both start empty and take the publication through control, as a
        // freshly deployed worker does.
        assert_eq!(archive.publish(&p0.0, &p0.1).await["built"], 2);
        assert_eq!(recent.publish(&p0.0, &p0.1).await["built"], 2);
        let faults = Faults::default();
        let app = Router::new().fallback(edge).with_state(Edge {
            archive: archive.live.router(),
            recent: recent.live.router(),
            faults: faults.clone(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            root,
            archive,
            recent,
            faults,
            url,
            server,
            a0,
            r0,
            a0_records,
            r0_records,
            p0,
        }
    }

    pub fn client(&self) -> DisplayClient {
        DisplayClient::new(&self.url, None, None).unwrap()
    }

    pub async fn get(&self, path: &str) -> (u16, serde_json::Value) {
        let response = reqwest::get(format!("{}{path}", self.url)).await.unwrap();
        let status = response.status().as_u16();
        let body = response.bytes().await.unwrap();
        (status, serde_json::from_slice(&body).unwrap_or_default())
    }
}

impl Drop for World {
    fn drop(&mut self) {
        self.server.abort();
    }
}
