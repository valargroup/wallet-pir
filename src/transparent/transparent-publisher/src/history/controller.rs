//! Continuous, chain-checked publication. Ingest owns the journal; builders use
//! copied suffix snapshots, and query servers pull each finalized full
//! publication over HTTPS.
mod startup;
pub use startup::run_managed;
mod chain_input;
use chain_input::*;
mod activation;
use activation::*;
mod restoration;
use restoration::*;
mod runtime_preparation;
use runtime_preparation::*;
mod distribution;
use distribution::*;
mod http;
use http::*;
mod collection;
use collection::*;
mod preparation;
mod workflow;
use workflow::HistoryPublication;
use zakura_pir_publisher::transparent::filter as transparent_filter;
use zakura_pir_publisher::transparent::layout::Geometry;
use zakura_pir_publisher::transparent::seal::DecoupledPolicy;

pub(crate) use crate::history::distribution::advance_distribution_revision;
#[cfg(all(test, feature = "fixtures"))]
pub(crate) use crate::history::distribution::advance_distribution_revision_with_sync;
use crate::history::publication::{self, PublishOptions};
use crate::history::shard_filters::ShardFilters;
use crate::ingest::blocks::build_fetched_block_events;
use crate::ingest::journal::Journal;
use crate::ingest::prevout::OutputCache;
use crate::ingest::snapshot::Snapshot;
use crate::ingest::zakura::NodeClient;
use crate::BoxError;
use axum::{
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Router,
};
use pir_pipeline::Input;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
};
use transparent_filter::BlockHash;
use transparent_shared::journal::events::{EventStore, EventStoreError, Refreshed};

pub use crate::history::config::Config;

#[derive(Clone, Serialize, Deserialize)]
pub struct ActivePublication {
    pub directory: PathBuf,
    pub map_sha256: String,
    pub height: u64,
    pub hash: String,
}
struct Public {
    _lease: crate::storage::PublicationLease,
    publication: ActivePublication,
    filters: ShardFilters,
    distribution: Option<transparent_shared::publication::Finalized>,
}
struct Inner {
    cycle_duration: pir_observability::Distribution,
    publication_freshness: pir_observability::Distribution,
    telemetry: pir_observability::HttpMetrics,
    publication_attempts: AtomicU64,
    publication_failures: AtomicU64,
    last_publication: AtomicU64,
    active: RwLock<Arc<Public>>,
    withdrawn: AtomicBool,
    epoch: AtomicU64,
    distribution_revision: AtomicU64,
    distribution_slots: Arc<tokio::sync::Semaphore>,
    /// The existing producer canonicality source, never provider credentials.
    canonicality: Option<NodeClient>,
    invalidated_from: AtomicU64,
    publication_gate: std::sync::Mutex<()>,
    status: RwLock<serde_json::Value>,
    observed: RwLock<BTreeMap<u64, std::time::Instant>>,
    /// The last candidate whose runtimes were prepared, as verified then: the
    /// next one reuses exact-file evidence for unchanged stored objects.
    prepared_set: std::sync::Mutex<Option<transparent_shared::history::shardset::ShardSet>>,
    /// The service's stop request. Candidate preparation observes it between
    /// files and segments and ends without activating; activation completes.
    shutdown: pir_pipeline::service::Shutdown,
    /// Distributed runtimes this process already loaded and self-checked; the
    /// next inventory rehashes them but checks only changed bytes again.
    checked_runtimes: std::sync::Mutex<transparent_shared::publication::CheckedRuntimes>,
    /// Runtime preparation outcomes since this process started.
    runtime_preparations: AtomicU64,
    runtime_preparation_failures: AtomicU64,
    /// Background body validation of restored publications.
    validation: super::validator::Queue,
    /// The active publication lists bytes that failed integrity validation.
    integrity_failed: AtomicBool,
    /// Candidate preparation and activation in progress; background
    /// validation waits while any runs.
    foreground: AtomicU64,
}
#[derive(Clone)]
pub struct Authority(Arc<Inner>);

impl Authority {
    pub fn router(&self) -> Router {
        self.0
            .telemetry
            .initialize(&["init", "map", "filter", "manifest"]);
        Router::new()
            .fallback(public_request)
            .layer(axum::middleware::from_fn_with_state(
                self.0.telemetry.clone(),
                transparent_shared::http::observe,
            ))
            .with_state(self.clone())
    }
}

fn read_public(directory: PathBuf) -> Result<Public, BoxError> {
    let lease = crate::storage::PublicationLease::acquire(&directory)?;
    let filters = ShardFilters::open(&directory)?;
    let end = filters.map().shards.last().ok_or("empty publication")?;
    let publication = ActivePublication {
        directory,
        map_sha256: filters.map_digest(),
        height: end.end_height,
        hash: end.terminal_block_hash.clone(),
    };
    Ok(Public {
        _lease: lease,
        publication,
        filters,
        distribution: None,
    })
}

/// Owns ingestion and authority through shutdown; transactions finish before stopping.
// Product-owned tokens retain the immutable source snapshot and activation
// evidence. The shared engine owns neither stores nor publication authority.
struct HistoryWork {
    old: Arc<Public>,
    epoch: u64,
    snapshot: Snapshot,
    height: u64,
    hash: String,
}
struct HistoryCandidate {
    candidate: Public,
    epoch: u64,
    height: u64,
    hash: String,
    started: std::time::Instant,
    published_unix: f64,
    prepared_unix: f64,
    runtimes: serde_json::Value,
    storage: serde_json::Value,
}

type HistorySeal = fn(&Geometry) -> DecoupledPolicy;

#[cfg(all(test, feature = "fixtures", feature = "qualification-tests"))]
async fn publish_once(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
) -> Result<(), BoxError> {
    publish_candidate(
        config,
        rpc,
        store,
        authority,
        DecoupledPolicy::for_geometry,
        Some(config.archive_geometry.clone()),
        Some(config.recent_from),
    )
    .await
}

#[cfg(all(test, feature = "fixtures"))]
async fn publish_candidate(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
    seal: HistorySeal,
    archive_geometry: Option<String>,
    recent_from: Option<u64>,
) -> Result<(), BoxError> {
    use pir_pipeline::publisher::PublisherExecutor;
    if let Input::Work(work) = acquire_history(config, rpc, store, authority).await? {
        crate::host_engine::PublisherEngine
            .publish(preparation::HistoryPreparation::new(
                config,
                rpc,
                authority,
                work,
                seal,
                archive_geometry,
                recent_from,
            ))
            .await?;
    }
    Ok(())
}
#[cfg(all(test, feature = "fixtures"))]
async fn prepare_candidate<'a>(
    config: &'a Config,
    rpc: &'a NodeClient,
    authority: &'a Authority,
    work: HistoryWork,
    seal: HistorySeal,
    archive_geometry: Option<String>,
    recent_from: Option<u64>,
) -> Result<
    Input<crate::host_engine::PreparedPublication<preparation::HistoryPreparation<'a>>>,
    BoxError,
> {
    match crate::host_engine::PublisherEngine
        .prepare(preparation::HistoryPreparation::new(
            config,
            rpc,
            authority,
            work,
            seal,
            archive_geometry,
            recent_from,
        ))
        .await
    {
        Err(_) if authority.0.shutdown.requested() => Ok(Input::Unchanged),
        result => result.map(Input::Work),
    }
}

async fn acquire_history(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
) -> Result<Input<HistoryWork>, BoxError> {
    let old = authority.0.active.read().unwrap().clone();
    let epoch = authority.0.epoch.load(Ordering::Acquire);
    {
        let journal = store.lock().await;
        if journal.covered_through() == Some(old.publication.height)
            && journal
                .block_at(old.publication.height)
                .is_some_and(|b| b.block_hash.to_display_hex() == old.publication.hash)
            && !authority.0.withdrawn.load(Ordering::Acquire)
        {
            return Ok(Input::Unchanged);
        }
    }
    authority
        .0
        .publication_attempts
        .fetch_add(1, Ordering::Relaxed);
    let snapshot = {
        let journal = store.lock().await;
        Snapshot::capture(&journal, old.filters.map(), config.snapshot_dir())?
    };
    let height = snapshot.end();
    let hash = snapshot
        .block_at(height)
        .ok_or("snapshot has no endpoint")?
        .block_hash
        .to_display_hex();
    if rpc.block_hash_display(height).await? != hash {
        return Err("journal endpoint not canonical".into());
    }
    if height == old.publication.height
        && hash == old.publication.hash
        && (!authority.0.withdrawn.load(Ordering::Acquire)
            || authority.0.integrity_failed.load(Ordering::Acquire))
    {
        // An integrity failure needs replacement bytes, not another full
        // preparation of the same ones.
        return Ok(Input::Unchanged);
    }
    Ok(Input::Work(HistoryWork {
        old,
        epoch,
        snapshot,
        height,
        hash,
    }))
}

/// Publication roots whose next activation a test interrupts once its intent
/// is written, as a process dying there would.
#[cfg(test)]
static INTERRUPT_AFTER_INTENT: std::sync::Mutex<std::collections::BTreeSet<PathBuf>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Concurrent distribution responses; background validation waits while any runs.
const DISTRIBUTION_SLOTS: usize = 4;
/// Artifacts whose bytes failed integrity validation, kept across restarts.
const INTEGRITY_FAILURES: &str = "integrity-failures.json";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct IntegrityFailure {
    path: String,
    expected_sha256: String,
    found_sha256: Option<String>,
    inventory_sha256: String,
    map_sha256: String,
    reason: String,
}

#[cfg(all(test, feature = "fixtures"))]
mod tests {

    #[test]
    fn unavailable_asks_wallets_to_retry() {
        let response = super::unavailable();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(response.headers()["retry-after"], "1");
    }

    use super::*;
    use axum::{routing::post, Json};

    /// A few empty blocks close a shard. The service follow path does not use this.
    #[cfg(not(feature = "qualification-tests"))]
    fn two_script_seal(geometry: &Geometry) -> DecoupledPolicy {
        let mut policy = DecoupledPolicy::for_geometry(geometry);
        policy.scripts.target = 2;
        policy
    }

    async fn publish_follow(
        config: &Config,
        rpc: &NodeClient,
        store: &Arc<tokio::sync::Mutex<EventStore>>,
        authority: &Authority,
    ) -> Result<(), BoxError> {
        #[cfg(not(feature = "qualification-tests"))]
        {
            return publish_candidate(config, rpc, store, authority, two_script_seal, None, None)
                .await;
        }
        #[cfg(feature = "qualification-tests")]
        {
            publish_once(config, rpc, store, authority).await
        }
    }

    async fn prepare_follow<'a>(
        config: &'a Config,
        rpc: &'a NodeClient,
        authority: &'a Authority,
        work: HistoryWork,
    ) -> Result<
        Input<crate::host_engine::PreparedPublication<preparation::HistoryPreparation<'a>>>,
        BoxError,
    > {
        #[cfg(not(feature = "qualification-tests"))]
        {
            return prepare_candidate(config, rpc, authority, work, two_script_seal, None, None)
                .await;
        }
        #[cfg(feature = "qualification-tests")]
        {
            prepare_candidate(
                config,
                rpc,
                authority,
                work,
                DecoupledPolicy::for_geometry,
                Some(config.archive_geometry.clone()),
                Some(config.recent_from),
            )
            .await
        }
    }
    type Chain = Arc<RwLock<Vec<Vec<u8>>>>;
    struct MockNode {
        chain: Chain,
        pause_ancestor: AtomicBool,
        ancestor_started: tokio::sync::Notify,
        resume: tokio::sync::Notify,
    }
    fn raw_block(parent: [u8; 32], height: u32, nonce: u8) -> Vec<u8> {
        // Syntactically valid headers and coinbase-only blocks with no outputs
        // are sufficient here: the mock node represents consensus acceptance,
        // which is not this controller's job. No proof-of-work is asserted by
        // this fixture. The chain parser rejects blocks without transactions.
        let mut raw = 4u32.to_le_bytes().to_vec();
        raw.extend(parent);
        raw.extend([0; 64]);
        raw.extend(1_700_000_000u32.to_le_bytes());
        raw.extend(0x1f07ffffu32.to_le_bytes());
        raw.extend([nonce; 32]);
        raw.extend([0xfd, 0x40, 0x05]);
        raw.extend([0; 1344]);
        raw.push(1);
        // A height-zero coinbase must carry the real genesis script, and nothing
        // here reads coinbase heights, so genesis encodes height one.
        raw.extend(zakura_server_chain::testing::coinbase(height.max(1), &[]).bytes());
        raw
    }
    fn hash(raw: &[u8]) -> BlockHash {
        let block = zakura_server_chain::transparent::FetchedBlock::from_bytes(raw).unwrap();
        BlockHash::from_internal_bytes(block.hash())
    }
    fn extend(chain: &mut Vec<Vec<u8>>, count: usize, tag: u8) {
        for i in 0..count {
            let parent = chain
                .last()
                .map(|b| *hash(b).internal_bytes())
                .unwrap_or([0; 32]);
            chain.push(raw_block(parent, chain.len() as u32, tag + i as u8));
        }
    }
    async fn rpc(
        State(node): State<Arc<MockNode>>,
        Json(request): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        let height = || {
            request["params"][0]
                .as_u64()
                .or_else(|| request["params"][0].as_str().and_then(|s| s.parse().ok()))
                .unwrap() as usize
        };
        if request["method"] == "getblockhash"
            && height() == 3
            && node.pause_ancestor.swap(false, Ordering::AcqRel)
        {
            node.ancestor_started.notify_one();
            node.resume.notified().await;
        }
        let chain = node.chain.read().unwrap();
        let result = match request["method"].as_str().unwrap() {
            "getblockcount" => serde_json::json!(chain.len() - 1),
            "getblockhash" => serde_json::json!(hash(&chain[height()]).to_display_hex()),
            "getblock" => serde_json::json!(hex::encode(&chain[height()])),
            method => panic!("unexpected RPC {method}"),
        };
        Json(serde_json::json!({"id":request["id"],"result":result,"error":null}))
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_follow_publishes_empty_blocks_and_recovers_a_sealed_reorg() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let chain: Chain = Arc::new(RwLock::new(Vec::new()));
        extend(&mut chain.write().unwrap(), 5, 1);
        let node = Arc::new(MockNode {
            chain: chain.clone(),
            pause_ancestor: AtomicBool::new(false),
            ancestor_started: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/", post(rpc)).with_state(node.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let cookie = root.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let client = crate::ingest::zakura::connect(format!("http://{address}"), &cookie).unwrap();
        let journal_dir = root.path().join("journal");
        let mut journal = EventStore::open(
            &journal_dir,
            &hash(&chain.read().unwrap()[0]).to_display_hex(),
            0,
        )
        .unwrap();
        for h in 0..3 {
            journal
                .append_block(h, hash(&chain.read().unwrap()[h as usize]), &[])
                .unwrap();
        }
        journal.commit().unwrap();
        let initial = root.path().join("initial");
        let options = PublishOptions {
            data_dir: journal_dir.clone(),
            output: initial.clone(),
            previous: None,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: if cfg!(feature = "qualification-tests") {
                Some("decoupled-32k-32k".into())
            } else {
                None
            },
            recent_from: if cfg!(feature = "qualification-tests") {
                Some(2)
            } else {
                None
            },
            zakura_rpc_url: String::new(),
            zakura_cookie: cookie.clone(),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
        };
        #[cfg(feature = "qualification-tests")]
        publication::publish(&options, &journal, BlockHash::from_internal_bytes([0; 32])).unwrap();
        #[cfg(not(feature = "qualification-tests"))]
        publication::publish_with(
            &options,
            &journal,
            BlockHash::from_internal_bytes([0; 32]),
            &two_script_seal,
        )
        .unwrap();
        let publications = root.path().join("publications");
        std::fs::create_dir(&publications).unwrap();
        let config = Config {
            data_dir: journal_dir,
            publication_root: publications,
            initial_publication: initial.clone(),
            // Without an archive tier every shard is recent.
            recent_from: if cfg!(feature = "qualification-tests") {
                2
            } else {
                0
            },
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: "decoupled-32k-32k".into(),
            rpc_url: format!("http://{address}"),
            rpc_cookie: cookie,
            listen: address,
            source_sha: "fixture".into(),
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
            follow_journal: false,
            snapshot_dir: None,
        };
        let authority = Authority(Arc::new(Inner {
            cycle_duration: Default::default(),
            publication_freshness: Default::default(),
            telemetry: Default::default(),
            publication_attempts: AtomicU64::new(0),
            publication_failures: AtomicU64::new(0),
            last_publication: AtomicU64::new(0),
            active: RwLock::new(Arc::new(read_public(initial).unwrap())),
            withdrawn: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            distribution_revision: AtomicU64::new(1),
            distribution_slots: Arc::new(tokio::sync::Semaphore::new(4)),
            canonicality: None,
            invalidated_from: AtomicU64::new(u64::MAX),
            publication_gate: std::sync::Mutex::new(()),
            status: RwLock::new(serde_json::json!({})),
            observed: RwLock::new(BTreeMap::new()),
            prepared_set: std::sync::Mutex::new(None),
            shutdown: Default::default(),
            checked_runtimes: Default::default(),
            runtime_preparations: AtomicU64::new(0),
            runtime_preparation_failures: AtomicU64::new(0),
            validation: Default::default(),
            integrity_failed: AtomicBool::new(false),
            foreground: AtomicU64::new(0),
        }));
        let store = Arc::new(tokio::sync::Mutex::new(journal));
        let (notify, _) = tokio::sync::watch::channel(0);
        let mut input = HistoryInputTask {
            config: config.clone(),
            rpc: client.clone(),
            store: store.clone(),
            authority: authority.clone(),
            notify: notify.clone(),
            cache: OutputCache::new(100),
            pending_invalidation: None,
        };
        pir_pipeline::Task::run(&mut input).await.unwrap();
        let mut cache = input.cache;
        assert_eq!(store.lock().await.covered_through(), Some(4));
        // A candidate that cannot be written fails preparation.
        std::fs::set_permissions(
            &config.publication_root,
            std::fs::Permissions::from_mode(0o555),
        )
        .unwrap();
        assert!(publish_follow(&config, &client, &store, &authority)
            .await
            .is_err());
        std::fs::set_permissions(
            &config.publication_root,
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        assert_eq!(authority.0.active.read().unwrap().publication.height, 2);
        assert!(!config.publication_root.join("activation.json").exists());
        // Preparation failure cannot undo the independent source commit; a
        // restarted reader sees it before any successful publication.
        assert_eq!(
            EventStore::open_existing(&config.data_dir)
                .unwrap()
                .covered_through(),
            Some(4)
        );
        // A candidate observed by shared progress has no activation authority.
        let Input::Work(work) = acquire_history(&config, &client, &store, &authority)
            .await
            .unwrap()
        else {
            panic!("new durable work expected");
        };
        let Input::Work(ready) = prepare_follow(&config, &client, &authority, work)
            .await
            .unwrap()
        else {
            panic!("prepared candidate expected");
        };
        authority.0.epoch.fetch_add(1, Ordering::AcqRel);
        assert!(ready
            .commit()
            .await
            .unwrap_err()
            .to_string()
            .contains("invalidated before activation"));
        assert!(!config.publication_root.join("activation.json").exists());
        assert!(!config.publication_root.join("active.json").exists());
        // Replacing journal-only blocks must cancel preparation without
        // withdrawing the still-canonical publication through height two.
        {
            let mut blocks = chain.write().unwrap();
            blocks.truncate(3);
            extend(&mut blocks, 2, 17);
        }
        ingest_once(&config, &client, &store, &authority, &mut cache, &notify)
            .await
            .unwrap();
        assert!(authority.0.epoch.load(Ordering::Acquire) > 0);
        assert!(
            !authority.0.withdrawn.load(Ordering::Acquire),
            "a journal-only fork must preserve canonical public coverage"
        );
        assert_eq!(authority.0.active.read().unwrap().publication.height, 2);
        // A candidate whose work predates another journal-only fork stops
        // preparing on its epoch check and never activates; old public
        // coverage stays available.
        let Input::Work(work) = acquire_history(&config, &client, &store, &authority)
            .await
            .unwrap()
        else {
            panic!("new durable work expected");
        };
        {
            let mut blocks = chain.write().unwrap();
            blocks.truncate(3);
            extend(&mut blocks, 2, 18);
        }
        ingest_once(&config, &client, &store, &authority, &mut cache, &notify)
            .await
            .unwrap();
        let response = public_request(
            State(authority.clone()),
            Request::builder()
                .uri("/v2/shards")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let legacy_map = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let canonical = public_request(
            State(authority.clone()),
            Request::builder()
                .uri("/transparent/v1/shards")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(canonical.status(), StatusCode::OK);
        let canonical_map = axum::body::to_bytes(canonical.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(canonical_map, legacy_map);
        for private in ["/transparent/v1/status", "/transparent/v1/metrics"] {
            let response = public_request(
                State(authority.clone()),
                Request::builder()
                    .uri(private)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{private}");
        }
        assert!(prepare_follow(&config, &client, &authority, work)
            .await
            .err()
            .expect("an invalidated candidate must not prepare")
            .to_string()
            .contains("candidate invalidated"));
        assert_eq!(authority.0.active.read().unwrap().publication.height, 2);
        publish_follow(&config, &client, &store, &authority)
            .await
            .unwrap();
        assert_eq!(authority.0.active.read().unwrap().publication.height, 4);
        let old = authority
            .0
            .active
            .read()
            .unwrap()
            .publication
            .map_sha256
            .clone();
        // Same-height fork of the published tip.
        {
            let mut blocks = chain.write().unwrap();
            blocks.truncate(1);
            extend(&mut blocks, 4, 20);
        }
        {
            node.pause_ancestor.store(true, Ordering::Release);
            let ingest = ingest_once(&config, &client, &store, &authority, &mut cache, &notify);
            tokio::pin!(ingest);
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                tokio::select! {
                    result = &mut ingest => panic!("ancestor lookup was not paused: {result:?}"),
                    _ = node.ancestor_started.notified() => {}
                }
            })
            .await
            .unwrap();
            let response = public_request(
                State(authority.clone()),
                Request::builder()
                    .uri("/v2/shards")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "orphaned coverage must be withdrawn before a slow ancestor search"
            );
            node.resume.notify_one();
            ingest.await.unwrap();
        }
        assert!(authority.0.withdrawn.load(Ordering::Acquire));
        publish_follow(&config, &client, &store, &authority)
            .await
            .unwrap();
        assert!(!authority.0.withdrawn.load(Ordering::Acquire));
        {
            let active = authority.0.active.read().unwrap();
            assert_eq!(active.publication.height, 4);
            assert_ne!(active.publication.map_sha256, old);
            assert_eq!(
                active.publication.hash,
                hash(&chain.read().unwrap()[4]).to_display_hex()
            );
        }
        let genesis = store.lock().await.block_at(0).unwrap().block_hash;
        {
            let mut blocks = chain.write().unwrap();
            blocks.clear();
            extend(&mut blocks, 5, 40);
        }
        let error = ingest_once(&config, &client, &store, &authority, &mut cache, &notify)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("node genesis changed"));
        assert!(authority.0.withdrawn.load(Ordering::Acquire));
        assert_eq!(store.lock().await.block_at(0).unwrap().block_hash, genesis);
        server.abort();
    }

    /// The real publication path prepares every runtime of each candidate
    /// beside its revisions and distributes them; the next candidate links the
    /// unchanged ones from the active publication and builds only what changed.
    #[cfg(feature = "qualification-tests")]
    #[tokio::test(flavor = "multi_thread")]
    async fn shipped_runtimes_are_prepared_linked_and_offered_with_the_candidate() {
        shipped_runtime_scenario(false).await;
    }

    /// Source-only candidates still exercise invalidation, verification against
    /// previously verified files, and a counted runtime write failure.
    #[cfg(feature = "qualification-tests")]
    #[tokio::test(flavor = "multi_thread")]
    async fn shipped_runtime_preparation_refuses_invalid_candidates_and_reports_write_failure() {
        shipped_runtime_scenario(true).await;
    }

    // Success exercises cold preparation and handover with both supported
    // geometries. Refusals need the same verified source files, but no prior
    // cryptographic build: keep that independent work in its own bounded case.
    #[cfg(feature = "qualification-tests")]
    async fn shipped_runtime_scenario(failures_only: bool) {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let root = tempfile::tempdir().unwrap();
        let chain: Chain = Arc::new(RwLock::new(Vec::new()));
        extend(&mut chain.write().unwrap(), 5, 1);
        let node = Arc::new(MockNode {
            chain: chain.clone(),
            pause_ancestor: AtomicBool::new(false),
            ancestor_started: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/", post(rpc)).with_state(node.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let cookie = root.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let client = crate::ingest::zakura::connect(format!("http://{address}"), &cookie).unwrap();
        let journal_dir = root.path().join("journal");
        let mut journal = EventStore::open(
            &journal_dir,
            &hash(&chain.read().unwrap()[0]).to_display_hex(),
            0,
        )
        .unwrap();
        for h in 0..3 {
            journal
                .append_block(h, hash(&chain.read().unwrap()[h as usize]), &[])
                .unwrap();
        }
        journal.commit().unwrap();
        let initial = root.path().join("initial");
        let options = PublishOptions {
            data_dir: journal_dir.clone(),
            output: initial.clone(),
            previous: None,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: Some("decoupled-32k-32k".into()),
            recent_from: Some(2),
            zakura_rpc_url: String::new(),
            zakura_cookie: cookie.clone(),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
        };
        publication::publish(&options, &journal, BlockHash::from_internal_bytes([0; 32])).unwrap();
        let publications = root.path().join("publications");
        std::fs::create_dir(&publications).unwrap();
        let config = Config {
            data_dir: journal_dir,
            publication_root: publications,
            initial_publication: initial.clone(),
            recent_from: 2,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: "decoupled-32k-32k".into(),
            rpc_url: format!("http://{address}"),
            rpc_cookie: cookie,
            listen: address,
            source_sha: "fixture".into(),
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
            follow_journal: false,
            snapshot_dir: None,
        };
        let authority = Authority(Arc::new(Inner {
            cycle_duration: Default::default(),
            publication_freshness: Default::default(),
            telemetry: Default::default(),
            publication_attempts: AtomicU64::new(0),
            publication_failures: AtomicU64::new(0),
            last_publication: AtomicU64::new(0),
            active: RwLock::new(Arc::new(read_public(initial).unwrap())),
            withdrawn: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            distribution_revision: AtomicU64::new(1),
            distribution_slots: Arc::new(tokio::sync::Semaphore::new(4)),
            canonicality: None,
            invalidated_from: AtomicU64::new(u64::MAX),
            publication_gate: std::sync::Mutex::new(()),
            status: RwLock::new(serde_json::json!({})),
            observed: RwLock::new(BTreeMap::new()),
            prepared_set: std::sync::Mutex::new(None),
            shutdown: Default::default(),
            checked_runtimes: Default::default(),
            runtime_preparations: AtomicU64::new(0),
            runtime_preparation_failures: AtomicU64::new(0),
            validation: Default::default(),
            integrity_failed: AtomicBool::new(false),
            foreground: AtomicU64::new(0),
        }));
        let store = Arc::new(tokio::sync::Mutex::new(journal));
        let (notify, _) = tokio::sync::watch::channel(0);
        let mut cache = OutputCache::new(100);
        let runtime_files = |dir: &Path| -> BTreeMap<String, (u64, u64)> {
            std::fs::read_dir(dir)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|e| e.file_name().to_string_lossy().ends_with(".runtime"))
                .map(|e| {
                    let meta = std::fs::metadata(e.path()).unwrap();
                    (
                        e.file_name().to_string_lossy().into_owned(),
                        (meta.dev(), meta.ino()),
                    )
                })
                .collect()
        };

        let source = if failures_only {
            authority.0.active.read().unwrap().publication.clone()
        } else {
            ingest_once(&config, &client, &store, &authority, &mut cache, &notify)
                .await
                .unwrap();
            publish_once(&config, &client, &store, &authority)
                .await
                .unwrap();
            let first = authority.0.active.read().unwrap().publication.clone();
            assert_eq!(first.height, 4);
            assert!(authority.0.active.read().unwrap().distribution.is_some());
            let set =
                transparent_shared::history::shardset::ShardSet::open(&first.directory, 3).unwrap();
            let files = runtime_files(&first.directory);
            assert_eq!(
                files.len(),
                set.warm_targets().len(),
                "one file per runtime"
            );
            let cycle = authority.0.status.read().unwrap()["last_cycle"].clone();
            let report = &cycle["runtime_preparation"];
            assert_eq!(report["built"], files.len(), "{cycle}");
            assert_eq!(report["failures"], serde_json::json!([]));
            assert!(cycle["activated_unix"].as_f64() >= cycle["prepared_unix"].as_f64());
            assert!(cycle["prepared_unix"].as_f64() >= cycle["published_unix"].as_f64());

            // The next block: the sealed archive shard's runtime is the same
            // file, linked; only the changed tail is built.
            extend(&mut chain.write().unwrap(), 1, 60);
            ingest_once(&config, &client, &store, &authority, &mut cache, &notify)
                .await
                .unwrap();
            publish_once(&config, &client, &store, &authority)
                .await
                .unwrap();
            let second = authority.0.active.read().unwrap().publication.clone();
            assert_eq!(second.height, 5);
            let report = authority.0.status.read().unwrap()["runtime_preparation"].clone();
            assert!(report["linked"].as_u64().unwrap() >= 1, "{report}");
            assert!(report["built"].as_u64().unwrap() >= 1, "{report}");
            let (before, after) = (files, runtime_files(&second.directory));
            let shared: Vec<_> = before
                .iter()
                .filter(|(name, _)| after.contains_key(*name))
                .collect();
            assert!(!shared.is_empty());
            for (name, identity) in shared {
                assert_eq!(&after[name], identity, "{name} is the same file");
            }
            assert_eq!(authority.0.runtime_preparations.load(Ordering::Relaxed), 2);

            server.abort();
            return;
        };

        // A preparation for an epoch an invalidation has since moved past
        // stops before its first segment, and nothing is offered.
        // Candidate inputs are immutable. Link them into a private tree and
        // omit runtime caches, rather than copying large files just to delete
        // them. Fault injection below unlinks a table before changing it.
        fn candidate_inputs(source: &Path, target: &Path) {
            std::fs::create_dir(target).unwrap();
            for entry in std::fs::read_dir(source).unwrap() {
                let entry = entry.unwrap();
                let kind = entry.file_type().unwrap();
                if kind.is_dir() {
                    candidate_inputs(&entry.path(), &target.join(entry.file_name()));
                } else {
                    assert!(kind.is_file());
                    if entry.path().extension().is_none_or(|ext| ext != "runtime") {
                        std::fs::hard_link(entry.path(), target.join(entry.file_name())).unwrap();
                    }
                }
            }
        }
        let candidate = config.publication_root.join("candidate-superseded");
        candidate_inputs(&source.directory, &candidate);
        for name in runtime_files(&candidate).keys() {
            std::fs::remove_file(candidate.join(name)).unwrap();
        }
        let stale = authority.0.epoch.load(Ordering::Acquire) + 1;
        let error = prepare_runtimes(
            &config,
            &authority,
            &candidate,
            stale,
            &mut Default::default(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("invalidated"), "{error}");
        assert!(runtime_files(&candidate).is_empty());
        assert!(authority.0.prepared_set.lock().unwrap().is_some());

        // A candidate whose table does not verify is not published.
        let epoch = authority.0.epoch.load(Ordering::Acquire);
        // Replacing a linked directory table makes a new file that the last
        // verified set does not vouch for.
        let candidate = config.publication_root.join("candidate-corrupt");
        candidate_inputs(&source.directory, &candidate);
        let mut corrupted = 0;
        for entry in std::fs::read_dir(&candidate)
            .unwrap()
            .filter_map(Result::ok)
        {
            let table = entry.path().join("directory.0.bin");
            if !table.is_file() {
                continue;
            }
            let mut bytes = std::fs::read(&table).unwrap();
            bytes[0] ^= 1;
            std::fs::remove_file(&table).unwrap();
            std::fs::write(&table, bytes).unwrap();
            let original = source
                .directory
                .join(entry.file_name())
                .join("directory.0.bin");
            assert_ne!(
                std::fs::metadata(&table).unwrap().ino(),
                std::fs::metadata(original).unwrap().ino()
            );
            corrupted += 1;
        }
        assert!(corrupted > 0);
        let error = prepare_runtimes(
            &config,
            &authority,
            &candidate,
            epoch,
            &mut Default::default(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("did not verify"), "{error}");

        // A runtime that cannot be written is reported, not fatal.
        let unwritable = config.publication_root.join("candidate-unwritable");
        // Permission failure is independent of archive size. Publish a real
        // recent-only candidate so this negative case does not rebuild the
        // large archive runtimes already checked above.
        let mut recent_only = options;
        recent_only.output = unwritable.clone();
        recent_only.archive_geometry = None;
        recent_only.recent_from = None;
        publication::publish(
            &recent_only,
            &*store.lock().await,
            BlockHash::from_internal_bytes([0; 32]),
        )
        .unwrap();
        for name in runtime_files(&unwritable).keys() {
            std::fs::remove_file(unwritable.join(name)).unwrap();
        }
        // The refusal fixture has never built or retained any runtime.
        for retained in [&config.initial_publication, &source.directory] {
            for name in runtime_files(retained).keys() {
                std::fs::remove_file(retained.join(name)).unwrap();
            }
        }
        std::fs::set_permissions(&unwritable, std::fs::Permissions::from_mode(0o555)).unwrap();
        let report = prepare_runtimes(
            &config,
            &authority,
            &unwritable,
            epoch,
            &mut Default::default(),
        )
        .await
        .unwrap();
        std::fs::set_permissions(&unwritable, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            !report["failures"].as_array().unwrap().is_empty(),
            "{report}"
        );
        assert_eq!(report["runtimes"], 0);
        assert_eq!(
            authority
                .0
                .runtime_preparation_failures
                .load(Ordering::Relaxed),
            report["failures"].as_array().unwrap().len() as u64
        );
        server.abort();
    }

    /// A five-block node whose journal and publication end at height two.
    struct Follow {
        _root: tempfile::TempDir,
        server: tokio::task::JoinHandle<()>,
        client: NodeClient,
        config: Config,
        store: Arc<tokio::sync::Mutex<EventStore>>,
        /// The node's chain; a test extends it to advance the tip.
        chain: Chain,
    }
    async fn follow_fixture() -> Follow {
        let root = tempfile::tempdir().unwrap();
        let chain: Chain = Arc::new(RwLock::new(Vec::new()));
        extend(&mut chain.write().unwrap(), 5, 1);
        let node = Arc::new(MockNode {
            chain: chain.clone(),
            pause_ancestor: AtomicBool::new(false),
            ancestor_started: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/", post(rpc)).with_state(node);
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let cookie = root.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let client = crate::ingest::zakura::connect(format!("http://{address}"), &cookie).unwrap();
        let journal_dir = root.path().join("journal");
        let mut journal = EventStore::open(
            &journal_dir,
            &hash(&chain.read().unwrap()[0]).to_display_hex(),
            0,
        )
        .unwrap();
        for h in 0..3 {
            journal
                .append_block(h, hash(&chain.read().unwrap()[h as usize]), &[])
                .unwrap();
        }
        journal.commit().unwrap();
        let initial = root.path().join("initial");
        let qualification = cfg!(feature = "qualification-tests");
        let options = PublishOptions {
            data_dir: journal_dir.clone(),
            output: initial.clone(),
            previous: None,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: qualification.then(|| "decoupled-32k-32k".into()),
            recent_from: qualification.then_some(2),
            zakura_rpc_url: String::new(),
            zakura_cookie: cookie.clone(),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
        };
        publication::publish_with(
            &options,
            &journal,
            BlockHash::from_internal_bytes([0; 32]),
            &follow_seal,
        )
        .unwrap();
        let publications = root.path().join("publications");
        std::fs::create_dir(&publications).unwrap();
        let config = Config {
            data_dir: journal_dir,
            publication_root: publications,
            initial_publication: initial,
            recent_from: if qualification { 2 } else { 0 },
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: "decoupled-32k-32k".into(),
            rpc_url: format!("http://{address}"),
            rpc_cookie: cookie,
            listen: address,
            source_sha: "fixture".into(),
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
            follow_journal: false,
            snapshot_dir: None,
        };
        let store = Arc::new(tokio::sync::Mutex::new(journal));
        Follow {
            _root: root,
            server,
            client,
            config,
            store,
            chain,
        }
    }
    /// The seal policy [`publish_follow`] publishes under.
    fn follow_seal(geometry: &Geometry) -> DecoupledPolicy {
        #[cfg(not(feature = "qualification-tests"))]
        {
            two_script_seal(geometry)
        }
        #[cfg(feature = "qualification-tests")]
        {
            DecoupledPolicy::for_geometry(geometry)
        }
    }
    impl Follow {
        /// The authority a publisher starting now would hold: the publication
        /// `active.json` names, checked against its map as at startup, or the
        /// initial one.
        fn authority(&self, shutdown: pir_pipeline::service::Shutdown) -> Authority {
            let saved = self.config.publication_root.join("active.json");
            let public = if saved.exists() {
                let saved: ActivePublication =
                    serde_json::from_slice(&std::fs::read(saved).unwrap()).unwrap();
                let loaded = read_public(saved.directory).unwrap();
                assert_eq!(loaded.publication.map_sha256, saved.map_sha256);
                loaded
            } else {
                read_public(self.config.initial_publication.clone()).unwrap()
            };
            Authority(Arc::new(Inner {
                cycle_duration: Default::default(),
                publication_freshness: Default::default(),
                telemetry: Default::default(),
                publication_attempts: AtomicU64::new(0),
                publication_failures: AtomicU64::new(0),
                last_publication: AtomicU64::new(0),
                active: RwLock::new(Arc::new(public)),
                withdrawn: AtomicBool::new(false),
                epoch: AtomicU64::new(0),
                distribution_revision: AtomicU64::new(1),
                distribution_slots: Arc::new(tokio::sync::Semaphore::new(4)),
                canonicality: None,
                invalidated_from: AtomicU64::new(u64::MAX),
                publication_gate: std::sync::Mutex::new(()),
                status: RwLock::new(serde_json::json!({})),
                observed: RwLock::new(BTreeMap::new()),
                prepared_set: std::sync::Mutex::new(None),
                shutdown,
                checked_runtimes: Default::default(),
                runtime_preparations: AtomicU64::new(0),
                runtime_preparation_failures: AtomicU64::new(0),
                validation: Default::default(),
                integrity_failed: AtomicBool::new(false),
                foreground: AtomicU64::new(0),
            }))
        }
        /// Brings the journal to the node's tip, height four.
        async fn ingest(&self, authority: &Authority) {
            let (notify, _) = tokio::sync::watch::channel(0);
            ingest_once(
                &self.config,
                &self.client,
                &self.store,
                authority,
                &mut OutputCache::new(100),
                &notify,
            )
            .await
            .unwrap();
            assert_eq!(self.store.lock().await.covered_through(), Some(4));
        }
    }

    /// A stop requested while the candidate is being published ends its
    /// preparation as a normal outcome: the build writes no map, nothing is
    /// activated and no failure is counted. A
    /// publisher started afterwards prepares and activates the same height.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stop_during_the_candidate_build_is_unchanged_and_a_restart_resumes() {
        static STOPPING: std::sync::LazyLock<pir_pipeline::service::Shutdown> =
            std::sync::LazyLock::new(Default::default);
        // Asked as the build starts, after preparation checked for a stop.
        fn stopping_seal(geometry: &Geometry) -> DecoupledPolicy {
            STOPPING.request();
            follow_seal(geometry)
        }
        let follow = follow_fixture().await;
        let config = &follow.config;
        let authority = follow.authority(STOPPING.clone());
        follow.ingest(&authority).await;
        let Input::Work(work) = acquire_history(config, &follow.client, &follow.store, &authority)
            .await
            .unwrap()
        else {
            panic!("new durable work expected");
        };
        let qualification = cfg!(feature = "qualification-tests");
        let prepared = prepare_candidate(
            config,
            &follow.client,
            &authority,
            work,
            stopping_seal,
            qualification.then(|| config.archive_geometry.clone()),
            qualification.then_some(config.recent_from),
        )
        .await;
        assert!(STOPPING.requested());
        assert!(
            matches!(prepared, Ok(Input::Unchanged)),
            "stopped as a normal outcome: {:?}",
            prepared.err()
        );
        assert_eq!(authority.0.active.read().unwrap().publication.height, 2);
        assert_eq!(authority.0.publication_failures.load(Ordering::Relaxed), 0);
        for name in ["active.json", "activation.json"] {
            assert!(!config.publication_root.join(name).exists(), "{name}");
        }
        let candidates: Vec<_> = std::fs::read_dir(&config.publication_root)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.is_dir())
            .collect();
        assert_eq!(candidates.len(), 1, "{candidates:?}");
        assert!(
            !candidates[0].join("shards.json").exists(),
            "no map written"
        );

        let restarted = follow.authority(Default::default());
        publish_follow(config, &follow.client, &follow.store, &restarted)
            .await
            .unwrap();
        assert_eq!(restarted.0.active.read().unwrap().publication.height, 4);
        follow.server.abort();
    }

    mod store;
}

#[cfg(all(test, feature = "fixtures"))]
#[path = "../../../../pir-publisher/tests/included/controller_distribution_tests.rs"]
pub(crate) mod distribution_tests;
