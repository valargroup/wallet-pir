//! Publisher-owned preparation and recovery, independent of consumer placement.
use crate::{
    distribution::Distribution,
    store::{JournalSnapshot, RecordJournal},
    zakura::ZakuraClient,
};
use enhance_query_server::{
    artifact,
    protocol::{self, Geometry, Lifecycle, Manifest, ShardState, MAX_QUERY_SHARDS},
    worker::Revocation,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Recovery {
    epoch: u64,
    domain_epochs: BTreeMap<u64, String>,
    revoked: BTreeSet<String>,
    sealed: BTreeMap<u64, (u64, String)>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Durable {
    version: u16,
    next_generation: u64,
    lifecycle: Lifecycle,
    recovery: Recovery,
}
impl Default for Durable {
    fn default() -> Self {
        Self {
            version: 1,
            next_generation: 1,
            lifecycle: Default::default(),
            recovery: Default::default(),
        }
    }
}
struct State {
    durable: Durable,
    poisoned: bool,
    preparing: Option<u64>,
    error: Option<String>,
}

/// A bounded work token pins the committed journal prefix and the withdrawal
/// observed before preparation. Acquisition may append concurrently. A rewind
/// invalidates this token before changing any journal bytes.
#[derive(Clone)]
pub struct Work {
    owner: String,
    snapshot: Arc<JournalSnapshot>,
    manifest: Manifest,
    withdrawal_revision: u64,
}
#[derive(Clone)]
pub struct Publisher {
    root: PathBuf,
    state: Arc<Mutex<State>>,
    distribution: Distribution,
    preparation: Arc<Semaphore>,
    incarnation: String,
    _owner: Arc<File>,
}
impl Publisher {
    pub fn open(root: &Path, rpc: ZakuraClient) -> Result<Self, String> {
        fs::create_dir_all(root).map_err(|e| e.to_string())?;
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("publisher.lock"))
            .map_err(|e| e.to_string())?;
        owner
            .try_lock()
            .map_err(|_| "Enhance publisher state already owned")?;
        let path = root.join("publisher.json");
        if !path.exists() && root.join("https/current.json").exists() {
            return Err(
                "publisher recovery state is missing; refusing to reconstruct lower floors".into(),
            );
        }
        let mut durable: Durable = if path.exists() {
            crate::publication::read_metadata(&path)?
        } else {
            Durable::default()
        };
        if durable.version != 1
            || durable.next_generation == 0
            || durable.lifecycle.next_id > MAX_QUERY_SHARDS
            || durable.recovery.domain_epochs.iter().any(|(id, value)| {
                *id >= MAX_QUERY_SHARDS
                    || value.parse::<u64>().map_or(true, |epoch| {
                        epoch.to_string() != *value || epoch > durable.recovery.epoch
                    })
            })
            || durable
                .recovery
                .revoked
                .iter()
                .any(|id| !protocol::canonical_hash(id))
            || durable
                .recovery
                .sealed
                .iter()
                .any(|(id, (_, hash))| *id >= MAX_QUERY_SHARDS || !protocol::canonical_hash(hash))
        {
            return Err("invalid Enhance publisher recovery state".into());
        }
        if durable.next_generation > 1 && !root.join("https/current.json").exists() {
            return Err(
                "publisher distribution state is missing; refusing lower publication floors".into(),
            );
        }
        persist(root, &durable)?;
        let distribution = Distribution::open(&root.join("https"), rpc)?;
        let floor = distribution.durable_current();
        durable.recovery.epoch = durable.recovery.epoch.max(floor.recovery_epoch);
        durable.recovery.revoked.extend(floor.revoked);
        for manifest in distribution.retained_manifests() {
            durable.next_generation = durable.next_generation.max(
                manifest
                    .generation
                    .checked_add(1)
                    .ok_or("generation exhausted")?,
            );
            for (id, value) in manifest.domain_recovery_epochs {
                let previous = durable
                    .recovery
                    .domain_epochs
                    .entry(id)
                    .or_insert_with(|| "0".into());
                let epoch: u64 = value.parse().map_err(|_| "invalid retained domain epoch")?;
                if epoch
                    > previous
                        .parse::<u64>()
                        .map_err(|_| "invalid domain epoch")?
                {
                    *previous = value;
                }
            }
        }
        persist(root, &durable)?;
        distribution.withdraw(&Revocation {
            recovery_epoch: durable.recovery.epoch,
            sessions: durable.recovery.revoked.clone(),
        })?;
        Ok(Self {
            root: root.into(),
            state: Arc::new(Mutex::new(State {
                durable,
                poisoned: false,
                preparing: None,
                error: None,
            })),
            distribution,
            preparation: Arc::new(Semaphore::new(1)),
            incarnation: format!("{:032x}", rand_id()),
            _owner: Arc::new(owner),
        })
    }
    fn save(&self, state: &mut State, next: Durable) -> Result<(), String> {
        if state.poisoned {
            return Err("Enhance publisher persistence failed; reopen required".into());
        }
        if let Err(error) = persist(&self.root, &next) {
            state.poisoned = true;
            self.distribution.poison();
            return Err(error);
        }
        state.durable = next;
        Ok(())
    }
    pub fn distribution(&self) -> &Distribution {
        &self.distribution
    }

    /// Durable revocation precedes journal rewind. Repeated delivery of the
    /// same unfinished rewind preserves its epoch and revoked-session set.
    pub fn revoke_after(&self, records: u64) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        let mut next = state.durable.clone();
        let first = records / (32768 * enhance_query_server::RECORDS_PER_ROW as u64);
        let deep = next.recovery.sealed.keys().any(|id| *id >= first);
        if deep {
            next.recovery.epoch = next
                .recovery
                .epoch
                .checked_add(1)
                .ok_or("recovery epoch exhausted")?;
            for id in first..MAX_QUERY_SHARDS {
                next.recovery
                    .domain_epochs
                    .insert(id, next.recovery.epoch.to_string());
            }
            for manifest in self.distribution.retained_manifests() {
                for shard in &manifest.coverage.shards {
                    if shard.id >= first {
                        next.recovery
                            .revoked
                            .insert(hex::encode(manifest.session_id(shard.id)?));
                    }
                }
            }
        }
        next.recovery.sealed.retain(|id, _| *id < first);
        self.save(&mut state, next)?;
        self.distribution.withdraw(&Revocation {
            recovery_epoch: state.durable.recovery.epoch,
            sessions: state.durable.recovery.revoked.clone(),
        })
    }
    /// Reserve only publisher identities. No worker or router address, capacity
    /// acknowledgment, or activation time participates in this work token.
    pub fn work(&self, journal: &RecordJournal) -> Result<Option<Work>, String> {
        let snapshot = journal.snapshot().map_err(|e| e.to_string())?;
        let anchor = snapshot.blocks().last().ok_or("empty Enhance journal")?;
        if self.state.lock().unwrap().poisoned {
            return Err("Enhance publisher persistence failed; reopen required".into());
        }
        let current = self.distribution.durable_current();
        if current.eligible
            && current.manifest.as_ref().is_some_and(|m| {
                m.anchor_height == anchor.height
                    && m.anchor_block_hash == anchor.hash
                    && m.coverage.records == snapshot.tree_size()
            })
        {
            return Ok(None);
        }
        let mut state = self.state.lock().unwrap();
        let mut next = state.durable.clone();
        let mut coverage = next
            .lifecycle
            .coverage(snapshot.tree_size(), Geometry::default())?;
        for shard in &mut coverage.shards {
            next.recovery
                .domain_epochs
                .entry(shard.id)
                .or_insert_with(|| "0".into());
            if shard.records == 32768 * enhance_query_server::RECORDS_PER_ROW as u64 {
                let end = (shard.id + 1) * 32768 * enhance_query_server::RECORDS_PER_ROW as u64;
                let crossing = snapshot
                    .blocks()
                    .iter()
                    .find(|b| b.first_position + b.action_count >= end)
                    .ok_or("missing completing block")?;
                if let Some((height, hash)) = next.recovery.sealed.get(&shard.id) {
                    if *height != crossing.height || *hash != crossing.hash {
                        return Err("sealed history changed without recovery fencing".into());
                    }
                    shard.state = ShardState::Sealed;
                } else if anchor.height.saturating_sub(crossing.height) >= 1000 {
                    next.recovery
                        .sealed
                        .insert(shard.id, (crossing.height, crossing.hash.clone()));
                    shard.state = ShardState::Sealed;
                }
            }
        }
        let generation = next.next_generation;
        next.next_generation = generation.checked_add(1).ok_or("generation exhausted")?;
        let epochs = coverage
            .shards
            .iter()
            .map(|s| (s.id, next.recovery.domain_epochs[&s.id].clone()))
            .collect();
        let manifest = Manifest {
            schema_version: protocol::SCHEMA_VERSION,
            protocol_revision: protocol::PROTOCOL_REVISION.into(),
            network: "main".into(),
            pool: "ironwood".into(),
            generation,
            placement_revision: 0,
            recovery_epoch: next.recovery.epoch,
            anchor_height: anchor.height,
            anchor_block_hash: anchor.hash.clone(),
            geometry: Geometry::default(),
            coverage,
            domain_recovery_epochs: epochs,
            sessions: Vec::new(),
            unit_identities: BTreeMap::new(),
        };
        self.save(&mut state, next)?;
        Ok(Some(Work {
            owner: self.incarnation.clone(),
            snapshot: Arc::new(snapshot),
            manifest,
            withdrawal_revision: current.withdrawal_revision,
        }))
    }
    pub async fn publish(&self, mut work: Work) -> Result<(), String> {
        if work.owner != self.incarnation {
            return Err("publication work belongs to another publisher".into());
        }
        let _permit = self
            .preparation
            .clone()
            .try_acquire_owned()
            .map_err(|_| "publication preparation already owned")?;
        if self.state.lock().unwrap().poisoned {
            return Err("Enhance publisher persistence failed; reopen required".into());
        }
        let current = self.distribution.durable_current();
        if current.withdrawal_revision != work.withdrawal_revision {
            return Err("publication withdrawn before preparation".into());
        }
        if current.eligible
            && current.manifest.as_ref().is_some_and(|m| {
                m.anchor_height == work.manifest.anchor_height
                    && m.anchor_block_hash == work.manifest.anchor_block_hash
                    && m.coverage.records == work.manifest.coverage.records
            })
        {
            return Ok(());
        }
        self.state.lock().unwrap().preparing = Some(work.manifest.anchor_height);
        let result = async {
            let root = self.distribution.root().to_path_buf();
            let coverage = work.manifest.coverage.clone();
            let epochs = work.manifest.domain_recovery_epochs.clone();
            let snapshot = work.snapshot;
            let prepared = tokio::task::spawn_blocking(move || {
                crate::publication::prepare(&root, coverage, &epochs, |start, count| {
                    snapshot
                        .read_records(start, count)
                        .map_err(|e| e.to_string())
                })
            })
            .await
            .map_err(|e| e.to_string())??;
            work.manifest.sessions = prepared
                .inventory()
                .domains
                .iter()
                .map(|d| d.reference())
                .collect::<Result<_, _>>()?;
            work.manifest.unit_identities = prepared
                .inventory()
                .domains
                .iter()
                .map(|d| (d.plan.shard.id, d.plan.units.clone()))
                .collect();
            work.manifest.validate()?;
            self.distribution
                .publish_if_unwithdrawn(work.manifest, prepared, work.withdrawal_revision)
                .await
        }
        .await;
        let mut state = self.state.lock().unwrap();
        state.preparing = None;
        state.error = result.as_ref().err().cloned();
        result
    }
    pub fn router(&self) -> axum::Router {
        axum::Router::new()
            .route("/v1/health", axum::routing::get(health))
            .route("/ready", axum::routing::get(ready))
            .route("/metrics", axum::routing::get(metrics))
            .with_state(self.clone())
    }
}
fn persist(root: &Path, state: &Durable) -> Result<(), String> {
    let bytes = serde_json::to_vec(state).map_err(|e| e.to_string())?;
    if bytes.len() as u64 > enhance_query_server::publication::METADATA_LIMIT {
        return Err("publisher state exceeds metadata limit".into());
    }
    artifact::write_atomic(root, "publisher.json", |file| file.write_all(&bytes))
        .and_then(|_| File::open(root)?.sync_all())
        .map_err(|e| e.to_string())
}
fn rand_id() -> u128 {
    // The incarnation scopes in-process, nonserializable work tokens. The
    // operating system supplies the same entropy used for request identities.
    use std::io::Read;
    let mut bytes = [0; 16];
    File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .expect("OS entropy");
    u128::from_le_bytes(bytes)
}
async fn health(
    axum::extract::State(p): axum::extract::State<Publisher>,
) -> axum::Json<serde_json::Value> {
    let current = p.distribution.durable_current();
    let state = p.state.lock().unwrap();
    axum::Json(
        serde_json::json!({"protocol": protocol::PROTOCOL_REVISION, "generation": current.manifest.as_ref().map(|m| m.generation),
        "anchor_height": current.manifest.as_ref().map(|m| m.anchor_height), "records": current.manifest.as_ref().map(|m| m.coverage.records),
        "eligible": current.eligible && !state.poisoned, "preparing_height": state.preparing, "last_error": state.error }),
    )
}
async fn ready(axum::extract::State(p): axum::extract::State<Publisher>) -> axum::http::StatusCode {
    match p.distribution.authority(None).await {
        Ok(observation) if observation.current.eligible => axum::http::StatusCode::OK,
        _ => axum::http::StatusCode::SERVICE_UNAVAILABLE,
    }
}
async fn metrics(
    axum::extract::State(p): axum::extract::State<Publisher>,
) -> impl axum::response::IntoResponse {
    let current = p.distribution.durable_current();
    let mut text = format!(
        "enhance_publication_eligible {}\n",
        u8::from(current.eligible)
    );
    if let Some(manifest) = current.manifest {
        text.push_str(&format!("enhance_published_anchor_height {}\nenhance_published_generation {}\nenhance_published_records {}\n", manifest.anchor_height, manifest.generation, manifest.coverage.records));
    }
    ([("content-type", "text/plain; version=0.0.4")], text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use enhance_query_server::types::{DatabaseId, ENHANCE_LAYOUT};
    fn journal(root: &Path) -> RecordJournal {
        let mut journal =
            RecordJournal::open(root.join("records"), DatabaseId::Enhance, ENHANCE_LAYOUT).unwrap();
        journal
            .append_block(
                1,
                format!("{:064x}", 1),
                &vec![vec![0u8; enhance_query_server::RECORD_BYTES]; 67],
            )
            .unwrap();
        journal
    }
    fn rpc(root: &Path, origin: &str) -> ZakuraClient {
        let cookie = root.join("cookie");
        fs::write(&cookie, "fixture:fixture").unwrap();
        ZakuraClient::from_cookie_file(origin, cookie).unwrap()
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_publication_needs_no_consumers_and_never_reuses_prewithdrawal_work() {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let root = tempfile::tempdir().unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let app = axum::Router::new().route("/", axum::routing::post(|axum::Json(request): axum::Json<serde_json::Value>| async move {
                assert_eq!(request["method"], "getblockhash");
                axum::Json(serde_json::json!({"id": request["id"], "error": null, "result": format!("{:064x}", request["params"][0].as_u64().unwrap())}))
            }));
            let node = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
            let rpc = rpc(root.path(), &origin);
            let publisher_root = root.path().join("publication");
            let publisher = Publisher::open(&publisher_root, rpc.clone()).unwrap();
            assert!(Publisher::open(&publisher_root, rpc.clone()).is_err());
            let journal = journal(root.path());
            let stale = publisher.work(&journal).unwrap().unwrap();
            let before = publisher.distribution.durable_current();
            publisher.revoke_after(0).unwrap();
            assert!(publisher.distribution.durable_current().withdrawal_revision > before.withdrawal_revision,
                "withdrawal must fence preparation even before the first eligible publication");
            assert!(publisher.publish(stale).await.is_err());
            let work = publisher.work(&journal).unwrap().unwrap();
            publisher.publish(work).await.unwrap();
            let first = publisher.distribution.durable_current();
            let manifest = first.manifest.as_ref().unwrap();
            assert!(first.eligible);
            let inventory: enhance_query_server::publication::Inventory = crate::publication::read_metadata(
                &publisher.distribution.root().join("inventories").join(format!("{}.json", first.inventory.as_ref().unwrap().sha256))).unwrap();
            assert!(inventory.supports(manifest).unwrap());
            crate::publication::verify(publisher.distribution.root(), inventory).unwrap();
            assert!(publisher.work(&journal).unwrap().is_none());
            let before_restart = publisher.distribution.durable_current();
            let old_work = publisher.work(&journal).unwrap();
            assert!(old_work.is_none());
            drop(publisher);
            let publisher = Publisher::open(&publisher_root, rpc).unwrap();
            assert!(!publisher.distribution.durable_current().eligible);
            assert!(publisher.distribution.durable_current().withdrawal_revision > before_restart.withdrawal_revision);
            let fresh = publisher.work(&journal).unwrap().unwrap();
            assert!(fresh.manifest.generation > before_restart.manifest.unwrap().generation);
            assert!(!publisher.distribution.durable_current().eligible, "reserving restart work cannot authorize service");
            publisher.revoke_after(0).unwrap();
            assert!(publisher.publish(fresh).await.is_err(), "restart work must also obey a later withdrawal");
            node.abort();
        }).await.expect("local publisher exceeded correctness budget");
    }
    #[tokio::test]
    async fn failed_publisher_persistence_cannot_grant_authority_after_storage_recovers() {
        let root = tempfile::tempdir().unwrap();
        let publisher_root = root.path().join("publication");
        let publisher =
            Publisher::open(&publisher_root, rpc(root.path(), "http://127.0.0.1:1")).unwrap();
        let journal = journal(root.path());
        fs::rename(
            publisher_root.join("publisher.json"),
            publisher_root.join("saved.json"),
        )
        .unwrap();
        fs::create_dir(publisher_root.join("publisher.json")).unwrap();
        assert!(publisher.work(&journal).is_err());
        assert!(publisher.distribution.authority(None).await.is_err());
        fs::remove_dir(publisher_root.join("publisher.json")).unwrap();
        fs::rename(
            publisher_root.join("saved.json"),
            publisher_root.join("publisher.json"),
        )
        .unwrap();
        assert!(publisher.work(&journal).is_err());
        assert!(publisher.distribution.authority(None).await.is_err());
    }
}
