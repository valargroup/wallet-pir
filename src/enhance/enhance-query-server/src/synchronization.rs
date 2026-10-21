//! Enhance-owned replay floors and authority over the shared HTTPS transport.
use crate::{
    protocol::{digest, Manifest},
    publication::{
        AuthorityRequest, AuthorityResponse, Current, Inventory, Object, METADATA_LIMIT, PREFIX,
    },
};
use pir_sync_manager::HttpsSource;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncWriteExt;

pub type Error = pir_sync_manager::Error;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub publisher_origin: String,
    pub trusted_ca: Option<PathBuf>,
    /// Bounds all response bodies in one pass, including refused responses.
    pub download_budget_bytes: u64,
    /// Bounds completed and partial cached objects together.
    pub cache_limit_bytes: u64,
}

impl Config {
    pub fn source(&self, budget: u64) -> Result<HttpsSource, Error> {
        let roots = self
            .trusted_ca
            .as_ref()
            .map(|path| {
                reqwest::Certificate::from_pem(&fs::read(path)?)
                    .map_err(|e| std::io::Error::other(e.to_string()))
            })
            .transpose()?
            .into_iter()
            .collect::<Vec<_>>();
        HttpsSource::new(&self.publisher_origin, PREFIX, budget, &roots)
    }

    /// Check the publisher's service discovery before any Enhance state is
    /// opened. Uses the configured origin and trust roots; nothing is cached.
    pub async fn discover(&self) -> Result<(), Error> {
        self.source(0)?.discover().await?;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.download_budget_bytes < METADATA_LIMIT * 3 || self.cache_limit_bytes == 0 {
            return Err("Enhance transfer and cache budgets are too small".into());
        }
        if self
            .trusted_ca
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            return Err("Enhance trusted CA path must be absolute".into());
        }
        HttpsSource::new(
            &self.publisher_origin,
            PREFIX,
            self.download_budget_bytes,
            &[],
        )
        .map(drop)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Floor {
    publisher: String,
    current: Option<Current>,
}

struct State {
    floor: Floor,
    poisoned: bool,
    _lock: File,
}

/// Persisted floors survive restart; a successful disk write never grants a lease.
#[derive(Clone)]
pub struct Safety {
    root: PathBuf,
    state: Arc<Mutex<State>>,
}

impl Safety {
    pub fn open(root: &Path, publisher: &str) -> Result<Self, Error> {
        fs::create_dir_all(root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("sync.lock"))?;
        lock.try_lock()
            .map_err(|e| format!("Enhance consumer state already owned: {e}"))?;
        let path = root.join("sync.json");
        let floor: Floor = if path.exists() {
            use std::io::Read;
            let mut bytes = Vec::new();
            File::open(&path)?
                .take(METADATA_LIMIT + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() as u64 > METADATA_LIMIT {
                return Err("Enhance safety state exceeds metadata limit".into());
            }
            serde_json::from_slice(&bytes)?
        } else {
            Floor {
                publisher: publisher.into(),
                current: None,
            }
        };
        if floor.publisher != publisher {
            return Err("consumer state belongs to another publisher".into());
        }
        if let Some(current) = &floor.current {
            current.validate()?;
        }
        Ok(Self {
            root: root.into(),
            state: Arc::new(Mutex::new(State {
                floor,
                poisoned: false,
                _lock: lock,
            })),
        })
    }

    pub fn observe(&self, current: &Current) -> Result<(), Error> {
        current.validate()?;
        let mut state = self.state.lock().unwrap();
        if state.poisoned {
            return Err("Enhance safety state could not be persisted".into());
        }
        if let Some(previous) = &state.floor.current {
            if !current.succeeds(previous) {
                return Err("Enhance publication or recovery replay".into());
            }
            if current == previous {
                return Ok(());
            }
        }
        let floor = Floor {
            publisher: state.floor.publisher.clone(),
            current: Some(current.clone()),
        };
        let saved = crate::artifact::write_atomic(&self.root, "sync.json", |file| {
            serde_json::to_writer(file, &floor).map_err(std::io::Error::other)
        })
        .and_then(|_| File::open(&self.root)?.sync_all());
        if let Err(error) = saved {
            state.poisoned = true;
            return Err(error.into());
        }
        state.floor = floor;
        Ok(())
    }

    pub fn permits(&self, manifest: &Manifest) -> Result<(), String> {
        self.permits_observation(manifest, None)
    }

    fn permits_observation(
        &self,
        manifest: &Manifest,
        observed: Option<&Current>,
    ) -> Result<(), String> {
        let state = self.state.lock().unwrap();
        let current = state
            .floor
            .current
            .as_ref()
            .ok_or("no publisher authority")?;
        if state.poisoned
            || !current.eligible
            || manifest.recovery_epoch > current.recovery_epoch
            || observed.is_some_and(|seen| seen.withdrawal_revision != current.withdrawal_revision)
        {
            return Err("publisher authority withdrawn".into());
        }
        for shard in &manifest.coverage.shards {
            if current
                .revoked
                .contains(&hex::encode(manifest.session_id(shard.id)?))
            {
                return Err("noncanonical_session".into());
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Lease {
    manifest: String,
    observation: AuthorityResponse,
    deadline: Instant,
}

/// A response fence keeps the exact manifest's observation through evaluation.
#[derive(Clone)]
pub struct Guard {
    pub manifest: Manifest,
    lease: Lease,
    safety: Safety,
}
impl Guard {
    pub fn check(&self) -> Result<(), String> {
        self.lease.check(&self.manifest, &self.safety)
    }
}

/// On-demand authority shares replay floors with the sequential download task.
#[derive(Clone)]
pub struct Authority {
    pub safety: Safety,
    client: Arc<tokio::sync::Mutex<Client>>,
    leases: Arc<Mutex<std::collections::BTreeMap<String, Guard>>>,
}
impl Authority {
    pub fn new(root: &Path, config: &Config, safety: Safety) -> Result<Self, Error> {
        Ok(Self {
            client: Arc::new(tokio::sync::Mutex::new(Client::new(
                root,
                config,
                safety.clone(),
            )?)),
            safety,
            leases: Default::default(),
        })
    }
    pub fn install(&self, manifest: Manifest, lease: Lease) -> Result<Guard, Error> {
        let guard = Guard {
            manifest,
            lease,
            safety: self.safety.clone(),
        };
        guard.check()?;
        let mut leases = self.leases.lock().unwrap();
        leases.retain(|_, lease| lease.check().is_ok());
        if leases.len() >= 2 * crate::protocol::RETAINED_GENERATIONS
            && !leases.contains_key(&digest(&guard.manifest))
        {
            return Err("Enhance authority retention capacity reached".into());
        }
        leases.insert(digest(&guard.manifest), guard.clone());
        Ok(guard)
    }
    pub async fn fresh(&self, manifest: &Manifest) -> Result<Guard, Error> {
        let id = digest(manifest);
        if let Some(guard) = self
            .leases
            .lock()
            .unwrap()
            .get(&id)
            .filter(|guard| guard.check().is_ok())
            .cloned()
        {
            return Ok(guard);
        }
        let mut client = self.client.lock().await;
        if let Some(guard) = self
            .leases
            .lock()
            .unwrap()
            .get(&id)
            .filter(|guard| guard.check().is_ok())
            .cloned()
        {
            return Ok(guard);
        }
        client.begin().await?;
        let lease = client.authority(manifest).await?;
        self.install(manifest.clone(), lease)
    }
    pub fn check(&self, manifest: &Manifest) -> Result<(), String> {
        self.leases
            .lock()
            .unwrap()
            .get(&digest(manifest))
            .ok_or("no fresh Enhance authority")?
            .check()
    }
}

pub fn now_ms() -> Result<u64, Error> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

impl Lease {
    pub fn current(&self) -> &Current {
        &self.observation.current
    }
    pub fn from_response(
        response: AuthorityResponse,
        manifest: &Manifest,
        safety: &Safety,
    ) -> Result<Self, Error> {
        safety.observe(&response.current)?;
        let now = now_ms()?;
        response.fresh(now)?;
        if response.authorized_manifest_sha256 != digest(manifest) {
            return Err("publisher authorized another manifest".into());
        }
        safety.permits_observation(manifest, Some(&response.current))?;
        let remaining = pir_control::CONTROL_WATCHDOG
            .saturating_sub(Duration::from_millis(now - response.observed_unix_ms));
        Ok(Self {
            manifest: digest(manifest),
            observation: response,
            deadline: Instant::now() + remaining,
        })
    }

    pub fn check(&self, manifest: &Manifest, safety: &Safety) -> Result<(), String> {
        if self.manifest != digest(manifest) || Instant::now() > self.deadline {
            return Err("Enhance source authority expired".into());
        }
        self.observation
            .fresh(now_ms().map_err(|e| e.to_string())?)?;
        safety.permits_observation(manifest, Some(&self.observation.current))
    }
}

pub struct Client {
    pub safety: Safety,
    source: HttpsSource,
    root: PathBuf,
    cache_limit: u64,
}

impl Client {
    pub fn new(root: &Path, config: &Config, safety: Safety) -> Result<Self, Error> {
        fs::create_dir_all(root.join("objects"))?;
        Ok(Self {
            safety,
            source: config.source(config.download_budget_bytes)?,
            root: root.into(),
            cache_limit: config.cache_limit_bytes,
        })
    }

    /// Start one pass or on-demand authority refresh: fresh service discovery
    /// before any product request, then a fresh download budget. A confirmed
    /// version mismatch is returned as a typed [`pir_sync_manager::DiscoveryError`].
    pub async fn begin(&mut self) -> Result<(), Error> {
        self.source.discover().await?;
        self.source.reset_budget();
        Ok(())
    }
    pub fn root(&self) -> &Path {
        &self.root
    }

    async fn metadata(&mut self, suffix: &str, body: Option<Vec<u8>>) -> Result<Vec<u8>, Error> {
        let mut response = match body {
            Some(body) => self.source.post_json(suffix, body).await?,
            None => self.source.request(suffix, None).await?,
        };
        if !response.status().is_success() {
            let status = response.status();
            response.discard().await?;
            return Err(format!("Enhance publisher refused {suffix}: {status}").into());
        }
        response.read_limited(METADATA_LIMIT as usize).await
    }

    pub async fn current(&mut self) -> Result<AuthorityResponse, Error> {
        let response: AuthorityResponse =
            serde_json::from_slice(&self.metadata("current", None).await?)?;
        // A withdrawal must be durable even though it cannot grant a lease.
        self.safety.observe(&response.current)?;
        Ok(response)
    }

    pub async fn authority(&mut self, manifest: &Manifest) -> Result<Lease, Error> {
        let body = serde_json::to_vec(&AuthorityRequest {
            manifest_sha256: digest(manifest),
        })?;
        let response = serde_json::from_slice(&self.metadata("authority", Some(body)).await?)?;
        Lease::from_response(response, manifest, &self.safety)
    }

    pub async fn inventory(
        &mut self,
        object: &Object,
        manifest: &Manifest,
    ) -> Result<Inventory, Error> {
        object.validate()?;
        if object.bytes > METADATA_LIMIT {
            return Err("inventory exceeds metadata limit".into());
        }
        let bytes = self
            .metadata(&format!("inventories/{}", object.sha256), None)
            .await?;
        if bytes.len() as u64 != object.bytes
            || hex::encode(Sha256::digest(&bytes)) != object.sha256
        {
            return Err("inventory length or checksum mismatch".into());
        }
        let inventory: Inventory = serde_json::from_slice(&bytes)?;
        if !inventory.supports(manifest)? {
            return Err("inventory does not support manifest".into());
        }
        Ok(inventory)
    }

    /// Remove only owned object names; downloaded bytes confer no authority.
    pub fn collect(&self, keep: &std::collections::BTreeSet<String>) -> Result<(), Error> {
        for entry in fs::read_dir(self.root.join("objects"))? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type()?.is_file()
                && ((crate::protocol::canonical_hash(&name) && !keep.contains(&name))
                    || name.ends_with(".part"))
            {
                fs::remove_file(entry.path())?;
            }
        }
        let occupied = fs::read_dir(self.root.join("objects"))?.try_fold(0u64, |sum, entry| {
            sum.checked_add(entry?.metadata()?.len())
                .ok_or_else(|| std::io::Error::other("cache size overflow"))
        })?;
        if occupied > self.cache_limit {
            return Err("Enhance cache capacity reached".into());
        }
        Ok(())
    }

    pub async fn object(&mut self, object: &Object) -> Result<PathBuf, Error> {
        object.validate()?;
        let root = self.root.clone();
        let checked = object.clone();
        if tokio::task::spawn_blocking(move || checked.verify(&root))
            .await?
            .is_ok()
        {
            return Ok(object.path(&self.root)?);
        }
        let path = object.path(&self.root)?;
        if path.exists() {
            fs::remove_file(&path)?;
        }
        let occupied = fs::read_dir(self.root.join("objects"))?.try_fold(0u64, |sum, entry| {
            let size = entry?.metadata()?.len();
            sum.checked_add(size)
                .ok_or_else(|| std::io::Error::other("cache size overflow"))
        })?;
        if occupied
            .checked_add(object.bytes)
            .is_none_or(|sum| sum > self.cache_limit)
        {
            return Err("Enhance cache capacity reached".into());
        }
        let temporary = self
            .root
            .join("objects")
            .join(format!("{}.part", object.sha256));
        let result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .await?;
            let mut response = self
                .source
                .request(&format!("objects/{}", object.sha256), None)
                .await?;
            if !response.status().is_success() {
                let status = response.status();
                response.discard().await?;
                return Err::<(), Error>(
                    format!("publication object unavailable: {status}").into(),
                );
            }
            let mut length = 0u64;
            let mut hash = Sha256::new();
            while let Some(chunk) = response.chunk().await? {
                length = length
                    .checked_add(chunk.len() as u64)
                    .ok_or("object size overflow")?;
                if length > object.bytes {
                    return Err("publication object exceeds declared length".into());
                }
                hash.update(&chunk);
                file.write_all(&chunk).await?;
            }
            if length != object.bytes || hex::encode(hash.finalize()) != object.sha256 {
                return Err("publication object length or checksum mismatch".into());
            }
            file.sync_all().await?;
            drop(file);
            tokio::fs::rename(&temporary, &path).await?;
            File::open(self.root.join("objects"))?.sync_all()?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result?;
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Manifest {
        let geometry = crate::protocol::Geometry::default();
        let coverage = crate::protocol::Lifecycle::default()
            .coverage(1, geometry)
            .unwrap();
        let plan =
            crate::runtime::plan(coverage.shards[0].clone(), crate::test_support::records).unwrap();
        Manifest {
            recovery_epoch: 0,
            placement_revision: 1,
            domain_recovery_epochs: [(0, "0".into())].into(),
            schema_version: crate::protocol::SCHEMA_VERSION,
            protocol_revision: crate::protocol::PROTOCOL_REVISION.into(),
            network: "main".into(),
            pool: "ironwood".into(),
            generation: 1,
            anchor_height: 1,
            anchor_block_hash: "01".repeat(32),
            geometry,
            coverage,
            sessions: vec![crate::protocol::SessionRef {
                shard_id: 0,
                public_params_sha256: "02".repeat(32),
                parameter_id: crate::protocol::parameter_id(plan.shard.logical_rows).unwrap(),
            }],
            unit_identities: [(0, plan.units)].into(),
        }
    }

    fn response(manifest: &Manifest) -> AuthorityResponse {
        AuthorityResponse {
            current: Current {
                version: crate::publication::VERSION,
                revision: 1,
                withdrawal_revision: 1,
                recovery_epoch: 0,
                revoked: Default::default(),
                eligible: true,
                manifest: Some(manifest.clone()),
                inventory: Some(Object {
                    sha256: "03".repeat(32),
                    bytes: 1,
                }),
            },
            authorized_manifest_sha256: digest(manifest),
            observed_unix_ms: now_ms().unwrap(),
        }
    }

    #[test]
    fn recovery_floors_survive_restart_and_cannot_be_replayed_or_rebound() {
        let root = tempfile::tempdir().unwrap();
        let safety = Safety::open(root.path(), "https://publisher.example").unwrap();
        assert!(Safety::open(root.path(), "https://publisher.example").is_err());
        let manifest = manifest();
        let initial = response(&manifest);
        let lease = Lease::from_response(initial.clone(), &manifest, &safety).unwrap();
        lease.check(&manifest, &safety).unwrap();
        let mut revoked = initial.current.clone();
        revoked.revision += 1;
        revoked.recovery_epoch += 1;
        revoked
            .revoked
            .insert(hex::encode(manifest.session_id(0).unwrap()));
        safety.observe(&revoked).unwrap();
        assert!(
            lease.check(&manifest, &safety).is_err(),
            "revocation closes an unexpired lease"
        );
        drop(safety);
        let reopened = Safety::open(root.path(), "https://publisher.example").unwrap();
        assert!(reopened.observe(&initial.current).is_err());
        let mut erased = revoked.clone();
        erased.revision += 1;
        erased.revoked.clear();
        assert!(reopened.observe(&erased).is_err());
        drop(reopened);
        assert!(Safety::open(root.path(), "https://another.example").is_err());
    }

    #[test]
    fn missed_withdrawal_closes_old_authority_without_revoking_unchanged_sessions() {
        let root = tempfile::tempdir().unwrap();
        let safety = Safety::open(root.path(), "https://publisher.example").unwrap();
        let manifest = manifest();
        let initial = response(&manifest);
        let lease = Lease::from_response(initial.clone(), &manifest, &safety).unwrap();
        let mut advanced = initial.clone();
        advanced.current.revision += 1;
        safety.observe(&advanced.current).unwrap();
        lease.check(&manifest, &safety).unwrap();

        // The consumer sees republication without ever seeing the ineligible pointer.
        advanced.current.revision += 2;
        advanced.current.withdrawal_revision = advanced.current.revision - 1;
        safety.observe(&advanced.current).unwrap();
        assert!(lease.check(&manifest, &safety).is_err());
        assert!(Lease::from_response(initial, &manifest, &safety).is_err());
        let renewed = Lease::from_response(advanced.clone(), &manifest, &safety).unwrap();
        renewed.check(&manifest, &safety).unwrap();
        let mut erased = advanced.current.clone();
        erased.revision += 1;
        erased.withdrawal_revision -= 1;
        assert!(safety.observe(&erased).is_err());
        drop(safety);
        let reopened = Safety::open(root.path(), "https://publisher.example").unwrap();
        assert!(reopened.observe(&erased).is_err());
        assert!(lease.check(&manifest, &reopened).is_err());

        let mut withdrawn = advanced.current.clone();
        withdrawn.revision += 1;
        withdrawn.withdrawal_revision = withdrawn.revision;
        withdrawn.eligible = false;
        reopened.observe(&withdrawn).unwrap();
        withdrawn.revision += 1;
        withdrawn.eligible = true;
        reopened.observe(&withdrawn).unwrap();
        assert!(
            renewed.check(&manifest, &reopened).is_err(),
            "observed withdrawal cannot resurrect old authority either"
        );
    }

    #[test]
    fn disk_failure_closes_admission_even_if_storage_recovers() {
        let root = tempfile::tempdir().unwrap();
        let safety = Safety::open(root.path(), "https://publisher.example").unwrap();
        let manifest = manifest();
        let initial = response(&manifest);
        let lease = Lease::from_response(initial.clone(), &manifest, &safety).unwrap();
        fs::rename(
            root.path().join("sync.json"),
            root.path().join("saved.json"),
        )
        .unwrap();
        fs::create_dir(root.path().join("sync.json")).unwrap();
        let mut advanced = initial.current;
        advanced.revision += 1;
        assert!(safety.observe(&advanced).is_err());
        fs::remove_dir(root.path().join("sync.json")).unwrap();
        fs::rename(
            root.path().join("saved.json"),
            root.path().join("sync.json"),
        )
        .unwrap();
        assert!(lease.check(&manifest, &safety).is_err());
        assert!(safety.observe(&advanced).is_err());
    }

    #[test]
    fn authority_cannot_bind_other_material_or_gain_time_from_a_heartbeat() {
        let root = tempfile::tempdir().unwrap();
        let safety = Safety::open(root.path(), "https://publisher.example").unwrap();
        let manifest = manifest();
        let initial = response(&manifest);
        let mut wrong = initial.clone();
        wrong.authorized_manifest_sha256 = "04".repeat(32);
        assert!(Lease::from_response(wrong, &manifest, &safety).is_err());
        let mut expired = initial.clone();
        expired.observed_unix_ms -= 5001;
        assert!(Lease::from_response(expired, &manifest, &safety).is_err());
        let mut future = initial.clone();
        future.observed_unix_ms += 60000;
        assert!(Lease::from_response(future, &manifest, &safety).is_err());
        let mut lease = Lease::from_response(initial, &manifest, &safety).unwrap();
        lease.deadline = Instant::now() - Duration::from_millis(1);
        assert!(lease.check(&manifest, &safety).is_err());
    }

    #[test]
    fn worker_static_configuration_checks_do_not_read_trusted_roots() {
        let value = serde_json::json!({ "role": "worker", "listen": "127.0.0.1:9000", "data_dir": "/unused/worker",
            "synchronization": { "publisher_origin": "https://publisher.example", "trusted_ca": "/unused/ca.pem",
                "download_budget_bytes": 8388608, "cache_limit_bytes": 8388608, "shards": [0] }});
        let role: crate::service::ServingRole = serde_json::from_value(value.clone()).unwrap();
        role.validate().unwrap();
        let mut invalid = value;
        invalid["synchronization"]["publisher_origin"] =
            serde_json::json!("http://publisher.example");
        let role: crate::service::ServingRole = serde_json::from_value(invalid).unwrap();
        assert!(role.validate().is_err());
    }

    #[test]
    fn a_smaller_cache_budget_also_limits_already_cached_objects() {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            publisher_origin: "https://publisher.example".into(),
            trusted_ca: None,
            download_budget_bytes: METADATA_LIMIT * 3,
            cache_limit_bytes: 1,
        };
        let safety = Safety::open(root.path(), &config.publisher_origin).unwrap();
        let client = Client::new(root.path(), &config, safety).unwrap();
        let name = "01".repeat(32);
        fs::write(root.path().join("objects").join(&name), [1, 2]).unwrap();
        assert!(client.collect(&[name].into()).is_err());
        client.collect(&Default::default()).unwrap();
    }
}
