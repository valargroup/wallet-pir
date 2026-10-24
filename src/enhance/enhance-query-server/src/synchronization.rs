//! Enhance-owned replay floors and authority over the shared HTTPS transport.
use crate::{
    protocol::{digest, Manifest},
    publication::{
        AuthorityRequest, AuthorityResponse, Current, Inventory, Object, METADATA_LIMIT, PREFIX,
    },
};
use pir_publication::{ObjectCache, OBJECTS_PATH};
use pir_sync_manager::HttpsSource;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};
#[cfg(test)]
use {crate::publication::SOURCE_AUTHORITY_LIFETIME, pir_publication::SourceObservation};

pub type Error = pir_sync_manager::Error;

/// An expected recovery wait, proven from a well-formed publisher observation
/// rather than inferred from an HTTP status or message. It refuses serving
/// exactly like any other failure; only the polling cadence differs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Deferral {
    /// The publisher withdrew authority, for example before a chain rewind.
    Withdrawn,
    /// The publisher's last successful node check is absent or older than
    /// [`crate::publication::SOURCE_AUTHORITY_LIFETIME`].
    SourceExpired,
    /// A fresh observation shows authority moved while this pass prepared.
    AuthorityAdvanced,
}
impl std::fmt::Display for Deferral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Withdrawn => "publisher authority withdrawn",
            Self::SourceExpired => "Enhance source authority is absent or expired",
            Self::AuthorityAdvanced => "publisher authority advanced during the pass",
        })
    }
}
impl std::error::Error for Deferral {}
impl Deferral {
    pub fn of(error: &Error) -> Option<Self> {
        error.downcast_ref::<Self>().copied()
    }
}

/// An absent or aged node check. A timestamp ahead of this host's clock is not
/// an expected recovery state and stays an ordinary failure.
fn source_expired(source: pir_publication::SourceObservation, now: SystemTime) -> bool {
    let now_ms = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64);
    source == pir_publication::SourceObservation::NONE
        || now_ms
            .checked_sub(source.observed_unix_ms)
            .is_some_and(|age| {
                age > crate::publication::SOURCE_AUTHORITY_LIFETIME.as_millis() as u64
            })
}

/// Recovery polling while a pass is deferred. The source authority lifetime is
/// five seconds, so a one-second cadence adopts a republished view within one
/// fifth of a lease; each deferred pass costs one discovery and one `current`
/// metadata request (plus, for a pass that prepared material, one authority request).
pub const RECOVERY_POLL: Duration = Duration::from_secs(1);
/// The manager's ordinary cadence, kept for steady state and long deferrals.
pub const STEADY_POLL: Duration = Duration::from_secs(5);
/// A deferral longer than this (a lasting withdrawal or node outage) returns to
/// [`STEADY_POLL`], bounding publisher load without adding failure backoff.
pub const RECOVERY_WINDOW: Duration = Duration::from_secs(60);

/// Converts expected deferrals into successful, deferred passes for
/// [`pir_sync_manager::SyncManager`], and chooses the following wait. Genuine
/// failures pass through unchanged and keep the manager's exponential backoff.
#[derive(Default)]
pub struct Pacing {
    deferred_since: Option<tokio::time::Instant>,
    reason: Option<Deferral>,
    failed: bool,
}
impl Pacing {
    /// Record one complete pass. A deferral becomes `Ok(Some(reason))`; a
    /// genuine failure is returned unchanged and ends any recovery window.
    pub fn finish(
        &mut self,
        role: &'static str,
        result: Result<(), Error>,
    ) -> Result<Option<Deferral>, Error> {
        let reason = match result {
            Ok(()) => None,
            Err(error) => match Deferral::of(&error) {
                Some(reason) => Some(reason),
                None => {
                    self.failed = true;
                    self.deferred_since = None;
                    self.reason = None;
                    return Err(error);
                }
            },
        };
        self.failed = false;
        match reason {
            Some(reason) => {
                if self.reason != Some(reason) {
                    tracing::warn!(
                        role,
                        %reason,
                        "Enhance synchronization deferred; serving stays refused"
                    );
                }
                self.deferred_since
                    .get_or_insert_with(tokio::time::Instant::now);
            }
            None => {
                if let Some(since) = self.deferred_since.take() {
                    tracing::info!(
                        role,
                        waited_ms = since.elapsed().as_millis() as u64,
                        "Enhance synchronization recovered from deferral"
                    );
                }
            }
        }
        self.reason = reason;
        Ok(reason)
    }
    /// The manager supplies [`RECOVERY_POLL`] after a successful or deferred
    /// pass and its own backoff after a failure. Only a steady successful pass,
    /// or a deferral older than [`RECOVERY_WINDOW`], is lengthened to [`STEADY_POLL`].
    pub fn deadline(&self, minimum: tokio::time::Instant) -> tokio::time::Instant {
        let recovering = self
            .deferred_since
            .is_some_and(|since| since.elapsed() < RECOVERY_WINDOW);
        if self.failed || recovering {
            minimum
        } else {
            minimum.max(tokio::time::Instant::now() + STEADY_POLL)
        }
    }
}

pub(crate) fn peer_origin(origin: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(origin).map_err(|e| e.to_string())?;
    if url.scheme() != "http"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("expected private HTTP origin".into());
    }
    Ok(())
}

/// Tagged/flattened role documents deserialize through Serde's intermediate
/// representation, where JSON object keys remain strings. Decode domain keys
/// explicitly and reject aliases rather than depending on JSON's key coercion.
pub(crate) fn domain_map<'de, D, T>(
    deserializer: D,
) -> Result<std::collections::BTreeMap<u64, T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    let values = std::collections::BTreeMap::<String, T>::deserialize(deserializer)?;
    values
        .into_iter()
        .map(|(key, value)| {
            let id: u64 = key.parse().map_err(serde::de::Error::custom)?;
            if id.to_string() != key || id >= crate::protocol::MAX_QUERY_SHARDS {
                return Err(serde::de::Error::custom(
                    "domain key must be canonical and below the protocol ceiling",
                ));
            }
            Ok((id, value))
        })
        .collect()
}

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
    /// Renew only from a real observation of this exact manifest. The admission
    /// withdrawal marker survives renewal, so withdrawal and reauthorization
    /// cannot resurrect an already-admitted response.
    pub async fn fresh_for_release(&self, authority: &Authority) -> Result<Self, Error> {
        self.safety
            .permits_observation(&self.manifest, Some(&self.lease.observation.current))?;
        let fresh = authority.fresh(&self.manifest).await?;
        if !fresh.lease.continues(&self.lease) {
            return Err("publication withdrawn during request".into());
        }
        fresh.check()?;
        Ok(fresh)
    }
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
            .filter(|guard| guard.check().is_ok() && guard.lease.has_release_time())
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
            .filter(|guard| guard.check().is_ok() && guard.lease.has_release_time())
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

impl Lease {
    pub(crate) fn has_release_time(&self) -> bool {
        self.deadline.saturating_duration_since(Instant::now()) >= Duration::from_secs(1)
    }
    pub(crate) fn continues(&self, previous: &Self) -> bool {
        self.manifest == previous.manifest
            && self.observation.current.withdrawal_revision
                == previous.observation.current.withdrawal_revision
    }
    pub fn current(&self) -> &Current {
        &self.observation.current
    }
    pub fn from_response(
        response: AuthorityResponse,
        manifest: &Manifest,
        safety: &Safety,
    ) -> Result<Self, Error> {
        safety.observe(&response.current)?;
        let now = SystemTime::now();
        let remaining = response.remaining_at(now).map_err(|error| -> Error {
            // `observe` validated `current`; classify only well-formed refusals.
            if !response.current.eligible {
                Deferral::Withdrawn.into()
            } else if crate::protocol::canonical_hash(&response.authorized_manifest_sha256)
                && source_expired(response.source, now)
            {
                Deferral::SourceExpired.into()
            } else {
                error.into()
            }
        })?;
        if response.authorized_manifest_sha256 != digest(manifest) {
            return Err("publisher authorized another manifest".into());
        }
        safety.permits_observation(manifest, Some(&response.current))?;
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
        self.observation.remaining_at(SystemTime::now())?;
        safety.permits_observation(manifest, Some(&self.observation.current))
    }
}

pub struct Client {
    pub safety: Safety,
    source: HttpsSource,
    root: PathBuf,
    /// Verified publication objects; holding one confers no authority.
    cache: ObjectCache,
    cache_limit: u64,
}

impl Client {
    pub fn new(root: &Path, config: &Config, safety: Safety) -> Result<Self, Error> {
        remove_digest_part_transfers(&root.join("objects"))?;
        Ok(Self {
            safety,
            source: config.source(config.download_budget_bytes)?,
            root: root.into(),
            cache: ObjectCache::open(root.join("objects"))?,
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

    /// Renew `observed` after a pass prepared its material. A publisher
    /// refusal carries no typed reason, so it is a deferral only when a fresh,
    /// durable `current` observation proves authority moved; otherwise the
    /// original error (transport, malformed or refusal) is returned.
    pub async fn renew(&mut self, manifest: &Manifest, observed: &Lease) -> Result<Lease, Error> {
        let error = match self.authority(manifest).await {
            Ok(lease) if lease.continues(observed) => return Ok(lease),
            Ok(_) => return Err(Deferral::AuthorityAdvanced.into()),
            Err(error) if Deferral::of(&error).is_some() => return Err(error),
            Err(error) => error,
        };
        match self.current().await {
            Ok(fresh)
                if !fresh.current.eligible
                    || fresh.current.withdrawal_revision
                        != observed.current().withdrawal_revision
                    || fresh.current.manifest.as_ref() != Some(manifest) =>
            {
                Err(Deferral::AuthorityAdvanced.into())
            }
            _ => Err(error),
        }
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

    /// Remove every cached object outside `keep`, then refuse when the objects
    /// still retained exceed the cache budget. Downloaded bytes confer no
    /// authority.
    pub fn collect(&self, keep: &std::collections::BTreeSet<String>) -> Result<(), Error> {
        self.cache.collect_unreferenced(keep)?;
        if self.occupied_bytes()? > self.cache_limit {
            return Err("Enhance cache capacity reached".into());
        }
        Ok(())
    }

    /// The verified cached path of `object`, downloading it within the cache
    /// budget when it is absent. A cached file whose bytes differ is replaced.
    pub async fn object(&mut self, object: &Object) -> Result<PathBuf, Error> {
        object.validate()?;
        let reference = object.reference();
        let cache = self.cache.clone();
        let cached = reference.clone();
        match tokio::task::spawn_blocking(move || cache.verified_cached(&cached)).await? {
            Ok(Some(path)) => return Ok(path),
            Ok(None) => {}
            Err(_) => fs::remove_file(self.cache.object_path(&reference)?)?,
        }
        if self
            .occupied_bytes()?
            .checked_add(object.bytes)
            .is_none_or(|sum| sum > self.cache_limit)
        {
            return Err("Enhance cache capacity reached".into());
        }
        let mut partial = self.cache.begin_partial(&reference)?;
        let mut file = tokio::fs::File::from_std(partial.take_file()?);
        let response = self
            .source
            .request(&format!("{OBJECTS_PATH}{}", object.sha256), None)
            .await?;
        response
            .copy_to(
                &mut file,
                object.bytes,
                pir_sync_manager::Verification::Sha256(&object.sha256),
            )
            .await?;
        let written = file.into_std().await;
        Ok(tokio::task::spawn_blocking(move || partial.promote(written)).await??)
    }

    fn occupied_bytes(&self) -> Result<u64, Error> {
        Ok(
            fs::read_dir(self.cache.directory())?.try_fold(0u64, |sum, entry| {
                sum.checked_add(entry?.metadata()?.len())
                    .ok_or_else(|| std::io::Error::other("cache size overflow"))
            })?,
        )
    }
}

/// Remove transfers interrupted while partial objects were named
/// `<sha256>.part`. The shared cache neither creates nor collects that name, yet
/// such a file counts against the cache budget. Other files are left alone.
fn remove_digest_part_transfers(objects: &Path) -> std::io::Result<()> {
    let entries = match fs::read_dir(objects) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type()?.is_file()
            && name
                .strip_suffix(".part")
                .is_some_and(crate::protocol::canonical_hash)
        {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
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
            source: SourceObservation::starting_now(),
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
        expired.source.observed_unix_ms -= SOURCE_AUTHORITY_LIFETIME.as_millis() as u64 + 1;
        assert!(Lease::from_response(expired, &manifest, &safety).is_err());
        let mut future = initial.clone();
        future.source.observed_unix_ms += 60000;
        assert!(Lease::from_response(future, &manifest, &safety).is_err());
        let mut lease = Lease::from_response(initial, &manifest, &safety).unwrap();
        lease.deadline = Instant::now() - Duration::from_millis(1);
        assert!(lease.check(&manifest, &safety).is_err());
    }

    #[test]
    fn only_well_formed_recovery_states_are_deferrals_and_none_grants_a_lease() {
        let root = tempfile::tempdir().unwrap();
        let safety = Safety::open(root.path(), "https://publisher.example").unwrap();
        let manifest = manifest();
        let initial = response(&manifest);
        let deferral = |response: AuthorityResponse| {
            Deferral::of(
                &Lease::from_response(response, &manifest, &safety)
                    .err()
                    .unwrap(),
            )
        };
        let mut expired = initial.clone();
        expired.source.observed_unix_ms -= SOURCE_AUTHORITY_LIFETIME.as_millis() as u64 + 1;
        assert_eq!(deferral(expired), Some(Deferral::SourceExpired));
        let mut absent = initial.clone();
        absent.source = SourceObservation::NONE;
        assert_eq!(deferral(absent), Some(Deferral::SourceExpired));
        // Malformed or suspicious responses keep failure backoff.
        let mut future = initial.clone();
        future.source.observed_unix_ms += 60000;
        assert_eq!(deferral(future), None);
        let mut wrong = initial.clone();
        wrong.authorized_manifest_sha256 = "04".repeat(32);
        assert_eq!(deferral(wrong), None);
        let mut noncanonical = initial.clone();
        noncanonical.authorized_manifest_sha256 = "zz".into();
        assert_eq!(deferral(noncanonical), None);
        let mut invalid = initial.clone();
        invalid.current.version += 1;
        assert_eq!(deferral(invalid), None);
        // A valid withdrawal is a deferral, is durable, and closes older leases.
        let lease = Lease::from_response(initial.clone(), &manifest, &safety).unwrap();
        let mut withdrawn = initial.clone();
        withdrawn.current.revision += 1;
        withdrawn.current.withdrawal_revision = withdrawn.current.revision;
        withdrawn.current.eligible = false;
        withdrawn.authorized_manifest_sha256 = String::new();
        withdrawn.source = SourceObservation::NONE;
        assert_eq!(deferral(withdrawn.clone()), Some(Deferral::Withdrawn));
        assert!(lease.check(&manifest, &safety).is_err());
        drop(safety);
        let reopened = Safety::open(root.path(), "https://publisher.example").unwrap();
        assert!(reopened.permits(&manifest).is_err());
        // A replay of the pre-withdrawal response is a replay, not a deferral.
        assert_eq!(
            Deferral::of(
                &Lease::from_response(initial, &manifest, &reopened)
                    .err()
                    .unwrap()
            ),
            None
        );
    }

    /// Drives the real manager loop through scripted passes under controlled
    /// time and records when each pass started, in milliseconds.
    struct Scripted {
        outcomes: std::collections::VecDeque<Result<(), Error>>,
        started: Arc<Mutex<Vec<tokio::time::Instant>>>,
        pacing: Pacing,
    }
    #[derive(Debug)]
    struct Stop;
    impl std::fmt::Display for Stop {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("script finished")
        }
    }
    impl std::error::Error for Stop {}
    impl pir_sync_manager::SyncTask for Scripted {
        type Report = Option<Deferral>;
        type Error = Error;
        async fn synchronize(&mut self) -> Result<Option<Deferral>, Error> {
            self.started
                .lock()
                .unwrap()
                .push(tokio::time::Instant::now());
            let outcome = self.outcomes.pop_front().unwrap_or(Err(Stop.into()));
            self.pacing.finish("test", outcome)
        }
        async fn wait_until(&mut self, deadline: tokio::time::Instant) {
            tokio::time::sleep_until(self.pacing.deadline(deadline)).await;
        }
        fn terminal(&self, error: &Error) -> bool {
            error.downcast_ref::<Stop>().is_some()
        }
    }

    /// Whole seconds between consecutive pass starts (jitter is below one second).
    async fn gaps(outcomes: Vec<Result<(), Error>>) -> Vec<u64> {
        let started = Arc::new(Mutex::new(Vec::new()));
        let manager = pir_sync_manager::SyncManager::new(Scripted {
            outcomes: outcomes.into(),
            started: started.clone(),
            pacing: Pacing::default(),
        })
        .with_success_interval(RECOVERY_POLL);
        manager.run().await;
        let started = started.lock().unwrap();
        started
            .windows(2)
            .map(|pair| (pair[1] - pair[0]).as_secs())
            .collect()
    }

    fn deferred(reason: Deferral) -> Result<(), Error> {
        Err(reason.into())
    }
    fn failed(message: &str) -> Result<(), Error> {
        Err(message.to_string().into())
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_deferrals_poll_promptly_without_accumulating_failure_backoff() {
        // Before this change every deferral below was an ordinary error: the
        // waits would have been 10, 20, 40, 80 and 120 seconds.
        let observed = gaps(vec![
            deferred(Deferral::Withdrawn),
            deferred(Deferral::Withdrawn),
            deferred(Deferral::SourceExpired),
            deferred(Deferral::AuthorityAdvanced),
            deferred(Deferral::Withdrawn),
            Ok(()),
            Ok(()),
        ])
        .await;
        assert_eq!(observed, [1, 1, 1, 1, 1, 5, 5]);
    }

    #[tokio::test(start_paused = true)]
    async fn genuine_failures_keep_exponential_backoff_around_deferrals() {
        let observed = gaps(vec![
            failed("Enhance publisher refused current: 503 Service Unavailable"),
            failed("connection refused"),
            failed("expected value at line 1 column 1"),
            // A proven deferral resets the streak; it does not hide it.
            deferred(Deferral::Withdrawn),
            failed("connection refused"),
            Ok(()),
        ])
        .await;
        assert_eq!(observed, [10, 20, 40, 1, 10, 5]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lasting_deferral_returns_to_the_steady_cadence() {
        let mut outcomes = Vec::new();
        outcomes.resize_with(70, || deferred(Deferral::SourceExpired));
        let observed = gaps(outcomes).await;
        let recovering = observed.iter().take_while(|gap| **gap == 1).count();
        assert_eq!(recovering as u64, RECOVERY_WINDOW.as_secs());
        assert!(observed[recovering..].iter().all(|gap| *gap == 5));
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

    #[test]
    fn restart_removes_digest_part_transfers_but_keeps_other_files() {
        let root = tempfile::tempdir().unwrap();
        let objects = root.path().join("objects");
        fs::create_dir_all(&objects).unwrap();
        let interrupted = objects.join(format!("{}.part", "01".repeat(32)));
        fs::write(&interrupted, [1, 2]).unwrap();
        fs::write(objects.join("notes.part"), b"").unwrap();
        let config = Config {
            publisher_origin: "https://publisher.example".into(),
            trusted_ca: None,
            download_budget_bytes: METADATA_LIMIT * 3,
            cache_limit_bytes: 1,
        };
        let safety = Safety::open(root.path(), &config.publisher_origin).unwrap();
        let client = Client::new(root.path(), &config, safety).unwrap();
        assert!(!interrupted.exists());
        assert!(objects.join("notes.part").exists());
        client.collect(&Default::default()).unwrap();
    }
}
