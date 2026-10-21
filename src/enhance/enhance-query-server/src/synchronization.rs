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
        let state = self.state.lock().unwrap();
        let current = state
            .floor
            .current
            .as_ref()
            .ok_or("no publisher authority")?;
        if state.poisoned || !current.eligible || manifest.recovery_epoch > current.recovery_epoch {
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
        safety.permits(manifest)?;
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
        safety.permits(manifest)
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

    pub fn begin(&mut self) {
        self.source.reset_budget();
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
