//! One-shot HTTPS bootstrap verification. No persisted download is serving
//! authority. This entry point intentionally cannot start a wallet listener:
//! fresh publisher canonicality attestation is checked around verification.
//! Continuous reconciliation/activation belongs to the manager.
use zakura_pir_protocol::transparent::filter as transparent_filter;

use crate::{
    history::service::{ReadinessMode, ServiceConfig, ServiceState},
    routing::router::{plan, RosterEntry},
};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
use tokio::io::AsyncWriteExt;
use transparent_shared::publication::{self, Current, Error, Inventory};
use transparent_shared::{
    history::{
        assignment::{GeneratedBy, WorkerRole},
        shardset::{LoadOptions, LoadScope, ShardSet},
    },
    runtime::disk::ShippedRuntimes,
};

pub struct Config {
    pub publisher: String,
    pub network: String,
    pub genesis_hash: String,
    pub directory: PathBuf,
    pub cache_bytes: u64,
    /// Hard transfer budget for one bootstrap, including metadata.
    pub max_download_bytes: u64,
    /// Additional trusted roots, never a TLS verification bypass.
    pub roots: Vec<reqwest::Certificate>,
}
#[derive(Serialize)]
pub struct Report {
    pub current: Current,
    pub map_sha256: String,
    pub directory: PathBuf,
    pub downloaded_bytes: u64,
    pub downloaded_files: usize,
    pub archive_tables_downloaded: usize,
    /// Verified material only: neither readiness nor serving authority.
    pub serving: bool,
    pub runtime_preparation: serde_json::Value,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HighWater {
    origin: String,
    current: Current,
}

pub(crate) struct Source {
    origin: reqwest::Url,
    client: reqwest::Client,
    budget: u64,
    pub(crate) downloaded: u64,
    observed: Option<Current>,
}
impl Source {
    pub(crate) fn new(config: &Config) -> Result<Self, Error> {
        let origin = reqwest::Url::parse(&config.publisher)?;
        if origin.scheme() != "https"
            || origin.host_str().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
        {
            return Err("publisher must be an explicit HTTPS origin without credentials, path, query or fragment".into());
        }
        let mut builder = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(120));
        for root in &config.roots {
            builder = builder.add_root_certificate(root.clone());
        }
        Ok(Self {
            origin,
            client: builder.build()?,
            budget: config.max_download_bytes,
            downloaded: 0,
            observed: None,
        })
    }
    fn budget_error(&self) -> Error {
        format!(
            "prepared publication exceeds download budget ({} body bytes consumed, cap {})",
            self.downloaded, self.budget
        )
        .into()
    }
    async fn get(&mut self, suffix: &str) -> Result<reqwest::Response, Error> {
        if self.downloaded >= self.budget {
            return Err(self.budget_error());
        }
        let url = self
            .origin
            .join(&format!("{}{}", publication::PREFIX, suffix))?;
        let mut response = self
            .client
            .get(url)
            .header("cache-control", "no-cache")
            .send()
            .await?;
        if response.status() != reqwest::StatusCode::OK {
            // Refused response bytes consume the same attempt budget. Never
            // retry or reset accounting after a status or validation failure.
            while self.chunk(&mut response).await?.is_some() {}
            return Err(format!(
                "publisher refused distribution with HTTP {}",
                response.status()
            )
            .into());
        }
        Ok(response)
    }
    async fn chunk(
        &mut self,
        response: &mut reqwest::Response,
    ) -> Result<Option<axum::body::Bytes>, Error> {
        let chunk = response.chunk().await?;
        if let Some(bytes) = &chunk {
            self.downloaded = self
                .downloaded
                .checked_add(bytes.len() as u64)
                .ok_or("download budget overflow")?;
            if self.downloaded > self.budget {
                return Err(self.budget_error());
            }
        }
        Ok(chunk)
    }
    async fn small(&mut self, suffix: &str, limit: usize) -> Result<Vec<u8>, Error> {
        let mut response = self.get(suffix).await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = self.chunk(&mut response).await? {
            if chunk.len() > limit.saturating_sub(bytes.len()) {
                return Err("distribution metadata exceeds limit".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
    pub(crate) fn observed_revision(&self, directory: &std::path::Path) -> Result<u64, Error> {
        let mut revision = 0;
        for name in [
            "publisher-high-water.json",
            "publisher-high-water-fallback.json",
        ] {
            match std::fs::read(directory.join(name)) {
                Ok(bytes) => {
                    let saved: HighWater = serde_json::from_slice(&bytes)?;
                    if saved.origin != self.origin.as_str() {
                        return Err("publisher origin change".into());
                    }
                    revision = revision.max(saved.current.revision);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(revision)
    }
    // Storage errors are distinct from contradictory/replayed evidence. Once
    // this exclusively owned Source has a trusted full observation, it can
    // order a negative response despite an unreadable record, but may not use
    // that response to activate. Check EVERY readable record before returning.
    fn validate_observation(
        &self,
        directory: &std::path::Path,
        current: &Current,
    ) -> Result<Option<Error>, Error> {
        if self
            .observed
            .as_ref()
            .is_some_and(|saved| !current.succeeds(saved))
        {
            return Err("publisher replay or contradictory in-memory current record".into());
        }
        let mut storage_error = None;
        for name in [
            "publisher-high-water.json",
            "publisher-high-water-fallback.json",
        ] {
            let saved = match std::fs::read(directory.join(name)) {
                Ok(bytes) => match serde_json::from_slice::<HighWater>(&bytes) {
                    Ok(saved) => Some(saved),
                    Err(error) => {
                        storage_error.get_or_insert_with(|| error.into());
                        None
                    }
                },
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    storage_error.get_or_insert_with(|| error.into());
                    None
                }
            };
            if let Some(saved) = saved {
                if saved.origin != self.origin.as_str() || !current.succeeds(&saved.current) {
                    return Err(
                        "publisher origin change, replay or contradictory current record".into(),
                    );
                }
            }
        }
        if self.observed.is_none() {
            if let Some(error) = storage_error.take() {
                return Err(error);
            }
        }
        Ok(storage_error)
    }
    pub(crate) fn observe(
        &mut self,
        directory: &std::path::Path,
        current: &Current,
    ) -> Result<(), Error> {
        let storage_error = self.validate_observation(directory, current)?;
        // A failed durable write cannot erase a positive observation in this
        // process. The manager still applies its exact revocation to the owner.
        self.observed = Some(current.clone());
        if let Some(error) = storage_error {
            return Err(error);
        }
        // Keep the full source identity as well as its revision: an equal
        // revision must not substitute another inventory after a write failure.
        let saved = HighWater {
            origin: self.origin.to_string(),
            current: current.clone(),
        };
        let primary = crate::control::persist(&directory.join("publisher-high-water.json"), &saved);
        let fallback = directory.join("publisher-high-water-fallback.json");
        if primary.is_err() || fallback.exists() {
            crate::control::persist(&fallback, &saved)?;
        }
        primary?;
        Ok(())
    }

    async fn check_anchor(
        &mut self,
        directory: &std::path::Path,
        current: &Current,
    ) -> Result<(), Error> {
        let check: publication::AnchorCheck = serde_json::from_slice(
            &self
                .small(
                    &format!("anchors/{}/{}", current.anchor_height, current.anchor_hash),
                    16 << 10,
                )
                .await?,
        )?;
        if check.revision != current.revision || check.recovery_epoch != current.recovery_epoch {
            return Err("publication superseded during canonicality validation".into());
        }
        if check.schema != publication::SCHEMA
            || check.genesis_hash != current.genesis_hash
            || check.height != current.anchor_height
            || check.hash != current.anchor_hash
        {
            return Err("canonicality attestation has another identity/recovery snapshot".into());
        }
        if !check.canonical {
            let mut rejected = current.clone();
            rejected.eligibility = publication::Eligibility::Noncanonical;
            self.observe(directory, &rejected)?;
            return Err("candidate anchor is noncanonical".into());
        }
        Ok(())
    }

    pub(crate) fn reset_budget(&mut self) {
        self.downloaded = 0;
    }
    pub(crate) async fn poll(
        &mut self,
        directory: &std::path::Path,
        previous: Option<&Current>,
        network: &str,
        genesis: &str,
    ) -> Result<Option<(Current, Option<Error>)>, Error> {
        if self.downloaded >= self.budget {
            return Err(self.budget_error());
        }
        let mut request = self
            .client
            .get(
                self.origin
                    .join(&format!("{}current", publication::PREFIX))?,
            )
            .header("cache-control", "no-cache");
        if let Some(previous) = previous {
            request = request.header(
                "if-none-match",
                format!(
                    "\"{}\"",
                    publication::digest(&serde_json::to_vec(previous)?)
                ),
            );
        }
        let mut response = request.send().await?;
        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            if previous.is_none() {
                return Err("unexpected unchanged response".into());
            }
            return Ok(None);
        }
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = self.chunk(&mut response).await? {
            if chunk.len() > (16usize << 10).saturating_sub(bytes.len()) {
                return Err("current exceeds limit".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        if status != reqwest::StatusCode::OK {
            return Err(format!("publisher refused current with HTTP {status}").into());
        }
        let current: Current = serde_json::from_slice(&bytes)?;
        if current.schema != publication::SCHEMA
            || current.revision == 0
            || !publication::is_digest(&current.inventory_sha256)
            || !publication::is_digest(&current.anchor_hash)
            || current.network != network
            || current.genesis_hash != genesis
        {
            return Err("invalid publisher current chain identity".into());
        }
        // Persistence belongs to the manager after returning the authenticated,
        // replay-checked snapshot, so write failure cannot hide invalidation.
        let storage_error = self.validate_observation(directory, &current)?;
        // Carry the FIRST read failure across the manager handoff, even if a
        // second read succeeds. Negative evidence still fences; activation fails.
        Ok(Some((current, storage_error)))
    }
    pub(crate) async fn anchor(
        &mut self,
        current: &Current,
        height: u64,
        hash: &str,
    ) -> Result<bool, Error> {
        let check: publication::AnchorCheck = serde_json::from_slice(
            &self
                .small(&format!("anchors/{height}/{hash}"), 16 << 10)
                .await?,
        )?;
        if check.schema != publication::SCHEMA
            || check.revision != current.revision
            || check.recovery_epoch != current.recovery_epoch
            || check.genesis_hash != current.genesis_hash
            || check.height != height
            || check.hash != hash
        {
            return Err("canonicality attestation identity/snapshot superseded".into());
        }
        Ok(check.canonical)
    }
    pub(crate) async fn current(&mut self, directory: &std::path::Path) -> Result<Current, Error> {
        let current: Current = serde_json::from_slice(&self.small("current", 16 << 10).await?)?;
        if current.schema != publication::SCHEMA
            || current.revision == 0
            || !publication::is_digest(&current.inventory_sha256)
            || !publication::is_digest(&current.genesis_hash)
            || !publication::is_digest(&current.anchor_hash)
        {
            return Err("invalid publisher current record".into());
        }
        self.observe(directory, &current)?;
        if !current.eligible() {
            return Err("publisher current publication is withdrawn or noncanonical".into());
        }
        Ok(current)
    }
}

pub async fn verify(config: Config) -> Result<Report, Error> {
    let mut source = Source::new(&config)?;
    std::fs::create_dir_all(&config.directory)?;
    if !std::fs::symlink_metadata(&config.directory)?.is_dir() {
        return Err("bootstrap root must be a real directory".into());
    }
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(config.directory.join("bootstrap.lock"))?;
    lock.try_lock()
        .map_err(|_| "another bootstrap owns this directory")?;
    let current = source.current(&config.directory).await?;
    if current.network != config.network || current.genesis_hash != config.genesis_hash {
        return Err("prepared publication chain identity mismatch".into());
    }
    let staged = stage(&config, &mut source, &current, false).await?;
    let stage = staged.directory.clone();
    let inventory = staged.inventory;
    let options = staged.options;
    let result = async {
        let root = stage.clone();
        let set =
            tokio::task::spawn_blocking(move || ShardSet::open_with(&root, &options)).await??;
        let state = ServiceState::build_with_runtimes(
            set,
            ServiceConfig {
                cache_bytes: config.cache_bytes,
                readiness: ReadinessMode::Warm,
                ..ServiceConfig::default()
            },
            None,
            Some(ShippedRuntimes::required(stage.clone())),
        )?;
        state.spawn_prewarm().await?;
        if !state.is_warm() {
            return Err("required prepared publication failed production loader/self-check".into());
        }
        // HTTP current, not files or a last-success timestamp, is the bootstrap
        // freshness source. Supersession/withdrawal/outage cannot activate it.
        source.check_anchor(&config.directory, &current).await?;
        if source.current(&config.directory).await? != current {
            return Err("publication superseded during bootstrap".into());
        }
        let runtime_preparation = state.preparation();
        drop(state);
        crate::control::persist(&stage.join("verified-inventory.json"), &inventory)?;
        Ok(Report {
            current,
            map_sha256: inventory.map_sha256,
            directory: stage.clone(),
            downloaded_bytes: source.downloaded,
            downloaded_files: staged.downloaded_files,
            archive_tables_downloaded: 0,
            serving: false,
            runtime_preparation,
        })
    }
    .await;
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&stage);
    }
    result
}

pub(crate) struct Staged {
    pub directory: PathBuf,
    pub inventory: Inventory,
    pub options: LoadOptions,
    pub downloaded_files: usize,
}
pub(crate) async fn stage(
    config: &Config,
    source: &mut Source,
    current: &Current,
    cache: bool,
) -> Result<Staged, Error> {
    let raw = source
        .small(
            &format!("inventories/{}", current.inventory_sha256),
            16 << 20,
        )
        .await?;
    if publication::digest(&raw) != current.inventory_sha256 {
        return Err("inventory digest mismatch".into());
    }
    let inventory: Inventory = serde_json::from_slice(&raw)?;
    inventory.validate(&config.network, &config.genesis_hash)?;
    let total = inventory
        .artifacts
        .iter()
        .filter(|f| f.kind.local())
        .try_fold(source.downloaded, |n, f| n.checked_add(f.bytes))
        .ok_or("download budget overflow")?;
    if !cache && total > config.max_download_bytes {
        return Err("prepared publication exceeds download budget".into());
    }
    // New private staging tree; never join untrusted paths into an existing
    // publication or follow its symlinks. Failed trees remain non-authoritative.
    let stage = config.directory.join(format!(
        "staging-{}-{}",
        current.revision,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir(&stage)?;
    let result = async {
        let mut downloaded_files = 0;
        let map_artifact = inventory
            .artifacts
            .iter()
            .find(|f| f.path == "shards.json")
            .ok_or("missing map")?;
        for artifact in std::iter::once(map_artifact).chain(
            inventory
                .artifacts
                .iter()
                .filter(|f| f.kind.local() && f.path != "shards.json"),
        ) {
            let target = stage.join(&artifact.path);
            std::fs::create_dir_all(target.parent().ok_or("artifact has no parent")?)?;
            let cached = config.directory.join("objects").join(&artifact.sha256);
            let reuse = cache
                && std::fs::symlink_metadata(&cached).is_ok_and(|m| m.is_file())
                && publication::verify_file(&cached, artifact).is_ok();
            if reuse {
                tokio::fs::copy(&cached, &target).await?;
            } else {
                let mut output = tokio::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&target)
                    .await?;
                let mut response = source
                    .get(&format!("artifacts/{}", artifact.sha256))
                    .await?;
                let mut remaining = artifact.bytes;
                while let Some(chunk) = source.chunk(&mut response).await? {
                    if chunk.len() as u64 > remaining {
                        return Err("artifact exceeds declared length".into());
                    }
                    output.write_all(&chunk).await?;
                    remaining -= chunk.len() as u64;
                }
                if remaining != 0 {
                    return Err("artifact incomplete".into());
                }
                output.sync_all().await?;
                drop(output);
                downloaded_files += 1;
            }
            let file = artifact.clone();
            let path = target.clone();
            tokio::task::spawn_blocking(move || publication::verify_file(&path, &file)).await??;
            if cache && !reuse {
                std::fs::create_dir_all(cached.parent().ok_or("missing cache parent")?)?;
                // Completed objects alone are reusable. A partial copy never has
                // its digest name; every reuse hashes again.
                let partial = cached.with_extension("partial");
                tokio::fs::copy(&target, &partial).await?;
                std::fs::File::open(&partial)?.sync_all()?;
                std::fs::rename(&partial, &cached)?;
                std::fs::File::open(cached.parent().unwrap())?.sync_all()?;
            }
            if artifact.path == "shards.json" {
                let map: transparent_filter::ShardMap =
                    serde_json::from_slice(&std::fs::read(&target)?)?;
                inventory.validate_map(&map)?;
                let end = map.shards.last().ok_or("empty publication")?;
                if current.anchor_height != end.end_height
                    || current.anchor_hash != end.terminal_block_hash
                {
                    return Err("canonicality attestation names another publication anchor".into());
                }
            }
        }
        let map: transparent_filter::ShardMap =
            serde_json::from_slice(&std::fs::read(stage.join("shards.json"))?)?;
        map.validate_supported()?;
        if publication::digest(&serde_json::to_vec(&map)?) != inventory.map_sha256 {
            return Err("downloaded map has another publication identity".into());
        }
        // Reuse the production assignment planner, with an explicitly inert
        // archive row for cost/accounting only. Nothing here qualifies routes.
        let mut roster = vec![RosterEntry {
            id: "provider".into(),
            role: WorkerRole::RecentReplica,
            replica_group: Some("recent".into()),
            ssh_host: String::new(),
            upstream: "127.0.0.1:0".into(),
            cache_bytes: config.cache_bytes,
            archive_range: None,
        }];
        if inventory.recent_from_shard > 0 {
            roster.push(RosterEntry {
                id: "archive-metadata-only".into(),
                role: WorkerRole::ArchiveOwner,
                replica_group: None,
                ssh_host: String::new(),
                upstream: "unconfigured.invalid:0".into(),
                cache_bytes: u64::MAX,
                archive_range: None,
            });
        }
        let assignment = plan(
            &map,
            &inventory.map_sha256,
            &roster,
            inventory.recent_from_shard,
            0.15,
            GeneratedBy {
                tool: "HTTPS bootstrap verifier".into(),
                source_sha: None,
                generated_at: "not-serving".into(),
            },
        )?;
        let options = LoadOptions {
            scope: LoadScope::Assigned {
                assignment: Arc::new(assignment),
                worker_id: "provider".into(),
            },
            ..LoadOptions::whole(0)
        };
        if let LoadScope::Assigned { assignment, .. } = &options.scope {
            crate::control::persist(&stage.join("assignment.json"), assignment.as_ref())?;
        }
        if cache {
            sync_tree(&stage)?;
        }
        Ok(Staged {
            directory: stage.clone(),
            inventory,
            options,
            downloaded_files,
        })
    }
    .await;
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&stage);
    }
    result
}

// Persist child entries before their parents and before the owner's active
// pointer. Cached copies require the same durability as downloaded files.
fn sync_tree(path: &std::path::Path) -> Result<(), Error> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_tree(&entry.path())?;
        } else {
            std::fs::File::open(entry.path())?.sync_all()?;
        }
    }
    std::fs::File::open(path)?.sync_all()?;
    std::fs::File::open(path.parent().ok_or("stage has no parent")?)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod observation_tests {
    use super::*;
    use publication::Eligibility;

    #[test]
    fn damaged_high_water_requires_memory_and_never_hides_a_readable_contradiction() {
        // The HTTP tests cover propagation into live serving and activation.
        // Exercise both file locations and error kinds here without PIR setup.
        for failed in [
            "publisher-high-water.json",
            "publisher-high-water-fallback.json",
        ] {
            for unreadable in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let current = Current {
                    schema: publication::SCHEMA.into(),
                    revision: 10,
                    recovery_epoch: 0,
                    inventory_sha256: "11".repeat(32),
                    network: "main".into(),
                    genesis_hash: "00".repeat(32),
                    anchor_height: 2,
                    anchor_hash: "22".repeat(32),
                    eligibility: Eligibility::Canonical,
                    invalidated_from: None,
                };
                let mut source = Source::new(&Config {
                    publisher: "https://publisher.invalid".into(),
                    network: current.network.clone(),
                    genesis_hash: current.genesis_hash.clone(),
                    directory: root.path().into(),
                    cache_bytes: 0,
                    max_download_bytes: 1024,
                    roots: Vec::new(),
                })
                .unwrap();
                let path = root.path().join(failed);
                if unreadable {
                    std::fs::create_dir(&path).unwrap();
                } else {
                    std::fs::write(&path, b"{").unwrap();
                }
                assert!(
                    source.validate_observation(root.path(), &current).is_err(),
                    "startup must refuse a damaged record: {failed}, unreadable={unreadable}"
                );
                source.observed = Some(current.clone());
                let mut next = current.clone();
                next.revision += 1;
                next.eligibility = Eligibility::Withdrawn;
                let storage = source.validate_observation(root.path(), &next).unwrap();
                assert!(
                    storage.is_some(),
                    "trusted memory orders evidence but does not forgive storage"
                );
                assert!(source.observe(root.path(), &next).is_err());
                assert_eq!(source.observed.as_ref(), Some(&next));
                assert!(
                    source.validate_observation(root.path(), &current).is_err(),
                    "a failed write cannot erase the in-process revision floor"
                );
                let mut reenable = next.clone();
                reenable.eligibility = Eligibility::Canonical;
                assert!(
                    source.validate_observation(root.path(), &reenable).is_err(),
                    "same-revision recovery cannot erase a withdrawal"
                );

                let companion = if failed.contains("fallback") {
                    "publisher-high-water.json"
                } else {
                    "publisher-high-water-fallback.json"
                };
                let mut saved = HighWater {
                    origin: source.origin.to_string(),
                    current: next.clone(),
                };
                crate::control::persist(&root.path().join(companion), &saved).unwrap();
                assert!(source
                    .validate_observation(root.path(), &next)
                    .unwrap()
                    .is_some());
                saved.current.revision += 1;
                crate::control::persist(&root.path().join(companion), &saved).unwrap();
                assert!(
                    source.validate_observation(root.path(), &next).is_err(),
                    "a damaged record must not hide a newer readable companion"
                );
                saved.current = next.clone();
                saved.current.inventory_sha256 = "33".repeat(32);
                crate::control::persist(&root.path().join(companion), &saved).unwrap();
                assert!(
                    source.validate_observation(root.path(), &next).is_err(),
                    "equal revisions cannot substitute another inventory"
                );
                saved.current = next.clone();
                saved.origin = "https://another.invalid/".into();
                crate::control::persist(&root.path().join(companion), &saved).unwrap();
                assert!(
                    source.validate_observation(root.path(), &next).is_err(),
                    "a damaged record must not hide a readable origin mismatch"
                );
            }
        }
    }
}
