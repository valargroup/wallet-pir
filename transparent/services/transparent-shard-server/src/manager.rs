//! Autonomous recent-worker synchronization. The private root has one owner;
//! completed bytes are reusable inputs, never saved serving authority.
use crate::{
    assignment::Assignment,
    bootstrap::{self, Config, Source},
    control::persist,
    live::{Command, LiveService, Publication},
    runtime::disk::ShippedRuntimes,
    service::{ReadinessMode, ServiceConfig, ServiceState},
    shardset::{LoadOptions, LoadScope, ShardSet},
};
use std::{collections::BTreeSet, path::Path, time::Duration};
use transparent_server_backend::distribution::{Current, Error};

pub struct Manager {
    config: Config,
    service: ServiceConfig,
    archive: Option<std::sync::Arc<crate::archive::Archive>>,
    source: Source,
    _lock: std::sync::Arc<std::fs::File>,
    live: Option<LiveService>,
    /// Only a successfully activated snapshot may supply a conditional token.
    current: Option<Current>,
    /// Exact-anchor negatives need their own revision floor: eligible Current
    /// alone does not encode them. Retain it before any fallible intent I/O.
    revocations: Revocations,
}
#[derive(Debug, serde::Serialize)]
pub struct Progress {
    pub unchanged: bool,
    pub revision: u64,
    pub downloaded_bytes: u64,
    pub downloaded_files: usize,
}
impl Manager {
    pub fn new(mut config: Config, mut service: ServiceConfig) -> Result<Self, Error> {
        let source = Source::new(&config)?;
        std::fs::create_dir_all(&config.directory)?;
        if !std::fs::symlink_metadata(&config.directory)?.is_dir() {
            return Err("provider root must be a real directory".into());
        }
        config.directory = std::fs::canonicalize(&config.directory)?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(config.directory.join("bootstrap.lock"))?;
        lock.try_lock()
            .map_err(|_| "another publication consumer owns this directory")?;
        let invalid: BTreeSet<String> =
            match std::fs::read(config.directory.join("active.invalid.json")) {
                Ok(bytes) => serde_json::from_slice(&bytes)?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeSet::new(),
                Err(e) => return Err(e.into()),
            };
        let mut intent = floor(&config.directory)?;
        if !invalid.is_subset(&intent.digests) {
            // If one of the two durability writes failed, conservatively bind
            // otherwise unrecorded owner revocations to the last observed source
            // revision before polling again. Equal-revision recovery stays shut.
            intent.revision = intent
                .revision
                .max(source.observed_revision(&config.directory)?);
            intent.digests.extend(invalid);
            persist_floor(&config.directory, &intent)?;
        }
        service.readiness = ReadinessMode::Warm;
        // No disk-cache restore/build path in this mode, including after restart.
        Ok(Self {
            config,
            service,
            archive: None,
            source,
            _lock: std::sync::Arc::new(lock),
            live: None,
            current: None,
            revocations: intent,
        })
    }
    pub fn with_archive(mut self, archive: std::sync::Arc<crate::archive::Archive>) -> Self {
        self.archive = Some(archive);
        self
    }
    pub fn live(&self) -> Option<LiveService> {
        self.live.clone()
    }
    pub async fn poll_once(&mut self) -> Result<Progress, Error> {
        self.source.reset_budget();
        if let Some(live) = &self.live {
            live.collect_memory().await?;
        }
        let current = poll_snapshot(
            &mut self.source,
            &self.config,
            self.live.as_ref(),
            self.current.as_ref(),
            &mut self.revocations,
        )
        .await?;
        let Some(current) = current else {
            return Ok(Progress {
                unchanged: true,
                revision: self.current.as_ref().unwrap().revision,
                downloaded_bytes: self.source.downloaded,
                downloaded_files: 0,
            });
        };
        if self.live.is_none() && self.config.directory.join("active.json").exists() {
            // A failed update does not discard the complete predecessor across
            // restart. Recovery still requires a fresh source snapshot and exact
            // retained anchors; loading this record alone never opens a listener.
            match self.recover(&current).await {
                Ok(live) => self.live = Some(live),
                Err(error) => tracing::warn!(%error, "saved publication not eligible for recovery"),
            }
        }
        // A recovered owner was not present for the first observation.
        if self.current.is_none() {
            apply_snapshot(
                &mut self.source,
                &self.config.directory,
                self.live.as_ref(),
                &current,
                &mut self.revocations,
            )
            .await?;
        }
        if !current.eligible() {
            return Err("publisher current publication is withdrawn or noncanonical".into());
        }
        let staged = bootstrap::stage(&self.config, &mut self.source, &current, true).await?;
        let publication = Publication {
            directory: staged.directory.clone(),
            assignment: Some(staged.directory.join("assignment.json")),
            map_sha256: staged.inventory.map_sha256.clone(),
            runtimes: Some(staged.directory.clone()),
        };
        let initial = self.live.is_none();
        let live = if let Some(live) = &self.live {
            live.clone()
        } else {
            let root = staged.directory.clone();
            let options = staged.options.clone();
            let set =
                tokio::task::spawn_blocking(move || ShardSet::open_with(&root, &options)).await??;
            let state = ServiceState::build_with_runtimes(
                set,
                self.service,
                None,
                Some(ShippedRuntimes::required(staged.directory.clone())),
            )?;
            let state = match &self.archive {
                Some(archive) => state.with_archive(archive.clone()),
                None => state,
            };
            state.spawn_prewarm().await?;
            if !state.is_warm() {
                return Err("required publication failed prewarm".into());
            }
            LiveService::new_required(
                state,
                publication.clone(),
                self.service,
                staged.options.clone(),
                self.config.directory.join("active.json"),
                self._lock.clone(),
            )?
        };
        if initial {
            live.fence_recovered(
                &combined_floor(&self.config.directory, &mut self.revocations)?.digests,
            )?;
        }
        let status = live.command(Command::Status).await?;
        let expected = status["active"]["map_sha256"]
            .as_str()
            .ok_or("owner has no predecessor")?
            .to_string();
        if !initial {
            live.command(Command::Prepare {
                expected: expected.clone(),
                publication: publication.clone(),
            })
            .await?;
        }
        let config = &self.config;
        let root = &config.directory;
        let revocations = &mut self.revocations;
        let source = &mut self.source;
        let checked_live = live.clone();
        let map_path = staged.directory.join("shards.json");
        // The owner obtains this check while it holds activation serialization.
        live.activate_checked(
            expected,
            publication.map_sha256.clone(),
            initial,
            || async {
                let result: Result<(), Error> = async {
                    let latest =
                        poll_snapshot(source, config, Some(&checked_live), None, revocations)
                            .await?
                            .ok_or("missing fresh current")?;
                    if latest != current || !latest.eligible() {
                        return Err("publication superseded before activation".into());
                    }
                    // A missed invalidation is not encoded in the latest range.
                    // Revalidate exact candidate anchors, including formerly revoked
                    // revisions, before permitting a newer recovery to clear them.
                    let map: transparent_filter::ShardMap =
                        serde_json::from_slice(&std::fs::read(&map_path)?)?;
                    let mut keep = BTreeSet::new();
                    for shard in &map.shards {
                        if !source
                            .anchor(&latest, shard.end_height, &shard.terminal_block_hash)
                            .await?
                        {
                            revoke_digests(
                                root,
                                &checked_live,
                                &latest,
                                BTreeSet::from([shard.manifest_digest.clone()]),
                                revocations,
                            )
                            .await?;
                            return Err("candidate anchor is noncanonical".into());
                        }
                        keep.insert(shard.manifest_digest.clone());
                    }
                    if latest.revision > combined_floor(root, revocations)?.revision {
                        checked_live.restore_verified(&keep)?;
                        let mut intent = combined_floor(root, revocations)?;
                        intent.digests.retain(|digest| !keep.contains(digest));
                        persist_floor(root, &intent)?;
                        *revocations = intent;
                    }
                    // Anchor checks cannot authorize a candidate across a source
                    // pointer/recovery change. Never use 304 for this final read.
                    let final_current =
                        poll_snapshot(source, config, Some(&checked_live), None, revocations)
                            .await?
                            .ok_or("missing final current")?;
                    if final_current != latest {
                        return Err("publication superseded during anchor checks".into());
                    }
                    Ok(())
                }
                .await;
                result.map_err(|e| e.to_string())
            },
        )
        .await?;
        persist(
            &staged.directory.join("verified-inventory.json"),
            &staged.inventory,
        )?;
        live.collect_memory().await?;
        self.live = Some(live);
        self.current = Some(current.clone());
        Ok(Progress {
            unchanged: false,
            revision: current.revision,
            downloaded_bytes: self.source.downloaded,
            downloaded_files: staged.downloaded_files,
        })
    }
    async fn recover(&mut self, current: &Current) -> Result<LiveService, Error> {
        let record = self.config.directory.join("active.json");
        let publication: Publication = serde_json::from_slice(&std::fs::read(&record)?)?;
        let directory = std::fs::canonicalize(&publication.directory)?;
        if !directory.starts_with(&self.config.directory) {
            return Err("saved publication escapes provider root".into());
        }
        let assignment = publication
            .assignment
            .as_ref()
            .ok_or("saved provider lacks assignment")?;
        let options = LoadOptions {
            scope: LoadScope::Assigned {
                worker_id: "provider".into(),
                assignment: std::sync::Arc::new(Assignment::load(assignment)?),
            },
            ..LoadOptions::whole(0)
        };
        let load_options = options.clone();
        let set =
            tokio::task::spawn_blocking(move || ShardSet::open_with(&directory, &load_options))
                .await??;
        if set.map_digest != publication.map_sha256
            || set.map.network != self.config.network
            || set.map.genesis_hash != self.config.genesis_hash
        {
            return Err("saved publication chain/map identity mismatch".into());
        }
        let state = ServiceState::build_with_runtimes(
            set,
            self.service,
            None,
            Some(ShippedRuntimes::required(
                publication
                    .runtimes
                    .clone()
                    .ok_or("saved provider lacks prepared runtimes")?,
            )),
        )?;
        let state = match &self.archive {
            Some(archive) => state.with_archive(archive.clone()),
            None => state,
        };
        state.spawn_prewarm().await?;
        let live = LiveService::new_required(
            state,
            publication.clone(),
            self.service,
            options,
            record,
            self._lock.clone(),
        )?;
        live.fence_recovered(
            &combined_floor(&self.config.directory, &mut self.revocations)?.digests,
        )?;
        let checked = live.clone();
        let source = &mut self.source;
        let config = &self.config;
        let revocations = &mut self.revocations;
        live.activate_checked(
            publication.map_sha256.clone(),
            publication.map_sha256,
            true,
            || async {
                let result: Result<(), Error> = async {
                    let latest = poll_snapshot(source, config, Some(&checked), None, revocations)
                        .await?
                        .ok_or("missing recovery current")?;
                    if &latest != current {
                        return Err("recovery snapshot superseded".into());
                    }
                    let final_current =
                        poll_snapshot(source, config, Some(&checked), None, revocations)
                            .await?
                            .ok_or("missing recovery recheck")?;
                    if latest != final_current {
                        return Err("recovery snapshot superseded during anchor checks".into());
                    }
                    Ok(())
                }
                .await;
                result.map_err(|e| e.to_string())
            },
        )
        .await?;
        Ok(live)
    }
    /// One sequential transfer at a time; bounded attempts, jitter and capped
    /// exponential backoff. An outage leaves the valid owner/router untouched.
    pub async fn run(mut self) {
        let mut failures = 0u32;
        loop {
            match self.poll_once().await {
                Ok(progress) => {
                    failures = 0;
                    tracing::info!(?progress, "publication synchronized");
                }
                Err(error) => {
                    failures = failures.saturating_add(1);
                    tracing::warn!(%error, "publication synchronization refused");
                }
            }
            let base = 5u64.saturating_mul(1u64 << failures.min(5)).min(120);
            let jitter = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| u64::from(d.subsec_nanos()) % 1000);
            tokio::time::sleep(Duration::from_millis(base * 1000 + jitter)).await;
        }
    }
}
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Revocations {
    revision: u64,
    digests: BTreeSet<String>,
}
fn read_floor(path: &Path) -> Result<Revocations, Error> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Revocations::default()),
        Err(e) => Err(e.into()),
    }
}
fn floor(root: &Path) -> Result<Revocations, Error> {
    let mut primary = read_floor(&root.join("revocation-floor.json"))?;
    let fallback = read_floor(&root.join("revocation-fallback.json"))?;
    primary.revision = primary.revision.max(fallback.revision);
    primary.digests.extend(fallback.digests);
    Ok(primary)
}
fn combined_floor(root: &Path, memory: &mut Revocations) -> Result<Revocations, Error> {
    let mut storage_error = None;
    for name in ["revocation-floor.json", "revocation-fallback.json"] {
        match read_floor(&root.join(name)) {
            Ok(durable) => {
                memory.revision = memory.revision.max(durable.revision);
                memory.digests.extend(durable.digests);
            }
            Err(error) => {
                storage_error.get_or_insert(error);
            }
        }
    }
    if let Some(error) = storage_error {
        return Err(error);
    }
    Ok(memory.clone())
}
fn persist_floor(root: &Path, intent: &Revocations) -> Result<(), Error> {
    // A failed primary write is still an error. Retain the SAME exact source
    // revision/digests through an independent atomic path before owner fencing;
    // a stale high-water cannot reconstruct a missed revision after restart.
    let primary = persist(&root.join("revocation-floor.json"), intent);
    let fallback = if primary.is_err() || root.join("revocation-fallback.json").exists() {
        persist(&root.join("revocation-fallback.json"), intent)
    } else {
        Ok(())
    };
    primary?;
    fallback?;
    Ok(())
}
async fn revoke(
    root: &Path,
    live: &LiveService,
    current: &Current,
    from: u64,
    memory: &mut Revocations,
) -> Result<(), Error> {
    let status = live.command(Command::Status).await?;
    let mut digests = BTreeSet::new();
    for revision in status["revisions"]
        .as_array()
        .ok_or("owner missing revisions")?
    {
        if revision["end_height"].as_u64().is_some_and(|h| h >= from) {
            digests.insert(
                revision["digest"]
                    .as_str()
                    .ok_or("owner missing digest")?
                    .into(),
            );
        }
    }
    revoke_digests(root, live, current, digests, memory).await
}
async fn revoke_digests(
    root: &Path,
    live: &LiveService,
    current: &Current,
    digests: BTreeSet<String>,
    memory: &mut Revocations,
) -> Result<(), Error> {
    memory.revision = memory.revision.max(current.revision);
    memory.digests.extend(digests.iter().cloned());
    let durable = (|| -> Result<(), Error> {
        let saved = combined_floor(root, memory);
        if let Err(error) = saved {
            // Preserve evidence in the existing fallback if readable; never
            // discard the in-process bound when any read or write also fails.
            if let Ok(fallback) = read_floor(&root.join("revocation-fallback.json")) {
                memory.revision = memory.revision.max(fallback.revision);
                memory.digests.extend(fallback.digests);
                let _ = persist(&root.join("revocation-fallback.json"), memory);
            }
            return Err(error);
        }
        let intent = memory.clone();
        persist_floor(root, &intent)
    })();
    // Storage failure cannot leave known orphaned material serving. The owner
    // still fences in memory and attempts its own durable write before we fail.
    let fenced = live.revoke_exact(digests).await;
    durable?;
    fenced?;
    Ok(())
}

async fn apply_snapshot(
    source: &mut Source,
    root: &Path,
    live: Option<&LiveService>,
    current: &Current,
    memory: &mut Revocations,
) -> Result<(), Error> {
    let Some(live) = live else {
        if !current.eligible() {
            memory.revision = memory.revision.max(current.revision);
            let intent = combined_floor(root, memory)?;
            persist_floor(root, &intent)?;
        }
        return Ok(());
    };
    let status = live.command(Command::Status).await?;
    let revisions = status["revisions"]
        .as_array()
        .ok_or("owner missing revisions")?;
    if let Some(from) = current.invalidated_from {
        if revisions
            .iter()
            .any(|s| s["end_height"].as_u64().is_some_and(|h| h >= from))
        {
            revoke(root, live, current, from, memory).await?;
        }
    }
    if !current.eligible() {
        let digests = revisions
            .iter()
            .filter(|s| {
                s["end_height"] == current.anchor_height
                    && s["terminal_block_hash"] == current.anchor_hash
            })
            .filter_map(|s| s["digest"].as_str().map(str::to_string))
            .collect();
        revoke_digests(root, live, current, digests, memory).await?;
    }
    // Revalidate all retained anchors after missed revisions, rather than infer
    // their validity from the publisher's most recent invalidation range.
    for revision in revisions {
        let height = revision["end_height"]
            .as_u64()
            .ok_or("invalid owner anchor")?;
        let hash = revision["terminal_block_hash"]
            .as_str()
            .ok_or("invalid owner hash")?;
        if !source.anchor(current, height, hash).await? {
            revoke_digests(
                root,
                live,
                current,
                BTreeSet::from([revision["digest"]
                    .as_str()
                    .ok_or("owner missing digest")?
                    .into()]),
                memory,
            )
            .await?;
        }
    }
    Ok(())
}

/// Every live poll boundary applies authenticated positive evidence even if a
/// control-record write fails. Transport, identity and replay failures never
/// become invalidation evidence; a 304 never grants new authority.
async fn poll_snapshot(
    source: &mut Source,
    config: &Config,
    live: Option<&LiveService>,
    previous: Option<&Current>,
    memory: &mut Revocations,
) -> Result<Option<Current>, Error> {
    let root = &config.directory;
    let Some((current, read_error)) = source
        .poll(root, previous, &config.network, &config.genesis_hash)
        .await?
    else {
        return Ok(None);
    };
    let recorded = combined_floor(root, memory);
    if current.revision < memory.revision {
        return Err("publisher replay below observed revocation revision".into());
    }
    let mut source_storage = Ok(());
    if current.revision == memory.revision && current.eligible() {
        match source.observed_revision(root) {
            Ok(revision) if revision < memory.revision => {
                return Err("equal revision lacks a durable exact source identity".into());
            }
            Err(error) => source_storage = Err(error),
            _ => {}
        }
    }
    let durable = source.observe(root, &current);
    let applied = apply_snapshot(source, root, live, &current, memory).await;
    if let Some(error) = read_error {
        return Err(error);
    }
    source_storage?;
    durable?;
    applied?;
    recorded?;
    Ok(Some(current))
}
