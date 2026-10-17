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
    source: Source,
    _lock: std::sync::Arc<std::fs::File>,
    live: Option<LiveService>,
    /// Only a successfully activated snapshot may supply a conditional token.
    current: Option<Current>,
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
            persist(&config.directory.join("revocation-floor.json"), &intent)?;
        }
        service.readiness = ReadinessMode::Warm;
        // No disk-cache restore/build path in this mode, including after restart.
        Ok(Self {
            config,
            service,
            source,
            _lock: std::sync::Arc::new(lock),
            live: None,
            current: None,
        })
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
            &self.config.directory,
            self.live.as_ref(),
            self.current.as_ref(),
            &self.config.network,
            &self.config.genesis_hash,
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
            live.fence_recovered(&floor(&self.config.directory)?.digests)?;
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
        let root = &self.config.directory;
        let network = &self.config.network;
        let genesis = &self.config.genesis_hash;
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
                        poll_snapshot(source, root, Some(&checked_live), None, network, genesis)
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
                            )
                            .await?;
                            return Err("candidate anchor is noncanonical".into());
                        }
                        keep.insert(shard.manifest_digest.clone());
                    }
                    if latest.revision > floor(root)?.revision {
                        checked_live.restore_verified(&keep)?;
                        let mut intent = floor(root)?;
                        intent.digests.retain(|digest| !keep.contains(digest));
                        persist(&root.join("revocation-floor.json"), &intent)?;
                    }
                    // Anchor checks cannot authorize a candidate across a source
                    // pointer/recovery change. Never use 304 for this final read.
                    let final_current =
                        poll_snapshot(source, root, Some(&checked_live), None, network, genesis)
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
        state.spawn_prewarm().await?;
        let live = LiveService::new_required(
            state,
            publication.clone(),
            self.service,
            options,
            record,
            self._lock.clone(),
        )?;
        live.fence_recovered(&floor(&self.config.directory)?.digests)?;
        let checked = live.clone();
        let source = &mut self.source;
        let root = &self.config.directory;
        let network = &self.config.network;
        let genesis = &self.config.genesis_hash;
        live.activate_checked(
            publication.map_sha256.clone(),
            publication.map_sha256,
            true,
            || async {
                let result: Result<(), Error> = async {
                    let latest =
                        poll_snapshot(source, root, Some(&checked), None, network, genesis)
                            .await?
                            .ok_or("missing recovery current")?;
                    if &latest != current {
                        return Err("recovery snapshot superseded".into());
                    }
                    let final_current =
                        poll_snapshot(source, root, Some(&checked), None, network, genesis)
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
#[derive(Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Revocations {
    revision: u64,
    digests: BTreeSet<String>,
}
fn floor(root: &Path) -> Result<Revocations, Error> {
    match std::fs::read(root.join("revocation-floor.json")) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Revocations::default()),
        Err(e) => Err(e.into()),
    }
}
async fn revoke(
    root: &Path,
    live: &LiveService,
    current: &Current,
    from: u64,
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
    revoke_digests(root, live, current, digests).await
}
async fn revoke_digests(
    root: &Path,
    live: &LiveService,
    current: &Current,
    digests: BTreeSet<String>,
) -> Result<(), Error> {
    let durable = (|| -> Result<(), Error> {
        let mut intent = floor(root)?;
        intent.revision = intent.revision.max(current.revision);
        intent.digests.extend(digests.iter().cloned());
        // Exact revocation intent precedes the owner's fence. Restart replays it
        // even if the process died before active.invalid.json reached disk.
        persist(&root.join("revocation-floor.json"), &intent)?;
        Ok(())
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
) -> Result<(), Error> {
    let Some(live) = live else {
        if !current.eligible() {
            let mut intent = floor(root)?;
            intent.revision = intent.revision.max(current.revision);
            persist(&root.join("revocation-floor.json"), &intent)?;
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
            revoke(root, live, current, from).await?;
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
        revoke_digests(root, live, current, digests).await?;
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
    root: &Path,
    live: Option<&LiveService>,
    previous: Option<&Current>,
    network: &str,
    genesis: &str,
) -> Result<Option<Current>, Error> {
    let Some(current) = source.poll(root, previous, network, genesis).await? else {
        return Ok(None);
    };
    let durable = source.observe(root, &current);
    let applied = apply_snapshot(source, root, live, &current).await;
    durable?;
    applied?;
    Ok(Some(current))
}
