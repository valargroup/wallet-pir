//! One-shot HTTPS bootstrap verification. No persisted download is serving
//! authority. This entry point intentionally cannot start a wallet listener:
//! fresh publisher canonicality attestation is checked around verification.
//! Continuous reconciliation/activation belongs to the manager.
use zakura_pir_protocol::transparent::filter as transparent_filter;

use crate::objects::{ensure_object, prepare_objects, stage_object};
#[cfg(test)]
use crate::objects::{link_object, remove_partials, PARTIAL};
pub(crate) use crate::source::sync_tree;
pub use crate::source::Config;
use crate::{
    history::service::{ServiceConfig, ServiceState},
    routing::router::{plan, RosterEntry},
    source::Source,
};
use serde::Serialize;
use std::{path::PathBuf, sync::Arc};
use transparent_shared::publication::{self, Current, Error, Inventory};
use transparent_shared::{
    history::{
        assignment::{GeneratedBy, WorkerRole},
        shardset::{LoadOptions, LoadScope, ShardSet},
    },
    runtime::disk::ShippedRuntimes,
};

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
pub async fn verify(config: Config) -> Result<Report, Error> {
    let mut source = Source::new(&config)?;
    source.discover().await?;
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
        let set = tokio::task::spawn_blocking(move || ShardSet::open_for_serving(&root, &options))
            .await??;
        let state = ServiceState::build_with_runtimes(
            set,
            ServiceConfig {
                cache_bytes: config.cache_bytes,
                ..ServiceConfig::default()
            },
            None,
            Some(ShippedRuntimes::required(stage.clone())),
        )?;
        let _prewarm = state.spawn_prewarm();
        state.check_required_material()?;
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
            downloaded_bytes: source.downloaded(),
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
    /// Wall time spent on the inventory request and artifact bodies; the rest
    /// of the stage is local copying, verification and syncing.
    pub transfer_seconds: f64,
    pub stage_seconds: f64,
}
pub(crate) async fn stage(
    config: &Config,
    source: &mut Source,
    current: &Current,
    cache: bool,
) -> Result<Staged, Error> {
    stage_holdings(config, source, current, cache, false).await
}
pub(crate) async fn stage_holdings(
    config: &Config,
    source: &mut Source,
    current: &Current,
    cache: bool,
    full: bool,
) -> Result<Staged, Error> {
    let started = std::time::Instant::now();
    let raw = source
        .small(
            &format!("inventories/{}", current.inventory_checksum),
            16 << 20,
        )
        .await?;
    if publication::digest(&raw) != current.inventory_checksum {
        return Err("inventory digest mismatch".into());
    }
    let mut transfer = started.elapsed();
    let inventory: Inventory = serde_json::from_slice(&raw)?;
    inventory.validate(&config.network, &config.genesis_hash)?;
    let total = inventory
        .artifacts
        .iter()
        .filter(|f| full || f.kind.local())
        .try_fold(source.downloaded(), |n, f| n.checked_add(f.bytes))
        .ok_or("download budget overflow")?;
    if !cache && total > config.max_download_bytes {
        return Err("prepared publication exceeds download budget".into());
    }
    // New private staging tree; only verified immutable objects enter the
    // sibling source catalog. Failed trees remain non-authoritative.
    // Validate the cache before enumerating it: a symlinked object directory
    // is refused without reading or cleaning its target.
    let objects = if cache {
        Some(prepare_objects(&config.directory)?)
    } else {
        None
    };
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
                .filter(|f| (full || f.kind.local()) && f.path != "shards.json"),
        ) {
            let target = stage.join(&artifact.path);
            if let Some(objects) = &objects {
                if ensure_object(source, objects, artifact, &mut transfer).await? {
                    downloaded_files += 1;
                }
                stage_object(
                    &config.directory,
                    &stage,
                    &objects.join(&artifact.checksum),
                    artifact,
                )?;
            } else {
                std::fs::create_dir_all(target.parent().ok_or("artifact has no parent")?)?;
                let fetching = std::time::Instant::now();
                let mut output = tokio::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&target)
                    .await?;
                let response = source
                    .get(&format!("artifacts/{}", artifact.checksum))
                    .await?;
                let verification = if artifact.kind == publication::Kind::Metadata {
                    pir_sync_manager::Verification::Artifact(&artifact.checksum)
                } else {
                    pir_sync_manager::Verification::Length
                };
                response
                    .copy_to(&mut output, artifact.bytes, verification)
                    .await?;
                transfer += fetching.elapsed();
                output.sync_all().await?;
                drop(output);
                downloaded_files += 1;
                publication::record_received_file(&target, artifact, &target)?;
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
        if publication::protocol_digest(&serde_json::to_vec(&map)?) != inventory.map_sha256 {
            return Err("downloaded map has another publication identity".into());
        }
        let options = holdings(config, &map, &inventory, full)?;
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
            transfer_seconds: transfer.as_secs_f64(),
            stage_seconds: started.elapsed().as_secs_f64(),
        })
    }
    .await;
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&stage);
    }
    result
}

/// Holdings selected for this provider by the production assignment planner,
/// with an explicitly inert archive row for cost/accounting only. Nothing here
/// qualifies routes. Deterministic for one map, inventory and configuration.
pub(crate) fn holdings(
    config: &Config,
    map: &transparent_filter::ShardMap,
    inventory: &Inventory,
    full: bool,
) -> Result<LoadOptions, Error> {
    Ok(if full {
        LoadOptions::whole(0)
    } else {
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
            map,
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
        LoadOptions {
            scope: LoadScope::Assigned {
                assignment: Arc::new(assignment),
                worker_id: "provider".into(),
            },
            ..LoadOptions::whole(0)
        }
    })
}

#[cfg(test)]
mod object_tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    fn object(root: &std::path::Path, body: &[u8]) -> (PathBuf, publication::Artifact) {
        let artifact = publication::Artifact {
            source_sha256: None,
            path: "shards.json".into(),
            checksum: publication::digest(body),
            bytes: body.len() as u64,
            kind: publication::Kind::Metadata,
        };
        let path = root.join(&artifact.checksum);
        std::fs::write(&path, body).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&path, permissions).unwrap();
        (path, artifact)
    }

    #[test]
    fn staged_objects_share_one_read_only_inode() {
        let root = tempfile::tempdir().unwrap();
        let (path, artifact) = object(root.path(), b"immutable");
        let target = root.path().join("stage");
        link_object(&path, &target, &artifact, |a, b| std::fs::hard_link(a, b)).unwrap();
        let (a, b) = (
            std::fs::metadata(&path).unwrap(),
            std::fs::metadata(&target).unwrap(),
        );
        assert_eq!((a.dev(), a.ino()), (b.dev(), b.ino()));
        assert!(b.permissions().readonly());
        assert!(std::fs::OpenOptions::new()
            .write(true)
            .open(&target)
            .is_err());
        // Cleanup unlinks the stage entry and leaves the cached object intact.
        std::fs::remove_file(&target).unwrap();
        publication::verify_file(&path, &artifact).unwrap();
    }

    #[test]
    fn only_unsupported_links_fall_back_to_a_size_checked_copy() {
        let root = tempfile::tempdir().unwrap();
        let (path, artifact) = object(root.path(), b"immutable");
        let unsupported = || std::io::Error::from(std::io::ErrorKind::Unsupported);
        let copy = root.path().join("copy");
        link_object(&path, &copy, &artifact, |_, _| Err(unsupported())).unwrap();
        assert_eq!(std::fs::read(&copy).unwrap(), b"immutable");
        assert_ne!(
            std::fs::metadata(&copy).unwrap().ino(),
            std::fs::metadata(&path).unwrap().ino()
        );
        let mut wrong = artifact.clone();
        wrong.bytes += 1;
        assert!(
            link_object(&path, &root.path().join("bad"), &wrong, |_, _| Err(
                unsupported()
            ))
            .is_err()
        );
        assert!(
            link_object(&path, &root.path().join("denied"), &artifact, |_, _| Err(
                std::io::Error::from(std::io::ErrorKind::PermissionDenied)
            ))
            .is_err()
        );
        assert!(
            !root.path().join("denied").exists(),
            "no copy on other errors"
        );
    }

    #[test]
    fn interrupted_partials_are_removed_and_never_reused() {
        let root = tempfile::tempdir().unwrap();
        let (path, _) = object(root.path(), b"complete");
        let partial = root.path().join(format!("{}{PARTIAL}1-2", "11".repeat(32)));
        let legacy = root.path().join(format!("{}.partial", "22".repeat(32)));
        std::fs::write(&partial, b"x").unwrap();
        std::fs::write(&legacy, b"x").unwrap();
        remove_partials(root.path()).unwrap();
        assert!(!partial.exists() && !legacy.exists() && path.exists());
        remove_partials(&root.path().join("missing")).unwrap();
    }

    #[test]
    fn symlinked_object_directory_is_refused_before_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let sentinel = outside
            .path()
            .join(format!("{}{PARTIAL}1-2", "11".repeat(32)));
        std::fs::write(&sentinel, b"outside").unwrap();
        std::os::unix::fs::symlink(
            outside.path(),
            root.path().join(pir_artifact::namespace("objects")),
        )
        .unwrap();
        assert!(prepare_objects(root.path()).is_err());
        assert_eq!(std::fs::read(&sentinel).unwrap(), b"outside");
        let real = tempfile::tempdir().unwrap();
        let objects = prepare_objects(real.path()).unwrap();
        assert!(std::fs::symlink_metadata(objects).unwrap().is_dir());
    }

    #[tokio::test]
    async fn writable_cached_object_is_made_read_only_before_linking() {
        let root = tempfile::tempdir().unwrap();
        let objects = prepare_objects(root.path()).unwrap();
        let (path, artifact) = object(&objects, b"legacy");
        // An owner-writable object, as an older cache left it.
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        // An unreachable publisher proves the valid object is reused.
        let mut source = Source::new(&Config {
            publisher: "https://publisher.invalid".into(),
            network: "main".into(),
            genesis_hash: "00".repeat(32),
            directory: root.path().into(),
            cache_bytes: 0,
            max_download_bytes: 1024,
            roots: Vec::new(),
        })
        .unwrap();
        let mut transfer = std::time::Duration::ZERO;
        assert!(
            !ensure_object(&mut source, &objects, &artifact, &mut transfer)
                .await
                .unwrap()
        );
        let target = root.path().join("stage");
        link_object(&path, &target, &artifact, |a, b| std::fs::hard_link(a, b)).unwrap();
        assert!(std::fs::metadata(&target).unwrap().permissions().readonly());
        assert!(std::fs::OpenOptions::new()
            .write(true)
            .open(&target)
            .is_err());
    }
}
