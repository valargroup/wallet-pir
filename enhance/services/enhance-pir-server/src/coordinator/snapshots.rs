//! Persistent snapshot restoration and owned-artifact retention.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SavedSnapshot {
    pub(super) manifest: Manifest,
    pub(super) routes: BTreeMap<u64, Vec<String>>,
    pub(super) hints: BTreeMap<u64, String>,
    pub(super) domain_keys: BTreeMap<u64, String>,
}

pub(super) struct Snapshot {
    pub(super) saved: SavedSnapshot,
    pub(super) packing: BTreeMap<u64, Arc<PublishedPacking>>,
}

pub(super) fn hint_name(name: &str) -> bool {
    name.strip_suffix(".bin")
        .is_some_and(|stem| stem.len() == 64 && stem.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Remove only owned, unreferenced artifacts after validating the entire keep set.
/// The caller must hold publication exclusion and have no unresolved candidate.
pub(super) fn collect_artifacts(root: &FsPath, published: &[Manifest]) -> Result<(), String> {
    let mut snapshots = BTreeSet::new();
    let mut hints = BTreeSet::new();
    for manifest in published {
        let name = format!("{}.json", manifest.generation);
        let saved: SavedSnapshot = serde_json::from_slice(
            &fs::read(root.join("snapshots").join(&name)).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        if &saved.manifest != manifest
            || saved.hints.len() != manifest.coverage.shards.len()
            || manifest
                .coverage
                .shards
                .iter()
                .any(|s| !saved.hints.contains_key(&s.id))
        {
            return Err(
                "cannot collect artifacts: retained snapshot differs from durable decision".into(),
            );
        }
        for name in saved.hints.values() {
            if !hint_name(name) || !root.join("hints").join(name).is_file() {
                return Err("cannot collect artifacts: invalid or missing retained hint".into());
            }
            hints.insert(name.clone());
        }
        snapshots.insert(name);
    }
    for (directory, keep) in [
        ("snapshots", snapshots),
        ("hints", hints.clone()),
        ("public", hints),
    ] {
        let directory_path = root.join(directory);
        for entry in fs::read_dir(&directory_path).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            if !entry.file_type().map_err(|e| e.to_string())?.is_file() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let owned = if directory == "snapshots" {
                name.strip_suffix(".json")
                    .and_then(|s| s.parse::<u64>().ok())
                    .is_some_and(|g| name == format!("{g}.json"))
            } else {
                hint_name(&name)
            };
            if owned && !keep.contains(&name) {
                fs::remove_file(entry.path()).map_err(|e| e.to_string())?;
            }
        }
        File::open(directory_path)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub(super) fn restore(
    root: &FsPath,
    manifest: &Manifest,
    revoked: &BTreeSet<String>,
    cache: &mut BTreeMap<String, Arc<PublishedPacking>>,
    remote_packing: bool,
    budget: &crate::PackingBudget,
) -> Result<Arc<Snapshot>, String> {
    manifest.validate()?;
    let saved: SavedSnapshot = serde_json::from_slice(
        &fs::read(
            root.join("snapshots")
                .join(format!("{}.json", manifest.generation)),
        )
        .map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    if &saved.manifest != manifest {
        return Err("snapshot differs from durable decision".into());
    }
    let mut packing = BTreeMap::new();
    for shard in &manifest.coverage.shards {
        if revoked.contains(&hex::encode(manifest.session_id(shard.id)?)) {
            continue;
        }
        let key = &saved.domain_keys[&shard.id];
        if let Some(pack) = cache.get(key) {
            packing.insert(shard.id, pack.clone());
            continue;
        }
        let name = saved.hints.get(&shard.id).ok_or("missing persisted hint")?;
        if !hint_name(name) {
            return Err("invalid persisted hint identity".into());
        }
        let public_path = root.join("public").join(name);
        let pack = if remote_packing && public_path.is_file() {
            PublishedPacking::metadata(
                shard.logical_rows,
                fs::read(&public_path).map_err(|e| e.to_string())?,
            )
        } else {
            let params = parameters(shard.logical_rows)?;
            let file = File::open(root.join("hints").join(name)).map_err(|e| e.to_string())?;
            let blocks = crate::wire::read_crs_blocks(
                file,
                params.db_cols / runtime::rlwe().d,
                runtime::rlwe().d,
            )
            .map_err(|e| e.to_string())?;
            let serving = Packing::new(shard.logical_rows, &blocks, budget)?;
            crate::artifact::write_atomic(&root.join("public"), name, |f| {
                f.write_all(&serving.public)
            })
            .map_err(|e| e.to_string())?;
            PublishedPacking::new(shard.logical_rows, serving, !remote_packing)
        };
        if !manifest.sessions.contains(&pack.reference(shard.id)?) {
            return Err("restored session digest differs".into());
        }
        let pack = Arc::new(pack);
        cache.insert(key.clone(), pack.clone());
        packing.insert(shard.id, pack);
    }
    Ok(Arc::new(Snapshot { saved, packing }))
}
